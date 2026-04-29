#!/usr/bin/env python3
"""Aggregate scored market-insight samples into direction cards and reports."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

from src.age_structure import build_category_topn_age_structure, build_direction_topn_age_structure, classify_new_product_entry_signal
from src.direction_confidence import (
    calculate_completeness_score,
    calculate_consistency_score,
    calculate_sample_score,
    compose_decision_confidence,
)
from src.direction_metrics import extract_historical_sales_value, seasonal_trend_from_ratio
from src.market_insight_models import MarketDirectionCard, ScoredProductSnapshot, ShopLandscapeSummary, VOCLightSummary


HAIR_STYLE_CLUSTER_ALIASES = {
    "基础通勤抓夹": "基础通勤型",
    "大体量气质抓夹": "大体量气质型",
    "韩系轻通勤": "韩系轻通勤型",
    "甜感装饰发夹": "甜感装饰型",
    "发圈套组": "发圈套组型",
    "发箍修饰型": "发箍修饰型",
    "盘发效率型": "盘发效率型",
    "头盔友好整理型": "头盔友好整理型",
    "少女礼物感": "少女礼物感型",
    "other": "other",
}


class MarketInsightAggregator(object):
    def __init__(self, taxonomy_dir: Optional[Path] = None):
        self.taxonomy_dir = Path(taxonomy_dir) if taxonomy_dir else Path(__file__).resolve().parents[1] / "configs" / "market_insight_taxonomies"
        self._direction_family_map_cache: Dict[str, Dict[str, str]] = {}
        self._taxonomy_cache: Dict[str, Dict[str, Any]] = {}
        self._decision_rules_cache: Dict[str, Any] = {}

    def build_direction_cards(self, scored_items: Iterable[ScoredProductSnapshot]) -> List[MarketDirectionCard]:
        groups: Dict[str, List[ScoredProductSnapshot]] = defaultdict(list)
        scored_list = self._dedupe_scored_items(scored_items)
        for item in scored_list:
            if not item.tag.is_valid_sample:
                continue
            style_cluster = self._normalized_style_cluster(item)
            groups[style_cluster].append(item)

        cards = []
        for style_cluster, items in groups.items():
            exemplar_items = sorted(items, key=lambda item: (-item.heat_score, item.snapshot.rank_index))
            first = exemplar_items[0]
            top_silhouette_forms = self._top_silhouette_forms(items, top_k=3)
            silhouette_distribution_by_count = self._silhouette_distribution_by_count(items)
            silhouette_distribution_by_sales = self._silhouette_distribution_by_sales(items)
            top_length_forms = self._top_length_forms(items, top_k=3)
            length_distribution_by_count = self._length_distribution_by_count(items)
            length_distribution_by_sales = self._length_distribution_by_sales(items)
            core_elements = self._top_values(items, lambda entry: entry.tag.element_tags, 3)
            scene_tags = self._top_values(items, lambda entry: entry.tag.scene_tags, 3)
            top_value_points = self._top_values(items, lambda entry: entry.tag.value_points, 3)
            price_bands = self._top_values(items, lambda entry: [entry.target_price_band], 3)
            avg_heat = round(sum(item.heat_score for item in items) / len(items), 2)
            avg_crowd = round(sum(item.crowd_score for item in items) / len(items), 2)
            direction_item_count = len(items)
            direction_sales_median_7d = round(self._median([item.snapshot.sales_7d for item in items]), 2)
            demand_structure = self._demand_structure(items)
            price_band_analysis = self._price_band_analysis(items, scored_list)
            direction_video_density_avg = round(
                sum(self._video_density(item) for item in items) / max(direction_item_count, 1),
                4,
            )
            direction_creator_density_avg = round(
                sum(self._creator_density(item) for item in items) / max(direction_item_count, 1),
                4,
            )
            heat_level = self._score_level(avg_heat)
            crowd_level = self._score_level(avg_crowd)
            priority_level = self._priority_level(heat_level, crowd_level, direction_item_count)
            direction_family = self._resolve_direction_family(category=first.snapshot.category, style_cluster=style_cluster)
            seasonal_trend_short, seasonal_trend_long, seasonal_trend = self._seasonal_trends(
                items,
                category=first.snapshot.category,
                style_cluster=style_cluster,
            )
            content_efficiency_signal, content_efficiency_source = self._aggregate_content_efficiency(items)
            direction_canonical_key = self._direction_canonical_key(
                country=first.snapshot.country,
                category=first.snapshot.category,
                style_cluster=style_cluster,
            )
            representative_products = [
                {
                    "product_id": item.snapshot.product_id,
                    "product_name": item.snapshot.product_name,
                }
                for item in exemplar_items[:3]
            ]
            cards.append(
                MarketDirectionCard(
                    direction_canonical_key=direction_canonical_key,
                    direction_instance_id=self._direction_instance_id(
                        batch_date=first.snapshot.batch_date,
                        direction_canonical_key=direction_canonical_key,
                    ),
                    batch_date=first.snapshot.batch_date,
                    country=first.snapshot.country,
                    category=first.snapshot.category,
                    direction_name=style_cluster,
                    style_cluster=style_cluster,
                    direction_family=direction_family,
                    direction_item_count=direction_item_count,
                    direction_sales_median_7d=direction_sales_median_7d,
                    direction_video_density_avg=direction_video_density_avg,
                    direction_creator_density_avg=direction_creator_density_avg,
                    seasonal_trend=seasonal_trend,
                    seasonal_trend_short=seasonal_trend_short,
                    seasonal_trend_long=seasonal_trend_long,
                    content_efficiency_signal=content_efficiency_signal,
                    content_efficiency_source=content_efficiency_source,
                    sample_count=direction_item_count,
                    sample_confidence=str(demand_structure.get("sample_confidence") or ""),
                    demand_structure=demand_structure,
                    competition_structure={
                        "item_count": direction_item_count,
                        "video_density": direction_video_density_avg,
                        "creator_density": direction_creator_density_avg,
                        "direction_tier": "",
                        "competition_summary": "",
                    },
                    price_band_analysis=price_band_analysis,
                    top_forms=top_silhouette_forms,
                    form_distribution=silhouette_distribution_by_count,
                    form_distribution_by_count=silhouette_distribution_by_count,
                    form_distribution_by_sales=silhouette_distribution_by_sales,
                    top_silhouette_forms=top_silhouette_forms,
                    top_length_forms=top_length_forms,
                    silhouette_distribution_by_count=silhouette_distribution_by_count,
                    silhouette_distribution_by_sales=silhouette_distribution_by_sales,
                    length_distribution_by_count=length_distribution_by_count,
                    length_distribution_by_sales=length_distribution_by_sales,
                    core_elements=core_elements,
                    scene_tags=scene_tags,
                    target_price_bands=price_bands,
                    heat_level=heat_level,
                    crowd_level=crowd_level,
                    top_value_points=top_value_points,
                    default_content_route_preference="neutral",
                    representative_products=representative_products,
                    priority_level=priority_level,
                    selection_advice=self._selection_advice(
                        category=first.snapshot.category,
                        style_cluster=style_cluster,
                        top_forms=top_silhouette_forms,
                        top_value_points=top_value_points,
                        core_elements=core_elements,
                        scene_tags=scene_tags,
                        priority_level=priority_level,
                    ),
                    avoid_notes=self._avoid_notes(
                        category=first.snapshot.category,
                        style_cluster=style_cluster,
                        crowd_level=crowd_level,
                        top_forms=top_silhouette_forms,
                        core_elements=core_elements,
                    ),
                    confidence=self._confidence(items, avg_heat, avg_crowd),
                    product_count=direction_item_count,
                    market_id=first.snapshot.market_id or first.snapshot.country,
                    category_id=first.snapshot.category_id or first.snapshot.category,
                    market_category_profile_version=getattr(first, "market_category_profile_version", "") or "",
                    average_heat_score=avg_heat,
                    average_crowd_score=avg_crowd,
                    direction_key=direction_canonical_key,
                )
            )
        self._apply_family_tiers(cards)
        self._propagate_direction_fields_to_products(scored_list, cards)
        self._apply_age_structures(scored_list, cards)
        cards.sort(key=lambda card: (-self._priority_sort(card.priority_level), -card.average_heat_score, card.direction_name))
        return cards

    def build_shop_landscape_summary(
        self,
        snapshots,
        batch_date: str,
        country: str,
        category: str,
    ) -> ShopLandscapeSummary:
        total_shops = len(snapshots)
        top_gmv = sorted((item.gmv_7d for item in snapshots), reverse=True)
        top_share = 0.0
        if top_gmv and sum(top_gmv) > 0:
            top_share = sum(top_gmv[:3]) / sum(top_gmv)
        active_counts = [item.active_product_count or 0.0 for item in snapshots]
        creator_counts = [item.creator_count for item in snapshots]
        new_product_shares = [item.new_product_share for item in snapshots if item.new_product_share is not None]
        competition_pattern = "少数强店吃量" if top_share >= 0.55 else "多店分散竞争"
        sku_structure_level = "集中" if active_counts and (sum(active_counts) / max(len(active_counts), 1)) <= 20 else "分散"
        creator_density_level = self._score_level(sum(creator_counts) / max(total_shops, 1))
        new_product_share_level = self._score_level((sum(new_product_shares) / max(len(new_product_shares), 1)) * 100.0) if new_product_shares else "unknown"
        head_store_pattern = "品牌店/成熟店为主" if total_shops and top_share >= 0.55 else "零售商/多店混战为主"
        return ShopLandscapeSummary(
            batch_date=batch_date,
            country=country,
            category=category,
            input_mode="shop_ranking",
            head_store_pattern=head_store_pattern,
            new_product_share_level=new_product_share_level,
            sku_structure_level=sku_structure_level,
            creator_density_level=creator_density_level,
            competition_pattern=competition_pattern,
            summary_lines=[
                "头部店铺格局：{value}".format(value=head_store_pattern),
                "新品成交占比：{value}".format(value=new_product_share_level),
                "动销 SKU 结构：{value}".format(value=sku_structure_level),
                "达人带货密度：{value}".format(value=creator_density_level),
                "竞争形态：{value}".format(value=competition_pattern),
            ],
        )

    def build_report_payload(
        self,
        scored_items: Iterable[ScoredProductSnapshot],
        cards: Iterable[MarketDirectionCard],
        voc_summary: VOCLightSummary,
    ) -> Dict[str, object]:
        scored_list = list(scored_items)
        card_list = list(cards)
        style_counter = Counter(self._normalized_style_cluster(item) for item in scored_list if item.tag.is_valid_sample)
        value_point_counter = Counter()
        for item in scored_list:
            value_point_counter.update(item.tag.value_points)
        high_heat_low_crowd = [card.to_dict() for card in card_list if card.heat_level == "high" and card.crowd_level in {"low", "medium"}][:5]
        high_heat_high_crowd = [card.to_dict() for card in card_list if card.heat_level == "high" and card.crowd_level == "high"][:5]
        testable = [card.to_dict() for card in card_list if card.priority_level == "high"][:5]
        hold = [card.to_dict() for card in card_list if card.priority_level == "low"][:5]
        direct_actions = []
        for card in card_list[:5]:
            direct_actions.append(
                {
                    "direction_canonical_key": card.direction_canonical_key,
                    "selection_advice": card.selection_advice,
                    "avoid_notes": card.avoid_notes,
                }
            )
        representative_samples = [
            {
                "direction_canonical_key": card.direction_canonical_key,
                "representative_products": card.representative_products,
            }
            for card in card_list[:5]
        ]
        return {
            "top_styles": [{"name": name, "count": count} for name, count in style_counter.most_common(3)],
            "top_value_points": [{"name": name, "count": count} for name, count in value_point_counter.most_common(3)],
            "high_heat_low_crowd_directions": high_heat_low_crowd,
            "high_heat_high_crowd_directions": high_heat_high_crowd,
            "testable_directions": testable,
            "hold_directions": hold,
            "voc_summary": voc_summary.to_dict(),
            "phase2_actions": direct_actions,
            "representative_samples": representative_samples,
        }

    def render_report_markdown(
        self,
        report_payload: Dict[str, object],
        cards: Iterable[MarketDirectionCard],
    ) -> str:
        card_list = list(cards)
        lines = ["# Market Insight Report", ""]
        lines.append("## 1. 本期主流风格 Top 3")
        for item in report_payload.get("top_styles", []):
            lines.append("- {name}（{count}）".format(**item))
        lines.extend(["", "## 2. 本期核心价值点 Top 3"])
        for item in report_payload.get("top_value_points", []):
            lines.append("- {name}（{count}）".format(**item))
        lines.extend(["", "## 3. 高热低卷方向"])
        for item in report_payload.get("high_heat_low_crowd_directions", []):
            lines.append("- {direction_name}：{selection_advice}".format(**item))
        lines.extend(["", "## 4. 高热高卷方向"])
        for item in report_payload.get("high_heat_high_crowd_directions", []):
            lines.append("- {direction_name}：{avoid_notes}".format(**item))
        lines.extend(["", "## 5. 可测试方向"])
        for item in report_payload.get("testable_directions", []):
            lines.append("- {direction_name}：{selection_advice}".format(**item))
        lines.extend(["", "## 6. 暂不建议重压方向"])
        for item in report_payload.get("hold_directions", []):
            lines.append("- {direction_name}：{avoid_notes}".format(**item))
        lines.extend(["", "## 7. 可选 VOC 摘要"])
        voc_summary = report_payload.get("voc_summary", {}) or {}
        lines.append("- voc_status: {status}".format(status=voc_summary.get("voc_status", "skipped")))
        for key in ("positive_needs", "pain_points", "objections", "scene_keywords", "top_user_words"):
            values = voc_summary.get(key) or []
            if values:
                lines.append("- {key}: {values}".format(key=key, values="、".join(values)))
        lines.extend(["", "## 8. 对阶段 2 选品的直接动作建议"])
        for item in report_payload.get("phase2_actions", []):
            lines.append("- {selection_advice}；少选：{avoid_notes}".format(**item))
        lines.extend(["", "## 9. 代表样本列表"])
        for card in card_list[:5]:
            form_text = "、".join(card.top_forms[:2]) if card.top_forms else "未聚焦形态"
            lines.append(
                "- {name}（主要承载：{forms}）: {products}".format(
                    name=card.direction_name,
                    forms=form_text,
                    products=" / ".join(
                        [str(item.get("product_name") or "") for item in (card.representative_products or []) if str(item.get("product_name") or "").strip()]
                    ),
                )
            )
        lines.append("")
        return "\n".join(lines).strip() + "\n"

    def _direction_canonical_key(self, country: str, category: str, style_cluster: str) -> str:
        return "__".join([part for part in [country, category, style_cluster] if str(part or "").strip()])

    def _direction_instance_id(self, batch_date: str, direction_canonical_key: str) -> str:
        return "{batch_date}__{canonical}".format(batch_date=batch_date, canonical=direction_canonical_key)

    def _top_values(self, items: Iterable[ScoredProductSnapshot], getter, top_k: int) -> List[str]:
        counter = Counter()
        for item in items:
            counter.update([value for value in getter(item) if value and value != "other"])
        if not counter:
            counter.update(["other"])
        return [name for name, _ in counter.most_common(top_k)]

    def _top_silhouette_forms(self, items: Iterable[ScoredProductSnapshot], top_k: int) -> List[str]:
        return self._top_values(items, lambda entry: [self._normalized_product_form(entry)], top_k=top_k)

    def _top_length_forms(self, items: Iterable[ScoredProductSnapshot], top_k: int) -> List[str]:
        return self._top_values(items, lambda entry: [self._normalized_length_form(entry)], top_k=top_k)

    def _silhouette_distribution_by_count(self, items: Iterable[ScoredProductSnapshot]) -> Dict[str, float]:
        counter = Counter(self._normalized_product_form(item) for item in items)
        total = sum(counter.values()) or 1
        return {name: round(count / total, 4) for name, count in counter.most_common()}

    def _silhouette_distribution_by_sales(self, items: Iterable[ScoredProductSnapshot]) -> Dict[str, float]:
        totals: Dict[str, float] = defaultdict(float)
        for item in items:
            totals[self._normalized_product_form(item)] += max(float(item.snapshot.sales_7d or 0.0), 0.0)
        grand_total = sum(totals.values()) or 1.0
        ordered = sorted(totals.items(), key=lambda pair: (-pair[1], pair[0]))
        return {name: round(value / grand_total, 4) for name, value in ordered}

    def _length_distribution_by_count(self, items: Iterable[ScoredProductSnapshot]) -> Dict[str, float]:
        counter = Counter(self._normalized_length_form(item) for item in items)
        total = sum(counter.values()) or 1
        return {name: round(count / total, 4) for name, count in counter.most_common()}

    def _length_distribution_by_sales(self, items: Iterable[ScoredProductSnapshot]) -> Dict[str, float]:
        totals: Dict[str, float] = defaultdict(float)
        for item in items:
            totals[self._normalized_length_form(item)] += max(float(item.snapshot.sales_7d or 0.0), 0.0)
        grand_total = sum(totals.values()) or 1.0
        ordered = sorted(totals.items(), key=lambda pair: (-pair[1], pair[0]))
        return {name: round(value / grand_total, 4) for name, value in ordered}

    def _normalized_style_cluster(self, item: ScoredProductSnapshot) -> str:
        raw = str(item.tag.style_cluster or item.tag.style_tag_main or "").strip()
        category = str(item.snapshot.category or "").strip()
        if category == "hair_accessory":
            return HAIR_STYLE_CLUSTER_ALIASES.get(raw, raw or "other")
        taxonomy = self._load_taxonomy(category)
        style_aliases = taxonomy.get("style_aliases") or {}
        if isinstance(style_aliases, dict):
            normalized = str(style_aliases.get(raw) or raw or "other").strip()
            return normalized or "other"
        return raw or "other"

    def _normalized_product_form(self, item: ScoredProductSnapshot) -> str:
        text = str(item.tag.product_form or item.tag.product_form_or_result or "").strip()
        if text in {"", "other", "unclear"}:
            return "other"
        return text

    def _normalized_length_form(self, item: ScoredProductSnapshot) -> str:
        text = str(getattr(item.tag, "length_form", "") or "").strip()
        if text in {"", "other", "unclear"}:
            return "other"
        return text

    def _resolve_direction_family(self, category: str, style_cluster: str) -> str:
        family_map = self._load_direction_family_map(category)
        return str(family_map.get(style_cluster) or "other")

    def _load_taxonomy(self, category: str) -> Dict[str, Any]:
        normalized = str(category or "").strip()
        if normalized not in self._taxonomy_cache:
            file_path = self.taxonomy_dir / "{category}_v1.json".format(category=normalized)
            payload: Dict[str, Any] = {}
            if file_path.exists():
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            self._taxonomy_cache[normalized] = payload
        return self._taxonomy_cache[normalized]

    def _load_direction_family_map(self, category: str) -> Dict[str, str]:
        normalized = str(category or "").strip()
        if normalized not in self._direction_family_map_cache:
            mapping: Dict[str, str] = {}
            file_path = self.taxonomy_dir / "{category}_v1.json".format(category=normalized)
            if file_path.exists():
                payload = json.loads(file_path.read_text(encoding="utf-8"))
                raw_mapping = payload.get("direction_family_map") or {}
                if isinstance(raw_mapping, dict):
                    mapping = {str(key or "").strip(): str(value or "").strip() for key, value in raw_mapping.items() if str(key or "").strip()}
            self._direction_family_map_cache[normalized] = mapping
        return self._direction_family_map_cache[normalized]

    def _decision_rules(self, country: str, category: str) -> Dict[str, Any]:
        cache_key = "__".join([str(country or "").strip(), str(category or "").strip()])
        if cache_key not in self._decision_rules_cache:
            config_path = self.taxonomy_dir.parent / "market_insight_decision_rules.yaml"
            payload: Dict[str, Any] = {
                "default": {
                    "sales_action_threshold": 250,
                    "min_sample": {"insufficient": 5, "distribution_metrics": 8, "p75": 12, "p90": 20},
                },
                "overrides": {},
            }
            if config_path.exists():
                loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
                if isinstance(loaded, dict):
                    self._deep_update(payload, loaded)
            rules = json.loads(json.dumps(payload.get("default") or {}, ensure_ascii=False))
            overrides = payload.get("overrides") or {}
            country_payload = overrides.get(str(country or "").strip()) or {}
            category_payload = country_payload.get(str(category or "").strip()) or {}
            if isinstance(category_payload, dict):
                self._deep_update(rules, category_payload)
            self._decision_rules_cache[cache_key] = rules
        return self._decision_rules_cache[cache_key]

    def _product_age_rules(self, country: str, category: str) -> Dict[str, Any]:
        rules = self._decision_rules(country=country, category=category)
        config_path = self.taxonomy_dir.parent / "market_insight_decision_rules.yaml"
        payload: Dict[str, Any] = {}
        if config_path.exists():
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            if isinstance(loaded, dict):
                payload = loaded
        return dict(payload.get("product_age") or {})

    def _deep_update(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        for key, value in dict(source or {}).items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                self._deep_update(target[key], value)
            else:
                target[key] = value

    def _apply_family_tiers(self, cards: List[MarketDirectionCard]) -> None:
        category_groups: Dict[str, List[MarketDirectionCard]] = defaultdict(list)
        for card in cards:
            category_groups[str(card.category or "").strip()].append(card)
        for category, category_cards in category_groups.items():
            family_groups: Dict[str, List[MarketDirectionCard]] = defaultdict(list)
            for card in category_cards:
                family_groups[str(card.direction_family or "other")].append(card)
            category_p75_sales = self._percentile([card.direction_sales_median_7d for card in category_cards], 0.75)
            category_p75_video = self._percentile([card.direction_video_density_avg for card in category_cards], 0.75)
            category_p75_creator = self._percentile([card.direction_creator_density_avg for card in category_cards], 0.75)
            for family_cards in family_groups.values():
                family_item_count = sum(max(int(card.direction_item_count or 0), 0) for card in family_cards)
                low_sample_min_count = self._low_sample_min_count(category)
                p50_sales_median = self._percentile([card.direction_sales_median_7d for card in family_cards], 0.50)
                family_p75_sales = self._percentile([card.direction_sales_median_7d for card in family_cards], 0.75)
                family_p75_video = self._percentile([card.direction_video_density_avg for card in family_cards], 0.75)
                family_p75_creator = self._percentile([card.direction_creator_density_avg for card in family_cards], 0.75)
                for card in family_cards:
                    item_share_in_family = (float(card.direction_item_count or 0) / float(family_item_count or 1))
                    if category == "light_tops":
                        effective_p75_sales = family_p75_sales if len(family_cards) >= 2 else category_p75_sales
                        effective_p75_video = family_p75_video if len(family_cards) >= 2 else category_p75_video
                        effective_p75_creator = family_p75_creator if len(family_cards) >= 2 else category_p75_creator
                        card.direction_tier = self._calculate_light_top_direction_tier(
                            item_count=int(card.direction_item_count or 0),
                            item_share_in_family=item_share_in_family,
                            sales_median_7d=float(card.direction_sales_median_7d or 0.0),
                            video_density_avg=float(card.direction_video_density_avg or 0.0),
                            creator_density_avg=float(card.direction_creator_density_avg or 0.0),
                            family_p75_sales=effective_p75_sales,
                            family_p75_video=effective_p75_video,
                            family_p75_creator=effective_p75_creator,
                            taxonomy=self._load_taxonomy(category),
                        )
                    else:
                        card.direction_tier = self._calculate_direction_tier(
                            item_count=int(card.direction_item_count or 0),
                            item_share_in_family=item_share_in_family,
                            sales_median_7d=float(card.direction_sales_median_7d or 0.0),
                            video_density_avg=float(card.direction_video_density_avg or 0.0),
                            creator_density_avg=float(card.direction_creator_density_avg or 0.0),
                            p50_sales_median=p50_sales_median,
                            p75_sales_median=family_p75_sales,
                            p75_video_density=family_p75_video,
                            p75_creator_density=family_p75_creator,
                        )
                    confidence_sample_score = calculate_sample_score(
                        item_count=int(card.direction_item_count or 0),
                        low_sample_min_count=low_sample_min_count,
                    )
                    consistency_result = calculate_consistency_score(
                        direction_tier=card.direction_tier,
                        sales_median_7d=float(card.direction_sales_median_7d or 0.0),
                        video_density_avg=float(card.direction_video_density_avg or 0.0),
                        creator_density_avg=float(card.direction_creator_density_avg or 0.0),
                        p75_sales_median=family_p75_sales,
                        p75_video_density=family_p75_video,
                        p75_creator_density=family_p75_creator,
                        seasonal_trend_short=card.seasonal_trend_short,
                        seasonal_trend_long=card.seasonal_trend_long,
                    )
                    completeness_result = calculate_completeness_score(
                        content_efficiency_source=card.content_efficiency_source,
                        seasonal_trend_short=card.seasonal_trend_short,
                        seasonal_trend_long=card.seasonal_trend_long,
                    )
                    reason_tags = []
                    if confidence_sample_score == 0:
                        reason_tags.append("sample_low")
                    reason_tags.extend(list(consistency_result.get("reason_tags") or []))
                    reason_tags.extend(list(completeness_result.get("reason_tags") or []))
                    composed_confidence = compose_decision_confidence(
                        confidence_sample_score=confidence_sample_score,
                        confidence_consistency_score=int(consistency_result.get("score") or 0),
                        confidence_completeness_score=int(completeness_result.get("score") or 0),
                        reason_tags=reason_tags,
                    )
                    card.confidence_sample_score = confidence_sample_score
                    card.confidence_consistency_score = int(consistency_result.get("score") or 0)
                    card.confidence_completeness_score = int(completeness_result.get("score") or 0)
                    card.decision_confidence = str(composed_confidence.get("decision_confidence") or "low")
                    card.confidence_reason_tags = list(composed_confidence.get("confidence_reason_tags") or [])
                    card.default_content_route_preference = self._default_content_route_preference(card.direction_tier)
                    card.competition_structure = {
                        "item_count": int(card.direction_item_count or 0),
                        "video_density": float(card.direction_video_density_avg or 0.0),
                        "creator_density": float(card.direction_creator_density_avg or 0.0),
                        "direction_tier": card.direction_tier,
                        "competition_summary": "方向处于 {tier} 数据状态；该状态代表竞争结构，不直接等同业务动作。".format(
                            tier=card.direction_tier or "unknown"
                        ),
                    }

    def _propagate_direction_fields_to_products(
        self,
        scored_items: List[ScoredProductSnapshot],
        cards: List[MarketDirectionCard],
    ) -> None:
        card_map = {card.style_cluster: card for card in cards}
        for item in scored_items:
            if not item.tag.is_valid_sample:
                continue
            style_cluster = self._normalized_style_cluster(item)
            card = card_map.get(style_cluster)
            if not card:
                continue
            item.direction_canonical_key = card.direction_canonical_key
            item.direction_family = card.direction_family
            item.direction_tier = card.direction_tier
            item.seasonal_trend = card.seasonal_trend
            item.seasonal_trend_short = card.seasonal_trend_short
            item.seasonal_trend_long = card.seasonal_trend_long
            item.content_efficiency_signal = card.content_efficiency_signal
            item.content_efficiency_source = card.content_efficiency_source
            item.default_content_route_preference = card.default_content_route_preference

    def _apply_age_structures(self, scored_items: List[ScoredProductSnapshot], cards: List[MarketDirectionCard]) -> None:
        if not cards:
            return
        first = cards[0]
        config = self._product_age_rules(country=first.country, category=first.category)
        top_n = int(config.get("top_n", 300) or 300)
        valid_items = [item for item in scored_items if item.tag.is_valid_sample]
        for item in valid_items:
            age_days = item.snapshot.product_age_days
            if age_days is None and item.snapshot.listing_days is not None:
                age_days = int(item.snapshot.listing_days)
            item.product_age_days = age_days
            if age_days is None:
                item.age_bucket = "unknown"
                item.listing_date_parse_status = "missing"
            else:
                item.age_bucket = self._age_bucket(age_days, config)
                item.listing_date_parse_status = "success"
        category_topn = sorted(
            valid_items,
            key=lambda item: (-float(item.snapshot.sales_7d or 0.0), int(item.snapshot.rank_index or 0)),
        )[:top_n]
        category_age_structure = build_category_topn_age_structure(category_topn, top_n=top_n, config=config)
        for card in cards:
            structure = build_direction_topn_age_structure(
                category_topn_items=category_topn,
                direction_id=card.direction_canonical_key,
                top_n=top_n,
                config=config,
            )
            structure["category_top300_age_structure"] = category_age_structure
            signal = classify_new_product_entry_signal(structure, config=config)
            card.product_age_structure = structure
            card.new_product_entry_signal = signal

    def _age_bucket(self, age_days: Optional[int], config: Dict[str, Any]) -> str:
        from src.product_age import assign_age_bucket

        return assign_age_bucket(age_days, config=config)

    def _video_density(self, item: ScoredProductSnapshot) -> float:
        denominator = max(float(item.snapshot.sales_7d or 0.0), 1.0)
        return float(item.snapshot.video_count or 0.0) / denominator

    def _creator_density(self, item: ScoredProductSnapshot) -> float:
        denominator = max(float(item.snapshot.sales_7d or 0.0), 1.0)
        return float(item.snapshot.creator_count or 0.0) / denominator

    def _demand_structure(self, items: List[ScoredProductSnapshot]) -> Dict[str, Any]:
        if not items:
            return {}
        first = items[0]
        rules = self._decision_rules(country=first.snapshot.country, category=first.snapshot.category)
        min_sample = dict(rules.get("min_sample") or {})
        sample_count = len(items)
        sales_values = [max(float(item.snapshot.sales_7d or 0.0), 0.0) for item in items]
        median_sales = round(self._median(sales_values), 2)
        mean_sales = round(sum(sales_values) / max(sample_count, 1), 2)
        sales_action_threshold = float(rules.get("sales_action_threshold", 250) or 250)
        distribution_min = int(min_sample.get("distribution_metrics", 8) or 8)
        p75_min = int(min_sample.get("p75", 12) or 12)
        p90_min = int(min_sample.get("p90", 20) or 20)
        top3_sales_share = None
        mean_median_ratio = None
        if sample_count >= distribution_min:
            total_sales = sum(sales_values)
            top3_sales_share = round(sum(sorted(sales_values, reverse=True)[:3]) / total_sales, 4) if total_sales > 0 else 0.0
            mean_median_ratio = round(mean_sales / median_sales, 4) if median_sales > 0 else None
        over_threshold_item_ratio = round(
            sum(1 for value in sales_values if value >= sales_action_threshold) / max(sample_count, 1),
            4,
        )
        return {
            "sample_count": sample_count,
            "sample_confidence": self._sample_confidence(sample_count, rules),
            "median_sales_7d": median_sales,
            "mean_sales_7d": mean_sales,
            "mean_median_ratio": mean_median_ratio,
            "top3_sales_share": top3_sales_share,
            "over_threshold_item_ratio": over_threshold_item_ratio,
            "sales_p75_7d": round(self._percentile(sales_values, 0.75), 2) if sample_count >= p75_min else None,
            "sales_p90_7d": round(self._percentile(sales_values, 0.90), 2) if sample_count >= p90_min else None,
            "confidence": self._sample_confidence(sample_count, rules),
        }

    def _price_band_analysis(
        self,
        direction_items: List[ScoredProductSnapshot],
        category_items: List[ScoredProductSnapshot],
    ) -> Dict[str, Any]:
        direction_prices = [
            float(item.snapshot.price_mid)
            for item in direction_items
            if item.snapshot.price_mid is not None
        ]
        category_prices = [
            float(item.snapshot.price_mid)
            for item in category_items
            if item.tag.is_valid_sample and item.snapshot.price_mid is not None
        ]
        if not direction_prices or not category_prices:
            return {
                "method": "unavailable",
                "best_price_band": None,
                "price_band_confidence": "insufficient",
                "notes": "缺少价格字段，暂不做价格带判断。",
            }
        p33 = self._percentile(category_prices, 0.33)
        p66 = self._percentile(category_prices, 0.66)
        buckets: Dict[str, List[float]] = {"low_price": [], "mid_price": [], "high_price": []}
        for item in direction_items:
            if item.snapshot.price_mid is None:
                continue
            price = float(item.snapshot.price_mid)
            sales = max(float(item.snapshot.sales_7d or 0.0), 0.0)
            if price <= p33:
                buckets["low_price"].append(sales)
            elif price <= p66:
                buckets["mid_price"].append(sales)
            else:
                buckets["high_price"].append(sales)
        bucket_stats = {}
        valid_candidates = []
        for name, sales_values in buckets.items():
            if len(sales_values) < 5:
                bucket_stats[name] = {"sample_count": len(sales_values), "median_sales_7d": None, "confidence": "insufficient"}
                continue
            median_sales = round(self._median(sales_values), 2)
            bucket_stats[name] = {"sample_count": len(sales_values), "median_sales_7d": median_sales, "confidence": "medium"}
            valid_candidates.append((name, median_sales))
        if not valid_candidates:
            return {
                "method": "dynamic_quantile_bucket",
                "best_price_band": None,
                "price_band_confidence": "insufficient",
                "bucket_stats": bucket_stats,
                "notes": "各价格带样本数均不足 5，不输出强价格带结论。",
            }
        best_name = sorted(valid_candidates, key=lambda pair: (-pair[1], pair[0]))[0][0]
        confidence = "high" if len(buckets[best_name]) >= 12 else "low"
        return {
            "method": "dynamic_quantile_bucket",
            "best_price_band": best_name,
            "price_band_confidence": confidence,
            "bucket_stats": bucket_stats,
            "notes": "按全类目价格 P33/P66 动态分桶；样本不足价格带不输出中位数。",
        }

    def _sample_confidence(self, sample_count: int, rules: Dict[str, Any]) -> str:
        min_sample = dict(rules.get("min_sample") or {})
        if sample_count < int(min_sample.get("insufficient", 5) or 5):
            return "insufficient"
        if sample_count < int(min_sample.get("distribution_metrics", 8) or 8):
            return "low"
        if sample_count < int(min_sample.get("p75", 12) or 12):
            return "medium"
        return "high"

    def _median(self, values: List[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(float(value or 0.0) for value in values)
        mid = len(ordered) // 2
        if len(ordered) % 2 == 1:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2.0

    def _percentile(self, values: List[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(float(value or 0.0) for value in values)
        if len(ordered) == 1:
            return ordered[0]
        position = max(0.0, min(1.0, percentile)) * (len(ordered) - 1)
        lower_index = int(math.floor(position))
        upper_index = int(math.ceil(position))
        if lower_index == upper_index:
            return ordered[lower_index]
        fraction = position - lower_index
        return ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction

    def _seasonal_trends(self, items: List[ScoredProductSnapshot], category: str, style_cluster: str) -> Tuple[str, str, str]:
        short = self._seasonal_trend_for_horizon(items=items, category=category, horizon="short")
        long = self._seasonal_trend_for_horizon(items=items, category=category, horizon="long")
        taxonomy = self._load_taxonomy(category)
        season_sensitive_styles = set(str(item or "").strip() for item in list(taxonomy.get("season_sensitive_styles") or []) if str(item or "").strip())
        if style_cluster in season_sensitive_styles:
            display = short if short != "unclear" else long
        else:
            display = long if long != "unclear" else short
        return short, long, display

    def _seasonal_trend_for_horizon(self, items: List[ScoredProductSnapshot], category: str, horizon: str) -> str:
        historical_values: List[float] = []
        for item in items:
            value = self._historical_sales_median(item, category, horizon=horizon)
            if value is not None and value > 0:
                historical_values.append(value)
        if not historical_values:
            return "unclear"
        current_median = self._median([float(item.snapshot.sales_7d or 0.0) for item in items])
        historical_median = self._median(historical_values)
        return seasonal_trend_from_ratio(current_median / historical_median if historical_median > 0 else None)

    def _historical_sales_median(self, item: ScoredProductSnapshot, category: str, horizon: str) -> Optional[float]:
        return extract_historical_sales_value(
            raw_fields=dict(item.snapshot.raw_fields or {}),
            category=category,
            horizon=horizon,
            taxonomy_dir=self.taxonomy_dir,
        )

    def _calculate_direction_tier(
        self,
        item_count: int,
        item_share_in_family: float,
        sales_median_7d: float,
        video_density_avg: float,
        creator_density_avg: float,
        p50_sales_median: float,
        p75_sales_median: float,
        p75_video_density: float,
        p75_creator_density: float,
    ) -> str:
        if item_count < 10:
            return "low_sample"
        if (
            (item_share_in_family >= 0.40 and sales_median_7d < p50_sales_median)
            or video_density_avg >= p75_video_density
            or creator_density_avg >= p75_creator_density
        ):
            return "crowded"
        if (
            sales_median_7d >= p75_sales_median
            and video_density_avg < p75_video_density
            and creator_density_avg < p75_creator_density
        ):
            return "priority"
        return "balanced"

    def _calculate_light_top_direction_tier(
        self,
        item_count: int,
        item_share_in_family: float,
        sales_median_7d: float,
        video_density_avg: float,
        creator_density_avg: float,
        family_p75_sales: float,
        family_p75_video: float,
        family_p75_creator: float,
        taxonomy: Dict[str, Any],
    ) -> str:
        absolute = taxonomy.get("absolute_thresholds") or {}
        low_sample_min_count = int(absolute.get("low_sample_min_count", 30) or 30)
        sales_floor_baseline = float(absolute.get("sales_floor_baseline", 200.0) or 200.0)
        if item_count < low_sample_min_count:
            return "low_sample"
        if (
            sales_median_7d >= sales_floor_baseline
            and sales_median_7d >= family_p75_sales
            and video_density_avg < family_p75_video
            and creator_density_avg < family_p75_creator
        ):
            return "priority"
        if (
            video_density_avg >= family_p75_video
            or creator_density_avg >= family_p75_creator
            or (item_share_in_family >= 0.40 and sales_median_7d < sales_floor_baseline)
        ):
            return "crowded"
        return "balanced"

    def _default_content_route_preference(self, direction_tier: str) -> str:
        if direction_tier == "crowded":
            return "original_preferred"
        return "neutral"

    def _low_sample_min_count(self, category: str) -> int:
        taxonomy = self._load_taxonomy(category)
        absolute = taxonomy.get("absolute_thresholds") or {}
        if str(category or "").strip() == "light_tops":
            return int(absolute.get("low_sample_min_count", 30) or 30)
        return int(absolute.get("low_sample_min_count", 10) or 10)

    def _aggregate_content_efficiency(self, items: List[ScoredProductSnapshot]) -> Tuple[float, str]:
        primary_values = [
            float(item.content_efficiency_signal or 0.0)
            for item in items
            if str(item.content_efficiency_source or "") == "primary"
        ]
        if primary_values:
            return round(sum(primary_values) / len(primary_values), 4), "primary"
        proxy_values = [
            float(item.content_efficiency_signal or 0.0)
            for item in items
            if str(item.content_efficiency_source or "") == "proxy"
        ]
        if proxy_values:
            return round(sum(proxy_values) / len(proxy_values), 4), "proxy"
        return 0.0, "missing"

    def _dedupe_scored_items(self, scored_items: Iterable[ScoredProductSnapshot]) -> List[ScoredProductSnapshot]:
        deduped: Dict[str, ScoredProductSnapshot] = {}
        for item in scored_items:
            product_key = str(item.snapshot.product_id or "").strip() or "rank_{index}".format(index=int(item.snapshot.rank_index or 0))
            existing = deduped.get(product_key)
            if existing is None:
                deduped[product_key] = item
                continue
            current_rank = (
                int(bool(item.tag.is_valid_sample)),
                float(item.heat_score or 0.0),
                -int(item.snapshot.rank_index or 0),
            )
            existing_rank = (
                int(bool(existing.tag.is_valid_sample)),
                float(existing.heat_score or 0.0),
                -int(existing.snapshot.rank_index or 0),
            )
            if current_rank > existing_rank:
                deduped[product_key] = item
        return list(deduped.values())

    def _score_level(self, score: float) -> str:
        if score >= 67:
            return "high"
        if score >= 34:
            return "medium"
        return "low"

    def _priority_level(self, heat_level: str, crowd_level: str, product_count: int) -> str:
        if heat_level == "high" and crowd_level in {"low", "medium"} and product_count >= 2:
            return "high"
        if heat_level == "high" or product_count >= 2:
            return "medium"
        return "low"

    def _priority_sort(self, value: str) -> int:
        return {"high": 3, "medium": 2, "low": 1}.get(value, 0)

    def _selection_advice(
        self,
        category: str,
        style_cluster: str,
        top_forms: List[str],
        top_value_points: List[str],
        core_elements: List[str],
        scene_tags: List[str],
        priority_level: str,
    ) -> str:
        value_point = top_value_points[0] if top_value_points else "主价值点"
        element_text = "、".join(core_elements[:2]) if core_elements else "基础表达点"
        scene_text = "、".join(scene_tags[:2]) if scene_tags else "日常场景"
        forms_text = "、".join(top_forms[:2]) if top_forms else "当前主形态"
        prefix = "优先补" if priority_level == "high" else "可以测试"
        if str(category or "").strip() == "light_tops":
            return "{prefix}{direction}，当前主要承载轮廓是{forms}，优先围绕{value_point}做款，重点看{element}和{scene}表达是否能把上身结果讲清。".format(
                prefix=prefix,
                direction=style_cluster,
                forms=forms_text,
                value_point=value_point,
                element=element_text,
                scene=scene_text,
            )
        return "{prefix}{direction}，当前主要承载形态是{forms}，围绕{value_point}做货，重点看{element}和{scene}表达。".format(
            prefix=prefix,
            direction=style_cluster,
            forms=forms_text,
            value_point=value_point,
            element=element_text,
            scene=scene_text,
        )

    def _avoid_notes(self, category: str, style_cluster: str, crowd_level: str, top_forms: List[str], core_elements: List[str]) -> str:
        forms_text = "、".join(top_forms[:2]) if top_forms else "同类形态"
        if str(category or "").strip() == "light_tops":
            if crowd_level == "high":
                return "不要只补{forms}里外观接近、缺少明确上身结果差异的 {style} 同类款。".format(forms=forms_text, style=style_cluster)
            if core_elements:
                return "不要只补 {element} 但上身结果和场景价值说不清的 {style} 同类款。".format(
                    element="、".join(core_elements[:2]),
                    style=style_cluster,
                )
            return "不要只补看起来相似、但缺少上身结果支撑的 {style} 同类款。".format(style=style_cluster)
        if crowd_level == "high":
            return "不要只补{forms}里外观接近、缺少新表达点的 {style} 同类款。".format(forms=forms_text, style=style_cluster)
        if core_elements:
            return "不要只补 {element} 但实际价值点支撑弱的 {style} 同类款。".format(
                element="、".join(core_elements[:2]),
                style=style_cluster,
            )
        return "不要只补看起来相似但缺少明确价值点支撑的 {style} 同类款。".format(style=style_cluster)

    def _confidence(self, items: List[ScoredProductSnapshot], avg_heat: float, avg_crowd: float) -> float:
        valid_ratio = sum(1 for item in items if item.tag.is_valid_sample) / max(len(items), 1)
        value = 0.45 + min(len(items), 5) * 0.06 + valid_ratio * 0.15 + (avg_heat / 100.0) * 0.15 - (avg_crowd / 100.0) * 0.05
        return round(max(0.35, min(0.95, value)), 2)
