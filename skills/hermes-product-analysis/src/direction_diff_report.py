#!/usr/bin/env python3
"""Dry-run diff report for market insight rule changes."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from src.market_insight_aggregator import MarketInsightAggregator
from src.market_insight_db import MarketInsightDatabase
from src.market_insight_models import MarketInsightProductTag, ProductRankingSnapshot, ScoredProductSnapshot, VOCLightSummary
from src.market_insight_report_generator import MarketInsightReportGenerator


def scored_item_from_payload(payload: Dict[str, Any]) -> ScoredProductSnapshot:
    snapshot_payload = dict(payload.get("snapshot") or {})
    tag_payload = dict(payload.get("tag") or {})
    return ScoredProductSnapshot(
        snapshot=ProductRankingSnapshot(
            batch_date=str(snapshot_payload.get("batch_date") or ""),
            batch_id=str(snapshot_payload.get("batch_id") or ""),
            country=str(snapshot_payload.get("country") or ""),
            category=str(snapshot_payload.get("category") or ""),
            product_id=str(snapshot_payload.get("product_id") or ""),
            product_name=str(snapshot_payload.get("product_name") or ""),
            shop_name=str(snapshot_payload.get("shop_name") or ""),
            price_min=snapshot_payload.get("price_min"),
            price_max=snapshot_payload.get("price_max"),
            price_mid=snapshot_payload.get("price_mid"),
            sales_7d=float(snapshot_payload.get("sales_7d") or 0.0),
            gmv_7d=float(snapshot_payload.get("gmv_7d") or 0.0),
            creator_count=float(snapshot_payload.get("creator_count") or 0.0),
            video_count=float(snapshot_payload.get("video_count") or 0.0),
            listing_days=snapshot_payload.get("listing_days"),
            image_url=str(snapshot_payload.get("image_url") or ""),
            product_url=str(snapshot_payload.get("product_url") or ""),
            rank_index=int(snapshot_payload.get("rank_index") or 0),
            raw_category=str(snapshot_payload.get("raw_category") or ""),
            raw_fields=dict(snapshot_payload.get("raw_fields") or {}),
        ),
        tag=MarketInsightProductTag(
            is_valid_sample=bool(tag_payload.get("is_valid_sample")),
            style_cluster=str(tag_payload.get("style_cluster") or ""),
            style_tags_secondary=list(tag_payload.get("style_tags_secondary") or []),
            product_form=str(tag_payload.get("product_form") or ""),
            length_form=str(tag_payload.get("length_form") or ""),
            element_tags=list(tag_payload.get("element_tags") or []),
            value_points=list(tag_payload.get("value_points") or []),
            scene_tags=list(tag_payload.get("scene_tags") or []),
            reason_short=str(tag_payload.get("reason_short") or ""),
        ),
        heat_score=float(payload.get("heat_score") or 0.0),
        heat_level=str(payload.get("heat_level") or ""),
        crowd_score=float(payload.get("crowd_score") or 0.0),
        crowd_level=str(payload.get("crowd_level") or ""),
        priority_level=str(payload.get("priority_level") or ""),
        target_price_band=str(payload.get("target_price_band") or ""),
        direction_canonical_key=str(payload.get("direction_canonical_key") or ""),
        direction_family=str(payload.get("direction_family") or ""),
        direction_tier=str(payload.get("direction_tier") or ""),
        seasonal_trend=str(payload.get("seasonal_trend") or "unclear"),
        seasonal_trend_short=str(payload.get("seasonal_trend_short") or "unclear"),
        seasonal_trend_long=str(payload.get("seasonal_trend_long") or "unclear"),
        content_efficiency_signal=float(payload.get("content_efficiency_signal") or 0.0),
        content_efficiency_source=str(payload.get("content_efficiency_source") or "missing"),
        default_content_route_preference=str(payload.get("default_content_route_preference") or ""),
    )


class MarketInsightDiffReporter(object):
    def __init__(self, artifacts_root: Path, skill_dir: Path, report_config_path: Path):
        self.database = MarketInsightDatabase(Path(artifacts_root) / "market_insight.db")
        self.aggregator = MarketInsightAggregator()
        self.report_generator = MarketInsightReportGenerator(skill_dir=skill_dir, config_path=report_config_path)

    def build_recent_run_diff(self, country: str, category: str, recent_runs: int = 3) -> Dict[str, Any]:
        runs = self.database.list_recent_completed_runs(country=country, category=category, limit=recent_runs)
        diff_runs: List[Dict[str, Any]] = []
        for run in runs:
            run_id = str(run.get("run_id") or "")
            old_cards = {
                str(card.get("direction_canonical_key") or ""): card
                for card in self.database.load_direction_cards_for_run(run_id)
            }
            scored_payloads = self.database.load_scored_products_for_run(run_id)
            scored_items = [scored_item_from_payload(item) for item in scored_payloads]
            if not scored_items:
                continue
            new_cards = self.aggregator.build_direction_cards(scored_items)
            new_card_map = {str(card.direction_canonical_key or ""): card.to_dict() for card in new_cards}
            report_payload, _, _ = self.report_generator.generate_report(
                cards=new_cards,
                voc_summary=VOCLightSummary(voc_status="skipped"),
                country=str(run.get("country") or country),
                category=str(run.get("category") or category),
                batch_date=str(run.get("batch_date") or ""),
                use_llm=False,
            )
            old_report_payload = dict(run.get("structured_report_json") or {})
            diff_runs.append(
                {
                    "run_id": run_id,
                    "batch_date": str(run.get("batch_date") or ""),
                    "direction_tier_changes": self._field_changes(old_cards, new_card_map, "direction_tier"),
                    "decision_confidence_changes": self._field_changes(old_cards, new_card_map, "decision_confidence"),
                    "report_action_changes": self._report_action_changes(old_report_payload, report_payload),
                    "blocked_changed": bool(
                        bool((old_report_payload.get("llm_meta") or {}).get("report_publish_blocked"))
                        != bool((report_payload.get("llm_meta") or {}).get("report_publish_blocked"))
                    ),
                }
            )
        payload = {
            "country": country,
            "category": category,
            "recent_runs": diff_runs,
        }
        payload["markdown"] = self._render_markdown(payload)
        return payload

    def _field_changes(self, old_cards: Dict[str, Dict[str, Any]], new_cards: Dict[str, Dict[str, Any]], field_name: str) -> List[Dict[str, str]]:
        rows = []
        keys = sorted(set(old_cards) | set(new_cards))
        for key in keys:
            old_value = str((old_cards.get(key) or {}).get(field_name) or "")
            new_value = str((new_cards.get(key) or {}).get(field_name) or "")
            if old_value != new_value:
                direction_name = str((new_cards.get(key) or old_cards.get(key) or {}).get("direction_name") or key)
                rows.append(
                    {
                        "direction_name": direction_name,
                        "old": old_value or "∅",
                        "new": new_value or "∅",
                    }
                )
        return rows

    def _report_action_changes(self, old_payload: Dict[str, Any], new_payload: Dict[str, Any]) -> List[Dict[str, str]]:
        old_map = self._summary_map(old_payload)
        new_map = self._summary_map(new_payload)
        rows = []
        keys = sorted(set(old_map) | set(new_map))
        for key in keys:
            if old_map.get(key) != new_map.get(key):
                rows.append({"direction_name": key, "old": old_map.get(key, "∅"), "new": new_map.get(key, "∅")})
        return rows

    def _summary_map(self, payload: Dict[str, Any]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        summary = dict(payload.get("decision_summary") or {})
        for bucket in ("enter", "watch", "avoid"):
            for item in list((summary.get(bucket) or {}).get("all_items") or []):
                name = str(item.get("style_cluster") or item.get("direction_name") or "")
                if name:
                    mapping[name] = bucket
        return mapping

    def _render_markdown(self, payload: Dict[str, Any]) -> str:
        lines = ["# Market Insight Diff Report", ""]
        for run in payload.get("recent_runs", []):
            lines.extend(
                [
                    "## {batch_date}".format(batch_date=run.get("batch_date", "")),
                    "",
                    "### direction_tier 变化",
                ]
            )
            tier_changes = list(run.get("direction_tier_changes") or [])
            if tier_changes:
                for row in tier_changes:
                    lines.append("- {direction_name}: {old} -> {new}".format(**row))
            else:
                lines.append("- 无")
            lines.extend(["", "### report_action 变化"])
            action_changes = list(run.get("report_action_changes") or [])
            if action_changes:
                for row in action_changes:
                    lines.append("- {direction_name}: {old} -> {new}".format(**row))
            else:
                lines.append("- 无")
            lines.extend(["", "### decision_confidence 变化"])
            confidence_changes = list(run.get("decision_confidence_changes") or [])
            if confidence_changes:
                for row in confidence_changes:
                    lines.append("- {direction_name}: {old} -> {new}".format(**row))
            else:
                lines.append("- 无")
            lines.extend(
                [
                    "",
                    "### blocked 变化",
                    "- {value}".format(value="有变化" if run.get("blocked_changed") else "无变化"),
                    "",
                ]
            )
        return "\n".join(lines).strip() + "\n"
