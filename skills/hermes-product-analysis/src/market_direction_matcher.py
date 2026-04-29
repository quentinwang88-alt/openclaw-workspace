#!/usr/bin/env python3
"""Stage-2 matcher that consumes latest Market Direction Cards."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.enums import MarketMatchStatus
from src.market_insight_db import MarketInsightDatabase
from src.market_insight_models import MarketDirectionMatchResult
from src.price_normalization import normalize_task_target_price_to_cny


class MarketDirectionMatcher(object):
    def __init__(self, artifacts_root: Path):
        self.artifacts_root = Path(artifacts_root)
        self.database = MarketInsightDatabase(self.artifacts_root / "market_insight.db")
        self.contract = self._load_consumer_contract()

    def match_candidate(self, task, final_category: str) -> MarketDirectionMatchResult:
        country = self._normalize_country(getattr(task, "target_market", ""))
        category = self._normalize_category(final_category)
        if not country or not category:
            return MarketDirectionMatchResult(market_match_status=MarketMatchStatus.UNCOVERED.value)
        cards = self._load_latest_cards(country=country, category=category)
        if not cards:
            return MarketDirectionMatchResult(market_match_status=MarketMatchStatus.UNCOVERED.value)

        title = str(getattr(task, "product_title", "") or "")
        keyword_tags = [str(item or "").strip() for item in getattr(task, "title_keyword_tags", []) if str(item or "").strip()]
        extra_texts = [
            str(getattr(task, "product_notes", "") or "").strip(),
            str(getattr(task, "competitor_notes", "") or "").strip(),
        ]
        target_price_cny = normalize_task_target_price_to_cny(task)
        text_corpus = "\n".join([title] + keyword_tags + extra_texts)

        best = None
        scored_candidates: List[Dict[str, Any]] = []
        best_score = 0.0
        for card in cards:
            normalized_card, contract_warning = self._normalize_card_for_consumer(card)
            score, reason, matched_terms = self._score_card(
                normalized_card,
                title=title,
                keyword_tags=keyword_tags,
                target_price_cny=target_price_cny,
                text_corpus=text_corpus,
            )
            scored_candidates.append(
                {
                    "direction_id": str(normalized_card.get("direction_canonical_key", "") or normalized_card.get("direction_instance_id", "")),
                    "direction_name": str(normalized_card.get("direction_name", "") or normalized_card.get("style_cluster", "")),
                    "score": round(score, 4),
                    "matched_terms": matched_terms,
                    "risk_flags": [contract_warning] if contract_warning else [],
                }
            )
            if score > best_score:
                best_score = score
                best = (normalized_card, reason, matched_terms, contract_warning)
        if not best:
            return MarketDirectionMatchResult(
                cards_available=bool(cards),
                market_match_status=MarketMatchStatus.UNCOVERED.value,
            )
        card, reason, matched_terms, contract_warning = best
        candidate_directions = sorted(scored_candidates, key=lambda item: float(item.get("score") or 0.0), reverse=True)[:3]
        if len(candidate_directions) >= 3:
            for item in candidate_directions:
                item.setdefault("risk_flags", []).append("multi_direction_candidate")
        has_semantic_match = any(term != "价格带接近" for term in matched_terms)
        if best_score >= 0.45:
            match_status = MarketMatchStatus.MATCHED.value
        elif has_semantic_match:
            match_status = MarketMatchStatus.WEAK_MATCHED.value
        else:
            match_status = MarketMatchStatus.UNCOVERED.value
        if match_status == MarketMatchStatus.UNCOVERED.value:
            fallback_reason = "方向卡未覆盖"
            if contract_warning:
                fallback_reason = "方向卡未覆盖（{warning}）".format(warning=contract_warning)
            return MarketDirectionMatchResult(
                cards_available=True,
                matched_market_direction_reason=fallback_reason[:120],
                market_match_status=match_status,
                decision_confidence=str(card.get("decision_confidence", "") or "low"),
                schema_version=str(self.contract.get("schema_version") or ""),
                contract_warning=contract_warning,
                candidate_directions=candidate_directions,
            )
        return MarketDirectionMatchResult(
            matched_market_direction_id=str(card.get("direction_canonical_key", "") or card.get("direction_instance_id", "")),
            matched_market_direction_name=str(card.get("direction_name", "")),
            matched_market_direction_reason=(reason or contract_warning or "方向卡弱匹配")[:120],
            score=round(best_score, 4),
            market_match_status=match_status,
            cards_available=True,
            style_cluster=str(card.get("style_cluster", "") or card.get("style_main", "") or ""),
            direction_family=str(card.get("direction_family", "") or ""),
            direction_tier=str(card.get("direction_tier", "") or ""),
            decision_confidence=str(card.get("decision_confidence", "") or "low"),
            default_content_route_preference=str(card.get("default_content_route_preference", "") or ""),
            schema_version=str(self.contract.get("schema_version") or ""),
            contract_warning=contract_warning,
            candidate_directions=candidate_directions,
            representative_products=[
                dict(item)
                for item in list(card.get("representative_products", []) or [])[:10]
                if isinstance(item, dict)
            ],
            direction_action=str(
                card.get("decision_action", "")
                or card.get("actual_action", "")
                or (card.get("direction_execution_brief") or {}).get("direction_action", "")
                or ""
            ),
            product_form=str(
                (
                    card.get("top_silhouette_forms")
                    or card.get("top_forms")
                    or [card.get("product_form_or_result", "")]
                )[0]
                or ""
            ),
            top_forms=[str(item or "").strip() for item in list(card.get("top_forms", []) or []) if str(item or "").strip()],
            top_silhouette_forms=[str(item or "").strip() for item in list(card.get("top_silhouette_forms", []) or []) if str(item or "").strip()],
            top_length_forms=[str(item or "").strip() for item in list(card.get("top_length_forms", []) or []) if str(item or "").strip()],
            core_elements=[str(item or "").strip() for item in list(card.get("core_elements", []) or []) if str(item or "").strip()],
            scene_tags=[str(item or "").strip() for item in list(card.get("scene_tags", []) or []) if str(item or "").strip()],
            target_price_bands=[str(item or "").strip() for item in list(card.get("target_price_bands", []) or []) if str(item or "").strip()],
            matched_terms=matched_terms,
            direction_execution_brief=dict(card.get("direction_execution_brief") or {}),
        )

    def _load_latest_cards(self, country: str, category: str) -> List[Dict[str, object]]:
        cards = self.database.load_latest_direction_cards(country=country, category=category)
        if cards:
            return cards
        latest_path = self.artifacts_root / "latest" / "{country}__{category}.json".format(country=country, category=category)
        if not latest_path.exists():
            return []
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
        if not self._latest_payload_is_consumable(payload):
            return []
        cards_path = Path(str(payload.get("cards_path", "") or ""))
        if not cards_path.exists():
            return []
        cards = json.loads(cards_path.read_text(encoding="utf-8"))
        return cards if isinstance(cards, list) else []

    def _latest_payload_is_consumable(self, payload: Dict[str, object]) -> bool:
        source_scope = str(payload.get("source_scope", "") or "official").strip().lower()
        if source_scope != "official":
            return False
        explicit_flag = payload.get("is_consumable")
        if explicit_flag is not None:
            return bool(explicit_flag)
        try:
            completed_product_count = int(payload.get("completed_product_count") or 0)
            direction_count = int(payload.get("direction_count") or 0)
            min_products = int(payload.get("min_consumable_product_count") or 100)
            min_directions = int(payload.get("min_consumable_direction_count") or 5)
        except (TypeError, ValueError):
            return False
        return completed_product_count >= min_products and direction_count >= min_directions

    def _score_card(
        self,
        card: Dict[str, object],
        title: str,
        keyword_tags: List[str],
        target_price_cny,
        text_corpus: str,
    ) -> Tuple[float, str, List[str]]:
        score = 0.0
        matched_terms: List[str] = []
        title_text = title.strip()
        keyword_set = {item for item in keyword_tags if item}
        searchable_text = text_corpus.strip()

        style_cluster = str(card.get("style_cluster", "") or card.get("style_main", "") or "")
        if style_cluster and style_cluster in searchable_text:
            score += 0.40
            matched_terms.append(style_cluster)

        form_matches = []
        top_forms = [str(item or "").strip() for item in list(card.get("top_silhouette_forms", []) or []) if str(item or "").strip()]
        if not top_forms:
            top_forms = [str(item or "").strip() for item in list(card.get("top_forms", []) or []) if str(item or "").strip()]
        legacy_form = str(card.get("product_form_or_result", "") or "").strip()
        if legacy_form and legacy_form not in top_forms:
            top_forms.append(legacy_form)
        for form in top_forms:
            if form in searchable_text or form in keyword_set:
                form_matches.append(form)
        if form_matches:
            score += 0.15
            matched_terms.extend(form_matches)

        length_matches = []
        for item in list(card.get("top_length_forms", []) or []):
            term = str(item or "").strip()
            if not term:
                continue
            if term in searchable_text or term in keyword_set:
                length_matches.append(term)
        if length_matches:
            score += 0.05
            matched_terms.extend(length_matches)

        core_matches = []
        for item in list(card.get("core_elements", []) or []):
            term = str(item or "").strip()
            if not term:
                continue
            if term in searchable_text or term in keyword_set:
                core_matches.append(term)
        if core_matches:
            score += 0.20 * min(len(core_matches) / 2.0, 1.0)
            matched_terms.extend(core_matches)

        scene_matches = []
        for item in list(card.get("scene_tags", []) or []):
            term = str(item or "").strip()
            if not term:
                continue
            if term in searchable_text or term in keyword_set:
                scene_matches.append(term)
        if scene_matches:
            score += 0.10 * min(len(scene_matches), 1.0)
            matched_terms.extend(scene_matches)

        auxiliary_matches = []
        for item in list(card.get("top_value_points", []) or []):
            term = str(item or "").strip()
            if not term:
                continue
            if term in searchable_text or term in keyword_set:
                auxiliary_matches.append(term)
        if auxiliary_matches:
            score += 0.10 * min(len(auxiliary_matches) / 2.0, 1.0)
        matched_terms.extend(auxiliary_matches)

        if target_price_cny is not None:
            price_bands = [str(item or "").strip() for item in (card.get("target_price_bands") or []) if str(item or "").strip()]
            if price_bands and self._target_price_matches_bands(target_price_cny, price_bands):
                score += 0.10
                matched_terms.append("价格带接近")
        if not matched_terms:
            return score, "", []
        reason = "标题/关键词命中 {terms}".format(terms="、".join(dict.fromkeys(matched_terms)))
        return score, reason, list(dict.fromkeys(matched_terms))

    def _normalize_card_for_consumer(self, card: Dict[str, object]) -> Tuple[Dict[str, object], str]:
        normalized = dict(card)
        missing_required = []
        direction_key = str(card.get("direction_canonical_key", "") or card.get("direction_instance_id", "") or "").strip()
        if not direction_key:
            missing_required.append("direction_canonical_key")
        for field_name in self.contract.get("required_fields", []):
            if field_name == "direction_canonical_key":
                continue
            value = card.get(field_name)
            if value in (None, "", []):
                missing_required.append(str(field_name))

        normalized["direction_canonical_key"] = direction_key
        normalized["style_cluster"] = str(card.get("style_cluster", "") or card.get("style_main", "") or "")
        normalized["core_elements"] = list(card.get("core_elements") or [])
        normalized["scene_tags"] = list(card.get("scene_tags") or [])
        normalized["target_price_bands"] = list(card.get("target_price_bands") or [])
        normalized["top_value_points"] = list(card.get("top_value_points") or [])
        normalized["top_forms"] = list(card.get("top_forms") or [])
        normalized["top_silhouette_forms"] = list(card.get("top_silhouette_forms") or [])
        normalized["top_length_forms"] = list(card.get("top_length_forms") or [])
        normalized["decision_action"] = str(card.get("decision_action", "") or card.get("actual_action", "") or "")
        normalized["default_content_route_preference"] = str(card.get("default_content_route_preference", "") or "")
        normalized["representative_products"] = list(card.get("representative_products") or [])
        normalized["decision_confidence"] = str(card.get("decision_confidence", "") or ("low" if missing_required else "medium"))
        contract_warning = ""
        if missing_required:
            contract_warning = "schema_mismatch: missing {fields}".format(fields=", ".join(missing_required))
            normalized["decision_confidence"] = "low"
        return normalized, contract_warning

    def _load_consumer_contract(self) -> Dict[str, Any]:
        contract_path = self.artifacts_root.parents[1] / "configs" / "direction_card_consumer_contract.json"
        if contract_path.exists():
            return json.loads(contract_path.read_text(encoding="utf-8"))
        return {
            "schema_version": "fallback",
            "required_fields": [
                "direction_canonical_key",
                "direction_family",
                "direction_tier",
                "core_elements",
                "scene_tags",
                "target_price_bands",
            ],
            "optional_fields": [
                "top_value_points",
                "decision_confidence",
            ],
            "fallback_strategy": {},
            "on_schema_mismatch": "mark_uncovered",
        }

    def _target_price_matches_bands(self, target_price, price_bands: List[str]) -> bool:
        try:
            value = float(target_price)
        except (TypeError, ValueError):
            return False
        for band in price_bands:
            normalized = (
                band.replace(" ", "")
                .replace("元", "")
                .replace("RMB", "")
                .replace("rmb", "")
                .replace("CNY", "")
                .replace("cny", "")
                .replace("¥", "")
                .replace("￥", "")
            )
            if "-" in normalized:
                parts = normalized.split("-", 1)
                try:
                    lower = float(parts[0])
                    upper = float(parts[1])
                except ValueError:
                    continue
                if lower <= value <= upper:
                    return True
            elif normalized.endswith("+"):
                try:
                    lower = float(normalized[:-1])
                except ValueError:
                    continue
                if value >= lower:
                    return True
        return False

    def _normalize_country(self, value: str) -> str:
        text = str(value or "").strip().upper()
        if text in {"越南", "VN"}:
            return "VN"
        if text in {"TH", "泰国"}:
            return "TH"
        return text

    def _normalize_category(self, value: str) -> str:
        mapping = {
            "发饰": "hair_accessory",
            "轻上装": "light_tops",
            "hair_accessory": "hair_accessory",
            "light_tops": "light_tops",
        }
        return mapping.get(str(value or "").strip(), "")
