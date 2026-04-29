#!/usr/bin/env python3
"""统一候选品评分与批内校准。"""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.enums import (
    FEATURE_LEVEL_TO_SCORE,
    MarketMatchStatus,
    REVERSED_FEATURE_LEVEL_TO_SCORE,
    SuggestedAction,
    SupportedCategory,
)
from src.market_insight_db import MarketInsightDatabase
from src.market_insight_models import MarketDirectionMatchResult
from src.models import (
    AccioSupplyResult,
    DecisionReason,
    FeatureAnalysisResult,
    PendingAnalysisItem,
    ScoredAnalysisResult,
    StorePositioningCard,
)
from src.price_normalization import normalize_task_target_price_to_cny
from src.product_selection_v2 import ProductSelectionV2Scorer


PRIMARY_SCORING_VARIANT = "A"
ROUTE_PRIORITY_TEST = "priority_test"
ROUTE_SMALL_TEST = "small_test"
ROUTE_RESERVE = "reserve"
ROUTE_REJECT = "reject"
ROUTE_PENDING_REVIEW = "pending_review"

VARIANT_WEIGHTS = {
    "A": {
        # 店铺是当前阶段的结构性切入点，因此 A 方案把店铺适配权重提到高于市场泛热度。
        "market_match": 0.25,
        "content_potential": 0.40,
        "store_fit": 0.35,
    },
    "B": {
        "market_match": 0.30,
        "content_potential": 0.50,
        "store_fit": 0.20,
    },
}

STORE_POSITIONING_CARD_FIELDS = [
    "StorePositioningCard",
    "store_positioning_card",
    "store_positioning_card_json",
    "店铺定位卡",
    "店铺定位卡JSON",
    "店铺定位",
]
STORE_STYLE_WHITELIST_FIELDS = ["style_whitelist", "风格白名单", "店铺风格白名单", "允许风格"]
STORE_STYLE_BLACKLIST_FIELDS = ["style_blacklist", "风格黑名单", "店铺风格黑名单", "禁用风格"]
STORE_SOFT_STYLE_BLACKLIST_FIELDS = [
    "soft_style_blacklist",
    "softBlacklist",
    "软黑名单",
    "阶段性不主推风格",
]
STORE_HARD_STYLE_BLACKLIST_FIELDS = [
    "hard_style_blacklist",
    "hardBlacklist",
    "硬黑名单",
    "绝对禁用风格",
]
STORE_PRICE_BAND_FIELDS = ["target_price_bands", "目标价格带", "店铺目标价格带", "price_bands"]
STORE_SCENE_FIELDS = ["core_scenes", "核心场景", "店铺核心场景", "scene_tags"]
STORE_CONTENT_TONE_FIELDS = ["content_tones", "内容调性", "店铺内容调性", "content_tone"]
STORE_VALUE_POINT_FIELDS = ["core_value_points", "核心价值点", "店铺核心价值点", "value_points"]
STORE_TARGET_AUDIENCE_FIELDS = ["target_audience", "目标人群", "店铺目标人群"]
STORE_SELECTION_PRINCIPLE_FIELDS = ["selection_principles", "选品原则", "店铺选品原则"]
STORE_NOTES_FIELDS = ["notes", "备注", "店铺备注"]
STORE_ID_FIELDS = ["store_id", "店铺ID", "shop_id", "店铺", "店铺名称", "店铺名"]
STORE_CARD_NAME_FIELDS = ["card_name", "店铺定位卡名称", "店铺名称", "店铺名"]
STORE_COUNTRY_FIELDS = ["country", "国家", "目标国家", "目标市场", "target_market"]
STORE_CATEGORY_FIELDS = ["category", "类目", "产品类目"]

SUPPLY_STATUS_FIELDS = ["supply_check_status", "供给检查状态", "供给状态", "货源检查状态", "货源状态"]
SUPPLY_SUMMARY_FIELDS = ["supply_summary", "供给摘要", "货源摘要", "Accio备注", "accio_note"]
SUPPLY_URL_FIELDS = ["accio_source_url", "source_url", "供应商链接", "推荐货源链接", "1688链接", "采购链接"]
SUPPLY_PRICE_FIELDS = ["procurement_price_rmb", "推荐采购价_rmb", "采购价_rmb", "采购价", "供货价"]
SUPPLY_GROSS_OK_FIELDS = ["gross_margin_ok", "粗利润是否成立", "毛利成立"]
SUPPLY_GROSS_MARGIN_FIELDS = ["gross_margin_rate", "商品粗毛利率", "粗毛利率"]
SUPPLY_DISTRIBUTION_MARGIN_FIELDS = ["distribution_margin_rate", "分销后毛利率"]
ACCIO_STATUS_FIELDS = ["accio_status", "Accio状态"]

COMPETITION_LEVEL_FIELDS = [
    "competition_reference_level",
    "竞争参考等级",
    "竞争等级",
    "competition_maturity",
    "竞争成熟度",
]
COMPETITION_CONFIDENCE_FIELDS = ["competition_confidence", "竞争参考置信度", "竞争置信度"]

SAMPLE_CHECK_STATUS_FIELDS = ["sample_check_status", "样品检查状态", "样品状态", "sample_status"]

RESERVE_REASON_FIELDS = ["reserve_reason", "备用池原因"]
RESERVE_CREATED_AT_FIELDS = ["created_at", "reserve_created_at", "备用池创建时间"]
RESERVE_EXPIRES_AT_FIELDS = ["expires_at", "reserve_expires_at", "备用池过期时间"]
RESERVE_STATUS_FIELDS = ["reserve_status", "备用池状态"]

STORE_FIT_FALLBACK_SCORE = 0.65
LOW_CONFIDENCE_MARKET_MATCH_CAP = 0.65
SOFT_BLACKLIST_STORE_FIT_CAP = 0.25

SERIOUS_CONTENT_RISK_TAGS = {
    "图片信息不足",
    "戴上效果不够直观",
    "演示场景较弱",
    "镜头识别度弱",
    "真人上身依赖强",
}


class ScoringEngine(object):
    def __init__(self, store_positioning_database=None, market_insight_artifacts_root: Optional[Path] = None):
        if store_positioning_database is not None:
            self.store_positioning_database = store_positioning_database
        else:
            repo_root = Path(market_insight_artifacts_root) if market_insight_artifacts_root else Path(__file__).resolve().parents[1] / "artifacts" / "market_insight"
            self.store_positioning_database = MarketInsightDatabase(Path(repo_root) / "market_insight.db")
        self.v2_scorer = ProductSelectionV2Scorer(Path(__file__).resolve().parents[1])

    def score_candidate(
        self,
        task,
        feature_result: FeatureAnalysisResult,
        market_direction_result: Optional[MarketDirectionMatchResult] = None,
    ) -> ScoredAnalysisResult:
        content_potential_score = self._compute_content_potential_score(feature_result)
        content_potential = self._score_to_level(content_potential_score, high_threshold=75.0, low_threshold=45.0)

        match_result = market_direction_result or MarketDirectionMatchResult()
        market_match_norm, market_match_score, market_match_status, direction_confidence_low = self._resolve_market_match_context(match_result)
        store_card = self._resolve_store_positioning_card(task=task, feature_result=feature_result)
        store_fit_norm, blacklist_status = self._compute_store_fit_context(
            task=task,
            feature_result=feature_result,
            store_card=store_card,
            market_direction_result=match_result,
        )
        product_potential = self._map_product_potential(market_match_score, market_match_status, store_fit_norm)

        supply_result = self._parse_supply_result(task)
        competition_level, competition_confidence = self._parse_competition_reference(task)
        sample_check_status = self._parse_sample_check_status(task)
        content_blocker = self._is_severe_content_blocker(feature_result)

        content_norm = self._score_to_norm(content_potential_score)
        core_score_a_norm, route_a, competition_downgraded_a = self._evaluate_route_variant(
            market_match_norm=market_match_norm,
            market_match_status=market_match_status,
            store_fit_norm=store_fit_norm,
            content_norm=content_norm,
            supply_check_status=supply_result.supply_check_status,
            competition_level=competition_level,
            competition_confidence=competition_confidence,
            severe_content_blocker=content_blocker,
            hard_blacklist_hit=blacklist_status == "hard",
            variant="A",
        )
        core_score_b_norm, route_b, _ = self._evaluate_route_variant(
            market_match_norm=market_match_norm,
            market_match_status=market_match_status,
            store_fit_norm=store_fit_norm,
            content_norm=content_norm,
            supply_check_status=supply_result.supply_check_status,
            competition_level=competition_level,
            competition_confidence=competition_confidence,
            severe_content_blocker=content_blocker,
            hard_blacklist_hit=blacklist_status == "hard",
            variant="B",
        )

        chosen_route = route_a
        needs_manual_review = chosen_route == ROUTE_PENDING_REVIEW
        manual_review_reason = self._manual_review_reason(
            route=chosen_route,
            market_match_status=market_match_status,
            content_norm=content_norm,
            direction_confidence_low=direction_confidence_low,
            feature_result=feature_result,
        )
        decision_reason = self._build_decision_reason(
            route=chosen_route,
            market_match_norm=market_match_norm,
            market_match_status=market_match_status,
            store_fit_norm=store_fit_norm,
            content_norm=content_norm,
            core_score_norm=core_score_a_norm,
            supply_check_status=supply_result.supply_check_status,
            competition_level=competition_level,
            competition_confidence=competition_confidence,
            competition_downgraded=competition_downgraded_a,
            direction_confidence_low=direction_confidence_low,
            hard_blacklist_hit=blacklist_status == "hard",
            soft_blacklist_hit=blacklist_status == "soft",
            severe_content_blocker=content_blocker,
            needs_manual_review=needs_manual_review,
        )
        reserve_reason, reserve_created_at, reserve_expires_at, reserve_status = self._build_reserve_fields(
            task=task,
            route=chosen_route,
            supply_check_status=supply_result.supply_check_status,
            market_match_norm=market_match_norm,
            market_match_status=market_match_status,
            store_fit_norm=store_fit_norm,
        )
        observation_tags = self._build_observation_tags(
            feature_result=feature_result,
            market_match_status=market_match_status,
            direction_confidence_low=direction_confidence_low,
            blacklist_status=blacklist_status,
            supply_result=supply_result,
            needs_manual_review=needs_manual_review,
        )
        return ScoredAnalysisResult(
            analysis_category=feature_result.analysis_category,
            product_potential=product_potential,
            content_potential=content_potential,
            batch_priority_score=round(core_score_a_norm * 100.0, 2),
            suggested_action=self._route_to_action(chosen_route),
            brief_reason=decision_reason.narrative,
            market_match_score=round(market_match_score * 100.0, 2) if market_match_score is not None else None,
            market_match_status=market_match_status,
            store_fit_score=round(store_fit_norm * 100.0, 2),
            content_potential_score=round(content_potential_score, 2),
            core_score_a=round(core_score_a_norm * 100.0, 2),
            route_a=route_a,
            core_score_b=round(core_score_b_norm * 100.0, 2),
            route_b=route_b,
            supply_check_status=supply_result.supply_check_status,
            supply_summary=supply_result.supply_summary,
            competition_reference_level=competition_level,
            competition_confidence=competition_confidence,
            decision_reason=decision_reason,
            needs_manual_review=needs_manual_review,
            manual_review_reason=manual_review_reason,
            observation_tags=observation_tags,
            recommended_content_formulas=self._recommended_content_formulas(feature_result),
            reserve_reason=reserve_reason,
            reserve_created_at=reserve_created_at,
            reserve_expires_at=reserve_expires_at,
            reserve_status=reserve_status,
            sample_check_status=sample_check_status,
            matched_market_direction_id=match_result.matched_market_direction_id,
            matched_market_direction_name=match_result.matched_market_direction_name,
            matched_market_direction_reason=match_result.matched_market_direction_reason,
            matched_market_direction_confidence=match_result.decision_confidence,
            matched_market_direction_family=match_result.direction_family,
            matched_market_direction_tier=match_result.direction_tier,
            default_content_route_preference=match_result.default_content_route_preference,
            v2_shadow_result=self.v2_scorer.score(
                task=task,
                feature_result=feature_result,
                match_result=match_result,
                supply_result=supply_result,
                core_score_a=round(core_score_a_norm * 100.0, 2),
                route_a=route_a,
            ),
        )

    def calibrate_group(self, items: List[PendingAnalysisItem]) -> List[PendingAnalysisItem]:
        if not items:
            return []

        sortable_items = sorted(items, key=lambda item: item.scored_result.core_score_a, reverse=True)
        allowed_priority_ids_a = self._allowed_priority_ids(sortable_items, route_field="route_a", score_field="core_score_a")
        allowed_priority_ids_b = self._allowed_priority_ids(sortable_items, route_field="route_b", score_field="core_score_b")

        calibrated = []
        for item in sortable_items:
            route_a = self._apply_priority_cap(
                current_route=item.scored_result.route_a,
                record_id=item.task.source_record_id,
                allowed_priority_ids=allowed_priority_ids_a,
            )
            route_b = self._apply_priority_cap(
                current_route=item.scored_result.route_b,
                record_id=item.task.source_record_id,
                allowed_priority_ids=allowed_priority_ids_b,
            )
            calibration_downgraded = route_a != item.scored_result.route_a
            decision_reason = item.scored_result.decision_reason
            if calibration_downgraded:
                decision_reason = self._build_decision_reason(
                    route=route_a,
                    market_match_norm=self._score_to_norm(item.scored_result.market_match_score),
                    market_match_status=item.scored_result.market_match_status,
                    store_fit_norm=self._score_to_norm(item.scored_result.store_fit_score),
                    content_norm=self._score_to_norm(item.scored_result.content_potential_score),
                    core_score_norm=self._score_to_norm(item.scored_result.core_score_a),
                    supply_check_status=item.scored_result.supply_check_status,
                    competition_level=item.scored_result.competition_reference_level,
                    competition_confidence=item.scored_result.competition_confidence,
                    competition_downgraded=False,
                    direction_confidence_low=item.scored_result.matched_market_direction_confidence == "low",
                    hard_blacklist_hit=False,
                    soft_blacklist_hit=False,
                    severe_content_blocker=False,
                    needs_manual_review=item.scored_result.needs_manual_review,
                    calibration_downgraded=True,
                )
            scored_result = replace(
                item.scored_result,
                route_a=route_a,
                route_b=route_b,
                suggested_action=self._route_to_action(route_a),
                brief_reason=decision_reason.narrative,
                decision_reason=decision_reason,
            )
            calibrated.append(
                PendingAnalysisItem(
                    task=item.task,
                    feature_result=item.feature_result,
                    scored_result=scored_result,
                )
            )
        self.v2_scorer.apply_cautious_quota(
            [
                item.scored_result.v2_shadow_result
                for item in calibrated
                if item.scored_result and item.scored_result.v2_shadow_result
            ]
        )
        return calibrated

    def _compute_content_potential_score(self, feature_result: FeatureAnalysisResult) -> float:
        category = feature_result.analysis_category
        if category == SupportedCategory.HAIR_ACCESSORY.value:
            return self._compute_hair_content_score(feature_result)
        return self._compute_light_top_content_score(feature_result)

    def _compute_hair_content_score(self, feature_result: FeatureAnalysisResult) -> float:
        scores = feature_result.feature_scores
        value = (
            0.30 * self._level_score(scores["wearing_change_strength"])
            + 0.25 * self._level_score(scores["demo_ease"])
            + 0.25 * self._level_score(scores["visual_memory_point"])
            + 0.15 * self._reversed_level_score(scores["homogenization_risk"])
            + 0.05 * self._level_score(scores["title_selling_clarity"])
        )
        return round(value, 2)

    def _compute_light_top_content_score(self, feature_result: FeatureAnalysisResult) -> float:
        scores = feature_result.feature_scores
        value = (
            0.28 * self._level_score(scores["upper_body_change_strength"])
            + 0.24 * self._level_score(scores["camera_readability"])
            + 0.18 * self._level_score(scores["design_signal_strength"])
            + 0.18 * self._level_score(scores["basic_style_escape_strength"])
            + 0.07 * self._level_score(scores["title_selling_clarity"])
            + 0.05 * self._level_score(scores["info_completeness"])
        )
        return round(value, 2)

    def _map_product_potential(
        self,
        market_match_norm: Optional[float],
        market_match_status: str,
        store_fit_norm: float,
    ) -> str:
        signals = [store_fit_norm]
        if market_match_status != MarketMatchStatus.UNCOVERED.value and market_match_norm is not None:
            signals.append(market_match_norm)
        combined = (sum(signals) / max(len(signals), 1)) * 100.0
        return self._score_to_level(combined, high_threshold=75.0, low_threshold=55.0)

    def _resolve_market_match_context(
        self,
        market_direction_result: Optional[MarketDirectionMatchResult],
    ) -> Tuple[Optional[float], Optional[float], str, bool]:
        if not market_direction_result:
            return None, None, MarketMatchStatus.UNCOVERED.value, False

        raw_score = min(max(float(market_direction_result.score or 0.0), 0.0), 1.0)
        status = str(getattr(market_direction_result, "market_match_status", "") or "").strip()
        if not status:
            if market_direction_result.matched_market_direction_id:
                status = MarketMatchStatus.MATCHED.value
            elif raw_score > 0:
                status = MarketMatchStatus.WEAK_MATCHED.value
            else:
                status = MarketMatchStatus.UNCOVERED.value

        if status == MarketMatchStatus.UNCOVERED.value:
            return None, None, status, False

        confidence = str(getattr(market_direction_result, "decision_confidence", "") or "").strip().lower()
        direction_confidence_low = confidence == "low"
        effective_score = raw_score
        if direction_confidence_low:
            effective_score = min(effective_score, LOW_CONFIDENCE_MARKET_MATCH_CAP)
        return effective_score, effective_score, status, direction_confidence_low

    def _compute_store_fit_context(
        self,
        task,
        feature_result: FeatureAnalysisResult,
        store_card: StorePositioningCard,
        market_direction_result: Optional[MarketDirectionMatchResult],
    ) -> Tuple[float, str]:
        if not self._has_store_card_data(store_card):
            return STORE_FIT_FALLBACK_SCORE, ""

        searchable_text = self._build_searchable_text(task, feature_result, market_direction_result)
        matched_style = str(getattr(market_direction_result, "style_main", "") or "").strip()
        matched_scenes = list(getattr(market_direction_result, "scene_tags", []) or [])

        weighted_scores: List[Tuple[float, float]] = []
        legacy_soft_blacklist = list(store_card.soft_style_blacklist or store_card.style_blacklist or [])
        hard_blacklist_hit = self._blacklist_matches(searchable_text, matched_style, store_card.hard_style_blacklist)
        soft_blacklist_hit = self._blacklist_matches(searchable_text, matched_style, legacy_soft_blacklist)

        if hard_blacklist_hit:
            return 0.0, "hard"
        if soft_blacklist_hit:
            weighted_scores.append((0.10, 0.40))
        elif store_card.style_whitelist:
            whitelist_hit = any(self._term_matches(searchable_text, term) for term in store_card.style_whitelist)
            if matched_style and matched_style in store_card.style_whitelist:
                whitelist_hit = True
            weighted_scores.append((1.0 if whitelist_hit else 0.35, 0.40))

        target_price_cny = normalize_task_target_price_to_cny(task)

        if store_card.target_price_bands:
            if target_price_cny is None:
                weighted_scores.append((0.55, 0.20))
            else:
                band_hit = self._target_price_matches_bands(target_price_cny, store_card.target_price_bands)
                weighted_scores.append((1.0 if band_hit else 0.35, 0.20))

        if store_card.core_scenes:
            scene_hit = any(self._term_matches(searchable_text, term) for term in store_card.core_scenes)
            if not scene_hit and matched_scenes:
                scene_hit = bool(set(store_card.core_scenes) & set(matched_scenes))
            weighted_scores.append((1.0 if scene_hit else 0.40, 0.20))

        if store_card.content_tones:
            tone_hit = any(self._term_matches(searchable_text, term) for term in store_card.content_tones)
            weighted_scores.append((1.0 if tone_hit else 0.45, 0.20))

        if not weighted_scores:
            return STORE_FIT_FALLBACK_SCORE, ""
        total_weight = sum(weight for _, weight in weighted_scores) or 1.0
        score = sum(value * weight for value, weight in weighted_scores) / total_weight
        blacklist_status = ""
        if soft_blacklist_hit:
            score = min(score, SOFT_BLACKLIST_STORE_FIT_CAP)
            blacklist_status = "soft"
        return round(min(max(score, 0.0), 1.0), 4), blacklist_status

    def _blacklist_matches(self, searchable_text: str, matched_style: str, terms: List[str]) -> bool:
        normalized_terms = [self._safe_text(term) for term in list(terms or []) if self._safe_text(term)]
        if not normalized_terms:
            return False
        if matched_style and matched_style in normalized_terms:
            return True
        return any(self._term_matches(searchable_text, term) for term in normalized_terms)

    def _is_severe_content_blocker(self, feature_result: FeatureAnalysisResult) -> bool:
        risk_tag = self._safe_text(feature_result.risk_tag)
        risk_tag_hit = risk_tag in SERIOUS_CONTENT_RISK_TAGS
        feature_scores = feature_result.feature_scores
        code_rule_hit = False
        if feature_result.analysis_category == SupportedCategory.HAIR_ACCESSORY.value:
            code_rule_hit = (
                feature_scores.get("info_completeness") == "低"
                or feature_scores.get("demo_ease") == "低"
                or feature_scores.get("wearing_change_strength") == "低"
            )
        else:
            code_rule_hit = (
                feature_scores.get("info_completeness") == "低"
                or feature_scores.get("camera_readability") == "低"
                or feature_scores.get("upper_body_change_strength") == "低"
            )
        return risk_tag_hit and code_rule_hit

    def _manual_review_reason(
        self,
        route: str,
        market_match_status: str,
        content_norm: float,
        direction_confidence_low: bool,
        feature_result: FeatureAnalysisResult,
    ) -> str:
        if route != ROUTE_PENDING_REVIEW:
            return ""
        if market_match_status == MarketMatchStatus.UNCOVERED.value:
            return "方向卡未覆盖，内容判断需人工兜底"
        if content_norm < 0.45 and direction_confidence_low:
            return "方向卡置信度偏低且内容分偏低，建议人工复核"
        if content_norm < 0.45:
            return "内容分处于边界区，需人工确认是否误杀"
        return "{tag}，建议人工复核".format(tag=self._safe_text(feature_result.risk_tag) or "边界案例")

    def _build_observation_tags(
        self,
        feature_result: FeatureAnalysisResult,
        market_match_status: str,
        direction_confidence_low: bool,
        blacklist_status: str,
        supply_result: AccioSupplyResult,
        needs_manual_review: bool,
    ) -> List[str]:
        tags = []
        if market_match_status == MarketMatchStatus.UNCOVERED.value:
            tags.append("direction_uncovered")
        elif market_match_status == MarketMatchStatus.WEAK_MATCHED.value:
            tags.append("style_borderline")
        if direction_confidence_low:
            tags.append("direction_confidence_low")
        if blacklist_status == "soft":
            tags.append("style_borderline")
        if needs_manual_review:
            tags.append("content_borderline")
        if supply_result.supply_check_status in {"watch", "fail"} and "毛利" in self._safe_text(supply_result.supply_summary):
            tags.append("price_mismatch")
        risk_tag = self._safe_text(feature_result.risk_tag)
        if risk_tag == "图片信息不足":
            tags.append("content_borderline")
        return list(dict.fromkeys(tags))

    def _evaluate_route_variant(
        self,
        market_match_norm: Optional[float],
        market_match_status: str,
        store_fit_norm: float,
        content_norm: float,
        supply_check_status: str,
        competition_level: str,
        competition_confidence: str,
        severe_content_blocker: bool,
        hard_blacklist_hit: bool,
        variant: str,
    ) -> Tuple[float, str, bool]:
        weights = VARIANT_WEIGHTS[variant]
        weighted_sum = content_norm * weights["content_potential"] + store_fit_norm * weights["store_fit"]
        total_weight = weights["content_potential"] + weights["store_fit"]
        if market_match_status != MarketMatchStatus.UNCOVERED.value and market_match_norm is not None:
            weighted_sum += market_match_norm * weights["market_match"]
            total_weight += weights["market_match"]
        core_score_norm = weighted_sum / max(total_weight, 1e-6)

        if hard_blacklist_hit:
            return core_score_norm, ROUTE_REJECT, False
        if supply_check_status == "fail":
            return core_score_norm, ROUTE_REJECT, False
        if store_fit_norm < 0.5:
            if market_match_status == MarketMatchStatus.UNCOVERED.value:
                route = ROUTE_RESERVE
            else:
                route = ROUTE_RESERVE if (market_match_norm or 0.0) >= 0.7 else ROUTE_REJECT
            return core_score_norm, route, False
        if content_norm < 0.45:
            if severe_content_blocker:
                return core_score_norm, ROUTE_REJECT, False
            return core_score_norm, ROUTE_PENDING_REVIEW, False

        if core_score_norm >= 0.75 and supply_check_status == "pass":
            route = ROUTE_PRIORITY_TEST
        elif 0.55 <= core_score_norm < 0.75 and supply_check_status in {"pass", "watch", "pending"}:
            route = ROUTE_SMALL_TEST
        elif market_match_status != MarketMatchStatus.UNCOVERED.value and (market_match_norm or 0.0) >= 0.7 and store_fit_norm < 0.5:
            route = ROUTE_RESERVE
        elif supply_check_status == "timeout":
            route = ROUTE_RESERVE
        else:
            route = ROUTE_REJECT

        competition_downgraded = False
        if (
            route == ROUTE_PRIORITY_TEST
            and competition_level == "high"
            and competition_confidence == "high"
            and 0.75 <= core_score_norm < 0.85
        ):
            route = ROUTE_SMALL_TEST
            competition_downgraded = True
        return core_score_norm, route, competition_downgraded

    def _build_decision_reason(
        self,
        route: str,
        market_match_norm: Optional[float],
        market_match_status: str,
        store_fit_norm: float,
        content_norm: float,
        core_score_norm: float,
        supply_check_status: str,
        competition_level: str,
        competition_confidence: str,
        competition_downgraded: bool,
        direction_confidence_low: bool,
        hard_blacklist_hit: bool,
        soft_blacklist_hit: bool,
        severe_content_blocker: bool,
        needs_manual_review: bool,
        calibration_downgraded: bool = False,
    ) -> DecisionReason:
        primary: List[str] = []
        secondary: List[str] = []
        supporting: List[str] = []

        if market_match_status == MarketMatchStatus.UNCOVERED.value:
            secondary.append("direction_uncovered")
        elif market_match_status == MarketMatchStatus.WEAK_MATCHED.value:
            secondary.append("market_match_weak")
        elif (market_match_norm or 0.0) >= 0.7:
            supporting.append("market_match_ok")
        if store_fit_norm >= 0.5:
            supporting.append("store_fit_ok")
        if content_norm >= 0.45:
            supporting.append("content_potential_ok")
        if supply_check_status == "pass":
            supporting.append("supply_pass")
        elif supply_check_status == "watch":
            secondary.append("supply_watch")
        elif supply_check_status == "pending":
            secondary.append("supply_pending")

        if competition_downgraded:
            secondary.append("competition_high")
        elif competition_level == "high" and competition_confidence == "low":
            secondary.append("competition_low_confidence_ignored")
        if direction_confidence_low:
            secondary.append("direction_confidence_low")
        if soft_blacklist_hit:
            secondary.append("soft_blacklist_hit")
        if calibration_downgraded:
            secondary.append("batch_calibration_limit")

        if hard_blacklist_hit:
            primary.append("hard_blacklist_hit")
            narrative = "命中店铺硬黑名单，当前不建议推进。"
        elif supply_check_status == "fail":
            primary.append("supply_fail")
            narrative = "供给证据不足或粗利润不成立，当前不建议推进。"
        elif content_norm < 0.45 and severe_content_blocker:
            primary.append("content_potential_low")
            narrative = "内容可做性偏低，且严重风险已同时命中，当前不建议推进。"
        elif needs_manual_review:
            primary.append("content_borderline_review")
            if market_match_status == MarketMatchStatus.UNCOVERED.value:
                narrative = "方向卡暂未覆盖该商品，且内容判断处于边界区，建议人工复核。"
            else:
                narrative = "内容可做性判断处于边界区，先进入人工复核，不直接否决。"
        elif store_fit_norm < 0.5 and market_match_status != MarketMatchStatus.UNCOVERED.value and (market_match_norm or 0.0) >= 0.7:
            primary.append("store_fit_low")
            supporting.append("market_match_strong")
            narrative = "市场方向匹配度较高，但和当前店铺定位不够贴合，先放入备用池。"
        elif store_fit_norm < 0.5:
            primary.append("store_fit_low")
            if market_match_status == MarketMatchStatus.UNCOVERED.value:
                narrative = "店铺适配度不足，且方向卡暂未覆盖，先放入备用池等待人工判断。"
            else:
                narrative = "店铺适配度不足，且市场匹配不够强，当前不建议推进。"
        elif supply_check_status == "timeout":
            primary.append("supply_timeout")
            narrative = "供给信息超时未回收，先放入备用池，等待人工补充判断。"
        elif calibration_downgraded:
            primary.append("priority_slot_limited")
            narrative = "基础分达到优先测试，但批内优先测试名额收敛后，降为低成本试款。"
        elif competition_downgraded:
            primary.append("competition_high")
            narrative = "三项主评分已过优先测试线，但高置信度竞争参考偏高，降为低成本试款。"
        elif route == ROUTE_PRIORITY_TEST:
            primary.append("core_score_high")
            narrative = "市场匹配、店铺匹配和内容可做性都过线，且供给检查通过，建议优先测试。"
        elif route == ROUTE_SMALL_TEST:
            primary.append("core_score_mid")
            if supply_check_status == "watch":
                narrative = "主评分达到可测试区间，但供给侧还有疑点，先做低成本试款。"
            elif supply_check_status == "pending":
                narrative = "主评分达到可测试区间，供给信息仍在等待回收，先保持低成本试款。"
            else:
                narrative = "主评分达到可测试区间，建议先做低成本试款验证。"
        elif route == ROUTE_RESERVE:
            primary.append("reserve_rule_hit")
            if market_match_status == MarketMatchStatus.UNCOVERED.value:
                narrative = "当前方向卡体系尚未覆盖该商品，先放入备用池，等待人工补充判断。"
            else:
                narrative = "当前更适合放入备用池，等待店铺定位或供给条件进一步明确。"
        else:
            primary.append("core_score_low")
            if core_score_norm < 0.55:
                narrative = "综合分未达到测试线，当前不建议推进。"
            else:
                narrative = "当前关键条件未同时满足，暂不建议推进。"

        return DecisionReason(
            primary_drivers=primary[:2],
            secondary_drivers=secondary[:3],
            supporting_factors=list(dict.fromkeys(supporting))[:4],
            narrative=narrative,
        )

    def _build_reserve_fields(
        self,
        task,
        route: str,
        supply_check_status: str,
        market_match_norm: Optional[float],
        market_match_status: str,
        store_fit_norm: float,
    ) -> Tuple[str, int, int, str]:
        extra_fields = getattr(task, "extra_fields", {}) or {}
        now = datetime.now()
        now_millis = int(now.timestamp() * 1000)
        created_at = self._parse_datetime_millis(self._find_extra_value(extra_fields, RESERVE_CREATED_AT_FIELDS))
        expires_at = self._parse_datetime_millis(self._find_extra_value(extra_fields, RESERVE_EXPIRES_AT_FIELDS))

        if route != ROUTE_RESERVE:
            existing_status = self._normalize_reserve_status(self._find_extra_value(extra_fields, RESERVE_STATUS_FIELDS))
            if created_at and not expires_at:
                expires_at = int((datetime.fromtimestamp(created_at / 1000.0) + timedelta(days=90)).timestamp() * 1000)
            if expires_at and expires_at <= now_millis:
                existing_status = "archived"
            return "", created_at, expires_at, existing_status

        if not created_at:
            created_at = now_millis
        if not expires_at:
            expires_at = int((datetime.fromtimestamp(created_at / 1000.0) + timedelta(days=90)).timestamp() * 1000)
        reserve_status = "archived" if expires_at <= now_millis else "active"

        reserve_reason = self._normalize_route_reason(self._find_extra_value(extra_fields, RESERVE_REASON_FIELDS))
        if not reserve_reason:
            if supply_check_status == "timeout":
                reserve_reason = "supply_timeout"
            elif market_match_status == MarketMatchStatus.UNCOVERED.value:
                reserve_reason = "direction_uncovered"
            elif (market_match_norm or 0.0) >= 0.7 and store_fit_norm < 0.5:
                reserve_reason = "market_ok_store_not_fit"
            else:
                reserve_reason = "reserve_rule_hit"
        return reserve_reason, created_at, expires_at, reserve_status

    def _resolve_store_positioning_card(self, task, feature_result: FeatureAnalysisResult) -> StorePositioningCard:
        store_card = self._load_store_positioning_card_from_db(task=task, analysis_category=feature_result.analysis_category)
        if self._has_store_card_data(store_card):
            return store_card
        return self._parse_store_positioning_card(getattr(task, "extra_fields", {}) or {})

    def _load_store_positioning_card_from_db(self, task, analysis_category: str) -> StorePositioningCard:
        extra_fields = getattr(task, "extra_fields", {}) or {}
        store_id = self._safe_text(self._find_extra_value(extra_fields, STORE_ID_FIELDS))
        card_name = self._safe_text(self._find_extra_value(extra_fields, STORE_CARD_NAME_FIELDS))
        batch_id = self._safe_text(getattr(task, "batch_id", ""))
        if not store_id and batch_id:
            store_id = batch_id
        if not card_name and batch_id:
            card_name = batch_id
        country = self._normalize_store_country(
            self._safe_text(getattr(task, "target_market", "")) or self._safe_text(self._find_extra_value(extra_fields, STORE_COUNTRY_FIELDS))
        )
        category = self._normalize_store_category(
            analysis_category
            or self._safe_text(getattr(task, "final_category", ""))
            or self._safe_text(self._find_extra_value(extra_fields, STORE_CATEGORY_FIELDS))
        )
        if not store_id and not card_name:
            return StorePositioningCard()
        return self.store_positioning_database.load_store_positioning_card(
            store_id=store_id,
            country=country,
            category=category,
            card_name=card_name,
        )

    def _parse_store_positioning_card(self, extra_fields: Dict[str, Any]) -> StorePositioningCard:
        payload = self._parse_json_value(self._find_extra_value(extra_fields, STORE_POSITIONING_CARD_FIELDS))
        if not isinstance(payload, dict):
            payload = {}
        return StorePositioningCard(
            store_id=self._safe_text(
                payload.get("store_id")
                or payload.get("shop_id")
                or self._find_extra_value(extra_fields, STORE_ID_FIELDS)
            ),
            source_record_id=self._safe_text(payload.get("source_record_id")),
            country=self._normalize_store_country(
                self._safe_text(payload.get("country") or self._find_extra_value(extra_fields, STORE_COUNTRY_FIELDS))
            ),
            category=self._normalize_store_category(
                self._safe_text(payload.get("category") or self._find_extra_value(extra_fields, STORE_CATEGORY_FIELDS))
            ),
            card_name=self._safe_text(
                payload.get("card_name")
                or payload.get("name")
                or self._find_extra_value(extra_fields, STORE_CARD_NAME_FIELDS)
            ),
            style_whitelist=self._to_string_list(
                payload.get("style_whitelist")
                or payload.get("allowed_styles")
                or self._find_extra_value(extra_fields, STORE_STYLE_WHITELIST_FIELDS)
            ),
            style_blacklist=self._to_string_list(
                payload.get("style_blacklist")
                or payload.get("blocked_styles")
                or self._find_extra_value(extra_fields, STORE_STYLE_BLACKLIST_FIELDS)
            ),
            soft_style_blacklist=self._to_string_list(
                payload.get("soft_style_blacklist")
                or payload.get("softBlacklist")
                or self._find_extra_value(extra_fields, STORE_SOFT_STYLE_BLACKLIST_FIELDS)
                or payload.get("style_blacklist")
                or payload.get("blocked_styles")
                or self._find_extra_value(extra_fields, STORE_STYLE_BLACKLIST_FIELDS)
            ),
            hard_style_blacklist=self._to_string_list(
                payload.get("hard_style_blacklist")
                or payload.get("hardBlacklist")
                or self._find_extra_value(extra_fields, STORE_HARD_STYLE_BLACKLIST_FIELDS)
            ),
            target_price_bands=self._to_string_list(
                payload.get("target_price_bands")
                or payload.get("price_bands")
                or self._find_extra_value(extra_fields, STORE_PRICE_BAND_FIELDS)
            ),
            core_scenes=self._to_string_list(
                payload.get("core_scenes")
                or payload.get("scene_tags")
                or self._find_extra_value(extra_fields, STORE_SCENE_FIELDS)
            ),
            content_tones=self._to_string_list(
                payload.get("content_tones")
                or payload.get("content_tone")
                or self._find_extra_value(extra_fields, STORE_CONTENT_TONE_FIELDS)
            ),
            core_value_points=self._to_string_list(
                payload.get("core_value_points")
                or payload.get("value_points")
                or self._find_extra_value(extra_fields, STORE_VALUE_POINT_FIELDS)
            ),
            target_audience=self._to_string_list(
                payload.get("target_audience")
                or self._find_extra_value(extra_fields, STORE_TARGET_AUDIENCE_FIELDS)
            ),
            selection_principles=self._to_string_list(
                payload.get("selection_principles")
                or self._find_extra_value(extra_fields, STORE_SELECTION_PRINCIPLE_FIELDS)
            ),
            notes=self._safe_text(
                payload.get("notes")
                or self._find_extra_value(extra_fields, STORE_NOTES_FIELDS)
            ),
        )

    def _parse_supply_result(self, task) -> AccioSupplyResult:
        extra_fields = getattr(task, "extra_fields", {}) or {}
        explicit_status = self._normalize_supply_status(self._find_extra_value(extra_fields, SUPPLY_STATUS_FIELDS))
        accio_status = self._normalize_supply_status(self._find_extra_value(extra_fields, ACCIO_STATUS_FIELDS))
        source_url = self._safe_text(self._find_extra_value(extra_fields, SUPPLY_URL_FIELDS))
        procurement_price_rmb = self._to_number(self._find_extra_value(extra_fields, SUPPLY_PRICE_FIELDS))
        gross_assessment = self._assess_supply_margin(extra_fields, task, procurement_price_rmb)
        status = explicit_status or accio_status

        if not status:
            if source_url or procurement_price_rmb is not None:
                status = "pass" if gross_assessment == "pass" else "watch"
            else:
                status = "pending"

        if gross_assessment == "fail":
            status = "fail"
        elif status == "pass" and gross_assessment == "watch":
            status = "watch"

        summary = self._safe_text(self._find_extra_value(extra_fields, SUPPLY_SUMMARY_FIELDS))
        if not summary:
            parts = []
            if source_url:
                parts.append(source_url)
            if procurement_price_rmb is not None:
                parts.append("采购价约 {price:.2f} RMB".format(price=procurement_price_rmb))
            if gross_assessment == "watch":
                parts.append("毛利边界需复核")
            elif gross_assessment == "fail":
                parts.append("粗利润不成立")
            summary = "；".join(parts)[:200]
        return AccioSupplyResult(
            supply_check_status=status,
            supply_summary=summary,
            source_url=source_url,
            procurement_price_rmb=procurement_price_rmb,
        )

    def _parse_competition_reference(self, task) -> Tuple[str, str]:
        extra_fields = getattr(task, "extra_fields", {}) or {}
        level = self._normalize_competition_level(self._find_extra_value(extra_fields, COMPETITION_LEVEL_FIELDS))
        confidence = self._normalize_confidence(self._find_extra_value(extra_fields, COMPETITION_CONFIDENCE_FIELDS))
        if not confidence:
            source_type = self._safe_text(getattr(task, "source_type", "")).lower()
            confidence = "high" if "fastmoss" in source_type else "low"
        return level or "medium", confidence

    def _parse_sample_check_status(self, task) -> str:
        extra_fields = getattr(task, "extra_fields", {}) or {}
        value = self._safe_text(self._find_extra_value(extra_fields, SAMPLE_CHECK_STATUS_FIELDS)).lower()
        if value in {"pass", "通过", "合格"}:
            return "pass"
        if value in {"fail", "失败", "不合格"}:
            return "fail"
        if value in {"switch_supplier", "换供应商", "切供应商"}:
            return "switch_supplier"
        return "pending"

    def _recommended_content_formulas(self, feature_result: FeatureAnalysisResult) -> List[str]:
        category = feature_result.analysis_category
        scores = feature_result.feature_scores
        if category == SupportedCategory.HAIR_ACCESSORY.value:
            formula_specs = [
                ("wearing_change_strength", "佩戴前后变化对比", False),
                ("demo_ease", "手部快速佩戴演示", False),
                ("visual_memory_point", "记忆点特写放大", False),
                ("homogenization_risk", "同场景替代对比", True),
                ("title_selling_clarity", "标题卖点直给开场", False),
            ]
        else:
            formula_specs = [
                ("upper_body_change_strength", "上身前后变化对比", False),
                ("camera_readability", "远近镜头切换试穿", False),
                ("design_signal_strength", "设计点特写放大", False),
                ("basic_style_escape_strength", "基础款替代对比", False),
                ("title_selling_clarity", "标题卖点直给开场", False),
                ("info_completeness", "多场景连续试穿", False),
            ]

        ranked = []
        for field_name, formula_name, use_reversed in formula_specs:
            if field_name not in scores:
                continue
            score = self._reversed_level_score(scores[field_name]) if use_reversed else self._level_score(scores[field_name])
            ranked.append((score, formula_name))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        formulas = [formula for _, formula in ranked[:3]]
        return formulas or ["基础信息不足，建议先补图再定内容公式"]

    def _allowed_priority_ids(
        self,
        items: List[PendingAnalysisItem],
        route_field: str,
        score_field: str,
    ) -> set[str]:
        if not items:
            return set()
        priority_candidates = [
            item
            for item in items
            if getattr(item.scored_result, route_field) == ROUTE_PRIORITY_TEST
            and float(getattr(item.scored_result, score_field) or 0.0) >= 75.0
        ]
        if not priority_candidates:
            return set()
        if len(priority_candidates) < 3:
            return {item.task.source_record_id for item in priority_candidates}
        priority_slots = max(1, int(math.ceil(len(items) * 0.3)))
        top_items = sorted(items, key=lambda item: float(getattr(item.scored_result, score_field) or 0.0), reverse=True)[:priority_slots]
        top_ids = {item.task.source_record_id for item in top_items}
        return {item.task.source_record_id for item in priority_candidates if item.task.source_record_id in top_ids}

    def _apply_priority_cap(self, current_route: str, record_id: str, allowed_priority_ids: set[str]) -> str:
        if current_route == ROUTE_PRIORITY_TEST and record_id not in allowed_priority_ids:
            return ROUTE_SMALL_TEST
        return current_route

    def _target_price_matches_bands(self, target_price, price_bands: List[str]) -> bool:
        try:
            value = float(target_price)
        except (TypeError, ValueError):
            return False
        for band in price_bands:
            normalized = (
                self._safe_text(band)
                .replace("元", "")
                .replace("RMB", "")
                .replace("rmb", "")
                .replace("CNY", "")
                .replace("cny", "")
                .replace("¥", "")
                .replace("￥", "")
                .replace(" ", "")
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

    def _assess_supply_margin(self, extra_fields: Dict[str, Any], task, procurement_price_rmb: Optional[float]) -> str:
        explicit_ok = self._find_extra_value(extra_fields, SUPPLY_GROSS_OK_FIELDS)
        explicit_ok_text = self._safe_text(explicit_ok).lower()
        if explicit_ok_text in {"true", "yes", "1", "通过", "成立", "pass"}:
            return "pass"
        if explicit_ok_text in {"false", "no", "0", "失败", "不成立", "fail"}:
            return "fail"

        distribution_margin = self._normalize_ratio(self._find_extra_value(extra_fields, SUPPLY_DISTRIBUTION_MARGIN_FIELDS))
        if distribution_margin is not None:
            if distribution_margin < 0.15:
                return "fail"
            if distribution_margin < 0.22:
                return "watch"
            return "pass"

        gross_margin = self._normalize_ratio(self._find_extra_value(extra_fields, SUPPLY_GROSS_MARGIN_FIELDS))
        if gross_margin is not None:
            if gross_margin < 0.25:
                return "fail"
            if gross_margin < 0.35:
                return "watch"
            return "pass"

        target_price_cny = normalize_task_target_price_to_cny(task)
        if procurement_price_rmb is not None and target_price_cny:
            try:
                ratio = float(procurement_price_rmb) / float(target_price_cny)
            except (TypeError, ValueError, ZeroDivisionError):
                return "unknown"
            if ratio >= 0.75:
                return "fail"
            if ratio >= 0.60:
                return "watch"
            return "pass"
        return "unknown"

    def _build_searchable_text(
        self,
        task,
        feature_result: FeatureAnalysisResult,
        market_direction_result: Optional[MarketDirectionMatchResult],
    ) -> str:
        parts = [
            self._safe_text(getattr(task, "product_title", "")),
            self._safe_text(getattr(task, "product_notes", "")),
            self._safe_text(getattr(task, "competitor_notes", "")),
            self._safe_text(feature_result.brief_observation),
        ]
        parts.extend([self._safe_text(item) for item in getattr(task, "title_keyword_tags", []) or []])
        if market_direction_result:
            parts.extend(list(getattr(market_direction_result, "matched_terms", []) or []))
        return "\n".join([part for part in parts if part])

    def _find_extra_value(self, mapping: Dict[str, Any], aliases: List[str]) -> Any:
        alias_set = {self._normalize_key(alias) for alias in aliases}
        for key, value in mapping.items():
            if self._normalize_key(key) in alias_set:
                return value
        return None

    def _parse_json_value(self, value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return value
        text = self._safe_text(value)
        if not text:
            return None
        try:
            return json.loads(text)
        except ValueError:
            return None

    def _to_string_list(self, value: Any) -> List[str]:
        parsed = self._parse_json_value(value)
        if isinstance(parsed, list):
            return [self._safe_text(item) for item in parsed if self._safe_text(item)]
        if isinstance(parsed, dict):
            return [self._safe_text(item) for item in parsed.values() if self._safe_text(item)]
        text = self._safe_text(value)
        if not text:
            return []
        normalized = text.replace("，", ",").replace("、", ",").replace("\n", ",")
        return [item.strip() for item in normalized.split(",") if item.strip()]

    def _to_number(self, value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        text = self._safe_text(value).replace("%", "").replace("¥", "").replace(",", "")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _normalize_ratio(self, value: Any) -> Optional[float]:
        number = self._to_number(value)
        if number is None:
            return None
        if isinstance(value, str) and "%" in value:
            return number / 100.0
        if number > 1.0:
            return number / 100.0
        return number

    def _normalize_supply_status(self, value: Any) -> str:
        text = self._safe_text(value).lower()
        if text in {"pass", "通过", "可行", "已通过", "已回收"}:
            return "pass"
        if text in {"watch", "观察", "待观察", "有疑虑"}:
            return "watch"
        if text in {"fail", "失败", "找不到货", "无货", "不成立"}:
            return "fail"
        if text in {"pending", "等待", "处理中", "已发送", "待回收", "未开始"}:
            return "pending"
        if text in {"timeout", "超时", "待人工补录"}:
            return "timeout"
        return ""

    def _normalize_competition_level(self, value: Any) -> str:
        text = self._safe_text(value).lower()
        if text in {"low", "低"}:
            return "low"
        if text in {"high", "高"}:
            return "high"
        if text in {"medium", "中", "mid"}:
            return "medium"
        return ""

    def _normalize_confidence(self, value: Any) -> str:
        text = self._safe_text(value).lower()
        if text in {"high", "高"}:
            return "high"
        if text in {"low", "低"}:
            return "low"
        return ""

    def _normalize_reserve_status(self, value: Any) -> str:
        text = self._safe_text(value).lower()
        if text in {"active", "激活", "有效"}:
            return "active"
        if text in {"archived", "归档", "已归档"}:
            return "archived"
        return ""

    def _normalize_route_reason(self, value: Any) -> str:
        return self._safe_text(value)

    def _parse_datetime_millis(self, value: Any) -> int:
        if value is None or value == "":
            return 0
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            numeric = int(value)
            return numeric if numeric > 10_000_000_000 else numeric * 1000
        text = self._safe_text(value)
        if not text:
            return 0
        if text.isdigit():
            numeric = int(text)
            return numeric if numeric > 10_000_000_000 else numeric * 1000
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return int(datetime.strptime(text, fmt).timestamp() * 1000)
            except ValueError:
                continue
        return 0

    def _has_store_card_data(self, store_card: StorePositioningCard) -> bool:
        return any(
            [
                store_card.store_id,
                store_card.card_name,
                store_card.style_whitelist,
                store_card.style_blacklist,
                store_card.soft_style_blacklist,
                store_card.hard_style_blacklist,
                store_card.target_price_bands,
                store_card.core_scenes,
                store_card.content_tones,
                store_card.core_value_points,
                store_card.target_audience,
                store_card.selection_principles,
                store_card.notes,
            ]
        )

    def _normalize_store_country(self, value: Any) -> str:
        text = self._safe_text(value).upper()
        if text in {"越南", "VN"}:
            return "VN"
        if text in {"泰国", "TH"}:
            return "TH"
        return text

    def _normalize_store_category(self, value: Any) -> str:
        mapping = {
            "发饰": "hair_accessory",
            "轻上装": "light_tops",
            "hair_accessory": "hair_accessory",
            "light_tops": "light_tops",
        }
        return mapping.get(self._safe_text(value), self._safe_text(value).lower())

    def _term_matches(self, searchable_text: str, term: str) -> bool:
        normalized_term = self._safe_text(term)
        if not normalized_term:
            return False
        return normalized_term in searchable_text

    def _score_to_level(self, score: float, high_threshold: float, low_threshold: float) -> str:
        if score >= high_threshold:
            return "高"
        if score < low_threshold:
            return "低"
        return "中"

    def _score_to_norm(self, score: float) -> float:
        numeric = float(score or 0.0)
        if numeric > 1.0:
            numeric = numeric / 100.0
        return min(max(numeric, 0.0), 1.0)

    def _route_to_action(self, route: str) -> str:
        mapping = {
            ROUTE_PRIORITY_TEST: SuggestedAction.PRIORITY_TEST.value,
            ROUTE_SMALL_TEST: SuggestedAction.LOW_COST_TEST.value,
            ROUTE_RESERVE: SuggestedAction.RESERVE.value,
            ROUTE_PENDING_REVIEW: SuggestedAction.NEED_MORE_INFO.value,
            ROUTE_REJECT: SuggestedAction.HOLD.value,
        }
        return mapping.get(route, SuggestedAction.HOLD.value)

    def _level_score(self, value: str) -> int:
        return FEATURE_LEVEL_TO_SCORE[value]

    def _reversed_level_score(self, value: str) -> int:
        return REVERSED_FEATURE_LEVEL_TO_SCORE[value]

    def _normalize_key(self, value: Any) -> str:
        return self._safe_text(value).replace("_", "").replace("-", "").replace(" ", "").lower()

    def _safe_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()
