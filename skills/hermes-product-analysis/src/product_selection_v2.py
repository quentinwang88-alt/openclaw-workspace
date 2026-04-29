#!/usr/bin/env python3
"""Shadow V2 product-selection scoring.

V2 is intentionally independent from the production ``core_score_a`` route.
It produces a complete audit object for SQLite and a small set of Feishu fields.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.enums import MarketMatchStatus, SupportedCategory
from src.market_insight_models import MarketDirectionMatchResult
from src.models import AccioSupplyResult, FeatureAnalysisResult
from src.price_normalization import normalize_task_target_price_to_cny
from selection.market_task_fit import (
    cap_action_by_pool,
    ensure_direction_execution_brief,
    evaluate_market_task_fit,
    lifecycle_for_pool,
    load_unified_decision_matrix,
    resolve_unified_decision,
    validate_task_fit_orthogonality,
)


ACTION_ORDER = {
    "eliminate": 0,
    "head_reference": 1,
    "observe": 2,
    "manual_review": 3,
    "select": 4,
}

ACTION_LABELS = {
    "select": "进入测品池",
    "manual_review": "人工复核后进入",
    "observe": "放入观察池",
    "head_reference": "头部参考留档",
    "eliminate": "淘汰",
}

LEVEL_TO_RATIO = {"高": 1.0, "中": 0.6, "低": 0.2}
REVERSE_LEVEL_TO_RATIO = {"高": 0.2, "中": 0.6, "低": 1.0}


def validate_direction_action_constraints(market_report_actions: List[str], constraints_config: Dict[str, Any]) -> None:
    configured = set((constraints_config.get("direction_action_constraints") or {}).keys())
    missing = sorted({str(action or "").strip() for action in market_report_actions if str(action or "").strip()} - configured)
    if missing:
        raise ValueError("方向动作缺少 V2 约束配置: {actions}".format(actions=", ".join(missing)))


class ProductSelectionV2Scorer(object):
    def __init__(self, skill_dir: Path):
        self.skill_dir = Path(skill_dir)
        self.config = self._load_yaml("product_selection_v2.yaml")
        self.constraints = self._load_yaml("product_selection_v2_constraints.yaml")
        self.dictionary = self._load_yaml("direction_tag_dictionary_v1.yaml")
        self.unified_decision_config = load_unified_decision_matrix(self.skill_dir)

    def score(
        self,
        task,
        feature_result: FeatureAnalysisResult,
        match_result: MarketDirectionMatchResult,
        supply_result: AccioSupplyResult,
        core_score_a: float,
        route_a: str,
    ) -> Dict[str, Any]:
        direction_match = self._score_direction_match(task, feature_result, match_result)
        product_quality = self._score_product_quality(task, feature_result, match_result, supply_result)
        content_potential = self._score_content_potential(feature_result, direction_match)
        differentiation = self._score_differentiation(task, match_result)
        brief_task_type = ""
        raw_brief = getattr(match_result, "direction_execution_brief", None) or {}
        if isinstance(raw_brief, dict):
            brief_task_type = str(raw_brief.get("task_type") or "")
        unified_decision = resolve_unified_decision(
            direction_action=str(getattr(match_result, "direction_action", "") or ""),
            brief_task_type=brief_task_type,
            config=self.unified_decision_config,
        )
        direction_execution_brief = ensure_direction_execution_brief(match_result, unified_decision, self.unified_decision_config)
        if direction_execution_brief.get("target_pool"):
            unified_decision["resolved_pool"] = direction_execution_brief.get("target_pool")
        market_task_fit = evaluate_market_task_fit(
            product=task,
            direction_execution_brief=direction_execution_brief,
            product_scores={
                "direction_match": direction_match,
                "product_quality": product_quality,
                "content_potential": content_potential,
                "differentiation": differentiation,
            },
            supporting_samples=list(getattr(match_result, "representative_products", []) or []),
        )
        orthogonality = validate_task_fit_orthogonality(direction_match, market_task_fit)
        if not orthogonality.get("is_orthogonal"):
            market_task_fit["score"] = min(
                float(market_task_fit.get("score") or 0.0),
                float(orthogonality.get("score_cap") or 10),
            )
            market_task_fit.setdefault("risk_flags", []).extend(list(orthogonality.get("risk_flags") or []))
            market_task_fit["risk_flags"] = self._dedupe(list(market_task_fit.get("risk_flags") or []))
        market_task_fit["orthogonality"] = orthogonality

        weights = dict(self.config.get("score_weights") or {})
        weighted_scores = self._weighted_scores(
            weights=weights,
            direction_match=direction_match,
            market_task_fit=market_task_fit,
            product_quality=product_quality,
            content_potential=content_potential,
            differentiation=differentiation,
        )
        total_score = round(
            sum(float(value or 0.0) for value in weighted_scores.values()),
            2,
        )
        raw_action = self._action_from_score(total_score)
        constraint = self._apply_direction_constraint(
            raw_action=raw_action,
            direction_action=str(getattr(match_result, "direction_action", "") or ""),
            content_score=float(content_potential["score"]),
            differentiation=differentiation,
        )
        pool_constraint = self._apply_unified_pool_constraint(
            raw_action=constraint["final_action"],
            unified_decision=unified_decision,
            market_task_fit=market_task_fit,
        )
        constraint["pre_unified_final_action"] = constraint["final_action"]
        constraint["final_action"] = pool_constraint["final_action"]
        constraint.setdefault("risk_flags", []).extend(pool_constraint.get("risk_flags") or [])
        constraint["risk_flags"] = self._dedupe(list(constraint.get("risk_flags") or []))
        final_action = constraint["final_action"]
        pool_meta = lifecycle_for_pool(pool_constraint["target_pool"], self.unified_decision_config)
        risk_flags = self._dedupe(
            list(direction_match.get("risk_flags") or [])
            + list(product_quality.get("risk_flags") or [])
            + list(content_potential.get("risk_flags") or [])
            + list(differentiation.get("risk_flags") or [])
            + list(market_task_fit.get("risk_flags") or [])
            + list(unified_decision.get("risk_flags") or [])
            + list(constraint.get("risk_flags") or [])
        )
        eliminate_reason = self._eliminate_reason(
            final_action=final_action,
            direction_match=direction_match,
            product_quality=product_quality,
            content_potential=content_potential,
            differentiation=differentiation,
            risk_flags=risk_flags,
        )
        result = {
            "schema_version": "product_selection_v2.shadow.2026-04-25",
            "phase_control": dict(self.config.get("phase_control") or {}),
            "product_id": str(getattr(task, "source_record_id", "") or ""),
            "product_name": str(getattr(task, "product_title", "") or ""),
            "market_id": str(getattr(task, "market_id", "") or getattr(task, "target_market", "") or ""),
            "market_name": str(getattr(task, "market_name", "") or getattr(task, "target_market", "") or ""),
            "category_id": str(getattr(task, "category_id", "") or getattr(match_result, "category_id", "") or ""),
            "category_name": str(getattr(task, "category_name", "") or getattr(task, "final_category", "") or ""),
            "market_category_profile_version": str(getattr(task, "market_category_profile_version", "") or getattr(match_result, "market_category_profile_version", "") or ""),
            "direction_id": str(getattr(match_result, "matched_market_direction_id", "") or ""),
            "direction_name": str(getattr(match_result, "matched_market_direction_name", "") or ""),
            "market_report_version": str(getattr(match_result, "schema_version", "") or ""),
            "direction_dictionary_version": str((self.dictionary.get("dictionary_meta") or {}).get("version") or ""),
            "score_weights": weights,
            "weighted_scores": weighted_scores,
            "direction_match": direction_match,
            "unified_decision": unified_decision,
            "direction_execution_brief_ref": self._brief_ref(direction_execution_brief),
            "market_task_fit": market_task_fit,
            "product_quality": product_quality,
            "content_potential": content_potential,
            "differentiation": differentiation,
            "head_product_comparison": differentiation.get("head_product_comparison") or {},
            "total_score": total_score,
            "raw_action": raw_action,
            "final_action": final_action,
            "final_action_label": ACTION_LABELS.get(final_action, final_action),
            "direction_action_constraint": constraint,
            "target_pool": pool_constraint["target_pool"],
            "pool_family": pool_meta["pool_family"],
            "dissection_subtype": pool_meta["dissection_subtype"],
            "lifecycle_status": pool_meta["lifecycle_status"],
            "risk_flags": risk_flags,
            "eliminate_reason": eliminate_reason,
            "core_score_a": round(float(core_score_a or 0.0), 2),
            "core_action": route_a,
            "shadow_mode": True,
            "v2_brief_reason": self._brief_reason(direction_match, content_potential, differentiation, final_action),
            "v2_differentiation_conclusion": differentiation.get("conclusion", ""),
            "score_history": {"last_3_batches": []},
            "score_change_attribution": {"main_reason": []},
        }
        return result

    def _weighted_scores(
        self,
        weights: Dict[str, Any],
        direction_match: Dict[str, Any],
        market_task_fit: Dict[str, Any],
        product_quality: Dict[str, Any],
        content_potential: Dict[str, Any],
        differentiation: Dict[str, Any],
    ) -> Dict[str, float]:
        return {
            "direction_match": round(self._rescale(float(direction_match.get("score") or 0.0), 30.0, float(weights.get("direction_match") or 20)), 2),
            "market_task_fit": round(self._rescale(float(market_task_fit.get("score") or 0.0), 20.0, float(weights.get("market_task_fit") or 20)), 2),
            "product_quality": round(self._rescale(float(product_quality.get("score") or 0.0), 35.0, float(weights.get("product_quality") or 30)), 2),
            "content_potential": round(self._rescale(float(content_potential.get("score") or 0.0), 20.0, float(weights.get("content_potential") or 15)), 2),
            "differentiation": round(self._rescale(float(differentiation.get("score_used") or 0.0), 15.0, float(weights.get("differentiation") or 15)), 2),
        }

    def _apply_unified_pool_constraint(
        self,
        raw_action: str,
        unified_decision: Dict[str, Any],
        market_task_fit: Dict[str, Any],
    ) -> Dict[str, Any]:
        pool = str(unified_decision.get("resolved_pool") or unified_decision.get("default_pool") or "manual_review_pool")
        max_pool = str(unified_decision.get("max_pool") or pool)
        pool_for_cap = max_pool if unified_decision.get("constraint_override") else pool
        final_action = cap_action_by_pool(raw_action, pool_for_cap)
        risk_flags = []
        if final_action != raw_action:
            risk_flags.append("unified_pool_constraint_downgrade")
        fit_level = str(market_task_fit.get("fit_level") or "")
        if fit_level in {"low", "not_applicable"} and final_action == "select":
            final_action = "manual_review"
            risk_flags.append("market_task_fit_low_downgrade")
        if unified_decision.get("has_conflict"):
            risk_flags.append("task_type_conflict")
        return {
            "target_pool": pool,
            "max_pool": max_pool,
            "raw_action": raw_action,
            "final_action": final_action,
            "risk_flags": self._dedupe(risk_flags),
        }

    def _brief_ref(self, brief: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "brief_source": str(brief.get("brief_source") or ""),
            "brief_confidence": str(brief.get("brief_confidence") or ""),
            "main_action": str(brief.get("direction_action") or ""),
            "task_type": str(brief.get("task_type") or ""),
            "target_pool": str(brief.get("target_pool") or ""),
            "risk_flags": list(brief.get("risk_flags") or []),
        }

    def _rescale(self, value: float, old_max: float, new_max: float) -> float:
        if old_max <= 0:
            return 0.0
        return max(0.0, min(new_max, value / old_max * new_max))

    def apply_cautious_quota(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for result in results:
            constraint = result.get("direction_action_constraint") or {}
            if constraint.get("direction_action") != "cautious_test" or result.get("final_action") != "select":
                continue
            direction = str((result.get("direction_match") or {}).get("matched_direction") or "方向不确定")
            grouped.setdefault(direction, []).append(result)
        for _, items in grouped.items():
            items.sort(key=lambda item: float(item.get("total_score") or 0.0), reverse=True)
            for item in items[3:]:
                item["final_action"] = "manual_review"
                item["final_action_label"] = ACTION_LABELS["manual_review"]
                item.setdefault("risk_flags", []).append("cautious_test_quota_downgrade")
                item.setdefault("direction_action_constraint", {})["quota_downgraded"] = True
        return results

    def _score_direction_match(self, task, feature_result: FeatureAnalysisResult, match_result: MarketDirectionMatchResult) -> Dict[str, Any]:
        candidates = list(getattr(match_result, "candidate_directions", []) or [])[:3]
        risk_flags = []
        if len(candidates) >= 3:
            risk_flags.append("multi_direction_candidate")
        matched_direction = str(getattr(match_result, "matched_market_direction_name", "") or "")
        status = str(getattr(match_result, "market_match_status", "") or "")
        evidence = self._direction_evidence(task, match_result)
        negative_hit = bool(evidence.get("negative_keyword_hit"))
        hit_count = sum(1 for key in ("form_match", "style_match", "scene_match") if evidence.get(key))
        partial_hit = bool(evidence.get("partial_match"))

        if not matched_direction or status == MarketMatchStatus.UNCOVERED.value:
            return {
                "matched_direction": "方向不确定",
                "candidate_directions": candidates,
                "score": 5,
                "match_level": "uncertain",
                "evidence": evidence,
                "risk_flags": ["direction_match_evidence_missing"] + risk_flags,
            }
        if not any(evidence.get(key) for key in ("form_match", "style_match", "scene_match", "partial_match")):
            return {
                "matched_direction": "方向不确定",
                "candidate_directions": candidates,
                "score": 5,
                "match_level": "evidence_missing",
                "evidence": evidence,
                "risk_flags": ["direction_match_evidence_missing"] + risk_flags,
            }
        if hit_count >= 3:
            score, level = 30, "strong"
        elif hit_count == 2 and partial_hit:
            score, level = 26, "strong_partial"
        elif hit_count == 2 and negative_hit:
            score, level = 18, "matched_with_minor_conflict"
        elif hit_count == 2:
            score, level = 22, "matched"
        elif hit_count == 1 and partial_hit:
            score, level = 14, "weak_plus"
        elif hit_count == 1:
            score, level = 10, "weak"
        else:
            score, level = 5, "uncertain"
        if negative_hit:
            risk_flags.append("negative_keyword_hit")
        return {
            "matched_direction": matched_direction,
            "candidate_directions": candidates,
            "score": score,
            "match_level": level,
            "evidence": evidence,
            "risk_flags": risk_flags,
        }

    def _score_product_quality(
        self,
        task,
        feature_result: FeatureAnalysisResult,
        match_result: MarketDirectionMatchResult,
        supply_result: AccioSupplyResult,
    ) -> Dict[str, Any]:
        scores = feature_result.feature_scores
        visual = self._scale_level(
            scores.get("visual_memory_point") or scores.get("design_signal_strength") or "中",
            10,
        )
        scene = self._scale_level(
            scores.get("demo_ease") or scores.get("camera_readability") or "中",
            8,
        )
        price_score, price_flags, price_evidence = self._price_fit_score(task, match_result)
        supply_score, supply_flags = self._supply_fit_score(task, supply_result)
        info = self._scale_level(scores.get("info_completeness", "中"), 5)
        total = round(visual + scene + price_score + supply_score + info, 2)
        return {
            "visual_style_score": visual,
            "use_scene_clarity_score": scene,
            "price_fit_score": price_score,
            "supply_fit_score": supply_score,
            "info_completeness_score": info,
            "score": total,
            "evidence": {
                "visual_style": self._one_sentence(feature_result.brief_observation),
                "use_scene": "演示/场景清晰度来自类目特征打点。",
                "price": price_evidence,
                "supply": supply_result.supply_summary,
                "info_completeness": scores.get("info_completeness", ""),
            },
            "risk_flags": price_flags + supply_flags,
        }

    def _score_content_potential(self, feature_result: FeatureAnalysisResult, direction_match: Dict[str, Any]) -> Dict[str, Any]:
        scores = feature_result.feature_scores
        if feature_result.analysis_category == SupportedCategory.HAIR_ACCESSORY.value:
            first = self._scale_level(scores.get("visual_memory_point", "中"), 6)
            demo = self._scale_level(scores.get("demo_ease", "中"), 6)
            scene = self._scale_level(scores.get("wearing_change_strength", "中"), 5)
            ai_fit = self._scale_level(scores.get("title_selling_clarity", "中"), 3)
        else:
            first = self._scale_level(scores.get("design_signal_strength", "中"), 6)
            demo = self._scale_level(scores.get("camera_readability", "中"), 6)
            scene = self._scale_level(scores.get("upper_body_change_strength", "中"), 5)
            ai_fit = self._scale_level(scores.get("title_selling_clarity", "中"), 3)
        risk_flags = []
        if direction_match.get("match_level") in {"uncertain", "evidence_missing"}:
            risk_flags.append("direction_uncertain")
        return {
            "first_frame_score": first,
            "demo_ease_score": demo,
            "scene_expression_score": scene,
            "ai_generation_fit_score": ai_fit,
            "score": round(first + demo + scene + ai_fit, 2),
            "evidence": {
                "first_frame": "首帧吸引力来自视觉记忆点/设计信号。",
                "demo_ease": "演示难度来自类目特征打点。",
                "scene_expression": "场景表达来自佩戴或上身变化。",
                "ai_generation_fit": "AI 生成适配度来自标题卖点清晰度。",
            },
            "risk_flags": risk_flags,
        }

    def _score_differentiation(self, task, match_result: MarketDirectionMatchResult) -> Dict[str, Any]:
        head_products = [dict(item) for item in list(getattr(match_result, "representative_products", []) or [])[:3]]
        comparison = {"compared_with_top_n": len(head_products), "head_products": head_products}
        if not getattr(match_result, "matched_market_direction_id", "") or not head_products:
            return self._differentiation_payload(0, 0, "insufficient", "没有可比较头部样本。", [], comparison, ["missing_head_products"])

        product_text = self._candidate_text(task)
        head_text = " ".join(self._head_product_text(item) for item in head_products)
        candidate_terms = self._meaningful_terms(product_text)
        head_terms = set(self._meaningful_terms(head_text))
        unique_terms = [term for term in candidate_terms if term not in head_terms][:5]
        price_difference = self._price_difference_text(task, head_products)
        difference_points = []
        if unique_terms:
            difference_points.append("外观/表达差异: " + "、".join(unique_terms[:3]))
        if price_difference:
            difference_points.append(price_difference)
        if not difference_points:
            difference_points.append("与头部样本高度接近，暂未发现明确可表达差异。")

        confidence = self._differentiation_confidence(task, head_products)
        if unique_terms and len(unique_terms) >= 3:
            raw_score = 15
        elif unique_terms:
            raw_score = 12
        elif price_difference:
            raw_score = 8
        else:
            raw_score = 3
        conclusion = "；".join(difference_points)
        if self._is_vague_difference(conclusion):
            raw_score = min(raw_score, 3)
        score_used = raw_score
        if confidence == "low":
            score_used = min(raw_score, 8)
        elif confidence == "insufficient":
            score_used = 0
        return self._differentiation_payload(raw_score, score_used, confidence, conclusion, difference_points, comparison, [])

    def _apply_direction_constraint(
        self,
        raw_action: str,
        direction_action: str,
        content_score: float,
        differentiation: Dict[str, Any],
    ) -> Dict[str, Any]:
        action = direction_action or "neutral"
        constraints = self.constraints.get("direction_action_constraints") or {}
        validate_direction_action_constraints([action], self.constraints)
        config = dict(constraints.get(action) or constraints.get("neutral") or {})
        max_action = str(config.get("max_action") or "select")
        final_action = raw_action
        risk_flags = []
        upgraded = False
        if action == "study_top_not_enter":
            upgrade = dict(config.get("upgrade_to_select_if") or {})
            can_upgrade = (
                float(differentiation.get("score_used") or 0.0) >= float(upgrade.get("differentiation_score_used_min") or 12)
                and content_score >= float(upgrade.get("content_potential_score_min") or 15)
                and str(differentiation.get("confidence") or "") in set(upgrade.get("differentiation_confidence_allowed") or [])
                and bool(differentiation.get("has_concrete_difference"))
                and int((differentiation.get("head_product_comparison") or {}).get("compared_with_top_n") or 0) > 0
            )
            if can_upgrade and raw_action == "select":
                final_action = "select"
                upgraded = True
            else:
                final_action = self._cap_action(raw_action, max_action)
                risk_flags.append("study_top_constraint")
        else:
            final_action = self._cap_action(raw_action, max_action)
        if final_action != raw_action and "direction_constraint_downgrade" not in risk_flags:
            risk_flags.append("direction_constraint_downgrade")
        return {
            "direction_action": action,
            "max_action": max_action,
            "raw_action": raw_action,
            "final_action": final_action,
            "risk_flags": risk_flags,
            "study_top_upgraded": upgraded,
            "quota_downgraded": False,
        }

    def _action_from_score(self, total_score: float) -> str:
        thresholds = dict(self.config.get("initial_action_thresholds") or {})
        if total_score >= float(thresholds.get("select", 75)):
            return "select"
        if total_score >= float(thresholds.get("manual_review", 65)):
            return "manual_review"
        if total_score >= float(thresholds.get("observe", 55)):
            return "observe"
        if total_score < float(thresholds.get("eliminate", 45)):
            return "eliminate"
        return "observe"

    def _direction_evidence(self, task, match_result: MarketDirectionMatchResult) -> Dict[str, Any]:
        text = self._candidate_text(task)
        matched_terms = list(getattr(match_result, "matched_terms", []) or [])
        direction_name = str(getattr(match_result, "matched_market_direction_name", "") or "")
        dictionary_entry = ((self.dictionary.get("directions") or {}).get(direction_name) or {})
        negative_terms = self._dict_terms(dictionary_entry, "negative_keywords")
        form_terms = set(list(getattr(match_result, "top_forms", []) or []) + list(getattr(match_result, "top_silhouette_forms", []) or []))
        style_terms = set([str(getattr(match_result, "style_cluster", "") or "")] + self._dict_terms(dictionary_entry, "style_keywords"))
        scene_terms = set(list(getattr(match_result, "scene_tags", []) or []) + self._dict_terms(dictionary_entry, "scenes"))
        form_match = [term for term in form_terms if term and term in text]
        style_match = [term for term in style_terms if term and term in text]
        scene_match = [term for term in scene_terms if term and term in text]
        partial_match = [term for term in matched_terms if term and term not in form_match + style_match + scene_match]
        negative_hit = [term for term in negative_terms if term and term in text]
        return {
            "form_match": form_match[:5],
            "style_match": style_match[:5],
            "scene_match": scene_match[:5],
            "partial_match": partial_match[:5],
            "negative_keyword_hit": negative_hit[:5],
        }

    def _price_fit_score(self, task, match_result: MarketDirectionMatchResult) -> Tuple[int, List[str], str]:
        price = normalize_task_target_price_to_cny(task)
        bands = list(getattr(match_result, "target_price_bands", []) or [])
        if price is None:
            return 4, ["price_missing"], "商品价格缺失，使用中性价格分。"
        if not bands:
            return 4, ["price_band_missing"], "方向推荐价格带缺失，使用中性价格分。"
        parsed_bands = [band for band in [self._parse_band(item) for item in bands] if band]
        if not parsed_bands:
            return 4, ["price_band_missing"], "方向推荐价格带无法解析，使用中性价格分。"
        lower, upper = parsed_bands[0]
        center = (lower + upper) / 2.0
        if lower <= price <= upper:
            return 7, [], "价格位于方向推荐价格带内。"
        distance = abs(price - center) / max(center, 1e-6)
        if distance <= 0.30:
            return 5, ["price_slight_mismatch"], "价格偏离方向价格带中心不超过 30%。"
        return 2, ["price_mismatch"], "价格偏离方向价格带中心超过 30%。"

    def _supply_fit_score(self, task, supply_result: AccioSupplyResult) -> Tuple[int, List[str]]:
        extra_fields = getattr(task, "extra_fields", {}) or {}
        raw = str(extra_fields.get("供应链匹配") or extra_fields.get("supply_match") or "").strip()
        mapping = {"高": 5, "中": 3, "低": 1}
        if raw in mapping:
            return mapping[raw], []
        status_mapping = {"pass": 5, "watch": 3, "fail": 1, "pending": 2, "timeout": 2}
        if supply_result.supply_check_status:
            return status_mapping.get(supply_result.supply_check_status, 2), ["supply_match_missing"]
        return 2, ["supply_match_missing"]

    def _differentiation_payload(
        self,
        score: int,
        score_used: int,
        confidence: str,
        conclusion: str,
        difference_points: List[str],
        comparison: Dict[str, Any],
        risk_flags: List[str],
    ) -> Dict[str, Any]:
        has_concrete = bool(difference_points) and not self._is_vague_difference(conclusion)
        return {
            "score": score,
            "score_used": score_used,
            "confidence": confidence,
            "conclusion": conclusion,
            "difference_points": difference_points,
            "differentiation_type": self._differentiation_types(difference_points),
            "has_concrete_difference": has_concrete,
            "head_product_comparison": comparison,
            "risk_flags": risk_flags,
        }

    def _differentiation_confidence(self, task, head_products: List[Dict[str, Any]]) -> str:
        if not head_products:
            return "insufficient"
        candidate_price_ok = normalize_task_target_price_to_cny(task) is not None
        complete_heads = [
            item
            for item in head_products
            if self._head_product_text(item) and self._head_price(item) is not None
        ]
        if len(head_products) >= 3 and candidate_price_ok and len(complete_heads) >= 3:
            return "high"
        if len(head_products) >= 2 and len(complete_heads) >= 1:
            return "medium"
        return "low"

    def _price_difference_text(self, task, head_products: List[Dict[str, Any]]) -> str:
        price = normalize_task_target_price_to_cny(task)
        head_prices = [self._head_price(item) for item in head_products]
        head_prices = [item for item in head_prices if item is not None]
        if price is None or not head_prices:
            return ""
        median = sorted(head_prices)[len(head_prices) // 2]
        if median <= 0:
            return ""
        diff = (price - median) / median
        if abs(diff) < 0.2:
            return ""
        if diff < 0:
            return "价格带差异: 当前产品比头部样本更低价，适合低成本测试。"
        return "价格带差异: 当前产品比头部样本更高价，需要验证质感表达。"

    def _brief_reason(self, direction_match: Dict[str, Any], content: Dict[str, Any], differentiation: Dict[str, Any], action: str) -> str:
        return "{direction}，内容分 {content:.1f}/20，差异化判断：{diff}，V2 建议 {action}。".format(
            direction=direction_match.get("matched_direction") or "方向不确定",
            content=float(content.get("score") or 0.0),
            diff=str(differentiation.get("conclusion") or "")[:60],
            action=ACTION_LABELS.get(action, action),
        )

    def _eliminate_reason(
        self,
        final_action: str,
        direction_match: Dict[str, Any],
        product_quality: Dict[str, Any],
        content_potential: Dict[str, Any],
        differentiation: Dict[str, Any],
        risk_flags: List[str],
    ) -> List[str]:
        if final_action != "eliminate":
            return []
        reasons = []
        if direction_match.get("score", 0) <= 5:
            reasons.append("direction_mismatch")
        if product_quality.get("visual_style_score", 0) <= 2:
            reasons.append("poor_visual")
        if "price_mismatch" in risk_flags:
            reasons.append("price_unreasonable")
        if product_quality.get("supply_fit_score", 0) <= 1:
            reasons.append("supply_chain_blocked")
        if differentiation.get("score_used", 0) <= 3:
            reasons.append("no_differentiation")
        if product_quality.get("info_completeness_score", 0) <= 1:
            reasons.append("info_incomplete")
        return self._dedupe(reasons)

    def _cap_action(self, action: str, max_action: str) -> str:
        return action if ACTION_ORDER.get(action, 0) <= ACTION_ORDER.get(max_action, 4) else max_action

    def _scale_level(self, value: str, max_score: float, reversed_score: bool = False) -> float:
        mapping = REVERSE_LEVEL_TO_RATIO if reversed_score else LEVEL_TO_RATIO
        return round(mapping.get(str(value or "中"), 0.6) * max_score, 2)

    def _candidate_text(self, task) -> str:
        parts = [
            getattr(task, "product_title", ""),
            getattr(task, "product_notes", ""),
            getattr(task, "competitor_notes", ""),
        ]
        parts.extend(list(getattr(task, "title_keyword_tags", []) or []))
        return " ".join(str(part or "") for part in parts)

    def _head_product_text(self, item: Dict[str, Any]) -> str:
        return " ".join(
            str(item.get(key) or "")
            for key in ("product_name", "title", "商品名称", "product_title", "reason_short")
        )

    def _head_price(self, item: Dict[str, Any]) -> Optional[float]:
        for key in ("price_mid", "price", "价格", "price_min"):
            value = item.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _meaningful_terms(self, text: str) -> List[str]:
        separators = " ，,。；;：:/|+_-（）()[]【】"
        normalized = str(text or "")
        for separator in separators:
            normalized = normalized.replace(separator, " ")
        terms = []
        for part in normalized.split():
            cleaned = part.strip()
            if len(cleaned) >= 2 and cleaned not in {"产品", "商品", "发饰", "头饰"}:
                terms.append(cleaned)
        return self._dedupe(terms)

    def _differentiation_types(self, points: List[str]) -> List[str]:
        mapping = {
            "外观": "外观差异",
            "功能": "功能差异",
            "场景": "场景差异",
            "价格": "价格带差异",
            "表达": "内容表达差异",
        }
        types = []
        text = " ".join(points)
        for token, label in mapping.items():
            if token in text:
                types.append(label)
        return types or ["外观差异"]

    def _is_vague_difference(self, text: str) -> bool:
        stripped = str(text or "").strip()
        if not stripped:
            return True
        vague = ["有一定差异", "风格不同", "更特别", "略有不同"]
        return any(item in stripped for item in vague)

    def _parse_band(self, value: Any) -> Optional[Tuple[float, float]]:
        text = str(value or "").strip()
        for token in ("RMB", "rmb", "CNY", "cny", "元", "¥", "￥", " "):
            text = text.replace(token, "")
        if "-" not in text:
            return None
        lower, upper = text.split("-", 1)
        try:
            return float(lower), float(upper)
        except ValueError:
            return None

    def _dict_terms(self, entry: Dict[str, Any], key: str) -> List[str]:
        payload = dict(entry.get(key) or {})
        values = list(payload.get("zh") or []) + list(payload.get("vi") or [])
        return [str(item).strip() for item in values if str(item).strip()]

    def _one_sentence(self, text: str) -> str:
        text = str(text or "").strip()
        if not text:
            return ""
        for separator in ("。", ".", "；", ";"):
            if separator in text:
                return text.split(separator, 1)[0][:80]
        return text[:80]

    def _dedupe(self, values: List[str]) -> List[str]:
        return list(dict.fromkeys([str(item) for item in values if str(item or "").strip()]))

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        path = self.skill_dir / "configs" / filename
        if not path.exists():
            return {}
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
