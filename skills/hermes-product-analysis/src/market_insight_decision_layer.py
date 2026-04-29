#!/usr/bin/env python3
"""Direction Decision Layer for market-insight reports."""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml


def re_split_words(text: str) -> List[str]:
    return [part for part in re.split(r"[\s/、，,。；;：:+\-]+", str(text or "")) if part]


DECISION_ACTION_LABELS = {
    "prioritize_low_cost_test": "优先低成本验证",
    "cautious_test": "谨慎切入验证",
    "hidden_small_test": "暗线小样本验证",
    "strong_signal_verify": "强信号待核验",
    "hidden_candidate": "暗线候选",
    "observe": "持续观察",
    "study_top_not_enter": "拆头部不直接入场",
    "avoid": "暂不投入",
}

OPPORTUNITY_TYPE_LABELS = {
    "insufficient_sample": "样本不足",
    "content_gap": "内容缺口型",
    "mature_strong_demand": "成熟强需求型",
    "head_concentrated": "头部集中型",
    # Kept for backward-compatible rendering of historical cards; V1.3 no
    # longer emits this as a primary opportunity type.
    "few_new_winners": "少数新品赢家",
    "hidden_scene": "暗线场景型",
    "hidden_scene_candidate": "暗线场景候选",
    "supply_bubble": "供给泡沫型",
    "aesthetic_homogeneous": "审美同质型",
    "general_observe": "普通观察型",
    "evidence_pending": "证据待核验型",
}


DEFAULT_DECISION_RULES = {
    "default": {
        "sales_action_threshold": 250,
        "min_sample": {"insufficient": 5, "distribution_metrics": 8, "p75": 12, "p90": 20},
        "head_concentrated": {
            "top3_sales_share_threshold": 0.45,
            "strong_top3_sales_share_threshold": 0.60,
            "mean_median_ratio_threshold": 2.0,
            "extreme_mean_median_ratio_threshold": 5.0,
        },
        "supply_bubble": {
            "min_sample": 12,
            "median_sales_vs_category_max": 0.8,
            "video_density_percentile_min": 0.75,
            "creator_density_percentile_min": 0.75,
        },
        "content_gap": {
            "min_sample": 8,
            "median_sales_vs_category_min": 1.0,
            "video_density_vs_category_max": 1.0,
            "creator_density_vs_category_max": 1.0,
        },
        "mature_strong_demand": {"min_sample": 12, "median_sales_vs_category_min": 1.1},
        "hidden_scene_candidate": {
            "min_sample": 5,
            "median_sales_vs_category_min": 0.9,
            "video_density_vs_category_max": 0.7,
            "scene_keywords": ["头盔", "摩托", "通勤", "热天", "上学", "上班", "宿舍", "办公室", "快速整理"],
        },
    },
        "action_mapping": {
        "content_gap": {"default_action": "prioritize_low_cost_test", "fallback_action": "observe"},
        "mature_strong_demand": {"default_action": "cautious_test", "fallback_action": "observe"},
        "head_concentrated": {"default_action": "study_top_not_enter", "upgrade_action": "cautious_test"},
        "hidden_scene": {"default_action": "hidden_small_test"},
        "hidden_scene_candidate": {"default_action": "observe"},
        "supply_bubble": {"default_action": "avoid"},
        "aesthetic_homogeneous": {"default_action": "study_top_not_enter"},
        "evidence_pending": {"default_action": "observe"},
        "insufficient_sample": {"default_action": "observe"},
        "general_observe": {"default_action": "observe"},
    },
    "team_capacity": {
        "max_study_top_directions_per_batch": 2,
        "max_test_directions_per_batch": 2,
        "max_hidden_candidates_per_batch": 2,
    },
    "overrides": {},
}


class DirectionDecisionLayer(object):
    def __init__(self, config_path: Path | None = None):
        self.config_path = Path(config_path) if config_path else None
        self.raw_config = self._load_config(self.config_path)

    def apply(
        self,
        cards: Iterable[Dict[str, Any]],
        country: str,
        category: str,
        batch_id: str,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        card_list = [dict(card) for card in cards]
        rules = self._rules_for(country=country, category=category)
        baselines = self._category_baselines(card_list)
        enriched = []
        for card in card_list:
            result = self.classify_direction(card, baselines, rules, batch_id=batch_id)
            merged = dict(card)
            merged.update(result)
            enriched.append(merged)
        enriched = self._validate_differentiation_uniqueness(enriched)
        enriched = self._apply_batch_capacity(enriched)
        return enriched, self._build_action_summary(enriched)

    def classify_direction(
        self,
        card: Dict[str, Any],
        category_baselines: Dict[str, float],
        rules: Dict[str, Any],
        batch_id: str,
    ) -> Dict[str, Any]:
        demand = self._demand_structure(card, rules)
        competition = self._competition_structure(card)
        sample_count = int(demand.get("sample_count") or card.get("direction_item_count") or 0)
        sample_confidence = self._sample_confidence(sample_count, rules)
        risk_tags = self._base_risk_tags(card, demand, rules)
        primary_type, hidden_scene_candidate = self._classify_primary_type(
            card=card,
            demand=demand,
            category_baselines=category_baselines,
            rules=rules,
            sample_count=sample_count,
        )
        if hidden_scene_candidate and primary_type == "general_observe":
            primary_type = "hidden_scene_candidate"
        capability_fit = self._capability_fit(card, risk_tags, sample_confidence=sample_confidence)
        price_band_analysis = self._price_band_analysis(card)
        default_action_by_type = self._default_action_for_type(primary_type)
        action_overrides: List[Dict[str, Any]] = []
        decision_action = self._map_action(
            primary_type=primary_type,
            card=card,
            risk_tags=risk_tags,
            capability_fit=capability_fit,
            rules=rules,
        )
        decision_action, primary_type, risk_tags, override = self._apply_strong_signal_actions(
            card=card,
            demand=demand,
            category_baselines=category_baselines,
            rules=rules,
            sample_confidence=sample_confidence,
            decision_action=decision_action,
            primary_type=primary_type,
            risk_tags=risk_tags,
        )
        if override:
            action_overrides.append(override)
        age_signal = dict(card.get("new_product_entry_signal") or {})
        signal_context = self._new_product_signal_context(
            raw_signal=age_signal,
            sample_confidence=sample_confidence,
            price_band_analysis=price_band_analysis,
            capability_fit=capability_fit,
        )
        decision_action, primary_type, risk_tags, override = self._adjust_decision_by_age_signal(
            decision_action=decision_action,
            primary_type=primary_type,
            risk_tags=risk_tags,
            age_signal={"type": signal_context["actionable_new_product_signal"], "confidence": signal_context["age_confidence"]},
            card=card,
        )
        if override:
            action_overrides.append(override)
        risk_tags = self._normalize_age_risk_tags(risk_tags, signal_context)
        decision_action, risk_tags = self._cap_decision_upgrade_by_confidence(
            decision_action=decision_action,
            risk_tags=risk_tags,
            sample_confidence=sample_confidence,
            price_band_analysis=price_band_analysis,
            capability_fit=capability_fit,
        )
        capability_fit = self._capability_fit(card, risk_tags, sample_confidence=sample_confidence)
        observe_reason = self._observe_reasons(
            card=card,
            demand=demand,
            primary_type=primary_type,
            decision_action=decision_action,
            capability_fit=capability_fit,
            risk_tags=risk_tags,
        )
        recommended_execution = self._recommended_execution(
            card=card,
            decision_action=decision_action,
            primary_type=primary_type,
            capability_fit=capability_fit,
            risk_tags=risk_tags,
            signal_context=signal_context,
        )
        if (
            decision_action == "cautious_test"
            and str(recommended_execution.get("test_sku_count") or "") == "2-3 款"
            and (
                signal_context["raw_new_product_signal"] in {"weak_new_entry", "old_product_dominated", "unknown"}
                or "high_video_density" in set(risk_tags)
            )
        ):
            action_overrides.append(
                self._make_action_override(
                    "OR-003",
                    "成熟强需求但新品窗口弱",
                    "需求基础存在，但新品进入窗口弱或内容竞争高，保持谨慎验证但压缩测款规模。",
                    decision_action,
                    decision_action,
                    [
                        {"metric": "测款数量", "value": "2-3 款", "threshold": "默认 3-5 款", "conclusion": "测款规模收紧"},
                        {"metric": "原始新品信号", "value": signal_context["raw_new_product_signal"], "conclusion": "新品窗口不强"},
                    ],
                )
            )
        scale_conditions = self._scale_conditions(decision_action, primary_type)
        stop_loss_conditions = self._stop_loss_conditions(decision_action, primary_type)
        alert = self._alert_for_conditions(scale_conditions, stop_loss_conditions)
        structure_tags = self._new_product_structure_tags(signal_context, risk_tags)
        opportunity_evidence = self._opportunity_evidence(
            primary_type=primary_type,
            card=card,
            demand=demand,
            category_baselines=category_baselines,
            risk_tags=risk_tags,
            sample_count=sample_count,
        )
        primary_type = self._normalize_primary_type(primary_type, opportunity_evidence)
        action_decision = self._build_action_decision(default_action_by_type, decision_action, action_overrides)
        return {
            "direction_id": str(card.get("direction_canonical_key") or card.get("direction_instance_id") or ""),
            "direction_name": str(card.get("direction_name") or card.get("style_cluster") or ""),
            "batch_id": batch_id,
            "sample_count": sample_count,
            "sample_confidence": sample_confidence,
            "primary_opportunity_type": primary_type,
            "primary_opportunity_type_label": OPPORTUNITY_TYPE_LABELS.get(primary_type, primary_type),
            "opportunity_evidence": opportunity_evidence,
            "risk_tags": risk_tags,
            "new_product_structure_tags": structure_tags,
            "observe_reason": observe_reason,
            "decision_action": decision_action,
            "decision_action_label": DECISION_ACTION_LABELS.get(decision_action, decision_action),
            "default_action_by_type": default_action_by_type,
            "actual_action": decision_action,
            "action_override": action_decision["action_override"],
            "action_overrides": action_decision["action_overrides"],
            "action_decision": action_decision,
            "demand_structure": demand,
            "competition_structure": competition,
            "price_band_analysis": price_band_analysis,
            "product_age_structure": dict(card.get("product_age_structure") or {}),
            "new_product_entry_signal": age_signal or {"type": "unknown", "confidence": "insufficient", "rationale": "上架时间样本不足，新品进入判断仅作参考。"},
            "raw_new_product_signal": signal_context["raw_new_product_signal"],
            "actionable_new_product_signal": signal_context["actionable_new_product_signal"],
            "new_product_signal_reason": signal_context["new_product_signal_reason"],
            "our_capability_fit": capability_fit,
            "recommended_execution": recommended_execution,
            "scale_condition": scale_conditions,
            "stop_loss_condition": stop_loss_conditions,
            "batch_comparison": self._batch_comparison_placeholder(batch_id),
            "alert": alert,
            "hidden_scene_candidate": hidden_scene_candidate,
        }

    def _classify_primary_type(
        self,
        card: Dict[str, Any],
        demand: Dict[str, Any],
        category_baselines: Dict[str, float],
        rules: Dict[str, Any],
        sample_count: int,
    ) -> Tuple[str, bool]:
        median_sales = float(demand.get("median_sales_7d") or 0.0)
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        creator_density = float(card.get("direction_creator_density_avg") or 0.0)
        category_median = max(float(category_baselines.get("median_sales_7d") or 0.0), 1.0)
        category_video_median = max(float(category_baselines.get("video_density_median") or 0.0), 0.0001)
        category_creator_median = max(float(category_baselines.get("creator_density_median") or 0.0), 0.0001)
        top3_share = demand.get("top3_sales_share")
        mean_median_ratio = demand.get("mean_median_ratio")
        min_sample = dict(rules.get("min_sample") or {})
        if sample_count < int(min_sample.get("insufficient", 5) or 5):
            return "insufficient_sample", self._has_scene_signal(card, rules)

        supply = dict(rules.get("supply_bubble") or {})
        if (
            sample_count >= int(supply.get("min_sample", 12) or 12)
            and median_sales < category_median * float(supply.get("median_sales_vs_category_max", 0.8) or 0.8)
            and video_density >= float(category_baselines.get("video_density_p75") or 0.0)
            and creator_density >= float(category_baselines.get("creator_density_p75") or 0.0)
        ):
            return "supply_bubble", False

        head = dict(rules.get("head_concentrated") or {})
        if (
            sample_count >= int(min_sample.get("p75", 12) or 12)
            and top3_share is not None
            and mean_median_ratio is not None
            and float(top3_share) >= float(head.get("top3_sales_share_threshold", 0.45) or 0.45)
            and float(mean_median_ratio) >= float(head.get("mean_median_ratio_threshold", 2.0) or 2.0)
        ):
            return "head_concentrated", False

        content_gap = dict(rules.get("content_gap") or {})
        if (
            sample_count >= int(content_gap.get("min_sample", 8) or 8)
            and median_sales >= category_median * float(content_gap.get("median_sales_vs_category_min", 1.0) or 1.0)
            and video_density <= category_video_median * float(content_gap.get("video_density_vs_category_max", 1.0) or 1.0)
            and creator_density <= category_creator_median * float(content_gap.get("creator_density_vs_category_max", 1.0) or 1.0)
        ):
            return "content_gap", False

        mature = dict(rules.get("mature_strong_demand") or {})
        if (
            sample_count >= int(mature.get("min_sample", 12) or 12)
            and median_sales >= category_median * float(mature.get("median_sales_vs_category_min", 1.1) or 1.1)
            and video_density > category_video_median
            and creator_density > category_creator_median
        ):
            return "mature_strong_demand", False

        if (
            sample_count >= int(content_gap.get("min_sample", 8) or 8)
            and "aesthetic_homogeneous" in self._base_risk_tags(card, demand, rules)
            and (
                str(card.get("direction_tier") or "") == "crowded"
                or demand.get("mean_median_ratio") is not None
                and float(demand.get("mean_median_ratio") or 0.0) >= 1.8
            )
        ):
            return "aesthetic_homogeneous", False

        hidden = dict(rules.get("hidden_scene_candidate") or {})
        hidden_scene_candidate = bool(
            sample_count >= int(hidden.get("min_sample", 5) or 5)
            and median_sales >= category_median * float(hidden.get("median_sales_vs_category_min", 0.9) or 0.9)
            and video_density <= category_video_median * float(hidden.get("video_density_vs_category_max", 0.7) or 0.7)
            and self._has_scene_signal(card, rules)
        )
        return "general_observe", hidden_scene_candidate

    def _map_action(
        self,
        primary_type: str,
        card: Dict[str, Any],
        risk_tags: List[str],
        capability_fit: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> str:
        mapping = dict(self.raw_config.get("action_mapping") or DEFAULT_DECISION_RULES["action_mapping"])
        action = str((mapping.get(primary_type) or {}).get("default_action") or "observe")
        if primary_type == "content_gap" and capability_fit.get("ai_content") == "low":
            return str((mapping.get(primary_type) or {}).get("fallback_action") or "observe")
        if primary_type == "mature_strong_demand" and not self._differentiation_angles(card, risk_tags):
            return str((mapping.get(primary_type) or {}).get("fallback_action") or "observe")
        if (
            primary_type == "head_concentrated"
            and self._level_at_least(str(capability_fit.get("sourcing_fit") or "low"), "medium")
            and self._level_at_least(str(capability_fit.get("replication") or "low"), "medium")
        ):
            return str((mapping.get(primary_type) or {}).get("upgrade_action") or action)
        return action

    def _default_action_for_type(self, primary_type: str) -> str:
        mapping = dict(self.raw_config.get("action_mapping") or DEFAULT_DECISION_RULES["action_mapping"])
        return str((mapping.get(primary_type) or {}).get("default_action") or "observe")

    def _make_action_override(
        self,
        rule_id: str,
        rule_name: str,
        reason: str,
        from_action: str,
        to_action: str,
        evidence: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        return {
            "override_rule_id": rule_id,
            "override_rule_name": rule_name,
            "override_reason": reason,
            "from_action": from_action,
            "to_action": to_action,
            "override_evidence": list(evidence or []),
        }

    def _build_action_decision(
        self,
        default_action: str,
        actual_action: str,
        overrides: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        normalized = [dict(item) for item in overrides if item]
        primary_override = normalized[0] if normalized else {
            "override_rule_id": "",
            "override_rule_name": "",
            "override_reason": "",
            "from_action": default_action,
            "to_action": actual_action,
            "override_evidence": [],
        }
        action_override = {
            "is_overridden": bool(normalized),
            **primary_override,
        }
        return {
            "default_action_by_type": default_action,
            "actual_action": actual_action,
            "action_override": action_override,
            "action_overrides": normalized,
        }

    def _append_action_override(self, card: Dict[str, Any], override: Dict[str, Any]) -> None:
        overrides = [dict(item) for item in list(card.get("action_overrides") or []) if item]
        overrides.append(dict(override))
        default_action = str(card.get("default_action_by_type") or overrides[0].get("from_action") or "observe")
        actual_action = str(card.get("decision_action") or card.get("actual_action") or default_action)
        decision = self._build_action_decision(default_action, actual_action, overrides)
        card["action_overrides"] = decision["action_overrides"]
        card["action_override"] = decision["action_override"]
        card["action_decision"] = decision
        card["actual_action"] = actual_action

    def _apply_strong_signal_actions(
        self,
        card: Dict[str, Any],
        demand: Dict[str, Any],
        category_baselines: Dict[str, float],
        rules: Dict[str, Any],
        sample_confidence: str,
        decision_action: str,
        primary_type: str,
        risk_tags: List[str],
    ) -> Tuple[str, str, List[str], Dict[str, Any] | None]:
        tags = list(risk_tags)
        sample_count = int(demand.get("sample_count") or 0)
        median_sales = float(demand.get("median_sales_7d") or 0.0)
        over_threshold = float(demand.get("over_threshold_item_ratio") or 0.0)
        category_median = max(float(category_baselines.get("median_sales_7d") or 0.0), 1.0)
        sales_action_threshold = float(rules.get("sales_action_threshold", 250) or 250)
        if (
            sample_count <= 3
            and median_sales >= sales_action_threshold
            and self._has_scene_signal(card, rules)
        ):
            tags.append("local_scene_fit")
            override = self._make_action_override(
                "OR-005",
                "本地场景强但样本极少",
                "样本极少，但本地场景真实，先作为暗线候选追踪。",
                decision_action,
                "hidden_candidate",
                [
                    {"metric": "样本数", "value": sample_count, "threshold": "<=3", "conclusion": "样本极少"},
                    {"metric": "7日销量中位数", "value": median_sales, "threshold": f">={sales_action_threshold:g}", "conclusion": "达到行动阈值"},
                    {"metric": "本地场景信号", "value": "命中", "conclusion": "具备本地场景追踪价值"},
                ],
            )
            return "hidden_candidate", primary_type, sorted(set(tags)), override
        if (
            median_sales >= category_median * 2.5
            and over_threshold >= 0.6
            and sample_confidence in {"low", "medium"}
            and decision_action in {"observe", "hidden_small_test"}
        ):
            tags.append("high_median_sales")
            tags.append("high_over_threshold_ratio")
            override = self._make_action_override(
                "OR-004",
                "高需求但样本不足",
                "需求信号强，但样本或供应链置信度不足，不直接测款，先进入样本池核验。",
                decision_action,
                "strong_signal_verify",
                [
                    {"metric": "7日销量中位数", "value": median_sales, "threshold": f">={category_median * 2.5:g}", "conclusion": "高于类目基准 2.5 倍"},
                    {"metric": "超过行动阈值商品占比", "value": over_threshold, "threshold": ">=0.60", "conclusion": "高需求信号较强"},
                    {"metric": "样本置信度", "value": sample_confidence, "threshold": "low/medium", "conclusion": "仍需核验"},
                ],
            )
            return "strong_signal_verify", primary_type, sorted(set(tags)), override
        return decision_action, primary_type, sorted(set(tags)), None

    def _normalize_primary_type(self, primary_type: str, evidence: Dict[str, Any]) -> str:
        allowed = {
            "insufficient_sample",
            "content_gap",
            "mature_strong_demand",
            "head_concentrated",
            "hidden_scene",
            "hidden_scene_candidate",
            "supply_bubble",
            "aesthetic_homogeneous",
            "general_observe",
            "evidence_pending",
        }
        if primary_type not in allowed:
            return "evidence_pending"
        if primary_type in {
            "content_gap",
            "mature_strong_demand",
            "head_concentrated",
            "supply_bubble",
            "aesthetic_homogeneous",
        } and not list(evidence.get("evidence_items") or []):
            return "evidence_pending"
        return primary_type

    def _new_product_structure_tags(self, signal_context: Dict[str, str], risk_tags: List[str]) -> List[str]:
        tags = []
        raw_signal = str(signal_context.get("raw_new_product_signal") or "unknown")
        mapping = {
            "few_new_winners": "few_new_winners",
            "old_product_dominated": "old_product_dominated",
            "strong_new_entry": "strong_new_entry",
            "moderate_new_entry": "moderate_new_entry",
            "weak_new_entry": "weak_new_entry",
            "noisy_new_supply": "noisy_new_supply",
        }
        if raw_signal in mapping:
            tags.append(mapping[raw_signal])
        for tag in risk_tags:
            if tag in {"few_new_winners", "old_product_dominated", "noisy_new_supply", "weak_new_entry_window"}:
                tags.append(tag)
        return sorted(set(tags))

    def _opportunity_evidence(
        self,
        primary_type: str,
        card: Dict[str, Any],
        demand: Dict[str, Any],
        category_baselines: Dict[str, float],
        risk_tags: List[str],
        sample_count: int,
    ) -> Dict[str, Any]:
        median_sales = float(demand.get("median_sales_7d") or 0.0)
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        creator_density = float(card.get("direction_creator_density_avg") or 0.0)
        category_median = float(category_baselines.get("median_sales_7d") or 0.0)
        category_video = float(category_baselines.get("video_density_median") or 0.0)
        category_creator = float(category_baselines.get("creator_density_median") or 0.0)
        top3_share = demand.get("top3_sales_share")
        mean_median_ratio = demand.get("mean_median_ratio")
        over_threshold = demand.get("over_threshold_item_ratio")
        items: List[Dict[str, Any]] = []
        rule = ""
        if primary_type == "content_gap":
            rule = "成交不弱 + 内容/达人覆盖低于类目基准"
            items.extend(
                [
                    self._evidence_item("7日销量中位数", median_sales, category_median, "不低于类目中位数", "成交基础不弱"),
                    self._evidence_item("视频密度", video_density, category_video, "低于或接近类目中位数", "内容供给相对不足"),
                    self._evidence_item("达人密度", creator_density, category_creator, "低于或接近类目中位数", "达人覆盖相对不足"),
                ]
            )
        elif primary_type == "mature_strong_demand":
            rule = "成交强 + 内容/达人覆盖已不低"
            items.extend(
                [
                    self._evidence_item("7日销量中位数", median_sales, category_median, "高于类目中位数", "需求基础强"),
                    self._evidence_item("视频密度", video_density, category_video, "高于类目中位数", "竞争和素材覆盖都不低"),
                    self._evidence_item("达人密度", creator_density, category_creator, "高于类目中位数", "达人覆盖已经较充分"),
                ]
            )
        elif primary_type == "head_concentrated":
            rule = "样本充足 + Top3 占比高 + 均值/中位数偏高"
            items.extend(
                [
                    self._evidence_item("方向样本数", sample_count, 12, "不低于强判断样本线", "样本足以判断头部结构"),
                    self._evidence_item("Top3销量占比", top3_share, 0.45, "达到头部集中阈值", "销量集中在少数头部商品"),
                    self._evidence_item("均值/中位数", mean_median_ratio, 2.0, "达到分布偏斜阈值", "均值被头部样本明显拉高"),
                ]
            )
        elif primary_type == "supply_bubble":
            rule = "成交弱于类目基准 + 视频/达人密度高"
            items.extend(
                [
                    self._evidence_item("7日销量中位数", median_sales, category_median, "低于类目中位数", "多数商品成交承接不足"),
                    self._evidence_item("视频密度", video_density, category_video, "高于类目中位数", "内容供给偏热"),
                    self._evidence_item("达人密度", creator_density, category_creator, "高于类目中位数", "达人覆盖偏热"),
                ]
            )
        elif primary_type == "aesthetic_homogeneous":
            rule = "审美型方向 + 同质化/拥挤/分布偏斜风险"
            items.extend(
                [
                    self._evidence_item("审美同质化标签", "命中", "命中", "已命中", "方向主要依赖审美记忆点，普通款易同质化"),
                    self._evidence_item("视频密度", video_density, category_video, "接近或高于类目中位数", "内容竞争不低"),
                    self._evidence_item("超过行动阈值商品占比", over_threshold, 0.0, "作为辅助观察", "判断普通款是否也能成交"),
                ]
            )
        elif primary_type in {"insufficient_sample", "general_observe", "hidden_scene_candidate"}:
            rule = "当前证据不足以输出强机会类型"
            if sample_count:
                items.append(self._evidence_item("方向样本数", sample_count, 12, "低于或接近强判断样本线", "需要继续补样本"))
        why_not = []
        if primary_type != "head_concentrated":
            if sample_count < 12:
                why_not.append({"type": "头部集中型", "reason": "样本数低于 12，不做强头部集中判定。"})
            elif top3_share is None or mean_median_ratio is None:
                why_not.append({"type": "头部集中型", "reason": "缺少 Top3 占比或均值/中位数，不能强判。"})
            elif "head_concentrated" not in risk_tags:
                why_not.append({"type": "头部集中型", "reason": "Top3 占比和均值/中位数未同时达到阈值。"})
        if primary_type != "supply_bubble":
            why_not.append({"type": "供给泡沫型", "reason": "未同时满足低成交承接和高内容/达人拥挤。"})
        return {
            "rule_matched": rule,
            "evidence_items": [item for item in items if item],
            "why_not_other_types": why_not[:3],
        }

    def _evidence_item(
        self,
        metric: str,
        direction_value: Any,
        baseline_value: Any,
        comparison: str,
        conclusion: str,
    ) -> Dict[str, Any]:
        return {
            "metric": metric,
            "direction_value": direction_value,
            "baseline_value": baseline_value,
            "comparison": comparison,
            "conclusion": conclusion,
        }

    def _demand_structure(self, card: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
        existing = dict(card.get("demand_structure") or {})
        sample_count = int(existing.get("sample_count") or card.get("direction_item_count") or 0)
        median_sales = float(existing.get("median_sales_7d") if existing.get("median_sales_7d") is not None else card.get("direction_sales_median_7d") or 0.0)
        mean_sales = existing.get("mean_sales_7d")
        if mean_sales is None:
            mean_sales = median_sales
        result = {
            "sample_count": sample_count,
            "sample_confidence": self._sample_confidence(sample_count, rules),
            "median_sales_7d": median_sales,
            "mean_sales_7d": float(mean_sales or 0.0),
            "mean_median_ratio": existing.get("mean_median_ratio"),
            "top3_sales_share": existing.get("top3_sales_share"),
            "over_threshold_item_ratio": existing.get("over_threshold_item_ratio"),
            "sales_p75_7d": existing.get("sales_p75_7d"),
            "sales_p90_7d": existing.get("sales_p90_7d"),
            "confidence": self._sample_confidence(sample_count, rules),
        }
        top3_share = result.get("top3_sales_share")
        if sample_count > 0 and top3_share is not None:
            expected = min(1.0, 3.0 / float(sample_count))
            result["top3_share_expected"] = round(expected, 4)
            result["top3_share_above_expected"] = round(
                (float(top3_share) - expected) / max(0.0001, 1.0 - expected),
                4,
            )
        else:
            result["top3_share_expected"] = None
            result["top3_share_above_expected"] = None
        return result

    def _competition_structure(self, card: Dict[str, Any]) -> Dict[str, Any]:
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        creator_density = float(card.get("direction_creator_density_avg") or 0.0)
        tier = str(card.get("direction_tier") or "")
        if tier == "crowded":
            summary = "方向处于拥挤状态，但拥挤代表进入难度，不等同于不能进入，需要结合需求和能力切口判断。"
        elif tier == "low_sample":
            summary = "方向样本偏少，当前更适合观察或小样本验证。"
        else:
            summary = "方向竞争结构相对可读，可结合成交与内容密度决定验证强度。"
        return {
            "item_count": int(card.get("direction_item_count") or 0),
            "video_density": video_density,
            "creator_density": creator_density,
            "direction_tier": tier,
            "competition_summary": summary,
        }

    def _price_band_analysis(self, card: Dict[str, Any]) -> Dict[str, Any]:
        existing = dict(card.get("price_band_analysis") or {})
        if existing and "recommended_price_band" in existing:
            return existing
        if existing:
            label = existing.get("best_price_band")
            bucket_stats = dict(existing.get("bucket_stats") or {})
            stat = dict(bucket_stats.get(str(label or "")) or {})
            return {
                "method": existing.get("method") or "dynamic_quantile_bucket",
                "recommended_price_band": {
                    "label": label,
                    "rmb_range": self._price_label_to_rmb_range(label),
                    "sample_count": int(stat.get("sample_count") or 0),
                    "median_sales_7d": stat.get("median_sales_7d"),
                    "confidence": stat.get("confidence") or existing.get("price_band_confidence") or "insufficient",
                },
                "bucket_stats": bucket_stats,
                "notes": existing.get("notes") or "",
            }
        bands = list(card.get("target_price_bands") or [])
        if not bands:
            return {
                "method": "unavailable",
                "recommended_price_band": {
                    "label": None,
                    "rmb_range": None,
                    "sample_count": 0,
                    "median_sales_7d": None,
                    "confidence": "insufficient",
                },
                "notes": "缺少价格字段，暂不做价格带判断。",
            }
        return {
            "method": "direction_top_price_band",
            "recommended_price_band": {
                "label": bands[0],
                "rmb_range": bands[0],
                "sample_count": int(card.get("direction_item_count") or 0),
                "median_sales_7d": float(card.get("direction_sales_median_7d") or 0.0),
                "confidence": "low",
            },
            "notes": "当前仅基于方向内高频价格带，后续需结合动态价格分桶校准。",
        }

    def _capability_fit(self, card: Dict[str, Any], risk_tags: List[str], sample_confidence: str = "") -> Dict[str, Any]:
        family = str(card.get("direction_family") or "")
        style = str(card.get("style_cluster") or "")
        if style == "other":
            return {
                "ai_content": "low",
                "replication": "low",
                "original_demo": "low",
                "scene_localization": "low",
                "sourcing_fit": "low",
                "rationale": "方向信息不完整，不建议分配内容资源。",
            }
        scenes = " ".join(str(item) for item in list(card.get("scene_tags") or []))
        values = " ".join(str(item) for item in list(card.get("top_value_points") or []))
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        text = " ".join([family, style, scenes, values])
        functional_keywords = ["效率", "整理", "头盔", "通勤", "快速", "防晒", "空调", "遮手臂", "显比例", "叠穿"]
        local_scene_keywords = ["头盔", "摩托", "通勤", "热天", "上学", "上班", "宿舍", "办公室", "快速整理"]
        ai_content = "high" if any(word in text for word in functional_keywords) else "medium"
        if "审美" in family and "aesthetic_homogeneous" in risk_tags:
            ai_content = "medium"
        replication = "high" if video_density >= 1.0 else ("medium" if video_density >= 0.3 else "low")
        original_demo = "high" if any(word in text for word in ["效率", "整理", "头盔", "快速", "防晒", "空调"]) else "medium"
        scene_localization = "high" if any(word in text for word in local_scene_keywords) else "medium"
        if "审美" in family:
            original_demo = "medium"
            scene_localization = "medium"
        if "功能结果" in family:
            original_demo = "high"
            ai_content = "high"
        sourcing_fit = "medium" if str(card.get("direction_tier") or "") != "low_sample" else "low"
        price_band = self._price_band_analysis(card).get("recommended_price_band") or {}
        if str(price_band.get("confidence") or "") == "insufficient" or sample_confidence in {"low", "insufficient"}:
            sourcing_fit = "low"
        rationale = "按方向大类、场景词、视频密度和可演示性做规则估计；这是建议路由，不替代单品评分。"
        return {
            "ai_content": ai_content,
            "replication": replication,
            "original_demo": original_demo,
            "scene_localization": scene_localization,
            "sourcing_fit": sourcing_fit,
            "rationale": rationale,
        }

    def _recommended_execution(
        self,
        card: Dict[str, Any],
        decision_action: str,
        primary_type: str,
        capability_fit: Dict[str, Any],
        risk_tags: List[str],
        signal_context: Dict[str, str],
    ) -> Dict[str, Any]:
        test_count = {
            "prioritize_low_cost_test": "4-6 款",
            "cautious_test": "3-5 款",
            "hidden_small_test": "1-2 款",
            "strong_signal_verify": "暂不测款，先人工核验样本池",
            "hidden_candidate": "暂不测款，进入暗线跟踪",
            "observe": "暂不测款，继续观察",
            "study_top_not_enter": "先拆 3-5 个头部样本，不按方向铺货",
            "avoid": "不建议测款",
        }.get(decision_action, "暂不测款")
        raw_signal = str(signal_context.get("raw_new_product_signal") or "unknown")
        if decision_action == "cautious_test" and (
            raw_signal in {"weak_new_entry", "old_product_dominated", "unknown"}
            or "high_video_density" in risk_tags
        ):
            test_count = "2-3 款"
        route = {
            "prioritize_low_cost_test": "复刻建立基线 + AI 内容放量测试",
            "cautious_test": "原创差异化优先，复刻为辅",
            "hidden_small_test": "本地场景化原创小样本",
            "strong_signal_verify": "先核查 Top 商品、代表新品和可采购性",
            "hidden_candidate": "进入样本池和下批重点追踪，不占用正式内容资源",
            "observe": "只观察指标，不占用内容资源",
            "study_top_not_enter": "拆头部爆款共性，不直接入场",
            "avoid": "暂停内容投入",
        }.get(decision_action, "观察")
        return {
            "test_sku_count": test_count,
            "content_route": route,
            "recommended_price_band": self._price_band_analysis(card).get("recommended_price_band") or {},
            "differentiation_angles": self._differentiation_angles(card, risk_tags),
        }

    def _adjust_decision_by_age_signal(
        self,
        decision_action: str,
        primary_type: str,
        risk_tags: List[str],
        age_signal: Dict[str, Any],
        card: Dict[str, Any],
    ) -> Tuple[str, str, List[str], Dict[str, Any] | None]:
        signal_type = str(age_signal.get("type") or "unknown")
        confidence = str(age_signal.get("confidence") or "insufficient")
        tags = list(risk_tags)
        override: Dict[str, Any] | None = None
        if confidence not in {"medium", "high"}:
            tags.append("age_data_insufficient")
            if signal_type in {"strong_new_entry", "moderate_new_entry"}:
                return decision_action, primary_type, sorted(set(tags)), None
        if signal_type == "strong_new_entry":
            if decision_action == "observe" and primary_type not in {"supply_bubble", "insufficient_sample"}:
                decision_action = "cautious_test"
            elif decision_action == "cautious_test":
                decision_action = "prioritize_low_cost_test"
        elif signal_type == "old_product_dominated":
            from_action = decision_action
            if decision_action in {"prioritize_low_cost_test", "cautious_test"}:
                decision_action = "study_top_not_enter"
                if primary_type == "content_gap":
                    age_structure = dict(card.get("product_age_structure") or {})
                    old_share = age_structure.get("old_180d_sales_share")
                    new90_share = age_structure.get("new_90d_sales_share")
                    old_share_text = self._format_share_for_evidence(old_share)
                    new90_share_text = self._format_share_for_evidence(new90_share)
                    override = self._make_action_override(
                        "OR-002",
                        "内容缺口但老品占位明显",
                        "内容缺口成立，但新品进入窗口弱，销量主要由老品贡献，直接铺货风险高。",
                        from_action,
                        decision_action,
                        [
                            {
                                "metric": "180天以上老品销量占比",
                                "value": old_share_text,
                                "threshold": ">=60%",
                                "conclusion": "老品占位明显",
                            },
                            {
                                "metric": "近90天新品销量占比",
                                "value": new90_share_text,
                                "threshold": "<15%",
                                "conclusion": "新品进入窗口弱",
                            },
                            {
                                "metric": "可行动新品信号",
                                "value": "老品占位明显",
                                "conclusion": "不适合新品直接铺货",
                            },
                        ],
                    )
            tags.append("old_product_dominated")
        elif signal_type == "noisy_new_supply":
            tags.append("noisy_new_supply")
            if decision_action == "cautious_test":
                decision_action = "observe"
        elif signal_type == "few_new_winners":
            decision_action = "study_top_not_enter"
            tags.append("few_new_winners")
        elif signal_type == "unknown":
            tags.append("age_data_insufficient")
        return decision_action, primary_type, sorted(set(tags)), override

    def _format_share_for_evidence(self, value: Any) -> Any:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return value
        return f"{number * 100:.1f}%"

    def _new_product_signal_context(
        self,
        raw_signal: Dict[str, Any],
        sample_confidence: str,
        price_band_analysis: Dict[str, Any],
        capability_fit: Dict[str, Any],
    ) -> Dict[str, str]:
        raw_type = str(raw_signal.get("type") or "unknown")
        age_confidence = str(raw_signal.get("confidence") or "insufficient")
        price_band = dict(price_band_analysis.get("recommended_price_band") or {})
        price_confidence = str(price_band.get("confidence") or "insufficient")
        sourcing_fit = str(capability_fit.get("sourcing_fit") or "low")
        actionable = raw_type
        if age_confidence not in {"medium", "high"}:
            actionable = "unknown"
            reason = "上架时间置信度不足，原始新品信号不能用于动作升级。"
        elif sample_confidence in {"low", "insufficient"}:
            actionable = "unknown"
            reason = "方向样本置信度不足，原始新品信号只能作为观察线索。"
        elif raw_type == "strong_new_entry" and (price_confidence == "insufficient" or sourcing_fit == "low"):
            actionable = "moderate_new_entry"
            reason = "新品信号较强，但价格带或供应链匹配不足，仅降级作为中等信号。"
        elif raw_type == "unknown":
            actionable = "unknown"
            reason = "新品进入信号不明确，不能用于动作升级。"
        else:
            reason = "新品信号可按当前等级参考。"
        return {
            "raw_new_product_signal": raw_type,
            "actionable_new_product_signal": actionable,
            "new_product_signal_reason": reason,
            "age_confidence": age_confidence,
        }

    def _normalize_age_risk_tags(self, risk_tags: List[str], signal_context: Dict[str, str]) -> List[str]:
        tags = [
            tag
            for tag in list(risk_tags)
            if tag not in {"age_data_insufficient", "new_entry_signal_unclear", "weak_new_entry_window"}
        ]
        age_confidence = str(signal_context.get("age_confidence") or "insufficient")
        raw_signal = str(signal_context.get("raw_new_product_signal") or "unknown")
        actionable_signal = str(signal_context.get("actionable_new_product_signal") or "unknown")
        if age_confidence in {"low", "insufficient"}:
            tags.append("age_data_insufficient")
        elif raw_signal == "weak_new_entry":
            tags.append("weak_new_entry_window")
        elif raw_signal == "unknown" or actionable_signal == "unknown":
            tags.append("new_entry_signal_unclear")
        return sorted(set(tags))

    def _cap_decision_upgrade_by_confidence(
        self,
        decision_action: str,
        risk_tags: List[str],
        sample_confidence: str,
        price_band_analysis: Dict[str, Any],
        capability_fit: Dict[str, Any],
    ) -> Tuple[str, List[str]]:
        max_action = None
        tags = list(risk_tags)
        price_band = dict(price_band_analysis.get("recommended_price_band") or {})
        if sample_confidence in {"low", "insufficient"}:
            max_action = "hidden_small_test"
            tags.append("low_sample")
        if str(price_band.get("confidence") or "") == "insufficient":
            max_action = "hidden_small_test"
            tags.append("price_band_insufficient")
        if str(capability_fit.get("sourcing_fit") or "") == "low":
            max_action = "hidden_small_test"
            tags.append("sourcing_fit_low")
        if not max_action:
            return decision_action, sorted(set(tags))
        rank = {
            "avoid": 0,
            "observe": 1,
            "study_top_not_enter": 2,
            "hidden_small_test": 3,
            "hidden_candidate": 3,
            "strong_signal_verify": 3,
            "cautious_test": 4,
            "prioritize_low_cost_test": 5,
        }
        if decision_action in {"study_top_not_enter", "avoid", "observe", "hidden_candidate", "strong_signal_verify"}:
            return decision_action, sorted(set(tags))
        if rank.get(decision_action, 0) > rank[max_action]:
            return max_action, sorted(set(tags))
        return decision_action, sorted(set(tags))

    def _observe_reasons(
        self,
        card: Dict[str, Any],
        demand: Dict[str, Any],
        primary_type: str,
        decision_action: str,
        capability_fit: Dict[str, Any],
        risk_tags: List[str],
    ) -> List[str]:
        if primary_type != "general_observe" and decision_action != "observe":
            return []
        reasons = []
        if float(demand.get("median_sales_7d") or 0.0) <= 0:
            reasons.append("weak_demand_signal")
        price = self._price_band_analysis(card).get("recommended_price_band") or {}
        if str(price.get("confidence") or "") in {"", "insufficient", "low"}:
            reasons.append("price_band_uncertain")
        if not self._differentiation_angles(card, risk_tags).get("product_angle"):
            reasons.append("insufficient_differentiation")
        if any(str(capability_fit.get(key) or "") == "low" for key in ["ai_content", "replication", "original_demo", "sourcing_fit"]):
            reasons.append("missing_capability_fit")
        age_signal = dict(card.get("new_product_entry_signal") or {})
        if str(age_signal.get("type") or "unknown") == "unknown":
            reasons.append("age_signal_uncertain")
        if "head_concentrated" in risk_tags or "crowded_direction" in risk_tags:
            reasons.append("conflicting_metrics")
        if str(capability_fit.get("ai_content") or "") == "low":
            reasons.append("no_clear_content_route")
        return sorted(set(reasons)) or ["insufficient_differentiation"]

    def _scale_conditions(self, decision_action: str, primary_type: str) -> List[Dict[str, Any]]:
        if primary_type == "head_concentrated":
            return [
                {"kind": "manual_review", "metric": "top_product_copyability", "operator": ">=", "threshold": "medium", "window": 1, "metric_source": "manual_or_llm"},
                {"kind": "internal_test", "metric": "tested_sku_with_sales_count", "operator": ">=", "threshold": 1, "window": 1, "metric_source": "internal_test"},
            ]
        if primary_type in {"hidden_scene", "hidden_scene_candidate"}:
            return [
                {"kind": "internal_content_signal", "metric": "scene_related_positive_comment_count", "operator": ">=", "threshold": 3, "window": 1, "metric_source": "internal_content"},
                {"kind": "internal_test", "metric": "tested_sku_with_sales_count", "operator": ">=", "threshold": 1, "window": 1, "metric_source": "internal_test"},
            ]
        return [
            {"kind": "consecutive_batches", "metric": "tested_sku_with_sales_count", "operator": ">=", "threshold": 3, "window": 2, "metric_source": "internal_test"},
            {"kind": "relative_baseline", "metric": "content_ctr", "operator": ">=", "threshold": "category_baseline * 1.1", "window": 1, "metric_source": "internal_content"},
        ]

    def _stop_loss_conditions(self, decision_action: str, primary_type: str) -> List[Dict[str, Any]]:
        if primary_type == "head_concentrated":
            return [
                {"kind": "internal_test", "metric": "tested_sku_with_sales_count", "operator": "==", "threshold": 0, "window": 2, "metric_source": "internal_test"},
                {"kind": "manual_review", "metric": "top_product_copyability", "operator": "==", "threshold": "low", "window": 1, "metric_source": "manual_or_llm"},
            ]
        if primary_type in {"hidden_scene", "hidden_scene_candidate"}:
            return [
                {"kind": "consecutive_batches", "metric": "tested_sku_with_sales_count", "operator": "==", "threshold": 0, "window": 2, "metric_source": "internal_test"},
                {"kind": "internal_content_signal", "metric": "scene_related_positive_comment_count", "operator": "==", "threshold": 0, "window": 2, "metric_source": "internal_content"},
            ]
        return [
            {"kind": "consecutive_batches", "metric": "tested_sku_with_sales_count", "operator": "==", "threshold": 0, "window": 2, "metric_source": "internal_test"},
            {"kind": "relative_baseline", "metric": "content_ctr", "operator": "<", "threshold": "category_baseline * 0.8", "window": 2, "metric_source": "internal_content"},
        ]

    def _alert_for_conditions(self, scale_conditions: List[Dict[str, Any]], stop_loss_conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
        missing = []
        for condition in list(scale_conditions) + list(stop_loss_conditions):
            metric = str(condition.get("metric") or "")
            source = str(condition.get("metric_source") or "")
            if source.startswith("internal") or source in {"manual_or_llm"}:
                missing.append(metric)
        return {
            "scale_triggered": None,
            "stop_loss_triggered": None,
            "watch_triggered": None,
            "missing_metrics": sorted(set(missing)),
        }

    def _base_risk_tags(self, card: Dict[str, Any], demand: Dict[str, Any], rules: Dict[str, Any]) -> List[str]:
        tags = []
        sample_count = int(demand.get("sample_count") or 0)
        if sample_count < int((rules.get("min_sample") or {}).get("distribution_metrics", 8) or 8):
            tags.append("low_sample")
        if str(card.get("direction_tier") or "") == "crowded":
            tags.append("crowded_direction")
        if float(card.get("direction_video_density_avg") or 0.0) >= 1.0:
            tags.append("high_video_density")
        if float(card.get("direction_creator_density_avg") or 0.0) >= 1.0:
            tags.append("high_creator_density")
        top3_share = demand.get("top3_sales_share")
        mean_median_ratio = demand.get("mean_median_ratio")
        head = dict(rules.get("head_concentrated") or {})
        head_threshold = float(head.get("top3_sales_share_threshold", 0.45) or 0.45)
        mean_threshold = float(head.get("mean_median_ratio_threshold", 2.0) or 2.0)
        if (
            sample_count >= int((rules.get("min_sample") or {}).get("p75", 12) or 12)
            and top3_share is not None
            and mean_median_ratio is not None
            and float(top3_share) >= head_threshold
            and float(mean_median_ratio) >= mean_threshold
        ):
            tags.append("head_concentrated")
        elif sample_count < int((rules.get("min_sample") or {}).get("p75", 12) or 12) and top3_share is not None and float(top3_share) >= head_threshold:
            tags.append("small_sample_top3_share_high")
        if (
            mean_median_ratio is not None
            and float(mean_median_ratio) >= mean_threshold
            and (top3_share is None or float(top3_share) < head_threshold)
        ):
            tags.append("sales_distribution_skew")
        top3_above_expected = demand.get("top3_share_above_expected")
        if (
            sample_count >= int((rules.get("min_sample") or {}).get("distribution_metrics", 8) or 8)
            and top3_above_expected is not None
            and float(top3_above_expected) >= 0.25
            and mean_median_ratio is not None
            and float(mean_median_ratio) >= 1.8
        ):
            tags.append("adjusted_head_concentration_risk")
        form_dist = dict(card.get("form_distribution_by_sales") or card.get("silhouette_distribution_by_sales") or {})
        if form_dist and max(float(value or 0.0) for value in form_dist.values()) >= 0.75:
            tags.append("form_concentration")
        if sample_count >= 8 and float(demand.get("over_threshold_item_ratio") or 0.0) < 0.2:
            tags.append("weak_conversion_signal")
        style = str(card.get("style_cluster") or "")
        if any(word in style for word in ["甜感", "韩系", "少女", "简洁", "轻熟"]):
            tags.append("aesthetic_homogeneous")
        return sorted(set(tags))

    def _category_baselines(self, cards: List[Dict[str, Any]]) -> Dict[str, float]:
        return {
            "median_sales_7d": self._median([float(card.get("direction_sales_median_7d") or 0.0) for card in cards]),
            "video_density_median": self._median([float(card.get("direction_video_density_avg") or 0.0) for card in cards]),
            "creator_density_median": self._median([float(card.get("direction_creator_density_avg") or 0.0) for card in cards]),
            "video_density_p75": self._percentile([float(card.get("direction_video_density_avg") or 0.0) for card in cards], 0.75),
            "creator_density_p75": self._percentile([float(card.get("direction_creator_density_avg") or 0.0) for card in cards], 0.75),
        }

    def _build_action_summary(self, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        grouped: Dict[str, List[Dict[str, Any]]] = {key: [] for key in DECISION_ACTION_LABELS}
        for card in cards:
            action = str(card.get("decision_action") or "observe")
            grouped.setdefault(action, []).append(card)
        summary = {}
        for action, items in grouped.items():
            items.sort(key=lambda card: (-float(card.get("direction_sales_median_7d") or 0.0), str(card.get("style_cluster") or "")))
            summary[action] = {
                "label": DECISION_ACTION_LABELS.get(action, action),
                "items": items,
                "display_names": [str(item.get("style_cluster") or "") for item in items[:3]],
                "total_count": len(items),
                "overflow_count": max(0, len(items) - 3),
            }
        return summary

    def _apply_batch_capacity(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        capacity = dict(self.raw_config.get("team_capacity") or DEFAULT_DECISION_RULES.get("team_capacity") or {})
        max_study_top = int(capacity.get("max_study_top_directions_per_batch", 2) or 2)
        study_cards = [card for card in cards if str(card.get("decision_action") or "") == "study_top_not_enter"]
        if len(study_cards) <= max_study_top:
            return cards
        keep_keys = {
            str(card.get("direction_canonical_key") or card.get("direction_id") or "")
            for card in sorted(study_cards, key=self._study_top_priority_key)[:max_study_top]
        }
        result = []
        for card in cards:
            cloned = dict(card)
            key = str(cloned.get("direction_canonical_key") or cloned.get("direction_id") or "")
            if str(cloned.get("decision_action") or "") == "study_top_not_enter" and key not in keep_keys:
                from_action = str(cloned.get("decision_action") or "study_top_not_enter")
                tags = sorted(set(list(cloned.get("risk_tags") or []) + ["study_top_capacity_limited"]))
                cloned["risk_tags"] = tags
                cloned["decision_action"] = "observe"
                cloned["decision_action_label"] = DECISION_ACTION_LABELS["observe"]
                cloned["actual_action"] = "observe"
                self._append_action_override(
                    cloned,
                    self._make_action_override(
                        "OR-006",
                        "拆头部配额已满",
                        "本批头部拆解资源有限，优先级较低方向降级为持续观察。",
                        from_action,
                        "observe",
                        [
                            {"metric": "头部拆解配额", "value": max_study_top, "conclusion": "本批头部拆解名额已满"},
                            {"metric": "方向优先级", "value": cloned.get("direction_name") or cloned.get("style_cluster"), "conclusion": "降级为持续观察 + 头部记录"},
                        ],
                    ),
                )
                cloned["recommended_execution"] = self._recommended_execution(
                    cloned,
                    decision_action="observe",
                    primary_type=str(cloned.get("primary_opportunity_type") or "general_observe"),
                    capability_fit=dict(cloned.get("our_capability_fit") or {}),
                    risk_tags=tags,
                    signal_context={
                        "raw_new_product_signal": str(cloned.get("raw_new_product_signal") or "unknown"),
                        "actionable_new_product_signal": str(cloned.get("actionable_new_product_signal") or "unknown"),
                    },
                )
                cloned["observe_reason"] = sorted(set(list(cloned.get("observe_reason") or []) + ["study_top_capacity_limited"]))
            result.append(cloned)
        return result

    def _study_top_priority_key(self, card: Dict[str, Any]) -> Tuple[int, float, float, str]:
        tags = set(str(tag) for tag in list(card.get("risk_tags") or []))
        capability = dict(card.get("our_capability_fit") or {})
        price_band = dict(card.get("price_band_analysis") or {}).get("recommended_price_band") or {}
        priority = 0
        if "old_product_dominated" in tags and "head_concentrated" in tags:
            priority += 50
        if "few_new_winners" in tags:
            priority += 40
        if self._level_at_least(str(capability.get("sourcing_fit") or "low"), "medium"):
            priority += 10
        if str(card.get("sample_confidence") or "") == "high":
            priority += 5
        if str(price_band.get("confidence") or "") == "high":
            priority += 3
        return (-priority, -float(card.get("direction_sales_median_7d") or 0.0), -float(card.get("direction_item_count") or 0.0), str(card.get("style_cluster") or ""))

    def _validate_differentiation_uniqueness(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = [dict(card) for card in cards]
        seen: List[Tuple[str, str]] = []
        for card in result:
            recommended = dict(card.get("recommended_execution") or {})
            angles = dict(recommended.get("differentiation_angles") or {})
            angle_text = " ".join(
                " ".join(str(item) for item in list(angles.get(key) or []))
                for key in ["product_angle", "scene_angle", "content_angle"]
            )
            if not angle_text.strip():
                continue
            duplicate = False
            for other_name, other_text in seen:
                if str(card.get("style_cluster") or "") == other_name:
                    continue
                if self._text_similarity(angle_text, other_text) > 0.70 or self._literal_overlap(angle_text, other_text) > 0.30:
                    duplicate = True
                    break
            if duplicate:
                tags = sorted(set(list(card.get("risk_tags") or []) + ["differentiation_angle_insufficient"]))
                card["risk_tags"] = tags
                recommended["differentiation_angles"] = {
                    "product_angle": ["切口待人工确认"],
                    "scene_angle": ["切口待人工确认"],
                    "content_angle": ["切口待人工确认"],
                }
                card["recommended_execution"] = recommended
                angle_text = "切口待人工确认"
            seen.append((str(card.get("style_cluster") or ""), angle_text))
        return result

    def _text_similarity(self, a: str, b: str) -> float:
        set_a = set(str(a or ""))
        set_b = set(str(b or ""))
        if not set_a or not set_b:
            return 0.0
        return len(set_a & set_b) / float(len(set_a | set_b))

    def _literal_overlap(self, a: str, b: str) -> float:
        tokens_a = [part for part in re_split_words(a) if part]
        tokens_b = set(part for part in re_split_words(b) if part)
        if not tokens_a or not tokens_b:
            return 0.0
        return sum(1 for token in tokens_a if token in tokens_b) / float(len(tokens_a))

    def _batch_comparison_placeholder(self, batch_id: str) -> Dict[str, Any]:
        return {
            "last_batch_id": None,
            "first_seen": True,
            "sample_count_delta": None,
            "median_sales_delta": None,
            "decision_action_change": None,
            "primary_type_change": None,
            "consecutive_batches_in_status": 1,
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

    def _has_scene_signal(self, card: Dict[str, Any], rules: Dict[str, Any]) -> bool:
        hidden = dict(rules.get("hidden_scene_candidate") or {})
        keywords = [str(item) for item in list(hidden.get("scene_keywords") or [])]
        text = " ".join(
            [
                str(card.get("style_cluster") or ""),
                " ".join(str(item) for item in list(card.get("scene_tags") or [])),
                " ".join(str(item) for item in list(card.get("top_value_points") or [])),
            ]
        )
        return any(keyword and keyword in text for keyword in keywords)

    def _differentiation_angles(self, card: Dict[str, Any], risk_tags: List[str]) -> Dict[str, List[str]]:
        style = str(card.get("style_cluster") or "")
        family = str(card.get("direction_family") or "")
        if style == "other":
            return {"product_angle": [], "scene_angle": [], "content_angle": []}
        style_templates = {
            "少女礼物感型": {
                "product_angle": ["礼物感强，但不能廉价幼稚", "小体积也要有视觉记忆点"],
                "scene_angle": ["学生朋友互送", "约会前出门搭配"],
                "content_angle": ["素色穿搭加一个发夹后变精致", "包装 / 上头效果 / 拍照出片三段式展示"],
            },
            "大体量气质型": {
                "product_angle": ["体量感明显，能撑起发量和头型", "视觉存在感强，但不能显笨重"],
                "scene_angle": ["出门前快速提升整体造型", "约会/拍照/通勤穿搭的发型收尾"],
                "content_angle": ["披发前后对比：松散到有造型", "侧后方展示抓夹体量和发型轮廓"],
            },
            "韩系轻通勤型": {
                "product_angle": ["低调但有细节，不幼稚", "适合学生/上班通勤，不夸张"],
                "scene_angle": ["上学前出门", "办公室/教室日常"],
                "content_angle": ["普通穿搭加发饰后更完整", "近景展示低调细节 + 远景展示整体搭配"],
            },
            "甜感装饰型": {
                "product_angle": ["甜感明显，但需要有记忆点", "避免过度幼稚和廉价感"],
                "scene_angle": ["拍照/约会/周末出门", "学生轻甜搭配"],
                "content_angle": ["同一套穿搭，有无发饰对比", "近景颜色和细节压镜"],
            },
            "发箍修饰型": {
                "product_angle": ["修饰头型", "压碎发、显脸小、不勒头"],
                "scene_angle": ["出门前整理刘海/碎发", "洗脸化妆或拍照前整理头顶轮廓"],
                "content_angle": ["戴前戴后脸型和头型变化", "侧脸/正脸对比"],
            },
            "发圈套组型": {
                "product_angle": ["多件组合更划算", "颜色搭配日常，不勒头发"],
                "scene_angle": ["上学/出门随手扎", "宿舍/办公室备用"],
                "content_angle": ["一组多色快速切换", "手腕佩戴 + 扎发双场景"],
            },
            "基础通勤型": {
                "product_angle": ["低调耐看，不显廉价", "基础款但要有材质或颜色细节"],
                "scene_angle": ["学生/上班通勤", "多场景日常搭配"],
                "content_angle": ["普通穿搭到轻精致的前后对比", "近景展示基础款细节"],
            },
            "盘发效率型": {
                "product_angle": ["操作简单，夹得稳", "适合快速盘发，不需要复杂技巧"],
                "scene_angle": ["热天出门前整理", "上班上学前赶时间整理"],
                "content_angle": ["30 秒手势演示", "一镜到底展示盘发过程"],
            },
            "头盔友好整理型": {
                "product_angle": ["不压头型，摘头盔后能快速恢复发型", "轻便好带，适合通勤随身整理"],
                "scene_angle": ["骑摩托通勤", "摘头盔进教室/办公室前"],
                "content_angle": ["摘头盔瞬间 + 头发压乱 + 快速整理对比", "通勤前后发型恢复演示"],
            },
        }
        if style in style_templates:
            return style_templates[style]
        if "功能结果" in family or any(word in style for word in ["盘发", "头盔", "整理"]):
            return {
                "product_angle": ["夹得稳，厚发也能固定", "不勒头皮，适合长时间佩戴"],
                "scene_angle": ["戴头盔后摘下，头发压乱后的快速整理", "热天出门前 30 秒快速盘发"],
                "content_angle": ["头发乱到整齐的前后对比", "一镜到底展示操作步骤，避免只拍结果"],
            }
        product_angle = []
        scene_angle = []
        content_angle = []
        value_points = [str(item) for item in list(card.get("top_value_points") or []) if str(item).strip()]
        scenes = [str(item) for item in list(card.get("scene_tags") or []) if str(item).strip()]
        elements = [str(item) for item in list(card.get("core_elements") or []) if str(item).strip()]
        joined = " ".join([style] + value_points + scenes)
        if any(word in joined for word in ["盘发", "头盔"]):
            product_angle.extend(["夹得稳，厚发也能固定", "不勒头皮，适合长时间通勤佩戴"])
            scene_angle.extend(["戴头盔后摘下，头发压乱后的快速整理", "热天出门前 30 秒快速盘发"])
            content_angle.extend(["头发乱到整齐的前后对比", "计时型演示：30 秒完成盘发"])
        elif any(word in joined for word in ["少女", "礼物", "拍照", "约会"]):
            product_angle.extend(["礼物感强，但不能廉价幼稚", "小体积但有视觉记忆点"])
            scene_angle.extend(["学生朋友互送", "约会前出门搭配"])
            content_angle.extend(["素色穿搭加一个发夹后变精致", "包装 / 上头效果 / 拍照出片三段式展示"])
        else:
            if value_points:
                product_angle.append("围绕{value}做明确可视卖点，不只停留在标题词。".format(value="、".join(value_points[:2])))
            if scenes:
                scene_angle.append("把{scene}拆成具体使用前后场景。".format(scene="、".join(scenes[:2])))
            if elements:
                content_angle.append("用{element}做镜头可见的差异点。".format(element="、".join(elements[:2])))
        return {
            "product_angle": product_angle[:2],
            "scene_angle": scene_angle[:2],
            "content_angle": content_angle[:2],
        }

    def _price_label_to_rmb_range(self, label: Any) -> Any:
        text = str(label or "")
        if "RMB" in text:
            return text
        mapping = {"low_price": "低价带", "mid_price": "中价带", "high_price": "高价带"}
        return mapping.get(text, text or None)

    def _level_at_least(self, value: str, minimum: str) -> bool:
        order = {"low": 1, "medium": 2, "high": 3}
        return order.get(value, 0) >= order.get(minimum, 0)

    def _rules_for(self, country: str, category: str) -> Dict[str, Any]:
        base = json.loads(json.dumps(self.raw_config.get("default") or DEFAULT_DECISION_RULES["default"], ensure_ascii=False))
        overrides = self.raw_config.get("overrides") or {}
        country_payload = overrides.get(str(country or "").strip()) or {}
        category_payload = country_payload.get(str(category or "").strip()) or {}
        self._deep_update(base, category_payload)
        return base

    def _load_config(self, path: Path | None) -> Dict[str, Any]:
        payload = json.loads(json.dumps(DEFAULT_DECISION_RULES, ensure_ascii=False))
        if path and path.exists():
            loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if isinstance(loaded, dict):
                self._deep_update(payload, loaded)
        return payload

    def _deep_update(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        for key, value in dict(source or {}).items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                self._deep_update(target[key], value)
            else:
                target[key] = value

    def _median(self, values: List[float]) -> float:
        clean = sorted(float(value or 0.0) for value in values)
        if not clean:
            return 0.0
        mid = len(clean) // 2
        if len(clean) % 2 == 1:
            return clean[mid]
        return (clean[mid - 1] + clean[mid]) / 2.0

    def _percentile(self, values: List[float], percentile: float) -> float:
        clean = sorted(float(value or 0.0) for value in values)
        if not clean:
            return 0.0
        if len(clean) == 1:
            return clean[0]
        position = max(0.0, min(1.0, percentile)) * (len(clean) - 1)
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        if lower == upper:
            return clean[lower]
        return clean[lower] + (clean[upper] - clean[lower]) * (position - lower)
