#!/usr/bin/env python3
"""Market-task fit layer for product selection V3.1.

This module keeps the market report's action/task/pool contract separate from
the product's static direction match. The goal is traceability: a product should
enter a pool because it fits this batch's task brief, not merely because it
looks like the direction.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


POOL_TO_MAX_ACTION = {
    "test_product_pool": "select",
    "content_baseline_pool": "manual_review",
    "new_winner_analysis_pool": "manual_review",
    "replacement_candidate_pool": "manual_review",
    "category_review_pool": "manual_review",
    "manual_review_pool": "manual_review",
    "observe_pool": "observe",
    "head_reference_pool": "head_reference",
    "eliminate": "eliminate",
}

ACTION_ORDER = {
    "eliminate": 0,
    "head_reference": 1,
    "observe": 2,
    "manual_review": 3,
    "select": 4,
}


def load_unified_decision_matrix(skill_dir: Path) -> Dict[str, Any]:
    path = Path(skill_dir) / "configs" / "selection" / "unified_decision_matrix.yaml"
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_unified_decision(
    direction_action: str,
    brief_task_type: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    config = config or {}
    matrix = config.get("unified_decision_matrix") or {}
    action = str(direction_action or "neutral")
    entry = dict(matrix.get(action) or matrix.get("neutral") or {})
    task_type = str(entry.get("task_type") or "low_cost_test")
    default_pool = str(entry.get("default_pool") or "test_product_pool")
    max_pool = str(entry.get("max_pool") or default_pool)
    brief_task = str(brief_task_type or "").strip()
    has_conflict = bool(brief_task and brief_task != task_type)
    risk_flags = ["task_type_conflict"] if has_conflict else []
    return {
        "direction_action": action,
        "display_name": str(entry.get("display_name") or action),
        "task_type": task_type,
        "default_pool": default_pool,
        "resolved_pool": default_pool,
        "max_pool": max_pool,
        "max_action": POOL_TO_MAX_ACTION.get(max_pool, "manual_review"),
        "constraint_override": bool(entry.get("constraint_override")),
        "select_quota_per_direction": entry.get("select_quota_per_direction"),
        "has_conflict": has_conflict,
        "conflict_reason": (
            "direction_action={action} 对应 task_type={resolved}，但 brief_task_type={brief}，已按统一矩阵处理。".format(
                action=action,
                resolved=task_type,
                brief=brief_task,
            )
            if has_conflict
            else ""
        ),
        "risk_flags": risk_flags,
    }


def build_fallback_direction_execution_brief(
    direction_action: str,
    direction_name: str = "",
    direction_id: str = "",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    config = config or {}
    fallback = config.get("fallback_mapping") or {}
    action = str(direction_action or "neutral")
    mapped = dict(fallback.get(action) or fallback.get("neutral") or {})
    return {
        "direction_id": direction_id,
        "direction_name": direction_name,
        "direction_action": action,
        "task_type": str(mapped.get("task_type") or "low_cost_test"),
        "target_pool": str(mapped.get("target_pool") or "test_product_pool"),
        "product_selection_requirements": ["按方向匹配、产品本体、内容潜力和差异化判断"],
        "positive_signals": [],
        "negative_signals": [],
        "sample_pool_requirements": [],
        "content_requirements": [],
        "upgrade_condition": [],
        "stop_condition": [],
        "brief_source": "auto_fallback",
        "brief_confidence": "low",
        "risk_flags": ["brief_auto_generated"],
    }


def ensure_direction_execution_brief(match_result: Any, unified_decision: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    raw = getattr(match_result, "direction_execution_brief", None) or {}
    if isinstance(raw, dict) and raw:
        brief = dict(raw)
        brief.setdefault("brief_source", "generated")
        brief.setdefault("brief_confidence", "medium")
        brief.setdefault("direction_action", unified_decision.get("direction_action"))
        brief.setdefault("task_type", unified_decision.get("task_type"))
        brief.setdefault("target_pool", unified_decision.get("resolved_pool"))
        brief.setdefault("risk_flags", [])
        return brief
    return build_fallback_direction_execution_brief(
        direction_action=str(unified_decision.get("direction_action") or ""),
        direction_name=str(getattr(match_result, "matched_market_direction_name", "") or ""),
        direction_id=str(getattr(match_result, "matched_market_direction_id", "") or ""),
        config=config,
    )


def evaluate_market_task_fit(
    product: Any,
    direction_execution_brief: Dict[str, Any],
    product_scores: Dict[str, Any],
    supporting_samples: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    supporting_samples = supporting_samples or []
    task_type = str(direction_execution_brief.get("task_type") or "observe")
    target_pool = str(direction_execution_brief.get("target_pool") or "")
    brief_source = str(direction_execution_brief.get("brief_source") or "")
    content_score = _score_value(product_scores.get("content_potential"), "score")
    differentiation_score = _score_value(product_scores.get("differentiation"), "score_used")
    product_quality_score = _score_value(product_scores.get("product_quality"), "score")
    price_fit_score = _score_value(product_scores.get("product_quality"), "price_fit_score")
    supply_fit_score = _score_value(product_scores.get("product_quality"), "supply_fit_score")
    age_days = _product_age_days(product)
    title = str(getattr(product, "product_title", "") or "")

    judgements: List[Dict[str, Any]] = []
    positive_hits = _positive_signal_hits(title, direction_execution_brief)
    judgements.append(_judgement(
        "matches_positive_signals",
        bool(positive_hits),
        "medium" if positive_hits else "low",
        "rule_keyword_overlap",
        positive_hits or ["未命中 brief 正向信号"],
        ["product_title", "direction_execution_brief.positive_signals"],
    ))
    is_new = age_days is not None and age_days <= 90
    judgements.append(_judgement(
        "product_age_in_task_window",
        is_new if age_days is not None else "unknown",
        "high" if age_days is not None else "low",
        "product_age_days_rule",
        ["上架天数={days}".format(days=age_days)] if age_days is not None else ["缺少上架天数"],
        ["product_age_days"],
    ))
    has_content_hook = content_score >= 10
    judgements.append(_judgement(
        "content_hook_potential",
        has_content_hook,
        "medium",
        "score_threshold",
        ["content_potential_score={score}".format(score=content_score)],
        ["content_potential.score"],
    ))
    has_difference = differentiation_score >= 8
    judgements.append(_judgement(
        "has_replacement_difference",
        has_difference,
        "medium",
        "score_threshold",
        ["differentiation_score_used={score}".format(score=differentiation_score)],
        ["differentiation.score_used"],
    ))
    has_head_samples = bool(supporting_samples)
    judgements.append(_judgement(
        "supporting_samples_available",
        has_head_samples,
        "medium" if has_head_samples else "low",
        "sample_presence",
        ["supporting_samples={n}".format(n=len(supporting_samples))],
        ["market_direction.representative_products"],
    ))

    score, fit_level, required_met, missing = _score_for_task(
        task_type=task_type,
        age_days=age_days,
        content_score=content_score,
        differentiation_score=differentiation_score,
        product_quality_score=product_quality_score,
        price_fit_score=price_fit_score,
        supply_fit_score=supply_fit_score,
        positive_hit_count=len(positive_hits),
        has_head_samples=has_head_samples,
    )

    risk_flags = list(direction_execution_brief.get("risk_flags") or [])
    if brief_source == "auto_fallback":
        score = min(score, 10)
        if fit_level == "high":
            fit_level = "medium"
        risk_flags.append("brief_auto_generated")
    if task_type == "category_review":
        fit_level = "not_applicable"
        score = 0
        required_met = ["进入分类复核，不做常规产品适配评分"]

    return {
        "task_type": task_type,
        "target_pool": target_pool,
        "fit_level": fit_level,
        "score": round(float(score), 2),
        "required_conditions_met": required_met,
        "missing_conditions": missing,
        "task_fit_reason": _task_fit_reason(task_type, fit_level, required_met, missing),
        "judgement_items": judgements,
        "risk_flags": _dedupe(risk_flags),
    }


def validate_task_fit_orthogonality(direction_match: Dict[str, Any], market_task_fit: Dict[str, Any]) -> Dict[str, Any]:
    dynamic_fields = {
        "product_age_in_task_window",
        "content_hook_potential",
        "has_replacement_difference",
        "supporting_samples_available",
    }
    true_dynamic = []
    for item in market_task_fit.get("judgement_items") or []:
        if item.get("field_name") in dynamic_fields and item.get("value") is True:
            true_dynamic.append(item.get("field_name"))
    if true_dynamic or market_task_fit.get("fit_level") == "not_applicable":
        return {
            "is_orthogonal": True,
            "score_cap": None,
            "risk_flags": [],
            "dynamic_evidence": true_dynamic,
        }
    return {
        "is_orthogonal": False,
        "score_cap": 10,
        "risk_flags": ["task_fit_not_orthogonal"],
        "dynamic_evidence": [],
    }


def lifecycle_for_pool(pool: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = config or {}
    rules = config.get("pool_lifecycle_rules") or {}
    entry = dict(rules.get(pool) or {})
    return {
        "target_pool": pool,
        "pool_family": str(entry.get("pool_family") or _pool_family(pool)),
        "dissection_subtype": str(entry.get("dissection_subtype") or ""),
        "lifecycle_status": str(entry.get("lifecycle_status") or "pending_review"),
    }


def cap_action_by_pool(raw_action: str, max_pool: str) -> str:
    max_action = POOL_TO_MAX_ACTION.get(str(max_pool or ""), "manual_review")
    if ACTION_ORDER.get(raw_action, 0) <= ACTION_ORDER.get(max_action, 0):
        return raw_action
    return max_action


def _score_for_task(
    task_type: str,
    age_days: Optional[int],
    content_score: float,
    differentiation_score: float,
    product_quality_score: float,
    price_fit_score: float,
    supply_fit_score: float,
    positive_hit_count: int,
    has_head_samples: bool,
) -> Tuple[float, str, List[str], List[str]]:
    required_met: List[str] = []
    missing: List[str] = []

    if task_type == "low_cost_test":
        if age_days is not None and age_days <= 90:
            required_met.append("近90天新品")
        elif age_days is None:
            missing.append("上架天数缺失")
        if content_score >= 10:
            required_met.append("内容潜力达到测款门槛")
        else:
            missing.append("内容潜力不足")
        if differentiation_score >= 8:
            required_met.append("具备可表达差异")
        else:
            missing.append("差异化不足")
        if price_fit_score >= 4:
            required_met.append("价格基本适配")
        if positive_hit_count >= 1:
            required_met.append("命中 brief 正向信号")
        score = min(20, len(required_met) * 4 + max(0, content_score - 8) * 0.5)
    elif task_type == "new_winner_deep_dive":
        if age_days is not None and age_days <= 180:
            required_met.append("新品/新势能窗口内")
        else:
            missing.append("不是新品窗口样本")
        if differentiation_score >= 8:
            required_met.append("有视觉或结构差异")
        if content_score >= 10:
            required_met.append("具备内容钩子")
        score = min(20, len(required_met) * 5 + (2 if supply_fit_score >= 3 else 0))
    elif task_type == "old_product_replace":
        if age_days is not None and age_days <= 365:
            required_met.append("不是超老链接")
        else:
            missing.append("上架时间不适合替代核验")
        if differentiation_score >= 8:
            required_met.append("相对老品存在替代差异")
        else:
            missing.append("替代差异不足")
        if product_quality_score >= 20:
            required_met.append("产品本体质量可核验")
        score = min(20, len(required_met) * 5 + (2 if has_head_samples else 0))
    elif task_type in {"head_dissection", "signal_verify"}:
        if has_head_samples:
            required_met.append("存在头部/参考样本")
        else:
            missing.append("缺少头部样本")
        if differentiation_score >= 6:
            required_met.append("存在初步可拆差异")
        score = min(20, len(required_met) * 6 + min(6, content_score * 0.2))
    elif task_type in {"observe", "hidden_candidate", "avoid"}:
        score = min(10, max(2, content_score * 0.2 + differentiation_score * 0.2))
        required_met.append("当前任务只要求进入低成本观察/留档")
    else:
        score = min(12, content_score * 0.3 + differentiation_score * 0.4)

    if score >= 15:
        level = "high"
    elif score >= 9:
        level = "medium"
    elif task_type in {"observe", "hidden_candidate", "avoid"}:
        level = "not_applicable"
    else:
        level = "low"
    return score, level, required_met, missing


def _judgement(field_name: str, value: Any, confidence: str, method: str, evidence: List[str], data_sources: List[str]) -> Dict[str, Any]:
    return {
        "field_name": field_name,
        "value": value,
        "confidence": confidence,
        "judgement_method": method,
        "evidence": evidence,
        "data_sources": data_sources,
    }


def _positive_signal_hits(title: str, brief: Dict[str, Any]) -> List[str]:
    text = title.lower()
    hits = []
    for signal in list(brief.get("positive_signals") or []):
        signal_text = str(signal or "").strip()
        if not signal_text:
            continue
        chunks = [chunk for chunk in signal_text.replace("/", " ").replace("、", " ").split() if chunk]
        if any(chunk.lower() in text for chunk in chunks):
            hits.append(signal_text)
    return hits[:5]


def _product_age_days(product: Any) -> Optional[int]:
    extra = getattr(product, "extra_fields", {}) or {}
    for key in ("product_age_days", "上架天数", "listing_age_days", "days_since_listing", "on_shelf_days", "shelf_days"):
        value = extra.get(key)
        if value is None or value == "":
            continue
        try:
            return int(float(value))
        except (TypeError, ValueError):
            continue
    return None


def _score_value(payload: Any, key: str) -> float:
    if isinstance(payload, dict):
        try:
            return float(payload.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _task_fit_reason(task_type: str, fit_level: str, required: List[str], missing: List[str]) -> str:
    prefix = "任务 {task} 适配度 {level}".format(task=task_type, level=fit_level)
    if required:
        prefix += "；命中：" + "、".join(required[:3])
    if missing:
        prefix += "；缺口：" + "、".join(missing[:3])
    return prefix


def _pool_family(pool: str) -> str:
    if pool in {"new_winner_analysis_pool", "replacement_candidate_pool", "head_reference_pool"}:
        return "dissection_pool"
    if pool in {"test_product_pool", "content_baseline_pool"}:
        return "test_pool"
    if pool in {"category_review_pool", "manual_review_pool"}:
        return "review_pool"
    return pool or "unknown"


def _dedupe(items: List[str]) -> List[str]:
    seen = set()
    output = []
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output
