#!/usr/bin/env python3
"""Category-specific scoring helpers."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def apply_category_risk_penalties(base_score: float, risk_flags: Iterable[str], scoring_config: Dict[str, Any]) -> Dict[str, Any]:
    penalties = dict(scoring_config.get("risk_penalties") or {})
    application = dict(scoring_config.get("risk_penalty_application") or {})
    cap_per_product = float(application.get("cap_per_product") or -6)
    floor_total_score = float(application.get("floor_total_score") or 0)
    manual_review_threshold = float(application.get("manual_review_trigger_if_penalty_lte") or -4)

    triggered: List[Dict[str, Any]] = []
    total_penalty = 0.0
    for flag in risk_flags:
        penalty = float(penalties.get(str(flag), 0) or 0)
        if penalty >= 0:
            continue
        triggered.append({"risk_flag": str(flag), "penalty": penalty})
        total_penalty += penalty

    if total_penalty < cap_per_product:
        total_penalty = cap_per_product
    adjusted_score = max(floor_total_score, round(float(base_score or 0) + total_penalty, 2))
    manual_review_required = total_penalty <= manual_review_threshold
    return {
        "base_score": round(float(base_score or 0), 2),
        "adjusted_score": adjusted_score,
        "total_penalty": round(total_penalty, 2),
        "triggered_penalties": triggered,
        "manual_review_required": manual_review_required,
        "risk_penalty_trigger_count": len(triggered),
    }
