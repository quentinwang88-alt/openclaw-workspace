#!/usr/bin/env python3
"""Confidence-aware resolution for market direction summary actions."""

from __future__ import annotations

from typing import Dict


def classify_avoid_signal_confidence(
    *,
    direction_tier: str,
    video_density_avg: float,
    item_count: int,
    sales_median_7d: float,
    video_density_high: float,
    video_density_crowded_enter: float,
    sales_median_baseline: float,
    crowded_supply_overhang_sales_median: float,
    top_item_count_threshold: int,
) -> str:
    if direction_tier != "crowded":
        return ""
    if video_density_avg >= video_density_high:
        return "high"
    if item_count >= top_item_count_threshold and sales_median_7d < crowded_supply_overhang_sales_median:
        return "high"
    if video_density_avg < video_density_crowded_enter and sales_median_7d >= sales_median_baseline:
        return "low"
    return "medium"


def resolve_summary_action(
    *,
    summary_bucket: str,
    decision_confidence: str,
    avoid_signal_confidence: str,
) -> Dict[str, object]:
    tags = []
    warning = ""
    error = ""
    final_bucket = str(summary_bucket or "")
    confidence = str(decision_confidence or "low")
    avoid_conf = str(avoid_signal_confidence or "")

    if final_bucket == "enter" and confidence == "low":
        final_bucket = "watch"
        tags.append("enter_downgraded_by_low_confidence")

    if final_bucket != "enter" or not avoid_conf:
        return {
            "summary_bucket": final_bucket,
            "reason_tags": tags,
            "warning": warning,
            "error": error,
        }

    if confidence == "high" and avoid_conf == "high":
        error = "enter_high_conflicts_with_avoid_high"
    elif confidence == "medium" and avoid_conf == "medium":
        error = "enter_medium_conflicts_with_avoid_medium"
    elif confidence == "low" and avoid_conf == "high":
        final_bucket = "avoid"
        tags.append("enter_replaced_by_high_risk")
    elif confidence == "high" and avoid_conf in {"low", "medium"}:
        warning = "enter_kept_despite_risk_signal"
    elif confidence == "medium" and avoid_conf == "high":
        final_bucket = "watch"
        tags.append("enter_softened_by_high_risk")

    return {
        "summary_bucket": final_bucket,
        "reason_tags": tags,
        "warning": warning,
        "error": error,
    }
