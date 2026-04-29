#!/usr/bin/env python3
"""Confidence scoring for market direction cards."""

from __future__ import annotations

from typing import Dict, List


def calculate_sample_score(item_count: int, low_sample_min_count: int) -> int:
    threshold = max(int(low_sample_min_count or 0), 1)
    if item_count < threshold:
        return 0
    if item_count < threshold * 2:
        return 1
    return 2


def calculate_consistency_score(
    *,
    direction_tier: str,
    sales_median_7d: float,
    video_density_avg: float,
    creator_density_avg: float,
    p75_sales_median: float,
    p75_video_density: float,
    p75_creator_density: float,
    seasonal_trend_short: str,
    seasonal_trend_long: str,
) -> Dict[str, object]:
    tags: List[str] = []
    if seasonal_trend_short == "declining" and seasonal_trend_long == "declining":
        tags.append("season_declining")
        return {"score": 0, "reason_tags": tags}
    signal_conflict = (
        sales_median_7d >= p75_sales_median
        and (video_density_avg >= p75_video_density or creator_density_avg >= p75_creator_density)
    )
    if signal_conflict:
        tags.append("signal_conflict")
    if (
        direction_tier == "priority"
        and sales_median_7d >= p75_sales_median
        and video_density_avg < p75_video_density
        and creator_density_avg < p75_creator_density
        and seasonal_trend_long != "declining"
    ):
        return {"score": 2, "reason_tags": tags}
    if direction_tier == "low_sample":
        tags.append("sample_low")
        return {"score": 0, "reason_tags": tags}
    if signal_conflict:
        return {"score": 1, "reason_tags": tags}
    if seasonal_trend_short == "declining" or seasonal_trend_long == "declining":
        tags.append("season_declining")
        return {"score": 1, "reason_tags": tags}
    return {"score": 2 if direction_tier == "balanced" else 1, "reason_tags": tags}


def calculate_completeness_score(
    *,
    content_efficiency_source: str,
    seasonal_trend_short: str,
    seasonal_trend_long: str,
) -> Dict[str, object]:
    tags: List[str] = []
    score = 2
    source = str(content_efficiency_source or "missing")
    if source == "proxy":
        score = min(score, 1)
        tags.append("efficiency_proxy")
    elif source == "missing":
        score = 0
        tags.append("efficiency_missing")
    if seasonal_trend_short == "unclear":
        score = min(score, 1)
        tags.append("season_short_missing")
    if seasonal_trend_long == "unclear":
        score = min(score, 1)
        tags.append("season_long_missing")
    return {"score": score, "reason_tags": tags}


def compose_decision_confidence(
    *,
    confidence_sample_score: int,
    confidence_consistency_score: int,
    confidence_completeness_score: int,
    reason_tags: List[str],
) -> Dict[str, object]:
    confidence_score = min(
        int(confidence_sample_score or 0),
        int(confidence_consistency_score or 0),
        int(confidence_completeness_score or 0),
    )
    decision_confidence = {2: "high", 1: "medium", 0: "low"}.get(confidence_score, "low")
    return {
        "decision_confidence": decision_confidence,
        "confidence_reason_tags": list(dict.fromkeys(reason_tags)),
    }
