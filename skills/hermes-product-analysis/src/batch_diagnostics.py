#!/usr/bin/env python3
"""Batch diagnostics grouped by market_id/category_id."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List


def build_batch_diagnostics(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    records = [dict(item) for item in records]
    category_distribution: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    category_resolution_distribution = Counter()
    keyword_hits = Counter()
    uncertain = 0
    multi_candidate = 0
    category_field_missing = 0
    risk_penalty_trigger = 0
    manual_review = 0

    for item in records:
        market_id = str(item.get("market_id") or "UNKNOWN")
        category_id = str(item.get("category_id") or "UNKNOWN")
        category_distribution[market_id][category_id] += 1
        category_resolution_distribution[str(item.get("category_resolution_method") or "missing")] += 1
        flags = set(str(flag) for flag in list(item.get("risk_flags") or []))
        if "manual_category_review_required" in flags:
            manual_review += 1
        if item.get("category_specific_field_missing"):
            category_field_missing += 1
        if item.get("risk_penalty_trigger_count"):
            risk_penalty_trigger += int(item.get("risk_penalty_trigger_count") or 0)
        if len(list(item.get("candidate_directions") or [])) > 1:
            multi_candidate += 1
        if not item.get("matched_direction") or item.get("direction_uncertain"):
            uncertain += 1
        keyword_source = str(item.get("keyword_hit_source") or "no_hit")
        keyword_hits[keyword_source] += 1

    total = max(len(records), 1)
    return {
        "category_distribution": {market: dict(categories) for market, categories in category_distribution.items()},
        "category_resolution_distribution": dict(category_resolution_distribution),
        "keyword_hit_rate": {
            "zh_hit": round(keyword_hits.get("zh", 0) / total, 4),
            "local_language_hit": round(keyword_hits.get("local", 0) / total, 4),
            "english_hit": round(keyword_hits.get("en", 0) / total, 4),
            "no_hit": round(keyword_hits.get("no_hit", 0) / total, 4),
        },
        "direction_uncertain_ratio": round(uncertain / total, 4),
        "multi_direction_candidate_ratio": round(multi_candidate / total, 4),
        "category_specific_field_missing_count": category_field_missing,
        "risk_penalty_trigger_count": risk_penalty_trigger,
        "manual_category_review_count": manual_review,
        "alerts": _build_alerts(uncertain / total),
    }


def _build_alerts(direction_uncertain_ratio: float) -> List[str]:
    alerts = []
    if direction_uncertain_ratio > 0.30:
        alerts.append("方向词典覆盖不足，需要补充本地语言关键词。")
    return alerts
