#!/usr/bin/env python3
"""Quality gate helpers for market insight publishing and consumption."""

from __future__ import annotations

from typing import Dict, Iterable


DEFAULT_MIN_REPORT_VALID_SAMPLE_RATIO = 0.70


def resolve_min_report_valid_sample_ratio(config) -> float:
    """Return the configured minimum valid-sample ratio for official reports."""
    raw_value = getattr(config, "min_report_valid_sample_ratio", None)
    if raw_value is None:
        report_output = getattr(config, "report_output", {}) or {}
        raw_value = report_output.get("min_valid_sample_ratio")
    if raw_value is None or raw_value == "":
        return DEFAULT_MIN_REPORT_VALID_SAMPLE_RATIO
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return DEFAULT_MIN_REPORT_VALID_SAMPLE_RATIO
    return max(0.0, min(1.0, value))


def evaluate_sample_quality(scored_items: Iterable[object], completed_product_count: int, config) -> Dict[str, object]:
    """Calculate valid-sample coverage and whether the report may be consumed."""
    scored_list = list(scored_items)
    denominator = int(completed_product_count or len(scored_list) or 0)
    valid_sample_count = 0
    for item in scored_list:
        tag = getattr(item, "tag", None)
        if bool(getattr(tag, "is_valid_sample", False)):
            valid_sample_count += 1
    invalid_sample_count = max(0, denominator - valid_sample_count)
    ratio = float(valid_sample_count) / float(denominator) if denominator else 0.0
    min_ratio = resolve_min_report_valid_sample_ratio(config)
    passed = bool(denominator > 0 and ratio >= min_ratio)
    reason = ""
    if not passed:
        reason = "有效样本率 {actual:.1%} 低于发布门槛 {threshold:.0%}".format(
            actual=ratio,
            threshold=min_ratio,
        )
    return {
        "valid_sample_count": valid_sample_count,
        "invalid_sample_count": invalid_sample_count,
        "valid_sample_ratio": ratio,
        "min_report_valid_sample_ratio": min_ratio,
        "quality_gate_passed": passed,
        "quality_gate_reason": reason,
    }
