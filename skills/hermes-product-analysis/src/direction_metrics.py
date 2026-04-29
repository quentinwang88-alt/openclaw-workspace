#!/usr/bin/env python3
"""Shared market-direction metrics and normalization helpers."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TAXONOMY_DIR = ROOT / "configs" / "market_insight_taxonomies"

PRIMARY_EFFICIENCY_KEYS = (
    "median_views_per_video",
    "views_per_video_median",
    "avg_views_per_video",
    "average_views_per_video",
    "engagement_per_video",
    "median_engagement_per_video",
    "avg_engagement_per_video",
)

PROXY_EFFICIENCY_KEYS = (
    "sales_per_video",
    "sales_per_creator",
)

SHORT_TREND_KEYS = (
    "past_14d_sales_median",
    "sales_median_14d",
    "过去14日销量中位数",
    "14日销量中位数",
    "14天销量中位数",
)

LONG_TREND_KEYS = (
    "past_28d_sales_median",
    "sales_median_28d",
    "过去28日销量中位数",
    "28日销量中位数",
    "28天销量中位数",
)

_TAXONOMY_CACHE: Dict[str, Dict[str, Any]] = {}


def load_taxonomy(category: str, taxonomy_dir: Optional[Path] = None) -> Dict[str, Any]:
    normalized = str(category or "").strip()
    if not normalized:
        return {}
    cache_key = "{root}:{category}".format(
        root=str(Path(taxonomy_dir or DEFAULT_TAXONOMY_DIR)),
        category=normalized,
    )
    if cache_key in _TAXONOMY_CACHE:
        return _TAXONOMY_CACHE[cache_key]
    file_path = Path(taxonomy_dir or DEFAULT_TAXONOMY_DIR) / "{category}_v1.json".format(category=normalized)
    payload: Dict[str, Any] = {}
    if file_path.exists():
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    _TAXONOMY_CACHE[cache_key] = payload
    return payload


def heat_score_mode_for_category(category: str, taxonomy_dir: Optional[Path] = None) -> str:
    taxonomy = load_taxonomy(category, taxonomy_dir=taxonomy_dir)
    scoring = taxonomy.get("scoring") or {}
    mode = str(scoring.get("heat_score_mode") or "linear").strip()
    if mode not in {"linear", "rank_blend"}:
        return "linear"
    return mode


def normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    minimum = min(values)
    maximum = max(values)
    if maximum <= minimum:
        return [0.5 for _ in values]
    return [(value - minimum) / (maximum - minimum) for value in values]


def rank_normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    indexed = sorted(enumerate(values), key=lambda pair: (pair[1], pair[0]))
    if len(indexed) == 1:
        return [0.5]
    ranks = [0.0 for _ in values]
    for position, (index, _) in enumerate(indexed):
        ranks[index] = position / float(len(indexed) - 1)
    return ranks


def compute_heat_score_components(
    sales_values: List[float],
    gmv_values: List[float],
    mode: str,
) -> Tuple[List[float], List[float]]:
    if mode == "rank_blend":
        return rank_normalize(sales_values), rank_normalize(gmv_values)
    return normalize(sales_values), normalize(gmv_values)


def coerce_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace(",", "")
            if not cleaned:
                return None
            return float(cleaned)
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_content_efficiency_metric(raw_fields: Dict[str, Any]) -> Tuple[Optional[float], str]:
    for key in PRIMARY_EFFICIENCY_KEYS:
        if key in raw_fields:
            value = coerce_float(raw_fields.get(key))
            if value is not None:
                return value, "primary"
    for key in PROXY_EFFICIENCY_KEYS:
        if key in raw_fields:
            value = coerce_float(raw_fields.get(key))
            if value is not None:
                return value, "proxy"
    return None, "missing"


def extract_content_efficiency_signals(raw_fields_list: Iterable[Dict[str, Any]]) -> Tuple[List[Optional[float]], List[str]]:
    values: List[Optional[float]] = []
    sources: List[str] = []
    for raw_fields in raw_fields_list:
        value, source = extract_content_efficiency_metric(dict(raw_fields or {}))
        values.append(value)
        sources.append(source)
    return values, sources


def normalize_optional_metric(values: List[Optional[float]]) -> List[float]:
    present = [float(value) for value in values if value is not None]
    if not present:
        return [0.0 for _ in values]
    normalized_present = normalize(present)
    iterator = iter(normalized_present)
    results: List[float] = []
    for value in values:
        if value is None:
            results.append(0.0)
        else:
            results.append(round(next(iterator) * 100.0, 4))
    return results


def seasonal_trend_from_ratio(ratio: Optional[float]) -> str:
    if ratio is None:
        return "unclear"
    if ratio > 1.2:
        return "rising"
    if ratio < 0.8:
        return "declining"
    return "stable"


def extract_historical_sales_value(
    raw_fields: Dict[str, Any],
    category: str,
    horizon: str,
    taxonomy_dir: Optional[Path] = None,
) -> Optional[float]:
    taxonomy = load_taxonomy(category, taxonomy_dir=taxonomy_dir)
    if horizon == "short":
        candidate_keys = list(taxonomy.get("seasonal_trend_short_keys") or [])
        candidate_keys.extend(SHORT_TREND_KEYS)
    else:
        candidate_keys = list(taxonomy.get("seasonal_trend_long_keys") or [])
        candidate_keys.extend(taxonomy.get("seasonal_trend_keys") or [])
        candidate_keys.extend(LONG_TREND_KEYS)
    for key in candidate_keys:
        if key not in raw_fields:
            continue
        value = coerce_float(raw_fields.get(key))
        if value is not None and value > 0:
            return value
    return None


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value or 0.0) for value in values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def percentile(values: List[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value or 0.0) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, percentile_value)) * (len(ordered) - 1)
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return ordered[lower_index]
    fraction = position - lower_index
    return ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction
