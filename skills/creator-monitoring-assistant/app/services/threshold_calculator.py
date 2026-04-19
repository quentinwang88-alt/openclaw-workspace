#!/usr/bin/env python3
"""市场阈值计算。"""

from __future__ import annotations

from typing import Dict, List, Optional

from app.config import get_settings
from app.db import Database
from app.services.validity import is_valid_creator_for_monitoring


def percentile(values: List[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return float(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def calculate_market_thresholds(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
) -> Dict[str, float]:
    settings = get_settings()
    database = db or Database()
    all_rows = database.fetchall(
        "SELECT * FROM creator_weekly_metrics WHERE stat_week = :stat_week AND store = :store",
        {"stat_week": stat_week, "store": store},
    )
    rows = [row for row in all_rows if is_valid_creator_for_monitoring(row)]

    def extract(name: str) -> List[float]:
        return [float(row.get(name) or 0) for row in rows]

    def extract_positive(name: str) -> List[float]:
        return [float(row.get(name) or 0) for row in rows if float(row.get(name) or 0) > 0]

    positive_action_values = extract_positive("gmv_per_action")
    positive_sample_values = extract_positive("gmv_per_sample")
    action_benchmark_ready = len(positive_action_values) >= settings.min_positive_efficiency_peer_count
    sample_benchmark_ready = len(positive_sample_values) >= settings.min_positive_efficiency_peer_count

    return {
        "gmv_50p": percentile(extract("gmv"), 0.50),
        "gmv_75p": percentile(extract("gmv"), 0.75),
        "gmv_80p": percentile(extract("gmv"), 0.80),
        "gmv_4w_80p": percentile(extract("gmv_4w"), 0.80),
        "gmv_lifetime_80p": percentile(extract("gmv_lifetime"), 0.80),
        "gmv_per_action_50p": percentile(positive_action_values, 0.50) if action_benchmark_ready else 0.0,
        "gmv_per_sample_50p": percentile(positive_sample_values, 0.50) if sample_benchmark_ready else 0.0,
        "gmv_per_action_positive_count": float(len(positive_action_values)),
        "gmv_per_sample_positive_count": float(len(positive_sample_values)),
        "refund_rate_75p": percentile(extract("refund_rate"), 0.75),
        "commission_rate_75p": percentile(extract("commission_rate"), 0.75),
    }
