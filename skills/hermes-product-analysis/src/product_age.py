#!/usr/bin/env python3
"""Product age helpers for market insight snapshots."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple


def parse_listing_datetime(raw_value: Any, timezone: str = "Asia/Shanghai") -> Tuple[Optional[datetime], str]:
    if raw_value is None or raw_value == "":
        return None, "missing"
    if isinstance(raw_value, datetime):
        return raw_value.replace(tzinfo=None), "success"
    if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
        value = float(raw_value)
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1000.0), "success"
        if value > 10_000:
            return datetime.fromtimestamp(value), "success"
        return None, "parse_failed"
    text = str(raw_value or "").strip()
    if not text:
        return None, "missing"
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y年%m月%d日",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(text, fmt), "success"
        except ValueError:
            continue
    return None, "parse_failed"


def parse_snapshot_datetime(batch_date: str, timezone: str = "Asia/Shanghai") -> datetime:
    text = str(batch_date or "").strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime.now()


def calculate_product_age_days(listing_datetime: Optional[datetime], snapshot_datetime: datetime) -> Tuple[Optional[int], str]:
    if listing_datetime is None:
        return None, "missing"
    age_days = (snapshot_datetime.date() - listing_datetime.date()).days
    if age_days < 0:
        return 0, "future_date_adjusted"
    return max(0, age_days), "success"


def assign_age_bucket(product_age_days: Optional[int], config: Dict[str, Any] | None = None) -> str:
    if product_age_days is None:
        return "unknown"
    buckets = dict((config or {}).get("buckets") or {})
    if not buckets:
        buckets = {
            "d0_30": [0, 30],
            "d31_90": [31, 90],
            "d91_180": [91, 180],
            "d181_365": [181, 365],
            "d365_plus": [366, None],
        }
    for name, bounds in buckets.items():
        if not isinstance(bounds, list) or len(bounds) != 2:
            continue
        lower = bounds[0]
        upper = bounds[1]
        if lower is not None and product_age_days < int(lower):
            continue
        if upper is not None and product_age_days > int(upper):
            continue
        return str(name)
    return "unknown"
