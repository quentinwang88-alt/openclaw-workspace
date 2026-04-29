#!/usr/bin/env python3
"""Direction-level product age structure for market insight."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from src.product_age import assign_age_bucket


AGE_BUCKETS = ["d0_30", "d31_90", "d91_180", "d181_365", "d365_plus"]


def build_category_topn_age_structure(items: Iterable[object], top_n: int, config: Dict[str, Any]) -> Dict[str, Any]:
    top_items = _top_items(items, top_n)
    return _age_structure(top_items, top_n=top_n, scope="category_top{n}_by_7d_sales".format(n=top_n), config=config)


def build_direction_topn_age_structure(category_topn_items: Iterable[object], direction_id: str, top_n: int, config: Dict[str, Any]) -> Dict[str, Any]:
    filtered = [
        item for item in category_topn_items
        if str(getattr(item, "direction_canonical_key", "") or "") == str(direction_id or "")
    ]
    return _age_structure(filtered, top_n=top_n, scope="category_top{n}_by_7d_sales".format(n=top_n), config=config)


def classify_new_product_entry_signal(age_structure: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    confidence = str(age_structure.get("age_confidence") or "insufficient")
    if confidence == "insufficient":
        return {
            "type": "unknown",
            "confidence": confidence,
            "rationale": "上架时间样本不足，新品进入判断仅作参考。",
        }
    signals = dict(config.get("signals") or {})
    new_90d_count_share = float(age_structure.get("new_90d_count_share") or 0.0)
    new_90d_sales_share = float(age_structure.get("new_90d_sales_share") or 0.0)
    old_180d_sales_share = float(age_structure.get("old_180d_sales_share") or 0.0)
    few = dict(signals.get("few_new_winners") or {})
    if (
        new_90d_count_share < float(few.get("new_90d_count_share_max", 0.20) or 0.20)
        and new_90d_sales_share >= float(few.get("new_90d_sales_share_min", 0.30) or 0.30)
    ):
        return {"type": "few_new_winners", "confidence": confidence, "rationale": "新品数量不多，但新品销量贡献较高，适合拆少数新品赢家。"}
    noisy = dict(signals.get("noisy_new_supply") or {})
    if (
        new_90d_count_share >= float(noisy.get("new_90d_count_share_min", 0.30) or 0.30)
        and new_90d_sales_share < float(noisy.get("new_90d_sales_share_max", 0.15) or 0.15)
    ):
        return {"type": "noisy_new_supply", "confidence": confidence, "rationale": "新品数量较多但销量贡献弱，可能是跟风上新或供给虚热。"}
    old = dict(signals.get("old_product_dominated") or {})
    if (
        new_90d_count_share < float(old.get("new_90d_count_share_max", 0.15) or 0.15)
        and old_180d_sales_share >= float(old.get("old_180d_sales_share_min", 0.60) or 0.60)
    ):
        return {"type": "old_product_dominated", "confidence": confidence, "rationale": "销量主要由 180 天以上老品贡献，新品进入难度较高。"}
    strong = dict(signals.get("strong_new_entry") or {})
    if (
        new_90d_count_share >= float(strong.get("new_90d_count_share_min", 0.30) or 0.30)
        and new_90d_sales_share >= float(strong.get("new_90d_sales_share_min", 0.30) or 0.30)
    ):
        return {"type": "strong_new_entry", "confidence": confidence, "rationale": "新品数量和销量贡献都较高，说明新品仍能进入榜单。"}
    moderate = dict(signals.get("moderate_new_entry") or {})
    if (
        new_90d_count_share >= float(moderate.get("new_90d_count_share_min", 0.20) or 0.20)
        and new_90d_sales_share >= float(moderate.get("new_90d_sales_share_min", 0.20) or 0.20)
    ):
        return {"type": "moderate_new_entry", "confidence": confidence, "rationale": "新品有一定进入能力，但还不构成强信号。"}
    if new_90d_count_share < 0.20 and new_90d_sales_share < 0.20:
        return {"type": "weak_new_entry", "confidence": confidence, "rationale": "新品数量和销量贡献都偏低，新品进入信号弱。"}
    return {"type": "unknown", "confidence": confidence, "rationale": "新品进入信号不明确，继续观察。"}


def _top_items(items: Iterable[object], top_n: int) -> List[object]:
    return sorted(
        list(items),
        key=lambda item: (-float(getattr(getattr(item, "snapshot", None), "sales_7d", 0.0) or 0.0), int(getattr(getattr(item, "snapshot", None), "rank_index", 0) or 0)),
    )[: max(1, int(top_n or 300))]


def _age_structure(items: Iterable[object], top_n: int, scope: str, config: Dict[str, Any]) -> Dict[str, Any]:
    item_list = list(items)
    total_count = len(item_list)
    total_sales = sum(max(float(getattr(item.snapshot, "sales_7d", 0.0) or 0.0), 0.0) for item in item_list)
    buckets = {name: {"count": 0, "sales": 0.0} for name in AGE_BUCKETS}
    valid_ages = []
    missing_age_count = 0
    for item in item_list:
        age_days = getattr(item, "product_age_days", None)
        if age_days is None:
            age_days = getattr(item.snapshot, "product_age_days", None)
        if age_days is None and getattr(item.snapshot, "listing_days", None) is not None:
            age_days = int(item.snapshot.listing_days)
        sales = max(float(item.snapshot.sales_7d or 0.0), 0.0)
        if age_days is None:
            missing_age_count += 1
            continue
        age_days = max(0, int(age_days))
        valid_ages.append(age_days)
        bucket = assign_age_bucket(age_days, config=config)
        if bucket not in buckets:
            continue
        buckets[bucket]["count"] += 1
        buckets[bucket]["sales"] += sales
    valid_count = len(valid_ages)
    distribution = {}
    for bucket, stats in buckets.items():
        count = int(stats["count"])
        sales = float(stats["sales"])
        distribution[bucket] = {
            "count": count,
            "count_share": round(count / total_count, 4) if total_count else 0.0,
            "sales": round(sales, 2),
            "sales_share": round(sales / total_sales, 4) if total_sales > 0 else 0.0,
        }
    new_30_sales = buckets["d0_30"]["sales"]
    new_90_count = buckets["d0_30"]["count"] + buckets["d31_90"]["count"]
    new_90_sales = buckets["d0_30"]["sales"] + buckets["d31_90"]["sales"]
    old_180_count = buckets["d181_365"]["count"] + buckets["d365_plus"]["count"]
    old_180_sales = buckets["d181_365"]["sales"] + buckets["d365_plus"]["sales"]
    missing_rate = round(missing_age_count / total_count, 4) if total_count else 1.0
    return {
        "top_n": int(top_n or 300),
        "scope": scope,
        "valid_age_sample_count": valid_count,
        "missing_age_count": missing_age_count,
        "missing_age_rate": missing_rate,
        "median_product_age_days": _median(valid_ages),
        "new_30d_count": int(buckets["d0_30"]["count"]),
        "new_30d_count_share": round(buckets["d0_30"]["count"] / total_count, 4) if total_count else 0.0,
        "new_30d_sales": round(new_30_sales, 2),
        "new_30d_sales_share": round(new_30_sales / total_sales, 4) if total_sales > 0 else 0.0,
        "new_90d_count": int(new_90_count),
        "new_90d_count_share": round(new_90_count / total_count, 4) if total_count else 0.0,
        "new_90d_sales": round(new_90_sales, 2),
        "new_90d_sales_share": round(new_90_sales / total_sales, 4) if total_sales > 0 else 0.0,
        "old_180d_count_share": round(old_180_count / total_count, 4) if total_count else 0.0,
        "old_180d_sales_share": round(old_180_sales / total_sales, 4) if total_sales > 0 else 0.0,
        "age_bucket_distribution": distribution,
        "age_confidence": _age_confidence(valid_count, missing_rate, config),
    }


def _age_confidence(valid_count: int, missing_rate: float, config: Dict[str, Any]) -> str:
    cfg = dict(config.get("confidence") or {})
    if valid_count < int(cfg.get("min_valid_low", 5) or 5):
        return "insufficient"
    if missing_rate >= float(cfg.get("max_missing_rate_low", 0.50) or 0.50):
        return "insufficient"
    if valid_count < int(cfg.get("min_valid_medium", 8) or 8) or missing_rate >= float(cfg.get("max_missing_rate_medium", 0.25) or 0.25):
        return "low"
    if valid_count < int(cfg.get("min_valid_high", 12) or 12) or missing_rate >= float(cfg.get("max_missing_rate_high", 0.10) or 0.10):
        return "medium"
    return "high"


def _median(values: List[int]):
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return round((ordered[mid - 1] + ordered[mid]) / 2.0, 2)
