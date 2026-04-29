#!/usr/bin/env python3
"""规则筛选。"""

from __future__ import annotations

from typing import Any, Dict, List

from app.models import RuleConfig, RuleEvaluationResult
from app.utils import clamp


def _between(value: Any, lower: float, upper: float) -> bool:
    return value is not None and lower <= float(value) <= upper


def _competition_maturity(video_density: Any, creator_density: Any, config: RuleConfig) -> str:
    if video_density is None or creator_density is None:
        return "中"
    video_ratio = float(video_density) / max(config.video_density_max, 0.0001)
    creator_ratio = float(creator_density) / max(config.creator_density_max, 0.0001)
    ratio = max(video_ratio, creator_ratio)
    if ratio <= 0.4:
        return "低"
    if ratio <= 0.8:
        return "中"
    return "高"


def _score_record(record: Dict[str, Any], config: RuleConfig, pool_type: str) -> float:
    sales_7d = float(record.get("sales_7d") or 0.0)
    total_sales = float(record.get("total_sales") or 0.0)
    video_density = float(record.get("video_competition_density") or config.video_density_max)
    creator_density = float(record.get("creator_competition_density") or config.creator_density_max)
    creator_order_rate = float(record.get("creator_order_rate") or 0.0)
    commission_rate = float(record.get("commission_rate") or 0.0)
    listing_days = record.get("listing_days")
    sales_ratio = sales_7d / total_sales if total_sales > 0 else 0.0

    if pool_type == "新品池":
        threshold = max(float(config.new_sales_7d_min), 1.0)
        freshness_bonus = 0.0
        if listing_days is not None:
            freshness_bonus = max(config.new_listing_days_threshold - float(listing_days), 0.0) / max(
                float(config.new_listing_days_threshold),
                1.0,
            )
        sales_score = min(sales_7d / threshold, 2.0) * 28.0
        pool_bonus = freshness_bonus * 12.0
    else:
        threshold = max(float(config.old_sales_7d_min), 1.0)
        sales_score = min(sales_7d / threshold, 2.0) * 25.0
        pool_bonus = min(sales_ratio / max(config.old_sales_ratio_min, 0.0001), 2.0) * 15.0

    total_sales_score = min(total_sales / max(float(config.total_sales_min), 1.0), 2.0) * 12.0
    video_score = max(0.0, 1.0 - (video_density / max(config.video_density_max, 0.0001))) * 16.0
    creator_score = max(0.0, 1.0 - (creator_density / max(config.creator_density_max, 0.0001))) * 16.0
    order_score = min(creator_order_rate, 0.30) / 0.30 * 10.0
    commission_score = min(commission_rate, 0.40) / 0.40 * 7.0
    total = sales_score + pool_bonus + total_sales_score + video_score + creator_score + order_score + commission_score
    return round(min(total, 100.0), 2)


def evaluate_rule_engine(records: List[Dict[str, Any]], config: RuleConfig) -> RuleEvaluationResult:
    all_records = []  # type: List[Dict[str, Any]]
    shortlist = []  # type: List[Dict[str, Any]]

    for original in records:
        record = dict(original)
        listing_days = record.get("listing_days")
        sales_7d = float(record.get("sales_7d") or 0.0)
        total_sales = float(record.get("total_sales") or 0.0)
        video_density = record.get("video_competition_density")
        creator_density = record.get("creator_competition_density")
        sales_ratio = sales_7d / total_sales if total_sales > 0 else 0.0

        pool_type = ""
        reasons = []
        new_pool = (
            listing_days is not None
            and listing_days <= config.new_listing_days_threshold
            and sales_7d >= config.new_sales_7d_min
            and _between(total_sales, config.total_sales_min, config.total_sales_max)
        )
        old_pool = (
            listing_days is not None
            and listing_days > config.new_listing_days_threshold
            and sales_7d >= config.old_sales_7d_min
            and sales_ratio >= config.old_sales_ratio_min
            and _between(total_sales, config.total_sales_min, config.total_sales_max)
        )

        if new_pool:
            pool_type = "新品池"
            reasons.append("上架天数 <= {days}".format(days=config.new_listing_days_threshold))
            reasons.append("7天销量 >= {sales}".format(sales=config.new_sales_7d_min))
        elif old_pool:
            pool_type = "老品爆发池"
            reasons.append("上架天数 > {days}".format(days=config.new_listing_days_threshold))
            reasons.append("7天销量 >= {sales}".format(sales=config.old_sales_7d_min))
            reasons.append("7天销量占比 >= {ratio:.0%}".format(ratio=config.old_sales_ratio_min))

        competition_ok = (
            video_density is not None
            and creator_density is not None
            and float(video_density) <= config.video_density_max
            and float(creator_density) <= config.creator_density_max
        )
        if competition_ok:
            reasons.append("视频竞争密度 <= {value}".format(value=config.video_density_max))
            reasons.append("达人竞争密度 <= {value}".format(value=config.creator_density_max))

        passed = bool(pool_type) and competition_ok
        competition_maturity = _competition_maturity(video_density, creator_density, config)
        record["pool_type"] = pool_type
        record["competition_maturity"] = competition_maturity
        record["rule_status"] = "通过" if passed else "淘汰"
        record["rule_pass_reason"] = "；".join(reasons) if reasons else "未命中规则"
        source_rule_score = _score_record(record, config, pool_type) if passed else 0.0
        record["source_rule_score"] = source_rule_score
        record["rule_score"] = source_rule_score

        all_records.append(record)
        if passed:
            shortlist.append(record)

    shortlist.sort(
        key=lambda item: (
            float(item.get("source_rule_score") or item.get("rule_score") or 0.0),
            float(item.get("sales_7d") or 0.0),
            float(item.get("total_sales") or 0.0),
        ),
        reverse=True,
    )
    top_n = clamp(int(config.top_n or 50), 30, 60)
    shortlist = shortlist[:top_n]
    return RuleEvaluationResult(
        shortlist=shortlist,
        all_records=all_records,
        total_candidates=len(records),
        passed_count=len([item for item in all_records if item.get("rule_status") == "通过"]),
    )
