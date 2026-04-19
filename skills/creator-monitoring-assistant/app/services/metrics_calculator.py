#!/usr/bin/env python3
"""周指标计算。"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Optional

from app.config import get_settings
from app.db import Database
from app.utils.date_utils import sort_stat_weeks


def as_decimal(value: object) -> Decimal:
    return Decimal(str(value or 0))


def calc_wow(curr: Decimal, prev: Optional[Decimal]) -> Optional[Decimal]:
    if prev is None or prev == 0:
        return None
    return (curr - prev) / prev


def as_optional_float(value: Optional[Decimal]) -> Optional[float]:
    if value is None:
        return None
    return float(value)


def compute_basic_metrics(clean_row: Dict[str, object]) -> Dict[str, Decimal]:
    gmv = as_decimal(clean_row.get("gmv"))
    refund_amount = as_decimal(clean_row.get("refund_amount"))
    estimated_commission = as_decimal(clean_row.get("estimated_commission"))
    order_count = Decimal(int(clean_row.get("order_count") or 0))
    sold_item_count = Decimal(int(clean_row.get("sold_item_count") or 0))
    action_count = Decimal(int(clean_row.get("content_action_count") or 0))
    sample_count = Decimal(int(clean_row.get("shipped_sample_count") or 0))

    refund_rate = refund_amount / gmv if gmv > 0 else Decimal("0")
    commission_rate = estimated_commission / gmv if gmv > 0 else Decimal("0")
    gmv_per_action = gmv / action_count if action_count > 0 else Decimal("0")
    gmv_per_sample = gmv / sample_count if sample_count > 0 else Decimal("0")
    items_per_order = sold_item_count / order_count if order_count > 0 else Decimal("0")

    return {
        "gmv": gmv,
        "refund_rate": refund_rate,
        "commission_rate": commission_rate,
        "gmv_per_action": gmv_per_action,
        "gmv_per_sample": gmv_per_sample,
        "items_per_order": items_per_order,
    }


def determine_action_result_state(clean_row: Dict[str, object]) -> str:
    has_action = bool(clean_row.get("has_action"))
    has_result = bool(clean_row.get("has_result"))
    if has_action and has_result:
        return "action_yes_result_yes"
    if has_action and not has_result:
        return "action_yes_result_no"
    if not has_action and has_result:
        return "action_no_result_yes"
    return "action_no_result_no"


def _sorted_history(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    week_map = {row["stat_week"]: row for row in rows}
    return [week_map[week] for week in sort_stat_weeks(week_map.keys())]


def build_weekly_metrics(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
) -> None:
    settings = get_settings()
    database = db or Database()
    database.execute(
        "DELETE FROM creator_weekly_metrics WHERE stat_week = :stat_week AND store = :store",
        {"stat_week": stat_week, "store": store},
    )
    current_rows = database.fetchall(
        """
        SELECT c.*, m.creator_key, m.creator_name, m.country
        FROM creator_weekly_clean c
        JOIN creator_master m ON m.id = c.creator_id
        WHERE c.stat_week = :stat_week
          AND c.store = :store
        """,
        {"stat_week": stat_week, "store": store},
    )

    payloads = []
    for current_row in current_rows:
        history_rows = database.fetchall(
            """
            SELECT *
            FROM creator_weekly_clean
            WHERE creator_id = :creator_id
            """,
            {"creator_id": current_row["creator_id"]},
        )
        ordered = _sorted_history(history_rows)
        current_index = next(index for index, row in enumerate(ordered) if row["stat_week"] == stat_week)
        previous = ordered[current_index - 1] if current_index > 0 else None
        rolling_rows = ordered[max(0, current_index - settings.rolling_window_weeks + 1):current_index + 1]
        lifetime_rows = ordered[:current_index + 1]

        current_basic = compute_basic_metrics(current_row)
        previous_basic = compute_basic_metrics(previous) if previous else None

        rolling_basics = [compute_basic_metrics(row) for row in rolling_rows]
        gmv_4w = sum(item["gmv"] for item in rolling_basics)
        order_count_4w = sum(int(row["order_count"]) for row in rolling_rows)
        action_count_4w = sum(int(row["content_action_count"]) for row in rolling_rows)
        avg_weekly_gmv_4w = gmv_4w / Decimal(len(rolling_rows)) if rolling_rows else Decimal("0")
        avg_gmv_per_action_4w = (
            sum(item["gmv_per_action"] for item in rolling_basics) / Decimal(len(rolling_rows))
            if rolling_rows else Decimal("0")
        )
        avg_refund_rate_4w = (
            sum(item["refund_rate"] for item in rolling_basics) / Decimal(len(rolling_rows))
            if rolling_rows else Decimal("0")
        )

        lifetime_basics = [compute_basic_metrics(row) for row in lifetime_rows]
        gmv_lifetime = sum(item["gmv"] for item in lifetime_basics)
        order_count_lifetime = sum(int(row["order_count"]) for row in lifetime_rows)
        weeks_active_lifetime = len(lifetime_rows)
        weeks_with_gmv_lifetime = sum(1 for row in lifetime_rows if int(row["has_result"]) == 1)
        weeks_with_action_lifetime = sum(1 for row in lifetime_rows if int(row["has_action"]) == 1)

        payloads.append(
            {
                "stat_week": stat_week,
                "creator_id": current_row["creator_id"],
                "store": current_row.get("store") or "",
                "gmv": float(current_basic["gmv"]),
                "order_count": int(current_row["order_count"]),
                "content_action_count": int(current_row["content_action_count"]),
                "video_count": int(current_row["video_count"]),
                "live_count": int(current_row["live_count"]),
                "shipped_sample_count": int(current_row["shipped_sample_count"]),
                "refund_rate": float(current_basic["refund_rate"]),
                "commission_rate": float(current_basic["commission_rate"]),
                "gmv_per_action": float(current_basic["gmv_per_action"]),
                "gmv_per_sample": float(current_basic["gmv_per_sample"]),
                "items_per_order": float(current_basic["items_per_order"]),
                "gmv_wow": as_optional_float(calc_wow(current_basic["gmv"], previous_basic["gmv"])) if previous_basic else None,
                "order_count_wow": as_optional_float(calc_wow(Decimal(int(current_row["order_count"])), Decimal(int(previous["order_count"])))) if previous else None,
                "action_count_wow": as_optional_float(calc_wow(Decimal(int(current_row["content_action_count"])), Decimal(int(previous["content_action_count"])))) if previous else None,
                "gmv_per_action_wow": as_optional_float(calc_wow(current_basic["gmv_per_action"], previous_basic["gmv_per_action"])) if previous_basic else None,
                "refund_rate_wow": as_optional_float(calc_wow(current_basic["refund_rate"], previous_basic["refund_rate"])) if previous_basic else None,
                "gmv_4w": float(gmv_4w),
                "order_count_4w": order_count_4w,
                "action_count_4w": action_count_4w,
                "avg_weekly_gmv_4w": float(avg_weekly_gmv_4w),
                "avg_gmv_per_action_4w": float(avg_gmv_per_action_4w),
                "avg_refund_rate_4w": float(avg_refund_rate_4w),
                "gmv_lifetime": float(gmv_lifetime),
                "order_count_lifetime": order_count_lifetime,
                "weeks_active_lifetime": weeks_active_lifetime,
                "weeks_with_gmv_lifetime": weeks_with_gmv_lifetime,
                "weeks_with_action_lifetime": weeks_with_action_lifetime,
                "action_result_state": determine_action_result_state(current_row),
            }
        )

    database.executemany(
        """
        INSERT INTO creator_weekly_metrics (
            stat_week, creator_id, store, gmv, order_count, content_action_count,
            video_count, live_count, shipped_sample_count, refund_rate,
            commission_rate, gmv_per_action, gmv_per_sample, items_per_order,
            gmv_wow, order_count_wow, action_count_wow, gmv_per_action_wow,
            refund_rate_wow, gmv_4w, order_count_4w, action_count_4w,
            avg_weekly_gmv_4w, avg_gmv_per_action_4w, avg_refund_rate_4w,
            gmv_lifetime, order_count_lifetime, weeks_active_lifetime,
            weeks_with_gmv_lifetime, weeks_with_action_lifetime, action_result_state
        ) VALUES (
            :stat_week, :creator_id, :store, :gmv, :order_count, :content_action_count,
            :video_count, :live_count, :shipped_sample_count, :refund_rate,
            :commission_rate, :gmv_per_action, :gmv_per_sample, :items_per_order,
            :gmv_wow, :order_count_wow, :action_count_wow, :gmv_per_action_wow,
            :refund_rate_wow, :gmv_4w, :order_count_4w, :action_count_4w,
            :avg_weekly_gmv_4w, :avg_gmv_per_action_4w, :avg_refund_rate_4w,
            :gmv_lifetime, :order_count_lifetime, :weeks_active_lifetime,
            :weeks_with_gmv_lifetime, :weeks_with_action_lifetime, :action_result_state
        )
        """,
        payloads,
    )
