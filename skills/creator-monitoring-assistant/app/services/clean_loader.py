#!/usr/bin/env python3
"""主数据同步与清洗层。"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Dict, List, Optional

from app.db import Database
from app.services.normalizer import normalize_creator_name, parse_int, parse_money


def build_creator_key(platform: str, country: str, creator_name: str, store: str = "") -> str:
    normalized_name = normalize_creator_name(creator_name)
    if store:
        return f"{platform}:{country}:{store}:{normalized_name}"
    return f"{platform}:{country}:{normalized_name}"


def sync_creator_master(import_batch_id: str, db: Optional[Database] = None) -> None:
    database = db or Database()
    rows = database.fetchall(
        """
        SELECT DISTINCT import_batch_id, stat_week, creator_name_raw, platform, country, store
        FROM creator_weekly_raw
        WHERE import_batch_id = :import_batch_id
        """,
        {"import_batch_id": import_batch_id},
    )

    for row in rows:
        creator_key = build_creator_key(row["platform"], row["country"], row["creator_name_raw"], row.get("store") or "")
        existing = database.fetchone(
            "SELECT * FROM creator_master WHERE creator_key = :creator_key",
            {"creator_key": creator_key},
        )
        if existing:
            database.execute(
                """
                UPDATE creator_master
                SET creator_name = :creator_name,
                    store = :store,
                    latest_seen_week = :latest_seen_week,
                    updated_at = CURRENT_TIMESTAMP
                WHERE creator_key = :creator_key
                """,
                {
                    "creator_name": row["creator_name_raw"].strip(),
                    "store": row.get("store") or "",
                    "latest_seen_week": row["stat_week"],
                    "creator_key": creator_key,
                },
            )
        else:
            database.execute(
                """
                INSERT INTO creator_master (
                    creator_key, creator_name, platform, country,
                    store, first_seen_week, latest_seen_week
                ) VALUES (
                    :creator_key, :creator_name, :platform, :country,
                    :store,
                    :first_seen_week, :latest_seen_week
                )
                """,
                {
                    "creator_key": creator_key,
                    "creator_name": row["creator_name_raw"].strip(),
                    "platform": row["platform"],
                    "country": row["country"],
                    "store": row.get("store") or "",
                    "first_seen_week": row["stat_week"],
                    "latest_seen_week": row["stat_week"],
                },
            )


def _aggregate_raw_rows(raw_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    grouped: Dict[str, Dict[str, object]] = {}
    for row in raw_rows:
        creator_key = build_creator_key(row["platform"], row["country"], row["creator_name_raw"], row.get("store") or "")
        bucket = grouped.setdefault(
            creator_key,
            {
                "creator_key": creator_key,
                "creator_name_raw": row["creator_name_raw"].strip(),
                "platform": row["platform"],
                "country": row["country"],
                "store": row.get("store") or "",
                "stat_week": row["stat_week"],
                "import_batch_id": row["import_batch_id"],
                "gmv": Decimal("0"),
                "refund_amount": Decimal("0"),
                "order_count": 0,
                "sold_item_count": 0,
                "refunded_item_count": 0,
                "avg_order_value": Decimal("0"),
                "avg_daily_sold_item_count": Decimal("0"),
                "video_count": 0,
                "live_count": 0,
                "estimated_commission": Decimal("0"),
                "shipped_sample_count": 0,
            },
        )
        bucket["gmv"] += parse_money(row["gmv_raw"])
        bucket["refund_amount"] += parse_money(row["refund_amount_raw"])
        bucket["order_count"] += parse_int(row["order_count_raw"])
        bucket["sold_item_count"] += parse_int(row["sold_item_count_raw"])
        bucket["refunded_item_count"] += parse_int(row["refunded_item_count_raw"])
        bucket["video_count"] += parse_int(row["video_count_raw"])
        bucket["live_count"] += parse_int(row["live_count_raw"])
        bucket["estimated_commission"] += parse_money(row["estimated_commission_raw"])
        bucket["shipped_sample_count"] += parse_int(row["shipped_sample_count_raw"])
        avg_order_value = parse_money(row["avg_order_value_raw"])
        if avg_order_value > 0:
            bucket["avg_order_value"] = avg_order_value
        avg_daily = parse_money(row["avg_daily_sold_item_count_raw"])
        if avg_daily > 0:
            bucket["avg_daily_sold_item_count"] = avg_daily
    return list(grouped.values())


def build_clean_records(
    stat_week: str,
    import_batch_id: str,
    store: str = "",
    db: Optional[Database] = None,
) -> None:
    database = db or Database()
    database.execute(
        "DELETE FROM creator_weekly_clean WHERE stat_week = :stat_week AND store = :store",
        {"stat_week": stat_week, "store": store},
    )
    raw_rows = database.fetchall(
        """
        SELECT *
        FROM creator_weekly_raw
        WHERE import_batch_id = :import_batch_id
        """,
        {"import_batch_id": import_batch_id},
    )
    grouped_rows = _aggregate_raw_rows(raw_rows)
    payloads = []

    for row in grouped_rows:
        master = database.fetchone(
            "SELECT * FROM creator_master WHERE creator_key = :creator_key",
            {"creator_key": row["creator_key"]},
        )
        if not master:
            continue
        content_action_count = int(row["video_count"]) + int(row["live_count"])
        payloads.append(
            {
                "stat_week": stat_week,
                "creator_id": master["id"],
                "import_batch_id": import_batch_id,
                "store": row.get("store") or "",
                "gmv": float(row["gmv"]),
                "refund_amount": float(row["refund_amount"]),
                "order_count": int(row["order_count"]),
                "sold_item_count": int(row["sold_item_count"]),
                "refunded_item_count": int(row["refunded_item_count"]),
                "avg_order_value": float(row["avg_order_value"]),
                "avg_daily_sold_item_count": float(row["avg_daily_sold_item_count"]),
                "video_count": int(row["video_count"]),
                "live_count": int(row["live_count"]),
                "estimated_commission": float(row["estimated_commission"]),
                "shipped_sample_count": int(row["shipped_sample_count"]),
                "content_action_count": content_action_count,
                "has_action": 1 if content_action_count > 0 else 0,
                "has_result": 1 if row["gmv"] > 0 else 0,
                "is_new_creator": 1 if master["first_seen_week"] == stat_week else 0,
            }
        )

    database.executemany(
        """
        INSERT INTO creator_weekly_clean (
            stat_week, creator_id, import_batch_id, store, gmv, refund_amount,
            order_count, sold_item_count, refunded_item_count, avg_order_value,
            avg_daily_sold_item_count, video_count, live_count,
            estimated_commission, shipped_sample_count, content_action_count,
            has_action, has_result, is_new_creator
        ) VALUES (
            :stat_week, :creator_id, :import_batch_id, :store, :gmv, :refund_amount,
            :order_count, :sold_item_count, :refunded_item_count, :avg_order_value,
            :avg_daily_sold_item_count, :video_count, :live_count,
            :estimated_commission, :shipped_sample_count, :content_action_count,
            :has_action, :has_result, :is_new_creator
        )
        """,
        payloads,
    )
