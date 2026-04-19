#!/usr/bin/env python3
"""导出联表结果。"""

from __future__ import annotations

from typing import Dict, List, Optional

from app.db import Database
from app.utils.date_utils import sort_stat_weeks


def fetch_weekly_monitoring_rows(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
) -> List[Dict[str, object]]:
    database = db or Database()
    return database.fetchall(
        """
        SELECT
            r.record_key,
            r.stat_week,
            cm.creator_name,
            cm.creator_key,
            cm.country,
            COALESCE(r.store, cm.store, '') AS store,
            cm.owner,
            mt.gmv,
            mt.order_count,
            mt.video_count,
            mt.live_count,
            mt.shipped_sample_count,
            mt.content_action_count,
            mt.refund_rate,
            mt.commission_rate,
            mt.gmv_per_action,
            mt.gmv_per_sample,
            mt.gmv_4w,
            r.primary_tag,
            r.risk_tags,
            r.priority_level,
            r.decision_reason,
            r.next_action
        FROM creator_monitoring_result r
        JOIN creator_weekly_metrics mt
            ON mt.creator_id = r.creator_id
           AND mt.stat_week = r.stat_week
           AND mt.store = r.store
        JOIN creator_master cm
            ON cm.id = r.creator_id
        WHERE r.stat_week = :stat_week
          AND r.store = :store
        ORDER BY cm.country, COALESCE(r.store, cm.store, ''), cm.creator_name
        """,
        {"stat_week": stat_week, "store": store},
    )


def _find_previous_metrics(
    creator_id: int,
    stat_week: str,
    store: str,
    database: Database,
) -> Optional[Dict[str, object]]:
    history_rows = database.fetchall(
        """
        SELECT stat_week, gmv, content_action_count
        FROM creator_weekly_metrics
        WHERE creator_id = :creator_id
          AND store = :store
        """,
        {"creator_id": creator_id, "store": store},
    )
    if not history_rows:
        return None

    week_map = {row["stat_week"]: row for row in history_rows}
    ordered_weeks = sort_stat_weeks(week_map.keys())
    if stat_week not in week_map:
        return None

    current_index = ordered_weeks.index(stat_week)
    if current_index <= 0:
        return None

    return week_map[ordered_weeks[current_index - 1]]


def fetch_current_action_rows(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
) -> List[Dict[str, object]]:
    database = db or Database()
    rows = database.fetchall(
        """
        SELECT
            cm.creator_key AS record_key,
            r.stat_week,
            r.creator_id,
            cm.creator_name,
            cm.creator_key,
            cm.country,
            COALESCE(r.store, cm.store, '') AS store,
            COALESCE(cm.owner, r.owner, '') AS owner,
            mt.gmv,
            mt.gmv_wow,
            mt.content_action_count,
            mt.action_count_wow,
            mt.gmv_per_action,
            mt.gmv_per_action_wow,
            mt.refund_rate,
            mt.refund_rate_wow,
            mt.gmv_4w,
            r.primary_tag,
            r.risk_tags,
            r.priority_level,
            r.decision_reason,
            r.next_action
        FROM creator_monitoring_result r
        JOIN creator_weekly_metrics mt
            ON mt.creator_id = r.creator_id
           AND mt.stat_week = r.stat_week
           AND mt.store = r.store
        JOIN creator_master cm
            ON cm.id = r.creator_id
        WHERE r.stat_week = :stat_week
          AND r.store = :store
        ORDER BY cm.country, cm.creator_name
        """,
        {"stat_week": stat_week, "store": store},
    )

    enriched_rows: List[Dict[str, object]] = []
    for row in rows:
        previous = _find_previous_metrics(
            creator_id=int(row["creator_id"]),
            stat_week=stat_week,
            store=store,
            database=database,
        ) or {}
        enriched = dict(row)
        enriched["prev_gmv"] = previous.get("gmv")
        enriched["prev_content_action_count"] = previous.get("content_action_count")
        enriched_rows.append(enriched)
    return enriched_rows
