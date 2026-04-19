#!/usr/bin/env python3
"""飞书单表同步。"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.config import get_settings
from app.db import Database
from app.services.exporter import fetch_current_action_rows
from app.services.feishu_client import FeishuBitableClient
from app.services.feishu_mapper import build_feishu_record_fields
from app.utils.date_utils import sort_stat_weeks


def get_feishu_field_map(client: Optional[FeishuBitableClient] = None) -> Dict[str, str]:
    feishu = client or FeishuBitableClient()
    return {item["field_name"]: item["field_id"] for item in feishu.list_fields()}


def fetch_feishu_existing_records(
    stat_week: Optional[str] = None,
    store: str = "",
    client: Optional[FeishuBitableClient] = None,
) -> Dict[str, Dict[str, object]]:
    feishu = client or FeishuBitableClient()
    existing: Dict[str, Dict[str, object]] = {}
    for item in feishu.list_all_records():
        fields = item.get("fields", {})
        record_key = fields.get("record_key") or fields.get("文本")
        if record_key:
            existing[str(record_key)] = {
                "record_id": item["record_id"],
                "fields": fields,
            }
    return existing


def build_feishu_current_action_payload(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
    client: Optional[FeishuBitableClient] = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    rows = fetch_current_action_rows(stat_week, store=store, db=db)
    feishu = client or FeishuBitableClient()
    field_map = get_feishu_field_map(feishu)
    primary_field_name = "文本" if "文本" in field_map else None
    existing_map = fetch_feishu_existing_records(client=feishu)
    create_records: List[Dict[str, object]] = []
    update_records: List[Dict[str, object]] = []

    for row in rows:
        fields = build_feishu_record_fields(row, primary_field_name=primary_field_name)
        record_key = str(fields["record_key"])
        if record_key in existing_map:
            update_records.append(
                {
                    "record_id": str(existing_map[record_key]["record_id"]),
                    "fields": fields,
                }
            )
        else:
            create_records.append({"fields": fields})

    return create_records, update_records


def build_feishu_upsert_payload(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
    client: Optional[FeishuBitableClient] = None,
) -> Tuple[List[Dict[str, object]], List[Tuple[str, Dict[str, object]]]]:
    create_records, update_records = build_feishu_current_action_payload(
        stat_week,
        store=store,
        db=db,
        client=client,
    )
    return create_records, [(item["record_id"], item["fields"]) for item in update_records]


def resolve_latest_sync_week(
    requested_stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
) -> str:
    database = db or Database()
    rows = database.fetchall(
        """
        SELECT DISTINCT stat_week
        FROM creator_monitoring_result
        WHERE store = :store
        """,
        {"store": store},
    )
    available_weeks = [str(row["stat_week"]) for row in rows if row.get("stat_week")]
    if not available_weeks:
        return requested_stat_week
    ordered = sort_stat_weeks(available_weeks)
    latest_week = ordered[-1]
    if requested_stat_week in available_weeks:
        return latest_week
    return latest_week


def sync_current_action_table_to_feishu(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
    client: Optional[FeishuBitableClient] = None,
) -> Dict[str, object]:
    settings = get_settings()
    if not settings.feishu_enable_sync:
        return {"enabled": False, "created": 0, "updated": 0, "failed": 0}

    database = db or Database()
    effective_stat_week = resolve_latest_sync_week(stat_week, store=store, db=database)
    feishu = client or FeishuBitableClient()
    get_feishu_field_map(feishu)
    create_records, update_records = build_feishu_current_action_payload(
        effective_stat_week,
        store=store,
        db=database,
        client=feishu,
    )
    failed: List[str] = []
    created = 0
    updated = 0

    batch_size = min(max(settings.feishu_write_batch_size, 1), 500)
    for start in range(0, len(create_records), batch_size):
        batch = create_records[start:start + batch_size]
        if not batch:
            continue
        try:
            feishu.batch_create_records(batch)
            created += len(batch)
        except Exception:
            failed.extend(str(record["fields"].get("record_key")) for record in batch)

    for start in range(0, len(update_records), batch_size):
        batch = update_records[start:start + batch_size]
        if not batch:
            continue
        try:
            feishu.batch_update_records(batch)
            updated += len(batch)
        except Exception:
            for item in batch:
                try:
                    feishu.update_record(str(item["record_id"]), item["fields"])
                    updated += 1
                except Exception:
                    failed.append(str(item["fields"].get("record_key")))

    return {
        "enabled": True,
        "requested_stat_week": stat_week,
        "synced_stat_week": effective_stat_week,
        "created": created,
        "updated": updated,
        "deleted": 0,
        "failed": len(failed),
        "failed_record_keys": failed,
    }


def sync_results_to_feishu(
    stat_week: str,
    store: str = "",
    db: Optional[Database] = None,
    client: Optional[FeishuBitableClient] = None,
) -> Dict[str, object]:
    return sync_current_action_table_to_feishu(
        stat_week,
        store=store,
        db=db,
        client=client,
    )
