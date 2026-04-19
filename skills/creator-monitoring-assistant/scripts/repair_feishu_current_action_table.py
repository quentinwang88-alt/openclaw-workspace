#!/usr/bin/env python3
"""Repair Feishu rows back to the current-action-table model."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import Database
from app.services.exporter import fetch_current_action_rows
from app.services.feishu_client import FeishuBitableClient
from app.services.feishu_mapper import build_feishu_record_fields
from app.services.feishu_sync import get_feishu_field_map


WEEK_PREFIX_RE = re.compile(r"^\d{4}-W\d{2}:.+$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair Feishu table to current-action-table mode.")
    parser.add_argument("--stat-week", required=True, help="Target latest stat week to keep in Feishu.")
    parser.add_argument("--store", required=True, help="Store name to build current rows from the database.")
    parser.add_argument("--database-url", default=None, help="Optional database URL override.")
    parser.add_argument("--app-token", default=None, help="Optional Feishu app token override.")
    parser.add_argument("--table-id", default=None, help="Optional Feishu table id override.")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without writing Feishu.")
    return parser.parse_args()


def normalize_record_key(record_key: object) -> str:
    value = str(record_key or "").strip()
    if WEEK_PREFIX_RE.match(value):
        return value.split(":", 1)[1]
    return value


def has_manual_values(fields: Dict[str, object]) -> bool:
    for name in ("负责人", "跟进状态", "人工备注"):
        if str(fields.get(name) or "").strip():
            return True
    return False


def pick_keeper(records: List[Dict[str, object]], stat_week: str, creator_key: str) -> Optional[Dict[str, object]]:
    if not records:
        return None

    def score(item: Dict[str, object]) -> tuple:
        fields = item.get("fields", {})
        record_key = str(fields.get("record_key") or fields.get("文本") or "")
        week = str(fields.get("当前统计周") or fields.get("统计周") or "")
        return (
            1 if record_key == creator_key else 0,
            1 if week == stat_week else 0,
            1 if not has_manual_values(fields) else 0,
        )

    return max(records, key=score)


def summarize_weeks(records: List[Dict[str, object]]) -> Dict[str, int]:
    weeks = Counter()
    for item in records:
        fields = item.get("fields", {})
        week = str(fields.get("当前统计周") or fields.get("统计周") or "")
        weeks[week] += 1
    return dict(sorted(weeks.items()))


def main() -> None:
    args = parse_args()

    database = Database(args.database_url) if args.database_url else Database()
    client = FeishuBitableClient(app_token=args.app_token, table_id=args.table_id)
    try:
        field_map = get_feishu_field_map(client)
    except Exception:
        field_map = {}
    primary_field_name = "文本" if "文本" in field_map else None

    desired_rows = fetch_current_action_rows(args.stat_week, store=args.store, db=database)
    desired_by_key = {
        str(row["record_key"]): build_feishu_record_fields(row, primary_field_name=primary_field_name)
        for row in desired_rows
    }

    existing_records = client.list_all_records()
    grouped_existing: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for item in existing_records:
        fields = item.get("fields", {})
        raw_key = fields.get("record_key") or fields.get("文本")
        normalized = normalize_record_key(raw_key)
        if normalized:
            grouped_existing[normalized].append(item)

    create_records: List[Dict[str, object]] = []
    update_records: List[Dict[str, object]] = []
    delete_record_ids: List[str] = []
    manual_risk_records: List[Dict[str, object]] = []

    for creator_key, fields in desired_by_key.items():
        duplicates = grouped_existing.pop(creator_key, [])
        keeper = pick_keeper(duplicates, args.stat_week, creator_key)
        if keeper is None:
            create_records.append({"fields": fields})
            continue

        update_records.append({"record_id": keeper["record_id"], "fields": fields})
        for item in duplicates:
            if item["record_id"] == keeper["record_id"]:
                continue
            delete_record_ids.append(item["record_id"])

    for leftover_key, leftovers in grouped_existing.items():
        for item in leftovers:
            if has_manual_values(item.get("fields", {})):
                manual_risk_records.append(
                    {
                        "record_id": item["record_id"],
                        "record_key": leftover_key,
                        "week": item.get("fields", {}).get("当前统计周") or item.get("fields", {}).get("统计周") or "",
                    }
                )
                continue
            delete_record_ids.append(item["record_id"])

    summary = {
        "target_week": args.stat_week,
        "target_store": args.store,
        "desired_rows": len(desired_by_key),
        "before_total": len(existing_records),
        "before_weeks": summarize_weeks(existing_records),
        "to_create": len(create_records),
        "to_update": len(update_records),
        "to_delete": len(delete_record_ids),
        "manual_risk_records": manual_risk_records,
    }

    if args.dry_run:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    batch_size = 200
    created = 0
    updated = 0
    deleted = 0

    for start in range(0, len(create_records), batch_size):
        batch = create_records[start:start + batch_size]
        if batch:
            client.batch_create_records(batch)
            created += len(batch)

    for start in range(0, len(update_records), batch_size):
        batch = update_records[start:start + batch_size]
        if batch:
            client.batch_update_records(batch)
            updated += len(batch)

    for record_id in delete_record_ids:
        client.delete_record(record_id)
        deleted += 1

    after_records = client.list_all_records()
    summary.update(
        {
            "created": created,
            "updated": updated,
            "deleted": deleted,
            "after_total": len(after_records),
            "after_weeks": summarize_weeks(after_records),
        }
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
