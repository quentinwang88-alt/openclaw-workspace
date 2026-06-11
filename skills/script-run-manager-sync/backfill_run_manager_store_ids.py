#!/usr/bin/env python3
"""Backfill 店铺ID in the short-video run manager table from script metadata."""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.sync import TARGET_FIELD_ALIASES, resolve_field_mapping  # noqa: E402


DEFAULT_TARGET_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "UvErb5HRWaGESXsBs18cvB3FnEe?table=tbl4eKSVgHw8IyDh&view=vewo6WdFGb"
)
DEFAULT_METADATA_DB_PATH = os.environ.get(
    "SHORT_VIDEO_AUTO_PUBLISH_DB_PATH",
    str(Path.home() / ".openclaw" / "shared" / "data" / "short_video_auto_publish.sqlite3"),
)

SCRIPT_ID_HEADER_PATTERN = re.compile(r"【(?:脚本ID|内容ID)】\s*[-:\n\r ]+\s*([A-Za-z0-9_]+)")
STRUCTURED_SCRIPT_ID_PATTERN = re.compile(r"^[A-Za-z0-9]+_[A-Za-z0-9]+_(?:M|V\d+)$")
TASK_NAME_SCRIPT_ID_PATTERN = re.compile(r"(?:^|\.)([A-Za-z0-9]+_[A-Za-z0-9]+_(?:M|V\d+))$")


def is_structured_script_id(value: object) -> bool:
    return bool(STRUCTURED_SCRIPT_ID_PATTERN.fullmatch(str(value or "").strip()))


def extract_script_id_from_prompt(prompt_text: object) -> str:
    match = SCRIPT_ID_HEADER_PATTERN.search(str(prompt_text or ""))
    if not match:
        return ""
    script_id = str(match.group(1) or "").strip()
    return script_id if is_structured_script_id(script_id) else ""


def extract_script_id_from_task_name(task_name: object) -> str:
    match = TASK_NAME_SCRIPT_ID_PATTERN.search(str(task_name or "").strip())
    if not match:
        return ""
    script_id = str(match.group(1) or "").strip()
    return script_id if is_structured_script_id(script_id) else ""


def resolve_record_script_id(fields: Dict[str, object], mapping: Dict[str, Optional[str]]) -> Tuple[str, str]:
    script_id_field = mapping.get("script_id")
    prompt_field = mapping.get("prompt")
    task_name_field = mapping.get("task_name")

    field_script_id = str(fields.get(script_id_field) or "").strip() if script_id_field else ""
    if is_structured_script_id(field_script_id):
        return field_script_id, "脚本ID字段"

    prompt_script_id = extract_script_id_from_prompt(fields.get(prompt_field)) if prompt_field else ""
    if prompt_script_id:
        return prompt_script_id, "提示词头"

    task_name_script_id = extract_script_id_from_task_name(fields.get(task_name_field)) if task_name_field else ""
    if task_name_script_id:
        return task_name_script_id, "任务名"

    return "", ""


def load_script_store_lookup(db_path: str) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT script_id, store_id
            FROM script_metadata
            WHERE COALESCE(script_id, '') <> ''
            """
        ).fetchall()
    finally:
        conn.close()

    stores_by_script: Dict[str, Set[str]] = defaultdict(set)
    for row in rows:
        script_id = str(row["script_id"] or "").strip()
        store_id = str(row["store_id"] or "").strip()
        if script_id and store_id:
            stores_by_script[script_id].add(store_id)

    lookup = {
        script_id: next(iter(stores))
        for script_id, stores in stores_by_script.items()
        if len(stores) == 1
    }
    ambiguous = {
        script_id: stores
        for script_id, stores in stores_by_script.items()
        if len(stores) > 1
    }
    return lookup, ambiguous


def ensure_store_id_field(client: FeishuBitableClient, field_names: List[str]) -> List[str]:
    if "店铺ID" in field_names:
        return field_names
    print("🧩 目标运行表缺少字段【店铺ID】，正在创建...")
    client.create_field("店铺ID", field_type=1, ui_type="Text")
    return client.list_field_names()


def compact_update(update: Dict[str, object]) -> Dict[str, object]:
    return {
        "record_id": update.get("record_id"),
        "fields": update.get("fields", {}),
        "script_id": update.get("script_id"),
        "source": update.get("source"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="回填短视频执行表历史记录的店铺ID")
    parser.add_argument("--target-feishu-url", default=DEFAULT_TARGET_FEISHU_URL, help="运行表飞书 URL")
    parser.add_argument("--metadata-db-path", default=DEFAULT_METADATA_DB_PATH, help="脚本主数据 SQLite 路径")
    parser.add_argument("--limit", type=int, help="限制扫描记录数")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有但不一致的店铺ID；默认只补空值")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不写回飞书")
    args = parser.parse_args()

    info = parse_feishu_bitable_url(args.target_feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {args.target_feishu_url}")

    client = FeishuBitableClient(app_token=info.app_token, table_id=info.table_id)
    field_names = ensure_store_id_field(client, client.list_field_names())
    mapping = resolve_field_mapping(field_names, TARGET_FIELD_ALIASES)

    store_field = mapping.get("store_id")
    if not store_field:
        raise ValueError("运行表缺少店铺ID字段")
    if not mapping.get("script_id") and not mapping.get("prompt") and not mapping.get("task_name"):
        raise ValueError("运行表缺少可解析脚本ID的字段")

    store_lookup, ambiguous_lookup = load_script_store_lookup(args.metadata_db_path)
    records = client.list_records(page_size=100, limit=args.limit)

    updates: List[Dict[str, object]] = []
    skipped_existing = 0
    skipped_no_script_id = 0
    skipped_no_store = 0
    skipped_ambiguous = 0
    conflicts: List[Dict[str, str]] = []
    missing_store: List[Dict[str, str]] = []
    source_counts: Dict[str, int] = defaultdict(int)

    for record in records:
        fields = record.fields
        script_id, source = resolve_record_script_id(fields, mapping)
        if not script_id:
            skipped_no_script_id += 1
            continue

        if script_id in ambiguous_lookup:
            skipped_ambiguous += 1
            continue

        target_store_id = store_lookup.get(script_id, "")
        if not target_store_id:
            skipped_no_store += 1
            missing_store.append(
                {
                    "record_id": record.record_id,
                    "script_id": script_id,
                    "source": source,
                }
            )
            continue

        current_store_id = str(fields.get(store_field) or "").strip()
        if current_store_id == target_store_id:
            skipped_existing += 1
            continue
        if current_store_id and current_store_id != target_store_id and not args.overwrite:
            conflicts.append(
                {
                    "record_id": record.record_id,
                    "script_id": script_id,
                    "current_store_id": current_store_id,
                    "target_store_id": target_store_id,
                }
            )
            continue

        source_counts[source] += 1
        updates.append(
            {
                "record_id": record.record_id,
                "fields": {store_field: target_store_id},
                "script_id": script_id,
                "source": source,
            }
        )

    print(
        {
            "scanned": len(records),
            "updates": len(updates),
            "source_counts": dict(source_counts),
            "skipped_existing": skipped_existing,
            "skipped_no_script_id": skipped_no_script_id,
            "skipped_no_store": skipped_no_store,
            "skipped_ambiguous": skipped_ambiguous,
            "conflicts": len(conflicts),
            "preview_updates": [compact_update(update) for update in updates[:10]],
            "preview_missing_store": missing_store[:10],
            "preview_conflicts": conflicts[:10],
        }
    )

    if args.dry_run or not updates:
        return

    batch_payload = [{"record_id": update["record_id"], "fields": update["fields"]} for update in updates]
    for start in range(0, len(batch_payload), 500):
        client.batch_update_records(batch_payload[start : start + 500])
    print({"updated": len(batch_payload)})


if __name__ == "__main__":
    main()
