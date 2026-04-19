#!/usr/bin/env python3
"""按任务名或提示词头部批量修复运行表脚本ID。"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


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

LEGACY_SLOT_ORDER = [
    "S1",
    "S1V1",
    "S1V2",
    "S1V3",
    "S1V4",
    "S1V5",
    "S2",
    "S2V1",
    "S2V2",
    "S2V3",
    "S2V4",
    "S2V5",
    "S3",
    "S3V1",
    "S3V2",
    "S3V3",
    "S3V4",
    "S3V5",
    "S4",
    "S4V1",
    "S4V2",
    "S4V3",
    "S4V4",
    "S4V5",
]

EXTRA_RUN_FIELD_ALIASES = {
    "prompt": ["提示词"],
    "result_sync_status": ["结果回传状态"],
    "run_status": ["状态", "跑视频状态"],
    "video_attachment": ["生成视频", "视频附件"],
    "video_link": ["视频链接", "视频附件 / 视频链接"],
}

SCRIPT_ID_HEADER_PATTERN = re.compile(r"【(?:脚本ID|内容ID)】\s*[-:\n\r ]+\s*([A-Za-z0-9_]+)")
STRUCTURED_SCRIPT_ID_PATTERN = re.compile(r"^[A-Za-z0-9]+_[A-Za-z0-9]+_(?:M|V\d+)$")


def parse_task_name(task_name: str) -> Tuple[str, Optional[str]]:
    text = str(task_name or "").strip()
    if "." not in text:
        return text, None
    product_code, raw_slot = text.rsplit(".", 1)
    slot_text = raw_slot.strip().upper()
    if not product_code.strip():
        return "", None
    if slot_text.startswith("S") and len(slot_text) >= 2:
        return product_code.strip(), slot_text
    if slot_text.isdigit():
        index = int(slot_text)
        if 1 <= index <= len(LEGACY_SLOT_ORDER):
            return product_code.strip(), LEGACY_SLOT_ORDER[index - 1]
    return product_code.strip(), None


def load_script_lookup(db_path: str) -> Dict[Tuple[str, str], str]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT script_id, product_id, script_slot
            FROM script_metadata
            """
        ).fetchall()
    finally:
        conn.close()
    lookup: Dict[Tuple[str, str], str] = {}
    for row in rows:
        product_id = str(row["product_id"] or "").strip()
        script_slot = str(row["script_slot"] or "").strip().upper()
        script_id = str(row["script_id"] or "").strip()
        if product_id and script_slot and script_id:
            lookup[(product_id, script_slot)] = script_id
    return lookup


def extract_script_id_from_prompt(prompt_text: object) -> str:
    text = str(prompt_text or "").strip()
    if not text:
        return ""
    match = SCRIPT_ID_HEADER_PATTERN.search(text)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def is_structured_script_id(script_id: object) -> bool:
    text = str(script_id or "").strip()
    return bool(STRUCTURED_SCRIPT_ID_PATTERN.fullmatch(text))


def has_video(fields: Dict[str, object], mapping: Dict[str, Optional[str]]) -> bool:
    for logical_name in ("video_attachment", "video_link"):
        field_name = mapping.get(logical_name)
        if field_name and fields.get(field_name):
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="按任务名批量修复运行表脚本ID")
    parser.add_argument("--target-feishu-url", default=DEFAULT_TARGET_FEISHU_URL, help="运行表飞书 URL")
    parser.add_argument("--metadata-db-path", default=DEFAULT_METADATA_DB_PATH, help="脚本主数据 SQLite 路径")
    parser.add_argument("--limit", type=int, help="限制扫描记录数")
    parser.add_argument("--uploaded-only", action="store_true", help="只修复结果回传状态为 uploaded 的记录")
    parser.add_argument("--has-video-only", action="store_true", help="只修复已有视频的记录")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不写回飞书")
    args = parser.parse_args()

    info = parse_feishu_bitable_url(args.target_feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {args.target_feishu_url}")
    client = FeishuBitableClient(app_token=info.app_token, table_id=info.table_id)
    field_names = client.list_field_names()
    aliases = dict(TARGET_FIELD_ALIASES)
    aliases.update(EXTRA_RUN_FIELD_ALIASES)
    mapping = resolve_field_mapping(field_names, aliases)
    task_name_field = mapping.get("task_name")
    prompt_field = mapping.get("prompt")
    script_id_field = mapping.get("script_id")
    if not task_name_field or not script_id_field:
        raise ValueError("运行表缺少任务名或脚本ID字段")

    script_lookup = load_script_lookup(args.metadata_db_path)
    records = client.list_records(page_size=100, limit=args.limit)

    updates: List[Dict[str, object]] = []
    unresolved: List[Dict[str, str]] = []
    skipped = 0
    for record in records:
        fields = record.fields
        if args.uploaded_only:
            status_field = mapping.get("result_sync_status")
            status_value = str(fields.get(status_field) or "").strip().lower() if status_field else ""
            if status_value != "uploaded":
                skipped += 1
                continue
        if args.has_video_only and not has_video(fields, mapping):
            skipped += 1
            continue

        current_script_id = str(fields.get(script_id_field) or "").strip()
        prompt_script_id = extract_script_id_from_prompt(fields.get(prompt_field)) if prompt_field else ""
        if is_structured_script_id(prompt_script_id) and current_script_id != prompt_script_id:
            updates.append({"record_id": record.record_id, "fields": {script_id_field: prompt_script_id}})
            continue

        task_name = str(fields.get(task_name_field) or "").strip()
        product_code, script_slot = parse_task_name(task_name)
        if not product_code or not script_slot:
            unresolved.append({"record_id": record.record_id, "task_name": task_name, "reason": "任务名无法解析"})
            continue

        target_script_id = script_lookup.get((product_code, script_slot))
        if not target_script_id:
            unresolved.append(
                {
                    "record_id": record.record_id,
                    "task_name": task_name,
                    "reason": f"主数据库未命中: {product_code} + {script_slot}",
                }
            )
            continue

        if current_script_id == target_script_id:
            skipped += 1
            continue
        updates.append({"record_id": record.record_id, "fields": {script_id_field: target_script_id}})

    print(
        {
            "scanned": len(records),
            "updates": len(updates),
            "unresolved": len(unresolved),
            "skipped": skipped,
            "preview_updates": updates[:10],
            "preview_unresolved": unresolved[:10],
        }
    )

    if args.dry_run or not updates:
        return

    for start in range(0, len(updates), 500):
        client.batch_update_records(updates[start:start + 500])
    print({"updated": len(updates)})


if __name__ == "__main__":
    main()
