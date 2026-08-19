#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from miaoshou_auto_listing.config import load_config  # noqa: E402
from miaoshou_auto_listing.services.feishu_task_table import (  # noqa: E402
    FeishuTaskError,
    FeishuTaskTable,
    _plain_text,
)


PENDING_MARKERS = (
    "SUBMITTED_PENDING_VERIFICATION",
    "发布结果未确认",
    "发布已提交",
    "等待产品ID",
)
INPUT_MARKERS = (
    "SIZE_CHART_REQUIRED",
    "SIZE_CHART_DETECTION_FAILED",
    "任务校验失败",
    "尺码图",
    "尺码表",
    "采购链接不是可识别",
    "短链接没有返回",
)


def classify_legacy_error(result: str, statuses: Dict[str, str]) -> str:
    if any(marker in result for marker in PENDING_MARKERS):
        return statuses["pending_verification"]
    if any(marker in result for marker in INPUT_MARKERS):
        return statuses["needs_input"]
    return ""


def ensure_status_options(table: FeishuTaskTable, *, apply: bool) -> Dict[str, Any]:
    field_name = table.settings["fields"]["status"]
    field = next(
        (item for item in table.list_fields() if item.get("field_name") == field_name),
        None,
    )
    if field is None:
        raise FeishuTaskError(f"飞书表中不存在字段：{field_name}")
    property_value = dict(field.get("property") or {})
    options: List[Dict[str, Any]] = [dict(item) for item in property_value.get("options", [])]
    existing = {_plain_text(item.get("name")) for item in options}
    required = list(dict.fromkeys(table.settings["statuses"].values()))
    added = [name for name in required if name not in existing]
    for offset, name in enumerate(added):
        options.append({"name": name, "color": (len(options) + offset) % 8})
    if apply and added:
        property_value["options"] = options
        table.update_field(
            _plain_text(field.get("field_id")),
            field_name,
            int(field.get("type")),
            _plain_text(field.get("ui_type")),
            property_value,
        )
    return {"field": field_name, "existing": sorted(existing), "added": added}


def migrate_records(table: FeishuTaskTable, *, apply: bool) -> List[Dict[str, str]]:
    fields = table.settings["fields"]
    statuses = table.settings["statuses"]
    changes: List[Dict[str, str]] = []
    for record in table.list_records(all_records=True):
        values = record.get("fields") or {}
        if _plain_text(values.get(fields["status"])) != statuses["error"]:
            continue
        result = _plain_text(values.get(fields["result"]))
        target = classify_legacy_error(result, statuses)
        if not target:
            continue
        record_id = _plain_text(record.get("record_id"))
        changes.append({"record_id": record_id, "to": target, "result": result[:160]})
        if apply:
            table.update_record(record_id, {fields["status"]: target})
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ensure listing status options and safely migrate legacy errors"
    )
    parser.add_argument("--config-dir", type=Path, default=PROJECT_ROOT / "config")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    table = FeishuTaskTable(load_config(args.config_dir.resolve()))
    report = {
        "mode": "apply" if args.apply else "dry_run",
        "field": ensure_status_options(table, apply=args.apply),
        "record_changes": migrate_records(table, apply=args.apply),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
