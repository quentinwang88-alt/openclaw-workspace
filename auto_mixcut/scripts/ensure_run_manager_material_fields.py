#!/usr/bin/env python3
"""Ensure non-destructive material archive fields on the shared run-manager table."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient  # type: ignore  # noqa: E402


APP_TOKEN = "UvErb5HRWaGESXsBs18cvB3FnEe"
TABLE_ID = "tbl4eKSVgHw8IyDh"
TABLES = {
    "run-manager": (APP_TOKEN, TABLE_ID),
    "light-review": ("ZukCb6jNya0pUMsqb33cc4Ujnmb", "tblOzefH1pgI7K9U"),
}


def single_select(options: list[str]) -> dict[str, Any]:
    return {
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {"options": [{"name": name, "color": index % 54} for index, name in enumerate(options)]},
    }


FIELDS = [
    {"name": "全球产品ID", "type": 1, "ui_type": "Text"},
    {"name": "素材入库策略", **single_select(["自动入库", "不入库", "历史补录"])},
    {"name": "素材入库状态", **single_select(["未处理", "入库中", "已入库", "待补信息", "失败", "已跳过"])},
    {"name": "素材ID", "type": 1, "ui_type": "Text"},
    {"name": "OSS对象ID", "type": 1, "ui_type": "Text"},
    {"name": "OSS路径", "type": 1, "ui_type": "Text"},
    {"name": "OSS预览", "type": 15, "ui_type": "Url"},
    {"name": "素材入库时间", "type": 5, "ui_type": "DateTime"},
    {"name": "素材入库错误", "type": 1, "ui_type": "Text"},
    {"name": "飞书附件状态", **single_select(["保留中", "可清理", "已清理", "清理失败"])},
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--source", choices=sorted(TABLES), default="run-manager")
    args = parser.parse_args()
    app_token, table_id = TABLES[args.source]
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    existing = {field.field_name: field for field in client.list_fields()}
    created: list[str] = []
    skipped: list[str] = []
    mismatched: list[dict[str, Any]] = []
    for spec in FIELDS:
        current = existing.get(spec["name"])
        if current:
            skipped.append(spec["name"])
            if current.field_type != spec["type"] or current.ui_type != spec["ui_type"]:
                mismatched.append(
                    {
                        "field": spec["name"],
                        "current": {"type": current.field_type, "ui_type": current.ui_type},
                        "wanted": {"type": spec["type"], "ui_type": spec["ui_type"]},
                    }
                )
            continue
        created.append(spec["name"])
        if not args.dry_run:
            client.create_field(
                field_name=spec["name"],
                field_type=spec["type"],
                ui_type=spec["ui_type"],
                property=spec.get("property"),
            )
    print(json.dumps({"source": args.source, "dry_run": args.dry_run, "created": created, "skipped": skipped, "mismatched": mismatched}, ensure_ascii=False, indent=2))
    return 1 if mismatched else 0


if __name__ == "__main__":
    raise SystemExit(main())
