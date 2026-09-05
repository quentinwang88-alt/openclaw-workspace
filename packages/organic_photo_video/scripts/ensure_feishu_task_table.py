#!/usr/bin/env python3
"""Idempotently install the compact OPV Feishu workbench schema."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from services.feishu_workflow import (  # noqa: E402
    FIELD_EXECUTE, FIELD_NOTES, FIELD_OUTPUT, FIELD_PRESET, FIELD_PRODUCT,
    FIELD_PROGRESS, FIELD_REVIEW, FIELD_QUANTITY, FIELD_CONFIRM_PUBLISH,
    FIELD_REVIEW_MODE, FIELD_REVIEW_STAGE, FIELD_RETRY_REVIEW, FIELD_REVIEW_TOKEN,
    ProductionPresetCatalog,
)


def options(values):
    return {"options": [{"name": value} for value in values]}


def rename_field(client, field, target_name: str) -> None:
    """Feishu validates the full field contract even for a primary rename."""
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}/"
        f"tables/{client.table_id}/fields/{field.field_id}"
    )
    payload = {
        "field_name": target_name,
        "type": field.field_type,
        "ui_type": field.ui_type or "Text",
    }
    if field.property is not None:
        payload["property"] = field.property
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"重命名主字段失败：{result.get('msg')}")


def add_missing_select_options(client, field, desired) -> bool:
    existing = list((field.property or {}).get("options") or [])
    names = {str(item.get("name") or "") for item in existing}
    missing = [value for value in desired if value not in names]
    if not missing:
        return False
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}/"
        f"tables/{client.table_id}/fields/{field.field_id}"
    )
    payload = {
        "field_name": field.field_name,
        "type": field.field_type,
        "ui_type": field.ui_type or "SingleSelect",
        "property": {**dict(field.property or {}), "options": existing + [{"name": value} for value in missing]},
    }
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"补充字段选项失败【{field.field_name}】：{result.get('msg')}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default="TR10wxEXHiCYIhk8clActVdenpc")
    parser.add_argument("--table-id", default="tblj3x846gU3rshB")
    parser.add_argument("--preset-only", help="append one known preset option only; do not alter other fields")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    client = FeishuBitableClient(resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)
    catalog = ProductionPresetCatalog()
    fields = client.list_fields()
    by_name = {field.field_name: field for field in fields}
    if args.preset_only:
        if args.preset_only not in catalog.names or FIELD_PRESET not in by_name:
            raise ValueError("known preset and existing production-preset field required")
        field = by_name[FIELD_PRESET]
        before = {str(item.get("name")) for item in (field.property or {}).get("options", [])}
        changed = False
        if not args.dry_run:
            changed = add_missing_select_options(client, field, [args.preset_only])
            checked = next(f for f in client.list_fields() if f.field_id == field.field_id)
            after = {str(item.get("name")) for item in (checked.property or {}).get("options", [])}
            if not before.issubset(after) or args.preset_only not in after:
                raise RuntimeError("preset append readback mismatch")
        print(json.dumps({"preset": args.preset_only, "dry_run": args.dry_run,
            "missing": args.preset_only not in before, "appended": changed, "record_writes": 0}, ensure_ascii=False))
        return 0
    if args.dry_run:
        print(json.dumps({"dry_run": True, "existing_fields": sorted(by_name), "writes": 0}, ensure_ascii=False))
        return 0
    created = []
    renamed = []
    if FIELD_PRODUCT not in by_name:
        primary = fields[0]
        rename_field(client, primary, FIELD_PRODUCT)
        renamed.append({"from": primary.field_name, "to": FIELD_PRODUCT})
        by_name[FIELD_PRODUCT] = primary
    specs = [
        (FIELD_PRESET, 3, "SingleSelect", options(catalog.names)),
        (FIELD_EXECUTE, 7, "Checkbox", None),
        (FIELD_QUANTITY, 2, "Number", {"formatter": "0"}),
        (FIELD_PROGRESS, 3, "SingleSelect", options([
            "待执行", "生成中", "待审核", "已完成", "需处理",
            "待排班", "已排期", "发布中", "已发布", "发布失败",
        ])),
        (FIELD_OUTPUT, 17, "Attachment", None),
        (FIELD_REVIEW, 3, "SingleSelect", options([
            "待审核", "无需审核", "通过", "重做P1", "重做P2", "重做P3", "重做P4",
            "重做P5", "重做成片", "整组重做", "排期发布",
        ])),
        (FIELD_CONFIRM_PUBLISH, 7, "Checkbox", None),
        (FIELD_REVIEW_MODE, 3, "SingleSelect", options(["自动审核", "人工确认"])),
        (FIELD_REVIEW_STAGE, 3, "SingleSelect", options(["锚点审核", "组图审核", "成片终审", "已验收", "处理中", "锚点技术检查", "组图技术检查", "成片技术检查", "技术完成"])),
        (FIELD_RETRY_REVIEW, 7, "Checkbox", None),
        (FIELD_REVIEW_TOKEN, 1, "Text", None),
        (FIELD_NOTES, 1, "Text", None),
    ]
    for name, field_type, ui_type, property_value in specs:
        if name in by_name:
            field = by_name[name]
            wanted = [item["name"] for item in (property_value or {}).get("options", [])]
            if wanted and add_missing_select_options(client, field, wanted):
                created.append(f"{name}:补充选项")
            continue
        client.create_field(name, field_type, ui_type, property_value)
        created.append(name)
    final_fields = client.list_fields()
    print(json.dumps({
        "renamed": renamed,
        "created": created,
        "field_names": [field.field_name for field in final_fields],
        "request_count": client.request_count,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
