#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient  # noqa: E402


STATUS_OPTIONS = ["待检查", "可发布", "不可发布", "需修改"]


def main() -> int:
    client = AutoMixcutFeishuClient("成片质检表")
    fields = {field.field_name: field for field in client.client.list_fields()}
    created = False
    updated_field = False
    migrated = []
    deleted = False

    if "人工质检状态" not in fields:
        client.client.create_field(
            field_name="人工质检状态",
            field_type=3,
            ui_type="SingleSelect",
            property=_select_property(),
        )
        created = True
        fields = {field.field_name: field for field in client.client.list_fields()}
    else:
        _update_status_field(client, fields["人工质检状态"].field_id)
        updated_field = True

    if "是否可发布" in fields:
        records = client.client.list_records(page_size=500)
        for record in records:
            current = _textish(record.fields.get("人工质检状态"))
            publishable = _boolish(record.fields.get("是否可发布"))
            if current in {"", "待检查"} and publishable is True:
                client.client.update_record_fields(record.record_id, {"人工质检状态": "可发布"})
                migrated.append(record.record_id)
        _delete_field(client, fields["是否可发布"].field_id)
        deleted = True

    print(
        json.dumps(
            {
                "success": True,
                "created_status_field": created,
                "updated_status_field": updated_field,
                "migrated_publishable_true_records": migrated,
                "deleted_publishable_field": deleted,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _select_property() -> dict[str, Any]:
    return {"options": [{"name": item, "color": idx % 54} for idx, item in enumerate(STATUS_OPTIONS)]}


def _update_status_field(client: AutoMixcutFeishuClient, field_id: str) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.client.app_token}/tables/{client.client.table_id}/fields/{field_id}"
    )
    payload = {"field_name": "人工质检状态", "type": 3, "ui_type": "SingleSelect", "property": _select_property()}
    response = client.client._request("PUT", url, headers=client.client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"更新人工质检状态字段失败: {result.get('msg')}")


def _delete_field(client: AutoMixcutFeishuClient, field_id: str) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.client.app_token}/tables/{client.client.table_id}/fields/{field_id}"
    )
    response = client.client._request("DELETE", url, headers=client.client._headers())
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"删除是否可发布字段失败: {result.get('msg')}")


def _boolish(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, list) and value:
        return _boolish(value[0])
    if isinstance(value, dict):
        for key in ("checked", "value", "text", "name"):
            if key in value:
                return _boolish(value[key])
    text = str(value or "").strip()
    if text in {"是", "可发布", "通过", "true", "True", "1", "yes", "Y"}:
        return True
    if text in {"否", "不可发布", "不通过", "false", "False", "0", "no", "N"}:
        return False
    return None


def _textish(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return ",".join(_textish(item) for item in value if _textish(item)).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            if key in value:
                return _textish(value[key])
    return str(value).strip()


if __name__ == "__main__":
    raise SystemExit(main())
