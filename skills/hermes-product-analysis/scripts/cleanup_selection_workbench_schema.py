#!/usr/bin/env python3
"""Clean the lightweight product-selection workbench schema.

This script keeps the workbench human-facing while preserving the system key
(`work_id`) used by the upstream FastMoss selection flow for idempotent upsert.
It backs up field definitions and all record fields before making destructive
field deletions.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests
from requests import HTTPError


ROOT = Path(__file__).resolve().parents[1]
SYNC_SKILL_DIR = ROOT.parent / "script-run-manager-sync"
sys.path.insert(0, str(SYNC_SKILL_DIR))

from core.bitable import (  # type: ignore  # noqa: E402
    FeishuAPIError,
    FeishuBitableClient,
    TableField,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}


RENAME_MAP = {
    "文本": "商品标题",
    "国家": "市场",
    "商品图片附件": "商品主图",
    "TikTok商品落地页地址": "商品链接",
    "7天成交均价_rmb": "价格",
    "推荐采购价_rmb": "采购价",
    "分销后毛利率": "毛利率",
    "规则总分": "core_score_a",
    "Accio备注": "采购链接/货源备注",
    "人工最终状态": "人工判断状态",
    "是否进入跟进": "是否加入测品池",
}


FIELD_SPECS = [
    {"name": "work_id", **TEXT},
    {"name": "batch_id", **TEXT},
    {"name": "product_id", **TEXT},
    {"name": "市场", **TEXT},
    {"name": "类目", **TEXT},
    {"name": "商品标题", **TEXT},
    {"name": "商品主图", "type": 17, "ui_type": "Attachment"},
    {"name": "商品链接", "type": 15, "ui_type": "Url"},
    {"name": "上架天数", **NUMBER},
    {"name": "7天销量", **NUMBER},
    {"name": "价格", **NUMBER},
    {"name": "采购价", **NUMBER},
    {"name": "毛利率", **NUMBER},
    {"name": "core_score_a", **NUMBER},
    {"name": "采购链接/货源备注", **TEXT},
    {"name": "V2总分", **NUMBER},
    {"name": "V2任务池", **TEXT},
    {"name": "任务适配度", **TEXT},
    {"name": "任务适配理由", **TEXT},
    {"name": "V2匹配方向", **TEXT},
    {"name": "差异化结论", **TEXT},
    {"name": "V2一句话理由", **TEXT},
    {"name": "V2风险标签", **TEXT},
    {"name": "生命周期状态", **TEXT},
    {"name": "人工判断状态", **TEXT},
    {"name": "负责人", **TEXT},
    {"name": "人工备注", **TEXT},
    {"name": "是否加入测品池", **CHECKBOX},
]


DESIRED_FIELD_NAMES = {str(item["name"]) for item in FIELD_SPECS}


DELETE_FIELD_NAMES = {
    "商品名称",
    "商品图片",
    "最低价_rmb",
    "最高价_rmb",
    "总销量",
    "竞争成熟度",
    "入池类型",
    "规则通过原因",
    "商品粗毛利率",
    "打法建议",
    "Hermes推荐动作",
    "Hermes推荐理由",
    "Hermes风险提醒",
}


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def _request(client: FeishuBitableClient, method: str, url: str, **kwargs) -> Dict[str, Any]:
    response = requests.request(method, url, timeout=30, **kwargs)
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != 0:
        raise FeishuAPIError(f"飞书字段请求失败: {payload.get('msg')}")
    return payload


def backup_schema_and_records(client: FeishuBitableClient) -> Path:
    fields = client.list_fields()
    records = client.list_records(page_size=100, limit=None)
    backup_dir = ROOT / "artifacts" / "schema_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"selection_workbench_schema_{time.strftime('%Y%m%d_%H%M%S')}.json"
    backup_path.write_text(
        json.dumps(
            {
                "app_token": client.app_token,
                "table_id": client.table_id,
                "fields": [
                    {
                        "field_id": item.field_id,
                        "field_name": item.field_name,
                        "field_type": item.field_type,
                        "ui_type": item.ui_type,
                        "property": item.property,
                    }
                    for item in fields
                ],
                "records": [
                    {
                        "record_id": item.record_id,
                        "fields": item.fields,
                    }
                    for item in records
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return backup_path


def copy_product_name_to_primary_title(client: FeishuBitableClient) -> int:
    existing_names = {item.field_name for item in client.list_fields()}
    target_field = "文本" if "文本" in existing_names else "商品标题" if "商品标题" in existing_names else None
    if not target_field or "商品名称" not in existing_names:
        return 0
    records = client.list_records(page_size=100, limit=None)
    updates: List[Dict[str, Any]] = []
    for item in records:
        title = str(item.fields.get("商品名称") or "").strip()
        current = str(item.fields.get(target_field) or "").strip()
        if title and not current:
            updates.append({"record_id": item.record_id, "fields": {target_field: title}})
    for batch in _chunked(updates, 100):
        client.batch_update_records(batch)
    return len(updates)


def rename_fields(client: FeishuBitableClient) -> List[str]:
    renamed: List[str] = []
    fields = client.list_fields()
    existing_names = {item.field_name for item in fields}
    for field in fields:
        target_name = RENAME_MAP.get(field.field_name)
        if not target_name or target_name == field.field_name:
            continue
        if target_name in existing_names and field.field_name != "文本":
            continue
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{client.app_token}/tables/{client.table_id}/fields/{field.field_id}"
        )
        payload: Dict[str, Any] = {
            "field_name": target_name,
            "type": field.field_type,
            "ui_type": field.ui_type or _ui_type_for_field_type(field.field_type),
        }
        if field.property is not None:
            payload["property"] = field.property
        _request(client, "PUT", url, headers=client._headers(), json=payload)
        renamed.append(f"{field.field_name} -> {target_name}")
        existing_names.discard(field.field_name)
        existing_names.add(target_name)
    return renamed


def ensure_fields(client: FeishuBitableClient) -> List[str]:
    existing_names = {item.field_name for item in client.list_fields()}
    created: List[str] = []
    for spec in FIELD_SPECS:
        name = str(spec["name"])
        if name in existing_names:
            continue
        client.create_field(field_name=name, field_type=int(spec["type"]), ui_type=str(spec["ui_type"]))
        created.append(name)
    return created


def delete_obsolete_fields(client: FeishuBitableClient) -> List[str]:
    deleted: List[str] = []
    for field in client.list_fields():
        field_name = str(field.field_name or "").strip()
        if field_name not in DELETE_FIELD_NAMES:
            continue
        if field_name in DESIRED_FIELD_NAMES:
            continue
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{client.app_token}/tables/{client.table_id}/fields/{field.field_id}"
        )
        if _delete_field_with_retry(client, url, field.field_id):
            deleted.append(field_name)
    return deleted


def _delete_field_with_retry(client: FeishuBitableClient, url: str, field_id: str) -> bool:
    """Delete a field, tolerating Feishu's occasional 504 after successful delete."""
    for attempt in range(3):
        try:
            _request(client, "DELETE", url, headers=client._headers())
            return True
        except HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in {502, 503, 504}:
                raise
            time.sleep(2 + attempt * 2)
            if not _field_exists(client, field_id):
                return True
    return not _field_exists(client, field_id)


def _field_exists(client: FeishuBitableClient, field_id: str) -> bool:
    return any(item.field_id == field_id for item in client.list_fields())


def _ui_type_for_field_type(field_type: int) -> str:
    return {
        1: "Text",
        2: "Number",
        3: "SingleSelect",
        7: "Checkbox",
        15: "Url",
        17: "Attachment",
    }.get(int(field_type), "Text")


def _chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean product selection workbench fields.")
    parser.add_argument("--feishu-url", required=True)
    args = parser.parse_args()

    client = resolve_client(args.feishu_url)
    backup_path = backup_schema_and_records(client)
    copied_titles = copy_product_name_to_primary_title(client)
    renamed = rename_fields(client)
    created = ensure_fields(client)
    deleted = delete_obsolete_fields(client)
    final_fields = [
        {"name": item.field_name, "type": item.field_type, "ui_type": item.ui_type}
        for item in client.list_fields()
    ]
    print(
        json.dumps(
            {
                "backup_path": str(backup_path),
                "copied_titles": copied_titles,
                "renamed": renamed,
                "created": created,
                "deleted": deleted,
                "final_field_count": len(final_fields),
                "final_fields": final_fields,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
