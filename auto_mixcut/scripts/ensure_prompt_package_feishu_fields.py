#!/usr/bin/env python3
"""Ensure fields for the Prompt Package Feishu workbench."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}
MULTI_SELECT = {"type": 4, "ui_type": "MultiSelect"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}
URL = {"type": 15, "ui_type": "Url"}
ATTACHMENT = {"type": 17, "ui_type": "Attachment"}


def single_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": SINGLE_SELECT["type"],
        "ui_type": SINGLE_SELECT["ui_type"],
        "property": {"options": [{"name": item, "color": idx % 54} for idx, item in enumerate(options)]},
    }


def multi_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": MULTI_SELECT["type"],
        "ui_type": MULTI_SELECT["ui_type"],
        "property": {"options": [{"name": item, "color": idx % 54} for idx, item in enumerate(options)]},
    }


PROMPT_PACKAGE_FIELDS: List[Dict[str, Any]] = [
    {"name": "提示词包ID", **TEXT},
    {"name": "商品ID", **TEXT},
    {"name": "商品名称", **TEXT},
    {"name": "SKU ID", **TEXT},
    {"name": "参考图包ID", **TEXT},
    {"name": "参考图版本", **NUMBER},
    {"name": "参考图预览地址", **URL},
    {"name": "参考图状态", **single_select(["可用", "缺失", "已归档", "更新失败"])},
    {"name": "市场", **single_select(["VN", "TH", "MY", "PH", "ID", "SG"])},
    {"name": "归一类目", **single_select(["发饰", "耳环", "围巾/帽子", "女装外套", "女装上衣", "通用服饰", "其它"])},
    {
        "name": "片段类型",
        **single_select(["商品展示", "手持商品", "细节氛围", "试戴/上身效果", "镜前日常", "居家生活", "出门前", "季节场景", "纯物静物", "拆包装", "平铺摆拍"]),
    },
    {"name": "生成档位", **single_select(["A-核心位", "B-支撑位", "C-氛围位"])},
    {"name": "素材角色", **single_select(["hero", "detail", "result", "scene", "ending"])},
    {"name": "镜头意图", **TEXT},
    {
        "name": "包状态",
        **single_select(["已创建", "待提单", "参考图异常", "已提单", "生成中", "已生成", "已回流", "已入库", "质检通过", "质检参考", "质检废弃", "失败", "暂停"]),
    },
    {"name": "人工审核结论", **single_select(["待审核", "可使用", "废弃"])},
    {"name": "是否可提单", **CHECKBOX},
    {"name": "提单优先级", **single_select(["普通", "高", "紧急", "暂缓"])},
    {"name": "短视频片段提示词", **TEXT},
    {"name": "预览地址", **URL},
    {"name": "生成视频回流", **ATTACHMENT},
    {"name": "回流质检等级", **single_select(["A-核心可用", "B-场景可用", "C-风格参考", "D-废弃"])},
    {"name": "失败原因", **TEXT},
    {"name": "备注", **TEXT},
]

PROMPT_PART_FIELDS = ["正向提示词", "负向提示词", "商品硬锚点", "禁止动作", "关键视觉约束"]


def option_names(property_value: Any) -> List[str]:
    if not isinstance(property_value, dict):
        return []
    options = property_value.get("options") or []
    return [str(item.get("name") or "").strip() for item in options if isinstance(item, dict) and item.get("name")]


def should_update_select_options(existing_field: Any, spec: Dict[str, Any]) -> bool:
    if spec.get("ui_type") not in {"SingleSelect", "MultiSelect"}:
        return False
    wanted = option_names(spec.get("property"))
    current = option_names(getattr(existing_field, "property", None))
    return bool(wanted) and wanted != current


def update_field_property(client: FeishuBitableClient, field_id: str, spec: Dict[str, Any]) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields/{field_id}"
    )
    payload = {
        "field_name": spec["name"],
        "type": spec["type"],
        "ui_type": spec["ui_type"],
        "property": spec.get("property"),
    }
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"更新字段选项失败: {spec['name']} {result.get('msg')}")


def delete_field(client: FeishuBitableClient, field_id: str, field_name: str) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields/{field_id}"
    )
    response = client._request("DELETE", url, headers=client._headers())
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"删除字段失败: {field_name} {result.get('msg')}")


def rename_field(client: FeishuBitableClient, field: Any, new_name: str) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields/{field.field_id}"
    )
    payload: Dict[str, Any] = {
        "field_name": new_name,
        "type": field.field_type,
        "ui_type": field.ui_type,
    }
    if field.property is not None:
        payload["property"] = field.property
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"重命名字段失败: {field.field_name} -> {new_name} {result.get('msg')}")


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def migrate_prompt_parts(client: FeishuBitableClient, dry_run: bool = False) -> Dict[str, Any]:
    records = client.list_records(page_size=100)
    migrated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, str]] = []
    failed: List[Dict[str, str]] = []

    for record in records:
        fields = record.fields or {}
        existing_combined = _text(fields.get("短视频片段提示词"))
        parts = {name: _text(fields.get(name)) for name in PROMPT_PART_FIELDS}
        non_empty = {name: value for name, value in parts.items() if value}
        if not non_empty:
            skipped.append({"record_id": record.record_id, "reason": "empty_prompt_parts"})
            continue
        combined = existing_combined or _format_prompt_parts(non_empty)
        if existing_combined:
            skipped.append({"record_id": record.record_id, "reason": "combined_already_exists"})
            continue
        if dry_run:
            migrated.append({"record_id": record.record_id, "parts": list(non_empty)})
            continue
        try:
            client.update_record_fields(record.record_id, {"短视频片段提示词": combined})
            migrated.append({"record_id": record.record_id, "parts": list(non_empty)})
        except Exception as exc:
            failed.append({"record_id": record.record_id, "error": str(exc)})

    return {"migrated": migrated, "skipped": skipped, "failed": failed}


def _format_prompt_parts(parts: Dict[str, str]) -> str:
    labels = {
        "正向提示词": "正向",
        "负向提示词": "负向",
        "商品硬锚点": "商品硬锚点",
        "禁止动作": "禁止动作",
        "关键视觉约束": "关键视觉约束",
    }
    lines = []
    for name in PROMPT_PART_FIELDS:
        value = parts.get(name)
        if value:
            lines.append(f"{labels[name]}：\n{value}")
    return "\n\n".join(lines)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return "\n".join(item for item in (_text(item) for item in value) if item).strip()
    return str(value).strip()


def ensure_fields(feishu_url: str, dry_run: bool = False, prune: bool = False) -> Dict[str, Any]:
    client = resolve_client(feishu_url)
    existing_fields = {field.field_name: field for field in client.list_fields()}
    wanted_names = {spec["name"] for spec in PROMPT_PACKAGE_FIELDS}
    created: List[str] = []
    skipped: List[str] = []
    updated: List[str] = []
    deleted: List[str] = []
    renamed: List[Dict[str, str]] = []
    prompt_migration: Dict[str, Any] = {"migrated": [], "skipped": [], "failed": []}
    type_mismatch: List[Dict[str, Any]] = []
    failed: List[Dict[str, str]] = []

    if prune and "文本" in existing_fields and "提示词包ID" in wanted_names:
        text_field = existing_fields["文本"]
        prompt_id_field = existing_fields.get("提示词包ID")
        if prompt_id_field:
            if dry_run:
                deleted.append("提示词包ID")
            else:
                try:
                    delete_field(client, prompt_id_field.field_id, "提示词包ID")
                    deleted.append("提示词包ID")
                except Exception as exc:
                    failed.append({"field": "提示词包ID", "error": str(exc)})
        if dry_run:
            renamed.append({"from": "文本", "to": "提示词包ID"})
            simulated_fields = dict(existing_fields)
            simulated_fields.pop("文本", None)
            simulated_fields.pop("提示词包ID", None)
            simulated_fields["提示词包ID"] = text_field
            existing_fields = simulated_fields
        else:
            try:
                rename_field(client, text_field, "提示词包ID")
                renamed.append({"from": "文本", "to": "提示词包ID"})
            except Exception as exc:
                failed.append({"field": "文本", "error": str(exc)})
            existing_fields = {field.field_name: field for field in client.list_fields()}

    for spec in PROMPT_PACKAGE_FIELDS:
        name = spec["name"]
        existing = existing_fields.get(name)
        if existing:
            if existing.field_type != spec["type"] or existing.ui_type != spec["ui_type"]:
                type_mismatch.append(
                    {
                        "field": name,
                        "current": {"type": existing.field_type, "ui_type": existing.ui_type},
                        "wanted": {"type": spec["type"], "ui_type": spec["ui_type"]},
                    }
                )
                skipped.append(name)
                continue
            if should_update_select_options(existing, spec):
                if not dry_run:
                    try:
                        update_field_property(client, existing.field_id, spec)
                    except Exception as exc:
                        failed.append({"field": name, "error": str(exc)})
                        continue
                updated.append(name)
            skipped.append(name)
            continue

        if dry_run:
            created.append(name)
            continue
        try:
            client.create_field(
                field_name=name,
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
                property=spec.get("property"),
            )
            created.append(name)
        except Exception as exc:
            failed.append({"field": name, "error": str(exc)})

    if prune:
        prompt_migration = migrate_prompt_parts(client, dry_run=dry_run)
        failed.extend({"field": "短视频片段提示词", "error": item["error"]} for item in prompt_migration.get("failed", []))
        latest_fields = {field.field_name: field for field in client.list_fields()} if not dry_run else existing_fields
        for name, field in latest_fields.items():
            if name in wanted_names:
                continue
            if dry_run:
                deleted.append(name)
                continue
            try:
                delete_field(client, field.field_id, name)
                deleted.append(name)
            except Exception as exc:
                failed.append({"field": name, "error": str(exc)})

    return {
        "url": feishu_url,
        "app_token": client.app_token,
        "table_id": client.table_id,
        "created": created,
        "updated": updated,
        "deleted": deleted,
        "renamed": renamed,
        "prompt_migration": prompt_migration,
        "skipped": skipped,
        "type_mismatch": type_mismatch,
        "failed": failed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-url", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--prune", action="store_true", help="Delete fields that are not part of the lean human workbench.")
    args = parser.parse_args()
    result = ensure_fields(args.table_url, dry_run=args.dry_run, prune=args.prune)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
