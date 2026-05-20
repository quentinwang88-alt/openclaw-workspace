#!/usr/bin/env python3
"""Refresh remake-flow product images from one original script-table task."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

WORKSPACE_DIR = Path(__file__).resolve().parents[3]
SYNC_SKILL_DIR = WORKSPACE_DIR / "skills" / "script-run-manager-sync"
VIDEO_REMAKE_DIR = WORKSPACE_DIR / "skills" / "video-remake-lite"
sys.path.insert(0, str(SYNC_SKILL_DIR))
sys.path.insert(0, str(VIDEO_REMAKE_DIR))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402
from sync_to_script_table import DEFAULT_SCRIPT_FEISHU_URL  # type: ignore  # noqa: E402


DEFAULT_FINAL_PROMPT_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "W2NhbdB2Eafp55sjXjMcoCLpnxc?table=tblalZ9WBwXyILkt&view=vewo4XqxdM"
)
DEFAULT_PRODUCT_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "Sl95b3FqLaNIp8slR47c7GzxnMb?table=tblbFOq4V4mqfZkW&view=vewSoOE9Mk"
)
DEFAULT_PRODUCT_IMAGE_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "DfPfbMxVXaYH7XscidMcYF6pnvg?table=tbluowLhwKya557l&view=vewHhNHfGE"
)

ID_FIELD_NAMES = ["产品ID", "产品编码", "商品编码", "SKU", "Product Code", "product_id"]
TASK_FIELD_NAMES = ["任务编号", "任务ID", "任务序号", "编号"]

TARGET_DEFS = {
    "final-prompt": {"url_arg": "final_prompt_url", "field": "产品图片"},
    "product": {"url_arg": "product_url", "field": "产品主图"},
    "product-image": {"url_arg": "product_image_url", "field": "图片"},
}


def resolve_feishu_config(feishu_url: str) -> Tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return app_token, info.table_id


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " / ".join(part for part in (normalize_text(item) for item in value) if part)
    if isinstance(value, dict):
        for key in ("text", "name", "url", "link"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
    return str(value).strip()


def extract_attachments(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    return []


def attachment_names(attachments: List[Dict[str, Any]]) -> List[str]:
    return [normalize_text(item.get("name")) or normalize_text(item.get("file_token")) for item in attachments]


def field_exact_match(fields: Dict[str, Any], candidates: Iterable[str], expected: str) -> bool:
    return any(normalize_text(fields.get(name)) == expected for name in candidates if name in fields)


def find_source_record(
    client: FeishuBitableClient,
    product_id: str,
    task_no: str,
    source_image_field: str,
) -> Tuple[Any, List[Dict[str, Any]]]:
    matches = []
    for record in client.list_records(page_size=100):
        fields = record.fields
        if field_exact_match(fields, ID_FIELD_NAMES, product_id) and field_exact_match(fields, TASK_FIELD_NAMES, task_no):
            matches.append(record)
    if len(matches) != 1:
        raise RuntimeError(f"原始脚本表必须唯一命中 产品ID={product_id} 任务编号={task_no}，实际命中 {len(matches)} 条")

    attachments = extract_attachments(matches[0].fields.get(source_image_field))
    if not attachments:
        raise RuntimeError(f"源记录 {matches[0].record_id} 的字段 {source_image_field} 没有附件图片")
    return matches[0], attachments


def find_target_records(client: FeishuBitableClient, product_id: str) -> List[Any]:
    return [
        record
        for record in client.list_records(page_size=100)
        if field_exact_match(record.fields, ID_FIELD_NAMES, product_id)
    ]


def upload_attachments(
    source_client: FeishuBitableClient,
    target_client: FeishuBitableClient,
    source_attachments: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    uploaded: List[Dict[str, Any]] = []
    for attachment in source_attachments:
        content, file_name, content_type, size = source_client.download_attachment_bytes(attachment)
        uploaded.append(
            target_client.upload_attachment(
                content=content,
                file_name=file_name,
                content_type=content_type,
                size=size,
            )
        )
    return uploaded


def parse_targets(raw: str) -> List[str]:
    targets = [item.strip() for item in raw.split(",") if item.strip()]
    if not targets or targets == ["all"]:
        return list(TARGET_DEFS)
    unknown = [target for target in targets if target not in TARGET_DEFS]
    if unknown:
        raise RuntimeError(f"未知 targets: {', '.join(unknown)}；可选值: all, {', '.join(TARGET_DEFS)}")
    return targets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="从原始脚本表指定任务刷新复刻流程产品图片")
    parser.add_argument("--product-id", required=True, help="产品ID / 产品编码")
    parser.add_argument("--task-no", required=True, help="原始脚本表任务编号，例如 118")
    parser.add_argument("--apply", action="store_true", help="实际更新飞书；默认只 dry-run")
    parser.add_argument("--targets", default="all", help="all / final-prompt / product / product-image，可逗号分隔")
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_FEISHU_URL, help="原始脚本管理表 URL")
    parser.add_argument("--final-prompt-url", default=DEFAULT_FINAL_PROMPT_URL, help="最终提示词表 URL")
    parser.add_argument("--product-url", default=DEFAULT_PRODUCT_URL, help="产品表 URL")
    parser.add_argument("--product-image-url", default=DEFAULT_PRODUCT_IMAGE_URL, help="产品图片表 URL")
    parser.add_argument("--source-image-field", default="产品图片", help="原始脚本表图片字段")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    targets = parse_targets(args.targets)

    source_app_token, source_table_id = resolve_feishu_config(args.script_url)
    source_client = FeishuBitableClient(source_app_token, source_table_id)
    source_record, source_attachments = find_source_record(
        source_client,
        args.product_id,
        args.task_no,
        args.source_image_field,
    )

    print(
        json.dumps(
            {
                "apply": args.apply,
                "product_id": args.product_id,
                "task_no": args.task_no,
                "source_record_id": source_record.record_id,
                "source_image_field": args.source_image_field,
                "source_image_count": len(source_attachments),
                "source_image_names": attachment_names(source_attachments),
                "targets": targets,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    summary: Dict[str, Dict[str, Any]] = {}
    for target_name in targets:
        target_def = TARGET_DEFS[target_name]
        target_url = getattr(args, target_def["url_arg"])
        target_field = target_def["field"]
        app_token, table_id = resolve_feishu_config(target_url)
        target_client = FeishuBitableClient(app_token, table_id)
        records = find_target_records(target_client, args.product_id)

        summary[target_name] = {"field": target_field, "matches": len(records), "updated": 0}
        print(json.dumps({"target": target_name, "field": target_field, "matches": len(records)}, ensure_ascii=False), flush=True)
        if not args.apply or not records:
            continue

        uploaded = upload_attachments(source_client, target_client, source_attachments)
        for record in records:
            target_client.update_record_fields(record.record_id, {target_field: uploaded})
            summary[target_name]["updated"] += 1
        summary[target_name]["uploaded_image_count"] = len(uploaded)
        summary[target_name]["uploaded_image_names"] = attachment_names(uploaded)

    print(json.dumps({"summary": summary}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
