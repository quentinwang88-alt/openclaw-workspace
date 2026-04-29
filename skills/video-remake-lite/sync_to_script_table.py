#!/usr/bin/env python3
"""把养号复刻完成结果同步到现有生产脚本表。

这个同步只把 video-remake-lite 作为前置生产表接入现有脚本池；
script_id 仍由现有脚本表字段规则推导，不单独创建新体系。
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
SYNC_SKILL_DIR = SKILL_DIR.parent / "script-run-manager-sync"
sys.path.insert(0, str(SYNC_SKILL_DIR))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402
from core.sync import _build_script_id  # type: ignore  # noqa: E402


DEFAULT_REMAKE_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "OzeJwBCzXit0mfkyylOcYioVnSd?table=tbliIkHtmm82qa5A&view=vewHPPmtMB"
)
DEFAULT_SCRIPT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "ZezEwZ7cKiUyeakdlI3cUuU1nRf?table=tblHRLMr9b3fvxBw&view=vewPpvR2oT"
)

STATUS_DONE = "已完成"
SYNC_PENDING = "待同步"
SYNC_IN_PROGRESS = "同步中"
SYNC_DONE = "已同步"

SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "status": ["复刻任务状态", "任务状态", "状态"],
    "sync_status": ["同步状态"],
    "synced_script_id": ["同步到脚本ID"],
    "sync_time": ["同步时间"],
    "sync_error": ["同步错误信息", "错误信息"],
    "task_no": ["任务编号", "task_id", "编号"],
    "store_id": ["店铺ID", "店铺", "店铺编号", "store_id"],
    "content_branch": ["内容分支"],
    "target_country": ["目标国家", "国家"],
    "target_language": ["目标语言", "语言"],
    "product_type": ["商品类型", "产品类型", "品类"],
    "final_storyboard": ["最终固定分镜", "最终复刻视频提示词", "final_execution_prompt"],
    "negative_words": ["负面限制词"],
}

TARGET_FIELD_ALIASES: Dict[str, List[str]] = {
    "task_no": ["任务编号", "任务ID", "任务序号", "编号"],
    "product_code": ["产品编码", "商品编码", "SKU", "Product Code"],
    "store_id": ["店铺ID", "店铺", "店铺编号", "store_id"],
    "script_s1": ["脚本方向一", "脚本_S1", "脚本S1"],
    "parent_slot_1": ["所属母版1"],
    "target_country": ["目标国家", "国家", "投放国家"],
    "target_language": ["目标语言", "语言"],
    "product_type": ["产品类型", "商品类型", "品类", "产品品类"],
    "script_source": ["脚本来源", "来源"],
    "publish_purpose": ["发布用途", "用途"],
    "cart_enabled": ["是否挂车", "挂车"],
    "content_branch": ["内容分支"],
    "final_storyboard": ["最终固定分镜"],
    "negative_words": ["负面限制词"],
    "script_status": ["脚本状态"],
    "source_remake_record_id": ["源复刻任务ID"],
    "sync_master_enabled": ["是否可同步母版"],
    "sync_enabled": ["是否可同步", "是否可同步脚本"],
}


def resolve_feishu_config(feishu_url: str) -> Tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return app_token, info.table_id


def resolve_field_mapping(field_names: Sequence[str], aliases: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    return {
        logical_name: next((candidate for candidate in candidates if candidate in field_names), None)
        for logical_name, candidates in aliases.items()
    }


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


def ensure_text_fields(client: FeishuBitableClient, field_names: List[str], required_names: Sequence[str]) -> List[str]:
    existing = set(field_names)
    changed = False
    for field_name in required_names:
        if field_name in existing:
            continue
        client.create_field(field_name, field_type=1, ui_type="Text")
        existing.add(field_name)
        changed = True
    return client.list_field_names() if changed else field_names


def extract_negative_words(final_storyboard: str) -> str:
    text = str(final_storyboard or "").strip()
    match = re.search(r"(?:^|\n)#+\s*三、负面限制词\s*\n(?P<body>[\s\S]+)$", text)
    if match:
        return match.group("body").strip()
    match = re.search(r"(?:^|\n)三、负面限制词\s*\n(?P<body>[\s\S]+)$", text)
    if match:
        return match.group("body").strip()
    return ""


def find_existing_script_id(
    target_records: Sequence[Any],
    target_mapping: Dict[str, Optional[str]],
    *,
    source_record_id: str,
    task_no: str,
) -> str:
    source_field = target_mapping.get("source_remake_record_id")
    source_name_field = target_mapping.get("script_source")
    product_code_field = target_mapping.get("product_code")
    task_no_field = target_mapping.get("task_no")
    script_id = _build_script_id(task_no, "YR1", None)
    for record in target_records:
        fields = record.fields
        if source_field and normalize_text(fields.get(source_field)) == source_record_id:
            return script_id
        if source_name_field and normalize_text(fields.get(source_name_field)) == "养号复刻":
            if product_code_field and normalize_text(fields.get(product_code_field)) == task_no:
                return script_id
            if task_no_field and normalize_text(fields.get(task_no_field)) == task_no:
                return script_id
    return ""


def build_target_fields(
    source_record: Any,
    source_mapping: Dict[str, Optional[str]],
    target_mapping: Dict[str, Optional[str]],
) -> Tuple[str, Dict[str, Any]]:
    fields = source_record.fields
    task_no = normalize_text(fields.get(source_mapping.get("task_no"))) or f"YR{source_record.record_id[-6:]}"
    final_storyboard = normalize_text(fields.get(source_mapping.get("final_storyboard")))
    negative_words = normalize_text(fields.get(source_mapping.get("negative_words"))) or extract_negative_words(final_storyboard)
    content_branch = normalize_text(fields.get(source_mapping.get("content_branch"))) or "商品展示型"
    script_id = _build_script_id(task_no, "YR1", None)

    target_fields: Dict[str, Any] = {}
    values = {
        "task_no": task_no,
        "product_code": task_no,
        "store_id": normalize_text(fields.get(source_mapping.get("store_id"))),
        "script_s1": final_storyboard,
        "parent_slot_1": "YR1",
        "target_country": normalize_text(fields.get(source_mapping.get("target_country"))),
        "target_language": normalize_text(fields.get(source_mapping.get("target_language"))),
        "product_type": normalize_text(fields.get(source_mapping.get("product_type"))),
        "script_source": "养号复刻",
        "publish_purpose": "养号",
        "cart_enabled": "否",
        "content_branch": content_branch,
        "final_storyboard": final_storyboard,
        "negative_words": negative_words,
        "script_status": "待生成",
        "source_remake_record_id": source_record.record_id,
        "sync_master_enabled": True,
        "sync_enabled": True,
    }
    for logical_name, value in values.items():
        field_name = target_mapping.get(logical_name)
        if field_name and value not in (None, ""):
            target_fields[field_name] = value
    return script_id, target_fields


def build_source_synced_fields(
    source_mapping: Dict[str, Optional[str]],
    script_id: str,
) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        source_mapping["sync_status"]: SYNC_DONE,
        source_mapping["synced_script_id"]: script_id,
    }
    if source_mapping.get("sync_time"):
        fields[source_mapping["sync_time"]] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return fields


def sync_records(args: argparse.Namespace) -> Dict[str, int]:
    source_app_token, source_table_id = resolve_feishu_config(args.remake_feishu_url)
    target_app_token, target_table_id = resolve_feishu_config(args.script_feishu_url)
    source_client = FeishuBitableClient(source_app_token, source_table_id)
    target_client = FeishuBitableClient(target_app_token, target_table_id)

    source_fields = ensure_text_fields(
        source_client,
        source_client.list_field_names(),
        ["同步状态", "同步到脚本ID", "同步时间"],
    )
    target_fields = ensure_text_fields(
        target_client,
        target_client.list_field_names(),
        ["脚本来源", "发布用途", "是否挂车", "内容分支", "最终固定分镜", "负面限制词", "源复刻任务ID", "脚本状态"],
    )
    source_mapping = resolve_field_mapping(source_fields, SOURCE_FIELD_ALIASES)
    target_mapping = resolve_field_mapping(target_fields, TARGET_FIELD_ALIASES)

    for required in ("status", "sync_status", "synced_script_id", "final_storyboard"):
        if not source_mapping.get(required):
            raise RuntimeError(f"养号复刻表缺少字段: {required}")
    if not target_mapping.get("script_s1"):
        raise RuntimeError("现有脚本表缺少脚本字段：脚本方向一 / 脚本_S1 / 脚本S1")

    source_records = source_client.list_records(page_size=100, limit=args.limit)
    target_records = target_client.list_records(page_size=100)
    stats = {"scanned": len(source_records), "created": 0, "skipped": 0, "failed": 0}

    for record in source_records:
        fields = record.fields
        status = normalize_text(fields.get(source_mapping.get("status")))
        sync_status = normalize_text(fields.get(source_mapping.get("sync_status")))
        synced_script_id = normalize_text(fields.get(source_mapping.get("synced_script_id")))
        final_storyboard = normalize_text(fields.get(source_mapping.get("final_storyboard")))
        if status != STATUS_DONE or sync_status != SYNC_PENDING or not final_storyboard or synced_script_id:
            stats["skipped"] += 1
            continue

        script_id, target_payload = build_target_fields(record, source_mapping, target_mapping)
        task_no = normalize_text(record.fields.get(source_mapping.get("task_no"))) or f"YR{record.record_id[-6:]}"
        duplicated_script_id = find_existing_script_id(
            target_records,
            target_mapping,
            source_record_id=record.record_id,
            task_no=task_no,
        )
        if duplicated_script_id:
            source_client.update_record_fields(
                record.record_id,
                build_source_synced_fields(source_mapping, duplicated_script_id),
            )
            stats["skipped"] += 1
            continue

        if args.dry_run:
            print({"record_id": record.record_id, "script_id": script_id, "fields": target_payload})
            stats["created"] += 1
            continue

        try:
            source_client.update_record_fields(record.record_id, {source_mapping["sync_status"]: SYNC_IN_PROGRESS})
            target_client.batch_create_records([{"fields": target_payload}])
            source_client.update_record_fields(
                record.record_id,
                build_source_synced_fields(source_mapping, script_id),
            )
            stats["created"] += 1
        except Exception as exc:
            stats["failed"] += 1
            failure_fields = {source_mapping["sync_status"]: f"同步失败：{exc}"}
            if source_mapping.get("sync_error"):
                failure_fields[source_mapping["sync_error"]] = str(exc)[:1000]
            source_client.update_record_fields(record.record_id, failure_fields)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="养号复刻表 -> 现有生产脚本表 同步")
    parser.add_argument("--remake-feishu-url", default=DEFAULT_REMAKE_FEISHU_URL, help="养号复刻脚本表 URL")
    parser.add_argument("--script-feishu-url", default=DEFAULT_SCRIPT_FEISHU_URL, help="现有生产脚本表 URL")
    parser.add_argument("--limit", type=int, help="限制扫描记录数")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不落表")
    args = parser.parse_args()
    print(sync_records(args))


if __name__ == "__main__":
    main()
