#!/usr/bin/env python3
"""把短视频复刻完成结果同步到现有生产脚本表。

这个同步只把 video-remake-lite 作为前置生产表接入现有脚本池；
script_id 忽略源表已有值，始终按现有脚本表字段规则重新推导。
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


DEFAULT_REMAKE_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "W2NhbdB2Eafp55sjXjMcoCLpnxc?table=tblalZ9WBwXyILkt&view=vewo4XqxdM"
)
DEFAULT_NURTURE_REMAKE_FEISHU_URL = (
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
SCRIPT_TYPE_SHORT_VIDEO_REMAKE = "短视频复刻"
SCRIPT_TYPE_NURTURE_REMAKE = "养号复刻"
PUBLISH_PURPOSE_NURTURE = "养号"
PROFILE_SHORT_VIDEO = "short-video"
PROFILE_NURTURE = "nurture"


def _build_script_id(task_no: str, parent_slot: str, variant_no: Optional[int]) -> str:
    suffix = "M" if variant_no is None else f"V{variant_no}"
    return f"{task_no}_{parent_slot}_{suffix}".replace(" ", "")

SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "status": ["复刻任务状态", "任务状态", "状态"],
    "sync_status": ["同步状态"],
    "synced_script_id": ["同步到脚本ID", "脚本ID", "script_id"],
    "sync_time": ["同步时间"],
    "sync_error": ["同步错误信息", "错误信息"],
    "task_no": ["任务编号", "产品ID", "产品编码", "task_id", "编号"],
    "store_id": ["店铺ID", "店铺", "店铺编号", "store_id"],
    "product_id": ["产品ID", "产品编码", "商品编码", "SKU", "Product Code", "product_id"],
    "product_images": ["产品图片", "商品图片", "图片", "参考图"],
    "content_branch": ["内容分支"],
    "target_country": ["目标国家", "国家"],
    "target_language": ["目标语言", "语言"],
    "product_type": ["商品类型", "产品类型", "品类"],
    "final_storyboard": ["最终视频提示词", "最终短视频提示词", "最终复刻视频提示词", "最终固定分镜", "final_execution_prompt"],
    "negative_words": ["负面限制词"],
    "video_duration": ["视频时长", "短视频时长", "时长", "视频秒数", "duration", "video_duration"],
}

TARGET_FIELD_ALIASES: Dict[str, List[str]] = {
    "task_no": ["任务编号", "任务ID", "任务序号", "编号"],
    "product_code": ["产品编码", "商品编码", "SKU", "Product Code"],
    "product_id": ["产品ID"],
    "product_images": ["产品图片", "商品图片", "图片"],
    "store_id": ["店铺ID", "店铺", "店铺编号", "store_id"],
    "script_s1": ["脚本方向一", "脚本_S1", "脚本S1"],
    "parent_slot_1": ["所属母版1"],
    "target_country": ["目标国家", "国家", "投放国家"],
    "target_language": ["目标语言", "语言"],
    "product_type": ["产品类型", "商品类型", "品类", "产品品类"],
    "script_source": ["脚本来源", "来源"],
    "script_type": ["脚本类型", "类型"],
    "publish_purpose": ["发布用途", "用途"],
    "cart_enabled": ["是否挂车", "挂车"],
    "content_branch": ["内容分支"],
    "final_storyboard": ["最终视频提示词", "最终短视频提示词", "最终复刻视频提示词", "最终固定分镜"],
    "negative_words": ["负面限制词"],
    "video_duration": ["视频时长", "短视频时长", "时长", "视频秒数", "duration", "video_duration"],
    "script_status": ["脚本状态"],
    "task_status": ["任务状态", "状态"],
    "source_remake_record_id": ["源复刻任务ID"],
    "sync_master_enabled": ["是否可同步母版", "同步母版"],
    "sync_enabled": ["是否可同步", "是否可同步脚本"],
}

SCRIPT_ID_HEADER_PATTERN = re.compile(r"\A\s*【脚本ID】\s*\n-\s*[^\n\r]+(?:\r?\n){0,2}")
SCRIPT_ID_LOOKUP_PATTERN = re.compile(r"【脚本ID】\s*[-:\n\r ]+\s*([A-Za-z0-9_]+)")


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


def remove_script_id_header(prompt_text: Any) -> str:
    text = normalize_text(prompt_text)
    if not text:
        return ""
    return SCRIPT_ID_HEADER_PATTERN.sub("", text, count=1).lstrip()


def prepend_script_id_header(prompt_text: Any, script_id: str) -> str:
    body = remove_script_id_header(prompt_text)
    script_id_text = normalize_text(script_id)
    if not script_id_text:
        return body
    return f"【脚本ID】\n- {script_id_text}\n\n{body}".rstrip()


def extract_script_id_from_prompt(prompt_text: Any) -> str:
    text = normalize_text(prompt_text)
    if not text:
        return ""
    match = SCRIPT_ID_LOOKUP_PATTERN.search(text)
    return match.group(1).strip() if match else ""


def build_parent_slot(source_record_id: str) -> str:
    stable_piece = re.sub(r"[^A-Za-z0-9-]", "", str(source_record_id or ""))[-6:]
    return f"VR{stable_piece or '1'}"


def extract_attachments(raw_value: Any) -> List[Dict[str, Any]]:
    attachments: List[Dict[str, Any]] = []
    if isinstance(raw_value, list):
        for item in raw_value:
            if isinstance(item, dict) and item.get("file_token"):
                attachments.append(item)
    elif isinstance(raw_value, dict) and raw_value.get("file_token"):
        attachments.append(raw_value)
    return attachments


def ensure_text_fields(
    client: FeishuBitableClient,
    field_names: List[str],
    required_names: Sequence[str],
    *,
    allow_create: bool = True,
) -> List[str]:
    existing = set(field_names)
    changed = False
    for field_name in required_names:
        if field_name in existing:
            continue
        if not allow_create:
            continue
        client.create_field(field_name, field_type=1, ui_type="Text")
        existing.add(field_name)
        changed = True
    return client.list_field_names() if changed else field_names


def ensure_field_specs(
    client: FeishuBitableClient,
    field_names: List[str],
    specs: Sequence[Tuple[str, int, str]],
    *,
    allow_create: bool = True,
) -> List[str]:
    existing = set(field_names)
    changed = False
    for field_name, field_type, ui_type in specs:
        if field_name in existing:
            continue
        if not allow_create:
            continue
        client.create_field(field_name, field_type=field_type, ui_type=ui_type)
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


def normalize_video_duration(value: Any, default_seconds: int = 15) -> int:
    text = normalize_text(value)
    if not text:
        return default_seconds
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return default_seconds
    try:
        duration = float(match.group(0))
    except ValueError:
        return default_seconds
    if duration <= 0:
        return default_seconds
    return int(round(duration))


def build_script_id_for_profile(profile: str, task_no: str, source_record_id: str) -> str:
    parent_slot = "YR1" if profile == PROFILE_NURTURE else build_parent_slot(source_record_id)
    return _build_script_id(task_no, parent_slot, None)


def find_existing_script_id(
    target_records: Sequence[Any],
    target_mapping: Dict[str, Optional[str]],
    *,
    source_record_id: str,
    task_no: str,
    profile: str,
) -> str:
    source_field = target_mapping.get("source_remake_record_id")
    script_fields = [
        target_mapping.get("script_s1"),
        target_mapping.get("final_storyboard"),
    ]
    script_id = build_script_id_for_profile(profile, task_no, source_record_id)
    for record in target_records:
        fields = record.fields
        if source_field and normalize_text(fields.get(source_field)) == source_record_id:
            return script_id
        for field_name in script_fields:
            if field_name and extract_script_id_from_prompt(fields.get(field_name)) == script_id:
                return script_id
    return ""


def build_target_fields(
    source_record: Any,
    source_mapping: Dict[str, Optional[str]],
    target_mapping: Dict[str, Optional[str]],
    profile: str = PROFILE_SHORT_VIDEO,
) -> Tuple[str, Dict[str, Any]]:
    fields = source_record.fields
    product_id = normalize_text(fields.get(source_mapping.get("product_id")))
    task_no = product_id or normalize_text(fields.get(source_mapping.get("task_no"))) or f"VR{source_record.record_id[-6:]}"
    final_storyboard = normalize_text(fields.get(source_mapping.get("final_storyboard")))
    negative_words = normalize_text(fields.get(source_mapping.get("negative_words"))) or extract_negative_words(final_storyboard)
    video_duration = normalize_video_duration(fields.get(source_mapping.get("video_duration")))
    content_branch = normalize_text(fields.get(source_mapping.get("content_branch")))
    parent_slot = "YR1" if profile == PROFILE_NURTURE else build_parent_slot(source_record.record_id)
    script_id = build_script_id_for_profile(profile, task_no, source_record.record_id)
    final_prompt_with_script_id = (
        final_storyboard if profile == PROFILE_NURTURE else prepend_script_id_header(final_storyboard, script_id)
    )
    script_source = SCRIPT_TYPE_NURTURE_REMAKE if profile == PROFILE_NURTURE else SCRIPT_TYPE_SHORT_VIDEO_REMAKE
    publish_purpose = PUBLISH_PURPOSE_NURTURE if profile == PROFILE_NURTURE else SCRIPT_TYPE_SHORT_VIDEO_REMAKE
    sync_master_enabled = profile == PROFILE_NURTURE
    sync_enabled = profile == PROFILE_NURTURE

    target_fields: Dict[str, Any] = {}
    values = {
        "task_no": task_no,
        "product_code": task_no,
        "product_id": product_id or task_no,
        "product_images": extract_attachments(fields.get(source_mapping.get("product_images"))),
        "store_id": normalize_text(fields.get(source_mapping.get("store_id"))),
        "script_s1": final_prompt_with_script_id,
        "parent_slot_1": parent_slot,
        "target_country": normalize_text(fields.get(source_mapping.get("target_country"))),
        "target_language": normalize_text(fields.get(source_mapping.get("target_language"))),
        "product_type": normalize_text(fields.get(source_mapping.get("product_type"))),
        "script_source": script_source,
        "script_type": script_source,
        "publish_purpose": publish_purpose,
        "cart_enabled": "否" if profile == PROFILE_NURTURE else "",
        "content_branch": content_branch,
        "final_storyboard": final_prompt_with_script_id,
        "negative_words": negative_words,
        "video_duration": video_duration,
        "script_status": "待人工确认",
        "task_status": STATUS_DONE,
        "source_remake_record_id": source_record.record_id,
        "sync_master_enabled": sync_master_enabled,
        "sync_enabled": sync_enabled,
    }
    for logical_name, value in values.items():
        field_name = target_mapping.get(logical_name)
        if field_name and value not in (None, ""):
            target_fields[field_name] = value
    return script_id, target_fields


def build_source_synced_fields(
    source_mapping: Dict[str, Optional[str]],
    script_id: str,
    final_storyboard: str = "",
) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        source_mapping["sync_status"]: SYNC_DONE,
        source_mapping["synced_script_id"]: script_id,
    }
    if final_storyboard and source_mapping.get("final_storyboard"):
        fields[source_mapping["final_storyboard"]] = prepend_script_id_header(final_storyboard, script_id)
    if source_mapping.get("sync_time"):
        fields[source_mapping["sync_time"]] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return fields


def should_process_source_record(
    fields: Dict[str, Any],
    source_mapping: Dict[str, Optional[str]],
) -> bool:
    status_field = source_mapping.get("status")
    status = normalize_text(fields.get(status_field)) if status_field else ""
    sync_status = normalize_text(fields.get(source_mapping.get("sync_status")))
    final_storyboard = normalize_text(fields.get(source_mapping.get("final_storyboard")))
    return (
        (not status_field or status == STATUS_DONE)
        and bool(final_storyboard)
        and sync_status != SYNC_DONE
        and not sync_status.startswith("同步失败")
    )


def transfer_attachments(
    source_client: FeishuBitableClient,
    target_client: FeishuBitableClient,
    attachments: List[Dict[str, Any]],
    cache: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    transferred: List[Dict[str, Any]] = []
    for attachment in attachments:
        source_file_token = str(attachment.get("file_token", "")).strip()
        if not source_file_token:
            continue
        if source_file_token in cache:
            transferred.append(dict(cache[source_file_token]))
            continue
        content, file_name, content_type, size = source_client.download_attachment_bytes(attachment)
        uploaded = target_client.upload_attachment(
            content=content,
            file_name=file_name,
            content_type=content_type,
            size=size,
        )
        cache[source_file_token] = uploaded
        transferred.append(dict(uploaded))
    return transferred


def is_short_video_remake_target_record(
    fields: Dict[str, Any],
    target_mapping: Dict[str, Optional[str]],
) -> bool:
    source_value = normalize_text(fields.get(target_mapping.get("script_source")))
    type_value = normalize_text(fields.get(target_mapping.get("script_type")))
    source_record_id = normalize_text(fields.get(target_mapping.get("source_remake_record_id")))
    purpose_value = normalize_text(fields.get(target_mapping.get("publish_purpose")))
    content_branch = normalize_text(fields.get(target_mapping.get("content_branch")))
    if (
        source_value == SCRIPT_TYPE_NURTURE_REMAKE
        or type_value == SCRIPT_TYPE_NURTURE_REMAKE
        or purpose_value == PUBLISH_PURPOSE_NURTURE
        or content_branch == "非商品展示型"
    ):
        return False
    return (
        source_value == SCRIPT_TYPE_SHORT_VIDEO_REMAKE
        or type_value == SCRIPT_TYPE_SHORT_VIDEO_REMAKE
        or bool(source_record_id)
    )


def is_nurture_remake_target_record(
    fields: Dict[str, Any],
    target_mapping: Dict[str, Optional[str]],
) -> bool:
    source_value = normalize_text(fields.get(target_mapping.get("script_source")))
    type_value = normalize_text(fields.get(target_mapping.get("script_type")))
    purpose_value = normalize_text(fields.get(target_mapping.get("publish_purpose")))
    content_branch = normalize_text(fields.get(target_mapping.get("content_branch")))
    return (
        source_value == SCRIPT_TYPE_NURTURE_REMAKE
        or type_value == SCRIPT_TYPE_NURTURE_REMAKE
        or purpose_value == PUBLISH_PURPOSE_NURTURE
        or content_branch == "非商品展示型"
    )


def should_repair_target_record(
    fields: Dict[str, Any],
    target_mapping: Dict[str, Optional[str]],
    include_kinds: Sequence[str],
) -> bool:
    kind_set = set(include_kinds)
    return (
        ("short-video" in kind_set and is_short_video_remake_target_record(fields, target_mapping))
        or ("nurture" in kind_set and is_nurture_remake_target_record(fields, target_mapping))
    )


def repair_target_task_status(args: argparse.Namespace) -> Dict[str, int]:
    target_app_token, target_table_id = resolve_feishu_config(args.script_feishu_url)
    target_client = FeishuBitableClient(target_app_token, target_table_id)
    target_fields = target_client.list_field_names()
    target_mapping = resolve_field_mapping(target_fields, TARGET_FIELD_ALIASES)
    if not target_mapping.get("task_status"):
        raise RuntimeError("现有脚本表缺少任务状态/状态字段，无法修复")

    records = target_client.list_records(page_size=100)
    status_counts: Counter[str] = Counter()
    stats: Dict[str, Any] = {"scanned": len(records), "matched": 0, "updated": 0, "skipped": 0}
    task_status_field = target_mapping["task_status"]
    include_kinds = args.repair_target_kind or ["short-video"]
    for record in records:
        if not should_repair_target_record(record.fields, target_mapping, include_kinds):
            stats["skipped"] += 1
            continue
        stats["matched"] += 1
        current_status = normalize_text(record.fields.get(task_status_field))
        status_counts[current_status or "<空>"] += 1
        if current_status == STATUS_DONE:
            stats["skipped"] += 1
            continue
        repairable_statuses = {"", "待开始", "待执行-全流程", "待执行-重跑脚本", "待执行-重跑全流程"}
        if PROFILE_NURTURE in include_kinds:
            repairable_statuses.update({"任务失败", "失败-输入不完整"})
        if current_status not in repairable_statuses:
            stats["skipped"] += 1
            continue
        if args.dry_run:
            print({"record_id": record.record_id, "from": current_status, "to": STATUS_DONE})
            stats["updated"] += 1
            continue
        target_client.update_record_fields(record.record_id, {task_status_field: STATUS_DONE})
        stats["updated"] += 1
        print(f"✅ 修复任务状态: record_id={record.record_id} {current_status or '<空>'} -> {STATUS_DONE}", flush=True)
    stats["status_counts"] = dict(status_counts)
    return stats


def sync_records(args: argparse.Namespace) -> Dict[str, int]:
    profile = args.sync_profile
    remake_feishu_url = args.remake_feishu_url or (
        DEFAULT_NURTURE_REMAKE_FEISHU_URL if profile == PROFILE_NURTURE else DEFAULT_REMAKE_FEISHU_URL
    )
    source_app_token, source_table_id = resolve_feishu_config(remake_feishu_url)
    target_app_token, target_table_id = resolve_feishu_config(args.script_feishu_url)
    source_client = FeishuBitableClient(source_app_token, source_table_id)
    target_client = FeishuBitableClient(target_app_token, target_table_id)

    source_fields = ensure_text_fields(
        source_client,
        source_client.list_field_names(),
        ["同步状态", "脚本ID", "同步时间", "同步错误信息"],
        allow_create=not args.dry_run,
    )
    target_fields = ensure_field_specs(
        target_client,
        target_client.list_field_names(),
        [
            ("产品ID", 1, "Text"),
            ("产品图片", 17, "Attachment"),
            ("脚本来源", 1, "Text"),
            ("脚本类型", 1, "Text"),
            ("发布用途", 1, "Text"),
            ("内容分支", 1, "Text"),
            ("最终视频提示词", 1, "Text"),
            ("负面限制词", 1, "Text"),
            ("视频时长", 2, "Number"),
            ("源复刻任务ID", 1, "Text"),
            ("脚本状态", 1, "Text"),
        ],
        allow_create=not args.dry_run,
    )
    source_mapping = resolve_field_mapping(source_fields, SOURCE_FIELD_ALIASES)
    target_mapping = resolve_field_mapping(target_fields, TARGET_FIELD_ALIASES)

    source_required_fields = ["final_storyboard"]
    if profile == PROFILE_SHORT_VIDEO:
        source_required_fields.extend(["product_id", "product_images"])
    if not args.dry_run:
        source_required_fields.extend(["sync_status", "synced_script_id"])
    for required in source_required_fields:
        if not source_mapping.get(required):
            raise RuntimeError(f"短视频复刻表缺少字段: {required}")
    if not args.dry_run:
        for required in ("product_id", "product_images"):
            if not target_mapping.get(required):
                raise RuntimeError(f"现有脚本表缺少字段: {required}")
    if not target_mapping.get("script_s1"):
        raise RuntimeError("现有脚本表缺少脚本字段：脚本方向一 / 脚本_S1 / 脚本S1")

    source_records = source_client.list_records(page_size=100, limit=args.limit)
    target_records = target_client.list_records(page_size=100)
    stats = {"scanned": len(source_records), "created": 0, "skipped": 0, "failed": 0}
    image_cache: Dict[str, Dict[str, Any]] = {}

    for record in source_records:
        fields = record.fields
        final_storyboard = normalize_text(fields.get(source_mapping.get("final_storyboard")))
        if not should_process_source_record(fields, source_mapping):
            stats["skipped"] += 1
            continue

        script_id, target_payload = build_target_fields(record, source_mapping, target_mapping, profile=profile)
        task_no = (
            normalize_text(record.fields.get(source_mapping.get("product_id")))
            or normalize_text(record.fields.get(source_mapping.get("task_no")))
            or f"VR{record.record_id[-6:]}"
        )
        duplicated_script_id = find_existing_script_id(
            target_records,
            target_mapping,
            source_record_id=record.record_id,
            task_no=task_no,
            profile=profile,
        )
        if duplicated_script_id:
            if args.dry_run:
                if not getattr(args, "summary_only", False):
                    print({"record_id": record.record_id, "script_id": duplicated_script_id, "action": "mark_existing"})
            else:
                print(f"🔁 已存在，回写源表: record_id={record.record_id} script_id={duplicated_script_id}", flush=True)
                source_client.update_record_fields(
                    record.record_id,
                    build_source_synced_fields(
                        source_mapping,
                        duplicated_script_id,
                        final_storyboard if profile == PROFILE_SHORT_VIDEO else "",
                    ),
                )
            stats["skipped"] += 1
            continue

        if args.dry_run:
            if not getattr(args, "summary_only", False):
                print({"record_id": record.record_id, "script_id": script_id, "fields": target_payload})
            stats["created"] += 1
            continue

        try:
            print(f"➡️ 同步记录: record_id={record.record_id} script_id={script_id}", flush=True)
            source_client.update_record_fields(record.record_id, {source_mapping["sync_status"]: SYNC_IN_PROGRESS})
            product_images_field = target_mapping.get("product_images")
            if product_images_field:
                print(f"   上传产品图片: {len(target_payload.get(product_images_field, []))} 张", flush=True)
                target_payload[product_images_field] = transfer_attachments(
                    source_client,
                    target_client,
                    target_payload.get(product_images_field, []),
                    image_cache,
                )
            target_client.batch_create_records([{"fields": target_payload}])
            source_client.update_record_fields(
                record.record_id,
                build_source_synced_fields(
                    source_mapping,
                    script_id,
                    final_storyboard if profile == PROFILE_SHORT_VIDEO else "",
                ),
            )
            stats["created"] += 1
            print(f"✅ 同步完成: record_id={record.record_id}", flush=True)
        except Exception as exc:
            stats["failed"] += 1
            print(f"❌ 同步失败: record_id={record.record_id} error={exc}", flush=True)
            failure_fields = {source_mapping["sync_status"]: f"同步失败：{exc}"}
            if source_mapping.get("sync_error"):
                failure_fields[source_mapping["sync_error"]] = str(exc)[:1000]
            source_client.update_record_fields(record.record_id, failure_fields)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="短视频复刻表 -> 现有生产脚本表 同步")
    parser.add_argument(
        "--sync-profile",
        choices=[PROFILE_SHORT_VIDEO, PROFILE_NURTURE],
        default=PROFILE_SHORT_VIDEO,
        help="同步配置：short-video=短视频复刻最终提示词表，nurture=养号复刻表",
    )
    parser.add_argument("--mode", choices=["manual", "scheduled"], default="manual", help="触发模式")
    parser.add_argument("--remake-feishu-url", help="覆盖源表飞书 URL")
    parser.add_argument("--script-feishu-url", default=DEFAULT_SCRIPT_FEISHU_URL, help="现有生产脚本表 URL")
    parser.add_argument("--limit", type=int, help="限制扫描记录数")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不落表")
    parser.add_argument("--summary-only", action="store_true", help="dry-run 时只输出统计，不打印完整字段")
    parser.add_argument("--repair-target-status-only", action="store_true", help="只修复原始脚本表中短视频复刻记录的任务状态")
    parser.add_argument(
        "--repair-target-kind",
        action="append",
        choices=["short-video", "nurture"],
        help="修复目标记录类型；可重复传入。默认 short-video",
    )
    args = parser.parse_args()
    if args.repair_target_status_only:
        print(repair_target_task_status(args))
    else:
        print(sync_records(args))


if __name__ == "__main__":
    main()
