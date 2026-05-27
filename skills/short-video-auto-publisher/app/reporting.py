#!/usr/bin/env python3
"""发布追踪表回写。"""

from __future__ import annotations

import mimetypes
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from app.db import AutoPublishDB


REPORT_FIELDS: Sequence[Dict[str, Any]] = (
    {"name": "内部脚本键", "type": 1, "ui_type": "Text"},
    {"name": "脚本ID", "type": 1, "ui_type": "Text"},
    {"name": "源脚本记录ID", "type": 1, "ui_type": "Text"},
    {"name": "脚本槽位", "type": 1, "ui_type": "Text"},
    {"name": "任务编号", "type": 1, "ui_type": "Text"},
    {"name": "店铺ID", "type": 1, "ui_type": "Text"},
    {"name": "产品ID", "type": 1, "ui_type": "Text"},
    {"name": "内容家族", "type": 1, "ui_type": "Text"},
    {"name": "短视频标题", "type": 1, "ui_type": "Text"},
    {"name": "运行表记录ID", "type": 1, "ui_type": "Text"},
    {"name": "视频来源类型", "type": 1, "ui_type": "Text"},
    {"name": "视频来源值", "type": 1, "ui_type": "Text"},
    {"name": "本地文件路径", "type": 1, "ui_type": "Text"},
    {"name": "下载状态", "type": 1, "ui_type": "Text"},
    {"name": "跑视频状态", "type": 1, "ui_type": "Text"},
    {"name": "发布状态", "type": 1, "ui_type": "Text"},
    {"name": "排期状态", "type": 1, "ui_type": "Text"},
    {"name": "分配账号ID", "type": 1, "ui_type": "Text"},
    {"name": "分配账号名称", "type": 1, "ui_type": "Text"},
    {"name": "计划发布时间", "type": 1, "ui_type": "Text"},
    {"name": "发布时间", "type": 1, "ui_type": "Text"},
    {"name": "发布任务ID", "type": 1, "ui_type": "Text"},
    {"name": "发布结果", "type": 1, "ui_type": "Text"},
    {"name": "错误信息", "type": 1, "ui_type": "Text"},
    {"name": "最后更新时间", "type": 1, "ui_type": "Text"},
)

MANUAL_QUEUE_FIELDS: Sequence[Dict[str, Any]] = (
    {"name": "清单唯一键", "type": 1, "ui_type": "Text"},
    {"name": "内部脚本键", "type": 1, "ui_type": "Text"},
    {"name": "日期", "type": 1, "ui_type": "Text"},
    {"name": "计划发布时间", "type": 1, "ui_type": "Text"},
    {"name": "发布时间段", "type": 1, "ui_type": "Text"},
    {"name": "店铺ID", "type": 1, "ui_type": "Text"},
    {"name": "账号名称", "type": 1, "ui_type": "Text"},
    {"name": "账号ID", "type": 1, "ui_type": "Text"},
    {"name": "短视频标题", "type": 1, "ui_type": "Text"},
    {"name": "商品ID", "type": 1, "ui_type": "Text"},
    {"name": "脚本ID", "type": 1, "ui_type": "Text"},
    {"name": "视频附件", "type": 17, "ui_type": "Attachment"},
    {"name": "本地文件路径", "type": 1, "ui_type": "Text"},
    {"name": "自动排期状态", "type": 1, "ui_type": "Text"},
    {"name": "自动发布状态", "type": 1, "ui_type": "Text"},
    {"name": "自动发布结果", "type": 1, "ui_type": "Text"},
    {"name": "自动发布时间", "type": 1, "ui_type": "Text"},
    {"name": "自动发布任务ID", "type": 1, "ui_type": "Text"},
    {"name": "自动异常信息", "type": 1, "ui_type": "Text"},
    {
        "name": "最终处理状态",
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {
            "options": [
                {"name": "自动已发布"},
                {"name": "待人工发布"},
                {"name": "自动发布失败"},
                {"name": "已人工发布"},
                {"name": "无需处理"},
                {"name": "处理失败"},
            ]
        },
    },
    {
        "name": "是否需要人工处理",
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {"options": [{"name": "是"}, {"name": "否"}]},
    },
    {
        "name": "人工处理状态",
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {"options": [{"name": "待人工发布"}, {"name": "已人工发布"}, {"name": "无需处理"}, {"name": "处理失败"}]},
    },
    {"name": "人工备注", "type": 1, "ui_type": "Text"},
    {"name": "同步时间", "type": 1, "ui_type": "Text"},
)

PRODUCT_PUBLISH_REPORT_FIELDS: Sequence[Dict[str, Any]] = (
    {"name": "汇总唯一键", "type": 1, "ui_type": "Text"},
    {"name": "店铺ID", "type": 1, "ui_type": "Text"},
    {"name": "产品ID", "type": 1, "ui_type": "Text"},
    {"name": "产品主图", "type": 17, "ui_type": "Attachment"},
    {"name": "本周已发布视频数", "type": 2, "ui_type": "Number"},
    {"name": "上周已发布视频数", "type": 2, "ui_type": "Number"},
    {"name": "本月已发布视频数", "type": 2, "ui_type": "Number"},
    {"name": "上月已发布视频数", "type": 2, "ui_type": "Number"},
    {"name": "本周统计周期", "type": 1, "ui_type": "Text"},
    {"name": "上周统计周期", "type": 1, "ui_type": "Text"},
    {"name": "本月统计周期", "type": 1, "ui_type": "Text"},
    {"name": "上月统计周期", "type": 1, "ui_type": "Text"},
    {"name": "最近发布时间", "type": 1, "ui_type": "Text"},
    {"name": "源脚本记录ID", "type": 1, "ui_type": "Text"},
    {"name": "同步时间", "type": 1, "ui_type": "Text"},
)


def ensure_report_fields(client: Any) -> Dict[str, int]:
    existing = {item.field_name for item in client.list_fields()}
    created = 0
    for spec in REPORT_FIELDS:
        if spec["name"] in existing:
            continue
        try:
            client.create_field(
                field_name=spec["name"],
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
            )
            created += 1
            existing.add(str(spec["name"]))
        except Exception as exc:
            if "FieldNameDuplicated" in str(exc):
                existing.add(str(spec["name"]))
                continue
            raise
    return {"created_fields": created, "existing_fields": len(existing)}


def ensure_manual_queue_fields(client: Any) -> Dict[str, int]:
    existing = {item.field_name for item in client.list_fields()}
    created = 0
    for spec in MANUAL_QUEUE_FIELDS:
        if spec["name"] in existing:
            continue
        try:
            client.create_field(
                field_name=spec["name"],
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
                property=spec.get("property"),
            )
        except Exception as exc:
            if "FieldNameDuplicated" in str(exc):
                existing.add(str(spec["name"]))
                continue
            if spec["name"] in {"人工处理状态", "最终处理状态", "是否需要人工处理"}:
                client.create_field(field_name=spec["name"], field_type=1, ui_type="Text")
            else:
                raise
        created += 1
        existing.add(str(spec["name"]))
    return {"created_fields": created, "existing_fields": len(existing)}


def ensure_product_publish_report_fields(client: Any) -> Dict[str, int]:
    existing = {item.field_name for item in client.list_fields()}
    created = 0
    for spec in PRODUCT_PUBLISH_REPORT_FIELDS:
        if spec["name"] in existing:
            continue
        try:
            client.create_field(
                field_name=spec["name"],
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
                property=spec.get("property"),
            )
        except Exception as exc:
            if "FieldNameDuplicated" in str(exc):
                existing.add(str(spec["name"]))
                continue
            raise
        created += 1
        existing.add(str(spec["name"]))
    return {"created_fields": created, "existing_fields": len(existing)}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _field_text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            text = _normalize_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return ",".join(item for item in parts if item)
    return _normalize_text(value)


def _has_attachment(value: Any) -> bool:
    if isinstance(value, list):
        return any(isinstance(item, dict) and item.get("file_token") for item in value)
    if isinstance(value, dict):
        return bool(value.get("file_token"))
    return False


def _first_attachment(value: Any) -> Dict[str, Any] | None:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item.get("file_token"):
                return item
    if isinstance(value, dict) and value.get("file_token"):
        return value
    return None


def _manual_queue_key(row: Dict[str, Any]) -> str:
    scheduled_for = _normalize_text(row.get("scheduled_for"))
    script_id = _normalize_text(row.get("script_id"))
    return f"{scheduled_for}|{script_id}"


def _build_video_attachment(client: Any, row: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str]:
    local_path = Path(_normalize_text(row.get("local_file_path")))
    if not str(local_path) or not local_path.exists():
        return [], "本地视频文件不存在，未上传附件"
    content = local_path.read_bytes()
    content_type = mimetypes.guess_type(local_path.name)[0] or "video/mp4"
    uploaded = client.upload_attachment(
        content=content,
        file_name=local_path.name,
        content_type=content_type,
        size=len(content),
        parent_type="bitable_file",
    )
    return [uploaded], ""


def _month_start(value: datetime) -> datetime:
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def product_publish_periods(reference_date: str = "") -> Dict[str, str]:
    if str(reference_date or "").strip():
        ref = datetime.strptime(str(reference_date).strip(), "%Y-%m-%d")
    else:
        ref = datetime.now()
    current_month_start = _month_start(ref)
    if current_month_start.month == 12:
        current_month_end = current_month_start.replace(year=current_month_start.year + 1, month=1)
    else:
        current_month_end = current_month_start.replace(month=current_month_start.month + 1)
    previous_month_end = current_month_start
    if current_month_start.month == 1:
        previous_month_start = current_month_start.replace(year=current_month_start.year - 1, month=12)
    else:
        previous_month_start = current_month_start.replace(month=current_month_start.month - 1)
    this_week_start = (ref - timedelta(days=ref.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    this_week_end = (ref + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    last_week_start = (ref - timedelta(days=ref.weekday() + 7)).replace(hour=0, minute=0, second=0, microsecond=0)
    last_week_end = last_week_start + timedelta(days=7)

    def text(value: datetime) -> str:
        return value.strftime("%Y-%m-%d %H:%M:%S")

    def period_label(start: datetime, end: datetime) -> str:
        return f"{start.strftime('%Y-%m-%d')} ~ {(end - timedelta(days=1)).strftime('%Y-%m-%d')}"

    return {
        "this_week_start": text(this_week_start),
        "this_week_end": text(this_week_end),
        "last_week_start": text(last_week_start),
        "last_week_end": text(last_week_end),
        "current_month_start": text(current_month_start),
        "current_month_end": text(current_month_end),
        "previous_month_start": text(previous_month_start),
        "previous_month_end": text(previous_month_end),
        "this_week_label": period_label(this_week_start, this_week_end),
        "last_week_label": period_label(last_week_start, last_week_end),
        "current_month_label": period_label(current_month_start, current_month_end),
        "previous_month_label": period_label(previous_month_start, previous_month_end),
    }


def _product_report_key(row: Dict[str, Any]) -> str:
    return f"{_normalize_text(row.get('store_id'))}|{_normalize_text(row.get('product_id'))}"


def _product_image_lookup(source_client: Any, source_image_field: str = "") -> Dict[str, Dict[str, Any]]:
    field_names = set(source_client.list_field_names())
    image_field = source_image_field or next(
        (
            name
            for name in ("产品主图", "产品图片", "商品主图", "商品图片", "图片")
            if name in field_names
        ),
        "",
    )
    if not image_field:
        return {}
    lookup: Dict[str, Dict[str, Any]] = {}
    for record in source_client.list_records(page_size=500):
        attachment = _first_attachment(record.fields.get(image_field))
        if attachment:
            lookup[record.record_id] = attachment
    return lookup


def _copy_product_image(
    *,
    source_client: Any,
    target_client: Any,
    attachment: Dict[str, Any] | None,
) -> List[Dict[str, Any]]:
    if not attachment:
        return []
    content, file_name, content_type, size = source_client.download_attachment_bytes(attachment)
    uploaded = target_client.upload_attachment(
        content=content,
        file_name=file_name,
        content_type=content_type,
        size=size,
        parent_type="bitable_image",
    )
    return [uploaded]


def build_product_publish_report_fields(
    row: Dict[str, Any],
    periods: Dict[str, str],
    *,
    product_image: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        "汇总唯一键": _product_report_key(row),
        "店铺ID": _normalize_text(row.get("store_id")),
        "产品ID": _normalize_text(row.get("product_id")),
        "本周已发布视频数": int(row.get("this_week_published") or 0),
        "上周已发布视频数": int(row.get("last_week_published") or 0),
        "本月已发布视频数": int(row.get("current_month_published") or 0),
        "上月已发布视频数": int(row.get("previous_month_published") or 0),
        "本周统计周期": periods["this_week_label"],
        "上周统计周期": periods["last_week_label"],
        "本月统计周期": periods["current_month_label"],
        "上月统计周期": periods["previous_month_label"],
        "最近发布时间": _normalize_text(row.get("latest_published_at")),
        "源脚本记录ID": _normalize_text(row.get("source_record_id")),
        "同步时间": AutoPublishDB._now_text(),
    }
    if product_image:
        fields["产品主图"] = product_image
    return fields


def _manual_status_from_existing(existing: Any | None) -> str:
    if not existing:
        return ""
    return _field_text(existing.fields.get("人工处理状态"))


def _resolve_manual_final_status(row: Dict[str, Any], manual_status: str = "") -> tuple[str, str, str]:
    schedule_status = _normalize_text(row.get("schedule_status"))
    publish_status = _normalize_text(row.get("publish_status"))
    publish_result = _normalize_text(row.get("publish_result"))
    if manual_status in {"已人工发布", "无需处理"}:
        return manual_status, "否", manual_status
    if schedule_status == "已发布" or publish_status == "已发布" or publish_result in {"发布成功", "人工发布成功"}:
        if publish_result == "人工发布成功":
            return "已人工发布", "否", "已人工发布"
        return "自动已发布", "否", "无需处理"
    if manual_status == "处理失败":
        return "处理失败", "是", manual_status
    if schedule_status == "发布失败" or publish_status == "发布失败" or publish_result == "发布失败":
        return "自动发布失败", "是", manual_status or "待人工发布"
    return "待人工发布", "是", manual_status or "待人工发布"


def build_manual_queue_fields(
    row: Dict[str, Any],
    *,
    uploaded_video: List[Dict[str, Any]] | None = None,
    upload_error: str = "",
    manual_status: str = "",
) -> Dict[str, Any]:
    scheduled_for = _normalize_text(row.get("scheduled_for"))
    time_text = scheduled_for[11:16] if len(scheduled_for) >= 16 else scheduled_for
    error_message = _normalize_text(row.get("slot_error_message")) or _normalize_text(row.get("asset_error_message"))
    final_status, need_manual, default_manual_status = _resolve_manual_final_status(row, manual_status=manual_status)
    fields: Dict[str, Any] = {
        "清单唯一键": _manual_queue_key(row),
        "内部脚本键": _normalize_text(row.get("canonical_script_key")),
        "日期": scheduled_for[:10],
        "计划发布时间": scheduled_for,
        "发布时间段": time_text,
        "店铺ID": _normalize_text(row.get("store_id")),
        "账号名称": _normalize_text(row.get("account_name")),
        "账号ID": _normalize_text(row.get("account_id")),
        "短视频标题": _normalize_text(row.get("short_video_title")),
        "商品ID": _normalize_text(row.get("product_id")),
        "脚本ID": _normalize_text(row.get("script_id")),
        "本地文件路径": _normalize_text(row.get("local_file_path")),
        "自动排期状态": _normalize_text(row.get("schedule_status")),
        "自动发布状态": _normalize_text(row.get("publish_status")),
        "自动发布结果": _normalize_text(row.get("publish_result")),
        "自动发布时间": _normalize_text(row.get("published_at")),
        "自动发布任务ID": _normalize_text(row.get("publish_task_id")),
        "自动异常信息": upload_error or error_message,
        "最终处理状态": final_status,
        "是否需要人工处理": need_manual,
        "同步时间": AutoPublishDB._now_text(),
    }
    if default_manual_status:
        fields["人工处理状态"] = default_manual_status
    if uploaded_video:
        fields["视频附件"] = uploaded_video
    return fields


def build_report_fields(row: Any) -> Dict[str, Any]:
    updated_candidates = [
        _normalize_text(row["slot_updated_at"]),
        _normalize_text(row["asset_updated_at"]),
        _normalize_text(row["metadata_updated_at"]),
    ]
    updated_at = max((item for item in updated_candidates if item), default="")
    planned_publish_at = _normalize_text(row["planned_publish_at"]) or _normalize_text(row["latest_slot_scheduled_for"])
    return {
        "内部脚本键": _normalize_text(row["canonical_script_key"]),
        "脚本ID": _normalize_text(row["script_id"]),
        "源脚本记录ID": _normalize_text(row["source_record_id"]),
        "脚本槽位": _normalize_text(row["script_slot"]),
        "任务编号": _normalize_text(row["task_no"]),
        "店铺ID": _normalize_text(row["store_id"]),
        "产品ID": _normalize_text(row["product_id"]),
        "内容家族": _normalize_text(row["content_family_key"]),
        "短视频标题": _normalize_text(row["short_video_title"]),
        "运行表记录ID": _normalize_text(row["run_manager_record_id"]),
        "视频来源类型": _normalize_text(row["video_source_type"]),
        "视频来源值": _normalize_text(row["video_source_value"]),
        "本地文件路径": _normalize_text(row["local_file_path"]),
        "下载状态": _normalize_text(row["download_status"]),
        "跑视频状态": _normalize_text(row["run_video_status"]),
        "发布状态": _normalize_text(row["publish_status"]),
        "排期状态": _normalize_text(row["latest_schedule_status"]),
        "分配账号ID": _normalize_text(row["account_id"]),
        "分配账号名称": _normalize_text(row["account_name"]),
        "计划发布时间": planned_publish_at,
        "发布时间": _normalize_text(row["published_at"]),
        "发布任务ID": _normalize_text(row["publish_task_id"]),
        "发布结果": _normalize_text(row["publish_result"]),
        "错误信息": _normalize_text(row["error_message"]),
        "最后更新时间": updated_at,
    }


def _chunked(items: Sequence[Dict[str, Any]], size: int = 500) -> Iterable[List[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


def sync_publish_report_table(db: AutoPublishDB, client: Any) -> Dict[str, int]:
    field_stats = ensure_report_fields(client)
    rows = db.list_publish_report_rows()
    existing_map: Dict[str, str] = {}
    for record in client.list_records(page_size=500):
        canonical_script_key = _normalize_text(record.fields.get("内部脚本键"))
        script_id = _normalize_text(record.fields.get("脚本ID"))
        if canonical_script_key and canonical_script_key not in existing_map:
            existing_map[canonical_script_key] = record.record_id
        if script_id and script_id not in existing_map:
            existing_map[script_id] = record.record_id

    creates: List[Dict[str, Any]] = []
    updates: List[Dict[str, Any]] = []
    for row in rows:
        canonical_script_key = _normalize_text(row["canonical_script_key"])
        script_id = _normalize_text(row["script_id"])
        if not canonical_script_key and not script_id:
            continue
        fields = build_report_fields(row)
        record_id = existing_map.get(canonical_script_key) or existing_map.get(script_id)
        if record_id:
            updates.append({"record_id": record_id, "fields": fields})
        else:
            creates.append({"fields": fields})

    for batch in _chunked(creates):
        client.batch_create_records(batch)
    for batch in _chunked(updates):
        client.batch_update_records(batch)

    return {
        "report_rows": len(rows),
        "created_fields": field_stats["created_fields"],
        "created_records": len(creates),
        "updated_records": len(updates),
    }


def apply_manual_publish_statuses_from_table(
    db: AutoPublishDB,
    client: Any,
    *,
    day: str = "",
    days: set[str] | None = None,
) -> Dict[str, int]:
    target_days = {str(item or "").strip() for item in (days or set()) if str(item or "").strip()}
    if str(day or "").strip():
        target_days.add(str(day).strip())
    checked = 0
    marked_published = 0
    skipped = 0
    failed = 0
    for record in client.list_records(page_size=500):
        fields = record.fields or {}
        record_day = _field_text(fields.get("日期"))
        scheduled_for = _field_text(fields.get("计划发布时间"))
        if not record_day and len(scheduled_for) >= 10:
            record_day = scheduled_for[:10]
        if target_days and record_day not in target_days:
            continue
        checked += 1
        manual_status = _field_text(fields.get("人工处理状态"))
        if manual_status != "已人工发布":
            skipped += 1
            continue
        try:
            ok = db.mark_manual_publish_result(
                canonical_script_key=_field_text(fields.get("内部脚本键")),
                script_id=_field_text(fields.get("脚本ID")),
                scheduled_for=scheduled_for,
                note=_field_text(fields.get("人工备注")),
            )
            if ok:
                marked_published += 1
            else:
                failed += 1
        except Exception:
            failed += 1
    return {
        "manual_rows_checked": checked,
        "manual_rows_marked_published": marked_published,
        "manual_rows_skipped": skipped,
        "manual_rows_failed": failed,
    }


def sync_manual_publish_queue_table(
    db: AutoPublishDB,
    client: Any,
    *,
    day: str,
    upload_videos: bool = True,
    force_upload_videos: bool = False,
    include_published: bool = False,
) -> Dict[str, int]:
    field_stats = ensure_manual_queue_fields(client)
    manual_stats = apply_manual_publish_statuses_from_table(db, client, day=day)
    rows = db.list_manual_publish_queue(day, include_published=True)

    existing_map: Dict[str, Any] = {}
    for record in client.list_records(page_size=500):
        key = _normalize_text(record.fields.get("清单唯一键"))
        if key and key not in existing_map:
            existing_map[key] = record

    creates: List[Dict[str, Any]] = []
    updates: List[Dict[str, Any]] = []
    uploaded_count = 0
    upload_failed_count = 0

    for row in rows:
        key = _manual_queue_key(row)
        existing = existing_map.get(key)
        existing_has_video = bool(existing and _has_attachment(existing.fields.get("视频附件")))
        uploaded_video: List[Dict[str, Any]] = []
        upload_error = ""

        should_upload = upload_videos and (force_upload_videos or not existing_has_video)
        if should_upload:
            try:
                uploaded_video, upload_error = _build_video_attachment(client, row)
                if uploaded_video:
                    uploaded_count += 1
                elif upload_error:
                    upload_failed_count += 1
            except Exception as exc:
                upload_error = f"视频附件上传失败: {exc}"
                upload_failed_count += 1

        manual_status = _manual_status_from_existing(existing)
        fields = build_manual_queue_fields(
            row,
            uploaded_video=uploaded_video,
            upload_error=upload_error,
            manual_status=manual_status,
        )
        if not existing:
            creates.append({"fields": fields})
        else:
            if not fields.get("人工处理状态"):
                fields.pop("人工处理状态", None)
            updates.append({"record_id": existing.record_id, "fields": fields})

    for batch in _chunked(creates):
        client.batch_create_records(batch)
    for batch in _chunked(updates):
        client.batch_update_records(batch)

    return {
        "queue_rows": len(rows),
        "created_fields": field_stats["created_fields"],
        "created_records": len(creates),
        "updated_records": len(updates),
        "videos_uploaded": uploaded_count,
        "video_upload_failed": upload_failed_count,
        **manual_stats,
    }


def sync_product_publish_report_table(
    db: AutoPublishDB,
    target_client: Any,
    *,
    source_client: Any | None = None,
    reference_date: str = "",
    source_image_field: str = "",
    upload_images: bool = True,
    force_upload_images: bool = False,
) -> Dict[str, int]:
    field_stats = ensure_product_publish_report_fields(target_client)
    periods = product_publish_periods(reference_date)
    rows = db.list_product_publish_summary_rows(
        this_week_start=periods["this_week_start"],
        this_week_end=periods["this_week_end"],
        last_week_start=periods["last_week_start"],
        last_week_end=periods["last_week_end"],
        current_month_start=periods["current_month_start"],
        current_month_end=periods["current_month_end"],
        previous_month_start=periods["previous_month_start"],
        previous_month_end=periods["previous_month_end"],
    )

    image_lookup = _product_image_lookup(source_client, source_image_field) if source_client and upload_images else {}
    existing_map: Dict[str, Any] = {}
    for record in target_client.list_records(page_size=500):
        key = _normalize_text(record.fields.get("汇总唯一键"))
        if key and key not in existing_map:
            existing_map[key] = record

    creates: List[Dict[str, Any]] = []
    updates: List[Dict[str, Any]] = []
    images_uploaded = 0
    image_upload_failed = 0
    for row in rows:
        key = _product_report_key(row)
        existing = existing_map.get(key)
        existing_has_image = bool(existing and _has_attachment(existing.fields.get("产品主图")))
        image_payload: List[Dict[str, Any]] = []
        if upload_images and source_client and (force_upload_images or not existing_has_image):
            try:
                image_payload = _copy_product_image(
                    source_client=source_client,
                    target_client=target_client,
                    attachment=image_lookup.get(_normalize_text(row.get("source_record_id"))),
                )
                if image_payload:
                    images_uploaded += 1
            except Exception:
                image_upload_failed += 1
        fields = build_product_publish_report_fields(row, periods, product_image=image_payload)
        if existing:
            updates.append({"record_id": existing.record_id, "fields": fields})
        else:
            creates.append({"fields": fields})

    for batch in _chunked(creates):
        target_client.batch_create_records(batch)
    for batch in _chunked(updates):
        target_client.batch_update_records(batch)

    return {
        "report_rows": len(rows),
        "created_fields": field_stats["created_fields"],
        "created_records": len(creates),
        "updated_records": len(updates),
        "images_uploaded": images_uploaded,
        "image_upload_failed": image_upload_failed,
    }
