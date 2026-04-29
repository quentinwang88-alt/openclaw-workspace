#!/usr/bin/env python3
"""发布追踪表回写。"""

from __future__ import annotations

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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


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
