"""One-row-one-script source adapter for the original production workbench."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from core.bitable import TableRecord
from core.sync import (
    ScriptSyncTask,
    extract_attachments,
    normalize_checkbox,
    normalize_text,
    normalize_video_duration,
    resolve_field_mapping,
)


ORIGINAL_BATCH_SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "script_id": ["脚本ID"],
    "product_code": ["产品编码", "产品ID"],
    "product_images": ["产品图片", "参考图"],
    "store_id": ["店铺ID"],
    "batch_id": ["批次ID"],
    "batch_item_id": ["批次ItemID"],
    "item_index": ["批次序号"],
    "script_title": ["脚本标题"],
    "product_type": ["产品类型"],
    "target_language": ["目标语言"],
    "business_category": ["一级类目"],
    "video_duration": ["视频时长"],
    "complete_script": ["完整生产脚本"],
    "video_prompt": ["视频生成提示词"],
    "sync_enabled": ["进入生产"],
    "sync_status": ["同步结果"],
    "sync_time": ["同步时间"],
    "processing_status": ["处理状态"],
    "run_task_id": ["运行任务ID"],
}


def resolve_original_batch_field_mapping(field_names: Sequence[str]) -> Dict[str, Optional[str]]:
    return resolve_field_mapping(field_names, ORIGINAL_BATCH_SOURCE_FIELD_ALIASES)


def build_original_batch_sync_tasks(
    records: Sequence[TableRecord],
    mapping: Dict[str, Optional[str]],
    *,
    product_code: Optional[str] = None,
    record_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[ScriptSyncTask]:
    tasks: List[ScriptSyncTask] = []
    for record in records:
        if record_id and record.record_id != record_id:
            continue
        fields = record.fields
        enabled = normalize_checkbox(fields.get(mapping.get("sync_enabled"))) if mapping.get("sync_enabled") else False
        if not enabled:
            continue
        code = normalize_text(fields.get(mapping.get("product_code"))) if mapping.get("product_code") else ""
        if product_code and code != product_code:
            continue
        script_id = normalize_text(fields.get(mapping.get("script_id"))) if mapping.get("script_id") else ""
        prompt = normalize_text(fields.get(mapping.get("video_prompt"))) if mapping.get("video_prompt") else ""
        if not prompt and mapping.get("complete_script"):
            prompt = normalize_text(fields.get(mapping.get("complete_script")))
        if not code or not script_id or not prompt:
            continue
        index_text = normalize_text(fields.get(mapping.get("item_index"))) if mapping.get("item_index") else ""
        try:
            item_index = int(float(index_text))
        except (TypeError, ValueError):
            item_index = len(tasks) + 1
        slot = f"D{item_index:02d}"
        batch_item_id = normalize_text(fields.get(mapping.get("batch_item_id"))) if mapping.get("batch_item_id") else ""
        title = normalize_text(fields.get(mapping.get("script_title"))) if mapping.get("script_title") else ""
        tasks.append(
            ScriptSyncTask(
                source_record_id=record.record_id,
                product_code=code,
                script_slot=slot,
                task_name=f"{code}.{script_id}",
                prompt_text=prompt,
                reference_images=extract_attachments(fields.get(mapping.get("product_images"))) if mapping.get("product_images") else [],
                internal_script_key=batch_item_id or f"{record.record_id}:{script_id}",
                product_type=normalize_text(fields.get(mapping.get("product_type"))) if mapping.get("product_type") else "",
                target_language=normalize_text(fields.get(mapping.get("target_language"))) if mapping.get("target_language") else "",
                business_category=normalize_text(fields.get(mapping.get("business_category"))) if mapping.get("business_category") else "",
                script_id=script_id,
                short_video_title=title,
                store_id=normalize_text(fields.get(mapping.get("store_id"))) if mapping.get("store_id") else "",
                product_id=code,
                parent_slot=slot,
                direction_label=title,
                variant_strength="母版",
                script_source="原创脚本",
                source_script_type="原创脚本",
                video_duration=normalize_video_duration(fields.get(mapping.get("video_duration")) if mapping.get("video_duration") else None),
            )
        )
        if limit is not None and len(tasks) >= limit:
            break
    return tasks
