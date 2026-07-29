#!/usr/bin/env python3
"""人工短视频脚本库的简表解析与脚本主数据登记。"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from core.bitable import TableRecord
from core.sync import ScriptSyncTask, extract_attachments, normalize_checkbox, normalize_text, normalize_video_duration


MANUAL_SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "script_id": ["脚本ID"],
    "script": ["脚本"],
    "purpose": ["用途"],
    "product_id": ["产品ID"],
    "store_id": ["店铺", "店铺ID"],
    "images": ["图片", "产品图片", "参考图"],
    "sync_enabled": ["同步", "是否可同步"],
    "target_language": ["语言", "目标语言"],
    "video_duration": ["时长", "视频时长"],
    "sync_status": ["状态", "同步状态"],
    "sync_time": ["时间", "同步时间"],
}


@dataclass(frozen=True)
class ManualBuildResult:
    tasks: List[ScriptSyncTask]
    errors: Dict[str, str]
    script_ids: Dict[str, str]


def resolve_manual_field_mapping(field_names: Sequence[str]) -> Dict[str, Optional[str]]:
    return {
        logical_name: next((name for name in aliases if name in field_names), None)
        for logical_name, aliases in MANUAL_SOURCE_FIELD_ALIASES.items()
    }


def _field_text(raw_value: Any) -> str:
    if isinstance(raw_value, dict):
        for key in ("text", "name", "value"):
            text = normalize_text(raw_value.get(key))
            if text:
                return text
        return ""
    if isinstance(raw_value, list):
        for item in raw_value:
            text = _field_text(item)
            if text:
                return text
        return ""
    return normalize_text(raw_value)


def manual_script_id(record_id: str) -> str:
    """Create a stable, structured ID without requiring an operator sequence number."""
    digest = hashlib.sha1(str(record_id or "").encode("utf-8")).hexdigest()[:10].upper()
    return f"MAN{digest}_M1_M"


def _valid_script_id(value: str) -> bool:
    parts = value.split("_")
    return (
        len(parts) == 3
        and all(part.isalnum() for part in parts[:2])
        and (parts[2] == "M" or (parts[2].startswith("V") and parts[2][1:].isdigit()))
    )


def _purpose_values(purpose: str) -> Dict[str, str]:
    if purpose == "带货":
        return {
            "publish_purpose": "带货",
            "cart_enabled": "是",
            "content_branch": "商品展示型",
            "script_source": "人工脚本",
        }
    if purpose == "非带货":
        return {
            "publish_purpose": "养号",
            "cart_enabled": "否",
            "content_branch": "非商品展示型",
            "script_source": "人工脚本",
        }
    return {}


def build_manual_sync_tasks(
    records: Sequence[TableRecord],
    mapping: Dict[str, Optional[str]],
    *,
    record_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> ManualBuildResult:
    tasks: List[ScriptSyncTask] = []
    errors: Dict[str, str] = {}
    script_ids: Dict[str, str] = {}

    for record in records:
        if record_id and record.record_id != record_id:
            continue
        fields = record.fields
        sync_field = mapping.get("sync_enabled")
        if not sync_field or not normalize_checkbox(fields.get(sync_field)):
            continue

        existing_id = _field_text(fields.get(mapping.get("script_id"))) if mapping.get("script_id") else ""
        script_id = existing_id if _valid_script_id(existing_id) else manual_script_id(record.record_id)
        script_ids[record.record_id] = script_id

        script_text = _field_text(fields.get(mapping.get("script"))) if mapping.get("script") else ""
        purpose = _field_text(fields.get(mapping.get("purpose"))) if mapping.get("purpose") else ""
        store_id = _field_text(fields.get(mapping.get("store_id"))) if mapping.get("store_id") else ""
        product_id = _field_text(fields.get(mapping.get("product_id"))) if mapping.get("product_id") else ""

        missing: List[str] = []
        if not script_text:
            missing.append("脚本")
        if purpose not in {"带货", "非带货"}:
            missing.append("用途（请选择带货或非带货）")
        if not store_id:
            missing.append("店铺")
        if purpose == "带货" and not product_id:
            missing.append("产品ID（带货脚本必填）")
        if missing:
            errors[record.record_id] = f"缺少或无效字段：{'、'.join(missing)}"
            continue

        images = extract_attachments(fields.get(mapping.get("images"))) if mapping.get("images") else []
        purpose_values = _purpose_values(purpose)
        task_name_prefix = product_id if product_id else "MANUAL"
        target_language = _field_text(fields.get(mapping.get("target_language"))) if mapping.get("target_language") else ""
        duration = normalize_video_duration(
            fields.get(mapping.get("video_duration")) if mapping.get("video_duration") else None,
        )
        tasks.append(
            ScriptSyncTask(
                source_record_id=record.record_id,
                product_code=product_id or "MANUAL",
                script_slot="S1",
                task_name=f"{task_name_prefix}.{script_id}",
                prompt_text=script_text,
                reference_images=images,
                internal_script_key=f"{record.record_id}:S1",
                target_language=target_language,
                script_id=script_id,
                store_id=store_id,
                product_id=product_id,
                parent_slot="M1",
                direction_label="人工脚本",
                variant_strength="母版",
                video_duration=duration,
                **purpose_values,
            )
        )
        if limit is not None and len(tasks) >= limit:
            break

    return ManualBuildResult(tasks=tasks, errors=errors, script_ids=script_ids)


def upsert_manual_metadata(tasks: Sequence[ScriptSyncTask], db_path: str) -> int:
    """Register manual rows before their generated videos are collected downstream."""
    if not tasks:
        return 0
    publisher_dir = Path(__file__).resolve().parents[2] / "short-video-auto-publisher"
    if str(publisher_dir) not in sys.path:
        sys.path.insert(0, str(publisher_dir))

    from app.db import AutoPublishDB
    from app.metadata import HeuristicTitleGenerator, infer_country_from_store_id
    from app.models import ScriptMetadata

    generator = HeuristicTitleGenerator()
    metadata_items: List[ScriptMetadata] = []
    for task in tasks:
        base = ScriptMetadata(
            canonical_script_key=task.internal_script_key,
            script_id=task.script_id,
            source_record_id=task.source_record_id,
            script_slot=task.script_slot,
            task_no=task.script_id.split("_", 1)[0],
            store_id=task.store_id,
            product_id=task.product_id,
            parent_slot=task.parent_slot,
            direction_label=task.direction_label,
            variant_strength=task.variant_strength,
            target_country=infer_country_from_store_id(task.store_id),
            product_type=task.product_type,
            content_family_key=(f"{task.product_id}_{task.parent_slot}" if task.product_id else f"{task.internal_script_key}:养号"),
            script_text=task.prompt_text,
            short_video_title="",
            title_source="",
            script_source=task.script_source,
            publish_purpose=task.publish_purpose,
            cart_enabled=task.cart_enabled,
            content_branch=task.content_branch,
        )
        metadata_items.append(
            ScriptMetadata(
                **{**base.__dict__, "short_video_title": generator.generate(base), "title_source": generator.source}
            )
        )
    return AutoPublishDB(Path(db_path)).upsert_script_metadata(metadata_items)
