#!/usr/bin/env python3
"""视频同步、账号同步、排班与结果回写。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

from app.db import AutoPublishDB, default_video_dir
from app.models import AccountConfig
from app.publishers import BasePublishAdapter, DryRunPublishAdapter


RUN_MANAGER_FIELD_ALIASES: Dict[str, List[str]] = {
    "canonical_script_key": ["内部脚本键", "稳定脚本键", "canonical_script_key"],
    "script_id": ["脚本ID"],
    "run_video_status": ["跑视频状态", "状态"],
    "publish_enabled": ["是否发布", "是否自动发布"],
    "video_attachment": ["视频附件", "生成视频"],
    "video_link": ["视频链接", "视频附件 / 视频链接"],
    "download_status": ["下载状态"],
    "local_file_path": ["本地文件路径"],
    "publish_status": ["发布状态"],
    "account_id": ["分配账号ID"],
    "account_name": ["分配账号名称"],
    "planned_publish_at": ["计划发布时间"],
    "published_at": ["发布时间"],
    "publish_result": ["发布结果"],
    "publish_task_id": ["发布任务ID"],
}

ACCOUNT_FIELD_ALIASES: Dict[str, List[str]] = {
    "account_id": ["账号ID"],
    "account_name": ["账号名称"],
    "store_id": ["店铺ID"],
    "account_status": ["账号状态"],
    "publish_time_1": ["发布时间1"],
    "publish_time_2": ["发布时间2"],
    "publish_time_3": ["发布时间3"],
}


def resolve_field_mapping(field_names: Sequence[str], aliases: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    for logical_name, candidates in aliases.items():
        mapping[logical_name] = next((candidate for candidate in candidates if candidate in field_names), None)
    return mapping


def normalize_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value).strip()


def normalize_checkbox(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "已勾选", "勾选", "checked"}
    return False


def extract_attachment(raw_value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw_value, list):
        for item in raw_value:
            if isinstance(item, dict) and item.get("file_token"):
                return item
    if isinstance(raw_value, dict) and raw_value.get("file_token"):
        return raw_value
    return None


def sync_accounts(records: Iterable[Any], mapping: Dict[str, Optional[str]], db: AutoPublishDB) -> int:
    configs: List[AccountConfig] = []
    for record in records:
        fields = record.fields
        account_id = normalize_text(fields.get(mapping.get("account_id")))
        account_name = normalize_text(fields.get(mapping.get("account_name")))
        store_id = normalize_text(fields.get(mapping.get("store_id")))
        if not account_id or not store_id:
            continue
        configs.append(
            AccountConfig(
                account_id=account_id,
                account_name=account_name or account_id,
                store_id=store_id,
                account_status=normalize_text(fields.get(mapping.get("account_status"))) or "暂停",
                publish_time_1=normalize_text(fields.get(mapping.get("publish_time_1"))),
                publish_time_2=normalize_text(fields.get(mapping.get("publish_time_2"))),
                publish_time_3=normalize_text(fields.get(mapping.get("publish_time_3"))),
            )
        )
    return db.upsert_account_configs(configs)


def _ensure_download_dir(download_dir: Optional[Path]) -> Path:
    path = Path(download_dir) if download_dir else default_video_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _download_from_url(url: str) -> bytes:
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return response.content


def sync_videos(
    records: Sequence[Any],
    mapping: Dict[str, Optional[str]],
    db: AutoPublishDB,
    *,
    download_dir: Optional[Path],
    client: Any,
) -> Dict[str, int]:
    stats = {"synced": 0, "skipped": 0, "download_failed": 0}
    base_dir = _ensure_download_dir(download_dir)

    for record in records:
        fields = record.fields
        canonical_script_key = normalize_text(fields.get(mapping.get("canonical_script_key")))
        script_id = normalize_text(fields.get(mapping.get("script_id")))
        if not canonical_script_key and not script_id:
            stats["skipped"] += 1
            continue
        run_status = normalize_text(fields.get(mapping.get("run_video_status")))
        publish_enabled = normalize_checkbox(fields.get(mapping.get("publish_enabled")))
        if not publish_enabled:
            stats["skipped"] += 1
            continue
        metadata = db.get_script_metadata(canonical_script_key or script_id)
        if metadata is None:
            stats["skipped"] += 1
            continue
        resolved_canonical_key = normalize_text(metadata["canonical_script_key"])
        resolved_script_id = normalize_text(metadata["script_id"]) or script_id

        attachment = extract_attachment(fields.get(mapping.get("video_attachment")))
        video_link = normalize_text(fields.get(mapping.get("video_link")))
        local_file_path = normalize_text(fields.get(mapping.get("local_file_path")))
        if local_file_path and Path(local_file_path).exists():
            resolved_local_path = local_file_path
        else:
            resolved_local_path = str(base_dir / f"{resolved_script_id}.mp4")
            if not Path(resolved_local_path).exists():
                try:
                    if attachment:
                        content, _, _, _ = client.download_attachment_bytes(attachment)
                    elif video_link:
                        content = _download_from_url(video_link)
                    else:
                        stats["download_failed"] += 1
                        continue
                    Path(resolved_local_path).write_bytes(content)
                except Exception:
                    stats["download_failed"] += 1
                    continue

        source_value = attachment.get("file_token", "") if attachment else video_link
        source_type = "attachment" if attachment else "link"
        db.upsert_video_asset(
            canonical_script_key=resolved_canonical_key,
            script_id=resolved_script_id,
            run_manager_record_id=record.record_id,
            video_source_type=source_type,
            video_source_value=str(source_value or ""),
            local_file_path=resolved_local_path,
            download_status="下载成功",
            run_video_status=run_status,
            publish_status=normalize_text(fields.get(mapping.get("publish_status"))) or "待排期",
        )
        stats["synced"] += 1
    return stats


@dataclass(frozen=True)
class SchedulingStats:
    slots_created: int = 0
    slots_examined: int = 0
    scheduled: int = 0
    skipped: int = 0


def schedule_slots(db: AutoPublishDB, publisher: BasePublishAdapter, now: Optional[datetime] = None) -> SchedulingStats:
    current_time = now or datetime.now()
    if not isinstance(publisher, DryRunPublishAdapter):
        db.recycle_dryrun_schedules()
    slots_created = db.generate_future_slots(current_time, window_hours=24)
    pending_slots = db.list_pending_slots(current_time, window_hours=24)

    scheduled = 0
    skipped = 0
    for slot in pending_slots:
        target_time = datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
        candidates = db.list_ready_candidates(str(slot["store_id"] or ""))
        selected = None
        for candidate in candidates:
            if db.has_recent_product_conflict(str(slot["account_id"] or ""), candidate.product_id, target_time, hours=72):
                continue
            if db.has_recent_family_conflict(str(slot["store_id"] or ""), candidate.content_family_key, target_time, hours=48):
                continue
            selected = candidate
            break
        if selected is None:
            skipped += 1
            continue
        task_id = publisher.create_scheduled_task(
            account_id=str(slot["account_id"] or ""),
            video_path=selected.publish_video_value,
            title=selected.short_video_title,
            publish_at=target_time,
            script_id=selected.script_id,
            product_id=selected.product_id,
            product_title=selected.product_title,
            ref_video_id=selected.ref_video_id,
        )
        db.assign_slot(
            slot_id=int(slot["slot_id"]),
            canonical_script_key=selected.canonical_script_key,
            script_id=selected.script_id,
            publish_task_id=task_id,
            account_id=str(slot["account_id"] or ""),
            account_name=str(slot["account_name"] or ""),
            planned_publish_at=target_time,
        )
        scheduled += 1

    return SchedulingStats(
        slots_created=slots_created,
        slots_examined=len(pending_slots),
        scheduled=scheduled,
        skipped=skipped,
    )


def sync_publish_results(db: AutoPublishDB, publisher: BasePublishAdapter) -> Dict[str, int]:
    stats = {"published": 0, "failed": 0, "pending": 0}
    for task in db.list_scheduled_tasks():
        scheduled_for = datetime.strptime(str(task["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
        status = publisher.query_task_status(task_id=str(task["publish_task_id"]), scheduled_for=scheduled_for)
        if status.state == "success":
            db.mark_publish_result(
                canonical_script_key=str(task["canonical_script_key"] or ""),
                script_id=str(task["script_id"]),
                publish_task_id=str(task["publish_task_id"]),
                schedule_status="已发布",
                publish_status="已发布",
                publish_result="发布成功",
                published_at=status.published_at or scheduled_for.strftime("%Y-%m-%d %H:%M:%S"),
            )
            stats["published"] += 1
        elif status.state == "failed":
            db.mark_publish_result(
                canonical_script_key=str(task["canonical_script_key"] or ""),
                script_id=str(task["script_id"]),
                publish_task_id=str(task["publish_task_id"]),
                schedule_status="发布失败",
                publish_status="发布失败",
                publish_result="发布失败",
                error_message=status.error_message,
            )
            stats["failed"] += 1
        else:
            stats["pending"] += 1
    return stats
