#!/usr/bin/env python3
"""视频同步、账号同步、排班与结果回写。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import os
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

from app.db import AutoPublishDB, default_video_dir, is_nurture_candidate
from app.metadata import sanitize_title
from app.models import AccountConfig
from app.publishers import BasePublishAdapter, DryRunPublishAdapter

RUN_MANAGER_FIELD_ALIASES: Dict[str, List[str]] = {
    "canonical_script_key": ["内部脚本键", "稳定脚本键", "canonical_script_key"],
    "script_id": ["脚本ID"],
    "run_video_status": ["跑视频状态", "状态"],
    "publish_enabled": ["是否发布", "是否自动发布"],
    "video_attachment": ["视频附件", "生成视频"],
    "voiceover_requested": ["是否配口播"],
    "voiceover_status": ["口播状态"],
    "voiceover_attachment": ["口播成片"],
    "short_video_title": ["短视频标题", "视频标题", "发布标题", "人工标题"],
    "video_link": ["视频链接", "视频附件 / 视频链接"],
    "oss_object_id": ["OSS对象ID"],
    "oss_path": ["OSS路径"],
    "material_asset_id": ["素材ID"],
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
    "publish_channel": ["发布渠道", "发布平台", "发布方式", "publish_channel"],
    "publish_time_1": ["发布时间1"],
    "publish_time_2": ["发布时间2"],
    "publish_time_3": ["发布时间3"],
    "nurture_enabled": ["是否开启养号"],
    "nurture_daily_count": ["每日养号条数"],
    "nurture_only": ["是否仅养号"],
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


def _choice_text(raw_value: Any) -> str:
    if isinstance(raw_value, dict):
        for key in ("text", "name", "value"):
            text = normalize_text(raw_value.get(key))
            if text:
                return text
        return ""
    if isinstance(raw_value, list):
        for item in raw_value:
            text = _choice_text(item)
            if text:
                return text
        return ""
    return normalize_text(raw_value)


def normalize_publish_channel(raw_value: Any) -> str:
    text = _choice_text(raw_value)
    if not text:
        return "GeeLark"
    compact = text.strip().lower().replace(" ", "").replace("-", "").replace("_", "")
    if compact in {"neobund", "neobundai", "neobundai"} or "neobund" in compact:
        return "NeoBund"
    if compact in {"geelark", "geelarkcloudphone"} or "geelark" in compact:
        return "GeeLark"
    if compact in {"manual", "hand", "human", "人工", "手动", "人工发布", "手动发布"}:
        return "手动"
    if compact in {"pause", "paused", "disabled", "disable", "stop", "暂停", "停用", "停止"}:
        return "暂停"
    return text


def normalize_checkbox(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "是", "已勾选", "勾选", "checked"}
    return False


def should_mark_ai_for_geelark(candidate: Any) -> Optional[bool]:
    """Original/remake videos should be marked as AI-generated in GeeLark."""
    markers = " ".join(
        str(getattr(candidate, attr, "") or "").strip().lower()
        for attr in ("script_source", "publish_purpose", "content_branch")
    )
    if "混剪" in markers or "mixcut" in markers:
        return None
    return True


def is_short_video_remake_candidate(candidate: Any) -> bool:
    markers = " ".join(
        str(getattr(candidate, attr, "") or "").strip()
        for attr in ("script_source", "publish_purpose", "content_branch")
    )
    return "短视频复刻" in markers


def normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value or "").strip()))
    except (TypeError, ValueError):
        return default


def extract_attachment(raw_value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw_value, list):
        for item in raw_value:
            if isinstance(item, dict) and item.get("file_token"):
                return item
    if isinstance(raw_value, dict) and raw_value.get("file_token"):
        return raw_value
    return None


def select_publish_attachment(
    fields: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Select the final publish artifact without leaking raw video.

    When narration was requested, the generated source video is deliberately
    blocked until a completed narration attachment exists.
    """
    requested = normalize_checkbox(fields.get(mapping.get("voiceover_requested")))
    if not requested:
        return extract_attachment(fields.get(mapping.get("video_attachment"))), "generated"
    status = _choice_text(fields.get(mapping.get("voiceover_status")))
    voiceover = extract_attachment(fields.get(mapping.get("voiceover_attachment")))
    if status == "已完成" and voiceover:
        return voiceover, "voiceover"
    return None, "waiting_voiceover"


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
                publish_channel=normalize_publish_channel(fields.get(mapping.get("publish_channel"))),
                publish_time_1=normalize_text(fields.get(mapping.get("publish_time_1"))),
                publish_time_2=normalize_text(fields.get(mapping.get("publish_time_2"))),
                publish_time_3=normalize_text(fields.get(mapping.get("publish_time_3"))),
                nurture_enabled=normalize_checkbox(fields.get(mapping.get("nurture_enabled"))),
                nurture_daily_count=max(normalize_int(fields.get(mapping.get("nurture_daily_count")), 2), 0),
                nurture_only=normalize_checkbox(fields.get(mapping.get("nurture_only"))),
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


def _oss_source_enabled() -> bool:
    return str(os.environ.get("PUBLISH_OSS_SOURCE_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _download_from_oss(*, object_id: str = "", object_key: str = "") -> bytes:
    root = Path(__file__).resolve().parents[3] / "auto_mixcut"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from auto_mixcut.core.bootstrap import build_context
    from auto_mixcut.core.storage_paths import resolve_oss_object_path

    ctx = build_context()
    object_id = str(object_id or "").strip().split(" / ", 1)[0]
    object_key = str(object_key or "").strip().split(" / ", 1)[0]
    if object_id:
        resolved = resolve_oss_object_path(ctx, object_id, "publisher")
        if not resolved.success:
            message = resolved.error.message if resolved.error else "OSS object resolve failed"
            raise RuntimeError(message)
        content = Path(str(resolved.data["path"])).read_bytes()
    elif object_key:
        dest = default_video_dir() / "oss_cache" / Path(object_key).name
        downloaded = ctx.oss.download(object_key, dest)
        if not downloaded.success:
            message = downloaded.error.message if downloaded.error else "OSS download failed"
            raise RuntimeError(message)
        content = dest.read_bytes()
    else:
        raise ValueError("missing OSS object source")
    if not content:
        raise ValueError("downloaded OSS video is empty")
    return content


def sync_videos(
    records: Sequence[Any],
    mapping: Dict[str, Optional[str]],
    db: AutoPublishDB,
    *,
    download_dir: Optional[Path],
    client: Any,
) -> Dict[str, int]:
    stats = {
        "synced": 0,
        "skipped": 0,
        "download_failed": 0,
        "titles_updated": 0,
        "waiting_voiceover": 0,
    }
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
        existing_asset = db.get_video_asset(resolved_canonical_key or resolved_script_id)
        if existing_asset is not None:
            existing_publish_status = normalize_text(existing_asset["publish_status"])
            existing_download_status = normalize_text(existing_asset["download_status"])
            if existing_publish_status == "已发布" and existing_download_status == "已清理":
                stats["skipped"] += 1
                continue

        attachment, video_variant = select_publish_attachment(fields, mapping)
        if video_variant == "waiting_voiceover":
            db.mark_video_waiting_voiceover(resolved_canonical_key or resolved_script_id)
            stats["waiting_voiceover"] += 1
            stats["skipped"] += 1
            continue
        video_link = normalize_text(fields.get(mapping.get("video_link")))
        oss_object_id = normalize_text(fields.get(mapping.get("oss_object_id")))
        oss_path = normalize_text(fields.get(mapping.get("oss_path")))
        local_file_path = normalize_text(fields.get(mapping.get("local_file_path")))
        if video_variant != "voiceover" and local_file_path and Path(local_file_path).exists():
            resolved_local_path = local_file_path
        else:
            if video_variant == "voiceover":
                source_token = str((attachment or {}).get("file_token") or "voiceover")
                source_suffix = hashlib.sha256(source_token.encode("utf-8")).hexdigest()[:10]
                resolved_local_path = str(base_dir / f"{resolved_script_id}_voiceover_{source_suffix}.mp4")
            else:
                resolved_local_path = str(base_dir / f"{resolved_script_id}.mp4")
            if not Path(resolved_local_path).exists():
                try:
                    if video_variant == "voiceover" and attachment:
                        content, _, _, _ = client.download_attachment_bytes(attachment)
                    elif _oss_source_enabled() and (oss_object_id or oss_path):
                        content = _download_from_oss(object_id=oss_object_id, object_key=oss_path)
                    elif attachment:
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

        if video_variant == "voiceover" and attachment:
            source_value = attachment.get("file_token", "")
            source_type = "voiceover_attachment"
        elif _oss_source_enabled() and (oss_object_id or oss_path):
            source_value = oss_object_id or oss_path
            source_type = "oss_object"
        elif attachment:
            source_value = attachment.get("file_token", "")
            source_type = "attachment"
        else:
            source_value = video_link
            source_type = "link"
        requested_publish_status = normalize_text(fields.get(mapping.get("publish_status")))
        if video_variant == "voiceover" and requested_publish_status == "等待口播":
            requested_publish_status = "待排期"
        db.upsert_video_asset(
            canonical_script_key=resolved_canonical_key,
            script_id=resolved_script_id,
            run_manager_record_id=record.record_id,
            video_source_type=source_type,
            video_source_value=str(source_value or ""),
            local_file_path=resolved_local_path,
            download_status="下载成功",
            run_video_status=run_status,
            publish_status=requested_publish_status or "待排期",
        )
        manual_title = sanitize_title(fields.get(mapping.get("short_video_title")))
        if manual_title:
            stats["titles_updated"] += db.update_short_video_title(
                canonical_script_key=resolved_canonical_key,
                short_video_title=manual_title,
                title_source="run_manager_manual",
            )
        stats["synced"] += 1
    return stats


@dataclass(frozen=True)
class SchedulingStats:
    slots_created: int = 0
    slots_examined: int = 0
    scheduled: int = 0
    skipped: int = 0
    create_failed: int = 0
    blocked_by_rules: int = 0
    retryable_create_failed: int = 0


DEFAULT_SCHEDULE_WINDOW_HOURS = 48


def _is_retryable_create_error(error_message: str) -> bool:
    text = str(error_message or "").lower()
    return any(
        marker in text
        for marker in (
            "balance not enough",
            "too many requests",
            "rate limit",
            "timeout",
            "temporarily",
            "temporary",
            "connection",
            "network",
        )
    )


def schedule_slots(
    db: AutoPublishDB,
    publisher: BasePublishAdapter,
    now: Optional[datetime] = None,
    *,
    window_hours: int = DEFAULT_SCHEDULE_WINDOW_HOURS,
) -> SchedulingStats:
    current_time = now or datetime.now()
    if not isinstance(publisher, DryRunPublishAdapter):
        db.recycle_dryrun_schedules()
    slots_created = db.generate_future_slots(current_time, window_hours=window_hours)
    pending_slots = db.list_pending_slots(current_time, window_hours=window_hours)

    scheduled = 0
    skipped = 0
    create_failed = 0
    blocked_by_rules = 0
    retryable_create_failed = 0
    disabled_accounts: set[str] = set()
    for slot in pending_slots:
        account_id = str(slot["account_id"] or "")
        account_name = str(slot["account_name"] or "")
        if account_id in disabled_accounts:
            skipped += 1
            continue
        target_time = datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
        candidates = db.list_ready_candidates(str(slot["store_id"] or ""))
        account = db.get_account_config(account_id)
        nurture_enabled = bool(account and int(account["nurture_enabled"] or 0))
        nurture_quota = int(account["nurture_daily_count"] or 2) if account else 0
        nurture_only = bool(account and int(account["nurture_only"] or 0))
        nurture_count = (
            db.count_scheduled_nurture_for_account_day(str(slot["account_id"] or ""), target_time)
            if nurture_enabled and nurture_quota > 0
            else 0
        )
        prefer_nurture = nurture_only or (nurture_enabled and nurture_count < nurture_quota)
        has_nurture_candidate = any(is_nurture_candidate(candidate) for candidate in candidates)
        if nurture_only:
            candidates = [candidate for candidate in candidates if is_nurture_candidate(candidate)]
        elif prefer_nurture:
            candidates = [candidate for candidate in candidates if is_nurture_candidate(candidate)] + [
                candidate for candidate in candidates if not is_nurture_candidate(candidate)
            ]
        else:
            candidates = [candidate for candidate in candidates if not is_nurture_candidate(candidate)] + [
                candidate for candidate in candidates if is_nurture_candidate(candidate)
            ]
        selected = None
        for candidate in candidates:
            if prefer_nurture and has_nurture_candidate and not is_nurture_candidate(candidate):
                break
            if db.has_active_script_assignment(candidate.canonical_script_key, exclude_slot_id=int(slot["slot_id"])):
                continue
            if not is_nurture_candidate(candidate):
                if db.count_recent_product_for_account(str(slot["account_id"] or ""), candidate.product_id, target_time, hours=24) >= 2:
                    continue
                if (
                    not is_short_video_remake_candidate(candidate)
                    and db.has_recent_family_conflict(str(slot["store_id"] or ""), candidate.content_family_key, target_time, hours=48)
                ):
                    continue
            selected = candidate
            break
        if selected is None:
            skipped += 1
            blocked_by_rules += 1
            db.mark_slot_pending_reason(
                int(slot["slot_id"]),
                reason="暂未找到符合规则的候选视频，等待后续自动补排",
            )
            continue
        try:
            task_id = publisher.create_scheduled_task(
                account_id=account_id,
                video_path=selected.publish_video_value,
                title=selected.short_video_title,
                publish_at=target_time,
                script_id=selected.script_id,
                product_id="" if is_nurture_candidate(selected) or str(selected.cart_enabled or "").strip() == "否" else selected.product_id,
                product_title=selected.product_title,
                ref_video_id=selected.ref_video_id,
                mark_ai=should_mark_ai_for_geelark(selected),
            )
        except Exception as exc:
            create_failed += 1
            skipped += 1
            error_message = str(exc)
            if "phone env not found" in error_message.lower():
                disabled_accounts.add(account_id)
                db.disable_account(account_id, reason=f"发布账号环境不存在，已暂停账号：{error_message}")
            elif _is_retryable_create_error(error_message):
                retryable_create_failed += 1
                disabled_accounts.add(account_id)
                db.mark_slot_pending_reason(int(slot["slot_id"]), reason=f"创建自动发布定时任务暂时失败，等待重试：{error_message}")
            else:
                db.cancel_slot(int(slot["slot_id"]), reason=f"创建自动发布定时任务失败：{error_message}")
            continue
        assigned = db.assign_slot(
            slot_id=int(slot["slot_id"]),
            canonical_script_key=selected.canonical_script_key,
            script_id=selected.script_id,
            publish_task_id=task_id,
            account_id=account_id,
            account_name=account_name,
            planned_publish_at=target_time,
        )
        if not assigned:
            skipped += 1
            blocked_by_rules += 1
            continue
        scheduled += 1

    return SchedulingStats(
        slots_created=slots_created,
        slots_examined=len(pending_slots),
        scheduled=scheduled,
        skipped=skipped,
        create_failed=create_failed,
        blocked_by_rules=blocked_by_rules,
        retryable_create_failed=retryable_create_failed,
    )


def sync_publish_results(
    db: AutoPublishDB,
    publisher: BasePublishAdapter,
    *,
    failure_grace_hours: int = 12,
) -> Dict[str, int]:
    stats = {"published": 0, "failed": 0, "pending": 0}
    tasks = db.list_scheduled_tasks()
    task_statuses = publisher.query_task_statuses(tasks)
    now = datetime.now()
    grace = timedelta(hours=max(0, int(failure_grace_hours)))
    for task in tasks:
        scheduled_for = datetime.strptime(str(task["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
        task_id = str(task["publish_task_id"])
        status = task_statuses.get(task_id) or publisher.query_task_status(task_id=task_id, scheduled_for=scheduled_for)
        if status.state == "success":
            db.mark_publish_result(
                canonical_script_key=str(task["canonical_script_key"] or ""),
                script_id=str(task["script_id"]),
                publish_task_id=task_id,
                schedule_status="已发布",
                publish_status="已发布",
                publish_result="发布成功",
                published_at=status.published_at or scheduled_for.strftime("%Y-%m-%d %H:%M:%S"),
            )
            stats["published"] += 1
        elif status.state == "failed":
            if not task_id.startswith("neobund:") and scheduled_for + grace > now:
                stats["pending"] += 1
                continue
            db.mark_publish_result(
                canonical_script_key=str(task["canonical_script_key"] or ""),
                script_id=str(task["script_id"]),
                publish_task_id=task_id,
                schedule_status="发布失败",
                publish_status="发布失败",
                publish_result="发布失败",
                error_message=status.error_message,
            )
            stats["failed"] += 1
        else:
            if str(task["publish_task_id"] or "").startswith("neobund:") and str(task["schedule_status"] or "") == "已取消":
                db.assign_slot(
                    slot_id=int(task["slot_id"]),
                    canonical_script_key=str(task["canonical_script_key"] or ""),
                    script_id=str(task["script_id"]),
                    publish_task_id=task_id,
                    account_id=str(task["account_id"] or ""),
                    account_name=str(task["account_name"] or ""),
                    planned_publish_at=scheduled_for,
                )
            stats["pending"] += 1
    return stats
