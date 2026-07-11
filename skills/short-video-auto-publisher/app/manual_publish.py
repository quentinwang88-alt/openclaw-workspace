#!/usr/bin/env python3
"""主动人工指定发布入口。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

from app.db import AutoPublishDB
from app.metadata import sanitize_title
from app.models import ScriptMetadata
from app.publishers import BasePublishAdapter
from app.scheduler import normalize_publish_channel, resolve_field_mapping


MANUAL_PUBLISH_FIELD_ALIASES: Dict[str, List[str]] = {
    "store_id": ["店铺", "店铺ID", "店铺 ID"],
    "account": ["计划发布账号", "发布账号", "账号", "账号ID", "计划发布账号ID"],
    "video_attachment": ["短视频上传", "视频上传", "视频附件", "短视频附件"],
    "short_video_title": ["短视频标题", "视频标题", "发布标题"],
    "scheduled_for": ["短视频发布时间", "发布时间", "计划发布时间"],
    "publish_date": ["发布日期", "发布日期", "计划发布日期"],
    "publish_channel": ["发布渠道", "发布通道", "发布平台"],
    "mark_ai": ["是否AI", "是否AI生成", "是否为AI生成", "AI生成内容"],
    "product_id": ["产品ID", "商品ID", "TikTok商品ID", "TikTok Shop商品ID"],
    "product_title": ["产品标题", "商品标题"],
    "request_status": ["处理状态"],
    "publish_task_id": ["发布任务ID"],
    "error_message": ["错误信息"],
    "script_id": ["内部脚本ID", "脚本ID"],
}


MANUAL_PUBLISH_FIELD_SPECS: Sequence[Dict[str, Any]] = (
    {
        "name": "处理状态",
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {"options": [{"name": name} for name in ("待创建", "待补充", "已创建", "已发布", "发布失败", "已取消")]},
    },
    {
        "name": "是否AI",
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {"options": [{"name": name} for name in ("是", "否")]},
    },
    {"name": "发布任务ID", "type": 1, "ui_type": "Text"},
    {"name": "错误信息", "type": 1, "ui_type": "Text"},
    {"name": "内部脚本ID", "type": 1, "ui_type": "Text"},
    {"name": "产品ID", "type": 1, "ui_type": "Text"},
)


@dataclass(frozen=True)
class ManualPublishRequest:
    record_id: str
    store_id: str
    account_id: str
    account_name: str
    video_attachment: Dict[str, Any]
    short_video_title: str
    scheduled_for: datetime
    publish_channel: str
    mark_ai: Optional[bool] = None
    product_id: str = ""
    product_title: str = ""
    script_id: str = ""
    canonical_script_key: str = ""


def ensure_manual_publish_fields(client: Any) -> Dict[str, int]:
    existing = {item.field_name for item in client.list_fields()}
    created = 0
    for spec in MANUAL_PUBLISH_FIELD_SPECS:
        if spec["name"] in existing:
            continue
        client.create_field(
            field_name=spec["name"],
            field_type=int(spec["type"]),
            ui_type=str(spec["ui_type"]),
            property=spec.get("property"),
        )
        created += 1
        existing.add(spec["name"])
    return {"created_fields": created, "existing_fields": len(existing)}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            text = _text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_text(item) for item in value]
        return " ".join(part for part in parts if part).strip()
    return str(value).strip()


def _first_attachment(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict) and value.get("file_token"):
        return value
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item.get("file_token"):
                return item
    return None


def _is_blank_record(fields: Dict[str, Any], mapping: Dict[str, Optional[str]]) -> bool:
    keys = ("store_id", "account", "video_attachment", "short_video_title", "scheduled_for", "product_id")
    return not any(_text(fields.get(mapping.get(key))) for key in keys)


def _parse_publish_date(value: Any) -> Optional[date]:
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000
        return datetime.fromtimestamp(raw).date()

    text = _text(value)
    if not text:
        return None
    if text.isdigit():
        return _parse_publish_date(int(text))
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _with_selected_date(value: datetime, selected_date: Optional[date]) -> datetime:
    if selected_date is None:
        return value
    return value.replace(year=selected_date.year, month=selected_date.month, day=selected_date.day)


def _parse_publish_time(
    value: Any,
    *,
    now: Optional[datetime] = None,
    selected_date: Optional[date] = None,
) -> Optional[datetime]:
    current = now or datetime.now()
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000
        return _with_selected_date(datetime.fromtimestamp(raw).replace(second=0, microsecond=0), selected_date)

    text = _text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            return _with_selected_date(datetime.strptime(text, fmt), selected_date)
        except ValueError:
            pass
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if selected_date is not None:
            return datetime.combine(selected_date, parsed.time()).replace(second=0, microsecond=0)
        candidate = current.replace(hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0)
        if candidate <= current:
            candidate += timedelta(days=1)
        return candidate
    return None


def _resolve_account(db: AutoPublishDB, account_text: str):
    text = str(account_text or "").strip()
    if not text:
        return None
    account = db.get_account_config(text)
    if account is not None:
        return account
    return db.get_account_config_by_name(text)


def _safe_file_suffix(file_name: str) -> str:
    suffix = Path(str(file_name or "")).suffix.lower()
    if suffix in {".mp4", ".mov", ".m4v"}:
        return suffix
    return ".mp4"


def _parse_ai_marker(value: Any) -> Optional[bool]:
    """Return a tri-state value so blank cells preserve the platform default."""
    text = _text(value).strip().lower()
    if not text:
        return None
    if text in {"是", "yes", "y", "true", "1", "ai", "ai生成", "aigenerated"}:
        return True
    if text in {"否", "no", "n", "false", "0", "非ai", "非ai生成", "not ai"}:
        return False
    return None


def _download_manual_video(client: Any, attachment: Dict[str, Any], *, script_id: str, video_dir: Path) -> str:
    video_dir.mkdir(parents=True, exist_ok=True)
    content, file_name, _, _ = client.download_attachment_bytes(attachment)
    target = video_dir / f"{script_id}{_safe_file_suffix(file_name)}"
    target.write_bytes(content)
    return str(target)


def _write_record_status(
    client: Any,
    record_id: str,
    *,
    status: str,
    task_id: str = "",
    error: str = "",
    script_id: str = "",
) -> None:
    fields: Dict[str, Any] = {"处理状态": status, "错误信息": error}
    if task_id:
        fields["发布任务ID"] = task_id
    if script_id:
        fields["内部脚本ID"] = script_id
    try:
        client.update_record_fields(record_id, fields)
    except Exception:
        if status != "待补充":
            raise
        fields["处理状态"] = "待创建"
        client.update_record_fields(record_id, fields)


def _request_from_record(
    record: Any,
    mapping: Dict[str, Optional[str]],
    db: AutoPublishDB,
    *,
    now: Optional[datetime] = None,
) -> tuple[Optional[ManualPublishRequest], str]:
    fields = record.fields or {}
    account_text = _text(fields.get(mapping.get("account")))
    account = _resolve_account(db, account_text)
    if account is None:
        return None, f"未找到发布账号：{account_text or '-'}"

    store_id = _text(fields.get(mapping.get("store_id"))) or str(account["store_id"] or "").strip()
    account_store_id = str(account["store_id"] or "").strip()
    if store_id and account_store_id and store_id != account_store_id:
        return None, f"店铺与账号不匹配：表格店铺={store_id}，账号店铺={account_store_id}"

    attachment = _first_attachment(fields.get(mapping.get("video_attachment")))
    if not attachment:
        return None, "缺少短视频上传附件"

    title = sanitize_title(fields.get(mapping.get("short_video_title")))
    if not title:
        return None, "缺少短视频标题"

    publish_date_value = fields.get(mapping.get("publish_date"))
    publish_date = _parse_publish_date(publish_date_value)
    if _text(publish_date_value) and publish_date is None:
        return None, "发布日期无法识别"

    scheduled_for = _parse_publish_time(
        fields.get(mapping.get("scheduled_for")),
        now=now,
        selected_date=publish_date,
    )
    if scheduled_for is None:
        return None, "短视频发布时间无法识别"
    if publish_date is not None and scheduled_for <= (now or datetime.now()):
        return None, "发布日期和短视频发布时间已过期"

    channel = normalize_publish_channel(fields.get(mapping.get("publish_channel")))
    if channel in {"手动", "暂停"}:
        return None, f"发布渠道不可用于自动创建任务：{channel}"

    script_id = _text(fields.get(mapping.get("script_id"))) or f"manual_{record.record_id}"
    canonical_script_key = f"manual:{record.record_id}"
    return (
        ManualPublishRequest(
            record_id=record.record_id,
            store_id=store_id or account_store_id,
            account_id=str(account["account_id"] or "").strip(),
            account_name=str(account["account_name"] or "").strip() or str(account["account_id"] or "").strip(),
            video_attachment=attachment,
            short_video_title=title,
            scheduled_for=scheduled_for,
            publish_channel=channel,
            mark_ai=_parse_ai_marker(fields.get(mapping.get("mark_ai"))),
            product_id=_text(fields.get(mapping.get("product_id"))),
            product_title=_text(fields.get(mapping.get("product_title"))),
            script_id=script_id,
            canonical_script_key=canonical_script_key,
        ),
        "",
    )


def _create_task(
    publisher: BasePublishAdapter,
    *,
    request: ManualPublishRequest,
    local_file_path: str,
) -> str:
    create_for_channel = getattr(publisher, "create_scheduled_task_for_channel", None)
    kwargs = {
        "account_id": request.account_id,
        "video_path": local_file_path,
        "title": request.short_video_title,
        "publish_at": request.scheduled_for,
        "script_id": request.script_id,
        "product_id": request.product_id,
        "product_title": request.product_title,
        "mark_ai": request.mark_ai,
    }
    if callable(create_for_channel):
        return str(create_for_channel(channel=request.publish_channel, **kwargs)).strip()
    return str(publisher.create_scheduled_task(**kwargs)).strip()


def _is_retryable_create_error(error_message: str) -> bool:
    lowered = str(error_message or "").lower()
    markers = (
        "timed out",
        "timeout",
        "max retries exceeded",
        "proxyerror",
        "cannot connect to proxy",
        "remotedisconnected",
        "connection aborted",
        "connection reset",
        "connectionerror",
        "httpsconnectionpool",
        "read timed out",
        "temporarily unavailable",
        "s3",
        "ffmpeg",
    )
    return any(marker in lowered for marker in markers)


def _is_frame_rate_check_failed(error_message: str) -> bool:
    return "frame_rate_check_failed" in str(error_message or "").strip().lower()


def _create_task_with_retries(
    publisher: BasePublishAdapter,
    *,
    request: ManualPublishRequest,
    local_file_path: str,
    attempts: int,
    sleep_seconds: float,
) -> tuple[str, int]:
    max_attempts = max(1, int(attempts or 1))
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            task_id = _create_task(publisher, request=request, local_file_path=local_file_path)
            return task_id, attempt - 1
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts or not _is_retryable_create_error(str(exc)):
                raise
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError("发布平台未返回任务ID")


def sync_manual_publish_requests(
    records: Iterable[Any],
    mapping: Dict[str, Optional[str]],
    db: AutoPublishDB,
    publisher: BasePublishAdapter,
    *,
    client: Any,
    video_dir: Path,
    now: Optional[datetime] = None,
    create_retry_attempts: int = 3,
    retry_sleep_seconds: float = 3.0,
) -> Dict[str, int]:
    stats = {
        "records_checked": 0,
        "blank_skipped": 0,
        "processed_skipped": 0,
        "validation_failed": 0,
        "download_failed": 0,
        "create_failed": 0,
        "create_retried": 0,
        "created": 0,
        "conflicted": 0,
    }

    for record in records:
        stats["records_checked"] += 1
        fields = record.fields or {}
        if _is_blank_record(fields, mapping):
            stats["blank_skipped"] += 1
            continue

        status = _text(fields.get(mapping.get("request_status")))
        task_id = _text(fields.get(mapping.get("publish_task_id")))
        retry_frame_rate_failure = status == "发布失败" and _is_frame_rate_check_failed(
            _text(fields.get(mapping.get("error_message")))
        )
        if (status in {"已创建", "已发布", "已取消"} or task_id) and not retry_frame_rate_failure:
            slot = db.get_manual_publish_slot(record.record_id)
            if slot is not None:
                slot_status = _text(slot["schedule_status"])
                slot_task_id = _text(slot["publish_task_id"]) or task_id
                slot_error = _text(slot["error_message"])
                if slot_status == "已发布" and status != "已发布":
                    db.mark_manual_publish_request(record_id=record.record_id, request_status="已发布", publish_task_id=slot_task_id)
                    _write_record_status(client, record.record_id, status="已发布", task_id=slot_task_id, error="")
                elif slot_status == "发布失败" and status != "发布失败":
                    db.mark_manual_publish_request(
                        record_id=record.record_id,
                        request_status="发布失败",
                        publish_task_id=slot_task_id,
                        error_message=slot_error,
                    )
                    _write_record_status(client, record.record_id, status="发布失败", task_id=slot_task_id, error=slot_error)
            stats["processed_skipped"] += 1
            continue
        if status and status not in {"待创建", "待补充"} and not retry_frame_rate_failure:
            stats["processed_skipped"] += 1
            continue

        request, error = _request_from_record(record, mapping, db, now=now)
        if request is None:
            stats["validation_failed"] += 1
            _write_record_status(client, record.record_id, status="待补充", error=error)
            continue

        scheduled_text = request.scheduled_for.strftime("%Y-%m-%d %H:%M:%S")
        script_id = request.script_id
        try:
            local_file_path = _download_manual_video(client, request.video_attachment, script_id=script_id, video_dir=video_dir)
        except Exception as exc:
            stats["download_failed"] += 1
            error = f"下载短视频附件失败：{exc}"
            _write_record_status(client, record.record_id, status="发布失败", error=error, script_id=script_id)
            continue

        db.upsert_script_metadata(
            [
                ScriptMetadata(
                    canonical_script_key=request.canonical_script_key,
                    script_id=script_id,
                    source_record_id=request.record_id,
                    script_slot="MANUAL",
                    task_no=request.record_id,
                    store_id=request.store_id,
                    product_id=request.product_id,
                    parent_slot="MANUAL",
                    direction_label="人工指定发布",
                    variant_strength="人工上传",
                    target_country="",
                    product_type="",
                    content_family_key=request.canonical_script_key,
                    script_text="",
                    short_video_title=request.short_video_title,
                    title_source="manual_publish_request",
                    script_source="人工上传",
                    publish_purpose="人工发布",
                    cart_enabled="是" if request.product_id else "否",
                    content_branch="人工指定发布",
                )
            ]
        )
        db.upsert_video_asset(
            canonical_script_key=request.canonical_script_key,
            script_id=script_id,
            run_manager_record_id=request.record_id,
            video_source_type="manual_attachment",
            video_source_value=str(request.video_attachment.get("file_token") or ""),
            local_file_path=local_file_path,
            download_status="下载成功",
            run_video_status="人工上传",
            publish_status="待排期",
        )
        db.upsert_manual_publish_request(
            record_id=request.record_id,
            canonical_script_key=request.canonical_script_key,
            script_id=script_id,
            store_id=request.store_id,
            account_id=request.account_id,
            account_name=request.account_name,
            scheduled_for=scheduled_text,
            publish_channel=request.publish_channel,
            product_id=request.product_id,
            short_video_title=request.short_video_title,
            local_file_path=local_file_path,
        )

        existing_slot = db.get_slot_for_account_time(account_id=request.account_id, scheduled_for=scheduled_text)
        if existing_slot is not None:
            existing_status = _text(existing_slot["schedule_status"])
            existing_task_id = _text(existing_slot["publish_task_id"])
            existing_record_id = _text(existing_slot["manual_request_record_id"])
            if existing_status == "已发布" or (existing_status == "已排期" and existing_task_id and existing_record_id != request.record_id):
                stats["conflicted"] += 1
                error = f"已有远端发布任务冲突：任务ID={existing_task_id or '-'}，状态={existing_status}"
                db.mark_manual_publish_request(record_id=request.record_id, request_status="发布失败", error_message=error)
                _write_record_status(client, request.record_id, status="发布失败", error=error, script_id=script_id)
                continue

        try:
            publish_task_id, retries = _create_task_with_retries(
                publisher,
                request=request,
                local_file_path=local_file_path,
                attempts=create_retry_attempts,
                sleep_seconds=retry_sleep_seconds,
            )
            stats["create_retried"] += retries
            if not publish_task_id:
                raise RuntimeError("发布平台未返回任务ID")
        except Exception as exc:
            stats["create_failed"] += 1
            error = f"创建人工发布任务失败：{exc}"
            db.mark_manual_publish_request(record_id=request.record_id, request_status="发布失败", error_message=error)
            _write_record_status(client, request.record_id, status="发布失败", error=error, script_id=script_id)
            continue

        assignment = db.assign_manual_slot(
            record_id=request.record_id,
            store_id=request.store_id,
            account_id=request.account_id,
            account_name=request.account_name,
            scheduled_for=scheduled_text,
            canonical_script_key=request.canonical_script_key,
            script_id=script_id,
            publish_task_id=publish_task_id,
            title_override=request.short_video_title,
            channel_override=request.publish_channel,
        )
        if assignment.get("conflict"):
            stats["conflicted"] += 1
            error = str(assignment.get("error") or "已有远端发布任务冲突")
            db.mark_manual_publish_request(record_id=request.record_id, request_status="发布失败", error_message=error)
            _write_record_status(client, request.record_id, status="发布失败", error=error, script_id=script_id)
            continue

        stats["created"] += 1
        _write_record_status(client, request.record_id, status="已创建", task_id=publish_task_id, script_id=script_id)

    return stats
