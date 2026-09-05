"""Retryable Feishu projection of durable local publishing state.

Nothing here submits or retries a remote publishing task. If a Feishu batch
fails, the next run recomputes the diff from the database and safely resumes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List


_FIELDS = {
    "发布状态": "publish_status", "发布任务ID": "publish_task_id",
    "计划发布时间": "planned_publish_at", "发布时间": "published_at",
    "分配账号ID": "account_id", "分配账号名称": "account_name",
    "发布结果": "publish_result",
    "错误信息": "error_message",
}
_DATES = {"计划发布时间", "发布时间"}
_CHINA_TZ = timezone(timedelta(hours=8))


def load_projection_rows(db: Any) -> List[Dict[str, Any]]:
    """Read only; favor the frozen task over unrelated newer cancelled slots."""
    with db._connect() as conn:
        rows = conn.execute("""
            SELECT va.*, sm.audio_mode, sm.cart_enabled, sm.publish_purpose,
                   sm.script_source, sm.content_branch,
                   COALESCE(ps.bgm_json, '') AS bgm_json,
                   COALESCE(ps.schedule_status, va.publish_status) AS schedule_status,
                   COALESCE(ps.error_message, va.error_message, '') AS bgm_error,
                   COALESCE(NULLIF(ps.channel_override, ''), ac.publish_channel, '') AS publish_channel,
                   ps.publish_task_id AS slot_task_id, ps.scheduled_for AS slot_time,
                   ps.account_id AS slot_account_id, ps.account_name AS slot_account_name
            FROM video_assets va
            JOIN script_metadata sm ON sm.canonical_script_key = va.canonical_script_key
            LEFT JOIN publish_slots ps ON ps.slot_id = COALESCE((
                SELECT frozen.slot_id FROM publish_slots frozen
                WHERE frozen.canonical_script_key = va.canonical_script_key
                  AND COALESCE(va.publish_task_id, '') <> ''
                  AND frozen.publish_task_id = va.publish_task_id
                ORDER BY frozen.updated_at DESC, frozen.slot_id DESC LIMIT 1
            ), (
                SELECT latest.slot_id FROM publish_slots latest
                WHERE latest.canonical_script_key = va.canonical_script_key
                ORDER BY latest.updated_at DESC, latest.slot_id DESC LIMIT 1
            ))
            LEFT JOIN account_configs ac ON ac.account_id = COALESCE(ps.account_id, va.account_id)
            WHERE COALESCE(va.run_manager_record_id, '') <> ''
        """).fetchall()
    result = []
    for record in rows:
        row = dict(record)
        row["error_message"] = row.pop("bgm_error")
        # A task ID is never fabricated from a pending/failed slot.
        if not row.get("publish_task_id") and row.get("slot_task_id") and row.get("schedule_status") in {"已排期", "已发布"}:
            row.update(publish_task_id=row["slot_task_id"], publish_status=row["schedule_status"],
                       planned_publish_at=row["slot_time"], account_id=row["slot_account_id"],
                       account_name=row["slot_account_name"])
        result.append(row)
    return result


def _normal(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, list) and all(isinstance(part, dict) and "text" in part for part in value):
        return "".join(str(part["text"]) for part in value)
    return value


def normalize_field_specs(fields: Iterable[Any]) -> List[Dict[str, Any]]:
    return [dict(field) if isinstance(field, dict) else {
        "field_name": field.field_name, "type": field.field_type,
        "property": getattr(field, "property", None),
    } for field in fields]


def write_changed_records(client: Any, updates: Iterable[Dict[str, Any]], *, records: Any = None) -> Dict[str, int]:
    if records is None:
        records = client.list_records(page_size=500)
    live = {}
    for record in records:
        if isinstance(record, dict):
            live[record["record_id"]] = record.get("fields", {})
        else:
            live[record.record_id] = record.fields
    changed, missing = [], 0
    for update in updates:
        record_id = update["record_id"]
        if record_id not in live:
            missing += 1
            continue
        fields = {key: value for key, value in update["fields"].items()
                  if _normal(value) != _normal(live[record_id].get(key))}
        if fields:
            changed.append({"record_id": record_id, "fields": fields})
    if changed:
        client.batch_update_records(changed)
    return {"records_updated": len(changed), "records_deleted_skipped": missing}


def _field_value(name: str, value: Any, spec: Dict[str, Any]) -> Any:
    field_type = int(spec.get("type") or 1)
    if field_type == 3 and not value:
        return None
    if name in _DATES and field_type == 5:
        if not value:
            return None
        date = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if date.tzinfo is None:
            date = date.replace(tzinfo=_CHINA_TZ)
        return int(date.timestamp() * 1000)
    return str(value or "")


def sync_run_manager_statuses(client: Any, db: Any, records: Any = None) -> Dict[str, int]:
    """One field read, one record read, one merged differential batch."""
    from app.bgm_reporting import build_bgm_status_updates, ensure_bgm_fields

    specs = normalize_field_specs(client.list_fields())
    field_map = {spec["field_name"]: spec for spec in specs}
    rows = load_projection_rows(db)
    bgm_updates = build_bgm_status_updates(rows)
    created = ensure_bgm_fields(client, specs) if bgm_updates else []
    merged = {row["run_manager_record_id"]: {} for row in rows}
    skipped_fields = 0
    for row in rows:
        fields = merged[row["run_manager_record_id"]]
        for name, column in _FIELDS.items():
            spec = field_map.get(name)
            if not spec or int(spec.get("type") or 1) not in {1, 3, 5}:
                continue
            try:
                fields[name] = _field_value(name, row.get(column), spec)
            except (ValueError, TypeError, OverflowError):
                skipped_fields += 1
    for update in bgm_updates:
        merged[update["record_id"]].update(update["fields"])
    updates = [{"record_id": record_id, "fields": fields} for record_id, fields in merged.items() if fields]
    stats = write_changed_records(client, updates, records=records)
    return {"fields_created": len(created), "invalid_fields_skipped": skipped_fields, **stats}
