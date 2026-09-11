"""Small, operator-facing BGM status projection for the run-manager table."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List

from app.script_pool import resolve_cart


BGM_FIELD_SPECS = {
    "BGM策略": (1, "Text", None),
    "BGM状态": (
        3,
        "SingleSelect",
        {"options": [{"name": name} for name in (
            "不适用", "待发布前选曲", "已选待发布", "待平台回读",
            "已确认一致", "实际BGM不一致", "选曲失败", "平台自动推荐",
            "已混入成片",
        )]},
    ),
    "选中BGM": (1, "Text", None),
    "实际BGM": (1, "Text", None),
    "BGM节奏方案": (1, "Text", None),
}

_SUPPORTED_AUDIO_MODES = {
    "silent_source_platform_bgm", "platform_auto_bgm",
    "generated_nonvoice", "clean_voice",
}


def ensure_bgm_fields(client: Any, fields: Any = None) -> List[str]:
    """Create only read-only observability fields; there is no manual gate."""
    from app.publish_writeback import normalize_field_specs
    normalized = normalize_field_specs(fields) if fields is not None else []
    existing = {item["field_name"] for item in normalized} if fields is not None else set(client.list_field_names())
    created: List[str] = []
    for name, (field_type, ui_type, property_) in BGM_FIELD_SPECS.items():
        if name not in existing:
            client.create_field(name, field_type=field_type, ui_type=ui_type, property=property_)
            created.append(name)
    status_field = next((item for item in normalized if item["field_name"] == "BGM状态"), None)
    if status_field and int(status_field.get("type") or 0) == 3:
        current_options = list((status_field.get("property") or {}).get("options") or [])
        current_names = {str(item.get("name") or "") for item in current_options}
        desired_names = [
            str(item.get("name") or "")
            for item in (BGM_FIELD_SPECS["BGM状态"][2] or {}).get("options", [])
        ]
        missing_options = [{"name": name} for name in desired_names if name and name not in current_names]
        if current_options and missing_options and callable(getattr(client, "update_field", None)):
            client.update_field(
                status_field.get("field_id"), field_name="BGM状态", field_type=3,
                property={"options": current_options + missing_options},
            )
            created.append("BGM状态:option")
    return created


def _compact_track(payload: Dict[str, Any]) -> str:
    music_id = str(payload.get("music_id") or "").strip()
    title = str(payload.get("music_title") or payload.get("title") or "").strip()
    author = str(payload.get("music_author") or payload.get("author") or "").strip()
    parts = [part for part in (title, author, f"ID:{music_id}" if music_id else "") if part]
    return " · ".join(parts)[:500]


def _fields_for_row(row: Any) -> Dict[str, Any]:
    try:
        audit = json.loads(str(row["bgm_json"] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        audit = {}
    if not isinstance(audit, dict):
        audit = {}
    actual = audit.get("actual") if isinstance(audit.get("actual"), dict) else {}
    selected_id = str(audit.get("music_id") or "").strip()
    actual_id = str(actual.get("music_id") or "").strip()
    error = str(row["error_message"] or "")
    platform_auto = audit.get("mode") == "platform_auto"
    local_mix = audit.get("mode") == "local_mix"
    if platform_auto:
        status = "平台自动推荐"
    elif local_mix and selected_id and actual_id == selected_id:
        status = "已混入成片"
    elif selected_id:
        if actual_id:
            status = "已确认一致" if selected_id == actual_id else "实际BGM不一致"
        elif str(row["schedule_status"] or "") == "已发布":
            status = "待平台回读"
        else:
            status = "已选待发布"
    elif "BGM" in error and ("失败" in error or "error" in error.lower()):
        status = "选曲失败"
    else:
        status = "待发布前选曲"
    profile = audit.get("profile") if isinstance(audit.get("profile"), dict) else {}
    timing = audit.get("timing_plan") if isinstance(audit.get("timing_plan"), dict) else {}
    rhythm = str(profile.get("rhythm_preference") or (
        "由平台决定" if platform_auto else "待实际选曲"
    ))
    sync_mode = str(timing.get("mode") or profile.get("sync_mode") or "")
    timing_text = " · ".join(part for part in (rhythm, sync_mode) if part)[:500]
    return {
        "BGM策略": (
            "TikTok自动推荐音乐" if platform_auto
            else "发布前本地混入" if local_mix
            else "自动平台BGM"
        ),
        "BGM状态": status,
        "选中BGM": _compact_track({
            "music_id": selected_id, "music_title": audit.get("music_title"),
            "music_author": audit.get("music_author"),
        }),
        "实际BGM": _compact_track(actual),
        "BGM节奏方案": timing_text,
    }


def build_bgm_status_updates(rows: Iterable[Any]) -> List[Dict[str, Any]]:
    updates: List[Dict[str, Any]] = []
    for row in rows:
        row = dict(row)
        record_id = str(row["run_manager_record_id"] or "").strip()
        audio_mode = str(row["audio_mode"] or "").strip()
        has_bgm = bool(str(row["bgm_json"] or "").strip())
        if not record_id or (audio_mode not in _SUPPORTED_AUDIO_MODES and not has_bgm):
            continue
        try:
            organic = resolve_cart(SimpleNamespace(**row)) == "否"
        except (TypeError, ValueError):
            organic = False
        markers = " ".join(str(row.get(key) or "") for key in ("script_source", "content_branch")).lower()
        channel = str(row.get("publish_channel") or "").lower()
        if not organic or "混剪" in markers or "mixcut" in markers or (
            channel and channel not in {"neobund", "creatok"}
        ):
            continue
        updates.append({"record_id": record_id, "fields": _fields_for_row(row)})
    return updates


def sync_bgm_statuses(client: Any, db: Any, records: Any = None) -> Dict[str, int]:
    from app.publish_writeback import load_projection_rows, write_changed_records

    fields = client.list_fields()
    created = ensure_bgm_fields(client, fields)
    updates = build_bgm_status_updates(load_projection_rows(db))
    stats = write_changed_records(client, updates, records=records)
    return {"fields_created": len(created), **stats}
