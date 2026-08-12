"""Read-only provider for approved creator persona templates.

The lightweight try-on database remains the only template store.  This module
exposes a deliberately small, versioned snapshot to the original-script
planner; titles, notes and unrelated brand settings never enter generation.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


PROVIDER_VERSION = "shared-persona-provider-v3-body-proportion"
DEFAULT_DB_PATH = (
    Path(__file__).resolve().parents[2]
    / "lightweight-tryon-video"
    / "var"
    / "light_tryon.sqlite3"
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = _text(value)
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return default


def _list(value: Any) -> List[Any]:
    parsed = _json(value, None)
    if isinstance(parsed, list):
        return parsed
    if isinstance(value, (tuple, set)):
        return list(value)
    text = _text(value)
    if not text:
        return []
    return [item.strip() for item in text.replace("，", ",").split(",") if item.strip()]


def _enabled() -> bool:
    value = _text(os.environ.get("ORIGINAL_SCRIPT_PERSONA_LIBRARY_ENABLED", "1")).lower()
    return value not in {"0", "false", "off", "no", "disabled"}


def _resolve_db_path(db_path: Optional[str | Path]) -> Path:
    configured = _text(os.environ.get("ORIGINAL_SCRIPT_PERSONA_TEMPLATE_DB_PATH"))
    return Path(db_path or configured or DEFAULT_DB_PATH).expanduser().resolve()


def _asset_id(value: Any) -> str:
    if isinstance(value, dict):
        return _text(
            value.get("file_token")
            or value.get("asset_id")
            or value.get("cached_path")
            or value.get("path")
            or value.get("url")
        )
    return _text(value)


def _stable_hash(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def load_persona_templates(
    *, db_path: Optional[str | Path] = None
) -> Dict[str, Any]:
    """Load configured personas that have an actual reference asset.

    Missing assets are reported, not converted into prompt-only personas.  A
    prompt-only identity would re-open the random-face behaviour this contract
    is intended to remove.
    """

    snapshot: Dict[str, Any] = {
        "provider_version": PROVIDER_VERSION,
        "enabled": _enabled(),
        "status": "UNAVAILABLE",
        "templates": [],
        "enabled_count": 0,
        "approved_asset_count": 0,
        "soft_warnings": [],
    }
    if not snapshot["enabled"]:
        snapshot["status"] = "DISABLED"
        return snapshot

    path = _resolve_db_path(db_path)
    snapshot["db_path"] = str(path)
    if not path.exists():
        snapshot["soft_warnings"] = ["PERSONA_TEMPLATE_DB_UNAVAILABLE"]
        return snapshot

    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
        columns = {
            _text(row[1])
            for row in conn.execute("PRAGMA table_info(persona_templates)").fetchall()
        }
        if not columns:
            conn.close()
            snapshot["soft_warnings"] = ["PERSONA_TEMPLATE_TABLE_UNAVAILABLE"]
            return snapshot
        selected_columns = [
            name
            for name in (
                "persona_id", "persona_name", "status", "gender", "age_group",
                "body_type", "hair_style", "hair_color", "skin_tone",
                "face_visibility", "makeup_style", "vibe", "prompt_core",
                "prompt_negative", "priority", "markets", "reference_images",
                "config_version", "consistency_version", "source_hash",
                "source_payload", "updated_at",
            )
            if name in columns
        ]
        rows = conn.execute(
            f"SELECT {','.join(selected_columns)} FROM persona_templates "
            "WHERE status IN (?, ?)",
            ("enabled", "testing"),
        ).fetchall()
        conn.close()
    except (OSError, sqlite3.Error) as exc:
        snapshot["soft_warnings"] = [f"PERSONA_TEMPLATE_READ_FAILED:{exc}"]
        return snapshot

    snapshot["enabled_count"] = len(rows)
    templates: List[Dict[str, Any]] = []
    missing_asset_ids: List[str] = []
    for raw in rows:
        row = dict(raw)
        source = _json(row.get("source_payload"), {})
        source = source if isinstance(source, dict) else {}
        references = _list(row.get("reference_images"))
        usable_references = [item for item in references if _asset_id(item)]
        if not usable_references:
            missing_asset_ids.append(_text(row.get("persona_id")))
            continue
        vibe = [str(item).strip() for item in _list(row.get("vibe")) if str(item).strip()]
        template = {
            "persona_id": _text(row.get("persona_id")),
            "persona_name": _text(row.get("persona_name")),
            "template_version": _text(
                row.get("config_version") or row.get("consistency_version") or "V1"
            ),
            "gender": _text(row.get("gender")),
            "age_group": _text(row.get("age_group")),
            "body_type": _text(row.get("body_type")),
            "hair_style": _text(row.get("hair_style")),
            "hair_color": _text(row.get("hair_color")),
            "skin_tone": _text(row.get("skin_tone")),
            "face_visibility": _text(row.get("face_visibility")),
            "makeup_style": _text(row.get("makeup_style")),
            "vibe_tags": vibe,
            "prompt_core": _text(row.get("prompt_core")),
            "prompt_negative": _text(row.get("prompt_negative")),
            "priority": int(row.get("priority") or 0),
            "markets": [str(item).strip() for item in _list(row.get("markets")) if str(item).strip()],
            "reference_images": usable_references,
            "reference_asset_ids": [_asset_id(item) for item in usable_references],
            "applicable_categories": [
                str(item).strip()
                for item in _list(source.get("applicable_categories"))
                if str(item).strip()
            ],
            "applicable_product_types": [
                str(item).strip()
                for item in _list(source.get("applicable_product_types"))
                if str(item).strip()
            ],
            "supported_demonstration_modes": [
                str(item).strip().upper()
                for item in _list(source.get("supported_demonstration_modes"))
                if str(item).strip()
            ],
            "supported_presentation_modes": [
                str(item).strip().upper()
                for item in _list(source.get("supported_presentation_modes"))
                if str(item).strip()
            ],
            "supported_capture_modes": [
                str(item).strip().upper()
                for item in _list(source.get("supported_capture_modes"))
                if str(item).strip()
            ],
            "framing_capabilities": [
                str(item).strip().upper()
                for item in _list(source.get("framing_capabilities"))
                if str(item).strip()
            ],
            "identity_text": _text(source.get("identity_text")),
            "appearance_text": _text(source.get("appearance_text")),
            "body_proportion_text": _text(source.get("body_proportion_text")),
            "hair_makeup_text": _text(source.get("hair_makeup_text")),
            "speaking_personality": _text(source.get("speaking_personality")),
            "source_hash": _text(row.get("source_hash")),
            "updated_at": _text(row.get("updated_at")),
        }
        template["structured_snapshot_hash"] = _stable_hash(template)
        templates.append(template)

    snapshot["templates"] = templates
    snapshot["approved_asset_count"] = sum(
        len(item.get("reference_asset_ids") or []) for item in templates
    )
    snapshot["status"] = "AVAILABLE" if templates else "UNAVAILABLE"
    if missing_asset_ids:
        snapshot["soft_warnings"].append(
            "PERSONA_REFERENCE_ASSET_MISSING:" + ",".join(missing_asset_ids)
        )
    return snapshot
