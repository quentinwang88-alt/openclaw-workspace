"""Read-only adapter for structured outfit templates shared by video flows.

The adapter intentionally does not select or read the template title, free-form
body, prompt core, notes, scene, persona, or action fields.  It exposes a small
normalized contract; the original-script allocator remains the selection
authority and always keeps its internal profiles as a fallback.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


PROVIDER_VERSION = "shared-outfit-provider-v1-structured-only"
DEFAULT_DB_PATH = (
    Path(__file__).resolve().parents[2]
    / "lightweight-tryon-video"
    / "var"
    / "light_tryon.sqlite3"
)

# Deliberately exhaustive: a future column cannot silently enter the contract.
STRUCTURED_COLUMNS = (
    "styling_id",
    "status",
    "applicable_product_codes",
    "applicable_product_type",
    "product_fit",
    "bottom_type",
    "bottom_color",
    "bottom_fit",
    "inner_type",
    "inner_color",
    "inner_requirements",
    "accessory_level",
    "footwear_visibility",
    "vibe_tag",
    "config_version",
    "priority",
)
IGNORED_UNSTRUCTURED_FIELDS = (
    "styling_name",
    "prompt_core",
    "notes",
    "suitable_color_rules",
    "forbidden_pairings",
)


_TYPE_ALIASES = {
    "外套": "outerwear",
    "夹克": "outerwear",
    "上衣": "top",
    "短上衣": "top",
    "t恤": "tshirt",
    "衬衫": "shirt",
    "针织": "knit_top",
    "针织衫": "knit_top",
    "背心": "tank_top",
    "吊带": "tank_top",
    "连衣裙": "dress",
    "套装": "set",
    "裤装": "pants",
    "裙装": "skirt",
    "家居服": "homewear",
}

_BOTTOM_LABELS = {
    "wide_leg_pants": "高腰阔腿裤",
    "straight_jeans": "直筒牛仔裤",
    "white_shorts": "白色短裤",
    "casual_shorts": "休闲短裤",
    "midi_skirt": "简洁半裙",
    "matching_set_bottom": "同款同色系下装",
}
_ACCESSORY_LABELS = {
    "none": "不额外增加配饰",
    "minimal": "只保留轻量配饰",
    "normal": "使用普通日常配饰",
    "无配饰": "不额外增加配饰",
    "轻量配饰": "只保留轻量配饰",
    "正常配饰": "使用普通日常配饰",
}
_FOOTWEAR_LABELS = {
    "not_required": "鞋子不要求入镜",
    "optional": "鞋子可自然入镜",
    "required": "鞋子需要完整入镜",
    "不要求入镜": "鞋子不要求入镜",
    "可以入镜": "鞋子可自然入镜",
    "必须入镜": "鞋子需要完整入镜",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    if value in (None, ""):
        return []
    text = _text(value)
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        parsed = None
    if isinstance(parsed, list):
        return [_text(item) for item in parsed if _text(item)]
    for separator in ("，", "；", ";", "\n", "\r"):
        text = text.replace(separator, ",")
    return [item.strip() for item in text.split(",") if item.strip()]


def _canonical_product_type(value: Any) -> str:
    text = _text(value).lower()
    return _TYPE_ALIASES.get(text, text)


def _provider_enabled() -> bool:
    value = _text(os.environ.get("ORIGINAL_SCRIPT_SHARED_OUTFIT_PROVIDER_ENABLED", "1")).lower()
    return value not in {"0", "false", "off", "no", "disabled"}


def _resolve_db_path(db_path: Optional[str | Path]) -> Path:
    configured = _text(os.environ.get("ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_DB_PATH"))
    return Path(db_path or configured or DEFAULT_DB_PATH).expanduser().resolve()


def _stale_after_hours() -> float:
    raw = _text(os.environ.get("ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_STALE_HOURS", "24"))
    try:
        return max(1.0, float(raw))
    except (TypeError, ValueError):
        return 24.0


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed


def get_outfit_template_provider_snapshot(
    *, db_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Describe local template freshness without refreshing or blocking work.

    ``last_synced_at`` is maintained by the lightweight-video Feishu sync and
    is the only refresh authority used here.  The original-script flow remains
    read-only: stale or unavailable data is reported, never treated as a
    generation failure.
    """
    stale_hours = _stale_after_hours()
    snapshot: Dict[str, Any] = {
        "provider_version": PROVIDER_VERSION,
        "enabled": _provider_enabled(),
        "refresh_status": "UNKNOWN",
        "last_refreshed_at": "",
        "source_updated_at": "",
        "template_count": 0,
        "enabled_template_count": 0,
        "stale_after_hours": stale_hours,
        "soft_warnings": [],
    }
    if not snapshot["enabled"]:
        snapshot["refresh_status"] = "DISABLED"
        return snapshot

    path = _resolve_db_path(db_path)
    if not path.exists():
        snapshot["refresh_status"] = "UNAVAILABLE"
        return snapshot
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
        existing = {
            _text(row[1]) for row in conn.execute("PRAGMA table_info(styling_templates)").fetchall()
        }
        if not existing:
            conn.close()
            snapshot["refresh_status"] = "UNAVAILABLE"
            return snapshot
        snapshot["template_count"] = int(
            conn.execute("SELECT COUNT(*) FROM styling_templates").fetchone()[0] or 0
        )
        if "status" in existing:
            snapshot["enabled_template_count"] = int(
                conn.execute(
                    "SELECT COUNT(*) FROM styling_templates WHERE status = ?", ("enabled",)
                ).fetchone()[0] or 0
            )
        if "last_synced_at" in existing:
            snapshot["last_refreshed_at"] = _text(
                conn.execute("SELECT MAX(last_synced_at) FROM styling_templates").fetchone()[0]
            )
        if "updated_at" in existing:
            snapshot["source_updated_at"] = _text(
                conn.execute("SELECT MAX(updated_at) FROM styling_templates").fetchone()[0]
            )
        conn.close()
    except (OSError, sqlite3.Error):
        snapshot["refresh_status"] = "UNAVAILABLE"
        return snapshot

    refreshed_at = _parse_datetime(snapshot["last_refreshed_at"])
    if refreshed_at is None:
        snapshot["refresh_status"] = "UNKNOWN"
        return snapshot
    age_hours = max(
        0.0,
        (datetime.now().astimezone() - refreshed_at.astimezone()).total_seconds() / 3600.0,
    )
    snapshot["refresh_age_hours_at_plan"] = round(age_hours, 2)
    if age_hours > stale_hours:
        snapshot["refresh_status"] = "STALE"
        snapshot["soft_warnings"] = [
            "OUTFIT_TEMPLATE_SNAPSHOT_STALE:建议先执行“刷新穿搭模板”；本批次仍可继续并自动降级"
        ]
    else:
        snapshot["refresh_status"] = "FRESH"
    return snapshot


def _structured_hash(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _normalized_bottom_fit(bottom_type: str, values: List[str]) -> tuple[List[str], List[str]]:
    """Resolve only obvious structural contradictions, without model judgment."""
    warnings: List[str] = []
    result = list(values)
    if bottom_type == "wide_leg_pants":
        conflicts = {"直筒", "修身", "straight", "slim"}
        removed = [item for item in result if item.lower() in conflicts or item in conflicts]
        if removed:
            result = [item for item in result if item not in removed]
            warnings.append("bottom_fit_conflict_removed:" + ",".join(removed))
    return result, warnings


def _base_outfit_direction(row: Dict[str, Any]) -> tuple[str, List[str]]:
    pieces: List[str] = []
    warnings: List[str] = []
    inner_type = _text(row.get("inner_type"))
    inner_color = _text(row.get("inner_color"))
    inner_requirements = _text(row.get("inner_requirements"))
    if inner_type or inner_color:
        inner = "".join(part for part in (inner_color, inner_type) if part)
        pieces.append(f"内搭使用{inner}")
    if inner_requirements:
        pieces.append(f"内搭要求：{inner_requirements}")

    bottom_type = _text(row.get("bottom_type"))
    bottom_label = _BOTTOM_LABELS.get(bottom_type, bottom_type)
    bottom_colors = _list(row.get("bottom_color"))
    bottom_fits, fit_warnings = _normalized_bottom_fit(bottom_type, _list(row.get("bottom_fit")))
    warnings.extend(fit_warnings)
    if bottom_label:
        # The structured type may already contain a fit word (for example
        # “高腰阔腿裤”).  Avoid mechanically repeating the same slot value.
        non_repeating_fits = [item for item in bottom_fits if item not in bottom_label]
        modifiers = [*bottom_colors, *non_repeating_fits]
        pieces.append(f"下装使用{'、'.join(modifiers)}的{bottom_label}" if modifiers else f"下装使用{bottom_label}")

    accessory = _ACCESSORY_LABELS.get(_text(row.get("accessory_level")), "")
    footwear = _FOOTWEAR_LABELS.get(_text(row.get("footwear_visibility")), "")
    if accessory:
        pieces.append(accessory)
    if footwear:
        pieces.append(footwear)
    vibes = _list(row.get("vibe_tag"))
    if vibes:
        pieces.append(f"整体保持{'、'.join(vibes)}的结构化风格方向")
    pieces.append("目标商品始终是主展示单品，穿搭不得遮挡或改写商品结构")
    return "；".join(pieces), warnings


def _silhouette_key(row: Dict[str, Any]) -> str:
    bottom_type = _text(row.get("bottom_type")) or "PRODUCT_LED"
    return "TEMPLATE_" + bottom_type.upper()


def load_structured_outfit_templates(
    *,
    product_code: str,
    product_type: str,
    db_path: Optional[str | Path] = None,
) -> List[Dict[str, Any]]:
    """Return eligible shared templates, or an empty list on any provider issue.

    Eligibility is explicit: exact product code or ``*``.  A blank product-code
    field is private to the lightweight flow and is not exposed here.
    """
    if not _provider_enabled():
        return []
    code = _text(product_code)
    if not code:
        return []
    path = _resolve_db_path(db_path)
    if not path.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
        existing = {
            _text(row[1]) for row in conn.execute("PRAGMA table_info(styling_templates)").fetchall()
        }
        if not set(STRUCTURED_COLUMNS).issubset(existing):
            conn.close()
            return []
        sql = (
            "SELECT " + ", ".join(STRUCTURED_COLUMNS)
            + " FROM styling_templates WHERE status = ? ORDER BY priority DESC, styling_id ASC"
        )
        rows = [dict(row) for row in conn.execute(sql, ("enabled",)).fetchall()]
        conn.close()
    except (OSError, sqlite3.Error):
        return []

    requested_type = _canonical_product_type(product_type)
    candidates: List[Dict[str, Any]] = []
    for row in rows:
        codes = _list(row.get("applicable_product_codes"))
        if code in codes:
            match_scope = "EXACT_PRODUCT_CODE"
            match_rank = 0
        elif "*" in codes:
            match_scope = "GENERIC_WILDCARD"
            match_rank = 1
        else:
            continue
        product_types = [_canonical_product_type(item) for item in _list(row.get("applicable_product_type"))]
        if product_types and "*" not in product_types and requested_type not in product_types:
            continue

        base_direction, warnings = _base_outfit_direction(row)
        structured_payload = {
            key: (_list(row.get(key)) if key in {
                "applicable_product_codes", "applicable_product_type", "product_fit",
                "bottom_color", "bottom_fit", "vibe_tag",
            } else row.get(key))
            for key in STRUCTURED_COLUMNS
        }
        candidates.append({
            "provider_version": PROVIDER_VERSION,
            "source_type": "LIGHTWEIGHT_TEMPLATE",
            "template_id": _text(row.get("styling_id")),
            "template_version": _text(row.get("config_version")) or "UNVERSIONED",
            "match_scope": match_scope,
            "match_rank": match_rank,
            "priority": int(row.get("priority") or 0),
            "applicable_product_codes": codes,
            "applicable_product_type": product_types,
            "product_fit": _list(row.get("product_fit")),
            "bottom_type": _text(row.get("bottom_type")),
            "bottom_color": _list(row.get("bottom_color")),
            "bottom_fit": _list(row.get("bottom_fit")),
            "inner_type": _text(row.get("inner_type")),
            "inner_color": _text(row.get("inner_color")),
            "inner_requirements": _text(row.get("inner_requirements")),
            "accessory_level": _text(row.get("accessory_level")),
            "footwear_visibility": _text(row.get("footwear_visibility")),
            "vibe_tag": _list(row.get("vibe_tag")),
            "silhouette_key": _silhouette_key(row),
            "style_family": (_list(row.get("vibe_tag")) or ["STRUCTURED_TEMPLATE"])[0],
            "hair_direction": "不由穿搭模板决定",
            "base_outfit_direction": base_direction,
            "normalization_warnings": warnings,
            "structured_snapshot_hash": _structured_hash(structured_payload),
            "structured_fields_used": list(STRUCTURED_COLUMNS),
            "ignored_unstructured_fields": list(IGNORED_UNSTRUCTURED_FIELDS),
        })
    return candidates
