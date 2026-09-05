"""Read-only adapter for structured outfit templates shared by video flows.

The adapter intentionally never sends the template title, free-form body,
prompt core, notes, or action fields to a model.  The title may be carried as
display-only metadata for the human workbench; it is excluded from the
structured snapshot and model-visible creative seed.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import sqlite3
from functools import lru_cache
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


PROVIDER_VERSION = "shared-outfit-provider-v7-identity-color-affinity"
DEFAULT_DB_PATH = (
    Path(__file__).resolve().parents[2]
    / "lightweight-tryon-video"
    / "var"
    / "light_tryon.sqlite3"
)

# Deliberately exhaustive: a future column cannot silently enter the contract.
BASE_STRUCTURED_COLUMNS = (
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
OPTIONAL_STRUCTURED_COLUMNS = (
    "target_role",
    "supported_demonstration_modes",
    "scene_families",
    "preferred_persona_ids",
    "style_intensity",
    "climate_profile",
    "silhouette_key",
    "outfit_recipe",
    "base_outfit_direction",
    "hair_direction",
    "neckline_direction",
    "outer_layer_direction",
    "palette_relation",
    "visibility_zones",
    "visibility_requirement",
    "finish_direction",
)
# Selection-only operator metadata; never send raw free-form rules to a model.
SELECTION_ONLY_COLUMNS = ("suitable_color_rules",)
STRUCTURED_COLUMNS = (*BASE_STRUCTURED_COLUMNS, *OPTIONAL_STRUCTURED_COLUMNS)
DISPLAY_ONLY_COLUMNS = ("styling_name",)
DISPLAY_ONLY_CONTRACT_FIELDS = frozenset({"template_display_name"})
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
    "丝巾": "silk_scarf",
    "围巾": "scarf",
    "秋冬围巾": "winter_scarf",
    "头巾": "headscarf",
    "发饰": "hair_accessory",
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
_SCENE_FAMILY_CODES = {
    "居家日常": "HOME_ROUTINE",
    "咖啡/品质室内": "CAFE_DINING",
    "街头/外出": "STREET_OUTING",
    "镜前/试穿": "VANITY_TRYON",
    "办公/通勤": "OFFICE_WORKBREAK",
    "乘车/等候": "CAR_TRANSIT",
}
_ONE_PIECE_TOKENS = ("连衣裙", "连体", "jumpsuit", "romper", "dress")


def _text(value: Any) -> str:
    return str(value or "").strip()


@lru_cache(maxsize=1)
def _product_code_normalizer():
    relative = Path("lightweight-tryon-video/scripts/light_tryon/product_codes.py")
    candidates = (
        Path(__file__).resolve().parents[2] / relative,
        Path.home() / ".openclaw/workspace/skills" / relative,
    )
    for path in candidates:
        if not path.is_file():
            continue
        spec = importlib.util.spec_from_file_location("shared_outfit_product_codes", path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.normalize_product_code
    # Standalone historical installations still read valid old IDs normally.
    return _text


def normalize_outfit_product_code(value: Any) -> str:
    return _product_code_normalizer()(value)


def outfit_product_color_tokens(value: Any, *, strict: bool = False) -> List[str]:
    """Reuse the existing colour vocabulary, narrowing its neutral bucket.

    Operator preferences accept only explicit colour lists. Product anchors
    may contain descriptive text, but only their already-observed fields are
    supplied by the caller. Unknown prose never becomes a new constraint.
    """
    from .original_batch_allocator import _COLOR_TOKEN_GROUPS, _semantic_text

    aliases = {
        token.lower(): group
        for group, tokens in _COLOR_TOKEN_GROUPS.items() for token in tokens
    }
    for group, tokens in {
        "WHITE": ("白色", "纯白", "正白", "white", "สีขาว"),
        "IVORY": ("米白", "奶白", "象牙白", "ivory", "cream", "สีครีม"),
        "BEIGE": ("米色", "杏色", "beige", "สีเบจ"),
        "KHAKI": ("卡其", "卡其色", "khaki", "สีกากี"),
        "YELLOW": ("黄色", "yellow", "สีเหลือง"),
    }.items():
        aliases.update({token: group for token in tokens})
    material = _semantic_text(value).strip().lower()
    if not material:
        return []
    if strict:
        parts = [part.strip() for part in re.split(r"[,，、/;；|\n]+", material) if part.strip()]
        if not parts or any(part not in aliases for part in parts):
            return []
        return sorted({aliases[part] for part in parts})
    # Longest matches prevent e.g. 卡其色 being split or 白色 within another
    # explicitly named shade from becoming an additional variant.
    pattern = "|".join(re.escape(token) for token in sorted(aliases, key=len, reverse=True))
    return sorted({aliases[match.group()] for match in re.finditer(pattern, material)})


def without_outfit_display_metadata(value: Any) -> Any:
    """Return a stable generation payload without human-only labels.

    Display names stay on the persisted contract for reports and Feishu, but
    renaming a template must not invalidate creative IDs, checkpoints, cache
    keys, script IDs, or first-frame assets.
    """

    if isinstance(value, Mapping):
        return {
            key: without_outfit_display_metadata(item)
            for key, item in value.items()
            if key not in DISPLAY_ONLY_CONTRACT_FIELDS
        }
    if isinstance(value, list):
        return [without_outfit_display_metadata(item) for item in value]
    if isinstance(value, tuple):
        return tuple(without_outfit_display_metadata(item) for item in value)
    return value


def _is_one_piece(value: Any) -> bool:
    text = _text(value).lower()
    return bool(text and any(token in text for token in _ONE_PIECE_TOKENS))


def _accessory_contract(value: Any) -> tuple[str, List[str], str]:
    raw = _text(value)
    label = _ACCESSORY_LABELS.get(raw, raw)
    if not raw or raw.lower() in {"none", "无配饰"}:
        return "NONE", [], label or "不额外增加配饰"
    if raw.lower() in {"minimal", "轻量配饰"} or "极简配饰" in raw:
        return "MINIMAL", [], label
    if raw.lower() in {"normal", "正常配饰"}:
        return "NORMAL", [], label
    return "SPECIFIED", [raw], raw


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


def _scene_family_codes(value: Any) -> List[str]:
    return list(dict.fromkeys(
        _SCENE_FAMILY_CODES.get(item, item.upper())
        for item in _list(value)
        if item
    ))


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
    mutually_exclusive_axes = (
        ({"高腰", "中腰", "低腰", "high_waist", "mid_waist", "low_waist"}, "waist"),
        ({"直筒", "阔腿", "宽松", "修身", "straight", "wide_leg", "loose", "slim"}, "leg_fit"),
    )
    for axis_values, axis_name in mutually_exclusive_axes:
        matches = [item for item in result if item.lower() in axis_values or item in axis_values]
        if len(matches) > 1:
            keep = matches[0]
            removed = matches[1:]
            result = [item for item in result if item not in removed]
            warnings.append(
                f"bottom_fit_{axis_name}_conflict_removed:" + ",".join(removed)
                + f";kept:{keep}"
            )
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
    one_piece = _is_one_piece(inner_type) or _is_one_piece(row.get("bottom_type"))
    if one_piece:
        one_piece_text = (
            "".join(part for part in (inner_color, inner_type) if part)
            if _is_one_piece(inner_type)
            else "".join([
                *(_list(row.get("bottom_color")) or ([inner_color] if inner_color else [])),
                _text(row.get("bottom_type")),
            ])
        )
        if one_piece_text:
            pieces.append(f"连体单品使用{one_piece_text}")
    elif inner_type or inner_color:
        inner = "".join(part for part in (inner_color, inner_type) if part)
        pieces.append(f"内搭使用{inner}")
    if inner_requirements:
        pieces.append(f"内搭要求：{inner_requirements}")

    bottom_type = _text(row.get("bottom_type"))
    bottom_label = _BOTTOM_LABELS.get(bottom_type, bottom_type)
    bottom_colors = _list(row.get("bottom_color"))
    bottom_fits, fit_warnings = _normalized_bottom_fit(bottom_type, _list(row.get("bottom_fit")))
    warnings.extend(fit_warnings)
    if bottom_label and not one_piece:
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
    explicit = _text(row.get("silhouette_key"))
    if explicit:
        return explicit
    bottom_type = _text(row.get("bottom_type")) or "PRODUCT_LED"
    return "TEMPLATE_" + bottom_type.upper()


def _outfit_recipe(row: Dict[str, Any]) -> Dict[str, str]:
    explicit = row.get("outfit_recipe")
    if isinstance(explicit, dict) and any(_text(value) for value in explicit.values()):
        return {str(key): _text(value) for key, value in explicit.items()}
    if isinstance(explicit, str) and explicit.strip():
        try:
            parsed = json.loads(explicit)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict) and any(_text(value) for value in parsed.values()):
            return {str(key): _text(value) for key, value in parsed.items()}
    inner = "".join(
        part for part in (_text(row.get("inner_color")), _text(row.get("inner_type")))
        if part
    )
    bottom_type = _text(row.get("bottom_type"))
    bottom_label = _BOTTOM_LABELS.get(bottom_type, bottom_type)
    bottom_parts = [*_list(row.get("bottom_color")), *_list(row.get("bottom_fit"))]
    bottom = "、".join([*bottom_parts, bottom_label] if bottom_label else bottom_parts)
    one_piece = _is_one_piece(row.get("inner_type")) or _is_one_piece(bottom_label)
    one_piece_text = (
        inner
        if _is_one_piece(row.get("inner_type"))
        else "".join([
            *(_list(row.get("bottom_color")) or ([_text(row.get("inner_color"))] if _text(row.get("inner_color")) else [])),
            bottom_label,
        ])
    )
    _, _, accessory_text = _accessory_contract(row.get("accessory_level"))
    return {
        "top": "" if one_piece else inner,
        "bottom": "" if one_piece else bottom,
        "one_piece": one_piece_text if one_piece else "",
        "footwear": _FOOTWEAR_LABELS.get(
            _text(row.get("footwear_visibility")),
            _text(row.get("footwear_visibility")),
        ),
        "bag": "",
        "other_accessories": accessory_text,
    }


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
    code = normalize_outfit_product_code(product_code)
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
        if not set(BASE_STRUCTURED_COLUMNS).issubset(existing):
            conn.close()
            return []
        structured_columns = [name for name in STRUCTURED_COLUMNS if name in existing]
        selected_columns = [
            *structured_columns,
            *(name for name in DISPLAY_ONLY_COLUMNS if name in existing),
            *(name for name in SELECTION_ONLY_COLUMNS if name in existing),
        ]
        sql = (
            "SELECT " + ", ".join(selected_columns)
            + " FROM styling_templates WHERE status = ? ORDER BY priority DESC, styling_id ASC"
        )
        rows = [dict(row) for row in conn.execute(sql, ("enabled",)).fetchall()]
        conn.close()
    except (OSError, sqlite3.Error):
        return []

    requested_type = _canonical_product_type(product_type)
    candidates: List[Dict[str, Any]] = []
    for row in rows:
        codes = list(dict.fromkeys(
            normalize_outfit_product_code(value)
            for value in _list(row.get("applicable_product_codes"))
        ))
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
        target_role = _text(row.get("target_role") or "TARGET_GARMENT").upper()
        supported_roles = [target_role] if target_role else ["TARGET_GARMENT"]
        if _text(row.get("base_outfit_direction")):
            base_direction = _text(row.get("base_outfit_direction"))
        structured_payload = {
            key: (_list(row.get(key)) if key in {
                "applicable_product_codes", "applicable_product_type", "product_fit",
                "bottom_color", "bottom_fit", "vibe_tag",
                "supported_demonstration_modes", "scene_families",
                "preferred_persona_ids", "visibility_zones",
            } else row.get(key))
            for key in structured_columns
        }
        structured_payload["applicable_product_codes"] = codes
        color_preferences = outfit_product_color_tokens(
            row.get("suitable_color_rules"), strict=True,
        )
        if color_preferences:
            structured_payload["product_color_preferences"] = color_preferences
        style_family = (_list(row.get("vibe_tag")) or ["STRUCTURED_TEMPLATE"])[0]
        outfit_structure = (
            "ONE_PIECE"
            if _is_one_piece(row.get("inner_type")) or _is_one_piece(row.get("bottom_type"))
            else "SEPARATES"
        )
        accessory_policy, accessory_items, _ = _accessory_contract(
            row.get("accessory_level")
        )
        candidates.append({
            "provider_version": PROVIDER_VERSION,
            "source_type": "LIGHTWEIGHT_TEMPLATE",
            "template_id": _text(row.get("styling_id")),
            "template_display_name": _text(row.get("styling_name")),
            "template_version": _text(row.get("config_version")) or "UNVERSIONED",
            "match_scope": match_scope,
            "match_rank": match_rank,
            "priority": int(row.get("priority") or 0),
            "product_color_preferences": color_preferences,
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
            "accessory_policy": accessory_policy,
            "accessory_items": accessory_items,
            "footwear_visibility": _text(row.get("footwear_visibility")),
            "vibe_tag": _list(row.get("vibe_tag")),
            "silhouette_key": _silhouette_key(row),
            "style_family": style_family,
            "target_role": target_role,
            "supported_target_roles": supported_roles,
            "supported_demonstration_modes": [
                item.upper() for item in _list(row.get("supported_demonstration_modes"))
            ],
            "scene_families": [
                item for item in _scene_family_codes(row.get("scene_families"))
            ],
            "preferred_persona_ids": list(dict.fromkeys(
                _list(row.get("preferred_persona_ids"))
            )),
            "style_intensity": _text(row.get("style_intensity") or "DAILY").upper(),
            "climate_profile": _text(row.get("climate_profile")).upper(),
            "outfit_recipe": _outfit_recipe(row),
            "outfit_structure": outfit_structure,
            "hair_direction": _text(row.get("hair_direction")) or "不由穿搭模板决定",
            "base_outfit_direction": base_direction,
            "neckline_direction": _text(row.get("neckline_direction")),
            "outer_layer_direction": _text(row.get("outer_layer_direction")),
            "palette_relation": _text(row.get("palette_relation")),
            "visibility_requirement": _text(row.get("visibility_requirement")),
            "visibility_zones": _list(row.get("visibility_zones")),
            "finish_direction": _text(row.get("finish_direction")),
            "normalization_warnings": warnings,
            "structured_snapshot_hash": _structured_hash(structured_payload),
            "structured_fields_used": list(structured_columns),
            "ignored_unstructured_fields": list(IGNORED_UNSTRUCTURED_FIELDS),
        })
    return candidates
