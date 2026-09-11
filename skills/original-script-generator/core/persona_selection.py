"""Deterministic approved-persona selection for original scripts."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from typing import Any, Dict, List, Sequence, Tuple

from core.persona_template_provider import load_persona_templates
from core.product_type_resolution import normalize_product_type


CONTRACT_VERSION = "persona-selection-v5-clean-projection"
OUTFIT_PERSONA_AFFINITY_VERSION = "outfit-persona-affinity-v1-soft"
FACE_ADJACENT_TYPES = {"headscarf", "earring", "hair_accessory", "hairclip"}
WEARER_PROMINENT_TYPES = {
    "scarf", "winter_scarf", "silk_scarf", "outerwear", "top", "dress",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _list(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    return [text] if text else []


def _metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    value = row.get("metadata")
    if isinstance(value, dict):
        return value
    value = row.get("metadata_json")
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _usage_persona_id(row: Dict[str, Any]) -> str:
    contract = row.get("persona_selection_contract")
    if not isinstance(contract, dict):
        contract = _metadata(row).get("persona_selection_contract")
    return _text(contract.get("persona_id")) if isinstance(contract, dict) else ""


def _canonical_market(value: str) -> str:
    text = _text(value).lower()
    if text in {"泰国", "thailand", "thai", "th"}:
        return "TH"
    return text.upper()


def _matches(values: Sequence[str], target: str) -> bool:
    normalized = {_text(value).lower() for value in values if _text(value)}
    return not normalized or "*" in normalized or _text(target).lower() in normalized


def _market_matches(values: Sequence[str], target: str) -> bool:
    normalized = {_canonical_market(value) for value in values if _text(value)}
    return not normalized or "*" in normalized or _canonical_market(target) in normalized


_PERSONA_TEXT_REPLACEMENTS = (
    ("粽色", "棕色"),
    ("帖头皮", "贴头皮"),
    ("不要网红精修脸", "自然未精修面部质感"),
    ("把模特妆容换成淡妆", ""),
    ("其余发型身材不变", ""),
    ("注意还原参考图的肤色以及皮肤自然纹理", "自然肤色与真实皮肤纹理"),
    ("面部不要碎发", "面部轮廓清楚，碎发不过度遮脸"),
    ("不要贴头皮", "发根自然蓬松"),
    ("保持原样", ""),
    ("和上一个一样", ""),
)


def _sanitize_persona_text(value: Any) -> str:
    """Turn operator editing notes into a clean executable description."""

    text = _text(value)
    for source, target in _PERSONA_TEXT_REPLACEMENTS:
        text = text.replace(source, target)
    text = re.sub(r"[，,]{2,}", "，", text)
    text = re.sub(r"[；;]{2,}", "；", text)
    text = re.sub(r"[。\.]{2,}", "。", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*([，。；,;])\s*", r"\1", text)
    text = re.sub(r"([，；,;])([。\.])", r"\2", text)
    return text.strip(" ，。；,;.")


def _freeze_hair_alternatives(value: str, *, seed_material: str) -> str:
    """Freeze one hairstyle choice without changing the source template."""

    parts = re.split(r"([，,；;])", value)
    frozen: List[str] = []
    clause_index = 0
    for part in parts:
        if part in {"，", ",", "；", ";"}:
            frozen.append("，" if part in {"，", ","} else "；")
            continue
        clause = _text(part)
        if "或" not in clause:
            frozen.append(clause)
            continue
        choices = [_text(item) for item in clause.split("或") if _text(item)]
        if len(choices) < 2:
            frozen.append(clause)
            continue
        digest = hashlib.sha256(
            f"{seed_material}|hair|{clause_index}|{clause}".encode("utf-8")
        ).digest()
        selected = choices[digest[0] % len(choices)]
        if (
            selected != choices[0]
            and choices[0].startswith("头发")
            and not selected.startswith("头发")
        ):
            selected = "头发" + selected
        frozen.append(selected)
        clause_index += 1
    return _sanitize_persona_text("".join(frozen))


def _freeze_inline_alternatives(value: str, *, seed_material: str) -> str:
    """Resolve simple inline choices such as 下班前或出门前."""

    parts = re.split(r"([，,；;。])", value)
    frozen: List[str] = []
    for index, part in enumerate(parts):
        clause = _text(part)
        if "或" not in clause or part in {"，", ",", "；", ";", "。"}:
            frozen.append(part)
            continue
        left, right_tail = clause.split("或", 1)
        left = _text(left)
        right_tail = _text(right_tail)
        if not left or not right_tail:
            frozen.append(clause)
            continue
        # When both alternatives share a natural boundary character (for
        # example 下班前 / 出门前 or 户外 / 半户外), keep the trailing action
        # text after the second option instead of dropping it.
        boundary = left[-1]
        boundary_index = right_tail.find(boundary)
        if 0 <= boundary_index <= 8:
            right = right_tail[: boundary_index + 1]
            tail = right_tail[boundary_index + 1 :]
        else:
            right = right_tail
            tail = ""
        digest = hashlib.sha256(
            f"{seed_material}|inline|{index}|{clause}".encode("utf-8")
        ).digest()
        frozen.append((left if digest[0] % 2 == 0 else right) + tail)
    return _sanitize_persona_text("".join(frozen))


def _script_projection(
    template: Dict[str, Any], country: str, *, seed_material: str
) -> Dict[str, str]:
    country_label = "泰国" if _canonical_market(country) == "TH" else _text(country)
    identity = _freeze_inline_alternatives(
        _sanitize_persona_text(template.get("identity_text")),
        seed_material=seed_material,
    )
    if not identity:
        identity = "、".join(
            value
            for value in (
                country_label,
                _text(template.get("age_group")),
                "女性日常创作者" if _text(template.get("gender")).lower() == "female" else "日常创作者",
            )
            if value
        )
    appearance_parts = [
        _sanitize_persona_text(template.get("appearance_text")),
        _sanitize_persona_text(template.get("body_type")),
    ]
    if not appearance_parts[0]:
        appearance_parts.extend([
            _sanitize_persona_text(template.get("skin_tone")),
            "自然未精修肤质",
            "、".join(_list(template.get("vibe_tags"))),
        ])
    appearance = "；".join(dict.fromkeys(
        value for value in appearance_parts if value
    ))
    hair_makeup = _sanitize_persona_text(template.get("hair_makeup_text")) or "；".join(
        value
        for value in (
            " ".join(
                value
                for value in (
                    _text(template.get("hair_color")),
                    _text(template.get("hair_style")),
                )
                if value
            ),
            _text(template.get("makeup_style")),
        )
        if value
    )
    hair_makeup = _freeze_hair_alternatives(
        hair_makeup,
        seed_material=seed_material,
    )
    return {
        "identity": identity,
        "appearance": appearance,
        "hair_makeup": hair_makeup,
        "speaking_personality": _sanitize_persona_text(template.get("speaking_personality"))
        or "像普通创作者对自己的手机镜头自然分享",
    }


def _reference_strategy(canonical_type: str) -> str:
    if canonical_type in FACE_ADJACENT_TYPES:
        return "PERSONA_PRODUCT_COMPOSITE_REQUIRED"
    if canonical_type in WEARER_PROMINENT_TYPES:
        return "PERSONA_PRODUCT_COMPOSITE_PREFERRED"
    return "DIRECT_PRODUCT_REFERENCE"


def _unavailable(
    *, status: str, provider: Dict[str, Any], presentation_mode: str,
    canonical_type: str,
) -> Dict[str, Any]:
    return {
        "schema_version": CONTRACT_VERSION,
        "availability": status,
        "authority": "UNAVAILABLE",
        "persona_id": "",
        "product_type": canonical_type,
        "presentation_mode": presentation_mode,
        "reference_strategy": "DIRECT_PRODUCT_REFERENCE",
        "non_authorities": [
            "PRODUCT_REFERENCE_PERSON",
            "PRODUCT_REFERENCE_FILTER",
            "PRODUCT_REFERENCE_SCENE",
        ],
        "provider_snapshot": {
            "provider_version": provider.get("provider_version", ""),
            "status": provider.get("status", "UNAVAILABLE"),
            "approved_asset_count": provider.get("approved_asset_count", 0),
            "soft_warnings": list(provider.get("soft_warnings") or []),
        },
        "hard_required": False,
    }


def select_persona_contract(
    *, product_type: str, top_category: str, country: str,
    presentation_mode: str, capture_mode: str, demonstration_mode: str,
    seed: int, recent_usage: Sequence[Dict[str, Any]], db_path: str = "",
    preferred_persona_ids: Sequence[str] = (),
    prefer_reference_pack: bool = False,
) -> Tuple[Dict[str, Any], int, int]:
    """Return one frozen persona without creating a new generation gate."""

    presentation = _text(presentation_mode).upper()
    canonical = normalize_product_type(product_type, top_category).canonical_type
    provider = load_persona_templates(db_path=db_path or None)
    if presentation not in {"PERSON_ON_CAMERA", "WEARER_ACTIVE", "MIXED"}:
        return _unavailable(
            status="NOT_APPLICABLE", provider=provider,
            presentation_mode=presentation, canonical_type=canonical,
        ), 0, 0
    templates = list(provider.get("templates") or [])
    if not templates:
        return _unavailable(
            status="UNAVAILABLE", provider=provider,
            presentation_mode=presentation, canonical_type=canonical,
        ), 0, 0

    market = _canonical_market(country)
    mode = _text(demonstration_mode).upper()
    compatible: List[Dict[str, Any]] = []
    for item in templates:
        if not _market_matches(item.get("markets") or [], market):
            continue
        if not _matches(item.get("applicable_categories") or [], top_category):
            continue
        if not _matches(item.get("applicable_product_types") or [], canonical):
            continue
        presentations = {
            value.upper()
            for value in _list(item.get("supported_presentation_modes"))
        }
        if presentations and presentation not in presentations:
            continue
        captures = {
            value.upper() for value in _list(item.get("supported_capture_modes"))
        }
        if captures and _text(capture_mode).upper() not in captures:
            continue
        modes = {value.upper() for value in _list(item.get("supported_demonstration_modes"))}
        if modes and mode and mode not in modes:
            continue
        compatible.append(dict(item))
    if not compatible:
        return _unavailable(
            status="UNAVAILABLE_COMPATIBLE_TEMPLATE", provider=provider,
            presentation_mode=presentation, canonical_type=canonical,
        ), 0, 0

    historical: Counter = Counter()
    batch: Counter = Counter()
    for row in recent_usage:
        persona_id = _usage_persona_id(row)
        if not persona_id:
            continue
        (batch if row.get("_batch_reserved") else historical)[persona_id] += 1

    preferred = list(dict.fromkeys(_list(preferred_persona_ids)))
    preferred_set = set(preferred)
    ranked = []
    for index, item in enumerate(compatible):
        persona_id = _text(item.get("persona_id"))
        tie_material = f"{seed}|{index}|{persona_id}|{canonical}|{mode}"
        tie = int(hashlib.sha256(tie_material.encode("utf-8")).hexdigest()[:8], 16)
        ranked.append((
            0 if not preferred_set or persona_id in preferred_set else 1,
            0 if not prefer_reference_pack or len({
                _text(ref.get("role")) for ref in item.get("reference_images") or []
                if isinstance(ref, dict) and ref.get("approved") is not False and ref.get("role")
            }) >= 2 else 1,
            batch[persona_id], historical[persona_id],
            -int(item.get("priority") or 0), tie, item,
        ))
    row = min(ranked, key=lambda value: value[:-1])
    selected = row[-1]
    persona_id = _text(selected.get("persona_id"))
    contract = {
        "schema_version": CONTRACT_VERSION,
        "availability": "AVAILABLE",
        "authority": "APPROVED_PERSONA_LIBRARY",
        "persona_id": persona_id,
        "persona_name": _text(selected.get("persona_name")),
        "template_version": _text(selected.get("template_version")) or "V1",
        "structured_snapshot_hash": _text(selected.get("structured_snapshot_hash")),
        "product_type": canonical,
        "presentation_mode": presentation,
        "capture_mode": _text(capture_mode).upper(),
        "demonstration_mode": mode,
        "reference_strategy": _reference_strategy(canonical),
        "reference_asset_ids": list(selected.get("reference_asset_ids") or []),
        "reference_images": list(selected.get("reference_images") or []),
        "identity_lock": {
            key: selected.get(key, "")
            for key in (
                "gender", "age_group", "body_type", "hair_style", "hair_color",
                "skin_tone", "face_visibility", "makeup_style", "vibe_tags",
                "body_proportion_text",
            )
        },
        "script_projection": _script_projection(
            selected,
            country,
            seed_material=f"{seed}|{persona_id}|{canonical}|{mode}",
        ),
        "prompt_core": _sanitize_persona_text(selected.get("prompt_core")),
        "prompt_negative": _sanitize_persona_text(selected.get("prompt_negative")),
        "projection_policy_version": "persona-projection-sanitize-v1",
        "non_authorities": [
            "PRODUCT_REFERENCE_PERSON",
            "PRODUCT_REFERENCE_FILTER",
            "PRODUCT_REFERENCE_SCENE",
        ],
        "preferred_persona_ids": preferred,
        "preference_match_status": (
            "MATCHED" if preferred_set and persona_id in preferred_set
            else "FALLBACK" if preferred_set else "NO_PREFERENCE"
        ),
        "selection_policy": ("OUTFIT_PREFERRED_THEN_REFERENCE_PACK_THEN_LEAST_USED"
                             if prefer_reference_pack else "OUTFIT_PREFERRED_COMPATIBLE_THEN_LEAST_USED"),
        "recent_usage_count": historical[persona_id],
        "batch_usage_count": batch[persona_id],
        "hard_required": False,
    }
    return contract, historical[persona_id], batch[persona_id]


def build_outfit_persona_affinity_contract(
    outfit_contract: Dict[str, Any], persona_contract: Dict[str, Any]
) -> Dict[str, Any]:
    """Record the soft outfit/persona relationship without creating a gate."""

    preferred = list(dict.fromkeys(
        _list(outfit_contract.get("preferred_persona_ids"))
    ))
    selected_id = _text(persona_contract.get("persona_id"))
    availability = _text(persona_contract.get("availability"))
    if availability == "NOT_APPLICABLE":
        status = "NOT_APPLICABLE"
        reason = "NON_PERSON_CARRIER"
    elif not preferred:
        status = "NO_PREFERENCE"
        reason = ""
    elif selected_id and selected_id in preferred:
        status = "MATCHED"
        reason = ""
    else:
        status = "FALLBACK"
        reason = (
            "PREFERRED_PERSONA_NOT_COMPATIBLE_OR_AVAILABLE"
            if availability.startswith("UNAVAILABLE") or selected_id
            else "PERSONA_LIBRARY_UNAVAILABLE"
        )
    return {
        "policy_version": OUTFIT_PERSONA_AFFINITY_VERSION,
        "outfit_template_id": _text(outfit_contract.get("template_id")),
        "outfit_template_version": _text(outfit_contract.get("template_version")),
        "outfit_source_type": _text(outfit_contract.get("source_type")),
        "preferred_persona_ids": preferred,
        "selected_persona_id": selected_id,
        "selected_persona_name": _text(persona_contract.get("persona_name")),
        "match_status": status,
        "fallback_reason": reason,
        "authority": "SOFT_PREFERENCE",
        "hard_required": False,
    }
