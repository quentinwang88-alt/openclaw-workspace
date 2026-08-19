"""Deterministic approved-persona selection for original scripts."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any, Dict, List, Sequence, Tuple

from core.persona_template_provider import load_persona_templates
from core.product_type_resolution import normalize_product_type


CONTRACT_VERSION = "persona-selection-v4-body-proportion"
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


def _script_projection(template: Dict[str, Any], country: str) -> Dict[str, str]:
    country_label = "泰国" if _canonical_market(country) == "TH" else _text(country)
    identity = _text(template.get("identity_text"))
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
        _text(template.get("appearance_text")),
        _text(template.get("body_type")),
    ]
    if not appearance_parts[0]:
        appearance_parts.extend([
            _text(template.get("skin_tone")),
            "自然未精修肤质",
            "、".join(_list(template.get("vibe_tags"))),
        ])
    appearance = "；".join(dict.fromkeys(
        value for value in appearance_parts if value
    ))
    hair_makeup = _text(template.get("hair_makeup_text")) or "；".join(
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
    return {
        "identity": identity,
        "appearance": appearance,
        "hair_makeup": hair_makeup,
        "speaking_personality": _text(template.get("speaking_personality"))
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
        "script_projection": _script_projection(selected, country),
        "prompt_core": _text(selected.get("prompt_core")),
        "prompt_negative": _text(selected.get("prompt_negative")),
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
        "selection_policy": "OUTFIT_PREFERRED_COMPATIBLE_THEN_LEAST_USED",
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
