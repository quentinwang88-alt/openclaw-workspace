"""Role-aware outfit candidate normalization and deterministic selection.

The shared template provider and category fallbacks are asset sources.  This
module is the single selection authority: it keeps target-product roles apart,
uses scene and demonstration compatibility only as soft ranking signals, and
never calls a model or turns styling quality into a generation gate.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from typing import Any, Dict, List, Sequence, Tuple


OUTFIT_SELECTION_CONTRACT_VERSION = "outfit-selection-v10-persona-affinity"

TARGET_GARMENT = "TARGET_GARMENT"
SUPPORTING_OUTFIT_NECK = "SUPPORTING_OUTFIT_NECK"
SUPPORTING_OUTFIT_HEAD = "SUPPORTING_OUTFIT_HEAD"
SUPPORTING_OUTFIT_WRIST = "SUPPORTING_OUTFIT_WRIST"
SUPPORTING_OUTFIT_HAIR = "SUPPORTING_OUTFIT_HAIR"
SUPPORTING_OUTFIT_HAND = "SUPPORTING_OUTFIT_HAND"

_WRIST_TYPES = {"bracelet", "bangle", "slim_bangle"}
_HAIR_TYPES = {
    "hair_accessory_generic", "claw_clip", "hair_clip", "headband",
    "scrunchie", "hair_tie", "ribbon", "hair_pin",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


_ONE_PIECE_TOKENS = ("连衣裙", "连体", "jumpsuit", "romper", "dress")


def _is_one_piece(value: Any) -> bool:
    text = _text(value).lower()
    return bool(text and any(token in text for token in _ONE_PIECE_TOKENS))


def _upgrade_one_piece_recipe(contract: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize legacy top/bottom dress projections without mutating source audit data."""

    upgraded = dict(contract)
    recipe = dict(upgraded.get("outfit_recipe") or {})
    explicit = _text(recipe.get("one_piece"))
    top = _text(recipe.get("top"))
    bottom = _text(recipe.get("bottom"))
    if explicit or _is_one_piece(top) or _is_one_piece(bottom):
        one_piece = explicit or (bottom if _is_one_piece(bottom) else top)
        recipe["one_piece"] = one_piece
        recipe["top"] = ""
        recipe["bottom"] = ""
        upgraded["outfit_structure"] = "ONE_PIECE"
    else:
        upgraded["outfit_structure"] = _text(
            upgraded.get("outfit_structure") or "SEPARATES"
        ).upper()
    upgraded["outfit_recipe"] = recipe
    return upgraded


def upgrade_outfit_structure_contract(contract: Dict[str, Any]) -> Dict[str, Any]:
    """Public read-time compatibility for renderers and first-frame jobs."""

    return _upgrade_one_piece_recipe(dict(contract or {}))


def _list(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    return [text] if text else []


def target_role_for(
    *, product_profile: str, canonical_type: str, demonstration_mode: str = ""
) -> str:
    if _text(product_profile).upper() == "WORN_APPAREL":
        return TARGET_GARMENT
    canonical = _text(canonical_type).lower()
    if canonical in _WRIST_TYPES or _text(demonstration_mode).upper() == "WRIST_WORN":
        return SUPPORTING_OUTFIT_WRIST
    if canonical == "ring" or _text(demonstration_mode).upper() == "FINGER_WORN":
        return SUPPORTING_OUTFIT_HAND
    if canonical in _HAIR_TYPES or _text(demonstration_mode).upper() == "HAIR_WORN":
        return SUPPORTING_OUTFIT_HAIR
    if canonical == "headscarf" or _text(
        demonstration_mode
    ).upper() in {"HEAD_WORN", "HAIR_TIE"}:
        return SUPPORTING_OUTFIT_HEAD
    if canonical in {
        "scarf", "winter_scarf", "silk_scarf",
    }:
        return SUPPORTING_OUTFIT_NECK
    return TARGET_GARMENT


def default_demonstration_mode(canonical_type: str) -> str:
    canonical = _text(canonical_type).lower()
    if canonical == "headscarf":
        return "HEAD_WORN"
    if canonical in _HAIR_TYPES:
        return "HAIR_WORN"
    if canonical in _WRIST_TYPES:
        return "WRIST_WORN"
    if canonical == "ring":
        return "FINGER_WORN"
    if canonical == "earring":
        return "EAR_WORN"
    if canonical in {"scarf", "winter_scarf", "silk_scarf"}:
        return "NECK_WORN"
    return "GARMENT_WORN"


def demonstration_mode_from_direction(
    direction: Dict[str, Any], canonical_type: str
) -> str:
    """Read only already-frozen category/content semantics; never infer copy."""

    extension = (
        direction.get("category_execution_extension")
        if isinstance(direction.get("category_execution_extension"), dict)
        else {}
    )
    profile = (
        extension.get("profile")
        if isinstance(extension.get("profile"), dict)
        else {}
    )
    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict)
        else {}
    )
    return (
        _text(profile.get("primary_demonstration_mode")).upper()
        or _text(argument.get("primary_demonstration_mode")).upper()
        or default_demonstration_mode(canonical_type)
    )


def _default_visibility_zones(target_role: str) -> List[str]:
    if target_role == SUPPORTING_OUTFIT_HEAD:
        return ["HEAD", "HAIR", "UPPER_BODY"]
    if target_role == SUPPORTING_OUTFIT_NECK:
        return ["NECK", "SHOULDER", "UPPER_BODY"]
    if target_role == SUPPORTING_OUTFIT_WRIST:
        return ["WRIST", "FOREARM", "UPPER_BODY"]
    if target_role == SUPPORTING_OUTFIT_HAIR:
        return ["HEAD", "HAIR", "SHOULDER", "UPPER_BODY"]
    if target_role == SUPPORTING_OUTFIT_HAND:
        return ["FINGER", "HAND", "FOREARM", "UPPER_BODY"]
    return ["TARGET_GARMENT", "BODY_PROPORTION"]


def _climate_profile(country: str) -> str:
    normalized = _text(country).lower()
    if any(token in normalized for token in ("泰国", "thailand", "thai")):
        return "TH_WARM"
    return "ALL_SEASON"


def normalize_outfit_candidate(
    candidate: Dict[str, Any],
    *,
    target_role: str,
    canonical_type: str,
    country: str,
    scene_family: str,
    demonstration_mode: str,
) -> Dict[str, Any]:
    normalized = dict(candidate)
    normalized["contract_version"] = OUTFIT_SELECTION_CONTRACT_VERSION
    normalized["target_role"] = _text(
        normalized.get("target_role") or target_role
    ).upper()
    normalized["product_type"] = _text(canonical_type)
    normalized["style_family"] = _text(
        normalized.get("style_family") or "DAILY_COMPATIBLE"
    )
    normalized["style_intensity"] = _text(
        normalized.get("style_intensity") or "DAILY"
    ).upper()
    normalized["climate_profile"] = _text(
        normalized.get("climate_profile") or _climate_profile(country)
    ).upper()
    normalized["demonstration_mode"] = _text(
        demonstration_mode or default_demonstration_mode(canonical_type)
    ).upper()
    normalized["supported_target_roles"] = _list(
        normalized.get("supported_target_roles") or [normalized["target_role"]]
    )
    normalized["supported_demonstration_modes"] = [
        item.upper()
        for item in _list(normalized.get("supported_demonstration_modes"))
    ]
    normalized["scene_families"] = [
        item.upper() for item in _list(normalized.get("scene_families"))
    ]
    normalized["preferred_persona_ids"] = list(dict.fromkeys(
        _list(normalized.get("preferred_persona_ids"))
    ))
    normalized["selected_scene_family"] = _text(scene_family).upper()
    visibility_zones = _list(
        normalized.get("visibility_zones")
        or _default_visibility_zones(normalized["target_role"])
    )
    if normalized["target_role"] == SUPPORTING_OUTFIT_HEAD:
        visibility_zones = list(dict.fromkeys([
            "HEAD", "HAIR", "UPPER_BODY", *visibility_zones,
        ]))
    elif normalized["target_role"] == SUPPORTING_OUTFIT_HAIR:
        visibility_zones = list(dict.fromkeys([
            "HEAD", "HAIR", "SHOULDER", "UPPER_BODY", *visibility_zones,
        ]))
    elif normalized["target_role"] == SUPPORTING_OUTFIT_WRIST:
        visibility_zones = list(dict.fromkeys([
            "WRIST", "FOREARM", "UPPER_BODY", *visibility_zones,
        ]))
    elif normalized["target_role"] == SUPPORTING_OUTFIT_HAND:
        visibility_zones = list(dict.fromkeys([
            "FINGER", "HAND", "FOREARM", "UPPER_BODY", *visibility_zones,
        ]))
    normalized["visibility_zones"] = visibility_zones
    recipe = normalized.get("outfit_recipe")
    if not isinstance(recipe, dict):
        recipe = {}
    if not any(_text(value) for value in recipe.values()):
        recipe = {
            "top": _text(normalized.get("inner_type")),
            "bottom": _text(normalized.get("bottom_type")),
            "footwear": _text(normalized.get("footwear_visibility")),
            "bag": "",
            "other_accessories": _text(normalized.get("accessory_level")),
        }
    normalized["outfit_recipe"] = recipe
    normalized["selection_policy"] = "ROLE_COMPATIBLE_SOFT_ROTATION"
    normalized["hard_required"] = False
    return _upgrade_one_piece_recipe(normalized)


def outfit_selection_key(contract: Dict[str, Any]) -> str:
    source = _text(contract.get("source_type")) or "INTERNAL_PROFILE"
    identity = _text(contract.get("template_id")) or _text(
        contract.get("silhouette_key")
    )
    return f"{source}:{identity}" if identity else ""


def _freeze_recipe_alternative(value: Any, *, seed: int, slot: str) -> str:
    """Choose one operator-authored option without asking another model.

    Outfit-table cells often contain compact alternatives such as
    ``吊带或短袖`` or ``凉鞋、平底鞋或运动鞋``.  Passing all alternatives to a
    video model reopens the decision and weakens cross-shot consistency.  The
    batch seed freezes one option while the original value remains auditable.
    """

    text = _text(value)
    if not text:
        return ""
    # ``、`` alone normally joins cumulative attributes (for example
    # “黑色、高腰、A字裙”), not alternatives.  Treat it as an option separator
    # only when the same cell explicitly contains “或” or a slash.
    has_explicit_choice = "或" in text or "/" in text
    options = [
        item.strip()
        for item in re.split(
            r"\s*(?:或|/|、)\s*" if has_explicit_choice else r"$^",
            text,
        )
        if item.strip()
    ]
    if len(options) <= 1:
        return text
    material = f"{seed}|{slot}|{text}"
    index = int(hashlib.sha256(material.encode("utf-8")).hexdigest()[:8], 16) % len(options)
    return options[index]


def freeze_outfit_recipe(contract: Dict[str, Any], *, seed: int) -> Dict[str, Any]:
    frozen = _upgrade_one_piece_recipe(dict(contract))
    source_recipe = (
        dict(frozen.get("outfit_recipe"))
        if isinstance(frozen.get("outfit_recipe"), dict)
        else {}
    )
    resolved_recipe = {
        str(slot): _freeze_recipe_alternative(value, seed=seed, slot=str(slot))
        for slot, value in source_recipe.items()
    }
    source_accessory_items = _list(frozen.get("accessory_items"))
    resolved_accessory_items = list(dict.fromkeys(
        resolved
        for index, value in enumerate(source_accessory_items)
        for resolved in [
            _freeze_recipe_alternative(
                value,
                seed=seed,
                slot=f"accessory_{index}",
            )
        ]
        if resolved
    ))
    if source_accessory_items:
        frozen["source_accessory_items"] = source_accessory_items
        frozen["accessory_items"] = resolved_accessory_items
        # Explicit accessories are part of the frozen outfit recipe.  Keep
        # simultaneously listed items, but resolve an operator-authored
        # "A或B" alternative once instead of reopening it in the model.
        resolved_recipe["other_accessories"] = "；".join(
            resolved_accessory_items
        )
    frozen["source_outfit_recipe"] = source_recipe
    frozen["outfit_recipe"] = resolved_recipe
    frozen["recipe_resolution"] = {
        "policy_version": "outfit-recipe-resolution-v1",
        "method": "DETERMINISTIC_SEED_OPTION",
        "model_call_added": False,
    }
    return frozen


def _usage_metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    metadata = row.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    raw = row.get("metadata_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def outfit_selection_key_from_usage(row: Dict[str, Any]) -> str:
    contract = row.get("outfit_selection_contract")
    if not isinstance(contract, dict):
        contract = _usage_metadata(row).get("outfit_selection_contract")
    if isinstance(contract, dict):
        key = outfit_selection_key(contract)
        if key:
            return key
    surface = row.get("surface_profile")
    if not isinstance(surface, dict):
        surface = _usage_metadata(row).get("surface_profile")
    if isinstance(surface, dict):
        silhouette = _text(
            surface.get("silhouette_key") or surface.get("surface_profile_key")
        )
        if silhouette:
            return f"INTERNAL_PROFILE:{silhouette}"
    return ""


def _compatible(candidate: Dict[str, Any], target_role: str, mode: str) -> bool:
    roles = [item.upper() for item in _list(candidate.get("supported_target_roles"))]
    if roles and target_role.upper() not in roles:
        return False
    modes = [
        item.upper() for item in _list(candidate.get("supported_demonstration_modes"))
    ]
    return not modes or mode.upper() in modes


def _candidate_source_tier(candidate: Dict[str, Any]) -> int:
    """Keep operator-authored sources ahead of generative fallbacks.

    A product-specific template remains eligible after its first use.  The old
    least-used-first ranking compared it with internal silhouettes, so a
    product with one approved outfit used that outfit once and then appeared
    to choose arbitrary dresses, denim or skirts.  Diversity is still applied
    *inside* the highest available source tier.
    """

    if _text(candidate.get("match_scope")).upper() == "EXACT_PRODUCT_CODE":
        return 0
    if _text(candidate.get("source_type")).upper() == "LIGHTWEIGHT_TEMPLATE":
        return 1
    return 2


def select_outfit_candidate(
    *,
    candidates: Sequence[Dict[str, Any]],
    recent_usage: Sequence[Dict[str, Any]],
    seed: int,
    target_role: str,
    demonstration_mode: str,
    scene_family: str,
    product_colors: Sequence[str] | None = None,
) -> Tuple[Dict[str, Any], int, int]:
    """Choose one compatible contract without adding a quality gate."""

    compatible = [
        dict(item)
        for item in candidates
        if _compatible(item, target_role, demonstration_mode)
    ]
    # Candidate builders always retain a category fallback.  This defensive
    # path avoids turning optional styling metadata into a production blocker.
    pool = compatible or [dict(item) for item in candidates]
    if not pool:
        return {}, 0, 0
    color_report: Dict[str, Any] = {}
    if product_colors:
        requested_colors = set(product_colors)
        color_report = {
            "policy_version": "outfit-product-color-v2-soft-preference",
            "product_colors": sorted(requested_colors),
            # Retained for existing reports: a preference mismatch is not an
            # explicit ban and must not shrink the operator-authorized pool.
            "explicit_conflict_count": 0,
            "preference_mismatch_count": sum(
                bool(item.get("product_color_preferences"))
                and not requested_colors.intersection(item["product_color_preferences"])
                for item in pool
            ),
            "authority": "SOFT_RANKING_ONLY",
        }
    best_source_tier = min(_candidate_source_tier(item) for item in pool)
    pool = [
        item for item in pool if _candidate_source_tier(item) == best_source_tier
    ]
    if product_colors:
        color_report["eligible_template_ids"] = [item.get("template_id") for item in pool]

    historical_counts: Counter = Counter()
    batch_counts: Counter = Counter()
    for row in recent_usage:
        key = outfit_selection_key_from_usage(row)
        if not key or key.endswith(":PRODUCT_LED"):
            continue
        target = batch_counts if row.get("_batch_reserved") else historical_counts
        target[key] += 1

    ranked: List[
        Tuple[int, int, int, int, int, int, int, int, Dict[str, Any]]
    ] = []
    selected_scene = _text(scene_family).upper()
    for index, candidate in enumerate(pool):
        key = outfit_selection_key(candidate)
        candidate_scenes = [
            item.upper() for item in _list(candidate.get("scene_families"))
        ]
        scene_mismatch = int(
            bool(selected_scene and candidate_scenes and selected_scene not in candidate_scenes)
        )
        tie_material = f"{seed}|{index}|{key}|{target_role}|{demonstration_mode}"
        tie_break = int(hashlib.sha256(tie_material.encode("utf-8")).hexdigest()[:8], 16)
        ranked.append((
            batch_counts[key],
            int(bool(product_colors) and not set(product_colors or []).intersection(
                candidate.get("product_color_preferences") or []
            )),
            int(candidate.get("source_preference") or 0),
            int(candidate.get("style_preference_rank") or 0),
            scene_mismatch,
            historical_counts[key],
            -int(candidate.get("priority") or 0),
            tie_break,
            candidate,
        ))
    row = min(ranked, key=lambda value: value[:-1])
    selected = freeze_outfit_recipe(dict(row[-1]), seed=seed)
    selected.pop("source_preference", None)
    selected.pop("style_preference_rank", None)
    selected.pop("match_rank", None)
    selected["source_tier"] = (
        "EXACT_PRODUCT_TEMPLATE"
        if best_source_tier == 0
        else "GENERIC_SHARED_TEMPLATE"
        if best_source_tier == 1
        else "INTERNAL_FALLBACK"
    )
    selected["source_tier_policy"] = "HIGHEST_AVAILABLE_TIER_THEN_SOFT_ROTATION"
    if color_report:
        color_report["match_status"] = (
            "MATCHED" if set(product_colors or []).intersection(
                selected.get("product_color_preferences") or []
            ) else "UNMATCHED_SOFT_PREFERENCE"
            if selected.get("product_color_preferences") else "UNSPECIFIED_FALLBACK"
        )
        selected["product_color_affinity"] = color_report
    key = outfit_selection_key(selected)
    return selected, historical_counts[key], batch_counts[key]
