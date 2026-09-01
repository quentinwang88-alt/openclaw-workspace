"""Validation contracts for OPV versioned configs and structured payloads.

Every ``validate_*`` function returns a list of human-readable error strings
(empty list means valid). :func:`ensure_valid` turns that list into a
:class:`ContractViolationError`. No third-party JSON-schema dependency is used
(runs on stdlib Python 3.9).

Contract versions implemented here:

- ``opv-market-pack-v1`` / ``opv-theme-v1`` / ``opv-render-preset-v1``
- ``opv-account-profile-v1`` (account import contract)
- ``opv-plan-v1`` (Content Planner output) and its shots
- ``opv-bgm-v1`` (BGM selection persisted into opv_publish_record)
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional

MARKET_PACK_SCHEMA_VERSION = "opv-market-pack-v1"
THEME_SCHEMA_VERSION = "opv-theme-v1"
RENDER_PRESET_SCHEMA_VERSION = "opv-render-preset-v1"
ACCOUNT_PROFILE_SCHEMA_VERSION = "opv-account-profile-v1"
PLAN_SCHEMA_VERSION = "opv-plan-v1"
BGM_SCHEMA_VERSION = "opv-bgm-v1"

PLAN_SHOT_COUNT = 5
PLAN_DURATION_MIN_MS = 10000
PLAN_DURATION_MAX_MS = 15000

AUDIO_STRATEGIES = ("platform_hot_bgm", "embedded_bgm", "no_bgm")
LOOK_SOURCE_TYPES = ("successful_look", "look_template", "ai_exploration")
ACCOUNT_STATUSES = ("active", "testing", "paused", "disabled")

_LOCALE_RE = re.compile(r"^[a-z]{2}-[A-Z]{2}$")
_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


class ContractViolationError(ValueError):
    """Raised by :func:`ensure_valid` when a payload fails its contract."""


def ensure_valid(errors: List[str], label: str) -> None:
    if errors:
        raise ContractViolationError(f"{label} invalid: " + "; ".join(errors))


# --------------------------------------------------------------------------
# Generic checks
# --------------------------------------------------------------------------

def _is_str(value: Any) -> bool:
    return isinstance(value, str)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_dict(value: Any) -> bool:
    return isinstance(value, dict)


def _is_list(value: Any) -> bool:
    return isinstance(value, list)


def _check_schema_version(
    errors: List[str], payload: Mapping[str, Any], expected: str
) -> None:
    if payload.get("schema_version") != expected:
        errors.append(
            f"schema_version must be {expected!r}, got {payload.get('schema_version')!r}"
        )


def _require_str(
    errors: List[str],
    payload: Mapping[str, Any],
    key: str,
    *,
    allow_empty: bool = False,
) -> None:
    value = payload.get(key)
    if not _is_str(value) or (not allow_empty and not value.strip()):
        errors.append(f"{key} must be a non-empty string, got {value!r}")


def _require_int(
    errors: List[str],
    payload: Mapping[str, Any],
    key: str,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> None:
    value = payload.get(key)
    if not _is_int(value):
        errors.append(f"{key} must be an integer, got {value!r}")
        return
    if minimum is not None and value < minimum:
        errors.append(f"{key} must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        errors.append(f"{key} must be <= {maximum}, got {value}")


def _require_dict(
    errors: List[str],
    payload: Mapping[str, Any],
    key: str,
    *,
    non_empty: bool = True,
) -> None:
    value = payload.get(key)
    if not _is_dict(value):
        errors.append(f"{key} must be an object, got {type(value).__name__}")
        return
    if non_empty and not value:
        errors.append(f"{key} must not be empty")


def _require_list(
    errors: List[str],
    payload: Mapping[str, Any],
    key: str,
    *,
    non_empty: bool = False,
    item_type: type = str,
) -> None:
    value = payload.get(key)
    if not _is_list(value):
        errors.append(f"{key} must be a list, got {type(value).__name__}")
        return
    if non_empty and not value:
        errors.append(f"{key} must not be empty")
    for index, item in enumerate(value):
        if not isinstance(item, item_type):
            errors.append(f"{key}[{index}] must be {item_type.__name__}, got {item!r}")


def _require_enum(
    errors: List[str], payload: Mapping[str, Any], key: str, allowed: tuple
) -> None:
    value = payload.get(key)
    if value not in allowed:
        errors.append(f"{key} must be one of {list(allowed)}, got {value!r}")


def _require_country(errors: List[str], payload: Mapping[str, Any], key: str) -> None:
    value = payload.get(key)
    if not _is_str(value) or not _COUNTRY_RE.match(value):
        errors.append(f"{key} must be an ISO alpha-2 uppercase code, got {value!r}")


def _require_locale(errors: List[str], payload: Mapping[str, Any], key: str) -> None:
    value = payload.get(key)
    if not _is_str(value) or not _LOCALE_RE.match(value):
        errors.append(f"{key} must look like 'th-TH', got {value!r}")


# --------------------------------------------------------------------------
# Versioned config contracts
# --------------------------------------------------------------------------

def validate_market_pack_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, MARKET_PACK_SCHEMA_VERSION)
    _require_str(errors, payload, "market_pack_id")
    _require_str(errors, payload, "pack_key")
    _require_int(errors, payload, "pack_version", minimum=1)
    _require_country(errors, payload, "target_country")
    _require_locale(errors, payload, "target_locale")
    _require_str(errors, payload, "pack_name")
    _require_str(errors, payload, "season_key", allow_empty=False)
    _require_enum(errors, payload, "status", ("draft", "active", "deprecated"))
    for rules_key in (
        "visual_rules",
        "copy_rules",
        "topic_rules",
        "safety_rules",
    ):
        _require_dict(errors, payload, rules_key)
    return errors


def validate_theme_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, THEME_SCHEMA_VERSION)
    _require_str(errors, payload, "theme_id")
    _require_str(errors, payload, "theme_key")
    _require_int(errors, payload, "theme_version", minimum=1)
    _require_str(errors, payload, "theme_name")
    _require_enum(errors, payload, "status", ("draft", "active", "deprecated"))
    _require_list(
        errors, payload, "applicable_markets", non_empty=True
    )
    for market in payload.get("applicable_markets") or []:
        if not _is_str(market) or not _COUNTRY_RE.match(market):
            errors.append(f"applicable_markets contains bad code {market!r}")
    _require_dict(errors, payload, "product_match_rules")
    _require_dict(errors, payload, "content_plan_rules")
    _validate_storyboard(errors, payload.get("default_storyboard"))
    return errors


def _validate_storyboard(errors: List[str], storyboard: Any) -> None:
    if not _is_dict(storyboard):
        errors.append("default_storyboard must be an object")
        return
    slots = storyboard.get("slots")
    if not _is_list(slots) or len(slots) != PLAN_SHOT_COUNT:
        errors.append(
            f"default_storyboard.slots must contain exactly {PLAN_SHOT_COUNT} slots"
        )
        return
    seen: set = set()
    for slot in slots:
        if not _is_dict(slot):
            errors.append(f"storyboard slot must be an object, got {slot!r}")
            continue
        index = slot.get("slot_index")
        if not _is_int(index) or not 1 <= index <= PLAN_SHOT_COUNT:
            errors.append(f"storyboard slot_index out of range: {index!r}")
            continue
        if index in seen:
            errors.append(f"storyboard duplicate slot_index {index}")
        seen.add(index)
        _require_str(errors, slot, "slot_role")
        _require_int(errors, slot, "duration_ms", minimum=500, maximum=8000)


def validate_render_preset_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, RENDER_PRESET_SCHEMA_VERSION)
    _require_str(errors, payload, "render_preset_id")
    _require_str(errors, payload, "preset_key")
    _require_int(errors, payload, "preset_version", minimum=1)
    _require_str(errors, payload, "preset_name")
    _require_enum(errors, payload, "status", ("draft", "active", "deprecated"))
    _require_int(errors, payload, "width_px", minimum=1)
    _require_int(errors, payload, "height_px", minimum=1)
    width = payload.get("width_px")
    height = payload.get("height_px")
    if _is_int(width) and _is_int(height):
        # V1 is vertical 9:16 only.
        if height * 9 != width * 16:
            errors.append(
                f"resolution {width}x{height} is not 9:16 vertical"
            )
    _require_int(errors, payload, "fps", minimum=24, maximum=60)
    _require_int(
        errors,
        payload,
        "target_duration_ms",
        minimum=PLAN_DURATION_MIN_MS,
        maximum=PLAN_DURATION_MAX_MS,
    )
    _require_str(errors, payload, "codec")
    _require_enum(errors, payload, "render_mode", ("still_slideshow",))
    _require_dict(errors, payload, "motion_rules")
    _require_dict(errors, payload, "transition_rules")
    _require_dict(errors, payload, "text_overlay_rules")
    _require_dict(errors, payload, "audio_rules")
    _require_dict(errors, payload, "output_rules")

    audio_rules = payload.get("audio_rules")
    if _is_dict(audio_rules):
        _require_enum(
            errors, audio_rules, "default_strategy", AUDIO_STRATEGIES
        )
        _require_enum(errors, audio_rules, "fallback_strategy", AUDIO_STRATEGIES)
        default_strategy = audio_rules.get("default_strategy")
        if default_strategy == "embedded_bgm":
            errors.append(
                "default_strategy must be platform_hot_bgm for V1; "
                "embedded_bgm is a downgrade only"
            )

    motion_rules = payload.get("motion_rules")
    if _is_dict(motion_rules):
        _require_list(
            errors, motion_rules, "allowed_presets", non_empty=True
        )

    transition_rules = payload.get("transition_rules")
    if _is_dict(transition_rules):
        _require_list(
            errors, transition_rules, "allowed_transitions", non_empty=True
        )
    return errors


def validate_account_profile_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, ACCOUNT_PROFILE_SCHEMA_VERSION)
    _require_str(errors, payload, "account_id")
    _require_str(errors, payload, "account_code")
    _require_str(errors, payload, "account_name")
    _require_enum(errors, payload, "platform", ("tiktok",))
    _require_country(errors, payload, "target_country")
    _require_locale(errors, payload, "default_locale")
    _require_str(errors, payload, "timezone")
    _require_enum(errors, payload, "status", ACCOUNT_STATUSES)
    _require_dict(errors, payload, "persona_snapshot")
    _require_dict(errors, payload, "visual_identity")
    _require_list(errors, payload, "allowed_style_refs")
    _require_list(errors, payload, "allowed_look_refs")
    _require_list(errors, payload, "allowed_scene_refs")
    _require_list(errors, payload, "core_scene_refs")
    _require_dict(errors, payload, "operating_rules")

    if payload.get("status") == "active":
        # A production account must have a real persona and asset boundaries.
        _require_str(errors, payload, "persona_ref_id")
        _require_list(errors, payload, "core_scene_refs", non_empty=True)
        _require_list(errors, payload, "allowed_look_refs", non_empty=True)
        _require_str(errors, payload, "default_market_pack_id")
        _require_str(errors, payload, "default_render_preset_id")
    return errors


# --------------------------------------------------------------------------
# Plan / shot / BGM contracts (docs/MODEL_HANDOFF.md section 8)
# --------------------------------------------------------------------------

def validate_plan_json(plan: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, plan, PLAN_SCHEMA_VERSION)

    market_pack = plan.get("market_pack")
    if not _is_dict(market_pack):
        errors.append("market_pack must be an object")
    else:
        _require_str(errors, market_pack, "id")
        _require_int(errors, market_pack, "version", minimum=1)
        _require_country(errors, market_pack, "country")
        _require_locale(errors, market_pack, "locale")

    theme = plan.get("theme")
    if not _is_dict(theme):
        errors.append("theme must be an object")
    else:
        _require_str(errors, theme, "id")
        _require_str(errors, theme, "topic")

    for section in ("persona", "look", "scene"):
        block = plan.get(section)
        if not _is_dict(block):
            errors.append(f"{section} must be an object")
            continue
        _require_str(errors, block, "ref_id")
        if not _is_dict(block.get("snapshot")):
            errors.append(f"{section}.snapshot must be an object")
        if section == "look":
            _require_enum(errors, block, "source_type", LOOK_SOURCE_TYPES)

    copy_block = plan.get("copy")
    if not _is_dict(copy_block):
        errors.append("copy must be an object")
    else:
        _require_str(errors, copy_block, "title")
        _require_str(errors, copy_block, "caption")
        _require_list(errors, copy_block, "hashtags")
        _require_str(errors, copy_block, "cover_text")

    audio_policy = plan.get("audio_policy")
    if not _is_dict(audio_policy):
        errors.append("audio_policy must be an object")
    else:
        _require_enum(errors, audio_policy, "strategy", AUDIO_STRATEGIES)
        _require_enum(errors, audio_policy, "fallback", AUDIO_STRATEGIES)

    render_contract = plan.get("render_contract")
    if render_contract is not None:
        if not _is_dict(render_contract):
            errors.append("render_contract must be an object")
        else:
            _require_str(errors, render_contract, "preset_id")
            _require_int(
                errors,
                render_contract,
                "target_duration_ms",
                minimum=PLAN_DURATION_MIN_MS,
                maximum=PLAN_DURATION_MAX_MS,
            )

    shots = plan.get("shots")
    if not _is_list(shots) or len(shots) != PLAN_SHOT_COUNT:
        errors.append(
            f"shots must contain exactly {PLAN_SHOT_COUNT} entries for V1"
        )
        return errors

    seen_slots: set = set()
    total_ms = 0
    for shot in shots:
        shot_errors = validate_shot_payload(shot)
        errors.extend(shot_errors)
        if not shot_errors and _is_dict(shot):
            seen_slots.add(shot["slot_index"])
            total_ms += shot["duration_ms"]
    if len(seen_slots) != PLAN_SHOT_COUNT:
        errors.append("shots slot_index must cover 1..5 exactly once")
    if not PLAN_DURATION_MIN_MS <= total_ms <= PLAN_DURATION_MAX_MS:
        errors.append(
            f"shots total duration {total_ms}ms outside "
            f"{PLAN_DURATION_MIN_MS}-{PLAN_DURATION_MAX_MS}ms"
        )
    if _is_dict(render_contract) and _is_int(render_contract.get("target_duration_ms")):
        target_ms = render_contract["target_duration_ms"]
        if total_ms != target_ms:
            errors.append(
                f"shots total duration {total_ms}ms must equal render target {target_ms}ms"
            )
    return errors


def validate_shot_payload(shot: Any) -> List[str]:
    if not _is_dict(shot):
        return [f"shot must be an object, got {type(shot).__name__}"]
    errors: List[str] = []
    _require_int(errors, shot, "slot_index", minimum=1, maximum=PLAN_SHOT_COUNT)
    _require_enum(errors, shot, "slot_role", tuple(sorted(
        {"hero", "full_look", "lifestyle", "detail", "second_angle"}
    )))
    _require_str(errors, shot, "purpose")
    _require_int(errors, shot, "duration_ms", minimum=500, maximum=8000)
    _require_str(errors, shot, "motion_preset")
    _require_str(errors, shot, "transition_out")
    _require_str(errors, shot, "overlay_text", allow_empty=True)
    _require_str(errors, shot, "generation_prompt", allow_empty=True)
    _require_list(errors, shot, "source_refs")
    if shot.get("narrative_function") is not None:
        _require_enum(errors, shot, "narrative_function", NARRATIVE_FUNCTIONS)
    return errors


def validate_bgm_selection_payload(payload: Mapping[str, Any]) -> List[str]:
    """Validate the opv-bgm-v1 structure stored in platform_metadata_json.

    Field names below are our own persistence contract only. They must NOT be
    used as NeoBund API payload until the real request fields are captured.
    """
    errors: List[str] = []
    _check_schema_version(errors, payload, BGM_SCHEMA_VERSION)
    _require_enum(errors, payload, "audio_strategy", AUDIO_STRATEGIES)
    _require_str(errors, payload, "selection_policy_version")
    _require_country(errors, payload, "country")
    for section in ("selected", "actual"):
        block = payload.get(section)
        if not _is_dict(block):
            errors.append(f"{section} must be an object")
    fallback_reason = payload.get("fallback_reason")
    if fallback_reason is not None and not _is_str(fallback_reason):
        errors.append("fallback_reason must be a string or null")
    return errors


def validate_idempotency_digest_key(value: Any) -> List[str]:
    if not _is_str(value) or not re.fullmatch(r"[0-9a-f]{64}", value or ""):
        return ["idempotency_key must be a 64-char lowercase sha256 hex digest"]
    return []


# --------------------------------------------------------------------------
# Capability upgrade contracts (product-driven content stories)
# --------------------------------------------------------------------------

PRODUCT_FACTS_SCHEMA_VERSION = "opv-product-facts-v1"
CONTENT_RECIPE_SCHEMA_VERSION = "opv-content-recipe-v1"
OUTFIT_PLAN_SCHEMA_VERSION = "opv-outfit-plan-v1"
CONTENT_PACKAGE_SCHEMA_VERSION = "opv-content-package-v1"

NARRATIVE_FUNCTIONS = ("HOOK", "CONTEXT", "TRANSFORMATION", "PROOF", "PAYOFF")

# Product identity that recipes/themes/outfits may never change.
PRODUCT_LOCKED_FEATURES = (
    "color",
    "pattern",
    "material",
    "fit",
    "length",
    "collar",
    "sleeve",
    "closure",
    "pockets",
)


def validate_product_facts(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, PRODUCT_FACTS_SCHEMA_VERSION)
    _require_str(errors, payload, "product_id")
    _require_str(errors, payload, "product_name")
    _require_str(errors, payload, "category")
    _require_str(errors, payload, "snapshot_version")
    _require_list(errors, payload, "reference_images", non_empty=True)
    facts = payload.get("facts")
    if not _is_dict(facts):
        errors.append("facts must be an object")
    locked = payload.get("locked_features")
    if not _is_list(locked) or not locked:
        errors.append("locked_features must be a non-empty list")
    return errors


def validate_content_recipe_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, CONTENT_RECIPE_SCHEMA_VERSION)
    _require_str(errors, payload, "recipe_id")
    _require_str(errors, payload, "recipe_key")
    _require_int(errors, payload, "recipe_version", minimum=1)
    _require_str(errors, payload, "content_goal")
    _require_enum(errors, payload, "status", ("draft", "active", "deprecated"))
    _require_int(errors, payload, "shot_count", minimum=1, maximum=10)
    _require_int(errors, payload, "anchor_slot", minimum=1, maximum=10)
    anchor = payload.get("anchor_slot")
    shot_count = payload.get("shot_count")
    if _is_int(anchor) and _is_int(shot_count) and anchor > shot_count:
        errors.append("anchor_slot must be <= shot_count")
    _require_list(errors, payload, "hook_types", non_empty=True)
    structure = payload.get("story_structure")
    if not _is_list(structure) or len(structure) != payload.get("shot_count"):
        errors.append(
            f"story_structure must contain exactly {payload.get('shot_count')} slots"
        )
        return errors
    functions_seen: List[str] = []
    for slot in structure:
        if not _is_dict(slot):
            errors.append(f"story slot must be an object, got {slot!r}")
            continue
        _require_int(errors, slot, "slot_index", minimum=1, maximum=shot_count or 10)
        _require_enum(errors, slot, "narrative_function", NARRATIVE_FUNCTIONS)
        _require_str(errors, slot, "slot_role")
        _require_str(errors, slot, "purpose")
        function_value = slot.get("narrative_function")
        if _is_str(function_value):
            functions_seen.append(function_value)
    expected = list(NARRATIVE_FUNCTIONS)[: len(structure)]
    if functions_seen and functions_seen != expected:
        errors.append(
            f"narrative functions must follow the {expected} arc, got {functions_seen}"
        )
    _require_dict(errors, payload, "copy_style")
    _require_list(errors, payload, "suitable_topics", non_empty=True)
    _require_str(errors, payload, "render_profile_id")
    _require_str(errors, payload, "quality_profile_id")
    return errors


def validate_outfit_plan_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, OUTFIT_PLAN_SCHEMA_VERSION)
    _require_str(errors, payload, "outfit_plan_id")
    _require_str(errors, payload, "anchor_product")
    _require_str(errors, payload, "theme")
    _require_str(errors, payload, "occasion")
    _require_str(errors, payload, "climate")
    _require_str(errors, payload, "styling_logic")
    for key in ("bottom", "shoes", "color_palette", "product_visibility_rules"):
        value = payload.get(key)
        if value is None:
            errors.append(f"{key} is required")
    if payload.get("product_is_core") is not True:
        errors.append("product must be the core item (product_is_core=true)")
    return errors


def validate_content_package_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, CONTENT_PACKAGE_SCHEMA_VERSION)
    _require_str(errors, payload, "content_package_id")
    _require_str(errors, payload, "task_id")
    _require_enum(
        errors,
        payload,
        "status",
        ("planning", "generating", "qa_review", "ready", "rendered", "invalid"),
    )
    _require_dict(errors, payload, "generation_lineage", non_empty=False)
    return errors


RENDER_PROFILE_SCHEMA_VERSION = "opv-render-profile-v1"
QUALITY_PROFILE_SCHEMA_VERSION = "opv-quality-profile-v1"


def validate_render_profile_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, RENDER_PROFILE_SCHEMA_VERSION)
    _require_str(errors, payload, "render_profile_id")
    _require_str(errors, payload, "profile_key")
    _require_enum(errors, payload, "status", ("draft", "active", "deprecated"))
    _require_str(errors, payload, "aspect_ratio")
    _require_str(errors, payload, "codec")
    _require_int(errors, payload, "profile_version", minimum=1)
    _require_int(errors, payload, "duration_min_ms", minimum=1000)
    _require_int(errors, payload, "duration_max_ms", minimum=1000)
    low = payload.get("duration_min_ms")
    high = payload.get("duration_max_ms")
    if _is_int(low) and _is_int(high) and low > high:
        errors.append("duration_min_ms must be <= duration_max_ms")
    _require_int(errors, payload, "fps", minimum=24, maximum=60)
    _require_int(errors, payload, "hook_window_ms", minimum=500)
    _require_dict(errors, payload, "motion_rules")
    _require_dict(errors, payload, "transition_rules")
    return errors


def validate_quality_profile_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    _check_schema_version(errors, payload, QUALITY_PROFILE_SCHEMA_VERSION)
    _require_str(errors, payload, "quality_profile_id")
    _require_str(errors, payload, "profile_key")
    _require_enum(errors, payload, "status", ("draft", "active", "deprecated"))
    _require_int(errors, payload, "profile_version", minimum=1)
    _require_dict(errors, payload, "dimensions")
    dims = payload.get("dimensions")
    if _is_dict(dims):
        expected = {"product_fidelity", "outfit_quality", "persona_consistency",
                    "narrative_quality", "group_quality"}
        missing = expected - set(dims)
        if missing:
            errors.append(f"quality dimensions missing: {sorted(missing)}")
        for key, rule in dims.items():
            if not _is_dict(rule):
                errors.append(f"quality dimension {key} must be an object")
                continue
            scopes = rule.get("scope")
            if not _is_list(scopes) or not scopes or any(
                scope not in ("single", "group") for scope in scopes
            ):
                errors.append(
                    f"quality dimension {key}.scope must use single/group"
                )
            score = rule.get("min_score")
            if not _is_int(score) or not 0 <= score <= 100:
                errors.append(
                    f"quality dimension {key}.min_score must be 0..100"
                )
            if not isinstance(rule.get("hard_gate"), bool):
                errors.append(f"quality dimension {key}.hard_gate must be boolean")
            if not _is_str(rule.get("fail_action")):
                errors.append(
                    f"quality dimension {key}.fail_action must be a string"
                )
    return errors
