"""Executable planning adapter for the TH daily thermal-transition flow.

One record produces exactly one post: the same person crossing outside heat,
cool transit and strong office air conditioning.  Presentation runs
``base → mid → outer`` (hot outside first), generation runs ``outer → mid →
base`` so every later state can be produced by *removing* one layer from a
thicker anchor — the same subtractive mechanics the layering line uses, which
keeps the person, camera and frozen base outfit stable.
"""
from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Mapping, Sequence

from services.photo_copy import (
    contract_copy_tokens, fill_contract_copy_tokens,
)
from services.photo_thermal_transition_contract import (
    THERMAL_TRANSITION_FLOW, ThermalTransitionContractError,
    validate_thermal_transition_plan,
)


# Kept as a module-level alias so callers do not re-declare the flow id.
FLOW = THERMAL_TRANSITION_FLOW

REQUIRED_VARIABLES = (
    "transition_key", "thermal_sensitivity", "dress_code", "style_series",
    "temperature_label_mode",
)


class PhotoThermalTransitionFlowError(ValueError):
    pass


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def _fill_copy(value: Any, tokens: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        return fill_contract_copy_tokens(
            value, token_values=tokens, contract_label="冷热切换",
        )
    if isinstance(value, list):
        return [_fill_copy(item, tokens) for item in value]
    if isinstance(value, Mapping):
        return {key: _fill_copy(item, tokens) for key, item in value.items()}
    return copy.deepcopy(value)


def _profile_for_variables(
    recipe_spec: Mapping[str, Any], variables: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Pick the frozen execution profile, failing loudly for closed combos.

    Phase 1 is canary-only, so an unimplemented ``transition_key`` must raise an
    explicit "not open yet" error.  Falling back to another scene silently is
    exactly the degradation this gate exists to prevent.
    """
    transition_key = str(variables.get("transition_key") or "")
    profiles = list(recipe_spec.get("execution_profiles") or [])
    open_keys = sorted({
        str((item.get("variables") or {}).get("transition_key") or "")
        for item in profiles
    } - {""})
    profile = next(
        (item for item in profiles
         if str((item.get("variables") or {}).get("transition_key") or "") == transition_key),
        None,
    )
    if not isinstance(profile, Mapping):
        raise PhotoThermalTransitionFlowError(
            "冷热切换 Phase 1 canary 尚未开放 transition_key="
            f"{transition_key or '未填写'}；当前仅开放：{'、'.join(open_keys) or '无'}"
        )
    return profile


def resolve_profile_binding(
    recipe_spec: Mapping[str, Any], variables: Mapping[str, Any],
    *, family: Mapping[str, Any], profile: Mapping[str, Any],
) -> dict[str, Any]:
    frozen = {**dict(profile.get("variables") or {}), **dict(variables or {})}
    missing = [key for key in REQUIRED_VARIABLES if not str(frozen.get(key) or "").strip()]
    if missing:
        raise PhotoThermalTransitionFlowError("冷热切换输入缺少：" + "、".join(missing))
    family_looks = list(family.get("looks") or [])
    base_stack = list((family_looks[0].get("expected_layer_stack") or []) if family_looks else [])
    core_base = str(
        (base_stack[0].get("garment_id") if isinstance(base_stack[0], Mapping) else base_stack[0])
        if base_stack else ""
    )
    declared = [
        str(value).strip() for value in (frozen.get("sourced_temperature_values") or [])
        if str(value).strip()
    ]
    sourced: list[str] = []
    for value in declared:
        sourced.extend([f"{value}°C", f"{value}℃"])
    return {
        "profile_id": str(profile.get("profile_id") or ""),
        "transition_key": str(frozen["transition_key"]),
        "thermal_sensitivity": str(frozen["thermal_sensitivity"]),
        "dress_code": str(frozen["dress_code"]),
        "style_series": str(frozen["style_series"]),
        "temperature_label_mode": str(frozen["temperature_label_mode"]),
        "family_key": str(family.get("family_id") or ""),
        "series_id": str(family.get("series_id") or frozen["style_series"]),
        "core_base_garment_id": core_base,
        "asset_set_key": str((profile.get("asset_set_keys") or [""])[0]),
        "sourced_temperature_values": sourced,
        "variables": frozen,
    }


def _normalize_family_looks(
    family: Mapping[str, Any], *, contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Freeze each family look with its thermal context from the contract."""
    states = list(contract.get("states") or [])
    looks = [copy.deepcopy(dict(item)) for item in family.get("looks") or []]
    if len(looks) != len(states):
        raise PhotoThermalTransitionFlowError(
            f"family {family.get('family_id')} 的层态数量与合同不一致"
        )
    item_catalog: dict[str, dict[str, str]] = {}
    for look in looks:
        for item in look.get("items") or []:
            garment_id = str(item.get("garment_id") or "")
            if garment_id:
                item_catalog[garment_id] = {
                    "garment_id": garment_id,
                    "slot": str(item.get("slot") or ""),
                    "item_type": str(item.get("item_type") or ""),
                }
    normalized = []
    for state, look in zip(states, looks):
        stack_ids = [
            str(item.get("garment_id") if isinstance(item, Mapping) else item)
            for item in look.get("expected_layer_stack") or []
        ]
        stack = []
        for garment_id in stack_ids:
            entry = item_catalog.get(garment_id)
            if not entry:
                raise PhotoThermalTransitionFlowError(
                    f"family {family.get('family_id')} 的层栈缺少单品定义 {garment_id}"
                )
            stack.append(dict(entry))
        items = list(look.get("items") or [])
        by_slot = {str(item.get("slot") or ""): item for item in items}
        upper = [entry for entry in stack]
        outer_piece = upper[-1] if len(upper) > 1 else None
        normalized.append({
            **look,
            "thermal_context": str(state.get("thermal_context") or ""),
            "expected_visible_layer_count": int(state.get("expected_visible_layer_count") or 0),
            "expected_layer_stack": stack,
            "added_garment_id": str(look.get("added_garment_id") or ""),
            "top_inner": " + ".join(item["item_type"] for item in upper[:-1] or upper),
            "outerwear": (
                str((outer_piece or {}).get("item_type") or "无额外外层")
                if outer_piece else "无额外外层"
            ),
            "bottom": str((by_slot.get("bottom") or {}).get("item_type") or "trousers"),
            "shoes": str((by_slot.get("shoes") or {}).get("item_type") or "shoes"),
            "styling_intent": str(family.get("styling_intent_zh") or ""),
        })
    return normalized


def build_thermal_transition_content_plan(
    *, record_id: str, recipe_id: str, recipe_spec: Mapping[str, Any],
    policy: Mapping[str, Any], theme: Mapping[str, Any], reference_mode: str,
    variables: Mapping[str, Any], copy_templates: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build one normalized, source-role-keyed daily thermal-transition post."""
    if reference_mode not in set(policy.get("supported_reference_modes") or []):
        raise PhotoThermalTransitionFlowError(
            f"冷热切换当前不支持参考模式 {reference_mode or '未选择'}"
        )
    if str(theme.get("theme_key") or "") not in set(policy.get("supported_theme_keys") or []):
        raise PhotoThermalTransitionFlowError("冷热切换主题与 planning policy 不一致")

    profile = _profile_for_variables(recipe_spec, variables)
    effective_variables = {**dict(profile.get("variables") or {}), **dict(variables or {})}
    if policy.get("canary_only") is True:
        profile_variables = dict(profile.get("variables") or {})
        drift = sorted(
            key for key in REQUIRED_VARIABLES
            if effective_variables.get(key) != profile_variables.get(key)
        )
        if drift:
            raise PhotoThermalTransitionFlowError(
                "冷热切换 Phase 1 canary 仅开放 室外热→BTS→办公室空调／正常体感／"
                "办公室着装／minimal_city／QUALITATIVE；以下变量尚未开放："
                + "、".join(drift)
            )

    transition_key = str(effective_variables.get("transition_key") or "")
    style_series = str(effective_variables.get("style_series") or "")
    family = next(
        (item for item in policy.get("families") or []
         if str(item.get("transition_key") or "") == transition_key
         and (not style_series or str(item.get("series_id") or "") == style_series)),
        None,
    )
    if not isinstance(family, Mapping):
        raise PhotoThermalTransitionFlowError(
            f"冷热切换 family 尚未覆盖 {transition_key}/{style_series or '未选择系列'}"
        )

    contract = dict(recipe_spec.get("thermal_transition_contract") or {})
    binding = resolve_profile_binding(
        recipe_spec, effective_variables, family=family, profile=profile,
    )
    looks = _normalize_family_looks(family, contract=contract)
    variants = list(copy_templates or profile.get("copy_variants") or [])
    if not variants:
        raise PhotoThermalTransitionFlowError("冷热切换缺少审核文案模板")
    selection = int.from_bytes(hashlib.sha256(
        f"{record_id}:{transition_key}".encode("utf-8")
    ).digest()[:4], "big") % len(variants)
    copy_variant = dict(variants[selection])
    tokens = contract_copy_tokens(recipe_spec, effective_variables)
    frozen_copy = _fill_copy(dict(copy_variant.get("copy") or {}), tokens)
    required_roles = list(
        (recipe_spec.get("asset_requirements") or {}).get("required_roles") or []
    )
    post = {
        "index": 1,
        "planning_flow": FLOW,
        "required_roles": required_roles,
        "profile_binding": binding,
        "family_id": str(family.get("family_id") or ""),
        "variation_id": str(family.get("family_id") or ""),
        "theme_key": str(theme.get("theme_key") or ""),
        "angle_zh": str(family.get("angle_zh") or "冷热切换穿搭"),
        "scene_zh": str(family.get("scene_zh") or ""),
        "palette_zh": str(family.get("palette_zh") or ""),
        "background_color": str(family.get("background_color") or ""),
        "background_prompt": str(family.get("background_prompt") or ""),
        "style_modifier": str(family.get("styling_intent_zh") or ""),
        "outline_plan": copy.deepcopy(dict(family.get("outline_plan") or {})),
        "presentation_type": "MODEL_FULL_BODY",
        "presentation_order": list(policy.get("presentation_order") or []),
        "generation_order": list(policy.get("generation_order") or []),
        "looks": looks,
        "copy": frozen_copy,
        "copy_variant_id": str(copy_variant.get("copy_id") or ""),
        "reference_uses_consumed": ["LAYER_PROGRESSION", "OUTFIT"],
        "scene_modifier": {},
        "difference_axes": {
            "family": str(family.get("family_id") or ""),
            "transition_key": transition_key,
            "thermal_sensitivity": str(effective_variables.get("thermal_sensitivity") or ""),
            "dress_code": str(effective_variables.get("dress_code") or ""),
        },
        "style_profile": {
            "planning_flow": FLOW,
            "thermal_transition_contract": copy.deepcopy(contract),
            "required_roles": required_roles,
        },
    }
    from services.photo_thermal_transition_contract import as_layering_contract
    post["style_profile"]["layering_contract"] = as_layering_contract(
        contract, transition_key=transition_key,
    )
    normalized = {
        "schema_version": "opv-photo-flow-plan-v1",
        "flow": FLOW,
        "planning_flow": FLOW,
        "record_id": record_id,
        "recipe_id": recipe_id,
        "policy_id": str(policy.get("policy_id") or ""),
        "policy_version": int(policy.get("policy_version") or 0),
        "theme_key": str(theme.get("theme_key") or ""),
        "reference_mode": reference_mode,
        "count": 1,
        "required_roles": list(required_roles),
        "posts": [post],
        "items": [post],
    }
    try:
        validate_thermal_transition_plan(
            normalized, thermal_transition_contract=contract,
        )
    except ThermalTransitionContractError as exc:
        raise PhotoThermalTransitionFlowError(str(exc)) from exc
    normalized["plan_sha256"] = _fingerprint(normalized)
    return normalized
