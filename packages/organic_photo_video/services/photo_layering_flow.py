"""Executable planning adapter for the TH temperature-layering flow."""
from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Mapping, Sequence

from services.photo_copy import (
    contract_copy_tokens, fill_contract_copy_tokens,
)
from services.photo_layering_contract import (
    LayeringPlanContractError, validate_layering_plan,
)


LAYERING_FLOW = "layering_two_step"


class PhotoLayeringFlowError(ValueError):
    pass


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def _fill_copy(value: Any, tokens: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        return fill_contract_copy_tokens(
            value, token_values=tokens, contract_label="温度穿搭",
        )
    if isinstance(value, list):
        return [_fill_copy(item, tokens) for item in value]
    if isinstance(value, Mapping):
        return {key: _fill_copy(item, tokens) for key, item in value.items()}
    return copy.deepcopy(value)


def _profile_for_variables(recipe_spec: Mapping[str, Any], variables: Mapping[str, Any]) -> Mapping[str, Any]:
    band_key = str(variables.get("band_key") or "")
    profiles = list(recipe_spec.get("execution_profiles") or [])
    profile = next(
        (item for item in profiles
         if str((item.get("variables") or {}).get("band_key") or "") == band_key),
        None,
    )
    if not isinstance(profile, Mapping):
        raise PhotoLayeringFlowError(f"温度穿搭缺少 {band_key or '未填写'} 的执行 profile")
    return profile


def resolve_profile_binding(
    recipe_spec: Mapping[str, Any], variables: Mapping[str, Any],
    *, family: Mapping[str, Any], profile: Mapping[str, Any],
) -> dict[str, Any]:
    frozen = {**dict(profile.get("variables") or {}), **dict(variables or {})}
    required = ("band_key", "thermal_sensitivity", "scene", "style_series")
    missing = [key for key in required if not str(frozen.get(key) or "").strip()]
    if missing:
        raise PhotoLayeringFlowError("温度穿搭输入缺少：" + "、".join(missing))
    band_order = {"t15": 10, "t10": 20, "t5": 30, "t0": 40}
    family_looks = list(family.get("looks") or [])
    base_stack = list((family_looks[0].get("expected_layer_stack") or []) if family_looks else [])
    core_base = str(
        (base_stack[0].get("garment_id") if isinstance(base_stack[0], Mapping) else base_stack[0])
        if base_stack else ""
    )
    return {
        "profile_id": str(profile.get("profile_id") or ""),
        "band_key": str(frozen["band_key"]),
        "thermal_sensitivity": str(frozen["thermal_sensitivity"]),
        "scene": str(frozen["scene"]),
        "style_series": str(frozen["style_series"]),
        "family_key": str(family.get("family_id") or ""),
        "series_id": str(family.get("series_id") or frozen["style_series"]),
        "thermal_index": band_order.get(str(frozen["band_key"]), 0),
        "core_base_garment_id": core_base,
        "asset_set_key": str((profile.get("asset_set_keys") or [""])[0]),
        "variables": frozen,
    }


def _normalize_family_looks(family: Mapping[str, Any]) -> list[dict[str, Any]]:
    looks = [copy.deepcopy(dict(item)) for item in family.get("looks") or []]
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
    for look in looks:
        stack_ids = [
            str(item.get("garment_id") if isinstance(item, Mapping) else item)
            for item in look.get("expected_layer_stack") or []
        ]
        stack = []
        for garment_id in stack_ids:
            entry = item_catalog.get(garment_id)
            if not entry:
                raise PhotoLayeringFlowError(
                    f"family {family.get('family_id')} 的层栈缺少单品定义 {garment_id}"
                )
            stack.append(dict(entry))
        items = list(look.get("items") or [])
        by_slot = {str(item.get("slot") or ""): item for item in items}
        upper = [entry for entry in stack]
        outer_piece = upper[-1] if len(upper) > 1 else None
        normalized.append({
            **look,
            "expected_layer_stack": stack,
            "top_inner": " + ".join(item["item_type"] for item in upper[:-1] or upper),
            "outerwear": (
                str((outer_piece or {}).get("item_type") or "无额外外层")
                if outer_piece else "无额外外层"
            ),
            "bottom": str((by_slot.get("bottom") or {}).get("item_type") or "trousers"),
            "shoes": str((by_slot.get("shoes") or {}).get("item_type") or "shoes"),
            "styling_intent": str(family.get("styling_intent_zh") or ""),
            "outfit_reference_indices": list(look.get("outfit_reference_indices") or []),
        })
    return normalized


def build_layering_content_plan(
    *, record_id: str, recipe_id: str, recipe_spec: Mapping[str, Any],
    policy: Mapping[str, Any], theme: Mapping[str, Any], reference_mode: str,
    variables: Mapping[str, Any], copy_templates: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build one normalized, source-role-keyed layering post."""
    if reference_mode not in set(policy.get("supported_reference_modes") or []):
        raise PhotoLayeringFlowError(
            f"温度穿搭当前不支持参考模式 {reference_mode or '未选择'}"
        )
    if str(theme.get("theme_key") or "") not in set(policy.get("supported_theme_keys") or []):
        raise PhotoLayeringFlowError("温度穿搭主题与 planning policy 不一致")
    profile = _profile_for_variables(recipe_spec, variables)
    effective_variables = {**dict(profile.get("variables") or {}), **dict(variables or {})}
    if policy.get("canary_only") is True:
        profile_variables = dict(profile.get("variables") or {})
        drift = sorted(
            key for key in ("band_key", "thermal_sensitivity", "scene", "style_series")
            if effective_variables.get(key) != profile_variables.get(key)
        )
        if drift:
            raise PhotoLayeringFlowError(
                "温度穿搭 Phase 1a 仅开放 15°C/正常体感/通勤 canary；"
                "以下变量尚未进入 Phase 2：" + "、".join(drift)
            )
    band_key = str(effective_variables.get("band_key") or "")
    style_series = str(effective_variables.get("style_series") or "")
    family = next(
        (item for item in policy.get("families") or []
         if str(item.get("band_key") or "") == band_key
         and (not style_series or str(item.get("series_id") or "") == style_series)),
        None,
    )
    if not isinstance(family, Mapping):
        raise PhotoLayeringFlowError(
            f"温度穿搭 family 尚未覆盖 {band_key}/{style_series or '未选择系列'}"
        )
    binding = resolve_profile_binding(
        recipe_spec, effective_variables, family=family, profile=profile,
    )
    looks = _normalize_family_looks(family)
    variants = list(copy_templates or profile.get("copy_variants") or [])
    if not variants:
        raise PhotoLayeringFlowError("温度穿搭缺少审核文案模板")
    selection = int.from_bytes(hashlib.sha256(
        f"{record_id}:{band_key}".encode("utf-8")
    ).digest()[:4], "big") % len(variants)
    copy_variant = dict(variants[selection])
    tokens = contract_copy_tokens(recipe_spec, effective_variables)
    frozen_copy = _fill_copy(dict(copy_variant.get("copy") or {}), tokens)
    post = {
        "index": 1,
        "planning_flow": LAYERING_FLOW,
        "required_roles": list((recipe_spec.get("asset_requirements") or {}).get("required_roles") or []),
        "profile_binding": binding,
        "family_id": str(family.get("family_id") or ""),
        "variation_id": str(family.get("family_id") or ""),
        "theme_key": str(theme.get("theme_key") or ""),
        "angle_zh": str(family.get("angle_zh") or "温度分层穿搭"),
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
            "band_key": band_key,
            "thermal_sensitivity": str(effective_variables.get("thermal_sensitivity") or ""),
            "scene": str(effective_variables.get("scene") or ""),
        },
        "style_profile": {
            "planning_flow": LAYERING_FLOW,
            "layering_contract": copy.deepcopy(dict(recipe_spec.get("layering_contract") or {})),
            "required_roles": list((recipe_spec.get("asset_requirements") or {}).get("required_roles") or []),
        },
    }
    normalized = {
        "schema_version": "opv-photo-flow-plan-v1",
        "flow": LAYERING_FLOW,
        "planning_flow": LAYERING_FLOW,
        "record_id": record_id,
        "recipe_id": recipe_id,
        "policy_id": str(policy.get("policy_id") or ""),
        "policy_version": int(policy.get("policy_version") or 0),
        "theme_key": str(theme.get("theme_key") or ""),
        "reference_mode": reference_mode,
        "count": 1,
        "required_roles": list(post["required_roles"]),
        "posts": [post],
        "items": [post],
    }
    try:
        validate_layering_plan(
            normalized, layering_contract=recipe_spec.get("layering_contract") or {},
        )
    except LayeringPlanContractError as exc:
        raise PhotoLayeringFlowError(str(exc)) from exc
    normalized["plan_sha256"] = _fingerprint(normalized)
    return normalized
