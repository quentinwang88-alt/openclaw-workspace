"""Deterministic contract checks for the TH temperature-layering flow.

This module deliberately has no workflow, model, or repository dependencies.
It validates the frozen plan before any paid image work starts.  Visual facts
belong to :mod:`services.photo_layering_qa`; this module only checks that the
plan itself is internally executable.
"""
from __future__ import annotations

import copy
from collections import defaultdict
from typing import Any, Mapping, Sequence


LAYERING_SOURCE_ROLES = ("base", "mid", "outer")
LAYERING_FLOW = "layering_two_step"


class LayeringPlanContractError(ValueError):
    """The frozen layering plan violates an executable contract."""


def _required_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LayeringPlanContractError(f"{label} must be an object")
    return value


def _required_nonempty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LayeringPlanContractError(f"{label} must be a non-empty string")
    return value.strip()


def _required_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LayeringPlanContractError(f"{label} must be an integer")
    return value


def _source_roles(contract: Mapping[str, Any]) -> tuple[str, ...]:
    roles = tuple(str(value) for value in contract.get("source_roles") or ())
    if roles != LAYERING_SOURCE_ROLES:
        raise LayeringPlanContractError(
            f"layering_contract.source_roles must be {list(LAYERING_SOURCE_ROLES)}, "
            f"got {list(roles)}"
        )
    return roles


def _bands(contract: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw = contract.get("bands")
    if not isinstance(raw, list) or not raw:
        raise LayeringPlanContractError("layering_contract.bands must be a non-empty list")
    result: dict[str, Mapping[str, Any]] = {}
    for index, item in enumerate(raw):
        band = _required_mapping(item, f"layering_contract.bands[{index}]")
        key = _required_nonempty_string(band.get("key"), f"bands[{index}].key")
        if key in result:
            raise LayeringPlanContractError(f"duplicate band key {key}")
        bounds = _required_mapping(
            band.get("final_visible_layer_bounds"),
            f"band {key}.final_visible_layer_bounds",
        )
        minimum = _required_integer(bounds.get("min"), f"band {key} bounds.min")
        maximum = _required_integer(bounds.get("max"), f"band {key} bounds.max")
        if minimum < 1 or maximum < minimum:
            raise LayeringPlanContractError(f"band {key} has invalid final layer bounds")
        result[key] = band
    return result


def _stack_entries(look: Mapping[str, Any], label: str) -> tuple[dict[str, str], ...]:
    raw = look.get("expected_layer_stack")
    if not isinstance(raw, list) or not raw:
        raise LayeringPlanContractError(f"{label}.expected_layer_stack must be non-empty")
    entries: list[dict[str, str]] = []
    garment_ids: set[str] = set()
    for index, item in enumerate(raw):
        entry = _required_mapping(item, f"{label}.expected_layer_stack[{index}]")
        garment_id = _required_nonempty_string(
            entry.get("garment_id"), f"{label}.expected_layer_stack[{index}].garment_id"
        )
        if garment_id in garment_ids:
            raise LayeringPlanContractError(f"{label} repeats garment_id {garment_id}")
        garment_ids.add(garment_id)
        item_type = str(entry.get("item_type") or "").strip().lower()
        entries.append({
            "garment_id": garment_id,
            "item_type": item_type,
            "slot": str(entry.get("slot") or "").strip(),
        })
    return tuple(entries)


def _item_rules(contract: Mapping[str, Any], band: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    allowed = band.get("allowed_item_types", contract.get("allowed_item_types", []))
    forbidden = band.get("forbidden_item_types", contract.get("forbidden_item_types", []))
    if not isinstance(allowed, list) or not isinstance(forbidden, list):
        raise LayeringPlanContractError("allowed_item_types/forbidden_item_types must be lists")
    return (
        {str(value).strip().lower() for value in allowed if str(value).strip()},
        {str(value).strip().lower() for value in forbidden if str(value).strip()},
    )


class PlanContractValidator:
    """Validate one normalized ``layering_two_step`` plan.

    ``validate`` returns a defensive copy so callers may freeze it directly.
    The method raises on the first contract violation; it never repairs or
    invents plan fields.
    """

    def validate(
        self, plan: Mapping[str, Any], *, layering_contract: Mapping[str, Any]
    ) -> dict[str, Any]:
        plan = _required_mapping(plan, "plan")
        contract = _required_mapping(layering_contract, "layering_contract")
        roles = _source_roles(contract)
        if str(plan.get("flow") or "") != LAYERING_FLOW:
            raise LayeringPlanContractError(f"plan.flow must be {LAYERING_FLOW}")
        bands = _bands(contract)
        sensitivity_rules = _required_mapping(
            contract.get("sensitivity_rules"), "layering_contract.sensitivity_rules"
        )
        posts = plan.get("posts")
        if not isinstance(posts, list) or not posts:
            raise LayeringPlanContractError("plan.posts must be a non-empty list")
        for index, post in enumerate(posts):
            self._validate_post(
                _required_mapping(post, f"posts[{index}]"),
                roles=roles,
                bands=bands,
                sensitivity_rules=sensitivity_rules,
                contract=contract,
                label=f"posts[{index}]",
            )
        self._validate_cross_band_posts(posts, contract=contract)
        return copy.deepcopy(dict(plan))

    def _validate_post(
        self,
        post: Mapping[str, Any],
        *,
        roles: Sequence[str],
        bands: Mapping[str, Mapping[str, Any]],
        sensitivity_rules: Mapping[str, Any],
        contract: Mapping[str, Any],
        label: str,
    ) -> None:
        binding = _required_mapping(post.get("profile_binding"), f"{label}.profile_binding")
        band_key = _required_nonempty_string(binding.get("band_key"), f"{label}.band_key")
        if band_key not in bands:
            raise LayeringPlanContractError(f"{label} references unknown band {band_key}")
        sensitivity = binding.get("thermal_sensitivity")
        if isinstance(sensitivity, (list, tuple, set)):
            raise LayeringPlanContractError(
                f"{label}.thermal_sensitivity must freeze exactly one value"
            )
        sensitivity = _required_nonempty_string(
            sensitivity, f"{label}.thermal_sensitivity"
        )
        if sensitivity not in sensitivity_rules:
            raise LayeringPlanContractError(
                f"{label}.thermal_sensitivity {sensitivity!r} is not declared by the contract"
            )
        for field in ("scene", "family_key", "series_id"):
            _required_nonempty_string(binding.get(field), f"{label}.{field}")
        cross_rules = contract.get("cross_band_rules") or {}
        if (isinstance(cross_rules, Mapping)
                and cross_rules.get("thermal_index_non_decreasing")):
            thermal_index = _required_integer(
                binding.get("thermal_index"), f"{label}.thermal_index"
            )
            if thermal_index < 0:
                raise LayeringPlanContractError(f"{label}.thermal_index must be non-negative")

        presentation = tuple(str(value) for value in post.get("presentation_order") or ())
        generation = tuple(str(value) for value in post.get("generation_order") or ())
        if presentation != tuple(roles):
            raise LayeringPlanContractError(
                f"{label}.presentation_order must be {list(roles)}"
            )
        if generation != tuple(reversed(roles)):
            raise LayeringPlanContractError(
                f"{label}.generation_order must be {list(reversed(roles))}"
            )

        looks = post.get("looks")
        if not isinstance(looks, list):
            raise LayeringPlanContractError(f"{label}.looks must be a list")
        by_role: dict[str, Mapping[str, Any]] = {}
        for look in looks:
            look = _required_mapping(look, f"{label}.looks[]")
            role = str(look.get("role") or "")
            if role not in roles:
                raise LayeringPlanContractError(f"{label}.looks has unknown role {role!r}")
            if role in by_role:
                raise LayeringPlanContractError(f"{label}.looks repeats role {role}")
            by_role[role] = look
        if tuple(by_role) != tuple(roles):
            if set(by_role) != set(roles):
                raise LayeringPlanContractError(
                    f"{label}.looks must cover exactly {list(roles)}, got {list(by_role)}"
                )

        allowed, forbidden = _item_rules(contract, bands[band_key])
        previous_ids: set[str] | None = None
        previous_stack_ids: list[str] | None = None
        base_stack_ids: list[str] = []
        final_count = 0
        for role in roles:
            look = by_role[role]
            count = _required_integer(
                look.get("expected_visible_layer_count"),
                f"{label}.{role}.expected_visible_layer_count",
            )
            stack = _stack_entries(look, f"{label}.{role}")
            if count != len(stack):
                raise LayeringPlanContractError(
                    f"{label}.{role} visible layer count {count} does not match stack size {len(stack)}"
                )
            ids = {entry["garment_id"] for entry in stack}
            stack_ids = [entry["garment_id"] for entry in stack]
            if role == "base":
                base_stack_ids = stack_ids
            if previous_ids is not None:
                if (not previous_ids < ids
                        or previous_stack_ids != stack_ids[:len(previous_stack_ids)]):
                    raise LayeringPlanContractError(
                        f"{label} layering must be strict stack inclusion: previous={sorted(previous_ids)}, "
                        f"{role}={sorted(ids)}"
                    )
                if len(ids - previous_ids) != 1:
                    raise LayeringPlanContractError(
                        f"{label}.{role} must add exactly one garment to the previous state"
                    )
                added = _required_nonempty_string(
                    look.get("added_garment_id"), f"{label}.{role}.added_garment_id"
                )
                if ids - previous_ids != {added}:
                    raise LayeringPlanContractError(
                        f"{label}.{role}.added_garment_id does not match its stack delta"
                    )
            item_types = {entry["item_type"] for entry in stack if entry["item_type"]}
            if (allowed or forbidden) and any(not entry["item_type"] for entry in stack):
                raise LayeringPlanContractError(
                    f"{label}.{role} stack entries require item_type when item rules are active"
                )
            if forbidden & item_types:
                raise LayeringPlanContractError(
                    f"{label}.{role} contains forbidden item types {sorted(forbidden & item_types)}"
                )
            if allowed and item_types - allowed:
                raise LayeringPlanContractError(
                    f"{label}.{role} contains item types outside allowlist {sorted(item_types - allowed)}"
                )
            all_items = {
                str(item.get("item_type") or "").strip().lower()
                for item in look.get("items") or [] if isinstance(item, Mapping)
            }
            if "" in all_items:
                raise LayeringPlanContractError(f"{label}.{role}.items require item_type")
            if forbidden & all_items:
                raise LayeringPlanContractError(
                    f"{label}.{role}.items contain forbidden item types {sorted(forbidden & all_items)}"
                )
            if allowed and all_items - allowed:
                raise LayeringPlanContractError(
                    f"{label}.{role}.items contain types outside allowlist {sorted(all_items - allowed)}"
                )
            if contract.get("stackability_rules"):
                stackability = _required_mapping(
                    look.get("stackability"), f"{label}.{role}.stackability"
                )
                for field in (
                    "collar_profile", "sleeve_volume", "outer_sleeve_capacity", "length_zone",
                ):
                    _required_nonempty_string(
                        stackability.get(field), f"{label}.{role}.stackability.{field}"
                    )
            previous_ids = ids
            previous_stack_ids = stack_ids
            final_count = count

        if isinstance(cross_rules, Mapping) and cross_rules.get("core_base_consistency") == "WITHIN_SERIES":
            core = _required_nonempty_string(
                binding.get("core_base_garment_id"), f"{label}.core_base_garment_id"
            )
            if not base_stack_ids or core != base_stack_ids[0]:
                raise LayeringPlanContractError(
                    f"{label}.core_base_garment_id must match the innermost base garment"
                )

        bounds = _required_mapping(
            bands[band_key].get("final_visible_layer_bounds"),
            f"band {band_key}.final_visible_layer_bounds",
        )
        if not int(bounds["min"]) <= final_count <= int(bounds["max"]):
            raise LayeringPlanContractError(
                f"{label}.outer layer count {final_count} is outside band {band_key} bounds"
            )

        sensitivity_rule = _required_mapping(
            sensitivity_rules[sensitivity], f"sensitivity_rules.{sensitivity}"
        )
        layer_choice = str(sensitivity_rule.get("layer_choice") or "")
        expected_final = {
            "LOWER_BOUND": int(bounds["min"]),
            "UPPER_BOUND": int(bounds["max"]),
            "DEFAULT": int(bands[band_key].get("default_final_visible_layer_count") or 0),
        }.get(layer_choice)
        if expected_final and final_count != expected_final:
            raise LayeringPlanContractError(
                f"{label}.outer layer count {final_count} does not satisfy "
                f"sensitivity {sensitivity} ({layer_choice})"
            )

        scene_rules = next(
            (item for item in contract.get("scene_modifiers") or []
             if isinstance(item, Mapping)
             and str(item.get("key") or "") == str(binding["scene"])),
            None,
        )
        if scene_rules is None and isinstance(contract.get("scene_rules"), Mapping):
            scene_rules = contract["scene_rules"]
        if not isinstance(scene_rules, Mapping):
            raise LayeringPlanContractError(
                f"{label}.scene {binding['scene']!r} is not declared by scene_modifiers"
            )
        if isinstance(scene_rules, Mapping) and scene_rules.get("may_change_layer_stack") is False:
            modifier = post.get("scene_modifier") or {}
            if not isinstance(modifier, Mapping):
                raise LayeringPlanContractError(f"{label}.scene_modifier must be an object")
            forbidden_fields = {
                "layer_stack", "visible_layer_count", "add_items", "remove_items",
                "add_item_types", "remove_item_types",
            }
            changed = sorted(field for field in forbidden_fields if modifier.get(field) not in (None, [], {}))
            if changed:
                raise LayeringPlanContractError(
                    f"{label}.scene_modifier may not change the layer stack: {changed}"
                )
            allowed_modifier_fields = set(scene_rules.get("allowed_modifier_fields") or (
                "footwear_type", "outerwear_state", "pose", "background", "mobility_level",
            ))
            unknown_fields = sorted(set(modifier) - allowed_modifier_fields)
            if unknown_fields:
                raise LayeringPlanContractError(
                    f"{label}.scene_modifier contains unsupported fields: {unknown_fields}"
                )

    def _validate_cross_band_posts(
        self, posts: Sequence[Mapping[str, Any]], *, contract: Mapping[str, Any]
    ) -> None:
        rules = contract.get("cross_band_rules") or {}
        if not isinstance(rules, Mapping):
            raise LayeringPlanContractError("cross_band_rules must be an object")
        order = [str(value) for value in rules.get("temperature_order") or []]
        if not order:
            return
        rank = {band: index for index, band in enumerate(order)}
        grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
        for post in posts:
            binding = post["profile_binding"]
            grouped[(str(binding["series_id"]), str(binding["thermal_sensitivity"]),
                     str(binding["scene"]))].append(post)
        for key, group in grouped.items():
            ordered = sorted(group, key=lambda value: rank.get(str(value["profile_binding"]["band_key"]), 10**6))
            previous_count: int | None = None
            previous_thermal: int | None = None
            previous_core: str | None = None
            for post in ordered:
                binding = post["profile_binding"]
                if str(binding["band_key"]) not in rank:
                    continue
                looks = {str(look["role"]): look for look in post["looks"]}
                count = int(looks["outer"]["expected_visible_layer_count"])
                thermal = binding.get("thermal_index")
                if thermal is not None:
                    thermal = _required_integer(thermal, "profile_binding.thermal_index")
                core = str(binding.get("core_base_garment_id") or "").strip()
                if previous_count is not None and rules.get("final_layer_count_non_decreasing") and count < previous_count:
                    raise LayeringPlanContractError(
                        f"cross-band final layer count regressed for series/sensitivity/scene {key}"
                    )
                if (previous_thermal is not None and thermal is not None
                        and rules.get("thermal_index_non_decreasing") and thermal < previous_thermal):
                    raise LayeringPlanContractError(
                        f"cross-band thermal_index regressed for series/sensitivity/scene {key}"
                    )
                if (previous_core and core and rules.get("core_base_consistency") == "WITHIN_SERIES"
                        and core != previous_core):
                    raise LayeringPlanContractError(
                        f"cross-band core base changed for series/sensitivity/scene {key}"
                    )
                previous_count = count
                previous_thermal = thermal if thermal is not None else previous_thermal
                previous_core = core or previous_core


def validate_layering_plan(
    plan: Mapping[str, Any], *, layering_contract: Mapping[str, Any]
) -> dict[str, Any]:
    """Functional entry point used by flow handlers."""
    return PlanContractValidator().validate(plan, layering_contract=layering_contract)
