"""Deterministic contract checks for the TH daily thermal-transition flow.

This module deliberately has no workflow, model or repository dependency.  It
validates the frozen plan *before* any paid image work starts.

Business meaning differs from the temperature-layering line: the three states
describe one day crossing outside heat, cool transit and strong office air
conditioning.  The *structural* mechanics (ordered layer stack, strict
inclusion, frozen base/bottom/shoes) are shared, but the semantics — thermal
context per state and the temperature claim rules — are owned here.

Layer-stack structure is reused from :mod:`services.photo_thermal_transition_qa`
via :func:`as_layering_contract`; this module only checks that the frozen plan
itself is internally executable.
"""
from __future__ import annotations

import copy
import re
from typing import Any, Mapping, Sequence


THERMAL_TRANSITION_SOURCE_ROLES = ("base", "mid", "outer")
THERMAL_TRANSITION_FLOW = "thermal_transition_two_step"
THERMAL_TRANSITION_CONTRACT_SCHEMA = "opv-photo-thermal-transition-contract-v1"

TEMPERATURE_LABEL_MODES = ("QUALITATIVE", "EXPLICIT_INPUT")

UNKNOWN_THERMAL_CONTEXT = "UNKNOWN_THERMAL_CONTEXT"
CONTEXT_ORDER_MISMATCH = "CONTEXT_ORDER_MISMATCH"
TEMPERATURE_VALUE_UNSOURCED = "TEMPERATURE_VALUE_UNSOURCED"
BASE_OUTFIT_DRIFT = "BASE_OUTFIT_DRIFT"
TRANSITION_NOT_VISIBLE = "TRANSITION_NOT_VISIBLE"
ITEM_SLOT_DUPLICATED = "ITEM_SLOT_DUPLICATED"

# 32°C / 24℃ / 18 摄氏度 —— 只有带单位的具体温度才算「具体温度数字」。
TEMPERATURE_NUMBER_PATTERN = re.compile(r"\d+\s*(?:°\s*C|℃|摄氏\s*度|度)")
# The garment slots that may legitimately carry the transition delta.  A
# transition can only be shown by adding a wearable upper layer; changing the
# bottom or the shoes is a different outfit, not a thermal transition.
UPPER_LAYER_SLOTS = ("base_top", "mid_piece", "outerwear")


class ThermalTransitionContractError(ValueError):
    """The frozen thermal-transition plan violates an executable contract."""


def _fail(code: str, message: str) -> None:
    raise ThermalTransitionContractError(f"{code}：{message}")


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ThermalTransitionContractError(f"{label} must be an object")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ThermalTransitionContractError(f"{label} must be a non-empty string")
    return value.strip()


def _integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ThermalTransitionContractError(f"{label} must be an integer")
    return value


def _flag(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise ThermalTransitionContractError(f"{label} must be a boolean")
    return value


def contract_roles(contract: Mapping[str, Any]) -> tuple[str, ...]:
    roles = tuple(str(value) for value in contract.get("source_roles") or ())
    if roles != THERMAL_TRANSITION_SOURCE_ROLES:
        raise ThermalTransitionContractError(
            f"thermal_transition_contract.source_roles must be "
            f"{list(THERMAL_TRANSITION_SOURCE_ROLES)}, got {list(roles)}"
        )
    return roles


def contract_states(contract: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    """Validate the three states and return them in frozen order.

    Visible layer counts must be exactly ``1..len(states)``: the first state is
    the hot-outdoor base layer, every later state adds exactly one upper layer.
    """
    roles = contract_roles(contract)
    raw = contract.get("states")
    if not isinstance(raw, list) or len(raw) != len(roles):
        raise ThermalTransitionContractError(
            f"thermal_transition_contract.states must contain exactly {len(roles)} states"
        )
    states: list[Mapping[str, Any]] = []
    contexts: list[str] = []
    for index, item in enumerate(raw):
        state = _mapping(item, f"states[{index}]")
        role = _text(state.get("role"), f"states[{index}].role")
        if role != roles[index]:
            raise ThermalTransitionContractError(
                f"states[{index}].role must be {roles[index]!r}, got {role!r}"
            )
        context = _text(
            state.get("thermal_context"), f"states[{index}].thermal_context"
        )
        if context in contexts:
            raise ThermalTransitionContractError(f"duplicate thermal_context {context}")
        contexts.append(context)
        count = _integer(
            state.get("expected_visible_layer_count"),
            f"states[{index}].expected_visible_layer_count",
        )
        if count != index + 1:
            raise ThermalTransitionContractError(
                f"states[{index}].expected_visible_layer_count must be {index + 1}, got {count}"
            )
        states.append(state)
    return tuple(states)


def transition_entries(contract: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw = contract.get("transitions")
    if not isinstance(raw, list) or not raw:
        raise ThermalTransitionContractError(
            "thermal_transition_contract.transitions must be a non-empty list"
        )
    entries: dict[str, Mapping[str, Any]] = {}
    for index, item in enumerate(raw):
        entry = _mapping(item, f"transitions[{index}]")
        key = _text(entry.get("key"), f"transitions[{index}].key")
        if key in entries:
            raise ThermalTransitionContractError(f"duplicate transition key {key}")
        contexts = entry.get("thermal_contexts")
        if not isinstance(contexts, list) or not contexts:
            raise ThermalTransitionContractError(
                f"transitions[{index}].thermal_contexts must be a non-empty list"
            )
        entries[key] = entry
    return entries


def transition_for(
    contract: Mapping[str, Any], transition_key: str
) -> Mapping[str, Any]:
    """Return one declared transition or fail with a routable code."""
    entries = transition_entries(contract)
    entry = entries.get(str(transition_key or "").strip())
    if entry is None:
        _fail(
            UNKNOWN_THERMAL_CONTEXT,
            f"未声明的冷热切换场景 {transition_key or '未填写'}；"
            f"已声明：{'、'.join(sorted(entries))}",
        )
    return entry


def unsourced_temperature_values(text: Any, *, temperature_label_mode: str) -> list[str]:
    """Return every concrete temperature number found in a copy block.

    The name is historical: for ``QUALITATIVE`` copy *any* number is unsourced,
    while for ``EXPLICIT_INPUT`` the caller still has to match the number
    against a declared source.  Extraction is therefore mode-independent; the
    mode only decides how the caller treats the result.
    """
    _ = temperature_label_mode  # kept so call sites read explicitly
    found: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, str):
            found.extend(match.group(0).strip() for match in TEMPERATURE_NUMBER_PATTERN.finditer(value))
        elif isinstance(value, Mapping):
            for child in value.values():
                walk(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                walk(child)

    walk(text)
    return sorted(set(found))


def as_layering_contract(
    contract: Mapping[str, Any], *, transition_key: str,
) -> dict[str, Any]:
    """Adapt the thermal contract to the shared structural-QA contract shape.

    Only the facts the structural QA needs are derived here (roles plus one
    synthetic band whose key is the transition key); the thermal semantics stay
    in the original contract so nothing is silently redefined.
    """
    roles = contract_roles(contract)
    states = contract_states(contract)
    entry = transition_for(contract, transition_key)
    return {
        "schema_version": "opv-photo-layering-contract-adapter-v1",
        "source_roles": list(roles),
        "bands": [{
            "key": str(entry["key"]),
            "final_visible_layer_bounds": {
                "min": int(states[-1]["expected_visible_layer_count"]),
                "max": int(states[-1]["expected_visible_layer_count"]),
            },
            "allowed_item_types": list(contract.get("allowed_item_types") or []),
            "forbidden_item_types": list(contract.get("forbidden_item_types") or []),
        }],
    }


class ThermalTransitionPlanValidator:
    """Validate one normalized ``thermal_transition_two_step`` plan.

    ``validate`` returns a defensive copy so callers may freeze it directly.
    The method raises on the first violation; it never repairs plan fields.
    """

    def validate(
        self, plan: Mapping[str, Any], *, thermal_transition_contract: Mapping[str, Any]
    ) -> dict[str, Any]:
        plan = _mapping(plan, "plan")
        contract = _mapping(thermal_transition_contract, "thermal_transition_contract")
        if str(contract.get("schema_version") or "") != THERMAL_TRANSITION_CONTRACT_SCHEMA:
            raise ThermalTransitionContractError(
                f"schema_version must be {THERMAL_TRANSITION_CONTRACT_SCHEMA}"
            )
        roles = contract_roles(contract)
        states = contract_states(contract)
        if str(contract.get("progression_relation") or "") != "STRICT_STACK_INCLUSION":
            raise ThermalTransitionContractError(
                "progression_relation must be STRICT_STACK_INCLUSION"
            )
        for flag in ("same_identity", "same_base_bottom_shoes", "stackability_required"):
            _flag(contract.get(flag), f"thermal_transition_contract.{flag}")
        presentation = tuple(str(value) for value in contract.get("presentation_order") or ())
        generation = tuple(str(value) for value in contract.get("generation_order") or ())
        if presentation != roles:
            raise ThermalTransitionContractError(
                f"contract.presentation_order must be {list(roles)}"
            )
        if generation != tuple(reversed(roles)):
            raise ThermalTransitionContractError(
                f"contract.generation_order must be {list(reversed(roles))}"
            )
        if str(plan.get("flow") or "") != THERMAL_TRANSITION_FLOW:
            raise ThermalTransitionContractError(f"plan.flow must be {THERMAL_TRANSITION_FLOW}")

        posts = plan.get("posts")
        if not isinstance(posts, list) or not posts:
            raise ThermalTransitionContractError("plan.posts must be a non-empty list")
        for index, post in enumerate(posts):
            self._validate_post(
                _mapping(post, f"posts[{index}]"),
                contract=contract, roles=roles, states=states,
                label=f"posts[{index}]",
            )
        return copy.deepcopy(dict(plan))

    def _validate_post(
        self, post: Mapping[str, Any], *, contract: Mapping[str, Any],
        roles: Sequence[str], states: Sequence[Mapping[str, Any]], label: str,
    ) -> None:
        binding = _mapping(post.get("profile_binding"), f"{label}.profile_binding")
        transition_key = _text(binding.get("transition_key"), f"{label}.transition_key")
        entry = transition_for(contract, transition_key)
        contexts = [str(value) for value in entry.get("thermal_contexts") or []]
        expected_contexts = [str(state["thermal_context"]) for state in states]
        if contexts != expected_contexts:
            _fail(
                CONTEXT_ORDER_MISMATCH,
                f"{label} 场景 {transition_key} 的体感顺序必须是 "
                f"{expected_contexts}，实际 {contexts}",
            )
        mode = _text(
            binding.get("temperature_label_mode"), f"{label}.temperature_label_mode"
        )
        if mode not in TEMPERATURE_LABEL_MODES:
            raise ThermalTransitionContractError(
                f"{label}.temperature_label_mode must be one of {list(TEMPERATURE_LABEL_MODES)}"
            )
        for field in ("thermal_sensitivity", "dress_code", "style_series", "family_key", "series_id"):
            _text(binding.get(field), f"{label}.{field}")

        frozen_roles = tuple(str(value) for value in post.get("required_roles") or ())
        if frozen_roles != tuple(roles):
            raise ThermalTransitionContractError(
                f"{label}.required_roles must be {list(roles)}"
            )
        if tuple(str(value) for value in post.get("presentation_order") or ()) != tuple(roles):
            raise ThermalTransitionContractError(
                f"{label}.presentation_order must be {list(roles)}"
            )
        if tuple(str(value) for value in post.get("generation_order") or ()) != tuple(reversed(roles)):
            raise ThermalTransitionContractError(
                f"{label}.generation_order must be {list(reversed(roles))}"
            )

        looks = post.get("looks")
        if not isinstance(looks, list):
            raise ThermalTransitionContractError(f"{label}.looks must be a list")
        by_role: dict[str, Mapping[str, Any]] = {}
        for look in looks:
            look = _mapping(look, f"{label}.looks[]")
            role = str(look.get("role") or "")
            if role not in roles:
                raise ThermalTransitionContractError(f"{label}.looks has unknown role {role!r}")
            if role in by_role:
                raise ThermalTransitionContractError(f"{label}.looks repeats role {role}")
            by_role[role] = look
        if tuple(by_role) != tuple(roles):
            raise ThermalTransitionContractError(
                f"{label}.looks must cover exactly {list(roles)} in order, got {list(by_role)}"
            )

        allowed = {str(value).strip().lower() for value in contract.get("allowed_item_types") or [] if str(value).strip()}
        forbidden = {str(value).strip().lower() for value in contract.get("forbidden_item_types") or [] if str(value).strip()}
        declared_contexts = {str(state["thermal_context"]) for state in states}
        previous_ids: set[str] | None = None
        previous_stack: list[str] | None = None
        frozen_outfit: dict[str, str] = {}
        for index, role in enumerate(roles):
            look = by_role[role]
            state = states[index]
            context = _text(look.get("thermal_context"), f"{label}.{role}.thermal_context")
            if context not in declared_contexts:
                _fail(
                    UNKNOWN_THERMAL_CONTEXT,
                    f"{label}.{role} 出现未声明的体感 {context!r}；"
                    f"已声明：{'、'.join(sorted(declared_contexts))}",
                )
            if context != str(state["thermal_context"]):
                _fail(
                    CONTEXT_ORDER_MISMATCH,
                    f"{label}.{role} 的体感必须是 {state['thermal_context']!r}，实际 {context!r}",
                )
            count = _integer(
                look.get("expected_visible_layer_count"),
                f"{label}.{role}.expected_visible_layer_count",
            )
            expected_count = int(state["expected_visible_layer_count"])
            if count != expected_count:
                raise ThermalTransitionContractError(
                    f"{label}.{role} 可见层数必须是 {expected_count}，实际 {count}"
                )
            stack_ids = self._stack(look, f"{label}.{role}")
            if len(stack_ids) != count:
                raise ThermalTransitionContractError(
                    f"{label}.{role} 可见层数 {count} 与层栈长度 {len(stack_ids)} 不一致"
                )
            if previous_ids is not None:
                if not previous_ids < set(stack_ids) or previous_stack != stack_ids[:len(previous_stack)]:
                    raise ThermalTransitionContractError(
                        f"{label} 层栈必须是严格包含关系：previous={sorted(previous_ids)}, "
                        f"{role}={sorted(set(stack_ids))}"
                    )
                if len(set(stack_ids) - previous_ids) != 1:
                    raise ThermalTransitionContractError(
                        f"{label}.{role} 只能比上一态多一层"
                    )
                added = _text(look.get("added_garment_id"), f"{label}.{role}.added_garment_id")
                if set(stack_ids) - previous_ids != {added}:
                    raise ThermalTransitionContractError(
                        f"{label}.{role}.added_garment_id 与层栈差集不一致"
                    )
            self._check_items(look, label=f"{label}.{role}", allowed=allowed, forbidden=forbidden)
            if contract.get("stackability_required"):
                stackability = _mapping(look.get("stackability"), f"{label}.{role}.stackability")
                for field in ("collar_profile", "sleeve_volume", "outer_sleeve_capacity", "length_zone"):
                    _text(stackability.get(field), f"{label}.{role}.stackability.{field}")
            current = self._frozen_slots(look, label=f"{label}.{role}")
            for slot, garment_id in current.items():
                if slot in frozen_outfit and frozen_outfit[slot] != garment_id:
                    _fail(
                        BASE_OUTFIT_DRIFT,
                        f"{label} 基础层/下装/鞋必须跨状态冻结：{slot} "
                        f"{frozen_outfit[slot]} → {garment_id}",
                    )
                frozen_outfit[slot] = garment_id
            previous_ids = set(stack_ids)
            previous_stack = stack_ids

        # The delta must be a wearable upper layer; swapping a bottom or shoes
        # is a different outfit, not a visible thermal transition.
        for role in roles[1:]:
            look = by_role[role]
            added = _text(look.get("added_garment_id"), f"{label}.{role}.added_garment_id")
            slot = self._slot_of(look, added)
            if slot not in UPPER_LAYER_SLOTS or slot == "base_top":
                _fail(
                    TRANSITION_NOT_VISIBLE,
                    f"{label}.{role} 的过渡必须由新增可穿上身层体现，实际 {added} 位于 {slot or '未声明槽位'}",
                )

        self._check_temperature_claim(post, binding=binding, mode=mode, label=label)

    @staticmethod
    def _stack(look: Mapping[str, Any], label: str) -> list[str]:
        raw = look.get("expected_layer_stack")
        if not isinstance(raw, list) or not raw:
            raise ThermalTransitionContractError(f"{label}.expected_layer_stack must be non-empty")
        entries: list[str] = []
        for index, item in enumerate(raw):
            if isinstance(item, Mapping):
                garment_id = str(item.get("garment_id") or "").strip()
            else:
                garment_id = str(item or "").strip()
            if not garment_id:
                raise ThermalTransitionContractError(
                    f"{label}.expected_layer_stack[{index}] 缺少 garment_id"
                )
            entries.append(garment_id)
        if len(set(entries)) != len(entries):
            raise ThermalTransitionContractError(f"{label}.expected_layer_stack 含重复单品")
        return entries

    @staticmethod
    def _check_items(
        look: Mapping[str, Any], *, label: str, allowed: set[str], forbidden: set[str],
    ) -> None:
        items = look.get("items")
        if not isinstance(items, list) or not items:
            raise ThermalTransitionContractError(f"{label}.items must be a non-empty list")
        types: list[str] = []
        slots: list[str] = []
        for index, item in enumerate(items):
            entry = _mapping(item, f"{label}.items[{index}]")
            slot = _text(entry.get("slot"), f"{label}.items[{index}].slot")
            slots.append(slot)
            _text(entry.get("garment_id"), f"{label}.items[{index}].garment_id")
            item_type = str(entry.get("item_type") or "").strip().lower()
            if not item_type:
                raise ThermalTransitionContractError(f"{label}.items[{index}] 缺少 item_type")
            types.append(item_type)
        duplicated = sorted({slot for slot in slots if slots.count(slot) > 1})
        if duplicated:
            # A second garment in the same slot would make the frozen
            # base/bottom/shoes ambiguous and silently smuggle an extra piece
            # into the outfit, so it must never validate.
            _fail(
                ITEM_SLOT_DUPLICATED,
                f"{label}.items 同一槽位出现多个单品 {duplicated}",
            )
        if forbidden & set(types):
            raise ThermalTransitionContractError(
                f"{label}.items 含禁用单品 {sorted(forbidden & set(types))}"
            )
        if allowed and set(types) - allowed:
            raise ThermalTransitionContractError(
                f"{label}.items 含允许清单外单品 {sorted(set(types) - allowed)}"
            )

    @staticmethod
    def _frozen_slots(look: Mapping[str, Any], *, label: str) -> dict[str, str]:
        frozen: dict[str, str] = {}
        for slot in ("base_top", "bottom", "shoes"):
            frozen[slot] = ThermalTransitionPlanValidator._item_in_slot(look, slot, label=label)
        return frozen

    @staticmethod
    def _item_in_slot(look: Mapping[str, Any], slot: str, *, label: str) -> str:
        for item in look.get("items") or []:
            if isinstance(item, Mapping) and str(item.get("slot") or "") == slot:
                garment_id = str(item.get("garment_id") or "").strip()
                if garment_id:
                    return garment_id
        raise ThermalTransitionContractError(f"{label}.items 缺少 {slot} 单品")

    @staticmethod
    def _slot_of(look: Mapping[str, Any], garment_id: str) -> str:
        for item in look.get("items") or []:
            if isinstance(item, Mapping) and str(item.get("garment_id") or "") == garment_id:
                return str(item.get("slot") or "")
        return ""

    @staticmethod
    def _check_temperature_claim(
        post: Mapping[str, Any], *, binding: Mapping[str, Any], mode: str, label: str,
    ) -> None:
        claims = unsourced_temperature_values(post.get("copy") or {}, temperature_label_mode=mode)
        if not claims:
            return
        normalize = lambda value: str(value).replace(" ", "").replace("摄氏", "")
        declared = {
            normalize(value)
            for value in (binding.get("sourced_temperature_values") or [])
            if str(value).strip()
        }
        if mode == "QUALITATIVE":
            _fail(
                TEMPERATURE_VALUE_UNSOURCED,
                f"{label} 温度为 QUALITATIVE 时文案不得出现具体温度数字：{'、'.join(claims)}",
            )
        unsourced = [value for value in claims if normalize(value) not in declared]
        if unsourced:
            _fail(
                TEMPERATURE_VALUE_UNSOURCED,
                f"{label} 文案出现无来源温度数字：{'、'.join(unsourced)}",
            )


def validate_thermal_transition_plan(
    plan: Mapping[str, Any], *, thermal_transition_contract: Mapping[str, Any]
) -> dict[str, Any]:
    """Functional entry point used by the thermal-transition flow handler."""
    return ThermalTransitionPlanValidator().validate(
        plan, thermal_transition_contract=thermal_transition_contract
    )
