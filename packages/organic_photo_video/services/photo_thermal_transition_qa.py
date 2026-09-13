"""Deterministic QA for the TH daily thermal-transition flow.

Layer-stack structure (counts, strict inclusion, frozen base, stackability,
identity and camera) is judged by the shared structural QA in
:mod:`services.photo_layering_qa`.  This module adds only what is specific to
the daily hot→cold line:

* every page must observe the thermal context frozen for its role;
* the three contexts must follow the frozen order;
* base/bottom/shoes must stay identical across the three pages;
* each transition must actually be visible as one added upper layer;
* concrete temperature numbers are rejected unless the plan declared a source.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from services.photo_layering_qa import (
    LAYERING_QA_SCHEMA, QA_SCHEMA_INCOMPLETE, LayeringSemanticQAError,
    normalize_layering_qa,
)
from services.photo_thermal_transition_contract import (
    BASE_OUTFIT_DRIFT, CONTEXT_ORDER_MISMATCH, TEMPERATURE_VALUE_UNSOURCED,
    THERMAL_TRANSITION_SOURCE_ROLES, TRANSITION_NOT_VISIBLE,
    UNKNOWN_THERMAL_CONTEXT, as_layering_contract, contract_states,
    transition_for, unsourced_temperature_values,
)


THERMAL_TRANSITION_QA_SCHEMA = "opv-photo-thermal-transition-qa-v1"

# Thermal-only observation fields are absent from the shared structural QA, so
# this module owns their schema errors and keeps the same routable code prefix.
THERMAL_OBSERVATION_FIELDS = (
    "observed_thermal_context", "added_garment_visible",
    "observed_base_signature", "observed_bottom_signature",
    "observed_shoes_signature",
)


class ThermalTransitionQAError(ValueError):
    """The model observation cannot be judged for the transition line."""


def _schema_error(message: str) -> ThermalTransitionQAError:
    return ThermalTransitionQAError(f"{QA_SCHEMA_INCOMPLETE}：{message}")


def _add_failure(result: dict[str, Any], code: str) -> None:
    if code not in result["failure_codes"]:
        result["failure_codes"].append(code)
    result["failure_code"] = result["failure_codes"][0]
    result["passed"] = False


def _add_group_failure(results: Sequence[dict[str, Any]], code: str) -> None:
    for result in results:
        _add_failure(result, code)


def _require_string(page: Mapping[str, Any], field: str, role: str) -> str:
    value = page.get(field)
    if not isinstance(value, str) or not value.strip():
        raise _schema_error(f"{role} 页 {field} 必须是非空字符串")
    return value.strip()


def _require_bool(page: Mapping[str, Any], field: str, role: str) -> bool:
    value = page.get(field)
    if not isinstance(value, bool):
        raise _schema_error(f"{role} 页 {field} 必须是布尔值")
    return value


def _to_structural_raw(
    raw: Mapping[str, Any], *, band_key: str,
) -> dict[str, Any]:
    """Project a thermal observation onto the shared structural QA schema."""
    pages = []
    for page in raw.get("pages") or []:
        if not isinstance(page, Mapping):
            raise ThermalTransitionQAError("pages 中每项都必须是对象")
        pages.append({**dict(page), "observed_band": band_key})
    return {
        "thermal_index": int(raw.get("thermal_index") or 0),
        "notes": str(raw.get("notes") or ""),
        "pages": pages,
    }


def evaluate_thermal_transition_qa(
    raw: Mapping[str, Any], *,
    look_plans: Sequence[Mapping[str, Any]],
    profile_binding: Mapping[str, Any],
    thermal_transition_contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Judge one transition source group; structural QA plus transition rules."""
    if not isinstance(raw, Mapping) or not isinstance(raw.get("pages"), list):
        raise ThermalTransitionQAError("冷热切换质检没有返回 pages 数组")
    transition_key = _require_string(
        profile_binding, "transition_key", "profile_binding"
    )
    entry = transition_for(thermal_transition_contract, transition_key)
    states = contract_states(thermal_transition_contract)
    expected_contexts = [str(state.get("thermal_context") or "") for state in states]
    declared_contexts = [str(value) for value in entry.get("thermal_contexts") or []]
    if declared_contexts != expected_contexts:
        raise ThermalTransitionQAError(
            f"{CONTEXT_ORDER_MISMATCH}：场景 {transition_key} 的体感顺序与 states 不一致"
        )
    roles = tuple(str(value) for value in thermal_transition_contract.get("source_roles") or ())
    if roles != THERMAL_TRANSITION_SOURCE_ROLES:
        raise _schema_error(
            f"source_roles 必须是 {list(THERMAL_TRANSITION_SOURCE_ROLES)}"
        )

    # Coverage and duplication are checked here first so every thermal QA
    # schema problem surfaces as ThermalTransitionQAError, not as a mixed bag
    # of error types from the shared structural QA.
    page_by_role: dict[str, Mapping[str, Any]] = {}
    for page in raw["pages"]:
        if not isinstance(page, Mapping):
            raise _schema_error("pages 中每项都必须是对象")
        role = str(page.get("role") or "")
        if role not in roles:
            raise _schema_error(f"pages 包含未知角色 {role!r}")
        if role in page_by_role:
            raise _schema_error(f"pages 重复角色 {role}")
        page_by_role[role] = page
    if tuple(page_by_role) != roles:
        raise _schema_error(
            f"pages 必须按 {list(roles)} 完整覆盖，实际 {list(page_by_role)}"
        )

    bound = {
        "band_key": transition_key,
        "thermal_sensitivity": _require_string(
            profile_binding, "thermal_sensitivity", "profile_binding"
        ),
        "scene": _require_string(profile_binding, "dress_code", "profile_binding"),
        "series_id": _require_string(profile_binding, "style_series", "profile_binding"),
        "family_key": str(profile_binding.get("family_key") or ""),
        "thermal_index": 0,
    }
    structural = normalize_layering_qa(
        _to_structural_raw(raw, band_key=transition_key),
        look_plans=look_plans, profile_binding=bound,
        layering_contract=as_layering_contract(
            thermal_transition_contract, transition_key=transition_key,
        ),
    )

    results = list(structural["roles"])
    by_role = {str(item["role"]): item for item in results}
    observed_contexts: dict[str, str] = {}
    signatures: dict[str, dict[str, str]] = {}
    for index, role in enumerate(roles):
        page = page_by_role[role]
        context = _require_string(page, "observed_thermal_context", role)
        observed_contexts[role] = context
        if context not in expected_contexts:
            _add_failure(by_role[role], UNKNOWN_THERMAL_CONTEXT)
        elif context != expected_contexts[index]:
            _add_failure(by_role[role], CONTEXT_ORDER_MISMATCH)
        signatures[role] = {
            slot: _require_string(page, f"observed_{slot}_signature", role)
            for slot in ("base", "bottom", "shoes")
        }
        if role != roles[0]:
            if _require_bool(page, "added_garment_visible", role) is not True:
                _add_failure(by_role[role], TRANSITION_NOT_VISIBLE)

    base_signature = signatures[roles[0]]
    for role in roles[1:]:
        if signatures[role] != base_signature:
            _add_failure(by_role[role], BASE_OUTFIT_DRIFT)
            _add_failure(by_role[roles[0]], BASE_OUTFIT_DRIFT)

    mode = _require_string(
        profile_binding, "temperature_label_mode", "profile_binding"
    )
    declared = {
        str(value).replace(" ", "")
        for value in (profile_binding.get("sourced_temperature_values") or [])
        if str(value).strip()
    }
    for role in roles:
        page = page_by_role[role]
        claim = page.get("temperature_claim_text")
        if claim is None:
            continue
        found = unsourced_temperature_values(claim, temperature_label_mode=mode)
        if not found:
            continue
        if mode == "QUALITATIVE" or any(
            str(value).replace(" ", "") not in declared for value in found
        ):
            _add_failure(by_role[role], TEMPERATURE_VALUE_UNSOURCED)

    passed = all(item["passed"] for item in results)
    return {
        "schema_version": THERMAL_TRANSITION_QA_SCHEMA,
        "structural_schema_version": LAYERING_QA_SCHEMA,
        "passed": passed,
        "profile_binding": {
            "transition_key": transition_key,
            "thermal_sensitivity": bound["thermal_sensitivity"],
            "dress_code": bound["scene"],
            "style_series": bound["series_id"],
            "family_key": bound["family_key"],
            "temperature_label_mode": mode,
        },
        "thermal_contexts": {
            "expected": expected_contexts,
            "observed": observed_contexts,
        },
        "roles": results,
        "summary": dict(structural.get("summary") or {}),
        "notes": str(raw.get("notes") or ""),
    }


def failed_roles_from_thermal_qa(
    qa: Mapping[str, Any], role_order: Sequence[str] = THERMAL_TRANSITION_SOURCE_ROLES,
) -> list[str]:
    failed = {
        str(item.get("role") or "")
        for item in qa.get("roles") or [] if item.get("passed") is False
    }
    return [role for role in role_order if role in failed]


def thermal_qa_as_alignment(qa: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "opv-photo-reference-alignment-v1",
        "scope": "THERMAL_TRANSITION_QA",
        "passed": bool(qa.get("passed")),
        "scores": {},
        "reason_codes": sorted({
            str(code)
            for item in qa.get("roles") or []
            for code in item.get("failure_codes") or [] if code
        }),
        "notes": str(qa.get("notes") or ""),
        "role_findings": [
            {
                "role": item.get("role"),
                "passed": item.get("passed"),
                "issues": list(item.get("failure_codes") or []),
            }
            for item in qa.get("roles") or []
        ],
        "thermal_transition_qa": qa,
    }


def thermal_transition_qa_markdown(qa: Mapping[str, Any]) -> str:
    binding = dict(qa.get("profile_binding") or {})
    contexts = dict(qa.get("thermal_contexts") or {})
    lines = [
        "# 冷热切换 Canary QA",
        "",
        f"- 场景：{binding.get('transition_key') or '未填写'}",
        f"- 体感：{binding.get('thermal_sensitivity') or '未填写'}",
        f"- 着装要求：{binding.get('dress_code') or '未填写'}",
        f"- 温度口径：{binding.get('temperature_label_mode') or '未填写'}",
        f"- 体感顺序：{' → '.join(str(value) for value in contexts.get('expected') or [])}",
        f"- 总体结果：{'通过' if qa.get('passed') else '未通过'}",
        "",
        "| 角色 | 体感 | 可见层数 | 层栈 | 结果 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in qa.get("roles") or []:
        stack = " → ".join(str(value) for value in item.get("visible_layer_stack") or [])
        lines.append(
            f"| {item.get('role')} | {item.get('observed_thermal_context') or ''} | "
            f"{item.get('visible_layer_count')} | {stack} | "
            f"{'通过' if item.get('passed') else '未通过'} |"
        )
    codes = sorted({
        str(code)
        for item in qa.get("roles") or []
        for code in item.get("failure_codes") or [] if code
    })
    if codes:
        lines += ["", f"- 失败码：{'、'.join(codes)}"]
    return "\n".join(lines)


def thermal_transition_qa_note_zh(qa: Mapping[str, Any]) -> str:
    if qa.get("passed"):
        return "冷热切换 QA 通过"
    codes = sorted({
        str(code)
        for item in qa.get("roles") or []
        for code in item.get("failure_codes") or [] if code
    })
    return "冷热切换 QA 未通过：" + "、".join(codes)


__all__ = [
    "THERMAL_TRANSITION_QA_SCHEMA", "ThermalTransitionQAError",
    "evaluate_thermal_transition_qa", "failed_roles_from_thermal_qa",
    "thermal_qa_as_alignment", "thermal_transition_qa_markdown",
    "thermal_transition_qa_note_zh",
]
