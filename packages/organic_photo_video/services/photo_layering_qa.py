"""Structured visual observations and deterministic layering verdicts."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping, Sequence

from services.photo_layering_contract import LAYERING_SOURCE_ROLES


LAYERING_QA_SCHEMA = "opv-photo-layering-semantic-qa-v1"
LAYERING_CROSS_BAND_QA_SCHEMA = "opv-photo-layering-cross-band-qa-v1"

UNKNOWN_BAND = "UNKNOWN_BAND"
LAYER_COUNT_MISMATCH = "LAYER_COUNT_MISMATCH"
LAYER_ORDER_MISMATCH = "LAYER_ORDER_MISMATCH"
NOT_STACKABLE = "NOT_STACKABLE"
BASE_INCONSISTENT = "BASE_INCONSISTENT"
FORBIDDEN_ITEM_PRESENT = "FORBIDDEN_ITEM_PRESENT"
ITEM_OUTSIDE_ALLOWLIST = "ITEM_OUTSIDE_ALLOWLIST"
CLAIM_MISMATCH = "CLAIM_MISMATCH"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
PERSON_DISASTER = "PERSON_DISASTER"
LAYER_SOURCE_IDENTITY_MISMATCH = "LAYER_SOURCE_IDENTITY_MISMATCH"
LAYER_SOURCE_CAMERA_MISMATCH = "LAYER_SOURCE_CAMERA_MISMATCH"
CROSS_BAND_LAYER_COUNT_REGRESSION = "CROSS_BAND_LAYER_COUNT_REGRESSION"
CROSS_BAND_THERMAL_INDEX_REGRESSION = "CROSS_BAND_THERMAL_INDEX_REGRESSION"
QA_SCHEMA_INCOMPLETE = "QA_SCHEMA_INCOMPLETE"


class LayeringSemanticQAError(ValueError):
    """The model observation is structurally incomplete and cannot be judged."""


def _schema_error(message: str) -> LayeringSemanticQAError:
    return LayeringSemanticQAError(f"{QA_SCHEMA_INCOMPLETE}：{message}")


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


def _require_integer(page: Mapping[str, Any], field: str, role: str) -> int:
    value = page.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise _schema_error(f"{role} 页 {field} 必须是正整数")
    return value


def _require_string_list(page: Mapping[str, Any], field: str, role: str) -> list[str]:
    value = page.get(field)
    if (not isinstance(value, list) or not value
            or any(not isinstance(item, str) or not item.strip() for item in value)):
        raise _schema_error(f"{role} 页 {field} 必须是至少一项非空字符串的数组")
    return [item.strip() for item in value]


def _stack_signatures(page: Mapping[str, Any], role: str) -> list[str]:
    raw = page.get("visible_layer_stack")
    if not isinstance(raw, list) or not raw:
        raise _schema_error(f"{role} 页 visible_layer_stack 必须是非空数组")
    signatures: list[str] = []
    for index, item in enumerate(raw):
        if isinstance(item, str):
            signature = item.strip()
        elif isinstance(item, Mapping):
            signature = str(item.get("garment_signature") or item.get("garment_id") or "").strip()
        else:
            signature = ""
        if not signature:
            raise _schema_error(
                f"{role} 页 visible_layer_stack[{index}] 缺少 garment_signature"
            )
        signatures.append(signature)
    if len(set(signatures)) != len(signatures):
        raise _schema_error(f"{role} 页 visible_layer_stack 含重复服装签名")
    return signatures


def _band_rules(
    layering_contract: Mapping[str, Any], band_key: str
) -> tuple[Mapping[str, Any], set[str], set[str]]:
    bands = {
        str(item.get("key") or ""): item
        for item in layering_contract.get("bands") or [] if isinstance(item, Mapping)
    }
    band = bands.get(band_key)
    if not band:
        raise LayeringSemanticQAError(f"UNKNOWN_BAND：合同中没有 {band_key}")
    allowed = band.get("allowed_item_types", layering_contract.get("allowed_item_types", []))
    forbidden = band.get("forbidden_item_types", layering_contract.get("forbidden_item_types", []))
    return (
        band,
        {str(value).strip().lower() for value in allowed or [] if str(value).strip()},
        {str(value).strip().lower() for value in forbidden or [] if str(value).strip()},
    )


def normalize_layering_qa(
    raw: Mapping[str, Any],
    *,
    look_plans: Sequence[Mapping[str, Any]],
    profile_binding: Mapping[str, Any],
    layering_contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize observations, then derive every pass/fail verdict in code."""
    if not isinstance(raw, Mapping) or not isinstance(raw.get("pages"), list):
        raise _schema_error("分层语义质检没有返回 pages 数组")
    expected_roles = tuple(str(value) for value in layering_contract.get("source_roles") or ())
    if expected_roles != LAYERING_SOURCE_ROLES:
        raise _schema_error(f"source_roles 必须是 {list(LAYERING_SOURCE_ROLES)}")
    plan_by_role = {str(look.get("role") or ""): look for look in look_plans}
    if set(plan_by_role) != set(expected_roles) or len(plan_by_role) != len(look_plans):
        raise _schema_error(f"look_plans 必须按 {list(expected_roles)} 完整覆盖")
    page_by_role: dict[str, Mapping[str, Any]] = {}
    for page in raw["pages"]:
        if not isinstance(page, Mapping):
            raise _schema_error("pages 中每项都必须是对象")
        role = str(page.get("role") or "")
        if role not in expected_roles:
            raise _schema_error(f"pages 包含未知角色 {role!r}")
        if role in page_by_role:
            raise _schema_error(f"pages 重复角色 {role}")
        page_by_role[role] = page
    if set(page_by_role) != set(expected_roles):
        raise _schema_error(
            f"pages 必须完整覆盖 {list(expected_roles)}，实际 {list(page_by_role)}"
        )

    band_key = str(profile_binding.get("band_key") or "").strip()
    sensitivity = profile_binding.get("thermal_sensitivity")
    if isinstance(sensitivity, (list, tuple, set)) or not isinstance(sensitivity, str) or not sensitivity:
        raise _schema_error("profile_binding.thermal_sensitivity 必须冻结单一值")
    scene = str(profile_binding.get("scene") or "").strip()
    series_id = str(profile_binding.get("series_id") or "").strip()
    if not band_key or not scene or not series_id:
        raise _schema_error("profile_binding 缺少 band_key/scene/series_id")
    band, allowed, forbidden = _band_rules(layering_contract, band_key)

    results: list[dict[str, Any]] = []
    observed_stacks: dict[str, list[str]] = {}
    identities: set[str] = set()
    cameras: set[str] = set()
    for role in expected_roles:
        page = page_by_role[role]
        observed_band = _require_string(page, "observed_band", role)
        visible_count = _require_integer(page, "visible_layer_count", role)
        stack = _stack_signatures(page, role)
        items = [value.lower() for value in _require_string_list(page, "observed_item_types", role)]
        evidence = _require_string_list(page, "layer_evidence", role)
        identity_id = _require_string(page, "identity_id", role)
        camera_signature = _require_string(page, "camera_signature", role)
        full_body = _require_bool(page, "full_body", role)
        collar_compatible = _require_bool(page, "collar_compatible", role)
        sleeve_conflict = _require_bool(page, "sleeve_conflict", role)
        hem_conflict = _require_bool(page, "hem_conflict", role)
        claim_matches = _require_bool(page, "temperature_claim_matches", role)
        flags = page.get("person_flags")
        if not isinstance(flags, Mapping):
            raise _schema_error(f"{role} 页 person_flags 必须是对象")
        deformity = flags.get("face_or_limb_deformity")
        unnatural_tilt = flags.get("obvious_unnatural_tilt")
        if not isinstance(deformity, bool) or not isinstance(unnatural_tilt, bool):
            raise _schema_error(f"{role} 页 person_flags 的灾难级标记必须是布尔值")

        expected_count = plan_by_role[role].get("expected_visible_layer_count")
        if isinstance(expected_count, bool) or not isinstance(expected_count, int):
            raise _schema_error(f"look_plans.{role} 缺少 expected_visible_layer_count")
        failures: list[str] = []
        if observed_band != band_key:
            failures.append(UNKNOWN_BAND)
        if visible_count != expected_count or visible_count != len(stack):
            failures.append(LAYER_COUNT_MISMATCH)
        forbidden_found = sorted(set(items) & forbidden)
        outside_allowlist = sorted(set(items) - allowed) if allowed else []
        if forbidden_found:
            failures.append(FORBIDDEN_ITEM_PRESENT)
        if outside_allowlist:
            failures.append(ITEM_OUTSIDE_ALLOWLIST)
        if not collar_compatible or sleeve_conflict or hem_conflict:
            failures.append(NOT_STACKABLE)
        if not claim_matches:
            failures.append(CLAIM_MISMATCH)
        if not full_body or not evidence:
            failures.append(INSUFFICIENT_EVIDENCE)
        if deformity or unnatural_tilt:
            failures.append(PERSON_DISASTER)

        identities.add(identity_id)
        cameras.add(camera_signature)
        observed_stacks[role] = stack
        results.append({
            "role": role,
            "passed": not failures,
            "failure_code": failures[0] if failures else "",
            "failure_codes": failures,
            "observed_band": observed_band,
            "visible_layer_count": visible_count,
            "visible_layer_stack": stack,
            "observed_item_types": items,
            "layer_evidence": evidence,
            "identity_id": identity_id,
            "camera_signature": camera_signature,
            "full_body": full_body,
            "stackability": {
                "collar_compatible": collar_compatible,
                "sleeve_conflict": sleeve_conflict,
                "hem_conflict": hem_conflict,
            },
            "temperature_claim_matches": claim_matches,
            "person_flags": {
                "face_or_limb_deformity": deformity,
                "obvious_unnatural_tilt": unnatural_tilt,
            },
            "forbidden_item_types": forbidden_found,
            "outside_allowed_item_types": outside_allowlist,
            "repair_instruction": str(page.get("repair_instruction") or "").strip(),
        })

    # Group evidence is relational.  When the relation breaks, all three
    # sources are unsafe as one progression group, even if a single image is
    # individually plausible.
    for previous_role, role in zip(expected_roles, expected_roles[1:]):
        previous_stack = observed_stacks[previous_role]
        current_stack = observed_stacks[role]
        if (not set(previous_stack) < set(current_stack)
                or previous_stack != current_stack[:len(previous_stack)]):
            _add_group_failure(results, LAYER_ORDER_MISMATCH)
            break
    core_base = observed_stacks["base"][0]
    if any(core_base not in observed_stacks[role] for role in expected_roles[1:]):
        _add_group_failure(results, BASE_INCONSISTENT)
    if len(identities) != 1:
        _add_group_failure(results, LAYER_SOURCE_IDENTITY_MISMATCH)
    if len(cameras) != 1:
        _add_group_failure(results, LAYER_SOURCE_CAMERA_MISMATCH)

    outer = next(item for item in results if item["role"] == "outer")
    bounds = band.get("final_visible_layer_bounds") or {}
    minimum, maximum = bounds.get("min"), bounds.get("max")
    if (isinstance(minimum, bool) or not isinstance(minimum, int)
            or isinstance(maximum, bool) or not isinstance(maximum, int)):
        raise _schema_error(f"band {band_key} 缺少合法 final_visible_layer_bounds")
    if not minimum <= outer["visible_layer_count"] <= maximum:
        _add_failure(outer, LAYER_COUNT_MISMATCH)

    passed = all(item["passed"] for item in results)
    return {
        "schema_version": LAYERING_QA_SCHEMA,
        "passed": passed,
        "profile_binding": {
            "band_key": band_key,
            "thermal_sensitivity": sensitivity,
            "scene": scene,
            "series_id": series_id,
            "family_key": str(profile_binding.get("family_key") or ""),
        },
        "roles": results,
        "summary": {
            "final_visible_layer_count": outer["visible_layer_count"],
            "thermal_index": _thermal_index(raw, outer, profile_binding),
            "core_base_garment_id": core_base,
        },
        "notes": str(raw.get("notes") or ""),
    }


def _thermal_index(
    raw: Mapping[str, Any], outer: Mapping[str, Any],
    profile_binding: Mapping[str, Any],
) -> int:
    # Temperature order is a planning fact, not something a vision model
    # should guess from clothing thickness.  Old fixtures may still provide
    # the observation-level value, hence the explicit fallback.
    value = profile_binding.get(
        "thermal_index", raw.get("thermal_index", outer.get("thermal_index"))
    )
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _schema_error("thermal_index 必须是非负整数")
    return value


def _add_failure(result: dict[str, Any], code: str) -> None:
    if code not in result["failure_codes"]:
        result["failure_codes"].append(code)
    result["failure_code"] = result["failure_codes"][0]
    result["passed"] = False


def _add_group_failure(results: Sequence[dict[str, Any]], code: str) -> None:
    for result in results:
        _add_failure(result, code)


def evaluate_cross_band_layering(
    reports: Sequence[Mapping[str, Any]], *, layering_contract: Mapping[str, Any]
) -> dict[str, Any]:
    """Compare only reports sharing series, sensitivity, and scene."""
    rules = layering_contract.get("cross_band_rules") or {}
    if not isinstance(rules, Mapping):
        raise LayeringSemanticQAError("cross_band_rules 必须是对象")
    order = [str(value) for value in rules.get("temperature_order") or []]
    rank = {band: index for index, band in enumerate(order)}
    grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for report in reports:
        if not isinstance(report, Mapping):
            raise LayeringSemanticQAError("跨档 QA 输入必须是对象数组")
        binding = report.get("profile_binding")
        summary = report.get("summary")
        if not isinstance(binding, Mapping) or not isinstance(summary, Mapping):
            raise LayeringSemanticQAError("跨档 QA 报告缺少 profile_binding/summary")
        key = (
            str(binding.get("series_id") or ""),
            str(binding.get("thermal_sensitivity") or ""),
            str(binding.get("scene") or ""),
        )
        if not all(key):
            raise LayeringSemanticQAError("跨档 QA 分组键不能为空")
        grouped[key].append(report)

    failures: list[dict[str, Any]] = []
    groups: list[dict[str, Any]] = []
    for key, group in grouped.items():
        ordered = sorted(
            group,
            key=lambda value: rank.get(
                str(value["profile_binding"].get("band_key") or ""), 10**6
            ),
        )
        comparisons: list[dict[str, Any]] = []
        for previous, current in zip(ordered, ordered[1:]):
            previous_band = str(previous["profile_binding"].get("band_key") or "")
            current_band = str(current["profile_binding"].get("band_key") or "")
            if previous_band not in rank or current_band not in rank:
                continue
            before, after = previous["summary"], current["summary"]
            codes: list[str] = []
            if (rules.get("final_layer_count_non_decreasing")
                    and int(after["final_visible_layer_count"]) < int(before["final_visible_layer_count"])):
                codes.append(CROSS_BAND_LAYER_COUNT_REGRESSION)
            if (rules.get("thermal_index_non_decreasing")
                    and int(after["thermal_index"]) < int(before["thermal_index"])):
                codes.append(CROSS_BAND_THERMAL_INDEX_REGRESSION)
            if (rules.get("core_base_consistency") == "WITHIN_SERIES"
                    and str(after["core_base_garment_id"]) != str(before["core_base_garment_id"])):
                codes.append(BASE_INCONSISTENT)
            comparison = {
                "from_band": previous_band,
                "to_band": current_band,
                "passed": not codes,
                "failure_codes": codes,
            }
            comparisons.append(comparison)
            for code in codes:
                failures.append({
                    "group": {
                        "series_id": key[0],
                        "thermal_sensitivity": key[1],
                        "scene": key[2],
                    },
                    "from_band": previous_band,
                    "to_band": current_band,
                    "failure_code": code,
                })
        groups.append({
            "series_id": key[0],
            "thermal_sensitivity": key[1],
            "scene": key[2],
            "bands": [str(item["profile_binding"].get("band_key") or "") for item in ordered],
            "comparisons": comparisons,
            "passed": all(item["passed"] for item in comparisons),
        })
    return {
        "schema_version": LAYERING_CROSS_BAND_QA_SCHEMA,
        "passed": not failures,
        "groups": groups,
        "failures": failures,
    }


def failed_roles_from_layering_qa(
    qa: Mapping[str, Any], role_order: Sequence[str] = LAYERING_SOURCE_ROLES
) -> list[str]:
    failed = {
        str(item.get("role") or "")
        for item in qa.get("roles") or [] if item.get("passed") is False
    }
    return [role for role in role_order if role in failed]


def layering_qa_as_alignment(qa: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "opv-photo-reference-alignment-v1",
        "scope": "LAYERING_SEMANTIC_QA",
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
        "layering_qa": qa,
    }
