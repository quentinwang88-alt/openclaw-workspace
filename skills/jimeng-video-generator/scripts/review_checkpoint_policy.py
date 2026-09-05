"""Pure test-review policy: explicitly replace known points, never mutate a mother."""
from __future__ import annotations

from copy import deepcopy
from typing import Any


CHECKPOINT_POLICY_VERSION = "wsr-effective-checkpoints-v1"


def _points(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{label}_LIST_REQUIRED")
    seen = set()
    for point in value:
        if not isinstance(point, dict):
            raise ValueError(f"{label}_POINT_INVALID")
        point_id = point.get("point_id")
        if not isinstance(point_id, str) or not point_id.strip() or point_id != point_id.strip():
            raise ValueError(f"{label}_POINT_ID_INVALID")
        if point_id in seen:
            raise ValueError(f"{label}_DUPLICATE_POINT_ID")
        seen.add(point_id)
        if not isinstance(point.get("requirement"), str) or not point["requirement"].strip():
            raise ValueError(f"{label}_REQUIREMENT_INVALID")
    return deepcopy(value)


def freeze_checkpoints(mother_core_points: list[dict[str, Any]], allowed_changes=None,
                       effective_checkpoints=None) -> dict[str, Any]:
    frozen = _points(mother_core_points, "MOTHER")
    effective = deepcopy(frozen)
    index = {point["point_id"]: position for position, point in enumerate(frozen)}
    changes = [] if allowed_changes is None else allowed_changes
    if not isinstance(changes, list):
        raise ValueError("ALLOWED_CHANGES_LIST_REQUIRED")
    seen = set()
    for change in changes:
        if not isinstance(change, dict) or set(change) != {
                "point_id", "operation", "replacement_requirement", "reason"}:
            raise ValueError("ALLOWED_CHANGE_FIELDS_INVALID")
        point_id = change["point_id"]
        if not isinstance(point_id, str) or point_id not in index:
            raise ValueError("ALLOWED_CHANGE_UNKNOWN_POINT_ID")
        if point_id in seen:
            raise ValueError("ALLOWED_CHANGE_DUPLICATE_POINT_ID")
        seen.add(point_id)
        if change["operation"] != "replace":
            raise ValueError("ALLOWED_CHANGE_OPERATION_INVALID")
        if not isinstance(change["replacement_requirement"], str) or not change["replacement_requirement"].strip():
            raise ValueError("ALLOWED_CHANGE_REQUIREMENT_INVALID")
        if not isinstance(change["reason"], str) or not change["reason"].strip():
            raise ValueError("ALLOWED_CHANGE_REASON_REQUIRED")
        effective[index[point_id]]["requirement"] = change["replacement_requirement"]
    if effective_checkpoints is not None and _points(effective_checkpoints, "EFFECTIVE") != effective:
        raise ValueError("EFFECTIVE_CHECKPOINTS_NOT_EXPLICITLY_AUTHORIZED")
    return {"checkpoint_policy_version": CHECKPOINT_POLICY_VERSION,
            "frozen_mother_core_points": frozen, "effective_checkpoints": effective,
            "allowed_changes": deepcopy(changes)}
