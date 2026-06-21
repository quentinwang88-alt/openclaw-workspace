from __future__ import annotations

import os
from typing import Any

from .context import SkillContext


READY_TO_SUBMIT_PACKAGE_STATUSES = {"created", "待提单", "已创建"}
RECOVERABLE_FAILED_PACKAGE_STATUSES = {"failed", "失败"}
IN_FLIGHT_PACKAGE_STATUSES = {
    "submitted",
    "generating",
    "returned",
    "imported",
    "已提单",
    "生成中",
    "已生成",
    "已回流",
    "质检中",
    "质检通过",
    "uploaded",
    "downloaded",
    "rendering",
    "observing",
}
IMPORTED_PACKAGE_STATUSES = {"imported", "已回流", "质检通过", "downloaded", "uploaded"}
CONSUMED_PACKAGE_STATUSES = {"consumed", "已消费"}


class AISupplementGatewaySkill:
    """Single source of truth for AI supplement package state and submit budget."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def package_state(self, product_id: str) -> dict[str, int]:
        rows = _list_packages(self.ctx, product_id)
        state = {
            "package_count": len(rows),
            "ready_to_submit_count": 0,
            "recoverable_failed_count": 0,
            "inflight_count": 0,
            "imported_package_count": 0,
            "consumed_package_count": 0,
            "failed_package_count": 0,
        }
        for row in rows:
            normalized = normalize_package(row)
            if normalized == "ready_to_submit":
                state["ready_to_submit_count"] += 1
            elif normalized == "recoverable_failed":
                state["recoverable_failed_count"] += 1
            elif normalized == "inflight":
                state["inflight_count"] += 1
            elif normalized == "imported":
                state["imported_package_count"] += 1
            elif normalized == "consumed":
                state["consumed_package_count"] += 1
            elif normalized == "failed":
                state["failed_package_count"] += 1
        state["active_package_count"] = (
            state["ready_to_submit_count"]
            + state["recoverable_failed_count"]
            + state["inflight_count"]
            + state["imported_package_count"]
        )
        return state

    def submit_budget(self, product_id: str, remaining_count: int | None = None, configured_limit: int | None = None) -> dict[str, int]:
        remaining = int(remaining_count if remaining_count is not None else self._remaining_count(product_id))
        remaining = max(0, remaining)
        limit = int(configured_limit if configured_limit is not None else _configured_submit_limit())
        state = self.package_state(product_id)
        return submit_budget_from_state(remaining, state, configured_limit=limit)

    def _remaining_count(self, product_id: str) -> int:
        rows = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
        task = rows[0] if rows else {}
        target = int(task.get("requested_variant_count") or task.get("allowed_variant_count") or 0)
        actual = int(task.get("actual_variant_count") or 0)
        return int(task.get("target_remaining_variant_count") or max(0, target - actual))


def normalize_package(row: dict[str, Any]) -> str:
    status = _status(row)
    result_sync = str(row.get("result_sync_status") or "").strip()
    failure = str(row.get("failure_reason") or "").strip()
    if status in CONSUMED_PACKAGE_STATUSES:
        return "consumed"
    if row.get("generated_asset_id") or row.get("generated_segment_id"):
        return "imported"
    if status in IMPORTED_PACKAGE_STATUSES or result_sync in IMPORTED_PACKAGE_STATUSES:
        return "imported"
    if status in IN_FLIGHT_PACKAGE_STATUSES or result_sync in IN_FLIGHT_PACKAGE_STATUSES:
        return "inflight"
    if status in READY_TO_SUBMIT_PACKAGE_STATUSES or result_sync in READY_TO_SUBMIT_PACKAGE_STATUSES:
        return "ready_to_submit"
    if status in RECOVERABLE_FAILED_PACKAGE_STATUSES:
        return "recoverable_failed" if is_recoverable_submit_failure(failure) else "failed"
    return "unknown"


def submit_budget_from_state(remaining_count: int, state: dict[str, Any], configured_limit: int | None = None) -> dict[str, int]:
    remaining = max(0, int(remaining_count or 0))
    limit = int(configured_limit if configured_limit is not None else _configured_submit_limit())
    active = int(state.get("active_package_count") or 0)
    ready = int(state.get("ready_to_submit_count") or 0)
    retry = int(state.get("recoverable_failed_count") or 0)
    inflight = int(state.get("inflight_count") or 0)
    imported = int(state.get("imported_package_count") or 0)
    consumed = int(state.get("consumed_package_count") or 0)
    ready_or_retry = ready + retry
    needed_after_active = max(0, remaining - active)
    needed_after_inflight = max(0, remaining - inflight - imported)
    submit_limit = min(limit, needed_after_active)
    if ready_or_retry > 0:
        submit_limit = min(limit, ready_or_retry, needed_after_inflight)
    return {
        "target_remaining": remaining,
        "remaining_count": remaining,
        "ai_submit_active_count": active,
        "ai_submit_inflight_count": inflight,
        "ready_to_submit_count": ready,
        "recoverable_failed_count": retry,
        "imported_package_count": imported,
        "consumed_package_count": consumed,
        "needed_after_active": needed_after_active,
        "needed_after_inflight": needed_after_inflight,
        "submit_limit": max(0, submit_limit),
    }


def is_recoverable_submit_failure(text: str) -> bool:
    lower = str(text or "").lower()
    return any(token in lower for token in ["imini_allow_real_submit", "real_submit_disabled", "真实提交默认关闭"])


def _list_packages(ctx: SkillContext, product_id: str) -> list[dict[str, Any]]:
    try:
        return ctx.repo.list_where("segment_prompt_packages", "product_id=?", (product_id,))
    except Exception:
        return []


def _status(row: dict[str, Any]) -> str:
    return str(row.get("package_status") or row.get("status") or "").strip()


def _configured_submit_limit() -> int:
    try:
        return max(1, int(os.environ.get("AUTO_MIXCUT_GUARD_AI_SUBMIT_LIMIT", "5") or "5"))
    except ValueError:
        return 5
