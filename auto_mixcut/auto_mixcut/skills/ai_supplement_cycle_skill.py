from __future__ import annotations

from typing import Any, Callable

from auto_mixcut.core.result import Result
from auto_mixcut.domain.statuses import NextAction

from .ai_supplement_gateway_skill import AISupplementGatewaySkill
from .context import SkillContext
from .mixcut_state_machine_skill import decide_factory_state


SubmitFn = Callable[[SkillContext, str, bool], dict[str, Any]]
RecoverFn = Callable[[SkillContext, str, bool], dict[str, Any]]
ImportReturnsFn = Callable[[str, bool], dict[str, Any]]
GuardFn = Callable[[str, bool, str], dict[str, Any]]


class AISupplementCycleSkill:
    """One AI supplement production cycle for a product.

    This skill owns the orchestration decision. Platform-specific actions stay
    in the existing heartbeat functions and are passed in as callbacks.
    """

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def inspect(self, product_id: str) -> Result:
        product_id = str(product_id or "").strip()
        if not product_id:
            return Result.fail("PRODUCT_ID_REQUIRED", "product_id is required")
        task = _latest_task(self.ctx, product_id)
        package_state = AISupplementGatewaySkill(self.ctx).package_state(product_id)
        task_state = _task_state(task, package_state)
        decision = decide_factory_state(task_state, package_state=package_state)
        cycle_status, next_action, reason = _inspect_cycle_status(task_state, package_state, decision)
        return Result.ok(
            {
                "product_id": product_id,
                "cycle_status": cycle_status,
                "next_action": next_action,
                "package_state": package_state,
                "reason": reason,
                "decision": _decision_payload(decision),
                "state_after": task_state,
            }
        )

    def run_once(
        self,
        product_id: str,
        *,
        submit: bool = True,
        recover: bool = True,
        import_returns: bool = True,
        run_guard: bool = True,
        dry_run: bool = False,
        lock_owner: str = "",
        submit_fn: SubmitFn | None = None,
        recover_fn: RecoverFn | None = None,
        import_returns_fn: ImportReturnsFn | None = None,
        guard_fn: GuardFn | None = None,
    ) -> Result:
        product_id = str(product_id or "").strip()
        if not product_id:
            return Result.fail("PRODUCT_ID_REQUIRED", "product_id is required")

        before = self.inspect(product_id)
        if not before.success:
            return before
        steps: dict[str, Any] = {}
        planned_actions = _planned_actions(before.data or {}, submit=submit, recover=recover, import_returns=import_returns, run_guard=run_guard)
        if dry_run:
            return Result.ok(
                {
                    "product_id": product_id,
                    "cycle_status": (before.data or {}).get("cycle_status"),
                    "next_action": (before.data or {}).get("next_action"),
                    "reason": (before.data or {}).get("reason"),
                    "dry_run": True,
                    "planned_actions": planned_actions,
                    "package_state": (before.data or {}).get("package_state") or {},
                    "steps": steps,
                    "state_before": (before.data or {}).get("state_after") or {},
                    "state_after": (before.data or {}).get("state_after") or {},
                    "continue_recommended": False,
                }
            )

        current = before.data or {}
        imported_count = 0
        submitted = False

        if submit and _has_ready_or_retry(current) and submit_fn:
            steps["submit"] = submit_fn(self.ctx, product_id, False)
            submitted = _step_success(steps["submit"])
            current = self.inspect(product_id).data or current
        elif submit and _has_ready_or_retry(current) and not submit_fn:
            steps["submit"] = {"status": "skipped", "reason": "submit_callback_missing"}
        elif _has_ready_or_retry(current):
            steps["submit"] = {"status": "skipped", "reason": "submit_disabled"}

        if recover and _has_inflight_or_recoverable(current) and recover_fn:
            steps["recover"] = recover_fn(self.ctx, product_id, False)
            current = self.inspect(product_id).data or current
        elif recover and _has_inflight_or_recoverable(current) and not recover_fn:
            steps["recover"] = {"status": "skipped", "reason": "recover_callback_missing"}

        if import_returns and import_returns_fn:
            steps["import_returns"] = import_returns_fn(product_id, False)
            imported_count = _imported_count(steps["import_returns"])
            current = self.inspect(product_id).data or current
        elif import_returns:
            steps["import_returns"] = {"status": "skipped", "reason": "import_returns_callback_missing"}

        should_run_guard = run_guard and guard_fn and _should_run_guard(current, imported_count)
        if should_run_guard:
            steps["guard"] = guard_fn(product_id, False, lock_owner)
            current = self.inspect(product_id).data or current
        elif run_guard and not guard_fn and _should_run_guard(current, imported_count):
            steps["guard"] = {"status": "skipped", "reason": "guard_callback_missing"}

        final = self.inspect(product_id)
        if not final.success:
            return final
        data = final.data or {}
        cycle_status = _final_cycle_status(data, steps, imported_count=imported_count, submitted=submitted)
        next_action = _next_action_for_cycle(cycle_status, data)
        reason = _final_reason(cycle_status, data, steps)
        return Result.ok(
            {
                "product_id": product_id,
                "cycle_status": cycle_status,
                "next_action": next_action,
                "reason": reason,
                "package_state": data.get("package_state") or {},
                "steps": steps,
                "state_before": before.data,
                "state_after": data.get("state_after") or {},
                "decision": data.get("decision") or {},
                "imported_count": imported_count,
                "continue_recommended": _continue_recommended(cycle_status, steps),
            }
        )


def _inspect_cycle_status(task_state: dict[str, Any], package_state: dict[str, Any], decision: Any) -> tuple[str, str, str]:
    remaining = _int(task_state.get("remaining_count"))
    material_capacity = _int(task_state.get("material_pool_extra_capacity"))
    ready = _int(package_state.get("ready_to_submit_count"))
    retry = _int(package_state.get("recoverable_failed_count"))
    inflight = _int(package_state.get("inflight_count"))
    if remaining <= 0:
        return "fulfilled", NextAction.NONE, "target_fulfilled"
    if material_capacity >= remaining:
        return "fulfilled", NextAction.RUN_GUARD_AGAIN, "material_capacity_available"
    if ready > 0 or retry > 0:
        return "ready_to_submit", NextAction.RUN_AI_SEGMENT_WORKER, "ready_or_recoverable_prompt_packages"
    if inflight > 0:
        return "waiting_return", NextAction.WAIT_AI_SEGMENT_RETURN, "waiting_ai_return"
    if getattr(decision, "is_blocked", False):
        return "blocked", getattr(decision, "next_action", "CHECK_PIPELINE_LOG"), getattr(decision, "stable_reason", "blocked")
    if getattr(decision, "is_error", False):
        return "failed", getattr(decision, "next_action", "CHECK_PIPELINE_LOG"), getattr(decision, "stable_reason", "error")
    return "blocked", "CHECK_PIPELINE_LOG", "ai_supplement_no_actionable_package"


def _final_cycle_status(data: dict[str, Any], steps: dict[str, Any], *, imported_count: int, submitted: bool) -> str:
    if any(_step_failed(value) for value in steps.values()):
        return "failed"
    package_state = data.get("package_state") or {}
    state = data.get("state_after") or {}
    remaining = _int(state.get("remaining_count"))
    material_capacity = _int(state.get("material_pool_extra_capacity"))
    ready = _int(package_state.get("ready_to_submit_count"))
    retry = _int(package_state.get("recoverable_failed_count"))
    inflight = _int(package_state.get("inflight_count"))
    if remaining <= 0:
        return "fulfilled"
    if imported_count > 0:
        return "imported_continue"
    if material_capacity >= remaining:
        return "fulfilled"
    if submitted and inflight > 0:
        return "submitted_waiting_return"
    if ready > 0 or retry > 0:
        return "ready_to_submit"
    if inflight > 0:
        return "waiting_return"
    return "blocked"


def _next_action_for_cycle(cycle_status: str, data: dict[str, Any]) -> str:
    if cycle_status == "fulfilled":
        state = data.get("state_after") or {}
        return NextAction.NONE if _int(state.get("remaining_count")) <= 0 else NextAction.RUN_GUARD_AGAIN
    if cycle_status in {"ready_to_submit"}:
        return NextAction.RUN_AI_SEGMENT_WORKER
    if cycle_status in {"submitted_waiting_return", "waiting_return"}:
        return NextAction.WAIT_AI_SEGMENT_RETURN
    if cycle_status == "imported_continue":
        return NextAction.RUN_GUARD_AGAIN
    return "CHECK_PIPELINE_LOG"


def _final_reason(cycle_status: str, data: dict[str, Any], steps: dict[str, Any]) -> str:
    if cycle_status == "failed":
        for value in steps.values():
            if _step_failed(value):
                return str(value.get("reason") or value.get("status") or "cycle_step_failed")
        return "cycle_failed"
    if cycle_status == "blocked":
        return str(data.get("reason") or "ai_supplement_no_actionable_package")
    return str(data.get("reason") or cycle_status)


def _continue_recommended(cycle_status: str, steps: dict[str, Any]) -> bool:
    if cycle_status == "imported_continue":
        return True
    if cycle_status == "ready_to_submit" and "submit" in steps and _step_success(steps["submit"]):
        return True
    return False


def _planned_actions(data: dict[str, Any], *, submit: bool, recover: bool, import_returns: bool, run_guard: bool) -> list[str]:
    actions: list[str] = []
    if submit and _has_ready_or_retry(data):
        actions.append("submit")
    if recover and _has_inflight_or_recoverable(data):
        actions.append("recover")
    if import_returns:
        actions.append("import_returns")
    if run_guard and _should_run_guard(data, 0):
        actions.append("guard")
    return actions


def _should_run_guard(data: dict[str, Any], imported_count: int) -> bool:
    if imported_count > 0:
        return True
    state = data.get("state_after") or {}
    remaining = _int(state.get("remaining_count"))
    material_capacity = _int(state.get("material_pool_extra_capacity"))
    return remaining > 0 and material_capacity >= remaining


def _has_ready_or_retry(data: dict[str, Any]) -> bool:
    package_state = data.get("package_state") or {}
    return _int(package_state.get("ready_to_submit_count")) > 0 or _int(package_state.get("recoverable_failed_count")) > 0


def _has_inflight_or_recoverable(data: dict[str, Any]) -> bool:
    package_state = data.get("package_state") or {}
    return _int(package_state.get("inflight_count")) > 0 or _int(package_state.get("recoverable_failed_count")) > 0


def _task_state(task: dict[str, Any] | None, package_state: dict[str, Any]) -> dict[str, Any]:
    task = task or {}
    target = _int(task.get("requested_variant_count"), _int(task.get("allowed_variant_count")))
    actual = _int(task.get("actual_variant_count"))
    remaining = _int(task.get("target_remaining_variant_count"), max(0, target - actual) if target else 0)
    return {
        "task_id": task.get("task_id"),
        "target_count": target,
        "actual_count": actual,
        "remaining_count": remaining,
        "task_status": task.get("task_status"),
        "pipeline_status": task.get("pipeline_status"),
        "next_action": task.get("next_action"),
        "last_error": task.get("last_error"),
        "ai_supplement_status": task.get("ai_supplement_status"),
        "material_pool_extra_capacity": task.get("material_pool_extra_capacity"),
        "first_slot_remaining_capacity": task.get("first_slot_remaining_capacity"),
        "ready_to_submit_count": package_state.get("ready_to_submit_count", 0),
        "inflight_count": package_state.get("inflight_count", 0),
        "recoverable_failed_count": package_state.get("recoverable_failed_count", 0),
        "imported_package_count": package_state.get("imported_package_count", 0),
        "consumed_package_count": package_state.get("consumed_package_count", 0),
    }


def _latest_task(ctx: SkillContext, product_id: str) -> dict[str, Any] | None:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))
    return rows[0] if rows else None


def _decision_payload(decision: Any) -> dict[str, Any]:
    return {
        "pipeline_status": decision.pipeline_status,
        "next_action": decision.next_action,
        "task_status": decision.task_status,
        "display_state": decision.display_state,
        "scanner_mode": decision.scanner_mode,
        "stable_reason": decision.stable_reason,
        "is_done": decision.is_done,
        "is_blocked": decision.is_blocked,
        "is_error": decision.is_error,
        "is_waiting_ai_return": decision.is_waiting_ai_return,
        "should_scan_from_rds": decision.should_scan_from_rds,
        "should_continue_ai_heartbeat": decision.should_continue_ai_heartbeat,
    }


def _imported_count(result: dict[str, Any]) -> int:
    payload = _json_from_stdout(str(result.get("stdout") or ""))
    if isinstance(payload, dict):
        direct = payload.get("imported_count")
        if direct is not None:
            return _int(direct)
        nested = payload.get("import") or {}
        if isinstance(nested, dict):
            return _int(nested.get("count"), _int(nested.get("imported_count")))
    return _int(result.get("imported_count"), _int(result.get("count")))


def _json_from_stdout(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return None
    for index, char in enumerate(stripped):
        if char not in "[{":
            continue
        try:
            import json

            return json.loads(stripped[index:])
        except Exception:
            continue
    return None


def _step_success(step: dict[str, Any]) -> bool:
    status = str(step.get("status") or "").strip().lower()
    if step.get("dry_run"):
        return True
    return status in {"ok", "completed", "success", "synced"} or step.get("success") is True


def _step_failed(step: dict[str, Any]) -> bool:
    status = str(step.get("status") or "").strip().lower()
    return status in {"failed", "timeout", "blocked"} or step.get("success") is False


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
