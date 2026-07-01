from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from auto_mixcut.domain.statuses import AI_WORKER_ACTIONS, RUNNABLE_PIPELINE_STATUSES, WAIT_AI_RETURN_ACTIONS, NextAction, PipelineStatus

DONE_STATUSES = {PipelineStatus.DONE}
RUNNABLE_ACTIONS = {NextAction.GUARD_PASS_STARTED, NextAction.RUN_GUARD_AGAIN, *AI_WORKER_ACTIONS}
WAITING_AI_RETURN_STATUS = PipelineStatus.WAITING_AI_RETURN
BLOCKED_STATUS = PipelineStatus.BLOCKED


@dataclass(frozen=True)
class MixcutStateDecision:
    pipeline_status: str
    next_action: str
    task_status: str
    display_state: str
    scanner_mode: str
    is_done: bool
    is_blocked: bool
    is_waiting_ai_return: bool
    should_scan_from_rds: bool


@dataclass(frozen=True)
class FactoryStateDecision:
    pipeline_status: str
    next_action: str
    task_status: str
    display_state: str
    scanner_mode: str
    stable_reason: str
    is_done: bool
    is_blocked: bool
    is_error: bool
    is_waiting_ai_return: bool
    should_scan_from_rds: bool
    should_continue_ads_loop: bool
    should_continue_ai_heartbeat: bool

    def task_patch(self) -> dict[str, str]:
        return {
            "task_status": self.task_status,
            "pipeline_status": self.pipeline_status,
            "next_action": self.next_action,
        }


def decide_mixcut_state(task: dict[str, Any] | None, feishu_state: str = "") -> MixcutStateDecision:
    decision = decide_factory_state(task, feishu_state=feishu_state)
    return MixcutStateDecision(
        pipeline_status=decision.pipeline_status,
        next_action=decision.next_action,
        task_status=decision.task_status,
        display_state=decision.display_state,
        scanner_mode=decision.scanner_mode,
        is_done=decision.is_done,
        is_blocked=decision.is_blocked,
        is_waiting_ai_return=decision.is_waiting_ai_return,
        should_scan_from_rds=decision.should_scan_from_rds,
    )


def decide_factory_state(
    task: dict[str, Any] | None,
    feishu_state: str = "",
    package_state: dict[str, Any] | None = None,
    facts: dict[str, Any] | None = None,
) -> FactoryStateDecision:
    task = task or {}
    package_state = package_state or {}
    facts = facts or {}

    task_status_in = _text(facts.get("task_status")) or _text(task.get("task_status"))
    pipeline_in = _text(facts.get("pipeline_status")) or _text(task.get("pipeline_status"))
    next_in = _text(facts.get("next_action")) or _text(task.get("next_action"))
    target = _int(facts.get("target_count"), _int(task.get("requested_variant_count"), _int(task.get("allowed_variant_count"), 0)))
    actual = _int(facts.get("actual_count"), _int(task.get("actual_variant_count"), 0))
    remaining = _int(facts.get("remaining_count"), _int(task.get("target_remaining_variant_count"), max(0, target - actual) if target else 0))
    if target > 0:
        remaining = max(0, remaining if remaining > 0 else target - actual)

    material_capacity = _int(facts.get("material_pool_extra_capacity"), _int(task.get("material_pool_extra_capacity"), 0))
    ready_to_submit = _int(package_state.get("ready_to_submit_count"), _int(facts.get("ready_to_submit_count"), 0))
    inflight = _int(package_state.get("inflight_count"), _int(facts.get("inflight_count"), 0))
    imported = _int(package_state.get("imported_package_count"), _int(facts.get("imported_package_count"), 0))
    ai_status = _text(facts.get("ai_status")).lower()
    render_status = _text(facts.get("render_status")).lower()

    explicit_done = pipeline_in == PipelineStatus.DONE or task_status_in == PipelineStatus.DONE
    target_done = target > 0 and (remaining <= 0 or actual >= target)
    if explicit_done or target_done:
        return _factory_decision(
            PipelineStatus.DONE,
            NextAction.NONE,
            "DONE",
            stable_reason="done",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )

    if render_status in {"failed", "timeout"}:
        return _factory_decision(
            PipelineStatus.BLOCKED,
            NextAction.CHECK_PIPELINE_LOG,
            "RENDER_BLOCKED",
            stable_reason="render_failed",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )

    if ai_status == "failed":
        return _factory_decision(
            PipelineStatus.ERROR,
            "CHECK_ERROR",
            "AI_SUPPLEMENT_FAILED",
            stable_reason="ai_supplement_failed",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )
    if ai_status == "blocked":
        return _factory_decision(
            PipelineStatus.BLOCKED,
            "NEED_MORE_MATERIAL_OR_AI_SUPPLEMENT",
            "AI_SUPPLEMENT_BLOCKED",
            stable_reason="ai_supplement_blocked",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )

    if material_capacity >= remaining and remaining > 0:
        return _factory_decision(
            PipelineStatus.READY_TO_CONTINUE,
            NextAction.RUN_GUARD_AGAIN,
            "RUNNING",
            stable_reason="ready_to_continue_with_existing_material",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )

    if ai_status in {"created", "needs_submit_retry"} or ready_to_submit > 0:
        action = NextAction.RUN_AI_SEGMENT_WORKER if ready_to_submit > 0 or ai_status == "needs_submit_retry" else NextAction.WAIT_AI_SEGMENT_RETURN
        return _factory_decision(
            PipelineStatus.WAITING_AI_RETURN,
            action,
            "AI_SUPPLEMENT_CREATED",
            stable_reason="ready_ai_packages_need_submit" if action == NextAction.RUN_AI_SEGMENT_WORKER else "waiting_ai_return",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )

    if inflight > 0 or ai_status in {"submitted", "generating"}:
        return _factory_decision(
            PipelineStatus.WAITING_AI_RETURN,
            NextAction.WAIT_AI_SEGMENT_RETURN,
            "AI_SUPPLEMENT_SUBMITTED",
            stable_reason="waiting_ai_return",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )

    if imported > 0:
        return _factory_decision(
            PipelineStatus.READY_TO_CONTINUE,
            NextAction.RUN_GUARD_AGAIN,
            "AI_RETURN_IMPORTED",
            stable_reason="ai_return_imported_continue",
            remaining=remaining,
            material_capacity=material_capacity,
            ready_to_submit=ready_to_submit,
            inflight=inflight,
        )

    if pipeline_in == PipelineStatus.ERROR:
        return _factory_decision(pipeline_in, next_in or NextAction.CHECK_PIPELINE_LOG, task_status_in or "ERROR", stable_reason="error", remaining=remaining, material_capacity=material_capacity, ready_to_submit=ready_to_submit, inflight=inflight)
    if pipeline_in == PipelineStatus.BLOCKED:
        return _factory_decision(pipeline_in, next_in or "NEED_MORE_MATERIAL_OR_AI_SUPPLEMENT", task_status_in or "BLOCKED", stable_reason="blocked", remaining=remaining, material_capacity=material_capacity, ready_to_submit=ready_to_submit, inflight=inflight)
    if next_in == NextAction.WAIT_AI_SUPPLEMENT_APPROVAL:
        return _factory_decision(pipeline_in or PipelineStatus.WAITING_AI_RETURN, next_in, task_status_in or "AI_SUPPLEMENT_PENDING_APPROVAL", stable_reason="waiting_ai_supplement_approval", remaining=remaining, material_capacity=material_capacity, ready_to_submit=ready_to_submit, inflight=inflight)
    if feishu_state == "等待AI回流" or pipeline_in == PipelineStatus.WAITING_AI_RETURN or next_in in WAIT_AI_RETURN_ACTIONS:
        return _factory_decision(PipelineStatus.WAITING_AI_RETURN, NextAction.WAIT_AI_SEGMENT_RETURN, task_status_in or "AI_SUPPLEMENT_SUBMITTED", stable_reason="waiting_ai_return", remaining=remaining, material_capacity=material_capacity, ready_to_submit=ready_to_submit, inflight=inflight)
    if pipeline_in in RUNNABLE_PIPELINE_STATUSES or next_in in RUNNABLE_ACTIONS:
        return _factory_decision(PipelineStatus.READY_TO_CONTINUE, NextAction.RUN_GUARD_AGAIN, task_status_in or "RUNNING", stable_reason="ready_to_continue", remaining=remaining, material_capacity=material_capacity, ready_to_submit=ready_to_submit, inflight=inflight)

    return _factory_decision(
        PipelineStatus.READY_TO_CONTINUE,
        NextAction.RUN_GUARD_AGAIN,
        task_status_in or "RUNNING",
        stable_reason="ready_to_continue",
        remaining=remaining,
        material_capacity=material_capacity,
        ready_to_submit=ready_to_submit,
        inflight=inflight,
    )


def guard_start_status(detail: dict[str, Any]) -> tuple[str, str]:
    if (
        str(detail.get("ai_supplement_status") or "") == "created"
        and int(detail.get("remaining_count") or 0) > 0
        and int(detail.get("first_slot_remaining_capacity") or 0) <= 0
    ):
        return WAITING_AI_RETURN_STATUS, NextAction.CHECK_AI_RETURN_THEN_CONTINUE
    return PipelineStatus.RUNNING, NextAction.GUARD_PASS_STARTED


def _factory_decision(
    pipeline_status: str,
    next_action: str,
    task_status: str,
    *,
    stable_reason: str,
    remaining: int,
    material_capacity: int,
    ready_to_submit: int,
    inflight: int,
) -> FactoryStateDecision:
    is_done = pipeline_status == PipelineStatus.DONE or task_status == PipelineStatus.DONE
    is_blocked = pipeline_status == PipelineStatus.BLOCKED
    is_error = pipeline_status == PipelineStatus.ERROR
    is_waiting_ai_return = pipeline_status == PipelineStatus.WAITING_AI_RETURN or next_action in WAIT_AI_RETURN_ACTIONS or next_action == NextAction.RUN_AI_SEGMENT_WORKER
    scanner_mode = _scanner_mode(pipeline_status, next_action, is_done, is_blocked, is_error)
    should_scan = scanner_mode in {"guard", "ai_return_heartbeat"}
    should_continue_ads = scanner_mode == "guard" and remaining > 0 and not (is_done or is_blocked or is_error)
    should_continue_ai = False
    if not (is_done or is_blocked or is_error) and remaining > 0:
        should_continue_ai = (
            next_action in {NextAction.RUN_GUARD_AGAIN, NextAction.RUN_AI_SEGMENT_WORKER, NextAction.WAIT_AI_SUPPLEMENT_APPROVAL}
            or pipeline_status == PipelineStatus.READY_TO_CONTINUE
            or material_capacity >= remaining
            or ready_to_submit > 0
            or inflight <= 0 and is_waiting_ai_return is False
        )
    return FactoryStateDecision(
        pipeline_status=pipeline_status,
        next_action=next_action,
        task_status=task_status,
        display_state=_display_state(pipeline_status=pipeline_status, next_action=next_action, task_status=task_status),
        scanner_mode=scanner_mode,
        stable_reason=stable_reason,
        is_done=is_done,
        is_blocked=is_blocked,
        is_error=is_error,
        is_waiting_ai_return=is_waiting_ai_return,
        should_scan_from_rds=should_scan,
        should_continue_ads_loop=should_continue_ads,
        should_continue_ai_heartbeat=should_continue_ai,
    )


def _scanner_mode(pipeline_status: str, next_action: str, is_done: bool, is_blocked: bool, is_error: bool) -> str:
    if is_done or is_blocked or is_error:
        return "none"
    if next_action in {NextAction.RUN_AI_SEGMENT_WORKER, NextAction.WAIT_AI_SEGMENT_RETURN}:
        return "ai_return_heartbeat"
    if pipeline_status == PipelineStatus.WAITING_AI_RETURN:
        return "ai_return_heartbeat"
    if next_action == NextAction.WAIT_AI_SUPPLEMENT_APPROVAL:
        return "none"
    return "guard"


def _display_state(pipeline_status: str, next_action: str, task_status: str) -> str:
    if pipeline_status in DONE_STATUSES or task_status in DONE_STATUSES:
        return "已完成"
    if pipeline_status == BLOCKED_STATUS:
        return "阻断需人工处理"
    if pipeline_status == WAITING_AI_RETURN_STATUS or next_action in WAIT_AI_RETURN_ACTIONS:
        return "等待AI回流"
    if next_action == NextAction.WAIT_AI_SUPPLEMENT_APPROVAL:
        return "等待AI补素材"
    if pipeline_status in RUNNABLE_PIPELINE_STATUSES or next_action in RUNNABLE_ACTIONS:
        return "运行中"
    return ""


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _text(value: Any) -> str:
    return str(value or "").strip()
