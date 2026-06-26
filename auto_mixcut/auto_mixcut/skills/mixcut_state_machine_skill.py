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


def decide_mixcut_state(task: dict[str, Any] | None, feishu_state: str = "") -> MixcutStateDecision:
    task = task or {}
    pipeline_status = str(task.get("pipeline_status") or "")
    next_action = str(task.get("next_action") or "")
    task_status = str(task.get("task_status") or "")
    is_done = pipeline_status in DONE_STATUSES or task_status in DONE_STATUSES
    is_blocked = pipeline_status == BLOCKED_STATUS
    waiting_ai_signal = (
        feishu_state == "等待AI回流"
        or pipeline_status == WAITING_AI_RETURN_STATUS
        or next_action in WAIT_AI_RETURN_ACTIONS
    )
    is_waiting_ai_return = waiting_ai_signal and not _has_material_capacity_to_continue(task)
    should_scan = (
        pipeline_status in RUNNABLE_PIPELINE_STATUSES
        or pipeline_status == WAITING_AI_RETURN_STATUS
        or next_action in RUNNABLE_ACTIONS
        or next_action in WAIT_AI_RETURN_ACTIONS
    )
    return MixcutStateDecision(
        pipeline_status=pipeline_status,
        next_action=next_action,
        task_status=task_status,
        display_state=_display_state(
            pipeline_status=pipeline_status,
            next_action=next_action,
            task_status=task_status,
        ),
        scanner_mode="ai_return_heartbeat" if is_waiting_ai_return else "guard",
        is_done=is_done,
        is_blocked=is_blocked,
        is_waiting_ai_return=is_waiting_ai_return,
        should_scan_from_rds=should_scan,
    )


def guard_start_status(detail: dict[str, Any]) -> tuple[str, str]:
    if (
        str(detail.get("ai_supplement_status") or "") == "created"
        and int(detail.get("remaining_count") or 0) > 0
        and int(detail.get("first_slot_remaining_capacity") or 0) <= 0
    ):
        return WAITING_AI_RETURN_STATUS, NextAction.CHECK_AI_RETURN_THEN_CONTINUE
    return PipelineStatus.RUNNING, NextAction.GUARD_PASS_STARTED


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


def _has_material_capacity_to_continue(task: dict[str, Any]) -> bool:
    try:
        remaining = int(task.get("target_remaining_variant_count") or task.get("remaining_count") or 0)
        extra_capacity = int(task.get("material_pool_extra_capacity") or 0)
    except (TypeError, ValueError):
        return False
    return remaining > 0 and extra_capacity >= remaining
