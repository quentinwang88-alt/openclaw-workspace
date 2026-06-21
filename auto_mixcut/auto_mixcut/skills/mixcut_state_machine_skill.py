from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DONE_STATUSES = {"DONE"}
WAIT_AI_RETURN_ACTIONS = {"WAIT_AI_SEGMENT_RETURN"}
AI_WORKER_ACTIONS = {"RUN_AI_SEGMENT_WORKER"}
RUNNABLE_ACTIONS = {"GUARD_PASS_STARTED", "RUN_GUARD_AGAIN", *AI_WORKER_ACTIONS}
RUNNABLE_PIPELINE_STATUSES = {"RUNNING", "READY_TO_CONTINUE"}
WAITING_AI_RETURN_STATUS = "WAITING_AI_RETURN"
BLOCKED_STATUS = "BLOCKED"


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
    is_waiting_ai_return = (
        feishu_state == "等待AI回流"
        or pipeline_status == WAITING_AI_RETURN_STATUS
        or next_action in WAIT_AI_RETURN_ACTIONS
    )
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
        return WAITING_AI_RETURN_STATUS, "CHECK_AI_RETURN_THEN_CONTINUE"
    return "RUNNING", "GUARD_PASS_STARTED"


def _display_state(pipeline_status: str, next_action: str, task_status: str) -> str:
    if pipeline_status in DONE_STATUSES or task_status in DONE_STATUSES:
        return "已完成"
    if pipeline_status == BLOCKED_STATUS:
        return "阻断需人工处理"
    if pipeline_status == WAITING_AI_RETURN_STATUS or next_action in WAIT_AI_RETURN_ACTIONS:
        return "等待AI回流"
    if next_action == "WAIT_AI_SUPPLEMENT_APPROVAL":
        return "等待AI补素材"
    if pipeline_status in RUNNABLE_PIPELINE_STATUSES or next_action in RUNNABLE_ACTIONS:
        return "运行中"
    return ""
