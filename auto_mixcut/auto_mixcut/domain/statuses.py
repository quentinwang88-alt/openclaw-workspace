from __future__ import annotations


class PipelineStatus:
    RUNNING = "RUNNING"
    READY_TO_CONTINUE = "READY_TO_CONTINUE"
    WAITING_AI_RETURN = "WAITING_AI_RETURN"
    BLOCKED = "BLOCKED"
    ERROR = "ERROR"
    DONE = "DONE"


class NextAction:
    NONE = "NONE"
    GUARD_PASS_STARTED = "GUARD_PASS_STARTED"
    RUN_GUARD_AGAIN = "RUN_GUARD_AGAIN"
    RUN_AI_SEGMENT_WORKER = "RUN_AI_SEGMENT_WORKER"
    WAIT_AI_SEGMENT_RETURN = "WAIT_AI_SEGMENT_RETURN"
    WAIT_AI_SUPPLEMENT_APPROVAL = "WAIT_AI_SUPPLEMENT_APPROVAL"
    CHECK_AI_RETURN_THEN_CONTINUE = "CHECK_AI_RETURN_THEN_CONTINUE"
    CHECK_PIPELINE_LOG = "CHECK_PIPELINE_LOG"


class PromptPackageStatus:
    CREATED = "created"
    NEEDS_FEISHU_SYNC = "needs_feishu_sync"
    READY_TO_SUBMIT = "ready_to_submit"
    SUBMITTED = "submitted"
    GENERATING = "generating"
    RETURNED = "returned"
    IMPORTED = "imported"
    CONSUMED = "consumed"
    FAILED = "failed"
    RECOVERABLE_FAILED = "recoverable_failed"


class SegmentStatus:
    QC_PASSED = "qc_passed"
    QC_FAILED = "qc_failed"
    AI_STAGE_FAILED = "ai_stage_failed"
    UNUSABLE = "unusable"


class OutputStatus:
    RENDERED = "rendered"
    PUBLISH_READY = "publish_ready"
    PASSED = "passed"
    PASSED_WITH_WARNING = "passed_with_warning"
    NEEDS_REVIEW = "needs_review"


TERMINAL_PIPELINE_STATUSES = {PipelineStatus.DONE, PipelineStatus.BLOCKED, PipelineStatus.ERROR}
RUNNABLE_PIPELINE_STATUSES = {PipelineStatus.RUNNING, PipelineStatus.READY_TO_CONTINUE}
WAIT_AI_RETURN_ACTIONS = {NextAction.WAIT_AI_SEGMENT_RETURN}
AI_WORKER_ACTIONS = {NextAction.RUN_AI_SEGMENT_WORKER}
