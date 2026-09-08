"""OPV status constants and guarded state-machine transitions.

Mirrors docs/MODEL_HANDOFF.md section 7:

    draft -> planned -> hero_generating -> image_generating -> image_review
    -> rendering -> video_review -> publish_preparing -> ready_to_publish
    -> publishing -> published -> metrics_collected -> archived

Any operational stage may enter ``failed``. Recovery rules rerun only the
failed stage: hero failures rerun hero only, single-shot failures add a new
shot version, render failures reuse approved images, BGM failures downgrade
audio strategy without touching images/video, and unclear publish responses
are re-queried (never blindly resubmitted).
"""

from __future__ import annotations

from typing import Dict, FrozenSet, Mapping


class InvalidTransitionError(ValueError):
    """Raised when a status change is not part of the declared state machine."""


# --------------------------------------------------------------------------
# Content task main status
# --------------------------------------------------------------------------

TASK_DRAFT = "draft"
TASK_PLANNED = "planned"
TASK_HERO_GENERATING = "hero_generating"
TASK_ANCHOR_REVIEW = "anchor_review"
TASK_IMAGE_GENERATING = "image_generating"
TASK_IMAGE_REVIEW = "image_review"
TASK_PHOTO_PACKAGING = "photo_packaging"
TASK_PHOTO_READY = "photo_ready"
TASK_REWORK_PENDING = "rework_pending"
TASK_RENDERING = "rendering"
TASK_VIDEO_REVIEW = "video_review"
TASK_PUBLISH_PREPARING = "publish_preparing"
TASK_READY_TO_PUBLISH = "ready_to_publish"
TASK_PUBLISHING = "publishing"
TASK_PUBLISHED = "published"
TASK_METRICS_COLLECTED = "metrics_collected"
TASK_ARCHIVED = "archived"
TASK_FAILED = "failed"

TASK_STATUSES: FrozenSet[str] = frozenset(
    {
        TASK_DRAFT,
        TASK_PLANNED,
        TASK_HERO_GENERATING,
        TASK_ANCHOR_REVIEW,
        TASK_IMAGE_GENERATING,
        TASK_IMAGE_REVIEW,
        TASK_PHOTO_PACKAGING,
        TASK_PHOTO_READY,
        TASK_REWORK_PENDING,
        TASK_RENDERING,
        TASK_VIDEO_REVIEW,
        TASK_PUBLISH_PREPARING,
        TASK_READY_TO_PUBLISH,
        TASK_PUBLISHING,
        TASK_PUBLISHED,
        TASK_METRICS_COLLECTED,
        TASK_ARCHIVED,
        TASK_FAILED,
    }
)

TASK_STATUS_TRANSITIONS: Dict[str, FrozenSet[str]] = {
    TASK_DRAFT: frozenset({TASK_PLANNED, TASK_FAILED}),
    # Asset-reuse photo plans can start image preparation without generating
    # an artificial product/persona anchor.
    TASK_PLANNED: frozenset(
        {TASK_HERO_GENERATING, TASK_IMAGE_GENERATING, TASK_FAILED}
    ),
    # V1 plans retain the direct hero -> image generation path.  V2 plans
    # stop here until the selected anchor has passed a scoped content review.
    TASK_HERO_GENERATING: frozenset({TASK_ANCHOR_REVIEW, TASK_IMAGE_GENERATING, TASK_FAILED}),
    TASK_ANCHOR_REVIEW: frozenset({TASK_HERO_GENERATING, TASK_IMAGE_GENERATING, TASK_REWORK_PENDING, TASK_FAILED}),
    TASK_IMAGE_GENERATING: frozenset({TASK_IMAGE_REVIEW, TASK_FAILED}),
    # image_review may loop back to image_generating when a single slot is redone.
    TASK_IMAGE_REVIEW: frozenset(
        {
            TASK_RENDERING,
            TASK_PHOTO_PACKAGING,
            TASK_IMAGE_GENERATING,
            TASK_REWORK_PENDING,
            TASK_FAILED,
        }
    ),
    TASK_PHOTO_PACKAGING: frozenset(
        {TASK_PHOTO_READY, TASK_IMAGE_REVIEW, TASK_REWORK_PENDING, TASK_FAILED}
    ),
    TASK_PHOTO_READY: frozenset(
        {TASK_PUBLISH_PREPARING, TASK_PHOTO_PACKAGING, TASK_REWORK_PENDING, TASK_FAILED}
    ),
    TASK_RENDERING: frozenset({TASK_VIDEO_REVIEW, TASK_FAILED}),
    # video_review may loop back to rendering when the group is re-rendered.
    TASK_VIDEO_REVIEW: frozenset(
        {TASK_PUBLISH_PREPARING, TASK_RENDERING, TASK_REWORK_PENDING, TASK_FAILED}
    ),
    TASK_PUBLISH_PREPARING: frozenset({TASK_READY_TO_PUBLISH, TASK_FAILED}),
    TASK_READY_TO_PUBLISH: frozenset({TASK_PUBLISHING, TASK_FAILED}),
    # Ambiguous publish responses stay in publishing while being re-queried;
    # resubmission requires an explicit decision, never an automatic retry.
    TASK_PUBLISHING: frozenset({TASK_PUBLISHED, TASK_FAILED}),
    # Metrics are optional for the native-photo MVP, so a completed post may
    # be archived directly while the historical metric path stays available.
    TASK_PUBLISHED: frozenset(
        {TASK_METRICS_COLLECTED, TASK_ARCHIVED, TASK_FAILED}
    ),
    TASK_METRICS_COLLECTED: frozenset({TASK_ARCHIVED}),
    TASK_ARCHIVED: frozenset(),
    # Recovery entry points. Metric collection failures do not need a status
    # detour: metric snapshots upsert idempotently per (publish_id, window).
    TASK_FAILED: frozenset(
        {
            TASK_HERO_GENERATING,
            TASK_IMAGE_GENERATING,
            TASK_IMAGE_REVIEW,
            TASK_PHOTO_PACKAGING,
            TASK_PHOTO_READY,
            TASK_RENDERING,
            TASK_VIDEO_REVIEW,
            TASK_PUBLISH_PREPARING,
            TASK_READY_TO_PUBLISH,
            TASK_PUBLISHING,
        }
    ),
    # A rework is always explicit.  The caller selects the narrowest safe
    # recovery stage instead of pretending every failure is a new task.
    TASK_REWORK_PENDING: frozenset(
        {TASK_PLANNED, TASK_HERO_GENERATING, TASK_IMAGE_GENERATING,
         TASK_IMAGE_REVIEW, TASK_PHOTO_PACKAGING, TASK_PHOTO_READY,
         TASK_RENDERING, TASK_FAILED}
    ),
}

# --------------------------------------------------------------------------
# Task stage (opv_content_task.current_stage) and its mapping from status
# --------------------------------------------------------------------------

STAGE_INTAKE = "intake"
STAGE_PLANNING = "planning"
STAGE_HERO_GENERATION = "hero_generation"
STAGE_IMAGE_GENERATION = "image_generation"
STAGE_IMAGE_REVIEW = "image_review"
STAGE_PHOTO_PACKAGING = "photo_packaging"
STAGE_PHOTO_REVIEW = "photo_review"
STAGE_RENDERING = "rendering"
STAGE_VIDEO_REVIEW = "video_review"
STAGE_PUBLISH_PREPARATION = "publish_preparation"
STAGE_PUBLISH = "publish"
STAGE_METRICS = "metrics"
STAGE_ARCHIVE = "archive"

STAGE_FOR_STATUS: Dict[str, str] = {
    TASK_DRAFT: STAGE_INTAKE,
    TASK_PLANNED: STAGE_PLANNING,
    TASK_HERO_GENERATING: STAGE_HERO_GENERATION,
    TASK_ANCHOR_REVIEW: STAGE_IMAGE_REVIEW,
    TASK_IMAGE_GENERATING: STAGE_IMAGE_GENERATION,
    TASK_IMAGE_REVIEW: STAGE_IMAGE_REVIEW,
    TASK_PHOTO_PACKAGING: STAGE_PHOTO_PACKAGING,
    TASK_PHOTO_READY: STAGE_PHOTO_REVIEW,
    TASK_REWORK_PENDING: STAGE_PLANNING,
    TASK_RENDERING: STAGE_RENDERING,
    TASK_VIDEO_REVIEW: STAGE_VIDEO_REVIEW,
    TASK_PUBLISH_PREPARING: STAGE_PUBLISH_PREPARATION,
    TASK_READY_TO_PUBLISH: STAGE_PUBLISH_PREPARATION,
    TASK_PUBLISHING: STAGE_PUBLISH,
    TASK_PUBLISHED: STAGE_METRICS,
    TASK_METRICS_COLLECTED: STAGE_METRICS,
    TASK_ARCHIVED: STAGE_ARCHIVE,
    TASK_FAILED: STAGE_METRICS,  # placeholder; real stage stays frozen on failure
}

TERMINAL_TASK_STATUSES: FrozenSet[str] = frozenset({TASK_ARCHIVED})


# --------------------------------------------------------------------------
# Content shot (opv_content_shot)
# --------------------------------------------------------------------------

SHOT_PLANNED = "planned"
SHOT_GENERATING = "generating"
SHOT_GENERATED = "generated"
SHOT_APPROVED = "approved"
SHOT_REJECTED = "rejected"
SHOT_FAILED = "failed"

SHOT_STATUS_TRANSITIONS: Dict[str, FrozenSet[str]] = {
    SHOT_PLANNED: frozenset({SHOT_GENERATING, SHOT_FAILED}),
    SHOT_GENERATING: frozenset({SHOT_GENERATED, SHOT_FAILED}),
    SHOT_GENERATED: frozenset({SHOT_APPROVED, SHOT_REJECTED, SHOT_FAILED}),
    SHOT_APPROVED: frozenset(),
    # Rejection is terminal per version; retries add a new shot_version row.
    SHOT_REJECTED: frozenset(),
    SHOT_FAILED: frozenset({SHOT_GENERATING}),
}

SHOT_QA_PENDING = "pending"
SHOT_QA_IN_REVIEW = "in_review"
SHOT_QA_PASSED = "passed"
SHOT_QA_FAILED = "failed"
SHOT_QA_WAIVED = "waived"

SHOT_QA_STATUSES: FrozenSet[str] = frozenset(
    {SHOT_QA_PENDING, SHOT_QA_IN_REVIEW, SHOT_QA_PASSED, SHOT_QA_FAILED, SHOT_QA_WAIVED}
)

# Default P1-P5 slot roles (opv_content_shot.slot_role).
SHOT_ROLES: FrozenSet[str] = frozenset(
    {"hero", "full_look", "lifestyle", "detail", "second_angle"}
)


# --------------------------------------------------------------------------
# Video render (opv_video_render)
# --------------------------------------------------------------------------

RENDER_QUEUED = "queued"
RENDER_RENDERING = "rendering"
RENDER_COMPLETED = "completed"
RENDER_FAILED = "failed"

RENDER_STATUS_TRANSITIONS: Dict[str, FrozenSet[str]] = {
    RENDER_QUEUED: frozenset({RENDER_RENDERING, RENDER_FAILED}),
    RENDER_RENDERING: frozenset({RENDER_COMPLETED, RENDER_FAILED}),
    RENDER_COMPLETED: frozenset(),
    # Re-render reuses approved images; failures requeue the same render row.
    RENDER_FAILED: frozenset({RENDER_QUEUED}),
}

QC_PENDING = "pending"
QC_PASSED = "passed"
QC_FAILED = "failed"

QC_STATUSES: FrozenSet[str] = frozenset({QC_PENDING, QC_PASSED, QC_FAILED})


# --------------------------------------------------------------------------
# Publish record (opv_publish_record)
# --------------------------------------------------------------------------

PUBLISH_READY = "ready"
PUBLISH_PREPARING = "preparing"
PUBLISH_SUBMITTED = "submitted"
PUBLISH_PUBLISHED = "published"
PUBLISH_FAILED = "failed"

PUBLISH_STATUS_TRANSITIONS: Dict[str, FrozenSet[str]] = {
    PUBLISH_READY: frozenset({PUBLISH_PREPARING, PUBLISH_FAILED}),
    PUBLISH_PREPARING: frozenset({PUBLISH_SUBMITTED, PUBLISH_FAILED}),
    PUBLISH_SUBMITTED: frozenset({PUBLISH_PUBLISHED, PUBLISH_FAILED}),
    PUBLISH_PUBLISHED: frozenset(),
    # Retry after manual re-verification only (回查确认), never automatic.
    PUBLISH_FAILED: frozenset({PUBLISH_PREPARING}),
}


# --------------------------------------------------------------------------
# Look feedback (opv_look_feedback)
# --------------------------------------------------------------------------

FEEDBACK_HUMAN_REVIEW = "human_review"
FEEDBACK_MACHINE_QA = "machine_qa"
FEEDBACK_METRICS_EVAL = "metrics_eval"
FEEDBACK_TYPES: FrozenSet[str] = frozenset(
    {FEEDBACK_HUMAN_REVIEW, FEEDBACK_MACHINE_QA, FEEDBACK_METRICS_EVAL}
)

DECISION_APPROVE = "approve"
DECISION_REJECT = "reject"
DECISION_CANDIDATE = "promote_candidate"
DECISION_CONFIRM = "promote_confirm"
DECISION_PROMOTION_REJECT = "promote_reject"
FEEDBACK_DECISIONS: FrozenSet[str] = frozenset(
    {
        DECISION_APPROVE,
        DECISION_REJECT,
        DECISION_CANDIDATE,
        DECISION_CONFIRM,
        DECISION_PROMOTION_REJECT,
    }
)

PROMOTION_NOT_REQUESTED = "not_requested"
PROMOTION_REQUESTED = "requested"
PROMOTION_CONFIRMED = "confirmed"
PROMOTION_REJECTED = "rejected"
PROMOTION_STATUSES: FrozenSet[str] = frozenset(
    {
        PROMOTION_NOT_REQUESTED,
        PROMOTION_REQUESTED,
        PROMOTION_CONFIRMED,
        PROMOTION_REJECTED,
    }
)


# --------------------------------------------------------------------------
# Feishu outbox (opv_feishu_outbox) - default holding, never auto-sent
# --------------------------------------------------------------------------

OUTBOX_HOLDING = "holding"
OUTBOX_PENDING = "pending"
OUTBOX_SENDING = "sending"
OUTBOX_SENT = "sent"
OUTBOX_FAILED = "failed"
OUTBOX_SKIPPED = "skipped"

OUTBOX_STATUS_TRANSITIONS: Dict[str, FrozenSet[str]] = {
    OUTBOX_HOLDING: frozenset({OUTBOX_PENDING, OUTBOX_SKIPPED}),
    OUTBOX_PENDING: frozenset({OUTBOX_SENDING, OUTBOX_SKIPPED}),
    OUTBOX_SENDING: frozenset({OUTBOX_SENT, OUTBOX_FAILED}),
    OUTBOX_SENT: frozenset(),
    OUTBOX_FAILED: frozenset({OUTBOX_PENDING, OUTBOX_SKIPPED}),
    OUTBOX_SKIPPED: frozenset(),
}


# --------------------------------------------------------------------------
# Content package (opv_content_package) - the reusable output object
# --------------------------------------------------------------------------

PACKAGE_PLANNING = "planning"
PACKAGE_GENERATING = "generating"
PACKAGE_QA_REVIEW = "qa_review"
PACKAGE_READY = "ready"
PACKAGE_RENDERED = "rendered"
PACKAGE_INVALID = "invalid"

PACKAGE_STATUS_TRANSITIONS: Dict[str, FrozenSet[str]] = {
    PACKAGE_PLANNING: frozenset({PACKAGE_GENERATING, PACKAGE_INVALID}),
    PACKAGE_GENERATING: frozenset({PACKAGE_QA_REVIEW, PACKAGE_INVALID}),
    PACKAGE_QA_REVIEW: frozenset({PACKAGE_READY, PACKAGE_GENERATING, PACKAGE_INVALID}),
    PACKAGE_READY: frozenset({PACKAGE_RENDERED, PACKAGE_INVALID}),
    PACKAGE_RENDERED: frozenset({PACKAGE_QA_REVIEW}),
    PACKAGE_INVALID: frozenset(),
}


# --------------------------------------------------------------------------
# Transition helpers
# --------------------------------------------------------------------------

def can_transition(
    transitions: Mapping[str, FrozenSet[str]], current: str, target: str
) -> bool:
    """Return True when ``current -> target`` is a declared transition."""
    allowed = transitions.get(current)
    if allowed is None:
        return False
    return target in allowed


def ensure_transition(
    transitions: Mapping[str, FrozenSet[str]], current: str, target: str
) -> None:
    """Raise InvalidTransitionError unless the transition is declared."""
    allowed = transitions.get(current)
    if allowed is None:
        raise InvalidTransitionError(
            f"unknown status {current!r}; known: {sorted(transitions)}"
        )
    if target not in allowed:
        raise InvalidTransitionError(
            f"illegal transition {current!r} -> {target!r}; allowed: {sorted(allowed)}"
        )


def task_can_transition(current: str, target: str) -> bool:
    return can_transition(TASK_STATUS_TRANSITIONS, current, target)


def task_ensure_transition(current: str, target: str) -> None:
    ensure_transition(TASK_STATUS_TRANSITIONS, current, target)
