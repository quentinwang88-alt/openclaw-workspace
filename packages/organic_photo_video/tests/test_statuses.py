#!/usr/bin/env python3
"""State-machine tests for domain/statuses.py."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import statuses
from domain.statuses import InvalidTransitionError


class TaskStatusMachineTest(unittest.TestCase):
    def test_full_main_line_path_walks_to_archived(self) -> None:
        main_line = [
            statuses.TASK_DRAFT,
            statuses.TASK_PLANNED,
            statuses.TASK_HERO_GENERATING,
            statuses.TASK_IMAGE_GENERATING,
            statuses.TASK_IMAGE_REVIEW,
            statuses.TASK_RENDERING,
            statuses.TASK_VIDEO_REVIEW,
            statuses.TASK_PUBLISH_PREPARING,
            statuses.TASK_READY_TO_PUBLISH,
            statuses.TASK_PUBLISHING,
            statuses.TASK_PUBLISHED,
            statuses.TASK_METRICS_COLLECTED,
            statuses.TASK_ARCHIVED,
        ]
        for current, target in zip(main_line, main_line[1:]):
            statuses.task_ensure_transition(current, target)

    def test_loop_backs_for_partial_retry(self) -> None:
        statuses.task_ensure_transition(statuses.TASK_IMAGE_REVIEW, statuses.TASK_IMAGE_GENERATING)
        statuses.task_ensure_transition(statuses.TASK_VIDEO_REVIEW, statuses.TASK_RENDERING)

    def test_skipping_states_is_illegal(self) -> None:
        with self.assertRaises(InvalidTransitionError):
            statuses.task_ensure_transition(statuses.TASK_DRAFT, statuses.TASK_RENDERING)
        with self.assertRaises(InvalidTransitionError):
            statuses.task_ensure_transition(statuses.TASK_HERO_GENERATING, statuses.TASK_PUBLISHED)

    def test_failed_reachable_from_every_operational_stage(self) -> None:
        exempt = {
            statuses.TASK_FAILED,
            statuses.TASK_ARCHIVED,
            statuses.TASK_METRICS_COLLECTED,  # capture failures surface from published
        }
        for status, allowed in statuses.TASK_STATUS_TRANSITIONS.items():
            if status in exempt:
                continue
            self.assertIn(
                statuses.TASK_FAILED,
                allowed,
                f"{status} must be able to enter failed",
            )

    def test_failed_can_reenter_operational_stages_only(self) -> None:
        allowed = statuses.TASK_STATUS_TRANSITIONS[statuses.TASK_FAILED]
        self.assertNotIn(statuses.TASK_DRAFT, allowed)
        self.assertNotIn(statuses.TASK_PLANNED, allowed)
        self.assertNotIn(statuses.TASK_ARCHIVED, allowed)
        statuses.task_ensure_transition(statuses.TASK_FAILED, statuses.TASK_IMAGE_GENERATING)
        statuses.task_ensure_transition(statuses.TASK_FAILED, statuses.TASK_PUBLISHING)

    def test_archived_is_terminal_and_stage_mapping_is_complete(self) -> None:
        self.assertEqual(statuses.TASK_STATUS_TRANSITIONS[statuses.TASK_ARCHIVED], frozenset())
        self.assertEqual(
            set(statuses.STAGE_FOR_STATUS), set(statuses.TASK_STATUSES)
        )
        self.assertEqual(
            statuses.STAGE_FOR_STATUS[statuses.TASK_DRAFT], statuses.STAGE_INTAKE
        )

    def test_native_photo_main_line_skips_video_render(self) -> None:
        transitions = statuses.TASK_STATUS_TRANSITIONS
        statuses.ensure_transition(
            transitions, statuses.TASK_PLANNED, statuses.TASK_IMAGE_GENERATING
        )
        statuses.ensure_transition(
            transitions, statuses.TASK_IMAGE_GENERATING, statuses.TASK_IMAGE_REVIEW
        )
        statuses.ensure_transition(
            transitions, statuses.TASK_IMAGE_REVIEW, statuses.TASK_PHOTO_PACKAGING
        )
        statuses.ensure_transition(
            transitions, statuses.TASK_PHOTO_PACKAGING, statuses.TASK_PHOTO_READY
        )
        statuses.ensure_transition(
            transitions, statuses.TASK_PHOTO_READY, statuses.TASK_PUBLISH_PREPARING
        )

    def test_photo_publish_can_archive_without_metrics(self) -> None:
        statuses.ensure_transition(
            statuses.TASK_STATUS_TRANSITIONS,
            statuses.TASK_PUBLISHED,
            statuses.TASK_ARCHIVED,
        )
        self.assertEqual(
            statuses.STAGE_FOR_STATUS[statuses.TASK_PHOTO_PACKAGING],
            statuses.STAGE_PHOTO_PACKAGING,
        )
        self.assertEqual(
            statuses.STAGE_FOR_STATUS[statuses.TASK_PHOTO_READY],
            statuses.STAGE_PHOTO_REVIEW,
        )


class ShotStatusMachineTest(unittest.TestCase):
    def test_planned_to_approved_main_line(self) -> None:
        transitions = statuses.SHOT_STATUS_TRANSITIONS
        statuses.ensure_transition(transitions, statuses.SHOT_PLANNED, statuses.SHOT_GENERATING)
        statuses.ensure_transition(transitions, statuses.SHOT_GENERATING, statuses.SHOT_GENERATED)
        statuses.ensure_transition(transitions, statuses.SHOT_GENERATED, statuses.SHOT_APPROVED)

    def test_rejected_is_terminal_per_version(self) -> None:
        transitions = statuses.SHOT_STATUS_TRANSITIONS
        self.assertEqual(transitions[statuses.SHOT_REJECTED], frozenset())
        statuses.ensure_transition(transitions, statuses.SHOT_GENERATED, statuses.SHOT_REJECTED)

    def test_failed_generation_can_retry(self) -> None:
        transitions = statuses.SHOT_STATUS_TRANSITIONS
        statuses.ensure_transition(transitions, statuses.SHOT_FAILED, statuses.SHOT_GENERATING)


class PublishRenderOutboxMachineTest(unittest.TestCase):
    def test_publish_main_line_and_manual_retry_after_failure(self) -> None:
        transitions = statuses.PUBLISH_STATUS_TRANSITIONS
        statuses.ensure_transition(transitions, statuses.PUBLISH_READY, statuses.PUBLISH_PREPARING)
        statuses.ensure_transition(transitions, statuses.PUBLISH_PREPARING, statuses.PUBLISH_SUBMITTED)
        statuses.ensure_transition(transitions, statuses.PUBLISH_SUBMITTED, statuses.PUBLISH_PUBLISHED)
        statuses.ensure_transition(transitions, statuses.PUBLISH_FAILED, statuses.PUBLISH_PREPARING)
        self.assertEqual(transitions[statuses.PUBLISH_PUBLISHED], frozenset())

    def test_render_failure_requeues_without_new_row(self) -> None:
        transitions = statuses.RENDER_STATUS_TRANSITIONS
        statuses.ensure_transition(transitions, statuses.RENDER_FAILED, statuses.RENDER_QUEUED)
        statuses.ensure_transition(transitions, statuses.RENDER_QUEUED, statuses.RENDER_RENDERING)

    def test_outbox_cannot_skip_holding_gate(self) -> None:
        transitions = statuses.OUTBOX_STATUS_TRANSITIONS
        with self.assertRaises(InvalidTransitionError):
            statuses.ensure_transition(transitions, statuses.OUTBOX_HOLDING, statuses.OUTBOX_SENT)
        statuses.ensure_transition(transitions, statuses.OUTBOX_HOLDING, statuses.OUTBOX_PENDING)

    def test_unknown_status_reports_known_statuses(self) -> None:
        with self.assertRaises(InvalidTransitionError) as ctx:
            statuses.ensure_transition(statuses.TASK_STATUS_TRANSITIONS, "nonsense", statuses.TASK_DRAFT)
        self.assertIn("unknown status", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
