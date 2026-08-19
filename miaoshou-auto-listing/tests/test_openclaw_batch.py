from pathlib import Path
import subprocess
import unittest

from scripts.run_openclaw_listing_batch import (
    execute_pending_batch,
    parse_execution_result,
)


class FakeTable:
    def __init__(self, actionable_values, verification_ids=None):
        self.actionable_values = iter(actionable_values)
        self.verification_ids = verification_ids or []

    def inspect(self):
        return {"records": 3, "pending": 3, "actionable": next(self.actionable_values)}

    def verification_pending_record_ids(self, limit=50):
        return self.verification_ids[:limit]


class OpenClawBatchTest(unittest.TestCase):
    def test_parse_execution_result_requires_task_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "task_id"):
            parse_execution_result('{"success": true}')

    def test_batch_reports_claim_validation_rejection(self) -> None:
        completed = subprocess.CompletedProcess(
            [], 0, "No pending Feishu task with a non-empty source URL.\n", ""
        )
        summary = execute_pending_batch(
            FakeTable([1, 0]),
            config_dir=Path("config"),
            max_items=20,
            command_runner=lambda command: completed,
        )
        self.assertEqual(
            summary["items"][0]["error_code"], "QUEUE_VALIDATION_REJECTED"
        )

    def test_batch_stops_after_first_error(self) -> None:
        results = iter(
            [
                subprocess.CompletedProcess(
                    [], 0, '{"task_id":"rec1","success":true,"platform_product_id":"p1"}', ""
                ),
                subprocess.CompletedProcess(
                    [], 1, '{"task_id":"rec2","success":false,"error_code":"LOGIN_EXPIRED"}', ""
                ),
            ]
        )
        summary = execute_pending_batch(
            FakeTable([2, 1, 1]),
            config_dir=Path("config"),
            max_items=20,
            command_runner=lambda command: next(results),
        )
        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["succeeded"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["skipped"], 0)
        self.assertEqual(summary["blocking_failed"], 1)
        self.assertEqual(summary["stopped_reason"], "first_error")
        self.assertEqual(summary["remaining_actionable"], 1)

    def test_batch_continues_after_safe_size_chart_error(self) -> None:
        results = iter(
            [
                subprocess.CompletedProcess(
                    [], 1, '{"task_id":"rec1","success":false,"error_code":"SIZE_CHART_REQUIRED"}', ""
                ),
                subprocess.CompletedProcess(
                    [], 0, '{"task_id":"rec2","success":true,"platform_product_id":"p2"}', ""
                ),
            ]
        )
        summary = execute_pending_batch(
            FakeTable([2, 1, 0, 0]),
            config_dir=Path("config"),
            max_items=20,
            command_runner=lambda command: next(results),
        )
        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["succeeded"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["blocking_failed"], 0)
        self.assertEqual(summary["stopped_reason"], "queue_empty")

    def test_batch_stops_when_queue_is_empty(self) -> None:
        summary = execute_pending_batch(
            FakeTable([0, 0]),
            config_dir=Path("config"),
            max_items=20,
            command_runner=lambda command: self.fail("runner should not be called"),
        )
        self.assertEqual(summary["processed"], 0)
        self.assertEqual(summary["stopped_reason"], "queue_empty")

    def test_submitted_pending_does_not_block_the_next_product(self) -> None:
        results = iter(
            [
                subprocess.CompletedProcess(
                    [], 1,
                    '{"task_id":"rec1","success":false,"published_status":"SUBMITTED_PENDING_VERIFICATION"}',
                    "",
                ),
                subprocess.CompletedProcess(
                    [], 0,
                    '{"task_id":"rec2","success":true,"platform_product_id":"p2"}',
                    "",
                ),
            ]
        )
        summary = execute_pending_batch(
            FakeTable([2, 1, 0, 0]),
            config_dir=Path("config"),
            max_items=20,
            command_runner=lambda command: next(results),
        )
        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["verification_pending_submissions"], 1)
        self.assertEqual(summary["blocking_failed"], 0)


if __name__ == "__main__":
    unittest.main()
