import unittest
from unittest.mock import patch

from scripts.run_feishu_operation_tasks import (
    _enable_production_category_extensions,
    _request_id,
)


class FeishuOperationReplanRequestTest(unittest.TestCase):
    def test_official_workbench_enables_registered_accessory_extension(self):
        with patch.dict("os.environ", {}, clear=True):
            _enable_production_category_extensions()
            import os

            self.assertEqual(
                "1", os.environ["ORIGINAL_SCRIPT_ACCESSORY_PROFILE_ENABLED"]
            )

    def test_replan_supersedes_previous_batch_without_losing_retry_idempotency(self):
        task = {
            "task_id": "TASK-1",
            "product_code": "1736682172005713699",
            "random_seed": 0,
            "test_phase": "INITIAL",
            "batch_id": "OCB_OLD",
        }
        initial = _request_id("rec1", task)
        first_replan = _request_id("rec1", task, replan=True)
        retry_replan = _request_id("rec1", dict(task), replan=True)

        self.assertNotEqual(initial, first_replan)
        self.assertEqual(first_replan, retry_replan)

        next_generation = dict(task, batch_id="OCB_NEW")
        self.assertNotEqual(
            first_replan,
            _request_id("rec1", next_generation, replan=True),
        )


if __name__ == "__main__":
    unittest.main()
