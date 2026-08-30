#!/usr/bin/env python3
"""Production runner process-lock tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.run_feishu_operation_tasks import _acquire_process_lock


class OperationRunnerLockTest(unittest.TestCase):
    def test_lock_rejects_overlapping_runner_and_releases_after_close(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "original-script-production.lock"
            first = _acquire_process_lock(lock_path)
            self.assertIsNotNone(first)
            try:
                self.assertIsNone(_acquire_process_lock(lock_path))
            finally:
                first.close()

            second = _acquire_process_lock(lock_path)
            self.assertIsNotNone(second)
            second.close()


if __name__ == "__main__":
    unittest.main()
