#!/usr/bin/env python3
"""运行入口超时策略回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from run_pipeline import build_parser, should_enable_global_timeout  # noqa: E402


class RunPipelineTimeoutTest(unittest.TestCase):
    def test_batch_queue_disables_cross_record_global_timeout(self) -> None:
        args = build_parser().parse_args(["--limit", "7", "--run-timeout", "7200"])

        self.assertFalse(should_enable_global_timeout(args))

    def test_single_record_keeps_global_timeout(self) -> None:
        args = build_parser().parse_args(["--record-id", "rec1", "--run-timeout", "7200"])

        self.assertTrue(should_enable_global_timeout(args))

    def test_limit_one_keeps_global_timeout(self) -> None:
        args = build_parser().parse_args(["--limit", "1", "--run-timeout", "7200"])

        self.assertTrue(should_enable_global_timeout(args))


if __name__ == "__main__":
    unittest.main()
