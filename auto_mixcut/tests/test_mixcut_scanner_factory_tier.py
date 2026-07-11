from __future__ import annotations

from types import SimpleNamespace
import sys
import unittest

from scripts import run_mixcut_task_scanner as scanner
from scripts.run_mixcut_task_scanner import (
    _ads_command,
    _ads_should_continue,
    _candidate_field_issue,
    _feishu_state_allows_scan,
    _mixcut_quantity_goal,
    _parse_factory_tier,
    _rds_needs_scanner,
)


class MixcutScannerFactoryTierTest(unittest.TestCase):
    def test_parse_factory_tier_from_common_labels(self):
        self.assertEqual(_parse_factory_tier("40"), 40)
        self.assertEqual(_parse_factory_tier("40条"), 40)
        self.assertEqual(_parse_factory_tier("投流档位 60"), 60)
        self.assertEqual(_parse_factory_tier("30条"), 0)

    def test_quantity_goal_prefers_factory_tier(self):
        goal = _mixcut_quantity_goal({"投流混剪档位": "40条", "目标混剪数量": 20})

        self.assertEqual(goal["goal_mode"], "factory_tier")
        self.assertEqual(goal["target"], 40)
        self.assertEqual(goal["factory_tier"], 40)

    def test_quantity_goal_falls_back_to_absolute_target(self):
        goal = _mixcut_quantity_goal({"目标混剪数量": 20})

        self.assertEqual(goal["goal_mode"], "absolute_target")
        self.assertEqual(goal["target"], 20)
        self.assertEqual(goal["factory_tier"], 0)

    def test_ads_command_passes_factory_tier_to_unattended_runner(self):
        cmd = _ads_command(
            {"product_id": "PROD_TIER", "target": 40, "goal_mode": "factory_tier", "factory_tier": 40},
            SimpleNamespace(guard_timeout=1800),
        )

        self.assertIn("--goal-mode", cmd)
        self.assertIn("factory_tier", cmd)
        self.assertIn("--factory-tier", cmd)
        self.assertIn("40", cmd)
        self.assertNotIn("--target-count", cmd)

    def test_ads_command_keeps_legacy_absolute_target(self):
        cmd = _ads_command(
            {"product_id": "PROD_TARGET", "target": 20, "goal_mode": "absolute_target"},
            SimpleNamespace(guard_timeout=1800),
        )

        self.assertIn("--target-count", cmd)
        self.assertIn("20", cmd)
        self.assertNotIn("--factory-tier", cmd)

    def test_candidate_field_issue_accepts_factory_tier_target(self):
        issue = _candidate_field_issue(
            {
                "product_name": "Hair Clip",
                "market": "TH",
                "category": "hair_accessories",
                "target": 40,
                "goal_mode": "factory_tier",
                "factory_tier": 40,
            }
        )

        self.assertEqual(issue, "")

    def test_paused_state_is_hard_skip_even_when_target_increased(self):
        self.assertFalse(_feishu_state_allows_scan("暂停", target_increased=True))
        self.assertFalse(_feishu_state_allows_scan("不处理", target_increased=True))
        self.assertTrue(_feishu_state_allows_scan("完成", target_increased=True))

    def test_missing_rds_task_does_not_make_blank_feishu_row_runnable(self):
        self.assertFalse(_rds_needs_scanner(None))

    def test_ads_continue_only_when_rds_requests_guard_again(self):
        self.assertTrue(
            _ads_should_continue(
                {
                    "actual_variant_count": 15,
                    "target_remaining_variant_count": 5,
                    "pipeline_status": "READY_TO_CONTINUE",
                    "next_action": "RUN_GUARD_AGAIN",
                },
                20,
            )
        )
        self.assertFalse(
            _ads_should_continue(
                {
                    "actual_variant_count": 20,
                    "target_remaining_variant_count": 0,
                    "pipeline_status": "DONE",
                    "next_action": "NONE",
                },
                20,
            )
        )
        self.assertFalse(
            _ads_should_continue(
                {
                    "actual_variant_count": 15,
                    "target_remaining_variant_count": 5,
                    "pipeline_status": "WAITING_AI_RETURN",
                    "next_action": "WAIT_AI_SEGMENT_RETURN",
                },
                20,
            )
        )

    def test_ads_run_forces_child_inline_feishu_skip(self):
        calls = []
        original_run = scanner.subprocess.run

        class Completed:
            returncode = 0
            stdout = "{}"
            stderr = ""

        def fake_run(cmd, cwd, env, text, capture_output, timeout):
            calls.append({"cmd": cmd, "env": env})
            return Completed()

        scanner.subprocess.run = fake_run
        try:
            result = scanner._run([sys.executable, "/tmp/run_ads_mixcut_unattended.py"], timeout=10)
        finally:
            scanner.subprocess.run = original_run

        self.assertEqual(result["status"], "ok")
        self.assertEqual(calls[0]["env"]["AUTO_MIXCUT_ADS_FAST_MODE"], "1")
        self.assertEqual(calls[0]["env"]["AUTO_MIXCUT_SKIP_INLINE_FEISHU_SYNC"], "1")

    def test_ads_candidate_loops_until_factory_target_met(self):
        state = {
            "task_id": "TASK_LOOP",
            "requested_variant_count": 3,
            "actual_variant_count": 0,
            "target_remaining_variant_count": 3,
            "pipeline_status": "READY_TO_CONTINUE",
            "next_action": "RUN_GUARD_AGAIN",
            "task_status": "RUNNING",
        }
        originals = {
            "build_context": scanner.build_context,
            "_latest_task": scanner._latest_task,
            "_ads_command": scanner._ads_command,
            "_mark_task_started": scanner._mark_task_started,
            "_mark_task_finished": scanner._mark_task_finished,
            "_run": scanner._run,
            "_sync_product_task_final": scanner._sync_product_task_final,
        }

        scanner.build_context = lambda: object()
        scanner._latest_task = lambda ctx, product_id: dict(state)
        scanner._ads_command = lambda item, args: [sys.executable, "/tmp/run_ads_mixcut_unattended.py"]
        scanner._mark_task_started = lambda ctx, product_id, owner, started_at: None
        scanner._mark_task_finished = lambda ctx, product_id, proc, finished_at, retry_backoff_minutes: None
        scanner._sync_product_task_final = lambda ctx, product_id, record_id, task, finished_at: {"status": "completed"}

        def fake_run(cmd, timeout):
            state["actual_variant_count"] += 1
            state["target_remaining_variant_count"] = max(0, 3 - int(state["actual_variant_count"]))
            if state["target_remaining_variant_count"] <= 0:
                state["pipeline_status"] = "DONE"
                state["next_action"] = "NONE"
                state["task_status"] = "DONE"
            else:
                state["pipeline_status"] = "READY_TO_CONTINUE"
                state["next_action"] = "RUN_GUARD_AGAIN"
            return {"status": "ok", "returncode": 0, "stdout": "{}", "stderr": ""}

        scanner._run = fake_run
        try:
            result = scanner._run_ads_candidate_until_settled(
                {"product_id": "PROD_LOOP", "target": 3, "goal_mode": "factory_tier", "factory_tier": 3},
                SimpleNamespace(guard_timeout=60, retry_backoff_minutes=1, max_ads_passes=5, max_no_progress_passes=2, no_ads_auto_continue=False),
                {"status": "skipped"},
                "owner",
            )
        finally:
            for name, value in originals.items():
                setattr(scanner, name, value)

        self.assertTrue(result["success"])
        self.assertEqual(result["terminal_reason"], "target_met")
        self.assertEqual(result["pass_count"], 3)
        self.assertEqual(result["final_effective_count"], 3)
        self.assertEqual(result["remaining_to_factory_target"], 0)


if __name__ == "__main__":
    unittest.main()
