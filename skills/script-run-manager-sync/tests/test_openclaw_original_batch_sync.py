from __future__ import annotations

import importlib.util
import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "openclaw_original_batch_sync.py"
)
SPEC = importlib.util.spec_from_file_location("openclaw_original_batch_sync", SCRIPT)
assert SPEC and SPEC.loader
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


class OpenClawOriginalBatchSyncTest(unittest.TestCase):
    def test_check_is_read_only_and_uses_new_source_kind(self):
        command = adapter.build_sync_command(action="check", limit=20)
        self.assertIn("--dry-run", command)
        self.assertEqual(command[command.index("--source-kind") + 1], "original-batch")

    def test_sync_one_record_uses_a_typed_selector(self):
        command = adapter.build_sync_command(action="sync", record_id="recAbc123", limit=1)
        self.assertEqual(command[command.index("--record-id") + 1], "recAbc123")
        self.assertNotIn("--dry-run", command)

    def test_product_code_selector_is_supported(self):
        command = adapter.build_sync_command(
            action="sync", product_code="1734482585843304442", limit=5
        )
        self.assertEqual(command[command.index("--product-code") + 1], "1734482585843304442")

    def test_internal_sku_selector_is_safe_and_supported(self):
        command = adapter.build_sync_command(action="check", product_code="S260724029604")
        self.assertEqual(command[command.index("--product-code") + 1], "S260724029604")
        for code in ("S260;id", "$(id)", "--help", "a b", "x" * 31):
            with self.assertRaisesRegex(ValueError, "product_code"):
                adapter.build_sync_command(action="sync", product_code=code)

    def test_longform_check_is_read_only(self):
        command = adapter.build_longform_command(action="check", limit=20)
        self.assertIn("--dry-run", command)
        self.assertNotIn("--allow-real-submit", command)
        self.assertNotIn("--allow-external-tts", command)

    def test_longform_sync_uses_unattended_paid_branch(self):
        command = adapter.build_longform_command(
            action="sync", product_code="1737141103233042426", limit=3
        )
        self.assertIn("--allow-real-submit", command)
        self.assertIn("--allow-external-tts", command)
        self.assertEqual(
            command[command.index("--product-code") + 1],
            "1737141103233042426",
        )

    def test_rejects_invalid_identifiers_and_limit(self):
        with self.assertRaisesRegex(ValueError, "record_id"):
            adapter.build_sync_command(action="sync", record_id="wrong")
        with self.assertRaisesRegex(ValueError, "最多同步"):
            adapter.build_sync_command(action="sync", limit=21)


class UnifiedEntryPointTest(unittest.TestCase):
    """The long-form branch owns both sources; the short branch stays separate."""

    def test_longform_branch_points_at_the_unified_producer(self):
        self.assertEqual(
            "run_feishu_longform_production_tasks.py", adapter.LONGFORM_RUNNER.name,
        )
        self.assertTrue(adapter.LONGFORM_RUNNER.is_file())
        command = adapter.build_longform_command(
            action="sync", record_id="recRemake001", limit=2,
        )
        self.assertEqual(str(adapter.LONGFORM_RUNNER), command[1])
        # The same selector serves an original row and a remake row: the
        # producer classifies the source, the caller does not.
        self.assertEqual("recRemake001", command[command.index("--record-id") + 1])
        self.assertNotIn("--source-kind", command)

    def test_only_the_sync_branch_authorizes_paid_calls(self):
        for action in ("check", "sync"):
            for selector in ({"record_id": "recA1"}, {"product_code": "P123"},
                             {"limit": 3}, {}):
                with self.subTest(action=action, selector=selector):
                    command = adapter.build_longform_command(action=action, **selector)
                    if action == "sync":
                        self.assertIn("--allow-real-submit", command)
                        self.assertIn("--allow-external-tts", command)
                    else:
                        self.assertIn("--dry-run", command)
                        self.assertNotIn("--allow-real-submit", command)
                        self.assertNotIn("--allow-external-tts", command)

    def test_check_branch_is_read_only_in_both_run_commands(self):
        longform = adapter.build_longform_command(action="check", limit=20)
        shortform = adapter.build_sync_command(action="check", limit=20)
        self.assertIn("--dry-run", longform)
        self.assertIn("--dry-run", shortform)
        for command in (longform, shortform):
            self.assertNotIn("--allow-real-submit", command)
            self.assertNotIn("--allow-external-tts", command)

    def test_main_runs_the_unified_longform_branch_first_then_short_sync(self):
        calls = []

        def fake_run(command, cwd=None, check=False):
            calls.append((list(command), cwd))
            return mock.Mock(returncode=0)

        stdout = io.StringIO()
        with mock.patch.object(adapter.subprocess, "run", side_effect=fake_run):
            with redirect_stdout(stdout):
                code = adapter.main(["sync", "--record-id", "recAbc123", "--limit", "4"])
        self.assertEqual(0, code)
        self.assertEqual(2, len(calls))
        longform_command, longform_cwd = calls[0]
        shortform_command, shortform_cwd = calls[1]
        # 1/2 unified long-form production (original + remake)
        self.assertEqual(str(adapter.LONGFORM_RUNNER), longform_command[1])
        self.assertIn("--allow-real-submit", longform_command)
        self.assertIn("--allow-external-tts", longform_command)
        self.assertEqual("recAbc123", longform_command[longform_command.index("--record-id") + 1])
        # 2/2 the 15s short-video synchronizer still runs on its own.
        self.assertEqual(str(adapter.RUNNER), shortform_command[1])
        self.assertEqual(
            "original-batch", shortform_command[shortform_command.index("--source-kind") + 1],
        )
        self.assertNotIn("--allow-real-submit", shortform_command)
        self.assertEqual(
            str(adapter.LONGFORM_RUNNER.parent.parent), longform_cwd,
        )
        self.assertEqual(str(adapter.SKILL_ROOT), shortform_cwd)
        output = stdout.getvalue()
        self.assertIn("1/2 统一长视频生产（原创 + 复刻）", output)
        self.assertIn("2/2 15秒短视频同步", output)
        self.assertLess(
            output.index("1/2 统一长视频生产（原创 + 复刻）"),
            output.index("2/2 15秒短视频同步"),
        )

    def test_check_main_stays_read_only_on_both_branches(self):
        calls = []

        def fake_run(command, cwd=None, check=False):
            calls.append(list(command))
            return mock.Mock(returncode=0)

        with mock.patch.object(adapter.subprocess, "run", side_effect=fake_run):
            with redirect_stdout(io.StringIO()):
                code = adapter.main(["check", "--limit", "20"])
        self.assertEqual(0, code)
        for command in calls:
            self.assertIn("--dry-run", command)
            self.assertNotIn("--allow-real-submit", command)
            self.assertNotIn("--allow-external-tts", command)

    def test_whitelist_validation_is_shared_by_both_branches(self):
        for builder in (adapter.build_sync_command, adapter.build_longform_command):
            with self.subTest(builder=builder.__name__):
                with self.assertRaisesRegex(ValueError, "record_id"):
                    builder(action="sync", record_id="wrong")
                with self.assertRaisesRegex(ValueError, "product_code"):
                    builder(action="sync", product_code="$(id)")
                with self.assertRaisesRegex(ValueError, "最多"):
                    builder(action="sync", limit=21)
                with self.assertRaisesRegex(ValueError, "未知 action"):
                    builder(action="publish", limit=1)


if __name__ == "__main__":
    unittest.main()
