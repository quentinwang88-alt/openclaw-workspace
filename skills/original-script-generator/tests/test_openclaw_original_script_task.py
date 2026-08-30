from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "openclaw_original_script_task.py"
)
SPEC = importlib.util.spec_from_file_location("openclaw_original_script_task", SCRIPT)
assert SPEC and SPEC.loader
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


class OpenClawOriginalScriptTaskTest(unittest.TestCase):
    def test_run_without_selector_defaults_to_one_task_row(self):
        command = adapter.build_runner_command(action="run", limit=1)
        self.assertEqual(command[-2:], ["--limit", "1"])
        self.assertIn("run_feishu_operation_tasks.py", command[1])

    def test_specific_resume_is_whitelisted(self):
        command = adapter.build_runner_command(
            action="resume", record_id="recAbc123"
        )
        self.assertEqual(
            command[-3:], ["--record-id", "recAbc123", "--resume-failed"]
        )

    def test_plan_requires_selector(self):
        with self.assertRaisesRegex(ValueError, "必须指定"):
            adapter.build_runner_command(action="plan")

    def test_limit_is_task_row_limit_not_unbounded_script_count(self):
        with self.assertRaisesRegex(ValueError, "最多处理 5"):
            adapter.build_runner_command(action="run", limit=6)

    def test_refresh_outfits_is_a_fixed_standalone_command(self):
        command = adapter.build_runner_command(action="refresh-outfits")
        self.assertIn("lightweight-tryon-video", command[1])
        self.assertEqual(command[-2:], ["--role", "styling"])
        with self.assertRaisesRegex(ValueError, "不接受"):
            adapter.build_runner_command(action="refresh-outfits", limit=1)

    def test_refresh_personas_is_a_fixed_standalone_command(self):
        command = adapter.build_runner_command(action="refresh-personas")
        self.assertIn("ensure_persona_template_workbench.py", command[1])
        self.assertIn("--pull-to-db", command)
        self.assertIn("--no-seed", command)
        with self.assertRaisesRegex(ValueError, "不接受"):
            adapter.build_runner_command(action="refresh-personas", limit=1)

    def test_refresh_production_config_composes_existing_refreshes(self):
        commands = adapter.build_refresh_commands("refresh-production-config")
        self.assertEqual(2, len(commands))
        self.assertIn("lightweight-tryon-video", commands[0][1])
        self.assertIn("ensure_persona_template_workbench.py", commands[1][1])

    def test_first_frame_actions_use_fixed_runner(self):
        check = adapter.build_runner_command(action="first-frame-check", limit=5)
        self.assertIn("run_first_frame_tasks.py", check[1])
        self.assertIn("--dry-run", check)
        retry = adapter.build_runner_command(
            action="first-frame-retry", record_id="recAbc123", limit=1
        )
        self.assertIn("--force", retry)
        self.assertEqual(retry[-2:], ["--limit", "1"])

    def test_first_frame_limit_is_script_row_limit(self):
        with self.assertRaisesRegex(ValueError, "最多处理 20"):
            adapter.build_runner_command(action="first-frame-run", limit=21)

    def test_product_resolution_requires_exactly_one_eligible_row(self):
        records = [SimpleNamespace(record_id="recOne"), SimpleNamespace(record_id="recTwo")]
        client = SimpleNamespace(list_records=lambda page_size: records)
        with patch.object(adapter, "_operation_client", return_value=client), patch.object(
            adapter,
            "operation_record_values",
            side_effect=[
                {"product_code": "1734482585843304442", "status": "待执行"},
                {"product_code": "1734482585843304442", "status": "已完成"},
            ],
        ):
            resolved = adapter.resolve_record_id_for_product(
                product_code="1734482585843304442", action="run"
            )
        self.assertEqual(resolved, "recOne")

    def test_product_resolution_rejects_ambiguity(self):
        records = [SimpleNamespace(record_id="recOne"), SimpleNamespace(record_id="recTwo")]
        client = SimpleNamespace(list_records=lambda page_size: records)
        with patch.object(adapter, "_operation_client", return_value=client), patch.object(
            adapter,
            "operation_record_values",
            return_value={"product_code": "1734482585843304442", "status": "待执行"},
        ):
            with self.assertRaisesRegex(ValueError, "多条"):
                adapter.resolve_record_id_for_product(
                    product_code="1734482585843304442", action="run"
                )


if __name__ == "__main__":
    unittest.main()
