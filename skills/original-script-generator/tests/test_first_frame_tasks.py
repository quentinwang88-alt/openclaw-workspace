from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_first_frame_tasks.py"
SPEC = importlib.util.spec_from_file_location("run_first_frame_tasks", SCRIPT)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class FirstFrameTasksTest(unittest.TestCase):
    def test_dry_run_selects_only_user_checked_rows(self):
        records = [
            SimpleNamespace(record_id="rec1", fields={
                "脚本ID": "S1", "产品编码": "1730000000000000000",
                "生成首帧（需勾选）": True,
            }),
            SimpleNamespace(record_id="rec2", fields={
                "脚本ID": "S2", "产品编码": "1730000000000000000",
                "生成首帧（需勾选）": False,
            }),
        ]
        client = SimpleNamespace(list_records=lambda page_size: records)
        with tempfile.TemporaryDirectory() as temp, patch.object(
            runner, "_client", return_value=client
        ):
            metrics = runner.run_tasks(
                dry_run=True, limit=5, db_path=str(Path(temp) / "first.sqlite3")
            )
        self.assertEqual(metrics["selected"], 1)
        self.assertEqual(metrics["ready"], 0)

    def test_product_filter_applies_before_limit(self):
        records = [
            SimpleNamespace(record_id="rec1", fields={
                "脚本ID": "S1", "产品编码": "1730000000000000001",
                "生成首帧（需勾选）": True,
            }),
            SimpleNamespace(record_id="rec2", fields={
                "脚本ID": "S2", "产品编码": "1730000000000000002",
                "生成首帧（需勾选）": True,
            }),
        ]
        client = SimpleNamespace(list_records=lambda page_size: records)
        with tempfile.TemporaryDirectory() as temp, patch.object(
            runner, "_client", return_value=client
        ):
            metrics = runner.run_tasks(
                product_code="1730000000000000002", dry_run=True, limit=1,
                db_path=str(Path(temp) / "first.sqlite3"),
            )
        self.assertEqual(metrics["selected"], 1)

    def test_image_subprocess_has_outer_timeout(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_runner = root / "run_pipeline.py"
            fake_runner.write_text("# test", encoding="utf-8")
            with patch.object(runner, "OPENAI_IMAGE_RUNNER", fake_runner), patch.object(
                runner.subprocess,
                "run",
                side_effect=subprocess.TimeoutExpired(cmd=["python"], timeout=60),
            ) as mocked:
                with self.assertRaisesRegex(TimeoutError, "超过 60 秒"):
                    runner._generate_image(
                        prompt="test", reference_paths=[], output_dir=root,
                        asset_id="A1", timeout_seconds=5,
                    )
            self.assertEqual(mocked.call_args.kwargs["timeout"], 60)

    def test_dry_run_does_not_recover_or_write_stale_state(self):
        records = [SimpleNamespace(record_id="rec1", fields={
            "脚本ID": "S1", "产品编码": "1730000000000000000",
            "生成首帧（需勾选）": True,
        })]
        client = SimpleNamespace(list_records=lambda page_size: records)
        with tempfile.TemporaryDirectory() as temp:
            db_path = Path(temp) / "first.sqlite3"
            storage = runner.FirstFrameStorage(db_path)
            storage.upsert_asset(
                fingerprint="fp", asset_id="A1", status="GENERATING",
            )
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute(
                    "UPDATE original_first_frame_asset SET updated_at='2000-01-01 00:00:00'"
                )
            with patch.object(runner, "_client", return_value=client):
                metrics = runner.run_tasks(
                    dry_run=True, limit=1, db_path=str(db_path),
                )
            with sqlite3.connect(str(db_path)) as conn:
                status = conn.execute(
                    "SELECT status FROM original_first_frame_asset WHERE asset_fingerprint='fp'"
                ).fetchone()[0]
        self.assertEqual(metrics["recovered_stale"], 0)
        self.assertEqual(status, "GENERATING")


if __name__ == "__main__":
    unittest.main()
