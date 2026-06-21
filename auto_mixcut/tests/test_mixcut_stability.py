from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.result import Result
from auto_mixcut.skills.ai_supplement_gateway_skill import AISupplementGatewaySkill
from auto_mixcut.skills.mixcut_state_machine_skill import decide_mixcut_state, guard_start_status
from auto_mixcut.skills.pipeline_run_skill import PipelineRunSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.segment_prompt_factory_skill import _ensure_prompt_package_table


class MixcutStabilityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
        os.environ["AUTO_MIXCUT_DB"] = str(root / "db.sqlite")
        os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(root / "oss")
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
        os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(root / "tmp")
        os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "1"
        os.environ["AUTO_MIXCUT_MOCK_LLM"] = "1"
        os.environ["AUTO_MIXCUT_FEISHU_ENABLED"] = "0"
        self.ctx = build_context()
        init = RDSRepositorySkill(self.ctx).init_db()
        self.assertTrue(init.success, init.to_dict())

    def tearDown(self):
        self.tmp.cleanup()

    def test_pipeline_step_log_can_attach_entity_context(self):
        logger = PipelineRunSkill(self.ctx)

        step_id = logger.start_step("PROD_PIPE_LOG", "segment.ai_anchor_check", entity_type="segment", entity_id="SEG_A1")
        logger.finish_step(step_id, Result.ok({"status": "completed"}))

        rows = self.ctx.repo.list_where("pipeline_step_runs", "step_run_id=?", (step_id,))
        self.assertEqual(rows[0]["detail_json"]["entity_type"], "segment")
        self.assertEqual(rows[0]["detail_json"]["entity_id"], "SEG_A1")

    def test_mixcut_state_machine_centralizes_scanner_state(self):
        waiting = decide_mixcut_state({"pipeline_status": "WAITING_AI_RETURN", "next_action": "WAIT_AI_SEGMENT_RETURN"})
        self.assertEqual(waiting.display_state, "等待AI回流")
        self.assertEqual(waiting.scanner_mode, "ai_return_heartbeat")

        done = decide_mixcut_state({"pipeline_status": "DONE"})
        self.assertTrue(done.is_done)
        self.assertEqual(done.display_state, "已完成")

        self.assertEqual(
            guard_start_status({"ai_supplement_status": "created", "remaining_count": 2, "first_slot_remaining_capacity": 0}),
            ("WAITING_AI_RETURN", "CHECK_AI_RETURN_THEN_CONTINUE"),
        )

    def test_ai_supplement_gateway_counts_active_packages_only(self):
        product_id = "PROD_AI_GATEWAY"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Earring", "MY", "earrings", 5)
        task = self.ctx.repo.list_where("content_tasks", "product_id=?", (product_id,))[0]
        self.ctx.repo.update("content_tasks", "task_id", task["task_id"], {"target_remaining_variant_count": 5})
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        rows = [
            ("SP_READY", "created", "", ""),
            ("SP_RETRY", "failed", "", "real_submit_disabled"),
            ("SP_SUBMITTED", "submitted", "", ""),
            ("SP_IMPORTED", "imported", "ASSET_AI", ""),
            ("SP_CONSUMED", "consumed", "", ""),
        ]
        for prompt_id, status, asset_id, failure in rows:
            self.ctx.repo.upsert(
                "segment_prompt_packages",
                "segment_prompt_id",
                {
                    "segment_prompt_id": prompt_id,
                    "product_id": product_id,
                    "package_status": status,
                    "generated_asset_id": asset_id,
                    "failure_reason": failure,
                },
            )

        gateway = AISupplementGatewaySkill(self.ctx)
        state = gateway.package_state(product_id)
        budget = gateway.submit_budget(product_id, remaining_count=5, configured_limit=5)

        self.assertEqual(state["ready_to_submit_count"], 1)
        self.assertEqual(state["recoverable_failed_count"], 1)
        self.assertEqual(state["inflight_count"], 1)
        self.assertEqual(state["imported_package_count"], 1)
        self.assertEqual(state["consumed_package_count"], 1)
        self.assertEqual(state["active_package_count"], 4)
        self.assertEqual(budget["submit_limit"], 2)


if __name__ == "__main__":
    unittest.main()
