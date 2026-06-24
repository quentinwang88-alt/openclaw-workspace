from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.result import Result
from auto_mixcut.skills.ai_supplement_gateway_skill import AISupplementGatewaySkill
from auto_mixcut.skills.final_video_qc_async_skill import FinalVideoQCAsyncSkill
from auto_mixcut.skills.final_video_qc_skill import _mark_product_mismatch_segments
from auto_mixcut.skills.feishu_review_skill import _select_anchor_canonical_record
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

    def test_waiting_ai_return_switches_back_to_guard_when_capacity_is_enough(self):
        decision = decide_mixcut_state(
            {
                "pipeline_status": "WAITING_AI_RETURN",
                "next_action": "WAIT_AI_SEGMENT_RETURN",
                "target_remaining_variant_count": 3,
                "material_pool_extra_capacity": 5,
            }
        )

        self.assertFalse(decision.is_waiting_ai_return)
        self.assertEqual(decision.scanner_mode, "guard")
        self.assertTrue(decision.should_scan_from_rds)

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
        self.assertEqual(state["active_package_count"], 2)
        self.assertEqual(budget["submit_limit"], 2)

    def test_ai_supplement_gateway_treats_rendered_import_as_consumed(self):
        product_id = "PROD_AI_USED_IMPORT"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Earring", "MY", "earrings", 5)
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        self.ctx.repo.upsert(
            "segment_prompt_packages",
            "segment_prompt_id",
            {
                "segment_prompt_id": "SP_IMPORTED_USED",
                "product_id": product_id,
                "package_status": "imported",
                "generated_asset_id": "ASSET_IMPORTED_USED",
                "generated_segment_id": "SEG_IMPORTED_USED",
            },
        )
        self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {
                "asset_id": "ASSET_IMPORTED_USED",
                "product_id": product_id,
                "source_type": "ai_generated",
                "prompt_package_id": "SP_IMPORTED_USED",
            },
        )
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_IMPORTED_USED",
                "asset_id": "ASSET_IMPORTED_USED",
                "product_id": product_id,
                "source_type": "ai_generated",
                "prompt_package_id": "SP_IMPORTED_USED",
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_IMPORTED_USED",
                "batch_id": "BATCH_IMPORTED_USED",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "TEMPLATE_TEST",
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
            },
        )
        self.ctx.repo.insert(
            "output_segments",
            {
                "output_id": "OUT_IMPORTED_USED",
                "segment_id": "SEG_IMPORTED_USED",
                "asset_id": "ASSET_IMPORTED_USED",
                "slot_index": 1,
                "role_used": "hero",
            },
        )

        state = AISupplementGatewaySkill(self.ctx).package_state(product_id)
        budget = AISupplementGatewaySkill(self.ctx).submit_budget(product_id, remaining_count=5, configured_limit=5)

        self.assertEqual(state["imported_package_count"], 0)
        self.assertEqual(state["consumed_package_count"], 1)
        self.assertEqual(state["active_package_count"], 0)
        self.assertEqual(budget["submit_limit"], 5)

    def test_ai_supplement_gateway_treats_stale_generating_as_recoverable(self):
        product_id = "PROD_AI_STALE_GATEWAY"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Earring", "MY", "earrings", 1)
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        old = (datetime.utcnow() - timedelta(hours=8)).isoformat(timespec="seconds")
        self.ctx.repo.upsert(
            "segment_prompt_packages",
            "segment_prompt_id",
            {
                "segment_prompt_id": "SP_STALE",
                "product_id": product_id,
                "package_status": "generating",
                "updated_at": old,
            },
        )

        gateway = AISupplementGatewaySkill(self.ctx)
        state = gateway.package_state(product_id)
        budget = gateway.submit_budget(product_id, remaining_count=1, configured_limit=5)

        self.assertEqual(state["inflight_count"], 0)
        self.assertEqual(state["recoverable_failed_count"], 1)
        self.assertEqual(state["stale_inflight_count"], 1)
        self.assertEqual(state["active_package_count"], 0)
        self.assertEqual(budget["submit_limit"], 1)

    def test_final_qc_product_mismatch_marks_ai_segments_suspect(self):
        output_id = "OUT_MISMATCH"
        segment_id = "SEG_MISMATCH_AI"
        self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {"asset_id": "ASSET_MISMATCH_AI", "product_id": "PROD_MISMATCH", "source_type": "ai_generated", "source_trust_level": "medium"},
        )
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": segment_id,
                "asset_id": "ASSET_MISMATCH_AI",
                "product_id": "PROD_MISMATCH",
                "source_type": "ai_generated",
                "effective_roles_json": ["hero"],
            },
        )
        self.ctx.repo.insert("output_segments", {"output_id": output_id, "slot_index": 1, "segment_id": segment_id, "asset_id": "ASSET_MISMATCH_AI", "role_used": "hero"})

        result = _mark_product_mismatch_segments(
            self.ctx,
            output_id,
            {"product_match_issue": True, "fail_reasons": ["商品展示不一致"]},
        )

        row = self.ctx.repo.get("segments", "segment_id", segment_id)
        self.assertEqual(result["updated_count"], 1)
        self.assertEqual(row["product_mismatch_suspect"], 1)
        self.assertEqual(row["effective_roles_json"], [])

    def test_anchor_sync_prefers_confirmed_anchor_record_over_image_only_row(self):
        image_only = SimpleNamespace(
            record_id="rec_image",
            fields={"商品ID": "P1", "商品主图": [{"file_token": "tok"}], "人工确认状态": "待确认"},
        )
        confirmed = SimpleNamespace(
            record_id="rec_anchor",
            fields={"商品ID": "P1", "AI生成锚点卡": "{}", "人工确认状态": "已确认"},
        )

        selected = _select_anchor_canonical_record([image_only, confirmed], [])

        self.assertEqual(selected.record_id, "rec_anchor")

    def test_final_video_qc_async_dispatch_is_non_blocking_in_mock_context(self):
        product_id = "PROD_FINAL_QC_ASYNC"
        batch_id = "BATCH_FINAL_QC_ASYNC"
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {
                "batch_id": batch_id,
                "product_id": product_id,
                "batch_status": "rendered",
                "final_qc_async_status": "",
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_FINAL_QC_ASYNC",
                "batch_id": batch_id,
                "product_id": product_id,
                "machine_quality_status": "passed",
                "final_qc_json": None,
            },
        )

        res = FinalVideoQCAsyncSkill(self.ctx).dispatch_batch(batch_id)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["status"], "skipped")
        self.assertEqual(res.data["reason"], "mock_ffmpeg_context")
        self.assertEqual(res.data["pending_count"], 1)


if __name__ == "__main__":
    unittest.main()
