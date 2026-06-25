from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.result import Result
from auto_mixcut.cli import _top_up_snapshot
from auto_mixcut.skills.ai_supplement_gateway_skill import AISupplementGatewaySkill
from auto_mixcut.skills.final_video_qc_async_skill import FinalVideoQCAsyncSkill
from auto_mixcut.skills.final_video_qc_skill import _mark_product_mismatch_segments
from auto_mixcut.skills.feishu_review_skill import _select_anchor_canonical_record
from auto_mixcut.skills.mixcut_state_machine_skill import decide_mixcut_state, guard_start_status
from auto_mixcut.skills.pipeline_run_skill import PipelineRunSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_plan_skill import _select_segments, _usable_existing_outputs
from auto_mixcut.skills.segment_prompt_factory_skill import _ensure_prompt_package_table
from auto_mixcut.skills.usage_counter_skill import count_good_rendered_outputs
from scripts.run_mixcut_guard import _compute_missing_effective_roles, _status_after_top_up
from scripts.sync_prompt_package_workbench_from_tasks import _voc_slot_plans


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
            ("SP_READY", "created", "", "", "rec_ready"),
            ("SP_RETRY", "failed", "", "real_submit_disabled", ""),
            ("SP_SUBMITTED", "submitted", "", "", ""),
            ("SP_IMPORTED", "imported", "ASSET_AI", "", ""),
            ("SP_CONSUMED", "consumed", "", "", ""),
        ]
        for prompt_id, status, asset_id, failure, feishu_record_id in rows:
            self.ctx.repo.upsert(
                "segment_prompt_packages",
                "segment_prompt_id",
                {
                    "segment_prompt_id": prompt_id,
                    "product_id": product_id,
                    "package_status": status,
                    "generated_asset_id": asset_id,
                    "failure_reason": failure,
                    "feishu_record_id": feishu_record_id,
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

    def test_strict_good_output_excludes_failed_segments(self):
        product_id = "PROD_STRICT_OUTPUT"
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_FAILED_STRICT",
                "asset_id": "ASSET_FAILED_STRICT",
                "product_id": product_id,
                "segment_status": "ai_stage_failed",
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_WITH_FAILED_SEG",
                "batch_id": "BATCH_STRICT",
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
                "output_id": "OUT_WITH_FAILED_SEG",
                "segment_id": "SEG_FAILED_STRICT",
                "asset_id": "ASSET_FAILED_STRICT",
                "slot_index": 1,
                "role_used": "hero",
            },
        )

        self.assertEqual(count_good_rendered_outputs(self.ctx, product_id, strict_segments=False), 1)
        self.assertEqual(count_good_rendered_outputs(self.ctx, product_id, strict_segments=True), 0)

    def test_render_plan_selection_excludes_failed_segments(self):
        product_id = "PROD_RENDER_ELIGIBLE"
        for segment_id, status in [
            ("SEG_FAILED_RENDER_POOL", "ai_stage_failed"),
            ("SEG_GOOD_RENDER_POOL", "qc_passed"),
        ]:
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_{segment_id}",
                    "product_id": product_id,
                    "source_type": "self_shot",
                    "source_trust_level": "high",
                    "product_match_status": "trusted_by_source",
                    "product_binding_type": "exact_sku",
                    "segment_status": status,
                    "effective_roles_json": ["hero"],
                    "duration_ms": 3000,
                },
            )

        result = _select_segments(
            self.ctx,
            product_id,
            [{"role": "hero", "duration_ms": 3000}],
            batch_state={"segments": set(), "segment_counts": {}, "core_segment_counts": {}, "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}, "template_counts": {}},
        )

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data[0]["segment_id"], "SEG_GOOD_RENDER_POOL")

    def test_ads_fast_existing_outputs_use_strict_segment_status(self):
        product_id = "PROD_STRICT_EXISTING"
        previous = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        try:
            for segment_id, status in [
                ("SEG_CLEAN_EXISTING", "qc_passed"),
                ("SEG_FAILED_EXISTING", "ai_stage_failed"),
            ]:
                self.ctx.repo.upsert(
                    "segments",
                    "segment_id",
                    {
                        "segment_id": segment_id,
                        "asset_id": f"ASSET_{segment_id}",
                        "product_id": product_id,
                        "segment_status": status,
                    },
                )
            for output_id, segment_id in [
                ("OUT_CLEAN_EXISTING", "SEG_CLEAN_EXISTING"),
                ("OUT_FAILED_EXISTING", "SEG_FAILED_EXISTING"),
            ]:
                self.ctx.repo.upsert(
                    "outputs",
                    "output_id",
                    {
                        "output_id": output_id,
                        "batch_id": "BATCH_STRICT_EXISTING",
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
                        "output_id": output_id,
                        "segment_id": segment_id,
                        "asset_id": f"ASSET_{segment_id}",
                        "slot_index": 1,
                        "role_used": "hero",
                    },
                )

            output_ids = [output["output_id"] for output in _usable_existing_outputs(self.ctx, product_id)]

            self.assertEqual(output_ids, ["OUT_CLEAN_EXISTING"])
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous

    def test_ads_fast_top_up_snapshot_uses_strict_segment_status(self):
        product_id = "PROD_STRICT_TOP_UP"
        previous = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        try:
            RDSRepositorySkill(self.ctx).create_product_task(product_id, "Hair Clip", "TH", "hair_accessories", 2)
            for segment_id, status in [
                ("SEG_CLEAN_TOP_UP", "qc_passed"),
                ("SEG_FAILED_TOP_UP", "ai_stage_failed"),
            ]:
                self.ctx.repo.upsert(
                    "segments",
                    "segment_id",
                    {
                        "segment_id": segment_id,
                        "asset_id": f"ASSET_{segment_id}",
                        "product_id": product_id,
                        "segment_status": status,
                    },
                )
            for output_id, segment_id in [
                ("OUT_CLEAN_TOP_UP", "SEG_CLEAN_TOP_UP"),
                ("OUT_FAILED_TOP_UP", "SEG_FAILED_TOP_UP"),
            ]:
                self.ctx.repo.upsert(
                    "outputs",
                    "output_id",
                    {
                        "output_id": output_id,
                        "batch_id": "BATCH_STRICT_TOP_UP",
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
                        "output_id": output_id,
                        "segment_id": segment_id,
                        "asset_id": f"ASSET_{segment_id}",
                        "slot_index": 1,
                        "role_used": "hero",
                    },
                )

            snapshot = _top_up_snapshot(self.ctx, product_id, count=2, refresh_capacity=False)

            self.assertEqual(snapshot["effective_outputs"], 1)
            self.assertEqual(snapshot["target_remaining_variant_count"], 1)
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous

    def test_guard_effective_role_limit_zero_skips_batch(self):
        product_id = "PROD_ROLE_LIMIT_ZERO"
        previous = os.environ.get("AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_LIMIT")
        os.environ["AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_LIMIT"] = "0"
        try:
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": "SEG_ROLE_LIMIT_ZERO",
                    "asset_id": "ASSET_ROLE_LIMIT_ZERO",
                    "product_id": product_id,
                    "source_type": "competitor",
                    "segment_status": "created",
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": "SEG_ROLE_LIMIT_ZERO",
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "risk_level": "low",
                },
            )

            res = _compute_missing_effective_roles(self.ctx, product_id, ["competitor"])

            self.assertTrue(res.success, res.to_dict())
            self.assertEqual(res.data["attempted_count"], 0)
            self.assertEqual(res.data["results"][0]["reason"], "outside_effective_role_batch")
            segment = self.ctx.repo.get("segments", "segment_id", "SEG_ROLE_LIMIT_ZERO")
            self.assertFalse(segment.get("effective_roles_updated_at"))
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_LIMIT", None)
            else:
                os.environ["AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_LIMIT"] = previous

    def test_guard_status_uses_final_remaining_over_stop_reason(self):
        product_id = "PROD_STOP_REASON_STRICT"
        previous = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        try:
            RDSRepositorySkill(self.ctx).create_product_task(product_id, "Hair Clip", "TH", "hair_accessories", 2)
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": "OUT_STOP_REASON_ONE",
                    "batch_id": "BATCH_STOP_REASON",
                    "product_id": product_id,
                    "variant_no": 1,
                    "template_id": "TEMPLATE_TEST",
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                },
            )
            top_up = Result.ok(
                {
                    "stop_reason": "target_filled",
                    "batch_ids": ["BATCH_STOP_REASON"],
                    "final": {"target_remaining_variant_count": 1},
                }
            )

            status = _status_after_top_up(self.ctx, product_id, 2, top_up)

            self.assertEqual(status["pipeline_status"], "READY_TO_CONTINUE")
            self.assertEqual(status["next_action"], "RUN_GUARD_AGAIN")
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous

    def test_voc_action_proof_slot_plans_avoid_static_display(self):
        plans = _voc_slot_plans(
            {
                "insight_id": "selling_hold_quality",
                "hook_intent": "contrast_reveal",
                "required_action_zh": "夹住侧边碎发后轻转头展示更利落",
            },
            "hair_accessories",
        )

        self.assertEqual(plans[0], ("tryon_result", "A", "hero"))
        segment_types = [segment_type for segment_type, _grade, _role in plans]
        self.assertNotIn("product_display", segment_types)
        self.assertNotIn("before_go_out", segment_types)

    def test_voc_action_proof_slot_plans_respect_category_contract(self):
        plans = _voc_slot_plans(
            {
                "insight_id": "selling_appearance_cute_color",
                "hook_intent": "tryon_result",
                "required_action_zh": "手拿耳饰靠近耳侧后轻转头展示效果",
            },
            "earrings",
        )

        segment_types = [segment_type for segment_type, _grade, _role in plans]
        self.assertEqual(segment_types, ["mirror_routine"])
        self.assertNotIn("tryon_result", segment_types)
        self.assertNotIn("before_go_out", segment_types)

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

    def test_ai_supplement_gateway_prioritizes_hero_for_first_slot_bottleneck(self):
        product_id = "PROD_AI_HERO_BUDGET"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Hair Clip", "TH", "hair_accessories", 2)
        task = self.ctx.repo.list_where("content_tasks", "product_id=?", (product_id,))[0]
        self.ctx.repo.update(
            "content_tasks",
            "task_id",
            task["task_id"],
            {
                "target_remaining_variant_count": 2,
                "first_slot_remaining_capacity": 0,
                "current_bottleneck": "已进入复用模式",
            },
        )
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        for prompt_id, role in [("SP_HERO_READY", "hero"), ("SP_DETAIL_READY", "detail")]:
            self.ctx.repo.upsert(
                "segment_prompt_packages",
                "segment_prompt_id",
                {
                    "segment_prompt_id": prompt_id,
                    "product_id": product_id,
                    "package_status": "created",
                    "feishu_record_id": f"rec_{prompt_id}",
                    "slot_role": role,
                },
            )

        gateway = AISupplementGatewaySkill(self.ctx)
        state = gateway.package_state(product_id)
        budget = gateway.submit_budget(product_id, remaining_count=2, configured_limit=5)

        self.assertEqual(state["ready_to_submit_count"], 2)
        self.assertEqual(state["role_counts"]["hero"]["ready_to_submit_count"], 1)
        self.assertEqual(state["role_counts"]["detail"]["ready_to_submit_count"], 1)
        self.assertEqual(budget["priority_role"], "hero")
        self.assertEqual(budget["role_ready_to_submit_count"], 1)
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
