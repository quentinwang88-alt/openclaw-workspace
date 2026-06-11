from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from auto_mixcut.agent.ai_diversity_budget import AIDiversityBudget
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.result import Result
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from scripts.sync_prompt_package_workbench_from_tasks import _gap_slots
from auto_mixcut.skills.readiness_check_skill import ReadinessCheckSkill, _calibrate_render_plan_capacity
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill, _choose_template, _load_templates, _passes_first_slot_floor, _segment_score, _select_segments
from auto_mixcut.skills.render_skill import _actual_generated_count, _bgm_audio_filter, _default_bgm_plan, _drawtext_filter, _subtitle_plan
from auto_mixcut.skills.batch_control_skill import BatchControlSkill
from auto_mixcut.skills.batch_report_skill import BatchReportSkill
from auto_mixcut.skills.capacity_counter_skill import CapacityCounterSkill
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill
from auto_mixcut.skills.segment_fingerprint_skill import SegmentFingerprintSkill
from auto_mixcut.skills.usage_counter_skill import refresh_segment_usage
from auto_mixcut.skills.feishu_review_skill import _output_cleanup_reason
from auto_mixcut.skills.final_video_qc_skill import _normalize_final_qc_response
from auto_mixcut.skills.pipeline_run_skill import PipelineRunSkill
from auto_mixcut.skills.ai_supplement_workbench_skill import _infer_capacity_gap_text, _supplement_state_summary
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill
from scripts.run_mixcut_guard import _compute_missing_effective_roles, _ensure_anchor_confirmed, _stale_repair_source_types, _stale_segment_summary, run_guard_pass
from scripts.run_mixcut_guard_loop import _dynamic_round_timeout, _parse_guard_stdout, _status_action_from_result
from auto_mixcut.cli import _cap_round_count, _create_render_plans_with_timeout, _skip_final_video_qc, _top_up_snapshot


def _restore_env(key: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value


class AIFactoryStrategyTest(unittest.TestCase):
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
        self.ctx = build_context()
        init = RDSRepositorySkill(self.ctx).init_db()
        self.assertTrue(init.success, init.to_dict())

    def tearDown(self):
        self.tmp.cleanup()

    def test_template_choice_rejects_unmatched_category_templates(self):
        templates = [template for template in _load_templates(self.ctx) if template.template_id.startswith("AI_HAIR_")]
        self.assertTrue(templates)

        choice = _choose_template(
            self.ctx,
            {"category": "womens_top"},
            templates,
            {"template_counts": {}},
            variant=1,
        )

        self.assertIsNone(choice["template"])
        self.assertEqual(choice["debug"]["skip_reason"], "no_category_matching_template")

    def test_readiness_skips_heavy_render_plan_estimate_for_large_pool(self):
        product_id = "VN_LARGE_POOL_ESTIMATE"
        segments = []
        for idx in range(81):
            segment_id = f"SEG_LARGE_EST_{idx:02d}"
            segments.append({"segment_id": segment_id})
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "secondary_roles_json": [],
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                },
            )

        estimate = _calibrate_render_plan_capacity(self.ctx, product_id, segments, 5)

        self.assertEqual(estimate["planned_count"], 5)
        self.assertEqual(estimate["estimate_mode"], "lightweight_skipped_heavy_render_plan")

    def test_ai_friendly_templates_are_selected_by_category_without_legacy_prefix(self):
        product_id = "VN_AI_TEMPLATE_ORDER"
        create = RDSRepositorySkill(self.ctx).create_product_task(product_id, "Clip", "VN", "hair_accessories", 5)
        self.assertTrue(create.success, create.to_dict())
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 5, "material_tier": "tier_3_full", "material_status": "ready"})
        for idx in range(36):
            segment_id = f"SEG_AI_TPL_{idx:02d}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_AI_TPL_{idx:02d}",
                    "product_id": product_id,
                    "source_type": "ai_generated" if idx % 3 == 0 else "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "anchor_pass",
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "secondary_roles_json": ["detail", "result", "scene", "ending"],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )
        res = RenderPlanSkill(self.ctx).create_plans(product_id, 5)
        self.assertTrue(res.success, res.to_dict())
        plans = self.ctx.repo.list_where("render_plans", "batch_id=? ORDER BY variant_no", (res.data["batch_id"],))
        template_ids = [p["template_id"] for p in plans]
        self.assertTrue(all(tid.startswith("AI_") for tid in template_ids[:4]), template_ids)
        self.assertTrue(any(tid.startswith("AI_HAIR_") for tid in template_ids[:3]), template_ids)
        self.assertNotEqual(template_ids[:4], ["GENERAL_BALANCED_15S", "RESULT_FIRST_15S", "DETAIL_HOOK_15S", "CLEAN_PRODUCT_PROOF_15S"])
        self.assertTrue({16000, 20000}.intersection({p["planned_duration_ms"] for p in plans}), template_ids)
        self.assertEqual(plans[0]["plan_json"]["template_selection"]["strategy"], "category_template_score_rotation")

    def test_pipeline_step_log_auto_creates_table(self):
        logger = PipelineRunSkill(self.ctx)

        step_id = logger.start_step("PROD_PIPE_LOG", "frames")
        logger.finish_step(step_id, Result.ok({"count": 1}))

        rows = self.ctx.repo.list_where("pipeline_step_runs", "step_run_id=?", (step_id,))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "success")
        self.assertEqual(rows[0]["detail_json"], {"count": 1})

    def test_guard_auto_confirms_anchor_before_upload_gate(self):
        product_id = "PROD_GUARD_ANCHOR"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Cat clip", "TH", "hair_accessories", 3)

        res = _ensure_anchor_confirmed(self.ctx, product_id)

        self.assertTrue(res.success, res.to_dict())
        product = self.ctx.repo.get("products", "product_id", product_id)
        self.assertEqual(product["anchor_status"], "confirmed")

    def test_guard_detects_stale_segments_missing_downstream_processing(self):
        old_require_phash = os.environ.get("AUTO_MIXCUT_GUARD_REQUIRE_PHASH")
        os.environ["AUTO_MIXCUT_GUARD_REQUIRE_PHASH"] = "1"
        self.addCleanup(_restore_env, "AUTO_MIXCUT_GUARD_REQUIRE_PHASH", old_require_phash)
        product_id = "PROD_GUARD_STALE"
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_GUARD_STALE",
                "asset_id": "ASSET_GUARD_STALE",
                "product_id": product_id,
                "source_type": "competitor",
                "effective_roles_json": None,
                "visual_phash": None,
            },
        )
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))

        summary = _stale_segment_summary(self.ctx, segments)

        self.assertEqual(summary["stale_count"], 1)
        self.assertEqual(_stale_repair_source_types(self.ctx, segments), ["competitor"])
        reasons = summary["sample"][0]["reasons"]
        self.assertIn("frames_missing", reasons)
        self.assertIn("visual_phash_missing", reasons)
        self.assertIn("tag_missing", reasons)
        self.assertIn("effective_roles_missing", reasons)

    def test_guard_repairs_stale_non_ai_sources(self):
        product_id = "PROD_GUARD_COMPETITOR_STALE"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Competitor stale", "TH", "womens_outerwear", 1)
        self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {
                "asset_id": "ASSET_COMPETITOR_STALE",
                "product_id": product_id,
                "source_type": "competitor",
                "asset_status": "ready",
            },
        )
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_COMPETITOR_STALE",
                "asset_id": "ASSET_COMPETITOR_STALE",
                "product_id": product_id,
                "source_type": "competitor",
                "visual_phash": None,
            },
        )

        with patch("scripts.run_mixcut_guard._run_incremental_postprocess", return_value=Result.ok({"repaired": True})) as repair:
            with patch("scripts.run_mixcut_guard._top_up", return_value=Result.ok({"stop_reason": "render_plan_empty", "batch_ids": [], "final": {"target_remaining_variant_count": 1}})):
                res = run_guard_pass(self.ctx, product_id, target=1, process_uploads=False)

        self.assertTrue(res.success, res.to_dict())
        repair.assert_called_once()
        self.assertEqual(repair.call_args.kwargs["source_types"], ["competitor"])

    def test_guard_bootstraps_missing_task_from_feishu_product_table(self):
        product_id = "PROD_FEISHU_BOOTSTRAP"
        feishu_row = {
            "product_name": "Feishu product",
            "market": "TH",
            "category": "womens_tops",
            "requested_variant_count": 5,
            "shop_id": "SHOP_TH_1",
            "priority": "normal",
            "record_id": "rec_feishu_bootstrap",
        }

        with patch("scripts.run_mixcut_guard._fetch_product_task_from_feishu", return_value=Result.ok(feishu_row)):
            with patch("scripts.run_mixcut_guard._ensure_anchor_confirmed", return_value=Result.ok({"anchor_status": "confirmed"})):
                res = run_guard_pass(self.ctx, product_id, process_uploads=False)

        self.assertTrue(res.success, res.to_dict())
        product = self.ctx.repo.get("products", "product_id", product_id)
        task = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))[0]
        self.assertEqual(product["product_name"], "Feishu product")
        self.assertEqual(product["market"], "TH")
        self.assertEqual(product["category"], "womens_tops")
        self.assertEqual(product["shop_id"], "SHOP_TH_1")
        self.assertEqual(task["requested_variant_count"], 5)
        self.assertEqual(task["created_by"], "feishu_product_task")

    def test_ai_tag_submit_counts_only_missing_tags(self):
        product_id = "PROD_TAG_CACHE"
        for idx in range(2):
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": f"SEG_TAG_CACHE_{idx}",
                    "asset_id": f"ASSET_TAG_CACHE_{idx}",
                    "product_id": product_id,
                    "source_type": "creator_authorized",
                },
            )
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": "SEG_TAG_CACHE_0",
                "tag_source": "test",
                "primary_shot_role": "detail",
                "secondary_roles_json": ["scene"],
                "mixcut_usability": "yes",
                "risk_level": "low",
            },
        )

        res = AITaggingSkill(self.ctx).submit_batch(product_id, source_types=["creator_authorized"])

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["total_segments"], 1)
        batch = self.ctx.repo.get("ai_batches", "ai_batch_id", res.data["ai_batch_id"])
        self.assertEqual(batch["total_segments"], 1)

    def test_guard_recomputes_roles_after_missing_tag_backfill(self):
        product_id = "PROD_RETAG_ROLE_REFRESH"
        self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {
                "asset_id": "ASSET_RETAG_ROLE_REFRESH",
                "product_id": product_id,
                "source_type": "creator_authorized",
                "source_trust_level": "high",
                "product_binding_type": "exact_sku",
            },
        )
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_RETAG_ROLE_REFRESH",
                "asset_id": "ASSET_RETAG_ROLE_REFRESH",
                "product_id": product_id,
                "source_type": "creator_authorized",
                "source_trust_level": "high",
                "product_binding_type": "exact_sku",
                "product_match_status": "anchor_pass",
                "effective_roles_json": ["scene", "ending"],
                "effective_roles_updated_at": "2026-06-10T00:00:00",
            },
        )
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": "SEG_RETAG_ROLE_REFRESH",
                "tag_source": "test",
                "primary_shot_role": "hero",
                "secondary_roles_json": ["detail", "result"],
                "product_visibility": "high",
                "hook_strength": "strong",
                "mixcut_usability": "yes",
                "risk_level": "low",
                "confidence": "high",
            },
        )

        res = _compute_missing_effective_roles(self.ctx, product_id, ["creator_authorized"], force_segment_ids=["SEG_RETAG_ROLE_REFRESH"])

        self.assertTrue(res.success, res.to_dict())
        segment = self.ctx.repo.get("segments", "segment_id", "SEG_RETAG_ROLE_REFRESH")
        self.assertIn("hero", segment["effective_roles_json"])
        self.assertIn("result", segment["effective_roles_json"])

    def test_guard_target_filled_does_not_full_rerun_for_stale_segments(self):
        product_id = "PROD_GUARD_FILLED"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Filled", "TH", "womens_outerwear", 1)
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {"batch_id": "BATCH_GUARD_FILLED", "product_id": product_id, "requested_count": 1, "batch_status": "rendered"},
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_GUARD_FILLED",
                "batch_id": "BATCH_GUARD_FILLED",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
                "human_quality_status": "pending",
            },
        )
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_GUARD_FILLED_STALE",
                "asset_id": "ASSET_GUARD_FILLED_STALE",
                "product_id": product_id,
                "source_type": "competitor",
                "visual_phash": None,
            },
        )

        with patch("scripts.run_mixcut_guard.AutoMixcutOrchestratorAgent.run_product", side_effect=AssertionError("full rerun should not happen")):
            res = run_guard_pass(self.ctx, product_id, target=1, process_uploads=False)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["pipeline_status"], "DONE")

    def test_guard_loop_parses_status_from_pretty_guard_stdout(self):
        stdout = """
        {
          "success": true,
          "data": {
            "pipeline_status": "READY_TO_CONTINUE",
            "next_action": "RUN_GUARD_AGAIN"
          }
        }
        """

        parsed = _parse_guard_stdout(stdout)
        status, action = _status_action_from_result(parsed)

        self.assertEqual(status, "READY_TO_CONTINUE")
        self.assertEqual(action, "RUN_GUARD_AGAIN")

    def test_guard_loop_dynamic_timeout_is_opt_in(self):
        old_enabled = os.environ.get("AUTO_MIXCUT_GUARD_DYNAMIC_TIMEOUT")
        old_per_output = os.environ.get("AUTO_MIXCUT_GUARD_TIMEOUT_PER_OUTPUT")
        old_buffer = os.environ.get("AUTO_MIXCUT_GUARD_TIMEOUT_BUFFER")
        try:
            os.environ.pop("AUTO_MIXCUT_GUARD_DYNAMIC_TIMEOUT", None)
            os.environ["AUTO_MIXCUT_GUARD_TIMEOUT_PER_OUTPUT"] = "60"
            os.environ["AUTO_MIXCUT_GUARD_TIMEOUT_BUFFER"] = "120"
            self.assertEqual(_dynamic_round_timeout("NO_TASK", 480), 480)

            os.environ["AUTO_MIXCUT_GUARD_DYNAMIC_TIMEOUT"] = "1"
            self.assertEqual(_dynamic_round_timeout("NO_TASK", 480), 480)
        finally:
            _restore_env("AUTO_MIXCUT_GUARD_DYNAMIC_TIMEOUT", old_enabled)
            _restore_env("AUTO_MIXCUT_GUARD_TIMEOUT_PER_OUTPUT", old_per_output)
            _restore_env("AUTO_MIXCUT_GUARD_TIMEOUT_BUFFER", old_buffer)

    def test_top_up_round_count_uses_snapshot_when_count_omitted(self):
        old_max = os.environ.get("AUTO_MIXCUT_TOP_UP_MAX_PER_ROUND")
        try:
            os.environ["AUTO_MIXCUT_TOP_UP_MAX_PER_ROUND"] = "5"
            count = _cap_round_count(
                {
                    "target_variant_count": 10,
                    "target_remaining_variant_count": 7,
                    "material_pool_extra_capacity": 4,
                },
                None,
            )
        finally:
            _restore_env("AUTO_MIXCUT_TOP_UP_MAX_PER_ROUND", old_max)

        self.assertEqual(count, 4)

    def test_final_video_qc_is_skipped_by_default_for_top_up(self):
        old_value = os.environ.get("AUTO_MIXCUT_SKIP_FINAL_VIDEO_QC")
        try:
            os.environ.pop("AUTO_MIXCUT_SKIP_FINAL_VIDEO_QC", None)
            self.assertTrue(_skip_final_video_qc())
            os.environ["AUTO_MIXCUT_SKIP_FINAL_VIDEO_QC"] = "0"
            self.assertFalse(_skip_final_video_qc())
        finally:
            _restore_env("AUTO_MIXCUT_SKIP_FINAL_VIDEO_QC", old_value)

    def test_ai_supplement_infers_gap_from_capacity_shortfall(self):
        gap_text = _infer_capacity_gap_text(
            self.ctx,
            {
                "requested_variant_count": 30,
                "actual_variant_count": 10,
                "target_remaining_variant_count": 20,
                "material_pool_extra_capacity": 10,
                "first_slot_remaining_capacity": 28,
                "current_bottleneck": "已进入复用模式",
            },
        )

        self.assertIn("AI补素材", gap_text)
        self.assertIn("hero首镜", gap_text)
        self.assertIn("detail细节", gap_text)

    def test_probe_product_fails_when_any_asset_probe_fails(self):
        product_id = "PROD_PROBE_FAIL"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Probe Fail", "TH", "womens_outerwear", 1)
        self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {
                "asset_id": "ASSET_PROBE_FAIL",
                "product_id": product_id,
                "original_oss_object_id": "OSS_MISSING",
                "media_type": "video",
                "source_type": "competitor",
                "source_trust_level": "low",
                "product_binding_type": "category_reference",
                "probe_status": "pending",
            },
        )

        res = MediaProbeSkill(self.ctx).probe_product(product_id)

        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "ASSET_PROBE_FAILED")

    def test_abort_batch_rejects_outputs_and_refreshes_usage(self):
        product_id = "PROD_ABORT_BATCH"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Abort Test", "VN", "womens_outerwear", 10)
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {"batch_id": "BATCH_ABORT", "product_id": product_id, "requested_count": 1, "batch_status": "rendered"},
        )
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_ABORT",
                "asset_id": "ASSET_ABORT",
                "product_id": product_id,
                "source_type": "authorized_creator",
                "effective_roles_json": ["hero"],
            },
        )
        self.ctx.repo.upsert(
            "bgm_tracks",
            "bgm_id",
            {
                "bgm_id": "BGM_ABORT",
                "track_name": "Abort Song",
                "mood_tags_json": [],
                "category_tags_json": [],
                "template_tags_json": [],
                "bgm_tag_status": "tagged",
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_ABORT",
                "batch_id": "BATCH_ABORT",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                "duration_ms": 20000,
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
                "human_quality_status": "pending",
                "bgm_plan_json": {"bgm_id": "BGM_ABORT", "track_name": "Abort Song"},
            },
        )
        self.ctx.repo.insert("output_segments", {"output_id": "OUT_ABORT", "slot_index": 1, "segment_id": "SEG_ABORT", "asset_id": "ASSET_ABORT", "role_used": "hero"})
        self.ctx.repo.insert(
            "bgm_usage_events",
            {
                "event_id": "BGMUSE_ABORT",
                "bgm_id": "BGM_ABORT",
                "output_id": "OUT_ABORT",
                "batch_id": "BATCH_ABORT",
                "product_id": product_id,
                "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                "usage_status": "rendered",
                "quality_status": "pending",
                "reason": "render_success",
            },
        )

        res = BatchControlSkill(self.ctx).abort_batch("BATCH_ABORT", "bad_test_batch")

        self.assertTrue(res.success, res.to_dict())
        output = self.ctx.repo.get("outputs", "output_id", "OUT_ABORT")
        segment = self.ctx.repo.get("segments", "segment_id", "SEG_ABORT")
        bgm = self.ctx.repo.get("bgm_tracks", "bgm_id", "BGM_ABORT")
        self.assertEqual(output["human_quality_status"], "rejected")
        self.assertEqual(segment["used_in_outputs_count"], 0)
        self.assertEqual(segment["used_in_rejected_outputs_count"], 1)
        self.assertEqual(bgm["rejected_usage_count"], 1)
        self.assertEqual(res.data["actual_variant_count"], 0)

    def test_batch_report_reads_cached_qc_without_rewriting_output_status(self):
        product_id = "PROD_REPORT_CACHE"
        self.ctx.repo.upsert("products", "product_id", {"product_id": product_id, "product_name": "Report", "market": "VN", "category": "womens_outerwear"})
        self.ctx.repo.upsert("mixcut_batches", "batch_id", {"batch_id": "BATCH_REPORT_CACHE", "product_id": product_id, "requested_count": 1, "batch_status": "rendered"})
        self.ctx.repo.upsert(
            "render_plans",
            "render_plan_id",
            {
                "render_plan_id": "PLAN_REPORT_CACHE",
                "batch_id": "BATCH_REPORT_CACHE",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                "planned_duration_ms": 20000,
                "render_status": "rendered",
                "plan_json": {
                    "segments": [
                        {
                            "slot_index": 1,
                            "role": "hero",
                            "segment_id": "SEG_REPORT_CACHE",
                            "source_type": "ai_generated",
                            "prompt_package_id": "SPK_REPORT",
                            "selection_score": 120,
                            "selection_reason": {"why": ["ai_prompt_package_identity", "segment_type_match"]},
                        }
                    ]
                },
            },
        )
        self.ctx.repo.upsert("segments", "segment_id", {"segment_id": "SEG_REPORT_CACHE", "asset_id": "ASSET_REPORT_CACHE", "product_id": product_id, "source_type": "ai_generated", "effective_roles_json": ["hero"]})
        self.ctx.repo.insert("segment_tags", {"segment_id": "SEG_REPORT_CACHE", "tag_source": "test", "primary_shot_role": "hero", "product_visibility": "high", "hook_strength": "strong", "risk_level": "low"})
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_REPORT_CACHE",
                "batch_id": "BATCH_REPORT_CACHE",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                "duration_ms": 18000,
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
                "human_quality_status": "pending",
                "final_qc_json": {"final_qc_status": "pass", "pass_reasons": ["ok"]},
                "bgm_plan_json": {"bgm_id": "BGM_REPORT", "track_name": "Report Song", "recommended_start_sec": 8},
            },
        )
        self.ctx.repo.insert("output_segments", {"output_id": "OUT_REPORT_CACHE", "slot_index": 1, "segment_id": "SEG_REPORT_CACHE", "asset_id": "ASSET_REPORT_CACHE", "role_used": "hero"})

        res = BatchReportSkill(self.ctx).generate("BATCH_REPORT_CACHE")

        self.assertTrue(res.success, res.to_dict())
        output = self.ctx.repo.get("outputs", "output_id", "OUT_REPORT_CACHE")
        self.assertEqual(output["machine_quality_status"], "publish_ready")
        self.assertEqual(res.data["summary"]["machine_quality"], {"publish_ready": 1})

    def test_final_qc_unstructured_response_is_normalized(self):
        qc = _normalize_final_qc_response({"text": "这条需要人工再看一下"})

        self.assertEqual(qc["final_qc_status"], "needs_review")
        self.assertIn("model_response_unstructured", qc["review_reasons"])
        self.assertEqual(qc["raw_text"], "这条需要人工再看一下")

    def test_fingerprint_skips_existing_visual_phash(self):
        product_id = "VN_FP_SKIP"
        segment_id = "SEG_FP_SKIP"
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": segment_id,
                "asset_id": "ASSET_FP_SKIP",
                "product_id": product_id,
                "source_type": "authorized_creator",
                "visual_phash": "abc123",
            },
        )

        res = SegmentFingerprintSkill(self.ctx).fingerprint_product(product_id, only_ai_generated=False)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["fingerprinted_segments"], 1)
        self.assertTrue(res.data["results"][0]["skipped"])
        self.assertEqual(res.data["results"][0]["phash"], "abc123")

    def test_readiness_limits_allowed_by_diversity_capacity_and_requests_ai_supplement(self):
        product_id = "VN_DIVERSITY_CAPACITY"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 10)
        for idx in range(13):
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": f"SEG_DIVERSITY_{idx:02d}",
                    "asset_id": f"ASSET_DIVERSITY_{idx % 4}",
                    "product_id": product_id,
                    "source_type": "ai_generated",
                    "source_trust_level": "medium",
                    "product_match_status": "anchor_pass",
                    "duration_ms": 5000,
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )

        res = ReadinessCheckSkill(self.ctx).check_product(product_id)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["role_allowed_variant_count"], 10)
        self.assertEqual(res.data["recommended_variant_count"], 5)
        self.assertEqual(res.data["no_ai_max_variant_count"], 7)
        self.assertEqual(res.data["allowed_variant_count"], 10)
        self.assertEqual(res.data["strict_plannable_variant_count"], 7)
        self.assertIn("AI补素材", "; ".join(res.data["gaps"]))
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(task["allowed_variant_count"], 10)
        self.assertIn("diversity capacity limited", task["blocked_reason"])

    def test_readiness_calibrates_allowed_count_with_render_plan_dry_run(self):
        product_id = "VN_RENDER_CAPACITY_CALIBRATION"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 10)
        for idx in range(13):
            segment_id = f"SEG_RENDER_CAP_{idx:02d}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_RENDER_CAP_{idx:02d}",
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "trusted_by_source",
                    "duration_ms": 5000,
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "secondary_roles_json": ["detail", "result", "scene", "ending"],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low" if idx == 0 else "high",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )

        res = ReadinessCheckSkill(self.ctx).check_product(product_id)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["no_ai_max_variant_count"], 7)
        self.assertEqual(res.data["render_plan_capacity_estimate"]["planned_count"], 1)
        self.assertEqual(res.data["strict_plannable_variant_count"], 1)
        self.assertEqual(res.data["allowed_variant_count"], 10)
        self.assertIn("strict render plan capacity limited", "; ".join(res.data["gaps"]))
        self.assertIn("AI补素材", "; ".join(res.data["gaps"]))

    def test_ai_supplement_hero_gap_generates_only_hero_slots(self):
        slots = _gap_slots("AI补素材: hero首镜2", "womens_outerwear", 2, 6)

        self.assertEqual(len(slots), 2)
        self.assertEqual({slot["slot_role"] for slot in slots}, {"hero"})
        self.assertNotIn("tryon_result", {slot["segment_type"] for slot in slots})

    def test_readiness_caps_outputs_by_unique_first_slot_and_requests_missing_heroes(self):
        product_id = "VN_ONE_HERO_CAP"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 3)
        rows = [
            ("SEG_ONE_HERO", ["hero", "detail", "scene"]),
            ("SEG_ONE_DETAIL_A", ["detail"]),
            ("SEG_ONE_DETAIL_B", ["detail"]),
            ("SEG_ONE_RESULT", ["result"]),
            ("SEG_ONE_SCENE_A", ["scene"]),
            ("SEG_ONE_SCENE_B", ["scene"]),
            ("SEG_ONE_ENDING", ["ending"]),
        ]
        for idx, (segment_id, roles) in enumerate(rows):
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_ONE_HERO_{idx}",
                    "product_id": product_id,
                    "source_type": "ai_generated" if idx == 0 else "authorized_creator",
                    "source_trust_level": "medium",
                    "product_match_status": "anchor_pass",
                    "duration_ms": 5000,
                    "effective_roles_json": roles,
                },
            )

        res = ReadinessCheckSkill(self.ctx).check_product(product_id, 3)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["role_allowed_variant_count"], 3)
        self.assertEqual(res.data["unique_first_slot_allowed_variant_count"], 1)
        self.assertEqual(res.data["allowed_variant_count"], 1)
        gap_text = "; ".join(res.data["gaps"])
        self.assertIn("first slot uniqueness limited", gap_text)
        self.assertIn("AI补素材: hero首镜2", gap_text)
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(task["allowed_variant_count"], 1)
        self.assertIn("AI补素材: hero首镜2", task["blocked_reason"])

    def test_earrings_ai_supplement_avoids_wearing_process_segment_types(self):
        slots = _gap_slots("AI补素材: hero首镜3; detail细节1; result上身1", "earrings", 5, 6)
        segment_types = {slot["segment_type"] for slot in slots}

        self.assertNotIn("before_go_out", segment_types)
        self.assertNotIn("tryon_result", segment_types)
        self.assertNotIn("detail_atmosphere", segment_types)
        self.assertTrue(segment_types.issubset({"product_display", "product_still", "flatlay", "mirror_routine"}))

    def test_bracelet_ai_supplement_uses_jewelry_safe_slots(self):
        slots = _gap_slots("AI补素材: hero首镜2; detail细节1; result试戴2", "bracelets", 5, 6)
        segment_types = {slot["segment_type"] for slot in slots}

        self.assertNotIn("before_go_out", segment_types)
        self.assertNotIn("tryon_result", segment_types)
        self.assertNotIn("detail_atmosphere", segment_types)
        self.assertTrue(segment_types.issubset({"product_display", "product_still", "flatlay", "mirror_routine"}))
        self.assertEqual(sum(1 for slot in slots if slot["slot_role"] == "result"), 2)

    def test_ai_prompt_package_slot_role_can_restore_result_role(self):
        product_id = "TH_BRACELET_RESULT_SLOT"
        segment_id = "SEG_BRACELET_RESULT_SLOT"
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": segment_id,
                "asset_id": "ASSET_BRACELET_RESULT_SLOT",
                "product_id": product_id,
                "source_type": "ai_generated",
                "source_trust_level": "medium",
                "anchor_match_level": "strict_pass",
                "allowed_core_roles_json": ["detail", "result"],
                "segment_type": "product_display",
                "slot_role": "result",
                "frame_consistency_status": "pass",
            },
        )
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": segment_id,
                "tag_source": "test",
                "primary_shot_role": "result",
                "secondary_roles_json": ["detail", "scene"],
                "product_visibility": "high",
                "mixcut_usability": "yes",
                "risk_level": "low",
                "confidence": "high",
            },
        )

        res = EffectiveRoleSkill(self.ctx).compute_segment(segment_id)

        self.assertTrue(res.success, res.to_dict())
        self.assertIn("result", res.data["effective_roles"])
        self.assertIn("detail", res.data["effective_roles"])

    def test_render_plan_enforces_batch_segment_reuse_cap(self):
        product_id = "VN_RENDER_REUSE_CAP"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 10)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 10, "material_tier": "tier_3_full", "material_status": "ready"})
        for idx in range(13):
            segment_id = f"SEG_REUSE_CAP_{idx:02d}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_REUSE_CAP_{idx % 4}",
                    "product_id": product_id,
                    "source_type": "ai_generated",
                    "source_trust_level": "medium",
                    "product_match_status": "anchor_pass",
                    "duration_ms": 5000,
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "secondary_roles_json": ["detail", "result", "scene", "ending"],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )

        res = RenderPlanSkill(self.ctx).create_plans(product_id, 10)

        self.assertTrue(res.success, res.to_dict())
        plans = self.ctx.repo.list_where("render_plans", "batch_id=? AND render_status='planned'", (res.data["batch_id"],))
        counts = {}
        first_counts = {}
        first_asset_counts = {}
        template_counts = {}
        for plan in plans:
            template_counts[plan["template_id"]] = template_counts.get(plan["template_id"], 0) + 1
            slots = (plan["plan_json"] or {}).get("segments") or []
            for slot in slots:
                counts[slot["segment_id"]] = counts.get(slot["segment_id"], 0) + 1
            if slots:
                first_counts[slots[0]["segment_id"]] = first_counts.get(slots[0]["segment_id"], 0) + 1
                first_asset_counts[slots[0]["asset_id"]] = first_asset_counts.get(slots[0]["asset_id"], 0) + 1
        self.assertLessEqual(max(counts.values()), 3)
        self.assertLessEqual(max(first_counts.values()), 3)
        self.assertLessEqual(max(first_asset_counts.values()), 4)
        self.assertLessEqual(max(template_counts.values()), 4)
        self.assertTrue(any((plan["plan_json"] or {}).get("reuse_mode") == "fill_target" for plan in plans))

    def test_render_plan_defaults_to_fill_gap_only(self):
        product_id = "VN_FILL_GAP_ONLY"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 10)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 10, "material_tier": "tier_3_full", "material_status": "ready"})
        for idx in range(16):
            segment_id = f"SEG_FILL_GAP_{idx:02d}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_FILL_GAP_{idx:02d}",
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_match_status": "anchor_pass",
                    "duration_ms": 5000,
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "secondary_roles_json": ["detail", "result", "scene", "ending"],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )
        for variant in range(1, 9):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_EXISTING_{variant}",
                    "batch_id": "BATCH_EXISTING",
                    "product_id": product_id,
                    "variant_no": variant,
                    "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                    "render_status": "rendered",
                    "machine_quality_status": "needs_review",
                    "human_quality_status": "pending",
                },
            )
        for suffix, machine_status, human_status in [
            ("DRAFT", "draft_only", "pending"),
            ("REJECTED", "publish_ready", "rejected"),
        ]:
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_EXISTING_{suffix}",
                    "batch_id": "BATCH_EXISTING",
                    "product_id": product_id,
                    "variant_no": 100,
                    "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                    "render_status": "rendered",
                    "machine_quality_status": machine_status,
                    "human_quality_status": human_status,
                },
            )

        res = RenderPlanSkill(self.ctx).create_plans(product_id)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["target_variant_count"], 10)
        self.assertEqual(res.data["existing_usable_outputs"], 8)
        self.assertEqual(res.data["fill_gap_count"], 2)
        plans = self.ctx.repo.list_where("render_plans", "batch_id=? ORDER BY variant_no", (res.data["batch_id"],))
        self.assertEqual([plan["variant_no"] for plan in plans], [9, 10])
        batch = self.ctx.repo.get("mixcut_batches", "batch_id", res.data["batch_id"])
        self.assertEqual(batch["requested_count"], 10)
        self.assertEqual(batch["allowed_count"], 2)
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(task["allowed_variant_count"], 10)
        self.assertEqual(task["actual_variant_count"], 8)
        self.assertIn("补差额: 目标=10; 已有效=8; 本轮计划=2", task["blocked_reason"])

    def test_render_plan_skips_when_active_planning_batch_exists(self):
        product_id = "VN_ACTIVE_BATCH_GUARD"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 5)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 5, "material_tier": "tier_3_full", "material_status": "ready"})
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {
                "batch_id": "BATCH_ACTIVE_PLANNING",
                "product_id": product_id,
                "requested_count": 5,
                "allowed_count": 5,
                "rendered_count": 0,
                "batch_status": "planning",
            },
        )

        res = RenderPlanSkill(self.ctx).create_plans(product_id)

        self.assertTrue(res.success, res.to_dict())
        self.assertTrue(res.data["skipped"])
        self.assertEqual(res.data["reason"], "active_planning_batch_exists")
        self.assertEqual(res.data["active_batch_id"], "BATCH_ACTIVE_PLANNING")
        batches = self.ctx.repo.list_where("mixcut_batches", "product_id=?", (product_id,))
        self.assertEqual(len(batches), 1)
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(task["task_status"], "RENDER_PLAN_SKIPPED_ACTIVE_BATCH")

    def test_render_plan_skips_when_pending_output_qc_exists(self):
        product_id = "VN_PENDING_OUTPUT_GUARD"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 5)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 5, "material_tier": "tier_3_full", "material_status": "ready"})
        self.ctx.repo.upsert(
            "mixcut_batches",
            "batch_id",
            {
                "batch_id": "BATCH_PENDING_OUTPUT",
                "product_id": product_id,
                "requested_count": 5,
                "allowed_count": 5,
                "rendered_count": 1,
                "batch_status": "rendered",
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_PENDING_OUTPUT",
                "batch_id": "BATCH_PENDING_OUTPUT",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                "render_status": "rendered",
                "machine_quality_status": "pending",
                "human_quality_status": "pending",
            },
        )

        res = RenderPlanSkill(self.ctx).create_plans(product_id)

        self.assertTrue(res.success, res.to_dict())
        self.assertTrue(res.data["skipped"])
        self.assertEqual(res.data["reason"], "pending_output_qc_exists")
        self.assertEqual(res.data["active_batch_id"], "BATCH_PENDING_OUTPUT")
        batches = self.ctx.repo.list_where("mixcut_batches", "product_id=?", (product_id,))
        self.assertEqual(len(batches), 1)
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(task["task_status"], "RENDER_PLAN_SKIPPED_PENDING_OUTPUT_QC")

    def test_low_trust_validated_hero_can_pass_first_slot_floor(self):
        segment_id = "SEG_LOW_TRUST_FIRST_SLOT"
        asset_id = "ASSET_LOW_TRUST_FIRST_SLOT"
        self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {
                "asset_id": asset_id,
                "product_id": "PROD_LOW_TRUST_FIRST_SLOT",
                "source_type": "douyin_repost",
                "source_trust_level": "low",
                "product_binding_type": "exact_sku",
                "has_watermark": "no",
            },
        )
        segment = {
            "segment_id": segment_id,
            "asset_id": asset_id,
            "product_id": "PROD_LOW_TRUST_FIRST_SLOT",
            "source_type": "douyin_repost",
            "source_trust_level": "low",
            "product_binding_type": "exact_sku",
            "product_match_status": "uncertain",
            "effective_roles_json": ["hero", "detail", "scene", "ending"],
        }
        self.ctx.repo.upsert("segments", "segment_id", segment)
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": segment_id,
                "tag_source": "test",
                "primary_shot_role": "hero",
                "secondary_roles_json": ["detail"],
                "product_visibility": "high",
                "hook_strength": "strong",
                "mixcut_usability": "yes",
                "risk_level": "low",
                "confidence": "high",
                "text_overlay_risk": "none",
            },
        )

        passed, reason = _passes_first_slot_floor(
            self.ctx,
            segment,
            {"require_no_watermark_for_first_slot": True, "avoid_subtitle_risk_in_first_slot": True},
        )

        self.assertTrue(passed, reason)

    def test_render_plan_full_refresh_ignores_existing_outputs(self):
        product_id = "VN_FULL_REFRESH"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 5)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 5, "material_tier": "tier_3_full", "material_status": "ready"})
        for idx in range(16):
            segment_id = f"SEG_FULL_REFRESH_{idx:02d}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_FULL_REFRESH_{idx:02d}",
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_match_status": "anchor_pass",
                    "duration_ms": 5000,
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "secondary_roles_json": ["detail", "result", "scene", "ending"],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_EXISTING_FULL_REFRESH",
                "batch_id": "BATCH_EXISTING",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                "render_status": "rendered",
                "machine_quality_status": "needs_review",
                "human_quality_status": "pending",
            },
        )

        res = RenderPlanSkill(self.ctx).create_plans(product_id, fill_gap_only=False)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["existing_usable_outputs"], 0)
        self.assertEqual(res.data["fill_gap_count"], 5)
        plans = self.ctx.repo.list_where("render_plans", "batch_id=? ORDER BY variant_no", (res.data["batch_id"],))
        self.assertEqual([plan["variant_no"] for plan in plans], [1, 2, 3, 4, 5])

    def test_capacity_counter_separates_target_gap_from_pool_extra_capacity(self):
        product_id = "VN_CAPACITY_COUNTER"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Jacket", "VN", "womens_outerwear", 10)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 10, "material_tier": "tier_3_full", "material_status": "ready"})
        for idx in range(16):
            segment_id = f"SEG_CAPACITY_{idx:02d}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_CAPACITY_{idx:02d}",
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "anchor_pass",
                    "duration_ms": 5000,
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "secondary_roles_json": ["detail", "result", "scene", "ending"],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )
        for variant in range(1, 11):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_CAPACITY_{variant}",
                    "batch_id": "BATCH_CAPACITY_EXISTING",
                    "product_id": product_id,
                    "variant_no": variant,
                    "template_id": "AI_OUTERWEAR_PRODUCT_FIRST_20S",
                    "render_status": "rendered",
                    "machine_quality_status": "needs_review",
                    "human_quality_status": "pending",
                },
            )

        res = CapacityCounterSkill(self.ctx).refresh_product(product_id, extra_probe_count=5)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["target_remaining_variant_count"], 0)
        self.assertGreater(res.data["material_pool_extra_capacity"], 0)
        self.assertIn("目标=10", res.data["capacity_note"])
        self.assertIn("目标缺口=0", res.data["capacity_note"])
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(task["target_remaining_variant_count"], 0)
        self.assertGreater(task["material_pool_extra_capacity"], 0)
        self.assertIn("first_slot_remaining_capacity", res.data)
        self.assertIn("current_bottleneck", res.data)

    def test_ai_supplement_state_summary_parses_requested_slots(self):
        summary = _supplement_state_summary(
            "AI补素材: hero首镜2; detail细节1; result上身1",
            created=[{"id": "a"}],
            skipped=[{"reason": "already_exists"}, {"reason": "other"}],
            failed=[],
        )

        self.assertEqual(summary["requested_slots"], {"hero": 2, "detail": 1, "result": 1})
        self.assertEqual(summary["available_task_package_count"], 2)
        self.assertEqual(summary["state"], "waiting_ai_return")

    def test_render_plan_timeout_aborts_planning_batch(self):
        product_id = "VN_RENDER_PLAN_TIMEOUT"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Clip", "VN", "hair_accessories", 1)
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 1, "material_status": "ready"})

        def slow_create(skill, product_id_arg, count=None, fill_gap_only=True):
            import time

            self.ctx.repo.upsert(
                "mixcut_batches",
                "batch_id",
                {"batch_id": "BATCH_TIMEOUT", "product_id": product_id_arg, "batch_status": "planning"},
            )
            self.ctx.repo.upsert(
                "render_plans",
                "render_plan_id",
                {"render_plan_id": "PLAN_TIMEOUT", "batch_id": "BATCH_TIMEOUT", "product_id": product_id_arg, "render_status": "planned"},
            )
            time.sleep(5)

        old_timeout = os.environ.get("AUTO_MIXCUT_RENDER_PLAN_TIMEOUT")
        os.environ["AUTO_MIXCUT_RENDER_PLAN_TIMEOUT"] = "1"
        try:
            with patch("auto_mixcut.cli.RenderPlanSkill.create_plans", slow_create):
                res = _create_render_plans_with_timeout(self.ctx, product_id, count=1)
        finally:
            if old_timeout is None:
                os.environ.pop("AUTO_MIXCUT_RENDER_PLAN_TIMEOUT", None)
            else:
                os.environ["AUTO_MIXCUT_RENDER_PLAN_TIMEOUT"] = old_timeout

        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "RENDER_PLAN_TIMEOUT")
        batch = self.ctx.repo.get("mixcut_batches", "batch_id", "BATCH_TIMEOUT")
        plan = self.ctx.repo.get("render_plans", "render_plan_id", "PLAN_TIMEOUT")
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(batch["batch_status"], "aborted_planning_timeout")
        self.assertEqual(plan["render_status"], "aborted_planning_timeout")
        self.assertEqual(task["task_status"], "RENDER_PLAN_TIMEOUT")
        self.assertEqual(task["pipeline_status"], "BLOCKED")

    def test_top_up_snapshot_uses_requested_target_not_allowed_cap(self):
        product_id = "VN_TOP_UP_TARGET_REMAINING"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Clip", "VN", "hair_accessories", 5)
        self.ctx.repo.update(
            "content_tasks",
            "product_id",
            product_id,
            {"allowed_variant_count": 3, "material_status": "ready"},
        )
        for variant in range(1, 4):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_TOP_UP_TARGET_{variant}",
                    "batch_id": "BATCH_TOP_UP_TARGET",
                    "product_id": product_id,
                    "variant_no": variant,
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                    "human_quality_status": "pending",
                },
            )

        snapshot = _top_up_snapshot(self.ctx, product_id, count=5, refresh_capacity=False)

        self.assertEqual(snapshot["target_variant_count"], 5)
        self.assertEqual(snapshot["effective_outputs"], 3)
        self.assertEqual(snapshot["target_remaining_variant_count"], 2)

    def test_render_plan_marks_allowed_cap_when_requested_target_not_filled(self):
        product_id = "VN_ALLOWED_CAP_NOT_FILLED"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Clip", "VN", "hair_accessories", 5)
        self.ctx.repo.update(
            "content_tasks",
            "product_id",
            product_id,
            {"allowed_variant_count": 3, "material_status": "ready"},
        )
        for variant in range(1, 4):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_ALLOWED_CAP_{variant}",
                    "batch_id": "BATCH_ALLOWED_CAP",
                    "product_id": product_id,
                    "variant_no": variant,
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                    "human_quality_status": "pending",
                },
            )

        res = RenderPlanSkill(self.ctx).create_plans(product_id, count=5)

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["fill_gap_count"], 0)
        task = self.ctx.repo.get("content_tasks", "product_id", product_id)
        self.assertEqual(task["task_status"], "RENDER_PLAN_SKIPPED_ALLOWED_CAP")
        self.assertIn("等待AI补素材释放更多容量", task["blocked_reason"])

    def test_render_subtitles_are_disabled_by_default(self):
        plan = {"product_id": "VN_NO_SUBTITLE", "plan_json": {"segments": []}}

        subtitles = _subtitle_plan(self.ctx, plan)

        self.assertEqual(subtitles, [])
        self.assertNotIn("drawtext", _drawtext_filter(subtitles))

    def test_render_subtitles_require_explicit_enabled_items(self):
        plan = {
            "product_id": "VN_EXPLICIT_SUBTITLE",
            "plan_json": {
                "segments": [],
                "subtitles": {
                    "enabled": True,
                    "items": [
                        {"start_ms": 300, "end_ms": 1200, "text": "Only when explicit"},
                        {"start_ms": 1500, "end_ms": 1400, "text": "invalid timing"},
                        {"start_ms": 2000, "end_ms": 3000, "text": ""},
                    ],
                },
            },
        }

        subtitles = _subtitle_plan(self.ctx, plan)

        self.assertEqual(subtitles, [{"start_ms": 300, "end_ms": 1200, "text": "Only when explicit"}])
        self.assertIn("drawtext", _drawtext_filter(subtitles))

    def test_bgm_filter_uses_audible_music_level_without_silent_padding(self):
        bgm_plan = _default_bgm_plan()

        audio_filter = _bgm_audio_filter(bgm_plan, 15000)

        self.assertIn("loudnorm=I=-10", audio_filter)
        self.assertIn("volume=1.000", audio_filter)
        self.assertIn("atrim=0:15.000", audio_filter)
        self.assertNotIn("apad", audio_filter)

    def test_segment_scoring_prefers_real_output_usage_over_planning_noise(self):
        slot = {"role": "hero", "duration_ms": 3000}
        planned_only = {
            "segment_id": "SEG_PLANNED_ONLY",
            "asset_id": "ASSET_PLANNED_ONLY",
            "source_type": "authorized_creator",
            "source_trust_level": "high",
            "product_binding_type": "exact_sku",
            "product_match_status": "trusted_by_source",
            "effective_roles_json": ["hero"],
            "duration_ms": 3000,
            "usage_count": 20,
            "used_in_outputs_count": 0,
            "risk_level": "low",
            "text_overlay_risk": "none",
        }
        actually_used = {
            **planned_only,
            "segment_id": "SEG_ACTUALLY_USED",
            "asset_id": "ASSET_ACTUALLY_USED",
            "usage_count": 20,
            "used_in_outputs_count": 10,
        }

        planned_only_score = _segment_score(self.ctx, planned_only, [], {"segments": set(), "assets": {}, "first_assets": set()}, slot, 2, 1)
        actually_used_score = _segment_score(self.ctx, actually_used, [], {"segments": set(), "assets": {}, "first_assets": set()}, slot, 2, 1)

        self.assertGreater(planned_only_score, actually_used_score)

    def test_rejected_outputs_do_not_count_as_normal_segment_usage(self):
        segment_id = "SEG_USAGE_SPLIT"
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": segment_id,
                "asset_id": "ASSET_USAGE_SPLIT",
                "product_id": "PROD_USAGE_SPLIT",
                "source_type": "authorized_creator",
                "source_trust_level": "high",
                "product_binding_type": "exact_sku",
                "product_match_status": "trusted_by_source",
                "effective_roles_json": ["hero"],
            },
        )
        outputs = [
            ("OUT_GOOD_USAGE", "passed", "pending"),
            ("OUT_REJECTED_USAGE", "passed", "rejected"),
        ]
        for output_id, machine_status, human_status in outputs:
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": output_id,
                    "batch_id": "BATCH_USAGE_SPLIT",
                    "product_id": "PROD_USAGE_SPLIT",
                    "template_id": "GENERAL_BALANCED_15S",
                    "render_status": "rendered",
                    "machine_quality_status": machine_status,
                    "human_quality_status": human_status,
                },
            )
            self.ctx.repo.insert(
                "output_segments",
                {"output_id": output_id, "segment_id": segment_id, "asset_id": "ASSET_USAGE_SPLIT", "slot_index": 1, "role_used": "hero"},
            )

        refresh_segment_usage(self.ctx, segment_id)

        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        self.assertEqual(segment.get("used_in_outputs_count"), 1)
        self.assertEqual(segment.get("used_in_rejected_outputs_count"), 1)

    def test_actual_generated_count_excludes_draft_only_and_human_rejected_outputs(self):
        product_id = "PROD_ACTUAL_GENERATED"
        rows = [
            ("OUT_ACTUAL_PASS", "rendered", "passed", "pending"),
            ("OUT_ACTUAL_REVIEW", "rendered", "needs_review", "pending"),
            ("OUT_ACTUAL_DRAFT", "rendered", "draft_only", "pending"),
            ("OUT_ACTUAL_REJECTED", "rendered", "passed", "rejected"),
            ("OUT_ACTUAL_PENDING_RENDER", "pending", "pending", "pending"),
        ]
        for output_id, render_status, machine_status, human_status in rows:
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": output_id,
                    "batch_id": "BATCH_ACTUAL_GENERATED",
                    "product_id": product_id,
                    "template_id": "GENERAL_BALANCED_15S",
                    "render_status": render_status,
                    "machine_quality_status": machine_status,
                    "human_quality_status": human_status,
                },
            )

        self.assertEqual(_actual_generated_count(self.ctx, product_id), 2)

    def test_rejected_usage_is_only_a_light_selection_penalty(self):
        slot = {"role": "hero", "duration_ms": 3000}
        base = {
            "segment_id": "SEG_REJECT_LIGHT_BASE",
            "asset_id": "ASSET_REJECT_LIGHT_BASE",
            "source_type": "authorized_creator",
            "source_trust_level": "high",
            "product_binding_type": "exact_sku",
            "product_match_status": "trusted_by_source",
            "effective_roles_json": ["hero"],
            "duration_ms": 3000,
            "usage_count": 3,
            "used_in_outputs_count": 0,
            "used_in_rejected_outputs_count": 0,
            "risk_level": "low",
            "text_overlay_risk": "none",
        }
        rejected_only = {**base, "segment_id": "SEG_REJECT_LIGHT_USED", "used_in_rejected_outputs_count": 3}
        actually_used = {**base, "segment_id": "SEG_REJECT_LIGHT_REAL", "used_in_outputs_count": 3}

        base_score = _segment_score(self.ctx, base, [], {"segments": set(), "assets": {}, "first_assets": set()}, slot, 2, 1)
        rejected_score = _segment_score(self.ctx, rejected_only, [], {"segments": set(), "assets": {}, "first_assets": set()}, slot, 2, 1)
        actually_used_score = _segment_score(self.ctx, actually_used, [], {"segments": set(), "assets": {}, "first_assets": set()}, slot, 2, 1)

        self.assertGreater(rejected_score, actually_used_score)
        self.assertLess(base_score - rejected_score, 3)

    def test_render_plan_uses_prompt_package_asset_metadata_for_segment_type(self):
        product_id = "PROD_PROMPT_PACKAGE_AI"
        rows = [
            ("SEG_GENERIC_AI", "ASSET_GENERIC_AI", ""),
            ("SEG_PACKAGE_AI", "ASSET_PACKAGE_AI", "product_still"),
        ]
        for segment_id, asset_id, scene_tag in rows:
            self.ctx.repo.upsert(
                "assets",
                "asset_id",
                {
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": "ai_generated",
                    "source_identity": f"SPK_{asset_id}" if scene_tag else "",
                    "scene_tag": scene_tag,
                    "generation_type": "image_to_video" if scene_tag else "",
                },
            )
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": "ai_generated",
                    "source_trust_level": "medium",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "anchor_pass",
                    "effective_roles_json": ["hero"],
                    "duration_ms": 4000,
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )

        res = _select_segments(
            self.ctx,
            product_id,
            [{"role": "hero", "duration_ms": 3000, "segment_type": "product_still"}],
            batch_state={"segments": set(), "segment_counts": {}, "core_segment_counts": {}, "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}},
        )

        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data[0]["segment_id"], "SEG_PACKAGE_AI")

    def test_output_cleanup_only_targets_terminal_outputs(self):
        self.assertEqual(_output_cleanup_reason({"published_at": "2026-06-04T00:00:00", "human_quality_status": "passed"}), "published")
        self.assertEqual(_output_cleanup_reason({"human_quality_status": "rejected"}), "rejected")
        self.assertEqual(_output_cleanup_reason({"human_quality_status": "passed"}), "")
        self.assertEqual(_output_cleanup_reason({"human_quality_status": "pending"}), "")

    def test_quality_gate_accepts_twenty_second_template_duration(self):
        output_id = "OUT_20S_OK"
        product_id = "VN_AI_20S"
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": output_id,
                "batch_id": "BATCH_20S",
                "product_id": product_id,
                "template_id": "AI_PRODUCT_FIRST_20S",
                "duration_ms": 20000,
                "width": 1080,
                "height": 1920,
                "machine_quality_status": "pending",
            },
        )
        for idx, role in enumerate(["hero", "detail", "result", "scene", "ending"], start=1):
            segment_id = f"SEG_20S_{idx}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_20S_{idx}",
                    "product_id": product_id,
                    "source_type": "ai_generated",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "anchor_pass",
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": role,
                    "secondary_roles_json": [],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )
            self.ctx.repo.insert(
                "output_segments",
                {
                    "output_id": output_id,
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_20S_{idx}",
                    "slot_index": idx,
                    "role_used": role,
                    "start_ms_in_output": (idx - 1) * 4000,
                    "end_ms_in_output": idx * 4000,
                },
            )
        res = QualityGateSkill(self.ctx).check_output(output_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertNotIn("duration out of supported range", res.data["reasons"])
        self.assertNotIn("duration does not match render plan", res.data["reasons"])

    def test_render_plan_skips_when_short_segments_cannot_reach_min_duration(self):
        product_id = "VN_SHORT_SEGMENTS"
        roles = ["hero", "scene", "detail", "ending"]
        for idx, role in enumerate(roles):
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": f"SEG_SHORT_{idx}",
                    "asset_id": f"ASSET_SHORT_{idx}",
                    "product_id": product_id,
                    "duration_ms": 2500,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "trusted_by_source",
                    "effective_roles_json": [role],
                },
            )
        slots = [{"role": role, "duration_ms": 4000} for role in roles]

        res = _select_segments(self.ctx, product_id, slots)

        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "SKIPPED_LOW_QUALITY")
        self.assertEqual(res.error.detail["planned_duration_ms"], 10000)
        self.assertEqual(res.error.detail["min_duration_ms"], 12000)

    def test_lightweight_ai_ratio_budget_warns_only_when_mature_and_real_shortage(self):
        product_id = "VN_AI_BUDGET"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Budget product", "VN", "hair_accessories", 1)
        for idx in range(81):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_BUDGET_{idx:03d}",
                    "batch_id": "BATCH_BUDGET",
                    "product_id": product_id,
                    "machine_quality_status": "publish_ready",
                    "human_quality_status": "pending",
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["phase"], "mature")
        self.assertEqual(res.data["ai_ratio_cap"], 0.3)
        self.assertEqual(res.data["warning"]["reason"], "diversity_exhausted_real_shortage")
        product = self.ctx.repo.get("products", "product_id", product_id)
        self.assertEqual(product["product_status"], "retire_candidate")
        alerts = self.ctx.repo.list_where("ai_diversity_alerts", "product_id=?", (product_id,))
        self.assertEqual(len(alerts), 1)

    def test_phash_cluster_signal_demotes_phase_early(self):
        product_id = "VN_AI_PHASH"
        for idx, phash in enumerate(["0000000000000000", "0000000000000001", "0000000000000003"]):
            self.ctx.repo.upsert(
                "segment_visual_fingerprints",
                "fingerprint_id",
                {
                    "fingerprint_id": f"FP_PHASH_{idx}",
                    "product_id": product_id,
                    "segment_id": f"SEG_PHASH_{idx}",
                    "source_type": "ai_generated",
                    "phash": phash,
                    "hash_method": "test",
                    "frame_count": 4,
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["base_phase"], "cold")
        self.assertEqual(res.data["phase"], "ramp")
        self.assertTrue(res.data["phash_signal"]["triggered"])

    def test_completion_decay_signal_demotes_phase_early(self):
        product_id = "VN_AI_COMPLETION"
        rates = [0.90, 0.88, 0.86, 0.25, 0.24, 0.23]
        for idx, rate in enumerate(rates):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_COMP_{idx}",
                    "batch_id": "BATCH_COMP",
                    "product_id": product_id,
                    "machine_quality_status": "publish_ready",
                    "avg_completion_rate": rate,
                    "published_at": f"2026-06-0{idx + 1}T00:00:00",
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertTrue(res.data["completion_signal"]["triggered"])
        self.assertEqual(res.data["phase"], "ramp")

    def test_trusted_real_anchor_count_prefers_source_identity_and_scene_tag(self):
        product_id = "VN_AI_REAL_DENSITY"
        for idx in range(3):
            asset_id = f"ASSET_REAL_DENSE_{idx}"
            self.ctx.repo.upsert(
                "assets",
                "asset_id",
                {
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "has_watermark": "no",
                    "source_identity": "same_creator",
                    "scene_tag": "same_room",
                },
            )
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": f"SEG_REAL_DENSE_{idx}",
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "trusted_by_source",
                    "effective_roles_json": ["hero", "detail"],
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["trusted_real_anchor_count"], 1)

    def test_segment_fingerprint_writes_visual_phash(self):
        product_id = "VN_AI_FP"
        segment_id = "SEG_FP_001"
        object_key = f"frames/{product_id}/{segment_id}/frame.jpg"
        path = self.ctx.settings.oss_root / object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fake-frame-image")
        self.ctx.repo.upsert("oss_objects", "object_id", {"object_id": "OBJ_FP_FRAME", "object_key": object_key, "object_type": "frame"})
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": segment_id,
                "asset_id": "ASSET_FP",
                "product_id": product_id,
                "source_type": "ai_generated",
            },
        )
        self.ctx.repo.upsert("segment_frames", "frame_id", {"frame_id": "FRAME_FP", "segment_id": segment_id, "frame_index": 1, "oss_object_id": "OBJ_FP_FRAME"})
        res = SegmentFingerprintSkill(self.ctx).fingerprint_segment(segment_id)
        self.assertTrue(res.success, res.to_dict())
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        self.assertEqual(segment["visual_phash"], res.data["phash"])


if __name__ == "__main__":
    unittest.main()
