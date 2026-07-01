from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest
from unittest.mock import patch

import auto_mixcut.cli as mixcut_cli
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.result import Result
from auto_mixcut.cli import _top_up_snapshot
from auto_mixcut.skills.ai_supplement_cycle_skill import AISupplementCycleSkill
from auto_mixcut.skills.ai_supplement_gateway_skill import AISupplementGatewaySkill
from auto_mixcut.skills.ai_supplement_scheduler_skill import approve_product, daytime_approval_required
from auto_mixcut.skills.ai_supplement_workbench_skill import _gap_text_for_role_shortfall, _parse_requested_slots, _role_package_shortfall, _update_task_ai_supplement
from auto_mixcut.skills.final_video_qc_async_skill import FinalVideoQCAsyncSkill
from auto_mixcut.skills.final_video_qc_skill import _mark_product_mismatch_segments
from auto_mixcut.skills.feishu_review_skill import _select_anchor_canonical_record
from auto_mixcut.skills.mixcut_state_machine_skill import decide_factory_state, decide_mixcut_state, guard_start_status
from auto_mixcut.skills.material_policy_skill import MaterialPolicySkill
from auto_mixcut.skills.material_usage_ledger_skill import MaterialUsageLedgerSkill
from auto_mixcut.skills.output_similarity_skill import OutputSimilaritySkill
from auto_mixcut.skills.pipeline_run_skill import PipelineRunSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill, TemplateSpec, _active_planning_batch, _ads_voc_quota_state, _select_segments, _usable_existing_outputs
from scripts import run_ads_mixcut_unattended as ads_unattended
from scripts import run_ai_supplement_heartbeat as ai_heartbeat
from scripts import run_mixcut_guard as mixcut_guard
from scripts.run_ads_mixcut_unattended import apply_full_run_defaults, build_parser as build_ads_parser, is_truthy_flag, plan_ads_mixcut, submit_hook_packages
from auto_mixcut.skills.segment_prompt_factory_skill import _ensure_prompt_package_table
from auto_mixcut.skills.usage_counter_skill import count_good_rendered_outputs
from scripts.run_mixcut_guard import _compute_missing_effective_roles, _guard_direct_ai_submit_enabled, _should_defer_after_ai_return_postprocess, _should_fallback_for_forced_ads_template, _should_postprocess_ai_returns, _status_after_top_up, run_guard_pass
from scripts.sync_prompt_package_workbench_from_tasks import _existing_prompt_records, _voc_slot_plans


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

    def _create_cycle_task(self, product_id: str, target: int, actual: int, remaining: int, material_capacity: int) -> str:
        created = RDSRepositorySkill(self.ctx).create_product_task(product_id, "AI Cycle", "TH", "hair_accessories", target)
        self.assertTrue(created.success, created.to_dict())
        task_id = created.data["task_id"]
        written = self.ctx.repo.update(
            "content_tasks",
            "task_id",
            task_id,
            {
                "actual_variant_count": actual,
                "target_remaining_variant_count": remaining,
                "material_pool_extra_capacity": material_capacity,
                "first_slot_remaining_capacity": material_capacity,
                "pipeline_status": "WAITING_AI_RETURN",
                "next_action": "WAIT_AI_SEGMENT_RETURN",
                "task_status": "AI_SUPPLEMENT_CREATED",
            },
        )
        self.assertTrue(written.success, written.to_dict())
        return task_id

    def test_pipeline_step_log_can_attach_entity_context(self):
        logger = PipelineRunSkill(self.ctx)

        step_id = logger.start_step("PROD_PIPE_LOG", "segment.ai_anchor_check", entity_type="segment", entity_id="SEG_A1")
        logger.finish_step(step_id, Result.ok({"status": "completed"}))

        rows = self.ctx.repo.list_where("pipeline_step_runs", "step_run_id=?", (step_id,))
        self.assertEqual(rows[0]["detail_json"]["entity_type"], "segment")
        self.assertEqual(rows[0]["detail_json"]["entity_id"], "SEG_A1")

    def test_repository_json_fields_accept_datetime_values(self):
        product_id = "PROD_JSON_DATETIME"
        created = RDSRepositorySkill(self.ctx).create_product_task(product_id, "JSON Date", "TH", "hair_accessories", 1)
        self.assertTrue(created.success, created.to_dict())
        task_id = created.data["task_id"]

        written = self.ctx.repo.update(
            "content_tasks",
            "task_id",
            task_id,
            {"guard_detail_json": {"seen_at": datetime(2026, 6, 30, 8, 0, 0)}},
        )

        self.assertTrue(written.success, written.to_dict())
        row = self.ctx.repo.get("content_tasks", "task_id", task_id)
        self.assertEqual(row["guard_detail_json"]["seen_at"], "2026-06-30 08:00:00")

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

    def test_factory_state_routes_ready_ai_packages_to_heartbeat(self):
        decision = decide_factory_state(
            {
                "requested_variant_count": 40,
                "actual_variant_count": 18,
                "target_remaining_variant_count": 22,
                "pipeline_status": "WAITING_AI_RETURN",
                "next_action": "RUN_AI_SEGMENT_WORKER",
            },
            package_state={"ready_to_submit_count": 3, "inflight_count": 0},
        )

        self.assertEqual(decision.pipeline_status, "WAITING_AI_RETURN")
        self.assertEqual(decision.next_action, "RUN_AI_SEGMENT_WORKER")
        self.assertEqual(decision.scanner_mode, "ai_return_heartbeat")
        self.assertTrue(decision.should_continue_ai_heartbeat)
        self.assertFalse(decision.should_continue_ads_loop)

    def test_factory_state_stops_on_inflight_ai_return(self):
        decision = decide_factory_state(
            {
                "requested_variant_count": 40,
                "actual_variant_count": 18,
                "target_remaining_variant_count": 22,
                "pipeline_status": "WAITING_AI_RETURN",
                "next_action": "WAIT_AI_SEGMENT_RETURN",
            },
            package_state={"ready_to_submit_count": 0, "inflight_count": 4},
        )

        self.assertEqual(decision.scanner_mode, "ai_return_heartbeat")
        self.assertEqual(decision.stable_reason, "waiting_ai_return")
        self.assertFalse(decision.should_continue_ai_heartbeat)

    def test_factory_state_routes_capacity_to_guard(self):
        decision = decide_factory_state(
            {
                "requested_variant_count": 40,
                "actual_variant_count": 18,
                "target_remaining_variant_count": 22,
                "pipeline_status": "WAITING_AI_RETURN",
                "next_action": "WAIT_AI_SEGMENT_RETURN",
                "material_pool_extra_capacity": 25,
            }
        )

        self.assertEqual(decision.pipeline_status, "READY_TO_CONTINUE")
        self.assertEqual(decision.next_action, "RUN_GUARD_AGAIN")
        self.assertEqual(decision.scanner_mode, "guard")
        self.assertTrue(decision.should_continue_ads_loop)

    def test_ai_supplement_cycle_inspect_routes_ready_packages(self):
        product_id = "PROD_AI_CYCLE_READY"
        task_id = self._create_cycle_task(product_id, target=8, actual=3, remaining=5, material_capacity=0)

        with patch(
            "auto_mixcut.skills.ai_supplement_cycle_skill.AISupplementGatewaySkill.package_state",
            return_value={
                "ready_to_submit_count": 2,
                "recoverable_failed_count": 0,
                "inflight_count": 0,
                "imported_package_count": 0,
                "consumed_package_count": 0,
            },
        ):
            result = AISupplementCycleSkill(self.ctx).inspect(product_id)

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data["cycle_status"], "ready_to_submit")
        self.assertEqual(result.data["next_action"], "RUN_AI_SEGMENT_WORKER")
        self.assertEqual(result.data["state_after"]["task_id"], task_id)

    def test_ai_supplement_cycle_inspect_waits_for_inflight(self):
        product_id = "PROD_AI_CYCLE_WAIT"
        self._create_cycle_task(product_id, target=8, actual=3, remaining=5, material_capacity=0)

        with patch(
            "auto_mixcut.skills.ai_supplement_cycle_skill.AISupplementGatewaySkill.package_state",
            return_value={
                "ready_to_submit_count": 0,
                "recoverable_failed_count": 0,
                "inflight_count": 2,
                "imported_package_count": 0,
                "consumed_package_count": 0,
            },
        ):
            result = AISupplementCycleSkill(self.ctx).inspect(product_id)

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data["cycle_status"], "waiting_return")
        self.assertEqual(result.data["next_action"], "WAIT_AI_SEGMENT_RETURN")

    def test_ai_supplement_cycle_inspect_fulfilled_by_material_capacity(self):
        product_id = "PROD_AI_CYCLE_CAPACITY"
        self._create_cycle_task(product_id, target=8, actual=3, remaining=5, material_capacity=5)

        with patch(
            "auto_mixcut.skills.ai_supplement_cycle_skill.AISupplementGatewaySkill.package_state",
            return_value={
                "ready_to_submit_count": 0,
                "recoverable_failed_count": 0,
                "inflight_count": 0,
                "imported_package_count": 0,
                "consumed_package_count": 0,
            },
        ):
            result = AISupplementCycleSkill(self.ctx).inspect(product_id)

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data["cycle_status"], "fulfilled")
        self.assertEqual(result.data["next_action"], "RUN_GUARD_AGAIN")

    def test_ai_supplement_cycle_run_once_submits_ready_package(self):
        product_id = "PROD_AI_CYCLE_SUBMIT"
        self._create_cycle_task(product_id, target=8, actual=3, remaining=5, material_capacity=0)
        package_states = [
            {
                "ready_to_submit_count": 1,
                "recoverable_failed_count": 0,
                "inflight_count": 0,
                "imported_package_count": 0,
                "consumed_package_count": 0,
            },
            {
                "ready_to_submit_count": 0,
                "recoverable_failed_count": 0,
                "inflight_count": 1,
                "imported_package_count": 0,
                "consumed_package_count": 0,
            },
            {
                "ready_to_submit_count": 0,
                "recoverable_failed_count": 0,
                "inflight_count": 1,
                "imported_package_count": 0,
                "consumed_package_count": 0,
            },
        ]
        calls = []

        def submit_fn(ctx, pid, dry_run):
            calls.append((pid, dry_run))
            return {"status": "ok", "submitted": 1}

        with patch(
            "auto_mixcut.skills.ai_supplement_cycle_skill.AISupplementGatewaySkill.package_state",
            side_effect=package_states,
        ):
            result = AISupplementCycleSkill(self.ctx).run_once(
                product_id,
                submit_fn=submit_fn,
                recover=False,
                import_returns=False,
                run_guard=False,
            )

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(calls, [(product_id, False)])
        self.assertEqual(result.data["cycle_status"], "submitted_waiting_return")
        self.assertEqual(result.data["next_action"], "WAIT_AI_SEGMENT_RETURN")

    def test_ads_full_run_delegates_submit_to_worker_by_default(self):
        previous = os.environ.get("AUTO_MIXCUT_ALLOW_DIRECT_SUBMIT")
        os.environ.pop("AUTO_MIXCUT_ALLOW_DIRECT_SUBMIT", None)
        try:
            args = build_ads_parser().parse_args(
                ["--product-id", "PROD_ADS_FACTORY", "--target-count", "20", "--full-run", "--write"]
            )
            apply_full_run_defaults(args)

            self.assertTrue(args.prepare_voc_hooks)
            self.assertTrue(args.write_prompt_workbench)
            self.assertTrue(args.import_returns)
            self.assertTrue(args.render)
            self.assertFalse(args.submit_hook_packages)
            self.assertFalse(args.wait_returns)
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_ALLOW_DIRECT_SUBMIT", None)
            else:
                os.environ["AUTO_MIXCUT_ALLOW_DIRECT_SUBMIT"] = previous

    def test_ads_direct_submit_is_explicit_escape_hatch(self):
        args = SimpleNamespace(
            submit_hook_packages=True,
            allow_direct_submit=False,
            write=True,
            submit_channel="jimeng",
        )

        result = submit_hook_packages({"gap": {"new_hook_segments_planned": 1}}, args, ["SP_DIRECT_BLOCKED"])

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "direct_submit_disabled")

    def test_ads_missing_voc_package_is_optional(self):
        result = ads_unattended.prepare_voc_hooks(
            {"voc_hook_package": {"found": False}},
            SimpleNamespace(use_voc_hooks=True, prepare_voc_hooks=True),
        )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "voc_hook_package_missing_optional")
        self.assertFalse(ads_unattended.result_has_failed_step({"prepare_voc_hooks": result}))

    def test_ads_ai_supplement_stock_target_scales_for_factory_tier(self):
        plan = {
            "remaining_to_target": 20,
            "target_count": 40,
            "factory_target_count": 40,
            "flow_summary": {
                "bottleneck": {
                    "material_pool_extra_capacity": 0,
                    "first_slot_remaining_capacity": 0,
                    "current_bottleneck": "首镜素材不足",
                }
            },
        }

        gap_text = ads_unattended._ads_ai_supplement_gap_text(plan)
        slots = _parse_requested_slots(gap_text)

        self.assertEqual(ads_unattended._ads_ai_supplement_package_stock_target(plan), 10)
        self.assertEqual(sum(slots.values()), 10)
        self.assertGreaterEqual(slots["hero"], slots["detail"])
        self.assertIn("scene", slots)

    def test_ads_stage_status_marks_factory_progress(self):
        status, current_step = ads_unattended.stage_status(
            {
                "target_count": 20,
                "render": {"status": "completed"},
                "final_qc": {"status": "completed"},
                "final_inspect": {
                    "good_outputs": 9,
                    "remaining_to_target": 11,
                    "quantity_goal": {"new_good_outputs": 4, "remaining_to_target": 11},
                },
            }
        )

        self.assertEqual(status, "in_progress")
        self.assertEqual(current_step, "final_qc")

    def test_ads_segment_summary_uses_latest_tags_in_bulk(self):
        product_id = "PROD_ADS_SEG_SUMMARY"
        _ensure_prompt_package_table(self.ctx)
        self.ctx.repo.upsert(
            "segment_prompt_packages",
            "segment_prompt_id",
            {
                "segment_prompt_id": "SP_ADS_SEG_SUMMARY",
                "product_id": product_id,
                "template_id": "VOC_ADS_HOOK_PACKAGE",
            },
        )
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_ADS_SEG_SUMMARY",
                "asset_id": "ASSET_ADS_SEG_SUMMARY",
                "product_id": product_id,
                "prompt_package_id": "SP_ADS_SEG_SUMMARY",
                "segment_status": "qc_passed",
                "source_type": "ai_generated",
                "effective_roles_json": ["hero"],
            },
        )
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": "SEG_ADS_SEG_SUMMARY",
                "tag_source": "old",
                "primary_shot_role": "scene",
                "hook_visual_type": "none",
                "hook_strength": "weak",
            },
        )
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": "SEG_ADS_SEG_SUMMARY",
                "tag_source": "latest",
                "primary_shot_role": "hero",
                "hook_visual_type": "appearance_transform",
                "hook_strength": "strong",
            },
        )
        with self.ctx.repo.connect() as conn:
            summary = ads_unattended.load_segment_summary(conn, product_id)

        self.assertEqual(summary["hook_segments"], 1)
        self.assertEqual(summary["voc_segments"]["usable"], 1)
        self.assertEqual(summary["segments"][0]["hook_visual_type"], "appearance_transform")

    def test_ads_render_guard_skips_upload_sync(self):
        previous = os.environ.get("AUTO_MIXCUT_RENDER_GUARD_SUBPROCESS")
        os.environ["AUTO_MIXCUT_RENDER_GUARD_SUBPROCESS"] = "1"
        args = SimpleNamespace(
            render=True,
            write=True,
            product_id="PROD_ADS_RENDER",
            target_count=20,
            guard_max_rounds=2,
            render_timeout_minutes=1,
        )
        calls = []
        original = ads_unattended.command_result

        def fake_command_result(cmd, cwd, env=None, timeout_minutes=0):
            calls.append({"cmd": cmd, "cwd": cwd, "env": env or {}, "timeout_minutes": timeout_minutes})
            return {"status": "completed"}

        ads_unattended.command_result = fake_command_result
        try:
            result = ads_unattended.run_render_guard(
                args,
                {"render": {"guard_target_count": 5, "final_target_count": 20, "planned_renders": 5, "max_renders_per_pass": 5}},
            )
        finally:
            ads_unattended.command_result = original
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_RENDER_GUARD_SUBPROCESS", None)
            else:
                os.environ["AUTO_MIXCUT_RENDER_GUARD_SUBPROCESS"] = previous

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["guard_target_count"], 5)
        self.assertIn("--skip-upload-sync", calls[0]["cmd"])
        self.assertEqual(calls[0]["env"]["AUTO_MIXCUT_ADS_FAST_MODE"], "1")
        self.assertEqual(calls[0]["env"]["AUTO_MIXCUT_SKIP_GUARD_INIT_DB"], "1")
        self.assertEqual(calls[0]["env"]["AUTO_MIXCUT_SKIP_RENDER_RUNTIME_SCHEMA"], "1")

    def test_ads_full_run_syncs_factory_tier_goal_back_to_task(self):
        product_id = "PROD_ADS_FACTORY_GOAL"
        created = RDSRepositorySkill(self.ctx).create_product_task(product_id, "Factory Goal", "TH", "earrings", 5)
        self.assertTrue(created.success, created.to_dict())

        result = ads_unattended.sync_content_task_goal(
            {
                "target_count": 20,
                "final_inspect": {"good_outputs": 0, "quantity_goal": {"remaining_to_target": 20}},
                "render": {"status": "completed"},
            },
            SimpleNamespace(product_id=product_id, target_count=20),
        )

        self.assertEqual(result["status"], "synced")
        row = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))[0]
        self.assertEqual(row["requested_variant_count"], 20)
        self.assertEqual(row["pipeline_status"], "READY_TO_CONTINUE")
        self.assertEqual(row["next_action"], "RUN_GUARD_AGAIN")

    def test_render_plan_explicit_stage_target_overrides_zero_allowed_cap(self):
        product_id = "PROD_STAGE_TARGET_ZERO_ALLOWED"
        created = RDSRepositorySkill(self.ctx).create_product_task(product_id, "Stage Target", "TH", "hair_accessories", 40)
        self.assertTrue(created.success, created.to_dict())
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 0, "material_status": "not_ready"})
        for idx in range(1, 8):
            segment_id = f"SEG_STAGE_TARGET_{idx}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_STAGE_TARGET_{idx}",
                    "product_id": product_id,
                    "source_type": "self_shot",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "anchor_pass",
                    "segment_status": "qc_passed",
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                    "duration_ms": 3000,
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

        result = RenderPlanSkill(self.ctx).create_plans(product_id, count=4)

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data["target_variant_count"], 4)
        self.assertGreater(len(result.data["render_plan_ids"]), 0)

    def test_guard_direct_ai_submit_is_disabled_by_default(self):
        previous = os.environ.get("AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES")
        os.environ.pop("AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES", None)
        try:
            self.assertFalse(_guard_direct_ai_submit_enabled())
            os.environ["AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES"] = "1"
            self.assertTrue(_guard_direct_ai_submit_enabled())
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES", None)
            else:
                os.environ["AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES"] = previous

    def test_guard_postprocesses_ai_returns_before_top_up(self):
        product_id = "PROD_AI_RETURN_POSTPROCESS"

        self.assertTrue(
            _should_postprocess_ai_returns(
                self.ctx,
                product_id,
                {"status": "ok", "imported_count": 2, "remaining_count": 5},
            )
        )
        self.assertFalse(
            _should_postprocess_ai_returns(
                self.ctx,
                product_id,
                {"status": "skipped", "reason": "target_already_filled", "remaining_count": 0},
            )
        )

        self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {
                "asset_id": "ASSET_AI_PENDING",
                "product_id": product_id,
                "source_type": "ai_generated",
                "media_type": "video",
                "probe_status": "pending",
            },
        )
        self.assertTrue(
            _should_postprocess_ai_returns(
                self.ctx,
                product_id,
                {"status": "ok", "imported_count": 0, "remaining_count": 5},
            )
        )

    def test_guard_defers_when_ai_return_postprocess_is_incomplete(self):
        self.assertTrue(
            _should_defer_after_ai_return_postprocess(
                {"remaining_count": 3, "material_pool_extra_capacity": 0},
                {"probe_pending_count": 1, "watermark_pending_count": 0, "unsegmented_asset_count": 0, "stale_segment_count": 0},
            )
        )
        self.assertFalse(
            _should_defer_after_ai_return_postprocess(
                {"remaining_count": 3, "material_pool_extra_capacity": 2},
                {"probe_pending_count": 1, "watermark_pending_count": 0, "unsegmented_asset_count": 0, "stale_segment_count": 0},
            )
        )

    def test_forced_ads_template_falls_back_only_on_hero_hook_gap(self):
        hero_hook_gap = Result.fail(
            "RENDER_PLAN_FAILED",
            "no segment available for role hero",
            {"role": "hero", "reason": "candidate_pool_empty_after_safety_filters"},
        )
        wrapped = Result.fail(
            "RENDER_PLAN_FAILED",
            "top-up failed at render_plan",
            {"stage": "render_plan", "cause": hero_hook_gap.to_dict()},
        )

        self.assertTrue(_should_fallback_for_forced_ads_template("AD_FAST_HOOK_8S", wrapped))
        self.assertFalse(_should_fallback_for_forced_ads_template("", wrapped))

        detail_gap = Result.fail(
            "RENDER_PLAN_FAILED",
            "no segment available for role detail",
            {"role": "detail", "reason": "candidate_pool_empty_after_safety_filters"},
        )
        self.assertFalse(_should_fallback_for_forced_ads_template("AD_FAST_HOOK_8S", detail_gap))

    def test_forced_ads_template_does_not_fallback_on_success(self):
        self.assertFalse(_should_fallback_for_forced_ads_template("AD_FAST_HOOK_8S", Result.ok({"status": "ok"})))

    def test_forced_ads_template_falls_back_on_empty_plan(self):
        empty_plan = Result.ok(
            {
                "stop_reason": "render_plan_empty",
                "rounds": [{"planned_count": 0, "skipped_plan_count": 1}],
            }
        )

        self.assertTrue(_should_fallback_for_forced_ads_template("AD_FAST_HOOK_8S", empty_plan))
        self.assertFalse(_should_fallback_for_forced_ads_template("AI_LIFESTYLE_20S", empty_plan))

    def test_ai_supplement_gateway_counts_active_packages_only(self):
        product_id = "PROD_AI_GATEWAY"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Earring", "MY", "earrings", 5)
        task = self.ctx.repo.list_where("content_tasks", "product_id=?", (product_id,))[0]
        self.ctx.repo.update("content_tasks", "task_id", task["task_id"], {"target_remaining_variant_count": 5})
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        rows = [
            ("SP_READY", "created", "", "", "rec_ready", ""),
            ("SP_RETRY", "failed", "", "real_submit_disabled", "", ""),
            ("SP_SUBMITTED", "submitted", "", "", "", "JM_TEST_SUBMITTED"),
            ("SP_IMPORTED", "imported", "ASSET_AI", "", "", ""),
            ("SP_CONSUMED", "consumed", "", "", "", ""),
        ]
        for prompt_id, status, asset_id, failure, feishu_record_id, external_job_id in rows:
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
                    "external_job_id": external_job_id,
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
        self.assertEqual(budget["submit_limit"], 0)
        self.assertEqual(budget["submit_block_reason"], "no_ready_or_recoverable_prompt_package")
        self.assertEqual(budget["package_shortfall_count"], 5)

    def test_ai_supplement_daytime_approval_stays_valid_after_workbench_update(self):
        product_id = "PROD_AI_APPROVAL_VALID"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Earring", "MY", "earrings", 10)
        env_backup = {
            "AUTO_MIXCUT_AI_SUPPLEMENT_DAY_APPROVAL": os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_DAY_APPROVAL"),
            "AUTO_MIXCUT_AI_SUPPLEMENT_DAY_START_HOUR": os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_DAY_START_HOUR"),
            "AUTO_MIXCUT_AI_SUPPLEMENT_NIGHT_START_HOUR": os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_NIGHT_START_HOUR"),
        }
        os.environ["AUTO_MIXCUT_AI_SUPPLEMENT_DAY_APPROVAL"] = "1"
        os.environ["AUTO_MIXCUT_AI_SUPPLEMENT_DAY_START_HOUR"] = "0"
        os.environ["AUTO_MIXCUT_AI_SUPPLEMENT_NIGHT_START_HOUR"] = "24"
        try:
            self.assertTrue(daytime_approval_required(self.ctx, product_id))
            approved = approve_product(self.ctx, product_id)
            self.assertTrue(approved.success, approved.to_dict())
            self.assertFalse(daytime_approval_required(self.ctx, product_id))

            task = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))[0]
            _update_task_ai_supplement(self.ctx, task, "created", 4, {"product_id": product_id, "workbench": {"created": 4}})

            row = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))[0]
            detail = row["ai_supplement_detail_json"]
            self.assertEqual(detail["daytime_approval_valid_date"], approved.data["daytime_approval_valid_date"])
            self.assertFalse(daytime_approval_required(self.ctx, product_id))
        finally:
            for key, value in env_backup.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_ai_supplement_role_shortfall_is_not_satisfied_by_other_roles(self):
        shortfall = _role_package_shortfall(
            {"hero": 2, "detail": 2, "result": 1},
            {
                "hero": {"future_package_count": 1},
                "detail": {"future_package_count": 5},
                "result": {"future_package_count": 0},
            },
        )

        self.assertEqual(shortfall, {"hero": 1, "result": 1})
        self.assertEqual(_parse_requested_slots(_gap_text_for_role_shortfall(shortfall)), {"hero": 1, "result": 1})

    def test_ai_supplement_gateway_rejected_result_sync_does_not_count_as_ready(self):
        product_id = "PROD_AI_REJECTED_SYNC"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Earring", "MY", "earrings", 3)
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        self.ctx.repo.upsert(
            "segment_prompt_packages",
            "segment_prompt_id",
            {
                "segment_prompt_id": "SP_TIMEOUT_READY_LOOKING",
                "product_id": product_id,
                "package_status": "created",
                "feishu_record_id": "rec_timeout",
                "result_sync_status": "timed_out",
            },
        )

        state = AISupplementGatewaySkill(self.ctx).package_state(product_id)
        budget = AISupplementGatewaySkill(self.ctx).submit_budget(product_id, remaining_count=3, configured_limit=5)

        self.assertEqual(state["ready_to_submit_count"], 0)
        self.assertEqual(state["failed_package_count"], 1)
        self.assertEqual(state["active_package_count"], 0)
        self.assertEqual(budget["submit_limit"], 0)
        self.assertEqual(budget["package_shortfall_count"], 3)

    def test_prompt_package_schema_supports_channel_and_result_sync(self):
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        self.ctx.repo.upsert(
            "segment_prompt_packages",
            "segment_prompt_id",
            {
                "segment_prompt_id": "SP_SCHEMA_FIELDS",
                "product_id": "PROD_SCHEMA_FIELDS",
                "package_status": "created",
                "feishu_record_id": "rec_schema",
                "submit_channel": "imini",
                "result_sync_status": "rendering",
            },
        )

        row = self.ctx.repo.get("segment_prompt_packages", "segment_prompt_id", "SP_SCHEMA_FIELDS")

        self.assertEqual(row["submit_channel"], "imini")
        self.assertEqual(row["result_sync_status"], "rendering")

    def test_ai_return_recovery_scans_only_actionable_prompt_packages(self):
        product_id = "PROD_AI_RECOVER_SCOPE"
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        rows = [
            ("SP_READY_ONLY", "created", "rec_ready", "", ""),
            ("SP_INFLIGHT", "generating", "rec_inflight", "", "JM_INFLIGHT"),
            ("SP_RETRY", "failed", "", "real_submit_disabled", ""),
            ("SP_DEAD", "failed", "", "长时间未完成闭环", ""),
        ]
        for prompt_id, status, feishu_record_id, failure_reason, external_job_id in rows:
            self.ctx.repo.upsert(
                "segment_prompt_packages",
                "segment_prompt_id",
                {
                    "segment_prompt_id": prompt_id,
                    "product_id": product_id,
                    "package_status": status,
                    "feishu_record_id": feishu_record_id,
                    "failure_reason": failure_reason,
                    "external_job_id": external_job_id,
                },
            )

        with patch.object(ai_heartbeat, "_run", return_value={"status": "ok"}) as run_mock:
            result = ai_heartbeat.recover_product_results(self.ctx, product_id, dry_run=False)

        self.assertEqual(result["status"], "ok")
        command = run_mock.call_args.args[0]
        task_names = command[command.index("--task-name") + 1]
        self.assertIn("SP_INFLIGHT", task_names)
        self.assertIn("SP_RETRY", task_names)
        self.assertNotIn("SP_READY_ONLY", task_names)
        self.assertNotIn("SP_DEAD", task_names)

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

    def test_ads_first_slot_blocks_low_trust_repost_by_default(self):
        product_id = "PROD_ADS_LOW_TRUST_FIRST"
        previous_mode = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        previous_allow = os.environ.get("AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        os.environ.pop("AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT", None)
        try:
            self.ctx.repo.upsert(
                "assets",
                "asset_id",
                {
                    "asset_id": "ASSET_LOW_TRUST_FIRST",
                    "product_id": product_id,
                    "source_type": "competitor",
                    "source_trust_level": "low",
                    "has_watermark": "no",
                },
            )
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": "SEG_LOW_TRUST_FIRST",
                    "asset_id": "ASSET_LOW_TRUST_FIRST",
                    "product_id": product_id,
                    "source_type": "competitor",
                    "source_trust_level": "low",
                    "product_binding_type": "same_style",
                    "product_match_status": "uncertain",
                    "segment_status": "qc_passed",
                    "effective_roles_json": ["hero"],
                    "duration_ms": 3000,
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": "SEG_LOW_TRUST_FIRST",
                    "tag_source": "test",
                    "primary_shot_role": "hero",
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "text_overlay_risk": "none",
                },
            )
            template = TemplateSpec(
                template_id="AD_FAST_HOOK_8S",
                duration_ms=3000,
                slots=[{"role": "hero", "duration_ms": 3000}],
                default_moods=[],
                suitable_categories=[],
                template_objective="ad_fast_hook",
                pacing="fast",
                required_roles=["hero"],
                risk_policy={},
                source_policy={},
                bgm_profile={},
            )

            blocked = _select_segments(
                self.ctx,
                product_id,
                [{"role": "hero", "duration_ms": 3000}],
                batch_state={"segments": set(), "segment_counts": {}, "core_segment_counts": {}, "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}, "template_counts": {}},
                template=template,
            )
            self.assertFalse(blocked.success)

            os.environ["AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT"] = "1"
            allowed = _select_segments(
                self.ctx,
                product_id,
                [{"role": "hero", "duration_ms": 3000}],
                batch_state={"segments": set(), "segment_counts": {}, "core_segment_counts": {}, "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}, "template_counts": {}},
                template=template,
            )
            self.assertTrue(allowed.success, allowed.to_dict())
        finally:
            if previous_mode is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous_mode
            if previous_allow is None:
                os.environ.pop("AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT"] = previous_allow

    def test_material_policy_blocks_published_segment_for_ads(self):
        product_id = "PROD_POLICY_PUBLISHED"
        segment_id = "SEG_POLICY_PUBLISHED"
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": segment_id,
                "asset_id": "ASSET_POLICY_PUBLISHED",
                "product_id": product_id,
                "source_type": "ai_generated",
                "source_trust_level": "high",
                "product_match_status": "anchor_pass",
                "segment_status": "qc_passed",
                "effective_roles_json": ["hero"],
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_POLICY_PUBLISHED",
                "batch_id": "BATCH_POLICY_PUBLISHED",
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "AD_FAST_HOOK_8S",
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
                "published_at": "2026-06-26T10:00:00",
            },
        )
        self.ctx.repo.insert(
            "output_segments",
            {
                "output_id": "OUT_POLICY_PUBLISHED",
                "segment_id": segment_id,
                "asset_id": "ASSET_POLICY_PUBLISHED",
                "slot_index": 1,
                "role_used": "hero",
            },
        )

        decision = MaterialPolicySkill(self.ctx).evaluate_segment(
            self.ctx.repo.get("segments", "segment_id", segment_id),
            usecase="ads_mixcut",
            slot_index=2,
            role="detail",
        )

        self.assertFalse(decision["ads_eligible"])
        self.assertFalse(decision["reuse_allowed"])
        self.assertIn("published_exposure_used", decision["block_reasons"])

    def test_ads_render_plan_filters_published_segments(self):
        product_id = "PROD_RENDER_POLICY_PUBLISHED"
        previous_mode = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        try:
            for segment_id in ["SEG_RENDER_PUBLISHED", "SEG_RENDER_FRESH"]:
                self.ctx.repo.upsert(
                    "segments",
                    "segment_id",
                    {
                        "segment_id": segment_id,
                        "asset_id": f"ASSET_{segment_id}",
                        "product_id": product_id,
                        "source_type": "self_shot",
                        "source_trust_level": "high",
                        "product_binding_type": "exact_sku",
                        "product_match_status": "anchor_pass",
                        "segment_status": "qc_passed",
                        "effective_roles_json": ["hero"],
                        "duration_ms": 3000,
                    },
                )
                self.ctx.repo.insert(
                    "segment_tags",
                    {
                        "segment_id": segment_id,
                        "tag_source": "test",
                        "primary_shot_role": "hero",
                        "product_visibility": "high",
                        "hook_strength": "strong",
                        "mixcut_usability": "yes",
                        "risk_level": "low",
                        "confidence": "high",
                        "text_overlay_risk": "none",
                    },
                )
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": "OUT_RENDER_PUBLISHED",
                    "batch_id": "BATCH_RENDER_PUBLISHED",
                    "product_id": product_id,
                    "variant_no": 1,
                    "template_id": "AD_FAST_HOOK_8S",
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                    "published_at": "2026-06-26T10:00:00",
                },
            )
            self.ctx.repo.insert(
                "output_segments",
                {
                    "output_id": "OUT_RENDER_PUBLISHED",
                    "segment_id": "SEG_RENDER_PUBLISHED",
                    "asset_id": "ASSET_SEG_RENDER_PUBLISHED",
                    "slot_index": 1,
                    "role_used": "hero",
                },
            )

            result = _select_segments(
                self.ctx,
                product_id,
                [{"role": "hero", "duration_ms": 3000}],
                batch_state={
                    "segments": set(),
                    "segment_counts": {},
                    "core_segment_counts": {},
                    "assets": {},
                    "first_assets": set(),
                    "first_asset_counts": {},
                    "first_segment_counts": {},
                    "template_counts": {},
                    "_selection_segments": [
                        {
                            "segment_id": "SEG_RENDER_PUBLISHED",
                            "asset_id": "ASSET_SEG_RENDER_PUBLISHED",
                            "product_id": product_id,
                            "source_type": "self_shot",
                            "source_trust_level": "high",
                            "product_binding_type": "exact_sku",
                            "product_match_status": "anchor_pass",
                            "segment_status": "qc_passed",
                            "effective_roles_json": ["hero"],
                            "duration_ms": 3000,
                            "risk_level": "low",
                            "primary_shot_role": "hero",
                            "product_visibility": "high",
                            "hook_strength": "strong",
                            "mixcut_usability": "yes",
                            "confidence": "high",
                            "text_overlay_risk": "none",
                            "_latest_tag_loaded": True,
                        },
                        {
                            "segment_id": "SEG_RENDER_FRESH",
                            "asset_id": "ASSET_SEG_RENDER_FRESH",
                            "product_id": product_id,
                            "source_type": "self_shot",
                            "source_trust_level": "high",
                            "product_binding_type": "exact_sku",
                            "product_match_status": "anchor_pass",
                            "segment_status": "qc_passed",
                            "effective_roles_json": ["hero"],
                            "duration_ms": 3000,
                            "risk_level": "low",
                            "primary_shot_role": "hero",
                            "product_visibility": "high",
                            "hook_strength": "strong",
                            "mixcut_usability": "yes",
                            "confidence": "high",
                            "text_overlay_risk": "none",
                            "_latest_tag_loaded": True,
                        },
                    ],
                },
            )

            self.assertTrue(result.success, result.to_dict())
            self.assertEqual(result.data[0]["segment_id"], "SEG_RENDER_FRESH")
        finally:
            if previous_mode is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous_mode

    def test_ads_render_plan_filters_first_slot_usage_cap(self):
        product_id = "PROD_RENDER_USAGE_CAP"
        previous_mode = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        try:
            for segment_id in ["SEG_USAGE_USED_FIRST", "SEG_USAGE_FRESH_FIRST"]:
                self.ctx.repo.upsert(
                    "segments",
                    "segment_id",
                    {
                        "segment_id": segment_id,
                        "asset_id": f"ASSET_{segment_id}",
                        "product_id": product_id,
                        "source_type": "self_shot",
                        "source_trust_level": "high",
                        "product_binding_type": "exact_sku",
                        "product_match_status": "anchor_pass",
                        "segment_status": "qc_passed",
                        "effective_roles_json": ["hero"],
                        "duration_ms": 3000,
                    },
                )
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": "OUT_USAGE_PREVIOUS",
                    "batch_id": "BATCH_USAGE_PREVIOUS",
                    "product_id": product_id,
                    "variant_no": 1,
                    "template_id": "AD_FAST_HOOK_8S",
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                },
            )
            self.ctx.repo.insert(
                "output_segments",
                {
                    "output_id": "OUT_USAGE_PREVIOUS",
                    "segment_id": "SEG_USAGE_USED_FIRST",
                    "asset_id": "ASSET_SEG_USAGE_USED_FIRST",
                    "slot_index": 1,
                    "role_used": "hero",
                },
            )

            refreshed = MaterialUsageLedgerSkill(self.ctx).refresh_product(product_id)
            self.assertTrue(refreshed.success, refreshed.to_dict())
            used = self.ctx.repo.get("mixcut_segment_usage_snapshot", "segment_id", "SEG_USAGE_USED_FIRST")
            self.assertEqual(used["first_slot_good_count"], 1)

            result = _select_segments(
                self.ctx,
                product_id,
                [{"role": "hero", "duration_ms": 3000}],
                batch_state={
                    "segments": set(),
                    "segment_counts": {},
                    "core_segment_counts": {},
                    "assets": {},
                    "first_assets": set(),
                    "first_asset_counts": {},
                    "first_segment_counts": {},
                    "template_counts": {},
                    "_selection_segments": [
                        {
                            "segment_id": "SEG_USAGE_USED_FIRST",
                            "asset_id": "ASSET_SEG_USAGE_USED_FIRST",
                            "product_id": product_id,
                            "source_type": "self_shot",
                            "source_trust_level": "high",
                            "product_binding_type": "exact_sku",
                            "product_match_status": "anchor_pass",
                            "segment_status": "qc_passed",
                            "effective_roles_json": ["hero"],
                            "duration_ms": 3000,
                            "risk_level": "low",
                        },
                        {
                            "segment_id": "SEG_USAGE_FRESH_FIRST",
                            "asset_id": "ASSET_SEG_USAGE_FRESH_FIRST",
                            "product_id": product_id,
                            "source_type": "self_shot",
                            "source_trust_level": "high",
                            "product_binding_type": "exact_sku",
                            "product_match_status": "anchor_pass",
                            "segment_status": "qc_passed",
                            "effective_roles_json": ["hero"],
                            "duration_ms": 3000,
                            "risk_level": "low",
                        },
                    ],
                },
            )

            self.assertTrue(result.success, result.to_dict())
            self.assertEqual(result.data[0]["segment_id"], "SEG_USAGE_FRESH_FIRST")
        finally:
            if previous_mode is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous_mode

    def test_output_similarity_blocks_same_first_segment(self):
        product_id = "PROD_SIMILARITY_FIRST"
        for output_id, variant in [("OUT_SIM_PREVIOUS", 1), ("OUT_SIM_CURRENT", 2)]:
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": output_id,
                    "batch_id": "BATCH_SIMILARITY",
                    "product_id": product_id,
                    "variant_no": variant,
                    "template_id": "AD_FAST_HOOK_8S",
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready" if variant == 1 else "pending",
                },
            )
            for slot_index, segment_id in enumerate(["SEG_SIM_FIRST", f"SEG_SIM_{variant}_DETAIL", f"SEG_SIM_{variant}_RESULT"], start=1):
                self.ctx.repo.insert(
                    "output_segments",
                    {
                        "output_id": output_id,
                        "segment_id": segment_id,
                        "asset_id": f"ASSET_{segment_id}",
                        "slot_index": slot_index,
                        "role_used": "hero" if slot_index == 1 else "detail" if slot_index == 2 else "result",
                    },
                )

        result = OutputSimilaritySkill(self.ctx).check_output("OUT_SIM_CURRENT")

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data["decision"], "duplicate_blocked")
        saved = self.ctx.repo.list_where("mixcut_output_similarity", "output_id=?", ("OUT_SIM_CURRENT",))
        self.assertEqual(saved[0]["decision"], "duplicate_blocked")

    def test_ads_plan_creates_voc_gap_even_when_generic_hooks_exist(self):
        previous_ratio = os.environ.get("AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO")
        previous_cap = os.environ.get("AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP")
        os.environ["AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO"] = "0.2"
        os.environ["AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP"] = "2"
        try:
            plan = plan_ads_mixcut(
                "PROD_VOC_GAP",
                {"product_id": "PROD_VOC_GAP", "task_status": "READY"},
                {
                    "total": 20,
                    "raw_total": 20,
                    "by_core_role": {"hero": 6, "result": 4, "detail": 4, "scene": 2, "ending": 1},
                    "hook_segments": 6,
                    "voc_segments": {"total": 0, "usable": 0, "unusable": 0},
                },
                {
                    "good_outputs": 0,
                    "total_outputs": 0,
                    "strict_good_outputs_with_voc_segments": 0,
                },
                {
                    "confirmed": True,
                    "package_id": "VOC_PACKAGE",
                    "readiness_status": "ready_for_hook_package",
                    "candidates": [{"insight_id": "A"}, {"insight_id": "B"}],
                },
                {},
                target=10,
                use_voc_hooks=True,
                max_hook=6,
                max_support=12,
            )

            self.assertGreaterEqual(plan["gap"]["new_hook_segments_planned"], 1)
            usage = plan["flow_summary"]["voc_output_usage"]
            self.assertEqual(usage["voc_desired_output_quota"], 2)
            self.assertEqual(usage["voc_segment_gap"], 1)
            self.assertEqual(usage["voc_quota_status"], "needs_voc_segment_generation")
        finally:
            if previous_ratio is None:
                os.environ.pop("AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO"] = previous_ratio
            if previous_cap is None:
                os.environ.pop("AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP"] = previous_cap

    def test_render_plan_can_force_voc_ads_hook_segment(self):
        product_id = "PROD_FORCE_VOC_SEGMENT"
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        self.ctx.repo.upsert(
            "segment_prompt_packages",
            "segment_prompt_id",
            {
                "segment_prompt_id": "SP_VOC_FORCE",
                "product_id": product_id,
                "template_id": "VOC_ADS_HOOK_PACKAGE",
                "package_status": "imported",
                "generated_segment_id": "SEG_VOC_FORCE",
            },
        )
        for asset_id, segment_id, source_type, prompt_id in [
            ("ASSET_NONVOC_FORCE", "SEG_NONVOC_FORCE", "self_shot", ""),
            ("ASSET_VOC_FORCE", "SEG_VOC_FORCE", "ai_generated", "SP_VOC_FORCE"),
        ]:
            self.ctx.repo.upsert(
                "assets",
                "asset_id",
                {
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": source_type,
                    "source_trust_level": "high",
                    "prompt_package_id": prompt_id,
                },
            )
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": source_type,
                    "source_trust_level": "high",
                    "product_match_status": "trusted_by_source",
                    "product_binding_type": "exact_sku",
                    "segment_status": "qc_passed",
                    "duration_ms": 4000,
                    "effective_roles_json": [] if segment_id == "SEG_VOC_FORCE" else ["hero"],
                    "prompt_package_id": prompt_id,
                },
            )
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": "SEG_VOC_FORCE",
                "tag_source": "test",
                "primary_shot_role": "hero",
                "secondary_roles_json": ["detail", "result"],
                "product_visibility": "high",
                "hook_strength": "medium",
                "hook_visual_type": "action",
                "mixcut_usability": "yes",
                "risk_level": "low",
                "confidence": "high",
                "needs_human_review": 0,
                "text_overlay_risk": "none",
            },
        )

        state = {"segments": set(), "segment_counts": {}, "core_segment_counts": {}, "assets": {}, "first_assets": set(), "first_asset_counts": {}, "first_segment_counts": {}, "template_counts": {}}
        result = _select_segments(
            self.ctx,
            product_id,
            [{"role": "hero", "duration_ms": 3000}],
            batch_state=state,
            constraints={"require_voc_prompt_package": True},
        )

        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data[0]["segment_id"], "SEG_VOC_FORCE")
        self.assertTrue(result.data[0]["is_voc_ads_hook_package"])
        self.assertIn("voc_ads_hook_package", result.data[0]["selection_reason"]["why"])

    def test_ads_voc_quota_counts_existing_strict_voc_outputs(self):
        product_id = "PROD_VOC_QUOTA_EXISTING"
        previous_mode = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        previous_ratio = os.environ.get("AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO")
        previous_cap = os.environ.get("AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        os.environ["AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO"] = "0.2"
        os.environ["AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP"] = "2"
        try:
            self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
            self.ctx.repo.upsert(
                "segment_prompt_packages",
                "segment_prompt_id",
                {
                    "segment_prompt_id": "SP_VOC_QUOTA",
                    "product_id": product_id,
                    "template_id": "VOC_ADS_HOOK_PACKAGE",
                    "package_status": "imported",
                    "generated_segment_id": "SEG_VOC_QUOTA",
                },
            )
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": "SEG_VOC_QUOTA",
                    "asset_id": "ASSET_VOC_QUOTA",
                    "product_id": product_id,
                    "source_type": "ai_generated",
                    "segment_status": "qc_passed",
                    "effective_roles_json": ["hero"],
                    "prompt_package_id": "SP_VOC_QUOTA",
                },
            )
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": "OUT_VOC_QUOTA",
                    "batch_id": "BATCH_VOC_QUOTA",
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
                    "output_id": "OUT_VOC_QUOTA",
                    "segment_id": "SEG_VOC_QUOTA",
                    "asset_id": "ASSET_VOC_QUOTA",
                    "slot_index": 1,
                    "role_used": "hero",
                },
            )

            quota = _ads_voc_quota_state(
                self.ctx,
                product_id,
                10,
                [{"output_id": "OUT_VOC_QUOTA"}],
                {},
            )

            self.assertTrue(quota["enabled"])
            self.assertEqual(quota["usable_segment_count"], 1)
            self.assertEqual(quota["required"], 2)
            self.assertEqual(quota["existing_filled"], 1)
        finally:
            if previous_mode is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous_mode
            if previous_ratio is None:
                os.environ.pop("AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO"] = previous_ratio
            if previous_cap is None:
                os.environ.pop("AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP"] = previous_cap

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

    def test_ads_fast_stale_planning_batch_aborts_active_plans(self):
        product_id = "PROD_STALE_PLANNING"
        previous_mode = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        previous_minutes = os.environ.get("AUTO_MIXCUT_STALE_PLANNING_BATCH_MINUTES")
        os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        os.environ["AUTO_MIXCUT_STALE_PLANNING_BATCH_MINUTES"] = "1"
        try:
            batch_id = "BATCH_STALE_PLANNING"
            self.ctx.repo.upsert(
                "mixcut_batches",
                "batch_id",
                {
                    "batch_id": batch_id,
                    "product_id": product_id,
                    "batch_status": "planning",
                    "created_at": (datetime.utcnow() - timedelta(minutes=15)).isoformat(timespec="seconds"),
                },
            )
            self.ctx.repo.upsert(
                "render_plans",
                "render_plan_id",
                {
                    "render_plan_id": "PLAN_STALE_PLANNING",
                    "batch_id": batch_id,
                    "product_id": product_id,
                    "variant_no": 1,
                    "template_id": "AD_FAST_HOOK_8S",
                    "plan_json": {"segments": []},
                    "render_status": "planned",
                },
            )

            active = _active_planning_batch(self.ctx, product_id)

            self.assertIsNone(active)
            batch = self.ctx.repo.get("mixcut_batches", "batch_id", batch_id)
            plan = self.ctx.repo.get("render_plans", "render_plan_id", "PLAN_STALE_PLANNING")
            self.assertEqual(batch["batch_status"], "aborted_stale_planning")
            self.assertEqual(plan["render_status"], "aborted_stale_planning")
        finally:
            if previous_mode is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous_mode
            if previous_minutes is None:
                os.environ.pop("AUTO_MIXCUT_STALE_PLANNING_BATCH_MINUTES", None)
            else:
                os.environ["AUTO_MIXCUT_STALE_PLANNING_BATCH_MINUTES"] = previous_minutes

    def test_render_batch_timeout_marks_batch_and_task(self):
        product_id = "PROD_RENDER_TIMEOUT"
        batch_id = "BATCH_RENDER_TIMEOUT"
        previous_timeout = os.environ.get("AUTO_MIXCUT_RENDER_BATCH_TIMEOUT")
        original_render_skill = mixcut_cli.RenderSkill

        class SlowRenderSkill:
            def __init__(self, ctx):
                self.ctx = ctx

            def render_batch(self, batch_id):
                time.sleep(2)
                return Result.ok({"batch_id": batch_id})

        os.environ["AUTO_MIXCUT_RENDER_BATCH_TIMEOUT"] = "1"
        mixcut_cli.RenderSkill = SlowRenderSkill
        try:
            created = RDSRepositorySkill(self.ctx).create_product_task(product_id, "Render Timeout", "TH", "hair_accessories", 20)
            self.assertTrue(created.success, created.to_dict())
            self.ctx.repo.upsert(
                "mixcut_batches",
                "batch_id",
                {"batch_id": batch_id, "product_id": product_id, "batch_status": "planned", "created_at": datetime.utcnow().isoformat(timespec="seconds")},
            )
            self.ctx.repo.upsert(
                "render_plans",
                "render_plan_id",
                {
                    "render_plan_id": "PLAN_RENDER_TIMEOUT",
                    "batch_id": batch_id,
                    "product_id": product_id,
                    "variant_no": 1,
                    "template_id": "AD_FAST_HOOK_8S",
                    "plan_json": {"segments": []},
                    "render_status": "planned",
                },
            )

            result = mixcut_cli._render_batch_with_timeout(self.ctx, batch_id, product_id)

            self.assertFalse(result.success)
            self.assertEqual(result.error.code, "RENDER_BATCH_TIMEOUT")
            batch = self.ctx.repo.get("mixcut_batches", "batch_id", batch_id)
            plan = self.ctx.repo.get("render_plans", "render_plan_id", "PLAN_RENDER_TIMEOUT")
            task = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))[0]
            self.assertEqual(batch["batch_status"], "render_timeout")
            self.assertEqual(plan["render_status"], "render_timeout")
            self.assertEqual(task["pipeline_status"], "BLOCKED")
            self.assertEqual(task["next_action"], "CHECK_PIPELINE_LOG")
        finally:
            mixcut_cli.RenderSkill = original_render_skill
            if previous_timeout is None:
                os.environ.pop("AUTO_MIXCUT_RENDER_BATCH_TIMEOUT", None)
            else:
                os.environ["AUTO_MIXCUT_RENDER_BATCH_TIMEOUT"] = previous_timeout

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

    def test_guard_stage_target_does_not_lower_factory_goal(self):
        product_id = "PROD_GUARD_STAGE_TARGET"
        created = RDSRepositorySkill(self.ctx).create_product_task(product_id, "Factory Guard", "TH", "earrings", 20)
        self.assertTrue(created.success, created.to_dict())
        for idx in range(1, 21):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_GUARD_STAGE_{idx}",
                    "batch_id": "BATCH_GUARD_STAGE",
                    "product_id": product_id,
                    "variant_no": idx,
                    "template_id": "AD_FAST_HOOK_8S",
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                },
            )

        result = run_guard_pass(self.ctx, product_id, target=13, max_rounds=1, process_uploads=False)

        self.assertTrue(result.success, result.to_dict())
        row = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))[0]
        self.assertEqual(row["requested_variant_count"], 20)

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

    def test_ai_supplement_gateway_does_not_count_unconfirmed_inflight_as_submitted(self):
        product_id = "PROD_AI_UNCONFIRMED_GATEWAY"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Earring", "MY", "earrings", 1)
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        stale_without_marker = (datetime.utcnow() - timedelta(minutes=20)).isoformat(timespec="seconds")
        fresh_without_marker = (datetime.utcnow() - timedelta(minutes=2)).isoformat(timespec="seconds")
        rows = [
            ("SP_UNCONFIRMED", "generating", "", stale_without_marker),
            ("SP_CONFIRMED", "submitted", "JM_TEST_CONFIRMED", stale_without_marker),
            ("SP_FRESH", "generating", "", fresh_without_marker),
        ]
        for prompt_id, status, external_job_id, updated_at in rows:
            self.ctx.repo.upsert(
                "segment_prompt_packages",
                "segment_prompt_id",
                {
                    "segment_prompt_id": prompt_id,
                    "product_id": product_id,
                    "package_status": status,
                    "external_job_id": external_job_id,
                    "updated_at": updated_at,
                },
            )

        state = AISupplementGatewaySkill(self.ctx).package_state(product_id)

        self.assertEqual(state["inflight_count"], 2)
        self.assertEqual(state["recoverable_failed_count"], 1)
        self.assertEqual(state["stale_inflight_count"], 1)

    def test_prompt_workbench_indexes_unconfirmed_inflight_for_refresh(self):
        product_id = "PROD_AI_REFRESH_GATEWAY"
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        self.ctx.repo.upsert(
            "segment_prompt_packages",
            "segment_prompt_id",
            {
                "segment_prompt_id": "SP_REFRESH_STALE",
                "product_id": product_id,
                "package_status": "failed",
                "failure_reason": "real_submit_disabled",
            },
        )
        record = SimpleNamespace(
            record_id="rec_refresh",
            fields={
                "提示词包ID": "SP_REFRESH_STALE",
                "商品ID": product_id,
                "SKU ID": "DEFAULT",
                "素材角色": "hero",
                "片段类型": "商品展示",
                "生成档位": "A-核心位",
                "镜头意图": "product_clarity",
                "包状态": "失败",
            },
        )

        normal_index = _existing_prompt_records([record], include_refreshable=False, ctx=self.ctx)
        refresh_index = _existing_prompt_records([record], include_refreshable=True, ctx=self.ctx)

        key = (product_id, "DEFAULT", "hero", "商品展示", "A-核心位", "product_clarity")
        self.assertNotIn(key, normal_index)
        self.assertIn(key, refresh_index)

    def test_ai_segment_llm_failure_isolated_from_stale_pool(self):
        product_id = "PROD_AI_BAD_SEGMENT_ISOLATED"
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": "SEG_AI_BAD_TAG",
                "asset_id": "ASSET_AI_BAD_TAG",
                "product_id": product_id,
                "source_type": "ai_generated",
                "segment_status": "created",
            },
        )

        mixcut_guard._mark_ai_segment_guard_failed(self.ctx, "SEG_AI_BAD_TAG", "tag_failed", "tag poll failed: LLM_CALL_EXHAUSTED")

        segment = self.ctx.repo.get("segments", "segment_id", "SEG_AI_BAD_TAG")
        summary = mixcut_guard._stale_segment_summary(self.ctx, [segment], mixcut_guard._build_stale_index(self.ctx, [segment]))
        self.assertEqual(segment["segment_status"], "tag_failed")
        self.assertEqual(summary["stale_count"], 0)

    def test_guard_ai_worker_command_defaults_to_imini(self):
        previous = os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_SUBMIT_CHANNEL")
        os.environ.pop("AUTO_MIXCUT_AI_SUPPLEMENT_SUBMIT_CHANNEL", None)
        try:
            command = mixcut_guard._ai_segment_worker_command("PROD_AI_CMD", 5, 20)
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_AI_SUPPLEMENT_SUBMIT_CHANNEL", None)
            else:
                os.environ["AUTO_MIXCUT_AI_SUPPLEMENT_SUBMIT_CHANNEL"] = previous

        self.assertIn("--channel=imini", command)

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
        self.assertEqual(budget["submit_slot_role"], "hero")

    def test_ai_supplement_budget_submits_general_stock_while_priority_role_inflight(self):
        product_id = "PROD_AI_PRIORITY_INFLIGHT_GENERAL"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Hair Clip", "TH", "hair_accessories", 3)
        self.ctx.repo.update(
            "content_tasks",
            "product_id",
            product_id,
            {
                "target_remaining_variant_count": 3,
                "current_bottleneck": "首镜容量不足",
                "first_slot_remaining_capacity": 0,
            },
        )
        self.assertTrue(_ensure_prompt_package_table(self.ctx).success)
        rows = [
            ("SP_HERO_INFLIGHT", "hero", "submitted", "", "JM_HERO"),
            ("SP_DETAIL_READY", "detail", "created", "rec_detail", ""),
            ("SP_RESULT_READY", "result", "created", "rec_result", ""),
        ]
        for prompt_id, role, status, feishu_record_id, external_job_id in rows:
            self.ctx.repo.upsert(
                "segment_prompt_packages",
                "segment_prompt_id",
                {
                    "segment_prompt_id": prompt_id,
                    "product_id": product_id,
                    "package_status": status,
                    "feishu_record_id": feishu_record_id,
                    "external_job_id": external_job_id,
                    "slot_role": role,
                },
            )

        budget = AISupplementGatewaySkill(self.ctx).submit_budget(product_id, remaining_count=3, configured_limit=5)

        self.assertEqual(budget["priority_role"], "hero")
        self.assertEqual(budget["budget_mode"], "general_while_priority_role_inflight")
        self.assertEqual(budget["submit_limit"], 2)
        self.assertEqual(budget["submit_slot_role"], "")

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

    def test_ads_plan_reports_voc_mismatch_suspects_as_unusable(self):
        plan = plan_ads_mixcut(
            product_id="PROD_VOC_MISMATCH",
            task={"product_id": "PROD_VOC_MISMATCH", "task_type": "ADS_FAST", "task_status": "running"},
            seg={
                "raw_total": 5,
                "total": 4,
                "by_core_role": {"hero": 3, "result": 3, "detail": 2, "scene": 1},
                "hook_segments": 3,
                "voc_segments": {"total": 4, "usable": 0, "unusable": 4, "mismatch_suspect": 4},
            },
            out={
                "good_outputs": 10,
                "total_outputs": 10,
                "strict_good_outputs_with_voc_segments": 0,
            },
            voc={
                "package_id": "VOC_PACK",
                "readiness_status": "confirmed",
                "confirmed": True,
                "candidates": [{"hook": "A"}],
            },
            voc_gap=None,
            target=12,
            use_voc_hooks=True,
            max_hook=6,
            max_support=12,
        )

        usage = plan["flow_summary"]["voc_output_usage"]
        self.assertEqual(usage["voc_usable_segment_count"], 0)
        self.assertEqual(usage["voc_unusable_segment_count"], 4)
        self.assertEqual(usage["voc_mismatch_suspect_segment_count"], 4)
        self.assertEqual(usage["voc_quota_status"], "needs_voc_segment_generation")

    def test_truthy_flag_accepts_mysql_and_text_values(self):
        self.assertTrue(is_truthy_flag(1))
        self.assertTrue(is_truthy_flag("是"))
        self.assertFalse(is_truthy_flag(0))
        self.assertFalse(is_truthy_flag(None))

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
