from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_plan_skill import _usable_existing_outputs
from auto_mixcut.skills.usage_counter_skill import count_good_rendered_outputs
from scripts.run_ads_mixcut_unattended import (
    build_parser,
    build_quantity_goal,
    normalize_goal_args,
    plan_ads_mixcut,
)
from auto_mixcut.cli import _top_up_snapshot


class ADSQuantityGoalTest(unittest.TestCase):
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

    def test_add_count_sets_incremental_goal(self):
        args = build_parser().parse_args(
            ["--product-id", "PROD_ADS_ADD", "--add-count", "10", "--full-run", "--write"]
        )
        normalize_goal_args(args)

        self.assertEqual(args.goal_mode, "incremental_add")
        self.assertEqual(args.target_count, 10)

        goal = build_quantity_goal({"good_outputs": 5}, args.target_count, args.goal_mode, args.add_count, args.factory_tier)

        self.assertEqual(goal["start_strict_good_count"], 5)
        self.assertEqual(goal["desired_new_good_count"], 10)
        self.assertEqual(goal["target_strict_good_count"], 15)
        self.assertEqual(goal["remaining_to_target"], 10)

    def test_plan_caps_guard_target_per_pass(self):
        out = {
            "good_outputs": 5,
            "total_outputs": 26,
            "strict_good_outputs_with_voc_segments": 0,
            "base_good_outputs": 5,
        }
        goal = build_quantity_goal(out, 20, "absolute_target")

        plan = plan_ads_mixcut(
            product_id="PROD_ADS_BATCH",
            task={"product_id": "PROD_ADS_BATCH", "task_type": "ADS_FAST", "task_status": "running"},
            seg={
                "raw_total": 20,
                "total": 20,
                "by_core_role": {"hero": 8, "result": 8, "detail": 8, "scene": 4, "ending": 4},
                "hook_segments": 4,
                "voc_segments": {"total": 0, "usable": 0, "unusable": 0, "mismatch_suspect": 0},
            },
            out=out,
            voc=None,
            voc_gap={"missing_reason": "product_not_in_voc_capture_pool"},
            target=20,
            use_voc_hooks=True,
            max_hook=6,
            max_support=12,
            quantity_goal=goal,
            max_renders_per_pass=4,
        )

        self.assertEqual(plan["remaining_to_target"], 15)
        self.assertEqual(plan["render"]["planned_renders"], 4)
        self.assertEqual(plan["render"]["guard_target_count"], 9)
        self.assertEqual(plan["render"]["final_target_count"], 20)

    def test_factory_tier_can_use_target_count_shortcut(self):
        args = build_parser().parse_args(
            ["--product-id", "PROD_ADS_TIER", "--goal-mode", "factory_tier", "--target-count", "40"]
        )
        normalize_goal_args(args)
        goal = build_quantity_goal({"good_outputs": 12}, args.target_count, args.goal_mode, args.add_count, args.factory_tier)

        self.assertEqual(args.factory_tier, 40)
        self.assertEqual(goal["factory_tier"], 40)
        self.assertEqual(goal["target_strict_good_count"], 40)
        self.assertEqual(goal["desired_new_good_count"], 28)

    def test_ads_fast_strict_count_ignores_non_ads_outputs(self):
        product_id = "PROD_ADS_COUNT"
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_ADS",
                "product_id": product_id,
                "template_id": "AD_FAST_HOOK_8S",
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
            },
        )
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": "OUT_GENERAL",
                "product_id": product_id,
                "template_id": "GENERAL_BALANCED_15S",
                "render_status": "rendered",
                "machine_quality_status": "publish_ready",
            },
        )

        previous = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        try:
            os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            self.assertEqual(count_good_rendered_outputs(self.ctx, product_id, strict_segments=True), 2)

            os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
            self.assertEqual(count_good_rendered_outputs(self.ctx, product_id, strict_segments=True), 1)
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous

    def test_forced_ads_top_up_snapshot_ignores_non_ads_outputs(self):
        product_id = "PROD_ADS_TOP_UP_COUNT"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Hair Clip", "TH", "hair_accessories", 2)
        for output_id, template_id in [
            ("OUT_ADS", "AD_FAST_HOOK_8S"),
            ("OUT_GENERAL", "GENERAL_BALANCED_15S"),
        ]:
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": output_id,
                    "product_id": product_id,
                    "template_id": template_id,
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                },
            )

        previous = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        try:
            os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"

            general_snapshot = _top_up_snapshot(self.ctx, product_id, count=2, refresh_capacity=False)
            ads_snapshot = _top_up_snapshot(self.ctx, product_id, count=2, refresh_capacity=False, template_id="AD_FAST_HOOK_8S")

            self.assertEqual(general_snapshot["effective_outputs"], 2)
            self.assertEqual(ads_snapshot["effective_outputs"], 1)
            self.assertEqual(ads_snapshot["target_remaining_variant_count"], 1)
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous

    def test_forced_ads_render_plan_existing_outputs_ignore_non_ads_outputs(self):
        product_id = "PROD_ADS_PLAN_COUNT"
        for output_id, template_id in [
            ("OUT_ADS", "AD_FAST_HOOK_8S"),
            ("OUT_GENERAL", "GENERAL_BALANCED_15S"),
        ]:
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": output_id,
                    "product_id": product_id,
                    "template_id": template_id,
                    "render_status": "rendered",
                    "machine_quality_status": "publish_ready",
                },
            )

        previous = os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE")
        try:
            os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"

            general_outputs = _usable_existing_outputs(self.ctx, product_id)
            ads_outputs = _usable_existing_outputs(self.ctx, product_id, template_id="AD_FAST_HOOK_8S")

            self.assertEqual([row["output_id"] for row in general_outputs], ["OUT_ADS", "OUT_GENERAL"])
            self.assertEqual([row["output_id"] for row in ads_outputs], ["OUT_ADS"])
        finally:
            if previous is None:
                os.environ.pop("AUTO_MIXCUT_ADS_FAST_MODE", None)
            else:
                os.environ["AUTO_MIXCUT_ADS_FAST_MODE"] = previous


if __name__ == "__main__":
    unittest.main()
