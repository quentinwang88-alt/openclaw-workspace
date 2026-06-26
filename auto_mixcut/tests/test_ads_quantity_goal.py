from __future__ import annotations

import unittest

from scripts.run_ads_mixcut_unattended import (
    build_parser,
    build_quantity_goal,
    normalize_goal_args,
    plan_ads_mixcut,
)


class ADSQuantityGoalTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
