from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from services.photo_content_planner import (
    PhotoContentPlanError, PhotoContentPlanStore, plan_th_choice_batch,
    recipe_has_planning_policy, validate_batch_plan,
)
from services.photo_theme import resolve_photo_theme
from tests.test_photo_reference_vision import recommendation


class PhotoContentPlannerTest(unittest.TestCase):
    def setUp(self):
        self.theme = resolve_photo_theme("秋季穿搭")

    def plan(self, mode="STYLE", count=3):
        return plan_th_choice_batch(
            record_id="rec-plan", recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
            theme=self.theme, reference_mode=mode, count=count,
        )

    def test_three_posts_freeze_different_real_looks_and_cover_copy(self):
        plan = self.plan()
        self.assertEqual(len(plan["items"]), 3)
        self.assertEqual(len({item["family_id"] for item in plan["items"]}), 3)
        self.assertEqual(len({item["copy"]["cover"] for item in plan["items"]}), 3)
        signatures = {
            (look["outerwear"], look["bottom"], look["shoes"])
            for item in plan["items"] for look in item["looks"]
        }
        self.assertEqual(len(signatures), 12)

    def test_product_mode_keeps_product_truth_but_changes_companion_items(self):
        plan = self.plan(mode="PRODUCT")
        outerwear = {
            look["outerwear"] for item in plan["items"] for look in item["looks"]
        }
        bottoms = {
            look["bottom"] for item in plan["items"] for look in item["looks"]
        }
        self.assertEqual(len(outerwear), 1)
        self.assertEqual(len(bottoms), 12)

    def test_warm_flat_lay_reference_selects_compatible_families_and_route(self):
        profile = {
            "schema_version": "opv-photo-style-profile-v1",
            "presentation_type": "FLAT_LAY", "season": "autumn",
            "palette": ["warm_brown", "camel", "cream"],
            "temperature": "warm", "style_tags": ["warm_neutral", "heritage", "layered"],
        }
        plan = plan_th_choice_batch(
            record_id="rec-warm-flat", recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
            theme=self.theme, reference_mode="STYLE", count=3, style_profile=profile,
        )
        self.assertEqual(
            [item["family_id"] for item in plan["items"]],
            ["warm_neutral", "soft_earth", "color_point"],
        )
        self.assertTrue(all(item["presentation_type"] == "FLAT_LAY" for item in plan["items"]))
        self.assertFalse({"monochrome", "sporty_layer"} & {item["family_id"] for item in plan["items"]})

    def test_doubao_contract_drives_dynamic_scene_looks_and_copy(self):
        profile = {
            "analysis_method": "doubao_seed_2_1", "presentation_type": "SCENE_MODEL",
            "palette": ["camel", "cream", "burgundy"], "temperature": "warm",
            "aggregate": {"background": "欧洲街角咖啡店"},
            "recommended_sets": [recommendation(1), recommendation(2)],
        }
        plan = plan_th_choice_batch(
            record_id="rec-dynamic", recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
            theme=self.theme, reference_mode="STYLE", count=2, style_profile=profile,
        )
        self.assertEqual([item["family_id"] for item in plan["items"]],
                         ["vision_dynamic_1", "vision_dynamic_2"])
        self.assertTrue(all(item["presentation_type"] == "SCENE_MODEL" for item in plan["items"]))
        self.assertEqual(plan["items"][0]["looks"][0]["outerwear"], "复古外套1a")
        self.assertEqual(plan["items"][1]["copy"]["title"], "แฟชั่นวินเทจ 2")

    def test_complete_look_mode_only_uses_neutral_visible_claims(self):
        plan = self.plan(mode="COMPLETE_LOOK", count=3)
        self.assertTrue(all(not item["looks"] for item in plan["items"]))
        self.assertEqual(len({item["copy"]["cover"] for item in plan["items"]}), 3)
        visible = " ".join(
            value for item in plan["items"] for value in item["copy"].values()
        )
        self.assertNotIn("สูง", visible)
        self.assertNotIn("°C", visible)

    def test_duplicate_family_is_rejected_before_generation(self):
        plan = self.plan()
        broken = copy.deepcopy(plan)
        broken["items"][1] = copy.deepcopy(broken["items"][0])
        broken["items"][1]["index"] = 2
        with self.assertRaisesRegex(PhotoContentPlanError, "重复"):
            validate_batch_plan(broken)

    def test_local_store_resumes_exact_plan_and_rejects_changed_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = PhotoContentPlanStore(Path(tmp))
            first = store.load_or_create(
                record_id="rec", input_contract={"quantity": 3}, create=self.plan,
            )
            second = store.load_or_create(
                record_id="rec", input_contract={"quantity": 3},
                create=lambda: self.fail("should load frozen plan"),
            )
            self.assertEqual(first, second)
            with self.assertRaisesRegex(PhotoContentPlanError, "已变化"):
                store.load_or_create(
                    record_id="rec", input_contract={"quantity": 2}, create=self.plan,
                )


class TravelOutfitPlannerTest(unittest.TestCase):
    def setUp(self):
        self.theme = resolve_photo_theme("凉爽旅行")

    def test_registry_routes_both_recipes_and_rejects_unregistered(self):
        self.assertTrue(recipe_has_planning_policy("PHOTO_TH_PICK_YOUR_LOOK_V3"))
        self.assertTrue(recipe_has_planning_policy("PHOTO_TH_TRAVEL_OUTFIT_V2"))
        self.assertFalse(recipe_has_planning_policy("PHOTO_TH_PETITE_STYLING_V1"))
        with self.assertRaisesRegex(PhotoContentPlanError, "尚未接入"):
            plan_th_choice_batch(
                record_id="rec-x", recipe_id="PHOTO_TH_PETITE_STYLING_V1",
                theme=self.theme, reference_mode="STYLE", count=1,
            )

    def test_travel_plan_freezes_travel_families_with_travel_copy(self):
        plan = plan_th_choice_batch(
            record_id="rec-travel", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=self.theme, reference_mode="STYLE", count=3,
        )
        self.assertEqual(plan["policy_id"], "TH_TRAVEL_OUTFIT_V1")
        self.assertEqual(len(plan["items"]), 3)
        self.assertEqual(len({item["family_id"] for item in plan["items"]}), 3)
        signatures = {
            (look["outerwear"], look["bottom"], look["shoes"])
            for item in plan["items"] for look in item["looks"]
        }
        self.assertEqual(len(signatures), 12)
        for item in plan["items"]:
            self.assertEqual([look["role"] for look in item["looks"]],
                             ["look_a", "look_b", "look_c", "look_d"])
            self.assertIn("A B C หรือ D", item["copy"]["cover"])
            self.assertTrue(item["scene_zh"] and item["angle_zh"])
        travel_families = {
            "airport_transit", "city_walk", "cafe_hopping", "night_market",
            "old_town_photo", "seaside_stroll", "mountain_town", "shopping_mall",
        }
        self.assertTrue(
            {item["family_id"] for item in plan["items"]} <= travel_families
        )

    def test_travel_recipe_rejects_product_mode_until_outfit_supply_exists(self):
        with self.assertRaisesRegex(PhotoContentPlanError, "不支持参考模式：PRODUCT"):
            plan_th_choice_batch(
                record_id="rec-travel", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
                theme=self.theme, reference_mode="PRODUCT", count=1,
            )

    def test_travel_doubao_contract_reuses_shared_vision_schema(self):
        profile = {
            "analysis_method": "doubao_seed_2_1", "presentation_type": "SCENE_MODEL",
            "palette": ["camel", "navy"], "temperature": "cool",
            "aggregate": {"background": "老城石板街"},
            "recommended_sets": [recommendation(1), recommendation(2)],
        }
        plan = plan_th_choice_batch(
            record_id="rec-travel-dynamic", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=self.theme, reference_mode="STYLE", count=2, style_profile=profile,
        )
        self.assertEqual([item["family_id"] for item in plan["items"]],
                         ["vision_dynamic_1", "vision_dynamic_2"])
        self.assertEqual(plan["items"][0]["copy"]["title"], "แฟชั่นวินเทจ 1")


if __name__ == "__main__":
    unittest.main()
