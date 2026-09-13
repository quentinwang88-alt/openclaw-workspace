"""Registry-driven routing for the daily hot→cold transition line."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_content_recipes
from services.feishu_workflow import (
    FIELD_DRESS_CODE, FIELD_TRANSITION_SCENE, FIELD_TRANSITION_SENSITIVITY,
    ProductionPresetCatalog, resolve_thermal_transition_variables,
)
from services.photo_content_planner import (
    PhotoContentPlanError, get_planning_flow, load_planning_policy,
    validate_batch_plan,
)
from services.photo_flow_registry import (
    PhotoFlowRegistryError, TRAVEL_TWO_STEP_FLOW, get_photo_flow_handler,
    is_layered_progression_flow, is_thermal_transition_flow,
    resolve_required_roles, role_marker, validate_ordered_roles,
)
from services.photo_theme import (
    THEME_OPTIONS, build_theme_copy, resolve_photo_theme, style_look_specs,
)
from services.photo_thermal_transition_flow import build_thermal_transition_content_plan
from services.photo_planner import PhotoReusePlannerService


RECIPE_ID = "PHOTO_TH_THERMAL_TRANSITION_V1"
TRAVEL_RECIPE_ID = "PHOTO_TH_TRAVEL_OUTFIT_V2"
RETIRED_RECIPE_ID = "PHOTO_TH_TEMPERATURE_DRESSING_V2"
VARIABLES = {
    "transition_key": "outdoor_bts_office", "thermal_sensitivity": "normal",
    "dress_code": "office", "style_series": "minimal_city",
    "temperature_label_mode": "QUALITATIVE",
}


class ThermalTransitionRoutingTest(unittest.TestCase):
    def test_registry_predicates_separate_the_three_layered_lines(self):
        self.assertTrue(is_thermal_transition_flow("thermal_transition_two_step"))
        self.assertTrue(is_layered_progression_flow("thermal_transition_two_step"))
        self.assertTrue(is_layered_progression_flow("layering_two_step"))
        self.assertFalse(is_thermal_transition_flow("layering_two_step"))
        self.assertFalse(is_layered_progression_flow(TRAVEL_TWO_STEP_FLOW))
        self.assertFalse(is_layered_progression_flow("reference_contract_v1"))
        self.assertFalse(is_thermal_transition_flow("reference_contract_v1"))

    def test_unknown_flow_never_silently_falls_back(self):
        with self.assertRaises(PhotoFlowRegistryError):
            get_photo_flow_handler("thermal_transition_v2")

    def test_thermal_flow_requires_explicit_roles(self):
        with self.assertRaises(PhotoFlowRegistryError):
            resolve_required_roles(planning_flow="thermal_transition_two_step")
        self.assertEqual(
            resolve_required_roles(
                planning_flow="thermal_transition_two_step",
                required_roles=("base", "mid", "outer"),
            ),
            ("base", "mid", "outer"),
        )

    def test_planner_policy_registry_resolves_the_new_recipe(self):
        self.assertEqual(
            get_planning_flow(RECIPE_ID), "thermal_transition_two_step"
        )
        policy = load_planning_policy(RECIPE_ID)
        self.assertEqual(policy["recipe_ids"], [RECIPE_ID])
        self.assertEqual(policy["supported_reference_modes"], ["COMPLETE_LOOK"])
        self.assertEqual(policy["supported_theme_keys"], ["THERMAL_TRANSITION"])
        self.assertIs(policy["canary_only"], True)
        self.assertEqual(policy["presentation_order"], ["base", "mid", "outer"])
        self.assertEqual(policy["generation_order"], ["outer", "mid", "base"])
        self.assertEqual(
            [family["family_id"] for family in policy["families"]],
            ["outdoor_bts_office_minimal_city"],
        )

    def test_existing_lines_keep_their_own_flows(self):
        self.assertEqual(get_planning_flow(TRAVEL_RECIPE_ID), "travel_two_step")
        # 2026-09-13: the layering line was retired with no RDS deployment, so
        # its flow must resolve to nothing rather than inherit another line's.
        self.assertEqual(get_planning_flow(RETIRED_RECIPE_ID), "")
        # A recipe without a planning policy stays unmanaged (blank flow) and
        # must never inherit another line's flow.
        self.assertEqual(get_planning_flow("PHOTO_MX_PICK_YOUR_HAIR_V2"), "")
        with self.assertRaises(PhotoContentPlanError):
            load_planning_policy("PHOTO_MX_PICK_YOUR_HAIR_V2")

    def test_role_markers_keep_legacy_letters_and_name_new_roles(self):
        self.assertEqual(role_marker("look_a", 0), "A")
        self.assertEqual(role_marker("look_d", 3), "D")
        self.assertEqual(role_marker("base", 0), "base")
        self.assertEqual(role_marker("outer", 2), "outer")

    def test_validate_ordered_roles_rejects_a_reordered_stack(self):
        _flow, roles = "thermal_transition_two_step", ("base", "mid", "outer")
        good = [{"role": role} for role in roles]
        validate_ordered_roles(
            good, planning_flow="thermal_transition_two_step", required_roles=roles
        )
        with self.assertRaises(PhotoFlowRegistryError):
            validate_ordered_roles(
                [{"role": "mid"}, {"role": "base"}, {"role": "outer"}],
                planning_flow="thermal_transition_two_step", required_roles=roles,
            )

    def test_theme_resolves_to_the_transition_profile_only(self):
        theme = resolve_photo_theme("冷热切换")
        self.assertEqual(theme["theme_key"], "THERMAL_TRANSITION")
        self.assertEqual(theme["hook_strategy"], "thermal_contrast")
        self.assertEqual(theme["label_zh"], "日常冷热切换穿搭")
        # The older layering theme must stay available and distinct.
        self.assertEqual(
            resolve_photo_theme("温度穿搭")["theme_key"], "TEMPERATURE_DRESSING"
        )
        for alias in ("冷热", "空调穿搭", "thermal transition"):
            self.assertEqual(
                resolve_photo_theme(alias)["theme_key"], "THERMAL_TRANSITION"
            )
        self.assertIn("冷热切换", THEME_OPTIONS)
        self.assertIn("温度穿搭", THEME_OPTIONS)

    def test_theme_copy_uses_the_frozen_role_markers(self):
        _recipe, _spec, plan = self._frozen_plan()
        post = plan["items"][0]
        assets = [{
            "role": role,
            "display_label": {"th-TH": label},
        } for role, label in zip(
            ("base", "mid", "outer"),
            ("ข้างนอกร้อน", "รถไฟฟ้า/ห้างเย็น", "ออฟฟิศแอร์แรง"),
        )]
        variation = {
            "planning_flow": "thermal_transition_two_step",
            "required_roles": ["base", "mid", "outer"],
            "looks": post["looks"],
            "copy": post["copy"],
        }
        copy_block = build_theme_copy(
            resolve_photo_theme("冷热切换"), assets, variation
        )
        self.assertEqual(len(copy_block["slide_texts"]), 5)
        self.assertEqual(copy_block["language_review_status"], "DRAFT")
        specs = style_look_specs(resolve_photo_theme("冷热切换"), variation)
        self.assertEqual([item["role"] for item in specs], ["base", "mid", "outer"])

    def test_style_look_specs_rejects_a_reordered_plan(self):
        _recipe, _spec, plan = self._frozen_plan()
        post = plan["items"][0]
        with self.assertRaisesRegex(ValueError, "冻结内容计划"):
            style_look_specs(resolve_photo_theme("冷热切换"), {
                "planning_flow": "thermal_transition_two_step",
                "required_roles": ["base", "mid", "outer"],
                "looks": [post["looks"][1], post["looks"][0], post["looks"][2]],
            })

    def test_frozen_plan_passes_batch_validation(self):
        _recipe, _spec, plan = self._frozen_plan()
        # The shared validator must accept the new flow without an A/B/C/D
        # assumption and must not rewrite the frozen item.
        validate_batch_plan(plan)
        self.assertEqual(plan["items"][0]["required_roles"], ["base", "mid", "outer"])

    def test_triptych_layout_variant_is_mapped(self):
        # The recipe declares the legacy story variant TRIPTYCH; the executable
        # page layout always comes from the frozen content card, but the
        # variant map must not silently degrade it to `single`.
        import inspect

        source = inspect.getsource(PhotoReusePlannerService)
        self.assertIn('"TRIPTYCH": "triptych_3"', source)
        recipe = next(
            item for item in load_content_recipes() if item.recipe_id == RECIPE_ID
        )
        variants = {
            str(story.get("layout_variant") or "")
            for story in recipe.story_structure_json
        }
        self.assertEqual(variants, {"TRIPTYCH", "FULL_BLEED"})

    def test_shipped_preset_for_the_new_line_is_disabled(self):
        path = PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        catalog = ProductionPresetCatalog(path)
        by_recipe = {
            str(task.get("recipe_id") or ""): item
            for item in payload["presets"]
            for task in item.get("tasks") or []
        }
        self.assertIn(RECIPE_ID, by_recipe)
        entry = by_recipe[RECIPE_ID]
        self.assertEqual(entry.get("status"), "disabled")
        self.assertNotIn(entry["name"], catalog.names)
        self.assertEqual(entry.get("default_product_mode"), "NO_PRODUCT")
        self.assertEqual(entry.get("default_asset_mode"), "ASSET_REUSE")
        self.assertEqual(entry["tasks"][0]["hook_strategy"], "thermal_contrast")
        # 2026-09-13: the superseded temperature preset was removed together
        # with its recipe (it never reached RDS), so no preset may reference it.
        self.assertNotIn(RETIRED_RECIPE_ID, by_recipe)

    def test_transition_fields_are_declared_locally(self):
        from scripts.ensure_feishu_task_table import task_field_specs

        catalog = ProductionPresetCatalog(
            PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        )
        specs = {name: (field_type, prop)
                 for name, field_type, _ui, prop in task_field_specs(catalog)}
        expected = {
            FIELD_TRANSITION_SCENE: [
                "室外热→BTS→办公室空调", "室外热→商场→影院", "校园室外→教室",
            ],
            FIELD_TRANSITION_SENSITIVITY: ["怕冷", "正常体感", "怕热"],
            FIELD_DRESS_CODE: ["办公室", "校园", "周末"],
        }
        for name, values in expected.items():
            self.assertIn(name, specs)
            self.assertEqual(
                [option["name"] for option in specs[name][1]["options"]], values
            )
        # Every declared option must be accepted by the resolver.
        for scene in expected[FIELD_TRANSITION_SCENE]:
            self.assertEqual(
                resolve_thermal_transition_variables({
                    FIELD_TRANSITION_SCENE: scene,
                    FIELD_TRANSITION_SENSITIVITY: "正常体感",
                    FIELD_DRESS_CODE: "办公室",
                })["transition_key"],
                {
                    "室外热→BTS→办公室空调": "outdoor_bts_office",
                    "室外热→商场→影院": "outdoor_mall_cinema",
                    "校园室外→教室": "campus_outdoor_classroom",
                }[scene],
            )
        for dress in expected[FIELD_DRESS_CODE]:
            self.assertTrue(
                resolve_thermal_transition_variables({
                    FIELD_TRANSITION_SCENE: "室外热→BTS→办公室空调",
                    FIELD_TRANSITION_SENSITIVITY: "正常体感",
                    FIELD_DRESS_CODE: dress,
                })["dress_code"]
            )

    @staticmethod
    def _frozen_plan():
        recipe = next(
            item for item in load_content_recipes() if item.recipe_id == RECIPE_ID
        )
        spec = dict(recipe.recipe_spec_json or {})
        plan = build_thermal_transition_content_plan(
            record_id="thermal-routing", recipe_id=RECIPE_ID, recipe_spec=spec,
            policy=load_planning_policy(RECIPE_ID),
            theme=resolve_photo_theme("冷热切换"), reference_mode="COMPLETE_LOOK",
            variables=dict(VARIABLES),
        )
        return recipe, spec, plan


if __name__ == "__main__":
    unittest.main()
