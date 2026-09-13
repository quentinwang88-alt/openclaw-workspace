from __future__ import annotations

import copy
import tempfile
import unittest
import sys
from pathlib import Path
from types import SimpleNamespace

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_board_layouts, load_content_recipes
from domain.photo_contracts import validate_variables, validate_execution_profiles
from services.asset_set_service import AssetSetService, AssetSetError
from services.photo_request_factory import (
    PhotoRequestFactory, PhotoRequestError, apply_travel_single_cover,
    fingerprint, validate_frozen_request,
)
from services.photo_planner import PhotoReusePlannerService
from test_photo_planner import PlannerRepo, PACKAGE_ROOT


class VariableContractTest(unittest.TestCase):
    def test_enum_values_and_scalar_type_are_enforced(self):
        schema = {"height": {"type": "enum", "values": [150, 155], "required": True}}
        self.assertEqual(validate_variables(schema, {"height": 155}), [])
        for value in (160, "155", True, 155.0, None, [], {}):
            with self.subTest(value=value):
                self.assertTrue(validate_variables(schema, {"height": value}))
        self.assertTrue(validate_variables(schema, {}))
        self.assertTrue(validate_variables(schema, {"height": 150, "unknown": 1}))

    def test_sets_are_unique_subsets_and_fixed_sets_are_complete(self):
        for kind in ("set", "fixed_set"):
            schema = {"v": {"type": kind, "values": ["a", "b"], "required": True}}
            self.assertEqual(validate_variables(schema, {"v": ["b", "a"]}), [])
            for value in ("a", [], ["a", "a"], ["a", "c"], [True], {"a": 1}):
                self.assertTrue(validate_variables(schema, {"v": value}))
            self.assertEqual(bool(validate_variables(schema, {"v": ["a"]})), kind == "fixed_set")

    def test_primitive_types_and_legacy_enum(self):
        schema = {"s": {"type": "string"}, "i": {"type": "integer"}, "n": {"type": "number"},
                  "b": {"type": "boolean"}, "e": {"enum": ["yes"]}}
        self.assertFalse(validate_variables(schema, {"s": "x", "i": 2, "n": 2.5, "b": False, "e": "yes"}))
        self.assertTrue(validate_variables(schema, {"s": "", "i": True, "n": "2", "b": 1, "e": "no"}))
        self.assertTrue(validate_variables({"v": {"type": "set", "values": ["a", "a"]}}, {}))


class FactoryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = PlannerRepo(Path(self.temp.name))
        self.repo.asset_set.asset_set_key = "TH_WOMENSWEAR_CHOICE"
        from photo_content_fixture import qualify
        self.asset_sets = qualify(self.repo, Path(self.temp.name))
        self.repo.list_asset_sets = lambda **kwargs: list(self.asset_sets)
        self.repo.get_asset_set = lambda identity: next((a for a in self.asset_sets if a.asset_set_id == identity), None)
        self.layouts = load_board_layouts()
        self.spec = SimpleNamespace(recipe_id=self.repo.recipe.recipe_id, market="TH", language="th-TH", account_id="acct")
        self.factory = PhotoRequestFactory(self.repo, layouts=self.layouts)

    def build(self, count=1, **kwargs):
        return self.factory.build_batch(record_id="record1", specs=[self.spec] * count,
                                        category_key="womenswear", **kwargs)

    def test_auto_request_deterministic_and_same_sources_do_not_multiply_inventory(self):
        first = self.build()
        self.assertEqual(first, self.build())
        validate_frozen_request(first[0])
        self.assertIn("content_card", first[0])
        with self.assertRaisesRegex(PhotoRequestError, "整批未冻结"):
            self.build(2)

    def test_travel_first_look_becomes_cover_without_duplicate_detail_page(self):
        request = self.build()[0]
        request["recipe_id"] = "PHOTO_TH_TRAVEL_OUTFIT_V2"
        snapshot = dict(request["recipe_snapshot"])
        snapshot["recipe_id"] = request["recipe_id"]
        request["recipe_snapshot"] = snapshot
        request["request_sha256"] = fingerprint({
            key: value for key, value in request.items() if key != "request_sha256"
        })

        selected = apply_travel_single_cover(request, {
            "role": "look_c", "source": "human_presentation_observation",
            "reason_zh": "穿搭清楚，环境适合封面",
        })

        self.assertEqual(selected["content_card"]["pages"][0]["layout"], "single")
        self.assertEqual(len(selected["content_card"]["pages"]), 4)
        self.assertEqual(
            selected["content_card"]["pages"][0]["source_roles"], ["look_a"]
        )
        self.assertEqual(selected["cover_selection"]["role"], "look_a")
        self.assertEqual(selected["cover_selection"]["source"], "fixed_first_look")
        self.assertEqual(
            [page["source_roles"] for page in selected["content_card"]["pages"]],
            [["look_a"], ["look_b"], ["look_c"], ["look_d"]],
        )
        self.assertEqual(len(selected["copy"]["slide_texts"]), 4)
        validate_frozen_request(selected)

    def test_travel_cover_invalid_role_falls_back_and_non_travel_is_unchanged(self):
        request = self.build()[0]
        unchanged = apply_travel_single_cover(request, {"role": "look_c"})
        self.assertEqual(unchanged, request)

        request["recipe_id"] = "PHOTO_TH_TRAVEL_OUTFIT_V2"
        snapshot = dict(request["recipe_snapshot"])
        snapshot["recipe_id"] = request["recipe_id"]
        request["recipe_snapshot"] = snapshot
        selected = apply_travel_single_cover(request, {"role": "missing"})
        self.assertEqual(selected["cover_selection"]["role"], "look_a")
        self.assertEqual(selected["cover_selection"]["source"], "fixed_first_look")

    def test_latest_enabled_is_chosen_and_no_fallback_to_old_match(self):
        old = self.repo.asset_set
        latest = copy.deepcopy(old)
        latest.asset_set_id, latest.asset_set_version = "new-v2", 2
        self.asset_sets.append(latest)
        self.assertEqual(self.build()[0]["asset_set_id"], "new-v2")
        latest.tags_json["scene"] = ["Airport"]
        with self.assertRaisesRegex(AssetSetError, "NEEDS_ASSET"):
            self.build()
        latest.status = "disabled"
        self.assertEqual(self.build()[0]["asset_set_id"], old.asset_set_id)

    def test_same_latest_version_is_ambiguous(self):
        duplicate = copy.deepcopy(self.repo.asset_set)
        duplicate.asset_set_id = "duplicate"
        self.asset_sets.append(duplicate)
        with self.assertRaisesRegex(AssetSetError, "ambiguous"):
            self.build()

    def test_roles_are_ordered_independently_of_manifest_array(self):
        self.repo.asset_set.manifest_json["assets"].reverse()
        request = self.build()[0]
        result = PhotoReusePlannerService(self.repo).plan_task(self.repo.task.task_id,
            recipe_id=request["recipe_id"], variables=request["variables"], copy_block=request["copy"],
            layout=request["layout_snapshot"], asset_set_id=request["asset_set_id"],
            recipe_snapshot=request["recipe_snapshot"], asset_snapshot=request["asset_snapshot"])
        self.assertEqual([s["source_asset_id"] for s in result["plan"]["shots"]], [f"look-{i}" for i in range(2, 6)])

    def test_missing_or_duplicate_role_blocks_and_matching_cannot_cross_market(self):
        self.repo.asset_set.manifest_json["assets"][0]["role"] = "look_b"
        with self.assertRaises(AssetSetError):
            self.build()
        self.repo.asset_set.manifest_json["assets"][0]["role"] = "look_a"
        self.repo.asset_set.market = "MX"
        with self.assertRaises(AssetSetError):
            self.build()

    def test_required_pair_relation_and_members_are_checked(self):
        requirements = {"required_roles": ["look_a", "look_b", "look_c", "look_d"],
                        "required_pairs": [{"roles": ["look_a", "look_b"], "relation": "same_identity"}]}
        with self.assertRaisesRegex(AssetSetError, "relationship"):
            AssetSetService.validate_requirements(self.repo.asset_set, requirements)
        self.repo.asset_set.manifest_json["pairs"] = [{"asset_ids": ["look-2", "look-3"], "relation": "same_identity"}]
        AssetSetService.validate_requirements(self.repo.asset_set, requirements)

    def test_override_is_validated_and_cannot_bypass_tags_or_roles(self):
        with self.assertRaisesRegex(PhotoRequestError, "不允许自由覆盖文案"):
            self.build(overrides=[{"copy": {"caption": "caption override"}, "asset_set_id": self.repo.asset_set.asset_set_id}])
        request = self.build(overrides=[{"asset_set_id": self.repo.asset_set.asset_set_id}])[0]
        self.assertEqual(request["asset_set_id"], self.repo.asset_set.asset_set_id)
        with self.assertRaises(PhotoRequestError):
            self.build(overrides=[{"variables": {"scene": "BAD"}}])
        with self.assertRaises(AssetSetError):
            self.build(overrides=[{"variables": {"scene": "Airport"}, "asset_set_id": self.repo.asset_set.asset_set_id}])
        with self.assertRaises(PhotoRequestError):
            self.build(overrides=[{"account_id": "other"}])

    def test_only_declared_visual_variables_match_assets(self):
        spec = self.repo.recipe.recipe_spec_json
        spec["variables_schema"]["logic"] = {"type": "enum", "values": ["soft"], "required": True}
        for profile in spec["execution_profiles"]:
            profile["variables"]["logic"] = "soft"
        self.assertEqual(self.build()[0]["variables"]["logic"], "soft")
        spec["asset_match_keys"].append("logic")
        with self.assertRaises(AssetSetError):
            self.build()

    def test_frozen_request_survives_config_rollover_but_rejects_changed_bytes(self):
        request = self.build()[0]
        self.repo.asset_set.status = "disabled"
        self.repo.recipe.recipe_spec_json["execution_profiles"] = []
        frozen = AssetSetService(self.repo).from_frozen(request["asset_snapshot"], category_key="womenswear", market="TH",
            tags=request["variables"], requirements={"required_roles": ["look_a", "look_b", "look_c", "look_d"]})
        self.assertEqual(frozen.status, "enabled")
        Path(frozen.manifest_json["assets"][0]["path"]).write_bytes(b"changed")
        with self.assertRaisesRegex(AssetSetError, "changed"):
            AssetSetService(self.repo).from_frozen(request["asset_snapshot"], category_key="womenswear", market="TH",
                tags=request["variables"], requirements={})
        request["copy"]["caption"] = "changed"
        with self.assertRaisesRegex(PhotoRequestError, "fingerprint"):
            validate_frozen_request(request)

    def test_newest_enabled_missing_file_is_not_ignored_for_old_version(self):
        latest = copy.deepcopy(self.repo.asset_set)
        latest.asset_set_id, latest.asset_set_version = "v2", 2
        latest.manifest_json["assets"][0]["path"] = "/nonexistent/photo.jpg"
        self.asset_sets.append(latest)
        with self.assertRaises(AssetSetError):
            self.build()


class ShippedProfileTest(unittest.TestCase):
    def test_shipped_recipes_have_valid_profiles_copy_and_explicit_visual_keys(self):
        recipes = [r for r in load_content_recipes() if r.recipe_id.startswith("PHOTO_")]
        # 2026-09-12: +1 dynamic-input recipe = daily thermal transition V1.
        # 2026-09-13: temperature-layering V2 retired (never deployed to RDS).
        # 2026-09-13 (VN scarf Phase 2): +1 country-agnostic travel V3 canary.
        # 2026-09-13 (VN scarf Phase 4): +1 country-agnostic matching V3 canary.
        self.assertEqual(len(recipes), 15)
        for recipe in recipes:
            with self.subTest(recipe=recipe.recipe_id):
                self.assertEqual(validate_execution_profiles(recipe.recipe_spec_json), [])
                self.assertGreaterEqual(len(recipe.recipe_spec_json["execution_profiles"]), 1)

    def test_preflight_separates_contract_errors_from_missing_asset_coverage(self):
        from scripts.preflight_native_photo import preflight
        result = preflight(PACKAGE_ROOT / "config")
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["external_writes"], 0)
        # 2026-09-13 (VN scarf Phase 2): +1 country-agnostic travel V3 canary,
        # which adds one execution profile and no market binding yet.
        # 2026-09-13 (VN scarf Phase 4): +1 country-agnostic matching V3 canary
        # adds another recipe and execution profile, both still market-unbound.
        self.assertEqual(result["recipe_count"], 15)
        self.assertEqual(result["profile_count"], 31)
        self.assertEqual(
            result["canary_market_unbound"],
            ["PHOTO_MATCHING_CHOICE_V3", "PHOTO_TRAVEL_OUTFIT_V3"],
        )
        self.assertNotIn("PHOTO_TRAVEL_OUTFIT_V3", result["needs_asset"])
        self.assertNotIn("PHOTO_MATCHING_CHOICE_V3", result["needs_asset"])
        self.assertIn("PHOTO_MX_FACE_SHAPE_MATCH_V1", result["needs_asset"])
        # The MX wig recipe ships without a seeded MX_WIG_CHOICE_GEN set;
        # per-row supply generates and qualifies its assets, so it lands in
        # needs_asset like the other unseeded MX recipes.
        self.assertIn("PHOTO_MX_PICK_YOUR_HAIR_V2", result["needs_asset"])
        # 2026-09-13: the retired layering recipe is gone; the daily
        # thermal-transition line takes the dynamic-input slot.
        self.assertNotIn(
            "PHOTO_TH_TEMPERATURE_DRESSING_V2", result["dynamic_input_required"]
        )
        self.assertIn("PHOTO_TH_THERMAL_TRANSITION_V1", result["dynamic_input_required"])


if __name__ == "__main__":
    unittest.main()
