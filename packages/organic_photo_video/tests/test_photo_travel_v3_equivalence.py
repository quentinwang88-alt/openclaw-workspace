#!/usr/bin/env python3
"""Offline equivalence: TH + WOMENSWEAR bound to V3 must behave as V2 does.

Phase 2 of the VN scarf cross-market plan adds ``PHOTO_TRAVEL_OUTFIT_V3``: the
same executable travel rules as ``PHOTO_TH_TRAVEL_OUTFIT_V2``, but with the
market, the category and every publish label moved out of the recipe.  This
module binds V3 to TH + womenswear — the only shipped category — and proves:

* the structural contract (story, assets, variables, travel moments, pages) is
  unchanged from V2;
* the TH Locale Pack reproduces V2's inline Thai tables exactly, so a V3 plan
  resolves the same copy without any ``_th`` field;
* V2 itself is untouched and keeps its legacy inline tables and live route.

No image generation, no Feishu, no network: this is the offline half of the
Phase 2 acceptance.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config import loader  # noqa: E402
from domain.contracts import validate_photo_execution_context_payload  # noqa: E402
from services import photo_content_planner as planner  # noqa: E402
from services.photo_category_registry import WOMENSWEAR_V1  # noqa: E402
from services.photo_copy import contract_copy_tokens, fill_travel_copy_tokens  # noqa: E402

V2_ID = "PHOTO_TH_TRAVEL_OUTFIT_V2"
V3_ID = "PHOTO_TRAVEL_OUTFIT_V3"
V2_RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / f"{V2_ID}.json"
V3_RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / f"{V3_ID}.json"
LOCALE_PACK_PATH = PACKAGE_ROOT / "config" / "locales" / "LOCALE_TH_TH_V1.json"
DESTINATION_PATH = PACKAGE_ROOT / "config" / "destinations" / "EAST_ASIA_COOL_V1.json"
FIXTURE_PATH = TESTS_DIR / "fixtures" / "PHASE2_EXECUTION_CONTEXT_TH_WOMENSWEAR_V3.json"

MOMENT_EXECUTABLE_FIELDS = (
    "key", "label_zh", "evidence_zh", "mobility_level",
    "allowed_footwear_types", "forbidden_footwear_types",
)
LOOK_GARMENT_FIELDS = (
    "role", "outerwear", "top_inner", "bottom", "shoes",
    "outerwear_type", "bottom_type",
)
FAMILY_FIELDS = (
    "family_id", "angle_zh", "scene_zh", "palette_zh",
    "background_color", "background_prompt",
)
THEME = {
    "theme_key": "COOL_WEATHER_TRAVEL",
    "visual_brief": "凉爽城市旅行穿搭",
    "cta": "คุณเลือก A B C หรือ D?",
}


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class TravelV3BoundaryTest(unittest.TestCase):
    """The recipe declares what it needs instead of fixing TH/womenswear."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.v3 = loader.load_content_recipe_file(V3_RECIPE_PATH)

    def test_recipe_is_v2_and_shipped_as_a_draft_canary(self):
        self.assertEqual(self.v3.recipe_id, V3_ID)
        self.assertEqual(self.v3.status, "draft")
        spec = self.v3.recipe_spec_json
        self.assertEqual(spec["schema_version"], "opv-photo-recipe-v2")
        self.assertEqual(spec["market_policy"], "MARKET_PACK_REQUIRED")
        self.assertEqual(spec["planning_flow"], "travel_two_step")
        self.assertEqual(spec["media_kind"], "native_photo")

    def test_recipe_no_longer_fixes_market_category_or_locale(self):
        spec = self.v3.recipe_spec_json
        self.assertNotIn("markets", spec)
        self.assertNotIn("category_key", spec)
        self.assertNotIn("locale", spec)
        self.assertNotIn("locale", self.v3.copy_style_json)
        self.assertEqual(spec["locale_copy_packs"], {"th-TH": "TH_TRAVEL_OUTFIT_V3"})

    def test_required_capabilities_are_provided_by_womenswear(self):
        required = set(self.v3.recipe_spec_json["required_category_capabilities"])
        self.assertTrue(required)
        self.assertLessEqual(required, set(WOMENSWEAR_V1.capabilities))

    def test_travel_contract_carries_no_publish_language(self):
        travel = self.v3.recipe_spec_json["travel_contract"]
        self.assertNotIn("destination_labels_th", travel)
        self.assertNotIn("temperature_labels_th", travel)
        for moment in travel["moments"]:
            self.assertNotIn("label_th", moment, moment["key"])


class TravelV3StructuralEquivalenceTest(unittest.TestCase):
    """Every executable rule of V2 survives the country-agnostic rewrite."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.v2 = loader.load_content_recipe_file(V2_RECIPE_PATH)
        cls.v3 = loader.load_content_recipe_file(V3_RECIPE_PATH)
        cls.v2_policy = planner.load_planning_policy(V2_ID)
        cls.v3_policy = planner.load_planning_policy(V3_ID)
        cls.locale_pack = loader.load_locale_pack_file(LOCALE_PACK_PATH)

    def test_story_structure_and_shot_contract_match(self):
        v2, v3 = self.v2, self.v3
        self.assertEqual(v2.shot_count, v3.shot_count)
        self.assertEqual(v2.anchor_slot, v3.anchor_slot)
        self.assertEqual(v2.story_structure_json, v3.story_structure_json)
        self.assertEqual(v2.hook_types_json, v3.hook_types_json)
        self.assertEqual(v2.content_goal, v3.content_goal)
        self.assertEqual(v2.quality_profile_id, v3.quality_profile_id)

    def test_asset_and_variable_contract_match(self):
        v2 = self.v2.recipe_spec_json
        v3 = self.v3.recipe_spec_json
        self.assertEqual(v2["template_id"], v3["template_id"])
        self.assertEqual(v2["template_version"], v3["template_version"])
        self.assertEqual(v2["asset_policy"], v3["asset_policy"])
        self.assertEqual(v2["asset_match_keys"], v3["asset_match_keys"])
        self.assertEqual(v2["asset_requirements"], v3["asset_requirements"])
        self.assertEqual(v2["variables_schema"], v3["variables_schema"])
        self.assertEqual(v2["supported_presentations"], v3["supported_presentations"])
        self.assertEqual(v2["visual_rules"], v3["visual_rules"])
        self.assertEqual(v2["theme_types"], v3["theme_types"])
        self.assertEqual(v2["product_modes"], v3["product_modes"])

    def test_execution_profile_variables_match(self):
        v2 = self.v2.recipe_spec_json["execution_profiles"][0]
        v3 = self.v3.recipe_spec_json["execution_profiles"][0]
        self.assertEqual(v2["profile_id"], v3["profile_id"])
        self.assertEqual(v2["variables"], v3["variables"])
        self.assertEqual(v2["asset_set_keys"], v3["asset_set_keys"])

    def test_travel_contract_executable_fields_match(self):
        v2 = self.v2.recipe_spec_json["travel_contract"]
        v3 = self.v3.recipe_spec_json["travel_contract"]
        self.assertEqual(v2["schema_version"], v3["schema_version"])
        self.assertEqual(v2["moments_per_post"], v3["moments_per_post"])
        self.assertEqual(v2["look_required_fields"], v3["look_required_fields"])
        self.assertEqual(v2["footwear_types"], v3["footwear_types"])
        self.assertEqual(len(v2["moments"]), len(v3["moments"]))
        for left, right in zip(v2["moments"], v3["moments"]):
            for field in MOMENT_EXECUTABLE_FIELDS:
                self.assertEqual(left[field], right[field], f"{right['key']}.{field}")

    def test_content_card_pages_match(self):
        v2 = self.v2.recipe_spec_json["content_card"]
        v3 = self.v3.recipe_spec_json["content_card"]
        self.assertEqual(v2["pages"], v3["pages"])
        for field in ("summary_zh", "question_zh", "comparison_basis_zh", "logic_key"):
            self.assertEqual(v2[field], v3[field], field)

    def test_policy_keeps_garment_rules(self):
        v2 = self.v2_policy
        v3 = self.v3_policy
        self.assertEqual(v3["policy_id"], "TRAVEL_OUTFIT_V3")
        self.assertEqual(v3["recipe_ids"], [V3_ID])
        self.assertEqual(v3["planning_flow"], v2["planning_flow"])
        for field in ("supported_reference_modes", "supported_theme_keys",
                      "minimum_cross_post_axis_difference", "theme_family_order"):
            self.assertEqual(v2[field], v3[field], field)
        self.assertEqual(len(v2["families"]), len(v3["families"]))
        for left, right in zip(v2["families"], v3["families"]):
            for field in FAMILY_FIELDS:
                self.assertEqual(left[field], right[field], f"{right['family_id']}.{field}")
            self.assertEqual(len(left["looks"]), len(right["looks"]))
            for look_left, look_right in zip(left["looks"], right["looks"]):
                for field in LOOK_GARMENT_FIELDS:
                    self.assertEqual(
                        look_left[field], look_right[field],
                        f"{right['family_id']}.{look_right['role']}.{field}",
                    )

    def test_policy_drops_only_locale_copy(self):
        for family in self.v3_policy["families"]:
            for field in ("title_th", "cover_th", "caption_th"):
                self.assertNotIn(field, family, family["family_id"])
            for look in family["looks"]:
                self.assertNotIn("display_label", look)

    def test_th_pack_covers_every_v3_moment_and_family(self):
        travel = self.v3.recipe_spec_json["travel_contract"]
        labels = self.locale_pack["labels"]["travel_moments"]
        for moment in travel["moments"]:
            self.assertTrue(labels.get(moment["key"]), moment["key"])
        for family in self.v3_policy["families"]:
            entry = self.locale_pack["family_copy"][family["family_id"]]
            for field in ("title", "cover", "caption"):
                self.assertTrue(entry[field], f"{family['family_id']}.{field}")
            roles = {look["role"] for look in family["looks"]}
            self.assertEqual(roles, set(entry["look_labels"]))


class TravelV3ThWomenswearEquivalenceTest(unittest.TestCase):
    """Binding V3 to TH + womenswear resolves exactly the V2 copy."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.v2 = loader.load_content_recipe_file(V2_RECIPE_PATH)
        cls.v3 = loader.load_content_recipe_file(V3_RECIPE_PATH)
        cls.v2_policy = planner.load_planning_policy(V2_ID)
        cls.v3_policy = planner.load_planning_policy(V3_ID)
        cls.locale_pack = loader.load_locale_pack_file(LOCALE_PACK_PATH)

    def test_copy_tokens_resolve_identically_without_th_tables(self):
        variables = {"destination": "beijing", "temperature_band": "5_10c"}
        legacy = contract_copy_tokens(self.v2.recipe_spec_json, variables)
        bound = contract_copy_tokens(
            self.v3.recipe_spec_json, variables, locale_pack=self.locale_pack
        )
        self.assertEqual(legacy, bound)
        self.assertTrue(all(legacy.values()))

    def test_template_filling_matches_the_v2_result(self):
        template = "ไอเดียแต่งตัวเที่ยว{destination} อากาศ {temperature}"
        variables = {"destination": "osaka", "temperature_band": "0_5c"}
        legacy = fill_travel_copy_tokens(
            template, travel_contract=self.v2.recipe_spec_json["travel_contract"],
            variables=variables,
        )
        bound = fill_travel_copy_tokens(
            template, travel_contract=self.v3.recipe_spec_json["travel_contract"],
            variables=variables, locale_pack=self.locale_pack,
        )
        self.assertEqual(legacy, bound)
        self.assertIn("โอซาก้า", bound)
        self.assertIn("0-5°C", bound)

    def test_v3_copy_pack_reproduces_the_th_templates(self):
        v2_profile = self.v2.recipe_spec_json["execution_profiles"][0]
        v3_profile = self.v3.recipe_spec_json["execution_profiles"][0]
        v2_by_id = {item["copy_id"]: item["copy"] for item in v2_profile["copy_variants"]}
        v3_by_id = {item["copy_id"]: item["copy"] for item in v3_profile["copy_variants"]}
        self.assertEqual(set(v2_by_id), set(v3_by_id))
        self.assertEqual(v2_by_id, v3_by_id)
        self.assertEqual(
            sorted(v3_profile["copy_variants_by_locale"]), ["th-TH"]
        )

    def test_family_plan_is_identical_under_the_th_binding(self):
        v2_by_id = {item["family_id"]: item for item in self.v2_policy["families"]}
        v3_by_id = {item["family_id"]: item for item in self.v3_policy["families"]}
        self.assertEqual(set(v2_by_id), set(v3_by_id))
        for family_id in sorted(v2_by_id):
            with self.subTest(family=family_id):
                legacy = planner._family_plan(
                    index=1, family=v2_by_id[family_id], theme=THEME,
                    policy=self.v2_policy, reference_mode="STYLE",
                )
                bound = planner._family_plan(
                    index=1, family=v3_by_id[family_id], theme=THEME,
                    policy=self.v3_policy, reference_mode="STYLE",
                    locale_pack=self.locale_pack,
                )
                self.assertEqual(legacy["copy"], bound["copy"])
                self.assertEqual(legacy["looks"], bound["looks"])
                for field in ("angle_zh", "scene_zh", "palette_zh",
                              "background_color", "background_prompt",
                              "difference_axes"):
                    self.assertEqual(legacy[field], bound[field], field)

    def test_v3_plans_through_the_generic_policy_registry(self):
        self.assertTrue(planner.recipe_has_planning_policy(V3_ID))
        self.assertEqual(planner.get_planning_flow(V3_ID), "travel_two_step")
        self.assertEqual(planner.get_planning_flow(V2_ID), "travel_two_step")


class V2RouteUntouchedTest(unittest.TestCase):
    """V2 keeps its live route and its legacy inline tables."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.v2 = loader.load_content_recipe_file(V2_RECIPE_PATH)
        cls.v2_policy = planner.load_planning_policy(V2_ID)

    def test_v2_is_still_active_with_its_original_policy(self):
        self.assertEqual(self.v2.status, "active")
        self.assertEqual(self.v2_policy["policy_id"], "TH_TRAVEL_OUTFIT_V1")
        self.assertEqual(self.v2_policy["recipe_ids"], [V2_ID])

    def test_v2_contract_keeps_its_inline_thai_tables(self):
        travel = self.v2.recipe_spec_json["travel_contract"]
        self.assertTrue(travel["destination_labels_th"])
        self.assertTrue(travel["temperature_labels_th"])
        for moment in travel["moments"]:
            self.assertTrue(moment["label_th"], moment["key"])

    def test_v2_policy_keeps_inline_thai_copy(self):
        for family in self.v2_policy["families"]:
            for field in ("title_th", "cover_th", "caption_th"):
                self.assertTrue(family[field], f"{family['family_id']}.{field}")
            for look in family["looks"]:
                self.assertTrue(look["display_label"])

    def test_v2_recipe_spec_is_still_v1(self):
        spec = self.v2.recipe_spec_json
        self.assertEqual(spec["schema_version"], "opv-photo-recipe-v1")
        self.assertEqual(spec["category_key"], "womenswear")
        self.assertEqual(spec["markets"], ["TH"])


class ExecutionContextContractTest(unittest.TestCase):
    """The frozen resolved snapshot (§4) rejects an incomplete bind."""

    @staticmethod
    def _snapshot() -> dict:
        return {
            "schema_version": "opv-photo-execution-context-v1",
            "recipe": {"id": V3_ID, "version": 1, "planning_flow": "travel_two_step"},
            "category": {
                "key": "womenswear", "profile_id": "WOMENSWEAR_V1",
                "profile_version": 1, "main_product_slot": "outerwear",
            },
            "market": {
                "country": "TH", "market_pack_id": "MP_TH_DEFAULT_V1",
                "market_pack_version": 1,
            },
            "locale": {
                "locale": "th-TH", "locale_pack_id": "LOCALE_TH_TH_V1",
                "locale_pack_version": 1, "copy_pack_id": "TH_TRAVEL_OUTFIT_V3",
            },
            "destination": {
                "destination_id": "SEOUL_WINTER", "destination_country": "KR",
                "destination_city": "Seoul",
            },
            "reference": {
                "mode": "STYLE", "input_fingerprint": "abc", "reference_hashes": ["h1"],
            },
            "product": {},
            "persona": {
                "persona_ref_id": "p1", "persona_pack_id": "PACK_1",
                "reference_hashes": ["h2"],
            },
        }

    def test_complete_snapshot_is_valid(self):
        self.assertEqual(validate_photo_execution_context_payload(self._snapshot()), [])

    def test_missing_locale_pack_is_rejected(self):
        payload = self._snapshot()
        payload["locale"].pop("locale_pack_id")
        errors = validate_photo_execution_context_payload(payload)
        self.assertTrue(any("locale_pack_id" in error for error in errors), errors)

    def test_country_and_locale_must_be_iso_shaped(self):
        payload = self._snapshot()
        payload["market"]["country"] = "Thailand"
        payload["locale"]["locale"] = "th_TH"
        errors = validate_photo_execution_context_payload(payload)
        self.assertTrue(any("country" in error for error in errors), errors)
        self.assertTrue(any("locale" in error for error in errors), errors)

    def test_destination_catalog_can_supply_the_snapshot(self):
        catalog = loader.load_destination_catalog_file(DESTINATION_PATH)
        entry = next(
            item for item in catalog["destinations"]
            if item["destination_id"] == "SEOUL_WINTER"
        )
        payload = self._snapshot()
        payload["destination"] = {
            "destination_id": entry["destination_id"],
            "destination_country": entry["destination_country"],
            "destination_city": entry["destination_city"],
        }
        self.assertEqual(validate_photo_execution_context_payload(payload), [])


class ExecutionContextFixtureTest(unittest.TestCase):
    """Spec §14.2: Phase 2 must ship a resolved execution context fixture.

    The fixture is regenerated from the shipped configs on every run, so it can
    never drift into documentation that no longer matches the code.
    """

    FIXTURE_SEEDS = {
        "input_fingerprint": "phase2-fixture:TH+womenswear+V3+SEOUL_WINTER/STYLE",
        "style_reference": "phase2-fixture:style-reference-01",
        "persona_reference": "phase2-fixture:persona-reference-01",
    }

    @classmethod
    def rebuild(cls) -> dict:
        import hashlib
        from services.photo_category_registry import get_photo_category_adapter
        from services.photo_execution_context import build_execution_context

        def digest(seed: str) -> str:
            return hashlib.sha256(seed.encode("utf-8")).hexdigest()

        catalog = loader.load_destination_catalog_file(DESTINATION_PATH)
        seoul = next(
            entry for entry in catalog["destinations"]
            if entry["destination_id"] == "SEOUL_WINTER"
        )
        adapter = get_photo_category_adapter("womenswear")
        return build_execution_context(
            recipe_id=V3_ID,
            recipe_version=1,
            planning_flow="travel_two_step",
            category_key=adapter.category_key,
            category_profile_id="WOMENSWEAR_V1",
            category_profile_version=1,
            main_product_slot=adapter.main_product_slot,
            market_country="TH",
            market_pack_id="MP_TH_DEFAULT_V1",
            market_pack_version=1,
            locale="th-TH",
            locale_pack_id="LOCALE_TH_TH_V1",
            locale_pack_version=1,
            copy_pack_id="TH_TRAVEL_OUTFIT_V3",
            reference_mode="STYLE",
            input_fingerprint=digest(cls.FIXTURE_SEEDS["input_fingerprint"]),
            reference_hashes=[digest(cls.FIXTURE_SEEDS["style_reference"])],
            destination=seoul,
            product=None,
            persona={
                "persona_ref_id": "FIXTURE_PERSONA_REF_TH_FEMALE",
                "persona_pack_id": "FIXTURE_PERSONA_PACK_TH_FEMALE_V1",
                "reference_hashes": [digest(cls.FIXTURE_SEEDS["persona_reference"])],
            },
        )

    def test_fixture_matches_the_shipped_configs(self):
        fixture = _read(FIXTURE_PATH)
        rebuilt = self.rebuild()
        for key, value in rebuilt.items():
            self.assertEqual(fixture[key], value, key)
        self.assertIn("_fixture_note", fixture)

    def test_fixture_is_a_valid_frozen_snapshot(self):
        fixture = {
            key: value for key, value in _read(FIXTURE_PATH).items()
            if not key.startswith("_")
        }
        self.assertEqual(validate_photo_execution_context_payload(fixture), [])

    def test_womenswear_binding_uses_the_registry_main_slot(self):
        rebuilt = self.rebuild()
        self.assertEqual(rebuilt["category"]["main_product_slot"], WOMENSWEAR_V1.main_product_slot)
        self.assertEqual(rebuilt["category"]["key"], WOMENSWEAR_V1.category_key)


class ExecutionContextBuilderTest(unittest.TestCase):
    def test_missing_market_binding_fails_loudly(self):
        from services.photo_execution_context import (
            PhotoExecutionContextError, build_execution_context,
        )
        with self.assertRaises(PhotoExecutionContextError):
            build_execution_context(
                recipe_id=V3_ID, recipe_version=1, planning_flow="travel_two_step",
                category_key="womenswear", category_profile_id="WOMENSWEAR_V1",
                category_profile_version=1, main_product_slot="outerwear",
                market_country="", market_pack_id="MP_TH_DEFAULT_V1",
                market_pack_version=1,
                locale="th-TH", locale_pack_id="LOCALE_TH_TH_V1",
                locale_pack_version=1, copy_pack_id="TH_TRAVEL_OUTFIT_V3",
                reference_mode="STYLE", input_fingerprint="fp",
                persona={"persona_ref_id": "p", "persona_pack_id": "pack"},
            )

    def test_frozen_snapshot_may_not_change_silently(self):
        from services.photo_execution_context import (
            PhotoExecutionContextError, assert_unchanged,
        )
        fixture = {
            key: value for key, value in _read(FIXTURE_PATH).items()
            if not key.startswith("_")
        }
        assert_unchanged(fixture, json.loads(json.dumps(fixture)))
        mutated = json.loads(json.dumps(fixture))
        mutated["locale"]["locale_pack_version"] = 2
        with self.assertRaises(PhotoExecutionContextError):
            assert_unchanged(fixture, mutated)


if __name__ == "__main__":
    unittest.main()
