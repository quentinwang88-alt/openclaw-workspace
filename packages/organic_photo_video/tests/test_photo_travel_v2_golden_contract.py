"""TH V2 golden/contract baseline (Phase 0 of the VN-scarf cross-market spec).

Freezes the stable surfaces of ``PHOTO_TH_TRAVEL_OUTFIT_V2`` so the later
phases can prove zero business-behaviour change while they extract shared
infrastructure (``PhotoReferenceContext``, Category Adapter, Recipe v2,
Locale Pack):

* Phase 1 moves reference resolution behind a common facade.
* Phase 2 introduces ``PHOTO_TRAVEL_OUTFIT_V3`` while V2 must keep its
  legacy route, families, copy and A-D order untouched.
* Phase 3 adds the SCARF category adapter.

These tests are intentionally snapshots, not new behaviour.  When a snapshot
fails, the correct fix is *never* to silently re-baseline it: first decide
whether the change is a reviewed, intentional change to the legacy V2
contract.  If it is not, the change is a regression.

The spec being implemented lives at
``docs/VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC_20260913.md``.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from services.photo_content_planner import (
    PhotoContentPlanError, load_planning_policy, plan_th_choice_batch,
    recipe_has_planning_policy,
)
from services.photo_reference import (
    REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_PRODUCT, REFERENCE_MODE_STYLE,
    REFERENCE_TYPE_OPTIONS, resolve_reference_mode,
)
from services.photo_theme import resolve_photo_theme


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / "PHOTO_TH_TRAVEL_OUTFIT_V2.json"

RECIPE_ID = "PHOTO_TH_TRAVEL_OUTFIT_V2"
POLICY_ID = "TH_TRAVEL_OUTFIT_V1"


def load_recipe() -> dict:
    return json.loads(RECIPE_PATH.read_text(encoding="utf-8"))


class TravelV2RecipeConfigGoldenTest(unittest.TestCase):
    """Freeze the recipe/config contract an operator cannot see change."""

    def setUp(self):
        self.recipe = load_recipe()
        self.spec = self.recipe["recipe_spec"]
        self.travel = self.spec["travel_contract"]

    def test_recipe_identity_is_frozen(self):
        self.assertEqual(
            {key: self.recipe[key] for key in (
                "schema_version", "recipe_id", "recipe_key", "recipe_version",
                "content_goal", "anchor_slot", "shot_count", "status",
            )},
            {
                "schema_version": "opv-content-recipe-v1",
                "recipe_id": RECIPE_ID,
                "recipe_key": "TH_TRAVEL_OUTFIT",
                "recipe_version": 7,
                "content_goal": "travel_look_choice",
                "anchor_slot": 2,
                "shot_count": 5,
                "status": "active",
            },
        )

    def test_story_structure_slots_roles_and_sources_are_frozen(self):
        # P1 is the travel cover and must always source look_a; P2-P5 are the
        # four ordered looks A/B/C/D.  Phase 1 must not reorder them.
        projected = [
            (entry["slot_index"], entry["role"], entry["layout_variant"],
             tuple(entry["source_roles"]))
            for entry in self.recipe["story_structure"]
        ]
        self.assertEqual(projected, [
            (1, "travel_cover", "FULL_BLEED", ("look_a",)),
            (2, "look_a", "CHOICE_DETAIL", ("look_a",)),
            (3, "look_b", "CHOICE_DETAIL", ("look_b",)),
            (4, "look_c", "CHOICE_DETAIL", ("look_c",)),
            (5, "look_d_with_cta", "CHOICE_DETAIL", ("look_d",)),
        ])

    def test_content_card_pages_are_frozen(self):
        card = self.spec["content_card"]
        self.assertEqual(card["schema_version"], "opv-photo-content-card-v1")
        self.assertEqual(
            [(page["index"], page["layout"], tuple(page["source_roles"]))
             for page in card["pages"]],
            [(1, "single", ("look_a",)), (2, "single", ("look_a",)),
             (3, "single", ("look_b",)), (4, "single", ("look_c",)),
             (5, "single", ("look_d",))],
        )

    def test_travel_contract_moments_and_required_fields_are_frozen(self):
        self.assertEqual(self.travel["schema_version"], "opv-photo-travel-contract-v1")
        self.assertEqual(self.travel["moments_per_post"], 4)
        self.assertEqual(
            tuple(moment["key"] for moment in self.travel["moments"]),
            ("airport_departure", "old_town_walk", "cafe_visit", "evening_stroll",
             "shopping_day", "photo_spot", "night_walk"),
        )
        self.assertEqual(
            tuple(self.travel["look_required_fields"]),
            ("travel_moment", "scene_prompt", "weather_logic", "footwear_type"),
        )

    def test_cafe_moment_footwear_rules_are_frozen(self):
        # The cafe moment deliberately allows any footwear except a stiletto;
        # a refactor that "tidies" the empty allowed-list would change QA.
        cafe = next(m for m in self.travel["moments"] if m["key"] == "cafe_visit")
        self.assertEqual(cafe["mobility_level"], "LOW")
        self.assertEqual(cafe["allowed_footwear_types"], [])
        self.assertEqual(cafe["forbidden_footwear_types"], ["STILETTO"])

    def test_destination_and_temperature_labels_cover_the_recipe_enums(self):
        # Phase 2 replaces these Thai label maps with a Locale Pack; the V2
        # recipe's own enum values must stay fully covered by its contract.
        variables = self.spec["variables_schema"]
        destinations = set(variables["destination"]["values"])
        temperature_bands = set(variables["temperature_band"]["values"])
        self.assertEqual(destinations, set(self.travel["destination_labels_th"]))
        self.assertEqual(temperature_bands, set(self.travel["temperature_labels_th"]))
        self.assertEqual(
            self.travel["destination_labels_th"]["generic_cool_city"], "เมืองอากาศเย็น")
        self.assertEqual(self.travel["temperature_labels_th"]["0_5c"], "0-5°C")

    def test_recipe_spec_identity_is_frozen(self):
        self.assertEqual(self.spec["schema_version"], "opv-photo-recipe-v1")
        self.assertEqual(self.spec["category_key"], "womenswear")
        self.assertEqual(self.spec["markets"], ["TH"])
        self.assertEqual(self.spec["theme_types"], ["CHOICE"])
        self.assertEqual(self.spec["product_modes"], ["NO_PRODUCT"])
        self.assertEqual(self.spec["template_id"], "PHOTO_TRAVEL_CARD_V3")
        self.assertEqual(self.spec["template_version"], 3)
        self.assertEqual(
            self.spec["supported_presentations"],
            ["FLAT_LAY", "MODEL_FULL_BODY", "SCENE_MODEL"],
        )
        self.assertEqual(
            tuple(self.spec["asset_requirements"]["required_roles"]),
            ("look_a", "look_b", "look_c", "look_d"),
        )
        self.assertEqual(self.recipe["copy_style"]["locale"], "th-TH")


class TravelV2PolicyGoldenTest(unittest.TestCase):
    """Freeze the recipe → policy registry and the travel family rotation."""

    def setUp(self):
        self.policy = load_planning_policy(RECIPE_ID)

    def test_registry_still_routes_travel_v2(self):
        self.assertTrue(recipe_has_planning_policy(RECIPE_ID))

    def test_policy_identity_and_supported_modes_are_frozen(self):
        self.assertEqual(self.policy["policy_id"], POLICY_ID)
        self.assertEqual(self.policy["policy_version"], 3)
        self.assertEqual(self.policy["recipe_ids"], [RECIPE_ID])
        self.assertEqual(self.policy["supported_reference_modes"],
                         ["STYLE", "COMPLETE_LOOK"])
        self.assertEqual(self.policy["planning_flow"], "travel_two_step")
        self.assertEqual(self.policy["minimum_cross_post_axis_difference"], 2)

    def test_travel_family_set_and_theme_orders_are_frozen(self):
        self.assertEqual(
            tuple(family["family_id"] for family in self.policy["families"]),
            ("airport_transit", "city_walk", "cafe_hopping", "night_market",
             "old_town_photo", "seaside_stroll", "mountain_town", "shopping_mall"),
        )
        self.assertEqual(
            self.policy["theme_family_order"]["COOL_WEATHER_TRAVEL"],
            ["airport_transit", "city_walk", "cafe_hopping", "old_town_photo",
             "night_market", "seaside_stroll", "mountain_town", "shopping_mall"],
        )
        # Every family referenced by any theme order must exist in `families`.
        known = {family["family_id"] for family in self.policy["families"]}
        for family_order in self.policy["theme_family_order"].values():
            self.assertTrue(set(family_order) <= known)


class TravelV2ReferenceModeContractTest(unittest.TestCase):
    """Freeze the operator-facing reference-mode resolution Phase 1 wraps."""

    def test_operator_enum_is_frozen(self):
        self.assertEqual(REFERENCE_TYPE_OPTIONS,
                         ("自动判断", "风格参考", "商品参考", "完整穿搭"))

    def mode(self, **overrides):
        kwargs = {
            "selected_type": "自动判断", "attachments": [], "product_id": "",
            "required_role_count": 4, "requested_count": 1,
            "required_roles": ("look_a", "look_b", "look_c", "look_d"),
        }
        kwargs.update(overrides)
        return resolve_reference_mode(**kwargs)

    def test_explicit_operator_choices_map_to_frozen_modes(self):
        self.assertEqual(self.mode(selected_type="风格参考", attachments=["a"]),
                         REFERENCE_MODE_STYLE)
        self.assertEqual(self.mode(selected_type="商品参考", product_id="SKU-1"),
                         REFERENCE_MODE_PRODUCT)
        self.assertEqual(self.mode(selected_type="完整穿搭", attachments=[1, 2, 3, 4]),
                         REFERENCE_MODE_COMPLETE_LOOK)

    def test_auto_prefers_product_then_complete_look_then_style(self):
        self.assertEqual(self.mode(product_id="SKU-1"), REFERENCE_MODE_PRODUCT)
        self.assertEqual(self.mode(attachments=[1, 2, 3, 4]),
                         REFERENCE_MODE_COMPLETE_LOOK)
        self.assertEqual(self.mode(attachments=[1, 2, 3]), REFERENCE_MODE_STYLE)

    def test_complete_look_requires_every_role_for_every_post(self):
        with self.assertRaisesRegex(ValueError, "4 张图片"):
            self.mode(selected_type="完整穿搭", attachments=[1, 2, 3])
        self.assertEqual(
            self.mode(selected_type="完整穿搭",
                      attachments=list(range(8)), requested_count=2),
            REFERENCE_MODE_COMPLETE_LOOK,
        )

    def test_empty_input_and_unknown_type_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "至少需要上传一张图片"):
            self.mode()
        with self.assertRaisesRegex(ValueError, "未知参考图类型"):
            self.mode(selected_type="随便")


class TravelV2PlanGoldenTest(unittest.TestCase):
    """Freeze a full deterministic TH V2 STYLE batch plan.

    With no vision profile the family rotation is a pure hash of
    ``record_id:recipe_id:theme_key:reference_mode``, so the whole plan is
    reproducible and safe to pin.
    """

    RECORD_ID = "rec-golden-th-v2"
    # Deliberately pinned digest: any silent drift in the V2 plan (families,
    # copy, A-D order, palette) will change this value.
    PLAN_SHA256 = "0acb4cb3e3bb255e9a7848710967341a94e235783af78008ea457563bbc5f1ca"
    FAMILIES = ("shopping_mall", "airport_transit", "city_walk")
    COVERS = (
        "วันช้อปใส่ลุคไหน?\nA B C หรือ D?",
        "ลุคไหนไปสนามบิน?\nA B C หรือ D?",
        "เดินเมืองเก่าใส่ลุคไหน?\nA B C หรือ D?",
    )

    def setUp(self):
        self.theme = resolve_photo_theme("凉爽旅行")
        self.assertEqual(self.theme["theme_key"], "COOL_WEATHER_TRAVEL")

    def plan(self):
        return plan_th_choice_batch(
            record_id=self.RECORD_ID, recipe_id=RECIPE_ID,
            theme=self.theme, reference_mode="STYLE", count=3,
        )

    def test_style_batch_families_and_copy_are_frozen(self):
        plan = self.plan()
        self.assertEqual(plan["policy_id"], POLICY_ID)
        self.assertEqual(plan["planning_flow"], "travel_two_step")
        self.assertEqual(plan["required_roles"],
                         ["look_a", "look_b", "look_c", "look_d"])
        self.assertEqual(tuple(item["family_id"] for item in plan["items"]),
                         self.FAMILIES)
        self.assertEqual(tuple(item["copy"]["cover"] for item in plan["items"]),
                         self.COVERS)

    def test_every_item_keeps_ordered_roles_and_scene_evidence(self):
        # The batch plan freezes the four ordered looks and their garments;
        # travel_moment/scene_prompt/weather_logic/footwear_type are injected
        # downstream by the travel flow and are asserted there.
        for item in self.plan()["items"]:
            self.assertEqual([look["role"] for look in item["looks"]],
                             ["look_a", "look_b", "look_c", "look_d"])
            for look in item["looks"]:
                for field in ("display_label", "outerwear", "top_inner",
                              "bottom", "shoes", "outerwear_type", "bottom_type"):
                    self.assertTrue(look.get(field),
                                    f"{field} missing on {look['role']}")
            self.assertTrue(item["scene_zh"] and item["angle_zh"])
            self.assertIn("A B C หรือ D", item["copy"]["cover"])

    def test_plan_digest_is_reproducible_and_pinned(self):
        first, second = self.plan(), self.plan()
        self.assertEqual(first["plan_sha256"], second["plan_sha256"])
        self.assertEqual(first["plan_sha256"], self.PLAN_SHA256)

    def test_product_mode_is_still_rejected_for_v2(self):
        with self.assertRaisesRegex(PhotoContentPlanError, "不支持参考模式：PRODUCT"):
            plan_th_choice_batch(
                record_id="rec-golden-th-v2-product", recipe_id=RECIPE_ID,
                theme=self.theme, reference_mode="PRODUCT", count=1,
            )


if __name__ == "__main__":
    unittest.main()
