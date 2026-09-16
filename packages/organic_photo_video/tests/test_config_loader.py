#!/usr/bin/env python3
"""Config loader tests: the shipped Phase 0 seed files must stay valid."""

from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config import loader
from domain.contracts import ContractViolationError


class ShippedConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.bundle = loader.load_seed_bundle()

    def test_bundle_counts_match_phase0_plan(self) -> None:
        # 2026-09-13 (VN scarf Phase 3): +1 category profile v2 (SCARF_V1, an
        # adapter contract rather than a copy profile) = 3 categories.
        self.assertEqual(len(self.bundle.categories), 3)
        # 2026-09-13 (VN scarf Phase 4): +MP_VN_DEFAULT_v1 (status=draft) = 3.
        self.assertEqual(len(self.bundle.market_packs), 3)
        self.assertEqual(len(self.bundle.themes), 10)
        self.assertEqual(len(self.bundle.render_presets), 1)
        # 2026-09-13: temperature-layering V2 never reached RDS and was retired;
        # the daily thermal-transition line took its slot, so the totals are
        # unchanged from the 2026-09-12 layering baseline.
        # 2026-09-13 (VN scarf Phase 2): +1 country-agnostic travel template
        # (PHOTO_TRAVEL_OUTFIT_V3, status=draft canary) = 19 recipes, 14 photos.
        # 2026-09-13 (VN scarf Phase 4): +1 country-agnostic daily matching
        # template (PHOTO_MATCHING_CHOICE_V3, status=draft canary) = 20 / 15.
        self.assertEqual(len(self.bundle.content_recipes), 20)
        self.assertEqual(len(self.bundle.render_profiles), 2)
        self.assertEqual(len(self.bundle.quality_profiles), 3)
        # 2026-09-15 (排版轮 B)：+2 structured_v1 布局 = 13。
        self.assertEqual(len(self.bundle.board_layouts), 13)
        self.assertEqual(len(self.bundle.variant_policies), 1)
        # The country-agnostic layer ships with the TH locale pack plus the VN
        # one added in Phase 4, and the V1 East-Asia destination catalog.
        self.assertEqual(len(self.bundle.locale_packs), 2)
        self.assertEqual(len(self.bundle.destination_catalogs), 1)
        self.assertIsNotNone(self.bundle.account_example)

    def test_outfit_breakdown_recipe_uses_p2_anchor_and_p1_board(self) -> None:
        recipe = next(
            item
            for item in self.bundle.content_recipes
            if item.recipe_id == "RECIPE_OUTFIT_BREAKDOWN_V1"
        )
        self.assertEqual(recipe.content_goal, "outfit_breakdown")
        self.assertEqual(recipe.anchor_slot, 2)
        self.assertEqual(recipe.story_structure_json[0]["slot_index"], 1)
        self.assertEqual(recipe.story_structure_json[0]["shot_kind"], "composite_board")
        self.assertEqual(recipe.recipe_version, 3)
        self.assertEqual(recipe.quality_profile_id, "QUALITY_OUTFIT_BREAKDOWN_V2")

    def test_multi_look_recipe_is_additive_with_independent_p1_anchor(self) -> None:
        recipe = next(item for item in self.bundle.content_recipes if item.recipe_id == "RECIPE_MULTI_LOOK_V1")
        self.assertEqual(recipe.content_goal, "multi_look")
        self.assertEqual(recipe.anchor_slot, 1)
        self.assertEqual(recipe.story_structure_json[0]["shot_kind"], "composite_board")
        self.assertTrue(all(slot["slot_role"] == "full_look" for slot in recipe.story_structure_json[1:]))
        profile = next(p for p in self.bundle.render_profiles if p.render_profile_id == recipe.render_profile_id)
        self.assertEqual(profile.motion_rules_json["allowed"], ["static_hold"])
        raw_presets = json.loads((PACKAGE_ROOT / "config" / "feishu_production_presets.json").read_text())["presets"]
        name = "TH｜五套穿搭｜拆解首图"
        self.assertEqual(next(p for p in raw_presets if p["name"] == name)["tasks"][0]["recipe_id"], recipe.recipe_id)
        self.assertEqual([name], next(p for p in raw_presets if "随机养号组合" in p["name"])["tasks_from"])

    def test_recipe_profiles_are_active_and_linked(self) -> None:
        render_ids = {p.render_profile_id for p in self.bundle.render_profiles}
        quality_ids = {p.quality_profile_id for p in self.bundle.quality_profiles}
        for recipe in self.bundle.content_recipes:
            self.assertEqual(recipe.status, "deprecated" if recipe.recipe_id in {
                "PHOTO_TH_PICK_YOUR_LOOK_V1", "PHOTO_TH_PICK_YOUR_LOOK_V2",
                "PHOTO_TH_TEMPERATURE_DRESSING_V1", "PHOTO_TH_TRAVEL_OUTFIT_V1",
            } else "draft" if recipe.recipe_id in {
                # Country-agnostic canaries: shipped but not routed until Phase 4
                # binds a Market Pack and passes acceptance.
                "PHOTO_TRAVEL_OUTFIT_V3",
                "PHOTO_MATCHING_CHOICE_V3",
            } else "active")
            if recipe.recipe_spec_json.get("media_kind") == "native_photo":
                self.assertIsNone(recipe.render_profile_id)
            else:
                self.assertIn(recipe.render_profile_id, render_ids)
            self.assertIn(recipe.quality_profile_id, quality_ids)

    def test_travel_departure_theme_targets_outerwear(self) -> None:
        travel = [
            t
            for t in self.bundle.themes
            if t.theme_key == "THEME_TH_TRAVEL_DEPARTURE"
        ]
        self.assertEqual(len(travel), 1)
        theme = travel[0]
        self.assertIn("outerwear", theme.product_match_rules_json["categories"])
        self.assertIn("puffer_jacket", theme.product_match_rules_json["categories"])

    def test_th_market_pack_is_active_and_localized(self) -> None:
        pack = next(p for p in self.bundle.market_packs if p.target_country == "TH")
        self.assertEqual(pack.market_pack_id, "MP_TH_DEFAULT_V1")
        self.assertEqual(pack.pack_key, "MP_TH_DEFAULT")
        self.assertEqual(pack.target_country, "TH")
        self.assertEqual(pack.target_locale, "th-TH")
        self.assertEqual(pack.status, "active")
        self.assertTrue(pack.visual_rules_json)
        self.assertFalse(pack.copy_rules_json.get("fallback_allowed"), True)

    def test_mx_market_pack_is_active_and_uses_mexican_spanish(self) -> None:
        pack = next(p for p in self.bundle.market_packs if p.target_country == "MX")
        self.assertEqual(pack.market_pack_id, "MP_MX_DEFAULT_V1")
        self.assertEqual(pack.target_locale, "es-MX")
        self.assertEqual(pack.status, "active")
        self.assertFalse(pack.copy_rules_json.get("fallback_allowed"), True)
        self.assertIn("hairline", " ".join(pack.visual_rules_json["hair_constraints"]))

    def test_photo_categories_are_config_only_and_active(self) -> None:
        categories = {item["category_key"]: item for item in self.bundle.categories}
        # 2026-09-13 (VN scarf Phase 3): +SCARF_V1, the first
        # opv-category-profile-v2 adapter contract. It shares the directory with
        # the v1 copy profiles; the loader dispatches on schema_version.
        self.assertEqual(set(categories), {"womenswear", "wig", "scarf"})
        for item in categories.values():
            self.assertEqual(item["status"], "active")
        v1 = {
            key: item for key, item in categories.items()
            if item["schema_version"] == loader.CATEGORY_PROFILE_SCHEMA
        }
        self.assertEqual(set(v1), {"womenswear", "wig"})
        for item in v1.values():
            self.assertEqual(
                item["content_rules"]["allowed_product_modes"],
                ["NO_PRODUCT", "SOFT_PRODUCT"],
            )
            self.assertEqual(item["content_rules"]["default_asset_mode"], "ASSET_REUSE")
        # The v2 profile declares the machine-readable adapter contract instead
        # of copy rules, and never binds a market or a publish language.
        scarf = categories["scarf"]
        self.assertEqual(scarf["schema_version"], loader.CATEGORY_PROFILE_V2_SCHEMA)
        self.assertEqual(scarf["main_product_slot"], "accessories")
        self.assertEqual(scarf["product_label_zh"], "目标围巾")
        self.assertEqual(scarf["accepted_product_categories"], ["scarf"])
        for forbidden in ("markets", "locale", "content_rules"):
            self.assertNotIn(forbidden, scarf)

    def test_eight_photo_recipes_have_stable_contracts(self) -> None:
        photos = [
            recipe for recipe in self.bundle.content_recipes
            if recipe.recipe_spec_json.get("media_kind") == "native_photo"
        ]
        # PHOTO_MX_PICK_YOUR_HAIR_V2 keeps its explicit four-page contract;
        # all other native-photo recipes, including the daily
        # thermal-transition line, use five.
        # 2026-09-13 (VN scarf Phase 2): +1 country-agnostic travel template = 14.
        # 2026-09-13 (VN scarf Phase 4): +1 country-agnostic matching template = 15.
        self.assertEqual(len(photos), 15)
        layout_ids = {
            layout["layout_id"] for layout in self.bundle.board_layouts
            if layout["schema_version"] in {loader.PHOTO_LAYOUT_SCHEMA, "opv-photo-layout-v2"}
        }
        self.assertEqual(
            layout_ids,
            {
                "PHOTO_SINGLE_LIGHT_TEXT_V1",
                "PHOTO_COMPARISON_V1",
                "PHOTO_CHOICE_GRID_V1",
                "PHOTO_CHOICE_CARD_V1",
                "PHOTO_CHOICE_CARD_V2",
                "PHOTO_TRAVEL_CARD_V3",
                "PHOTO_MX_HAIR_CARD_V1",
                "PHOTO_THERMAL_ROUTE_V1",
                # 2026-09-15 (排版轮 B)：structured_v1 结构化排版两个模板
                # （CLEAN=纯色/室内直排；SCENE=旅行留白+柔和底）。
                "PHOTO_STRUCTURED_CLEAN_V1",
                "PHOTO_STRUCTURED_SCENE_V1",
            },
        )
        for recipe in photos:
            spec = recipe.recipe_spec_json
            # The country-agnostic travel template is v2: same executable rules,
            # but market/category/locale are bound at request time instead of
            # being fixed in the recipe.
            is_v2 = spec["schema_version"] == "opv-photo-recipe-v2"
            self.assertIn(
                spec["schema_version"], {"opv-photo-recipe-v1", "opv-photo-recipe-v2"}
            )
            if is_v2:
                self.assertNotIn("markets", spec)
                self.assertNotIn("category_key", spec)
                self.assertEqual(spec["market_policy"], "MARKET_PACK_REQUIRED")
                # 2026-09-13 (VN scarf Phase 4): two country-agnostic templates
                # now share the v2 schema -- the travel line keeps
                # ``travel_two_step`` and the daily matching line uses the
                # reference contract flow.
                self.assertEqual(
                    spec["planning_flow"],
                    "travel_two_step"
                    if recipe.recipe_id == "PHOTO_TRAVEL_OUTFIT_V3"
                    else "reference_contract_v1",
                )
                self.assertTrue(spec["required_category_capabilities"])
                self.assertTrue(spec["locale_copy_packs"])
            self.assertIn("NO_PRODUCT", spec["product_modes"])
            self.assertTrue(spec["variables_schema"])
            self._assert_acyclic_story(recipe.story_structure_json)
            if spec.get("execution_flow") == "mx_wig_choice_v1":
                self.assertEqual(recipe.recipe_id, "PHOTO_MX_PICK_YOUR_HAIR_V2")
                self.assertEqual(recipe.shot_count, 4, recipe.recipe_id)
                self.assertEqual(
                    [slot["slot_index"] for slot in recipe.story_structure_json],
                    [1, 2, 3, 4],
                    recipe.recipe_id,
                )
                self.assertEqual(
                    [slot["role"] for slot in recipe.story_structure_json],
                    ["hair_a", "hair_b", "hair_c", "hair_d"],
                    recipe.recipe_id,
                )
                self.assertEqual(spec["asset_policy"], {
                    "default": "AI_GENERATE", "on_missing": "NEEDS_ASSET",
                })
                self.assertEqual(spec["template_id"], "PHOTO_MX_HAIR_CARD_V1")
                self.assertEqual(recipe.recipe_spec_json["content_card"]["logic_key"],
                                 "wig_four_choice_es")
                continue
            self.assertEqual(recipe.shot_count, 5, recipe.recipe_id)
            self.assertEqual(
                [slot["slot_index"] for slot in recipe.story_structure_json],
                [1, 2, 3, 4, 5],
                recipe.recipe_id,
            )
            self.assertEqual(spec["asset_policy"], {
                "default": "ASSET_REUSE", "on_missing": "NEEDS_ASSET",
            })
            self.assertIn(spec["template_id"], layout_ids)

    def test_pick_your_look_uses_operator_friendly_copy_pack(self) -> None:
        recipe = next(
            item for item in self.bundle.content_recipes
            if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3"
        )
        profile = recipe.recipe_spec_json["execution_profiles"][0]
        self.assertEqual(profile["copy_pack_id"], "TH_PICK_YOUR_LOOK_V3")
        self.assertEqual(len(profile["copy_variants"]), 4)
        self.assertTrue(all(
            variant["copy"]["language_review_status"] == "production_copy_pack"
            for variant in profile["copy_variants"]
        ))

    def test_travel_v2_uses_copy_pack_draft_templates_with_tokens(self) -> None:
        recipe = next(
            item for item in self.bundle.content_recipes
            if item.recipe_id == "PHOTO_TH_TRAVEL_OUTFIT_V2"
        )
        self.assertEqual(recipe.status, "active")
        # 2026-09-13: travel v7 adds the optional article-level
        # ``thermal_sensitivity`` variable consumed only by the TEMPERATURE theme.
        self.assertEqual(recipe.recipe_version, 7)
        self.assertEqual(recipe.story_structure_json[0]["layout_variant"], "FULL_BLEED")
        self.assertEqual(recipe.story_structure_json[0]["source_roles"], ["look_a"])
        self.assertEqual(
            recipe.recipe_spec_json["content_card"]["pages"][0]["layout"], "single"
        )
        profile = recipe.recipe_spec_json["execution_profiles"][0]
        self.assertEqual(profile["copy_pack_id"], "TH_TRAVEL_OUTFIT_V2")
        self.assertEqual(len(profile["copy_variants"]), 4)
        self.assertTrue(all(
            variant["copy"]["language_review_status"] == "DRAFT"
            for variant in profile["copy_variants"]
        ))
        for variant in profile["copy_variants"]:
            self.assertIn("A B C หรือ D", variant["copy"]["caption"])
            self.assertIn("{{label_a}}", variant["copy"]["slide_texts"][1])
            self.assertIn("{{label_d}}", variant["copy"]["slide_texts"][4])
            self.assertIn("{destination}", variant["copy"]["title"])
            self.assertIn("{temperature}", variant["copy"]["caption"])

    @staticmethod
    def _assert_acyclic_story(story) -> None:
        dependencies = {
            slot["slot_index"]: list(slot.get("source_slots") or []) for slot in story
        }
        visiting, visited = set(), set()

        def visit(slot):
            if slot in visiting:
                raise AssertionError(f"photo recipe contains dependency cycle at slide {slot}")
            if slot in visited:
                return
            visiting.add(slot)
            for dependency in dependencies.get(slot, []):
                visit(dependency)
            visiting.remove(slot)
            visited.add(slot)

        for slot in dependencies:
            visit(slot)

    def test_all_themes_apply_to_thailand_and_are_active(self) -> None:
        for theme in self.bundle.themes:
            self.assertEqual(theme.status, "active", theme.theme_id)
            self.assertIn("TH", theme.applicable_markets_json, theme.theme_id)
            slots = theme.default_storyboard_json.get("slots", [])
            self.assertEqual(len(slots), 5, theme.theme_id)
            self.assertEqual(
                [s["slot_index"] for s in slots], [1, 2, 3, 4, 5], theme.theme_id
            )
            self.assertEqual(
                theme.content_plan_rules_json.get("look_source_priority"),
                ["successful_look", "look_template", "ai_exploration"],
                theme.theme_id,
            )

    def test_theme_ids_are_unique(self) -> None:
        theme_ids = [t.theme_id for t in self.bundle.themes]
        theme_keys = [t.theme_key for t in self.bundle.themes]
        self.assertEqual(len(set(theme_ids)), len(theme_ids))
        self.assertEqual(len(set(theme_keys)), len(theme_keys))

    def test_render_preset_matches_fastcut_spec(self) -> None:
        preset = self.bundle.render_presets[0]
        self.assertEqual(preset.render_preset_id, "RP_STILL_FASTCUT_10S_V1")
        self.assertEqual((preset.width_px, preset.height_px), (1080, 1920))
        self.assertEqual(preset.fps, 30)
        self.assertEqual(preset.target_duration_ms, 10000)
        self.assertEqual(preset.codec, "h264")
        self.assertEqual(preset.render_mode, "still_slideshow")
        self.assertEqual(
            preset.audio_rules_json["default_strategy"], "platform_hot_bgm"
        )
        self.assertTrue(preset.audio_rules_json["forbid_double_audio_track"])
        # operator feedback 2026-08-31: hard cuts only, no dissolves
        self.assertEqual(
            preset.transition_rules_json["allowed_transitions"], ["cut"]
        )
        self.assertEqual(preset.transition_rules_json["default"], "cut")

    def test_preset_timeline_defaults_sum_to_target_duration(self) -> None:
        preset = self.bundle.render_presets[0]
        timeline = preset.output_rules_json["timeline_defaults_ms"]
        self.assertEqual(
            sum(int(v) for v in timeline.values()), preset.target_duration_ms
        )
        self.assertLess(int(timeline["1"]), int(timeline["5"]))
        self.assertGreater(len({int(v) for v in timeline.values()}), 1)

    def test_example_account_is_testing_not_active(self) -> None:
        example = self.bundle.account_example
        self.assertEqual(example.status, "testing")
        self.assertEqual(example.default_market_pack_id, "MP_TH_DEFAULT_V1")

    def test_test_account_defaults_to_auto_render_without_model_qa(self) -> None:
        account = loader.load_account_import_file(
            PACKAGE_ROOT / "config" / "accounts" / "OPV_TH_TEST_001.json"
        )
        self.assertFalse(account.operating_rules_json["human_review_required"])
        self.assertFalse(account.operating_rules_json["visual_qa_required"])


class LoaderFailureTest(unittest.TestCase):
    def test_invalid_photo_category_fails_at_load_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "WIG_BAD.json"
            bad.write_text(json.dumps({
                "schema_version": "opv-category-profile-v1",
                "category_key": "wig",
                "category_name": "假发",
                "status": "active",
                "interest_drivers": [],
                "visual_dimensions": ["hairline"],
                "asset_requirements": ["clear"],
                "content_rules": {"default_asset_mode": "ASSET_REUSE"},
            }), encoding="utf-8")
            with self.assertRaises(ContractViolationError):
                loader.load_category_file(bad)

    def test_invalid_photo_layout_does_not_fall_back_to_board_validator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "PHOTO_BAD.json"
            bad.write_text(json.dumps({
                "schema_version": "opv-photo-layout-v1",
                "layout_id": "PHOTO_BAD",
                "layout_version": 1,
                "layout_kind": "CHOICE_GRID",
                "status": "active",
                "canvas": {"width": 1080, "height": 1920},
                "page_variants": {
                    "BAD": {"regions": [
                        {"id": "outside", "kind": "image", "rect": {
                            "x": 1000, "y": 0, "width": 200, "height": 400,
                        }},
                        {"id": "headline", "kind": "text", "rect": {
                            "x": 0, "y": 0, "width": 400, "height": 100,
                        }},
                    ]},
                },
            }), encoding="utf-8")
            with self.assertRaises(ContractViolationError):
                loader.load_board_layouts(Path(tmp))

    def test_invalid_theme_file_fails_at_load_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "THEME_BAD_v1.json"
            bad.write_text('{"schema_version": "wrong"}', encoding="utf-8")
            with self.assertRaises(ContractViolationError):
                loader.load_theme_file(bad)

    def test_invalid_json_fails_with_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "MP_BAD.json"
            bad.write_text("{not json", encoding="utf-8")
            with self.assertRaises(loader.ConfigLoadError) as ctx:
                loader.load_market_pack_file(bad)
            self.assertIn("MP_BAD.json", str(ctx.exception))

    def test_unknown_payload_key_is_rejected(self) -> None:
        valid = {
            "schema_version": "opv-render-preset-v1",
            "render_preset_id": "RP_X_V1",
            "preset_key": "RP_X",
            "preset_version": 1,
            "preset_name": "p",
            "status": "draft",
            "width_px": 1080,
            "height_px": 1920,
            "fps": 30,
            "target_duration_ms": 12500,
            "codec": "h264",
            "render_mode": "still_slideshow",
            "motion_rules": {"allowed_presets": ["slow_push"]},
            "transition_rules": {"allowed_transitions": ["cut"]},
            "text_overlay_rules": {"safe_area_px": {"top": 1}},
            "audio_rules": {"default_strategy": "platform_hot_bgm", "fallback_strategy": "no_bgm"},
            "output_rules": {"container": "mp4"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "RP_BAD.json"
            valid["typo_field"] = 1
            bad.write_text(json.dumps(valid), encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                loader.load_render_preset_file(bad)
            self.assertIn("typo_field", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
