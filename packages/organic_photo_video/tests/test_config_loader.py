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
        self.assertEqual(len(self.bundle.market_packs), 1)
        self.assertEqual(len(self.bundle.themes), 9)  # 8 pilot themes + travel departure
        self.assertEqual(len(self.bundle.render_presets), 1)
        self.assertEqual(len(self.bundle.content_recipes), 3)
        self.assertEqual(len(self.bundle.render_profiles), 1)
        self.assertEqual(len(self.bundle.quality_profiles), 1)
        self.assertIsNotNone(self.bundle.account_example)

    def test_recipe_profiles_are_active_and_linked(self) -> None:
        render_ids = {p.render_profile_id for p in self.bundle.render_profiles}
        quality_ids = {p.quality_profile_id for p in self.bundle.quality_profiles}
        for recipe in self.bundle.content_recipes:
            self.assertEqual(recipe.status, "active")
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
        pack = self.bundle.market_packs[0]
        self.assertEqual(pack.market_pack_id, "MP_TH_DEFAULT_V1")
        self.assertEqual(pack.pack_key, "MP_TH_DEFAULT")
        self.assertEqual(pack.target_country, "TH")
        self.assertEqual(pack.target_locale, "th-TH")
        self.assertEqual(pack.status, "active")
        self.assertTrue(pack.visual_rules_json)
        self.assertFalse(pack.copy_rules_json.get("fallback_allowed"), True)

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

    def test_example_account_is_testing_not_active(self) -> None:
        example = self.bundle.account_example
        self.assertEqual(example.status, "testing")
        self.assertEqual(example.default_market_pack_id, "MP_TH_DEFAULT_V1")


class LoaderFailureTest(unittest.TestCase):
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
