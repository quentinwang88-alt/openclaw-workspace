#!/usr/bin/env python3
"""Contract validator tests for domain/contracts.py."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import contracts
from domain.contracts import ContractViolationError


def market_pack_payload() -> dict:
    return {
        "schema_version": "opv-market-pack-v1",
        "market_pack_id": "MP_X_V1",
        "pack_key": "MP_X",
        "pack_version": 1,
        "target_country": "TH",
        "target_locale": "th-TH",
        "pack_name": "test pack",
        "season_key": "all_season",
        "status": "active",
        "visual_rules": {"a": 1},
        "copy_rules": {"b": 2},
        "topic_rules": {"c": 3},
        "safety_rules": {"d": 4},
    }


def render_preset_payload() -> dict:
    return {
        "schema_version": "opv-render-preset-v1",
        "render_preset_id": "RP_X_V1",
        "preset_key": "RP_X",
        "preset_version": 1,
        "preset_name": "test preset",
        "status": "active",
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


def theme_payload() -> dict:
    return {
        "schema_version": "opv-theme-v1",
        "theme_id": "THEME_X_V1",
        "theme_key": "THEME_X",
        "theme_version": 1,
        "theme_name": "test theme",
        "status": "active",
        "applicable_markets": ["TH"],
        "product_match_rules": {"categories": ["dress"]},
        "content_plan_rules": {"copy_tone": "casual"},
        "default_storyboard": {
            "slots": [
                {"slot_index": i, "slot_role": "hero", "duration_ms": 2200}
                for i in range(1, 6)
            ]
        },
    }


def plan_payload() -> dict:
    return {
        "schema_version": "opv-plan-v1",
        "market_pack": {"id": "MP_TH_DEFAULT_V1", "version": 1, "country": "TH", "locale": "th-TH"},
        "theme": {"id": "THEME_TH_CAFE_DATE_V1", "topic": "หวานสไตล์คาเฟ่"},
        "persona": {"ref_id": "PERSONA_1", "snapshot": {"age_range": "20s"}},
        "look": {"ref_id": "LOOK_1", "source_type": "look_template", "snapshot": {"items": ["dress"]}},
        "scene": {"ref_id": "SCENE_1", "snapshot": {"place": "cafe"}},
        "copy": {"title": "t", "caption": "c", "hashtags": ["#a"], "cover_text": "x"},
        "audio_policy": {"strategy": "platform_hot_bgm", "fallback": "no_bgm"},
        "shots": [
            {
                "slot_index": 1,
                "slot_role": "hero",
                "purpose": "hook",
                "duration_ms": 2200,
                "motion_preset": "slow_push",
                "transition_out": "short_dissolve",
                "overlay_text": "",
                "generation_prompt": "p",
                "source_refs": [],
            },
            {
                "slot_index": 2,
                "slot_role": "full_look",
                "purpose": "look",
                "duration_ms": 2600,
                "motion_preset": "light_pan",
                "transition_out": "short_dissolve",
                "overlay_text": "",
                "generation_prompt": "p",
                "source_refs": [],
            },
            {
                "slot_index": 3,
                "slot_role": "lifestyle",
                "purpose": "life",
                "duration_ms": 2500,
                "motion_preset": "static_hold",
                "transition_out": "short_dissolve",
                "overlay_text": "",
                "generation_prompt": "p",
                "source_refs": [],
            },
            {
                "slot_index": 4,
                "slot_role": "detail",
                "purpose": "detail",
                "duration_ms": 2200,
                "motion_preset": "detail_zoom",
                "transition_out": "short_dissolve",
                "overlay_text": "",
                "generation_prompt": "p",
                "source_refs": [],
            },
            {
                "slot_index": 5,
                "slot_role": "second_angle",
                "purpose": "close",
                "duration_ms": 3000,
                "motion_preset": "light_pan",
                "transition_out": "cut",
                "overlay_text": "",
                "generation_prompt": "p",
                "source_refs": [],
            },
        ],
    }


class ConfigContractTest(unittest.TestCase):
    def test_valid_market_pack_passes(self) -> None:
        self.assertEqual(contracts.validate_market_pack_payload(market_pack_payload()), [])

    def test_market_pack_rejects_bad_schema_version_and_locale(self) -> None:
        payload = market_pack_payload()
        payload["schema_version"] = "opv-market-pack-v2"
        payload["target_locale"] = "th_TH"
        errors = contracts.validate_market_pack_payload(payload)
        self.assertTrue(any("schema_version" in e for e in errors))
        self.assertTrue(any("target_locale" in e for e in errors))

    def test_market_pack_rejects_empty_rules(self) -> None:
        payload = market_pack_payload()
        payload["safety_rules"] = {}
        errors = contracts.validate_market_pack_payload(payload)
        self.assertTrue(any("safety_rules" in e for e in errors))

    def test_valid_render_preset_passes(self) -> None:
        self.assertEqual(contracts.validate_render_preset_payload(render_preset_payload()), [])

    def test_render_preset_requires_vertical_916(self) -> None:
        payload = render_preset_payload()
        payload["width_px"] = 1080
        payload["height_px"] = 1080
        errors = contracts.validate_render_preset_payload(payload)
        self.assertTrue(any("9:16" in e for e in errors))

    def test_render_preset_duration_bounds(self) -> None:
        payload = render_preset_payload()
        payload["target_duration_ms"] = 9000
        errors = contracts.validate_render_preset_payload(payload)
        self.assertTrue(any("target_duration_ms" in e for e in errors))

    def test_render_preset_rejects_embedded_bgm_as_default(self) -> None:
        payload = render_preset_payload()
        payload["audio_rules"] = {
            "default_strategy": "embedded_bgm",
            "fallback_strategy": "no_bgm",
        }
        errors = contracts.validate_render_preset_payload(payload)
        self.assertTrue(any("platform_hot_bgm" in e for e in errors))

    def test_theme_storyboard_must_have_five_slots(self) -> None:
        payload = theme_payload()
        payload["default_storyboard"]["slots"] = payload["default_storyboard"]["slots"][:4]
        errors = contracts.validate_theme_payload(payload)
        self.assertTrue(any("exactly 5" in e for e in errors))

    def test_theme_storyboard_rejects_duplicate_slots(self) -> None:
        payload = theme_payload()
        payload["default_storyboard"]["slots"][1]["slot_index"] = 1
        errors = contracts.validate_theme_payload(payload)
        self.assertTrue(any("duplicate slot_index" in e for e in errors))


class AccountProfileContractTest(unittest.TestCase):
    def base_payload(self) -> dict:
        return {
            "schema_version": "opv-account-profile-v1",
            "account_id": "OPV_TEST_1",
            "account_code": "opv-test-1",
            "account_name": "test account",
            "platform": "tiktok",
            "target_country": "TH",
            "default_locale": "th-TH",
            "timezone": "Asia/Bangkok",
            "status": "testing",
            "persona_snapshot": {"age_range": "20s"},
            "visual_identity": {"tone": "warm"},
            "allowed_style_refs": [],
            "allowed_look_refs": [],
            "allowed_scene_refs": [],
            "core_scene_refs": [],
            "operating_rules": {"daily_volume": {"min": 5, "max": 10}},
        }

    def test_testing_account_can_have_empty_refs(self) -> None:
        self.assertEqual(contracts.validate_account_profile_payload(self.base_payload()), [])

    def test_active_account_requires_persona_and_assets(self) -> None:
        payload = self.base_payload()
        payload["status"] = "active"
        errors = contracts.validate_account_profile_payload(payload)
        self.assertTrue(any("persona_ref_id" in e for e in errors))
        self.assertTrue(any("core_scene_refs" in e for e in errors))
        self.assertTrue(any("allowed_look_refs" in e for e in errors))

    def test_active_account_with_assets_passes(self) -> None:
        payload = self.base_payload()
        payload["status"] = "active"
        payload["persona_ref_id"] = "PERSONA_1"
        payload["core_scene_refs"] = ["SCENE_1"]
        payload["allowed_look_refs"] = ["LOOK_1"]
        payload["default_market_pack_id"] = "MP_TH_DEFAULT_V1"
        payload["default_render_preset_id"] = "RP_STILL_VERTICAL_12S_V1"
        self.assertEqual(contracts.validate_account_profile_payload(payload), [])


class PlanContractTest(unittest.TestCase):
    def test_valid_plan_passes(self) -> None:
        self.assertEqual(contracts.validate_plan_json(plan_payload()), [])

    def test_plan_requires_five_unique_slots(self) -> None:
        plan = plan_payload()
        plan["shots"] = plan["shots"][:4]
        errors = contracts.validate_plan_json(plan)
        self.assertTrue(any("exactly 5" in e for e in errors))

    def test_plan_rejects_out_of_range_total_duration(self) -> None:
        plan = plan_payload()
        for shot in plan["shots"]:
            shot["duration_ms"] = 4000
        errors = contracts.validate_plan_json(plan)
        self.assertTrue(any("total duration" in e for e in errors))

    def test_plan_rejects_bad_audio_strategy(self) -> None:
        plan = plan_payload()
        plan["audio_policy"]["strategy"] = "burned_cd"
        errors = contracts.validate_plan_json(plan)
        self.assertTrue(any("strategy" in e for e in errors))

    def test_plan_rejects_bad_look_source(self) -> None:
        plan = plan_payload()
        plan["look"]["source_type"] = "guess"
        errors = contracts.validate_plan_json(plan)
        self.assertTrue(any("source_type" in e for e in errors))

    def test_plan_slot_roles_must_match_vocabulary(self) -> None:
        plan = plan_payload()
        plan["shots"][0]["slot_role"] = "selfie"
        errors = contracts.validate_plan_json(plan)
        self.assertTrue(any("slot_role" in e for e in errors))


class BgmContractTest(unittest.TestCase):
    def test_valid_bgm_selection_passes(self) -> None:
        payload = {
            "schema_version": "opv-bgm-v1",
            "audio_strategy": "platform_hot_bgm",
            "selection_policy_version": "opv-bgm-v1",
            "country": "TH",
            "auth_id": "auth-1",
            "selected": {"music_id": "m1", "title": "song", "rank": 1, "selected_at": "", "score": 0.9},
            "actual": {"music_id": "m1", "title": "song", "confirmed_at": ""},
            "fallback_reason": None,
        }
        self.assertEqual(contracts.validate_bgm_selection_payload(payload), [])

    def test_bgm_rejects_unknown_strategy(self) -> None:
        payload = {
            "schema_version": "opv-bgm-v1",
            "audio_strategy": "vinyl",
            "selection_policy_version": "opv-bgm-v1",
            "country": "TH",
            "selected": {},
            "actual": {},
            "fallback_reason": None,
        }
        errors = contracts.validate_bgm_selection_payload(payload)
        self.assertTrue(any("audio_strategy" in e for e in errors))


class EnsureValidTest(unittest.TestCase):
    def test_ensure_valid_raises_with_joined_messages(self) -> None:
        with self.assertRaises(ContractViolationError) as ctx:
            contracts.ensure_valid(["bad a", "bad b"], "plan")
        self.assertIn("bad a; bad b", str(ctx.exception))

    def test_idempotency_digest_shape(self) -> None:
        digest = "a" * 64
        self.assertEqual(contracts.validate_idempotency_digest_key(digest), [])
        self.assertTrue(contracts.validate_idempotency_digest_key("short"))


if __name__ == "__main__":
    unittest.main()
