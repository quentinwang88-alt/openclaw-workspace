#!/usr/bin/env python3
"""Content Planner tests with in-memory fakes (no real RDS, no real assets)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import contracts
from domain.models import (
    AccountProfile,
    ContentTask,
    MarketPack,
    RenderPreset,
    ThemeCatalog,
)
from domain.statuses import TASK_DRAFT, TASK_IMAGE_REVIEW, TASK_PLANNED
from services.content_planner import ContentPlannerError, ContentPlannerService
from config import loader


class FakeRepository:
    def __init__(self) -> None:
        self.tasks = {}
        self.accounts = {}
        self.packs = {}
        self.presets = {}
        self.themes = {}
        self.recipes = {}
        self.render_profiles = {}
        self.quality_profiles = {}
        self.transitions = []

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def get_account_profile(self, account_id):
        return self.accounts.get(account_id)

    def get_market_pack(self, pack_id):
        return self.packs.get(pack_id)

    def get_render_preset(self, preset_id):
        return self.presets.get(preset_id)

    def get_theme(self, theme_id):
        return self.themes.get(theme_id)

    def list_themes(self, status=None):
        return [t for t in self.themes.values() if status is None or t.status == status]

    def get_content_recipe(self, recipe_id):
        return self.recipes.get(recipe_id)

    def get_render_profile(self, profile_id):
        return self.render_profiles.get(profile_id)

    def get_quality_profile(self, profile_id):
        return self.quality_profiles.get(profile_id)

    def update_task_plan(self, task_id, **fields):
        task = self.tasks[task_id]
        for key, value in fields.items():
            setattr(task, key, value)

    def transition_task(self, task_id, from_status, to_status, **kwargs):
        task = self.tasks[task_id]
        assert task.task_status == from_status, f"stale: {task.task_status} != {from_status}"
        task.task_status = to_status
        self.transitions.append((task_id, from_status, to_status))


class FakeAssetReader:
    def __init__(self) -> None:
        self.personas = {}
        self.looks = {}
        self.scenes = {}

    def get_persona(self, ref_id):
        return self.personas[ref_id]

    def get_look(self, ref_id):
        return self.looks[ref_id]

    def get_scene(self, ref_id):
        return self.scenes[ref_id]


def market_pack() -> MarketPack:
    return MarketPack(
        market_pack_id="MP_TH_DEFAULT_V1",
        pack_key="MP_TH_DEFAULT",
        target_country="TH",
        target_locale="th-TH",
        pack_name="泰国默认",
        pack_version=1,
        status="active",
    )


def render_preset() -> RenderPreset:
    return RenderPreset(
        render_preset_id="RP_STILL_VERTICAL_12S_V1",
        preset_key="RP_STILL_VERTICAL_12S",
        preset_name="12.5s",
        status="active",
        audio_rules_json={"default_strategy": "platform_hot_bgm", "fallback_strategy": "no_bgm"},
    )


def travel_theme() -> ThemeCatalog:
    return ThemeCatalog(
        theme_id="THEME_TH_TRAVEL_DEPARTURE_V1",
        theme_key="THEME_TH_TRAVEL_DEPARTURE",
        theme_name="机场旅行出发",
        status="active",
        applicable_markets_json=["TH"],
        product_match_rules_json={"categories": ["outerwear", "puffer_jacket"]},
        content_plan_rules_json={
            "look_source_priority": ["successful_look", "look_template", "ai_exploration"],
            "hashtag_seed": ["#ป้ายยา", "#ชุดเที่ยว"],
            "overlay_text_hint": "ไปเที่ยวแล้ว",
        },
        default_storyboard_json={
            "topic_template": "ลุคสนามบิน: {product}",
            "slots": [
                {"slot_index": 1, "slot_role": "hero", "purpose": "hook", "duration_ms": 2200, "motion": "slow_push", "transition_out": "short_dissolve"},
                {"slot_index": 2, "slot_role": "full_look", "purpose": "look", "duration_ms": 2600, "motion": "light_pan", "transition_out": "short_dissolve"},
                {"slot_index": 3, "slot_role": "lifestyle", "purpose": "life", "duration_ms": 2500, "motion": "static_hold", "transition_out": "short_dissolve"},
                {"slot_index": 4, "slot_role": "detail", "purpose": "detail", "duration_ms": 2200, "motion": "detail_zoom", "transition_out": "short_dissolve"},
                {"slot_index": 5, "slot_role": "second_angle", "purpose": "close", "duration_ms": 3000, "motion": "light_pan", "transition_out": "cut"},
            ],
        },
    )


def account() -> AccountProfile:
    return AccountProfile(
        account_id="OPV_UNIT_TEST_001",
        account_code="opv-th-test-001",
        account_name="test",
        target_country="TH",
        default_locale="th-TH",
        timezone="Asia/Bangkok",
        status="testing",
        persona_ref_id="TH_APPAREL_CAFE_001",
        allowed_look_refs_json=["STYLE_OPV_PUFFER_TRAVEL_001"],
        allowed_scene_refs_json=["ENV_AIRPORT_DEPART_001", "SCENE_A_001"],
        core_scene_refs_json=["ENV_AIRPORT_DEPART_001"],
        default_market_pack_id="MP_TH_DEFAULT_V1",
        default_render_preset_id="RP_STILL_VERTICAL_12S_V1",
        operating_rules_json={
            "human_review_required": False,
            "allowed_persona_refs": [
                "TH_APPAREL_CAFE_001", "TH_APPAREL_BRIGHT_B1_001"
            ]
        },
    )


def task(task_id="opv_task_1", status=TASK_DRAFT) -> ContentTask:
    return ContentTask(
        task_id=task_id,
        idempotency_key="a" * 64,
        account_id="OPV_UNIT_TEST_001",
        product_id="1737141103233042426",
        target_country="TH",
        target_locale="th-TH",
        market_pack_id="MP_TH_DEFAULT_V1",
        task_status=status,
        current_stage="intake",
        product_snapshot_json={
            "product": {
                "product_id": "1737141103233042426",
                "product_name": "浅蓝色短款蓬松外套",
                "category": "outerwear",
                "planned_look_ref": "STYLE_OPV_PUFFER_TRAVEL_001",
                "planned_scene_ref": "ENV_AIRPORT_DEPART_001",
                "reference_images": ["/tmp/ref_01.jpg", "/tmp/ref_02.jpg"],
            }
        },
    )


def fake_assets() -> FakeAssetReader:
    reader = FakeAssetReader()
    reader.personas["TH_APPAREL_CAFE_001"] = {
        "ref_id": "TH_APPAREL_CAFE_001",
        "name": "泰国甜妹",
        "prompt_core": "sweet thai girl",
        "reference_images": ["/tmp/persona.jpg"],
    }
    reader.personas["TH_APPAREL_BRIGHT_B1_001"] = {
        "ref_id": "TH_APPAREL_BRIGHT_B1_001",
        "name": "泰国明亮日常穿搭女生 B1",
        "prompt_core": "bright B1 creator",
        "local_reference_images": ["/tmp/B1.png"],
    }
    reader.looks["STYLE_OPV_PUFFER_TRAVEL_001"] = {
        "ref_id": "STYLE_OPV_PUFFER_TRAVEL_001",
        "name": "机场出发白裤组合",
        "recipe": {"bottom": "白色直筒宽松牛仔裤"},
        "prompt_core": "travel look",
    }
    reader.scenes["ENV_AIRPORT_DEPART_001"] = {
        "ref_id": "ENV_AIRPORT_DEPART_001",
        "name": "机场出发值机大厅",
        "prompt_core": "bright airport hall",
        "required_anchors": ["large_glass_facade"],
    }
    return reader


def build_service(repo: FakeRepository, assets=None) -> ContentPlannerService:
    return ContentPlannerService(repo, asset_reader=assets or fake_assets())


class PlannerHappyPathTest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = FakeRepository()
        self.repo.packs["MP_TH_DEFAULT_V1"] = market_pack()
        self.repo.presets["RP_STILL_VERTICAL_12S_V1"] = render_preset()
        self.repo.themes["THEME_TH_TRAVEL_DEPARTURE_V1"] = travel_theme()
        self.repo.accounts["OPV_UNIT_TEST_001"] = account()
        self.repo.tasks["opv_task_1"] = task()
        self.service = build_service(self.repo)

    def test_plan_moves_draft_to_planned_with_valid_contract(self) -> None:
        result = self.service.plan_task("opv_task_1")
        self.assertTrue(result.created)
        self.assertEqual(result.task.task_status, TASK_PLANNED)
        self.assertEqual(
            self.repo.transitions, [("opv_task_1", TASK_DRAFT, TASK_PLANNED)]
        )
        errors = contracts.validate_plan_json(result.plan)
        self.assertEqual(errors, [])
        plan = result.plan

        self.assertEqual(len(plan["content_signature"]["sha256"]), 64)
        self.assertEqual(plan["theme"]["id"], "THEME_TH_TRAVEL_DEPARTURE_V1")
        self.assertEqual(len(plan["shots"]), 5)
        self.assertEqual(
            sum(s["duration_ms"] for s in plan["shots"]), 12500
        )
        self.assertEqual(
            [s["slot_role"] for s in plan["shots"]],
            ["hero", "full_look", "lifestyle", "detail", "second_angle"],
        )
        contracts_by_slot = [s["composition_contract"] for s in plan["shots"]]
        self.assertEqual(
            [item["camera_angle"] for item in contracts_by_slot],
            [
                "front_eye_level",
                "front_eye_level",
                "three_quarter_dynamic",
                "front_or_three_quarter_close",
                "rear_three_quarter",
            ],
        )
        self.assertEqual(plan["shots"][3]["motion_preset"], "upper_body_focus")
        self.assertIn("full_body", contracts_by_slot[3]["forbidden"])
        self.assertEqual(plan["audio_policy"], {"strategy": "platform_hot_bgm", "fallback": "no_bgm"})
        self.assertIn("product_image:/tmp/ref_01.jpg", plan["shots"][0]["source_refs"])
        self.assertEqual(
            plan["look"]["snapshot"]["recipe"], {"bottom": "白色直筒宽松牛仔裤"}
        )
        self.assertEqual(plan["persona"]["snapshot"]["name"], "泰国甜妹")
        self.assertEqual(
            plan["persona"]["lock"]["persona_id"], "TH_APPAREL_CAFE_001"
        )
        self.assertEqual(
            plan["persona"]["lock"]["selection_scope"], "TASK_FROZEN"
        )
        self.assertEqual(
            len(plan["persona"]["lock"]["structured_snapshot_hash"]), 64
        )
        self.assertEqual(
            plan["render_contract"]["target_duration_ms"], 12500
        )

    def test_batch_grammar_changes_first_shot_scene_zone_and_safe_title(self) -> None:
        product = self.repo.tasks["opv_task_1"].product_snapshot_json["product"]
        product.update({
            "product_name": "1737141103233042426",
            "planned_shot_grammar": "G2",
            "planned_scene_zone": "玻璃幕墙",
            "planned_title_suffix": "ดูดีเทลชัดๆ",
        })
        self.repo.themes["THEME_TH_TRAVEL_DEPARTURE_V1"].default_storyboard_json[
            "topic_template"
        ] = "เดรสตัวเดียว 3 ลุค: {product}"
        result = self.service.plan_task("opv_task_1")
        self.assertEqual(result.plan["shot_grammar"]["id"], "G2")
        self.assertEqual(
            result.plan["shots"][0]["composition_contract"]["framing"],
            "waist_up_product",
        )
        self.assertIn("整理领口", result.plan["shots"][0]["purpose"])
        self.assertEqual(result.plan["scene"]["snapshot"]["selected_zone"], "玻璃幕墙")
        self.assertNotIn("1737141103233042426", result.plan["copy"]["title"])
        self.assertNotIn("เดรส", result.plan["copy"]["title"])
        self.assertIn("ดูดีเทลชัดๆ", result.plan["copy"]["title"])

    def test_task_can_freeze_an_allowed_persona(self) -> None:
        self.repo.tasks["opv_task_1"].product_snapshot_json["product"][
            "planned_persona_ref"
        ] = "TH_APPAREL_BRIGHT_B1_001"
        result = self.service.plan_task("opv_task_1")
        self.assertEqual(
            result.plan["persona"]["ref_id"], "TH_APPAREL_BRIGHT_B1_001"
        )
        self.assertEqual(
            result.plan["persona"]["snapshot"]["name"],
            "泰国明亮日常穿搭女生 B1",
        )

    def test_task_rejects_persona_outside_account_allow_list(self) -> None:
        self.repo.tasks["opv_task_1"].product_snapshot_json["product"][
            "planned_persona_ref"
        ] = "TH_APPAREL_NOT_ALLOWED"
        with self.assertRaises(ContentPlannerError):
            self.service.plan_task("opv_task_1")

    def test_task_rejects_persona_snapshot_with_different_stable_id(self) -> None:
        assets = fake_assets()
        assets.personas["TH_APPAREL_CAFE_001"] = {
            **assets.personas["TH_APPAREL_CAFE_001"],
            "persona_id": "TH_APPAREL_SELECTED_03_001",
        }
        with self.assertRaises(ContentPlannerError):
            build_service(self.repo, assets=assets).plan_task("opv_task_1")

    def test_fastcut_preset_normalizes_old_storyboard(self) -> None:
        preset = self.repo.presets["RP_STILL_VERTICAL_12S_V1"]
        preset.target_duration_ms = 10000
        preset.transition_rules_json = {
            "allowed_transitions": ["cut"],
            "default": "cut",
        }
        result = self.service.plan_task("opv_task_1")
        self.assertEqual(sum(s["duration_ms"] for s in result.plan["shots"]), 10000)
        self.assertEqual(
            [s["transition_out"] for s in result.plan["shots"]], ["cut"] * 5
        )

    def test_plan_is_idempotent_for_planned_task(self) -> None:
        first = self.service.plan_task("opv_task_1")
        second = self.service.plan_task("opv_task_1")
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.plan, second.plan)
        self.assertEqual(len(self.repo.transitions), 1)

    def test_force_replan_updates_existing_plan(self) -> None:
        self.service.plan_task("opv_task_1")
        forced = self.service.plan_task("opv_task_1", force=True, topic_text="ลุคใหม่")
        self.assertTrue(forced.created)
        self.assertEqual(forced.plan["theme"]["topic"], "ลุคใหม่")
        self.assertEqual(forced.task.task_status, TASK_PLANNED)

    def test_copy_sections_are_present_and_flagged_draft(self) -> None:
        result = self.service.plan_task("opv_task_1")
        copy = result.plan["copy"]
        self.assertEqual(copy["title"], result.task.topic_text)
        self.assertEqual(copy["hashtags"], ["#ป้ายยา", "#ชุดเที่ยว"])
        self.assertEqual(copy["cover_text"], "ไปเที่ยวแล้ว")
        self.assertEqual(copy["copy_status"], "ready_for_auto_render")
        self.assertEqual(result.task.copy_json, copy)

    def test_real_detail_reference_keeps_detail_zoom(self) -> None:
        product = self.repo.tasks["opv_task_1"].product_snapshot_json["product"]
        product["reference_roles"] = {"front": ["/tmp/ref_01.jpg"], "detail": ["/tmp/ref_02.jpg"]}
        result = self.service.plan_task("opv_task_1")
        self.assertEqual(result.plan["shots"][3]["motion_preset"], "detail_zoom")

    def test_outfit_breakdown_plan_puts_board_on_p1_and_anchor_on_p2(self) -> None:
        recipe = loader.load_content_recipe_file(
            PACKAGE_ROOT / "config" / "recipes" / "RECIPE_OUTFIT_BREAKDOWN_V1.json"
        )
        theme = loader.load_theme_file(
            PACKAGE_ROOT / "config" / "themes" / "THEME_TH_OUTFIT_BREAKDOWN_v1.json"
        )
        render_profile = loader.load_render_profile_file(
            PACKAGE_ROOT / "config" / "profiles" / "IMAGE_STORY_VIDEO_V1.json"
        )
        quality_profile = loader.load_quality_profile_file(
            PACKAGE_ROOT / "config" / "profiles" / f"{recipe.quality_profile_id}.json"
        )
        self.repo.recipes[recipe.recipe_id] = recipe
        self.repo.themes[theme.theme_id] = theme
        self.repo.render_profiles[render_profile.render_profile_id] = render_profile
        self.repo.quality_profiles[quality_profile.quality_profile_id] = quality_profile
        product = self.repo.tasks["opv_task_1"].product_snapshot_json["product"]
        product["planned_theme_id"] = theme.theme_id
        result = self.service.plan_task(
            "opv_task_1", recipe_id=recipe.recipe_id, theme_id=theme.theme_id,
            variant_index=2,
        )
        plan = result.plan
        self.assertEqual(contracts.validate_plan_json(plan), [])
        self.assertEqual(plan["anchor_slot"], 2)
        self.assertEqual(plan["quality_contract"]["assessment_schema_version"], 2)
        self.assertEqual(plan["quality_contract"]["decision_policy"], "hard_gates_only")
        self.assertEqual(plan["recipe_execution"]["content_goal"], "outfit_breakdown")
        self.assertEqual(plan["shots"][0]["shot_kind"], "composite_board")
        self.assertEqual(plan["shots"][0]["fit_mode"], "contain")
        self.assertEqual(plan["shots"][0]["generation_prompt"], "")
        self.assertEqual(plan["shots"][0]["board_spec"]["source_person_slot"], 2)
        self.assertEqual(
            plan["shots"][0]["board_spec"]["layout_variant"],
            plan["variation_plan"]["layout_variant"],
        )
        self.assertTrue(all(
            shot["outfit_state_ref"] == "FINAL" for shot in plan["shots"]
        ))

    def test_pure_color_account_profile_locks_reference_cover_layout(self) -> None:
        recipe = loader.load_content_recipe_file(
            PACKAGE_ROOT / "config" / "recipes" / "RECIPE_OUTFIT_BREAKDOWN_V1.json"
        )
        theme = loader.load_theme_file(
            PACKAGE_ROOT / "config" / "themes" / "THEME_TH_OUTFIT_BREAKDOWN_v1.json"
        )
        render_profile = loader.load_render_profile_file(
            PACKAGE_ROOT / "config" / "profiles" / "IMAGE_STORY_VIDEO_V1.json"
        )
        quality_profile = loader.load_quality_profile_file(
            PACKAGE_ROOT / "config" / "profiles" / f"{recipe.quality_profile_id}.json"
        )
        self.repo.recipes[recipe.recipe_id] = recipe
        self.repo.themes[theme.theme_id] = theme
        self.repo.render_profiles[render_profile.render_profile_id] = render_profile
        self.repo.quality_profiles[quality_profile.quality_profile_id] = quality_profile
        self.repo.accounts["OPV_UNIT_TEST_001"].operating_rules_json["presentation_profile"] = {
            "profile_id": "TH_PURE_COLOR_FASHION_V1",
            "background_mode": "solid_color",
            "background_color": "#F6F5F2",
        }
        product = self.repo.tasks["opv_task_1"].product_snapshot_json["product"]
        product["planned_theme_id"] = theme.theme_id
        plan = self.service.plan_task(
            "opv_task_1", recipe_id=recipe.recipe_id, theme_id=theme.theme_id
        ).plan
        self.assertEqual(plan["presentation_profile"]["background_mode"], "solid_color")
        self.assertEqual(
            plan["shots"][0]["board_spec"]["layout_id"],
            "LAYOUT_OUTFIT_REFERENCE_LEFT_V1",
        )
        self.assertEqual(plan["shots"][0]["board_spec"]["layout_variant"], "REF_LEFT_HERO")


class PlannerResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = FakeRepository()
        self.repo.packs["MP_TH_DEFAULT_V1"] = market_pack()
        self.repo.presets["RP_STILL_VERTICAL_12S_V1"] = render_preset()
        self.repo.themes["THEME_TH_TRAVEL_DEPARTURE_V1"] = travel_theme()
        self.repo.accounts["OPV_UNIT_TEST_001"] = account()
        self.service = build_service(self.repo)

    def test_theme_resolved_by_product_category(self) -> None:
        self.repo.tasks["t"] = task("t")
        result = self.service.plan_task("t")
        self.assertEqual(result.theme.theme_id, "THEME_TH_TRAVEL_DEPARTURE_V1")

    def test_explicit_theme_override(self) -> None:
        other = travel_theme()
        other.theme_id = "THEME_OTHER_V1"
        other.theme_key = "THEME_OTHER"
        other.product_match_rules_json = {"categories": ["dress"]}
        self.repo.themes["THEME_OTHER_V1"] = other
        self.repo.tasks["t"] = task("t")
        result = self.service.plan_task("t", theme_id="THEME_OTHER_V1")
        self.assertEqual(result.theme.theme_id, "THEME_OTHER_V1")

    def test_no_matching_theme_raises(self) -> None:
        task_fixture = task("t")
        task_fixture.product_snapshot_json["product"]["category"] = "swimwear"
        self.repo.tasks["t"] = task_fixture
        with self.assertRaises(ContentPlannerError):
            self.service.plan_task("t")

    def test_topic_from_template_uses_product_name(self) -> None:
        task_fixture = task("t")
        task_fixture.topic_text = None
        self.repo.tasks["t"] = task_fixture
        result = self.service.plan_task("t")
        self.assertIn("浅蓝色短款蓬松外套", result.plan["theme"]["topic"])

    def test_internal_product_placeholder_is_localized_in_thai_copy(self) -> None:
        task_fixture = task("t")
        task_fixture.topic_text = None
        task_fixture.product_snapshot_json["product"]["product_name"] = "目标外套"
        task_fixture.product_snapshot_json["product"]["category"] = "outerwear"
        self.repo.tasks["t"] = task_fixture
        result = self.service.plan_task("t")
        topic = result.plan["theme"]["topic"]
        copy = result.plan["copy"]
        self.assertIn("เสื้อตัวนอกตัวนี้", topic)
        self.assertIn("เสื้อตัวนอกตัวนี้", copy["caption"])
        for placeholder in (
            "目标外套", "目标连衣裙", "目标上装", "目标下装", "目标商品",
        ):
            self.assertNotIn(placeholder, topic)
            self.assertNotIn(placeholder, copy["title"])
            self.assertNotIn(placeholder, copy["caption"])

    def test_all_internal_placeholders_use_category_localized_display(self) -> None:
        cases = {
            "outerwear": ("目标外套", "เสื้อตัวนอกตัวนี้"),
            "dress": ("目标连衣裙", "เดรสตัวนี้"),
            "top": ("目标上装", "เสื้อตัวนี้"),
            "bottom": ("目标下装", "กางเกงตัวนี้"),
            "unknown": ("目标商品", "ไอเทมชิ้นนี้"),
        }
        for category, (placeholder, expected) in cases.items():
            with self.subTest(category=category):
                self.assertEqual(
                    self.service._safe_product_display_name({
                        "product_name": placeholder, "category": category,
                    }),
                    expected,
                )

    def test_planned_look_outside_allowlist_rejected(self) -> None:
        task_fixture = task("t")
        task_fixture.product_snapshot_json["product"]["planned_look_ref"] = "STYLE_NOT_ALLOWED"
        self.repo.tasks["t"] = task_fixture
        with self.assertRaises(ContentPlannerError) as ctx:
            self.service.plan_task("t")
        self.assertIn("allow-list", str(ctx.exception))

    def test_automatic_look_filters_product_and_rotates_compatible_candidates(self) -> None:
        acct = self.repo.accounts["OPV_UNIT_TEST_001"]
        acct.allowed_look_refs_json = ["STYLE_A", "STYLE_WRONG", "STYLE_B"]
        assets = fake_assets()
        assets.looks.update({
            "STYLE_A": {
                "ref_id": "STYLE_A",
                "applicable_product_codes": ["1737141103233042426"],
                "compatibility": {"product_types": ["outerwear"]},
                "recipe": {"top_inner": "白色背心", "bottom": "白色长裤"},
            },
            "STYLE_WRONG": {
                "ref_id": "STYLE_WRONG",
                "applicable_product_codes": ["another_product"],
                "compatibility": {"product_types": ["outerwear"]},
                "recipe": {"bottom": "黑色短裙"},
            },
            "STYLE_B": {
                "ref_id": "STYLE_B",
                "applicable_product_codes": ["1737141103233042426"],
                "compatibility": {"product_types": ["outerwear"]},
                "recipe": {"top_inner": "针织连衣裙", "bottom": "连衣裙"},
            },
        })
        service = build_service(self.repo, assets=assets)
        product_snapshot = task("t").product_snapshot_json["product"]
        product_snapshot.pop("planned_look_ref")

        first, _ = service._resolve_look(
            acct, product_snapshot, theme=travel_theme(), variant_index=1
        )
        second, _ = service._resolve_look(
            acct, product_snapshot, theme=travel_theme(), variant_index=2
        )

        self.assertEqual({first, second}, {"STYLE_A", "STYLE_B"})
        self.assertNotEqual(first, second)

    def test_explicit_incompatible_look_is_rejected(self) -> None:
        acct = self.repo.accounts["OPV_UNIT_TEST_001"]
        acct.allowed_look_refs_json = ["STYLE_WRONG"]
        assets = fake_assets()
        assets.looks["STYLE_WRONG"] = {
            "ref_id": "STYLE_WRONG",
            "applicable_product_codes": ["another_product"],
            "compatibility": {"product_types": ["outerwear"]},
            "recipe": {"bottom": "黑色短裙"},
        }
        service = build_service(self.repo, assets=assets)
        product_snapshot = task("t").product_snapshot_json["product"]
        product_snapshot["planned_look_ref"] = "STYLE_WRONG"

        with self.assertRaisesRegex(ContentPlannerError, "incompatible"):
            service._resolve_look(acct, product_snapshot, theme=travel_theme())

    def test_account_without_persona_rejected(self) -> None:
        acct = account()
        acct.persona_ref_id = None
        self.repo.accounts["OPV_UNIT_TEST_001"] = acct
        self.repo.tasks["t"] = task("t")
        with self.assertRaises(ContentPlannerError):
            self.service.plan_task("t")

    def test_non_plannable_status_rejected(self) -> None:
        self.repo.tasks["t"] = task("t", status=TASK_IMAGE_REVIEW)
        with self.assertRaises(ContentPlannerError):
            self.service.plan_task("t")

    def test_missing_task_rejected(self) -> None:
        with self.assertRaises(ContentPlannerError):
            self.service.plan_task("ghost")


class PlannerWithoutAssetReaderTest(unittest.TestCase):
    def test_plan_works_with_ref_ids_only(self) -> None:
        repo = FakeRepository()
        repo.packs["MP_TH_DEFAULT_V1"] = market_pack()
        repo.presets["RP_STILL_VERTICAL_12S_V1"] = render_preset()
        repo.themes["THEME_TH_TRAVEL_DEPARTURE_V1"] = travel_theme()
        repo.accounts["OPV_UNIT_TEST_001"] = account()
        repo.tasks["t"] = task("t")
        service = ContentPlannerService(repo, asset_reader=None)
        result = service.plan_task("t")
        self.assertEqual(result.plan["look"]["snapshot"], {})
        self.assertEqual(contracts.validate_plan_json(result.plan), [])


if __name__ == "__main__":
    unittest.main()
