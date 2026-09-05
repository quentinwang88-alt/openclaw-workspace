#!/usr/bin/env python3
"""Capability upgrade tests: product facts / outfit plan / package / facade."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest
from datetime import datetime

from domain import statuses

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import statuses
from domain.models import (
    AccountProfile,
    ContentPackage,
    ContentTask,
    MarketPack,
    QualityProfile,
    RenderProfile,
    RenderPreset,
    ThemeCatalog,
)
from services.content_package import ContentPackageError, ContentPackageService
from services.content_story import (
    StoryGenerationError,
    generate_product_image_story,
    resolve_account_for_market,
)
from services.outfit_planner import (
    build_outfit_plan,
    build_outfit_states,
    build_product_facts,
)


def product(category="outerwear"):
    return {
        "product_id": "1737141103233042426",
        "product_name": "浅蓝色短款蓬松外套",
        "category": category,
        "color": "light blue",
        "reference_images": ["/tmp/ref_01.jpg"],
    }


def travel_plan_theme_id():
    return "THEME_TH_TRAVEL_DEPARTURE_V1"


def _recipe_stub():
    from domain.models import ContentRecipe

    return ContentRecipe(
        recipe_id="RECIPE_SCENE_SOLUTION_V1",
        recipe_key="RECIPE_SCENE_SOLUTION",
        content_goal="scene_solution",
        hook_types_json=["scene_problem", "final_effect"],
        anchor_slot=1,
        shot_count=5,
        status="active",
        render_profile_id="IMAGE_STORY_VIDEO_V1",
        quality_profile_id="QUALITY_STANDARD_V1",
        suitable_topics_json=["travel_departure"],
        story_structure_json=[
            {"slot_index": i, "narrative_function": fn, "slot_role": "hero", "purpose": "p"}
            for i, fn in enumerate(
                ["HOOK", "CONTEXT", "TRANSFORMATION", "PROOF", "PAYOFF"], 1
            )
        ],
    )


class ProductFactsTest(unittest.TestCase):
    def test_facts_extract_and_lock(self) -> None:
        facts = build_product_facts(product())
        self.assertEqual(facts["product_id"], "1737141103233042426")
        self.assertEqual(facts["category"], "outerwear")
        self.assertEqual(facts["facts"]["color"], "light blue")
        self.assertIn("color", facts["locked_features"])
        self.assertEqual(facts["facts"]["material"], "unknown")

    def test_fails_without_reference_images(self) -> None:
        bad = product()
        bad["reference_images"] = []
        with self.assertRaises(Exception):
            build_product_facts(bad)


class OutfitPlanTest(unittest.TestCase):
    def test_default_palette_uses_only_explicit_product_color(self) -> None:
        unknown_product = product()
        unknown_product.pop("color")
        unknown_plan = build_outfit_plan(
            outfit_plan_id="unknown-color",
            theme_id=travel_plan_theme_id(),
            product_facts=build_product_facts(unknown_product),
        )
        normalized = [str(value).lower() for value in unknown_plan["color_palette"]]
        self.assertNotIn("product_color", normalized)
        self.assertNotIn("unknown", normalized)
        self.assertNotIn("light blue", normalized)

        brown_product = product()
        brown_product["color"] = "brown"
        brown_plan = build_outfit_plan(
            outfit_plan_id="brown-color",
            theme_id=travel_plan_theme_id(),
            product_facts=build_product_facts(brown_product),
        )
        self.assertEqual(brown_plan["color_palette"][0], "brown")
        self.assertNotIn("light blue", brown_plan["color_palette"])

    def test_plan_answers_styling_logic(self) -> None:
        rules = {
            "by_content_key": {
                "travel_departure": {
                    "bottom": {"type": "白色直筒宽松牛仔裤", "color": "白色"},
                    "shoes": "小白鞋",
                    "bag": "小包",
                    "accessories": "极简",
                    "color_palette": ["white"],
                    "style_direction": "旅行出发",
                    "occasion": "airport",
                }
            },
            "climate_rules": {"tropical_humid": "轻薄透气"},
            "product_visibility_rules": {"product_is_core": True},
        }
        facts = build_product_facts(product())
        plan = build_outfit_plan(
            outfit_plan_id="opv_outfit_1",
            theme_id=travel_plan_theme_id(),
            product_facts=facts,
            market_pack={"climate_zone": "tropical_humid"},
            rules=rules,
        )
        self.assertEqual(plan["bottom"]["type"], "白色直筒宽松牛仔裤")
        self.assertIn("产品是造型核心单品", plan["styling_logic"])
        self.assertIn("旅行出发", plan["styling_logic"])
        self.assertTrue(plan["product_visibility_rules"]["product_is_core"])
        self.assertEqual(plan["theme"], "travel_departure")

    def test_recipe_goals_create_explicit_outfit_states(self) -> None:
        plan = {
            "bottom": {"type": "高腰直筒裤", "color": "白色"},
            "shoes": "小白鞋",
            "bag": "小包",
            "accessories": "耳饰",
            "color_palette": ["light blue", "white"],
            "style_direction": "清爽",
        }
        self.assertEqual(
            set(build_outfit_states(plan, content_goal="scene_solution")),
            {"FINAL"},
        )
        self.assertEqual(
            set(build_outfit_states(plan, content_goal="outfit_breakdown")),
            {"FINAL"},
        )
        self.assertEqual(
            set(build_outfit_states(plan, content_goal="visual_transform")),
            {"BASE", "FINAL", "ALT_1"},
        )

    def test_selected_look_drives_final_outfit_state(self) -> None:
        facts = build_product_facts(product())
        plan = build_outfit_plan(
            outfit_plan_id="opv_outfit_look",
            theme_id=travel_plan_theme_id(),
            product_facts=facts,
            look={
                "name": "一衣多穿",
                "recipe": {
                    "top_inner": "白色针织连衣裙",
                    "bottom": "连衣裙（无独立下装）",
                    "footwear": "小白鞋",
                    "accessories": "细腰带",
                    "overall_style": "一衣多穿",
                },
            },
            content_goal="visual_transform",
        )
        states = build_outfit_states(plan, content_goal="visual_transform")
        self.assertEqual(states["FINAL"]["top_inner"], "白色针织连衣裙")
        self.assertEqual(states["FINAL"]["bottom"]["type"], "连衣裙（无独立下装）")
        self.assertEqual(states["BASE"]["bottom"], states["FINAL"]["bottom"])
        self.assertEqual(states["ALT_1"]["top_inner"], states["FINAL"]["top_inner"])
        self.assertEqual(states["ALT_1"]["bottom"], states["FINAL"]["bottom"])
        self.assertIn("outfit_state", plan["product_visibility_rules"]["consistency"])


class FakeRepo:
    def __init__(self):
        self.accounts = {}
        self.tasks = {}
        self.packages = {}
        self.themes = {}
        self.presets = {}
        self.packs = {}
        self.recipes = {
            "RECIPE_SCENE_SOLUTION_V1": _recipe_stub()
        }
        self.render_profiles = {
            "IMAGE_STORY_VIDEO_V1": RenderProfile(
                render_profile_id="IMAGE_STORY_VIDEO_V1",
                profile_key="IMAGE_STORY_VIDEO",
                status="active",
                motion_rules_json={"allowed": ["slow_push", "light_pan"]},
                transition_rules_json={"allowed": ["cut"], "default": "cut"},
            )
        }
        self.quality_profiles = {
            "QUALITY_STANDARD_V1": QualityProfile(
                quality_profile_id="QUALITY_STANDARD_V1",
                profile_key="QUALITY_STANDARD",
                status="active",
                dimensions_json={
                    "product_fidelity": {
                        "scope": ["single", "group"], "min_score": 80,
                        "hard_gate": True,
                        "fail_action": "regenerate_offending_slot",
                    }
                },
            )
        }

    def list_account_profiles(self):
        return list(self.accounts.values())

    def get_account_profile(self, account_id):
        return self.accounts.get(account_id)

    def get_market_pack(self, pack_id):
        return self.packs.get(pack_id)

    def get_render_preset(self, preset_id):
        return self.presets.get(preset_id)

    def get_theme(self, theme_id):
        return self.themes.get(theme_id)

    def get_content_recipe(self, recipe_id):
        return self.recipes.get(recipe_id)

    def get_render_profile(self, profile_id):
        return self.render_profiles.get(profile_id)

    def get_quality_profile(self, profile_id):
        return self.quality_profiles.get(profile_id)

    def get_content_package_by_task(self, task_id):
        for pkg in self.packages.values():
            if pkg.task_id == task_id:
                return pkg
        return None

    def update_task_plan(self, task_id, **fields):
        task = self.tasks.get(task_id)
        if task is not None:
            for k, v in fields.items():
                setattr(task, k, v)

    def transition_task(self, task_id, from_status, to_status, **kwargs):
        statuses.task_ensure_transition(from_status, to_status)
        task = self.tasks[task_id]
        assert task.task_status == from_status
        task.task_status = to_status

    def create_task_idempotent(self, task):
        if task.idempotency_key in {
            t.idempotency_key for t in self.tasks.values()
        }:
            for existing in self.tasks.values():
                if existing.idempotency_key == task.idempotency_key:
                    return existing, False
        self.tasks[task.task_id] = task
        return task, True

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def insert_content_package(self, pkg):
        self.packages[pkg.content_package_id] = pkg

    def get_content_package(self, pkg_id):
        return self.packages.get(pkg_id)

    def get_content_package_by_task(self, task_id):
        for pkg in self.packages.values():
            if pkg.task_id == task_id:
                return pkg
        return None

    def update_content_package(self, pkg_id, **fields):
        for k, v in fields.items():
            setattr(self.packages[pkg_id], k, v)


def world():
    repo = FakeRepo()
    account = AccountProfile(
        account_id="OPV_UNIT_TEST_001",
        account_code="c",
        account_name="n",
        target_country="TH",
        default_locale="th-TH",
        timezone="Asia/Bangkok",
        status="testing",
        persona_ref_id="P1",
        allowed_look_refs_json=["LOOK_1"],
        allowed_scene_refs_json=["SCENE_1"],
        core_scene_refs_json=["SCENE_1"],
        operating_rules_json={"allowed_persona_refs": ["P1"]},
    )
    repo.accounts[account.account_id] = account
    task = ContentTask(
        task_id="opv_task_story_1",
        idempotency_key="a" * 64,
        account_id="OPV_UNIT_TEST_001",
        product_id="P1",
        target_country="TH",
        target_locale="th-TH",
        task_status="planned",
        current_stage="planning",
        plan_json={
            "schema_version": "opv-plan-v1",
            "theme": {"id": "THEME_T", "topic": "t"},
            "recipe": {"id": "RECIPE_SCENE_SOLUTION_V1", "version": 1},
            "anchor_slot": 1,
            "persona": {"ref_id": "P1", "snapshot": {}},
            "look": {"ref_id": "L1", "source_type": "look_template", "snapshot": {}},
            "scene": {"ref_id": "S1", "snapshot": {}},
            "copy": {"title": "t", "caption": "c", "hashtags": [], "cover_text": ""},
            "audio_policy": {"strategy": "platform_hot_bgm", "fallback": "no_bgm"},
            "shots": [
                {"slot_index": i, "slot_role": "hero", "purpose": "p", "duration_ms": 2000,
                 "motion_preset": "slow_push", "transition_out": "cut",
                 "narrative_function": fn}
                for i, fn in enumerate(
                    ["HOOK", "CONTEXT", "TRANSFORMATION", "PROOF", "PAYOFF"], 1
                )
            ],
        },
        content_package_id=None,
        recipe_id="RECIPE_SCENE_SOLUTION_V1",
    )
    repo.tasks[task.task_id] = task
    return repo, task


class ContentPackageTest(unittest.TestCase):
    def test_lifecycle_planning_to_rendered(self) -> None:
        repo, task = world()
        service = ContentPackageService(repo)
        pkg = service.create_for_task(task, recipe_id=task.recipe_id, plan=task.plan_json)
        self.assertEqual(pkg.status, "planning")
        # creating twice returns the same package (no duplicates)
        again = service.create_for_task(task, recipe_id=task.recipe_id)
        self.assertEqual(again.content_package_id, pkg.content_package_id)

        service.advance_for_task(task.task_id, statuses.PACKAGE_GENERATING)
        service.advance_for_task(task.task_id, statuses.PACKAGE_QA_REVIEW)
        service.advance_for_task(task.task_id, statuses.PACKAGE_READY,
                                 cover_title="t")
        pkg = service.advance_for_task(task.task_id, statuses.PACKAGE_RENDERED,
                                       render_ids_json=["render_1"])
        self.assertEqual(pkg.status, "rendered")
        self.assertEqual(pkg.render_ids_json, ["render_1"])

    def test_advance_noop_without_package(self) -> None:
        repo, task = world()
        task.content_package_id = None
        service = ContentPackageService(repo)
        self.assertIsNone(service.advance_for_task(task.task_id, "generating"))

    def test_illegal_advance_keeps_state(self) -> None:
        repo, task = world()
        service = ContentPackageService(repo)
        pkg = service.create_for_task(task, recipe_id="r", plan=task.plan_json)
        with self.assertRaises(ContentPackageError):
            service.advance_for_task(task.task_id, "rendered")
        self.assertEqual(pkg.status, "planning")


class StoryFacadeTest(unittest.TestCase):
    def test_recipe_rejects_incompatible_theme(self) -> None:
        from services.content_planner import ContentPlannerError, ContentPlannerService

        repo, _task = world()
        incompatible = ThemeCatalog(
            theme_id="THEME_TH_COLOR_POP_V1",
            theme_key="THEME_TH_COLOR_POP",
            theme_name="color",
            status="active",
        )
        with self.assertRaisesRegex(ContentPlannerError, "not suitable"):
            ContentPlannerService(repo)._validate_recipe_theme(
                repo.recipes["RECIPE_SCENE_SOLUTION_V1"], incompatible
            )

    def test_requires_product_snapshot(self) -> None:
        repo, _task = world()
        with self.assertRaises(StoryGenerationError):
            generate_product_image_story(
                repo, product_id="P1", market="TH", language="th-TH",
                recipe_id="RECIPE_SCENE_SOLUTION_V1",
            )

    def test_requires_matching_account(self) -> None:
        repo, _task = world()
        with self.assertRaises(StoryGenerationError):
            generate_product_image_story(
                repo, product_id="P1", market="MX", language="es-MX",
                recipe_id="r", product_snapshot=product(),
            )

    def test_creates_idempotent_story_tasks(self) -> None:
        repo, _task = world()
        # need a planner-compatible theme + preset for plan_task
        repo.themes["THEME_TH_TRAVEL_DEPARTURE_V1"] = ThemeCatalog(
            theme_id="THEME_TH_TRAVEL_DEPARTURE_V1",
            theme_key="THEME_TH_TRAVEL_DEPARTURE",
            theme_name="travel",
            status="active",
            applicable_markets_json=["TH"],
            product_match_rules_json={"categories": ["outerwear"]},
            content_plan_rules_json={"overlay_text_hint": "x"},
            default_storyboard_json={
                "topic_template": "ลุคสนามบิน: {product}",
                "slots": [
                    {"slot_index": i, "slot_role": "hero", "duration_ms": 2000}
                    for i in range(1, 6)
                ],
            },
        )
        repo.presets["RP_STILL_FASTCUT_10S_V1"] = RenderPreset(
            render_preset_id="RP_STILL_FASTCUT_10S_V1",
            preset_key="RP_STILL_FASTCUT_10S",
            preset_name="10s",
            status="active",
            target_duration_ms=10000,
            transition_rules_json={"allowed_transitions": ["cut"], "default": "cut"},
            output_rules_json={"timeline_defaults_ms": {str(i): 2000 for i in range(1, 6)}},
        )
        repo.packs["MP_TH_DEFAULT_V1"] = MarketPack(
            market_pack_id="MP_TH_DEFAULT_V1",
            pack_key="MP_TH_DEFAULT",
            target_country="TH",
            target_locale="th-TH",
            pack_name="p",
            status="active",
        )
        account = repo.accounts["OPV_UNIT_TEST_001"]
        account.default_market_pack_id = "MP_TH_DEFAULT_V1"
        account.default_render_preset_id = "RP_STILL_FASTCUT_10S_V1"
        # re-register account so profile list is fresh
        repo.accounts["OPV_UNIT_TEST_001"] = account

        class NullAssets:
            def get_persona(self, ref):
                return {"persona_id": ref, "name": ref}

            def get_look(self, ref):
                return {}

            def get_scene(self, ref):
                return {}

        results = generate_product_image_story(
            repo,
            product_id="1737141103233042426",
            market="TH",
            language="th-TH",
            recipe_id="RECIPE_SCENE_SOLUTION_V1",
            theme_id="THEME_TH_TRAVEL_DEPARTURE_V1",
            variant_count=2,
            product_snapshot=product(),
            asset_reader=NullAssets(),
        )
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["created"] for r in results))
        self.assertNotEqual(results[0]["task_id"], results[1]["task_id"])
        self.assertEqual(len(results[0]["shots"]), 5)
        self.assertEqual(results[0]["shots"][0]["narrative_function"], "HOOK")
        self.assertEqual(results[0]["hook_strategy"], "scene_problem")
        self.assertEqual(results[1]["hook_strategy"], "final_effect")
        self.assertTrue(
            all(s["outfit_state_ref"] == "FINAL" for s in results[0]["shots"])
        )
        plan = repo.tasks[results[0]["task_id"]].plan_json
        self.assertEqual(
            plan["render_contract"]["render_profile_id"],
            "IMAGE_STORY_VIDEO_V1",
        )
        self.assertEqual(
            plan["quality_contract"]["quality_profile_id"],
            "QUALITY_STANDARD_V1",
        )

        # same-day repeat is idempotent on the intake side
        repeat = generate_product_image_story(
            repo,
            product_id="1737141103233042426",
            market="TH",
            language="th-TH",
            recipe_id="RECIPE_SCENE_SOLUTION_V1",
            theme_id="THEME_TH_TRAVEL_DEPARTURE_V1",
            variant_count=2,
            product_snapshot=product(),
            asset_reader=NullAssets(),
        )
        self.assertFalse(repeat[0]["created"])

    def test_account_selection_requires_explicit_id_when_ambiguous(self) -> None:
        repo, _task = world()
        second = repo.accounts["OPV_UNIT_TEST_001"]
        import copy

        second = copy.deepcopy(second)
        second.account_id = "OPV_TH_TEST_002"
        repo.accounts[second.account_id] = second
        with self.assertRaisesRegex(StoryGenerationError, "multiple"):
            generate_product_image_story(
                repo,
                product_id="P1",
                market="TH",
                language="th-TH",
                recipe_id="RECIPE_SCENE_SOLUTION_V1",
                product_snapshot=product(),
            )
        chosen = resolve_account_for_market(
            repo, "TH", "th-TH", account_id="OPV_TH_TEST_002"
        )
        self.assertEqual(chosen.account_id, "OPV_TH_TEST_002")


if __name__ == "__main__":
    unittest.main()
