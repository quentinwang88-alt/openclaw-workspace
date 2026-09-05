import copy
import unittest
from collections import Counter
from dataclasses import replace

from config import loader
from domain import contracts
from services.content_planner import ContentPlannerService, ContentPlannerError
from services.multi_look_planner import (history_look_usage, multi_look_durations,
                                         select_multi_look_sequence, sequence_axes)
from services.batch_diversity_planner import BatchDiversityPlanner
from tests.test_content_planner import FakeRepository, account, fake_assets, market_pack, task
from tests.test_look_library_selection import LibraryAssets
from tests.test_batch_diversity_planner import FakeRepo, products, specs


def looks(count=6):
    bottoms = ["白色高腰阔腿裤", "黑色百褶短裙", "深蓝色直筒牛仔裤", "棕色直筒长裤", "浅蓝色阔腿牛仔裤", "灰色长裙"]
    return [{"ref_id": f"LOOK_LIB_{i + 1}", "name": f"真实模板{i + 1}", "status": "enabled",
             "applicable_product_codes": ["1737141103233042426"],
             "compatibility": {"product_types": ["outerwear"]},
             "recipe": {"top_inner": "白色修身T恤", "bottom": bottom, "footwear": "白色平底运动鞋", "bag": "none", "accessories": "none"},
             "recipe_field_sources": {"bottom": {"source": "structured_columns", "columns": ["bottom_type"]}}}
            for i, bottom in enumerate(bottoms[:count])]


class MultiLookSelectionTests(unittest.TestCase):
    def test_fresh_template_beats_repeated_rare_shape_with_fixed_first_look(self):
        rows = looks(3)
        rows[0]["recipe"]["bottom"] = "白色直筒长裤"
        rows[1]["recipe"].update(bottom="黑色短裙", footwear="黑色短靴")
        rows[2]["recipe"]["bottom"] = "蓝色直筒长裤"
        history = Counter({("look_ref", rows[1]["ref_id"]): 5})
        chosen, _ = select_multi_look_sequence(rows, first_ref=rows[0]["ref_id"], history=history)
        self.assertEqual(chosen[1]["look_ref"], rows[2]["ref_id"])

    def test_explicit_shoes_alias_survives_prepare_and_freeze_without_fallback(self):
        from services.look_selection import prepare_look_for_policy
        from services.outfit_planner import freeze_multi_look_state
        profile = account()
        profile.operating_rules_json["look_selection_mode"] = "auto_library"
        look = looks(1)[0]
        look["recipe"].pop("footwear")
        look["recipe"]["shoes"] = "黑色玛丽珍单鞋"
        prepared = prepare_look_for_policy(profile, look)
        self.assertNotIn("footwear", prepared["recipe"])
        self.assertNotIn("footwear_neutral_fallback", prepared.get("normalization_warnings", []))
        state = freeze_multi_look_state(prepared)
        self.assertEqual(state["shoes"], "黑色玛丽珍单鞋")
        chosen, _ = select_multi_look_sequence([prepared])
        self.assertEqual(chosen[0]["visible_axes"]["footwear_family"], "MARY_JANE")

    def test_visible_changes_prefer_shape_shoes_and_inner_over_color_only(self):
        rows = looks(4)
        rows[0]["recipe"].update(bottom="白色直筒长裤")
        rows[1]["recipe"].update(bottom="黑色直筒长裤")
        rows[2]["recipe"].update(bottom="蓝色直筒长裤", footwear="黑色短靴", top_inner="灰色针织衫")
        rows[3]["recipe"].update(bottom="黑色百褶短裙", footwear="黑色玛丽珍", top_inner="条纹短T恤")
        chosen, report = select_multi_look_sequence(rows, first_ref=rows[0]["ref_id"])
        self.assertEqual(chosen[1]["look_ref"], rows[3]["ref_id"])
        self.assertEqual(report["selected_footwear_family_count"], 3)
        self.assertEqual(report["selected_shape_count"], 2)
        self.assertEqual(report["duration_ms"], 6000)
        # When all silhouettes are the same, shoe + inner changes beat a new ID/colour.
        chosen, _ = select_multi_look_sequence(rows[:3], first_ref=rows[0]["ref_id"])
        self.assertEqual(chosen[1]["look_ref"], rows[2]["ref_id"])

    def test_five_distinct_states_soft_silhouettes_and_stable_selection(self):
        chosen, report = select_multi_look_sequence(looks(), first_ref="LOOK_LIB_1")
        retry, _ = select_multi_look_sequence(looks(), first_ref="LOOK_LIB_1")
        self.assertEqual(chosen, retry)
        self.assertEqual(len({entry["fingerprint"] for entry in chosen}), 5)
        self.assertGreaterEqual(report["selected_silhouette_count"], 3)
        self.assertEqual([entry["state_id"] for entry in chosen], [f"LOOK_{i:02d}" for i in range(1, 6)])
        self.assertTrue(all(a["silhouette_key"] != b["silhouette_key"] for a, b in zip(chosen, chosen[1:])))

    def test_duplicate_names_and_bag_changes_do_not_invent_extra_pages(self):
        rows = looks(2)
        duplicate = copy.deepcopy(rows[0])
        duplicate["ref_id"] = "RENAMED"
        duplicate["recipe"]["bag"] = "black bag"
        selected, report = select_multi_look_sequence(rows + [duplicate])
        self.assertEqual(len(selected), 2)
        self.assertEqual(report["actual_count"], 2)
        self.assertIn("fewer_distinct_outfits_reduce_actual_pages", report["degradation_reasons"])

    def test_similar_silhouettes_are_soft_not_blocking(self):
        rows = looks(5)
        for index, row in enumerate(rows):
            row["recipe"].update(bottom="白色直筒裤", top_inner=f"pattern-{index} tee")
        selected, report = select_multi_look_sequence(rows)
        self.assertEqual(len(selected), 5)
        self.assertIn("silhouette_variety_below_soft_preference", report["degradation_reasons"])

    def test_all_page_usage_counted_once_and_history_balances_next_selection(self):
        chosen, _ = select_multi_look_sequence(looks())
        axes = sequence_axes(chosen)
        usage = history_look_usage([{**axes[0], "look_sequence": axes}])
        self.assertEqual(sum(n for (axis, _), n in usage.items() if axis == "look_ref"), 5)
        next_batch, _ = select_multi_look_sequence(looks(), history=usage)
        unused = {row["ref_id"] for row in looks()} - {row["look_ref"] for row in chosen}
        self.assertIn(next_batch[0]["look_ref"], unused)

    def test_one_to_five_page_durations_sum_to_six_seconds(self):
        self.assertEqual(multi_look_durations(5), [1600, 1000, 1000, 1000, 1400])
        for count in range(1, 6):
            durations = multi_look_durations(count)
            self.assertEqual(len(durations), count)
            self.assertEqual(sum(durations), 6000)
            self.assertTrue(all(duration > 0 for duration in durations))

    def test_batch_freezes_five_templates_and_histories_include_all_pages(self):
        repo, assets = FakeRepo(), LibraryAssets()
        recipe = loader.load_content_recipe_file(loader.RECIPE_DIR / "RECIPE_MULTI_LOOK_V1.json")
        repo.recipes[recipe.recipe_id] = recipe
        multi_specs = [replace(s, recipe_id=recipe.recipe_id, look_ref="") for s in specs()[:2]]
        batch = BatchDiversityPlanner(repo, assets).plan(record_id="multi-batch", specs=multi_specs, products=products()[:2])
        for assignment in batch:
            self.assertEqual(len(assignment.product_snapshot["planned_look_sequence"]), 5)
            self.assertEqual(assignment.metadata["axes"]["actual_look_count"], 5)
            self.assertEqual(len(assignment.metadata["axes"]["look_sequence"]), 5)
        usage = history_look_usage([row.metadata["axes"] for row in batch])
        self.assertEqual(sum(n for (axis, _), n in usage.items() if axis == "look_ref"), 10)


class MultiLookContentPlanTests(unittest.TestCase):
    def make_service(self, count=6):
        repo, assets = FakeRepository(), fake_assets()
        bundle = loader.load_seed_bundle()
        profile = account()
        profile.allowed_look_refs_json = [row["ref_id"] for row in looks(count)]
        profile.default_render_preset_id = bundle.render_presets[0].render_preset_id
        profile.operating_rules_json.pop("presentation_profile", None)
        repo.accounts[profile.account_id] = profile
        repo.packs["MP_TH_DEFAULT_V1"] = market_pack()
        repo.presets = {p.render_preset_id: p for p in bundle.render_presets}
        repo.themes = {theme.theme_id: theme for theme in bundle.themes}
        repo.recipes = {r.recipe_id: r for r in bundle.content_recipes}
        repo.render_profiles = {p.render_profile_id: p for p in bundle.render_profiles}
        repo.quality_profiles = {p.quality_profile_id: p for p in bundle.quality_profiles}
        current = task()
        current.recipe_id = "RECIPE_MULTI_LOOK_V1"
        current.theme_id = "THEME_TH_OUTFIT_BREAKDOWN_V1"
        current.product_snapshot_json["product"]["planned_look_ref"] = "LOOK_LIB_1"
        repo.tasks[current.task_id] = current
        assets.looks = {row["ref_id"]: row for row in looks(count)}
        return repo, assets, ContentPlannerService(repo, assets)

    def test_new_plan_full_b_to_e_and_independent_a_board(self):
        repo, _, service = self.make_service()
        plan = service.plan_task("opv_task_1").plan
        self.assertEqual(contracts.validate_plan_json(plan), [])
        self.assertEqual(plan["workflow_version"], 2)
        self.assertEqual(plan["anchor_slot"], 1)
        self.assertEqual(plan["actual_shot_count"], 5)
        self.assertEqual(set(plan["outfit_states"]), {f"LOOK_{i:02d}" for i in range(1, 6)})
        self.assertEqual([s["outfit_state_ref"] for s in plan["shots"]], [f"LOOK_{i:02d}" for i in range(1, 6)])
        self.assertEqual([s["duration_ms"] for s in plan["shots"]], [1600, 1000, 1000, 1000, 1400])
        first = plan["shots"][0]
        self.assertEqual(first["shot_kind"], "composite_board")
        self.assertEqual(first["board_spec"]["source_person_slot"], 1)
        self.assertEqual(first["board_spec"]["source_outfit_state_ref"], "LOOK_01")
        self.assertEqual(first["board_spec"]["item_contrast_policy"], "soft_silhouette_v1")
        self.assertEqual(first["board_spec"]["item_layout_policy"], "compact_stack_v2")
        self.assertEqual(plan["anchor_photo_spec"]["shot_kind"], "generated_photo")
        self.assertEqual(plan["anchor_photo_spec"]["outfit_state_ref"], "LOOK_01")
        self.assertNotEqual(first["source_look_ref"], plan["shots"][1]["source_look_ref"])
        for shot in plan["shots"][1:]:
            self.assertEqual(shot["composition_contract"]["framing"], "full_body")
            self.assertNotIn("back", shot["composition_contract"]["pose"])
            self.assertEqual(shot["motion_preset"], "static_hold")
            self.assertEqual(shot["fit_mode"], "contain")
            self.assertEqual(shot["render_background"], "#F1F3F5")
        self.assertEqual(plan["copy"]["actual_look_count"], 5)
        self.assertNotIn("穿搭完全一致", plan["outfit_plan"]["product_visibility_rules"]["consistency"])
        self.assertEqual(repo.tasks["opv_task_1"].topic_text, plan["copy"]["title"])
        self.assertEqual(len(plan["content_signature"]["axes"]["look_sequence"]), 5)

    def test_fewer_candidates_reduce_pages_and_copy_without_fake_pass_or_duplicates(self):
        for count in (1, 2, 3, 4):
            with self.subTest(count=count):
                _, _, service = self.make_service(count)
                plan = service.plan_task("opv_task_1").plan
                self.assertEqual(contracts.validate_plan_json(plan), [])
                self.assertEqual(plan["actual_shot_count"], count)
                self.assertEqual(len(plan["shots"]), count)
                self.assertEqual(sum(s["duration_ms"] for s in plan["shots"]), 6000)
                self.assertTrue(plan["copy"]["title"].startswith(str(count) + " "))
                self.assertEqual(plan["copy"]["actual_page_count"], count)

    def test_frozen_sequence_is_reused_when_library_changes(self):
        repo, assets, service = self.make_service()
        sequence, _ = select_multi_look_sequence(list(assets.looks.values()), first_ref="LOOK_LIB_1")
        repo.tasks["opv_task_1"].product_snapshot_json["product"]["planned_look_sequence"] = copy.deepcopy(sequence)
        assets.looks.clear()
        plan = service.plan_task("opv_task_1").plan
        self.assertEqual([entry["snapshot"] for entry in plan["outfit_sequence"]], [entry["snapshot"] for entry in sequence])
        self.assertFalse(service.plan_task("opv_task_1").created)

    def test_bad_frozen_fingerprint_fails_without_reselecting(self):
        repo, assets, service = self.make_service()
        sequence, _ = select_multi_look_sequence(list(assets.looks.values()), first_ref="LOOK_LIB_1")
        sequence[0]["fingerprint"] = "wrong"
        repo.tasks["opv_task_1"].product_snapshot_json["product"]["planned_look_sequence"] = sequence
        with self.assertRaisesRegex(ContentPlannerError, "fingerprint"):
            service.plan_task("opv_task_1")


if __name__ == "__main__":
    unittest.main()
