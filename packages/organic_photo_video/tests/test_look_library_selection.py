import copy
import unittest
from dataclasses import replace
from types import SimpleNamespace

from services.batch_diversity_planner import BatchDiversityPlanner
from services.look_selection import look_candidate_refs, prepare_look_for_policy
from tests.test_batch_diversity_planner import FakeAssets, FakeRepo, products, specs


class LibraryAssets(FakeAssets):
    def list_look_ids(self, statuses=None):
        return [f"L{i}" for i in range(1, 7)]

    def get_look(self, ref):
        snapshot = super().get_look(ref)
        snapshot["status"] = "enabled"
        snapshot["recipe"].update(top_inner="white tee", footwear="white sneakers")
        return snapshot


class LookLibrarySelectionTests(unittest.TestCase):
    def setUp(self):
        self.repo = FakeRepo()
        self.repo.account.operating_rules_json.update(
            look_selection_mode="auto_library", presentation_profile={"background_mode": "solid_color"},
            item_count_policy="dynamic_2_or_3")
        self.assets = LibraryAssets()

    def plan(self, task_specs=None, snapshots=None, record="library"):
        task_specs = task_specs or specs()
        snapshots = snapshots or products()[:len(task_specs)]
        return BatchDiversityPlanner(self.repo, self.assets).plan(record_id=record, specs=task_specs, products=snapshots)

    def test_auto_library_reads_beyond_old_allowlist_and_honors_exclusions(self):
        self.repo.account.allowed_look_refs_json = ["L1"]
        self.repo.account.operating_rules_json["exclude_look_refs"] = ["L6"]
        batch = self.plan()
        self.assertEqual({row.spec.look_ref for row in batch}, {f"L{i}" for i in range(1, 6)})
        self.assertEqual(batch[0].metadata["pool_diagnostics"]["candidate_count"], 5)
        self.repo.account.operating_rules_json.pop("look_selection_mode")
        self.assertEqual(look_candidate_refs(self.repo.account, self.assets), ["L1"])

    def test_frozen_explicit_look_is_exact_and_exclusion_does_not_silently_change_it(self):
        batch = self.plan(specs()[:1], [{**products()[0], "planned_look_ref": "L5"}])
        self.assertEqual(batch[0].spec.look_ref, "L5")
        self.repo.account.operating_rules_json["exclude_look_refs"] = ["L5"]
        with self.assertRaisesRegex(ValueError, "排除"):
            self.plan(specs()[:1], [{**products()[0], "planned_look_ref": "L5"}])

    def test_pure_colour_mismatched_occasion_is_soft_not_product_compatibility(self):
        getter = self.assets.get_look
        def looks(ref):
            value = getter(ref)
            value["compatibility"]["scene_families"] = ["street_outing"]
            if ref == "L6":
                value["applicable_product_codes"] = ["wrong-product"]
            return value
        self.assets.get_look = looks
        batch = self.plan()
        diag = batch[0].metadata["pool_diagnostics"]
        self.assertEqual(diag["scene_policy"], "soft_preference")
        self.assertEqual(diag["scene_compatible_count"], 5)
        self.assertEqual(diag["rejections"]["L6"], ["product_code"])

    def test_outfit_coverage_precedes_grammar_and_occasion_rotation(self):
        batch = self.plan()
        self.assertEqual(len({x.metadata["axes"]["outfit_fingerprint"] for x in batch[:6]}), 6)
        self.assertTrue(batch[6].metadata["diversity_degraded"])
        self.assertIn("compatible_outfits_reused_after_pool_exhaustion", batch[6].metadata["degradation_reasons"])
        first_fp = batch[0].metadata["axes"]["outfit_fingerprint"]
        self.repo.list_recent_diversity_axes = lambda *args, **kwargs: [{**batch[0].metadata["axes"], "visible_silhouette": "legacy"}] * 10
        refreshed = self.plan(specs()[:1])
        self.assertNotEqual(refreshed[0].metadata["axes"]["outfit_fingerprint"], first_fp)

    def transform_specs(self, count=3):
        self.repo.recipes["RECIPE_VISUAL_TRANSFORM_V1"] = SimpleNamespace(hook_types_json=["contrast"])
        return [replace(s, recipe_id="RECIPE_VISUAL_TRANSFORM_V1") for s in specs()[:count]]

    def test_transform_freezes_distinct_second_look_and_counts_both_looks(self):
        batch = self.plan(self.transform_specs())
        used = []
        for item in batch:
            alternate = item.product_snapshot["planned_alternate_look_ref"]
            self.assertNotEqual(item.spec.look_ref, alternate)
            self.assertFalse(item.product_snapshot["alternate_selection"]["same_look"])
            self.assertEqual(item.product_snapshot["planned_alternate_look_snapshot"]["ref_id"], alternate)
            used.extend([item.spec.look_ref, alternate])
        self.assertEqual(len(set(used)), 6)

    def test_explicit_final_still_selects_alternate_from_library(self):
        batch = self.plan(self.transform_specs(1), [{**products()[0], "planned_look_ref": "L2"}])
        self.assertEqual(batch[0].spec.look_ref, "L2")
        self.assertNotEqual(batch[0].product_snapshot["planned_alternate_look_ref"], "L2")

    def test_bag_only_change_is_not_claimed_as_two_outfits(self):
        original = self.assets.get_look("L1")
        def same_main(ref):
            result = copy.deepcopy(original)
            result.update(ref_id=ref, name=ref)
            result["recipe"]["bag"] = ref + " bag"
            return result
        self.assets.get_look = same_main
        batch = self.plan(self.transform_specs(1))
        self.assertTrue(batch[0].product_snapshot["alternate_selection"]["same_look"])
        self.assertIn("no_distinct_alternate_look", batch[0].metadata["degradation_reasons"])

    def test_active_new_policy_neutral_shoe_is_disclosed_and_legacy_unchanged(self):
        look = {"status": "enabled", "recipe": {"top_inner": "white tee", "bottom": "jeans"}}
        self.repo.account.status = "active"
        prepared = prepare_look_for_policy(self.repo.account, look)
        self.assertEqual(prepared["recipe_field_sources"]["footwear"]["source"], "policy_neutral_fallback")
        self.assertNotIn("footwear", look["recipe"])
        self.repo.account.operating_rules_json = {}
        self.assertNotIn("footwear", prepare_look_for_policy(self.repo.account, look)["recipe"])


if __name__ == "__main__":
    unittest.main()
