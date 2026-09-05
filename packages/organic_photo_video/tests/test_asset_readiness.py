import copy
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from services.asset_readiness import check_generation_assets, require_generation_assets, AssetReadinessError
from services.asset_compatibility import compatibility_errors


class AssetReadinessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.paths = []
        for i in range(4):
            p = Path(self.tmp.name) / f"{i}.png"
            p.write_bytes(f"fixture-{i}".encode())
            self.paths.append(str(p))
        self.account = SimpleNamespace(status="testing", persona_ref_id="DEFAULT_NOT_SELECTED", operating_rules_json={})
        self.product = {"product_id": "SKU", "category": "outerwear", "reference_images": self.paths[:1], "reference_roles": {"front": self.paths[:1]}}
        self.plan = {
            "persona": {"ref_id": "ACTUAL", "snapshot": {"status": "testing", "local_reference_images": self.paths[1:2]}},
            "look": {"snapshot": {"status": "enabled", "recipe": {"top_inner": "white tee", "bottom": "jeans", "footwear": "sneakers"}}},
            "scene": {"ref_id": "SCENE_A_001", "snapshot": {"status": "enabled", "prompt_core": "bedroom"}},
            "shots": [{"composition_contract": {"framing": "garment_neck_to_upper_thigh"}}],
        }

    def test_testing_warns_but_does_not_claim_detail_or_real_styling_assets(self):
        report = require_generation_assets(self.account, self.product, self.plan)
        self.assertEqual(report["selected_persona_ref"], "ACTUAL")
        self.assertFalse(report["capabilities"]["detail_proof"])
        self.assertEqual(report["capabilities"]["derived_styling_roles"], ["top_inner", "bottom"])
        self.assertEqual(report["mode"], "testing")

    def test_missing_selected_persona_blocks_even_if_default_exists(self):
        self.plan["persona"]["snapshot"]["local_reference_images"] = ["/missing.png"]
        with self.assertRaises(AssetReadinessError):
            require_generation_assets(self.account, self.product, self.plan)

    def test_production_needs_enabled_and_three_references(self):
        self.account.status = "active"
        self.assertFalse(check_generation_assets(self.account, self.product, self.plan)["ready"])
        self.plan["persona"]["snapshot"].update(status="enabled", local_reference_images=self.paths[1:])
        self.assertTrue(check_generation_assets(self.account, self.product, self.plan)["ready"])

    def test_incomplete_footwear_is_production_error(self):
        self.plan["look"]["snapshot"]["recipe"].pop("footwear")
        self.assertTrue(check_generation_assets(self.account, self.product, self.plan)["ready"])
        self.account.status = "active"
        self.assertIn("footwear_unspecified", [x["code"] for x in check_generation_assets(self.account, self.product, self.plan)["issues"] if x["severity"] == "error"])

    def test_reference_hash_mutation_blocked(self):
        self.product["reference_assets"] = [{"local_path": self.paths[0], "sha256": "0" * 64}]
        with self.assertRaisesRegex(AssetReadinessError, "冻结参考"):
            require_generation_assets(self.account, self.product, self.plan)

    def test_missing_detail_blocks_macro_but_allows_structural_midshot(self):
        self.assertTrue(check_generation_assets(self.account, self.product, self.plan)["ready"])
        self.plan["shots"][0]["composition_contract"]["framing"] = "macro"
        self.assertFalse(check_generation_assets(self.account, self.product, self.plan)["ready"])
        self.product["reference_roles"]["detail"] = self.paths[:1]
        self.assertTrue(check_generation_assets(self.account, self.product, self.plan)["ready"])

    def test_scene_family_mismatch_blocks(self):
        self.plan["look"]["snapshot"]["compatibility"] = {"scene_families": ["airport_departure_travel"]}
        self.assertFalse(check_generation_assets(self.account, self.product, self.plan)["ready"])
        self.plan["scene"]["ref_id"] = "ENV_AIRPORT_DEPART_001"
        self.assertTrue(check_generation_assets(self.account, self.product, self.plan)["ready"])

    def test_explicit_fit_mismatch_not_ignored(self):
        self.product["product_fit"] = "宽松"
        self.assertIn("product_fit", compatibility_errors({"compatibility": {"product_fits": ["合体"]}}, self.product))

    def test_missing_product_blocks(self):
        self.product["reference_images"] = []
        self.assertFalse(check_generation_assets(self.account, self.product, self.plan)["ready"])

    def test_no_independent_bottom_cannot_use_three_item_board(self):
        self.plan["recipe"] = {"id": "RECIPE_OUTFIT_BREAKDOWN_V1"}
        self.plan["look"]["snapshot"]["recipe"]["bottom"] = "连衣裙（无独立下装）"
        self.assertFalse(check_generation_assets(self.account, self.product, self.plan)["ready"])

    def test_new_pure_color_softens_only_scene_and_declares_shoe_fallback(self):
        self.plan["presentation_profile"] = {"background_mode": "solid_color", "item_count_policy": "dynamic_2_or_3"}
        self.plan["look"]["snapshot"]["compatibility"] = {"scene_families": ["street_outing"]}
        self.plan["look"]["snapshot"]["recipe_field_sources"] = {"footwear": {"source": "policy_neutral_fallback"}}
        report = require_generation_assets(self.account, self.product, self.plan)
        self.assertIn("scene_preference_only", [x["code"] for x in report["issues"]])
        self.assertIn("footwear_neutral_fallback", [x["code"] for x in report["issues"]])
        self.plan["look"]["snapshot"]["applicable_product_codes"] = ["WRONG"]
        self.assertFalse(check_generation_assets(self.account, self.product, self.plan)["ready"])

    def test_dynamic_dress_accepts_two_roles_but_legacy_stays_blocked(self):
        self.plan["recipe"] = {"id": "RECIPE_OUTFIT_BREAKDOWN_V1"}
        self.plan["presentation_profile"] = {"item_count_policy": "dynamic_2_or_3"}
        self.plan["look"]["snapshot"]["recipe"] = {"onepiece": "白色针织连衣裙", "footwear": "白鞋"}
        self.assertTrue(check_generation_assets(self.account, self.product, self.plan)["ready"])
        self.assertEqual(check_generation_assets(self.account, self.product, self.plan)["capabilities"]["derived_styling_roles"], ["onepiece"])
        self.plan.pop("presentation_profile")
        self.assertFalse(check_generation_assets(self.account, self.product, self.plan)["ready"])

    def test_scene_aliases_and_code_artifacts_are_normalized(self):
        look = {"applicable_product_codes": ["SKU\ufffc"], "compatibility": {"scene_families": ["CAFE_DINING"]}}
        self.assertEqual(compatibility_errors(look, self.product, "ENV_CAFE_001"), [])
        self.assertIn("scene_family", compatibility_errors(look, self.product, "ENV_AIRPORT_DEPART_001"))


if __name__ == "__main__":
    unittest.main()
