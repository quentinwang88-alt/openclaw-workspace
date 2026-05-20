#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = SKILL_DIR / "core"
sys.path.insert(0, str(CORE_DIR))

from product_truth import heuristic_product_truth, normalize_product_truth  # noqa: E402
from prompt_builder import build_label, build_main_image_prompt, select_main_image_layout  # noqa: E402
from qa import normalize_qa_result  # noqa: E402


class ProductTruthTests(unittest.TestCase):
    def test_normalize_type_name_and_defaults(self) -> None:
        truth = normalize_product_truth({
            "subtype": "suede_jacket",
            "product_type_name_en": "",
            "confidence": 1.5,
        })
        self.assertEqual(truth["product_type_name_en"], "SUEDE JACKET")
        self.assertEqual(truth["confidence"], 1.0)
        self.assertIn("leather shine", truth["must_not_add"])

    def test_heuristic_infers_leather(self) -> None:
        truth = heuristic_product_truth(["black_pu_leather_jacket.jpg"])
        self.assertEqual(truth["subtype"], "leather_jacket")


class PromptBuilderTests(unittest.TestCase):
    def test_default_label(self) -> None:
        self.assertEqual(
            build_label(brand_name="likeU", product_type="SUEDE JACKET", strategy="likeU + 产品类型"),
            "likeU · SUEDE JACKET",
        )

    def test_prompt_contains_no_accessory_policy(self) -> None:
        truth = heuristic_product_truth(["khaki_utility_jacket.jpg"])
        prompt = build_main_image_prompt(product_truth=truth, brand_name="likeU", country="TH")
        self.assertIn("product-only", prompt)
        self.assertIn("No handbags", prompt)
        self.assertIn("likeU", prompt)

    def test_multicolor_uses_triptych_layout(self) -> None:
        truth = normalize_product_truth({
            "subtype": "faux_fur_jacket",
            "product_type_name_en": "FAUX FUR JACKET",
            "main_color": "taupe brown",
            "is_probably_multicolor": True,
            "sellable_colors_observed": ["taupe brown", "black", "cream"],
            "material": "faux fur",
            "closure": "open front, no visible buttons",
            "pockets": "no visible pockets",
        })
        layout = select_main_image_layout(truth)
        prompt = build_main_image_prompt(product_truth=truth, brand_name="likeU", country="TH")
        self.assertEqual(layout["template"], "womens_tops_multicolor_triptych")
        self.assertIn("three-zone split layout", prompt)
        self.assertIn("IMAGE 1 is the promoted default color/style", prompt)
        self.assertIn("3 colors", prompt)

    def test_single_material_with_no_buttons_uses_material_layout(self) -> None:
        truth = normalize_product_truth({
            "subtype": "faux_fur_jacket",
            "product_type_name_en": "FAUX FUR JACKET",
            "source_image_type": "on_body_model",
            "has_on_body_model": True,
            "main_color": "taupe brown",
            "is_probably_multicolor": False,
            "sellable_colors_observed": ["taupe brown"],
            "material": "faux fur",
            "closure": "open front, no visible buttons",
            "pockets": "no visible pockets",
        })
        layout = select_main_image_layout(truth)
        self.assertEqual(layout["template"], "womens_tops_material_mood_split")

    def test_product_only_reference_uses_faceless_tryon_layout(self) -> None:
        truth = normalize_product_truth({
            "subtype": "puffer_jacket",
            "product_type_name_en": "PUFFER JACKET",
            "source_image_type": "hanger",
            "has_on_body_model": False,
            "main_color": "dark brown",
            "material": "puffer",
            "closure": "front snap buttons",
            "pockets": "two slanted side pockets",
        })
        layout = select_main_image_layout(truth)
        prompt = build_main_image_prompt(product_truth=truth, brand_name="likeU", country="TH")
        self.assertEqual(layout["template"], "womens_tops_product_only_to_tryon_truth_split")
        self.assertIn("faceless cropped try-on image", prompt)
        self.assertIn("do not show a full perfect face", prompt)
        self.assertIn("refined product-only proof", prompt)


class QATests(unittest.TestCase):
    def test_normalize_invalid_result_to_review(self) -> None:
        result = normalize_qa_result({"result": "ok", "score": 2, "issues": "bad pocket"})
        self.assertEqual(result["result"], "需人工复核")
        self.assertEqual(result["score"], 1.0)
        self.assertEqual(result["issues"], ["bad pocket"])


if __name__ == "__main__":
    unittest.main()
