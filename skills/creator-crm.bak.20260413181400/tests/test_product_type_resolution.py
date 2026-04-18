import sys
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.product_type_resolution import (  # noqa: E402
    build_prompt_contract,
    normalize_product_type,
    resolve_product_context,
)


class ProductTypeResolutionTests(unittest.TestCase):
    def test_normalize_slim_bangle_from_jewelry_alias(self):
        context = normalize_product_type("细手环", "首饰")

        self.assertEqual(context.canonical_family, "jewelry")
        self.assertEqual(context.canonical_slot, "wrist")
        self.assertEqual(context.canonical_type, "slim_bangle")
        self.assertEqual(context.display_type, "细手环")

    def test_normalize_light_top_from_business_category(self):
        context = normalize_product_type("轻上装", "轻上装")

        self.assertEqual(context.canonical_family, "apparel")
        self.assertEqual(context.canonical_slot, "upper_body")
        self.assertEqual(context.canonical_type, "light_top")

    def test_accessory_category_can_route_to_hair_accessory(self):
        context = normalize_product_type("抓夹", "配饰")

        self.assertEqual(context.canonical_family, "hair_accessory")
        self.assertEqual(context.canonical_slot, "hair")
        self.assertEqual(context.canonical_type, "claw_clip")

    def test_resolve_high_conflict_prefers_table_type(self):
        context = resolve_product_context(
            raw_product_type="细手圈",
            business_category="首饰",
            vision_type="项圈",
            vision_confidence=0.86,
        )

        self.assertEqual(context.canonical_family, "jewelry")
        self.assertEqual(context.canonical_slot, "wrist")
        self.assertEqual(context.canonical_type, "slim_bangle")
        self.assertEqual(context.vision_type, "choker")
        self.assertEqual(context.conflict_level, "high")
        self.assertTrue(context.review_required)
        self.assertEqual(context.resolution_policy, "prefer_table")

    def test_resolve_low_conflict_when_slot_matches(self):
        context = resolve_product_context(
            raw_product_type="细手圈",
            business_category="首饰",
            vision_type="手镯",
            vision_confidence=0.66,
        )

        self.assertEqual(context.conflict_level, "low")
        self.assertFalse(context.review_required)
        self.assertEqual(context.canonical_slot, "wrist")

    def test_prompt_contract_contains_required_and_forbidden_terms(self):
        context = normalize_product_type("细手圈", "首饰")
        contract = build_prompt_contract(context)

        self.assertIn("标准佩戴/使用部位：手腕佩戴", contract)
        self.assertIn("必须围绕这些词展开：手腕、腕部、佩戴在手上", contract)
        self.assertIn("项圈", contract)
        self.assertIn("脖子留白", contract)


if __name__ == "__main__":
    unittest.main()
