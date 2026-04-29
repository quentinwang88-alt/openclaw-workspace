#!/usr/bin/env python3
"""产品类型归一与类型守卫回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.product_type_resolution import normalize_product_type, resolve_product_context  # noqa: E402
from core.script_type_validator import validate_generated_text  # noqa: E402


class ProductTypeResolutionTest(unittest.TestCase):
    def test_normalize_product_type_maps_generic_hair_accessory(self) -> None:
        normalized = normalize_product_type("发饰", "配饰")

        self.assertEqual(normalized.canonical_type, "hair_accessory_generic")
        self.assertEqual(normalized.canonical_family, "hair_accessory")
        self.assertEqual(normalized.canonical_slot, "hair")
        self.assertFalse(normalized.fallback_used)
        self.assertIn("头发", normalized.required_terms)
        self.assertIn("手镯", normalized.forbidden_terms)

    def test_normalize_product_type_maps_earline_to_earring_family(self) -> None:
        normalized = normalize_product_type("耳线", "配饰")

        self.assertEqual(normalized.canonical_type, "earring")
        self.assertEqual(normalized.canonical_slot, "ear")
        self.assertEqual(normalized.display_type, "耳线")
        self.assertIn("耳部", normalized.required_terms)
        self.assertIn("手腕", normalized.forbidden_terms)

    def test_earline_guard_rejects_wrist_wear_language(self) -> None:
        resolved = resolve_product_context("耳线", "配饰")
        result = validate_generated_text(
            "这款耳线戴在手腕上更显白，腕部翻转一下很顺眼。",
            resolved,
        )

        self.assertFalse(result.is_valid)
        self.assertIn("手腕", result.matched_forbidden_terms)

    def test_high_confidence_visual_conflict_blocks_unrecognized_accessory_type(self) -> None:
        resolved = resolve_product_context(
            "长耳线款",
            "配饰",
            vision_type="耳饰",
            vision_family="jewelry",
            vision_slot="ear",
            vision_confidence=0.97,
        )

        self.assertFalse(resolved.recognized_by_registry)
        self.assertTrue(resolved.fallback_used)
        self.assertEqual(resolved.canonical_slot, "wrist")
        self.assertEqual(resolved.conflict_level, "high")
        self.assertEqual(resolved.resolution_policy, "block_unrecognized_table_type")
        self.assertTrue(resolved.block_required)
        self.assertIn("未命中守卫词典", resolved.block_reason or "")

    def test_low_confidence_visual_conflict_keeps_review_without_hard_block(self) -> None:
        resolved = resolve_product_context(
            "长耳线款",
            "配饰",
            vision_type="耳饰",
            vision_family="jewelry",
            vision_slot="ear",
            vision_confidence=0.42,
        )

        self.assertEqual(resolved.conflict_level, "high")
        self.assertTrue(resolved.review_required)
        self.assertFalse(resolved.block_required)
        self.assertEqual(resolved.resolution_policy, "prefer_table")

    def test_hair_accessory_generic_does_not_block_matching_high_confidence_vision(self) -> None:
        resolved = resolve_product_context(
            "发饰",
            "配饰",
            vision_type="抓夹",
            vision_family="hair_accessory",
            vision_slot="hair",
            vision_confidence=0.98,
        )

        self.assertTrue(resolved.recognized_by_registry)
        self.assertFalse(resolved.fallback_used)
        self.assertEqual(resolved.canonical_family, "hair_accessory")
        self.assertEqual(resolved.canonical_slot, "hair")
        self.assertFalse(resolved.block_required)
        self.assertEqual(resolved.conflict_level, "low")


if __name__ == "__main__":
    unittest.main()
