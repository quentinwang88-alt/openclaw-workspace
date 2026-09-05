from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.board_layout import (  # noqa: E402
    load_board_layout,
    load_variant_policy,
    presentation_variant,
    validate_board_layout,
)


class BoardLayoutTest(unittest.TestCase):
    def test_reference_layout_has_one_large_left_hero_and_optional_copy(self):
        layout = load_board_layout(PACKAGE_ROOT / "config/layouts/LAYOUT_OUTFIT_REFERENCE_LEFT_V1.json")
        self.assertEqual(layout["layout_version"], 1)
        self.assertEqual(layout["background_policy"], "fixed")
        self.assertEqual(layout["canvas"]["background"], "#FFFFFF")
        self.assertEqual(set(layout["variants"]), {"REF_LEFT_HERO"})
        variant = layout["variants"]["REF_LEFT_HERO"]
        self.assertGreater(variant["hero"]["height"] / layout["canvas"]["height"], .9)
        self.assertNotIn("item_list", variant)
        self.assertFalse(layout["text"]["enabled"])
        self.assertLess(variant["target_product"]["y"], variant["companion_1"]["y"])
        self.assertLess(variant["companion_1"]["y"], variant["companion_2"]["y"])
        self.assertNotEqual(variant["target_product"]["x"], variant["companion_1"]["x"])
        self.assertNotEqual(variant["companion_1"]["x"], variant["companion_2"]["x"])
        for box in variant.values():
            self.assertGreaterEqual(box["x"], 0)
            self.assertGreaterEqual(box["y"], 0)
            self.assertLessEqual(box["x"] + box["width"], layout["canvas"]["width"])
            self.assertLessEqual(box["y"] + box["height"], layout["canvas"]["height"])

    def test_legacy_layouts_still_require_item_list_and_keep_four_variants(self):
        for version in (1, 2):
            layout = load_board_layout(PACKAGE_ROOT / f"config/layouts/LAYOUT_OUTFIT_BREAKDOWN_V{version}.json")
            self.assertEqual(len(layout["variants"]), 4)
            layout["variants"]["LEFT_HERO"].pop("item_list")
            self.assertTrue(any("item_list" in error for error in validate_board_layout(layout)))

    def test_shipped_layout_and_policy_are_valid(self):
        layout = load_board_layout()
        policy = load_variant_policy()
        self.assertEqual(layout["canvas"], {
            "width": 1080, "height": 1920, "background": "#FAFAF7",
        })
        self.assertEqual(len(layout["variants"]), 4)
        self.assertEqual(policy["mode"], "balanced")

    def test_presentation_variants_are_repeatable_and_rotate(self):
        first = presentation_variant(record_key="rec-1", variant_index=1)
        again = presentation_variant(record_key="rec-1", variant_index=1)
        second = presentation_variant(record_key="rec-1", variant_index=2)
        self.assertEqual(first, again)
        self.assertNotEqual(first["presentation_signature"], second["presentation_signature"])
        self.assertNotEqual(first["layout_variant"], second["layout_variant"])


if __name__ == "__main__":
    unittest.main()
