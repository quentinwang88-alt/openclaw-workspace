import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_category_profile import load_market_category_profile  # noqa: E402


class MarketCategoryProfileLoaderTest(unittest.TestCase):
    def test_market_category_profiles_are_isolated(self):
        vn = load_market_category_profile("VN", "earrings", skill_dir=ROOT)
        th = load_market_category_profile("TH", "earrings", skill_dir=ROOT)
        self.assertEqual(vn.category_id, "earrings")
        self.assertEqual(th.category_id, "earrings")
        self.assertNotEqual(vn.profile_version, th.profile_version)
        self.assertIn("bông tai", vn.tag_dictionary["forms"]["local"])
        self.assertIn("ต่างหู", th.tag_dictionary["forms"]["local"])
        self.assertNotIn("bông tai", th.tag_dictionary["forms"]["local"])

    def test_category_does_not_fallback_to_hair_accessory(self):
        earrings = load_market_category_profile("VN", "earrings", skill_dir=ROOT)
        hair = load_market_category_profile("VN", "hair_accessory", skill_dir=ROOT)
        self.assertIn("product_form", earrings.product_anchor_schema["product_anchor_schema"])
        self.assertIn("wearing_type", earrings.product_anchor_schema["product_anchor_schema"])
        self.assertNotIn("wearing_type", hair.product_anchor_schema["product_anchor_schema"])


if __name__ == "__main__":
    unittest.main()
