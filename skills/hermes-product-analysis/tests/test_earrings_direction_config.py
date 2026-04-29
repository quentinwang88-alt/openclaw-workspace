import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_category_profile import load_market_category_profile  # noqa: E402
from src.market_insight_taxonomy import MarketInsightTaxonomyLoader  # noqa: E402


class EarringsDirectionConfigTest(unittest.TestCase):
    def test_earrings_has_seven_directions_with_category_prefix(self):
        profile = load_market_category_profile("VN", "earrings", skill_dir=ROOT)
        directions = profile.directions["directions"]
        self.assertEqual(len(directions), 7)
        for slug, payload in directions.items():
            self.assertTrue(slug.startswith("earrings__"))
            self.assertIn("direction_name", payload)
            self.assertIn("product_forms", payload)
            self.assertIn("wearing_type", payload)
            canonical = profile.canonical_direction_id(slug)
            self.assertTrue(canonical.startswith("VN__earrings__"))

    def test_mutual_exclusion_rules_exist(self):
        profile = load_market_category_profile("VN", "earrings", skill_dir=ROOT)
        rule_ids = {item["id"] for item in profile.directions["mutual_exclusion_rules"]}
        self.assertEqual(rule_ids, {"ER-001", "ER-002", "ER-003"})

    def test_earrings_market_insight_taxonomy_loads(self):
        taxonomy = MarketInsightTaxonomyLoader(ROOT / "configs" / "market_insight_taxonomies").load("earrings")
        self.assertIn("少女礼物感型", taxonomy["style_cluster"])
        self.assertIn("耳夹", taxonomy["product_form"])
        self.assertIn("中长款", taxonomy["length_form"])


if __name__ == "__main__":
    unittest.main()
