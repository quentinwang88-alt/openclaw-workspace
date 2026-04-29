import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from selection.category_resolution import resolve_market_and_category  # noqa: E402


class CategoryResolutionTest(unittest.TestCase):
    def test_missing_market_goes_to_exception_pool(self):
        [record] = resolve_market_and_category([{"商品标题": "珍珠耳环"}])
        self.assertEqual(record["market_id"], "")
        self.assertIn("market_id_missing", record["risk_flags"])

    def test_keyword_category_resolution_requires_manual_review_when_ambiguous(self):
        [record] = resolve_market_and_category([{"市场": "VN", "商品标题": "珍珠耳环发夹套装"}])
        self.assertEqual(record["market_id"], "VN")
        self.assertLess(record["category_confidence"], 0.8)
        self.assertIn("manual_category_review_required", record["risk_flags"])

    def test_manual_category_maps_to_earrings(self):
        [record] = resolve_market_and_category([{"市场": "TH", "类目": "耳环", "商品标题": "pearl earrings"}])
        self.assertEqual(record["market_id"], "TH")
        self.assertEqual(record["category_id"], "earrings")
        self.assertEqual(record["category_confidence"], 1.0)


if __name__ == "__main__":
    unittest.main()
