import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from selection.category_resolution import group_by_market_and_category  # noqa: E402


class CategoryIsolationTest(unittest.TestCase):
    def test_groups_by_market_and_category(self):
        grouped = group_by_market_and_category(
            [
                {"市场": "VN", "类目": "耳环", "商品标题": "珍珠耳环"},
                {"市场": "TH", "类目": "耳环", "商品标题": "pearl earrings"},
                {"市场": "MY", "类目": "发饰", "商品标题": "hair clip"},
            ]
        )
        self.assertEqual(set(grouped), {("VN", "earrings"), ("TH", "earrings"), ("MY", "hair_accessory")})


if __name__ == "__main__":
    unittest.main()
