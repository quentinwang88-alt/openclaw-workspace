import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_category_profile import load_market_category_profile  # noqa: E402


class HairAccessoryRegressionTest(unittest.TestCase):
    def test_hair_accessory_directions_remain_ready(self):
        profile = load_market_category_profile("VN", "hair_accessory", skill_dir=ROOT)
        directions = profile.directions["directions"]
        self.assertIn("hair_accessory__hair_up_efficiency", directions)
        self.assertIn("hair_accessory__sweet_gift", directions)
        self.assertNotIn("earrings__sweet_gift", directions)

    def test_womens_tops_placeholder_not_ready(self):
        profile = load_market_category_profile("VN", "womens_tops", skill_dir=ROOT)
        self.assertEqual(profile.status, "not_ready")
        placeholder = profile.directions["directions"]["womens_tops__placeholder"]
        self.assertEqual(placeholder["status"], "not_ready")


if __name__ == "__main__":
    unittest.main()
