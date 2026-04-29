import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_category_profile import load_market_category_profile  # noqa: E402


class MarketRegistryTest(unittest.TestCase):
    def test_loads_vn_earrings_profile(self):
        profile = load_market_category_profile("VN", "earrings", skill_dir=ROOT)
        self.assertEqual(profile.market_id, "VN")
        self.assertEqual(profile.market_display_name, "越南")
        self.assertEqual(profile.category_id, "earrings")
        self.assertEqual(profile.language, "vi")

    def test_unknown_market_fails(self):
        with self.assertRaises(Exception):
            load_market_category_profile("ID", "earrings", skill_dir=ROOT)


if __name__ == "__main__":
    unittest.main()
