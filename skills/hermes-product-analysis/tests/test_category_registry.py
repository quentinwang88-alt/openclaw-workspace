import sys
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class CategoryRegistryTest(unittest.TestCase):
    def test_all_target_market_category_profiles_are_registered(self):
        payload = yaml.safe_load((ROOT / "configs" / "market_category_profiles.yaml").read_text(encoding="utf-8"))
        profiles = payload["market_category_profiles"]
        for market in ("VN", "TH", "MY"):
            self.assertIn(market, profiles)
            for category in ("hair_accessory", "earrings", "womens_tops"):
                self.assertIn(category, profiles[market])
                self.assertTrue(profiles[market][category]["enabled"])


if __name__ == "__main__":
    unittest.main()
