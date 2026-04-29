import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.category_scoring import apply_category_risk_penalties  # noqa: E402
from src.market_category_profile import load_market_category_profile  # noqa: E402


class EarringsScoringConfigTest(unittest.TestCase):
    def test_subscore_weights_sum_to_35(self):
        profile = load_market_category_profile("VN", "earrings", skill_dir=ROOT)
        weights = profile.scoring["product_quality_subscore_weights"]
        self.assertEqual(sum(weights.values()), 35)

    def test_risk_penalty_affects_score_and_manual_review(self):
        profile = load_market_category_profile("VN", "earrings", skill_dir=ROOT)
        result = apply_category_risk_penalties(
            80,
            ["无耳洞/有耳洞容易混淆", "主图信息不足"],
            profile.scoring,
        )
        self.assertEqual(result["total_penalty"], -6)
        self.assertEqual(result["adjusted_score"], 74)
        self.assertTrue(result["manual_review_required"])


if __name__ == "__main__":
    unittest.main()
