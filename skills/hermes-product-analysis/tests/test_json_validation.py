import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.hermes_analyzer import HermesAnalyzer  # noqa: E402
from src.models import HermesOutputError  # noqa: E402


class JsonValidationTest(unittest.TestCase):
    def setUp(self):
        self.analyzer = HermesAnalyzer(skill_dir=ROOT, hermes_bin="/tmp/does-not-matter")

    def test_validate_category_result_accepts_valid_payload(self):
        result = self.analyzer.validate_category_result(
            {
                "predicted_category": "轻上装",
                "confidence": "high",
                "reason": "标题与图片都指向薄款开衫",
            }
        )

        self.assertEqual(result.predicted_category, "轻上装")
        self.assertEqual(result.confidence, "high")

    def test_validate_feature_result_accepts_valid_light_top_payload(self):
        result = self.analyzer.validate_feature_result(
            {
                "analysis_category": "轻上装",
                "upper_body_change_strength": "中",
                "camera_readability": "中",
                "design_signal_strength": "低",
                "basic_style_escape_strength": "低",
                "title_selling_clarity": "高",
                "info_completeness": "中",
                "risk_tag": "脱离基础款能力弱",
                "risk_note": "标题有表达点，但镜头支撑一般",
                "brief_observation": "更像基础稳款，拉开差距能力有限",
            },
            expected_category="轻上装",
        )

        self.assertEqual(result.analysis_category, "轻上装")
        self.assertEqual(result.feature_scores["camera_readability"], "中")

    def test_validate_feature_result_rejects_invalid_risk_tag(self):
        with self.assertRaises(HermesOutputError):
            self.analyzer.validate_feature_result(
                {
                    "analysis_category": "发饰",
                    "wearing_change_strength": "高",
                    "demo_ease": "高",
                    "visual_memory_point": "中",
                    "homogenization_risk": "中",
                    "title_selling_clarity": "高",
                    "info_completeness": "中",
                    "risk_tag": "不在枚举里",
                    "risk_note": "补充说明",
                    "brief_observation": "适合做前后对比",
                },
                expected_category="发饰",
            )


if __name__ == "__main__":
    unittest.main()
