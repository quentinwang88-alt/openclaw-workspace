import sys
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.product_type_resolution import resolve_product_context  # noqa: E402
from core.script_type_validator import validate_generated_text  # noqa: E402


class ScriptTypeValidatorTests(unittest.TestCase):
    def test_wrist_accessory_rejects_neck_terms(self):
        context = resolve_product_context(
            raw_product_type="细手圈",
            business_category="首饰",
            vision_type="项圈",
            vision_confidence=0.9,
        )
        result = validate_generated_text(
            "这是一款贴颈佩戴的细金项圈，适合锁骨位置叠戴。",
            context,
        )

        self.assertFalse(result.is_valid)
        self.assertTrue(any(term in result.matched_forbidden_terms for term in ["项圈", "贴颈", "锁骨"]))

    def test_wrist_accessory_accepts_correct_anchor_terms(self):
        context = resolve_product_context(
            raw_product_type="细手圈",
            business_category="首饰",
        )
        result = validate_generated_text(
            "这款细手圈佩戴在手腕上更显利落，单圈腕部线条清晰。",
            context,
        )

        self.assertTrue(result.is_valid)
        self.assertEqual(result.matched_forbidden_terms, [])

    def test_high_conflict_without_anchor_terms_is_invalid(self):
        context = resolve_product_context(
            raw_product_type="细手圈",
            business_category="首饰",
            vision_type="项圈",
            vision_confidence=0.9,
        )
        result = validate_generated_text(
            "这款金属环线条细致，整体极简有光泽。",
            context,
        )

        self.assertFalse(result.is_valid)
        self.assertTrue(any("高风险结果" in item for item in result.violations))


if __name__ == "__main__":
    unittest.main()
