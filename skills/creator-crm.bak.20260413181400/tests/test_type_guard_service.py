import sys
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.type_guard_service import (  # noqa: E402
    prepare_generation_type_guard,
    validate_generation_output,
)


class TypeGuardServiceTests(unittest.TestCase):
    def test_prepare_generation_type_guard_builds_context_and_contract(self):
        guard = prepare_generation_type_guard(
            raw_product_type="细手圈",
            business_category="首饰",
            vision_type="项圈",
            vision_confidence=0.87,
        )

        self.assertEqual(guard.context.canonical_type, "slim_bangle")
        self.assertEqual(guard.context.conflict_level, "high")
        self.assertIn("标准佩戴/使用部位：手腕佩戴", guard.prompt_contract)
        self.assertEqual(guard.prompt_payload["display_type"], "细手圈")

    def test_validate_generation_output_uses_guard_context(self):
        guard = prepare_generation_type_guard(
            raw_product_type="细手圈",
            business_category="首饰",
            vision_type="项圈",
            vision_confidence=0.87,
        )
        result = validate_generation_output(
            "这款单圈手镯佩戴在手腕上更显利落，腕部线条清晰。",
            guard,
        )

        self.assertTrue(result.is_valid)


if __name__ == "__main__":
    unittest.main()
