import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.enums import AnalysisStatus  # noqa: E402
from src.models import CandidateTask  # noqa: E402
from src.rule_checker import RuleChecker  # noqa: E402


class RuleCheckerTest(unittest.TestCase):
    def test_missing_images_returns_insufficient_info(self):
        checker = RuleChecker()
        task = CandidateTask(source_table_id="t1", source_record_id="rec_1", product_images=[])

        result = checker.check(task, supported_manual_categories=["发饰", "轻上装"])

        self.assertFalse(result.should_continue)
        self.assertEqual(result.terminal_status, AnalysisStatus.INSUFFICIENT_INFO.value)

    def test_unsupported_manual_category_blocks_analysis(self):
        checker = RuleChecker()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_1",
            product_images=["https://example.com/1.jpg"],
            manual_category="鞋子",
        )

        result = checker.check(task, supported_manual_categories=["发饰", "轻上装"])

        self.assertFalse(result.should_continue)
        self.assertEqual(result.terminal_status, AnalysisStatus.UNSUPPORTED_CATEGORY.value)

    def test_supported_manual_category_allows_continue(self):
        checker = RuleChecker()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_1",
            product_images=["https://example.com/1.jpg"],
            manual_category="轻上装",
        )

        result = checker.check(task, supported_manual_categories=["发饰", "轻上装"])

        self.assertTrue(result.should_continue)
        self.assertIsNone(result.terminal_status)


if __name__ == "__main__":
    unittest.main()
