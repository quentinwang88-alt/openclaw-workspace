import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.title_parser import TitleParser  # noqa: E402


class TitleParserTest(unittest.TestCase):
    def test_parse_light_tops_title(self):
        parser = TitleParser()
        result = parser.parse_title("夏季薄款冰丝防晒针织开衫外搭上衣")

        self.assertIn("薄款", result.title_keyword_tags)
        self.assertIn("冰丝", result.title_keyword_tags)
        self.assertIn("开衫", result.title_keyword_tags)
        self.assertEqual(result.title_category_hint, "轻上装")
        self.assertEqual(result.title_category_confidence, "high")

    def test_parse_empty_title(self):
        parser = TitleParser()
        result = parser.parse_title("")

        self.assertEqual(result.title_keyword_tags, [])
        self.assertEqual(result.title_category_hint, "")
        self.assertEqual(result.title_category_confidence, "")


if __name__ == "__main__":
    unittest.main()
