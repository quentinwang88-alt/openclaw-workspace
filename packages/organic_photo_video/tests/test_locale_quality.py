from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.locale_quality import copy_locale_issues, visible_text_issues


class LocaleQualityTest(unittest.TestCase):
    def test_thai_text_passes(self):
        self.assertEqual(visible_text_issues("ไปเที่ยวกัน", "th-TH", "title"), [])

    def test_cjk_leak_is_reported(self):
        issues = visible_text_issues("ไปเที่ยว 类短句", "th-TH", "cover_text")
        self.assertIn("cover_text contains CJK characters", issues)

    def test_copy_checks_all_visible_fields(self):
        issues = copy_locale_issues(
            {"title": "中文", "caption": "泰语文案", "cover_text": "", "hashtags": ["#OOTD"]},
            "th-TH",
        )
        self.assertTrue(any("title" in issue for issue in issues))
        self.assertFalse(any("hashtags[0]" in issue for issue in issues))


if __name__ == "__main__":
    unittest.main()
