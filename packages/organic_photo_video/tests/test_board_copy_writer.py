from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.board_copy_writer import board_copy, numbered_item_lines  # noqa: E402


class BoardCopyWriterTest(unittest.TestCase):
    def test_multi_look_copy_does_not_claim_three_pieces_or_fixed_outfit_count(self):
        copy = board_copy("multi_look")
        self.assertEqual(copy["title"], "ไอเท็มเดียว แต่งได้หลายลุค")
        self.assertNotIn("3", str(copy))
        self.assertNotEqual(copy, board_copy("outfit_formula"))
        self.assertEqual(numbered_item_lines([{"role": "onepiece", "label": "白色连衣裙"}], "th-TH"), ["ชุดชิ้นเดียว"])

    def test_copy_is_localized_and_hook_specific(self):
        self.assertIn("3", board_copy("three_piece_formula")["title"])
        self.assertNotEqual(
            board_copy("save_this_look")["title"],
            board_copy("what_i_wore")["title"],
        )

    def test_item_lines_do_not_leak_chinese_template_text(self):
        lines = numbered_item_lines([
            {"role": "target_product", "label": "黑色羽绒服"},
            {"role": "bottom", "label_i18n": {"th-TH": "กระโปรงสั้น"}},
        ], "th-TH")
        self.assertEqual(lines[0], "ไอเท็มหลัก")
        self.assertEqual(lines[1], "กระโปรงสั้น")


if __name__ == "__main__":
    unittest.main()
