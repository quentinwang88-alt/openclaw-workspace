import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.feishu import FeishuDocClient  # noqa: E402


class FeishuDocClientMarkdownTest(unittest.TestCase):
    def test_markdown_children_render_headings_lists_and_tables(self):
        client = FeishuDocClient()
        children = client._markdown_children(
            "# 标题\n\n## 小节\n- 项目1\n1. 项目2\n\n|A|B|\n|---|---|\n|1|2|\n"
        )

        self.assertEqual(children[0]["block_type"], 3)
        self.assertIn("heading1", children[0])
        self.assertEqual(children[1]["block_type"], 4)
        self.assertIn("heading2", children[1])
        self.assertEqual(children[2]["block_type"], 12)
        self.assertIn("bullet", children[2])
        self.assertEqual(children[3]["block_type"], 13)
        self.assertIn("ordered", children[3])
        self.assertEqual(children[4]["block_type"], 5)
        self.assertIn("heading3", children[4])
        self.assertEqual(children[5]["block_type"], 2)
        self.assertIn("text", children[5])


if __name__ == "__main__":
    unittest.main()
