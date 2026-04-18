import sys
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from run_product_description import ProductDescriptionGenerator  # noqa: E402


class ProductAnchorPromptTests(unittest.TestCase):
    def test_anchor_info_block_includes_params_rules_when_present(self):
        block = ProductDescriptionGenerator._build_anchor_info_block(
            product_type="手镯",
            business_category="配饰",
            product_params="细手圈，内径约56mm，圈宽2mm，开口可微调",
        )

        self.assertIn("表格产品类型：手镯", block)
        self.assertIn("业务大类：配饰", block)
        self.assertIn("产品参数信息：细手圈，内径约56mm，圈宽2mm，开口可微调", block)
        self.assertIn("参数锚点规则：", block)

    def test_anchor_info_block_skips_param_rules_when_absent(self):
        block = ProductDescriptionGenerator._build_anchor_info_block(
            product_type="手镯",
            business_category="配饰",
            product_params="",
        )

        self.assertIn("表格产品类型：手镯", block)
        self.assertIn("业务大类：配饰", block)
        self.assertNotIn("参数锚点规则：", block)


if __name__ == "__main__":
    unittest.main()
