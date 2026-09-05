#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.asset_resolver import LightTryonAssetReader  # noqa: E402


class LookRecipeNormalizationTest(unittest.TestCase):
    def test_structured_feishu_columns_become_executable_recipe(self):
        recipe = LightTryonAssetReader._normalized_look_recipe({
            "inner_type": "修身背心",
            "inner_color": "白色",
            "bottom_type": "高腰直筒裤",
            "bottom_color": '["米白色"]',
            "accessory_level": "极简",
            "base_outfit_direction": "清爽显高",
            "styling_name": "备用名称",
        })

        self.assertEqual(recipe["top_inner"], "白色 修身背心")
        self.assertEqual(recipe["bottom"], "米白色 高腰直筒裤")
        self.assertEqual(recipe["target_outer"], "目标商品本身")
        self.assertEqual(recipe["overall_style"], "清爽显高")


if __name__ == "__main__":
    unittest.main()
