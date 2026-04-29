import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_input_mode_detector import (  # noqa: E402
    PRODUCT_MODE,
    SHOP_MODE,
    UNKNOWN_MODE,
    detect_input_mode,
)


class MarketInsightInputModeDetectorTest(unittest.TestCase):
    def test_detects_product_ranking_from_product_headers(self):
        mode = detect_input_mode(["商品名称", "商品图片", "7天销量"])
        self.assertEqual(mode, PRODUCT_MODE)

    def test_detects_shop_ranking_from_shop_headers(self):
        mode = detect_input_mode(["店铺名称", "在售商品数", "新品成交占比"])
        self.assertEqual(mode, SHOP_MODE)

    def test_returns_unknown_when_headers_are_insufficient(self):
        mode = detect_input_mode(["标题", "说明"])
        self.assertEqual(mode, UNKNOWN_MODE)


if __name__ == "__main__":
    unittest.main()
