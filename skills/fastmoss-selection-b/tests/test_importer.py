import io
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.importer import normalize_fastmoss_rows  # noqa: E402
from app.models import RuleConfig  # noqa: E402
from app.rules import evaluate_rule_engine  # noqa: E402


class ImporterTest(unittest.TestCase):
    def test_normalize_fastmoss_rows_and_rules(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "fastmoss.xlsx"
            pd.DataFrame(
                [
                    {
                        "商品名称": "Cloud Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384756?item_id=1729384756",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/1.jpg",
                        "预估商品上架时间": "2026-03-20",
                        "售价": "THB 120 - 180",
                        "7天销量": 240,
                        "7天销售额": 36000,
                        "总销量": 1200,
                        "总销售额": 156000,
                        "带货达人总数": 18,
                        "达人出单率": "12.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    },
                    {
                        "商品名称": "Crowded Item",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/9988776655",
                        "预估商品上架时间": "2026-03-10",
                        "售价": "THB 100",
                        "7天销量": 180,
                        "7天销售额": 18000,
                        "总销量": 800,
                        "总销售额": 76000,
                        "带货达人总数": 30,
                        "达人出单率": "8%",
                        "带货视频总数": 12,
                        "带货直播总数": 0,
                        "佣金比例": "10%",
                    },
                ]
            ).to_excel(file_path, index=False)

            config = RuleConfig(
                config_id="cfg_1",
                country="TH",
                category="Apparel",
                enabled=True,
                fx_rate_to_rmb=4.8,
                accio_chat_id="chat_accio",
            )
            result = normalize_fastmoss_rows(str(file_path), "batch_001", "2026-04-14 12:00:00", config)
            self.assertEqual(result.total_rows, 2)
            self.assertEqual(result.skipped_rows, 0)
            record = result.records[0]
            self.assertEqual(record["product_id"], "1729384756")
            self.assertEqual(record["listing_days"], 25)
            self.assertAlmostEqual(record["price_mid_local"], 150.0)
            self.assertAlmostEqual(record["price_mid_rmb"], 31.25)
            self.assertAlmostEqual(record["avg_price_7d_rmb"], 31.25)
            self.assertAlmostEqual(record["video_competition_density"], 3.3333, places=4)
            self.assertAlmostEqual(record["creator_competition_density"], 15.0)
            self.assertAlmostEqual(record["creator_order_rate"], 0.125)
            self.assertAlmostEqual(record["commission_rate"], 0.15)

            rule_result = evaluate_rule_engine(result.records, config)
            self.assertEqual(rule_result.total_candidates, 2)
            self.assertEqual(len(rule_result.shortlist), 1)
            self.assertEqual(rule_result.shortlist[0]["product_id"], "1729384756")
            self.assertEqual(rule_result.shortlist[0]["pool_type"], "新品池")
            self.assertEqual(rule_result.shortlist[0]["rule_status"], "通过")


if __name__ == "__main__":
    unittest.main()
