import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_models import MarketInsightConfig, MarketInsightProductTag, ProductRankingSnapshot  # noqa: E402
from src.market_insight_scoring import MarketInsightScoringEngine  # noqa: E402


class MarketInsightScoringEnginePriceBandTest(unittest.TestCase):
    def test_hair_accessory_price_band_converts_to_rmb_with_five_yuan_steps(self):
        engine = MarketInsightScoringEngine()
        config = MarketInsightConfig(
            table_id="t",
            table_name="t",
            enabled=True,
            default_country="VN",
            default_category="hair_accessory",
            source_currency="VND",
            price_to_cny_rate=0.000259,
            price_band_step_rmb=5,
        )
        snapshot = ProductRankingSnapshot(
            batch_date="2026-04-21",
            batch_id="b",
            country="VN",
            category="hair_accessory",
            product_id="p1",
            product_name="发饰",
            shop_name="店铺",
            price_min=16999.0,
            price_max=36999.0,
            price_mid=26999.0,
            sales_7d=10.0,
            gmv_7d=1000.0,
            creator_count=3.0,
            video_count=4.0,
            listing_days=7,
        )
        tag = MarketInsightProductTag(
            is_valid_sample=True,
            style_cluster="韩系轻通勤型",
            product_form="发夹",
            element_tags=["布艺"],
            value_points=["提升精致度"],
            scene_tags=["出门"],
            reason_short="ok",
        )

        result = engine.score_products([snapshot], [tag], config)[0]

        self.assertEqual(result.target_price_band, "5-10 RMB")

    def test_light_tops_price_band_uses_twenty_yuan_steps(self):
        engine = MarketInsightScoringEngine()
        config = MarketInsightConfig(
            table_id="t",
            table_name="t",
            enabled=True,
            default_country="VN",
            default_category="light_tops",
            source_currency="VND",
            price_to_cny_rate=0.000259,
        )
        snapshot = ProductRankingSnapshot(
            batch_date="2026-04-21",
            batch_id="b",
            country="VN",
            category="light_tops",
            product_id="p2",
            product_name="轻上装",
            shop_name="店铺",
            price_min=150000.0,
            price_max=170000.0,
            price_mid=160000.0,
            sales_7d=10.0,
            gmv_7d=1000.0,
            creator_count=3.0,
            video_count=4.0,
            listing_days=7,
        )
        tag = MarketInsightProductTag(
            is_valid_sample=True,
            style_cluster="空调房轻外套",
            product_form="other",
            element_tags=["轻薄垂感"],
            value_points=["空调房需求"],
            scene_tags=["空调房"],
            reason_short="ok",
        )

        result = engine.score_products([snapshot], [tag], config)[0]

        self.assertEqual(result.target_price_band, "40-60 RMB")

    def test_content_efficiency_uses_sales_per_video_proxy_when_not_explicitly_mapped(self):
        engine = MarketInsightScoringEngine()
        config = MarketInsightConfig(
            table_id="t",
            table_name="t",
            enabled=True,
            default_country="TH",
            default_category="hair_accessory",
        )
        snapshots = [
            ProductRankingSnapshot(
                batch_date="2026-04-23",
                batch_id="b",
                country="TH",
                category="hair_accessory",
                product_id="p1",
                product_name="商品1",
                shop_name="店铺",
                price_min=10.0,
                price_max=10.0,
                price_mid=10.0,
                sales_7d=100.0,
                gmv_7d=1000.0,
                creator_count=5.0,
                video_count=10.0,
                listing_days=7,
            ),
            ProductRankingSnapshot(
                batch_date="2026-04-23",
                batch_id="b",
                country="TH",
                category="hair_accessory",
                product_id="p2",
                product_name="商品2",
                shop_name="店铺",
                price_min=10.0,
                price_max=10.0,
                price_mid=10.0,
                sales_7d=10.0,
                gmv_7d=100.0,
                creator_count=5.0,
                video_count=10.0,
                listing_days=7,
            ),
        ]
        tags = [
            MarketInsightProductTag(
                is_valid_sample=True,
                style_cluster="韩系轻通勤型",
                product_form="发夹",
                element_tags=["布艺"],
                value_points=["提升精致度"],
                scene_tags=["出门"],
                reason_short="ok",
            ),
            MarketInsightProductTag(
                is_valid_sample=True,
                style_cluster="韩系轻通勤型",
                product_form="发夹",
                element_tags=["布艺"],
                value_points=["提升精致度"],
                scene_tags=["出门"],
                reason_short="ok",
            ),
        ]

        results = engine.score_products(snapshots, tags, config)

        self.assertEqual(results[0].content_efficiency_source, "proxy")
        self.assertEqual(results[1].content_efficiency_source, "proxy")
        self.assertGreater(results[0].content_efficiency_signal, results[1].content_efficiency_signal)


if __name__ == "__main__":
    unittest.main()
