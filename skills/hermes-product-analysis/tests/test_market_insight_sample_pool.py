import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_models import (  # noqa: E402
    MarketDirectionCard,
    MarketInsightProductTag,
    ProductRankingSnapshot,
    ScoredProductSnapshot,
)
from src.market_insight_sample_pool import build_direction_sample_pool  # noqa: E402
from src.market_insight_sample_pool_feishu_sync import build_sample_pool_field_specs  # noqa: E402


def scored(product_id, sales, age_days, form="抓夹"):
    snapshot = ProductRankingSnapshot(
        batch_date="2026-04-21",
        batch_id="batch_1",
        country="VN",
        category="hair_accessory",
        product_id=product_id,
        product_name=f"商品 {product_id}",
        shop_name="测试店",
        price_min=10,
        price_max=10,
        price_mid=10,
        sales_7d=sales,
        gmv_7d=sales * 10,
        creator_count=2,
        video_count=3,
        listing_days=age_days,
        product_images=[f"https://example.com/{product_id}.jpg"],
        raw_product_images=[{"file_token": f"tok_{product_id}", "name": f"{product_id}.jpg", "type": "image/jpeg"}],
        image_url=f"https://example.com/{product_id}.jpg",
        product_url=f"https://example.com/product?id={product_id}",
        rank_index=int(product_id.replace("p", "")),
    )
    return ScoredProductSnapshot(
        snapshot=snapshot,
        tag=MarketInsightProductTag(
            is_valid_sample=True,
            style_cluster="盘发效率型",
            product_form=form,
            element_tags=["低饱和纯色"],
            value_points=["快速整理"],
            scene_tags=["出门"],
        ),
        heat_score=1,
        heat_level="high",
        crowd_score=1,
        crowd_level="low",
        priority_level="medium",
        target_price_band="10-15 RMB",
        direction_canonical_key="VN__hair_accessory__盘发效率型",
        direction_family="功能结果型",
    )


class MarketInsightSamplePoolTest(unittest.TestCase):
    def test_builds_top_and_new_rows_without_duplicates(self):
        items = [scored(f"p{i}", sales=100 - i, age_days=(20 if i <= 3 else 220)) for i in range(1, 13)]
        card = MarketDirectionCard(
            direction_canonical_key="VN__hair_accessory__盘发效率型",
            direction_instance_id="2026-04-21__VN__hair_accessory__盘发效率型",
            batch_date="2026-04-21",
            country="VN",
            category="hair_accessory",
            direction_name="盘发效率型",
            style_cluster="盘发效率型",
            direction_family="功能结果型",
            decision_action="study_top_not_enter",
            primary_opportunity_type="few_new_winners",
            demand_structure={"top3_sales_share": 0.5, "sales_action_threshold": 80},
        )

        rows = build_direction_sample_pool(items, [card])

        self.assertEqual(len(rows), 10)
        first = rows[0]
        self.assertIn("头部Top10", first["样本类型"])
        self.assertIn("代表新品", first["样本类型"])
        self.assertIn("少数新品赢家", first["样本类型"])
        old_rows = [row for row in rows if "老品占位头部" in row["样本类型"]]
        self.assertTrue(old_rows)
        self.assertEqual(first["方向动作"], "拆头部不直接入场")
        self.assertEqual(first["主机会类型"], "少数新品赢家")
        self.assertEqual(first["商品主图"][0]["file_token"], "tok_p1")
        self.assertIn("30秒", "、".join(first["内容可表达点"]))

    def test_feishu_field_specs_include_core_product_level_fields(self):
        names = {item["name"] for item in build_sample_pool_field_specs()}
        for field_name in ["商品标题", "商品主图", "价格", "7日销量", "上架天数", "FastMoss链接"]:
            self.assertIn(field_name, names)
        self.assertLessEqual(len(names), 18)


if __name__ == "__main__":
    unittest.main()
