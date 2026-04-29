import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_aggregator import MarketInsightAggregator  # noqa: E402
from src.market_insight_models import (  # noqa: E402
    MarketInsightProductTag,
    ProductRankingSnapshot,
    ScoredProductSnapshot,
    VOCLightSummary,
)


def build_scored_item(
    product_id: str,
    product_name: str,
    heat_score: float,
    crowd_score: float,
    heat_level: str,
    crowd_level: str,
    priority_level: str,
    product_form_or_result: str = "抓夹",
    length_form: str = "other",
    value_points=None,
    style_cluster: str = "头盔友好整理型",
    sales_7d: float = 100.0,
    creator_count: float = 30.0,
    video_count: float = 40.0,
    category: str = "hair_accessory",
    raw_fields=None,
):
    value_points = value_points or ["快速整理头发", "头盔友好"]
    raw_fields = raw_fields or {}
    return ScoredProductSnapshot(
        snapshot=ProductRankingSnapshot(
            batch_date="2026-04-21",
            batch_id="vn_fastmoss_hair_product_ranking_20260421",
            country="VN",
            category=category,
            product_id=product_id,
            product_name=product_name,
            shop_name="店铺A",
            price_min=16999.0,
            price_max=36999.0,
            price_mid=26999.0,
            sales_7d=sales_7d,
            gmv_7d=3000000.0,
            creator_count=creator_count,
            video_count=video_count,
            listing_days=7,
            product_images=["https://cdn.example.com/1.jpg"],
            image_url="https://cdn.example.com/1.jpg",
            product_url="https://shop.example.com/product?id={product_id}".format(product_id=product_id),
            rank_index=1,
            raw_category="时尚配件",
            raw_fields=raw_fields,
        ),
        tag=MarketInsightProductTag(
            is_valid_sample=True,
            style_cluster=style_cluster,
            style_tags_secondary=["韩系轻通勤"],
            product_form=product_form_or_result,
            length_form=length_form,
            element_tags=["大抓齿", "低饱和纯色"],
            value_points=value_points,
            scene_tags=["通勤骑行", "出门"],
            reason_short="标题和图片都指向通勤快速整理。",
        ),
        heat_score=heat_score,
        heat_level=heat_level,
        crowd_score=crowd_score,
        crowd_level=crowd_level,
        priority_level=priority_level,
        target_price_band="0-50",
    )


class MarketInsightAggregatorTest(unittest.TestCase):
    def test_build_direction_cards_group_by_style_cluster_even_when_forms_and_value_points_differ(self):
        aggregator = MarketInsightAggregator()
        items = [
            build_scored_item("p1", "低饱和大抓夹", 90.0, 40.0, "high", "medium", "high", value_points=["快速整理头发", "头盔友好"]),
            build_scored_item(
                "p2",
                "通勤骑行发夹",
                82.0,
                35.0,
                "high",
                "medium",
                "high",
                product_form_or_result="发夹",
                value_points=["提升精致度", "上学通勤百搭"],
            ),
        ]

        cards = aggregator.build_direction_cards(items)

        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card.style_cluster, "头盔友好整理型")
        self.assertEqual(card.direction_name, "头盔友好整理型")
        self.assertEqual(card.direction_family, "功能结果型")
        self.assertEqual(card.direction_tier, "low_sample")
        self.assertEqual(card.top_forms, ["抓夹", "发夹"])
        self.assertEqual(card.top_value_points[0], "快速整理头发")
        self.assertEqual(card.default_content_route_preference, "neutral")
        self.assertEqual(card.priority_level, "high")
        self.assertEqual(card.product_count, 2)
        self.assertEqual(card.direction_item_count, 2)
        self.assertEqual(card.form_distribution_by_count, {"抓夹": 0.5, "发夹": 0.5})
        self.assertEqual(card.form_distribution_by_sales, {"抓夹": 0.5, "发夹": 0.5})
        self.assertEqual(
            [item["product_id"] for item in card.representative_products],
            ["p1", "p2"],
        )
        self.assertEqual(card.direction_canonical_key, "VN__hair_accessory__头盔友好整理型")
        self.assertEqual(items[0].direction_canonical_key, card.direction_canonical_key)
        self.assertEqual(items[0].direction_family, "功能结果型")
        self.assertEqual(items[0].direction_tier, "low_sample")

    def test_build_direction_cards_normalizes_old_hair_style_names_to_cluster(self):
        aggregator = MarketInsightAggregator()
        items = [
            build_scored_item(
                "p1",
                "蝴蝶结发夹",
                88.0,
                40.0,
                "high",
                "medium",
                "high",
                product_form_or_result="发夹",
                value_points=["提升精致度", "拍照/约会"],
            ),
            build_scored_item(
                "p2",
                "甜感装饰款",
                32.0,
                18.0,
                "low",
                "low",
                "low",
                product_form_or_result="other",
                value_points=["送礼"],
            ),
        ]
        for item in items:
            item.tag.style_cluster = "甜感装饰发夹"
            item.tag.element_tags = ["蝴蝶结", "布艺"]

        cards = aggregator.build_direction_cards(items)

        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card.direction_name, "甜感装饰型")
        self.assertEqual(card.direction_family, "审美风格型")
        self.assertEqual(card.top_forms, ["发夹"])
        self.assertEqual(card.product_count, 2)
        self.assertEqual(card.direction_canonical_key, "VN__hair_accessory__甜感装饰型")

    def test_build_direction_cards_keeps_form_distribution_inside_card(self):
        aggregator = MarketInsightAggregator()
        dominant = [
            build_scored_item(
                "p1",
                "甜感发夹A",
                90.0,
                40.0,
                "high",
                "medium",
                "high",
                product_form_or_result="发夹",
            ),
            build_scored_item(
                "p2",
                "甜感发夹B",
                85.0,
                35.0,
                "high",
                "medium",
                "high",
                product_form_or_result="发夹",
            ),
        ]
        tail = build_scored_item(
            "p3",
            "甜感盘发工具",
            28.0,
            15.0,
            "low",
            "low",
            "low",
            product_form_or_result="盘发工具",
        )
        items = dominant + [tail]
        for item in items:
            item.tag.style_cluster = "甜感装饰发夹"

        cards = aggregator.build_direction_cards(items)

        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card.direction_name, "甜感装饰型")
        self.assertEqual(card.top_forms[0], "发夹")
        self.assertIn("盘发工具", card.form_distribution)
        self.assertEqual(card.form_distribution_by_count["发夹"], 0.6667)
        self.assertEqual(card.form_distribution_by_count["盘发工具"], 0.3333)
        self.assertEqual(card.form_distribution_by_sales["发夹"], 0.6667)
        self.assertEqual(card.form_distribution_by_sales["盘发工具"], 0.3333)
        self.assertEqual(card.product_count, 3)

    def test_build_direction_cards_calculates_family_tier_with_code_rules(self):
        aggregator = MarketInsightAggregator()
        items = []
        for index in range(12):
            items.append(
                build_scored_item(
                    product_id=f"p_priority_{index}",
                    product_name=f"优先方向{index}",
                    heat_score=90.0,
                    crowd_score=30.0,
                    heat_level="high",
                    crowd_level="low",
                    priority_level="high",
                    style_cluster="韩系轻通勤型",
                    product_form_or_result="抓夹",
                    sales_7d=220.0,
                    creator_count=10.0,
                    video_count=15.0,
                )
            )
        for index in range(12):
            items.append(
                build_scored_item(
                    product_id=f"p_crowded_{index}",
                    product_name=f"拥挤方向{index}",
                    heat_score=70.0,
                    crowd_score=80.0,
                    heat_level="high",
                    crowd_level="high",
                    priority_level="medium",
                    style_cluster="甜感装饰型",
                    product_form_or_result="发夹",
                    sales_7d=60.0,
                    creator_count=80.0,
                    video_count=120.0,
                )
            )

        cards = aggregator.build_direction_cards(items)
        card_map = {card.style_cluster: card for card in cards}

        self.assertEqual(card_map["韩系轻通勤型"].direction_family, "审美风格型")
        self.assertEqual(card_map["韩系轻通勤型"].direction_tier, "priority")
        self.assertEqual(card_map["韩系轻通勤型"].default_content_route_preference, "neutral")
        self.assertEqual(card_map["甜感装饰型"].direction_tier, "crowded")
        self.assertEqual(card_map["甜感装饰型"].default_content_route_preference, "original_preferred")

    def test_report_payload_contains_phase2_actions(self):
        aggregator = MarketInsightAggregator()
        cards = aggregator.build_direction_cards(
            [
                build_scored_item("p1", "低饱和大抓夹", 90.0, 40.0, "high", "medium", "high"),
                build_scored_item("p2", "通勤骑行抓夹", 82.0, 35.0, "high", "medium", "high"),
            ]
        )
        payload = aggregator.build_report_payload(
            scored_items=[
                build_scored_item("p1", "低饱和大抓夹", 90.0, 40.0, "high", "medium", "high"),
                build_scored_item("p2", "通勤骑行抓夹", 82.0, 35.0, "high", "medium", "high"),
            ],
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
        )
        markdown = aggregator.render_report_markdown(payload, cards)

        self.assertTrue(payload["phase2_actions"])
        self.assertIn("selection_advice", payload["phase2_actions"][0])
        self.assertTrue(payload["top_value_points"])
        self.assertIn("## 8. 对阶段 2 选品的直接动作建议", markdown)

    def test_light_tops_direction_card_uses_new_family_and_shape_fields(self):
        aggregator = MarketInsightAggregator()
        items = [
            build_scored_item(
                "lt_1",
                "薄针织短款开衫",
                82.0,
                28.0,
                "high",
                "low",
                "high",
                product_form_or_result="开衫",
                length_form="短款",
                value_points=["轻薄不闷", "空调房实用"],
                style_cluster="薄针织开衫",
                sales_7d=260.0,
                creator_count=30.0,
                video_count=52.0,
                category="light_tops",
                raw_fields={"过去28日销量中位数": 180.0},
            ),
            build_scored_item(
                "lt_2",
                "薄针织常规开衫",
                80.0,
                30.0,
                "high",
                "low",
                "high",
                product_form_or_result="开衫",
                length_form="常规",
                value_points=["轻薄不闷", "空调房实用"],
                style_cluster="薄针织开衫",
                sales_7d=240.0,
                creator_count=22.0,
                video_count=45.0,
                category="light_tops",
                raw_fields={"过去28日销量中位数": 190.0},
            ),
        ]

        cards = aggregator.build_direction_cards(items)

        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card.direction_family, "穿着诉求型")
        self.assertEqual(card.style_cluster, "薄针织开衫")
        self.assertEqual(card.top_silhouette_forms, ["开衫"])
        self.assertEqual(card.top_length_forms, ["短款", "常规"])
        self.assertEqual(card.silhouette_distribution_by_count, {"开衫": 1.0})
        self.assertEqual(card.length_distribution_by_count, {"短款": 0.5, "常规": 0.5})
        self.assertEqual(card.seasonal_trend, "rising")
        self.assertEqual(items[0].seasonal_trend, "rising")

    def test_light_tops_direction_tier_uses_absolute_and_dynamic_thresholds(self):
        aggregator = MarketInsightAggregator()
        items = []
        for index in range(30):
            items.append(
                build_scored_item(
                    f"priority_{index}",
                    f"空调房轻外套{index}",
                    90.0,
                    20.0,
                    "high",
                    "low",
                    "high",
                    product_form_or_result="开衫",
                    length_form="常规",
                    value_points=["空调房实用"],
                    style_cluster="空调房轻外套",
                    sales_7d=320.0,
                    creator_count=18.0,
                    video_count=25.0,
                    category="light_tops",
                )
            )
        for index in range(30):
            items.append(
                build_scored_item(
                    f"crowded_{index}",
                    f"防晒轻罩衫{index}",
                    72.0,
                    80.0,
                    "high",
                    "high",
                    "medium",
                    product_form_or_result="罩衫",
                    length_form="常规",
                    value_points=["防晒不厚重"],
                    style_cluster="防晒轻罩衫",
                    sales_7d=210.0,
                    creator_count=130.0,
                    video_count=180.0,
                    category="light_tops",
                )
            )
        for index in range(30):
            items.append(
                build_scored_item(
                    f"balanced_{index}",
                    f"轻薄衬衫外搭{index}",
                    76.0,
                    46.0,
                    "high",
                    "medium",
                    "medium",
                    product_form_or_result="衬衫",
                    length_form="常规",
                    value_points=["通勤百搭"],
                    style_cluster="轻薄衬衫外搭",
                    sales_7d=240.0,
                    creator_count=40.0,
                    video_count=70.0,
                    category="light_tops",
                )
            )
        for index in range(12):
            items.append(
                build_scored_item(
                    f"low_{index}",
                    f"韩系轻通勤型{index}",
                    65.0,
                    35.0,
                    "medium",
                    "medium",
                    "medium",
                    product_form_or_result="开衫",
                    length_form="短款",
                    value_points=["通勤百搭"],
                    style_cluster="韩系轻通勤型",
                    sales_7d=260.0,
                    creator_count=28.0,
                    video_count=55.0,
                    category="light_tops",
                )
            )

        cards = aggregator.build_direction_cards(items)
        card_map = {card.style_cluster: card for card in cards}

        self.assertEqual(card_map["空调房轻外套"].direction_family, "穿着诉求型")
        self.assertEqual(card_map["韩系轻通勤型"].direction_family, "风格气质型")
        self.assertEqual(card_map["空调房轻外套"].direction_tier, "priority")
        self.assertEqual(card_map["防晒轻罩衫"].direction_tier, "crowded")
        self.assertEqual(card_map["轻薄衬衫外搭"].direction_tier, "balanced")
        self.assertEqual(card_map["韩系轻通勤型"].direction_tier, "low_sample")


if __name__ == "__main__":
    unittest.main()
