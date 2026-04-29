import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_decision_layer import DirectionDecisionLayer  # noqa: E402


def build_card(name, item_count, median_sales, video_density, creator_density, demand=None, tier="crowded"):
    return {
        "direction_canonical_key": "VN__hair_accessory__" + name,
        "direction_name": name,
        "style_cluster": name,
        "country": "VN",
        "category": "hair_accessory",
        "direction_family": "功能结果型",
        "direction_item_count": item_count,
        "direction_sales_median_7d": median_sales,
        "direction_video_density_avg": video_density,
        "direction_creator_density_avg": creator_density,
        "direction_tier": tier,
        "scene_tags": ["通勤骑行", "出门"],
        "top_value_points": ["快速整理头发"],
        "core_elements": ["大抓齿"],
        "target_price_bands": ["5-10 RMB"],
        "demand_structure": demand or {
            "sample_count": item_count,
            "median_sales_7d": median_sales,
            "mean_sales_7d": median_sales,
            "mean_median_ratio": 1.0,
            "top3_sales_share": 0.3,
            "over_threshold_item_ratio": 0.5,
            "sales_p75_7d": median_sales + 50 if item_count >= 12 else None,
            "sales_p90_7d": median_sales + 80 if item_count >= 20 else None,
        },
        "price_band_analysis": {
            "method": "dynamic_quantile_bucket",
            "best_price_band": "mid_price",
            "price_band_confidence": "medium",
            "notes": "",
        },
    }


class DirectionDecisionLayerTest(unittest.TestCase):
    def test_crowded_content_gap_can_still_be_test_action(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        cards, summary = layer.apply(
            [
                build_card("盘发效率型", 18, 380, 0.1, 0.1),
                build_card("甜感装饰型", 80, 240, 1.5, 1.4),
            ],
            country="VN",
            category="hair_accessory",
            batch_id="2026-04-24",
        )
        card = next(item for item in cards if item["style_cluster"] == "盘发效率型")
        self.assertEqual(card["primary_opportunity_type"], "content_gap")
        self.assertEqual(card["decision_action"], "prioritize_low_cost_test")
        self.assertNotEqual(card["decision_action"], "avoid")
        self.assertTrue(card["scale_condition"])
        self.assertTrue(card["stop_loss_condition"])
        self.assertIn("tested_sku_with_sales_count", card["alert"]["missing_metrics"])
        self.assertIn("盘发效率型", summary["prioritize_low_cost_test"]["display_names"])

    def test_head_concentrated_studies_top_before_entering(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        demand = {
            "sample_count": 20,
            "median_sales_7d": 260,
            "mean_sales_7d": 780,
            "mean_median_ratio": 3.0,
            "top3_sales_share": 0.65,
            "over_threshold_item_ratio": 0.25,
            "sales_p75_7d": 900,
            "sales_p90_7d": 1200,
        }
        cards, _ = layer.apply(
            [
                build_card("甜感装饰型", 20, 260, 0.4, 0.3, demand=demand, tier="balanced"),
                build_card("基础通勤型", 20, 220, 0.5, 0.4, tier="balanced"),
            ],
            country="VN",
            category="hair_accessory",
            batch_id="2026-04-24",
        )
        card = next(item for item in cards if item["style_cluster"] == "甜感装饰型")
        self.assertEqual(card["primary_opportunity_type"], "head_concentrated")
        self.assertIn(card["decision_action"], {"study_top_not_enter", "cautious_test"})
        self.assertIn("head_concentrated", card["risk_tags"])

    def test_tiny_sample_observes_and_suppresses_distribution_metrics(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        cards, _ = layer.apply(
            [build_card("头盔友好整理型", 3, 500, 0.05, 0.05)],
            country="VN",
            category="hair_accessory",
            batch_id="2026-04-24",
        )
        card = cards[0]
        self.assertEqual(card["sample_confidence"], "insufficient")
        self.assertEqual(card["primary_opportunity_type"], "insufficient_sample")
        self.assertEqual(card["decision_action"], "hidden_candidate")
        self.assertIsNone(card["demand_structure"]["sales_p75_7d"])

    def test_small_sample_top3_share_does_not_trigger_head_concentrated(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        demand = {
            "sample_count": 9,
            "median_sales_7d": 300,
            "mean_sales_7d": 351,
            "mean_median_ratio": 1.17,
            "top3_sales_share": 0.55,
            "over_threshold_item_ratio": 0.7,
            "sales_p75_7d": None,
            "sales_p90_7d": None,
        }
        cards, _ = layer.apply(
            [build_card("基础通勤型", 9, 300, 0.4, 0.3, demand=demand, tier="balanced")],
            country="VN",
            category="hair_accessory",
            batch_id="2026-04-24",
        )
        card = cards[0]
        self.assertNotEqual(card["primary_opportunity_type"], "head_concentrated")
        self.assertNotIn("head_concentrated", card["risk_tags"])
        self.assertIn("small_sample_top3_share_high", card["risk_tags"])
        self.assertIn("top3_share_expected", card["demand_structure"])

    def test_strong_new_entry_low_confidence_stays_observe_with_actionable_unknown(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        card = build_card("发圈套组型", 6, 320, 0.12, 0.08, tier="low_sample")
        card["new_product_entry_signal"] = {
            "type": "strong_new_entry",
            "confidence": "high",
            "rationale": "新品数量和销量贡献都较高。",
        }
        cards, _ = layer.apply([card], country="VN", category="hair_accessory", batch_id="2026-04-24")
        result = cards[0]
        self.assertEqual(result["decision_action"], "observe")
        self.assertEqual(result["raw_new_product_signal"], "strong_new_entry")
        self.assertEqual(result["actionable_new_product_signal"], "unknown")
        self.assertIn("样本置信度不足", result["new_product_signal_reason"])
        self.assertNotIn("age_data_insufficient", result["risk_tags"])
        self.assertIn("new_entry_signal_unclear", result["risk_tags"])
        self.assertEqual(result["recommended_execution"]["test_sku_count"], "暂不测款，继续观察")

    def test_aesthetic_direction_does_not_use_functional_headwear_angles(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        cards, _ = layer.apply(
            [build_card("大体量气质型", 18, 360, 0.35, 0.25, tier="balanced")],
            country="VN",
            category="hair_accessory",
            batch_id="2026-04-24",
        )
        angles = cards[0]["recommended_execution"]["differentiation_angles"]
        rendered = " ".join(angles["product_angle"] + angles["scene_angle"] + angles["content_angle"])
        self.assertIn("体量感", rendered)
        self.assertNotIn("头盔", rendered)
        self.assertNotIn("30 秒", rendered)

    def test_mean_median_skew_is_not_head_concentrated_without_top3_share(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        demand = {
            "sample_count": 20,
            "median_sales_7d": 260,
            "mean_sales_7d": 540,
            "mean_median_ratio": 2.08,
            "top3_sales_share": 0.268,
            "over_threshold_item_ratio": 0.25,
            "sales_p75_7d": 600,
            "sales_p90_7d": 900,
        }
        cards, _ = layer.apply(
            [build_card("甜感装饰型", 20, 260, 0.45, 0.35, demand=demand, tier="balanced")],
            country="VN",
            category="hair_accessory",
            batch_id="2026-04-24",
        )
        result = cards[0]
        self.assertNotEqual(result["primary_opportunity_type"], "head_concentrated")
        self.assertNotIn("head_concentrated", result["risk_tags"])
        self.assertIn("sales_distribution_skew", result["risk_tags"])

    def test_few_new_winners_is_structure_tag_not_primary_type(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        card = build_card("甜感装饰型", 20, 260, 0.45, 0.35, tier="balanced")
        card["new_product_entry_signal"] = {
            "type": "few_new_winners",
            "confidence": "high",
            "rationale": "新品不多，但少数新品贡献较高销量。",
        }
        cards, _ = layer.apply([card], country="VN", category="hair_accessory", batch_id="2026-04-24")
        result = cards[0]
        self.assertNotEqual(result["primary_opportunity_type"], "few_new_winners")
        self.assertIn("few_new_winners", result["new_product_structure_tags"])
        self.assertEqual(result["decision_action"], "study_top_not_enter")

    def test_high_age_confidence_unknown_signal_does_not_mark_age_insufficient(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        card = build_card("少女礼物感型", 13, 287, 0.31, 0.34, tier="balanced")
        card["new_product_entry_signal"] = {
            "type": "unknown",
            "confidence": "high",
            "rationale": "新品进入信号不明确。",
        }
        cards, _ = layer.apply([card], country="VN", category="hair_accessory", batch_id="2026-04-24")
        result = cards[0]
        self.assertNotIn("age_data_insufficient", result["risk_tags"])
        self.assertIn("new_entry_signal_unclear", result["risk_tags"])

    def test_content_gap_old_product_dominated_records_action_override(self):
        layer = DirectionDecisionLayer(ROOT / "configs" / "market_insight_decision_rules.yaml")
        card = build_card("盘发效率型", 18, 380, 0.1, 0.1)
        card["product_age_structure"] = {
            "old_180d_sales_share": 0.912,
            "new_90d_sales_share": 0.047,
            "age_confidence": "high",
        }
        card["new_product_entry_signal"] = {
            "type": "old_product_dominated",
            "confidence": "high",
            "rationale": "销量主要由老品贡献。",
        }
        cards, _ = layer.apply([card], country="VN", category="hair_accessory", batch_id="2026-04-24")
        result = cards[0]
        self.assertEqual(result["primary_opportunity_type"], "content_gap")
        self.assertEqual(result["default_action_by_type"], "prioritize_low_cost_test")
        self.assertEqual(result["decision_action"], "study_top_not_enter")
        self.assertTrue(result["action_override"]["is_overridden"])
        self.assertEqual(result["action_override"]["override_rule_id"], "OR-002")
        self.assertEqual(result["action_override"]["from_action"], "prioritize_low_cost_test")
        self.assertEqual(result["action_override"]["to_action"], "study_top_not_enter")


if __name__ == "__main__":
    unittest.main()
