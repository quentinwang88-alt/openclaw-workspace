import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_models import MarketDirectionMatchResult  # noqa: E402
from src.models import AccioSupplyResult, CandidateTask, FeatureAnalysisResult  # noqa: E402
from src.product_selection_v2 import ProductSelectionV2Scorer  # noqa: E402


def build_task(title: str = "头盔友好低饱和大抓夹") -> CandidateTask:
    return CandidateTask(
        source_table_id="t1",
        source_record_id="rec_v2",
        product_title=title,
        product_notes="通勤骑行快速整理",
        target_market="VN",
        target_price=35,
        manual_category="发饰",
        extra_fields={"上架天数": 45},
    )


def build_feature_result() -> FeatureAnalysisResult:
    return FeatureAnalysisResult(
        analysis_category="发饰",
        feature_scores={
            "wearing_change_strength": "高",
            "demo_ease": "高",
            "visual_memory_point": "高",
            "homogenization_risk": "中",
            "title_selling_clarity": "高",
            "info_completeness": "高",
        },
        risk_tag="无明显主要风险",
        risk_note="信息完整",
        brief_observation="大抓夹佩戴前后变化明显",
    )


def build_match_result(**overrides) -> MarketDirectionMatchResult:
    payload = {
        "matched_market_direction_id": "dir_helmet",
        "matched_market_direction_name": "头盔友好整理型",
        "score": 0.85,
        "market_match_status": "matched",
        "style_cluster": "头盔友好整理型",
        "top_forms": ["抓夹"],
        "scene_tags": ["通勤骑行"],
        "target_price_bands": ["30-40 RMB"],
        "matched_terms": ["抓夹", "通勤骑行"],
        "representative_products": [
            {"product_id": "p1", "product_name": "基础通勤大抓夹", "price": 42},
            {"product_id": "p2", "product_name": "头盔通勤鲨鱼夹", "price": 39},
            {"product_id": "p3", "product_name": "低饱和整理抓夹", "price": 45},
        ],
    }
    payload.update(overrides)
    return MarketDirectionMatchResult(**payload)


class ProductSelectionV2Test(unittest.TestCase):
    def test_missing_head_products_forces_zero_differentiation(self):
        scorer = ProductSelectionV2Scorer(ROOT)
        result = scorer.score(
            task=build_task(),
            feature_result=build_feature_result(),
            match_result=build_match_result(representative_products=[]),
            supply_result=AccioSupplyResult(supply_check_status="pass"),
            core_score_a=82,
            route_a="priority_test",
        )

        self.assertEqual(result["differentiation"]["confidence"], "insufficient")
        self.assertEqual(result["differentiation"]["score_used"], 0)
        self.assertIn("missing_head_products", result["risk_flags"])

    def test_direction_action_does_not_change_direction_match_score(self):
        scorer = ProductSelectionV2Scorer(ROOT)
        base = scorer.score(
            task=build_task(),
            feature_result=build_feature_result(),
            match_result=build_match_result(direction_action="prioritize_low_cost_test"),
            supply_result=AccioSupplyResult(supply_check_status="pass"),
            core_score_a=82,
            route_a="priority_test",
        )
        constrained = scorer.score(
            task=build_task(),
            feature_result=build_feature_result(),
            match_result=build_match_result(direction_action="study_top_not_enter"),
            supply_result=AccioSupplyResult(supply_check_status="pass"),
            core_score_a=82,
            route_a="priority_test",
        )

        self.assertEqual(base["direction_match"]["score"], constrained["direction_match"]["score"])

    def test_study_top_requires_strict_upgrade_conditions_for_select(self):
        scorer = ProductSelectionV2Scorer(ROOT)
        result = scorer.score(
            task=build_task(title="低饱和礼物包装可展示大抓夹"),
            feature_result=build_feature_result(),
            match_result=build_match_result(direction_action="study_top_not_enter"),
            supply_result=AccioSupplyResult(supply_check_status="pass"),
            core_score_a=82,
            route_a="priority_test",
        )

        if result["final_action"] == "select":
            self.assertGreaterEqual(result["differentiation"]["score_used"], 12)
            self.assertGreaterEqual(result["content_potential"]["score"], 15)
            self.assertIn(result["differentiation"]["confidence"], {"medium", "high"})
            self.assertTrue(result["differentiation"]["has_concrete_difference"])
        else:
            self.assertIn(result["final_action"], {"manual_review", "observe", "head_reference", "eliminate"})

    def test_v31_outputs_task_pool_and_fallback_brief(self):
        scorer = ProductSelectionV2Scorer(ROOT)
        result = scorer.score(
            task=build_task(),
            feature_result=build_feature_result(),
            match_result=build_match_result(direction_action="observe"),
            supply_result=AccioSupplyResult(supply_check_status="pass"),
            core_score_a=82,
            route_a="priority_test",
        )

        self.assertIn("unified_decision", result)
        self.assertIn("market_task_fit", result)
        self.assertEqual(result["direction_execution_brief_ref"]["brief_source"], "auto_fallback")
        self.assertLessEqual(result["market_task_fit"]["score"], 10)
        self.assertIn("brief_auto_generated", result["risk_flags"])
        self.assertEqual(result["target_pool"], "observe_pool")
        self.assertTrue(result["lifecycle_status"])

    def test_v31_task_type_conflict_resolves_to_unified_matrix(self):
        scorer = ProductSelectionV2Scorer(ROOT)
        result = scorer.score(
            task=build_task(title="礼物感包装大抓夹"),
            feature_result=build_feature_result(),
            match_result=build_match_result(
                direction_action="study_top_not_enter",
                direction_execution_brief={
                    "direction_action": "study_top_not_enter",
                    "task_type": "low_cost_test",
                    "target_pool": "test_product_pool",
                    "brief_source": "generated",
                    "brief_confidence": "medium",
                    "positive_signals": ["礼物感", "包装"],
                },
            ),
            supply_result=AccioSupplyResult(supply_check_status="pass"),
            core_score_a=82,
            route_a="priority_test",
        )

        self.assertTrue(result["unified_decision"]["has_conflict"])
        self.assertIn("task_type_conflict", result["risk_flags"])
        self.assertEqual(result["unified_decision"]["task_type"], "head_dissection")
        self.assertIn(result["final_action"], {"manual_review", "observe", "head_reference", "eliminate"})


if __name__ == "__main__":
    unittest.main()
