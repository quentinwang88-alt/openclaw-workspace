import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_db import MarketInsightDatabase  # noqa: E402
from src.models import CandidateTask, FeatureAnalysisResult, PendingAnalysisItem  # noqa: E402
from src.market_insight_models import MarketDirectionMatchResult  # noqa: E402
from src.models import StorePositioningCard  # noqa: E402
from src.scoring_engine import ScoringEngine  # noqa: E402


class ScoringEngineTest(unittest.TestCase):
    def test_variant_a_core_score_uses_updated_store_heavier_weights(self):
        engine = ScoringEngine()

        core_score_norm, route, competition_downgraded = engine._evaluate_route_variant(
            market_match_norm=0.8,
            market_match_status="matched",
            store_fit_norm=0.6,
            content_norm=0.7,
            supply_check_status="pass",
            competition_level="medium",
            competition_confidence="low",
            severe_content_blocker=False,
            hard_blacklist_hit=False,
            variant="A",
        )

        self.assertAlmostEqual(core_score_norm, 0.69, places=4)
        self.assertEqual(route, "small_test")
        self.assertFalse(competition_downgraded)

    def test_score_light_top_candidate(self):
        engine = ScoringEngine()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_1",
            batch_id="store_a",
            product_title="法式收腰罩衫",
            product_notes="新店需要轻法式通勤内容",
            target_price=49.0,
            extra_fields={
                "StorePositioningCard": {
                    "style_whitelist": ["法式通勤"],
                    "target_price_bands": ["0-50"],
                    "core_scenes": ["通勤"],
                    "content_tones": ["轻法式"],
                },
                "supply_check_status": "pass",
                "competition_reference_level": "medium",
            },
        )
        feature_result = FeatureAnalysisResult(
            analysis_category="轻上装",
            feature_scores={
                "upper_body_change_strength": "高",
                "camera_readability": "高",
                "design_signal_strength": "中",
                "basic_style_escape_strength": "中",
                "title_selling_clarity": "高",
                "info_completeness": "中",
            },
            risk_tag="无明显主要风险",
            risk_note="基础信息较完整",
            brief_observation="镜头可读性较强，适合优先测试",
        )
        market_direction_result = MarketDirectionMatchResult(
            matched_market_direction_id="dir_1",
            matched_market_direction_name="法式通勤薄外搭",
            matched_market_direction_reason="标题/关键词命中 法式通勤、外搭",
            score=0.82,
            cards_available=True,
            style_cluster="法式通勤",
            product_form="外搭",
            scene_tags=["通勤"],
            target_price_bands=["0-50"],
        )

        scored = engine.score_candidate(task=task, feature_result=feature_result, market_direction_result=market_direction_result)

        self.assertEqual(scored.product_potential, "高")
        self.assertEqual(scored.content_potential, "高")
        self.assertGreaterEqual(scored.batch_priority_score, 80)
        self.assertEqual(scored.suggested_action, "优先测试")
        self.assertGreaterEqual(scored.market_match_score, 80)
        self.assertGreaterEqual(scored.store_fit_score, 80)
        self.assertEqual(scored.route_a, "priority_test")
        self.assertEqual(scored.route_b, "priority_test")
        self.assertEqual(scored.supply_check_status, "pass")
        self.assertTrue(scored.v2_shadow_result.get("shadow_mode"))
        self.assertIn("total_score", scored.v2_shadow_result)

    def test_score_candidate_uses_rmb_normalized_target_price_for_store_fit(self):
        engine = ScoringEngine()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_vn",
            batch_id="store_vn",
            product_title="低饱和蓝色大抓齿抓夹",
            target_market="VN",
            target_price=150000,
            extra_fields={
                "StorePositioningCard": {
                    "style_whitelist": ["头盔友好整理型"],
                    "target_price_bands": ["35-40 RMB"],
                    "core_scenes": ["通勤骑行"],
                    "content_tones": ["实用整理"],
                },
                "supply_check_status": "pass",
                "competition_reference_level": "medium",
            },
        )
        feature_result = FeatureAnalysisResult(
            analysis_category="发饰",
            feature_scores={
                "wearing_change_strength": "高",
                "demo_ease": "高",
                "visual_memory_point": "中",
                "homogenization_risk": "中",
                "title_selling_clarity": "高",
                "info_completeness": "中",
            },
            risk_tag="无明显主要风险",
            risk_note="信息完整",
            brief_observation="适合快速整理头发类内容",
        )
        market_direction_result = MarketDirectionMatchResult(
            matched_market_direction_id="dir_vn_1",
            matched_market_direction_name="头盔友好整理型",
            matched_market_direction_reason="标题/关键词命中 头盔友好整理型、抓夹、价格带接近",
            score=0.83,
            cards_available=True,
            style_cluster="头盔友好整理型",
            product_form="抓夹",
            top_forms=["抓夹", "发夹"],
            scene_tags=["通勤骑行"],
            target_price_bands=["35-40 RMB"],
            matched_terms=["头盔友好整理型", "抓夹"],
        )

        scored = engine.score_candidate(task=task, feature_result=feature_result, market_direction_result=market_direction_result)

        self.assertGreaterEqual(scored.store_fit_score, 80)

    def test_supply_margin_assessment_uses_rmb_normalized_target_price(self):
        engine = ScoringEngine()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_margin",
            batch_id="store_vn",
            product_title="低饱和蓝色大抓齿抓夹",
            target_market="VN",
            target_price=150000,
            extra_fields={
                "采购价": 30,
            },
        )
        feature_result = FeatureAnalysisResult(
            analysis_category="发饰",
            feature_scores={
                "wearing_change_strength": "高",
                "demo_ease": "中",
                "visual_memory_point": "中",
                "homogenization_risk": "中",
                "title_selling_clarity": "高",
                "info_completeness": "中",
            },
            risk_tag="无明显主要风险",
            risk_note="信息完整",
            brief_observation="适合快速整理头发类内容",
        )

        scored = engine.score_candidate(task=task, feature_result=feature_result, market_direction_result=None)

        self.assertEqual(scored.supply_check_status, "fail")

    def test_score_candidate_reads_store_positioning_card_from_database(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database = MarketInsightDatabase(Path(temp_dir) / "market_insight.db")
            database.upsert_store_positioning_cards(
                source_table_id="tb_store_cards",
                cards=[
                    StorePositioningCard(
                        source_record_id="rec_store_1",
                        store_id="store_vn_1",
                        card_name="越南发饰店",
                        country="VN",
                        category="hair_accessory",
                        style_whitelist=["头盔友好整理型"],
                        target_price_bands=["35-40 RMB"],
                        core_scenes=["通勤骑行"],
                        content_tones=["实用整理"],
                        core_value_points=["快速整理头发"],
                    )
                ],
                updated_at_epoch=1,
            )
            engine = ScoringEngine(store_positioning_database=database)
            task = CandidateTask(
                source_table_id="t1",
                source_record_id="rec_db_store",
                batch_id="store_vn_1",
                product_title="低饱和蓝色大抓齿抓夹",
                target_market="VN",
                target_price=150000,
                extra_fields={
                    "店铺ID": "store_vn_1",
                    "supply_check_status": "pass",
                },
            )
            feature_result = FeatureAnalysisResult(
                analysis_category="发饰",
                feature_scores={
                    "wearing_change_strength": "高",
                    "demo_ease": "高",
                    "visual_memory_point": "中",
                    "homogenization_risk": "中",
                    "title_selling_clarity": "高",
                    "info_completeness": "中",
                },
                risk_tag="无明显主要风险",
                risk_note="信息完整",
                brief_observation="适合快速整理头发类内容",
            )
            market_direction_result = MarketDirectionMatchResult(
                matched_market_direction_id="dir_vn_store",
                matched_market_direction_name="头盔友好整理型",
                matched_market_direction_reason="标题/关键词命中 头盔友好整理型、抓夹、价格带接近",
                score=0.83,
                cards_available=True,
                style_cluster="头盔友好整理型",
                product_form="抓夹",
                scene_tags=["通勤骑行"],
                target_price_bands=["35-40 RMB"],
                matched_terms=["头盔友好整理型", "抓夹"],
            )

            scored = engine.score_candidate(task=task, feature_result=feature_result, market_direction_result=market_direction_result)

            self.assertGreaterEqual(scored.store_fit_score, 80)

    def test_uncovered_market_match_renormalizes_core_score(self):
        engine = ScoringEngine()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_uncovered",
            batch_id="store_vn",
            product_title="立体蝴蝶结抓夹",
            target_price=39.0,
            extra_fields={
                "StorePositioningCard": {
                    "style_whitelist": ["甜美发饰"],
                    "target_price_bands": ["0-50"],
                    "core_scenes": ["通勤"],
                    "content_tones": ["佩戴变化"],
                },
                "supply_check_status": "pass",
            },
        )
        feature_result = FeatureAnalysisResult(
            analysis_category="发饰",
            feature_scores={
                "wearing_change_strength": "高",
                "demo_ease": "高",
                "visual_memory_point": "高",
                "homogenization_risk": "中",
                "title_selling_clarity": "中",
                "info_completeness": "中",
            },
            risk_tag="无明显主要风险",
            risk_note="信息完整",
            brief_observation="佩戴前后变化明显",
        )

        scored = engine.score_candidate(
            task=task,
            feature_result=feature_result,
            market_direction_result=MarketDirectionMatchResult(
                market_match_status="uncovered",
                matched_market_direction_reason="方向卡未覆盖",
                cards_available=True,
            ),
        )

        expected_core_score = round(
            (((scored.content_potential_score / 100.0) * 0.40) + ((scored.store_fit_score / 100.0) * 0.35)) / 0.75 * 100.0,
            2,
        )
        self.assertIsNone(scored.market_match_score)
        self.assertEqual(scored.market_match_status, "uncovered")
        self.assertAlmostEqual(scored.core_score_a, expected_core_score, places=2)

    def test_low_content_without_severe_blocker_goes_pending_review(self):
        engine = ScoringEngine()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_review",
            batch_id="store_review",
            product_title="基础纯色细发箍",
            product_notes="轻基础，店铺可接受",
            target_price=39.0,
            extra_fields={
                "StorePositioningCard": {
                    "style_whitelist": ["基础日常"],
                    "target_price_bands": ["0-50"],
                    "core_scenes": ["通勤"],
                    "content_tones": ["快速佩戴"],
                },
                "supply_check_status": "pass",
            },
        )
        feature_result = FeatureAnalysisResult(
            analysis_category="发饰",
            feature_scores={
                "wearing_change_strength": "低",
                "demo_ease": "中",
                "visual_memory_point": "低",
                "homogenization_risk": "高",
                "title_selling_clarity": "中",
                "info_completeness": "中",
            },
            risk_tag="同质化偏高",
            risk_note="内容表达空间有限，但不是绝对不可做",
            brief_observation="表达空间偏窄",
        )
        market_direction_result = MarketDirectionMatchResult(
            matched_market_direction_id="dir_review",
            matched_market_direction_name="基础日常发箍",
            matched_market_direction_reason="标题/关键词命中 基础日常、发箍",
            score=0.72,
            market_match_status="matched",
            cards_available=True,
            style_cluster="基础日常",
            product_form="发箍",
            scene_tags=["通勤"],
        )

        scored = engine.score_candidate(task=task, feature_result=feature_result, market_direction_result=market_direction_result)

        self.assertLess(scored.content_potential_score, 45.0)
        self.assertEqual(scored.route_a, "pending_review")
        self.assertEqual(scored.suggested_action, "补信息后再看")
        self.assertTrue(scored.needs_manual_review)
        self.assertIn("人工", scored.manual_review_reason)

    def test_hard_blacklist_directly_rejects(self):
        engine = ScoringEngine()
        task = CandidateTask(
            source_table_id="t1",
            source_record_id="rec_blacklist",
            batch_id="store_blacklist",
            product_title="学院风蝴蝶结抓夹",
            target_price=45.0,
            extra_fields={
                "StorePositioningCard": {
                    "style_whitelist": ["日常基础"],
                    "hard_style_blacklist": ["学院风"],
                    "target_price_bands": ["0-50"],
                    "core_scenes": ["通勤"],
                    "content_tones": ["快速佩戴"],
                },
                "supply_check_status": "pass",
            },
        )
        feature_result = FeatureAnalysisResult(
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
            brief_observation="风格表达清晰",
        )
        market_direction_result = MarketDirectionMatchResult(
            matched_market_direction_id="dir_blacklist",
            matched_market_direction_name="学院风发饰",
            matched_market_direction_reason="标题/关键词命中 学院风、蝴蝶结",
            score=0.84,
            market_match_status="matched",
            cards_available=True,
            style_cluster="学院风",
            product_form="抓夹",
            scene_tags=["通勤"],
        )

        scored = engine.score_candidate(task=task, feature_result=feature_result, market_direction_result=market_direction_result)

        self.assertEqual(scored.route_a, "reject")
        self.assertEqual(scored.suggested_action, "暂不建议推进")
        self.assertIn("硬黑名单", scored.brief_reason)

    def test_calibrate_group_limits_priority_slots(self):
        engine = ScoringEngine()
        items = []
        for index, score in enumerate([88, 82, 78, 55], start=1):
            feature_result = FeatureAnalysisResult(
                analysis_category="轻上装",
                feature_scores={
                    "upper_body_change_strength": "高" if index < 3 else "中",
                    "camera_readability": "高" if index < 3 else "中",
                    "design_signal_strength": "中",
                    "basic_style_escape_strength": "中",
                    "title_selling_clarity": "高",
                    "info_completeness": "中",
                },
                risk_tag="无明显主要风险" if index < 4 else "同质化偏高",
                risk_note="说明",
                brief_observation="观察",
            )
            scored = engine.score_candidate(
                task=CandidateTask(
                    source_table_id="t1",
                    source_record_id=f"rec_{index}",
                    batch_id="batch_1",
                    product_title=f"产品{index}",
                    final_category="轻上装",
                    category_confidence="manual",
                    target_price=39.0,
                    extra_fields={"supply_check_status": "pass"},
                ),
                feature_result=feature_result,
                market_direction_result=MarketDirectionMatchResult(score=0.80, matched_market_direction_id="dir_1", cards_available=True),
            )
            scored = scored.__class__(
                analysis_category=scored.analysis_category,
                product_potential=scored.product_potential,
                content_potential=scored.content_potential,
                batch_priority_score=score,
                suggested_action="优先测试",
                brief_reason=scored.brief_reason,
                market_match_score=scored.market_match_score,
                store_fit_score=scored.store_fit_score,
                content_potential_score=scored.content_potential_score,
                core_score_a=score,
                route_a="priority_test",
                core_score_b=score,
                route_b="priority_test",
                supply_check_status="pass",
                supply_summary=scored.supply_summary,
                competition_reference_level="medium",
                competition_confidence="low",
                decision_reason=scored.decision_reason,
                recommended_content_formulas=scored.recommended_content_formulas,
                reserve_reason=scored.reserve_reason,
                reserve_created_at=scored.reserve_created_at,
                reserve_expires_at=scored.reserve_expires_at,
                reserve_status=scored.reserve_status,
                sample_check_status=scored.sample_check_status,
                matched_market_direction_id=scored.matched_market_direction_id,
                matched_market_direction_name=scored.matched_market_direction_name,
                matched_market_direction_reason=scored.matched_market_direction_reason,
            )
            items.append(
                PendingAnalysisItem(
                    task=CandidateTask(
                        source_table_id="t1",
                        source_record_id=f"rec_{index}",
                        batch_id="batch_1",
                        product_title=f"产品{index}",
                        final_category="轻上装",
                        category_confidence="manual",
                    ),
                    feature_result=feature_result,
                    scored_result=scored,
                )
            )

        calibrated = engine.calibrate_group(items)
        actions = {item.task.source_record_id: item.scored_result.suggested_action for item in calibrated}

        self.assertEqual(actions["rec_1"], "优先测试")
        self.assertEqual(actions["rec_2"], "优先测试")
        self.assertEqual(actions["rec_3"], "低成本试款")
        self.assertEqual(actions["rec_4"], "低成本试款")

    def test_calibrate_group_keeps_priority_when_fewer_than_three_qualified(self):
        engine = ScoringEngine()
        items = []
        for index, score in enumerate([88, 81], start=1):
            feature_result = FeatureAnalysisResult(
                analysis_category="轻上装",
                feature_scores={
                    "upper_body_change_strength": "高",
                    "camera_readability": "高",
                    "design_signal_strength": "中",
                    "basic_style_escape_strength": "中",
                    "title_selling_clarity": "高",
                    "info_completeness": "中",
                },
                risk_tag="无明显主要风险",
                risk_note="说明",
                brief_observation="观察",
            )
            items.append(
                PendingAnalysisItem(
                    task=CandidateTask(
                        source_table_id="t1",
                        source_record_id=f"rec_keep_{index}",
                        batch_id="batch_keep",
                        product_title=f"产品{index}",
                        final_category="轻上装",
                        category_confidence="manual",
                    ),
                    feature_result=feature_result,
                    scored_result=engine.score_candidate(
                        task=CandidateTask(
                            source_table_id="t1",
                            source_record_id=f"rec_keep_{index}",
                            batch_id="batch_keep",
                            product_title=f"产品{index}",
                            final_category="轻上装",
                            category_confidence="manual",
                            target_price=49.0,
                            extra_fields={"supply_check_status": "pass"},
                        ),
                        feature_result=feature_result,
                        market_direction_result=MarketDirectionMatchResult(
                            score=0.80,
                            matched_market_direction_id="dir_keep",
                            market_match_status="matched",
                            cards_available=True,
                        ),
                    ).__class__(
                        analysis_category="轻上装",
                        product_potential="高",
                        content_potential="高",
                        batch_priority_score=score,
                        suggested_action="优先测试",
                        brief_reason="测试",
                        market_match_score=80.0,
                        market_match_status="matched",
                        store_fit_score=78.0,
                        content_potential_score=82.0,
                        core_score_a=score,
                        route_a="priority_test",
                        core_score_b=score,
                        route_b="priority_test",
                        supply_check_status="pass",
                        supply_summary="供给证据完整",
                    ),
                )
            )

        calibrated = engine.calibrate_group(items)
        actions = {item.task.source_record_id: item.scored_result.suggested_action for item in calibrated}

        self.assertEqual(actions["rec_keep_1"], "优先测试")
        self.assertEqual(actions["rec_keep_2"], "优先测试")


if __name__ == "__main__":
    unittest.main()
