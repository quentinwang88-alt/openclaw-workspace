import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_direction_matcher import MarketDirectionMatcher  # noqa: E402
from src.market_insight_db import MarketInsightDatabase  # noqa: E402
from src.market_insight_models import MarketInsightProductRunState, MarketDirectionCard  # noqa: E402


class FakeTask(object):
    def __init__(self):
        self.target_market = "VN"
        self.product_title = "低饱和蓝色大抓齿抓夹"
        self.title_keyword_tags = ["低饱和纯色", "大抓齿", "快速整理头发"]
        self.target_price = 150000
        self.extra_fields = {}


class FakeLightTopTask(object):
    def __init__(self):
        self.target_market = "VN"
        self.product_title = "短款薄针织开衫 空调房外搭"
        self.title_keyword_tags = ["短款", "开衫", "空调房实用", "轻薄不闷"]
        self.target_price = 320000
        self.extra_fields = {}


class MarketDirectionMatcherTest(unittest.TestCase):
    def test_match_candidate_reads_latest_cards_and_returns_reason(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            latest_dir = root / "latest"
            latest_dir.mkdir(parents=True, exist_ok=True)
            cards_path = root / "cards.json"
            cards_path.write_text(
                json.dumps(
                    [
                        {
                            "direction_canonical_key": "VN__hair_accessory__头盔友好整理型",
                            "direction_instance_id": "2026-04-21__VN__hair_accessory__头盔友好整理型",
                            "direction_name": "头盔友好整理型",
                            "style_cluster": "头盔友好整理型",
                            "top_forms": ["抓夹", "发夹"],
                            "core_elements": ["大抓齿", "低饱和纯色"],
                            "top_value_points": ["快速整理头发", "头盔友好"],
                            "target_price_bands": ["35-40 RMB"],
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (latest_dir / "VN__hair_accessory.json").write_text(
                json.dumps(
                    {
                        "cards_path": str(cards_path),
                        "source_scope": "official",
                        "is_consumable": True,
                        "completed_product_count": 300,
                        "direction_count": 5,
                        "min_consumable_product_count": 100,
                        "min_consumable_direction_count": 5,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            matcher = MarketDirectionMatcher(root)
            result = matcher.match_candidate(FakeTask(), final_category="发饰")

            self.assertEqual(result.matched_market_direction_id, "VN__hair_accessory__头盔友好整理型")
            self.assertEqual(result.matched_market_direction_name, "头盔友好整理型")
            self.assertIn("快速整理头发", result.matched_market_direction_reason)
            self.assertGreaterEqual(result.score, 0.5)
            self.assertTrue(result.cards_available)
            self.assertIn("价格带接近", result.matched_market_direction_reason)

    def test_match_candidate_marks_uncovered_when_cards_do_not_cover_product(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            latest_dir = root / "latest"
            latest_dir.mkdir(parents=True, exist_ok=True)
            cards_path = root / "cards.json"
            cards_path.write_text(
                json.dumps(
                    [
                        {
                            "direction_canonical_key": "VN__hair_accessory__通勤整理型",
                            "direction_instance_id": "2026-04-21__VN__hair_accessory__通勤整理型",
                            "direction_name": "通勤整理型",
                            "style_cluster": "通勤整理型",
                            "top_forms": ["抓夹"],
                            "core_elements": ["大抓齿"],
                            "scene_tags": ["通勤骑行"],
                            "target_price_bands": ["35-40 RMB"],
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (latest_dir / "VN__hair_accessory.json").write_text(
                json.dumps(
                    {
                        "cards_path": str(cards_path),
                        "source_scope": "official",
                        "is_consumable": True,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            task = FakeTask()
            task.product_title = "夸张金属感发圈"
            task.title_keyword_tags = ["金属感", "发圈"]
            matcher = MarketDirectionMatcher(root)
            result = matcher.match_candidate(task, final_category="发饰")

            self.assertEqual(result.market_match_status, "uncovered")
            self.assertEqual(result.matched_market_direction_id, "")
            self.assertIn("方向卡未覆盖", result.matched_market_direction_reason)

    def test_match_candidate_marks_weak_matched_on_partial_hit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            latest_dir = root / "latest"
            latest_dir.mkdir(parents=True, exist_ok=True)
            cards_path = root / "cards.json"
            cards_path.write_text(
                json.dumps(
                    [
                        {
                            "direction_canonical_key": "VN__hair_accessory__低饱和整理型",
                            "direction_instance_id": "2026-04-21__VN__hair_accessory__低饱和整理型",
                            "direction_name": "低饱和整理型",
                            "style_cluster": "低饱和整理型",
                            "top_forms": ["抓夹"],
                            "core_elements": ["低饱和纯色"],
                            "target_price_bands": ["35-40 RMB"],
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (latest_dir / "VN__hair_accessory.json").write_text(
                json.dumps(
                    {
                        "cards_path": str(cards_path),
                        "source_scope": "official",
                        "is_consumable": True,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            task = FakeTask()
            task.product_title = "低饱和蓝色发圈"
            task.title_keyword_tags = ["低饱和纯色", "发圈"]
            matcher = MarketDirectionMatcher(root)
            result = matcher.match_candidate(task, final_category="发饰")

            self.assertEqual(result.market_match_status, "weak_matched")
            self.assertGreater(result.score, 0.0)
            self.assertLess(result.score, 0.45)

    def test_match_candidate_reads_from_database_without_latest_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database = MarketInsightDatabase(root / "market_insight.db")
            run_state = MarketInsightProductRunState(
                table_id="table_1",
                table_name="测试表",
                input_mode="product_ranking",
                batch_date="2026-04-21",
                country="VN",
                category="hair_accessory",
                artifacts_dir=str(root / "run_1"),
                source_scope="official",
                min_consumable_product_count=100,
                min_consumable_direction_count=1,
                product_snapshot_path="",
                product_tags_path="",
                direction_cards_path="",
                report_json_path="",
                report_md_path="",
                report_delivery_path="",
                progress_json_path="",
                voc_status="skipped",
            )
            database.upsert_direction_card_run(
                table_id="table_1",
                run_state=run_state,
                snapshots=[],
                scored_items=[],
                direction_cards=[
                    MarketDirectionCard(
                        direction_canonical_key="VN__hair_accessory__头盔友好整理型",
                        direction_instance_id="2026-04-21__VN__hair_accessory__头盔友好整理型",
                        batch_date="2026-04-21",
                        country="VN",
                        category="hair_accessory",
                        direction_name="头盔友好整理型",
                        style_cluster="头盔友好整理型",
                        top_forms=["抓夹", "发夹"],
                        form_distribution={"抓夹": 70.0, "发夹": 30.0},
                        core_elements=["大抓齿", "低饱和纯色"],
                        scene_tags=["通勤骑行"],
                        target_price_bands=["35-40 RMB"],
                        heat_level="high",
                        crowd_level="medium",
                        top_value_points=["快速整理头发"],
                        representative_products=[{"product_id": "p1", "product_name": "商品1"}],
                        priority_level="high",
                        selection_advice="优先补",
                        avoid_notes="避免同质化",
                        confidence=0.8,
                        product_count=2,
                        average_heat_score=80.0,
                        average_crowd_score=40.0,
                        direction_key="VN__hair_accessory__头盔友好整理型",
                    )
                ],
                completed_product_count=300,
                total_product_count=300,
                run_status="completed",
                updated_at_epoch=1,
                report_json_path="",
                report_md_path="",
                progress_json_path="",
                report_payload={},
                report_delivery={},
                llm_fallback_count=0,
                source_scope="official",
                min_consumable_product_count=100,
                min_consumable_direction_count=1,
            )

            matcher = MarketDirectionMatcher(root)
            result = matcher.match_candidate(FakeTask(), final_category="发饰")

            self.assertEqual(result.matched_market_direction_id, "VN__hair_accessory__头盔友好整理型")
            self.assertEqual(result.matched_market_direction_name, "头盔友好整理型")
            self.assertTrue(result.cards_available)

    def test_match_candidate_ignores_newer_smoke_test_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database = MarketInsightDatabase(root / "market_insight.db")

            official_state = MarketInsightProductRunState(
                table_id="table_1",
                table_name="正式表",
                input_mode="product_ranking",
                batch_date="2026-04-21",
                country="VN",
                category="hair_accessory",
                artifacts_dir=str(root / "run_official"),
                source_scope="official",
                min_consumable_product_count=100,
                min_consumable_direction_count=1,
                product_snapshot_path="",
                product_tags_path="",
                direction_cards_path="",
                report_json_path="",
                report_md_path="",
                report_delivery_path="",
                progress_json_path="",
                voc_status="skipped",
            )
            database.upsert_direction_card_run(
                table_id="table_1",
                run_state=official_state,
                snapshots=[],
                scored_items=[],
                direction_cards=[
                    MarketDirectionCard(
                        direction_canonical_key="VN__hair_accessory__头盔友好整理型",
                        direction_instance_id="2026-04-21__VN__hair_accessory__头盔友好整理型",
                        batch_date="2026-04-21",
                        country="VN",
                        category="hair_accessory",
                        direction_name="头盔友好整理型",
                        style_cluster="头盔友好整理型",
                        top_forms=["抓夹"],
                        form_distribution={"抓夹": 100.0},
                        core_elements=["大抓齿", "低饱和纯色"],
                        scene_tags=["通勤骑行"],
                        target_price_bands=["35-40 RMB"],
                        heat_level="high",
                        crowd_level="medium",
                        top_value_points=["快速整理头发"],
                        representative_products=[{"product_id": "p1", "product_name": "商品1"}],
                        priority_level="high",
                        selection_advice="优先补",
                        avoid_notes="避免同质化",
                        confidence=0.8,
                        product_count=120,
                        average_heat_score=80.0,
                        average_crowd_score=40.0,
                        direction_key="VN__hair_accessory__头盔友好整理型",
                    )
                ],
                completed_product_count=300,
                total_product_count=300,
                run_status="completed",
                updated_at_epoch=1,
                report_json_path="",
                report_md_path="",
                progress_json_path="",
                report_payload={},
                report_delivery={},
                llm_fallback_count=0,
                source_scope="official",
                min_consumable_product_count=100,
                min_consumable_direction_count=1,
            )

            smoke_state = MarketInsightProductRunState(
                table_id="table_1",
                table_name="测试表",
                input_mode="product_ranking",
                batch_date="2026-04-22",
                country="VN",
                category="hair_accessory",
                artifacts_dir=str(root / "run_smoke"),
                source_scope="smoke_test",
                min_consumable_product_count=100,
                min_consumable_direction_count=1,
                product_snapshot_path="",
                product_tags_path="",
                direction_cards_path="",
                report_json_path="",
                report_md_path="",
                report_delivery_path="",
                progress_json_path="",
                voc_status="skipped",
            )
            database.upsert_direction_card_run(
                table_id="table_1",
                run_state=smoke_state,
                snapshots=[],
                scored_items=[],
                direction_cards=[
                    MarketDirectionCard(
                        direction_canonical_key="VN__hair_accessory__测试方向",
                        direction_instance_id="2026-04-22__VN__hair_accessory__测试方向",
                        batch_date="2026-04-22",
                        country="VN",
                        category="hair_accessory",
                        direction_name="测试方向",
                        style_cluster="测试方向",
                        top_forms=["发夹"],
                        form_distribution={"发夹": 100.0},
                        core_elements=["亮色"],
                        scene_tags=["出门"],
                        target_price_bands=["0-5 RMB"],
                        heat_level="low",
                        crowd_level="low",
                        top_value_points=["送礼"],
                        representative_products=[{"product_id": "p2", "product_name": "商品2"}],
                        priority_level="low",
                        selection_advice="观察",
                        avoid_notes="样本少",
                        confidence=0.5,
                        product_count=1,
                        average_heat_score=20.0,
                        average_crowd_score=20.0,
                        direction_key="VN__hair_accessory__测试方向",
                    )
                ],
                completed_product_count=1,
                total_product_count=1,
                run_status="completed",
                updated_at_epoch=2,
                report_json_path="",
                report_md_path="",
                progress_json_path="",
                report_payload={},
                report_delivery={},
                llm_fallback_count=0,
                source_scope="smoke_test",
                min_consumable_product_count=100,
                min_consumable_direction_count=1,
            )

            matcher = MarketDirectionMatcher(root)
            result = matcher.match_candidate(FakeTask(), final_category="发饰")

            self.assertEqual(result.matched_market_direction_name, "头盔友好整理型")
            self.assertNotEqual(result.matched_market_direction_name, "测试方向")

    def test_match_candidate_prefers_light_top_silhouette_and_length_forms(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            latest_dir = root / "latest"
            latest_dir.mkdir(parents=True, exist_ok=True)
            cards_path = root / "cards.json"
            cards_path.write_text(
                json.dumps(
                    [
                        {
                            "direction_canonical_key": "VN__light_tops__薄针织开衫",
                            "direction_instance_id": "2026-04-21__VN__light_tops__薄针织开衫",
                            "direction_name": "薄针织开衫",
                            "style_cluster": "薄针织开衫",
                            "top_forms": ["开衫"],
                            "top_silhouette_forms": ["开衫"],
                            "top_length_forms": ["短款", "常规"],
                            "core_elements": ["薄针织", "开衫纽扣"],
                            "top_value_points": ["轻薄不闷", "空调房实用"],
                            "target_price_bands": ["80-100 RMB"],
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (latest_dir / "VN__light_tops.json").write_text(
                json.dumps(
                    {
                        "cards_path": str(cards_path),
                        "source_scope": "official",
                        "is_consumable": True,
                        "completed_product_count": 300,
                        "direction_count": 5,
                        "min_consumable_product_count": 100,
                        "min_consumable_direction_count": 5,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            matcher = MarketDirectionMatcher(root)
            result = matcher.match_candidate(FakeLightTopTask(), final_category="轻上装")

            self.assertEqual(result.matched_market_direction_id, "VN__light_tops__薄针织开衫")
            self.assertEqual(result.matched_market_direction_name, "薄针织开衫")
            self.assertIn("开衫", result.matched_terms)
            self.assertIn("短款", result.matched_terms)
            self.assertEqual(result.top_silhouette_forms, ["开衫"])
            self.assertEqual(result.top_length_forms, ["短款", "常规"])


if __name__ == "__main__":
    unittest.main()
