import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_models import (  # noqa: E402
    MarketDirectionCard,
    MarketInsightConfig,
    MarketInsightProductTag,
    ProductRankingSnapshot,
    ScoredProductSnapshot,
    VOCLightSummary,
)
from src.market_insight_writer import MarketInsightWriter  # noqa: E402


def build_snapshot(product_id: str) -> ProductRankingSnapshot:
    return ProductRankingSnapshot(
        batch_date="2026-04-21",
        batch_id="batch_1",
        country="VN",
        category="hair_accessory",
        product_id=product_id,
        product_name=f"商品{product_id}",
        shop_name="店铺A",
        price_min=10.0,
        price_max=20.0,
        price_mid=15.0,
        sales_7d=10.0,
        gmv_7d=100.0,
        creator_count=2.0,
        video_count=3.0,
        listing_days=5,
        product_images=["https://cdn.example.com/1.jpg"],
        image_url="https://cdn.example.com/1.jpg",
        product_url=f"https://example.com/{product_id}",
        rank_index=1,
    )


def build_scored(snapshot: ProductRankingSnapshot, is_valid_sample: bool = True) -> ScoredProductSnapshot:
    return ScoredProductSnapshot(
        snapshot=snapshot,
        tag=MarketInsightProductTag(
            is_valid_sample=is_valid_sample,
            style_cluster="韩系轻通勤型",
            style_tags_secondary=[],
            product_form="抓夹",
            element_tags=["布艺"],
            value_points=["提升精致度"],
            scene_tags=["出门"],
            reason_short="测试",
        ),
        heat_score=80.0,
        heat_level="high",
        crowd_score=40.0,
        crowd_level="medium",
        priority_level="high",
        target_price_band="0-50",
    )


def build_card(product_id: str, product_name: str) -> MarketDirectionCard:
    return MarketDirectionCard(
        direction_canonical_key="VN__hair_accessory__韩系轻通勤型",
        direction_instance_id="2026-04-21__VN__hair_accessory__韩系轻通勤型",
        batch_date="2026-04-21",
        country="VN",
        category="hair_accessory",
        direction_name="韩系轻通勤型",
        style_cluster="韩系轻通勤型",
        top_forms=["抓夹"],
        form_distribution={"抓夹": 100.0},
        core_elements=["布艺"],
        scene_tags=["出门"],
        target_price_bands=["0-50"],
        heat_level="high",
        crowd_level="medium",
        top_value_points=["提升精致度"],
        priority_level="high",
        representative_products=[{"product_id": product_id, "product_name": product_name}],
        selection_advice="优先补韩系轻通勤。",
        avoid_notes="不要只补相似外观。",
        confidence=0.82,
        product_count=1,
        average_heat_score=80.0,
        average_crowd_score=40.0,
        direction_key="VN__hair_accessory__韩系轻通勤型",
    )


class MarketInsightWriterTest(unittest.TestCase):
    def test_writer_updates_progress_and_latest_index_incrementally(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MarketInsightWriter(Path(tmpdir) / "artifacts")
            config = MarketInsightConfig(table_id="table_1", table_name="测试表", enabled=True)
            first_snapshot = build_snapshot("p1")
            state = writer.start_product_run(
                config=config,
                input_mode="product_ranking",
                first_snapshot=first_snapshot,
                total_product_count=2,
                voc_summary=VOCLightSummary(voc_status="skipped"),
            )

            latest_index_path = Path(tmpdir) / "artifacts" / "latest" / "VN__hair_accessory.json"
            latest_payload = json.loads(latest_index_path.read_text(encoding="utf-8"))
            self.assertEqual(latest_payload["run_status"], "running")
            self.assertEqual(latest_payload["completed_product_count"], 0)
            self.assertEqual(latest_payload["total_product_count"], 2)
            self.assertEqual(latest_payload["source_scope"], "official")
            self.assertFalse(latest_payload["is_consumable"])

            scored = [build_scored(first_snapshot)]
            result = writer.update_product_run(
                config=config,
                run_state=state,
                snapshots=[first_snapshot],
                scored_items=scored,
                direction_cards=[build_card("p1", "商品p1")],
                report_payload={
                    "decision_summary": {
                        "enter": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                        "watch": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                        "avoid": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                    }
                },
                report_markdown="# partial\n",
                completed_product_count=1,
                total_product_count=2,
                run_status="running",
                report_delivery={"status": "skipped", "notification_status": "skipped"},
                llm_fallback_count=0,
            )

            self.assertEqual(result.product_snapshot_count, 1)
            self.assertEqual(result.total_product_count, 2)
            self.assertEqual(result.run_status, "running")

            latest_payload = json.loads(latest_index_path.read_text(encoding="utf-8"))
            progress_payload = json.loads(Path(state.progress_json_path).read_text(encoding="utf-8"))
            self.assertEqual(latest_payload["completed_product_count"], 1)
            self.assertEqual(latest_payload["run_status"], "running")
            self.assertEqual(latest_payload["source_scope"], "official")
            self.assertFalse(latest_payload["is_consumable"])
            self.assertEqual(progress_payload["direction_count"], 1)
            self.assertEqual(progress_payload["source_scope"], "official")
            self.assertFalse(progress_payload["is_consumable"])
            self.assertEqual(progress_payload["valid_sample_count"], 1)
            self.assertEqual(progress_payload["invalid_sample_count"], 0)
            self.assertEqual(progress_payload["quality_gate_passed"], True)
            self.assertTrue(str(latest_payload["database_path"]).endswith("market_insight.db"))
            self.assertEqual(json.loads(Path(state.report_delivery_path).read_text(encoding="utf-8"))["status"], "skipped")

            conn = sqlite3.connect(str(Path(tmpdir) / "artifacts" / "market_insight.db"))
            try:
                run_count = conn.execute("SELECT COUNT(*) FROM market_insight_runs").fetchone()[0]
                snapshot_count = conn.execute("SELECT COUNT(*) FROM market_insight_product_snapshots").fetchone()[0]
                tag_count = conn.execute("SELECT COUNT(*) FROM market_insight_product_tags").fetchone()[0]
                card_count = conn.execute("SELECT COUNT(*) FROM market_direction_cards").fetchone()[0]
                llm_fallback_count = conn.execute("SELECT llm_fallback_count FROM market_insight_runs").fetchone()[0]
                source_scope = conn.execute("SELECT source_scope FROM market_insight_runs").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(run_count, 1)
            self.assertEqual(snapshot_count, 1)
            self.assertEqual(tag_count, 1)
            self.assertEqual(card_count, 1)
            self.assertEqual(llm_fallback_count, 0)
            self.assertEqual(source_scope, "official")

    def test_writer_persists_direction_execution_brief_to_cards_and_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MarketInsightWriter(Path(tmpdir) / "artifacts")
            config = MarketInsightConfig(table_id="table_1", table_name="测试表", enabled=True)
            snapshot = build_snapshot("p1")
            state = writer.start_product_run(
                config=config,
                input_mode="product_ranking",
                first_snapshot=snapshot,
                total_product_count=1,
                voc_summary=VOCLightSummary(voc_status="skipped"),
            )
            brief = {
                "direction_id": "VN__hair_accessory__韩系轻通勤型",
                "direction_name": "韩系轻通勤型",
                "direction_action": "prioritize_low_cost_test",
                "task_type": "low_cost_test",
                "target_pool": "test_product_pool",
                "brief_source": "generated",
                "brief_confidence": "high",
                "product_selection_requirements": ["近90天新品优先"],
            }
            writer.update_product_run(
                config=config,
                run_state=state,
                snapshots=[snapshot],
                scored_items=[build_scored(snapshot)],
                direction_cards=[build_card("p1", "商品p1")],
                report_payload={
                    "direction_actions": [
                        {
                            "direction": "韩系轻通勤型",
                            "direction_execution_brief": brief,
                        }
                    ],
                    "direction_execution_briefs": [brief],
                    "direction_decision_cards": [
                        {
                            "direction_canonical_key": "VN__hair_accessory__韩系轻通勤型",
                            "direction_name": "韩系轻通勤型",
                        }
                    ],
                },
                report_markdown="# completed\n",
                completed_product_count=1,
                total_product_count=1,
                run_status="completed",
                report_delivery={"status": "skipped", "notification_status": "skipped"},
                llm_fallback_count=0,
            )

            cards = json.loads(Path(state.direction_cards_path).read_text(encoding="utf-8"))
            self.assertEqual(cards[0]["direction_execution_brief"]["task_type"], "low_cost_test")
            report_payload = json.loads(Path(state.report_json_path).read_text(encoding="utf-8"))
            self.assertEqual(report_payload["direction_decision_cards"][0]["direction_execution_brief"]["target_pool"], "test_product_pool")

            conn = sqlite3.connect(str(Path(tmpdir) / "artifacts" / "market_insight.db"))
            try:
                raw_brief = conn.execute("SELECT direction_execution_brief_json FROM market_direction_cards").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(json.loads(raw_brief)["brief_source"], "generated")

    def test_resume_prefers_furthest_progress_run_over_newer_empty_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MarketInsightWriter(Path(tmpdir) / "artifacts")
            config = MarketInsightConfig(table_id="table_1", table_name="测试表", enabled=True)
            country = "VN"
            category = "hair_accessory"
            batch_date = "2026-04-21"

            bucket = Path(tmpdir) / "artifacts" / f"{country}__{category}"
            older = bucket / "20260421__table_1__101010"
            newer = bucket / "20260421__table_1__121212"
            older.mkdir(parents=True, exist_ok=True)
            newer.mkdir(parents=True, exist_ok=True)

            (older / "market_insight_progress.json").write_text(
                json.dumps(
                    {
                        "run_status": "running",
                        "source_scope": "official",
                        "completed_product_count": 156,
                        "updated_at_epoch": 100,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (newer / "market_insight_progress.json").write_text(
                json.dumps(
                    {
                        "run_status": "running",
                        "source_scope": "official",
                        "completed_product_count": 0,
                        "updated_at_epoch": 200,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            resumable = writer._find_resumable_run_dir(
                config=config,
                country=country,
                category=category,
                batch_date=batch_date,
                source_scope="official",
            )

            self.assertEqual(resumable, older)

    def test_completed_run_is_not_consumable_when_valid_sample_ratio_is_too_low(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MarketInsightWriter(Path(tmpdir) / "artifacts")
            config = MarketInsightConfig(
                table_id="table_1",
                table_name="测试表",
                enabled=True,
                min_consumable_product_count=2,
                min_consumable_direction_count=1,
                min_report_valid_sample_ratio=0.70,
            )
            first_snapshot = build_snapshot("p1")
            second_snapshot = build_snapshot("p2")
            state = writer.start_product_run(
                config=config,
                input_mode="product_ranking",
                first_snapshot=first_snapshot,
                total_product_count=2,
                voc_summary=VOCLightSummary(voc_status="skipped"),
            )

            result = writer.update_product_run(
                config=config,
                run_state=state,
                snapshots=[first_snapshot, second_snapshot],
                scored_items=[build_scored(first_snapshot, True), build_scored(second_snapshot, False)],
                direction_cards=[build_card("p1", "商品p1")],
                report_payload={"decision_summary": {}},
                report_markdown="# done\n",
                completed_product_count=2,
                total_product_count=2,
                run_status="completed",
                report_delivery={"status": "blocked", "notification_status": "skipped"},
                llm_fallback_count=0,
            )

            latest_payload = json.loads((Path(tmpdir) / "artifacts" / "latest" / "VN__hair_accessory.json").read_text(encoding="utf-8"))
            progress_payload = json.loads(Path(state.progress_json_path).read_text(encoding="utf-8"))
            self.assertFalse(result.is_consumable)
            self.assertFalse(latest_payload["is_consumable"])
            self.assertFalse(progress_payload["is_consumable"])
            self.assertEqual(latest_payload["valid_sample_count"], 1)
            self.assertEqual(latest_payload["invalid_sample_count"], 1)
            self.assertAlmostEqual(latest_payload["valid_sample_ratio"], 0.5)
            self.assertFalse(latest_payload["quality_gate_passed"])
            self.assertIn("有效样本率", latest_payload["quality_gate_reason"])


if __name__ == "__main__":
    unittest.main()
