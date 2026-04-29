import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agent_data_store import MARKET_AGENT, SELECTION_AGENT, AgentDataStore  # noqa: E402
from src.standardized_snapshot import READY_STATUS, snapshot_from_fields  # noqa: E402


def build_snapshot(product_id="p1"):
    return snapshot_from_fields(
        {
            "crawl_batch_id": "batch_1",
            "product_snapshot_id": "snap_" + product_id,
            "product_id": product_id,
            "market_id": "VN",
            "category_id": "hair_accessory",
            "title": "通勤抓夹",
            "main_image_url": "https://example.com/a.jpg",
            "price_rmb": "12.5",
            "sales_7d": "300",
            "sales_30d": "900",
            "video_count": "10",
            "creator_count": "4",
            "product_age_days": "30",
            "fastmoss_url": "https://fastmoss.example/p",
            "data_status": READY_STATUS,
            "is_valid": True,
        }
    )


class AgentDataStoreTest(unittest.TestCase):
    def test_market_and_selection_import_are_isolated(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AgentDataStore(Path(tmp) / "agents.sqlite3")
            snapshot = build_snapshot()

            store.import_snapshots(MARKET_AGENT, [snapshot], feishu_table_id="tbl1")
            store.import_snapshots(SELECTION_AGENT, [snapshot], feishu_table_id="tbl1")

            with sqlite3.connect(str(store.db_path)) as conn:
                market_count = conn.execute("SELECT COUNT(*) FROM market_raw_product_snapshot").fetchone()[0]
                selection_count = conn.execute("SELECT COUNT(*) FROM selection_raw_product_snapshot").fetchone()[0]
                log_count = conn.execute("SELECT COUNT(*) FROM agent_import_log").fetchone()[0]

            self.assertEqual(market_count, 1)
            self.assertEqual(selection_count, 1)
            self.assertEqual(log_count, 2)

    def test_brief_persists_and_fallback_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AgentDataStore(Path(tmp) / "agents.sqlite3")
            store.save_direction_execution_briefs(
                report_id="r1",
                crawl_batch_id="batch_1",
                market_id="VN",
                category_id="hair_accessory",
                briefs=[
                    {
                        "direction_id": "VN__hair_accessory__commute",
                        "direction_name": "基础通勤型",
                        "direction_action": "prioritize_low_cost_test",
                        "task_type": "low_cost_test",
                        "target_pool": "test_product_pool",
                        "positive_signals": ["通勤"],
                    }
                ],
            )

            brief = store.load_latest_direction_execution_brief(
                market_id="VN",
                category_id="hair_accessory",
                direction_id="VN__hair_accessory__commute",
                crawl_batch_id="batch_1",
            )
            fallback = store.load_latest_direction_execution_brief(
                market_id="VN",
                category_id="hair_accessory",
                direction_id="missing",
                direction_action="observe",
                direction_name="缺失方向",
            )

            self.assertEqual(brief["task_type"], "low_cost_test")
            self.assertEqual(brief["positive_signals"], ["通勤"])
            self.assertEqual(fallback["brief_source"], "auto_fallback")
            self.assertEqual(fallback["target_pool"], "observe_pool")

    def test_run_log_and_lock_support_weekly_orchestration(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AgentDataStore(Path(tmp) / "agents.sqlite3")

            run_id = store.record_agent_run(
                agent_name=SELECTION_AGENT,
                crawl_batch_id="batch_1",
                market_id="VN",
                category_id="hair_accessory",
                status="success",
                batch_data_hash="hash_1",
                processed_count=10,
            )
            record = store.get_agent_run(SELECTION_AGENT, "batch_1", "VN", "hair_accessory")
            lock = store.acquire_run_lock(SELECTION_AGENT, "batch_1", "VN", "hair_accessory")
            second_lock = store.acquire_run_lock(SELECTION_AGENT, "batch_1", "VN", "hair_accessory")
            store.release_run_lock(lock["lock_key"])
            third_lock = store.acquire_run_lock(SELECTION_AGENT, "batch_1", "VN", "hair_accessory")

            self.assertTrue(run_id.startswith(SELECTION_AGENT))
            self.assertEqual(record["status"], "success")
            self.assertEqual(record["batch_data_hash"], "hash_1")
            self.assertTrue(lock["acquired"])
            self.assertFalse(second_lock["acquired"])
            self.assertTrue(third_lock["acquired"])

    def test_latest_consumable_report_and_ready_brief_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AgentDataStore(Path(tmp) / "agents.sqlite3")
            store.save_market_direction_report(
                report_id="r1",
                crawl_batch_id="batch_1",
                market_id="VN",
                category_id="hair_accessory",
                report_version="v1",
                report_date="2026-04-28",
                sample_count=1,
                valid_sample_count=1,
                direction_count=1,
                report_status="consumable",
                business_summary_markdown="summary",
                full_report_markdown="full",
                structured_json={},
            )
            store.save_direction_execution_briefs(
                report_id="r1",
                crawl_batch_id="batch_1",
                market_id="VN",
                category_id="hair_accessory",
                briefs=[
                    {
                        "direction_id": "VN__hair_accessory__gift",
                        "direction_name": "少女礼物感型",
                        "direction_action": "prioritize_low_cost_test",
                        "task_type": "low_cost_test",
                        "target_pool": "test_product_pool",
                    }
                ],
            )

            report = store.latest_consumable_market_report("VN", "hair_accessory", "batch_1")
            count = store.count_ready_briefs("VN", "hair_accessory", "batch_1")

            self.assertEqual(report["report_id"], "r1")
            self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
