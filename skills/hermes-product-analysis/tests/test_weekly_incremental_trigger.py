import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from selection.weekly_incremental_trigger import (  # noqa: E402
    BatchKey,
    check_market_brief_ready,
    check_selection_increment,
    compute_batch_data_hash,
    group_ready_snapshots_by_batch,
    plan_selection_runs,
)
from src.agent_data_store import SELECTION_AGENT, AgentDataStore  # noqa: E402
from src.standardized_snapshot import READY_STATUS, snapshot_from_fields  # noqa: E402


def snapshot(product_id="p1", batch="batch_1"):
    return snapshot_from_fields(
        {
            "crawl_batch_id": batch,
            "product_snapshot_id": "snap_" + product_id,
            "product_id": product_id,
            "market_id": "MY",
            "category_id": "earrings",
            "title": "gift earrings",
            "price_rmb": "12",
            "sales_7d": "100",
            "product_age_days": "20",
            "data_status": READY_STATUS,
            "is_valid": True,
        }
    )


class WeeklyIncrementalTriggerTest(unittest.TestCase):
    def test_group_and_hash_ready_snapshots(self):
        rows = [snapshot("p1"), snapshot("p2"), snapshot("p3", batch="batch_2")]
        grouped = group_ready_snapshots_by_batch(rows)
        batch_hash = compute_batch_data_hash(grouped[BatchKey("batch_1", "MY", "earrings")])

        self.assertEqual(len(grouped), 2)
        self.assertEqual(len(batch_hash), 64)

    def test_increment_skips_success_until_hash_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AgentDataStore(Path(tmp) / "agents.sqlite3")
            key = BatchKey("batch_1", "MY", "earrings")
            store.record_agent_run(
                SELECTION_AGENT,
                key.crawl_batch_id,
                key.market_id,
                key.category_id,
                status="success",
                batch_data_hash="hash_1",
            )

            skip = check_selection_increment(store, key, "hash_1")
            rerun = check_selection_increment(store, key, "hash_2")

            self.assertFalse(skip.should_run)
            self.assertTrue(rerun.should_run)
            self.assertEqual(rerun.rerun_reason, "standardized_data_changed")

    def test_brief_ready_waits_then_allows_previous_or_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AgentDataStore(Path(tmp) / "agents.sqlite3")
            key = BatchKey("batch_1", "MY", "earrings")
            waiting = check_market_brief_ready(store, key, retry_attempt=0)
            fallback = check_market_brief_ready(store, key, retry_attempt=3)

            self.assertFalse(waiting.ready)
            self.assertEqual(waiting.status, "waiting_for_market_brief")
            self.assertTrue(fallback.ready)
            self.assertEqual(fallback.status, "ready_with_fallback_brief")

    def test_plan_selection_runs_requires_increment_and_ready_brief(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AgentDataStore(Path(tmp) / "agents.sqlite3")
            store.save_market_direction_report(
                report_id="r1",
                crawl_batch_id="batch_1",
                market_id="MY",
                category_id="earrings",
                report_version="v",
                report_date="2026-04-28",
                sample_count=1,
                valid_sample_count=1,
                direction_count=1,
                report_status="consumable",
                business_summary_markdown="",
                full_report_markdown="",
                structured_json={},
            )
            store.save_direction_execution_briefs(
                "r1",
                "batch_1",
                "MY",
                "earrings",
                [{"direction_id": "MY__earrings__gift", "task_type": "low_cost_test", "target_pool": "test_product_pool"}],
            )
            plans = plan_selection_runs(store, [snapshot("p1")])

            self.assertTrue(plans[0]["trigger_selection_agent_run"])
            self.assertEqual(plans[0]["brief_readiness"]["status"], "ready")


if __name__ == "__main__":
    unittest.main()
