import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.standardized_snapshot import (  # noqa: E402
    READY_STATUS,
    load_standard_product_snapshots_from_feishu,
    snapshot_from_fields,
)


class FakeRecord(object):
    def __init__(self, record_id, fields):
        self.record_id = record_id
        self.fields = fields


class FakeClient(object):
    def __init__(self, records):
        self.records = records

    def list_records(self, limit=None):
        return self.records[:limit] if limit else list(self.records)


class StandardizedSnapshotTest(unittest.TestCase):
    def test_snapshot_accepts_ready_valid_rows(self):
        snapshot = snapshot_from_fields(
            {
                "crawl_batch_id": "batch_1",
                "product_snapshot_id": "snap_1",
                "product_id": "p1",
                "market_id": "my",
                "category_id": "earrings",
                "title": "Gift earrings",
                "data_status": READY_STATUS,
                "is_valid": True,
                "sales_7d": "123",
                "product_age_days": "45",
            },
            source_record_id="rec1",
        )

        self.assertTrue(snapshot.ready_for_agents)
        self.assertEqual(snapshot.market_id, "MY")
        self.assertEqual(snapshot.sales_7d, 123.0)
        self.assertEqual(snapshot.product_age_days, 45)

    def test_loader_skips_not_ready_rows(self):
        client = FakeClient(
            [
                FakeRecord(
                    "rec_ready",
                    {
                        "crawl_batch_id": "batch_1",
                        "product_id": "p1",
                        "market_id": "VN",
                        "category_id": "hair_accessory",
                        "title": "clip",
                        "data_status": READY_STATUS,
                        "is_valid": True,
                    },
                ),
                FakeRecord(
                    "rec_pending",
                    {
                        "crawl_batch_id": "batch_1",
                        "product_id": "p2",
                        "market_id": "VN",
                        "category_id": "hair_accessory",
                        "title": "clip",
                        "data_status": "pending",
                        "is_valid": True,
                    },
                ),
            ]
        )

        result = load_standard_product_snapshots_from_feishu(client)

        self.assertEqual(len(result.snapshots), 1)
        self.assertEqual(result.skipped[0]["reason"], "data_status_not_ready")


if __name__ == "__main__":
    unittest.main()
