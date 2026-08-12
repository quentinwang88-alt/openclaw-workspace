from pathlib import Path
import unittest

from miaoshou_auto_listing.config import load_config
from miaoshou_auto_listing.services.selection_workbench_bridge import (
    QUEUE_TRACKING_FIELDS,
    SelectionListingBridge,
)


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


class FakeTable:
    def __init__(self, records=None):
        self.records = list(records or [])
        self.created = []
        self.updated = []

    def list_records(self, *, all_records=False):
        return list(self.records)

    def create_record(self, fields):
        record_id = f"rec-created-{len(self.created) + 1}"
        self.created.append((record_id, fields))
        self.records.append({"record_id": record_id, "fields": dict(fields)})
        return record_id

    def update_record(self, record_id, fields):
        self.updated.append((record_id, fields))
        for record in self.records:
            if record["record_id"] == record_id:
                record["fields"].update(fields)
                break


class SelectionWorkbenchBridgeTest(unittest.TestCase):
    def setUp(self):
        self.config = load_config(CONFIG_DIR)

    def workbench_record(self, **overrides):
        fields = {
            "商品快照ID": "123__20260812__TH",
            "市场": "TH",
            "是否测品": True,
            "找货状态": "已找到",
            "推荐货源链接": {
                "text": "1688货源",
                "link": "https://detail.1688.com/offer/975683523984.html?offerId=975683523984",
            },
            "采购价人民币": 20.9,
            "目标店铺": "LikeU shop",
            "定价方式": "固定售价",
            "定价": 45,
            "每SKU库存": 100,
            "确认上架": True,
        }
        fields.update(overrides)
        return {"record_id": "sel001", "fields": fields}

    def test_sync_creates_one_idempotent_queue_task_and_links_source(self):
        workbench = FakeTable([self.workbench_record()])
        queue = FakeTable()
        report = SelectionListingBridge(self.config, workbench, queue).sync(
            dry_run=False
        )
        self.assertEqual(report.created, ["rec-created-1"])
        queue_fields = queue.created[0][1]
        self.assertEqual(queue_fields["任务来源"], "选品工作台")
        self.assertEqual(queue_fields["选品记录ID"], "sel001")
        self.assertEqual(queue_fields["执行状态"], "待执行")
        self.assertEqual(queue_fields["采购链接"], "https://detail.1688.com/offer/975683523984.html")
        self.assertTrue(queue_fields["上架唯一键"])
        source_updates = workbench.updated[-1][1]
        self.assertEqual(source_updates["上架任务ID"], "rec-created-1")
        self.assertEqual(source_updates["上架状态"], "待执行")
        self.assertFalse(source_updates["确认上架"])

        second = SelectionListingBridge(self.config, workbench, queue).sync(
            dry_run=False
        )
        self.assertFalse(second.created)
        self.assertEqual(second.reconciled, ["sel001"])

    def test_dry_run_never_writes(self):
        workbench = FakeTable([self.workbench_record()])
        queue = FakeTable()
        report = SelectionListingBridge(self.config, workbench, queue).sync(
            dry_run=True
        )
        self.assertEqual(len(report.created), 1)
        self.assertFalse(workbench.updated)
        self.assertFalse(queue.created)

    def test_confirm_listing_overrides_lookup_status_after_manual_review(self):
        workbench = FakeTable(
            [self.workbench_record(**{"找货状态": "需人工确认"})]
        )
        queue = FakeTable()

        report = SelectionListingBridge(self.config, workbench, queue).sync(
            dry_run=False
        )

        self.assertEqual(report.created, ["rec-created-1"])
        self.assertEqual(queue.created[0][1]["执行状态"], "待执行")
        self.assertEqual(workbench.updated[-1][1]["上架状态"], "待执行")

    def test_unconfirmed_row_still_uses_lookup_status_for_progress(self):
        workbench = FakeTable(
            [
                self.workbench_record(
                    **{"找货状态": "需人工确认", "确认上架": False}
                )
            ]
        )
        queue = FakeTable()

        report = SelectionListingBridge(self.config, workbench, queue).sync(
            dry_run=False
        )

        self.assertFalse(report.created)
        self.assertEqual(workbench.updated[-1][1]["上架状态"], "待找货")

    def test_market_shop_mismatch_is_blocked_before_queue_creation(self):
        workbench = FakeTable(
            [self.workbench_record(**{"市场": "VN", "目标店铺": "LikeU shop"})]
        )
        queue = FakeTable()
        report = SelectionListingBridge(self.config, workbench, queue).sync(
            dry_run=False
        )
        self.assertIn("sel001", report.blocked)
        self.assertIn("不一致", report.blocked["sel001"])
        self.assertFalse(queue.created)
        self.assertEqual(workbench.updated[-1][1]["上架状态"], "异常")

    def test_linked_success_is_reconciled_without_creating_duplicate(self):
        queue_record = {
            "record_id": "queue001",
            "fields": {
                "执行状态": "成功",
                "执行结果": "发布成功",
                "TikTok产品ID": "tt001",
                QUEUE_TRACKING_FIELDS["unique_key"]: "key001",
                QUEUE_TRACKING_FIELDS["fingerprint"]: "fp001",
            },
        }
        workbench = FakeTable(
            [self.workbench_record(**{"上架任务ID": "queue001", "确认上架": False})]
        )
        queue = FakeTable([queue_record])
        report = SelectionListingBridge(self.config, workbench, queue).sync(
            dry_run=False
        )
        self.assertEqual(report.reconciled, ["sel001"])
        self.assertFalse(queue.created)
        updates = workbench.updated[-1][1]
        self.assertEqual(updates["上架状态"], "成功")
        self.assertEqual(updates["TikTok产品ID"], "tt001")

    def test_pending_linked_task_accepts_price_change_without_duplication(self):
        initial_workbench = FakeTable([self.workbench_record()])
        queue = FakeTable()
        SelectionListingBridge(self.config, initial_workbench, queue).sync(
            dry_run=False
        )
        source = initial_workbench.records[0]
        source["fields"]["定价"] = 49
        source["fields"]["确认上架"] = True

        report = SelectionListingBridge(self.config, initial_workbench, queue).sync(
            dry_run=False
        )

        self.assertFalse(report.created)
        self.assertEqual(report.updated, ["rec-created-1"])
        self.assertEqual(queue.records[0]["fields"]["定价"], 49.0)
        self.assertFalse(initial_workbench.records[0]["fields"]["确认上架"])


if __name__ == "__main__":
    unittest.main()
