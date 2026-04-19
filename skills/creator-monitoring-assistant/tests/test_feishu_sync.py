import tempfile
import unittest
from pathlib import Path

import pandas as pd

from app.db import Database
from app.services.clean_loader import build_clean_records, sync_creator_master
from app.services.feishu_sync import build_feishu_current_action_payload, sync_current_action_table_to_feishu
from app.services.metrics_calculator import build_weekly_metrics
from app.services.raw_loader import load_raw_excel
from app.services.tag_engine import run_tag_engine
from app.services.threshold_calculator import calculate_market_thresholds


class FakeFeishuClient:
    def __init__(self):
        self.records = []
        self.created_batches = []
        self.updated_batches = []
        self.deleted_record_ids = []

    def list_fields(self):
        return [{"field_name": name, "field_id": name} for name in [
            "record_key", "达人名称", "国家", "店铺", "负责人", "当前统计周", "本周GMV", "上周GMV", "GMV环比",
            "本周内容动作数", "上周内容动作数", "动作数环比", "本周单动作GMV", "单动作GMV环比",
            "本周退款率", "退款率变化", "近4周GMV", "当前主标签", "当前风险标签", "优先级",
            "核心原因", "本周建议动作", "跟进状态", "人工备注", "最近更新时间"
        ]]

    def list_all_records(self):
        return self.records

    def batch_create_records(self, records):
        self.created_batches.append(records)
        for index, item in enumerate(records, start=1):
            self.records.append(
                {
                    "record_id": f"new_{len(self.records) + index}",
                    "fields": dict(item["fields"]),
                }
            )

    def batch_update_records(self, records):
        self.updated_batches.append(records)
        for item in records:
            self.update_record(item["record_id"], item["fields"])

    def update_record(self, record_id, fields):
        for item in self.records:
            if item["record_id"] == record_id:
                merged = dict(item["fields"])
                merged.update(fields)
                item["fields"] = merged
                return
        raise KeyError(record_id)

    def delete_record(self, record_id):
        self.deleted_record_ids.append(record_id)
        self.records = [item for item in self.records if item["record_id"] != record_id]


def _run_week(database: Database, stat_week: str, excel_path: Path, store: str) -> None:
    batch_id = load_raw_excel(stat_week, str(excel_path), "tiktok", "th", store=store, db=database)
    sync_creator_master(batch_id, db=database)
    build_clean_records(stat_week, batch_id, store=store, db=database)
    build_weekly_metrics(stat_week, store=store, db=database)
    thresholds = calculate_market_thresholds(stat_week, store=store, db=database)
    run_tag_engine(stat_week, thresholds, store=store, db=database)


class FeishuSyncTest(unittest.TestCase):
    def test_build_current_action_payload_updates_existing_creator_row(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.sqlite3"
            database = Database(f"sqlite:///{db_path}")
            database.init_schema()

            week_12_path = Path(temp_dir) / "week_12.xlsx"
            pd.DataFrame(
                [
                    {
                        "达人名称": "Alice",
                        "联盟归因 GMV": 500,
                        "退款金额": 10,
                        "归因订单数": 5,
                        "联盟归因成交件数": 6,
                        "已退款的商品件数": 1,
                        "平均订单金额": 100,
                        "日均商品成交件数": 1,
                        "视频数": 1,
                        "直播数": 0,
                        "预计佣金": 50,
                        "已发货样品数": 1,
                    }
                ]
            ).to_excel(week_12_path, index=False)

            week_13_path = Path(temp_dir) / "week_13.xlsx"
            pd.DataFrame(
                [
                    {
                        "达人名称": "Alice",
                        "联盟归因 GMV": 1000,
                        "退款金额": 20,
                        "归因订单数": 10,
                        "联盟归因成交件数": 12,
                        "已退款的商品件数": 1,
                        "平均订单金额": 100,
                        "日均商品成交件数": 2,
                        "视频数": 2,
                        "直播数": 1,
                        "预计佣金": 100,
                        "已发货样品数": 2,
                    }
                ]
            ).to_excel(week_13_path, index=False)

            _run_week(database, "2026-W12", week_12_path, "泰国服装1店")
            _run_week(database, "2026-W13", week_13_path, "泰国服装1店")

            client = FakeFeishuClient()
            client.records = [
                {
                    "record_id": "rec_1",
                    "fields": {
                        "record_key": "tiktok:th:泰国服装1店:alice",
                        "当前统计周": "2026-W12",
                        "负责人": "Owner A",
                        "跟进状态": "pending",
                        "人工备注": "keep",
                    },
                }
            ]

            create_records, update_records = build_feishu_current_action_payload(
                "2026-W13",
                store="泰国服装1店",
                db=database,
                client=client,
            )
            self.assertEqual(len(create_records), 0)
            self.assertEqual(len(update_records), 1)
            fields = update_records[0]["fields"]
            self.assertEqual(fields["record_key"], "tiktok:th:泰国服装1店:alice")
            self.assertEqual(fields["店铺"], "泰国服装1店")
            self.assertEqual(fields["当前统计周"], "2026-W13")
            self.assertEqual(fields["上周GMV"], 500.0)
            self.assertEqual(fields["GMV环比"], 100.0)
            self.assertEqual(fields["本周退款率"], 2.0)
            self.assertEqual(fields["退款率变化"], 0.0)
            self.assertTrue("负责人" not in fields)

    def test_store_partition_does_not_overwrite_other_store(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.sqlite3"
            database = Database(f"sqlite:///{db_path}")
            database.init_schema()

            store_one_path = Path(temp_dir) / "store_one.xlsx"
            pd.DataFrame(
                [
                    {
                        "达人名称": "Alice",
                        "联盟归因 GMV": 1000,
                        "退款金额": 20,
                        "归因订单数": 10,
                        "联盟归因成交件数": 12,
                        "已退款的商品件数": 1,
                        "平均订单金额": 100,
                        "日均商品成交件数": 2,
                        "视频数": 2,
                        "直播数": 1,
                        "预计佣金": 100,
                        "已发货样品数": 2,
                    }
                ]
            ).to_excel(store_one_path, index=False)

            store_two_path = Path(temp_dir) / "store_two.xlsx"
            pd.DataFrame(
                [
                    {
                        "达人名称": "Bella",
                        "联盟归因 GMV": 800,
                        "退款金额": 10,
                        "归因订单数": 8,
                        "联盟归因成交件数": 10,
                        "已退款的商品件数": 1,
                        "平均订单金额": 100,
                        "日均商品成交件数": 2,
                        "视频数": 1,
                        "直播数": 1,
                        "预计佣金": 80,
                        "已发货样品数": 1,
                    }
                ]
            ).to_excel(store_two_path, index=False)

            _run_week(database, "2026-W13", store_one_path, "泰国服装1店")
            _run_week(database, "2026-W13", store_two_path, "泰国配饰1店")

            rows = database.fetchall(
                """
                SELECT record_key, store
                FROM creator_monitoring_result
                WHERE stat_week = :stat_week
                """,
                {"stat_week": "2026-W13"},
            )
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["store"] for row in rows}, {"泰国服装1店", "泰国配饰1店"})

    def test_sync_keeps_stale_rows_in_feishu(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.sqlite3"
            database = Database(f"sqlite:///{db_path}")
            database.init_schema()

            excel_path = Path(temp_dir) / "sample.xlsx"
            pd.DataFrame(
                [
                    {
                        "达人名称": "Alice",
                        "联盟归因 GMV": 1000,
                        "退款金额": 20,
                        "归因订单数": 10,
                        "联盟归因成交件数": 12,
                        "已退款的商品件数": 1,
                        "平均订单金额": 100,
                        "日均商品成交件数": 2,
                        "视频数": 2,
                        "直播数": 1,
                        "预计佣金": 100,
                        "已发货样品数": 2,
                    }
                ]
            ).to_excel(excel_path, index=False)

            _run_week(database, "2026-W13", excel_path, "泰国服装1店")

            client = FakeFeishuClient()
            client.records = [
                {
                    "record_id": "stale_1",
                    "fields": {
                        "record_key": "tiktok:th:泰国服装1店:stale_creator",
                        "当前统计周": "2026-W12",
                        "负责人": "Owner B",
                    },
                }
            ]

            summary = sync_current_action_table_to_feishu(
                "2026-W13",
                store="泰国服装1店",
                db=database,
                client=client,
            )

            self.assertEqual(summary["created"], 1)
            self.assertEqual(summary["updated"], 0)
            self.assertEqual(summary["deleted"], 0)
            self.assertEqual(client.deleted_record_ids, [])
            self.assertEqual(len(client.records), 2)


if __name__ == "__main__":
    unittest.main()
