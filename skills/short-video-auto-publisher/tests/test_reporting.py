#!/usr/bin/env python3
"""发布追踪表回写测试。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
import sys


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.db import AutoPublishDB
from app.models import AccountConfig, ScriptMetadata
from app.reporting import REPORT_FIELDS, ensure_report_fields, sync_publish_report_table


class DummyField:
    def __init__(self, field_name: str) -> None:
        self.field_name = field_name


class DummyRecord:
    def __init__(self, record_id: str, fields: dict) -> None:
        self.record_id = record_id
        self.fields = fields


class DummyClient:
    def __init__(self, fields=None, records=None) -> None:
        self._fields = list(fields or [])
        self._records = list(records or [])
        self.created_fields = []
        self.created_records = []
        self.updated_records = []

    def list_fields(self):
        return [DummyField(name) for name in self._fields]

    def create_field(self, field_name: str, field_type: int = 1, ui_type: str = "Text", property=None):
        self._fields.append(field_name)
        self.created_fields.append((field_name, field_type, ui_type, property))
        return {"field_name": field_name}

    def list_records(self, page_size: int = 500):
        return list(self._records)

    def batch_create_records(self, records):
        self.created_records.extend(records)

    def batch_update_records(self, records):
        self.updated_records.extend(records)


class ReportingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = AutoPublishDB(Path(self.temp_dir.name) / "autopublish.sqlite3")
        self.db.upsert_account_configs(
            [
                AccountConfig(
                    account_id="acc-1",
                    account_name="账号1",
                    store_id="SHOP-01",
                    account_status="可用",
                    publish_time_1="12:00",
                    publish_time_2="",
                    publish_time_3="",
                )
            ]
        )
        self.db.upsert_script_metadata(
            [
                ScriptMetadata(
                    script_id="001_M1_M",
                    source_record_id="rec-001",
                    script_slot="S1",
                    task_no="001",
                    store_id="SHOP-01",
                    product_id="P1001",
                    parent_slot="M1",
                    direction_label="日常轻分享流",
                    variant_strength="母版",
                    target_country="Thailand",
                    product_type="耳环",
                    content_family_key="P1001_M1",
                    script_text="script",
                    short_video_title="title-001",
                    title_source="test",
                )
            ]
        )
        self.db.upsert_video_asset(
            script_id="001_M1_M",
            run_manager_record_id="run-001",
            video_source_type="link",
            video_source_value="https://example.com/video.mp4",
            local_file_path="/tmp/001_M1_M.mp4",
            download_status="下载成功",
            run_video_status="已提交",
            publish_status="已排期",
        )
        self.db.generate_future_slots(datetime(2026, 4, 13, 10, 0, 0), 24)
        slot = self.db.list_pending_slots(datetime(2026, 4, 13, 10, 0, 0), 24)[0]
        self.db.assign_slot(
            slot_id=int(slot["slot_id"]),
            script_id="001_M1_M",
            publish_task_id="task-001",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=datetime(2026, 4, 13, 12, 0, 0),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_ensure_report_fields_creates_missing_fields(self) -> None:
        client = DummyClient(fields=["脚本ID"])
        stats = ensure_report_fields(client)
        self.assertEqual(stats["created_fields"], len(REPORT_FIELDS) - 1)
        self.assertIn("发布状态", [item[0] for item in client.created_fields])

    def test_sync_publish_report_table_updates_existing_and_creates_missing(self) -> None:
        existing = DummyRecord("rpt-1", {"脚本ID": "001_M1_M"})
        client = DummyClient(fields=[spec["name"] for spec in REPORT_FIELDS], records=[existing])
        stats = sync_publish_report_table(self.db, client)

        self.assertEqual(stats["report_rows"], 1)
        self.assertEqual(stats["updated_records"], 1)
        self.assertEqual(stats["created_records"], 0)
        self.assertEqual(client.updated_records[0]["record_id"], "rpt-1")
        self.assertEqual(client.updated_records[0]["fields"]["内部脚本键"], "rec-001:S1")
        self.assertEqual(client.updated_records[0]["fields"]["发布任务ID"], "task-001")


if __name__ == "__main__":
    unittest.main()
