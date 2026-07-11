#!/usr/bin/env python3
"""主动人工发布入口测试。"""

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
from app.manual_publish import MANUAL_PUBLISH_FIELD_ALIASES, sync_manual_publish_requests
from app.models import AccountConfig
from app.scheduler import resolve_field_mapping


class DummyRecord:
    def __init__(self, record_id: str, fields: dict) -> None:
        self.record_id = record_id
        self.fields = fields


class DummyManualClient:
    def __init__(self) -> None:
        self.updates = []

    def download_attachment_bytes(self, attachment: dict):
        return b"manual-video", attachment.get("name") or "manual.mp4", "video/mp4", len(b"manual-video")

    def update_record_fields(self, record_id: str, fields: dict) -> None:
        self.updates.append({"record_id": record_id, "fields": dict(fields)})


class ChannelPublisher:
    def __init__(self) -> None:
        self.calls = []

    def create_scheduled_task_for_channel(self, **kwargs) -> str:
        self.calls.append(dict(kwargs))
        return f"task-{kwargs['channel']}-{kwargs['script_id']}"


class FlakyPublisher(ChannelPublisher):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    def create_scheduled_task_for_channel(self, **kwargs) -> str:
        self.calls.append(dict(kwargs))
        if len(self.calls) <= self.failures:
            raise RuntimeError("HTTPSConnectionPool: Max retries exceeded; ProxyError Cannot connect to proxy")
        return f"task-{kwargs['channel']}-{kwargs['script_id']}"


class ManualPublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = AutoPublishDB(Path(self.temp_dir.name) / "manual.sqlite3")
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
                    publish_channel="GeeLark",
                )
            ]
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _mapping(self):
        fields = ["店铺", "计划发布账号", "短视频上传", "短视频标题", "短视频发布时间", "发布日期", "发布渠道", "是否AI", "处理状态", "发布任务ID", "错误信息", "产品ID"]
        return resolve_field_mapping(fields, MANUAL_PUBLISH_FIELD_ALIASES)

    def _record(self, record_id: str = "rec-manual-1") -> DummyRecord:
        return DummyRecord(
            record_id,
            {
                "店铺": "SHOP-01",
                "计划发布账号": "账号1",
                "短视频上传": [{"file_token": "file-token-1", "name": "manual.mp4"}],
                "短视频标题": "Manual title",
                "短视频发布时间": "2026-04-14 12:00",
                "发布日期": "",
                "发布渠道": "GeeLark",
                "是否AI": "",
                "处理状态": "",
                "发布任务ID": "",
                "产品ID": "",
            },
        )

    def test_manual_neobund_request_without_product_keeps_non_shoppable_publish(self) -> None:
        record = self._record("rec-manual-neobund")
        record.fields["短视频发布时间"] = "2026-04-14 13:00"
        record.fields["发布渠道"] = "NeoBund"
        client = DummyManualClient()
        publisher = ChannelPublisher()

        stats = sync_manual_publish_requests(
            [record],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
        )

        self.assertEqual(stats["created"], 1)
        self.assertEqual(publisher.calls[0]["channel"], "NeoBund")
        self.assertEqual(publisher.calls[0]["product_id"], "")

    def test_manual_request_passes_selected_ai_marker_to_channel(self) -> None:
        record = self._record("rec-manual-ai")
        record.fields["是否AI"] = "否"
        client = DummyManualClient()
        publisher = ChannelPublisher()

        stats = sync_manual_publish_requests(
            [record],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
        )

        self.assertEqual(stats["created"], 1)
        self.assertIs(publisher.calls[0]["mark_ai"], False)

    def test_frame_rate_failed_manual_request_retries_after_rescheduling(self) -> None:
        record = self._record("rec-manual-frame-rate-retry")
        record.fields.update(
            {
                "短视频发布时间": "2099-04-14 12:00",
                "处理状态": "发布失败",
                "发布任务ID": "neobund:old-task",
                "错误信息": "frame_rate_check_failed",
                "发布渠道": "NeoBund",
            }
        )
        client = DummyManualClient()
        publisher = ChannelPublisher()

        stats = sync_manual_publish_requests(
            [record],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
        )

        self.assertEqual(stats["created"], 1)
        self.assertEqual(publisher.calls[0]["channel"], "NeoBund")
        self.assertEqual(client.updates[-1]["fields"]["处理状态"], "已创建")

    def test_manual_request_uses_selected_publish_date_with_time(self) -> None:
        record = self._record("rec-manual-date")
        record.fields["发布日期"] = 1781193600000
        record.fields["短视频发布时间"] = "23:50"
        client = DummyManualClient()
        publisher = ChannelPublisher()

        stats = sync_manual_publish_requests(
            [record],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
            now=datetime(2026, 6, 10, 9, 0, 0),
        )

        self.assertEqual(stats["created"], 1)
        self.assertEqual(publisher.calls[0]["publish_at"], datetime(2026, 6, 12, 23, 50, 0))

    def test_manual_request_rejects_expired_selected_publish_date(self) -> None:
        record = self._record("rec-manual-expired-date")
        record.fields["发布日期"] = "2026-06-10"
        record.fields["短视频发布时间"] = "08:50"
        client = DummyManualClient()
        publisher = ChannelPublisher()

        stats = sync_manual_publish_requests(
            [record],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
            now=datetime(2026, 6, 10, 9, 0, 0),
        )

        self.assertEqual(stats["validation_failed"], 1)
        self.assertEqual(len(publisher.calls), 0)
        self.assertIn("已过期", client.updates[-1]["fields"]["错误信息"])

    def test_manual_request_reuses_pending_auto_slot(self) -> None:
        self.db.generate_future_slots(datetime(2026, 4, 14, 11, 0, 0), window_hours=2)
        client = DummyManualClient()
        publisher = ChannelPublisher()

        stats = sync_manual_publish_requests(
            [self._record()],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
        )

        slot = self.db.get_slot_for_account_time(account_id="acc-1", scheduled_for="2026-04-14 12:00:00")
        asset = self.db.get_video_asset("manual:rec-manual-1")
        self.assertEqual(stats["created"], 1)
        self.assertEqual(len(publisher.calls), 1)
        self.assertEqual(publisher.calls[0]["channel"], "GeeLark")
        self.assertEqual(publisher.calls[0]["title"], "Manual title")
        self.assertEqual(slot["slot_source"], "manual")
        self.assertEqual(slot["manual_request_record_id"], "rec-manual-1")
        self.assertEqual(slot["publish_task_id"], "task-GeeLark-manual_rec-manual-1")
        self.assertEqual(asset["publish_status"], "已排期")
        self.assertEqual(client.updates[-1]["fields"]["处理状态"], "已创建")

    def test_manual_request_fails_when_remote_task_conflicts(self) -> None:
        self.db.generate_future_slots(datetime(2026, 4, 14, 11, 0, 0), window_hours=2)
        slot = self.db.get_slot_for_account_time(account_id="acc-1", scheduled_for="2026-04-14 12:00:00")
        with self.db._connect() as conn:
            conn.execute(
                """
                UPDATE publish_slots
                SET schedule_status = '已排期',
                    publish_task_id = 'task-auto'
                WHERE slot_id = ?
                """,
                (int(slot["slot_id"]),),
            )
        client = DummyManualClient()
        publisher = ChannelPublisher()

        stats = sync_manual_publish_requests(
            [self._record("rec-manual-conflict")],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
        )

        self.assertEqual(stats["conflicted"], 1)
        self.assertEqual(len(publisher.calls), 0)
        self.assertEqual(client.updates[-1]["fields"]["处理状态"], "发布失败")
        self.assertIn("已有远端发布任务冲突", client.updates[-1]["fields"]["错误信息"])

    def test_validation_failure_waits_for_input_and_retries_after_fields_are_fixed(self) -> None:
        record = self._record("rec-manual-wait")
        record.fields["短视频发布时间"] = ""
        client = DummyManualClient()
        publisher = ChannelPublisher()

        first_stats = sync_manual_publish_requests(
            [record],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
        )

        self.assertEqual(first_stats["validation_failed"], 1)
        self.assertEqual(client.updates[-1]["fields"]["处理状态"], "待补充")
        self.assertIn("短视频发布时间无法识别", client.updates[-1]["fields"]["错误信息"])
        self.assertEqual(len(publisher.calls), 0)

        record.fields["处理状态"] = "待补充"
        record.fields["短视频发布时间"] = "2026-04-14 12:00"
        second_stats = sync_manual_publish_requests(
            [record],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
        )

        self.assertEqual(second_stats["created"], 1)
        self.assertEqual(client.updates[-1]["fields"]["处理状态"], "已创建")
        self.assertEqual(len(publisher.calls), 1)

    def test_retryable_create_error_is_retried_before_marking_failed(self) -> None:
        client = DummyManualClient()
        publisher = FlakyPublisher(failures=1)

        stats = sync_manual_publish_requests(
            [self._record("rec-manual-retry")],
            self._mapping(),
            self.db,
            publisher,
            client=client,
            video_dir=Path(self.temp_dir.name) / "videos",
            create_retry_attempts=2,
            retry_sleep_seconds=0,
        )

        self.assertEqual(stats["created"], 1)
        self.assertEqual(stats["create_failed"], 0)
        self.assertEqual(stats["create_retried"], 1)
        self.assertEqual(len(publisher.calls), 2)
        self.assertEqual(client.updates[-1]["fields"]["处理状态"], "已创建")


if __name__ == "__main__":
    unittest.main()
