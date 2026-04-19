#!/usr/bin/env python3
"""排班规则测试。"""

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
from app.models import PublishTaskStatus
from app.models import AccountConfig, ScriptMetadata
from app.publishers import BasePublishAdapter, DryRunPublishAdapter
from app.scheduler import RUN_MANAGER_FIELD_ALIASES, resolve_field_mapping, schedule_slots, sync_videos


class DummyRecord:
    def __init__(self, record_id: str, fields: dict) -> None:
        self.record_id = record_id
        self.fields = fields


class DummyClient:
    def download_attachment_bytes(self, attachment: dict):
        return b"video-bytes", "video.mp4", "video/mp4", len(b"video-bytes")


class RealishPublisher(BasePublishAdapter):
    def create_scheduled_task(
        self,
        *,
        account_id: str,
        video_path: str,
        title: str,
        publish_at: datetime,
        script_id: str,
        product_id: str = "",
        product_title: str = "",
        ref_video_id: str = "",
    ) -> str:
        return f"real-{account_id}-{script_id}"

    def query_task_status(self, *, task_id: str, scheduled_for: datetime) -> PublishTaskStatus:
        return PublishTaskStatus(state="pending", result="待执行")


class SchedulerTest(unittest.TestCase):
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
                    publish_time_2="17:00",
                    publish_time_3="20:00",
                )
            ]
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _upsert_script(self, script_id: str, product_id: str, family: str) -> None:
        self.db.upsert_script_metadata(
            [
                ScriptMetadata(
                    script_id=script_id,
                    source_record_id=f"rec-{script_id}",
                    script_slot="S1",
                    task_no="001",
                    store_id="SHOP-01",
                    product_id=product_id,
                    parent_slot="M1",
                    direction_label="日常轻分享流",
                    variant_strength="母版",
                    target_country="Thailand",
                    product_type="耳环",
                    content_family_key=family,
                    script_text="script",
                    short_video_title=f"title-{script_id}",
                    title_source="test",
                )
            ]
        )
        self.db.upsert_video_asset(
            script_id=script_id,
            run_manager_record_id=f"run-{script_id}",
            video_source_type="link",
            video_source_value="https://example.com/video.mp4",
            local_file_path=f"/tmp/{script_id}.mp4",
            download_status="下载成功",
            run_video_status="成功",
            publish_status="待排期",
        )

    def test_schedule_slots_respects_recent_product_rule(self) -> None:
        self._upsert_script("001_M1_M", "P1001", "P1001_M1")
        self._upsert_script("002_M1_M", "P1001", "P1001_M1")
        self._upsert_script("003_M2_M", "P1002", "P1002_M2")
        self._upsert_script("004_M3_M", "P1003", "P1003_M3")

        first_run = schedule_slots(
            self.db,
            DryRunPublishAdapter(),
            now=datetime(2026, 4, 9, 11, 0, 0),
        )
        self.assertEqual(first_run.scheduled, 3)

        second_run = schedule_slots(
            self.db,
            DryRunPublishAdapter(),
            now=datetime(2026, 4, 9, 16, 0, 0),
        )
        self.assertEqual(second_run.scheduled, 0)

    def test_schedule_slots_blocks_same_family_at_same_time_across_accounts(self) -> None:
        self.db.upsert_account_configs(
            [
                AccountConfig(
                    account_id="acc-2",
                    account_name="账号2",
                    store_id="SHOP-01",
                    account_status="可用",
                    publish_time_1="12:00",
                    publish_time_2="",
                    publish_time_3="",
                )
            ]
        )
        self._upsert_script("011_M3_M", "P2011", "P2011_M3")
        self._upsert_script("011_M3_V1", "P2011", "P2011_M3")

        stats = schedule_slots(
            self.db,
            DryRunPublishAdapter(),
            now=datetime(2026, 4, 9, 11, 0, 0),
        )

        self.assertEqual(stats.scheduled, 1)
        with self.db._connect() as conn:
            scheduled = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM publish_slots
                WHERE schedule_status = '已排期'
                  AND scheduled_for = '2026-04-09 12:00:00'
                """
            ).fetchone()
        self.assertEqual(int(scheduled["count"]), 1)

    def test_sync_videos_accepts_light_run_manager_fields(self) -> None:
        self._upsert_script("005_M1_M", "P1005", "P1005_M1")
        field_names = ["脚本ID", "状态", "是否发布", "生成视频"]
        mapping = resolve_field_mapping(field_names, RUN_MANAGER_FIELD_ALIASES)
        record = DummyRecord(
            "run-rec-1",
            {
                "脚本ID": "005_M1_M",
                "状态": "已完成",
                "是否发布": True,
                "生成视频": [{"file_token": "file-token-1", "name": "005_M1_M.mp4"}],
            },
        )

        stats = sync_videos(
            [record],
            mapping,
            self.db,
            download_dir=Path(self.temp_dir.name) / "videos",
            client=DummyClient(),
        )

        self.assertEqual(stats["synced"], 1)
        asset = self.db.get_video_asset("005_M1_M")
        self.assertIsNotNone(asset)
        self.assertEqual(asset["download_status"], "下载成功")
        self.assertEqual(asset["run_video_status"], "已完成")

    def test_sync_videos_requires_publish_checkbox(self) -> None:
        self._upsert_script("006_M1_V2", "P1006", "P1006_M1")
        field_names = ["脚本ID", "状态", "是否发布", "生成视频"]
        mapping = resolve_field_mapping(field_names, RUN_MANAGER_FIELD_ALIASES)
        record = DummyRecord(
            "run-rec-2",
            {
                "脚本ID": "006_M1_V2",
                "状态": "已完成",
                "是否发布": False,
                "生成视频": [{"file_token": "file-token-2", "name": "006_M1_V2.mp4"}],
            },
        )

        stats = sync_videos(
            [record],
            mapping,
            self.db,
            download_dir=Path(self.temp_dir.name) / "videos",
            client=DummyClient(),
        )

        self.assertEqual(stats["synced"], 0)
        self.assertEqual(stats["skipped"], 1)

    def test_sync_videos_can_match_by_canonical_script_key(self) -> None:
        self._upsert_script("006_M1_V3", "P1006", "P1006_M1")
        field_names = ["内部脚本键", "是否发布", "生成视频"]
        mapping = resolve_field_mapping(field_names, RUN_MANAGER_FIELD_ALIASES)
        record = DummyRecord(
            "run-rec-canonical",
            {
                "内部脚本键": "rec-006_M1_V3:S1",
                "是否发布": True,
                "生成视频": [{"file_token": "file-token-canonical", "name": "006_M1_V3.mp4"}],
            },
        )

        stats = sync_videos(
            [record],
            mapping,
            self.db,
            download_dir=Path(self.temp_dir.name) / "videos",
            client=DummyClient(),
        )

        self.assertEqual(stats["synced"], 1)
        asset = self.db.get_video_asset("rec-006_M1_V3:S1")
        self.assertIsNotNone(asset)
        self.assertEqual(asset["script_id"], "006_M1_V3")

    def test_sync_videos_accepts_checked_publish_even_if_status_is_submitted(self) -> None:
        self._upsert_script("007_M2_V2", "P1007", "P1007_M2")
        field_names = ["脚本ID", "状态", "是否发布", "生成视频"]
        mapping = resolve_field_mapping(field_names, RUN_MANAGER_FIELD_ALIASES)
        record = DummyRecord(
            "run-rec-3",
            {
                "脚本ID": "007_M2_V2",
                "状态": "已提交",
                "是否发布": True,
                "生成视频": [{"file_token": "file-token-3", "name": "007_M2_V2.mp4"}],
            },
        )

        stats = sync_videos(
            [record],
            mapping,
            self.db,
            download_dir=Path(self.temp_dir.name) / "videos",
            client=DummyClient(),
        )

        self.assertEqual(stats["synced"], 1)
        asset = self.db.get_video_asset("007_M2_V2")
        self.assertIsNotNone(asset)
        self.assertEqual(asset["run_video_status"], "已提交")

    def test_schedule_slots_recycles_dryrun_assignments_before_real_schedule(self) -> None:
        self._upsert_script("008_M1_V3", "P1008", "P1008_M1")

        dryrun_run = schedule_slots(
            self.db,
            DryRunPublishAdapter(),
            now=datetime(2026, 4, 13, 11, 0, 0),
        )
        self.assertEqual(dryrun_run.scheduled, 1)
        asset_before = self.db.get_video_asset("008_M1_V3")
        self.assertTrue(str(asset_before["publish_task_id"]).startswith("dryrun-"))

        second_run = schedule_slots(
            self.db,
            RealishPublisher(),
            now=datetime(2026, 4, 13, 11, 5, 0),
        )
        self.assertEqual(second_run.scheduled, 1)

        asset_after = self.db.get_video_asset("008_M1_V3")
        self.assertIsNotNone(asset_after)
        self.assertEqual(asset_after["publish_status"], "已排期")
        self.assertTrue(str(asset_after["publish_task_id"]).startswith("real-"))

        with self.db._connect() as conn:
            dryrun_slots = conn.execute(
                "SELECT COUNT(*) AS count FROM publish_slots WHERE publish_task_id LIKE 'dryrun-%'"
            ).fetchone()
        self.assertEqual(int(dryrun_slots["count"]), 0)


if __name__ == "__main__":
    unittest.main()
