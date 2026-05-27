#!/usr/bin/env python3
"""排班规则测试。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
import sys


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.db import AutoPublishDB
from app.models import PublishTaskStatus
from app.models import AccountConfig, ScriptMetadata
from app.notifications import format_daily_publish_summary
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
    def __init__(self) -> None:
        self.calls = []

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
        self.calls.append(
            {
                "account_id": account_id,
                "script_id": script_id,
                "product_id": product_id,
                "publish_at": publish_at,
            }
        )
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

    def _upsert_nurture_script(self, script_id: str, content_branch: str = "商品展示型") -> None:
        self.db.upsert_script_metadata(
            [
                ScriptMetadata(
                    script_id=script_id,
                    source_record_id=f"rec-{script_id}",
                    script_slot="S1",
                    task_no="028",
                    store_id="SHOP-01",
                    product_id="P-NURTURE",
                    parent_slot="YR1",
                    direction_label="养号复刻",
                    variant_strength="母版",
                    target_country="Thailand",
                    product_type="轻上装",
                    content_family_key=f"{script_id}:养号",
                    script_text="nurture script",
                    short_video_title=f"title-{script_id}",
                    title_source="test",
                    script_source="养号复刻",
                    publish_purpose="养号",
                    cart_enabled="否",
                    content_branch=content_branch,
                )
            ]
        )
        self.db.upsert_video_asset(
            script_id=script_id,
            run_manager_record_id=f"run-{script_id}",
            video_source_type="link",
            video_source_value="https://example.com/nurture.mp4",
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

    def test_nurture_enabled_account_prefers_two_nurture_videos_without_product_binding(self) -> None:
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
                    nurture_enabled=True,
                    nurture_daily_count=2,
                )
            ]
        )
        self._upsert_nurture_script("YR028_YR1_M")
        self._upsert_nurture_script("YR029_YR1_M")
        self._upsert_script("010_M1_M", "P1010", "P1010_M1")
        publisher = RealishPublisher()

        stats = schedule_slots(
            self.db,
            publisher,
            now=datetime(2026, 4, 15, 11, 0, 0),
        )

        self.assertEqual(stats.scheduled, 3)
        self.assertEqual([call["script_id"] for call in publisher.calls[:2]], ["YR028_YR1_M", "YR029_YR1_M"])
        self.assertEqual([call["product_id"] for call in publisher.calls[:2]], ["", ""])
        self.assertEqual(publisher.calls[2]["product_id"], "P1010")

    def test_nurture_only_account_never_falls_back_to_product_video(self) -> None:
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
                    nurture_enabled=True,
                    nurture_daily_count=2,
                    nurture_only=True,
                )
            ]
        )
        self._upsert_nurture_script("YR030_YR1_M")
        self._upsert_script("011_M1_M", "P1011", "P1011_M1")
        publisher = RealishPublisher()

        stats = schedule_slots(
            self.db,
            publisher,
            now=datetime(2026, 4, 15, 11, 0, 0),
        )

        self.assertEqual(stats.scheduled, 1)
        self.assertEqual([call["script_id"] for call in publisher.calls], ["YR030_YR1_M"])
        self.assertEqual([call["product_id"] for call in publisher.calls], [""])

    def test_assign_slot_clears_stale_failure_state_when_rescheduling(self) -> None:
        self._upsert_script("009_M1_V1", "P1009", "P1009_M1")

        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 24)
        first_slot = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 24)[0]
        self.db.assign_slot(
            slot_id=int(first_slot["slot_id"]),
            script_id="009_M1_V1",
            publish_task_id="task-failed",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=datetime(2026, 4, 13, 12, 0, 0),
        )
        self.db.mark_publish_result(
            script_id="009_M1_V1",
            publish_task_id="task-failed",
            schedule_status="发布失败",
            publish_status="发布失败",
            publish_result="发布失败",
            error_message="Product not found",
        )

        self.db.generate_future_slots(datetime(2026, 4, 14, 11, 0, 0), 24)
        second_slot = self.db.list_pending_slots(datetime(2026, 4, 14, 11, 0, 0), 24)[0]
        self.db.assign_slot(
            slot_id=int(second_slot["slot_id"]),
            script_id="009_M1_V1",
            publish_task_id="task-retry",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=datetime(2026, 4, 14, 12, 0, 0),
        )

        asset = self.db.get_video_asset("009_M1_V1")
        self.assertIsNotNone(asset)
        self.assertEqual(asset["publish_status"], "已排期")
        self.assertEqual(asset["publish_task_id"], "task-retry")
        self.assertEqual(asset["publish_result"], None)
        self.assertEqual(asset["error_message"], None)
        self.assertEqual(asset["published_at"], None)

    def test_schedule_slots_allows_retry_after_product_failures(self) -> None:
        self._upsert_script("010_M1_V1", "P1010", "P1010_M1")

        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 24)
        for index, slot in enumerate(
            self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 24)[:3],
            start=1,
        ):
            self.db.assign_slot(
                slot_id=int(slot["slot_id"]),
                script_id="010_M1_V1",
                publish_task_id=f"task-failed-{index}",
                account_id="acc-1",
                account_name="账号1",
                planned_publish_at=datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
            )
            self.db.mark_publish_result(
                script_id="010_M1_V1",
                publish_task_id=f"task-failed-{index}",
                schedule_status="发布失败",
                publish_status="发布失败",
                publish_result="发布失败",
                error_message="Product not found",
            )
            self.db.upsert_video_asset(
                script_id="010_M1_V1",
                run_manager_record_id="run-010_M1_V1",
                video_source_type="link",
                video_source_value="https://example.com/video.mp4",
                local_file_path="/tmp/010_M1_V1.mp4",
                download_status="下载成功",
                run_video_status="成功",
                publish_status="待排期",
            )

        publisher = RealishPublisher()
        stats = schedule_slots(self.db, publisher, now=datetime(2026, 4, 14, 11, 0, 0))

        self.assertEqual(stats.scheduled, 1)
        self.assertEqual([call["script_id"] for call in publisher.calls], ["010_M1_V1"])

    def test_disable_product_blocks_future_candidates(self) -> None:
        self._upsert_script("011_M1_V1", "P1011", "P1011_M1")

        stats = self.db.disable_product("P1011", reason="下架")
        self.assertEqual(stats["disabled_products"], 1)
        self.assertEqual(stats["video_assets_skipped"], 1)

        self.db.upsert_video_asset(
            script_id="011_M1_V1",
            run_manager_record_id="run-011_M1_V1",
            video_source_type="link",
            video_source_value="https://example.com/video.mp4",
            local_file_path="/tmp/011_M1_V1.mp4",
            download_status="下载成功",
            run_video_status="成功",
            publish_status="待排期",
        )

        asset = self.db.get_video_asset("011_M1_V1")
        self.assertEqual(asset["publish_status"], "已跳过")
        self.assertEqual(self.db.list_ready_candidates("SHOP-01"), [])

    def test_disable_account_cancels_future_slot_and_requeues_asset(self) -> None:
        self._upsert_script("012_M1_V1", "P1012", "P1012_M1")
        now = datetime.now().replace(second=0, microsecond=0)
        self.db.generate_future_slots(now, 24)
        slot = self.db.list_pending_slots(now, 24)[0]
        self.db.assign_slot(
            slot_id=int(slot["slot_id"]),
            script_id="012_M1_V1",
            publish_task_id="task-future",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=now + timedelta(hours=1),
        )

        stats = self.db.disable_account("acc-1", reason="账号异常")

        self.assertEqual(stats["paused_accounts"], 1)
        self.assertGreaterEqual(stats["future_slots_cancelled"], 1)
        self.assertEqual(stats["assets_requeued"], 1)
        account = self.db.get_account_config("acc-1")
        self.assertEqual(account["account_status"], "暂停")
        asset = self.db.get_video_asset("012_M1_V1")
        self.assertEqual(asset["publish_status"], "待排期")
        self.assertEqual(asset["publish_task_id"], None)

    def test_mark_publish_result_preserves_slot_error_message(self) -> None:
        self._upsert_script("013_M1_V1", "P1013", "P1013_M1")
        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 24)
        slot = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 24)[0]
        self.db.assign_slot(
            slot_id=int(slot["slot_id"]),
            script_id="013_M1_V1",
            publish_task_id="task-failed",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
        )
        self.db.mark_publish_result(
            script_id="013_M1_V1",
            publish_task_id="task-failed",
            schedule_status="发布失败",
            publish_status="发布失败",
            publish_result="发布失败",
            error_message="account blocked",
        )

        rows = self.db._connect().execute(
            "SELECT error_message FROM publish_slots WHERE publish_task_id = ?",
            ("task-failed",),
        ).fetchall()
        self.assertEqual(rows[0]["error_message"], "account blocked")

    def test_manual_publish_marks_asset_and_cancels_future_active_slot(self) -> None:
        self._upsert_script("013_M2_V1", "P1013", "P1013_M2")
        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        slots = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        first_time = datetime.strptime(str(slots[0]["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
        second_time = datetime.strptime(str(slots[1]["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
        self.db.assign_slot(
            slot_id=int(slots[0]["slot_id"]),
            script_id="013_M2_V1",
            publish_task_id="task-manual-base",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=first_time,
        )
        self.db.assign_slot(
            slot_id=int(slots[1]["slot_id"]),
            script_id="013_M2_V1",
            publish_task_id="task-should-cancel",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=second_time,
        )

        ok = self.db.mark_manual_publish_result(
            script_id="013_M2_V1",
            scheduled_for=first_time.strftime("%Y-%m-%d %H:%M:%S"),
            note="人工已发",
        )

        self.assertTrue(ok)
        asset = self.db.get_video_asset("013_M2_V1")
        self.assertEqual(asset["publish_status"], "已发布")
        self.assertEqual(asset["publish_result"], "人工发布成功")
        rows = self.db._connect().execute(
            """
            SELECT publish_task_id, schedule_status, error_message
            FROM publish_slots
            WHERE script_id = ?
            ORDER BY scheduled_for ASC
            """,
            ("013_M2_V1",),
        ).fetchall()
        self.assertEqual(rows[0]["schedule_status"], "已发布")
        self.assertEqual(rows[1]["schedule_status"], "已取消")
        self.assertIn("已人工发布", rows[1]["error_message"])

    def test_enforce_retry_limit_requeues_product_failure(self) -> None:
        self._upsert_script("014_M1_V1", "P1014", "P1014_M1")
        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        slots = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        for index, slot in enumerate(slots[:3], start=1):
            self.db.assign_slot(
                slot_id=int(slot["slot_id"]),
                script_id="014_M1_V1",
                publish_task_id=f"task-failed-{index}",
                account_id="acc-1",
                account_name="账号1",
                planned_publish_at=datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
            )
            self.db.mark_publish_result(
                script_id="014_M1_V1",
                publish_task_id=f"task-failed-{index}",
                schedule_status="发布失败",
                publish_status="发布失败",
                publish_result="发布失败",
                error_message="Product not found",
            )
            self.db.upsert_video_asset(
                script_id="014_M1_V1",
                run_manager_record_id="run-014_M1_V1",
                video_source_type="link",
                video_source_value="https://example.com/video.mp4",
                local_file_path="/tmp/014_M1_V1.mp4",
                download_status="下载成功",
                run_video_status="成功",
                publish_status="待排期",
            )
        future_slot = self.db.list_pending_slots(datetime(2026, 4, 14, 11, 0, 0), 24)[0]
        self.db.assign_slot(
            slot_id=int(future_slot["slot_id"]),
            script_id="014_M1_V1",
            publish_task_id="task-legacy-active",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=datetime.strptime(str(future_slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
        )

        stats = self.db.enforce_retry_limit(max_auto_retries=2)

        self.assertEqual(stats["candidates"], 1)
        self.assertEqual(stats["video_assets_skipped"], 0)
        self.assertEqual(stats["video_assets_requeued"], 1)
        self.assertGreaterEqual(stats["active_slots_cancelled"], 1)
        self.assertIn("task-legacy-active", stats["remote_task_ids"])
        asset = self.db.get_video_asset("014_M1_V1")
        self.assertEqual(asset["publish_status"], "待排期")
        self.assertEqual(asset["publish_task_id"], None)
        self.assertIn("复活待重新排期", asset["error_message"])

    def test_enforce_retry_limit_requeues_account_environment_failures(self) -> None:
        self._upsert_script("014_M2_V1", "P1014", "P1014_M2")
        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        slots = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        for index, slot in enumerate(slots[:3], start=1):
            self.db.assign_slot(
                slot_id=int(slot["slot_id"]),
                script_id="014_M2_V1",
                publish_task_id=f"task-env-failed-{index}",
                account_id="acc-1",
                account_name="账号1",
                planned_publish_at=datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
            )
            self.db.mark_publish_result(
                script_id="014_M2_V1",
                publish_task_id=f"task-env-failed-{index}",
                schedule_status="发布失败",
                publish_status="发布失败",
                publish_result="发布失败",
                error_message="Proxy detection failed",
            )
            self.db.upsert_video_asset(
                script_id="014_M2_V1",
                run_manager_record_id="run-014_M2_V1",
                video_source_type="link",
                video_source_value="https://example.com/video.mp4",
                local_file_path="/tmp/014_M2_V1.mp4",
                download_status="下载成功",
                run_video_status="成功",
                publish_status="待排期",
            )
        future_slot = self.db.list_pending_slots(datetime(2026, 4, 14, 11, 0, 0), 24)[0]
        self.db.assign_slot(
            slot_id=int(future_slot["slot_id"]),
            script_id="014_M2_V1",
            publish_task_id="task-env-active",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=datetime.strptime(str(future_slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
        )

        stats = self.db.enforce_retry_limit(max_auto_retries=2)

        self.assertEqual(stats["candidates"], 1)
        self.assertEqual(stats["video_assets_skipped"], 0)
        self.assertEqual(stats["video_assets_requeued"], 1)
        self.assertGreaterEqual(stats["active_slots_cancelled"], 1)
        asset = self.db.get_video_asset("014_M2_V1")
        self.assertEqual(asset["publish_status"], "待排期")
        self.assertEqual(asset["publish_task_id"], None)
        self.assertIn("复活待重新排期", asset["error_message"])

    def test_schedule_slots_allows_requeued_account_environment_failures(self) -> None:
        self._upsert_script("014_M3_V1", "P1014", "P1014_M3")
        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        slots = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        for index, slot in enumerate(slots[:3], start=1):
            self.db.assign_slot(
                slot_id=int(slot["slot_id"]),
                script_id="014_M3_V1",
                publish_task_id=f"task-login-failed-{index}",
                account_id="acc-1",
                account_name="账号1",
                planned_publish_at=datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
            )
            self.db.mark_publish_result(
                script_id="014_M3_V1",
                publish_task_id=f"task-login-failed-{index}",
                schedule_status="发布失败",
                publish_status="发布失败",
                publish_result="发布失败",
                error_message="The account is not logged in",
            )
            self.db.upsert_video_asset(
                script_id="014_M3_V1",
                run_manager_record_id="run-014_M3_V1",
                video_source_type="link",
                video_source_value="https://example.com/video.mp4",
                local_file_path="/tmp/014_M3_V1.mp4",
                download_status="下载成功",
                run_video_status="成功",
                publish_status="待排期",
            )

        stats = schedule_slots(self.db, RealishPublisher(), now=datetime(2026, 4, 14, 11, 0, 0))

        self.assertEqual(stats.scheduled, 1)

    def test_enforce_retry_limit_can_revive_previously_skipped_failure(self) -> None:
        self._upsert_script("014_M4_V1", "P1014", "P1014_M4")
        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        slots = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 72)
        for index, slot in enumerate(slots[:3], start=1):
            self.db.assign_slot(
                slot_id=int(slot["slot_id"]),
                script_id="014_M4_V1",
                publish_task_id=f"task-old-env-failed-{index}",
                account_id="acc-1",
                account_name="账号1",
                planned_publish_at=datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
            )
            self.db.mark_publish_result(
                script_id="014_M4_V1",
                publish_task_id=f"task-old-env-failed-{index}",
                schedule_status="发布失败",
                publish_status="发布失败",
                publish_result="发布失败",
                error_message="account login expired",
            )
        with self.db._connect() as conn:
            conn.execute(
                """
                UPDATE video_assets
                SET publish_status = '已跳过',
                    publish_result = '已跳过',
                    error_message = '超过自动重试上限'
                WHERE script_id = ?
                """,
                ("014_M4_V1",),
            )

        stats = self.db.enforce_retry_limit(max_auto_retries=2)

        self.assertEqual(stats["video_assets_requeued"], 1)
        asset = self.db.get_video_asset("014_M4_V1")
        self.assertEqual(asset["publish_status"], "待排期")

    def test_daily_publish_summary_message_includes_failure_context(self) -> None:
        self._upsert_script("015_M1_V1", "P1015", "P1015_M1")
        self.db.generate_future_slots(datetime(2026, 4, 13, 11, 0, 0), 24)
        slot = self.db.list_pending_slots(datetime(2026, 4, 13, 11, 0, 0), 24)[0]
        self.db.assign_slot(
            slot_id=int(slot["slot_id"]),
            script_id="015_M1_V1",
            publish_task_id="task-summary-failed",
            account_id="acc-1",
            account_name="账号1",
            planned_publish_at=datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
        )
        self.db.mark_publish_result(
            script_id="015_M1_V1",
            publish_task_id="task-summary-failed",
            schedule_status="发布失败",
            publish_status="发布失败",
            publish_result="发布失败",
            error_message="Product unavailable",
        )

        summary = self.db.build_daily_publish_summary("2026-04-13")
        message = format_daily_publish_summary(summary)

        self.assertIn("发布失败：1", message)
        self.assertIn("账号1", message)
        self.assertIn("脚本ID=015_M1_V1", message)
        self.assertIn("Product unavailable", message)


if __name__ == "__main__":
    unittest.main()
