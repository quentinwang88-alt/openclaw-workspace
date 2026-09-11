from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from app.bgm_reporting import BGM_FIELD_SPECS, build_bgm_status_updates, sync_bgm_statuses  # noqa: E402
from app.publish_writeback import sync_run_manager_statuses, load_projection_rows
from app.db import AutoPublishDB
sys.path.insert(0, str(SKILL_ROOT.parent / "script-run-manager-sync"))
from core.bitable import TableField


class BgmReportingTest(unittest.TestCase):
    def test_creatok_photo_platform_auto_is_not_reported_as_track_selection(self):
        updates = build_bgm_status_updates([{
            "run_manager_record_id": "rec-photo", "audio_mode": "platform_auto_bgm",
            "cart_enabled": "否", "bgm_json": json.dumps({
                "mode": "platform_auto", "selection": "platform_recommended",
                "music_id": None, "profile": {"rhythm_preference": "soft"},
            }),
            "schedule_status": "已排期", "error_message": "", "publish_channel": "CreatOK",
        }])
        fields = updates[0]["fields"]
        self.assertEqual(fields["BGM策略"], "TikTok自动推荐音乐")
        self.assertEqual(fields["BGM状态"], "平台自动推荐")
        self.assertEqual(fields["选中BGM"], "")

    def test_creatok_embedded_local_music_needs_no_platform_readback(self):
        updates = build_bgm_status_updates([{
            "run_manager_record_id": "rec-local", "audio_mode": "silent_source_platform_bgm",
            "cart_enabled": "否", "publish_channel": "CreatOK",
            "bgm_json": json.dumps({
                "mode": "local_mix", "music_id": "LOCAL_1", "music_title": "Autumn Walk",
                "music_author": "Artist", "profile": {"rhythm_preference": "strong"},
                "actual": {"music_id": "LOCAL_1", "title": "Autumn Walk", "author": "Artist"},
            }),
            "schedule_status": "已排期", "error_message": "",
            "script_source": "图文养号", "content_branch": "非商品展示型",
        }])
        fields = updates[0]["fields"]
        self.assertEqual(fields["BGM策略"], "发布前本地混入")
        self.assertEqual(fields["BGM状态"], "已混入成片")
        self.assertIn("Autumn Walk", fields["选中BGM"])

    def test_pending_generated_video_is_visible_without_manual_bgm_switch(self):
        updates = build_bgm_status_updates([{
            "run_manager_record_id": "rec-1", "audio_mode": "generated_nonvoice", "cart_enabled": "否",
            "bgm_json": "", "schedule_status": "待排期", "error_message": "",
        }])
        self.assertEqual(updates[0]["fields"]["BGM策略"], "自动平台BGM")
        self.assertEqual(updates[0]["fields"]["BGM状态"], "待发布前选曲")

    def test_confirmed_platform_track_is_projected(self):
        updates = build_bgm_status_updates([{
            "run_manager_record_id": "rec-2", "audio_mode": "generated_nonvoice", "cart_enabled": "否",
            "bgm_json": ('{"music_id":"m1","music_title":"Dance","music_author":"A",'
                         '"actual":{"music_id":"m1","title":"Dance","author":"A"},'
                         '"music_match":true,"profile":{"rhythm_preference":"strong"},'
                         '"timing_plan":{"mode":"reveal"}}'),
            "schedule_status": "已发布", "error_message": "",
        }])
        fields = updates[0]["fields"]
        self.assertEqual(fields["BGM状态"], "已确认一致")
        self.assertIn("Dance", fields["选中BGM"])
        self.assertIn("reveal", fields["BGM节奏方案"])

    def test_incorrect_match_flag_does_not_confirm_different_ids(self):
        row = dict(run_manager_record_id="rec-1", audio_mode="generated_nonvoice", cart_enabled="否",
                   bgm_json=json.dumps({"music_id": "a", "actual": {"music_id": "b"}, "music_match": True}),
                   schedule_status="已发布", error_message="")
        self.assertEqual(build_bgm_status_updates([row])[0]["fields"]["BGM状态"], "实际BGM不一致")
        row["cart_enabled"] = "是"
        self.assertEqual(build_bgm_status_updates([row]), [])
        row.update(cart_enabled="否", publish_channel="GeeLark")
        self.assertEqual(build_bgm_status_updates([row]), [])


class PublishWritebackTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = AutoPublishDB(Path(self.tmp.name) / "test.sqlite3")
        with self.db._connect() as conn:
            conn.execute("""INSERT INTO script_metadata
                (canonical_script_key,script_id,source_record_id,script_slot,task_no,audio_mode,cart_enabled,created_at,updated_at)
                VALUES ('k','s','src','S1','t','generated_nonvoice','否','now','now')""")
            conn.execute("""INSERT INTO video_assets
                (canonical_script_key,script_id,run_manager_record_id,publish_status,publish_task_id,planned_publish_at,created_at,updated_at)
                VALUES ('k','s','rec-1','已排期','123','2026-09-04 10:00:00','now','now')""")
            conn.execute("""INSERT INTO publish_slots
                (store_id,account_id,account_name,scheduled_for,canonical_script_key,script_id,schedule_status,publish_task_id,bgm_json,created_at,updated_at)
                VALUES ('store','acct','name','2026-09-04 10:00:00','k','s','已排期','123','{"music_id":"m"}','a','a')""")
            conn.execute("""INSERT INTO publish_slots
                (store_id,account_id,account_name,scheduled_for,canonical_script_key,script_id,schedule_status,created_at,updated_at)
                VALUES ('store','acct','name','2026-09-05 10:00:00','k','s','已取消','z','z')""")
        self.client = Mock()
        self.client.list_fields.return_value = [
            {"field_name": name, "type": value[0]} for name, value in BGM_FIELD_SPECS.items()
        ] + [{"field_name": "发布状态", "type": 3}, {"field_name": "发布任务ID", "type": 1},
             {"field_name": "计划发布时间", "type": 5}, {"field_name": "是否发布", "type": 7}]
        self.client.list_fields.return_value = [TableField(
            field_id=str(i), field_name=f["field_name"], field_type=f["type"], ui_type=None, property=None,
        ) for i, f in enumerate(self.client.list_fields.return_value)]

    def test_frozen_task_projection_diff_and_date(self):
        self.assertEqual(load_projection_rows(self.db)[0]["schedule_status"], "已排期")
        records = [SimpleNamespace(record_id="rec-1", fields={"是否发布": True})]
        stats = sync_run_manager_statuses(self.client, self.db, records=records)
        self.assertEqual(stats["records_updated"], 1)
        self.client.list_records.assert_not_called()
        self.client.list_fields.assert_called_once()
        fields = self.client.batch_update_records.call_args.args[0][0]["fields"]
        self.assertEqual(fields["发布任务ID"], "123")
        self.assertEqual(fields["计划发布时间"], 1788487200000)
        self.assertNotIn("是否发布", fields)
        self.assertIn("ID:m", fields["选中BGM"])
        # Feishu rich text normalizes to the plain projection, avoiding writes.
        fields["发布任务ID"] = [{"text": "123", "type": "text"}]
        records[0].fields.update(fields)
        self.client.batch_update_records.reset_mock()
        self.assertEqual(sync_run_manager_statuses(self.client, self.db, records=records)["records_updated"], 0)
        self.client.batch_update_records.assert_not_called()

    def test_deleted_rows_skipped_and_retry_leaves_task_unchanged(self):
        self.assertEqual(sync_bgm_statuses(self.client, self.db, records=[])["records_updated"], 0)
        self.client.batch_update_records.side_effect = RuntimeError("Feishu unavailable")
        with self.assertRaises(RuntimeError):
            sync_run_manager_statuses(self.client, self.db, records=[{"record_id": "rec-1", "fields": {}}])
        self.assertEqual(load_projection_rows(self.db)[0]["publish_task_id"], "123")
        self.client.batch_update_records.side_effect = None
        self.assertEqual(sync_run_manager_statuses(self.client, self.db, records=[{"record_id": "rec-1", "fields": {}}])["records_updated"], 1)


if __name__ == "__main__":
    unittest.main()
