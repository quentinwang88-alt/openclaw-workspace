#!/usr/bin/env python3
"""NeoBund 发布适配器测试。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.neobund_publish import NeoBundPublishAdapter, NeoBundUploadResult  # noqa: E402


class NeoBundPublishAdapterTest(unittest.TestCase):
    def test_upload_video_remuxes_quicktime_container_before_neobund_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "video.mp4"
            source.write_bytes(b"\x00\x00\x00\x18ftypqt  " + b"\x00" * 128)

            client = Mock()
            client.get_upload_token.return_value = {"token": "ok"}
            client.save_file_record.return_value = {"fileId": "file-1"}
            uploader = Mock()
            uploader.upload_file.return_value = {
                "bucketName": "demo-bucket",
                "fileName": "video.neobund.mp4",
                "fileSize": 12,
                "fileType": "video/mp4",
                "key": "videos/video.neobund.mp4",
                "url": "https://cdn.example/video.neobund.mp4",
            }
            adapter = NeoBundPublishAdapter(client=client, uploader=uploader)

            def fake_run(command, **kwargs):
                Path(command[-1]).write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 128)
                return subprocess.CompletedProcess(command, 0, "", "")

            with patch.object(NeoBundPublishAdapter, "_find_ffmpeg", return_value="/usr/bin/ffmpeg"):
                with patch.object(NeoBundPublishAdapter, "_video_needs_standardization", return_value=False):
                    with patch("app.neobund_publish.subprocess.run", side_effect=fake_run) as run_mock:
                        result = adapter.upload_video(str(source))

            target = Path(tmpdir) / "video.neobund.mp4"
            self.assertEqual(result.file_id, "file-1")
            self.assertTrue(target.exists())
            self.assertEqual(uploader.upload_file.call_args.args[0], str(target))
            client.get_upload_token.assert_called_once_with(file_name="video.neobund.mp4", file_type="video/mp4", asset_type=2)
            run_mock.assert_called_once()
            command = run_mock.call_args.args[0]
            self.assertIn("0:v:0", command)
            self.assertIn("0:a?", command)
            self.assertIn("-dn", command)
            self.assertIn("-sn", command)
            self.assertEqual(command[command.index("-map_metadata") + 1], "-1")

    def test_upload_video_leaves_normal_mp4_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "video.mp4"
            source.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 128)

            client = Mock()
            client.get_upload_token.return_value = {"token": "ok"}
            client.save_file_record.return_value = {"fileId": "file-2"}
            uploader = Mock()
            uploader.upload_file.return_value = {
                "bucketName": "demo-bucket",
                "fileName": "video.mp4",
                "fileSize": 12,
                "fileType": "video/mp4",
                "key": "videos/video.mp4",
                "url": "",
            }
            adapter = NeoBundPublishAdapter(client=client, uploader=uploader)

            with patch.object(NeoBundPublishAdapter, "_video_needs_standardization", return_value=False):
                adapter.upload_video(str(source))

            self.assertEqual(uploader.upload_file.call_args.args[0], str(source))
            client.get_upload_token.assert_called_once_with(file_name="video.mp4", file_type="video/mp4", asset_type=2)

    def test_upload_video_standardizes_abnormal_frame_rate_before_neobund_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "video.mp4"
            source.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 128)
            client = Mock()
            client.get_upload_token.return_value = {"token": "ok"}
            client.save_file_record.return_value = {"fileId": "file-3"}
            uploader = Mock()
            uploader.upload_file.return_value = {
                "bucketName": "demo-bucket",
                "fileName": "video.neobund-standard.mp4",
                "fileSize": 12,
                "fileType": "video/mp4",
                "key": "videos/video.neobund-standard.mp4",
                "url": "",
            }
            adapter = NeoBundPublishAdapter(client=client, uploader=uploader)

            def fake_run(command, **kwargs):
                Path(command[-1]).write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 128)
                return subprocess.CompletedProcess(command, 0, "", "")

            with patch.object(NeoBundPublishAdapter, "_video_needs_standardization", side_effect=[True, False]):
                with patch.object(NeoBundPublishAdapter, "_find_ffmpeg", return_value="/usr/bin/ffmpeg"):
                    with patch("app.neobund_publish.subprocess.run", side_effect=fake_run) as run_mock:
                        adapter.upload_video(str(source))

            target = Path(tmpdir) / "video.neobund-standard.mp4"
            self.assertEqual(uploader.upload_file.call_args.args[0], str(target))
            command = run_mock.call_args.args[0]
            self.assertIn("fps=30,format=yuv420p", command)
            self.assertEqual(command[command.index("-c:v") + 1], "libx264")

    def test_upload_video_fails_fast_when_quicktime_container_needs_missing_ffmpeg(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "video.mp4"
            source.write_bytes(b"\x00\x00\x00\x18ftypqt  " + b"\x00" * 128)
            adapter = NeoBundPublishAdapter(client=Mock(), uploader=Mock())

            with patch.object(NeoBundPublishAdapter, "_find_ffmpeg", return_value=""):
                with self.assertRaisesRegex(RuntimeError, "ffmpeg"):
                    adapter.upload_video(str(source))

    def test_create_scheduled_task_uses_auth_id_directly_and_product_id_as_tt_product_id(self) -> None:
        client = Mock()
        client.commit_shoppable_video.return_value = {"id": 991}
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="file-123",
                key="videos/demo.mp4",
                bucket_name="demo-bucket",
            )
        )

        task_id = adapter.create_scheduled_task(
            account_id="621288192900859568",
            video_path="/tmp/demo.mp4",
            title="hello #tag",
            publish_at=datetime(2026, 6, 29, 18, 30, 0),
            script_id="VNPS01.S1",
            product_id="7498614361651",
            product_title="Demo Product Title That Is Long",
        )

        self.assertEqual(task_id, "neobund:991")
        self.assertEqual(
            client.commit_shoppable_video.call_args.args[0],
            {
                "authId": 621288192900859568,
                "authType": 1,
                "ttProductId": "7498614361651",
                "productTitle": "Demo Product Title That Is Long"[:30],
                "videoTitle": "hello #tag",
                "scheduledReleaseTime": "2026-06-29 18:30:00",
                "attachFileId": "file-123",
                "isPrecheck": 0,
                "remark": "VNPS01.S1",
            },
        )

    def test_create_scheduled_task_marks_shoppable_ai_content_when_selected(self) -> None:
        client = Mock()
        client.commit_shoppable_video.return_value = {"id": 992}
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(file_id="file-124", key="videos/demo.mp4", bucket_name="demo-bucket")
        )

        adapter.create_scheduled_task(
            account_id="621288192900859568",
            video_path="/tmp/demo.mp4",
            title="ai content",
            publish_at=datetime(2026, 6, 29, 18, 30, 0),
            script_id="VNPS01.S1",
            product_id="7498614361651",
            mark_ai=True,
        )

        self.assertIs(client.commit_shoppable_video.call_args.args[0]["isAIGC"], True)

    def test_create_scheduled_task_supports_optional_account_id_map(self) -> None:
        client = Mock()
        client.commit_shoppable_video.return_value = {"taskId": "task-abc"}
        adapter = NeoBundPublishAdapter(
            client=client,
            account_id_map={"old-account": {"neobund_auth_id": "12345"}},
            is_precheck=1,
        )
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="456",
                key="videos/demo.mp4",
                bucket_name="demo-bucket",
            )
        )

        task_id = adapter.create_scheduled_task(
            account_id="old-account",
            video_path="/tmp/demo.mp4",
            title="mapped",
            publish_at=datetime(2026, 6, 29, 19, 0, 0),
            script_id="VNPS01.S2",
            product_id="7498614361651",
        )

        self.assertEqual(task_id, "neobund:task-abc")
        payload = client.commit_shoppable_video.call_args.args[0]
        self.assertEqual(payload["authId"], 12345)
        self.assertEqual(payload["attachFileId"], 456)
        self.assertEqual(payload["isPrecheck"], 1)

    def test_create_scheduled_task_without_product_uses_organic_video(self) -> None:
        client = Mock()
        client.commit_organic_video.return_value = {"id": 701}
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="789",
                key="videos/organic.mp4",
                bucket_name="demo-bucket",
            )
        )

        task_id = adapter.create_scheduled_task(
            account_id="39291",
            video_path="/tmp/organic.mp4",
            title="organic caption",
            publish_at=datetime(2026, 6, 29, 20, 0, 0),
            script_id="manual_rec_1",
            product_id="",
            mark_ai=False,
        )

        self.assertEqual(task_id, "neobund:701")
        client.commit_shoppable_video.assert_not_called()
        self.assertEqual(
            client.commit_organic_video.call_args.args[0],
            {
                "authId": 39291,
                "authType": 2,
                "videoTitle": "organic caption",
                "scheduledReleaseTime": "2026-06-29 20:00:00",
                "attachFileId": 789,
                "isPrecheck": 0,
                "remark": "manual_rec_1",
                "isAIGC": False,
            },
        )

    def test_create_scheduled_task_resolves_visible_username_to_auth_id(self) -> None:
        client = Mock()
        client.list_creator_accounts.return_value = {
            "records": [
                {
                    "authId": 39291,
                    "username": "user97605042600660",
                    "registerRegion": "TH",
                }
            ]
        }
        client.commit_shoppable_video.return_value = {"id": 992}
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="456",
                key="videos/demo.mp4",
                bucket_name="demo-bucket",
            )
        )

        adapter.create_scheduled_task(
            account_id="user97605042600660",
            video_path="/tmp/demo.mp4",
            title="username",
            publish_at=datetime(2026, 6, 29, 19, 0, 0),
            script_id="THFJ01.S1",
            product_id="7498614361651",
        )

        payload = client.commit_shoppable_video.call_args.args[0]
        self.assertEqual(payload["authId"], 39291)

    def test_create_organic_task_resolves_tiktok_username_to_auth_id(self) -> None:
        client = Mock()
        client.list_tiktok_accounts.return_value = {
            "records": [
                {
                    "id": 800,
                    "creatorUsername": "tubellezamas5",
                    "creatorNickname": "TuBellezaMas",
                }
            ]
        }
        client.commit_organic_video.return_value = {"id": 994}
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="456",
                key="videos/demo.mp4",
                bucket_name="demo-bucket",
            )
        )

        adapter.create_scheduled_task(
            account_id="tubellezamas5",
            video_path="/tmp/demo.mp4",
            title="organic username",
            publish_at=datetime(2026, 6, 29, 19, 0, 0),
            script_id="manual-rec",
            product_id="",
        )

        payload = client.commit_organic_video.call_args.args[0]
        self.assertEqual(payload["authId"], 800)
        self.assertEqual(payload["authType"], 2)
        client.list_creator_accounts.assert_not_called()

    def test_create_scheduled_task_looks_up_product_title_when_missing(self) -> None:
        client = Mock()
        client.list_products.return_value = {
            "records": [
                {
                    "id": "7498614361651",
                    "title": "กิ๊บหนีบผมใหญ่โลหะ สำหรับติดผมด้านหลัง",
                }
            ]
        }
        client.commit_shoppable_video.return_value = {"id": 993}
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="456",
                key="videos/demo.mp4",
                bucket_name="demo-bucket",
            )
        )

        adapter.create_scheduled_task(
            account_id="39291",
            video_path="/tmp/demo.mp4",
            title="title lookup",
            publish_at=datetime(2026, 6, 29, 19, 0, 0),
            script_id="THFJ01.S2",
            product_id="7498614361651",
        )

        payload = client.commit_shoppable_video.call_args.args[0]
        self.assertEqual(payload["productTitle"], "กิ๊บหนีบผมใหญ่โลหะ สำหรับติดผม")

    def test_create_scheduled_task_sanitizes_product_link_title_punctuation_and_emoji(self) -> None:
        client = Mock()
        client.list_products.return_value = {
            "records": [
                {
                    "id": "1730471644075886171",
                    "title": "Set 3 Cincin Anti Luntur | Bol 💍",
                }
            ]
        }
        client.commit_shoppable_video.return_value = {"id": 994}
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="456",
                key="videos/demo.mp4",
                bucket_name="demo-bucket",
            )
        )

        adapter.create_scheduled_task(
            account_id="40056",
            video_path="/tmp/demo.mp4",
            title="title sanitize",
            publish_at=datetime(2026, 6, 29, 19, 0, 0),
            script_id="775_M1_M",
            product_id="1730471644075886171",
        )

        payload = client.commit_shoppable_video.call_args.args[0]
        self.assertEqual(payload["productTitle"], "Set 3 Cincin Anti Luntur Bol")

    def test_create_scheduled_task_finds_task_id_when_commit_returns_empty_body(self) -> None:
        client = Mock()
        client.list_products.return_value = {"records": []}
        client.commit_shoppable_video.return_value = {}
        client.list_shoppable_videos.return_value = {
            "records": [
                {
                    "id": 623335,
                    "remark": "2337_M1_M",
                    "videoTitle": "มุมนี้ทำให้ของชิ้นนี้ดูน่ารักขึ้น",
                    "ttProductId": "1735933492508132405",
                    "scheduledReleaseTime": "2026-06-29 18:00:00",
                }
            ]
        }
        adapter = NeoBundPublishAdapter(client=client)
        adapter.upload_video = Mock(
            return_value=NeoBundUploadResult(
                file_id="1025246",
                key="videos/demo.mp4",
                bucket_name="demo-bucket",
            )
        )

        task_id = adapter.create_scheduled_task(
            account_id="39291",
            video_path="/tmp/demo.mp4",
            title="มุมนี้ทำให้ของชิ้นนี้ดูน่ารักขึ้น",
            publish_at=datetime(2026, 6, 29, 18, 0, 0),
            script_id="2337_M1_M",
            product_id="1735933492508132405",
        )

        self.assertEqual(task_id, "neobund:623335")

    def test_query_status_maps_neobund_numeric_states(self) -> None:
        client = Mock()
        adapter = NeoBundPublishAdapter(client=client)
        scheduled_for = datetime(2026, 6, 29, 18, 30, 0)

        client.list_shoppable_videos.return_value = {"records": [{"id": 1, "status": 120}]}
        pending = adapter.query_task_status(task_id="neobund:1", scheduled_for=scheduled_for)
        self.assertEqual(pending.state, "pending")

        client.list_shoppable_videos.return_value = {"records": [{"id": 1, "status": 350}]}
        success = adapter.query_task_status(task_id="neobund:1", scheduled_for=scheduled_for)
        self.assertEqual(success.state, "success")

        client.list_shoppable_videos.return_value = {"records": [{"id": 1, "status": 350, "errorMessage": "precheck failed"}]}
        failed = adapter.query_task_status(task_id="neobund:1", scheduled_for=scheduled_for)
        self.assertEqual(failed.state, "failed")
        self.assertEqual(failed.error_message, "precheck failed")

    def test_query_status_falls_back_to_organic_video_list(self) -> None:
        client = Mock()
        adapter = NeoBundPublishAdapter(client=client)
        scheduled_for = datetime(2026, 6, 29, 20, 0, 0)
        client.list_shoppable_videos.return_value = {"records": []}
        client.list_organic_videos.return_value = {"records": [{"id": 701, "status": 350}]}

        status = adapter.query_task_status(task_id="neobund:701", scheduled_for=scheduled_for)

        self.assertEqual(status.state, "success")
        client.list_shoppable_videos.assert_called_once_with({"id": 701})
        client.list_organic_videos.assert_called_once_with({"id": 701})

    def test_batch_status_skips_non_neobund_task_ids(self) -> None:
        client = Mock()
        adapter = NeoBundPublishAdapter(client=client)
        rows = [
            {"publish_task_id": "geelark-task-1", "scheduled_for": "2026-06-29 18:30:00"},
            {"publish_task_id": "neobund:2", "scheduled_for": "2026-06-29 19:30:00"},
        ]
        client.list_shoppable_videos.return_value = {"records": [{"id": 2, "status": 350}]}

        statuses = adapter.query_task_statuses(rows)

        self.assertEqual(statuses["geelark-task-1"].state, "pending")
        self.assertEqual(statuses["neobund:2"].state, "success")
        self.assertEqual(client.list_shoppable_videos.call_count, 1)


if __name__ == "__main__":
    unittest.main()
