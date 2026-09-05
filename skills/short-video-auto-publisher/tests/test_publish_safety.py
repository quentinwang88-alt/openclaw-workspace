from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
import json
import subprocess
import sys
import unittest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.neobund_publish import NeoBundPublishAdapter, NeoBundUploadResult, AmbiguousPublishError, PreCommitUploadError
from app.scheduler import schedule_slots, reconcile_submissions, sync_publish_results, _verify_live_opv_release, OpvReleaseVerificationError
from app.models import AccountConfig, PublishTaskStatus
import test_scheduler
from test_scheduler import RealishPublisher


class CommitSafetyTest(unittest.TestCase):
    def test_live_release_structured_errors_hide_secrets_and_classify_retry(self):
        for code, retryable in (("dependency_unavailable", True), ("release_mismatch", False),
                                ("verification_error", False)):
            with self.subTest(code=code), patch("app.scheduler.subprocess.run", return_value=Mock(
                returncode=1, stdout=json.dumps({"code": code, "retryable": retryable, "errno": 2003,
                                               "stage": "repository_import", "error_type": "ModuleNotFoundError"}),
                stderr="mysql://user:SECRET@host")):
                with self.assertRaises(OpvReleaseVerificationError) as raised:
                    _verify_live_opv_release("task", "hash")
                self.assertEqual(raised.exception.retryable, retryable)
                self.assertTrue(raised.exception.submission_not_sent)
                self.assertNotIn("SECRET", str(raised.exception))
                self.assertIn(code, str(raised.exception))
                self.assertIn("stage=repository_import", str(raised.exception))
                self.assertIn("error_type=ModuleNotFoundError", str(raised.exception))

    def test_live_release_read_is_bounded_and_fails_closed(self):
        with patch("app.scheduler.subprocess.run", side_effect=subprocess.TimeoutExpired("verify", 20)) as run:
            with self.assertRaisesRegex(ValueError, "未发送"):
                _verify_live_opv_release("task", "hash")
            self.assertEqual(run.call_args.kwargs["timeout"], 20)
        with patch("app.scheduler.subprocess.run", return_value=Mock(returncode=1, stdout="")):
            with self.assertRaises(ValueError):
                _verify_live_opv_release("task", "hash")
        with patch("app.scheduler.subprocess.run", return_value=Mock(returncode=0, stdout="release_verified\n")):
            _verify_live_opv_release("task", "hash")

    def adapter(self):
        client = Mock()
        adapter = NeoBundPublishAdapter(client=client, uploader=Mock())
        adapter.upload_video = Mock(return_value=NeoBundUploadResult("file-1", "key", "bucket"))
        return adapter, client

    def test_upload_ssl_failure_never_commits_and_is_safe_to_retry(self):
        for kind in ("organic", "shoppable"):
            adapter, client = self.adapter()
            adapter.upload_video.side_effect = requests.exceptions.SSLError("SSL EOF SECRET")
            create = getattr(adapter, f"_create_{kind}_scheduled_task")
            kwargs = {"product_id": "123"} if kind == "shoppable" else {}
            with self.subTest(kind=kind), self.assertRaises(PreCommitUploadError) as raised:
                create(auth_id="5250", video_path="/unused.mp4", title="title",
                       publish_at=datetime(2026, 9, 3, 12), script_id="unique", mark_ai=True, **kwargs)
            self.assertTrue(raised.exception.submission_not_sent)
            self.assertTrue(raised.exception.retryable)
            self.assertNotIn("SECRET", str(raised.exception))
            client.commit_organic_video.assert_not_called()
            client.commit_shoppable_video.assert_not_called()

    def test_upload_missing_file_does_not_retry(self):
        adapter, _ = self.adapter()
        adapter.upload_video.side_effect = FileNotFoundError("missing")
        with self.assertRaises(PreCommitUploadError) as raised:
            adapter._upload_before_commit("missing")
        self.assertFalse(raised.exception.retryable)

    def test_timeout_reconciles_without_a_second_commit(self):
        adapter, client = self.adapter()
        client.commit_organic_video.side_effect = TimeoutError("response lost")
        adapter._find_committed_task_id_with_retry = Mock(return_value="remote-1")
        task_id = adapter._create_organic_scheduled_task(
            auth_id="5250", video_path="/unused.mp4", title="same title",
            publish_at=datetime(2026, 9, 3, 12), script_id="unique-script", mark_ai=True,
        )
        self.assertEqual(task_id, "neobund:remote-1")
        client.commit_organic_video.assert_called_once()

    def test_unknown_result_is_ambiguous_even_when_lookup_fails(self):
        adapter, client = self.adapter()
        client.commit_organic_video.side_effect = TimeoutError("response lost")
        adapter._find_committed_task_id_with_retry = Mock(side_effect=ConnectionError("offline"))
        with self.assertRaises(AmbiguousPublishError):
            adapter._commit_once({"authId": 5250, "remark": "unique", "videoTitle": "title",
                                  "scheduledReleaseTime": "2026-09-03 12:00:00"}, content_type="organic")
        client.commit_organic_video.assert_called_once()

    def test_matching_title_does_not_reconcile_another_business_key(self):
        adapter, client = self.adapter()
        client.list_organic_videos.return_value = {"records": [{
            "id": "wrong", "remark": "another-script", "videoTitle": "same",
            "scheduledReleaseTime": "2026-09-03 12:00:00",
        }]}
        self.assertEqual(adapter._find_committed_task_id(
            auth_id="5250", script_id="wanted-script", video_title="same", product_id="",
            scheduled_for="2026-09-03 12:00:00", content_type="organic"), "")


class SchedulerSafetyTest(unittest.TestCase):
    setUp = test_scheduler.SchedulerTest.setUp
    tearDown = test_scheduler.SchedulerTest.tearDown
    _upsert_script = test_scheduler.SchedulerTest._upsert_script

    def test_upload_failure_retries_three_rounds_without_ambiguous_reservation(self):
        self._upsert_script("future", "P-1", "F-1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(side_effect=PreCommitUploadError(requests.exceptions.SSLError("EOF")))
        now = datetime(2026, 9, 3, 9)
        for hours in (0, 0, 1, 1, 3, 8, 24):
            schedule_slots(self.db, publisher, now=now + timedelta(hours=hours))
        self.assertEqual(publisher.create_scheduled_task.call_count, 3)
        self.assertEqual(self.db.list_unresolved_submissions(), [])
        key = self.db.get_script_metadata("future")["canonical_script_key"]
        self.assertFalse(self.db.candidate_retry_ready(key, now + timedelta(days=10)))

    def test_live_release_temporary_failure_retries_at_most_three_rounds_without_sending(self):
        self._upsert_script("future", "P-1", "F-1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock()
        now = datetime(2026, 9, 3, 9)
        with patch("app.scheduler._validate_opv_upload", side_effect=OpvReleaseVerificationError(
                "数据库暂时不可用", retryable=True)) as verify:
            for hours in (0, 0, 1, 1, 3, 8, 24):
                schedule_slots(self.db, publisher, now=now + timedelta(hours=hours))
            self.assertEqual(verify.call_count, 3)
        publisher.create_scheduled_task.assert_not_called()
        self.assertEqual(self.db.list_unresolved_submissions(), [])
        key = self.db.get_script_metadata("future")["canonical_script_key"]
        self.assertFalse(self.db.candidate_retry_ready(key, now + timedelta(days=10)))

    def test_live_release_mismatch_blocks_on_first_attempt_without_sending(self):
        self._upsert_script("future", "P-1", "F-1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock()
        with patch("app.scheduler._validate_opv_upload", side_effect=OpvReleaseVerificationError(
                "release mismatch")) as verify:
            for day in (3, 4):
                schedule_slots(self.db, publisher, now=datetime(2026, 9, day, 9))
            self.assertEqual(verify.call_count, 1)
        publisher.create_scheduled_task.assert_not_called()
        self.assertEqual(self.db.list_unresolved_submissions(), [])

    def test_unknown_remote_result_stays_reserved_across_scans(self):
        self._upsert_script("unique", "P-1", "F-1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(side_effect=AmbiguousPublishError("timeout"))
        now = datetime(2026, 9, 3, 11)
        first = schedule_slots(self.db, publisher, now=now, window_hours=2)
        self.assertEqual(first.scheduled, 0)
        unresolved = self.db.list_unresolved_submissions()
        self.assertEqual(len(unresolved), 1)
        context = json.loads(unresolved[0]["submission_context_json"])
        self.assertEqual(context["script_id"], "unique")
        schedule_slots(self.db, publisher, now=now, window_hours=2)
        publisher.create_scheduled_task.assert_called_once()
        self.assertEqual(len(self.db.list_unresolved_submissions()), 1)
        self._upsert_script("unique", "P-1", "F-1")
        self.assertEqual(self.db.get_video_asset(unresolved[0]["canonical_script_key"])["publish_status"], "提交中")
        publisher.reconcile_scheduled_task = Mock(return_value="neobund:resolved")
        stats = reconcile_submissions(self.db, publisher)
        self.assertEqual(stats["reconciled"], 1)
        self.assertEqual(self.db.list_unresolved_submissions(), [])
        publisher.create_scheduled_task.assert_called_once()

    def test_release_gate_failure_does_not_call_publisher(self):
        self._upsert_script("unique", "P-1", "F-1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(return_value="should-not-send")
        with patch("app.scheduler._validate_opv_upload", side_effect=ValueError("release hash mismatch")):
            schedule_slots(self.db, publisher, now=datetime(2026, 9, 3, 11), window_hours=2)
        publisher.create_scheduled_task.assert_not_called()

    def test_bgm_submission_is_created_a_day_ahead_and_frozen_across_runs(self):
        self._upsert_script("future", "P-1", "F-1")
        with self.db._connect() as conn:
            conn.execute("UPDATE account_configs SET publish_channel='NeoBund', capability_status='ok', shoppable_capable=1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(return_value="neobund:future")
        now = datetime(2026, 9, 3, 21)
        with patch("app.scheduler.bgm.requires_platform_bgm", return_value=True), patch(
            "app.scheduler.bgm.select_platform_bgm", return_value=({"music_id": "frozen"}, '{"music_id":"frozen"}')
        ) as select:
            first = schedule_slots(self.db, publisher, now=now)
            second = schedule_slots(self.db, publisher, now=now)
        self.assertEqual(first.scheduled, 1)
        self.assertEqual(second.scheduled, 0)
        self.assertEqual(publisher.create_scheduled_task.call_args.kwargs["publish_at"], datetime(2026, 9, 4, 12))
        publisher.create_scheduled_task.assert_called_once()
        select.assert_called_once()
        self.assertIn("frozen", self.db.list_scheduled_tasks()[0]["bgm_json"])

    def test_under_thirty_minutes_rolls_forward_without_immediate_post(self):
        self._upsert_script("future", "P-1", "F-1")
        publisher = RealishPublisher()
        schedule_slots(self.db, publisher, now=datetime(2026, 9, 3, 11, 31), window_hours=8)
        self.assertEqual(publisher.calls[0]["publish_at"], datetime(2026, 9, 3, 17))

    def test_confirmed_id_survives_assignment_failure_and_is_not_reposted(self):
        self._upsert_script("future", "P-1", "F-1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(return_value="neobund:confirmed")
        now = datetime(2026, 9, 3, 11)
        with patch.object(self.db, "assign_slot", side_effect=RuntimeError("local writeback unavailable")):
            schedule_slots(self.db, publisher, now=now, window_hours=2)
        row = self.db.list_unresolved_submissions()[0]
        self.assertEqual(json.loads(row["submission_context_json"])["confirmed_task_id"], "neobund:confirmed")
        self.assertEqual(row["schedule_status"], "提交中")
        schedule_slots(self.db, publisher, now=now, window_hours=2)
        self.assertEqual(self.db.list_unresolved_submissions(), [])
        self.assertEqual(self.db.list_scheduled_tasks()[0]["publish_task_id"], "neobund:confirmed")
        publisher.create_scheduled_task.assert_called_once()

    def test_unknown_error_is_reserved_and_never_automatically_retried(self):
        self._upsert_script("future", "P-1", "F-1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(side_effect=RuntimeError("unexpected SDK error after request"))
        now = datetime(2026, 9, 3, 11)
        schedule_slots(self.db, publisher, now=now, window_hours=2)
        schedule_slots(self.db, publisher, now=now, window_hours=2)
        self.assertEqual(len(self.db.list_unresolved_submissions()), 1)
        publisher.create_scheduled_task.assert_called_once()

    def test_auth_failure_pauses_shared_adapter_without_content_retry_penalty(self):
        self._upsert_script("future", "P-1", "F-1")
        self.db.upsert_account_configs([AccountConfig(
            account_id="acc-2", account_name="Second", store_id="SHOP-01", account_status="可用",
            publish_time_1="12:00", publish_time_2="17:00", publish_time_3="20:00",
        )])
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(side_effect=RuntimeError("invalid_token TOKEN_KICKED_BY_LOGIN"))
        schedule_slots(self.db, publisher, now=datetime(2026, 9, 3, 11))
        publisher.create_scheduled_task.assert_called_once()
        self.assertEqual(self.db.list_unresolved_submissions(), [])
        with self.db._connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM publish_candidate_retries").fetchone()[0], 0)

    def test_bgm_retries_three_rounds_then_stays_blocked_until_explicit_reset(self):
        self._upsert_script("future", "P-1", "F-1")
        with self.db._connect() as conn:
            conn.execute("UPDATE account_configs SET publish_channel='NeoBund', capability_status='ok', shoppable_capable=1")
        publisher = RealishPublisher()
        now = datetime(2026, 9, 3, 9)
        with patch("app.scheduler.bgm.requires_platform_bgm", return_value=True), patch(
            "app.scheduler.bgm.select_platform_bgm", side_effect=RuntimeError("audio candidate temporarily unavailable")
        ) as select:
            for hours in (0, 0, 1, 1, 3, 8, 24):
                schedule_slots(self.db, publisher, now=now + timedelta(hours=hours))
            self.assertEqual(select.call_count, 3)
        key = self.db.get_script_metadata("future")["canonical_script_key"]
        self.assertFalse(self.db.candidate_retry_ready(key, now + timedelta(days=10)))
        self.assertIn("已停止自动重试", self.db.get_video_asset("future")["error_message"])
        self.assertIn("BGM选择失败", self.db.get_video_asset("future")["error_message"])
        self.db.reset_candidate_retry(key)
        self.assertTrue(self.db.candidate_retry_ready(key, now + timedelta(days=10)))
        self.assertIsNone(self.db.get_video_asset("future")["error_message"])

    def test_single_status_query_error_does_not_hide_later_success(self):
        self._upsert_script("first", "P-1", "F-1")
        self._upsert_script("second", "P-2", "F-2")
        publisher = RealishPublisher()
        schedule_slots(self.db, publisher, now=datetime(2026, 9, 3, 11))
        publisher.query_task_status = Mock(side_effect=[
            RuntimeError("historic malformed response"), PublishTaskStatus(state="success", result="ok"),
        ])
        result = sync_publish_results(self.db, publisher)
        self.assertEqual(result["errors"], 1)
        self.assertEqual(result["published"], 1)
        self.assertEqual(result["pending"], 1)
        self.assertEqual(self.db.get_video_asset("first")["publish_status"], "已排期")

    def test_bad_bgm_candidate_does_not_block_other_content_on_same_account(self):
        self._upsert_script("a-bad", "P-1", "F-1")
        self._upsert_script("b-good", "P-2", "F-2")
        with self.db._connect() as conn:
            conn.execute("UPDATE account_configs SET publish_channel='NeoBund', capability_status='ok', shoppable_capable=1")
        publisher = RealishPublisher()
        publisher.create_scheduled_task = Mock(return_value="neobund:good")
        with patch("app.scheduler.bgm.requires_platform_bgm", return_value=True), patch(
            "app.scheduler.bgm.select_platform_bgm", side_effect=[ValueError("bad media duration"), ({"music_id": "good"}, "{}")]
        ) as select:
            stats = schedule_slots(self.db, publisher, now=datetime(2026, 9, 3, 11))
            self.assertEqual(stats.scheduled, 1)
            self.assertEqual(select.call_count, 2)
            self.assertEqual(publisher.create_scheduled_task.call_args.kwargs["script_id"], "b-good")
            schedule_slots(self.db, publisher, now=datetime(2026, 9, 5, 11))
            self.assertEqual(select.call_count, 2)
        key = self.db.get_script_metadata("a-bad")["canonical_script_key"]
        self.assertFalse(self.db.candidate_retry_ready(key, datetime(2027, 1, 1)))

    def test_result_writeback_failure_does_not_abort_other_tasks(self):
        self._upsert_script("first", "P-1", "F-1")
        self._upsert_script("second", "P-2", "F-2")
        publisher = RealishPublisher()
        schedule_slots(self.db, publisher, now=datetime(2026, 9, 3, 11))
        publisher.query_task_status = Mock(return_value=PublishTaskStatus(state="success", result="ok"))
        original = self.db.mark_publish_result
        def mark(**kwargs):
            if kwargs["script_id"] == "first":
                raise RuntimeError("single-row local error")
            return original(**kwargs)
        with patch.object(self.db, "mark_publish_result", side_effect=mark):
            stats = sync_publish_results(self.db, publisher)
        self.assertEqual(stats["errors"], 1)
        self.assertEqual(stats["published"], 1)
        self.assertEqual(self.db.get_video_asset("first")["publish_status"], "已排期")
