#!/usr/bin/env python3
"""CreatOK 发布适配器测试。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.creatok_publish import (  # noqa: E402
    CREATOK_CLI_ENV,
    CREATOK_SUPPORTED_TIMEZONES,
    CreatOKAuthError,
    CreatOKCLI,
    CreatOKEnvelopeBuilder,
    CreatOKError,
    CreatOKPublishAdapter,
    CreatOKPreSubmitError,
    CreatOKResultUnknownError,
    CreatOKSubmissionNotReadyError,
    _sanitize_error_text,
    create_idempotency_key,
    parse_task_status,
    validate_content_photo_metadata,
)
from app.models import PublishRequest, PublishTaskStatus  # noqa: E402
from app.publishers import RoutedPublishAdapter, DryRunPublishAdapter  # noqa: E402


def _request(**overrides) -> PublishRequest:
    base = dict(
        account_id="acct-1",
        content_type="video",
        commerce_type="organic",
        media_paths=["/tmp/video.mp4"],
        title="标题",
        publish_at=datetime(2026, 9, 10, 12, 0, 0),
        timezone="Asia/Bangkok",
        script_id="S-1",
    )
    base.update(overrides)
    return PublishRequest(**base)


def _cli_payload(payload: dict) -> dict:
    return {"ok": True, "status": "succeeded", "command": "publish", "data": payload}


class TaskStatusMappingTest(unittest.TestCase):
    def test_ready_states_map_to_scheduled(self) -> None:
        for state in ("queued", "scheduled", "processing"):
            status = parse_task_status(_cli_payload({"job": {"status": state}}))
            self.assertEqual(status.state, "pending")
            self.assertEqual(status.result, "已排期")

    def test_waiting_confirmation_maps_to_pending_confirmation(self) -> None:
        status = parse_task_status(_cli_payload({"job": {"status": "waiting_confirmation"}}))
        self.assertEqual(status.state, "pending_confirmation")
        self.assertEqual(status.result, "待账号确认")

    def test_succeeded_maps_to_published(self) -> None:
        status = parse_task_status(_cli_payload({"job": {"status": "succeeded", "published_at": "2026-09-10T12:00:00Z"}}))
        self.assertEqual(status.state, "success")
        self.assertEqual(status.result, "发布成功")

    def test_failed_without_result_unknown_maps_to_failed(self) -> None:
        payload = {
            "ok": False,
            "status": "failed",
            "error": {"reason": "rejected", "result_unknown": False},
            "data": {"job": {"status": "failed"}},
        }
        status = parse_task_status(payload)
        self.assertEqual(status.state, "failed")
        self.assertEqual(status.result, "发布失败")

    def test_result_unknown_never_maps_to_failed(self) -> None:
        payload = {
            "ok": False,
            "status": "failed",
            "error": {"reason": "timeout", "result_unknown": True},
            "data": {"job": {"status": "failed", "result_unknown": True}},
        }
        status = parse_task_status(payload)
        self.assertEqual(status.state, "unknown")

    def test_canceled_maps_to_terminated(self) -> None:
        status = parse_task_status(_cli_payload({"job": {"status": "canceled"}}))
        self.assertEqual(status.state, "terminated")
        self.assertEqual(status.result, "已取消")


class IdempotencyKeyTest(unittest.TestCase):
    def test_key_is_stable_for_same_request(self) -> None:
        key_a = create_idempotency_key(_request())
        key_b = create_idempotency_key(_request())
        self.assertEqual(key_a, key_b)
        self.assertEqual(len(key_a), 64)

    def test_key_changes_with_media(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = Path(tmpdir) / "a.mp4"
            f2 = Path(tmpdir) / "b.mp4"
            f1.write_bytes(b"abc")
            f2.write_bytes(b"abd")
            key_a = create_idempotency_key(_request(media_paths=[str(f1)]))
            key_b = create_idempotency_key(_request(media_paths=[str(f2)]))
            self.assertNotEqual(key_a, key_b)

    def test_key_changes_with_copy_and_music_decision(self) -> None:
        baseline = create_idempotency_key(_request())
        self.assertNotEqual(baseline, create_idempotency_key(_request(title="另一个标题")))
        self.assertNotEqual(baseline, create_idempotency_key(_request(description="正文")))
        self.assertNotEqual(baseline, create_idempotency_key(_request(auto_add_music=True)))
        self.assertNotEqual(
            baseline,
            create_idempotency_key(_request(music_selection={"mode": "local_mix", "music_id": "LOCAL_1"})),
        )

    def test_key_is_deterministic_across_processes(self) -> None:
        # 通过子进程验证相同输入得到相同 key（崩溃恢复的前提）。
        code = (
            "import sys; sys.path.insert(0, r'%s');"
            "from app.models import PublishRequest;"
            "from app.creatok_publish import create_idempotency_key;"
            "from datetime import datetime;"
            "r = PublishRequest(account_id='acct-1', content_type='video', commerce_type='organic',"
            "media_paths=['/tmp/video.mp4'], title='标题', publish_at=datetime(2026,9,10,12,0,0),"
            "timezone='Asia/Bangkok', script_id='S-1');"
            "print(create_idempotency_key(r))" % str(SKILL_DIR)
        )
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        self.assertEqual(out.stdout.strip(), create_idempotency_key(_request()))


class SubmissionContextTest(unittest.TestCase):
    def test_context_freezes_identity_before_submit(self) -> None:
        adapter = CreatOKPublishAdapter(account_connections={"acct-1": "conn-uid-1"})
        context = adapter.build_submission_context(_request())
        self.assertEqual(context["provider"], "CreatOK")
        self.assertEqual(context["connection_uid"], "conn-uid-1")
        self.assertEqual(context["content_type"], "video")
        self.assertEqual(context["commerce_type"], "organic")
        self.assertEqual(context["timezone"], "Asia/Bangkok")
        self.assertEqual(len(context["operation_id"]), 32)
        self.assertEqual(context["idempotency_key"], create_idempotency_key(_request()))
        self.assertTrue(context["media_sha256"])

    def test_context_reuses_same_operation_for_reserved_request(self) -> None:
        adapter = CreatOKPublishAdapter(account_connections={"acct-1": "conn-uid-1"})
        first = adapter.build_submission_context(_request())
        second = adapter.build_submission_context(_request())
        self.assertEqual(first["operation_id"], second["operation_id"])
        self.assertEqual(first["idempotency_key"], second["idempotency_key"])

    def test_photo_context_records_auto_music_and_aigc_transport_gap(self) -> None:
        adapter = CreatOKPublishAdapter(account_connections={"acct-1": "conn-uid-1"})
        context = adapter.build_submission_context(_request(
            content_type="photo", auto_add_music=True, mark_ai=True,
        ))
        self.assertEqual(context["music_mode"], "platform_auto")
        self.assertTrue(context["ai_generated"])
        self.assertEqual(
            context["provider_ai_label_status"],
            "unsupported_by_creatok_v0.13.0",
        )

    def test_video_context_records_local_mix(self) -> None:
        adapter = CreatOKPublishAdapter(account_connections={"acct-1": "conn-uid-1"})
        context = adapter.build_submission_context(_request(
            music_selection={"mode": "local_mix", "music_id": "LOCAL_1"},
        ))
        self.assertEqual(context["music_mode"], "local_mix")
        self.assertEqual(context["music_selection"]["music_id"], "LOCAL_1")


class SubmitGateTest(unittest.TestCase):
    def test_create_publish_task_refuses_without_fixture_contract(self) -> None:
        adapter = CreatOKPublishAdapter(account_connections={"acct-1": "conn-uid-1"})
        with self.assertRaises(CreatOKSubmissionNotReadyError) as ctx:
            adapter.create_publish_task(_request())
        self.assertTrue(ctx.exception.submission_not_sent)
        self.assertTrue(ctx.exception.retryable)

    def test_create_publish_task_refuses_without_connection_uid(self) -> None:
        adapter = CreatOKPublishAdapter()
        with self.assertRaises(CreatOKError):
            adapter.create_publish_task(_request())

    def test_submit_ok_payload_never_leaks_key(self) -> None:
        adapter = CreatOKPublishAdapter(account_connections={"acct-1": "conn-uid-1"})
        with patch.object(adapter.cli, "run", return_value={"ok": True, "status": "succeeded", "data": {"job_id": "j-1"}}):
            with patch("app.creatok_publish.uuid.uuid4", return_value=Mock(hex="a" * 32)):
                # envelope builder 未接入时依然拒绝提交，确保未获契约前不会发出真实发布。
                with self.assertRaises(CreatOKSubmissionNotReadyError):
                    adapter.create_publish_task(_request())


class QueryStatusTest(unittest.TestCase):
    def test_query_strips_prefix_and_parses(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({"job": {"status": "succeeded"}}))
        status = adapter.query_task_status(task_id="creatok:job-123", scheduled_for=datetime(2026, 9, 10))
        self.assertEqual(status.state, "success")
        adapter.cli.run.assert_called_once()
        self.assertEqual(adapter.cli.run.call_args.args[0][:4], ["publish", "job", "get", "job-123"])

    def test_query_statuses_skips_foreign_prefixes(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({"job": {"status": "succeeded"}}))
        statuses = adapter.query_task_statuses([
            {"publish_task_id": "neobund:123", "scheduled_for": "2026-09-10 12:00:00"},
            {"publish_task_id": "creatok:456", "scheduled_for": "2026-09-10 12:00:00"},
        ])
        self.assertEqual(statuses["neobund:123"].state, "pending")
        self.assertEqual(statuses["creatok:456"].state, "success")


class ReconcileTest(unittest.TestCase):
    def test_reconcile_uses_idempotency_key_and_returns_prefixed_id(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({"job_id": "job-9"}))
        task_id = adapter.reconcile_scheduled_task({"idempotency_key": "k-1", "operation_id": "op-1"})
        self.assertEqual(task_id, "creatok:job-9")
        args = adapter.cli.run.call_args.args[0]
        self.assertIn("--idempotency-key", args)
        self.assertIn("k-1", args)

    def test_reconcile_without_job_returns_empty(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({}))
        task_id = adapter.reconcile_scheduled_task({"idempotency_key": "k-1"})
        self.assertEqual(task_id, "")

    def test_reconcile_without_key_returns_empty_without_calling_cli(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock()
        task_id = adapter.reconcile_scheduled_task({})
        self.assertEqual(task_id, "")
        adapter.cli.run.assert_not_called()


class CLIRunnerTest(unittest.TestCase):
    def test_key_only_via_env_not_argv(self) -> None:
        fake = subprocess.CompletedProcess(["creatok"], 0, '{"ok": true, "status": "succeeded"}', "")
        runner = CreatOKCLI(api_key_env_name="CREATOK_API_KEY_TEST_ALIAS")
        with patch.dict(os.environ, {"CREATOK_API_KEY_TEST_ALIAS": "ok_secret_key_value"}):
            with patch("app.creatok_publish.subprocess.run", return_value=fake) as run_mock:
                payload = runner.run(["doctor"])
        self.assertTrue(payload["ok"])
        command_args = run_mock.call_args.args[0]
        joined = " ".join(command_args)
        self.assertNotIn("ok_secret_key_value", joined)
        env = run_mock.call_args.kwargs["env"]
        self.assertEqual(env.get(CREATOK_CLI_ENV), "ok_secret_key_value")
        self.assertFalse(run_mock.call_args.kwargs.get("shell", True))

    def test_missing_key_raises_auth_error(self) -> None:
        runner = CreatOKCLI(api_key_env_name="CREATOK_API_KEY_DOES_NOT_EXIST")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(CreatOKAuthError):
                runner.run(["doctor"])

    def test_cli_error_is_sanitized(self) -> None:
        fake = subprocess.CompletedProcess(
            ["creatok"], 1,
            '{"ok": false, "status": "failed", "error": {"kind": "server", "message": "upstream boom", "request_id": "req-1"}}',
            "",
        )
        runner = CreatOKCLI(api_key_env_name="CREATOK_API_KEY_TEST_ALIAS")
        with patch.dict(os.environ, {"CREATOK_API_KEY_TEST_ALIAS": "ok_secret_key_value"}):
            with patch("app.creatok_publish.subprocess.run", return_value=fake):
                with self.assertRaises(CreatOKError) as ctx:
                    runner.run(["publish", "connection", "capabilities"])
        message = str(ctx.exception)
        self.assertNotIn("ok_secret_key_value", message)
        self.assertIn("req-1", message)

    def test_result_unknown_raises_ambiguous(self) -> None:
        fake = subprocess.CompletedProcess(
            ["creatok"], 1,
            '{"ok": false, "status": "failed", "error": {"kind": "server", "message": "timeout", "result_unknown": true}}',
            "",
        )
        runner = CreatOKCLI(api_key_env_name="CREATOK_API_KEY_TEST_ALIAS")
        with patch.dict(os.environ, {"CREATOK_API_KEY_TEST_ALIAS": "ok_secret_key_value"}):
            with patch("app.creatok_publish.subprocess.run", return_value=fake):
                with self.assertRaises(CreatOKResultUnknownError):
                    runner.run(["publish", "job", "submit"])

    def test_timeout_raises_ambiguous(self) -> None:
        runner = CreatOKCLI(api_key_env_name="CREATOK_API_KEY_TEST_ALIAS")
        with patch.dict(os.environ, {"CREATOK_API_KEY_TEST_ALIAS": "ok_secret_key_value"}):
            with patch("app.creatok_publish.subprocess.run", side_effect=subprocess.TimeoutExpired(["creatok"], 10)):
                with self.assertRaises(CreatOKResultUnknownError):
                    runner.run(["publish", "job", "submit"])

    def test_sanitize_error_text_strips_keys(self) -> None:
        long_token = "abcDEF0123456789XYZ" + "ABC0123456789"
        text = _sanitize_error_text(f"failed with ok_secret_key_value and token={long_token}")
        self.assertNotIn("ok_secret_key_value", text)
        self.assertNotIn(long_token, text)


class UploadAssetTest(unittest.TestCase):
    def test_upload_asset_returns_object_key(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({"asset": {"uid": "uid-1", "object_key": "uploads/videos/v.mp4"}}))
        key = adapter.upload_asset(asset_type="video", file_path="/tmp/v.mp4")
        self.assertEqual(key, "uploads/videos/v.mp4")
        args = adapter.cli.run.call_args.args[0]
        self.assertEqual(args[:4], ["assets", "create", "--type", "video"])
        self.assertNotIn("--operation-id", args)

    def test_resolve_asset_media_url_uses_signed_url_from_list(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({
            "items": [
                {"uid": "FVqy71brQUtYh2jGMFQGt", "media_url": "https://content.creatok.com/uploads/v.mp4?sign=abc"},
            ]
        }))
        url = adapter.resolve_asset_media_url("FVqy71brQUtYh2jGMFQGt")
        self.assertEqual(url, "https://content.creatok.com/uploads/v.mp4?sign=abc")

    def test_resolve_asset_media_url_missing_raises(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({"items": []}))
        with self.assertRaises(CreatOKError):
            adapter.resolve_asset_media_url("nope")

    def test_upload_asset_rejects_unknown_type(self) -> None:
        adapter = CreatOKPublishAdapter()
        with self.assertRaises(CreatOKError):
            adapter.upload_asset(asset_type="audio", file_path="/tmp/a.mp3")

    def test_verified_upload_records_asset_uid_and_exact_byte_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "v.mp4"
            path.write_bytes(b"video-bytes")
            adapter = CreatOKPublishAdapter(asset_verifier=lambda uid, local: {
                "upload_verified": True,
                "verified_sha256": hashlib.sha256(Path(local).read_bytes()).hexdigest(),
                "verified_size_bytes": Path(local).stat().st_size,
            })
            adapter.cli.run = Mock(return_value=_cli_payload({
                "asset": {"uid": "uid-verified", "object_key": "uploads/videos/v.mp4"}
            }))
            details = adapter.upload_asset_details(
                asset_type="video", file_path=str(path), verify=True,
            )
            self.assertEqual(details["asset_uid"], "uid-verified")
            self.assertTrue(details["upload_verified"])
            self.assertEqual(details["local_sha256"], details["verified_sha256"])

    def test_prepare_shop_video_passes_asset_json(self) -> None:
        adapter = CreatOKPublishAdapter()
        adapter.cli.run = Mock(return_value=_cli_payload({"file_id": "fid-1"}))
        file_id = adapter.prepare_shop_video(
            connection_uid="conn-1",
            asset_json={"object_key": "media/abc", "file_size": 10, "file_name": "v.mp4"},
        )
        self.assertEqual(file_id, "fid-1")
        self.assertEqual(adapter.cli.run.call_args.args[0][:5], ["publish", "video", "prepare", "--connection", "conn-1"])


class RoutingTest(unittest.TestCase):
    def test_routed_adapter_dispatches_creatok_requests(self) -> None:
        creatok = CreatOKPublishAdapter(account_connections={"acct-1": "conn-1"})
        routed = RoutedPublishAdapter(
            default_adapter=DryRunPublishAdapter(),
            channel_adapters={"CreatOK": creatok},
            account_channels={"acct-1": "CreatOK"},
            task_prefix_adapters={"creatok:": creatok},
            default_channel="GeeLark",
        )
        with self.assertRaises(CreatOKSubmissionNotReadyError):
            routed.create_publish_task(_request())

    def test_routed_adapter_keeps_legacy_channels_on_old_path(self) -> None:
        dryrun = DryRunPublishAdapter()
        routed = RoutedPublishAdapter(
            default_adapter=dryrun,
            channel_adapters={},
            account_channels={"acct-1": "GeeLark"},
            task_prefix_adapters={},
            default_channel="GeeLark",
        )
        task_id = routed.create_publish_task(_request())
        self.assertTrue(task_id.startswith("dryrun-"))

    def test_routed_adapter_normalizes_creatok_channel(self) -> None:
        creatok = CreatOKPublishAdapter(account_connections={"acct-1": "conn-1"})
        routed = RoutedPublishAdapter(
            default_adapter=DryRunPublishAdapter(),
            channel_adapters={"CreatOK": creatok},
            account_channels={"acct-1": "creatok"},
            task_prefix_adapters={},
            default_channel="GeeLark",
        )
        self.assertEqual(routed._normalize_channel("creatok"), "CreatOK")
        self.assertEqual(routed._normalize_channel("CreatOK"), "CreatOK")


class EnvelopeBuilderTest(unittest.TestCase):
    """基于正式账号采集的官方 fixture 回归（CLI v0.13.0）。"""

    def test_shop_video_envelope_matches_official_fixture(self) -> None:
        fixture = json.loads(
            (TESTS_DIR / "fixtures" / "creatok_envelopes" / "shop_video_envelope_valid.json").read_text()
        )
        fixture.pop("_comment", None)
        builder = CreatOKEnvelopeBuilder()
        request = _request(
            commerce_type="shop",
            product_id="1736519949980763739",
            product_title="CreatOK fixture test ring",
            timezone="GMT+8",
            title="CreatOK fixture sample",
        )
        envelope = builder.build_shop_video_envelope(
            request=request,
            connection_uid="conn_ts_4zDg4wT0Wd9L82ysizOb66",
            operation_id="op-ignored",
            idempotency_key="probe-layer5-0001",
            file_id="v14d00g50024dadpr0f0g65u58m5rpo0",
        )
        self.assertEqual(envelope, fixture)

    def test_shop_video_envelope_rejects_iana_timezone(self) -> None:
        builder = CreatOKEnvelopeBuilder()
        with self.assertRaises(CreatOKError):
            builder.build_shop_video_envelope(
                request=_request(commerce_type="shop", product_id="p-1", timezone="Asia/Bangkok"),
                connection_uid="conn-1",
                operation_id="op",
                idempotency_key="k",
                file_id="fid",
            )

    def test_shop_video_envelope_requires_product_id(self) -> None:
        builder = CreatOKEnvelopeBuilder()
        with self.assertRaises(CreatOKError):
            builder.build_shop_video_envelope(
                request=_request(commerce_type="shop", timezone="GMT+8"),
                connection_uid="conn-1",
                operation_id="op",
                idempotency_key="k",
                file_id="fid",
            )

    def test_supported_timezones_match_fixture(self) -> None:
        self.assertIn("GMT+8", CREATOK_SUPPORTED_TIMEZONES)

    def test_job_failed_fixture_parses_as_definite_failure(self) -> None:
        fixture = json.loads(
            (TESTS_DIR / "fixtures" / "creatok_envelopes" / "shop_video_job_failed_precheck.json").read_text()
        )
        status = parse_task_status(fixture)
        self.assertEqual(status.state, "failed")
        self.assertIn("Video id is invalid", status.error_message)

    def test_job_failed_fixture_is_compatible_with_status_endpoint(self) -> None:
        # status --job-id 与 submit 响应共享 data.job 结构，reconcile 解析路径一致。
        fixture = json.loads(
            (TESTS_DIR / "fixtures" / "creatok_envelopes" / "shop_video_job_failed_precheck.json").read_text()
        )
        self.assertEqual(fixture["data"]["job"]["result_unknown"], False)

    def test_content_video_envelope_matches_official_fixture(self) -> None:
        fixture = json.loads(
            (TESTS_DIR / "fixtures" / "creatok_envelopes" / "content_video_envelope_valid.json").read_text()
        )
        fixture.pop("_comment", None)
        builder = CreatOKEnvelopeBuilder()
        request = _request(timezone="GMT+8", delivery_mode="direct_post")
        envelope = builder.build_content_video_envelope(
            request=request,
            connection_uid="conn_tc_3cWjrL84udzEWMsixHyspt",
            operation_id="op",
            idempotency_key="probe-org-l4",
            object_key="uploads/videos/k0b14Ru6nnpg0dXZ47QJg59tmrbtp5rA/1788583431115_pniqr6e2ru8.mp4",
            file_name="creatok_fixture_test2.mp4",
        )
        self.assertEqual(envelope, fixture)

    def test_content_video_envelope_supports_schedule_object(self) -> None:
        builder = CreatOKEnvelopeBuilder()
        request = _request(timezone="GMT+8", delivery_mode="direct_post")
        envelope = builder.build_content_video_envelope(
            request=request,
            connection_uid="conn-1",
            operation_id="op",
            idempotency_key="k",
            object_key="obj",
            file_name="f.mp4",
            schedule_at_ms=1789015457219,
            schedule_timezone="GMT+8",
        )
        self.assertEqual(envelope["schedule"], {"at": 1789015457219, "timezone": "GMT+8"})

    def test_publish_at_epoch_ms_uses_account_iana_timezone(self) -> None:
        from datetime import datetime
        from app.creatok_publish import publish_at_epoch_ms
        naive = datetime(2026, 9, 6, 15, 0, 0)
        ms = publish_at_epoch_ms(naive, "Asia/Bangkok")
        # Bangkok 15:00 = UTC 08:00
        self.assertEqual(ms, int(datetime(2026, 9, 6, 8, 0, 0, tzinfo=__import__("zoneinfo").ZoneInfo("UTC")).timestamp() * 1000))

    def test_creatok_schedule_timezone_maps_asia_to_gmt8(self) -> None:
        from app.creatok_publish import creatok_schedule_timezone
        self.assertEqual(creatok_schedule_timezone("Asia/Bangkok"), "GMT+8")
        self.assertEqual(creatok_schedule_timezone("Asia/Kuala_Lumpur"), "GMT+8")
        self.assertEqual(creatok_schedule_timezone("PST"), "PST")
        self.assertEqual(creatok_schedule_timezone(""), "GMT+8")

    def test_content_photo_envelope_matches_official_fixture(self) -> None:
        fixture = json.loads(
            (TESTS_DIR / "fixtures" / "creatok_envelopes" / "content_photo_envelope_valid.json").read_text()
        )
        fixture.pop("_comment", None)
        builder = CreatOKEnvelopeBuilder()
        request = _request(content_type="photo", timezone="GMT+8", delivery_mode="direct_post")
        envelope = builder.build_content_photo_envelope(
            request=request,
            connection_uid="conn_tc_3cWjrL84udzEWMsixHyspt",
            operation_id="op",
            idempotency_key="probe-photo-l2",
            object_keys=["uploads/images/k0b14Ru6nnpg0dXZ47QJg59tmrbtp5rA/1788588177317_dpmb63lv4q4.png"],
        )
        self.assertEqual(envelope, fixture)

    def test_content_photo_envelope_requires_object_keys(self) -> None:
        builder = CreatOKEnvelopeBuilder()
        with self.assertRaises(CreatOKError):
            builder.build_content_photo_envelope(
                request=_request(content_type="photo"),
                connection_uid="conn-1",
                operation_id="op",
                idempotency_key="k",
                object_keys=[],
            )

    def test_content_photo_envelope_uses_request_auto_music(self) -> None:
        envelope = CreatOKEnvelopeBuilder().build_content_photo_envelope(
            request=_request(content_type="photo", auto_add_music=True),
            connection_uid="conn-1", operation_id="op", idempotency_key="k",
            object_keys=["uploads/images/a.jpg"],
        )
        self.assertTrue(envelope["provider_options"]["auto_add_music"])

    def test_photo_metadata_rejects_utf16_limits_before_upload(self) -> None:
        with self.assertRaises(CreatOKPreSubmitError):
            validate_content_photo_metadata(_request(content_type="photo", title="😀" * 46))
        with self.assertRaises(CreatOKPreSubmitError):
            validate_content_photo_metadata(_request(content_type="photo", description="😀" * 2001))

    def test_job_running_fixture_parses_as_scheduled(self) -> None:
        fixture = json.loads(
            (TESTS_DIR / "fixtures" / "creatok_envelopes" / "content_video_job_running.json").read_text()
        )
        status = parse_task_status(fixture)
        self.assertEqual(status.state, "pending")
        self.assertEqual(status.result, "已排期")


class SubmitFlowTest(unittest.TestCase):
    """完整提交流程（mock CLI）：upload → resolve → envelope → submit → creatok:<job_id>。"""

    @staticmethod
    def _future_time() -> datetime:
        from datetime import timedelta
        return datetime.now().replace(microsecond=0) + timedelta(days=3)

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.video_path = str(Path(self.temp.name) / "video.mp4")
        Path(self.video_path).write_bytes(b"unit-test-video")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _adapter(self) -> CreatOKPublishAdapter:
        return CreatOKPublishAdapter(
            account_connections={"acct-1": "conn-1"},
            video_preparer=lambda path: {
                "path": path, "original_path": path,
                "original_sha256": "original", "upload_sha256": "upload",
                "compatibility_profile": "test",
            },
            asset_verifier=lambda uid, path: {
                "upload_verified": True, "verified_sha256": "upload",
                "verified_size_bytes": Path(path).stat().st_size,
            },
        )

    def test_organic_video_full_flow_returns_prefixed_job_id(self) -> None:
        adapter = self._adapter()
        captured: dict = {}

        def recorder(args):
            if args[:3] == ["publish", "job", "submit"]:
                captured["envelope"] = json.loads(Path(args[args.index("--file") + 1]).read_text())
                return _cli_payload({"created": True, "job": {"job_id": 349999, "state": "queued"}})
            if args[:2] == ["assets", "create"]:
                return _cli_payload({"asset": {"uid": "uid-1", "object_key": "uploads/videos/v.mp4"}})
            raise AssertionError(f"意外的 CLI 调用: {args[:2]}")

        adapter.cli.run = Mock(side_effect=recorder)
        request = _request(media_paths=[self.video_path], publish_at=self._future_time())
        task_id = adapter.create_publish_task(request)
        self.assertEqual(task_id, "creatok:349999")
        envelope = captured["envelope"]
        self.assertEqual(envelope["kind"], "tiktok_content_video")
        self.assertEqual(envelope["media"]["object_key"], "uploads/videos/v.mp4")
        self.assertEqual(envelope["media"]["file_name"], "video.mp4")
        receipt = adapter.submission_receipt(create_idempotency_key(request))
        self.assertTrue(receipt["upload_assets"][0]["upload_verified"])
        self.assertEqual(receipt["compatibility_media"]["compatibility_profile"], "test")
        self.assertEqual(receipt["creatok_job_id"], "349999")
        self.assertEqual(
            receipt["actual_operation_id"],
            adapter.build_submission_context(request)["operation_id"],
        )
        from app.creatok_publish import publish_at_epoch_ms
        self.assertEqual(envelope["schedule"], {
            "at": publish_at_epoch_ms(request.publish_at, request.timezone),
            "timezone": "GMT+8",
        })
        self.assertEqual(envelope["provider_options"]["post_mode"], "DIRECT_POST")
        submit_args = next(call.args[0] for call in adapter.cli.run.call_args_list if call.args[0][:3] == ["publish", "job", "submit"])
        self.assertIn("--operation-id", submit_args)

    def test_organic_photo_full_flow_uploads_each_image(self) -> None:
        adapter = self._adapter()
        captured: dict = {}

        def recorder(args):
            if args[:3] == ["publish", "job", "submit"]:
                captured["envelope"] = json.loads(Path(args[args.index("--file") + 1]).read_text())
                return _cli_payload({"created": True, "job": {"job_id": 350001, "state": "queued"}})
            if args[:2] == ["assets", "create"]:
                return _cli_payload({"asset": {"uid": f"uid-{args[-1]}", "object_key": f"uploads/images/{args[-1]}"}})
            raise AssertionError(f"意外的 CLI 调用: {args[:2]}")

        adapter.cli.run = Mock(side_effect=recorder)
        request = _request(
            content_type="photo",
            media_paths=["/tmp/a.png", "/tmp/b.png"],
            description="正文 #标签",
            publish_at=self._future_time(),
        )
        task_id = adapter.create_publish_task(request)
        self.assertEqual(task_id, "creatok:350001")
        envelope = captured["envelope"]
        self.assertEqual(envelope["kind"], "tiktok_content_photo")
        self.assertEqual(envelope["media"]["object_keys"], [
            "uploads/images//tmp/a.png",
            "uploads/images//tmp/b.png",
        ])
        self.assertEqual(envelope["provider_options"]["title"], "标题")
        self.assertEqual(envelope["provider_options"]["description"], "正文 #标签")
        from app.creatok_publish import publish_at_epoch_ms
        self.assertEqual(envelope["schedule"]["at"], publish_at_epoch_ms(request.publish_at, request.timezone))
        self.assertEqual(envelope["schedule"]["timezone"], "GMT+8")

    def test_window_beyond_7_days_keeps_pending(self) -> None:
        adapter = self._adapter()
        from datetime import timedelta
        request = _request(publish_at=datetime.now().replace(microsecond=0) + timedelta(days=8))
        with self.assertRaises(CreatOKSubmissionNotReadyError) as ctx:
            adapter.create_publish_task(request)
        self.assertTrue(ctx.exception.submission_not_sent)

    def test_window_below_60_seconds_keeps_pending(self) -> None:
        adapter = self._adapter()
        from datetime import timedelta
        request = _request(publish_at=datetime.now().replace(microsecond=0) + timedelta(seconds=10))
        with self.assertRaises(CreatOKSubmissionNotReadyError):
            adapter.create_publish_task(request)

    def test_upload_failure_is_pre_submit_safe(self) -> None:
        adapter = self._adapter()
        adapter.cli.run = Mock(side_effect=CreatOKError("CLI boom"))
        request = _request(media_paths=[self.video_path], publish_at=self._future_time())
        with self.assertRaises(CreatOKPreSubmitError) as ctx:
            adapter.create_publish_task(request)
        self.assertTrue(ctx.exception.submission_not_sent)
        self.assertTrue(ctx.exception.retryable)

    def test_submit_without_job_id_marks_ambiguous(self) -> None:
        adapter = self._adapter()
        adapter.cli.run = Mock(side_effect=[
            _cli_payload({"asset": {"uid": "uid-1", "object_key": "uploads/v.mp4"}}),
            _cli_payload({"created": True, "job": {}}),
        ])
        request = _request(media_paths=[self.video_path], publish_at=self._future_time())
        with self.assertRaises(CreatOKResultUnknownError) as ctx:
            adapter.create_publish_task(request)
        self.assertTrue(ctx.exception.submission_ambiguous)


if __name__ == "__main__":
    unittest.main()
