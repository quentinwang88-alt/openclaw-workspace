"""Native-photo queue contract tests; all databases and media are temporary."""

from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import bgm
from app.db import AutoPublishDB
from app.models import AccountConfig, PublishTaskStatus, ScriptMetadata
from app.publishers import BasePublishAdapter, RoutedPublishAdapter
from app.scheduler import (
    _validate_opv_upload, _verify_live_opv_release, account_can_publish_candidate,
    schedule_slots, sync_publish_results,
)


def signed_manifest(payload):
    unsigned = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    digest = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":")).encode()).hexdigest()
    return {**unsigned, "manifest_sha256": digest}


class PhotoAdapter(BasePublishAdapter):
    def __init__(self):
        self.requests = []
        self.status = PublishTaskStatus("pending", "待执行")
        self.error = None

    def create_scheduled_task(self, **kwargs):
        raise AssertionError("Photos must never enter the video-only interface")

    def create_publish_task(self, request):
        self.requests.append(request)
        if self.error:
            raise self.error
        return "photo:job-1"

    def query_task_status(self, **kwargs):
        return self.status

    def submission_receipt(self, _idempotency_key):
        return {"diagnostic_receipt": "stored-before-assignment"}


class NativePhotoQueueTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = patch.dict("os.environ", {
            "OPENCLAW_SHARED_DATA_DIR": str(self.root),
            "SHORT_VIDEO_AUTO_PUBLISH_CONFIG_PATH": str(self.root / "unused.json"),
        })
        self.env.start()
        self.db_path = self.root / "queue.sqlite3"
        self.db = AutoPublishDB(self.db_path)
        self.account = AccountConfig(
            "photo-account", "照片测试账号", "THFZ01", "可用", "12:00", "", "",
            publish_channel="CreatOK", nurture_enabled=True, nurture_only=True,
        )
        self.db.upsert_account_configs([self.account])
        self.db.update_account_provider_info(self.account.account_id, content_photo=True)
        self.key, self.task_id = "opv:test-photo", "test-photo"
        slides = []
        for index in range(1, 6):
            path = self.root / f"page-{index}.png"
            path.write_bytes(f"fixture-photo-{index}".encode())
            slides.append({
                "index": index, "asset_id": f"slide-{index}", "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "mime_type": "image/png", "width": 1080, "height": 1920,
            })
        self.manifest = signed_manifest({
            "schema_version": "opv-photo-release-v1", "media_kind": "native_photo",
            "task_id": self.task_id, "revision_id": "revision-1", "content_package_id": "package-1",
            "review_id": "review-1", "input_fingerprint": "fingerprint-1",
            "selection_hash": "selection-1", "input_snapshot_hash": "snapshot-1",
            "template_id": "pick", "template_version": 1, "cover_index": 1,
            "copy": {
                "title": "Choose a look", "caption": "Pick A B C or D",
                "hashtags": ["#outfit", "#choice"],
            }, "slides": slides,
        })
        self.metadata = ScriptMetadata(
            script_id=self.task_id, canonical_script_key=self.key, source_record_id="source-1",
            script_slot="OPV:test-photo", task_no="1", store_id="THFZ01", product_id="",
            parent_slot="OPV", direction_label="pick", variant_strength="",
            target_country="TH", product_type="apparel", content_family_key="photo-family",
            script_text="", short_video_title="Choose a look", title_source="opv_copy",
            script_source="图文养号", publish_purpose="养号", cart_enabled="否",
            content_branch="非商品展示型", audio_mode="silent_source_platform_bgm",
        )
        self.write_photo()
        self.adapter = PhotoAdapter()
        self.publisher = RoutedPublishAdapter(
            default_adapter=self.adapter, channel_adapters={"CreatOK": self.adapter},
            account_channels={self.account.account_id: "CreatOK"},
            task_prefix_adapters={"photo:": self.adapter},
        )
        self.now = datetime(2026, 9, 5, 11)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def write_photo(self, manifest=None, metadata_manifest=None):
        manifest = self.manifest if manifest is None else manifest
        metadata_manifest = manifest if metadata_manifest is None else metadata_manifest
        context = {"workflow_version": 2, "publish_title": "Choose a look",
                   "release_manifest": metadata_manifest, "recipe_id": "pick"}
        self.db.upsert_script_metadata([replace(self.metadata, script_text=json.dumps(context))])
        self.db.upsert_video_asset(
            canonical_script_key=self.key, script_id=self.task_id, run_manager_record_id="source-1",
            video_source_type="opv_photo_package", video_source_value="package-1", local_file_path=None,
            download_status="下载成功", run_video_status="已完成", media_kind="native_photo",
            photo_manifest_json=manifest,
        )

    def schedule(self):
        return schedule_slots(self.db, self.publisher, now=self.now, window_hours=2)

    def test_migration_preserves_legacy_video_rows(self):
        legacy = self.root / "legacy.sqlite3"
        old = AutoPublishDB(legacy)
        with old._connect() as conn:
            for column in ("media_kind", "photo_manifest_json"):
                conn.execute(f"ALTER TABLE video_assets DROP COLUMN {column}")
            for column in ("platform_post_id", "platform_post_url", "published_at"):
                conn.execute(f"ALTER TABLE publish_slots DROP COLUMN {column}")
            conn.execute("INSERT INTO video_assets (canonical_script_key,script_id,local_file_path,created_at,updated_at) "
                         "VALUES ('legacy','legacy','/legacy.mp4','before','before')")
        upgraded = AutoPublishDB(legacy)
        row = upgraded.get_video_asset("legacy")
        self.assertEqual(row["media_kind"], "video")
        self.assertEqual(row["local_file_path"], "/legacy.mp4")
        self.assertIsNone(row["photo_manifest_json"])
        with upgraded._connect() as conn:
            info = {row["name"]: row for row in conn.execute("PRAGMA table_info(video_assets)")}
            self.assertFalse(info["local_file_path"]["notnull"])
            self.assertIn("platform_post_id", {row["name"] for row in conn.execute("PRAGMA table_info(publish_slots)")})

    def test_candidate_reads_ordered_photos_without_video_path(self):
        candidates = self.db.list_ready_candidates("THFZ01")
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.content_type, "photo")
        self.assertEqual(candidate.media_paths, [slide["path"] for slide in self.manifest["slides"]])
        self.assertEqual(candidate.publish_video_value, "")
        self.assertIsNone(self.db.get_video_asset(self.key)["local_file_path"])
        self.assertFalse(bgm.requires_platform_bgm(candidate))

    def test_incomplete_or_mismatched_manifest_is_not_a_candidate(self):
        variants = [
            {**self.manifest, "manifest_sha256": "changed"},
            signed_manifest({**self.manifest, "slides": []}),
            signed_manifest({**self.manifest, "slides": list(reversed(self.manifest["slides"]))}),
            signed_manifest({**self.manifest, "task_id": "another-task"}),
        ]
        for manifest in variants:
            with self.subTest(manifest=manifest["manifest_sha256"]):
                self.write_photo(manifest)
                self.assertEqual(self.db.list_ready_candidates("THFZ01"), [])
        self.write_photo(metadata_manifest={})
        self.assertEqual(self.db.list_ready_candidates("THFZ01"), [])

    def test_missing_photo_file_is_not_a_candidate(self):
        Path(self.manifest["slides"][0]["path"]).unlink()
        self.assertEqual(self.db.list_ready_candidates("THFZ01"), [])

    def test_photo_requires_explicit_photo_capability(self):
        candidate = self.db.list_ready_candidates("THFZ01")[0]
        with self.db._connect() as conn:
            conn.execute("UPDATE account_configs SET content_photo_capable=NULL,organic_capable=1")
        self.assertFalse(account_can_publish_candidate(self.db.get_account_config(self.account.account_id), candidate))
        with patch("app.scheduler._validate_opv_upload") as verify:
            self.assertEqual(self.schedule().scheduled, 0)
        verify.assert_not_called()
        self.assertEqual(self.adapter.requests, [])

    def test_schedule_uses_photo_request_and_retains_release_gate(self):
        with patch("app.scheduler._validate_opv_upload") as verify, \
                patch("app.scheduler.bgm.requires_platform_bgm", side_effect=AssertionError("video BGM called")):
            self.assertEqual(self.schedule().scheduled, 1)
            self.assertEqual(self.schedule().scheduled, 0)
        verify.assert_called_once()
        self.assertEqual(len(self.adapter.requests), 1)
        request = self.adapter.requests[0]
        self.assertEqual(request.content_type, "photo")
        self.assertEqual(request.commerce_type, "organic")
        self.assertEqual(request.media_paths, [slide["path"] for slide in self.manifest["slides"]])
        slot = self.db.get_publish_slot_by_task_id("photo:job-1")
        context = json.loads(slot["submission_context_json"])
        self.assertEqual(context["release_manifest"], self.manifest)
        self.assertEqual(context["diagnostic_receipt"], "stored-before-assignment")
        self.assertIsNone(slot["platform_post_id"])

    def test_creatok_photo_schedule_passes_description_and_platform_auto_music(self):
        with patch("app.scheduler._validate_opv_upload"):
            self.assertEqual(self.schedule().scheduled, 1)
        request = self.adapter.requests[0]
        self.assertTrue(request.auto_add_music)
        self.assertEqual(request.description, "Pick A B C or D #outfit #choice")
        slot = self.db.get_publish_slot_by_task_id("photo:job-1")
        audit = json.loads(slot["bgm_json"])
        self.assertEqual(audit["mode"], "platform_auto")
        self.assertIsNone(audit["music_id"])

    def test_dual_channel_photo_uses_creatok_slot_even_when_account_default_is_neobund(self):
        neobund = PhotoAdapter()
        with self.db._connect() as conn:
            conn.execute(
                "UPDATE account_configs SET publish_channel='NeoBund',publish_time_1='' WHERE account_id=?",
                (self.account.account_id,),
            )
        self.db.replace_account_channel_bindings(self.account.account_id, [
            {"publish_channel": "NeoBund", "content_scope": "shoppable", "publish_time_1": "11:30"},
            {"publish_channel": "CreatOK", "content_scope": "organic", "publish_time_1": "12:00"},
        ])
        self.publisher = RoutedPublishAdapter(
            default_adapter=neobund,
            channel_adapters={"NeoBund": neobund, "CreatOK": self.adapter},
            account_channels={self.account.account_id: "NeoBund"},
            task_prefix_adapters={"photo:": self.adapter},
        )
        with patch("app.scheduler._validate_opv_upload"):
            self.assertEqual(self.schedule().scheduled, 1)
        self.assertEqual(neobund.requests, [])
        self.assertEqual(len(self.adapter.requests), 1)
        slot = self.db.get_publish_slot_by_task_id("photo:job-1")
        self.assertEqual(slot["publish_channel_used"], "CreatOK")
        self.assertEqual(json.loads(slot["bgm_json"])["provider"], "CreatOK")

    def test_upload_verifier_routes_photo_and_live_revision_checks(self):
        candidate = self.db.list_ready_candidates("THFZ01")[0]
        module = SimpleNamespace(validate_photo_upload=Mock(), validate_upload=Mock())
        spec = SimpleNamespace(loader=SimpleNamespace(exec_module=Mock()))
        with patch("app.scheduler.importlib.util.spec_from_file_location", return_value=spec), \
                patch("app.scheduler.importlib.util.module_from_spec", return_value=module), \
                patch("app.scheduler._verify_live_opv_release") as live:
            _validate_opv_upload(candidate)
        module.validate_upload.assert_not_called()
        self.assertEqual(module.validate_photo_upload.call_args.kwargs["media_paths"], candidate.media_paths)
        live.assert_called_once_with(self.task_id, self.manifest["manifest_sha256"], media_kind="native_photo")
        with patch("app.scheduler.subprocess.run", return_value=Mock(returncode=0, stdout="release_verified")) as run:
            _verify_live_opv_release(self.task_id, "hash", media_kind="native_photo")
        self.assertEqual(run.call_args.args[0][-2:], ["--media-kind", "native_photo"])

    def test_stale_release_never_submits(self):
        with patch("app.scheduler._validate_opv_upload", side_effect=ValueError("release changed")):
            self.assertEqual(self.schedule().scheduled, 0)
        self.assertEqual(self.adapter.requests, [])
        self.assertEqual(self.db.list_unresolved_submissions(), [])

    def test_unknown_submission_remains_reserved_and_never_resubmits(self):
        self.adapter.error = requests.Timeout("response lost")
        with patch("app.scheduler._validate_opv_upload"):
            self.schedule()
            self.schedule()
        self.assertEqual(len(self.adapter.requests), 1)
        self.assertEqual(len(self.db.list_unresolved_submissions()), 1)
        self.assertEqual(self.db.get_video_asset(self.key)["publish_status"], "提交中")

    def test_video_only_adapter_rejects_photo_before_remote_submission(self):
        self.adapter.create_publish_task = BasePublishAdapter.create_publish_task.__get__(self.adapter)
        with patch("app.scheduler._validate_opv_upload"):
            self.assertEqual(self.schedule().scheduled, 0)
        self.assertEqual(self.adapter.requests, [])
        self.assertEqual(self.db.list_unresolved_submissions(), [])

    def test_assigned_photo_cannot_be_replaced(self):
        with patch("app.scheduler._validate_opv_upload"):
            self.schedule()
        with self.assertRaisesRegex(ValueError, "不能替换"):
            self.write_photo(signed_manifest({**self.manifest, "revision_id": "revision-2"}))

    def test_result_stores_true_post_identity_and_not_provider_task_id(self):
        with patch("app.scheduler._validate_opv_upload"):
            self.schedule()
        self.adapter.status = PublishTaskStatus("success", "发布成功")
        sync_publish_results(self.db, self.publisher)
        slot = self.db.get_publish_slot_by_task_id("photo:job-1")
        self.assertIsNone(slot["platform_post_id"])
        self.assertIsNone(slot["platform_post_url"])
        self.assertIsNone(slot["published_at"])
        self.db.mark_publish_result(
            script_id=self.task_id, canonical_script_key=self.key, publish_task_id="photo:job-1",
            schedule_status="已排期", publish_status="已排期", publish_result="待核对",
        )
        self.adapter.status = PublishTaskStatus(
            "success", "发布成功", "2026-09-05 12:01:00", platform_post_id="tiktok-post-1",
            platform_post_url="https://example.invalid/photo/tiktok-post-1",
        )
        sync_publish_results(self.db, self.publisher)
        slot = self.db.get_publish_slot_by_task_id("photo:job-1")
        self.assertEqual(slot["platform_post_id"], "tiktok-post-1")
        self.assertEqual(slot["platform_post_url"], self.adapter.status.platform_post_url)
        self.assertEqual(slot["published_at"], "2026-09-05 12:01:00")


if __name__ == "__main__":
    unittest.main()
