from pathlib import Path
from types import SimpleNamespace
import hashlib
import json
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_main_schedule_bridge
from services.main_schedule_bridge import MainScheduleBridge, MainScheduleBridgeError
from services.release_gate import (ReleaseGateError, freeze_release, validate_upload,
                                   assert_main_queue_rework_allowed, contract_hash,
                                   expected_render_fingerprint)


class ReleaseGateTest(unittest.TestCase):
    setUp = test_main_schedule_bridge.MainScheduleBridgeTest.setUp
    tearDown = test_main_schedule_bridge.MainScheduleBridgeTest.tearDown

    def test_explicit_technical_evidence_survives_upload_guard_without_model_claim(self):
        from services.technical_production import TechnicalCheckService
        from services.release_gate import TECHNICAL_REVIEWER
        self.release_fixture()
        self.review.scope = "render"
        self.review.reviewer_type, self.review.reviewer = "technical", TECHNICAL_REVIEWER
        self.review.evidence_json = TechnicalCheckService.evidence("render", self.revision,
            render_sha256=self.render.output_sha256)
        MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        queued = json.loads(self.db.get_script_metadata("opv:task-1")["script_text"])
        self.assertEqual(queued["release_manifest"]["reviewer_type"], "technical")
        manifest = freeze_release(self.repo, self.task, self.render)
        self.assertEqual(manifest["reviewer_type"], "technical")
        context = {"workflow_version": 2, "release_manifest": manifest, "publish_title": "frozen title"}
        validate_upload(context, video_path=str(self.video), script_id="task-1", title="frozen title")
        manifest["technical_evidence"]["visual_review_performed"] = True
        manifest["manifest_sha256"] = contract_hash({k: v for k, v in manifest.items() if k != "manifest_sha256"})
        with self.assertRaises(ReleaseGateError):
            validate_upload(context, video_path=str(self.video), script_id="task-1", title="frozen title")

    def test_bare_technical_actor_cannot_skip_evidence(self):
        self.release_fixture()
        self.review.scope, self.review.reviewer_type, self.review.reviewer = "render", "technical", "opv_technical_pipeline"
        with self.assertRaises(ReleaseGateError):
            freeze_release(self.repo, self.task, self.render)

    def release_fixture(self):
        self.task.workflow_version = 2
        self.task.active_revision_id = self.task.released_revision_id = "rev-1"
        self.render.origin_revision_id = "rev-1"
        self.render.render_id = "render-1"
        self.render.publish_ready = True
        self.render.qc_status = "passed"
        self.render.input_fingerprint = "fingerprint"
        self.render.output_sha256 = hashlib.sha256(b"video").hexdigest()
        self.render.copy_snapshot_json = {"title": "frozen title"}
        self.revision = SimpleNamespace(
            revision_id="rev-1", revision_status="released", task_id="task-1", selection_hash="selection",
            input_snapshot_hash="inputs", plan_snapshot_json={
                "plan": {"bgm_mood_hints": ["calm"], "shots": [{"slot_index": 1}]},
                "copy": {"title": "frozen title"},
                "product_snapshot": {"product": {"product_id": "frozen-product"}},
            },
            asset_manifest_json={"selected": {"shot:1": "shot-1"},
                                 "candidates": {"shot-1": {"asset_id": "shot-1", "sha256": "shotsha"}}},
        )
        self.revision.input_snapshot_hash = contract_hash(self.revision.plan_snapshot_json)
        self.revision.selection_hash = contract_hash(self.revision.asset_manifest_json["selected"])
        self.render.input_fingerprint = expected_render_fingerprint(self.revision)
        self.review = SimpleNamespace(
            input_fingerprint=self.render.input_fingerprint, decision="passed", reviewer_type="model",
            reviewer="independent_vision", review_id="review-1",
            evidence_json={"render_sha256": self.render.output_sha256},
        )
        self.reviews = [self.review]
        self.repo.get_task_revision = lambda _: self.revision
        self.repo.list_quality_reviews = lambda *args, **kwargs: self.reviews

    def test_freezes_title_product_and_checks_uploaded_bytes(self):
        self.release_fixture()
        MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        metadata = self.db.get_script_metadata("opv:task-1")
        context = json.loads(metadata["script_text"])
        self.assertEqual(metadata["short_video_title"], "frozen title")
        self.assertEqual(context["source_product_id"], "frozen-product")
        validate_upload(context, video_path=str(self.video), script_id="task-1", title="frozen title")
        self.video.write_bytes(b"different video")
        with self.assertRaisesRegex(ReleaseGateError, "SHA256"):
            validate_upload(context, video_path=str(self.video), script_id="task-1", title="frozen title")

    def test_changed_video_fails_before_enqueue(self):
        self.release_fixture()
        self.video.write_bytes(b"different")
        with self.assertRaises(MainScheduleBridgeError):
            MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        self.assertIsNone(self.db.get_video_asset("opv:task-1"))

    def test_latest_failed_review_overrides_old_pass(self):
        self.release_fixture()
        self.reviews.append(SimpleNamespace(**{**vars(self.review), "decision": "failed"}))
        with self.assertRaises(ReleaseGateError):
            freeze_release(self.repo, self.task, self.render)

    def test_revision_must_be_released_and_match_render_selection(self):
        self.release_fixture()
        self.revision.revision_status = "working"
        with self.assertRaises(ReleaseGateError):
            freeze_release(self.repo, self.task, self.render)
        self.revision.revision_status = "released"
        self.revision.asset_manifest_json["candidates"]["shot-1"]["sha256"] = "new-shot"
        with self.assertRaisesRegex(ReleaseGateError, "指纹"):
            freeze_release(self.repo, self.task, self.render)

    def test_assistant_or_fake_human_cannot_pass(self):
        self.release_fixture()
        for kind, reviewer in [("assistant", "codex"), ("human", "codex_manual")]:
            self.review.reviewer_type, self.review.reviewer = kind, reviewer
            with self.assertRaises(ReleaseGateError):
                freeze_release(self.repo, self.task, self.render)

    def test_v2_missing_manifest_fails_closed_legacy_unchanged(self):
        with self.assertRaises(ReleaseGateError):
            validate_upload({"workflow_version": 2}, video_path=str(self.video), script_id="task-1", title="x")
        with self.assertRaises(ReleaseGateError):
            validate_upload({"schema_version": "opv-main-publish-v2"}, video_path=str(self.video), script_id="task-1", title="x")
        validate_upload({}, video_path=str(self.video), script_id="task-1", title="x")

    def test_queued_asset_blocks_rework_read_only(self):
        assert_main_queue_rework_allowed("task-1", db_path=self.db.db_path)
        MainScheduleBridge(self.repo, db=self.db).enqueue_task("task-1")
        before = self.db.db_path.read_bytes()
        with self.assertRaisesRegex(MainScheduleBridgeError, "主发布队列"):
            assert_main_queue_rework_allowed("task-1", db_path=self.db.db_path)
        self.assertEqual(self.db.db_path.read_bytes(), before)
