from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from test_hero_first import FakeRepository as HeroRepo, ScriptedGenerator, build_task
from test_video_render import FakeRepository as RenderRepo
from services.feishu_workflow import (
    FeishuTaskWorkflow, FIELD_REVIEW, FIELD_PROGRESS, FIELD_NOTES, FIELD_OUTPUT,
    FIELD_CONFIRM_PUBLISH, FIELD_REVIEW_MODE, FIELD_REVIEW_STAGE, FIELD_REVIEW_TOKEN,
    FIELD_RETRY_REVIEW,
)
from services.hero_first import HeroFirstProducer
from services.workflow_v2 import RevisionAssetResolver, RevisionService, ScopedReviewService, ReworkService
from services.workflow_v2 import RenderReviewService


class Repo(HeroRepo, RenderRepo):
    def __init__(self):
        HeroRepo.__init__(self)
        self.renders, self.feedback = {}, []

    def list_tasks_by_source_prefix(self, _kind, _prefix):
        return list(self.tasks.values())

    def list_quality_reviews(self, revision_id, *, scope=None, target_id=None):
        return [r for r in self.reviews if r.revision_id == revision_id
                and (scope is None or r.scope == scope) and (target_id is None or r.target_id == target_id)]

    def list_publish_records_by_task(self, _task_id):
        return []

    def set_render_publish_ready(self, render_id, ready):
        self.renders[render_id].publish_ready = ready

    def release_revision(self, task_id, revision_id, *, expected_task_row_version, render_id, review_id=None,
                         expected_selection_hash=None, expected_input_snapshot_hash=None):
        task = self.tasks[task_id]
        assert task.row_version == expected_task_row_version
        assert self.revisions[revision_id].selection_hash == expected_selection_hash
        assert self.revisions[revision_id].input_snapshot_hash == expected_input_snapshot_hash
        self.revisions[revision_id].revision_status = "released"
        self.renders[render_id].publish_ready = True
        task.released_revision_id, task.selected_render_id = revision_id, render_id
        task.row_version += 1


class Client:
    def __init__(self):
        self.fields, self.updates, self.uploads = {}, [], []

    def update_record_fields(self, _record_id, fields):
        self.fields.update(copy.deepcopy(fields))
        self.updates.append(copy.deepcopy(fields))

    def upload_attachment(self, data, name, content_type, size, *, parent_type):
        self.uploads.append((name, parent_type))
        return {"file_token": f"file{len(self.uploads)}"}

    def get_record(self, _record_id):
        return SimpleNamespace(record_id="rec", fields=copy.deepcopy(self.fields))

    def list_records(self, page_size=500):
        return [self.get_record("rec")]


class Renderer:
    def render(self, _slots, output):
        output.write_bytes(b"test silent mp4")
        return True, ""

    def qc_video(self, output, expected):
        return {"passed": True, "duration_ms": expected, "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}


class DecodableGenerator(ScriptedGenerator):
    def generate_shot(self, request):
        result = super().generate_shot(request)
        if result.ok:
            from PIL import Image
            Image.new("RGB", (result.width, result.height), (request.slot_index * 20, 80, 110)).save(result.image_path)
        return result


class FailedAdapter:
    def review(self, payload):
        return {"dimensions": {key: 10 for key in payload["required_dimensions"]}, "notes": "failed"}


class FeishuV2Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo, self.client = Repo(), Client()
        self.task = build_task()
        self.task.workflow_version = 2
        self.task.plan_json.update(workflow_version=2, anchor_slot=2)
        self.repo.tasks[self.task.task_id] = self.task
        self.generator = DecodableGenerator(self.root, {})
        self.workflow = FeishuTaskWorkflow(self.repo, self.client,
            catalog=SimpleNamespace(overlay_profile_id=""), generator=self.generator,
            renderer=Renderer(), output_root=self.root, asset_readiness_gate=lambda *_: None)
        HeroFirstProducer(self.repo, self.generator, output_root=self.root,
            asset_readiness_gate=lambda *_: None).produce(self.task.task_id)
        self.workflow._project_v2(self.record(), [self.task])

    def record(self):
        return self.client.get_record("rec")

    def execute(self):
        self.client.fields["执行"] = True
        with patch("services.visual_qa.CreatorCrmVisualQaAdapter", side_effect=AssertionError("no visual model")):
            result = self.workflow.scan(record_id="rec")
        self.assertEqual(result["errors"], [], result)
        return result

    def test_single_execution_finishes_with_technical_audit_and_no_publish(self):
        self.client.fields[FIELD_REVIEW_MODE] = "自动审核"
        self.execute()
        self.assertEqual(self.generator.calls, [2, 1, 3, 4, 5])
        self.assertEqual(self.task.task_status, "video_review")
        self.assertEqual(self.client.fields[FIELD_REVIEW_STAGE], "技术完成")
        self.assertEqual(self.client.fields[FIELD_REVIEW], "无需审核")
        self.assertIsNone(self.client.fields[FIELD_REVIEW_MODE])
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")
        self.assertFalse(self.client.fields[FIELD_CONFIRM_PUBLISH])
        self.assertEqual([r.scope for r in self.repo.reviews], ["anchor", "group", "render"])
        self.assertTrue(all(r.reviewer_type == "technical" for r in self.repo.reviews))
        self.assertTrue(all(r.evidence_json["visual_review_performed"] is False for r in self.repo.reviews))
        self.assertEqual(self.task.active_revision_id, self.task.released_revision_id)
        running = [fields for fields in self.client.updates if fields.get(FIELD_PROGRESS) == "生成中"]
        self.assertTrue(running)
        self.assertTrue(all(fields[FIELD_REVIEW] == "无需审核" and fields[FIELD_REVIEW_MODE] is None for fields in running))

    def test_waiting_and_legacy_review_fields_never_start_generation(self):
        count = len(self.client.updates)
        for mode in ("自动审核", "人工确认", "无需审核", ""):
            self.client.fields.update({FIELD_REVIEW_MODE: mode, FIELD_REVIEW: "通过", FIELD_RETRY_REVIEW: True})
            self.assertEqual(self.workflow.scan(record_id="rec")["eligible"], 0)
        self.assertEqual(self.generator.calls, [2])
        self.assertEqual(len(self.client.updates), count)
        self.assertEqual(self.repo.reviews, [])

    def test_human_mode_does_not_create_a_human_approval(self):
        self.client.fields[FIELD_REVIEW_MODE] = "人工确认"
        self.execute()
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")
        self.assertFalse(any(r.reviewer_type == "human" for r in self.repo.reviews))

    def test_old_failed_visual_history_is_preserved_not_a_production_gate(self):
        from services.workflow_v2 import AnchorReviewService
        revision = self.repo.get_task_revision(self.task.active_revision_id)
        asset = next(iter(revision.asset_manifest_json["candidates"].values()))
        AnchorReviewService(self.repo).record(self.task.task_id, asset_id=asset["asset_id"],
            decision="failed", dimensions={"naturalness": 60}, reviewer_type="model", reviewer="old_vision")
        self.execute()
        self.assertEqual(self.repo.reviews[0].decision, "failed")
        self.assertEqual(self.repo.reviews[0].reviewer_type, "model")
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")

    def test_changed_anchor_bytes_stop_without_generation_or_fake_pass(self):
        anchor = next(s for s in self.repo.list_shots(self.task.task_id) if s.slot_index == 2)
        Path(anchor.image_url).write_bytes(b"changed")
        self.execute()
        self.assertEqual(self.generator.calls, [2])
        self.assertEqual(self.repo.reviews, [])
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "需处理")

    def test_matching_hash_but_undecodable_anchor_is_rejected(self):
        anchor = next(s for s in self.repo.list_shots(self.task.task_id) if s.slot_index == 2)
        Path(anchor.image_url).write_bytes(b"not an image")
        anchor.image_sha256 = hashlib.sha256(b"not an image").hexdigest()
        anchor.qa_json["media_qc"]["sha256"] = anchor.image_sha256
        revision = self.repo.get_task_revision(self.task.active_revision_id)
        revision.asset_manifest_json["candidates"][anchor.shot_id]["sha256"] = anchor.image_sha256
        self.execute()
        self.assertEqual(self.repo.reviews, [])
        self.assertIn("decoded", self.client.fields[FIELD_NOTES])

    def test_missing_p4_explicit_retry_only_fills_failed_slot(self):
        self.generator.script[4] = "fail"
        self.execute()
        self.assertEqual(self.task.task_status, "image_review")
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "需处理")
        self.assertEqual(len(self.client.fields[FIELD_OUTPUT]), 4)
        self.generator.script[4] = "ok"
        self.execute()
        self.assertEqual(self.generator.calls, [2, 1, 3, 4, 5, 4])
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")

    def test_unknown_inflight_shot_cannot_duplicate_submit(self):
        row = next(s for s in self.repo.list_shots(self.task.task_id) if s.slot_index == 3)
        row.shot_status = "generating"
        self.execute()
        self.assertEqual(self.generator.calls, [2])
        self.assertEqual(self.repo.reviews, [])
        self.assertIn("unknown", self.client.fields[FIELD_NOTES])

    def test_unknown_inflight_render_cannot_duplicate_submit(self):
        from unittest.mock import Mock
        self.workflow.renderer.render = Mock(side_effect=RuntimeError("process interrupted"))
        self.execute()
        self.assertEqual(self.task.task_status, "rendering")
        self.assertEqual(self.workflow.renderer.render.call_count, 1)
        count = len(self.generator.calls)
        self.execute()
        self.assertEqual(len(self.generator.calls), count)
        self.assertEqual(self.workflow.renderer.render.call_count, 1)
        self.assertIn("render outcome unknown", self.client.fields[FIELD_NOTES])

    def test_p4_rework_only_invalidates_p4_and_render(self):
        self.execute()
        parent = self.repo.get_task_revision(self.task.active_revision_id)
        old = copy.deepcopy(parent.asset_manifest_json["selected"])
        old_render = self.task.selected_render_id
        self.client.fields[FIELD_REVIEW] = "重做P4"
        self.assertEqual(self.workflow.scan(record_id="rec")["errors"], [])
        current = self.repo.get_task_revision(self.task.active_revision_id)
        self.assertEqual(current.parent_revision_id, parent.revision_id)
        self.assertEqual(self.generator.calls, [2, 1, 3, 4, 5, 4])
        for slot in (1, 2, 3, 5):
            self.assertEqual(current.asset_manifest_json["selected"][f"shot:{slot}"], old[f"shot:{slot}"])
        self.assertNotEqual(self.task.selected_render_id, old_render)
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")
        self.assertFalse(self.client.fields[FIELD_CONFIRM_PUBLISH])

    def test_anchor_rework_invalidates_all_downstream_and_autocontinues(self):
        self.execute()
        parent = self.task.active_revision_id
        self.client.fields[FIELD_REVIEW] = "重做P2"
        self.assertEqual(self.workflow.scan(record_id="rec")["errors"], [])
        self.assertEqual(self.generator.calls, [2, 1, 3, 4, 5, 2, 1, 3, 4, 5])
        self.assertNotEqual(parent, self.task.active_revision_id)
        self.assertEqual(self.task.active_revision_id, self.task.released_revision_id)

    def test_failed_render_rework_reuses_all_images(self):
        class BadRenderer(Renderer):
            def qc_video(self, output, expected):
                return {**super().qc_video(output, expected), "passed": False}
        self.workflow.renderer = BadRenderer()
        self.execute()
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "需处理")
        self.assertFalse(self.task.released_revision_id)
        self.workflow.renderer = Renderer()
        self.client.fields[FIELD_REVIEW] = "重做成片"
        self.assertEqual(self.workflow.scan(record_id="rec")["errors"], [])
        self.assertEqual(self.generator.calls, [2, 1, 3, 4, 5])
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")

    def test_repeated_completion_does_not_regenerate_or_rereview(self):
        self.execute()
        calls, reviews = len(self.generator.calls), len(self.repo.reviews)
        self.workflow._advance_v2(self.record(), [self.task])
        for _ in range(3):
            self.workflow.scan(record_id="rec")
        self.assertEqual(len(self.generator.calls), calls)
        self.assertEqual(len(self.repo.reviews), reviews)

    def test_old_publish_choice_without_checkbox_does_not_schedule(self):
        self.execute()
        self.workflow.publish_scheduler = SimpleNamespace(enqueue_task=lambda *_a, **_k: self.fail("no enqueue"))
        self.client.fields[FIELD_REVIEW] = "排期发布"
        self.assertEqual(self.workflow.scan(record_id="rec")["eligible"], 0)

    def test_unreleased_video_cannot_schedule_despite_completed_ui(self):
        self.workflow.publish_scheduler = SimpleNamespace(enqueue_task=lambda *_a, **_k: self.fail("no enqueue"))
        self.client.fields.update({FIELD_PROGRESS: "已完成", FIELD_CONFIRM_PUBLISH: True})
        self.assertEqual(len(self.workflow.scan(record_id="rec")["errors"]), 1)

    def test_write_failure_does_not_start_provider(self):
        def broken(*_a, **_k):
            raise ConnectionError("Feishu offline")
        self.client.update_record_fields = broken
        self.client.fields["执行"] = True
        result = self.workflow.scan(record_id="rec")
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(self.repo.reviews, [])
        self.assertEqual(self.generator.calls, [2])

    def test_six_existing_anchors_mixed_states_are_reused_for_24_remaining_slots(self):
        from services.technical_production import TechnicalCheckService
        for index in range(2, 7):
            task = build_task()
            task.task_id = f"task_{index}"
            task.source_record_id = f"rec:{index}:recipe:1"
            task.workflow_version = 2
            task.plan_json.update(workflow_version=2, anchor_slot=2)
            self.repo.tasks[task.task_id] = task
            HeroFirstProducer(self.repo, self.generator, output_root=self.root,
                asset_readiness_gate=lambda *_: None).produce(task.task_id)
        for task in list(self.repo.tasks.values())[:2]:
            TechnicalCheckService(self.repo).anchor(task.task_id)
        self.client.fields["生成数量"] = 6
        self.assertEqual(self.generator.calls, [2] * 6)
        self.execute()
        self.assertEqual(len(self.generator.calls), 30)
        self.assertEqual(self.generator.calls.count(2), 6)
        self.assertEqual(len(self.repo.renders), 6)
        self.assertTrue(all(t.active_revision_id == t.released_revision_id for t in self.repo.tasks.values()))
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")
        self.assertFalse(self.client.fields[FIELD_CONFIRM_PUBLISH])

    def test_one_task_failure_does_not_skip_remaining_batch_tasks(self):
        from services.technical_production import TechnicalProductionFlow
        second = copy.deepcopy(self.task)
        second.task_id = "second"
        with patch.object(TechnicalProductionFlow, "run", side_effect=[RuntimeError("one failed"), second]) as run:
            with self.assertRaisesRegex(RuntimeError, "one failed"):
                self.workflow._v2_continue_generation([self.task, second])
        self.assertEqual(run.call_count, 2)

    def test_production_accepts_existing_941x1672_anchor(self):
        from PIL import Image
        anchor = next(s for s in self.repo.list_shots(self.task.task_id) if s.slot_index == 2)
        Image.new("RGB", (941, 1672), "gray").save(anchor.image_url)
        anchor.image_width, anchor.image_height = 941, 1672
        anchor.image_sha256 = hashlib.sha256(Path(anchor.image_url).read_bytes()).hexdigest()
        anchor.qa_json["media_qc"].update(width=941, height=1672, sha256=anchor.image_sha256)
        revision = self.repo.get_task_revision(self.task.active_revision_id)
        revision.asset_manifest_json["candidates"][anchor.shot_id]["sha256"] = anchor.image_sha256
        self.execute()
        self.assertEqual(self.client.fields[FIELD_PROGRESS], "已完成")
        self.assertEqual(self.generator.calls.count(2), 1)


if __name__ == "__main__":
    unittest.main()
