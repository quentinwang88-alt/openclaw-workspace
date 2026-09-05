"""Local V2 group-first, technical gate and transaction regressions."""
import copy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
for path in (PACKAGE_ROOT, Path(__file__).resolve().parent):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from domain.models import TaskRevision, VideoRender, LookFeedback
from repositories.rds_repository import RdsRepository, StaleStatusError
from services.visual_qa import VisualQaService, VisualQaError, RenderVisualQaService, file_hash
from services.video_render_flow import VideoRenderFlow, VideoRenderFlowError
from services.workflow_v2 import AnchorReviewService, ScopedReviewService, ReworkService, canonical_hash
from test_quality_policy_v2 import AdvisoryAdapter, contract
from test_rds_repository import FakeConnection
from test_video_render import FakeRepository, build_task_and_shots


class GroupRepo(FakeRepository):
    def __init__(self, directory):
        super().__init__()
        self.task = build_task_and_shots(self, directory)
        self.task.workflow_version, self.task.active_revision_id = 2, "revision"
        self.task.plan_json["quality_contract"] = contract()
        self.task.plan_json["anchor_slot"] = 2
        selected, candidates = {}, {}
        for shot, context in zip(self.shots.values(), self.task.plan_json["shots"]):
            context["shot_kind"] = "composite_board" if shot.slot_index == 1 else "generated_photo"
            shot.image_sha256 = file_hash(shot.image_url)
            shot.qa_json = {"media_qc": {"size_ok": True, "width": shot.image_width,
                "height": shot.image_height, "sha256": shot.image_sha256}}
            selected[f"shot:{shot.slot_index}"] = shot.shot_id
            candidates[shot.shot_id] = {"asset_id": shot.shot_id, "slot_index": shot.slot_index,
                "asset_type": "shot", "path": shot.image_url, "sha256": shot.image_sha256}
        selected["anchor"] = "shot_2"
        snapshot = {"plan": copy.deepcopy(self.task.plan_json), "product_snapshot": {"product": {}}, "copy": self.task.copy_json}
        revision = TaskRevision(revision_id="revision", task_id=self.task.task_id, revision_no=1,
            plan_snapshot_json=snapshot, input_snapshot_hash=canonical_hash(snapshot),
            asset_manifest_json={"selected": selected, "candidates": candidates}, selection_hash=canonical_hash(selected))
        self.revisions, self.reviews = {revision.revision_id: revision}, []

    def get_task_revision(self, revision_id):
        return self.revisions.get(revision_id)

    def list_task_revisions(self, task_id):
        return list(self.revisions.values())

    def insert_quality_review(self, review):
        self.reviews.append(review)

    def list_quality_reviews(self, revision_id, *, scope=None, target_id=None):
        return [r for r in self.reviews if r.revision_id == revision_id
                and (scope is None or r.scope == scope) and (target_id is None or r.target_id == target_id)]

    def update_shot_qa(self, shot_id, *, qa_status, qa_json, failure_detail=None):
        row = self.shots[shot_id]
        row.qa_status, row.qa_json, row.failure_detail = qa_status, qa_json, failure_detail

    def update_revision_manifest(self, revision_id, *, expected_lock_version, asset_manifest_json, selection_hash):
        row = self.revisions[revision_id]
        assert row.lock_version == expected_lock_version
        row.asset_manifest_json, row.selection_hash = copy.deepcopy(asset_manifest_json), selection_hash
        row.lock_version += 1
        return copy.deepcopy(row)

    def create_revision_and_activate(self, revision, *, expected_task_row_version, transition_to=None):
        assert self.task.row_version == expected_task_row_version
        self.revisions[revision.revision_id] = copy.deepcopy(revision)
        self.task.active_revision_id = revision.revision_id
        self.task.plan_json = copy.deepcopy(revision.plan_snapshot_json["plan"])
        self.task.group_qa_json = {}
        self.task.row_version += 1
        if transition_to:
            self.task.task_status = transition_to

    def group_pass(self):
        return ScopedReviewService(self).record(self.task.task_id, scope="group", target_id="group",
            decision="passed", dimensions={"product_fidelity": 90}, reviewer_type="model", reviewer="fixture_vision")


class FailingAdapter(AdvisoryAdapter):
    def review(self, payload):
        result = super().review(payload)
        result["dimensions"]["product_fidelity"] = 30
        result["passed"] = False
        return result


class GroupFirstV2Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = GroupRepo(self.root)
        self.task = self.repo.task
        self.flow = VideoRenderFlow(self.repo, mock.Mock(), output_root=self.root)

    def test_normal_path_calls_anchor_group_and_existing_render_once_each(self):
        adapter = AdvisoryAdapter()
        self.task.task_status = "anchor_review"
        anchor = AnchorReviewService(self.repo).review_with_adapter(self.task.task_id, asset_id="shot_2", adapter=adapter)
        self.assertTrue(anchor.advanced)
        self.task.task_status = "image_review"  # generated fixtures stand for expansion
        report = VisualQaService(self.repo, adapter).review_task(self.task.task_id)
        self.assertTrue(report.passed)
        self.assertEqual(report.single, {})
        self.assertEqual([p["scope"] for p in adapter.payloads], ["anchor", "five_shot_group"])
        self.assertTrue(all("visual_model_qa" not in s.qa_json for s in self.repo.shots.values()))
        output = self.root / "render.mp4"
        output.write_bytes(b"render-fixture")
        render = VideoRender(render_id="render", task_id=self.task.task_id, render_preset_id="preset",
            origin_revision_id=self.task.active_revision_id, output_url=str(output), output_sha256=file_hash(str(output)),
            qc_status="passed", input_fingerprint="fixture")
        self.repo.renders[render.render_id] = render
        self.task.task_status, self.task.selected_render_id = "video_review", render.render_id
        service = RenderVisualQaService(self.repo, adapter)
        with mock.patch.object(service, "_extract_frames", return_value=[{"path": "frame.png"}]):
            with mock.patch("services.workflow_v2.RenderReviewService.record") as record:
                service.review_task(self.task.task_id, render_id=render.render_id)
        self.assertEqual([p["scope"] for p in adapter.payloads], ["anchor", "five_shot_group", "render"])
        self.assertEqual(record.call_args.kwargs["decision"], "passed")

    def test_group_failure_does_not_overwrite_technical_status_or_fake_single_results(self):
        before = {key: copy.deepcopy(s.qa_json) for key, s in self.repo.shots.items()}
        report = VisualQaService(self.repo, FailingAdapter()).review_task(self.task.task_id)
        self.assertFalse(report.passed)
        self.assertEqual(report.single, {})
        self.assertEqual(before, {key: s.qa_json for key, s in self.repo.shots.items()})
        self.assertTrue(all(s.qa_status == "passed" for s in self.repo.shots.values()))

    def test_explicit_single_pass_is_cached_but_never_approves_the_group(self):
        adapter = AdvisoryAdapter()
        service = VisualQaService(self.repo, adapter)
        self.assertTrue(service.review_shot(self.task.task_id, 4).passed)
        self.assertTrue(service.review_shot(self.task.task_id, 4).passed)
        self.assertEqual(len(adapter.payloads), 1)
        self.assertEqual(adapter.payloads[0]["current_shot"]["slot_role"], "detail")
        self.assertEqual(self.repo.reviews, [])
        with self.assertRaises(VideoRenderFlowError):
            self.flow.approve_group(self.task.task_id, reviewer="operator")

    def test_targeted_hard_failure_invalidates_old_group_pass_without_changing_media_qc(self):
        self.repo.group_pass()
        result = VisualQaService(self.repo, FailingAdapter()).review_shot(self.task.task_id, 4)
        self.assertFalse(result.passed)
        self.assertEqual(self.repo.reviews[-1].decision, "failed")
        self.assertEqual(self.repo.reviews[-1].evidence_json["source_scope"], "targeted_single")
        self.assertEqual(self.repo.shots["shot_4"].qa_status, "passed")
        with self.assertRaises(VideoRenderFlowError):
            self.flow.approve_group(self.task.task_id, reviewer="operator")

    def test_formal_render_cannot_bypass_current_group_gate_by_changing_status(self):
        self.task.task_status = "rendering"
        with self.assertRaises(VideoRenderFlowError):
            self.flow.render(self.task.task_id)
        self.flow._renderer.render.assert_not_called()

    def test_technical_qc_and_group_pass_allow_legacy_content_failed_projection(self):
        self.repo.shots["shot_2"].qa_status = "failed"
        self.repo.group_pass()
        self.flow.approve_group(self.task.task_id, reviewer="operator", approval_mode="operator_auto")
        self.assertEqual(self.task.task_status, "rendering")
        self.assertEqual(self.repo.shots["shot_2"].qa_status, "failed")

    def test_stale_media_blocks_before_model_and_formal_approval(self):
        self.repo.group_pass()
        Path(self.repo.shots["shot_4"].image_url).write_bytes(b"changed")
        adapter = AdvisoryAdapter()
        with self.assertRaises(VisualQaError):
            VisualQaService(self.repo, adapter).review_task(self.task.task_id)
        self.assertEqual(adapter.payloads, [])
        with self.assertRaises(VideoRenderFlowError):
            self.flow.approve_group(self.task.task_id, reviewer="operator")

    def test_render_only_rework_reuses_verified_group_evidence_and_recovers_idempotently(self):
        source = self.repo.group_pass()
        self.task.task_status = "video_review"
        parent = self.repo.get_task_revision(self.task.active_revision_id)
        service = ReworkService(self.repo, publication_guard=lambda task_id: None)
        kwargs = {"expected_revision_id": parent.revision_id, "expected_lock_version": parent.lock_version,
                  "scope": "render", "reason": "retry edit", "idempotency_key": "once"}
        child = service.begin(self.task.task_id, **kwargs)
        self.assertEqual(child.rework_spec_json["resume_stage"], "rendering")
        self.assertEqual(self.repo.reviews[-1].evidence_json["reused_review_id"], source.review_id)
        self.assertFalse(self.repo.reviews[-1].evidence_json["model_called"])
        service.begin(self.task.task_id, **kwargs)
        self.assertEqual(len(self.repo.reviews), 2)
        self.task.task_status = "rendering"
        self.flow._require_v2_content_gate(self.task, list(self.repo.shots.values()))

    def test_render_rework_without_valid_source_group_returns_to_image_review_flow(self):
        self.task.task_status = "video_review"
        parent = self.repo.get_task_revision(self.task.active_revision_id)
        child = ReworkService(self.repo, publication_guard=lambda task_id: None).begin(self.task.task_id,
            expected_revision_id=parent.revision_id, expected_lock_version=parent.lock_version,
            scope="render", reason="retry edit", idempotency_key="no-group")
        self.assertEqual(child.rework_spec_json["resume_stage"], "image_generating")
        self.assertEqual(self.repo.reviews, [])

    def _transaction(self, *, decision="passed", with_guards=True):
        review = self.repo.group_pass()
        revision = self.repo.get_task_revision(self.task.active_revision_id)
        review.decision = decision
        for shot in self.repo.shots.values():
            shot.qa_status = "failed"  # old content projection is not technical QC
        connection = FakeConnection([
            ("rows", [{"task_status": "image_review", "content_package_id": None,
                       "workflow_version": 2, "active_revision_id": revision.revision_id}]),
            ("rows", [revision.to_row()]), ("rows", [review.to_row()]),
            ("rows", [shot.to_row() for shot in self.repo.shots.values()]),
            ("rowcount", 5), ("rowcount", 5), ("rowcount", 1), ("rowcount", 1),
        ])
        guard = {"expected_revision_id": revision.revision_id, "expected_selection_hash": revision.selection_hash,
                 "group_review_id": review.review_id} if with_guards else {}
        def execute():
            return RdsRepository(lambda: connection).commit_group_approval(task_id=self.task.task_id,
                shots=list(self.repo.shots.values()),
                feedback=LookFeedback(feedback_id="feedback", task_id=self.task.task_id, account_id="account",
                    product_id="product", feedback_type="human_review", decision="approve", reviewer="fixture"),
                group_qa_json={}, package_fields={}, **guard)
        return connection, execute

    def test_v2_transaction_verifies_revision_review_and_media_without_legacy_qa_condition(self):
        connection, execute = self._transaction()
        execute()
        self.assertEqual(connection.commits, 1)
        sql = next(sql for sql, params in connection.statements if "SET is_selected=1" in sql)
        self.assertNotIn("qa_status", sql)
        self.assertGreaterEqual(sum("FOR UPDATE" in sql for sql, _ in connection.statements), 4)

    def test_v2_transaction_cannot_drop_guards_or_approve_failed_group(self):
        for options in ({"with_guards": False}, {"decision": "failed"}):
            with self.subTest(options=options):
                connection, execute = self._transaction(**options)
                with self.assertRaises(StaleStatusError):
                    execute()
                self.assertEqual(connection.rollbacks, 1)
                self.assertEqual(connection.commits, 0)


if __name__ == "__main__":
    unittest.main()
