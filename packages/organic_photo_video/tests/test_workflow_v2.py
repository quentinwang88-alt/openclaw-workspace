from __future__ import annotations

import copy
from pathlib import Path
import sys
import unittest
import tempfile

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain import statuses
from domain.models import ContentTask, QualityReview, TaskRevision, VideoRender
from domain.statuses import TASK_ANCHOR_REVIEW, TASK_IMAGE_GENERATING
from services.workflow_v2 import (
    AnchorReviewService,
    RevisionAssetResolver,
    RevisionSelectionError,
    RevisionService,
    RenderReviewService,
    canonical_hash,
)


class MemoryRepo:
    def __init__(self):
        self.task = ContentTask(
            task_id="task", idempotency_key="a" * 64, account_id="a", product_id="p",
            target_country="TH", target_locale="th-TH", task_status=TASK_ANCHOR_REVIEW,
            workflow_version=2, row_version=1,
            product_snapshot_json={"product": {"reference_images": []}},
            plan_json={
                "workflow_version": 2,
                "quality_contract": {
                    "quality_profile_id": "Q", "quality_profile_version": 2,
                    "dimensions": {
                        "product_fidelity": {"scope": ["anchor"], "min_score": 80, "hard_gate": True},
                        "anchor_naturalness": {"scope": ["anchor"], "min_score": 78, "hard_gate": True},
                    },
                },
            },
        )
        self.revisions = {}
        self.reviews = []
        self.renders = {}

    def get_task(self, task_id):
        return self.task if task_id == "task" else None

    def list_task_revisions(self, task_id):
        return sorted(
            [r for r in self.revisions.values() if r.task_id == task_id],
            key=lambda r: r.revision_no,
        )

    def get_task_revision(self, revision_id):
        return self.revisions.get(revision_id)

    def create_revision_and_activate(self, revision, *, expected_task_row_version, transition_to=None):
        if self.task.row_version != expected_task_row_version:
            raise RuntimeError("stale task")
        self.revisions[revision.revision_id] = copy.deepcopy(revision)
        self.task.active_revision_id = revision.revision_id
        self.task.workflow_version = 2
        if transition_to:
            self.task.task_status = transition_to
            self.task.plan_json = copy.deepcopy(revision.plan_snapshot_json.get("plan") or {})
            self.task.group_qa_json = {}
        self.task.row_version += 1
        return self.task

    def update_revision_manifest(self, revision_id, *, expected_lock_version, asset_manifest_json, selection_hash):
        revision = self.revisions[revision_id]
        if revision.lock_version != expected_lock_version or revision.revision_status != "working":
            raise RuntimeError("stale revision")
        revision.asset_manifest_json = copy.deepcopy(asset_manifest_json)
        revision.selection_hash = selection_hash
        revision.lock_version += 1
        return copy.deepcopy(revision)

    def insert_quality_review(self, review: QualityReview):
        self.reviews.append(review)

    def list_quality_reviews(self, revision_id, *, scope=None, target_id=None):
        return [r for r in self.reviews if r.revision_id == revision_id
                and (scope is None or r.scope == scope) and (target_id is None or r.target_id == target_id)]

    def get_render(self, render_id):
        return self.renders.get(render_id)

    def set_render_publish_ready(self, render_id, publish_ready):
        self.renders[render_id].publish_ready = publish_ready

    def release_revision(self, task_id, revision_id, *, expected_task_row_version, render_id, review_id=None,
                         expected_selection_hash=None, expected_input_snapshot_hash=None):
        assert task_id == self.task.task_id
        assert self.task.row_version == expected_task_row_version
        revision = self.revisions[revision_id]
        if expected_selection_hash is not None:
            assert revision.selection_hash == expected_selection_hash
        if expected_input_snapshot_hash is not None:
            assert revision.input_snapshot_hash == expected_input_snapshot_hash
        self.task.released_revision_id = revision_id
        self.task.selected_render_id = render_id
        self.revisions[revision_id].revision_status = "released"
        self.renders[render_id].publish_ready = True
        self.task.row_version += 1

    def transition_task(self, task_id, frm, to, **_kwargs):
        assert self.task.task_status == frm
        statuses.task_ensure_transition(frm, to)
        self.task.task_status = to


class WorkflowV2Test(unittest.TestCase):
    def setUp(self):
        self.repo = MemoryRepo()
        self.revisions = RevisionService(self.repo)
        self.revision = self.revisions.ensure_working(self.repo.task)

    def _candidate(self, asset_id="anchor_a", sha="a" * 64):
        return {
            "asset_id": asset_id, "asset_type": "shot", "slot_index": 2,
            "path": f"/tmp/{asset_id}.png", "sha256": sha,
            "parent_asset_ids": [], "parameters_hash": "p" * 64,
        }

    def test_plan_snapshot_is_frozen_and_candidate_is_not_selected(self):
        self.repo.task.plan_json["mutated"] = True
        self.assertNotIn("mutated", self.revision.plan_snapshot_json["plan"])
        revision = self.revisions.register_candidate(self.revision, self._candidate())
        self.assertEqual(revision.asset_manifest_json["selected"], {})
        with self.assertRaises(RevisionSelectionError):
            RevisionAssetResolver.selected(revision, "anchor")

    def test_anchor_pass_selects_exact_candidate_and_advances(self):
        revision = self.revisions.register_candidate(self.revision, self._candidate())
        result = AnchorReviewService(self.repo).record(
            "task", asset_id="anchor_a", decision="passed",
            dimensions={"product_fidelity": 92, "anchor_naturalness": 88},
            reviewer_type="human", reviewer="qa",
            expected_lock_version=revision.lock_version,
        )
        self.assertTrue(result.advanced)
        self.assertEqual(self.repo.task.task_status, TASK_IMAGE_GENERATING)
        selected = RevisionAssetResolver.selected(result.revision, "anchor")
        self.assertEqual(selected["sha256"], "a" * 64)
        self.assertEqual(len(self.repo.reviews), 1)
        self.assertEqual(self.repo.reviews[0].input_fingerprint, canonical_hash({
            "anchor": {"asset_id": "anchor_a", "sha256": "a" * 64},
            "inputs": self.revision.input_snapshot_hash,
            "profile": ["Q", 2],
        }))

    def test_failed_review_does_not_select_or_expand(self):
        revision = self.revisions.register_candidate(self.revision, self._candidate())
        result = AnchorReviewService(self.repo).record(
            "task", asset_id="anchor_a", decision="failed",
            dimensions={"product_fidelity": 60}, expected_lock_version=revision.lock_version,
        )
        self.assertFalse(result.advanced)
        self.assertEqual(self.repo.task.task_status, TASK_ANCHOR_REVIEW)
        self.assertEqual(result.revision.asset_manifest_json["selected"], {})

    def test_anchor_fingerprint_ignores_unselected_downstream_candidate(self):
        revision = self.revisions.register_candidate(self.revision, self._candidate())
        anchor_fp = canonical_hash({
            "anchor": {"asset_id": "anchor_a", "sha256": "a" * 64},
            "inputs": revision.input_snapshot_hash, "profile": ["Q", 2],
        })
        revision = self.revisions.register_candidate(revision, self._candidate("p3", "b" * 64))
        same_anchor_fp = canonical_hash({
            "anchor": {"asset_id": "anchor_a", "sha256": "a" * 64},
            "inputs": revision.input_snapshot_hash, "profile": ["Q", 2],
        })
        self.assertEqual(anchor_fp, same_anchor_fp)

    def test_render_pass_releases_the_exact_revision_and_render(self):
        from services.release_gate import expected_render_fingerprint, file_hash
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        video = Path(tmp.name) / "render.mp4"
        video.write_bytes(b"deterministic-media-fixture")
        current = self.repo.get_task_revision(self.revision.revision_id)
        current.plan_snapshot_json["plan"]["shots"] = [{"slot_index": 1}]
        current.input_snapshot_hash = canonical_hash(current.plan_snapshot_json)
        current.asset_manifest_json = {"candidates": {"s1": {"asset_id": "s1", "sha256": "s" * 64}}, "selected": {"shot:1": "s1"}}
        current.selection_hash = canonical_hash(current.asset_manifest_json["selected"])
        self.repo.task.task_status = "video_review"
        self.repo.task.selected_render_id = "render_a"
        self.repo.renders["render_a"] = VideoRender(
            render_id="render_a", task_id="task", render_preset_id="preset",
            origin_revision_id=self.revision.revision_id,
            input_fingerprint=expected_render_fingerprint(current), output_sha256=file_hash(str(video)), output_url=str(video),
            qc_status="passed", publish_ready=False,
        )
        review = RenderReviewService(self.repo).record(
            "task", render_id="render_a", decision="passed",
            dimensions={"render_visual_quality": 90}, reviewer="qa",
        )
        self.assertEqual(review.scope, "render")
        self.assertEqual(self.repo.task.released_revision_id, self.revision.revision_id)
        self.assertEqual(self.repo.task.selected_render_id, "render_a")
        self.assertTrue(self.repo.renders["render_a"].publish_ready)
        duplicate = RenderReviewService(self.repo).record("task", render_id="render_a", decision="passed", dimensions={}, reviewer="qa")
        self.assertEqual(duplicate.review_id, review.review_id)
        self.assertEqual(len(self.repo.reviews), 1)
        with self.assertRaisesRegex(RuntimeError, "immutable"):
            RenderReviewService(self.repo).record("task", render_id="render_a", decision="failed", dimensions={}, reviewer="qa")

    def test_assistant_cannot_impersonate_human_anchor_approval(self):
        revision = self.revisions.register_candidate(self.revision, self._candidate())
        result = AnchorReviewService(self.repo).record(
            "task", asset_id="anchor_a", decision="passed", dimensions={"product_fidelity": 95},
            reviewer_type="human", reviewer="codex_manual_review", expected_lock_version=revision.lock_version,
        )
        self.assertFalse(result.advanced)
        self.assertEqual(self.repo.reviews[-1].reviewer_type, "assistant")
        self.assertEqual(self.repo.task.task_status, TASK_ANCHOR_REVIEW)


if __name__ == "__main__":
    unittest.main()
