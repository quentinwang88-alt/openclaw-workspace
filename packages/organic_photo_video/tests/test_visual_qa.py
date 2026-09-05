from pathlib import Path
import sys
import unittest
import subprocess
import tempfile
from unittest import mock

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.models import ContentShot, ContentTask, TaskRevision, VideoRender
from domain.statuses import TASK_IMAGE_REVIEW
from services.visual_qa import (
    CreatorCrmVisualQaAdapter,
    VisualQaError,
    VisualQaService,
    VisualQaTimeout,
    RenderVisualQaService,
    file_hash,
    normalize_decision,
)


class FakeRepo:
    def __init__(self):
        self.task = ContentTask(
            task_id="t", idempotency_key="a" * 64, account_id="a", product_id="p",
            target_country="TH", target_locale="th-TH", task_status=TASK_IMAGE_REVIEW,
            product_snapshot_json={"product": {"reference_images": ["p.jpg"]}},
            plan_json={"shots": []},
        )
        self.shots = [
            ContentShot(
                shot_id=f"s{i}", task_id="t", slot_index=i, slot_role="hero",
                duration_ms=2000, shot_status="generated", qa_status="passed",
                image_url=f"/tmp/P{i}.png", qa_json={"media_qc": {"size_ok": True}},
            )
            for i in range(1, 6)
        ]

    def get_task(self, task_id):
        return self.task if task_id == "t" else None

    def list_shots(self, task_id):
        return self.shots

    def update_shot_qa(self, shot_id, *, qa_status, qa_json, failure_detail=None):
        shot = next(s for s in self.shots if s.shot_id == shot_id)
        shot.qa_status, shot.qa_json, shot.failure_detail = qa_status, qa_json, failure_detail

    def update_task_plan(self, task_id, **fields):
        for key, value in fields.items():
            setattr(self.task, key, value)


class PassingAdapter:
    def __init__(self):
        self.calls = 0

    def review(self, payload):
        self.calls += 1
        return {
            "passed": True,
            "dimensions": {key: 90 for key in payload["required_dimensions"]},
            "provider": "fake",
            "model": "vision-test",
        }


class ProfileAdapter:
    def review(self, payload):
        scores = {key: 90 for key in payload["required_dimensions"]}
        if payload["scope"] == "single_shot" and payload["slot_index"] == 2:
            scores["outfit_quality"] = 70
        return {"passed": True, "dimensions": scores}


class FakeVisionClient:
    model = "vision-fake"
    _working_model = "vision-fake"

    def chat_with_multiple_images(self, image_paths, prompt, max_tokens=0):
        self.image_paths = image_paths
        self.prompt = prompt
        return {"parsed": True}

    def parse_json_response(self, response):
        return {
            "passed": True,
            "dimensions": {"product_fidelity": 90},
            "reason_codes": [],
            "notes": "ok",
        }


class VisualQaTest(unittest.TestCase):
    def test_explicit_force_recheck_does_not_reuse_previous_single_decisions(self):
        repo, adapter = FakeRepo(), PassingAdapter()
        service = VisualQaService(repo, adapter)
        service.review_task("t")
        self.assertEqual(adapter.calls, 6)
        service.review_task("t")
        self.assertEqual(adapter.calls, 7)
        service.review_task("t", force_recheck=True)
        self.assertEqual(adapter.calls, 13)

    def test_v2_reviews_selected_assets_and_resumes_only_matching_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = FakeRepo()
            repo.task.workflow_version = 2
            repo.task.active_revision_id = "rev"
            repo.task.plan_json = {"shots": [{"slot_index": i} for i in range(1, 6)]}
            reference = Path(tmp) / "product.png"
            reference.write_bytes(b"first-reference")
            product = {"reference_images": [str(reference)]}
            candidates, selected = {}, {}
            for shot in repo.shots:
                path = Path(tmp) / f"{shot.shot_id}.png"
                path.write_bytes(shot.shot_id.encode())
                shot.image_url, shot.image_sha256 = str(path), file_hash(str(path))
                candidates[shot.shot_id] = {"asset_id": shot.shot_id, "path": str(path), "sha256": shot.image_sha256}
                selected[f"shot:{shot.slot_index}"] = shot.shot_id
            revision = TaskRevision(
                revision_id="rev", task_id="t", revision_no=1, input_snapshot_hash="inputs",
                plan_snapshot_json={"plan": repo.task.plan_json, "product_snapshot": {"product": product}},
                asset_manifest_json={"candidates": candidates, "selected": selected}, selection_hash="selected",
            )
            repo.get_task_revision = lambda revision_id: revision
            repo.shots.append(ContentShot(
                shot_id="unselected", task_id="t", slot_index=1, slot_role="hero", duration_ms=2000,
                shot_version=2, image_url="/missing/unselected.png",
            ))
            first = PassingAdapter()
            self.assertTrue(VisualQaService(repo, first).review_task("t").passed)
            self.assertEqual(first.calls, 6)
            second = PassingAdapter()
            self.assertTrue(VisualQaService(repo, second).review_task("t").passed)
            self.assertEqual(second.calls, 1)
            reference.write_bytes(b"updated-reference")
            third = PassingAdapter()
            self.assertTrue(VisualQaService(repo, third).review_task("t").passed)
            self.assertEqual(third.calls, 6)

    def test_render_service_timeout_never_writes_a_release(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "test.mp4"
            output.write_bytes(b"verified-media-fixture")
            repo = mock.Mock()
            repo.get_task.return_value = ContentTask(
                task_id="t", idempotency_key="a" * 64, account_id="a", product_id="p",
                target_country="TH", target_locale="th-TH", workflow_version=2,
                task_status="video_review", active_revision_id="rev",
            )
            repo.get_task_revision.return_value = TaskRevision(
                revision_id="rev", task_id="t", revision_no=1,
                plan_snapshot_json={"plan": {}}, input_snapshot_hash="s",
            )
            repo.get_render.return_value = VideoRender(
                render_id="r", task_id="t", render_preset_id="p", origin_revision_id="rev",
                output_url=str(output), output_sha256=file_hash(str(output)), qc_status="passed",
                input_fingerprint="frozen-input",
            )
            adapter = mock.Mock()
            adapter.review.side_effect = VisualQaTimeout("unavailable")
            service = RenderVisualQaService(repo, adapter)
            with mock.patch.object(service, "_extract_frames", return_value=[{"path": "frame.png"}]):
                with self.assertRaises(VisualQaTimeout):
                    service.review_task("t", render_id="r")
            repo.insert_quality_review.assert_not_called()
            repo.release_revision.assert_not_called()

    def test_provider_deadline_kills_request_and_cannot_return_pass(self):
        with mock.patch("services.visual_qa.subprocess.run", side_effect=subprocess.TimeoutExpired("worker", 2)) as run:
            with self.assertRaises(VisualQaTimeout):
                CreatorCrmVisualQaAdapter(timeout_seconds=120).review({"review_timeout_seconds": 2})
        self.assertEqual(run.call_args.kwargs["timeout"], 2)

    def test_total_budget_stops_before_another_provider_request(self):
        adapter = PassingAdapter()
        service = VisualQaService(FakeRepo(), adapter)
        service._deadline = 9
        with mock.patch("services.visual_qa.time.monotonic", return_value=10):
            with self.assertRaises(VisualQaTimeout):
                service._review({"scope": "single_shot"})
        self.assertEqual(adapter.calls, 0)

    def test_provider_failure_does_not_write_group_pass(self):
        repo = FakeRepo()
        adapter = PassingAdapter()
        adapter.review = mock.Mock(side_effect=VisualQaTimeout("deadline"))
        with self.assertRaises(VisualQaTimeout):
            VisualQaService(repo, adapter).review_task("t")
        self.assertFalse((repo.task.group_qa_json or {}).get("stage_c_visual"))

    def test_full_review_persists_single_and_group(self):
        repo = FakeRepo()
        report = VisualQaService(repo, PassingAdapter()).review_task("t")
        self.assertTrue(report.passed)
        self.assertEqual(repo.task.group_qa_json["stage_c_visual"]["decision"], "passed")
        self.assertTrue(all(s.qa_json["visual_model_qa"]["passed"] for s in repo.shots))

    def test_resume_reuses_completed_single_shot_decisions(self):
        repo = FakeRepo()
        dimensions = {
            "product_fidelity": 90,
            "persona_fidelity": 90,
            "look_fidelity": 90,
            "scene_fit": 90,
            "creator_realism": 90,
        }
        for shot in repo.shots[:2]:
            shot.qa_json["visual_model_qa"] = {
                "passed": True,
                "score": 90,
                "dimensions": dimensions,
                "reason_codes": [],
            }
        adapter = PassingAdapter()
        report = VisualQaService(repo, adapter).review_task("t")
        self.assertTrue(report.passed)
        self.assertEqual(adapter.calls, 4)  # P3-P5 plus the group review.

    def test_product_hard_fail_overrides_average(self):
        decision = normalize_decision(
            {"passed": True, "dimensions": {
                "product_fidelity": 60, "persona_fidelity": 100,
                "look_fidelity": 100, "scene_fit": 100, "creator_realism": 100,
            }},
            ("product_fidelity", "persona_fidelity", "look_fidelity", "scene_fit", "creator_realism"),
        )
        self.assertFalse(decision.passed)

    def test_adapter_is_required(self):
        with self.assertRaises(VisualQaError):
            VisualQaService(FakeRepo(), None)

    def test_production_adapter_reuses_multi_image_client(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            product = Path(tmp) / "product.png"
            generated = Path(tmp) / "generated.png"
            persona = Path(tmp) / "persona.png"
            product.write_bytes(b"p")
            generated.write_bytes(b"g")
            persona.write_bytes(b"i")
            client = FakeVisionClient()
            result = CreatorCrmVisualQaAdapter(client).review({
                "scope": "single_shot",
                "image_paths": [str(generated)],
                "product": {"reference_images": [str(product)]},
                "plan": {"persona": {"snapshot": {
                    "local_reference_images": [str(persona)]
                }}},
                "required_dimensions": ["product_fidelity"],
                "hard_fail_keys": ["product_fidelity"],
            })
            self.assertEqual(
                client.image_paths,
                [str(product), str(persona), str(generated)],
            )
            self.assertEqual(result["provider"], "creator-crm-vision-route")
            self.assertIn("product_fidelity", client.prompt)
            self.assertIn("persona_reference_images_next", client.prompt)

    def test_persona_hard_fail_overrides_average(self):
        decision = normalize_decision(
            {"passed": True, "dimensions": {
                "product_fidelity": 100, "persona_fidelity": 60,
                "look_fidelity": 100, "scene_fit": 100, "creator_realism": 100,
            }},
            ("product_fidelity", "persona_fidelity", "look_fidelity", "scene_fit", "creator_realism"),
        )
        self.assertFalse(decision.passed)

    def test_quality_profile_uses_independent_thresholds_and_actions(self):
        repo = FakeRepo()
        repo.task.plan_json["quality_contract"] = {
            "decision_policy": "independent_dimensions",
            "dimensions": {
                "product_fidelity": {
                    "scope": ["single", "group"],
                    "min_score": 80,
                    "hard_gate": True,
                    "fail_action": "regenerate_offending_slot",
                },
                "outfit_quality": {
                    "scope": ["single", "group"],
                    "min_score": 78,
                    "fail_action": "redo_outfit_plan",
                },
                "group_quality": {
                    "scope": ["group"],
                    "min_score": 75,
                    "fail_action": "replan_storyboard",
                },
            },
        }
        report = VisualQaService(repo, ProfileAdapter()).review_task("t")
        self.assertFalse(report.passed)
        self.assertIsNone(report.single[2].score)
        self.assertEqual(report.single[2].next_actions, ["redo_outfit_plan"])


if __name__ == "__main__":
    unittest.main()
