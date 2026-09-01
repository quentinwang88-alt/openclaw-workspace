from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.models import ContentShot, ContentTask
from domain.statuses import TASK_IMAGE_REVIEW
from services.visual_qa import (
    CreatorCrmVisualQaAdapter,
    VisualQaError,
    VisualQaService,
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
    def review(self, payload):
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
    def test_full_review_persists_single_and_group(self):
        repo = FakeRepo()
        report = VisualQaService(repo, PassingAdapter()).review_task("t")
        self.assertTrue(report.passed)
        self.assertEqual(repo.task.group_qa_json["stage_c_visual"]["decision"], "passed")
        self.assertTrue(all(s.qa_json["visual_model_qa"]["passed"] for s in repo.shots))

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
