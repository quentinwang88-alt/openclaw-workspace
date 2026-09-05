"""Pure local regressions for the opt-in, shot-aware QA policy."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
for path in (PACKAGE_ROOT, Path(__file__).resolve().parent):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from domain.models import TaskRevision
from services.visual_qa import (
    HARD_GATES_ONLY, CreatorCrmVisualQaAdapter, VisualQaError, VisualQaService,
    assessment_v2_enabled, decision_to_dict, file_hash, normalize_decision,
    review_decision_policy, select_quality_rules,
)
from services.workflow_v2 import AnchorReviewService
from test_visual_qa import FakeRepo


def contract():
    profile = json.loads((PACKAGE_ROOT / "config/profiles/QUALITY_OUTFIT_BREAKDOWN_V2.json").read_text())
    return {"quality_profile_id": profile["quality_profile_id"], "quality_profile_version": 1,
            "assessment_schema_version": 2, "decision_policy": HARD_GATES_ONLY,
            "dimensions": profile["dimensions"]}


def plan():
    roles = ("hero", "full_look", "lifestyle", "detail", "second_angle")
    return {"quality_contract": contract(), "anchor_slot": 2,
            "shots": [{"slot_index": index, "slot_role": role,
                       "shot_kind": "composite_board" if index == 1 else "generated_photo"}
                      for index, role in enumerate(roles, 1)]}


class AdvisoryAdapter:
    def __init__(self):
        self.payloads = []

    def review(self, payload):
        self.payloads.append(payload)
        scores = {key: 90 for key in payload["required_dimensions"]}
        soft = [key for key, rule in payload["dimension_rules"].items() if rule.get("hard_gate") is False]
        for key in soft:
            scores[key] = 60
        # Simulate a provider still using its old aggregate pass decision.
        return {"passed": not soft, "dimensions": scores, "notes": "soft style suggestions"}


class QualityPolicyV2Test(unittest.TestCase):
    def test_soft_low_score_does_not_block_or_require_rework(self):
        rules = {"product": {"hard_gate": True, "min_score": 80},
                 "style": {"hard_gate": False, "min_score": 78, "fail_action": "consider_style"}}
        raw = {"passed": False, "dimensions": {"product": 90, "style": 60}}
        result = normalize_decision(raw, tuple(rules), dimension_rules=rules, decision_policy=HARD_GATES_ONLY)
        self.assertTrue(result.passed)
        self.assertEqual(result.next_actions, [])
        self.assertEqual(result.warnings[0]["dimension"], "style")
        self.assertEqual(decision_to_dict(result)["warnings"], result.warnings)
        self.assertFalse(normalize_decision(raw, tuple(rules), dimension_rules=rules).passed)

    def test_hard_low_score_still_blocks_and_missing_score_never_passes(self):
        rules = {"product": {"hard_gate": True, "min_score": 80, "fail_action": "redo_product"}}
        result = normalize_decision({"passed": True, "dimensions": {"product": 60}}, tuple(rules),
                                    dimension_rules=rules, decision_policy=HARD_GATES_ONLY)
        self.assertFalse(result.passed)
        self.assertEqual(result.next_actions, ["redo_product"])
        with self.assertRaises(VisualQaError):
            normalize_decision({"passed": True, "dimensions": {"product": None}}, tuple(rules),
                               dimension_rules=rules, decision_policy=HARD_GATES_ONLY)

    def test_unexplained_provider_rejection_is_not_silently_passed(self):
        with self.assertRaises(VisualQaError):
            normalize_decision({"passed": False, "dimensions": {"product": 90}}, ("product",),
                dimension_rules={"product": {"hard_gate": True, "min_score": 80}}, decision_policy=HARD_GATES_ONLY)

    def test_only_explicit_frozen_contract_enables_new_policy(self):
        self.assertTrue(assessment_v2_enabled(contract()))
        historical = contract()
        historical.pop("assessment_schema_version")
        self.assertFalse(assessment_v2_enabled(historical))
        self.assertEqual(review_decision_policy(historical), "independent_dimensions")
        historical["assessment_schema_version"] = 2
        historical["decision_policy"] = "independent_dimensions"
        self.assertFalse(assessment_v2_enabled(historical))

    def test_board_rule_applies_only_to_board_and_p4_does_not_require_face(self):
        frozen = plan()
        for shot in frozen["shots"]:
            dimensions, rules = select_quality_rules(frozen["quality_contract"], "single", shots=[shot])
            self.assertEqual("board_integrity" in dimensions, shot["slot_index"] == 1)
            self.assertEqual("persona_consistency" in dimensions, shot["slot_index"] != 4)
            self.assertIn("product_fidelity", dimensions)
            self.assertTrue(all(rule["applicable_slot_indexes"] == [shot["slot_index"]] for rule in rules.values()))
        _, group = select_quality_rules(frozen["quality_contract"], "group", shots=frozen["shots"])
        self.assertEqual(group["board_integrity"]["applicable_slot_indexes"], [1])
        self.assertEqual(group["persona_consistency"]["applicable_slot_indexes"], [1, 2, 3, 5])

    def test_bad_applicability_fails_closed_without_changing_legacy_rules(self):
        frozen = contract()
        frozen["dimensions"]["product_fidelity"]["applies_to"] = {"slot_index_typo": [2]}
        with self.assertRaises(VisualQaError):
            select_quality_rules(frozen, "single", shots=plan()["shots"])
        frozen.pop("assessment_schema_version")
        dimensions, _ = select_quality_rules(frozen, "single", shots=[plan()["shots"][3]])
        self.assertIn("board_integrity", dimensions)  # old frozen policy unchanged

    def test_new_prompt_distinguishes_p2_and_p4_without_rewriting_legacy_prompt(self):
        frozen = plan()
        payload = {"scope": "single_shot", "plan": frozen, "image_paths": ["unused.png"]}
        prompts = [CreatorCrmVisualQaAdapter._prompt({**payload, "slot_index": index}, 1, 1, ["product_fidelity"])
                   for index in (2, 4)]
        self.assertNotEqual(*prompts)
        p4 = json.loads(prompts[1].split("质检合同：", 1)[1])
        self.assertEqual(p4["current_shot"]["slot_role"], "detail")
        self.assertEqual(p4["shots_being_reviewed"][0]["slot_index"], 4)
        self.assertIn("hard_gate=false的低分仅是优化建议", prompts[1])
        frozen["quality_contract"].pop("assessment_schema_version")
        old = [CreatorCrmVisualQaAdapter._prompt({**payload, "slot_index": index}, 1, 1, ["product_fidelity"])
               for index in (2, 4)]
        self.assertEqual(*old)
        self.assertNotIn("current_shot", old[0])

    def test_service_selects_each_shots_rules_and_resumes_warnings_from_cache(self):
        repo = FakeRepo()
        repo.task.workflow_version, repo.task.active_revision_id = 2, "rev"
        repo.task.plan_json = plan()
        with tempfile.TemporaryDirectory() as directory:
            candidates, selected = {}, {}
            for shot, context in zip(repo.shots, repo.task.plan_json["shots"]):
                image = Path(directory) / f"{shot.shot_id}.png"
                image.write_bytes(shot.shot_id.encode())
                shot.slot_role = context["slot_role"]
                shot.image_url, shot.image_sha256 = str(image), file_hash(str(image))
                shot.image_width, shot.image_height = 941, 1672
                shot.qa_json["media_qc"] = {"size_ok": True, "width": 941, "height": 1672,
                                           "sha256": shot.image_sha256}
                candidates[shot.shot_id] = {"asset_id": shot.shot_id, "path": str(image), "sha256": shot.image_sha256}
                selected[f"shot:{shot.slot_index}"] = shot.shot_id
            revision = TaskRevision(revision_id="rev", task_id="t", revision_no=1, input_snapshot_hash="inputs",
                plan_snapshot_json={"plan": copy.deepcopy(repo.task.plan_json), "product_snapshot": {"product": {}}},
                asset_manifest_json={"candidates": candidates, "selected": selected}, selection_hash="selected")
            repo.get_task_revision = lambda revision_id: revision
            adapter = AdvisoryAdapter()
            report = VisualQaService(repo, adapter).review_task("t")
            self.assertTrue(report.passed)
            self.assertTrue(report.group.warnings)
            self.assertEqual(len(adapter.payloads), 1)
            self.assertEqual(report.single, {})
            self.assertTrue(all("visual_model_qa" not in shot.qa_json for shot in repo.shots))
            targeted = AdvisoryAdapter()
            self.assertTrue(VisualQaService(repo, targeted).review_shot("t", 4).passed)
            p4 = targeted.payloads[0]
            self.assertEqual(p4["current_shot"]["slot_role"], "detail")
            self.assertNotIn("board_integrity", p4["required_dimensions"])
            self.assertNotIn("persona_consistency", p4["required_dimensions"])
            resumed = AdvisoryAdapter()
            self.assertTrue(VisualQaService(repo, resumed).review_task("t").passed)
            self.assertEqual(len(resumed.payloads), 0)
            self.assertTrue(VisualQaService(repo, resumed).review_shot("t", 2).passed)
            self.assertTrue(repo.shots[1].qa_json["targeted_visual_qa"]["warnings"])
            again = AdvisoryAdapter()
            self.assertTrue(VisualQaService(repo, again).review_shot("t", 2).passed)
            self.assertEqual(len(again.payloads), 0)

    def test_anchor_receives_frozen_product_slot_context_and_soft_warnings(self):
        frozen = plan()
        revision = TaskRevision(revision_id="rev", task_id="t", revision_no=1, input_snapshot_hash="inputs",
            plan_snapshot_json={"plan": frozen, "product_snapshot": {"product": {"authority": "frozen"}}},
            asset_manifest_json={"candidates": {"anchor": {"asset_id": "anchor", "slot_index": 2,
                "path": "anchor.png", "sha256": "frozen-sha"}}})
        repo = mock.Mock()
        repo.get_task.return_value.product_snapshot_json = {"product": {"authority": "mutable"}}
        service = AnchorReviewService(repo)
        service._revisions.ensure_working = mock.Mock(return_value=revision)
        service.record = mock.Mock()
        adapter = AdvisoryAdapter()
        service.review_with_adapter("t", asset_id="anchor", adapter=adapter)
        self.assertEqual(adapter.payloads[0]["product"]["authority"], "frozen")
        self.assertEqual(adapter.payloads[0]["current_shot"]["slot_index"], 2)
        self.assertEqual(service.record.call_args.kwargs["decision"], "passed")
        self.assertTrue(service.record.call_args.kwargs["evidence"]["warnings"])


if __name__ == "__main__":
    unittest.main()
