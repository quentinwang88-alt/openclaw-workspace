"""Human-scene production integration tests over the style supply service.

Fake generator + fake reviewers only — no paid image or vision calls.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from services.image_generator import (
    GenerationOutcome, compose_shot_prompt,
)
from services.photo_style_reference_supply import PhotoStyleReferenceSupplyService
from services.photo_theme import resolve_photo_theme


def scene_model_variation():
    return {
        "family_id": "vision_dynamic_1", "presentation_type": "SCENE_MODEL",
        "scene_zh": "街角咖啡店", "background_prompt": "欧洲街角咖啡店自然光",
        "style_profile": {"analysis_method": "doubao_seed_2_1",
                          "presentation_type": "SCENE_MODEL"},
        "looks": [
            {"role": f"look_{letter}", "display_label": letter.upper(),
             "outerwear": f"外套{letter}", "top_inner": "蕾丝内搭",
             "bottom": f"下装{letter}", "shoes": "酒红皮鞋"}
            for letter in "abcd"
        ],
    }


def pack_persona(folder, persona_id="TH_APPAREL_REAL_01_001"):
    base = Path(folder) / "persona" / persona_id
    base.mkdir(parents=True, exist_ok=True)
    items = []
    for role in ("FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER",
                 "BODY_FULL_NEUTRAL", "BODY_FULL_MOTION"):
        path = base / f"{role.lower()}.png"
        Image.new("RGB", (60, 90), (120, 110, 100)).save(path)
        items.append({"local_path": str(path), "role": role,
                      "approved": True, "name": path.name})
    return {"persona_id": persona_id, "reference_items": items}


def human_observation(role, *, pose_family="RELAXED_STAND", gaze="CAMERA",
                      expression="NEUTRAL", head_tilt="NONE",
                      head_tilt_direction="NONE", ai_face_signs=None,
                      limb=False, drift=False, repair_instruction="",
                      scores=None):
    return {
        "role": role,
        "scores": scores or {
            "face_realism": 90, "head_posture": 92, "body_posture": 88,
            "gesture_naturalness": 85, "expression_naturalness": 86,
            "creator_photo_feel": 90,
        },
        "observations": {
            "head_tilt": head_tilt, "head_tilt_direction": head_tilt_direction,
            "gaze": gaze, "pose_family": pose_family, "expression": expression,
            "ai_face_signs": ai_face_signs or [],
            "limb_structure_implausible": limb, "identity_drift": drift,
        },
        "issues": [], "repair_instruction": repair_instruction,
    }


def passing_group_observation(**overrides):
    observations = {
        "look_a": human_observation("look_a", pose_family="RELAXED_STAND",
                                    gaze="CAMERA", expression="SOFT_SMILE"),
        "look_b": human_observation("look_b", pose_family="WALKING_CANDID",
                                    gaze="FORWARD"),
        "look_c": human_observation("look_c", pose_family="SCENE_INTERACTION",
                                    gaze="SIDE"),
        "look_d": human_observation("look_d", pose_family="TURN_BACK",
                                    gaze="CAMERA"),
    }
    observations.update(overrides)
    return {"roles": [observations[role] for role in
                      ("look_a", "look_b", "look_c", "look_d")]}


class RecordingGenerator:
    def __init__(self, fail_calls=None):
        self.requests = []
        self.prompts = []
        self.fail_calls = set(fail_calls or [])
        self.total_calls = 0

    def generate_shot(self, request):
        self.total_calls += 1
        self.prompts.append(compose_shot_prompt(request))
        self.requests.append(request)
        if self.total_calls in self.fail_calls:
            return GenerationOutcome(ok=False, error="模拟生图中断")
        path = (Path(request.output_dir)
                / f"look-{request.slot_index}_v{request.shot_version}.png")
        Image.new("RGB", (120, 180), (40 * request.slot_index, 80, 100)).save(path)
        return GenerationOutcome(ok=True, image_path=str(path),
                                 request_id=str(request.slot_index))


class FakeVisionReviewer:
    """Queues for review_alignment booleans + review_human_presentation raws."""

    def __init__(self, alignment_passed, human_reviews):
        self.alignment_passed = list(alignment_passed)
        self.human_reviews = list(human_reviews)
        self.alignment_calls = []
        self.human_calls = []

    def review_alignment(self, **kwargs):
        self.alignment_calls.append(kwargs)
        return {"passed": bool(self.alignment_passed.pop(0)),
                "notes": "对齐检查"}

    def review_human_presentation(self, *, image_paths, role_order,
                                  persona_reference_paths=(), pose_contracts=None):
        self.human_calls.append({
            "image_paths": list(image_paths), "role_order": list(role_order),
            "persona_reference_paths": list(persona_reference_paths),
            "pose_contracts": dict(pose_contracts or {}),
        })
        review = self.human_reviews.pop(0)
        return json.loads(json.dumps(review))


def prepare_kwargs(folder, record_id, persona, generator, reviewer):
    reference = Path(folder) / "reference.png"
    Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
    return {
        "record_id": record_id, "reference_paths": [str(reference)],
        "theme": resolve_photo_theme("秋季穿搭"),
        "account": SimpleNamespace(persona_ref_id="TH_APPAREL_REAL_01_001"),
        "persona": persona, "variation": scene_model_variation(),
        "progress": None,
    }, generator, reviewer


class HumanProductionPromptTest(unittest.TestCase):
    def test_four_roles_get_distinct_pose_contracts_and_human_contract_prompt(self):
        with tempfile.TemporaryDirectory() as folder:
            generator = RecordingGenerator()
            reviewer = FakeVisionReviewer(
                [True, True],
                [passing_group_observation(), passing_group_observation()],
            )
            kwargs, _, _ = prepare_kwargs(
                folder, "rec-prompt", pack_persona(folder), generator, reviewer,
            )
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            service.prepare(**kwargs)
            families = sorted(
                request.plan_shot["composition_contract"]["pose_contract"]["pose_family"]
                for request in generator.requests
            )
            self.assertEqual(families, sorted(["RELAXED_STAND", "WALKING_CANDID",
                                              "SCENE_INTERACTION", "TURN_BACK"]))
            for prompt in generator.prompts:
                self.assertIn("【人物摄影合同】", prompt)
                self.assertIn("头颈保持自然直立，不向左右肩膀倾斜", prompt)
                self.assertIn("保持人物参考中的身份特征", prompt)
                self.assertIn("不照搬参考照片的头部倾斜和固定笑容", prompt)
                self.assertIn("动作合同：", prompt)
            walking_prompt = next(
                prompt for prompt in generator.prompts if "WALKING_CANDID" in prompt
            )
            self.assertIn("自然行走", walking_prompt)
            self.assertIn("看向前方行进方向", walking_prompt)
            self.assertNotIn("50mm", walking_prompt)
            # 人物包身份参考替代生成图锚点。
            identity_paths = (
                generator.requests[0].reference_roles["persona_identity_images"]
            )
            # 主脸 + 全身：默认只传两张身份参考（歪头辅助图不传入）。
            self.assertEqual(len(identity_paths), 2)
            for request in generator.requests[1:]:
                self.assertEqual(
                    request.reference_roles["identity_anchor"], "",
                    "人物场景模式不允许把生成图当身份锚点",
                )

    def test_single_selfie_persona_blocks_scene_model_production(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder) / "persona"
            base.mkdir(parents=True)
            selfie = base / "reference_01.png"
            Image.new("RGB", (60, 90), (120, 110, 100)).save(selfie)
            persona = {"persona_id": "TH_APPAREL_SELECTED_01_001",
                       "reference_items": [
                           {"local_path": str(selfie), "approved": True},
                       ]}
            generator = RecordingGenerator()
            reviewer = FakeVisionReviewer([True], [])
            kwargs, _, _ = prepare_kwargs(folder, "rec-block", persona, generator, reviewer)
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            with self.assertRaisesRegex(ValueError, "人物包未通过预检"):
                service.prepare(**kwargs)
            self.assertEqual(generator.total_calls, 0)


class LookAGateTest(unittest.TestCase):
    def test_look_a_human_failure_stops_before_generating_other_roles(self):
        with tempfile.TemporaryDirectory() as folder:
            generator = RecordingGenerator()
            failing = human_observation(
                "look_a", head_tilt="OBVIOUS", head_tilt_direction="RIGHT",
                repair_instruction="头颈保持水平，改为自然行走抓拍",
            )
            reviewer = FakeVisionReviewer(
                [True, True],
                [ {"roles": [failing]}, {"roles": [failing]} ],
            )
            kwargs, _, _ = prepare_kwargs(
                folder, "rec-gate-stop", pack_persona(folder), generator, reviewer,
            )
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            with self.assertRaisesRegex(ValueError, "首张人物表现检查两次未通过"):
                service.prepare(**kwargs)
            look_a_versions = sorted(
                request.shot_version for request in generator.requests
                if request.slot_index == 1
            )
            self.assertEqual(look_a_versions, [1, 2],
                             "只允许重生 Look A，禁止带坏锚点继续产出 B/C/D")
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-gate-stop"
                             / "supply_manifest.json")
            if manifest_path.is_file():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                recorded = sorted(item["role"] for item in manifest["sources"])
                self.assertNotIn("look_b", recorded,
                                 "门禁失败的坏锚点不得进入 manifest 污染整组")
            retried = next(
                request for request in generator.requests
                if request.slot_index == 1 and request.shot_version == 2
            )
            self.assertIn("人物表现质检未通过", retried.plan_shot["purpose"])

    def test_look_a_retry_passes_and_generation_completes(self):
        with tempfile.TemporaryDirectory() as folder:
            generator = RecordingGenerator()
            failing = human_observation(
                "look_a", head_tilt="OBVIOUS", head_tilt_direction="RIGHT",
                repair_instruction="头颈保持水平",
            )
            reviewer = FakeVisionReviewer(
                [True, True],
                [
                    {"roles": [failing]},          # gate 第一次：失败
                    {"roles": [human_observation("look_a")]},  # 重试后通过
                    passing_group_observation(),   # 整组
                ],
            )
            kwargs, _, _ = prepare_kwargs(
                folder, "rec-gate-retry", pack_persona(folder), generator, reviewer,
            )
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            result = service.prepare(**kwargs)
            self.assertEqual(len(generator.requests), 5)
            self.assertEqual(result["sources"][0]["human_gate_qa"]["passed"], True)
            self.assertTrue(result["group_human_presentation_qa"]["passed"])


class GroupHumanRepairTest(unittest.TestCase):
    def test_group_human_qa_regenerates_only_blamed_role(self):
        with tempfile.TemporaryDirectory() as folder:
            generator = RecordingGenerator()
            blamed = human_observation(
                "look_c", pose_family="STATIC_MANNEQUIN", gaze="CAMERA",
                repair_instruction="上一版双臂僵直像人台；改为整理袖口的自然互动",
            )
            reviewer = FakeVisionReviewer(
                [True, True, True],
                [
                    passing_group_observation(),   # Look A gate
                    passing_group_observation(**{"look_c": blamed}),
                    passing_group_observation(),   # 修复后整组
                ],
            )
            kwargs, _, _ = prepare_kwargs(
                folder, "rec-repair-c", pack_persona(folder), generator, reviewer,
            )
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            result = service.prepare(**kwargs)
            self.assertEqual(result["repaired_roles_this_run"], ["look_c"])
            self.assertEqual(result["group_repair_attempts"], 1)
            self.assertEqual(len(generator.requests), 5)
            self.assertEqual(generator.requests[4].slot_index, 3)
            self.assertEqual(generator.requests[4].shot_version, 2)
            self.assertIn("整理袖口", generator.requests[4].plan_shot["purpose"])
            self.assertTrue(result["group_human_presentation_qa"]["passed"])
            # 整组通过后每个 source 带人物 QA 结果与动作合同证据。
            for source in result["sources"]:
                self.assertIn("human_presentation_qa", source)
                self.assertTrue(source["human_presentation_qa"]["passed"])
                self.assertIn("pose_contract", source)
                self.assertEqual(source["persona_pack_id"], "TH_APPAREL_REAL_01_001")
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-repair-c"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(
                manifest["attempt_history"][0]["human_alignment"]["failed_roles"],
                ["look_c"],
            )
            self.assertTrue(manifest["group_human_presentation_qa"]["passed"])

    def test_exhausted_human_repairs_save_evidence_and_fail_fast(self):
        with tempfile.TemporaryDirectory() as folder:
            generator = RecordingGenerator()
            blamed = human_observation(
                "look_b", pose_family="STATIC_MANNEQUIN", limb=True,
                repair_instruction="手臂僵直；改为自然行走摆臂",
            )
            reviewer = FakeVisionReviewer(
                [True, True, True, True],
                [
                    passing_group_observation(),   # gate
                    passing_group_observation(**{"look_b": blamed}),
                    passing_group_observation(**{"look_b": blamed}),
                    passing_group_observation(**{"look_b": blamed}),
                ],
            )
            kwargs, _, _ = prepare_kwargs(
                folder, "rec-exhaust", pack_persona(folder), generator, reviewer,
            )
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            with self.assertRaisesRegex(ValueError, "重做次数已用尽"):
                service.prepare(**kwargs)
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-exhaust"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "group_failed")
            # QA 瘦身后 MAX_GROUP_REPAIR_ATTEMPTS=1：一轮修复即用尽。
            self.assertEqual(manifest["group_repair_attempts"], 1)
            for entry in manifest["attempt_history"]:
                self.assertEqual(entry["human_alignment"]["failed_roles"], ["look_b"])
                self.assertEqual(entry["failed_roles"], ["look_b"])
            self.assertEqual(len(reviewer.human_calls), 3, "门禁1次+整组2轮后 group_failed，续跑不再调视觉模型")
            with self.assertRaisesRegex(ValueError, "已用尽"):
                service.prepare(**kwargs)

    def test_resume_keeps_human_qa_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            generator = RecordingGenerator(fail_calls={5})
            blamed = human_observation(
                "look_c", pose_family="STATIC_MANNEQUIN", limb=True,
                repair_instruction="改为整理袖口的自然互动",
            )
            reviewer = FakeVisionReviewer(
                [True, True, True],
                [
                    passing_group_observation(),   # gate
                    passing_group_observation(**{"look_c": blamed}),
                    passing_group_observation(),   # 续跑后整组
                ],
            )
            kwargs, _, _ = prepare_kwargs(
                folder, "rec-resume", pack_persona(folder), generator, reviewer,
            )
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            with self.assertRaisesRegex(ValueError, "look_c 风格参考生图失败"):
                service.prepare(**kwargs)
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-resume"
                             / "supply_manifest.json")
            pending = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(pending["status"], "group_repair_pending")
            self.assertEqual(
                pending["attempt_history"][0]["human_alignment"]["failed_roles"],
                ["look_c"],
            )
            self.assertTrue(pending["attempt_history"][0]["repair_notes"]["look_c"])
            result = service.prepare(**kwargs)
            self.assertEqual(result["generated_this_run"], 1)
            self.assertEqual(result["repaired_roles_this_run"], ["look_c"])
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "complete")
            self.assertTrue(manifest["group_human_presentation_qa"]["passed"])
            self.assertTrue(all(
                source.get("pose_contract") for source in manifest["sources"]
            ))


if __name__ == "__main__":
    unittest.main()
