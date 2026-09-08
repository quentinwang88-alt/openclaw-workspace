"""Planning-layer upgrade tests: codex vision client, global color plan,
numeric camera params, cross-image consistency (all fixture-based)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from PIL import Image

from services.image_generator import GenerationOutcome, compose_shot_prompt
from services.photo_color_consistency import (
    color_stats,
    evaluate_group_consistency,
    evaluate_visual_consistency,
)
from services.photo_reference_vision import (
    PhotoReferenceVisionError, PhotoReferenceVisionService,
    _normalize_color_grading_plan, _normalize_outfit_aesthetic,
    _normalize_palette_hex,
)
from services.photo_style_reference_supply import PhotoStyleReferenceSupplyService
from services.photo_theme import resolve_photo_theme
from tests.test_photo_reference_vision import FakeVisionClient
from tests.test_photo_human_production import (
    FakeVisionReviewer, RecordingGenerator, human_observation, pack_persona,
    passing_group_observation, scene_model_variation,
)


class ConsistencyAwareReviewer(FakeVisionReviewer):
    """human_reviews 消耗完后默认返回全通过观察；支持组级一致性观察。"""

    def __init__(self, alignment_passed, human_reviews, consistency_reviews=None):
        super().__init__(alignment_passed, human_reviews)
        self.consistency_reviews = list(consistency_reviews or [])
        self.consistency_calls = []

    def review_alignment(self, **kwargs):
        if not self.alignment_passed:
            self.alignment_passed.append(True)
        return super().review_alignment(**kwargs)

    def review_human_presentation(self, *, image_paths, role_order,
                                  persona_reference_paths=(), pose_contracts=None):
        self.human_calls.append({
            "image_paths": list(image_paths), "role_order": list(role_order),
            "persona_reference_paths": list(persona_reference_paths),
            "pose_contracts": dict(pose_contracts or {}),
        })
        if self.human_reviews:
            review = self.human_reviews.pop(0)
        else:
            review = {"roles": [
                human_observation(role, pose_family="RELAXED_STAND", gaze="CAMERA",
                                  expression="NEUTRAL")
                for role in role_order
            ]}
        return json.loads(json.dumps(review))

    def review_group_consistency(self, *, image_paths, role_order,
                                 persona_reference_paths=(), color_grading_plan=None):
        self.consistency_calls.append({"role_order": list(role_order)})
        if self.consistency_reviews:
            return self.consistency_reviews.pop(0)
        return {"roles": [
            {"role": role, "skin_tone_match": 92, "color_grading_match": 90,
             "lighting_match": 88, "drift_note_zh": ""}
            for role in role_order
        ]}


def _full_set():
    return {
        "content_angle_zh": "角度", "scene_zh": "街角",
        "palette_zh": "驼色", "background_prompt": "街景",
        "style_modifier": "松弛",
        "looks": [
            {"role": f"look_{letter}", "display_label": letter.upper(),
             "outerwear": f"外套{letter}", "top_inner": "内搭",
             "bottom": f"下装{letter}", "shoes": "德训鞋",
             "outfit_aesthetic": {"harmony": 90, "layering": 88,
                                  "color_balance": 90, "proportion": 87}}
            for letter in "abcd"
        ],
        "copy": {"title": "t", "cover": "c", "caption": "cap", "cta": "cta"},
    }


def sse_lines(*deltas):
    lines = [f"data: {json.dumps({'type': 'response.output_text.delta', 'delta': d})}"
             for d in deltas]
    lines.append("data: [DONE]")
    return lines


class FakeStreamResponse:
    def __init__(self, status_code=200, lines=None, body=b""):
        self.status_code = status_code
        self._lines = lines or []
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def iter_lines(self):
        return iter(self._lines)

    def read(self):
        return self._body


def envelope(text):
    return {"choices": [{"message": {"content": text}}]}


class CodexVisionClientTest(unittest.TestCase):
    def setUp(self):
        from services.codex_vision_client import CodexVisionClient
        self.client = CodexVisionClient(
            model="gpt-5.6-sol", reasoning_effort="medium",
            access_token="token", base_url="https://codex.example",
        )
        self.image = self._png()

    def _png(self, color=(120, 110, 100)):
        folder = Path(tempfile.mkdtemp())
        path = folder / "img.png"
        Image.new("RGB", (40, 60), color).save(path)
        return str(path)

    def _stream(self, responses, calls):
        import services.codex_vision_client as module

        def fake_stream(method, url, headers=None, json=None, timeout=None):
            calls.append({"method": method, "url": url, "payload": json})
            return responses.pop(0)
        return patch.object(module.httpx, "stream", side_effect=fake_stream)

    def test_stream_success_assembles_deltas(self):
        calls = []
        response = FakeStreamResponse(200, sse_lines('{"a"', ':1}'))
        with self._stream([response], calls):
            result = self.client.chat_with_multiple_images(
                [self.image], "提示词", instructions="只输出 JSON")
        self.assertEqual(result, envelope('{"a":1}'))
        self.assertEqual(json.loads(result["choices"][0]["message"]["content"]), {"a": 1})
        payload = calls[0]["payload"]
        self.assertTrue(payload["stream"])
        self.assertFalse(payload["store"])
        self.assertNotIn("max_output_tokens", payload)
        self.assertEqual(payload["model"], "gpt-5.6-sol")
        self.assertEqual(payload["reasoning"], {"effort": "medium"})
        input_image = payload["input"][0]["content"][0]
        self.assertTrue(input_image["image_url"].startswith("data:image/png;base64,"))

    def test_http_400_raises_without_retry(self):
        calls = []
        response = FakeStreamResponse(400, body=b'{"detail":"bad"}')
        with self._stream([response, response], calls):
            with self.assertRaisesRegex(Exception, "codex HTTP 400"):
                self.client.chat_with_multiple_images([self.image], "p")
        self.assertEqual(len(calls), 1, "4xx 参数错误不允许重试")

    def test_http_429_retries_once_then_succeeds(self):
        calls = []
        with self._stream([
            FakeStreamResponse(429, body=b"rate limited"),
            FakeStreamResponse(200, sse_lines('{"ok":true}')),
        ], calls):
            result = self.client.chat_with_multiple_images([self.image], "p")
        self.assertEqual(json.loads(result["choices"][0]["message"]["content"]), {"ok": True})
        self.assertEqual(len(calls), 2)

    def test_missing_token_raises(self):
        from services.codex_vision_client import CodexVisionClient
        with patch("services.codex_vision_client.resolve_codex_runtime",
                   return_value=("", "https://codex.example")):
            with self.assertRaisesRegex(Exception, "token"):
                CodexVisionClient(model="gpt-5.6-sol")


class ProviderRoutingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.images = []
        for index in range(2):
            path = self.root / f"ref-{index}.jpg"
            Image.new("RGB", (60, 90), (120, 90, 70)).save(path)
            self.images.append(str(path))

    def test_default_provider_is_codex_and_records_provider(self):
        contract = {
            "per_reference": [
                {"index": 1, "presentation": "SCENE_MODEL"},
                {"index": 2, "presentation": "SCENE_MODEL"},
            ],
            "aggregate": {"primary_presentation": "SCENE_MODEL", "confidence": .9},
            "recommended_sets": [_full_set()],
        }
        client = FakeVisionClient([contract])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        result = service.analyze(record_id="rec-route", paths=self.images,
                                 theme={}, category_key="womenswear", count=1)
        self.assertEqual(result["vision_provider"], "codex")

    def test_codex_failure_falls_back_to_doubao_once(self):
        class ExplodingClient:
            def chat_with_multiple_images(self, *args, **kwargs):
                raise RuntimeError("codex down")

        contract = {
            "per_reference": [
                {"index": 1, "presentation": "SCENE_MODEL"},
                {"index": 2, "presentation": "SCENE_MODEL"},
            ],
            "aggregate": {"primary_presentation": "SCENE_MODEL", "confidence": .9},
            "recommended_sets": [_full_set()],
        }
        fallback = FakeVisionClient([contract])
        service = PhotoReferenceVisionService(root=self.root)
        env = {
            "OPV_PHOTO_VISION_MODEL": "doubao-test",
            "OPV_PHOTO_VISION_API_URL": "https://doubao.example",
            "OPV_PHOTO_VISION_API_KEY": "key",
        }
        with patch.dict("os.environ", env):
            with patch.object(service, "_build_client",
                              side_effect=[ExplodingClient(), fallback]):
                result = service.analyze(record_id="rec-fb", paths=self.images,
                                         theme={}, category_key="womenswear", count=1)
        self.assertEqual(result["vision_provider"], "doubao_fallback")

    def test_doubao_provider_never_falls_back(self):
        service = PhotoReferenceVisionService(root=self.root)

        class ExplodingClient:
            def chat_with_multiple_images(self, *args, **kwargs):
                raise RuntimeError("doubao down")

        with patch.object(service, "_build_client", return_value=ExplodingClient()):
            with self.assertRaises(RuntimeError):
                service._chat(self.images, "p", 100)


class NormalizeAndReplanTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.images = []
        for index in range(2):
            path = self.root / f"ref-{index}.jpg"
            Image.new("RGB", (60, 90), (120, 90, 70)).save(path)
            self.images.append(str(path))

    def contract(self, scores=(90, 90, 90, 90)):
        return {
            "per_reference": [
                {"index": 1, "presentation": "SCENE_MODEL"},
                {"index": 2, "presentation": "SCENE_MODEL"},
            ],
            "aggregate": {"primary_presentation": "SCENE_MODEL", "confidence": .9},
            "color_grading_plan": {
                "temperature": "warm_4800k", "saturation": "medium_soft",
                "contrast": "gentle", "skin_tone_anchor": "冷白透亮",
                "tone_note_zh": "统一暖调",
            },
            "recommended_sets": [{
                "content_angle_zh": "角度", "scene_zh": "街角",
                "palette_zh": "驼色", "background_prompt": "街景",
                "style_modifier": "松弛",
                "looks": [
                    {"role": f"look_{letter}", "display_label": letter.upper(),
                     "outerwear": f"外套{letter}", "top_inner": "内搭",
                     "bottom": f"下装{letter}", "shoes": "德训鞋",
                     "palette_hex": ["#C9B99A", "#F5F1E8"],
                     "outfit_aesthetic": {
                         "harmony": scores[0], "layering": scores[1],
                         "color_balance": scores[2], "proportion": scores[3],
                         "issues": [], "revise_zh": "改配色"}}
                    for letter in "abcd"
                ],
                "copy": {"title": "t", "cover": "c", "caption": "cap", "cta": "cta"},
            }],
        }

    def service(self, contracts):
        return PhotoReferenceVisionService(
            root=self.root, client=FakeVisionClient(contracts))

    def test_analyze_passes_with_complete_plan(self):
        result = self.service([self.contract()]).analyze(
            record_id="rec-plan", paths=self.images, theme={},
            category_key="womenswear", count=1)
        self.assertEqual(result["color_grading_plan"]["temperature"], "warm_4800k")
        looks = result["recommended_sets"][0]["looks"]
        self.assertEqual(looks[0]["palette_hex"], ["#C9B99A", "#F5F1E8"])
        self.assertEqual(looks[0]["outfit_aesthetic"]["harmony"], 90)
        self.assertEqual(result["vision_provider"], "codex")

    def test_missing_color_plan_gets_safe_defaults(self):
        payload = self.contract()
        payload.pop("color_grading_plan")
        normalized = PhotoReferenceVisionService(
            root=self.root, client=FakeVisionClient([payload])
        ).analyze(record_id="rec-default", paths=self.images, theme={},
                  category_key="womenswear", count=1)
        self.assertEqual(normalized["color_grading_plan"]["temperature"], "neutral")
        self.assertIn("人物参考图", normalized["color_grading_plan"]["skin_tone_anchor"])

    def test_invalid_hex_is_dropped(self):
        payload = self.contract()
        looks = payload["recommended_sets"][0]["looks"]
        looks[0]["palette_hex"] = ["C9B99A", "zzzzzz", "#12"]
        result = PhotoReferenceVisionService(
            root=self.root, client=FakeVisionClient([payload])
        ).analyze(record_id="rec-hex", paths=self.images, theme={},
                  category_key="womenswear", count=1)
        self.assertEqual(
            result["recommended_sets"][0]["looks"][0]["palette_hex"], ["#C9B99A"])

    def test_low_aesthetic_records_but_never_replans(self):
        # 2026-09-07 用户裁决：自评低分不再触发第二次模型调用。
        weak = self.contract(scores=(70, 88, 80, 90))
        client = FakeVisionClient([weak])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        result = service.analyze(record_id="rec-replan", paths=self.images,
                                 theme={}, category_key="womenswear", count=1)
        self.assertEqual(result["recommended_sets"][0]["looks"][0]
                         ["outfit_aesthetic"]["harmony"], 70)
        self.assertEqual(len(client.calls), 1, "自评低分不得再次调用模型")

    def test_normalize_helpers(self):
        self.assertEqual(_normalize_palette_hex(["c9b99a", "#F5F1E8", "bad"]),
                         ["#C9B99A", "#F5F1E8"])
        self.assertEqual(_normalize_color_grading_plan(None)["temperature"], "neutral")
        self.assertEqual(_normalize_outfit_aesthetic("bad"), {})
        clamped = _normalize_outfit_aesthetic({"harmony": 180, "layering": -1})
        self.assertEqual((clamped["harmony"], clamped["layering"]), (100, 0))


class ColorConsistencyTest(unittest.TestCase):
    def _png(self, folder, name, color):
        path = Path(folder) / name
        Image.new("RGB", (120, 180), color).save(path)
        return str(path)

    def test_identical_images_pass_and_single_image_never_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = [self._png(folder, f"{i}.png", (150, 140, 130)) for i in range(4)]
            verdict = evaluate_group_consistency(paths)
            self.assertTrue(verdict["passed"], verdict["issues"])
            single = evaluate_group_consistency(paths[:1])
            self.assertTrue(single["passed"])

    def test_color_divergent_group_fails_with_attribution(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = [
                self._png(folder, "warm.png", (200, 180, 160)),
                self._png(folder, "warm2.png", (198, 182, 158)),
                self._png(folder, "cool.png", (90, 160, 220)),
                self._png(folder, "warm3.png", (202, 178, 162)),
            ]
            verdict = evaluate_group_consistency(paths)
            self.assertFalse(verdict["passed"])
            self.assertTrue(verdict["issues"])
            self.assertEqual(len(verdict["failed_roles"]), 1)
            failed_path = paths[verdict["failed_roles"][0]]
            self.assertIn("cool.png", failed_path)
            stats = color_stats(paths[0])
            self.assertIn("rb_ratio", stats)

    def test_visual_consistency_verdict_ignores_model_suggestion(self):
        review = {"roles": [
            {"role": "look_a", "skin_tone_match": 92, "color_grading_match": 90,
             "lighting_match": 88, "drift_note_zh": ""},
            {"role": "look_b", "skin_tone_match": 60, "color_grading_match": 88,
             "lighting_match": 90, "drift_note_zh": "肤色偏黄"},
        ]}
        verdict = evaluate_visual_consistency(review, ["look_a", "look_b"])
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["failed_roles"], ["look_b"])
        self.assertIn("肤色偏黄", verdict["roles"]["look_b"]["drift_note_zh"])


class PromptRenderingTest(unittest.TestCase):
    def _request(self, presentation="SCENE_MODEL", with_color=True):
        from services.image_generator import ShotGenerationRequest
        recipe_execution = {
            "content_goal": "multi_look", "reference_mode": "STYLE",
            "transform_mode": "style_reference_variation", "theme_brief": {},
            "presentation_profile": {"presentation_type": presentation,
                                     "reference_style_profile": {}},
            "locale": "th-TH",
            "human_presentation_contract": {
                "head": {"require_level_neck": True},
                "face": {"require_real_skin_texture": True},
                "body": {"require_believable_weight_distribution": True},
                "identity": {"forbid_copying_reference_pose": True},
            },
        }
        if with_color:
            recipe_execution["color_grading_plan"] = {
                "temperature": "warm_4800k", "saturation": "medium_soft",
                "contrast": "gentle", "skin_tone_anchor": "冷白透亮",
                "tone_note_zh": "统一暖调",
            }
        return ShotGenerationRequest(
            task_id="demo", slot_index=2, slot_role="full_look", shot_version=1,
            plan_shot={"slot_index": 2, "slot_role": "look_b", "purpose": "p",
                       "composition_contract": {
                           "framing": "full_body", "instruction": "全身",
                           "camera_angle": "eye_level", "pose": "行走",
                           "pose_contract": {
                               "pose_family": "WALKING_CANDID", "gaze": "FORWARD",
                               "head_posture": "LEVEL",
                               "action_zh": "自然行走", "gaze_zh": "看向前方",
                               "head_zh": "水平", "body_zh": "摆臂",
                               "camera_params": {"subject_height_ratio": "0.72-0.82",
                                                 "lens": "50mm",
                                                 "camera_angle_deg": 45,
                                                 "camera_position": "front_right",
                                                 "horizon": "upper_third"},
                           }}},
            product={}, persona_snapshot={"name": "p"},
            look_snapshot={"recipe": {"top_inner": "内搭",
                                      "palette_hex": ["#C9B99A", "#F5F1E8"]}},
            scene_snapshot={"name": "s", "prompt_core": "海边"},
            output_dir="/tmp", continuity_reference_images=[],
            recipe_execution=recipe_execution,
        )

    def test_color_contract_and_numeric_camera_rendered(self):
        prompt = compose_shot_prompt(self._request())
        self.assertIn("【全局色彩合同】", prompt)
        self.assertIn("全组统一色调：色温基准 warm_4800k", prompt)
        self.assertIn("人物肤色以人物参考图为唯一权威标准", prompt)
        self.assertIn("#C9B99A", prompt)
        # 伪精确参数已删：构图只保留画面意图。
        self.assertIn("自然行走", prompt)
        self.assertNotIn("构图数值（必须执行）", prompt)
        self.assertNotIn("50mm", prompt)

    def test_flat_lay_gets_color_but_not_skin_or_human_contract(self):
        request = self._request(presentation="FLAT_LAY")
        request.recipe_execution["presentation_profile"]["presentation_type"] = "FLAT_LAY"
        prompt = compose_shot_prompt(request)
        self.assertIn("【全局色彩合同】", prompt)
        self.assertNotIn("人物肤色以人物参考图为唯一权威标准", prompt)
        self.assertNotIn("【人物摄影合同】", prompt)


class SupplyConsistencyIntegrationTest(unittest.TestCase):
    def _kwargs(self, folder, record_id, generator, reviewer, with_plan=True):
        reference = Path(folder) / "reference.png"
        Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
        variation = scene_model_variation()
        if with_plan:
            variation["style_profile"]["color_grading_plan"] = {
                "temperature": "warm_4800k", "saturation": "medium_soft",
                "contrast": "gentle", "skin_tone_anchor": "冷白透亮",
                "tone_note_zh": "统一暖调",
            }
        return {
            "record_id": record_id, "reference_paths": [str(reference)],
            "theme": resolve_photo_theme("秋季穿搭"),
            "account": SimpleNamespace(persona_ref_id="TH_APPAREL_REAL_01_001"),
            "persona": pack_persona(folder), "variation": variation,
            "progress": None,
        }

    def test_consistency_pass_full_flow(self):
        with tempfile.TemporaryDirectory() as folder:
            class UniformGenerator(RecordingGenerator):
                def generate_shot(self, request):
                    self.prompts.append(compose_shot_prompt(request))
                    self.requests.append(request)
                    self.total_calls += 1
                    path = (Path(request.output_dir)
                            / f"look-{request.slot_index}_v{request.shot_version}.png")
                    Image.new("RGB", (120, 180), (150, 140, 130)).save(path)
                    return GenerationOutcome(ok=True, image_path=str(path),
                                             request_id="x")

            generator = UniformGenerator()
            reviewer = ConsistencyAwareReviewer(
                [True, True],
                [
                    {"roles": [human_observation("look_a")]},
                    passing_group_observation(),
                ],
            )
            kwargs = self._kwargs(folder, "rec-cons-pass", generator, reviewer)
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer)
            result = service.prepare(**kwargs)
            consistency = result["group_consistency_qa"]
            self.assertTrue(consistency["program"]["passed"])
            # 肤色视觉 QA 默认关闭：只有程序化全图调色一致性。
            self.assertNotIn("visual", consistency)
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-cons-pass"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(manifest["quality"]["quality_gate"], "passed")
            self.assertTrue(manifest["quality"]["publish_ready"])

    def test_visual_consistency_failure_triggers_targeted_repair(self):
        with tempfile.TemporaryDirectory() as folder:
            class UniformGenerator(RecordingGenerator):
                def generate_shot(self, request):
                    self.prompts.append(compose_shot_prompt(request))
                    self.requests.append(request)
                    self.total_calls += 1
                    path = (Path(request.output_dir)
                            / f"look-{request.slot_index}_v{request.shot_version}.png")
                    Image.new("RGB", (120, 180), (150, 140, 130)).save(path)
                    return GenerationOutcome(ok=True, image_path=str(path), request_id="x")

            generator = UniformGenerator()
            drift = {
                "roles": [
                    {"role": role, "skin_tone_match": 92, "color_grading_match": 90,
                     "lighting_match": 88, "drift_note_zh": ""}
                    for role in ("look_a", "look_b", "look_d")
                ] + [
                    {"role": "look_c", "skin_tone_match": 70, "color_grading_match": 88,
                     "lighting_match": 90, "drift_note_zh": "肤色偏暗偏黄"},
                ],
            }
            reviewer = ConsistencyAwareReviewer(
                [True, True],
                [{"roles": [human_observation("look_a")]}, passing_group_observation(),
                 passing_group_observation()],
                consistency_reviews=[drift, {"roles": [
                    {"role": role, "skin_tone_match": 92, "color_grading_match": 90,
                     "lighting_match": 88, "drift_note_zh": ""}
                    for role in ("look_a", "look_b", "look_c", "look_d")
                ]}],
            )
            kwargs = self._kwargs(folder, "rec-visual-fail", generator, reviewer)
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer)
            with patch.dict("os.environ", {"OPV_PHOTO_SKIN_QA": "on"}):
                result = service.prepare(**kwargs)
            # 肤色视觉差异只入 quality_warnings，不触发付费重生。
            self.assertEqual(result["repaired_roles_this_run"], [])
            self.assertEqual(result["group_repair_attempts"], 0)
            self.assertFalse(result["group_consistency_qa"]["visual"]["passed"])
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-visual-fail"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(manifest["quality"]["quality_gate"], "passed")
            skin = [w for w in manifest["quality"]["quality_warnings"]
                    if w["code"] == "SKIN_TONE_NOTE"]
            self.assertTrue(skin)

    def test_consistency_fail_repairs_divergent_roles(self):
        with tempfile.TemporaryDirectory() as folder:
            generator = RecordingGenerator()
            reviewer = ConsistencyAwareReviewer([True, True], [])
            kwargs = self._kwargs(folder, "rec-cons-fail", generator, reviewer)
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer)
            with self.assertRaisesRegex(ValueError, "跨图一致性检查未通过且重做次数已用尽"):
                service.prepare(**kwargs)
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-cons-fail"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "group_failed")
            self.assertFalse(manifest["group_consistency_qa"]["program"]["passed"])
            first_round = manifest["attempt_history"][0]
            self.assertIn("consistency_alignment", first_round)
            self.assertTrue(any("肤色/色调偏离" in note
                                for note in first_round["repair_notes"].values()))


if __name__ == "__main__":
    unittest.main()
