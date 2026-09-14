from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from services.photo_reference_vision import (  # noqa: F401
    PhotoReferenceVisionError, PhotoReferenceVisionService, parse_vision_envelope,
)


def recommendation(index=1):
    return {
        "content_angle_zh": f"法式复古第{index}组", "scene_zh": "街角咖啡店",
        "palette_zh": "驼色、奶油色、酒红色", "background_prompt": "欧洲街角咖啡店自然光",
        "style_modifier": "法式复古、松弛自然",
        "looks": [
            {"role": f"look_{letter}", "display_label": f"ลุค {letter.upper()}",
             "outerwear": f"复古外套{index}{letter}", "top_inner": f"奶油蕾丝内搭{index}",
             "bottom": f"深色下装{index}{letter}", "shoes": f"酒红皮鞋{index}",
             "outerwear_type": f"coat_{index}_{letter}", "bottom_type": f"bottom_{index}_{letter}"}
            for letter in "abcd"
        ],
        "copy": {"title": f"แฟชั่นวินเทจ {index}", "cover": "ลุคไหนที่คุณชอบ",
                 "caption": "เลือกหนึ่งลุคที่คุณชอบ", "cta": "A B C หรือ D?"},
    }


class FakeVisionClient:
    """Provider-compatible fake: returns real-client envelopes."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def chat_with_multiple_images(self, paths, prompt, max_tokens):
        self.calls.append((list(paths), prompt, max_tokens))
        payload = self.responses.pop(0)
        return {"choices": [{"message": {"content": json.dumps(payload, ensure_ascii=False)}}]}

    @staticmethod
    def parse_json_response(response):
        return parse_vision_envelope(response)


class PhotoReferenceVisionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.images = []
        for index in range(2):
            path = self.root / f"ref-{index}.jpg"
            Image.new("RGB", (60, 90), (120 + index, 90, 70)).save(path)
            self.images.append(str(path))

    def tearDown(self):
        self.tmp.cleanup()

    def analysis(self, count=1):
        return {
            "per_reference": [
                {"index": 1, "reference_uses": ["ENVIRONMENT", "VISUAL_STYLE"],
                 "presentation": "SCENE_MODEL", "background_cues": ["街角咖啡店"]},
                {"index": 2, "reference_uses": ["OUTFIT", "VISUAL_STYLE"],
                 "presentation": "EDITORIAL_COLLAGE", "outfit_formula": "短外套配高腰裤"},
            ],
            "aggregate": {
                "primary_presentation": "SCENE_MODEL", "confidence": .94,
                "visual_styles": ["french_vintage"], "season": "autumn",
                "palette": ["camel", "cream", "burgundy"], "temperature": "warm",
                "materials": ["wool", "lace"], "scenes": ["cafe", "european_street"],
                "lighting": "soft natural", "background": "street cafe",
                "composition": "full body environmental portrait", "avoid_tags": ["studio"],
            },
            "recommended_sets": [recommendation(index) for index in range(1, count + 1)],
        }

    def test_analysis_builds_scene_model_contract_and_uses_cache(self):
        client = FakeVisionClient([self.analysis(count=2)])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        kwargs = {
            "record_id": "rec-vision", "paths": self.images,
            "theme": {"theme_key": "AUTUMN_OUTFIT", "label_zh": "秋季穿搭"},
            "category_key": "womenswear", "content_requirement": "法系复古氛围感",
            "count": 2,
        }
        result = service.analyze(**kwargs)
        cached = service.analyze(**kwargs)
        self.assertEqual(result, cached)
        self.assertEqual(result["presentation_type"], "SCENE_MODEL")
        self.assertEqual(result["recommended_sets"][1]["index"], 2)
        self.assertEqual(result["environment_reference"]["indices"], [1])
        self.assertEqual(result["outfit_reference"]["indices"], [2])
        self.assertEqual(result["visual_style_reference"]["indices"], [1, 2])
        self.assertEqual(len(client.calls), 1)

    def test_truncated_analysis_retries_once_with_enlarged_budget(self):
        """预算被用尽时重试一次；这条线正是被 2200 token 打断的（2026-09-14）。"""
        truncated = {"choices": [{"finish_reason": "length", "message": {
            "content": '{"per_reference": [{"index": 1, "reference_uses": ["OUTFIT"'}}]}
        budgets = []
        payload = self.analysis()

        class RecordingClient:
            def chat_with_multiple_images(self, paths, prompt, max_tokens):
                budgets.append(max_tokens)
                if len(budgets) == 1:
                    return truncated
                return {"choices": [{"finish_reason": "stop", "message": {
                    "content": json.dumps(payload, ensure_ascii=False)}}]}

        service = PhotoReferenceVisionService(root=self.root, client=RecordingClient())
        result = service.analyze(
            record_id="rec-retry", paths=self.images, theme={},
            category_key="womenswear", count=1,
        )
        self.assertEqual(len(budgets), 2, "截断后必须放大预算重试一次")
        self.assertEqual(budgets[1], budgets[0] * 2)
        self.assertEqual(result["presentation_type"], "SCENE_MODEL")

    def test_low_confidence_is_rejected(self):
        payload = self.analysis()
        payload["aggregate"]["confidence"] = .4
        service = PhotoReferenceVisionService(root=self.root, client=FakeVisionClient([payload]))
        with self.assertRaisesRegex(PhotoReferenceVisionError, "置信度"):
            service.analyze(record_id="low", paths=self.images, theme={},
                            category_key="womenswear")

    def test_alignment_requires_each_core_score_to_pass(self):
        response = {"passed": True, "scores": {
            "presentation_alignment": 92, "style_alignment": 88,
            "scene_alignment": 70, "palette_alignment": 90, "look_difference": 90,
        }, "reason_codes": ["SCENE_DRIFT"], "notes": "场景退化"}
        service = PhotoReferenceVisionService(root=self.root, client=FakeVisionClient([response]))
        result = service.review_alignment(
            reference_paths=self.images[:1], generated_paths=self.images[1:],
            contract={"presentation_type": "SCENE_MODEL"}, scope="FIRST_LOOK_A",
        )
        self.assertFalse(result["passed"])

    def test_alignment_normalizes_per_look_findings_for_group_repair(self):
        response = {"passed": False, "scores": {
            "presentation_alignment": 90, "style_alignment": 86,
            "scene_alignment": 58, "palette_alignment": 88, "look_difference": 82,
        }, "reason_codes": ["SCENE_DRIFT"], "notes": "一张场景退化",
            "per_look": [
                {"role": "look_a", "passed": True, "issues": []},
                {"role": "look_b", "passed": False, "issues": ["场景退化成纯色背景"]},
                {"role": "look_x", "passed": False, "issues": ["未知角色"]},
                {"role": "look_c", "passed": "yes", "issues": []},
            ]}
        service = PhotoReferenceVisionService(root=self.root, client=FakeVisionClient([response]))
        result = service.review_alignment(
            reference_paths=self.images[:1], generated_paths=self.images[1:],
            contract={"presentation_type": "SCENE_MODEL"}, scope="FULL_LOOK_GROUP",
            generated_roles=["look_a", "look_b", "look_c", "look_d"],
        )
        self.assertFalse(result["passed"])
        findings = {item["role"]: item for item in result["role_findings"]}
        self.assertEqual(set(findings), {"look_a", "look_b"})
        self.assertTrue(findings["look_a"]["passed"])
        self.assertFalse(findings["look_b"]["passed"])
        self.assertEqual(findings["look_b"]["issues"], ["场景退化成纯色背景"])
        bare = PhotoReferenceVisionService(
            root=self.root, client=FakeVisionClient([{**response, "per_look": None}]),
        ).review_alignment(
            reference_paths=self.images[:1], generated_paths=self.images[1:],
            contract={"presentation_type": "SCENE_MODEL"}, scope="FULL_LOOK_GROUP",
            generated_roles=["look_a"],
        )
        self.assertEqual(bare["role_findings"], [], "缺少逐张归因时必须返回空列表以触发整组重做")


class ParseVisionEnvelopeTest(unittest.TestCase):
    def test_parses_fenced_and_junk_wrapped_json(self):
        fenced = {"choices": [{"message": {"content": "```json\n{\"ok\": 1}\n```"}}]}
        self.assertEqual(parse_vision_envelope(fenced), {"ok": 1})
        junk = {"choices": [{"message": {
            "content": "好的，以下是分析：\n{\"ok\": 2}\n希望有帮助"}}]}
        self.assertEqual(parse_vision_envelope(junk), {"ok": 2})


class ReferenceAnalysisOutputBudgetTest(unittest.TestCase):
    """参考图描述输出预算随张数增长，截断时放大一次重试。

    2026-09-14 越南围巾线：3 张参考图的逐图描述在 4919 字符处被 2600 tokens
    截断（finish_reason=length），整行规划失败。
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.images = []
        for index in range(3):
            path = self.root / f"ref-{index}.png"
            Image.new("RGB", (60, 90), (110 + index, 95, 75)).save(path)
            self.images.append(str(path))

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def _analysis(count):
        return {
            "per_reference": [
                {"index": index, "reference_uses": ["OUTFIT", "VISUAL_STYLE"],
                 "use_source": "auto", "presentation": "SCENE_MODEL",
                 "garment_cues": ["米色针织围巾"], "background_cues": ["石板路"]}
                for index in range(1, count + 1)
            ],
            "aggregate": {
                "primary_presentation": "SCENE_MODEL", "confidence": .9,
                "visual_styles": ["korean_casual"], "season": "winter",
                "palette": ["beige", "brown"], "temperature": "cool",
                "materials": ["wool"], "lighting": "overcast",
                "background": "old town street", "climate": "urban_old_town",
                "destination_visual_style": "河内老城街道", "avoid_tags": [],
            },
        }

    def test_budget_scales_with_reference_count_and_caps(self):
        service = PhotoReferenceVisionService(root=self.root)
        self.assertEqual(service.reference_analysis_token_budget(1), 3200)
        self.assertEqual(service.reference_analysis_token_budget(3), 5200)
        self.assertEqual(service.reference_analysis_token_budget(4), 6200)
        self.assertEqual(
            service.reference_analysis_token_budget(20),
            service.VISION_OUTPUT_CEILING,
        )

    def test_analysis_budget_covers_references_and_look_panels(self):
        service = PhotoReferenceVisionService(root=self.root)
        # 2026-09-14 越南围巾线实况：1 篇 × 4 look + 3 张参考图，旧公式 2200
        # token 被 doubao 用尽即截断。
        self.assertEqual(
            service.analysis_token_budget(count=1, image_count=3, role_count=4), 7500,
        )
        self.assertGreater(
            service.analysis_token_budget(count=1, image_count=3, role_count=4), 2200,
        )
        self.assertGreater(
            service.analysis_token_budget(count=5, image_count=3, role_count=4),
            service.analysis_token_budget(count=1, image_count=3, role_count=4),
        )
        self.assertEqual(
            service.analysis_token_budget(count=9, image_count=9, role_count=9),
            service.VISION_OUTPUT_CEILING,
        )

    def test_truncated_response_retries_once_with_enlarged_budget(self):
        truncated = {"choices": [{
            "finish_reason": "length",
            "message": {"content": '{"per_reference":[{"index":1,"reference_uses":["OUTFIT"'},
        }]}
        valid = {"choices": [{"finish_reason": "stop", "message": {
            "content": json.dumps(self._analysis(3), ensure_ascii=False)}}]}
        budgets = []

        class RecordingClient:
            def chat_with_multiple_images(self, paths, prompt, max_tokens):
                budgets.append(max_tokens)
                return truncated if len(budgets) == 1 else valid

        service = PhotoReferenceVisionService(root=self.root, client=RecordingClient())
        analysis = service.analyze_reference(
            record_id="rec-truncated", paths=self.images,
            theme={"theme_key": "MATCHING_CHOICE", "label_zh": "围巾搭配"},
            category_key="scarf", content_requirement="照着参考图的三套搭配出图",
        )
        self.assertEqual(budgets, [5200, 10400], "截断后必须放大预算重试一次")
        self.assertEqual(analysis["presentation_type"], "SCENE_MODEL")
        self.assertEqual(len(analysis["per_reference"]), 3)

    def test_other_failures_do_not_retry(self):
        unparsable = {"choices": [{"finish_reason": "stop", "message": {
            "content": "模型抽风输出"}}]}
        budgets = []

        class RecordingClient:
            def chat_with_multiple_images(self, paths, prompt, max_tokens):
                budgets.append(max_tokens)
                return unparsable

        service = PhotoReferenceVisionService(root=self.root, client=RecordingClient())
        with self.assertRaisesRegex(PhotoReferenceVisionError, "没有返回有效 JSON"):
            service.analyze_reference(
                record_id="rec-garbage", paths=self.images, theme={},
                category_key="scarf",
            )
        self.assertEqual(len(budgets), 1, "非截断错误不得重试")

    def test_truncated_response_reports_length_and_diagnostics(self):
        truncated = {"choices": [{"finish_reason": "length",
                                  "message": {"content": "{\"ok\": 1, \"items\": ["}}]}
        with self.assertRaisesRegex(PhotoReferenceVisionError, "截断.*finish_reason=length"):
            parse_vision_envelope(truncated)

    def test_empty_content_reports_head_diagnostics(self):
        empty = {"choices": [{"finish_reason": "stop", "message": {"content": ""}}],
                 "error": {"message": "quota"}}
        with self.assertRaisesRegex(PhotoReferenceVisionError, "content为空.*quota"):
            parse_vision_envelope(empty)


class DoubaoFastPathRetryTest(unittest.TestCase):
    """偶发坏 JSON 必须就地重试一次，而不是让整轮 QA 失败。"""

    def test_fast_path_retries_once_on_garbage_then_returns_valid(self):
        import types
        responses = [
            {"choices": [{"message": {"content": "模型抽风输出"}}]},
            {"choices": [{"message": {"content": "{\"ok\": true}"}}]},
        ]
        calls = []

        class FakeDoubao:
            def chat_with_multiple_images(self, paths, prompt, max_tokens):
                calls.append(max_tokens)
                return responses.pop(0)

        from services.photo_reference_vision import PhotoReferenceVisionService
        service = PhotoReferenceVisionService.__new__(PhotoReferenceVisionService)
        service.client = None
        service.provider = "doubao"
        service.model, service.api_url, service.api_key = "m", "https://x", "k"
        service._build_client = lambda provider: FakeDoubao()
        response, used = service._chat([], "prompt", 2600, prefer="fast")
        self.assertEqual(used, "doubao")
        self.assertEqual(len(calls), 2, "第一次坏响应后应重试一次")
        self.assertEqual(parse_vision_envelope(response), {"ok": True})

    def test_fast_path_gives_up_after_two_bad_responses(self):
        class FakeDoubao:
            def chat_with_multiple_images(self, paths, prompt, max_tokens):
                return {"choices": [{"message": {"content": "还是坏的"}}]}

        from services.photo_reference_vision import PhotoReferenceVisionService
        service = PhotoReferenceVisionService.__new__(PhotoReferenceVisionService)
        service.client = None
        service.provider = "doubao"
        service.model, service.api_url, service.api_key = "m", "https://x", "k"
        service._build_client = lambda provider: FakeDoubao()
        with self.assertRaises(PhotoReferenceVisionError):
            service._chat([], "prompt", 2600, prefer="fast")


if __name__ == "__main__":
    unittest.main()
