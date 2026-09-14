from __future__ import annotations

import hashlib
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

    # --- 指定商品（围巾）属于核心商品，不适用「配饰有无不受罚」(2026-09-14) ----
    # 围巾搭配线走的是组级 alignment 检查：它原本把「配饰有无」列进不受罚清单，
    # 而类目适配器（SCARF_V1）声明围巾的主槽位正是 accessories —— 于是指定围巾
    # 丢了也不会失败。这里守住「类目适配器说的主槽位就是核心商品」这条边界。

    @staticmethod
    def _passing_response():
        return {"passed": True, "scores": {
            "presentation_alignment": 90, "style_alignment": 88,
            "scene_alignment": 86, "palette_alignment": 90, "look_difference": 84,
        }, "reason_codes": [], "notes": "通过"}

    @staticmethod
    def _digest(path: str) -> str:
        """服务会把每张图归一化复制成 reference_contracts/_model_inputs/<sha256>.jpg。

        因此"商品图有没有真的进模型输入"只能按**文件内容**比对，比原路径必然对不上。
        """
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def _product_image(self) -> str:
        path = self.root / "product-scarf.jpg"
        Image.new("RGB", (60, 90), (200, 60, 40)).save(path)
        return str(path)

    def _generated_image(self) -> str:
        path = self.root / "generated-look-a.jpg"
        Image.new("RGB", (60, 90), (12, 34, 56)).save(path)
        return str(path)

    def test_group_alignment_puts_the_designated_product_first_and_names_it(self):
        product = self._product_image()
        generated = self._generated_image()
        client = FakeVisionClient([self._passing_response()])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        service.review_alignment(
            reference_paths=self.images,
            generated_paths=[generated],
            contract={"presentation_type": "SCENE_MODEL"},
            scope="FULL_LOOK_GROUP", generated_roles=["look_a", "look_b"],
            product_reference_paths=[product],
            product_context={"product_id": "P1", "product_name": "格纹羊毛围巾",
                             "category": "scarf"},
        )
        sent_paths, prompt, _ = client.calls[0]
        # 检查端必须真的看到商品参考图，而不只是提示词声称有商品：
        # 商品图排最前，其后依次是风格参考与生成结果，且进模型的字节与源文件一致。
        self.assertEqual(Path(sent_paths[0]).stem, self._digest(product))
        self.assertEqual(
            [Path(value).stem for value in sent_paths],
            [self._digest(product)] + [self._digest(value) for value in self.images]
            + [self._digest(generated)],
        )
        self.assertIn("【核心商品】", prompt)
        self.assertIn("格纹羊毛围巾", prompt)
        self.assertIn("前 1 张是指定商品参考图", prompt)
        # 明确写出「配饰有无不受罚」不覆盖这个指定商品。
        self.assertIn("不适用于上面【核心商品】里点名的指定商品", prompt)
        self.assertIn("围法、褶皱、佩戴位置与细微纹理差异只写 notes", prompt)

    def test_group_alignment_without_a_product_keeps_the_accessory_leniency(self):
        """无指定商品的围巾内容保持自由搭配：不得要求四套围巾一致。"""
        generated = self._generated_image()
        # 故意重复一张风格参考：无指定商品时服务必须原样保留顺序与重复，
        # 参考图数量与旧行为逐字一致（不得顺手去重）。
        repeated = list(self.images) + self.images[:1]
        client = FakeVisionClient([self._passing_response()])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        service.review_alignment(
            reference_paths=repeated, generated_paths=[generated],
            contract={"presentation_type": "SCENE_MODEL"}, scope="FULL_LOOK_GROUP",
            generated_roles=["look_a"],
        )
        sent_paths, prompt, _ = client.calls[0]
        self.assertEqual(
            [Path(value).stem for value in sent_paths],
            [self._digest(value) for value in repeated] + [self._digest(generated)],
        )
        self.assertNotIn("【核心商品】", prompt)
        self.assertNotIn("不适用于上面", prompt)
        # 原宽松规则仍在：配饰有无只写 notes。
        self.assertIn("配饰有无", prompt)

    # --- 核心商品错误必须是结构化硬信号，不靠中文关键词白名单 (2026-09-14) ----
    # 复现：role_findings 里 look_b passed=false，issues 是「围巾颜色家族错误／
    # 围巾结构错误／指定围巾完全看不到」——三个短语一个都不在供给侧的
    # FAILURE_HINTS 白名单里，被判成细节提示 ⇒ 不重生任何图、只烧组级重做次数
    # ⇒ group_failed（QA 空转）。修法是在模型响应里带一个布尔硬信号，并在
    # 「没指定商品」时**强制**为假，自由搭配的配饰变化保持原宽松路径。

    @staticmethod
    def _core_product_failure_response():
        return {
            "passed": False,
            "scores": {"presentation_alignment": 90, "style_alignment": 88,
                       "scene_alignment": 86, "palette_alignment": 90,
                       "look_difference": 84},
            "reason_codes": ["PRODUCT_MISMATCH"], "notes": "look_b 的围巾不是指定商品",
            "per_look": [
                {"role": "look_a", "passed": True, "issues": []},
                {"role": "look_b", "passed": False,
                 "issues": ["围巾颜色家族错误", "围巾结构错误", "指定围巾完全看不到"],
                 "core_product_mismatch": True},
            ],
        }

    def test_core_product_mismatch_is_kept_for_a_designated_product(self):
        product = self._product_image()
        client = FakeVisionClient([self._core_product_failure_response()])
        result = PhotoReferenceVisionService(
            root=self.root, client=client,
        ).review_alignment(
            reference_paths=self.images[:1], generated_paths=[self._generated_image()],
            contract={}, scope="FULL_LOOK_GROUP", generated_roles=["look_a", "look_b"],
            product_reference_paths=[product],
            product_context={"product_id": "P1", "product_name": "格纹羊毛围巾"},
        )
        findings = {item["role"]: item for item in result["role_findings"]}
        self.assertTrue(findings["look_b"]["core_product_mismatch"])
        self.assertFalse(findings["look_a"]["core_product_mismatch"])
        self.assertEqual(findings["look_b"]["issues"],
                         ["围巾颜色家族错误", "围巾结构错误", "指定围巾完全看不到"])
        # 提示词必须明确要求这个字段，否则模型不会返回。
        prompt = client.calls[0][1]
        self.assertIn("core_product_mismatch", prompt)
        self.assertIn("【核心商品】", prompt)

    def test_core_product_flag_is_forced_off_without_a_designated_product(self):
        """同一份响应：没指定商品时旗标必须为假，自由搭配仍按原宽松路径。"""
        client = FakeVisionClient([self._core_product_failure_response()])
        result = PhotoReferenceVisionService(
            root=self.root, client=client,
        ).review_alignment(
            reference_paths=self.images[:1], generated_paths=[self._generated_image()],
            contract={}, scope="FULL_LOOK_GROUP", generated_roles=["look_a", "look_b"],
        )
        findings = {item["role"]: item for item in result["role_findings"]}
        self.assertFalse(findings["look_b"]["core_product_mismatch"])
        # 无指定商品时提示词逐字不变：既不出现【核心商品】，也不要求该字段。
        prompt = client.calls[0][1]
        self.assertNotIn("【核心商品】", prompt)
        self.assertNotIn("core_product_mismatch", prompt)

    def test_legacy_response_without_the_field_keeps_the_old_path(self):
        """旧响应没有这个键 ⇒ 视为假，不追溯把历史任务拦成硬失败。"""
        response = self._core_product_failure_response()
        for item in response["per_look"]:
            item.pop("core_product_mismatch", None)
        result = PhotoReferenceVisionService(
            root=self.root, client=FakeVisionClient([response]),
        ).review_alignment(
            reference_paths=self.images[:1], generated_paths=[self._generated_image()],
            contract={}, scope="FULL_LOOK_GROUP", generated_roles=["look_a", "look_b"],
            product_reference_paths=[self._product_image()],
            product_context={"product_id": "P1", "product_name": "格纹羊毛围巾"},
        )
        findings = {item["role"]: item for item in result["role_findings"]}
        self.assertFalse(findings["look_b"]["core_product_mismatch"])

    def test_a_missing_product_image_is_not_claimed_as_sent(self):
        """商品参考图文件缺失时，提示词不得声称"前 N 张是商品参考图"。"""
        generated = self._generated_image()
        client = FakeVisionClient([self._passing_response()])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        service.review_alignment(
            reference_paths=self.images, generated_paths=[generated],
            contract={}, scope="FULL_LOOK_GROUP", generated_roles=["look_a"],
            product_reference_paths=[str(self.root / "does-not-exist.jpg")],
            product_context={"product_id": "P1", "product_name": "格纹羊毛围巾"},
        )
        sent_paths, prompt, _ = client.calls[0]
        self.assertEqual(
            [Path(value).stem for value in sent_paths],
            [self._digest(value) for value in self.images] + [self._digest(generated)],
        )
        self.assertNotIn("是指定商品参考图", prompt)
        self.assertIn("未随附商品参考图", prompt)


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
