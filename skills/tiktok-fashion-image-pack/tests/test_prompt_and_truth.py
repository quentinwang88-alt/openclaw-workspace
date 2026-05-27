#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = SKILL_DIR / "core"
sys.path.insert(0, str(CORE_DIR))

from product_truth import heuristic_product_truth, normalize_product_truth, repair_multicolor_truth_from_sources  # noqa: E402
from prompt_builder import build_label, build_main_image_prompt, select_main_image_layout  # noqa: E402
from qa import normalize_qa_result  # noqa: E402
from scene_prompt_builder import build_scene_image_prompts, choose_detail_action, parse_scene_slots  # noqa: E402
from feedback_processor import (  # noqa: E402
    FIX_METHOD_STRUCTURE,
    build_feedback_fix_prompt,
    expand_feedback_issues,
    resolve_feedback_fix_method,
)
from feedback_qa import classify_feedback_qa_issue, qa_feedback_fix  # noqa: E402
from circuit_breaker import CircuitBreakerOpen, ModelCircuitBreaker, classify_failure  # noqa: E402
from feishu import unique_download_path  # noqa: E402


class ProductTruthTests(unittest.TestCase):
    def test_normalize_type_name_and_defaults(self) -> None:
        truth = normalize_product_truth({
            "subtype": "suede_jacket",
            "product_type_name_en": "",
            "confidence": 1.5,
        })
        self.assertEqual(truth["product_type_name_en"], "SUEDE JACKET")
        self.assertEqual(truth["confidence"], 1.0)
        self.assertIn("leather shine", truth["must_not_add"])

    def test_heuristic_infers_leather(self) -> None:
        truth = heuristic_product_truth(["black_pu_leather_jacket.jpg"])
        self.assertEqual(truth["subtype"], "leather_jacket")

    def test_local_color_fallback_repairs_multicolor(self) -> None:
        import tempfile
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp:
            black = Path(tmp) / "black.png"
            ivory = Path(tmp) / "ivory.png"
            Image.new("RGB", (120, 120), (20, 20, 20)).save(black)
            Image.new("RGB", (120, 120), (235, 232, 220)).save(ivory)
            truth = normalize_product_truth({
                "subtype": "cardigan",
                "main_color": "ivory white",
                "is_probably_multicolor": False,
                "sellable_colors_observed": ["ivory white"],
            })
            repaired = repair_multicolor_truth_from_sources(truth, [str(black), str(ivory)])
            self.assertTrue(repaired["is_probably_multicolor"])
            self.assertEqual(repaired["sellable_colors_observed"], ["black", "ivory white"])
            self.assertEqual(repaired["main_color"], "black")


class PromptBuilderTests(unittest.TestCase):
    def test_default_label(self) -> None:
        self.assertEqual(
            build_label(brand_name="likeU", product_type="SUEDE JACKET", strategy="likeU + 产品类型"),
            "likeU · SUEDE JACKET",
        )

    def test_prompt_contains_no_accessory_policy(self) -> None:
        truth = heuristic_product_truth(["khaki_utility_jacket.jpg"])
        prompt = build_main_image_prompt(product_truth=truth, brand_name="likeU", country="TH")
        self.assertIn("product-only", prompt)
        self.assertIn("No handbags", prompt)
        self.assertIn("likeU", prompt)

    def test_multicolor_uses_triptych_layout(self) -> None:
        truth = normalize_product_truth({
            "subtype": "faux_fur_jacket",
            "product_type_name_en": "FAUX FUR JACKET",
            "main_color": "taupe brown",
            "is_probably_multicolor": True,
            "sellable_colors_observed": ["taupe brown", "black", "cream"],
            "material": "faux fur",
            "closure": "open front, no visible buttons",
            "pockets": "no visible pockets",
        })
        layout = select_main_image_layout(truth)
        prompt = build_main_image_prompt(product_truth=truth, brand_name="likeU", country="TH")
        self.assertEqual(layout["template"], "womens_tops_multicolor_triptych")
        self.assertIn("three-zone split layout", prompt)
        self.assertIn("IMAGE 1 is the promoted default color/style", prompt)
        self.assertIn("3 colors", prompt)

    def test_single_material_with_no_buttons_uses_material_layout(self) -> None:
        truth = normalize_product_truth({
            "subtype": "faux_fur_jacket",
            "product_type_name_en": "FAUX FUR JACKET",
            "source_image_type": "on_body_model",
            "has_on_body_model": True,
            "main_color": "taupe brown",
            "is_probably_multicolor": False,
            "sellable_colors_observed": ["taupe brown"],
            "material": "faux fur",
            "closure": "open front, no visible buttons",
            "pockets": "no visible pockets",
        })
        layout = select_main_image_layout(truth)
        self.assertEqual(layout["template"], "womens_tops_material_mood_split")

    def test_product_only_reference_uses_faceless_tryon_layout(self) -> None:
        truth = normalize_product_truth({
            "subtype": "puffer_jacket",
            "product_type_name_en": "PUFFER JACKET",
            "source_image_type": "hanger",
            "has_on_body_model": False,
            "main_color": "dark brown",
            "material": "puffer",
            "closure": "front snap buttons",
            "pockets": "two slanted side pockets",
        })
        layout = select_main_image_layout(truth)
        prompt = build_main_image_prompt(product_truth=truth, brand_name="likeU", country="TH")
        self.assertEqual(layout["template"], "womens_tops_product_only_to_tryon_truth_split")
        self.assertIn("faceless cropped try-on image", prompt)
        self.assertIn("do not show a full perfect face", prompt)
        self.assertIn("refined product-only proof", prompt)


class QATests(unittest.TestCase):
    def test_normalize_invalid_result_to_review(self) -> None:
        result = normalize_qa_result({"result": "ok", "score": 2, "issues": "bad pocket"})
        self.assertEqual(result["result"], "需人工复核")
        self.assertEqual(result["score"], 1.0)
        self.assertEqual(result["issues"], ["bad pocket"])


class FeedbackFixTests(unittest.TestCase):
    def test_feedback_prompt_includes_scene_reference_role(self) -> None:
        prompt = build_feedback_fix_prompt(
            issues="衣服左右两边有口袋，具体位置参考原始场景参考图",
            fix_method="局部修正",
            target="S1",
            product_truth={"pockets": "two side pockets"},
            product_ref_count=1,
            scene_ref_count=2,
        )
        self.assertIn("图片 2（原始商品图）", prompt)
        self.assertIn("图片 3-4（原始场景参考图）", prompt)
        self.assertIn("扣子数量、扣子位置、口袋位置、左右结构", prompt)
        self.assertIn("该结构清晰可见", prompt)
        self.assertIn("不要镜像或反向理解", prompt)

    def test_feedback_expander_strengthens_button_count(self) -> None:
        expanded = expand_feedback_issues("衣服正面扣子为4颗")
        self.assertIn("系统扩写验收标准", expanded)
        self.assertIn("清晰可数", expanded)
        self.assertIn("不能多一颗或少一颗", expanded)
        self.assertEqual(resolve_feedback_fix_method("局部修正", expanded), FIX_METHOD_STRUCTURE)

    def test_structure_method_adds_proof_composition(self) -> None:
        prompt = build_feedback_fix_prompt(
            issues=expand_feedback_issues("左右两边有口袋"),
            fix_method=FIX_METHOD_STRUCTURE,
            target="S2",
            product_truth={"pockets": "two lower pockets"},
            product_ref_count=1,
            scene_ref_count=1,
        )
        self.assertIn("结构优先重生", prompt)
        self.assertIn("衣服主体占画面约 70%", prompt)
        self.assertIn("不要遮挡被反馈点名的结构", prompt)

    def test_feedback_qa_receives_previous_and_references(self) -> None:
        class FakeVisionClient:
            def __init__(self) -> None:
                self.image_paths = []

            def call_json(self, prompt, image_paths, max_output_tokens=3500):
                self.image_paths = image_paths
                self.prompt = prompt
                return {"result": "通过", "score": 1, "items": [], "summary": "ok"}

        fake = FakeVisionClient()
        result = qa_feedback_fix(
            fix_image_path="fixed.png",
            previous_image_path="previous.png",
            product_reference_paths=["product.png"],
            scene_reference_paths=["scene.png"],
            issues="衣服正面有五颗扣子",
            vision_client=fake,
        )
        self.assertEqual(result["result"], "通过")
        self.assertEqual(fake.image_paths, ["fixed.png", "previous.png", "product.png", "scene.png"])
        self.assertIn("图片 3 起", fake.prompt)

    def test_feedback_qa_issue_classification(self) -> None:
        self.assertEqual(classify_feedback_qa_issue("衣服正面扣子为4颗", "仍可见5颗扣子"), "数量不精确")
        self.assertEqual(classify_feedback_qa_issue("左右口袋", "右侧被袖子遮挡"), "结构被遮挡")


class CircuitBreakerTests(unittest.TestCase):
    def test_auth_error_stops_immediately(self) -> None:
        breaker = ModelCircuitBreaker(enabled=True)
        with self.assertRaises(CircuitBreakerOpen):
            breaker.record_model_failure(RuntimeError("Codex API key not found"))

    def test_model_failure_threshold_opens(self) -> None:
        breaker = ModelCircuitBreaker(model_failure_threshold=2, rate_limit_cooldown_seconds=0)
        breaker.record_model_failure(RuntimeError("read operation timed out"))
        with self.assertRaises(CircuitBreakerOpen):
            breaker.record_model_failure(RuntimeError("peer closed connection"))

    def test_rate_limit_is_classified(self) -> None:
        kind = classify_failure(RuntimeError("429 too many requests"))
        self.assertEqual(kind.category, "限流/服务繁忙")
        self.assertTrue(kind.should_cooldown)


class FeishuDownloadTests(unittest.TestCase):
    def test_unique_download_path_avoids_overwrite(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp) / "image.png"
            first.write_bytes(b"first")
            self.assertEqual(unique_download_path(first).name, "image_2.png")


class ScenePromptBuilderTests(unittest.TestCase):
    def test_default_scene_prompts_create_four_slots(self) -> None:
        truth = normalize_product_truth({
            "subtype": "suede_jacket",
            "product_type_name_en": "SUEDE JACKET",
            "source_image_type": "on_body_model",
            "has_on_body_model": True,
            "main_color": "khaki",
            "material": "suede",
            "closure": "front snap buttons",
            "pockets": "two flap chest pockets",
        })
        prompts = build_scene_image_prompts(product_truth=truth, brand_name="likeU", country="TH")
        self.assertEqual([item["image_id"] for item in prompts], ["S1", "S2", "S3", "S4"])
        self.assertIn("IMAGE 1 is the default promoted color/style", prompts[0]["prompt"])
        self.assertIn("Thai/Korean everyday fashion ecommerce", prompts[1]["prompt"])

    def test_product_only_scene_prefers_weak_face_tryon(self) -> None:
        truth = normalize_product_truth({
            "subtype": "puffer_jacket",
            "product_type_name_en": "PUFFER JACKET",
            "source_image_type": "white_bg",
            "has_on_body_model": False,
            "main_color": "dark brown",
            "material": "puffer",
            "closure": "front snap buttons",
        })
        prompts = build_scene_image_prompts(product_truth=truth, brand_name="likeU", country="TH", scene_slots=["S1"])
        self.assertIn("faceless or weak-face try-on", prompts[0]["prompt"])
        self.assertIn("Avoid a full perfect AI face", prompts[0]["prompt"])

    def test_s4_does_not_choose_zipper_when_no_zipper(self) -> None:
        truth = normalize_product_truth({
            "subtype": "faux_fur_jacket",
            "product_type_name_en": "FAUX FUR JACKET",
            "source_image_type": "on_body_model",
            "has_on_body_model": True,
            "main_color": "taupe",
            "material": "faux fur",
            "closure": "open front, no visible zipper, no visible buttons",
            "pockets": "no visible pockets",
        })
        action = choose_detail_action(truth)
        self.assertNotIn("zipper", action)
        self.assertIn("material", action)

    def test_parse_scene_slots_filters_invalid_values(self) -> None:
        self.assertEqual(parse_scene_slots("S2, S4, bad, S2"), ["S2", "S4"])
        self.assertEqual(
            parse_scene_slots([{"text": "S1 主点击试穿"}, {"text": "S4 材质结构细节"}]),
            ["S1", "S4"],
        )

    def test_multicolor_default_scene_prompts_expand_to_six(self) -> None:
        truth = normalize_product_truth({
            "subtype": "leather_jacket",
            "product_type_name_en": "LEATHER JACKET",
            "source_image_type": "mixed",
            "has_on_body_model": True,
            "main_color": "black",
            "is_probably_multicolor": True,
            "sellable_colors_observed": ["black", "pink", "cream", "brown"],
            "material": "PU leather",
            "closure": "front snap buttons",
            "pockets": "two chest pockets",
        })
        prompts = build_scene_image_prompts(product_truth=truth, brand_name="likeU", country="TH")
        self.assertEqual([item["image_id"] for item in prompts], ["S1", "S2", "S3", "S4", "S5", "S6"])
        self.assertEqual(prompts[4]["target_color"], "pink")
        self.assertEqual(prompts[5]["target_color"], "cream")
        self.assertIn("alternate color on-body try-on", prompts[4]["prompt"])

    def test_scene_prompt_discourages_perfect_front_face(self) -> None:
        truth = normalize_product_truth({
            "subtype": "faux_fur_jacket",
            "product_type_name_en": "FAUX FUR JACKET",
            "source_image_type": "on_body_model",
            "has_on_body_model": True,
            "main_color": "taupe brown",
            "material": "faux fur",
        })
        prompts = build_scene_image_prompts(product_truth=truth, brand_name="likeU", country="TH", scene_slots=["S1", "S2"])
        joined = "\n".join(item["prompt"] for item in prompts)
        self.assertIn("avoid a full front-facing beauty smile", joined)
        self.assertIn("weak-face realism", joined)
        self.assertIn("direct camera-facing beauty smile", joined)


if __name__ == "__main__":
    unittest.main()
