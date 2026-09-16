from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from services.image_generator import GenerationOutcome
from services.photo_reference import (
    REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_PRODUCT, REFERENCE_MODE_STYLE,
    resolve_reference_mode,
)
from services.photo_style_reference_supply import PhotoStyleReferenceSupplyService
from services.photo_theme import build_theme_copy, resolve_photo_theme


class FakeGenerator:
    def __init__(self, fail_calls=None):
        self.requests = []
        self.fail_calls = set(fail_calls or [])
        self.total_calls = 0

    def generate_shot(self, request):
        self.total_calls += 1
        self.requests.append(request)
        if self.total_calls in self.fail_calls:
            return GenerationOutcome(ok=False, error="模拟生图中断")
        path = (Path(request.output_dir)
                / f"look-{request.slot_index}_v{request.shot_version}.png")
        flat_lay = request.recipe_execution.get("presentation_profile", {}).get("presentation_type") == "FLAT_LAY"
        colour = ((150, 105, 70) if flat_lay else
                  (40 * request.slot_index + 10 * request.shot_version, 80, 100))
        Image.new("RGB", (120, 180), colour).save(path)
        return GenerationOutcome(ok=True, image_path=str(path), request_id=str(request.slot_index))


class FakeVisionReviewer:
    def __init__(self, passed):
        self.passed = list(passed)
        self.calls = []

    def review_alignment(self, **kwargs):
        self.calls.append(kwargs)
        ok = self.passed.pop(0)
        if isinstance(ok, dict):
            return dict(ok)
        return {"passed": ok, "notes": "需要保留咖啡店场景" if not ok else "通过"}


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


def pack_persona(folder, persona_id="P"):
    """Build a role-complete persona pack fixture (face + full-body evidence)."""
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


def group_failure_with_look_b_blamed():
    return {
        "passed": False, "notes": "look_b 场景退化成纯色棚拍",
        "reason_codes": ["SCENE_DRIFT"],
        "role_findings": [
            {"role": "look_a", "passed": True, "issues": []},
            {"role": "look_b", "passed": False, "issues": ["场景退化成纯色背景"]},
            {"role": "look_c", "passed": True, "issues": []},
            {"role": "look_d", "passed": True, "issues": []},
        ],
    }


def core_product_failure_look_b():
    """2026-09-14 真实复现：issues 全部是中文，且都不在 FAILURE_HINTS 白名单里。

    供给侧按旧实现会把它们当"细节提示"⇒ failed_roles 为空 ⇒ 不重生任何图、
    只把组级重做次数烧完（QA 空转）。修好后由 core_product_mismatch 这个结构化
    硬信号直接定为 look_b 失败。
    """
    return {
        "passed": False, "notes": "look_b 的围巾不是指定商品",
        "reason_codes": ["PRODUCT_MISMATCH"],
        "role_findings": [
            {"role": "look_a", "passed": True, "issues": []},
            {"role": "look_b", "passed": False,
             "issues": ["围巾颜色家族错误", "围巾结构错误", "指定围巾完全看不到"],
             "core_product_mismatch": True},
            {"role": "look_c", "passed": True, "issues": []},
            {"role": "look_d", "passed": True, "issues": []},
        ],
    }


def attributed_but_nothing_repairable():
    """逐张都有归因、但没有任何一张被判失败：既定位不到单张，组级又没过。"""
    return {
        "passed": False, "notes": "整组配色略偏，说不上是哪一张",
        "role_findings": [
            {"role": role, "passed": True, "issues": []}
            for role in ("look_a", "look_b", "look_c", "look_d")
        ],
    }


class PhotoThemeReferenceTest(unittest.TestCase):
    def test_auto_reference_mode_uses_complete_only_for_exact_role_count(self):
        self.assertEqual(resolve_reference_mode(
            selected_type="自动判断", attachments=[1, 2], product_id="", required_role_count=4,
        ), REFERENCE_MODE_STYLE)
        self.assertEqual(resolve_reference_mode(
            selected_type="自动判断", attachments=[1, 2, 3, 4], product_id="", required_role_count=4,
        ), REFERENCE_MODE_COMPLETE_LOOK)
        self.assertEqual(resolve_reference_mode(
            selected_type="自动判断", attachments=[1], product_id="P1", required_role_count=4,
        ), REFERENCE_MODE_PRODUCT)
        self.assertEqual(resolve_reference_mode(
            selected_type="风格参考", attachments=[1, 2], product_id="P1",
            required_role_count=4,
        ), REFERENCE_MODE_STYLE)
        self.assertEqual(resolve_reference_mode(
            selected_type="完整穿搭", attachments=list(range(8)), product_id="",
            required_role_count=4, requested_count=2,
        ), REFERENCE_MODE_COMPLETE_LOOK)
        with self.assertRaisesRegex(ValueError, "需要.*8 张"):
            resolve_reference_mode(
                selected_type="完整穿搭", attachments=list(range(4)), product_id="",
                required_role_count=4, requested_count=2,
            )

    def test_theme_copy_keeps_overlay_and_publish_copy_in_one_profile(self):
        theme = resolve_photo_theme("秋天的穿搭")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ชุด {letter.upper()}"}}
                  for letter in "abcd"]
        copy = build_theme_copy(theme, assets)
        self.assertEqual(len(copy["slide_texts"]), 5)
        self.assertIn("อากาศเย็น", copy["title"])
        self.assertIn("A B C หรือ D", copy["caption"])

    def test_style_supply_generates_four_role_bound_looks_and_resumes(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (100, 90, 80)).save(reference)
            generator = FakeGenerator()
            service = PhotoStyleReferenceSupplyService(generator=generator, root=Path(folder))
            kwargs = {
                "record_id": "rec-style", "reference_paths": [str(reference)],
                "theme": resolve_photo_theme("秋季穿搭"),
                "account": SimpleNamespace(persona_ref_id="P"),
                "persona": {"persona_id": "P"},
            }
            first = service.prepare(**kwargs)
            second = service.prepare(**kwargs)
            self.assertEqual([item["role"] for item in first["sources"]],
                             ["look_a", "look_b", "look_c", "look_d"])
            self.assertEqual(first["sources"][1]["outerwear_signature"],
                             first["sources"][2]["outerwear_signature"])
            self.assertEqual(len(generator.requests), 4)
            self.assertEqual(second["generated_this_run"], 0)

    def test_fixed_background_executes_at_request_level(self):
        """固定背景必须在请求层执行（2026-09-15 七样审查）：背景模式、场景核心、
        摄影基准与环境参考分流，而不是只停留在规划提示词。"""
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (100, 90, 80)).save(reference)
            generator = FakeGenerator()
            service = PhotoStyleReferenceSupplyService(generator=generator, root=Path(folder))
            variation = {
                "index": 1, "variation_id": "v1", "family_id": "f", "angle_zh": "同商品多搭配",
                "looks": [
                    {"role": f"look_{letter}", "travel_moment": "old_town_walk",
                     "scene_prompt": "老城街道漫步", "display_label": f"ลุค {letter.upper()}",
                     "outerwear": f"外套{letter}",
                     "top_inner": "内搭", "bottom": "下装", "shoes": "鞋",
                     "footwear_type": "SNEAKER"}
                    for letter in "abcd"
                ],
                "style_profile": {
                    "presentation_type": "MODEL_FULL_BODY",
                    "environment_reference": {"indices": [1]},
                    "visual_preset": {
                        "preset_id": "VP_SOLID_COLOR_V1", "name": "纯色搭配解析",
                        "version": 2, "background_mode": "fixed", "source": "task",
                        "background": {"mode": "fixed", "kind": "solid",
                                       "fixed_scene_zh": "奶白纯色背景",
                                       "stability_zh": "四页背景一致"},
                        "photography_baseline_zh": "均匀柔光、色彩还原准确",
                    },
                },
            }
            with mock.patch(
                    "services.photo_style_reference_analyzer.validate_style_alignment",
                    return_value=None):
                service.prepare(
                    record_id="rec-fixed", reference_paths=[str(reference)],
                theme=resolve_photo_theme("凉爽旅行"),
                account=SimpleNamespace(persona_ref_id=""),
                persona={}, variation=variation,
            )
            self.assertEqual(len(generator.requests), 4)
            for request in generator.requests:
                presentation = request.recipe_execution["presentation_profile"]
                self.assertEqual(presentation["background_mode"], "solid_color")
                self.assertEqual(presentation["photography_baseline_zh"], "均匀柔光、色彩还原准确")
                self.assertIn("奶白纯色背景", request.scene_snapshot["prompt_core"])
                self.assertIn("不得出现街道", request.scene_snapshot["prompt_core"])
                # ENVIRONMENT 单用途参考在固定背景不发送
                self.assertEqual(request.reference_roles["environment_reference_images"], [])
                # 旅行机位提示不进入固定背景
                self.assertNotIn("老城街道漫步", request.scene_snapshot["prompt_core"])

    def test_fixed_background_drops_environment_only_reference(self):
        """断点 A1：固定背景只过滤「仅环境用途」参考；同图承担穿搭/风格用途保留。"""
        with tempfile.TemporaryDirectory() as folder:
            ref_a = Path(folder) / "ref_outfit.png"
            ref_b = Path(folder) / "ref_env_only.png"
            Image.new("RGB", (120, 180), (110, 90, 80)).save(ref_a)
            Image.new("RGB", (120, 180), (90, 110, 120)).save(ref_b)
            generator = FakeGenerator()
            service = PhotoStyleReferenceSupplyService(generator=generator, root=Path(folder))
            variation = {
                "index": 1, "variation_id": "v1", "family_id": "f", "angle_zh": "同商品多搭配",
                "looks": [
                    {"role": f"look_{letter}", "travel_moment": "old_town_walk",
                     "scene_prompt": "固定", "display_label": f"ลุค {letter.upper()}",
                     "outerwear": "外套", "top_inner": "内搭", "bottom": "下装", "shoes": "鞋",
                     "footwear_type": "SNEAKER", "outfit_reference_indices": [1]}
                    for letter in "abcd"
                ],
                "style_profile": {
                    "presentation_type": "MODEL_FULL_BODY",
                    "per_reference": [
                        {"index": 1, "reference_uses": ["OUTFIT", "VISUAL_STYLE"]},
                        {"index": 2, "reference_uses": ["ENVIRONMENT"]},
                    ],
                    "outfit_reference": {"indices": [1]},
                    "visual_preset": {
                        "preset_id": "VP_SOLID_COLOR_V1", "name": "纯色搭配解析",
                        "version": 2, "background_mode": "fixed", "source": "task",
                        "background": {"mode": "fixed", "kind": "solid",
                                       "fixed_scene_zh": "奶白纯色背景",
                                       "stability_zh": "一致"},
                    },
                },
            }
            with mock.patch(
                    "services.photo_style_reference_analyzer.validate_style_alignment",
                    return_value=None):
                service.prepare(
                    record_id="rec-a1", reference_paths=[str(ref_a), str(ref_b)],
                    theme=resolve_photo_theme("凉爽旅行"),
                    account=SimpleNamespace(persona_ref_id=""),
                    persona={}, variation=variation,
                )
            request = generator.requests[0]
            used = [str(p) for p in request.continuity_reference_images]
            self.assertIn(str(Path(ref_a).resolve()), used)
            self.assertNotIn(str(Path(ref_b).resolve()), used)

    def test_destination_preset_keeps_travel_execution(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (100, 90, 80)).save(reference)
            generator = FakeGenerator()
            service = PhotoStyleReferenceSupplyService(generator=generator, root=Path(folder))
            variation = {
                "index": 1, "variation_id": "v1", "family_id": "f", "angle_zh": "旅行",
                "looks": [
                    {"role": f"look_{letter}", "travel_moment": "old_town_walk",
                     "scene_prompt": f"老城街道{letter}", "display_label": f"ลุค {letter.upper()}",
                     "outerwear": "外套",
                     "top_inner": "内搭", "bottom": "下装", "shoes": "鞋",
                     "footwear_type": "SNEAKER"}
                    for letter in "abcd"
                ],
                "style_profile": {
                    "presentation_type": "SCENE_MODEL",
                    "environment_reference": {"indices": [1]},
                    "visual_preset": {
                        "preset_id": "VP_TRAVEL_SCENE_V1", "name": "旅行场景穿搭",
                        "version": 2, "background_mode": "scene", "source": "task",
                        "background": {"mode": "scene", "kind": "destination"},
                    },
                },
            }
            with mock.patch(
                    "services.photo_style_reference_analyzer.validate_style_alignment",
                    return_value=None):
                service.prepare(
                    record_id="rec-dest", reference_paths=[str(reference)],
                theme=resolve_photo_theme("凉爽旅行"),
                account=SimpleNamespace(persona_ref_id=""),
                persona={}, variation=variation,
            )
            request = next(r for r in generator.requests if r.slot_index == 1)
            presentation = request.recipe_execution["presentation_profile"]
            self.assertEqual(presentation["background_mode"], "creator_environment")
            self.assertIn("老城街道a", request.scene_snapshot["prompt_core"])
            self.assertEqual(request.reference_roles["environment_reference_images"],
                             [str(Path(reference).resolve())])

    def test_flat_lay_style_supply_does_not_require_or_anchor_a_persona(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (150, 105, 70)).save(reference)
            generator = FakeGenerator()
            service = PhotoStyleReferenceSupplyService(generator=generator, root=Path(folder))
            variation = {
                "family_id": "warm_neutral", "presentation_type": "FLAT_LAY",
                "style_profile": {"presentation_type": "FLAT_LAY", "temperature": "warm"},
                "looks": [
                    {"role": f"look_{letter}", "display_label": letter.upper(),
                     "outerwear": f"棕色外套{letter}", "top_inner": "奶油针织",
                     "bottom": f"下装{letter}", "shoes": "棕色鞋"}
                    for letter in "abcd"
                ],
            }
            service.prepare(
                record_id="rec-flat", reference_paths=[str(reference)],
                theme=resolve_photo_theme("秋季穿搭"), account=SimpleNamespace(persona_ref_id=""),
                persona={}, variation=variation,
            )
            first = generator.requests[0]
            self.assertEqual(first.persona_snapshot, {})
            self.assertEqual(first.recipe_execution["presentation_profile"]["presentation_type"], "FLAT_LAY")
            self.assertEqual(first.plan_shot["composition_contract"]["framing"], "flat_lay")
            self.assertNotIn(str(Path(folder) / "style_reference_supply" / "rec-flat" / "look-1_v1.png"),
                             generator.requests[1].continuity_reference_images)

    def test_scene_model_runs_first_image_retry_and_group_review(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([False, True, True])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            variation = scene_model_variation()
            result = service.prepare(
                record_id="rec-scene", reference_paths=[str(reference)],
                theme=resolve_photo_theme("秋季穿搭"),
                account=SimpleNamespace(persona_ref_id="P"), persona=pack_persona(folder),
                variation=variation,
            )
            self.assertEqual(len(generator.requests), 5)
            look_a_retry = next(
                request for request in generator.requests
                if request.slot_index == 1 and request.shot_version == 2
            )
            self.assertIsNotNone(look_a_retry)
            self.assertEqual(generator.requests[0].recipe_execution["presentation_profile"]["background_mode"],
                             "creator_environment")
            self.assertTrue(result["group_alignment"]["passed"])
            self.assertEqual([call["scope"] for call in reviewer.calls],
                             ["FIRST_LOOK_A", "FIRST_LOOK_A_RETRY", "FULL_LOOK_GROUP"])

    def test_style_reference_can_keep_product_and_select_reference_uses(self):
        with tempfile.TemporaryDirectory() as folder:
            environment = Path(folder) / "environment.png"
            outfit = Path(folder) / "outfit.png"
            product_ref = Path(folder) / "product.png"
            for path, colour in ((environment, (80, 100, 120)),
                                 (outfit, (180, 150, 120)),
                                 (product_ref, (210, 200, 190))):
                Image.new("RGB", (120, 180), colour).save(path)
            variation = scene_model_variation()
            variation["style_profile"].update({
                "per_reference": [
                    {"index": 1, "reference_uses": ["ENVIRONMENT", "VISUAL_STYLE"]},
                    {"index": 2, "reference_uses": ["OUTFIT"]},
                ],
                "environment_reference": {"indices": [1]},
                "visual_style_reference": {"indices": [1]},
                "outfit_reference": {"indices": [2]},
            })
            for look in variation["looks"]:
                look["outfit_reference_indices"] = [2]
                look["styling_intent"] = "保留短外套与高腰线"
            generator = FakeGenerator()
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder),
                vision_service=FakeVisionReviewer([True, True]),
            )
            product = {
                "product_id": "SKU-1", "category": "outerwear",
                "reference_images": [str(product_ref)],
            }
            service.prepare(
                record_id="rec-product-style",
                reference_paths=[str(environment), str(outfit)],
                theme=resolve_photo_theme("秋季穿搭"),
                account=SimpleNamespace(persona_ref_id="P"),
                persona=pack_persona(folder), variation=variation, product=product,
            )
            first = generator.requests[0]
            self.assertEqual(first.product["product_id"], "SKU-1")
            self.assertEqual(first.outfit_state["outerwear"],
                             "指定商品，以商品参考图的颜色、版型和结构为准")
            self.assertEqual(first.reference_roles["environment_reference_images"],
                             [str(environment.resolve())])
            self.assertEqual(first.reference_roles["outfit_reference_images"],
                             [str(outfit.resolve())])
            self.assertIn(str(product_ref), first.reference_roles["product_identity_images"])

    def test_group_failure_redoes_only_attributed_roles_and_keeps_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([
                True, group_failure_with_look_b_blamed(), True,
            ])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            kwargs = {
                "record_id": "rec-repair", "reference_paths": [str(reference)],
                "theme": resolve_photo_theme("秋季穿搭"),
                "account": SimpleNamespace(persona_ref_id="P"), "persona": pack_persona(folder),
                "variation": scene_model_variation(),
            }
            result = service.prepare(**kwargs)
            self.assertTrue(result["group_alignment"]["passed"])
            self.assertEqual(result["group_repair_attempts"], 1)
            self.assertEqual(result["repair_rounds"], 1)
            self.assertEqual(result["repaired_roles_this_run"], ["look_b"])
            self.assertEqual([call["scope"] for call in reviewer.calls],
                             ["FIRST_LOOK_A", "FULL_LOOK_GROUP", "FULL_LOOK_GROUP"])
            self.assertTrue(all("generated_roles" in call for call in reviewer.calls[1:]))
            self.assertEqual(len(generator.requests), 5)
            self.assertEqual(generator.requests[4].slot_index, 2)
            self.assertEqual(generator.requests[4].shot_version, 2)
            self.assertEqual([item["role"] for item in result["sources"]],
                             ["look_a", "look_b", "look_c", "look_d"])
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-repair"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(manifest["group_repair_attempts"], 1)
            round_entry = manifest["attempt_history"][0]
            self.assertEqual(round_entry["failed_roles"], ["look_b"])
            self.assertFalse(round_entry["alignment"]["passed"])
            retired = round_entry["retired"][0]
            self.assertEqual(retired["role"], "look_b")
            old_path = Path(retired["path"])
            new_path = Path(result["sources"][1]["path"])
            self.assertTrue(old_path.is_file(), "被作废的旧素材必须保留在磁盘上作为证据")
            self.assertNotEqual(retired["sha256"], result["sources"][1]["sha256"])
            self.assertNotEqual(old_path, new_path)

    def test_group_failure_without_role_findings_redoes_whole_group(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([
                True, {"passed": False, "notes": "整组风格不统一"}, True, True,
            ])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            result = service.prepare(
                record_id="rec-group", reference_paths=[str(reference)],
                theme=resolve_photo_theme("秋季穿搭"),
                account=SimpleNamespace(persona_ref_id="P"), persona=pack_persona(folder),
                variation=scene_model_variation(),
            )
            self.assertEqual(result["group_repair_attempts"], 1)
            self.assertEqual(result["repaired_roles_this_run"],
                             ["look_a", "look_b", "look_c", "look_d"])
            self.assertEqual(len(generator.requests), 8)
            self.assertEqual([request.shot_version for request in generator.requests[4:]], [2, 2, 2, 2])
            self.assertTrue(result["group_alignment"]["passed"])

    def test_group_failure_stops_at_max_attempts_and_resume_fails_fast(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([
                True, group_failure_with_look_b_blamed(),
                group_failure_with_look_b_blamed(),
                group_failure_with_look_b_blamed(),
            ])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            kwargs = {
                "record_id": "rec-exhaust", "reference_paths": [str(reference)],
                "theme": resolve_photo_theme("秋季穿搭"),
                "account": SimpleNamespace(persona_ref_id="P"), "persona": pack_persona(folder),
                "variation": scene_model_variation(),
            }
            with self.assertRaisesRegex(ValueError, "重做次数已用尽"):
                service.prepare(**kwargs)
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-exhaust"
                             / "supply_manifest.json")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "group_failed")
            # QA 瘦身后 MAX_GROUP_REPAIR_ATTEMPTS=1：一轮修复即用尽。
            self.assertEqual(manifest["group_repair_attempts"], 1)
            self.assertEqual(len(manifest["attempt_history"]), 1)
            retired_paths = [Path(item["path"])
                             for entry in manifest["attempt_history"]
                             for item in entry["retired"]]
            self.assertEqual(len(retired_paths), 1)
            self.assertTrue(all(path.is_file() for path in retired_paths))
            self.assertEqual(len(reviewer.calls), 3)
            with self.assertRaisesRegex(ValueError, "已用尽"):
                service.prepare(**kwargs)
            self.assertEqual(len(reviewer.calls), 3, "续跑必须直接失败，不允许再次调用视觉模型")

    # --- 只改发布文案（语言修复）⇒ 复用已付费图片，生成器零调用 (2026-09-14) --

    def _frozen_supply(self, folder, variation, reviewer=None):
        """跑一次完整供给，返回 (service, generator, kwargs, 首次结果)。"""
        reference = Path(folder) / "reference.png"
        Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
        # 真实链路里参考分析合同一定在（149 个目录中 132 个有 ``reference_analysis.json``），
        # 所以这里也建上：否则 ``assert_identity_unchanged`` 的「参考图哈希」一项会被
        # 静默跳过，测出来的复用比生产松。
        contract = Path(folder) / "reference_contracts" / "rec-copy-only"
        contract.mkdir(parents=True, exist_ok=True)
        (contract / "reference_analysis.json").write_text(json.dumps({
            "reference_hashes": [hashlib.sha256(reference.read_bytes()).hexdigest()],
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        generator = FakeGenerator()
        service = PhotoStyleReferenceSupplyService(
            generator=generator, root=Path(folder),
            vision_service=reviewer or FakeVisionReviewer([True, True]),
        )
        kwargs = {
            "record_id": "rec-copy-only", "reference_paths": [str(reference)],
            "theme": resolve_photo_theme("秋季穿搭"),
            "account": SimpleNamespace(persona_ref_id="P"),
            "persona": pack_persona(folder), "variation": variation,
        }
        return service, generator, kwargs, service.prepare(**kwargs)

    @staticmethod
    def _variation_with(copy_block):
        variation = scene_model_variation()
        variation["copy"] = dict(copy_block)
        return variation

    def test_copy_only_change_reuses_paid_looks_without_generating(self):
        """验收 2：旧泰文文案换成越南语 ⇒ 保留原四张付费图，生成器零调用。"""
        with tempfile.TemporaryDirectory() as folder:
            thai = self._variation_with({
                "title": "ลุคเดิม", "cover": "ลุคเดิม", "caption": "ลุคเดิม",
                "hashtags": [], "slide_texts": ["ลุค A"],
            })
            service, generator, kwargs, first = self._frozen_supply(folder, thai)
            requests_after_first = len(generator.requests)
            self.assertEqual(len(first["sources"]), 4)
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-copy-only"
                             / "supply_manifest.json")
            stored = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(stored["status"], "complete")
            old_hash = stored["input_hash"]

            vietnamese = copy.deepcopy(thai)
            vietnamese["copy"] = {
                "title": "Gợi ý phối đồ du lịch", "cover": "Look du lịch",
                "caption": "Bạn thích look nào?", "hashtags": ["#OOTD"],
                "slide_texts": ["Look A"],
            }
            # 清单从不保存 variation，所以「只改了文案」只能由握着新旧两版计划
            # 的调用方给出对照物：这里就是被就地重建前的那一条旧计划条目。
            second = service.prepare(
                **{**kwargs, "variation": vietnamese, "copy_repaired_from": thai})

            self.assertEqual(second["generated_this_run"], 0, "只改文案不得重新生图")
            self.assertEqual(len(generator.requests), requests_after_first,
                             "图片生成器一次都不该再被调用")
            self.assertEqual([item["path"] for item in second["sources"]],
                             [item["path"] for item in first["sources"]])
            self.assertEqual([item["sha256"] for item in second["sources"]],
                             [item["sha256"] for item in first["sources"]])
            rebaselined = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertNotEqual(rebaselined["input_hash"], old_hash, "基线必须重写")
            self.assertEqual(rebaselined["status"], "complete")
            evidence = rebaselined["copy_rebaseline"]
            self.assertEqual(evidence["reason"], "copy_only_language_fix")
            self.assertNotEqual(evidence["former_copy_sha256"], evidence["new_copy_sha256"])

    def test_look_label_change_alone_also_reuses_the_paid_looks(self):
        """look 的上片标签归 Locale Pack：只换标签同样零生成、复用旧图。"""
        with tempfile.TemporaryDirectory() as folder:
            thai = self._variation_with({
                "title": "T", "cover": "C", "caption": "P",
            })
            for letter, look in zip("ABCD", thai["looks"]):
                look["display_label"] = f"ลุค {letter}"
            service, generator, kwargs, first = self._frozen_supply(folder, thai)
            requests_after_first = len(generator.requests)

            vietnamese = copy.deepcopy(thai)
            for letter, look in zip("ABCD", vietnamese["looks"]):
                look["display_label"] = f"Look {letter}"
            second = service.prepare(
                **{**kwargs, "variation": vietnamese, "copy_repaired_from": thai})
            self.assertEqual(second["generated_this_run"], 0)
            self.assertEqual(len(generator.requests), requests_after_first)

            # 但 look 的穿搭字段仍受保护：标签之外再动画面就必须拦下。
            restyled = copy.deepcopy(vietnamese)
            restyled["looks"][0]["bottom"] = "换一条完全不同的下装"
            with self.assertRaisesRegex(ValueError, "参考图或主题已变化"):
                service.prepare(
                    **{**kwargs, "variation": restyled, "copy_repaired_from": thai})
            self.assertEqual(len(generator.requests), requests_after_first)

    def test_a_copy_change_without_the_old_plan_item_is_refused(self):
        """没有旧计划条目作对照时，拒绝复用（保守默认，不猜「只是文案」）。"""
        with tempfile.TemporaryDirectory() as folder:
            thai = self._variation_with({
                "title": "ลุคเดิม", "cover": "ลุคเดิม", "caption": "ลุคเดิม",
                "hashtags": [], "slide_texts": ["ลุค A"],
            })
            service, generator, kwargs, first = self._frozen_supply(folder, thai)
            requests_after_first = len(generator.requests)
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-copy-only"
                             / "supply_manifest.json")
            before = json.loads(manifest_path.read_text(encoding="utf-8"))["input_hash"]

            vietnamese = copy.deepcopy(thai)
            vietnamese["copy"]["caption"] = "Bạn thích look nào?"
            with self.assertRaisesRegex(ValueError, "参考图或主题已变化"):
                service.prepare(**{**kwargs, "variation": vietnamese})
            self.assertEqual(len(generator.requests), requests_after_first)
            self.assertEqual(
                json.loads(manifest_path.read_text(encoding="utf-8"))["input_hash"], before,
                "被拒的路径不得动基线")

    def test_an_identical_copy_change_is_not_a_rebaseline_reason(self):
        """文案没变时不得重定基线：差异另有出处，交给原路径拦截。"""
        with tempfile.TemporaryDirectory() as folder:
            thai = self._variation_with({
                "title": "ลุคเดิม", "cover": "ลุคเดิม", "caption": "ลุคเดิม",
                "hashtags": [], "slide_texts": ["ลุค A"],
            })
            service, generator, kwargs, first = self._frozen_supply(folder, thai)
            requests_after_first = len(generator.requests)
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-copy-only"
                             / "supply_manifest.json")
            before = json.loads(manifest_path.read_text(encoding="utf-8"))["input_hash"]

            # 文案逐字未动，但画面字典里多了一个无关键 ⇒ hash 仍会变。
            shifted = copy.deepcopy(thai)
            shifted["unexpected_note"] = "同一份文案，别的字段变了"
            with self.assertRaisesRegex(ValueError, "参考图或主题已变化"):
                service.prepare(
                    **{**kwargs, "variation": shifted, "copy_repaired_from": thai})
            self.assertEqual(len(generator.requests), requests_after_first)
            self.assertEqual(
                json.loads(manifest_path.read_text(encoding="utf-8"))["input_hash"], before)

    def test_copy_only_rebaseline_cannot_smuggle_in_a_real_visual_change(self):
        """验收 3：真改参考图/画面输入不被这条兼容吞掉，仍要求新建任务。"""
        with tempfile.TemporaryDirectory() as folder:
            thai = self._variation_with({
                "title": "ลุคเดิม", "cover": "ลุคเดิม", "caption": "ลุคเดิม",
                "hashtags": [], "slide_texts": ["ลุค A"],
            })
            service, generator, kwargs, first = self._frozen_supply(folder, thai)
            requests_after_first = len(generator.requests)

            vietnamese = copy.deepcopy(thai)
            vietnamese["copy"]["title"] = "Gợi ý phối đồ du lịch"
            # 同一时刻把参考图换成另外一张：文案变了、画面输入也变了。
            # 即使调用方给出了旧计划条目（= 声称「只改了文案」），也必须被拦下。
            Image.new("RGB", (120, 180), (10, 20, 30)).save(kwargs["reference_paths"][0])
            with self.assertRaisesRegex(ValueError, "参考图或主题已变化"):
                service.prepare(
                    **{**kwargs, "variation": vietnamese, "copy_repaired_from": thai})
            self.assertEqual(len(generator.requests), requests_after_first,
                             "被拒的路径同样不得生图")

            # 真改穿搭规格（画面）：同样拦。
            Image.new("RGB", (120, 180), (130, 95, 75)).save(kwargs["reference_paths"][0])
            restyled = copy.deepcopy(vietnamese)
            restyled["looks"][1]["outerwear"] = "换一件完全不同的外套"
            with self.assertRaisesRegex(ValueError, "参考图或主题已变化"):
                service.prepare(
                    **{**kwargs, "variation": restyled, "copy_repaired_from": thai})
            self.assertEqual(len(generator.requests), requests_after_first)

    def test_an_untouched_variation_still_reuses_without_rebaselining(self):
        """没改任何输入时走原路径：不重写基线，也不生图。"""
        with tempfile.TemporaryDirectory() as folder:
            variation = self._variation_with({"title": "T", "cover": "C", "caption": "P"})
            service, generator, kwargs, first = self._frozen_supply(folder, variation)
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-copy-only"
                             / "supply_manifest.json")
            before = json.loads(manifest_path.read_text(encoding="utf-8"))["input_hash"]
            again = service.prepare(**kwargs)
            self.assertEqual(again["generated_this_run"], 0)
            self.assertEqual(
                json.loads(manifest_path.read_text(encoding="utf-8"))["input_hash"], before)

    # --- 指定商品的核心商品失败：定向重拍 + 不许 QA 空转 (2026-09-14) --------

    def _core_product_kwargs(self, folder, reference, product_ref):
        return {
            "record_id": "rec-core-product", "reference_paths": [str(reference)],
            "theme": resolve_photo_theme("秋季穿搭"),
            "account": SimpleNamespace(persona_ref_id="P"),
            "persona": pack_persona(folder),
            "variation": scene_model_variation(),
            "product": {"product_id": "SCARF-1", "category": "scarf",
                        "reference_images": [str(product_ref)]},
        }

    def test_core_product_failure_repairs_only_the_blamed_look(self):
        """验收 1／4：判定来自结构化信号，只重拍 look_b，且不产生空转轮。"""
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            product_ref = Path(folder) / "product.png"
            for path, colour in ((reference, (130, 95, 75)), (product_ref, (205, 190, 175))):
                Image.new("RGB", (120, 180), colour).save(path)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([True, core_product_failure_look_b(), True])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            result = service.prepare(**self._core_product_kwargs(folder, reference, product_ref))

            self.assertTrue(result["group_alignment"]["passed"])
            self.assertEqual(result["group_repair_attempts"], 1)
            self.assertEqual(result["repaired_roles_this_run"], ["look_b"])
            # 整个流程只调用三次视觉模型：首图门禁 + 一次组级检查 + 修复后的复检。
            # 旧实现在这里会因为 failed_roles 为空而多烧一轮"再查一次"。
            self.assertEqual([call["scope"] for call in reviewer.calls],
                             ["FIRST_LOOK_A", "FULL_LOOK_GROUP", "FULL_LOOK_GROUP"])
            # 只重生 look_b：生成器只被多调用一次，且落在 slot_index 2 / v2。
            self.assertEqual(len(generator.requests), 5)
            self.assertEqual(
                [(item.slot_index, item.shot_version) for item in generator.requests[4:]],
                [(2, 2)],
            )
            # A/C/D 仍指向第一轮的 v1 文件（从未被重写），只有 B 换成了 v2。
            self.assertEqual(
                [Path(item["path"]).name for item in result["sources"]],
                ["look-1_v1.png", "look-2_v2.png", "look-3_v1.png", "look-4_v1.png"],
            )
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-core-product"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(manifest["attempt_history"][0]["failed_roles"], ["look_b"])
            self.assertEqual(
                [item["role"] for item in manifest["attempt_history"][0]["retired"]],
                ["look_b"],
            )

    def test_failed_product_repair_stops_at_the_existing_attempt_cap(self):
        """验收 2：商品修复仍失败时按现有次数上限结束，不增加新 QA 轮次。"""
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            product_ref = Path(folder) / "product.png"
            for path, colour in ((reference, (130, 95, 75)), (product_ref, (205, 190, 175))):
                Image.new("RGB", (120, 180), colour).save(path)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([
                True, core_product_failure_look_b(), core_product_failure_look_b(),
            ])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            with self.assertRaisesRegex(ValueError, "重做次数已用尽"):
                service.prepare(**self._core_product_kwargs(folder, reference, product_ref))

            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-core-product"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "group_failed")
            # MAX_GROUP_REPAIR_ATTEMPTS=1：一次定向重拍即用尽；全程三次模型调用。
            self.assertEqual(manifest["group_repair_attempts"], 1)
            self.assertEqual(len(reviewer.calls), 3)
            self.assertEqual(len(generator.requests), 5)

    def test_failure_without_a_repairable_role_does_not_count_a_phantom_round(self):
        """不允许把"对原图再查一次"记成一轮修复：直接给出可读的无法归因错误。"""
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([True, attributed_but_nothing_repairable()])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            with self.assertRaisesRegex(ValueError, "无法归因到任何单张"):
                service.prepare(
                    record_id="rec-no-role", reference_paths=[str(reference)],
                    theme=resolve_photo_theme("秋季穿搭"),
                    account=SimpleNamespace(persona_ref_id="P"),
                    persona=pack_persona(folder), variation=scene_model_variation(),
                )
            # 只调用两次：首图门禁 + 一次组级检查。没有为了"再查一次"多跑一轮。
            self.assertEqual(len(reviewer.calls), 2)
            # 也没有重生任何图（旧实现会把重做次数烧掉而图一张不变）。
            self.assertEqual(len(generator.requests), 4)
            manifest = json.loads(
                (Path(folder) / "style_reference_supply" / "rec-no-role"
                 / "supply_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "group_failed")
            self.assertEqual(manifest["group_repair_attempts"], 0)
            self.assertEqual(manifest["attempt_history"], [])

    def test_without_a_product_the_same_chinese_issues_stay_lenient(self):
        """验收 3：未指定商品时这三条中文描述不触发重拍（自由搭配保持宽松）。"""
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator()
            # 旗标缺失（自由搭配线上不会返回它）⇒ 回落原关键词路径 ⇒ 判为细节提示。
            lenient = core_product_failure_look_b()
            for item in lenient["role_findings"]:
                item.pop("core_product_mismatch", None)
            reviewer = FakeVisionReviewer([True, lenient])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            with self.assertRaisesRegex(ValueError, "无法归因到任何单张"):
                service.prepare(
                    record_id="rec-lenient", reference_paths=[str(reference)],
                    theme=resolve_photo_theme("秋季穿搭"),
                    account=SimpleNamespace(persona_ref_id="P"),
                    persona=pack_persona(folder), variation=scene_model_variation(),
                )
            self.assertEqual(len(generator.requests), 4, "宽松路径不得触发任何重拍")

    def test_resume_after_repair_interruption_regenerates_only_invalid_roles(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator(fail_calls={5})
            reviewer = FakeVisionReviewer([True, group_failure_with_look_b_blamed(), True])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            kwargs = {
                "record_id": "rec-resume", "reference_paths": [str(reference)],
                "theme": resolve_photo_theme("秋季穿搭"),
                "account": SimpleNamespace(persona_ref_id="P"), "persona": pack_persona(folder),
                "variation": scene_model_variation(),
            }
            with self.assertRaisesRegex(ValueError, "look_b 风格参考生图失败"):
                service.prepare(**kwargs)
            manifest_path = (Path(folder) / "style_reference_supply" / "rec-resume"
                             / "supply_manifest.json")
            self.assertEqual(
                json.loads(manifest_path.read_text(encoding="utf-8"))["status"],
                "group_repair_pending",
            )
            result = service.prepare(**kwargs)
            self.assertEqual(result["generated_this_run"], 1)
            self.assertEqual(result["repaired_roles_this_run"], ["look_b"])
            self.assertEqual(result["group_repair_attempts"], 1)
            self.assertEqual(len(reviewer.calls), 3)
            regenerated = generator.requests[-1]
            self.assertEqual(regenerated.slot_index, 2)
            self.assertEqual(regenerated.shot_version, 2)
            # 取消生成图锚定：重生页只拿风格参考 + 角色化人物包，不再拿 look_a 成片。
            continuity = [str(path) for path in regenerated.continuity_reference_images]
            self.assertNotIn(
                str((Path(folder) / "style_reference_supply" / "rec-resume"
                     / "look-1_v1.png").resolve()),
                continuity,
            )
            from services.persona_pack import select_identity_references
            persona_items = select_identity_references(kwargs["persona"])
            self.assertEqual(
                [str(Path(value).resolve()) for value in continuity],
                [str(Path(kwargs["reference_paths"][0]).resolve())]
                + [str(Path(item["local_path"]).resolve()) for item in persona_items],
            )
            self.assertEqual(
                len(regenerated.reference_roles.get("persona_identity_images") or []), 2,
            )

    def test_completed_manifest_resume_skips_group_recheck(self):
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            generator = FakeGenerator()
            reviewer = FakeVisionReviewer([True, True])
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder), vision_service=reviewer,
            )
            kwargs = {
                "record_id": "rec-idem", "reference_paths": [str(reference)],
                "theme": resolve_photo_theme("秋季穿搭"),
                "account": SimpleNamespace(persona_ref_id="P"), "persona": pack_persona(folder),
                "variation": scene_model_variation(),
            }
            first = service.prepare(**kwargs)
            self.assertEqual(len(reviewer.calls), 2)
            second = service.prepare(**kwargs)
            self.assertEqual(len(reviewer.calls), 2, "已完成的 manifest 续跑不应重复整组视觉检查")
            self.assertEqual(second["generated_this_run"], 0)
            self.assertTrue(second["group_alignment"]["passed"])
            self.assertEqual([item["sha256"] for item in second["sources"]],
                             [item["sha256"] for item in first["sources"]])

    def test_no_theme_retake_recovers_the_frozen_theme_from_the_manifest(self):
        """无主题首跑后的重拍：主题必须从供给清单恢复，而不是重读空表格。

        2026-09-14 复现：无主题 STYLE 首跑用的是 ``effective_theme``（中性主题），
        表格里仍是空的；重拍重读表格得到 ``None``，而 ``_input_hash`` 会执行
        ``dict(theme)`` ⇒ ``TypeError: 'NoneType' object is not iterable``，
        该行的重拍被永久堵死。
        """
        from services.feishu_workflow import retake_theme

        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (100, 90, 80)).save(reference)
            generator = FakeGenerator()
            service = PhotoStyleReferenceSupplyService(
                generator=generator, root=Path(folder),
                vision_service=FakeVisionReviewer([True] * 12))
            variation = scene_model_variation()
            # 与 feishu_workflow 的无主题首跑逐字一致（theme_key 留空 = 运营未选）。
            neutral = {
                "theme_key": "", "label_zh": "自动差异化穿搭",
                "visual_brief": "保持同一商品或参考风格，变化场景、配色和穿搭组合",
            }
            account = SimpleNamespace(persona_ref_id="P")
            first = service.prepare(
                record_id="rec-no-theme_item_1", reference_paths=[str(reference)],
                theme=neutral, account=account, persona=pack_persona(folder),
                variation=variation,
            )
            manifest = json.loads(
                Path(first["supply_manifest"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["theme_brief"], neutral)

            recovered = retake_theme(None, manifest)
            self.assertEqual(recovered, neutral)
            # 恢复出来的主题必须能复现首跑的 input_hash——那才叫「同一身份」。
            self.assertEqual(
                service.input_fingerprint(
                    [str(reference)], recovered, variation, account),
                manifest["input_hash"],
            )
            # 表格空 + 直接重读（修复前的行为）会当场炸。
            with self.assertRaises(TypeError):
                service.input_fingerprint([str(reference)], None, variation, account)

            # 真重拍 look_b：只有 b 被重生，其余沿用（与运营重拍的实际顺序一致）。
            calls_before = len(generator.requests)
            item_dir = Path(first["supply_manifest"]).parent
            service.regenerate_roles(
                item_dir=item_dir, roles=["look_b"], reason="运营手动重拍")
            retaken = service.prepare(
                record_id="rec-no-theme_item_1", reference_paths=[str(reference)],
                theme=recovered, account=account, persona=pack_persona(folder),
                variation=variation,
            )
            self.assertEqual(retaken["generated_this_run"], 1)
            self.assertEqual(len(generator.requests) - calls_before, 1,
                             "重拍只能新增一次生成调用")
            for role in ("look_a", "look_c", "look_d"):
                self.assertEqual(
                    next(s["sha256"] for s in retaken["sources"] if s["role"] == role),
                    next(s["sha256"] for s in manifest["sources"] if s["role"] == role),
                    f"{role} 不应被重拍重生",
                )

            # 再跑一次（模拟重拍中断后重勾执行）：不该重复处理已完成素材。
            again = service.prepare(
                record_id="rec-no-theme_item_1", reference_paths=[str(reference)],
                theme=recovered, account=account, persona=pack_persona(folder),
                variation=variation,
            )
            self.assertEqual(again["generated_this_run"], 0)

    def test_retake_theme_still_surfaces_a_real_theme_change(self):
        """真改了主题不能靠「恢复清单」被悄悄放过。"""
        from services.feishu_workflow import retake_theme

        frozen = {"theme_key": "AUTO", "label_zh": "自动差异化穿搭"}
        manifest = {"theme_brief": frozen}
        # 表格仍为空 ⇒ 沿用冻结值。
        self.assertEqual(retake_theme(None, manifest), frozen)
        # 表格与冻结一致 ⇒ 等价，仍走冻结值。
        self.assertEqual(retake_theme(dict(frozen), manifest), frozen)
        # 真换了主题 ⇒ 原样交回表格值，让既有身份检查拒绝。
        changed = resolve_photo_theme("凉爽旅行")
        self.assertEqual(retake_theme(changed, manifest), changed)
        # 旧清单没有 theme_brief ⇒ 保持历史兼容（表格值直通），不擅自补主题。
        self.assertEqual(retake_theme(changed, {}), changed)
        self.assertIsNone(retake_theme(None, {}))

    def test_repair_identity_tolerates_theme_keys_added_after_freeze(self):
        """冻结清单缺「之后才新增的主题键」时，重拍不应被判成换主题。

        2026-09-14 实测：5 行历史旅行图文因为 theme 多出
        ``thermal_sensitivity_planning``（旧清单里还没这个键）而报
        「旅行·打卡穿搭」改为「旅行·打卡穿搭」—— 两个值一模一样，
        运营无法据以操作，重拍被永久堵死。
        """
        with tempfile.TemporaryDirectory() as folder:
            reference = Path(folder) / "reference.png"
            Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
            service = PhotoStyleReferenceSupplyService(
                generator=FakeGenerator(), root=Path(folder),
            )
            theme = {**resolve_photo_theme("秋季穿搭"),
                     "thermal_sensitivity_planning": {}}
            frozen = {key: value for key, value in theme.items()
                      if key != "thermal_sensitivity_planning"}
            item_dir = Path(folder) / "style_reference_supply" / "rec-freeze_item_1"
            item_dir.mkdir(parents=True)
            (item_dir / "supply_manifest.json").write_text(json.dumps({
                "record_id": "rec-freeze_item_1", "theme_brief": frozen,
                "sources": [], "persona_pack_id": "", "input_hash": "stale",
            }, ensure_ascii=False), encoding="utf-8")
            variation = scene_model_variation()
            account = SimpleNamespace(persona_ref_id="P")

            # 同一主题，只是多出一个冻结之后才新增的空值键 ⇒ 必须放行。
            service.verify_and_rebaseline_identity(
                item_dir=item_dir, paths=[str(reference)], theme=theme,
                account=account, variation=variation)

            # 真换了主题 ⇒ 仍然拦截，并且错误里要能看出是哪个字段变了。
            with self.assertRaisesRegex(ValueError, "差异字段"):
                service.verify_and_rebaseline_identity(
                    item_dir=item_dir, paths=[str(reference)],
                    theme=resolve_photo_theme("咖啡约会"),
                    account=account, variation=variation)


if __name__ == "__main__":
    unittest.main()
