from __future__ import annotations

import json
import tempfile
import unittest
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
