"""Travel semantic-loop tests: two-step planning, per-look scenes, page QA,
targeted repair, template copy, and the native-review release gate."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from PIL import Image

from services.photo_reference_vision import (
    PhotoReferenceVisionError, PhotoReferenceVisionService,
)
from services.photo_travel_qa import (
    FAILURE_SCENE_MISMATCH, FAILURE_WEATHER_MISMATCH, moment_rules_from_contract,
    failed_roles_from_travel_qa, normalize_travel_qa, travel_qa_as_alignment,
)
from services.photo_style_reference_supply import (
    PhotoStyleReferenceSupplyService, _with_repair_note,
)
from services.photo_content_planner import (
    PhotoContentPlanError, plan_th_choice_batch, validate_batch_plan,
)
from services.photo_copy import resolve_photo_copy
from services.photo_theme import resolve_photo_theme
from tests.test_photo_theme_reference import FakeGenerator
from tests.test_photo_reference_vision import FakeVisionClient


def travel_contract():
    return {
        "schema_version": "opv-photo-travel-contract-v1",
        "moments_per_post": 4,
        "moments": [
            {"key": "airport_departure", "label_zh": "机场出发", "label_th": "ลุคไปสนามบิน",
             "evidence_zh": "航站楼、行李箱、登机区域", "mobility_level": "HIGH",
             "allowed_footwear_types": ["SNEAKER", "LOAFER", "FLAT", "MARY_JANE", "LOW_BOOT"],
             "forbidden_footwear_types": ["HIGH_HEEL", "STILETTO"]},
            {"key": "old_town_walk", "label_zh": "老城街拍", "label_th": "ลุคเดินเล่นในเมืองเก่า",
             "evidence_zh": "老城街道、建筑立面", "mobility_level": "HIGH",
             "allowed_footwear_types": ["SNEAKER", "LOAFER", "FLAT", "MARY_JANE", "LOW_BOOT"],
             "forbidden_footwear_types": ["HIGH_HEEL", "STILETTO"]},
            {"key": "cafe_visit", "label_zh": "咖啡店", "label_th": "ลุคไปคาเฟ่",
             "evidence_zh": "咖啡店座位、杯子", "mobility_level": "LOW",
             "allowed_footwear_types": [],
             "forbidden_footwear_types": ["STILETTO"]},
            {"key": "evening_stroll", "label_zh": "傍晚散步", "label_th": "ลุคเดินเล่นช่วงเย็น",
             "evidence_zh": "傍晚光线、街灯", "mobility_level": "HIGH",
             "allowed_footwear_types": ["SNEAKER", "LOAFER", "FLAT", "MARY_JANE", "LOW_BOOT"],
             "forbidden_footwear_types": ["HIGH_HEEL", "STILETTO"]},
        ],
        "look_required_fields": ["travel_moment", "scene_prompt", "weather_logic"],
        "destination_labels_th": {"generic_cool_city": "เมืองอากาศเย็น", "seoul": "โซล"},
        "temperature_labels_th": {"15_22c": "15-22°C"},
    }


def travel_looks():
    footwear = {"a": "SNEAKER", "b": "LOAFER", "c": "LOW_HEEL", "d": "FLAT"}
    return [
        {"role": f"look_{letter}", "display_label": "ลุค", "travel_moment": moment,
         "scene_prompt": prompt, "weather_logic": "室内外温度过渡",
         "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}", "bottom": f"下装{letter}",
         "shoes": f"鞋{letter}", "footwear_type": footwear[letter],
         "outerwear_type": f"t{letter}", "bottom_type": f"b{letter}"}
        for letter, moment, prompt in (
            ("a", "airport_departure", "现代航站楼内，随身登机箱"),
            ("b", "old_town_walk", "老城石板街道与建筑立面"),
            ("c", "cafe_visit", "咖啡店窗边座位与咖啡杯"),
            ("d", "evening_stroll", "傍晚街灯亮起的暖色光线"),
        )
    ]


def travel_style_profile(looks=None):
    return {
        "analysis_method": "doubao_seed_2_1", "planning_flow": "travel_two_step",
        "presentation_type": "SCENE_MODEL", "palette": ["camel"], "temperature": "cool",
        "travel_variables": {"destination": "generic_cool_city", "temperature_band": "15_22c"},
        "aggregate": {"background": "欧式老城"},
        "recommended_sets": [{
            "content_angle_zh": "四场景旅行轻层搭", "scene_zh": "凉爽城市旅行",
            "palette_zh": "卡其、深蓝", "background_prompt": "", "style_modifier": "",
            "looks": travel_looks() if looks is None else looks, "copy": {},
        }],
    }


def travel_copy_templates():
    return [{
        "copy_id": "travel_test_v1",
        "copy": {
            "title": "ไอเดียแต่งตัวเที่ยว{destination}",
            "caption": "อากาศ {temperature} 4 ลุค คุณเลือก A B C หรือ D?",
            "hashtags": ["#แต่งตัวเที่ยว"],
            "slide_texts": ["ไอเดียแต่งตัวเที่ยว{destination}\nเลือก A B C หรือ D",
                            "A · {{label_a}}", "B · {{label_b}}", "C · {{label_c}}",
                            "D · {{label_d}}\nทริปนี้คุณเลือกลุคไหน?"],
            "language_review_status": "DRAFT",
        },
    }]


class TravelSemanticQATest(unittest.TestCase):
    moment_rules = {
        item["key"]: {"allowed": item.get("allowed_footwear_types") or [],
                      "forbidden": item.get("forbidden_footwear_types") or []}
        for item in travel_contract()["moments"]
    }

    def qa_raw(self, **per_page_overrides):
        pages = []
        footwear = {"a": "SNEAKER", "b": "LOAFER", "c": "LOW_HEEL", "d": "FLAT"}
        for look, moment in zip(travel_looks(), [
                "airport_departure", "old_town_walk", "cafe_visit", "evening_stroll"]):
            page = {
                "role": look["role"], "observed_moment": moment,
                "scene_evidence": ["可见证据一", "可见证据二"],
                "outfit_matches": True, "weather_matches": True,
                "mobility_matches": True, "observed_footwear_type": footwear[look["role"][-1]],
                "repair_instruction": "",
            }
            page.update(per_page_overrides.get(look["role"]) or {})
            pages.append(page)
        return {"pages": pages, "style_uniform": True, "notes": ""}

    def normalize(self, raw):
        return normalize_travel_qa(raw, look_plans=travel_looks(),
                                   moment_rules=self.moment_rules)

    def test_all_pass_returns_passed_with_evidence(self):
        qa = self.normalize(self.qa_raw())
        self.assertTrue(qa["passed"])
        first = qa["roles"][0]
        self.assertEqual(first["scene_evidence"], ["可见证据一", "可见证据二"])
        self.assertEqual(first["observed_footwear_type"], "SNEAKER")
        self.assertTrue(all(item["passed"] for item in qa["roles"]))

    def test_cover_recommendation_is_preserved_without_affecting_qa(self):
        raw = self.qa_raw()
        raw["cover_recommendation"] = {
            "role": "look_c", "reason_zh": "穿搭清楚，旅行环境适合封面",
        }
        qa = self.normalize(raw)
        self.assertTrue(qa["passed"])
        self.assertEqual(qa["cover_recommendation"]["role"], "look_c")
        projected = travel_qa_as_alignment(qa)
        self.assertEqual(projected["cover_recommendation"]["role"], "look_c")

    def test_invalid_cover_recommendation_degrades_to_fallback_signal(self):
        raw = self.qa_raw()
        raw["cover_recommendation"] = {"role": "cover", "reason_zh": "bad"}
        qa = self.normalize(raw)
        self.assertTrue(qa["passed"])
        self.assertEqual(qa["cover_recommendation"]["role"], "")

    def test_airport_title_over_cafe_background_must_fail(self):
        raw = self.qa_raw(look_a={"observed_moment": "cafe_visit"})
        qa = self.normalize(raw)
        result = next(item for item in qa["roles"] if item["role"] == "look_a")
        self.assertFalse(result["passed"])
        self.assertEqual(result["failure_code"], FAILURE_SCENE_MISMATCH)
        self.assertEqual(result["expected"], "airport_departure")
        self.assertEqual(result["observed"], "cafe_visit")
        self.assertIn("airport_departure", result["repair_instruction"])
        self.assertEqual(failed_roles_from_travel_qa(qa, ["look_a", "look_b", "look_c", "look_d"]),
                         ["look_a"])

    def test_unknown_scene_must_fail_with_dedicated_code(self):
        raw = self.qa_raw(look_b={"observed_moment": "unknown"})
        qa = self.normalize(raw)
        result = next(item for item in qa["roles"] if item["role"] == "look_b")
        self.assertFalse(result["passed"])
        self.assertEqual(result["failure_code"], "UNKNOWN_SCENE")

    def test_missing_scene_evidence_is_schema_error(self):
        raw = self.qa_raw(look_a={"scene_evidence": []})
        with self.assertRaisesRegex(Exception, "scene_evidence"):
            self.normalize(raw)
        raw2 = self.qa_raw(look_b={"scene_evidence": "航站楼"})
        with self.assertRaisesRegex(Exception, "scene_evidence"):
            self.normalize(raw2)

    def test_missing_or_typed_booleans_must_not_default_to_pass(self):
        for field in ("outfit_matches", "weather_matches", "mobility_matches"):
            raw = self.qa_raw(look_c={field: None})
            with self.assertRaises(Exception):
                self.normalize(raw)
        raw = self.qa_raw(look_d={"outfit_matches": "true"})
        with self.assertRaises(Exception):
            self.normalize(raw)

    def test_style_uniform_missing_must_not_default_to_pass(self):
        raw = self.qa_raw()
        del raw["style_uniform"]
        with self.assertRaisesRegex(Exception, "style_uniform"):
            self.normalize(raw)

    def test_observed_footwear_missing_or_invalid_is_schema_error(self):
        raw = self.qa_raw(look_a={"observed_footwear_type": ""})
        with self.assertRaisesRegex(Exception, "footwear"):
            self.normalize(raw)
        raw2 = self.qa_raw(look_a={"observed_footwear_type": "UNICORN"})
        with self.assertRaisesRegex(Exception, "footwear"):
            self.normalize(raw2)

    def test_stiletto_on_walking_scene_fails_regardless_of_model_bool(self):
        raw = self.qa_raw(look_a={"observed_footwear_type": "STILETTO",
                                  "mobility_matches": True})
        qa = self.normalize(raw)
        result = next(item for item in qa["roles"] if item["role"] == "look_a")
        self.assertFalse(result["passed"])
        self.assertEqual(result["failure_code"], "MOBILITY_MISMATCH")
        self.assertEqual(
            failed_roles_from_travel_qa(qa, ["look_a", "look_b", "look_c", "look_d"]),
            ["look_a"])

    def test_cafe_footwear_rules_follow_contract(self):
        raw = self.qa_raw(look_c={"observed_footwear_type": "LOW_HEEL"})
        qa = self.normalize(raw)
        cafe = next(item for item in qa["roles"] if item["role"] == "look_c")
        self.assertTrue(cafe["passed"])
        # 咖啡馆属低步行场景：合同只禁 STILETTO，HIGH_HEEL 允许。
        raw2 = self.qa_raw(look_c={"observed_footwear_type": "HIGH_HEEL"})
        qa2 = self.normalize(raw2)
        cafe2 = next(item for item in qa2["roles"] if item["role"] == "look_c")
        self.assertTrue(cafe2["passed"])
        raw3 = self.qa_raw(look_c={"observed_footwear_type": "STILETTO"})
        qa3 = self.normalize(raw3)
        cafe3 = next(item for item in qa3["roles"] if item["role"] == "look_c")
        self.assertFalse(cafe3["passed"])
        self.assertEqual(cafe3["failure_code"], "MOBILITY_MISMATCH")

    def test_evening_stroll_in_daylight_must_fail(self):
        raw = self.qa_raw(look_d={"weather_matches": False})
        qa = self.normalize(raw)
        result = next(item for item in qa["roles"] if item["role"] == "look_d")
        self.assertFalse(result["passed"])
        self.assertEqual(result["failure_code"], FAILURE_WEATHER_MISMATCH)

    def test_minor_outfit_difference_is_warning_in_standard(self):
        raw = self.qa_raw(look_c={
            "outfit_matches": False, "outfit_severity": "MINOR",
            "repair_instruction": "少一层同色薄开衫",
        })
        qa = self.normalize(raw)
        self.assertTrue(qa["passed"])
        self.assertTrue(qa["roles"][2]["passed"])
        self.assertEqual(qa["quality_warnings"][0]["code"], "MINOR_OUTFIT_VARIATION")

    def test_major_outfit_difference_still_blocks(self):
        raw = self.qa_raw(look_c={
            "outfit_matches": False, "outfit_severity": "MAJOR",
            "repair_instruction": "主体外套完全缺失",
        })
        qa = self.normalize(raw)
        self.assertFalse(qa["passed"])
        self.assertEqual(qa["roles"][2]["failure_code"], "OUTFIT_MISMATCH")

    def test_product_replacement_is_hard_failure(self):
        raw = self.qa_raw()
        for page in raw["pages"]:
            page["product_matches"] = True
        raw["pages"][1]["product_matches"] = False
        qa = normalize_travel_qa(
            raw, look_plans=travel_looks(), moment_rules=self.moment_rules,
            has_product=True,
        )
        self.assertFalse(qa["passed"])
        self.assertEqual(qa["roles"][1]["failure_code"], "OUTFIT_MISMATCH")

    def test_destination_conflict_blocks_group_but_missing_landmark_does_not(self):
        raw = self.qa_raw()
        raw["destination_conflict"] = False
        qa = normalize_travel_qa(
            raw, look_plans=travel_looks(), moment_rules=self.moment_rules,
            travel_place="河口湖",
        )
        self.assertTrue(qa["passed"])
        raw["destination_conflict"] = True
        raw["destination_evidence"] = ["富士山", "瑞士城堡"]
        qa2 = normalize_travel_qa(
            raw, look_plans=travel_looks(), moment_rules=self.moment_rules,
            travel_place="河口湖",
        )
        self.assertFalse(qa2["passed"])

    def test_four_identical_scenes_claiming_four_moments_must_all_fail(self):
        raw = self.qa_raw()
        for page in raw["pages"]:
            page["observed_moment"] = "cafe_visit"
        qa = self.normalize(raw)
        self.assertFalse(qa["passed"])
        self.assertTrue(all(item["passed"] is False for item in qa["roles"]))

    def test_style_not_uniform_redoes_whole_group(self):
        raw = self.qa_raw()
        raw["style_uniform"] = False
        qa = self.normalize(raw)
        self.assertFalse(qa["passed"])
        self.assertEqual(
            failed_roles_from_travel_qa(qa, ["look_a", "look_b", "look_c", "look_d"]),
            ["look_a", "look_b", "look_c", "look_d"],
        )

    def test_missing_page_is_rejected(self):
        raw = self.qa_raw()
        raw["pages"] = raw["pages"][:3]
        with self.assertRaisesRegex(Exception, "全部"):
            self.normalize(raw)

    def test_alignment_projection_keeps_role_findings_and_evidence(self):
        qa = self.normalize(self.qa_raw(
            look_b={"observed_moment": "airport_departure"}))
        projected = travel_qa_as_alignment(qa)
        self.assertEqual(projected["scope"], "TRAVEL_SEMANTIC_QA")
        self.assertFalse(projected["passed"])
        self.assertIn(FAILURE_SCENE_MISMATCH, projected["reason_codes"])
        findings = {item["role"]: item["passed"] for item in projected["role_findings"]}
        self.assertEqual(findings, {"look_a": True, "look_b": False,
                                    "look_c": True, "look_d": True})
        self.assertTrue(projected["travel_qa"]["roles"][0]["scene_evidence"])

class TravelTwoStepVisionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.images = []
        for index in range(1):
            path = self.root / f"ref-{index}.jpg"
            Image.new("RGB", (60, 90), (120, 90, 70)).save(path)
            self.images.append(str(path))

    def tearDown(self):
        self.tmp.cleanup()

    def analysis_payload(self):
        return {
            "per_reference": [
                {"index": 1, "presentation": "SCENE_MODEL", "notes_zh": "法式复古"},
            ],
            "aggregate": {
                "primary_presentation": "SCENE_MODEL", "confidence": .9,
                "visual_styles": ["french_vintage"], "season": "autumn",
                "palette": ["camel"], "temperature": "cool",
                "materials": ["wool"], "lighting": "soft", "background": "老城",
                "avoid_tags": [],
            },
        }

    def test_reference_analysis_describes_only_and_caches(self):
        client = FakeVisionClient([self.analysis_payload()])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        kwargs = {"record_id": "rec-t2s", "paths": self.images, "theme": {},
                  "category_key": "womenswear"}
        first = service.analyze_reference(**kwargs)
        second = service.analyze_reference(**kwargs)
        self.assertEqual(first, second)
        self.assertEqual(first["presentation_type"], "SCENE_MODEL")
        self.assertNotIn("recommended_sets", first)
        self.assertEqual(len(client.calls), 1)

    def travel_plan_payload(self, moments=("airport_departure", "old_town_walk",
                                           "cafe_visit", "evening_stroll")):
        looks = []
        footwear = {"a": "SNEAKER", "b": "LOAFER", "c": "LOW_HEEL", "d": "FLAT"}
        for letter, moment in zip("abcd", moments):
            looks.append({
                "role": f"look_{letter}", "travel_moment": moment,
                "scene_prompt": f"场景{letter}", "weather_logic": "室内外过渡",
                "display_label": "模型乱写的标签", "footwear_type": footwear[letter],
                "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
                "bottom": f"下装{letter}", "shoes": f"鞋{letter}",
                "outerwear_type": f"t{letter}", "bottom_type": f"b{letter}",
            })
        return {"travel_variables": {}, "posts": [{
            "content_angle_zh": "旅行四场景", "scene_zh": "老城", "palette_zh": "卡其",
            "background_prompt": "", "style_modifier": "", "looks": looks,
        }]}

    def test_travel_plan_forces_audited_labels_and_validates(self):
        client = FakeVisionClient([self.analysis_payload(), self.travel_plan_payload()])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        analysis = service.analyze_reference(
            record_id="rec-t2s", paths=self.images, theme={}, category_key="womenswear")
        plan = service.plan_travel_content(
            record_id="rec-t2s", analysis=analysis, travel_contract=travel_contract(),
            variables={"destination": "generic_cool_city", "temperature_band": "15_22c"},
            count=1)
        labels = {look["travel_moment"]: look["display_label"] for look in plan["posts"][0]["looks"]}
        contract_labels = {item["key"]: item["label_th"] for item in travel_contract()["moments"]}
        self.assertEqual(labels, contract_labels)
        profile = service.build_travel_style_profile(analysis, plan, count=1)
        self.assertEqual(profile["planning_flow"], "travel_two_step")
        self.assertTrue(all(look.get("scene_prompt") for look in profile["recommended_sets"][0]["looks"]))

    def test_travel_plan_sees_reference_images_and_records_astra_route(self):
        client = FakeVisionClient([self.analysis_payload(), self.travel_plan_payload()])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        analysis = service.analyze_reference(
            record_id="rec-direct-vision", paths=self.images, theme={},
            category_key="womenswear")
        plan = service.plan_travel_content(
            record_id="rec-direct-vision", analysis=analysis,
            travel_contract=travel_contract(), variables={}, count=1,
            reference_paths=self.images,
        )

        self.assertEqual(1, len(client.calls[1][0]))
        self.assertIn("直接提供给规划模型的图片", client.calls[1][1])
        self.assertEqual("gpt-6-astra", plan["model_routing"]["requested_model"])
        stored = json.loads(
            (self.root / "reference_contracts" / "rec-direct-vision" /
             "travel_plan.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            "gpt-6-astra", stored["input_contract"]["planning_route"]["model"]
        )
        self.assertEqual(1, len(stored["input_contract"]["planning_images"]))
        self.assertTrue(stored["input_contract"]["planning_images"][0]["sha256"])

    def test_travel_plan_auto_revises_once_then_stops(self):
        broken = self.travel_plan_payload(
            moments=("airport_departure", "airport_departure",
                     "cafe_visit", "evening_stroll"))
        client = FakeVisionClient([self.analysis_payload(), broken, self.travel_plan_payload()])
        service = PhotoReferenceVisionService(root=self.root, client=client)
        analysis = service.analyze_reference(
            record_id="rec-revise", paths=self.images, theme={}, category_key="womenswear")
        plan = service.plan_travel_content(
            record_id="rec-revise", analysis=analysis, travel_contract=travel_contract(),
            variables={}, count=1, reference_paths=self.images)
        self.assertEqual(len(client.calls), 3)  # analysis + broken plan + revised plan
        self.assertEqual(client.calls[1][0], client.calls[2][0])
        moments = [look["travel_moment"] for look in plan["posts"][0]["looks"]]
        self.assertEqual(len(set(moments)), 4)

        client_two_bad = FakeVisionClient([self.analysis_payload(), broken, broken])
        service_two = PhotoReferenceVisionService(root=self.root, client=client_two_bad)
        analysis_two = service_two.analyze_reference(
            record_id="rec-revise2", paths=self.images, theme={}, category_key="womenswear")
        with self.assertRaisesRegex(PhotoReferenceVisionError, "两次未通过"):
            service_two.plan_travel_content(
                record_id="rec-revise2", analysis=analysis_two,
                travel_contract=travel_contract(), variables={}, count=1)


class TravelPlanTemplateCopyTest(unittest.TestCase):
    def plan_travel(self, profile=None, templates=None):
        return plan_th_choice_batch(
            record_id="rec-tpl", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=resolve_photo_theme("凉爽旅行"), reference_mode="STYLE", count=1,
            style_profile=profile or travel_style_profile(),
            travel_contract=travel_contract(),
            copy_templates=templates if templates is not None else travel_copy_templates(),
        )

    def test_copy_comes_from_template_fill_not_model_thai(self):
        plan = self.plan_travel()
        item = plan["items"][0]
        self.assertEqual(item["copy_source"], "template_fill")
        self.assertEqual(item["template_review_status"], "DRAFT")
        self.assertIn("เมืองอากาศเย็น", item["copy"]["title"])
        self.assertIn("15-22°C", item["copy"]["caption"])
        self.assertNotIn("{destination}", item["copy"]["cover"])
        self.assertNotIn("{temperature}", item["copy"]["caption"])
        looks = item["looks"]
        self.assertEqual([look["travel_moment"] for look in looks],
                         ["airport_departure", "old_town_walk", "cafe_visit", "evening_stroll"])

    def test_duplicated_travel_moments_are_rejected_in_frozen_plan(self):
        looks = travel_looks()
        looks[1]["travel_moment"] = "airport_departure"
        with self.assertRaisesRegex(PhotoContentPlanError, "旅行场景重复"):
            self.plan_travel(profile=travel_style_profile(looks=looks))

    def test_travel_flow_without_templates_is_rejected(self):
        with self.assertRaisesRegex(PhotoContentPlanError, "缺少 travel_contract 或审核文案模板"):
            self.plan_travel(templates=[])


class TravelSupplySceneTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def base_kwargs(self):
        reference = Path(self.tmp.name) / "reference.png"
        Image.new("RGB", (120, 180), (130, 95, 75)).save(reference)
        persona_dir = Path(self.tmp.name) / "persona" / "P"
        persona_dir.mkdir(parents=True, exist_ok=True)
        items = []
        for role in ("FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER",
                     "BODY_FULL_NEUTRAL", "BODY_FULL_MOTION"):
            img = persona_dir / f"{role.lower()}.png"
            Image.new("RGB", (60, 90), (120, 110, 100)).save(img)
            items.append({"local_path": str(img), "role": role, "approved": True})
        return {
            "record_id": "rec-travel-supply",
            "reference_paths": [str(reference)],
            "theme": resolve_photo_theme("凉爽旅行"),
            "account": SimpleNamespace(persona_ref_id="P"),
            "persona": {"persona_id": "P", "reference_items": items},
            "variation": {
                "family_id": "vision_dynamic_1", "presentation_type": "SCENE_MODEL",
                "scene_zh": "欧式老城", "background_prompt": "",
                "style_profile": {"analysis_method": "doubao_seed_2_1",
                                  "planning_flow": "travel_two_step",
                                  "presentation_type": "SCENE_MODEL"},
                "looks": travel_looks(),
            },
        }

    def test_each_look_uses_its_own_scene_and_shared_identity_constraints(self):
        generator = FakeGenerator()

        class PassingQA:
            def review_alignment(self, **kwargs):
                return {"passed": True, "notes": "首图风格通过"}

            def review_travel_pages(self, **kwargs):
                return {"schema_version": "opv-photo-travel-semantic-qa-v1",
                        "passed": True, "style_uniform": True,
                        "roles": [{"role": look["role"], "passed": True,
                                   "failure_code": "", "expected": look["travel_moment"],
                                   "observed": look["travel_moment"], "title": "",
                                   "repair_instruction": ""}
                                  for look in travel_looks()],
                        "notes": ""}

        service = PhotoStyleReferenceSupplyService(
            generator=generator, root=Path(self.tmp.name), vision_service=PassingQA())
        result = service.prepare(**self.base_kwargs())
        self.assertTrue(result["group_alignment"]["passed"])
        self.assertEqual(result["group_alignment"]["scope"], "TRAVEL_SEMANTIC_QA")
        # 2026-09-15 提速：B/C/D 预取后请求到达顺序不定，按槽位排序断言。
        ordered = sorted(generator.requests, key=lambda r: r.slot_index)
        scene_prompts = [request.scene_snapshot["prompt_core"] for request in ordered]
        self.assertEqual(scene_prompts, ["现代航站楼内，随身登机箱", "老城石板街道与建筑立面",
                                         "咖啡店窗边座位与咖啡杯", "傍晚街灯亮起的暖色光线"])
        for request in generator.requests:
            self.assertIn("同一人物身份、同一目的地视觉体系、统一色彩基调",
                          request.outfit_state["style_direction"])
            self.assertIn("travel_moment=", request.plan_shot["purpose"])
        self.assertEqual(len(generator.requests), 4)

    def test_styling_intent_reaches_image_prompt_direction(self):
        generator = FakeGenerator()

        class PassingQA:
            def review_alignment(self, **kwargs):
                return {"passed": True, "notes": "ok"}

            def review_travel_pages(self, **kwargs):
                return {"schema_version": "opv-photo-travel-semantic-qa-v1",
                        "passed": True, "style_uniform": True,
                        "roles": [{"role": look["role"], "passed": True,
                                   "failure_code": "", "expected": look["travel_moment"],
                                   "observed": look["travel_moment"], "title": "",
                                   "repair_instruction": ""}
                                  for look in travel_looks()],
                        "notes": ""}

        kwargs = self.base_kwargs()
        kwargs["variation"]["looks"][0]["styling_intent"] = (
            "用顺直裤线平衡短外套体积，裤脚与修长乐福鞋自然衔接"
        )
        service = PhotoStyleReferenceSupplyService(
            generator=generator, root=Path(self.tmp.name), vision_service=PassingQA())
        service.prepare(**kwargs)
        look_a = next(r for r in generator.requests if r.slot_index == 1)
        direction = look_a.outfit_state["style_direction"]
        self.assertIn("穿搭比例与下装鞋履衔接", direction)
        self.assertIn("裤脚与修长乐福鞋自然衔接", direction)

    def test_style_direction_carries_reference_palette(self):
        generator = FakeGenerator()

        class PassingQA:
            def review_alignment(self, **kwargs):
                return {"passed": True, "notes": "ok"}

            def review_travel_pages(self, **kwargs):
                return {"schema_version": "opv-photo-travel-semantic-qa-v1",
                        "passed": True, "style_uniform": True,
                        "roles": [{"role": look["role"], "passed": True,
                                   "failure_code": "", "expected": look["travel_moment"],
                                   "observed": look["travel_moment"], "title": "",
                                   "repair_instruction": ""} for look in travel_looks()],
                        "notes": ""}

        kwargs = self.base_kwargs()
        kwargs["variation"]["style_profile"]["palette"] = ["鲜红色", "深靛蓝色", "藏蓝色", "白色"]
        service = PhotoStyleReferenceSupplyService(
            generator=generator, root=Path(self.tmp.name), vision_service=PassingQA())
        service.prepare(**kwargs)
        for request in generator.requests:
            self.assertIn("参考图配色仅作为审美方向（鲜红色、深靛蓝色、藏蓝色、白色）",
                          request.outfit_state["style_direction"])
            self.assertIn("不得把四套配套单品强行收敛到同一色域",
                          request.outfit_state["style_direction"])

    def test_travel_prompt_core_uses_frozen_scene_without_background_injection(self):
        generator = FakeGenerator()

        class PassingQA:
            def review_alignment(self, **kwargs):
                return {"passed": True, "notes": "首图通过"}

            def review_travel_pages(self, **kwargs):
                return {"schema_version": "opv-photo-travel-semantic-qa-v1",
                        "passed": True, "style_uniform": True,
                        "roles": [{"role": look["role"], "passed": True,
                                   "failure_code": "", "expected": look["travel_moment"],
                                   "observed": look["travel_moment"], "title": "",
                                   "repair_instruction": ""} for look in travel_looks()],
                        "notes": ""}

        kwargs = self.base_kwargs()
        kwargs["variation"]["style_profile"]["background_features"] = ["积雪的街道", "木质路灯"]
        service = PhotoStyleReferenceSupplyService(
            generator=generator, root=Path(self.tmp.name), vision_service=PassingQA())
        service.prepare(**kwargs)
        # 背景注入已移除：prompt_core 直接使用冻结 scene_prompt。
        for request in generator.requests:
            self.assertNotIn("背景必须延续参考图", request.scene_snapshot["prompt_core"])
        # 非旅行 Look（无 travel_moment）不拼接锚定句。
        kwargs_plain = self.base_kwargs()
        kwargs_plain["variation"]["style_profile"] = {
            "analysis_method": "doubao_seed_2_1", "presentation_type": "MODEL_FULL_BODY"}
        generator2 = FakeGenerator()

        class PlainReviewer:
            def review_alignment(self, **kwargs):
                return {"passed": True, "notes": "ok"}

        service2 = PhotoStyleReferenceSupplyService(
            generator=generator2, root=Path(self.tmp.name), vision_service=PlainReviewer())
        result2 = service2.prepare(record_id="rec-plain", **{
            key: value for key, value in kwargs_plain.items() if key != "record_id"})
        for request in generator2.requests:
            self.assertNotIn("背景必须延续参考图", request.scene_snapshot["prompt_core"])

    def test_scene_mismatch_only_regenerates_blamed_role_with_repair_note(self):
        generator = FakeGenerator()

        class OneCafeA:
            def __init__(self):
                self.calls = 0

            def review_alignment(self, **kwargs):
                return {"passed": True, "notes": "首图风格通过"}

            def review_travel_pages(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    roles = []
                    for look in travel_looks():
                        failed = look["role"] == "look_a" and look["travel_moment"] == "airport_departure"
                        roles.append({
                            "role": look["role"],
                            "passed": not failed,
                            "failure_code": FAILURE_SCENE_MISMATCH if failed else "",
                            "expected": look["travel_moment"],
                            "observed": "cafe_visit" if failed else look["travel_moment"],
                            "title": "", "repair_instruction": (
                                "当前图片是咖啡馆，目标是机场，请加入航站楼和行李箱线索"
                                if failed else ""),
                        })
                    return {"schema_version": "opv-photo-travel-semantic-qa-v1",
                            "passed": False, "style_uniform": True, "roles": roles,
                            "notes": "A 场景错位"}
                return {"schema_version": "opv-photo-travel-semantic-qa-v1",
                        "passed": True, "style_uniform": True,
                        "roles": [{"role": look["role"], "passed": True, "failure_code": "",
                                   "expected": look["travel_moment"],
                                   "observed": look["travel_moment"], "title": "",
                                   "repair_instruction": ""} for look in travel_looks()],
                        "notes": ""}

        reviewer = OneCafeA()
        events = []
        service = PhotoStyleReferenceSupplyService(
            generator=generator, root=Path(self.tmp.name), vision_service=reviewer)
        result = service.prepare(**self.base_kwargs(),
                                 progress=lambda event, **data: events.append((event, data)))
        self.assertEqual(len(generator.requests), 5)  # 4 + regenerate look_a only
        self.assertEqual(generator.requests[4].slot_index, 1)
        self.assertIn("质检修正", generator.requests[4].plan_shot["purpose"])
        self.assertIn("航站楼", generator.requests[4].plan_shot["purpose"])
        self.assertEqual(result["repaired_roles_this_run"], ["look_a"])
        self.assertEqual(result["group_repair_attempts"], 1)
        self.assertIn(("qa_started", {}), events)
        self.assertTrue(any(
            event == "repair_scheduled" and list(data.get("roles") or []) == ["look_a"]
            for event, data in events
        ))
        self.assertEqual([event for event, _ in events if event == "asset_generated"],
                         ["asset_generated"] * 5)

    def test_repair_note_never_stacks(self):
        purpose = "生成 look_a 完整穿搭"
        once = _with_repair_note(purpose, "场景错位：咖啡馆→机场")
        twice = _with_repair_note(once, "再次失败：仍是咖啡馆")
        self.assertEqual(twice.count("；质检修正："), 1)
        self.assertIn("再次失败", twice)
        self.assertNotIn("场景错位", twice)


class TravelReleaseGateTest(unittest.TestCase):
    @staticmethod
    def workflow(copy_block=None, package_theme=None, frozen_theme=None):
        theme = {"theme_key": "COOL_WEATHER_TRAVEL", "travel_theme_type": "CHECK_IN"}
        package = SimpleNamespace(photo_manifest_json={
            "copy": copy_block or {
                "title": "ไอเดียแต่งตัวเที่ยว", "caption": "คุณชอบลุคไหน",
                "hashtags": ["#OOTD"],
                "slide_texts": ["ปก", "ลุคเอ", "ลุคบี", "ลุคซี", "ลุคดี"],
            },
            "theme_brief": package_theme or theme,
        })
        revision = SimpleNamespace(plan_snapshot_json={
            "plan": {"theme_brief": frozen_theme or theme},
        })
        repository = SimpleNamespace(
            get_content_package=lambda _package_id: package,
            get_task_revision=lambda _revision_id: revision,
        )
        return SimpleNamespace(repository=repository)

    @staticmethod
    def task(recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2"):
        return SimpleNamespace(
            recipe_id=recipe_id, content_package_id="package-1",
            active_revision_id="revision-1",
        )

    def test_publish_checks_actual_frozen_copy_instead_of_static_template_status(self):
        from services.feishu_workflow import FeishuTaskWorkflow, FeishuWorkflowError
        tasks = [self.task()]
        workflow = self.workflow()
        FeishuTaskWorkflow._require_travel_copy_release_allowed(workflow, tasks)

        bad_copy = dict(workflow.repository.get_content_package("").photo_manifest_json["copy"])
        bad_copy["slide_texts"] = list(bad_copy["slide_texts"])
        bad_copy["slide_texts"][4] = "ลุคริม湖"
        with self.assertRaisesRegex(FeishuWorkflowError, "CJK"):
            FeishuTaskWorkflow._require_travel_copy_release_allowed(
                self.workflow(copy_block=bad_copy), tasks
            )

        non_travel = [self.task("PHOTO_TH_PICK_YOUR_LOOK_V3")]
        FeishuTaskWorkflow._require_travel_copy_release_allowed(None, non_travel)

    def test_publish_blocks_theme_drift(self):
        from services.feishu_workflow import FeishuTaskWorkflow, FeishuWorkflowError
        with self.assertRaisesRegex(FeishuWorkflowError, "主题"):
            FeishuTaskWorkflow._require_travel_copy_release_allowed(
                self.workflow(package_theme={
                    "theme_key": "COOL_WEATHER_TRAVEL",
                    "travel_theme_type": "COLOR_MATCH",
                }), [self.task()]
            )


class TravelBackgroundContinuityTest(unittest.TestCase):
    """背景延续（2026-09-08 放松版）：延续旅行氛围而非复制地标——
    scene_prompt 与参考背景特征零重叠不再拒绝，特征只作记录；
    生成端不追加背景注入，以当页冻结 scene_prompt 为准。"""

    @staticmethod
    def analysis_payload(background_features=None, climate="snow",
                         destination_style="北欧雪国小镇街道"):
        return {
            "per_reference": [
                {"index": 1, "presentation": "SCENE_MODEL", "notes_zh": "法式复古",
                 "background_cues": list(background_features or ["积雪的街道"])},
            ],
            "aggregate": {
                "primary_presentation": "SCENE_MODEL", "confidence": .9,
                "visual_styles": ["french_vintage"], "season": "winter",
                "palette": ["camel"], "temperature": "cold",
                "materials": ["wool"], "lighting": "soft", "background": "雪国小镇",
                "climate": climate, "destination_visual_style": destination_style,
                "background_features": list(
                    background_features or ["积雪的街道", "木质路灯"]),
                "avoid_tags": [],
            },
        }

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.image = self.root / "ref-0.jpg"
        Image.new("RGB", (60, 90), (220, 230, 240)).save(self.image)

    def tearDown(self):
        self.tmp.cleanup()

    def make_service(self, responses):
        return PhotoReferenceVisionService(
            root=self.root, client=FakeVisionClient(responses))

    def travel_plan_payload(self, scene_prompts, background_notes=None):
        moments = ("airport_departure", "old_town_walk", "cafe_visit", "evening_stroll")
        footwear = {"a": "SNEAKER", "b": "LOAFER", "c": "LOW_HEEL", "d": "FLAT"}
        shoes = {"a": "运动鞋", "b": "乐福鞋", "c": "低跟鞋", "d": "平底单鞋"}
        looks = []
        for letter, moment, prompt in zip("abcd", moments, scene_prompts):
            looks.append({
                "role": f"look_{letter}", "travel_moment": moment,
                "scene_prompt": prompt, "weather_logic": "室内外过渡",
                "display_label": "", "footwear_type": footwear[letter],
                "background_feature_zh": (background_notes or {}).get(letter, ""),
                "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
                "bottom": f"下装{letter}", "shoes": shoes[letter],
                "outerwear_type": f"t{letter}", "bottom_type": f"b{letter}",
            })
        return {"travel_variables": {}, "posts": [{
            "content_angle_zh": "雪国四场景", "scene_zh": "雪国小镇", "palette_zh": "米白",
            "background_prompt": "", "style_modifier": "", "looks": looks,
        }]}

    def plan_via_service(self, scene_prompts, background_notes=None,
                         features=None):
        plan_payload = self.travel_plan_payload(scene_prompts, background_notes)
        service = self.make_service([self.analysis_payload(features), plan_payload])
        analysis = service.analyze_reference(
            record_id="rec-bg", paths=[str(self.image)], theme={},
            category_key="womenswear")
        return service, analysis, service.plan_travel_content(
            record_id="rec-bg", analysis=analysis, travel_contract=travel_contract(),
            variables={}, count=1)

    def test_reference_analysis_extracts_background_features(self):
        service = self.make_service([self.analysis_payload()])
        analysis = service.analyze_reference(
            record_id="rec-bg-cache", paths=[str(self.image)], theme={},
            category_key="womenswear")
        self.assertEqual(analysis["background_features"], ["积雪的街道", "木质路灯"])
        self.assertEqual(analysis["climate"], "snow")
        self.assertEqual(analysis["destination_visual_style"], "北欧雪国小镇街道")
        cached = service.analyze_reference(
            record_id="rec-bg-cache", paths=[str(self.image)], theme={},
            category_key="womenswear")
        self.assertEqual(cached, analysis)

    def test_scene_prompts_weaving_background_pass(self):
        prompts = [
            "积雪停机坪旁的航站楼落地窗前，登机箱压出雪痕",
            "雪后老城石板街，木质路灯与欧式老楼立面",
            "窗外积雪街道的木屋咖啡馆临窗座位",
            "傍晚雪后街道，木质路灯暖光映在雪面上",
        ]
        service, analysis, plan = self.plan_via_service(prompts)
        looks = plan["posts"][0]["looks"]
        self.assertTrue(all(look["background_feature_zh"] is not None for look in looks))
        self.assertIn("积雪", looks[2]["scene_prompt"])

    def test_scene_prompt_dropping_background_is_no_longer_rejected(self):
        # 背景放松：场景与参考特征零重叠不再触发重规划。
        prompts = [
            "现代机场航站楼内",
            "热带棕榈大道步行街",
            "现代商场连锁咖啡店内",
            "城市夜晚步行街灯光明亮",
        ]
        service = self.make_service([
            self.analysis_payload(), self.travel_plan_payload(prompts)])
        analysis = service.analyze_reference(
            record_id="rec-bg-fail", paths=[str(self.image)], theme={},
            category_key="womenswear")
        plan = service.plan_travel_content(
            record_id="rec-bg-fail", analysis=analysis,
            travel_contract=travel_contract(), variables={}, count=1)
        self.assertEqual(plan["posts"][0]["looks"][0]["scene_prompt"], "现代机场航站楼内")
        self.assertEqual(len(service.client.calls), 2)

    def test_partial_background_overlap_passes_without_revision(self):
        # 每个 Look 命中任一特征（完整短语或核心词）即可，无需覆盖全部特征。
        prompts = [
            "积雪覆盖的机场航站楼",
            "老城石板街与木质路灯",
            "窗外积雪街道的木屋咖啡馆",
            "傍晚木质路灯下的步道",
        ]
        plan_payload = self.travel_plan_payload(prompts)
        service = self.make_service([self.analysis_payload(), plan_payload])
        analysis = service.analyze_reference(
            record_id="rec-bg-partial", paths=[str(self.image)], theme={},
            category_key="womenswear")
        plan = service.plan_travel_content(
            record_id="rec-bg-partial", analysis=analysis,
            travel_contract=travel_contract(), variables={}, count=1)
        self.assertEqual(len(service.client.calls), 2)

    def test_single_look_dropping_background_passes_without_revision(self):
        prompts = [
            "积雪机场航站楼", "积雪老城石板街",
            "现代商场连锁咖啡店",
            "傍晚木质路灯步道",
        ]
        plan_payload = self.travel_plan_payload(prompts)
        service = self.make_service([
            self.analysis_payload(), plan_payload,
            self.travel_plan_payload([
                "积雪机场航站楼", "积雪老城石板街",
                "窗外积雪的木屋咖啡馆座位", "傍晚木质路灯步道"]),
        ])
        analysis = service.analyze_reference(
            record_id="rec-bg-one", paths=[str(self.image)], theme={},
            category_key="womenswear")
        plan = service.plan_travel_content(
            record_id="rec-bg-one", analysis=analysis,
            travel_contract=travel_contract(), variables={}, count=1)
        self.assertEqual(plan["posts"][0]["looks"][2]["scene_prompt"],
                         "现代商场连锁咖啡店")
        self.assertEqual(len(service.client.calls), 2)

    def test_prompts_carry_background_rules(self):
        analysis_prompt = PhotoReferenceVisionService._reference_analysis_prompt(
            theme={"label_zh": "凉爽旅行"}, category_key="womenswear",
            content_requirement="")
        self.assertIn("背景描述要求", analysis_prompt)
        self.assertIn("background_cues", analysis_prompt)
        plan_prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis=self.analysis_payload(), travel_contract=travel_contract(),
            variables={}, content_requirement="", count=1)
        self.assertIn("背景延续（按优先级）", plan_prompt)
        self.assertIn("background_feature_zh", plan_prompt)
        self.assertIn("具体地标和景观不必在每页出现", plan_prompt)
        self.assertIn("不强制文字重叠", plan_prompt)
        self.assertNotIn("组合方式", plan_prompt)
        self.assertNotIn("零重叠", plan_prompt)


class TravelPlanDifferenceTest(unittest.TestCase):
    """旅行差异保持可感知，同时允许复用成熟裤型和协调鞋履。"""

    def plan_with_looks(self, looks):
        return PhotoReferenceVisionService(root=Path("/tmp"))._normalize_travel_plan(
            {"posts": [{"content_angle_zh": "x", "scene_zh": "s", "palette_zh": "p",
                        "background_prompt": "", "style_modifier": "", "looks": looks}]},
            travel_contract(), 1)

    def base_looks(self):
        footwear = {"a": "SNEAKER", "b": "LOAFER", "c": "FLAT", "d": "LOW_BOOT"}
        moments = ("airport_departure", "old_town_walk", "cafe_visit", "evening_stroll")
        looks = []
        for letter, moment in zip("abcd", moments):
            looks.append({
                "role": f"look_{letter}", "travel_moment": moment,
                "scene_prompt": f"场景{letter}", "weather_logic": "室内外过渡",
                "display_label": "", "footwear_type": footwear[letter],
                "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
                "bottom": f"下装{letter}", "shoes": f"鞋{letter}",
                "outerwear_type": f"t{letter}", "bottom_type": f"b{letter}",
            })
        return looks

    def test_a_c_can_reuse_outerwear_inner_and_shoes(self):
        looks = self.base_looks()
        looks[2]["outerwear"] = looks[0]["outerwear"]      # 同外套
        looks[2]["top_inner"] = looks[0]["top_inner"]      # 同内搭
        looks[2]["shoes"] = looks[0]["shoes"]              # 同鞋
        looks[2]["footwear_type"] = looks[0]["footwear_type"]
        plan, errors = self.plan_with_looks(looks)
        self.assertEqual(errors, [], errors)

    def test_b_d_can_reuse_outerwear_inner_and_shoes(self):
        looks = self.base_looks()
        looks[3]["outerwear"] = looks[1]["outerwear"]
        looks[3]["top_inner"] = looks[1]["top_inner"]
        looks[3]["shoes"] = looks[1]["shoes"]
        looks[3]["footwear_type"] = looks[1]["footwear_type"]
        plan, errors = self.plan_with_looks(looks)
        self.assertEqual(errors, [], errors)

    def test_two_core_field_difference_passes(self):
        looks = self.base_looks()
        looks[2]["outerwear"] = looks[0]["outerwear"]
        looks[2]["top_inner"] = looks[0]["top_inner"]
        plan, errors = self.plan_with_looks(looks)  # 底+鞋不同 → 距离 2
        self.assertEqual(errors, [], errors)
        self.assertEqual(len(plan["posts"][0]["looks"]), 4)

    def test_completely_identical_outfit_is_still_rejected(self):
        looks = self.base_looks()
        for field in ("outerwear", "top_inner", "bottom", "shoes"):
            looks[2][field] = looks[0][field]
        looks[2]["footwear_type"] = looks[0]["footwear_type"]
        plan, errors = self.plan_with_looks(looks)
        self.assertTrue(any("重复 Look" in e for e in errors), errors)

    def test_two_upper_body_combinations_are_allowed(self):
        looks = self.base_looks()
        for index in (2, 3):
            looks[index]["outerwear"] = looks[index % 2]["outerwear"]
            looks[index]["top_inner"] = looks[index % 2]["top_inner"]
        plan, errors = self.plan_with_looks(looks)
        self.assertEqual(errors, [], errors)


class TravelFootwearContractTest(unittest.TestCase):
    """改造二：moment 鞋履规则 + footwear_type 枚举 + 文字矛盾。"""

    def normalize_plan(self, looks):
        return PhotoReferenceVisionService(root=Path("/tmp"))._normalize_travel_plan(
            {"posts": [{"content_angle_zh": "x", "scene_zh": "s", "palette_zh": "p",
                        "background_prompt": "", "style_modifier": "", "looks": looks}]},
            travel_contract(), 1)

    def look(self, *, moment, footwear, shoes):
        letter = moment.split("_")[0][0] + moment[-1]
        return {
            "role": "look_a", "travel_moment": moment,
            "scene_prompt": "场景", "weather_logic": "室内外过渡",
            "display_label": "", "footwear_type": footwear, "shoes": shoes,
            "outerwear": "外套", "top_inner": "内搭", "bottom": "下装",
            "outerwear_type": "t", "bottom_type": "b",
        }

    def full_four_looks(self, first_look):
        looks = [first_look]
        filler = [
            ("look_b", "old_town_walk", "LOAFER", "乐福鞋"),
            ("look_c", "cafe_visit", "LOW_HEEL", "低跟鞋"),
            ("look_d", "evening_stroll", "FLAT", "平底单鞋"),
        ]
        for role, moment, ftype, shoes in filler:
            looks.append({**first_look, "role": role, "travel_moment": moment,
                          "footwear_type": ftype, "shoes": shoes,
                          "outerwear": f"外套{role}", "top_inner": f"内搭{role}",
                          "bottom": f"下装{role}"})
        return looks

    def test_stiletto_on_old_town_walk_rejected(self):
        first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="运动鞋")
        looks = self.full_four_looks(first)
        looks[1]["footwear_type"] = "STILETTO"
        looks[1]["shoes"] = "细跟高跟鞋"
        plan, errors = self.normalize_plan(looks)
        self.assertTrue(any("look_b" in e and "鞋" in e for e in errors), errors)

    def test_high_heel_on_evening_stroll_rejected(self):
        first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="运动鞋")
        looks = self.full_four_looks(first)
        looks[3]["footwear_type"] = "HIGH_HEEL"
        looks[3]["shoes"] = "粗跟高跟鞋"
        plan, errors = self.normalize_plan(looks)
        self.assertTrue(any("look_d" in e and "鞋" in e for e in errors), errors)

    def test_low_heel_in_cafe_allowed(self):
        first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="运动鞋")
        looks = self.full_four_looks(first)
        plan, errors = self.normalize_plan(looks)
        self.assertEqual(errors, [], errors)

    def test_footwear_type_missing_or_out_of_enum_rejected(self):
        first = self.look(moment="airport_departure", footwear="", shoes="运动鞋")
        plan, errors = self.normalize_plan(self.full_four_looks(first))
        self.assertTrue(any("footwear_type" in e for e in errors), errors)
        first2 = self.look(moment="airport_departure", footwear="UNICORN", shoes="运动鞋")
        plan2, errors2 = self.normalize_plan(self.full_four_looks(first2))
        self.assertTrue(any("footwear_type" in e for e in errors2), errors2)

    def test_shoes_text_contradicting_footwear_type_rejected(self):
        first = self.look(moment="airport_departure", footwear="FLAT", shoes="细跟高跟鞋")
        plan, errors = self.normalize_plan(self.full_four_looks(first))
        self.assertTrue(any("矛盾" in e for e in errors), errors)
        ok_first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="厚底运动鞋")
        plan2, errors2 = self.normalize_plan(self.full_four_looks(ok_first))
        self.assertEqual(errors2, [], errors2)

    def test_flat_sole_low_boot_description_is_not_a_contradiction(self):
        # 生产回归：row recvuwy4SLaaxj 计划里"平底低筒皮靴"（LOW_BOOT）被误判矛盾。
        first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="运动鞋")
        looks = self.full_four_looks(first)
        looks[1]["footwear_type"] = "LOW_BOOT"
        looks[1]["shoes"] = "黑色圆头平底低筒皮靴"
        plan, errors = self.normalize_plan(looks)
        self.assertEqual([e for e in errors if "矛盾" in e], [], errors)

    def test_multi_attribute_description_with_declared_type_passes(self):
        first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="运动鞋")
        looks = self.full_four_looks(first)
        looks[1]["footwear_type"] = "LOW_BOOT"
        looks[1]["shoes"] = "酒红色圆头防滑平底低筒靴"
        plan, errors = self.normalize_plan(looks)
        self.assertEqual([e for e in errors if "矛盾" in e], [], errors)

    def test_chelsea_and_laceup_boots_are_low_boot(self):
        # 生产回归二：切尔西靴/系带靴（LOW_BOOT）不再被子串匹配漏判。
        first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="运动鞋")
        looks = self.full_four_looks(first)
        looks[1]["footwear_type"] = "LOW_BOOT"
        looks[1]["shoes"] = "黑色圆头低筒切尔西靴，配平底防滑橡胶鞋底"
        looks[2]["footwear_type"] = "LOW_BOOT"
        looks[2]["shoes"] = "深棕色圆头低筒系带靴，配平底防滑纹橡胶鞋底"
        plan, errors = self.normalize_plan(looks)
        self.assertEqual([e for e in errors if "矛盾" in e], [], errors)

    def test_weather_logic_with_precise_temperature_rejected(self):
        for text in ("机场室内约20°C，室外16度", "傍晚 21℃", "咖啡馆 20 摄氏度"):
            first = self.look(moment="airport_departure", footwear="SNEAKER", shoes="运动鞋")
            first["weather_logic"] = text
            plan, errors = self.normalize_plan(self.full_four_looks(first))
            self.assertTrue(any("精确温度" in e for e in errors), (text, errors))

    def test_band_level_weather_logic_passes(self):
        first = self.look(moment="airport_departure", footwear="SNEAKER",
                          shoes="运动鞋")
        first["weather_logic"] = "室内外切换，方便穿脱的轻层搭"
        plan, errors = self.normalize_plan(self.full_four_looks(first))
        self.assertEqual(errors, [], errors)


class TravelFinalPageQATest(unittest.TestCase):
    """改造五：最终套版页面检查。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.image_paths = []
        for index in range(1, 5):
            path = Path(self.tmp.name) / f"page-{index}.jpg"
            Image.new("RGB", (60, 90), (90, 90, 90)).save(path)
            self.image_paths.append(str(path))

    def tearDown(self):
        self.tmp.cleanup()

    def vision(self, response):
        return PhotoReferenceVisionService(root=Path(self.tmp.name), client=FakeVisionClient([response]))

    def pages(self, **overrides):
        pages = []
        for index in range(1, 5):
            page = {"index": index, "text_readable": True, "text_matches_expected": True,
                    "text_clipped": False, "text_garbled": False, "subject_obscured": False,
                    "issues": []}
            page.update(overrides.get(index) or {})
            pages.append(page)
        return {"pages": pages, "notes": ""}

    def test_final_pages_all_pass(self):
        service = self.vision(self.pages())
        qa = service.review_travel_final_pages(
            image_paths=self.image_paths,
            expected_texts=[f"文字{i}" for i in range(1, 5)],
            role_order=["look_a", "look_b", "look_c", "look_d"],
        )
        self.assertTrue(qa["passed"])
        self.assertEqual(len(qa["pages"]), 4)

    def test_final_pages_must_cover_all_four(self):
        service = self.vision(self.pages())
        with self.assertRaisesRegex(Exception, "4"):
            service.review_travel_final_pages(
                image_paths=self.image_paths[:3],
                expected_texts=[f"文字{i}" for i in range(1, 5)],
                role_order=["look_a", "look_b", "look_c", "look_d"],
            )

    def test_clipped_or_garbled_text_fails(self):
        raw = self.pages()
        raw["pages"][2]["text_clipped"] = True
        raw["pages"][3]["text_garbled"] = True
        service = self.vision(raw)
        qa = service.review_travel_final_pages(
            image_paths=self.image_paths,
            expected_texts=[f"文字{i}" for i in range(1, 5)],
            role_order=["look_a", "look_b", "look_c", "look_d"],
        )
        self.assertFalse(qa["passed"])
        self.assertEqual(qa["pages"][2]["text_clipped"], True)
        self.assertEqual(qa["pages"][3]["text_garbled"], True)

    def test_missing_bool_field_is_structural_failure(self):
        bad = self.pages()
        del bad["pages"][1]["text_matches_expected"]
        service = self.vision(bad)
        with self.assertRaisesRegex(Exception, "text_matches_expected"):
            service.review_travel_final_pages(
                image_paths=self.image_paths,
                expected_texts=[f"文字{i}" for i in range(1, 5)],
                role_order=["look_a", "look_b", "look_c", "look_d"],
            )

    def test_travel_flow_blocks_technical_completion_on_failed_qa(self):
        from services.photo_package import NativePhotoProductionFlow
        manifest = {
            "content_package_id": "pkg-1",
            "copy": {"slide_texts": [f"文字{i}" for i in range(1, 5)]},
            "slides": [{"index": i, "path": f"p{i}.jpg", "sha256": "x"} for i in range(1, 5)],
        }
        saved = {}

        class Repo:
            def update_content_package(self, package_id, **kwargs):
                saved["package_id"] = package_id
                saved.update(kwargs)

        class Vision:
            def __init__(self, qa):
                self.qa = qa
                self.calls = 0

            def review_travel_final_pages(self, **kwargs):
                self.calls += 1
                return dict(self.qa)

        failed_qa = {"schema_version": "opv-photo-travel-final-qa-v1", "passed": False,
                     "pages": [{"index": 3, "text_clipped": True,
                                "issues": ["文字被裁切"]}],
                     "notes": ""}
        task = SimpleNamespace(recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2")
        vision = Vision(failed_qa)
        flow = NativePhotoProductionFlow(None, None, output_root=Path("/tmp"), vision_service=vision)
        flow.repository = Repo()
        with self.assertRaisesRegex(Exception, "旅行最终页面"):
            flow._run_travel_final_page_qa(task, manifest)
        self.assertIn("travel_final_page_qa", manifest)
        self.assertIn("package_fingerprint", manifest)
        self.assertEqual(saved["package_id"], "pkg-1")
        self.assertEqual(vision.calls, 1)

        # Passing QA stores evidence without raising.
        manifest2 = {**manifest}
        ok_qa = {"schema_version": "opv-photo-travel-final-qa-v1", "passed": True,
                 "pages": [{"index": i, "issues": []} for i in range(1, 5)], "notes": ""}
        vision2 = Vision(ok_qa)
        flow2 = NativePhotoProductionFlow(None, None, output_root=Path("/tmp"), vision_service=vision2)
        flow2.repository = Repo()
        flow2._run_travel_final_page_qa(task, manifest2)
        self.assertTrue(manifest2["travel_final_page_qa"]["passed"])

    def test_non_travel_recipe_skips_final_qa(self):
        from services.photo_package import NativePhotoProductionFlow

        class Vision:
            def __init__(self):
                self.calls = 0

            def review_travel_final_pages(self, **kwargs):
                self.calls += 1
                return {}

        vision = Vision()
        flow = NativePhotoProductionFlow(None, None, output_root=Path("/tmp"), vision_service=vision)
        task = SimpleNamespace(recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3")
        self.assertIsNone(flow._run_travel_final_page_qa(task, {"copy": {}, "slides": []}))
        self.assertEqual(vision.calls, 0)


class TravelCopyTokenTest(unittest.TestCase):
    def test_resolve_photo_copy_fills_travel_tokens(self):
        copy_block = {
            "title": "ไอเดียแต่งตัวเที่ยว{destination}",
            "caption": "อากาศ {temperature} เลือก A B C หรือ D?",
            "hashtags": ["#travel"],
            "slide_texts": ["ไอเดียแต่งตัวเที่ยว{destination}", "A · {{label_a}}",
                            "B · {{label_b}}", "C · {{label_c}}", "D · {{label_d}}"],
        }
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ลุค {letter}"}}
                  for letter in "abcd"]
        resolved = resolve_photo_copy(
            copy_block, assets=assets, locale="th-TH",
            extra_tokens={"destination": "เมืองอากาศเย็น", "temperature": "15-22°C"})
        self.assertIn("เมืองอากาศเย็น", resolved["title"])
        self.assertIn("15-22°C", resolved["caption"])
        self.assertEqual(resolved["slide_texts"][1], "A · ลุค a")


if __name__ == "__main__":
    unittest.main()


class FixedBackgroundGuardTest(unittest.TestCase):
    """固定背景（Phase 3）：同场景与目的地冲突护栏按背景方式豁免。"""

    def setUp(self):
        self.moment_rules = moment_rules_from_contract(travel_contract())

    def raw_all_same_scene(self):
        pages = []
        footwear = {"a": "SNEAKER", "b": "LOAFER", "c": "LOW_HEEL", "d": "FLAT"}
        for look in travel_looks():
            pages.append({
                "role": look["role"], "observed_moment": "old_town_walk",
                "scene_evidence": ["纯色背景", "人物全身"],
                "outfit_matches": True, "weather_matches": True,
                "mobility_matches": True,
                "observed_footwear_type": footwear[look["role"][-1]],
                "repair_instruction": "",
            })
        return {"pages": pages, "style_uniform": True,
                "destination_conflict": True,
                "destination_evidence": ["无地标"], "notes": ""}

    def test_fixed_background_same_scene_and_no_place_do_not_fail(self):
        raw = self.raw_all_same_scene()
        qa = normalize_travel_qa(
            raw, look_plans=travel_looks(), moment_rules=self.moment_rules,
            travel_place="东京", fixed_background=True,
        )
        self.assertTrue(qa["passed"])
        self.assertTrue(all(item["passed"] for item in qa["roles"]))

    def test_without_fixed_background_the_guard_still_fails(self):
        raw = self.raw_all_same_scene()
        raw["destination_conflict"] = False
        qa = normalize_travel_qa(
            raw, look_plans=travel_looks(), moment_rules=self.moment_rules,
            travel_place="东京",
        )
        self.assertFalse(qa["passed"])
        self.assertTrue(any(
            item["failure_code"] == FAILURE_SCENE_MISMATCH for item in qa["roles"]))


class FixedBackgroundNoShortCircuitTest(unittest.TestCase):
    """固定背景豁免不得短路核心检查（2026-09-15 七样审查修复）。"""

    def setUp(self):
        self.moment_rules = moment_rules_from_contract(travel_contract())

    def base_raw(self):
        pages = []
        footwear = {"a": "SNEAKER", "b": "LOAFER", "c": "LOW_HEEL", "d": "FLAT"}
        for look in travel_looks():
            pages.append({
                "role": look["role"], "observed_moment": "old_town_walk",
                "scene_evidence": ["纯色背景", "人物全身"],
                "outfit_matches": True, "weather_matches": True,
                "mobility_matches": True,
                "observed_footwear_type": footwear[look["role"][-1]],
                "repair_instruction": "",
                "product_matches": True,
            })
        return {"pages": pages, "style_uniform": True,
                "destination_conflict": False, "notes": ""}

    def test_fixed_background_product_mismatch_still_fails(self):
        raw = self.base_raw()
        raw["pages"][1]["product_matches"] = False
        qa = normalize_travel_qa(
            raw, look_plans=travel_looks(), moment_rules=self.moment_rules,
            has_product=True, fixed_background=True,
        )
        self.assertFalse(qa["passed"])
        self.assertEqual(qa["roles"][1]["failure_code"], "OUTFIT_MISMATCH")

    def test_fixed_background_person_disaster_still_fails(self):
        raw = self.base_raw()
        raw["pages"][2]["person_flags"] = {
            "face_or_limb_deformity": True, "obvious_unnatural_tilt": False}
        qa = normalize_travel_qa(
            raw, look_plans=travel_looks(), moment_rules=self.moment_rules,
            fixed_background=True,
        )
        self.assertFalse(qa["passed"])
        self.assertEqual(qa["roles"][2]["failure_code"], "PERSON_DISASTER")

    def test_fixed_background_clean_pages_still_pass(self):
        qa = normalize_travel_qa(
            self.base_raw(), look_plans=travel_looks(),
            moment_rules=self.moment_rules, fixed_background=True,
        )
        self.assertTrue(qa["passed"])
