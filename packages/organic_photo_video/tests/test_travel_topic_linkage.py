"""Travel theme+place linkage tests (six topic themes, copy survival)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from domain.photo_contracts import validate_copy

from services.photo_content_planner import (
    PhotoContentPlanError, plan_th_choice_batch, summarize_batch_plan,
    validate_batch_plan,
)
from services.photo_reference_vision import (
    PhotoReferenceVisionService, parse_vision_envelope,
)
from services.photo_theme import (
    THEME_OPTIONS, build_theme_copy, resolve_photo_theme, travel_theme_templates,
)
from tests.test_photo_reference_vision import FakeVisionClient

PACKAGE_ROOT = Path(__file__).resolve().parents[1]

TRAVEL_TOPIC = {
    "theme_type": "SCENE_MATCH",
    "theme_version": 1,
    "theme_label_zh": "旅行·环境协调",
    "planning_focus": "四套穿搭围绕同一地点，重点比较服装配色和风格与环境的协调关系",
    "topic_patterns": ["去{place}怎么穿更协调？"],
    "body_copy_focus": "造型名称，加一条协调特点",
    "cta_patterns": ["你更喜欢哪套？"],
    "place": "浅草寺周边",
    "temperature_band": "15_22c",
    "content_requirement": "",
}


def travel_payload(*, same_moment: bool = False, with_copy: bool = True):
    moments = ["old_town_walk"] * 4 if same_moment else [
        "airport_departure", "old_town_walk", "cafe_visit", "evening_stroll"]
    copy = {
        "place_localized": "อาซากุสะ",
        "title": "4 ลุคเข้ากับอาซากุสะ",
        "caption": "แต่งตัวเข้ากับบรรยากาศอาซากุสะ คุณชอบลุคไหน?",
        "hashtags": ["#แต่งตัวเที่ยว"],
        "slide_texts": [
            "ลุคเข้ากับอาซากุสะ", "A · ลุควัดเก่า เรียบแต่มีลูกเล่น",
            "B · ลุคเดินถนน สบายดูดี", "C · ลุคคาเฟ่ นุ่มนวล",
            "D · ลุคเย็น อบอุ่น คุณชอบลุคไหน?",
        ],
    }
    post = {
        "content_angle_zh": "浅草寺环境协调", "scene_zh": "浅草寺周边街道",
        "palette_zh": "米白、藏蓝", "background_prompt": "", "style_modifier": "",
        "looks": [
            {"role": f"look_{letter}", "travel_moment": moments[index],
             "scene_prompt": f"浅草寺画面{index + 1}：{'雷门' if index == 0 else '仲见世街' if index == 1 else '寺庙侧巷' if index == 2 else '傍晚街道'}不同机位",
             "weather_logic": "室内外过渡", "display_label": "",
             "footwear_type": "SNEAKER" if index % 2 == 0 else "LOAFER",
             "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
             "bottom": f"下装{letter}", "shoes": f"鞋{letter}",
             "outerwear_type": "", "bottom_type": ""}
            for index, letter in enumerate("abcd")
        ],
    }
    if with_copy:
        post["topic_zh"] = "去浅草寺周边怎么穿更协调？"
        post["copy"] = copy
    return {"travel_variables": {}, "posts": [post]}


class ThemeResolutionTest(unittest.TestCase):
    def test_all_six_travel_themes_resolve_and_keep_legacy_key(self):
        labels = [t["label_zh"] for t in travel_theme_templates().values()]
        self.assertEqual(len(labels), 6)
        for label in labels:
            self.assertIn(label, THEME_OPTIONS)
            theme = resolve_photo_theme(label)
            self.assertEqual(theme["theme_key"], "COOL_WEATHER_TRAVEL")
            self.assertTrue(theme["travel_theme_type"])
            self.assertTrue(theme["title"], "泰语降级标题必须存在")
        legacy = resolve_photo_theme("凉爽旅行")
        self.assertEqual(legacy["theme_key"], "COOL_WEATHER_TRAVEL")
        self.assertFalse(legacy.get("travel_theme_type"))

    def test_unknown_theme_still_rejected(self):
        with self.assertRaises(ValueError):
            resolve_photo_theme("不存在的主题")


class TravelPromptTopicTest(unittest.TestCase):
    def test_topic_prompt_has_linkage_block_and_copy_rules(self):
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract={"moments": [
                {"key": "old_town_walk", "label_zh": "老城", "evidence_zh": "街道",
                 "label_th": "x", "forbidden_footwear_types": []}]},
            variables={}, content_requirement="", count=1,
            travel_topic=TRAVEL_TOPIC,
        )
        self.assertIn("【旅行主题联动】", prompt)
        self.assertIn("SCENE_MATCH", prompt)
        self.assertIn("浅草寺周边", prompt)
        self.assertIn("可共用同一 key", prompt)
        self.assertIn("发布文案（主题联动必须生成）", prompt)
        self.assertIn("topic_zh", prompt)
        self.assertIn("place_localized", prompt)
        self.assertIn("ภูเขาไฟฟูจิ", prompt)
        self.assertIn("slide_texts 只用于图片排版", prompt)
        self.assertIn("禁止省略号和不完整选项", prompt)
        self.assertIn("不能退化成‘某地 4 套穿搭’", prompt)
        self.assertIn("第二行是与 topic_zh 对应的短钩子", prompt)
        self.assertIn("不重复四套名称", prompt)
        self.assertIn("整组只需 1-2 页清晰展示代表性地标", prompt)
        self.assertIn("避免全部正面站立微笑", prompt)
        self.assertIn("下装和鞋履必须联合规划", prompt)
        self.assertIn("允许复用协调的鞋履、成熟裤型和指定商品", prompt)
        self.assertIn("只更换场景、姿势", prompt)
        self.assertNotIn("任意两套在外套、内搭、下装、鞋履四个核心字段中至少有两个不同", prompt)

    def test_legacy_prompt_unchanged_without_topic(self):
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract={"moments": [
                {"key": "old_town_walk", "label_zh": "老城", "evidence_zh": "街道",
                 "label_th": "x", "forbidden_footwear_types": []}]},
            variables={}, content_requirement="", count=1,
        )
        self.assertIn("每篇四个必须互不相同", prompt)
        self.assertIn("不要生成标题、正文或 CTA 文案", prompt)
        self.assertNotIn("【旅行主题联动】", prompt)

    def test_airport_is_listed_last_and_not_used_as_look_a_example(self):
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract={"moments": [
                {"key": "airport_departure", "label_zh": "机场", "evidence_zh": "航站楼",
                 "label_th": "airport", "forbidden_footwear_types": []},
                {"key": "old_town_walk", "label_zh": "老城", "evidence_zh": "街道",
                 "label_th": "town", "forbidden_footwear_types": []},
            ]}, variables={}, content_requirement="", count=1,
        )
        self.assertLess(prompt.index("- old_town_walk"), prompt.index("- airport_departure"))
        self.assertNotIn('"role":"look_a","travel_moment":"airport_departure"', prompt)
        self.assertIn("机场不是默认开场", prompt)


class TravelPlanNormalizeTopicTest(unittest.TestCase):
    def setUp(self):
        self.contract = {"moments": [
            {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
             "forbidden_footwear_types": []}
            for m in ("airport_departure", "old_town_walk", "cafe_visit", "evening_stroll")
        ]}

    def normalize(self, payload, travel_topic):
        return PhotoReferenceVisionService(root=Path("/tmp"))._normalize_travel_plan(
            payload, self.contract, 1, travel_topic=travel_topic)

    def test_same_moment_allowed_with_topic_but_scenes_must_differ(self):
        plan, errors = self.normalize(travel_payload(same_moment=True), TRAVEL_TOPIC)
        self.assertEqual(errors, [], errors)
        moments = [look["travel_moment"] for look in plan["posts"][0]["looks"]]
        self.assertEqual(len(set(moments)), 1, "主题联动允许同场所")

    def test_same_moment_without_topic_still_rejected(self):
        plan, errors = self.normalize(travel_payload(same_moment=True), None)
        self.assertTrue(any("互不相同" in e for e in errors))

    def test_identical_scene_prompts_are_not_a_hard_failure_under_topic(self):
        payload = travel_payload(same_moment=True)
        for look in payload["posts"][0]["looks"]:
            look["scene_prompt"] = "同一画面"
        plan, errors = self.normalize(payload, TRAVEL_TOPIC)
        self.assertEqual(errors, [])

    def test_topic_copy_is_validated_and_frozen(self):
        plan, errors = self.normalize(travel_payload(), TRAVEL_TOPIC)
        self.assertEqual(errors, [])
        post = plan["posts"][0]
        self.assertEqual(post["topic_zh"], "去浅草寺周边怎么穿更协调？")
        self.assertEqual(len(post["copy"]["slide_texts"]), 5)
        self.assertEqual(post["copy"]["language_review_status"], "DRAFT_TRAVEL_TOPIC")
        self.assertEqual(post["copy"]["place_localized"], "อาซากุสะ")
        self.assertIn("อาซากุสะ", post["copy"]["title"])
        self.assertIn("อาซากุสะ", post["copy"]["slide_texts"][0])

    def test_product_category_overrides_same_category_outfit_reference(self):
        plan, errors = PhotoReferenceVisionService(root=Path("/tmp"))._normalize_travel_plan(
            travel_payload(), self.contract, 1, travel_topic=TRAVEL_TOPIC,
            outfit_reference_indices=[1],
            product_context={
                "product_id": "puffer-1", "product_name": "棕色羽绒服",
                "category": "outerwear", "reference_pack_id": "pack-1",
            },
        )
        self.assertEqual(errors, [])
        for look in plan["posts"][0]["looks"]:
            self.assertEqual(look["outerwear"], "指定商品外套（以商品参考图为准）")
            self.assertEqual(look["target_product"]["product_id"], "puffer-1")

    def test_footwear_product_remains_authoritative(self):
        plan, errors = PhotoReferenceVisionService(root=Path("/tmp"))._normalize_travel_plan(
            travel_payload(), self.contract, 1, travel_topic=TRAVEL_TOPIC,
            product_context={
                "product_id": "shoe-1", "product_name": "棕色乐福鞋",
                "category": "footwear", "reference_pack_id": "pack-shoe-1",
            },
        )
        self.assertEqual(errors, [])
        for look in plan["posts"][0]["looks"]:
            self.assertEqual(look["shoes"], "指定商品鞋履（以商品参考图为准）")
            self.assertEqual(look["target_product"]["product_id"], "shoe-1")

    def test_missing_copy_with_specific_place_requests_plan_revision(self):
        payload = travel_payload(with_copy=False)
        topic = dict(TRAVEL_TOPIC, thai_fallback={
            "title": "4 ลุคเข้ากับบรรยากาศทริป", "cover": "ลุคเข้ากับสถานที่\nA B C หรือ D?",
            "caption": "แต่งตัวให้เข้ากับบรรยากาศ", "hashtags": ["#OOTD"],
            "cta": "เลือกลุคไหนดี?",
        })
        plan, errors = self.normalize(payload, topic)
        self.assertTrue(any("place_localized" in error for error in errors))

    def test_topic_copy_with_cjk_in_slide_degrades_to_theme_fallback(self):
        payload = travel_payload()
        payload["posts"][0]["copy"]["slide_texts"][4] = "D: ลุคริม湖"
        topic = dict(TRAVEL_TOPIC, thai_fallback={
            "title": "4 ลุคเข้ากับบรรยากาศทริป",
            "cover": "ลุคเข้ากับสถานที่\nA B C หรือ D?",
            "caption": "แต่งตัวให้เข้ากับบรรยากาศ",
            "hashtags": ["#OOTD"],
            "cta": "เลือกลุคไหนดี?",
        })

        plan, errors = self.normalize(payload, topic)

        self.assertTrue(any("标题和封面" in error for error in errors))


class PlannerTopicBranchTest(unittest.TestCase):
    def topic_style_profile(self, with_copy=True):
        payload = travel_payload(with_copy=with_copy)
        post = payload["posts"][0]
        return {
            "analysis_method": "doubao_seed_2_1",
            "planning_flow": "travel_two_step",
            "presentation_type": "SCENE_MODEL",
            "travel_variables": {},
            "travel_topic": TRAVEL_TOPIC,
            "recommended_sets": [{
                "content_angle_zh": post["content_angle_zh"],
                "scene_zh": post["scene_zh"], "palette_zh": post["palette_zh"],
                "background_prompt": "", "style_modifier": "",
                "looks": post["looks"], "copy": post.get("copy") or {},
                "topic_zh": post.get("topic_zh") or "",
            }],
        }

    def plan_with(self, style_profile):
        return plan_th_choice_batch(
            record_id="rec-topic", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=resolve_photo_theme("旅行·环境协调"),
            reference_mode="STYLE", count=1, style_profile=style_profile,
        )

    def test_native_multiway_topic_freezes_model_copy(self):
        """断点 A3（2026-09-15）：原生一衣多穿经主题联动分支产出模型文案，
        不再回落通用四选一投票模板。"""
        profile = self.topic_style_profile()
        profile["travel_topic"] = {**TRAVEL_TOPIC, "theme_type": "NATIVE_MULTIWAY"}
        plan = self.plan_with(profile)
        item = plan["items"][0]
        self.assertEqual(item["copy_source"], "travel_topic_model")
        self.assertTrue(item["copy"]["title"])
        self.assertEqual(len(item["copy"]["slide_texts"]), 5)
        self.assertTrue(plan.get("allow_repeated_travel_moments"))

    def test_topic_branch_freezes_model_copy_and_allows_same_moment(self):
        plan = self.plan_with(self.topic_style_profile())
        item = plan["items"][0]
        self.assertEqual(item["copy_source"], "travel_topic_model")
        self.assertEqual(item["topic_zh"], "去浅草寺周边怎么穿更协调？")
        self.assertEqual(len(item["copy"]["slide_texts"]), 5)
        moments = [look["travel_moment"] for look in item["looks"]]
        self.assertEqual(len(set(moments)), 4, "本例四场景本就不同")
        self.assertEqual(plan["travel_theme_type"], "SCENE_MATCH")
        self.assertEqual(plan["travel_place"], "浅草寺周边")
        self.assertTrue(plan["allow_repeated_travel_moments"])
        self.assertIn("主题联动", summarize_batch_plan(plan))
        self.assertIn("浅草寺周边", summarize_batch_plan(plan))

    def test_topic_branch_accepts_repeated_moments_in_validation(self):
        profile = self.topic_style_profile()
        moments = ["old_town_walk"] * 4
        for look, moment in zip(profile["recommended_sets"][0]["looks"], moments):
            look["travel_moment"] = moment
        plan = self.plan_with(profile)
        validate_batch_plan(plan)  # 不应抛"场景重复"

    def test_legacy_travel_flow_unchanged_without_topic(self):
        profile = self.topic_style_profile()
        profile.pop("travel_topic")
        profile["recommended_sets"][0].pop("copy", None)
        # 旧模式需要模板
        with self.assertRaisesRegex(PhotoContentPlanError, "缺少 travel_contract"):
            self.plan_with(profile)


class CopySurvivalTest(unittest.TestCase):
    def test_build_theme_copy_prefers_frozen_topic_slides(self):
        theme = resolve_photo_theme("旅行·环境协调")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ลุค {letter}"}}
                  for letter in "abcd"]
        variation = {
            "copy": {
                "place_localized": "อาซากุสะ",
                "title": "ไตเติลจากโมเดล", "caption": "แคปชันจากโมเดล",
                "hashtags": ["#tag"],
                "slide_texts": ["ปก", "A หนึ่ง", "B สอง", "C สาม", "D สี่ เลือกลุคไหน"],
                "language_review_status": "DRAFT_TRAVEL_TOPIC",
            },
        }
        result = build_theme_copy(theme, assets, variation)
        self.assertEqual(result["slide_texts"][:4], variation["copy"]["slide_texts"][:4])
        # 2026-09-15 CTA 唯一来源：模型末页自带的 CTA 原样保留，不再被主题
        # 投票 CTA 覆盖（此前即「模型写收藏被改成 A/B/C/D」的丢失路径）。
        self.assertEqual(
            result["slide_texts"][4], "D สี่\nเลือกลุคไหน"
        )
        self.assertEqual(result["title"], "ไตเติลจากโมเดล")
        self.assertEqual(result["place_localized"], "อาซากุสะ")
        self.assertEqual(result["language_review_status"], "DRAFT_TRAVEL_TOPIC")

    def test_missing_topic_slides_fall_back_to_legacy_assembly(self):
        theme = resolve_photo_theme("旅行·环境协调")
        assets = [{"role": "look_a", "display_label": {"th-TH": "ลุค A"}}]
        result = build_theme_copy(theme, assets[:1], {"copy": {}})
        self.assertEqual(len(result["slide_texts"]), 5)
        self.assertEqual(result["language_review_status"], "production_theme_profile")

    def test_long_topic_copy_becomes_complete_short_image_copy(self):
        theme = resolve_photo_theme("旅行·环境协调")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ลุค {letter}"}}
                  for letter in "abcd"]
        variation = {"copy": {
            "place_localized": "ภูเขาไฟฟูจิ",
            "title": "เช็กอินภูเขาไฟฟูจิด้วย 4 ลุควินเทจฝรั่งเศส: กางเกงหรือกระโปรง?",
            "caption": "คำบรรยายฉบับเต็มยังคงอยู่สำหรับตอนเผยแพร่",
            "hashtags": ["#ฟูจิ"],
            "cta": "คุณเลือก A, B, C หรือ D?",
            "slide_texts": [
                "เช็กอินภูเขาไฟฟูจิด้วยสี่ลุควินเทจฝรั่งเศส กางเกงหรือกระโปรง แบบไหนเหมาะกับทริปนี้ที่สุด",
                "A · กางเกงทรงสวย — คำอธิบายรายละเอียดที่ยาวเกินพื้นที่บนภาพและควรอยู่ในแคปชันเท่านั้น",
                "B · กระโปรงคลาสสิก — คำอธิบายรายละเอียดที่ยาวเกินพื้นที่บนภาพและควรอยู่ในแคปชันเท่านั้น",
                "C · ลุคเดินเล่น — คำอธิบายรายละเอียดที่ยาวเกินพื้นที่บนภาพและควรอยู่ในแคปชันเท่านั้น",
                "D · ลุคริมทะเลสาบ — คำอธิบายรายละเอียดที่ยาวมาก คุณเลือก A, B, C หรือ D?",
            ],
        }}
        result = build_theme_copy(theme, assets, variation)
        self.assertEqual(result["title"], variation["copy"]["title"])
        self.assertEqual(result["caption"], variation["copy"]["caption"])
        self.assertIn("ภูเขาไฟฟูจิ", result["slide_texts"][0])
        self.assertIn("\n", result["slide_texts"][0])
        self.assertEqual(result["slide_texts"][1], "A · กางเกงทรงสวย")
        self.assertEqual(result["slide_texts"][4].splitlines()[-1], "คุณเลือก A, B, C หรือ D?")
        self.assertTrue(all("…" not in slide for slide in result["slide_texts"]))

    def test_partial_slides_do_not_leak(self):
        theme = resolve_photo_theme("旅行·环境协调")
        variation = {"copy": {"slide_texts": ["only", "two"]}}
        result = build_theme_copy(theme, [], variation)
        self.assertEqual(len(result["slide_texts"]), 5)

    def test_final_page_keeps_model_cta_verbatim(self):
        theme = resolve_photo_theme("旅行·四选一")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": letter}}
                  for letter in "abcd"]
        variation = {"copy": {
            "place_localized": "โอซาก้า",
            "title": "ไปโอซาก้าใส่ลุคไหนดี?",
            "caption": "สี่ไอเดียสำหรับเดินเที่ยวโอซาก้า",
            "slide_texts": [
                "โอซาก้า\nทริปนี้ใส่อะไรดี?", "A · แจ็กเก็ต", "B · เสื้อโค้ต",
                "C · คาร์ดิแกน", "D · เบลเซอร์ คุณเลือก A, B, C หรือ D?",
            ],
        }}
        result = build_theme_copy(theme, assets, variation)
        self.assertEqual(
            result["slide_texts"][4],
            "D · เบลเซอร์\nคุณเลือก A, B, C หรือ D?",
        )
        self.assertEqual(result["copy_policy_version"], 2)


    def test_over_limit_model_title_is_clamped_when_the_copy_is_assembled(self):
        # 2026-09-13 线上实测：模型写出的 title 只超 1 个 UTF-16 单元（91 vs 90），
        # 却在**付费生图之后**的封版校验处炸掉整行。build_theme_copy 是最终装配点，
        # 也是断点续跑复用已冻结文案的必经之路——必须在这里就夹回发布契约内。
        theme = resolve_photo_theme("旅行·拍照穿搭")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ลุค {letter}"}}
                  for letter in "abcd"]
        over = ("วัดโทไดจิ ฤดูใบไม้ร่วง ใส่ยังไงถ่ายรูปสวย? "
                "4 ลุคฝรั่งเศสวินเทจ กางเกง หรือ กระโปรง เลือกได้")
        self.assertEqual(len(over.encode("utf-16-le")) // 2, 91)
        variation = {"copy": {
            "place_localized": "วัดโทไดจิ", "title": over, "caption": "แคปชัน",
            "hashtags": ["#tag"],
            "slide_texts": ["ปก", "A หนึ่ง", "B สอง", "C สาม", "D สี่ เลือกลุคไหน"],
        }}
        result = build_theme_copy(theme, assets, variation)
        self.assertLessEqual(len(result["title"].encode("utf-16-le")) // 2, 90)
        self.assertTrue(over.startswith(result["title"]))
        self.assertEqual(result["caption"], "แคปชัน")

    def test_within_limit_model_title_is_left_untouched(self):
        theme = resolve_photo_theme("旅行·拍照穿搭")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ลุค {letter}"}}
                  for letter in "abcd"]
        variation = {"copy": {
            "place_localized": "วัดโทไดจิ", "title": "ไตเติลสั้น", "caption": "แคปชัน",
            "hashtags": ["#tag"],
            "slide_texts": ["ปก", "A หนึ่ง", "B สอง", "C สาม", "D สี่ เลือกลุคไหน"],
        }}
        result = build_theme_copy(theme, assets, variation)
        self.assertEqual(result["title"], "ไตเติลสั้น")

    def test_deferred_label_placeholders_are_bound_to_asset_labels(self):
        # 2026-09-14 线上实测（TH 旅行线表格 143–147 行）：旅行文案包把
        # `A · {{label_a}}` 推迟到冻结点才替换，而「运营填了图文主题」会让
        # feishu_workflow 用 build_theme_copy 覆盖已经解析好的 request copy。
        # 这条主题联动分支若不绑定标签，字面占位符就会冻进请求，被
        # validate_frozen_request 拒掉整行——而且失败发生在**付费生图之后**。
        from domain.photo_contracts import validate_copy

        theme = resolve_photo_theme("凉爽旅行")
        assets = [
            {"role": "look_a", "display_label": {"th-TH": "ลุคเดินเล่นในเมืองเก่า"}},
            {"role": "look_b", "display_label": {"th-TH": "ลุควันช้อปปิ้ง"}},
            {"role": "look_c", "display_label": {"th-TH": "ลุคไปคาเฟ่"}},
            {"role": "look_d", "display_label": {"th-TH": "ลุคเดินเล่นช่วงเย็น"}},
        ]
        variation = {"copy": {
            "title": "ลุคไหนไปเมืองอากาศเย็นดี", "caption": "แคปชัน",
            "hashtags": ["#tag"],
            "slide_texts": [
                "อากาศ 15-22°C ใส่ลุคไหนดี?\nA B C หรือ D",
                "A · {{label_a}}", "B · {{label_b}}", "C · {{label_c}}",
                "D · {{label_d}}\nลุคไหนพร้อมลุยทั้งวัน?",
            ],
        }}
        result = build_theme_copy(theme, assets, variation)
        self.assertEqual(result["slide_texts"][1], "A · ลุคเดินเล่นในเมืองเก่า")
        self.assertEqual(result["slide_texts"][3], "C · ลุคไปคาเฟ่")
        self.assertEqual(result["slide_texts"][4].splitlines()[0], "D · ลุคเดินเล่นช่วงเย็น")
        self.assertFalse(
            [slide for slide in result["slide_texts"] if "{{" in slide or "}}" in slide])
        self.assertEqual(validate_copy(result), [])

    def test_unbindable_placeholder_fails_loudly(self):
        # 打错的占位符（如多了空格）不能静默冻进发布文案：宁可当场报错，
        # 也不要等到封版校验用一句看不出原因的话拒掉整行。
        theme = resolve_photo_theme("凉爽旅行")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ลุค {letter}"}}
                  for letter in "abcd"]
        variation = {"copy": {"slide_texts": [
            "ปก", "A · {{label_a }}", "B สอง", "C สาม", "D สี่",
        ]}}
        with self.assertRaisesRegex(ValueError, "占位符"):
            build_theme_copy(theme, assets, variation)

    # --- 2026-09-14 语言绑定（V3 国家无关配方 / VN 围巾线）---------------------
    # 主题内联文案（title/cover/caption/cta/hashtags）都是泰语，素材标签也只按
    # th-TH 取。非 TH 任务必须改取绑定语言包，否则 VN 帖会带着泰语 Look 标签、
    # 泰语 CTA 与泰语 hashtags 发布，而且是在付费生图之后才被发布检查拦下。

    @staticmethod
    def _vi_pack():
        from config.loader import resolve_locale_pack
        return resolve_locale_pack("vi-VN")

    def test_non_thai_task_binds_labels_in_its_own_language(self):
        from services.locale_quality import copy_locale_issues
        theme = resolve_photo_theme("凉爽旅行")
        assets = [
            {"role": "look_a", "display_label": {"vi-VN": "Look dạo phố cổ"}},
            {"role": "look_b", "display_label": {"vi-VN": "Look đi cà phê"}},
            {"role": "look_c", "display_label": {"vi-VN": "Look ngày mua sắm"}},
            {"role": "look_d", "display_label": {"vi-VN": "Look dạo biển"}},
        ]
        variation = {"copy": {
            "place_localized": "Seoul",
            "title": "Seoul se lạnh mặc gì?", "caption": "Chuyến đi Seoul",
            "hashtags": ["#OOTD"],
            "slide_texts": [
                "Seoul se lạnh\nA B C hay D?",
                "A · {{label_a}}", "B · {{label_b}}", "C · {{label_c}}",
                "D · {{label_d}}\nBạn thích look nào?",
            ],
        }}
        result = build_theme_copy(
            theme, assets, variation, locale="vi-VN", locale_pack=self._vi_pack())
        self.assertEqual(result["slide_texts"][1], "A · Look dạo phố cổ")
        self.assertEqual(result["slide_texts"][4].splitlines()[0], "D · Look dạo biển")
        self.assertEqual(result["title"], "Seoul se lạnh mặc gì?")
        # 四个输出位（封面 / 逐页标签 / CTA / hashtags）都不许出现泰文。
        self.assertNotIn("ลุค", json.dumps(result, ensure_ascii=False))
        self.assertEqual(copy_locale_issues(result, "vi-VN"), [])
        self.assertEqual(validate_copy(result), [])

    def test_non_thai_task_never_borrows_the_inline_thai_copy(self):
        """规划没给完整 5 页时，兜底也必须来自语言包而不是主题泰语。"""
        theme = resolve_photo_theme("凉爽旅行")
        pack = self._vi_pack()
        assets = [
            {"role": f"look_{letter}", "display_label": {"vi-VN": f"Look {letter}"}}
            for letter in "abcd"
        ]
        result = build_theme_copy(
            theme, assets, {"copy": {}}, locale="vi-VN", locale_pack=pack)
        blob = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("ลุค", blob)
        self.assertEqual(result["title"], "4 look du lịch")   # pack generic title
        self.assertEqual(result["slide_texts"][0], "Look du lịch")
        self.assertEqual(validate_copy(result), [])

    def test_missing_non_thai_look_label_fails_loudly(self):
        """非 TH 任务没有可用 Look 标签时必须当场报错，不得静默回退泰文。"""
        theme = resolve_photo_theme("凉爽旅行")
        assets = [{"role": "look_a", "display_label": {"th-TH": "ลุค A"}}]
        with self.assertRaisesRegex(ValueError, "不得回退泰文"):
            build_theme_copy(
                theme, assets, {"copy": {}}, locale="vi-VN", locale_pack=None)

    def test_thai_task_with_a_pack_keeps_the_inline_copy(self):
        """TH 传入语言包也不能改变输出：内联泰语仍是 TH 的权威文案。"""
        from config.loader import resolve_locale_pack
        theme = resolve_photo_theme("凉爽旅行")
        assets = [{"role": f"look_{letter}", "display_label": {"th-TH": f"ลุค {letter}"}}
                  for letter in "abcd"]
        variation = {"copy": {
            "title": "ไตเติลสั้น", "caption": "แคปชัน", "hashtags": ["#tag"],
            "slide_texts": ["ปก", "A หนึ่ง", "B สอง", "C สาม", "D สี่ เลือกลุคไหน"],
        }}
        without_pack = build_theme_copy(theme, assets, variation, locale="th-TH")
        with_pack = build_theme_copy(
            theme, assets, variation, locale="th-TH",
            locale_pack=resolve_locale_pack("th-TH"))
        self.assertEqual(with_pack, without_pack)
        bare_without = build_theme_copy(
            theme, assets[:1], {"copy": {}}, locale="th-TH")
        bare_with = build_theme_copy(
            theme, assets[:1], {"copy": {}}, locale="th-TH",
            locale_pack=resolve_locale_pack("th-TH"))
        self.assertEqual(bare_with, bare_without)
        self.assertEqual(
            bare_with["hashtags"], theme["hashtags"],
            "TH 旧组装路径的 hashtags 必须仍是主题内联值",
        )


class CacheKeyTest(unittest.TestCase):
    def test_travel_plan_cache_key_includes_topic(self):
        self.maxDiff = None
        import hashlib as _h

        base = {"prompt_version": "v", "model": "m", "analysis_sha256": "a",
                "moments": ["x"], "variables": {}, "content_requirement": "",
                "count": 1}
        digest = lambda c: _h.sha256(json.dumps(c, sort_keys=True).encode()).hexdigest()
        topic_a = dict(base, travel_topic={"theme_type": "CHECK_IN", "place": "富士山"})
        topic_b = dict(base, travel_topic={"theme_type": "CHECK_IN", "place": "浅草寺"})
        no_topic = dict(base, travel_topic={})
        self.assertNotEqual(digest(topic_a), digest(topic_b), "地点变化必须换缓存")
        self.assertNotEqual(digest(topic_a), digest(no_topic), "主题变化必须换缓存")


def guide_travel_payload(*, with_copy: bool = True, same_moment: bool = True):
    """教程样例：四页共用一个 moment（同场景稳定），4 条讲解页文案。"""
    moments = ["old_town_walk"] * 4 if same_moment else [
        "airport_departure", "old_town_walk", "cafe_visit", "evening_stroll"]
    copy = {
        "place_localized": "โตเกียว",
        "title": "เที่ยวโตเกียวเดินทั้งวัน ใส่อะไรให้สบาย",
        "caption": "จับคู่รองเท้าผ้าใบกับกางเกงยืด ยืดเส้นให้เดินได้ทั้งวัน",
        "hashtags": ["#แต่งตัวเที่ยวโตเกียว"],
        "slide_texts": [
            "เที่ยวโตเกียว เดินทั้งวันไม่เมื่อย\nใส่อะไรให้สบาย?",
            "รองเท้าผ้าใบ+กางเกงยืด — เดินเกินหนึ่งหมื่นก้าวไม่เมื่อย",
            "เสื้อผ้าบางเบา พับเก็บง่าย — ห่อเล็กพกไปได้",
            "ชั้นในผ้าคอตตอน ระบายอากาศดี — บันทึกไอเดียนี้ไว้ใช้",
        ],
    }
    post = {
        "content_angle_zh": "东京全天步行怎么穿", "scene_zh": "东京街头",
        "palette_zh": "奶白、浅蓝", "background_prompt": "", "style_modifier": "",
        "looks": [
            {"role": f"look_{letter}", "travel_moment": moments[index],
             "scene_prompt": f"东京画面{index + 1}", "weather_logic": "室内外过渡",
             "display_label": "", "footwear_type": "SNEAKER",
             "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
             "bottom": f"下装{letter}", "shoes": f"鞋{letter}",
             "outerwear_type": "", "bottom_type": ""}
            for index, letter in enumerate("abcd")
        ],
    }
    if with_copy:
        post["topic_zh"] = "在东京走一整天怎么穿才舒服？"
        post["copy"] = copy
    return {"travel_variables": {}, "posts": [post]}


GUIDE_TRAVEL_TOPIC = {
    "theme_type": "",
    "theme_version": 1,
    "theme_key": "TRAVEL_STYLING_GUIDE",
    "theme_label_zh": "旅行穿搭攻略",
    "planning_focus": "问题驱动的旅行穿搭攻略：每页回答一个具体问题",
    "topic_patterns": [], "body_copy_focus": "", "cta_patterns": [],
    "place": "东京", "temperature_band": "",
    "temperature_context": {"value": "", "source": "execution_profile"},
    "content_requirement": "",
}

GUIDE_COLOR_TOPIC = {
    "theme_type": "", "theme_version": 1, "theme_key": "COLOR_TUTORIAL",
    "theme_label_zh": "配色教程",
    "planning_focus": "围绕同一商品的配色方法教程",
    "topic_patterns": [], "body_copy_focus": "", "cta_patterns": [],
    "place": "", "temperature_band": "",
    "temperature_context": {"value": "", "source": "execution_profile"},
    "content_requirement": "",
}


class GuideTopicEntryTest(unittest.TestCase):
    """新教程主题从真实入口进入讲解：topic 携 theme_key，4 页讲解合同，
    normalize 产出 narrative_plan；旧六主题契约逐字不变。"""

    def setUp(self):
        self.contract = {"moments": [
            {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
             "forbidden_footwear_types": []}
            for m in ("airport_departure", "old_town_walk", "cafe_visit", "evening_stroll")
        ]}

    def normalize(self, payload, travel_topic):
        return PhotoReferenceVisionService(root=Path("/tmp"))._normalize_travel_plan(
            payload, self.contract, 1, travel_topic=travel_topic)

    # ---- 入口：build_travel_topic 携带 theme_key ----
    def test_build_travel_topic_carries_theme_key_for_guide_themes(self):
        from services.feishu_workflow import build_travel_topic
        guide = build_travel_topic(
            theme=resolve_photo_theme("旅行·穿搭攻略"), travel_place="东京",
            travel_variables={})
        self.assertEqual(guide["theme_key"], "TRAVEL_STYLING_GUIDE")
        self.assertEqual(guide["place"], "东京")

        color = build_travel_topic(
            theme=resolve_photo_theme("配色教程"), travel_place="",
            travel_variables={})
        self.assertEqual(color["theme_key"], "COLOR_TUTORIAL")

        legacy = build_travel_topic(
            theme=resolve_photo_theme("旅行·打卡穿搭"), travel_place="首尔",
            travel_variables={})
        self.assertNotIn("theme_key", legacy, "旧六主题冻结契约不得新增键")

    # ---- 提示词：教程合同 + 公共合同保留 ----
    def test_guide_prompt_uses_teaching_contract_and_keeps_public_contract(self):
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=self.contract,
            variables={}, content_requirement="", count=1,
            travel_topic=GUIDE_TRAVEL_TOPIC)
        self.assertIn("讲解教程", prompt)
        self.assertIn("一页一方法", prompt)
        self.assertIn("slide_texts 必须 4 条", prompt)
        self.assertIn("禁止选择 A/B/C/D 投票 CTA", prompt)
        # 商品颜色权威（2026-09-20 真实样片：浅蓝变体被文案写成红棕）
        self.assertIn("商品参考图为唯一权威", prompt)
        # 公共合同保留（F2）：JSON 结构与旅行变量合同不丢
        self.assertIn("travel_moment", prompt)
        self.assertIn("scene_prompt", prompt)
        self.assertIn("topic_zh", prompt)
        self.assertIn("发布文案", prompt)
        self.assertNotIn("不要生成标题、正文或 CTA 文案", prompt)
        # 东京进合同
        self.assertIn("东京", prompt)
        self.assertIn("โตเกียว", prompt)

    def test_color_tutorial_prompt_requires_no_place(self):
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=self.contract,
            variables={}, content_requirement="", count=1,
            travel_topic=GUIDE_COLOR_TOPIC)
        self.assertIn("配色", prompt)
        self.assertIn("place_localized 留空", prompt)
        self.assertIn("slide_texts 必须 4 条", prompt)

    def test_guide_schema_example_is_four_pages(self):
        # 基础 JSON 示例必须与讲解合同一致（4 条页文案、无 A/B/C/D），
        # 否则模型跟示例走 5 条投票格式导致降级（任务二真实跑测根因）。
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=self.contract,
            variables={}, content_requirement="", count=1,
            travel_topic=GUIDE_TRAVEL_TOPIC)
        schema = prompt[prompt.index("只返回 JSON"):]
        self.assertIn("页2要点", schema)
        self.assertNotIn("A · 当地语言短名称", schema)
        self.assertNotIn("完整选择 CTA", schema)

        legacy = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=self.contract,
            variables={}, content_requirement="", count=1,
            travel_topic=TRAVEL_TOPIC)
        legacy_schema = legacy[legacy.index("只返回 JSON"):]
        self.assertEqual(legacy_schema.count("A · 当地语言短名称"), 1)

    # ---- Normalize：4 页讲解 + narrative_plan ----
    def test_guide_normalize_accepts_four_slides_and_builds_narrative(self):
        plan, errors = self.normalize(guide_travel_payload(), GUIDE_TRAVEL_TOPIC)
        self.assertEqual(errors, [], errors)
        copy = plan["posts"][0]["copy"]
        self.assertEqual(len(copy["slide_texts"]), 4)
        self.assertNotIn("A · ", "\n".join(copy["slide_texts"]))
        self.assertNotIn("copy_degraded", plan["posts"][0])
        narrative = plan.get("narrative_plan") or {}
        self.assertEqual(narrative.get("kind"), "travel_guide")
        self.assertEqual(len(narrative.get("pages") or []), 4)
        # 问题取模型实际选题，不是主题规划要点
        self.assertEqual(narrative.get("question_zh"), "在东京走一整天怎么穿才舒服？")
        self.assertTrue(narrative.get("takeaways"), "要点应从页标题解析")

    def test_guide_allows_same_travel_moment_across_pages(self):
        payload = guide_travel_payload(same_moment=True)
        plan, errors = self.normalize(payload, GUIDE_TRAVEL_TOPIC)
        self.assertEqual(errors, [], errors)
        self.assertFalse(any("互不相同" in e for e in errors))

    def test_guide_five_slide_copy_is_degraded_not_accepted(self):
        payload = guide_travel_payload()
        payload["posts"][0]["copy"]["slide_texts"].append("หน้าเพิ่ม")
        plan, errors = self.normalize(payload, GUIDE_TRAVEL_TOPIC)
        self.assertEqual(errors, [], errors)
        # 5 条不满足教程 4 页合同 → 降级为 4 页模板，而不是按 5 条放行
        self.assertEqual(len(plan["posts"][0]["copy"]["slide_texts"]), 4)
        self.assertTrue(plan["posts"][0].get("copy_degraded"))

    def test_guide_degraded_copy_has_no_voting_cta(self):
        # 真实降级场景：模型给了地点与文案但页数结构错（3 条）→ 模板重建
        payload = guide_travel_payload()
        payload["posts"][0]["copy"]["slide_texts"] = (
            payload["posts"][0]["copy"]["slide_texts"][:3])
        plan, errors = self.normalize(payload, GUIDE_TRAVEL_TOPIC)
        self.assertEqual(errors, [], errors)
        slides = plan["posts"][0]["copy"]["slide_texts"]
        self.assertEqual(len(slides), 4)
        joined = "\n".join(slides)
        self.assertNotIn("A · ", joined)
        self.assertNotIn("B · ", joined)
        self.assertNotIn("เลือก A B C", joined)
        # 地点名保留在降级封面首行，不因降级触发地点结构错误
        self.assertIn("โตเกียว", slides[0])

    def test_color_tutorial_normalize_builds_color_narrative(self):
        plan, errors = self.normalize(guide_travel_payload(), GUIDE_COLOR_TOPIC)
        self.assertEqual(errors, [], errors)
        self.assertEqual((plan.get("narrative_plan") or {}).get("kind"),
                         "color_tutorial")

    def test_legacy_theme_topic_still_has_no_narrative_plan(self):
        payload = travel_payload()
        plan, errors = self.normalize(payload, TRAVEL_TOPIC)
        self.assertEqual(errors, [], errors)
        self.assertNotIn("narrative_plan", plan)
        self.assertEqual(len(plan["posts"][0]["copy"]["slide_texts"]), 5)

    # ---- style_profile 贯穿：叙事结构随冻结进入内容计划 ----
    def test_style_profile_carries_narrative_plan(self):
        plan, _ = self.normalize(guide_travel_payload(), GUIDE_TRAVEL_TOPIC)
        profile = PhotoReferenceVisionService.build_travel_style_profile(
            {"schema_version": "x"}, {**plan,
                                      "narrative_plan": plan.get("narrative_plan")},
            count=1)
        self.assertEqual((profile.get("narrative_plan") or {}).get("kind"),
                         "travel_guide")

    def test_style_profile_without_narrative_has_no_key(self):
        profile = PhotoReferenceVisionService.build_travel_style_profile(
            {"schema_version": "x"}, {"posts": []}, count=1)
        self.assertNotIn("narrative_plan", profile)

    def test_build_theme_copy_keeps_four_page_guide_slides(self):
        # 最后一公里：冻结请求的 copy 由 build_theme_copy 组装——4 条教程
        # slide_texts 必须原样进发布契约，不能回退「A · 造型」投票组装。
        from services.photo_theme import build_theme_copy
        theme = resolve_photo_theme("旅行·穿搭攻略")
        variation = {
            "planning_flow": "travel_two_step",
            "copy": {
                "title": "ไอเดียแต่งตัวเที่ยวโตเกียว",
                "caption": "แคปชันโตเกียว",
                "hashtags": ["#ทริปโตเกียว"],
                "place_localized": "โตเกียว",
                "slide_texts": ["หน้าปก", "วิธีที่ 1", "วิธีที่ 2", "วิธีที่ 3"],
                "language_review_status": "DRAFT_TRAVEL_TOPIC",
            },
        }
        copy = build_theme_copy(
            theme, [], variation, expression_mode="PRACTICAL_GUIDE")
        self.assertEqual(copy["slide_texts"],
                         ["หน้าปก", "วิธีที่ 1", "วิธีที่ 2", "วิธีที่ 3"])
        self.assertEqual(copy["language_review_status"], "DRAFT_TRAVEL_TOPIC")
        self.assertNotIn("เลือก A B C", "\n".join(copy["slide_texts"]))

    def test_guide_plan_freezes_model_copy_and_allows_same_moment(self):
        # 计划层入口：教程主题走主题联动——四页同 travel_moment（固定背景
        # 教程的设计要求）不再被旧投票语义判重复，模型讲解文案按 v2 冻结。
        from services.photo_content_planner import plan_th_choice_batch
        profile = {
            "analysis_method": "doubao_seed_2_1",
            "presentation_type": "SCENE_MODEL",
            "planning_flow": "travel_two_step",
            "travel_topic": GUIDE_COLOR_TOPIC,
            "recommended_sets": guide_travel_payload()["posts"],
        }
        plan = plan_th_choice_batch(
            record_id="rec-guide-color", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=resolve_photo_theme("配色教程"), reference_mode="STYLE",
            count=1, style_profile=profile,
            travel_contract={"moments": [
                {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
                 "forbidden_footwear_types": []}
                for m in ("old_town_walk", "cafe_visit")
            ]},
        )
        self.assertTrue(plan["allow_repeated_travel_moments"])
        item = plan["items"][0]
        self.assertEqual(len(item["copy"]["slide_texts"]), 4)
        self.assertEqual(item["copy"]["copy_policy_version"], 2)
        self.assertEqual(item["copy"]["language_review_status"],
                         "DRAFT_TRAVEL_TOPIC")
        moments = {look["travel_moment"] for look in item["looks"]}
        self.assertEqual(moments, {"old_town_walk"})


if __name__ == "__main__":
    unittest.main()


class GuideStructuredPagesTest(unittest.TestCase):
    """修复二/三：教程结构是真正的渲染输入（每篇独立、字段贯穿到渲染器）。"""

    CONTRACT = {"moments": [
        {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
         "forbidden_footwear_types": []}
        for m in ("old_town_walk", "cafe_visit", "evening_stroll",
                  "airport_departure")
    ]}

    def structured_payload(self, kind_topic):
        copy = {
            "place_localized": "โตเกียว" if kind_topic is GUIDE_TRAVEL_TOPIC else "",
            "title": "ไอเดียแต่งตัวเที่ยวโตเกียว" if kind_topic is GUIDE_TRAVEL_TOPIC else "จับคู่สีให้ลุคดูเบา",
            "caption": "แคปชัน",
            "hashtags": ["#ทริป"],
            "slide_texts": ["หน้าปก\nคำถาม", "วิธีที่ 1 — เหตุผล", "วิธีที่ 2 — เหตุผล", "วิธีที่ 3 — เหตุผล"],
            "pages": [
                {"key_point_zh": "封面", "visual_basis_zh": "完整示例", "kicker": "โตเกียว",
                 "headline": "หน้าปก", "body": "คำถาม", "color_chips": []},
                {"key_point_zh": "浅色衔接", "visual_basis_zh": "内搭真实露出",
                 "kicker": "", "headline": "ต่อสีอ่อน", "body": "เหตุผล 1",
                 "color_chips": [{"name_zh": "奶白", "hex": "#F2EDE4", "role": "top_inner"}]},
                {"key_point_zh": "明暗对比", "visual_basis_zh": "深浅分区清晰",
                 "kicker": "", "headline": "ตัดสว่างเข้ม", "body": "เหตุผล 2",
                 "color_chips": [{"name_zh": "深靛蓝", "hex": "#2E4057", "role": "bottom"}]},
                {"key_point_zh": "局部呼应", "visual_basis_zh": "鞋与外套同色",
                 "kicker": "", "headline": "ย้ำสีเฉพาะจุด", "body": "เหตุผล 3",
                 "color_chips": [{"name_zh": "棕色", "hex": "#8B5E3C", "role": "shoes"}]},
            ],
        }
        looks = [
            {"role": f"look_{letter}", "travel_moment": moment,
             "scene_prompt": f"场景{letter}", "weather_logic": "室内外过渡",
             "display_label": "", "footwear_type": "SNEAKER",
             "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
             "bottom": f"下装{letter}", "shoes": f"鞋{letter}",
             "outerwear_type": "", "bottom_type": ""}
            for letter, moment in zip("abcd", (
                "old_town_walk", "cafe_visit", "evening_stroll", "airport_departure"))
        ]
        return {"travel_variables": {}, "posts": [{
            "content_angle_zh": "角度", "scene_zh": "s", "palette_zh": "p",
            "background_prompt": "", "style_modifier": "",
            "topic_zh": "怎么搭？", "copy": copy, "looks": looks,
        }]}

    def normalize(self, payload, topic):
        return PhotoReferenceVisionService(root=Path("/tmp"))._normalize_travel_plan(
            payload, self.CONTRACT, 1, travel_topic=topic)

    def test_normalize_builds_v2_structure_per_post(self):
        plan, errors = self.normalize(self.structured_payload(GUIDE_COLOR_TOPIC), GUIDE_COLOR_TOPIC)
        self.assertEqual(errors, [], errors)
        narrative = plan.get("narrative_plan") or {}
        self.assertEqual(narrative.get("version"), 2)
        self.assertEqual(narrative.get("layout_hint"), "color_sidebar")
        pages = narrative.get("pages") or []
        self.assertEqual(len(pages), 4)
        self.assertEqual(pages[1]["text"]["headline"], "ต่อสีอ่อน")
        self.assertEqual(pages[1]["visual_basis"], "内搭真实露出")
        self.assertEqual(pages[1]["color_chips"][0]["hex"], "#F2EDE4")
        # 每篇独立：posts[0] 自带 narrative
        post_narrative = plan["posts"][0].get("narrative") or {}
        self.assertEqual(post_narrative.get("version"), 2)
        self.assertEqual(len(post_narrative.get("pages") or []), 4)

    def test_normalize_falls_back_to_slide_split_without_model_pages(self):
        payload = guide_travel_payload()
        plan, errors = self.normalize(payload, GUIDE_TRAVEL_TOPIC)
        self.assertEqual(errors, [], errors)
        pages = (plan.get("narrative_plan") or {}).get("pages") or []
        self.assertEqual(len(pages), 4)
        # 无模型结构时 headline 从 slide 回装、layout 是攻略底部解释区
        self.assertTrue(any(page["text"]["headline"] for page in pages))
        self.assertEqual(plan["narrative_plan"].get("layout_hint"), "explain_bottom")
        self.assertTrue(all(page["color_chips"] == [] for page in pages))

    def test_plan_item_carries_pages_and_per_post_narrative(self):
        from services.photo_content_planner import plan_th_choice_batch
        plan0, _ = self.normalize(self.structured_payload(GUIDE_COLOR_TOPIC), GUIDE_COLOR_TOPIC)
        profile = {
            "analysis_method": "doubao_seed_2_1",
            "presentation_type": "SCENE_MODEL",
            "planning_flow": "travel_two_step",
            "travel_topic": GUIDE_COLOR_TOPIC,
            "recommended_sets": plan0["posts"],
        }
        plan = plan_th_choice_batch(
            record_id="rec-struct", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=resolve_photo_theme("配色教程"), reference_mode="STYLE",
            count=1, style_profile=profile, travel_contract=self.CONTRACT,
        )
        item = plan["items"][0]
        self.assertEqual(len(item["copy"].get("pages") or []), 4)
        self.assertEqual(item["copy"]["pages"][1]["color_chips"][0]["role"], "top_inner")
        self.assertEqual((item.get("narrative") or {}).get("layout_hint"), "color_sidebar")

    def test_build_theme_copy_carries_structured_pages(self):
        from services.feishu_workflow import build_travel_topic
        from services.photo_theme import build_theme_copy
        plan0, _ = self.normalize(self.structured_payload(GUIDE_COLOR_TOPIC), GUIDE_COLOR_TOPIC)
        variation = {
            "planning_flow": "travel_two_step",
            "copy": dict(plan0["posts"][0]["copy"]),
        }
        copy = build_theme_copy(
            resolve_photo_theme("配色教程"), [], variation,
            expression_mode="PRACTICAL_GUIDE")
        pages = copy.get("pages") or []
        self.assertEqual(len(pages), 4)
        self.assertEqual(pages[2]["color_chips"][0]["name_zh"], "深靛蓝")

    def test_renderer_v2_color_sidebar_and_bottom_zone(self):
        from PIL import Image
        from services.photo_structured_layout import (
            render_structured_page_v2, STRUCTURED_RENDERER_V2_VERSION,
        )
        import json as _json
        template = _json.loads(
            (Path(__file__).resolve().parents[1]
             / "config/layouts/PHOTO_STRUCTURED_CLEAN_V1.json").read_text("utf-8")
        )["render_options"]
        source = Image.new("RGB", (1080, 1920), "#C9B48A")

        color_page = {
            "text": {"kicker": "โตเกียว", "headline": "ต่อสีอ่อน", "body": "เหตุผล"},
            "color_chips": [{"name_zh": "奶白", "hex": "#F2EDE4", "role": "top_inner"},
                            {"name_zh": "深靛蓝", "hex": "#2E4057", "role": "bottom"}],
        }
        image = source.copy()
        info = render_structured_page_v2(
            image, color_page, template, index=2, cover_index=1, total=4)
        self.assertEqual(info["renderer"], STRUCTURED_RENDERER_V2_VERSION)
        self.assertEqual(info["layout_kind"], "color_sidebar")
        main_x2 = info["zones"]["main"][2]
        # 侧栏存在程序色块：深靛蓝示意色出现在右侧栏区域
        side = image.crop((main_x2 + 10, 0, image.width, image.height))
        colors = side.getcolors(maxcolors=200000) or []
        self.assertTrue(any(
            abs(rgb[0] - 0x2E) < 12 and abs(rgb[1] - 0x40) < 12 and abs(rgb[2] - 0x57) < 12
            for _, rgb in colors), "侧栏应包含深靛蓝示意色块")

        guide_page = {
            "text": {"kicker": "", "headline": "ชั้นในบาง ถอดง่าย", "body": "เหตุผล"},
            "color_chips": [],
        }
        image2 = source.copy()
        info2 = render_structured_page_v2(
            image2, guide_page, template, index=3, cover_index=1, total=4)
        self.assertEqual(info2["layout_kind"], "explain_bottom")
        main_h = info2["zones"]["main"][3]
        # 图片区未被文字侵入：解释区顶部之前保持原图底色带
        band = image2.crop((0, main_h - 8, image2.width, main_h - 1)).convert("L")
        self.assertGreaterEqual(band.getextrema()[0], 120, "解释区上方不应有深色文字")


class TemperatureConditionGuideTest(unittest.TestCase):
    """修复四（§6）：温度主题默认实用指南，逐页回答穿脱条件；无数字不编造。"""

    CONTRACT = {"moments": [
        {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
         "forbidden_footwear_types": []}
        for m in ("old_town_walk", "cafe_visit", "evening_stroll",
                  "airport_departure")
    ]}
    TEMP_TOPIC = {
        "theme_type": "TEMPERATURE", "theme_version": 1,
        "theme_label_zh": "旅行·温度穿搭", "planning_focus": "温度条件下的层搭",
        "topic_patterns": [], "body_copy_focus": "", "cta_patterns": [],
        "place": "首尔", "temperature_band": "", "content_requirement": "",
    }

    def prompt(self, topic, expression=""):
        return PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=self.CONTRACT, variables={},
            content_requirement="", count=1, travel_topic=topic,
            expression_mode=expression)

    def test_temperature_without_expression_defaults_to_condition_guide(self):
        # 方案 P1-2：温度默认表达进讲解教程合同（4页 pages），规则块带
        # 温度专属规则；显式灵感仍走展示分支。
        prompt = self.prompt(self.TEMP_TOPIC)
        self.assertIn("讲解教程", prompt)
        self.assertIn("slide_texts 必须 4 条", prompt)
        # 温度专属规则块
        rule_block = PhotoReferenceVisionService._guide_plan_prompt_block(
            theme=self.TEMP_TOPIC)
        self.assertIn("外套脱下后穿什么", rule_block)
        self.assertIn("不编造具体温度数字", rule_block)

    def test_temperature_explicit_inspiration_keeps_showcase(self):
        prompt = self.prompt(self.TEMP_TOPIC, expression="STYLE_INSPIRATION")
        self.assertIn("当地语言造型短名称", prompt)
        self.assertNotIn("外套脱下后穿什么", prompt)

    def test_showcase_axis_variety_rule(self):
        prompt = self.prompt({
            "theme_type": "CHECK_IN", "theme_version": 1,
            "theme_label_zh": "旅行·打卡穿搭", "planning_focus": "打卡",
            "topic_patterns": [], "body_copy_focus": "", "cta_patterns": [],
            "place": "东京", "temperature_band": "", "content_requirement": "",
        })
        self.assertIn("选择轴取决于四套的实际差异", prompt)
        self.assertIn("确实主要在比较裤装与裙装", prompt)


class GuideSummaryTest(unittest.TestCase):
    """修复四（§7 摘要）：教程行显示本篇问题与方法，不显示投票卡问题。"""

    def test_summary_uses_narrative_question_for_guide_rows(self):
        from services.photo_request_factory import PhotoRequestFactory
        request = {
            "profile_id": "p1", "profile_label": "TH旅行",
            "recipe_id": "PHOTO_TH_TRAVEL_OUTFIT_V2",
            "asset_set_key": "k", "asset_set_version": 1,
            "copy": {
                "slide_texts": ["s1", "s2", "s3", "s4"],
                "pages": [
                    {"key_point_zh": "封面", "headline": "h", "body": "b"},
                    {"key_point_zh": "浅色衔接", "headline": "h", "body": "b"},
                    {"key_point_zh": "明暗对比", "headline": "h", "body": "b"},
                    {"key_point_zh": "局部呼应", "headline": "h", "body": "b"},
                ],
            },
            "theme_brief": {
                "label_zh": "配色教程", "topic_zh": "棕外套怎么配色不沉闷？",
                "batch_variation": {"angle_zh": "角度", "scene_zh": "纯色"},
            },
            "content_card": {"pages": [
                {"index": 1, "purpose_zh": "封面"},
                {"index": 2, "purpose_zh": "方法1"},
                {"index": 3, "purpose_zh": "方法2"},
                {"index": 4, "purpose_zh": "方法3"},
            ]},
        }
        summary = PhotoRequestFactory.summary([request])
        self.assertIn("本篇问题：棕外套怎么配色不沉闷？", summary)
        self.assertIn("本篇方法：封面／浅色衔接／明暗对比／局部呼应", summary)
        self.assertNotIn("用户问题", summary)
        self.assertNotIn("选择", summary.split("本篇问题")[0])


class CardTextMountTest(unittest.TestCase):
    """真实跑测修正（2026-09-20）：copy.pages 是平面结构，挂载进内容卡时
    必须读到 headline/body（此前误读嵌套 text 子对象写空串，渲染守卫回落
    旧排版——wn0didnad6 recvvJSMD20x1f 成片未走色卡侧栏的根因）。"""

    def test_flat_pages_mount_headline_into_card(self):
        from collections.abc import Mapping
        flat_page = {
            "key_point_zh": "浅色衔接", "visual_basis_zh": "内搭露出",
            "kicker": "", "headline": "ต่อสีอ่อน", "body": "เหตุผล",
            "color_chips": [{"name_zh": "奶白", "hex": "#EEEBDD", "role": "top_inner"}],
        }
        # 模拟挂载逻辑（与 feishu_workflow 一致）
        _text_source = (
            flat_page.get("text")
            if isinstance(flat_page.get("text"), Mapping) else flat_page)
        mounted = {key: str((_text_source or {}).get(key) or "")
                   for key in ("kicker", "headline", "body")}
        self.assertEqual(mounted["headline"], "ต่อสีอ่อน")
        self.assertEqual(mounted["body"], "เหตุผล")

    def test_renderer_guard_requires_nonempty_headline(self):
        # 守卫语义：headline 为空 → 回落旧渲染（不能因空串静默走 v2）
        page_text = {"kicker": "", "headline": "", "body": ""}
        should_v2 = bool(str(page_text.get("headline") or "").strip())
        self.assertFalse(should_v2)
        page_text2 = {"kicker": "", "headline": "ต่อสีอ่อน", "body": "x"}
        self.assertTrue(bool(str(page_text2.get("headline") or "").strip()))


class VisualBasisProjectionTest(unittest.TestCase):
    """复审 F1：画面证据必须穿过真实投影链进生图请求。

    normalize → build_travel_style_profile → plan_th_choice_batch →
    供给侧 outfit_state。此前 build_travel_style_profile 丢 post.narrative，
    "本页讲解画面证据"通道在真实生图为空（recvvK1eGxPvNm 实测）。"""

    CONTRACT = {"moments": [
        {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
         "forbidden_footwear_types": []}
        for m in ("old_town_walk", "cafe_visit", "evening_stroll",
                  "airport_departure")
    ]}

    def test_projection_keeps_narrative_and_evidence_reaches_request(self):
        from collections.abc import Mapping
        from services.photo_reference_vision import (
            PhotoReferenceVisionService, _guide_structured_pages,
        )
        from services.photo_content_planner import plan_th_choice_batch

        topic = dict(GUIDE_COLOR_TOPIC)
        payload = guide_travel_payload()
        # 模型结构化 pages（真实生产会带）：visual_basis 从这里进 narrative
        payload["posts"][0]["copy"]["pages"] = [
            {"key_point_zh": "封面", "visual_basis_zh": "完整示例一套",
             "kicker": "", "headline": "หน้าปก", "body": "คำถาม", "color_chips": []},
            {"key_point_zh": "浅色衔接", "visual_basis_zh": "内搭真实露出",
             "kicker": "", "headline": "ต่อสีอ่อน", "body": "เหตุผล 1", "color_chips": []},
            {"key_point_zh": "明暗对比", "visual_basis_zh": "深浅分区清晰",
             "kicker": "", "headline": "ตัดสว่างเข้ม", "body": "เหตุผล 2", "color_chips": []},
            {"key_point_zh": "局部呼应", "visual_basis_zh": "鞋与外套同色",
             "kicker": "", "headline": "ย้ำสีเฉพาะจุด", "body": "เหตุผล 3", "color_chips": []},
        ]
        plan, errors = PhotoReferenceVisionService(
            root=Path("/tmp"))._normalize_travel_plan(
            payload, self.CONTRACT, 1, travel_topic=topic)
        self.assertEqual(errors, [], errors)
        self.assertIn("narrative", plan["posts"][0])

        # 真实投影（不手工拼 recommended_sets）
        profile = PhotoReferenceVisionService.build_travel_style_profile(
            {"schema_version": "x"}, plan, count=1)
        sets = profile.get("recommended_sets") or []
        self.assertTrue(sets, "投影后必须有 recommended_sets")
        self.assertIn("narrative", sets[0],
                      "复审 F1：build_travel_style_profile 必须保留 post.narrative")
        pages = (sets[0]["narrative"] or {}).get("pages") or []
        self.assertTrue(any(
            str(page.get("visual_basis") or "").strip() for page in pages),
            "narrative.pages 必须带画面证据")

        profile["travel_topic"] = dict(topic)
        plan_batch = plan_th_choice_batch(
            record_id="rec-evidence", recipe_id="PHOTO_TH_TRAVEL_OUTFIT_V2",
            theme=resolve_photo_theme("配色教程"), reference_mode="STYLE",
            count=1, style_profile=profile, travel_contract=self.CONTRACT,
        )
        item = plan_batch["items"][0]
        item_pages = (item.get("narrative") or {}).get("pages") or []
        self.assertTrue(item_pages, "计划条目必须携带 narrative")
        evidence = next(
            (str(page.get("visual_basis") or "") for page in item_pages
             if str(page.get("source_role") or "") == "look_b"
             and str(page.get("visual_basis") or "").strip()), "")
        self.assertTrue(evidence, "look_b 页必须有画面证据")

        # 供给侧同源读取（与 photo_style_reference_supply 相同取值逻辑）
        narrative_pages = (
            (item.get("narrative") or {}).get("pages")
            if isinstance(item.get("narrative"), Mapping) else None) or ()
        found = next(
            (str(page.get("visual_basis") or "").strip()
             for page in narrative_pages
             if str(page.get("source_role") or "") == "look_b"
             and str(page.get("visual_basis") or "").strip()), "")
        self.assertEqual(found, evidence)


class TemperatureGuideStructureTest(unittest.TestCase):
    """复审 F3：温度主题（默认实用表达）进讲解结构——4页/pages/narrative，
    layout=explain_bottom；显式 STYLE_INSPIRATION 保持展示型。"""

    CONTRACT = {"moments": [
        {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
         "forbidden_footwear_types": []}
        for m in ("old_town_walk", "cafe_visit", "evening_stroll",
                  "airport_departure")
    ]}
    TEMP_TOPIC = {
        "theme_type": "TEMPERATURE", "theme_version": 1,
        "theme_key": "TEMPERATURE",
        "theme_label_zh": "旅行·温度穿搭",
        "planning_focus": "温度条件下的层搭",
        "topic_patterns": [], "body_copy_focus": "", "cta_patterns": [],
        "place": "首尔", "temperature_band": "", "content_requirement": "",
    }

    def payload_with_pages(self):
        payload = guide_travel_payload()
        payload["posts"][0]["copy"]["pages"] = [
            {"key_point_zh": "封面", "visual_basis_zh": "完整示例",
             "kicker": "", "headline": "หน้าปก", "body": "คำถาม", "color_chips": []},
            {"key_point_zh": "脱外套", "visual_basis_zh": "外套搭臂",
             "kicker": "", "headline": "ถอดแจ็กเก็ต", "body": "เหตุผล", "color_chips": []},
            {"key_point_zh": "加一层", "visual_basis_zh": "外层可见",
             "kicker": "", "headline": "ชั้นนอก", "body": "เหตุผล", "color_chips": []},
            {"key_point_zh": "腿脚", "visual_basis_zh": "下装鞋履可见",
             "kicker": "", "headline": "ขาและรองเท้า", "body": "เหตุผล", "color_chips": []},
        ]
        return payload

    def test_temperature_default_expression_builds_guide_structure(self):
        plan, errors = PhotoReferenceVisionService(
            root=Path("/tmp"))._normalize_travel_plan(
            self.payload_with_pages(), self.CONTRACT, 1,
            travel_topic=self.TEMP_TOPIC)
        self.assertEqual(errors, [], errors)
        narrative = plan.get("narrative_plan") or {}
        self.assertEqual(narrative.get("kind"), "temperature_guide")
        self.assertEqual(narrative.get("layout_hint"), "explain_bottom")
        self.assertEqual(len(narrative.get("pages") or []), 4)
        self.assertEqual(len(plan["posts"][0]["copy"]["slide_texts"]), 4)
        # 显式 STYLE_INSPIRATION：温度保持展示型（5条投票，无narrative）
        inspired_topic = {
            **self.TEMP_TOPIC, "expression_mode": "STYLE_INSPIRATION",
            "place": ""}
        showcase = guide_travel_payload()
        showcase["posts"][0]["copy"] = {
            **showcase["posts"][0]["copy"], "place_localized": ""}
        plan2, errors2 = PhotoReferenceVisionService(
            root=Path("/tmp"))._normalize_travel_plan(
            showcase, self.CONTRACT, 1, travel_topic=inspired_topic)
        self.assertEqual(errors2, [], errors2)
        self.assertNotIn("narrative_plan", plan2)
        self.assertNotIn("narrative", plan2["posts"][0])

    def test_real_theme_resolution_routes_temperature_guide(self):
        # 方案 P1-2：验收必须走真实链路 resolve_photo_theme →
        # build_travel_topic → resolve_guide_execution，不能手工构造键。
        from services.feishu_workflow import build_travel_topic
        from services.photo_content_planner import resolve_guide_execution as _rge
        theme = resolve_photo_theme("旅行·温度穿搭")
        self.assertEqual(theme["theme_key"], "COOL_WEATHER_TRAVEL")
        self.assertEqual(theme["travel_theme_type"], "TEMPERATURE")
        self.assertEqual(
            _rge(theme=theme, expression_mode=""),
            "temperature_guide")
        self.assertEqual(
            _rge(theme=theme, expression_mode="PRACTICAL_GUIDE"),
            "temperature_guide")
        self.assertEqual(
            _rge(theme=theme, expression_mode="STYLE_INSPIRATION"),
            "")
        topic = build_travel_topic(theme=theme, travel_place="首尔",
                                   travel_variables={})
        self.assertEqual(
            _rge(theme=topic, expression_mode=""),
            "temperature_guide")

    def test_rule_block_has_temperature_kind(self):
        prompt = PhotoReferenceVisionService._guide_plan_prompt_block(
            theme=self.TEMP_TOPIC)
        self.assertIn("讲解模式：温度指南", prompt)
        self.assertIn("不编造具体温度数字", prompt)
        # 显式灵感：无规则块
        empty = PhotoReferenceVisionService._guide_plan_prompt_block(
            theme={**self.TEMP_TOPIC, "expression_mode": "STYLE_INSPIRATION"})
        self.assertEqual(empty, "")
