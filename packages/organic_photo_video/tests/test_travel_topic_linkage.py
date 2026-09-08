"""Travel theme+place linkage tests (six topic themes, copy survival)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

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

    def test_missing_copy_degrades_to_theme_fallback_not_error(self):
        # 方案 §7.3：文案结构无效→同主题模板降级，不阻塞生产。
        payload = travel_payload(with_copy=False)
        topic = dict(TRAVEL_TOPIC, thai_fallback={
            "title": "4 ลุคเข้ากับบรรยากาศทริป", "cover": "ลุคเข้ากับสถานที่\nA B C หรือ D?",
            "caption": "แต่งตัวให้เข้ากับบรรยากาศ", "hashtags": ["#OOTD"],
            "cta": "เลือกลุคไหนดี?",
        })
        plan, errors = self.normalize(payload, topic)
        self.assertEqual(errors, [])
        post = plan["posts"][0]
        self.assertEqual(len(post["copy"]["slide_texts"]), 5)
        self.assertTrue(all(post["copy"]["slide_texts"]))
        self.assertEqual(post["copy"]["language_review_status"], "DRAFT_FALLBACK")
        self.assertTrue(post.get("copy_degraded"))

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

        self.assertEqual(errors, [])
        post = plan["posts"][0]
        self.assertEqual(post["copy"]["language_review_status"], "DRAFT_FALLBACK")
        self.assertNotIn("湖", "".join(post["copy"]["slide_texts"]))


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
                "title": "ไตเติลจากโมเดล", "caption": "แคปชันจากโมเดล",
                "hashtags": ["#tag"],
                "slide_texts": ["ปก", "A หนึ่ง", "B สอง", "C สาม", "D สี่ เลือกลุคไหน"],
                "language_review_status": "DRAFT_TRAVEL_TOPIC",
            },
        }
        result = build_theme_copy(theme, assets, variation)
        self.assertEqual(result["slide_texts"], variation["copy"]["slide_texts"])
        self.assertEqual(result["title"], "ไตเติลจากโมเดล")
        self.assertEqual(result["language_review_status"], "DRAFT_TRAVEL_TOPIC")

    def test_missing_topic_slides_fall_back_to_legacy_assembly(self):
        theme = resolve_photo_theme("旅行·环境协调")
        assets = [{"role": "look_a", "display_label": {"th-TH": "ลุค A"}}]
        result = build_theme_copy(theme, assets[:1], {"copy": {}})
        self.assertEqual(len(result["slide_texts"]), 5)
        self.assertEqual(result["language_review_status"], "production_theme_profile")

    def test_partial_slides_do_not_leak(self):
        theme = resolve_photo_theme("旅行·环境协调")
        variation = {"copy": {"slide_texts": ["only", "two"]}}
        result = build_theme_copy(theme, [], variation)
        self.assertEqual(len(result["slide_texts"]), 5)


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


if __name__ == "__main__":
    unittest.main()
