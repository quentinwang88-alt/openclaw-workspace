"""Phase 1/2 专项：中文全文翻译服务 与 视觉预设解析。"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.photo_reference_vision import PhotoReferenceVisionService
from services.copy_translation import (
    CopyTranslationError, CopyTranslationService, compose_full_copy_zh,
    copy_fingerprint,
)
from services.visual_preset import (
    VisualPresetError, entry_default_preset_id, resolve_preset_snapshot,
    resolve_visual_preset, visual_preset_fingerprint, visual_preset_options,
)

COPY = {
    "title": "เชียงใหม่ ใส่อะไรดี",
    "caption": "4 ลุคสำหรับเดินเมืองเก่า",
    "hashtags": ["#เชียงใหม่", "#ootd"],
    "slide_texts": ["เชียงใหม่\nเดินเมืองเก่า", "A · ลุคอบอุ่น — กันลม",
                    "B · ลุคบางเบา", "C · ลุคยืด", "D · ลุคคลาสสิก\nเซฟไว้เลย"],
}


class FakeClient:
    def __init__(self, payload=None, error=None):
        self.payload = payload or {
            "title_zh": "清迈怎么穿",
            "caption_zh": "逛老城的四套搭配",
            "hashtags_zh": ["#เชียงใหม่ → 清迈", "#ootd → 穿搭分享"],
            "slides_zh": ["清迈/逛老城", "A · 保暖造型 — 防风",
                          "B · 轻薄造型", "C · 弹性造型", "D · 经典造型/收藏起来"],
        }
        self.error = error
        self.calls = 0

    def chat_with_multiple_images(self, paths, prompt, max_tokens=None):
        self.calls += 1
        if self.error:
            raise self.error
        return {"choices": [{"message": {"content": json.dumps(self.payload, ensure_ascii=False)}}]}


class CopyTranslationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def service(self, client):
        return CopyTranslationService(root=self.root, client=client)

    def test_translate_and_cache(self):
        client = FakeClient()
        first = self.service(client).translate(COPY)
        self.assertEqual(first["title_zh"], "清迈怎么穿")
        self.assertEqual(len(first["slides_zh"]), 5)
        second = self.service(FakeClient()).translate(COPY)  # 新实例走缓存
        self.assertEqual(second["title_zh"], "清迈怎么穿")
        self.assertEqual(client.calls, 1)  # 只真调一次

    def test_revision_changes_fingerprint_and_retranslates(self):
        client = FakeClient()
        self.service(client).translate(COPY)
        revised = {**COPY, "caption": "แคปชันใหม่"}
        self.assertNotEqual(copy_fingerprint(revised), copy_fingerprint(COPY))
        self.service(client).translate(revised)
        self.assertEqual(client.calls, 2)

    def test_failure_is_retryable_and_page_count_enforced(self):
        with self.assertRaises(CopyTranslationError):
            self.service(FakeClient(error=RuntimeError("network"))).translate(COPY)
        bad = FakeClient(payload={"title_zh": "t", "caption_zh": "c",
                                  "hashtags_zh": ["#x → y"], "slides_zh": ["a", "b"]})
        with self.assertRaisesRegex(CopyTranslationError, "页数"):
            self.service(bad).translate(COPY)

    def test_compose_format(self):
        result = self.service(FakeClient()).translate(COPY)
        text = compose_full_copy_zh(result)
        self.assertIn("标题：清迈怎么穿", text)
        self.assertIn("封面：清迈/逛老城", text)
        self.assertIn("第 5 页：D · 经典造型/收藏起来", text)
        multi = compose_full_copy_zh(result, set_index=2, total_sets=3)
        self.assertTrue(multi.startswith("【第 2 套】"))


class VisualPresetTest(unittest.TestCase):
    def test_resolve_and_options(self):
        self.assertEqual(
            [resolve_visual_preset(name)["preset_id"] for name in visual_preset_options()],
            ["VP_TRAVEL_SCENE_V1", "VP_CLEAN_INDOOR_V1", "VP_SOLID_COLOR_V1"])
        with self.assertRaises(VisualPresetError):
            resolve_visual_preset("不存在的预设")

    def test_snapshot_priority_and_compat(self):
        travel = resolve_visual_preset("旅行场景穿搭")
        indoor = resolve_visual_preset("清爽室内穿搭")
        # 任务选择 > 账号默认 > 入口默认（入口默认只对账号绑定行生效）。
        snap = resolve_preset_snapshot(
            task_value="清爽室内穿搭", account_default="纯色搭配解析",
            entry_preset_id=entry_default_preset_id("native_photo_style_plan_v1"),
            entry_default_allowed=True)
        self.assertEqual((snap["preset_id"], snap["source"]),
                         ("VP_CLEAN_INDOOR_V1", "task"))
        snap = resolve_preset_snapshot(
            task_value="", account_default="纯色搭配解析",
            entry_preset_id="VP_TRAVEL_SCENE_V1", entry_default_allowed=True)
        self.assertEqual((snap["preset_id"], snap["source"]),
                         ("VP_SOLID_COLOR_V1", "account_default"))
        snap = resolve_preset_snapshot(
            task_value="", account_default="",
            entry_preset_id="VP_TRAVEL_SCENE_V1", entry_default_allowed=True)
        self.assertEqual((snap["preset_id"], snap["source"]),
                         ("VP_TRAVEL_SCENE_V1", "entry_default"))
        # 无绑定旧行：三层缺省时返回 None；入口默认不允许生效。
        self.assertIsNone(resolve_preset_snapshot(
            task_value="", account_default="", entry_preset_id="",
            entry_default_allowed=True))
        self.assertIsNone(resolve_preset_snapshot(
            task_value="", account_default="",
            entry_preset_id="VP_TRAVEL_SCENE_V1", entry_default_allowed=False))
        self.assertEqual(travel["background"]["mode"], "scene")
        self.assertEqual(indoor["background"]["mode"], "fixed")
        self.assertTrue(visual_preset_fingerprint(indoor))


if __name__ == "__main__":
    unittest.main()


class FixedBackdropPromptTest(unittest.TestCase):
    CONTRACT = {"moments": [
        {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
         "forbidden_footwear_types": []}
        for m in ("old_town_walk", "cafe_visit", "evening_stroll", "shopping_day")
    ]}
    BACKDROP = {"mode": "fixed",
                "fixed_scene_zh": "浅色墙面与木色地板的简洁室内，窗边自然柔光",
                "stability_zh": "四页同一室内气质，只变机位与姿势"}

    def test_prompt_fixed_backdrop_block_conditional(self):
        base = dict(analysis={}, travel_contract=self.CONTRACT, variables={},
                    content_requirement="", count=1)
        with_block = PhotoReferenceVisionService._travel_plan_prompt(
            **{**base, "fixed_background": self.BACKDROP})
        self.assertIn("【固定背景合同（优先于场景规则）】", with_block)
        self.assertIn("浅色墙面与木色地板", with_block)
        self.assertIn("不是画面要求", with_block)
        legacy = PhotoReferenceVisionService._travel_plan_prompt(**base)
        self.assertNotIn("固定背景合同", legacy)
        scene_mode = PhotoReferenceVisionService._travel_plan_prompt(
            **{**base, "fixed_background": {"mode": "scene"}})
        self.assertNotIn("固定背景合同", scene_mode)


