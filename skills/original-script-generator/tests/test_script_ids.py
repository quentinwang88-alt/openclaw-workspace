#!/usr/bin/env python3
"""统一脚本 ID 测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.script_ids import (  # noqa: E402
    build_script_id_from_context,
    build_script_id_from_fields,
    extract_unified_id_from_text,
    is_valid_unified_id,
    parse_slot_from_logical_name,
    prepend_unified_id,
)
from core.script_renderer import render_internal_script, render_script  # noqa: E402


class ScriptIdsTest(unittest.TestCase):
    def test_build_script_id_from_context_uses_task_no_and_parent_slot(self) -> None:
        context = {
            "task_no": "003",
            "product_code": "BH001",
            "parent_slot_1": "M1",
            "parent_slot_2": "M2",
        }
        self.assertEqual(build_script_id_from_context(context, 1, None, record_id="rec123"), "003_M1_M")
        self.assertEqual(build_script_id_from_context(context, 2, 3, record_id="rec123"), "003_M2_V3")

    def test_build_script_id_from_fields_supports_fallback_parent_slot(self) -> None:
        fields = {
            "任务编号": "003",
            "产品编码": "BH001",
        }
        mapping = {
            "task_no": "任务编号",
            "product_code": "产品编码",
            "product_id": None,
            "parent_slot_1": None,
            "parent_slot_2": None,
            "parent_slot_3": None,
            "parent_slot_4": None,
        }
        self.assertEqual(build_script_id_from_fields(fields, mapping, 1, None, record_id="rec123"), "003_M1_M")
        self.assertEqual(build_script_id_from_fields(fields, mapping, 4, 5, record_id="rec123"), "003_M4_V5")

    def test_parse_slot_from_logical_name(self) -> None:
        self.assertEqual(parse_slot_from_logical_name("script_s1"), (1, None))
        self.assertEqual(parse_slot_from_logical_name("video_prompt_s3"), (3, None))
        self.assertEqual(parse_slot_from_logical_name("script_2_variant_4"), (2, 4))

    def test_unified_id_helpers_support_legacy_and_structured_values(self) -> None:
        self.assertTrue(is_valid_unified_id("123456"))
        self.assertTrue(is_valid_unified_id("003_M1_V2"))
        text = prepend_unified_id("这里是正文", "003_M1_V2")
        self.assertEqual(extract_unified_id_from_text(text), "003_M1_V2")

    def test_render_internal_script_uses_script_id_header(self) -> None:
        rendered = render_internal_script(
            {
                "content_id": "003_M1_M",
                "script_positioning": {},
                "storyboard": [],
                "execution_constraints": {},
                "negative_constraints": [],
            }
        )
        self.assertIn("【脚本ID】", rendered)
        self.assertIn("003_M1_M", rendered)

    def test_render_script_outputs_seedance_six_section_format(self) -> None:
        rendered = render_script(
            {
                "content_id": "003_M1_M",
                "storyboard": [
                    {
                        "duration": "2.5s",
                        "shot_content": "手提双只耳饰入镜，先看小环再看下方吊坠层次。",
                        "person_action": "仅手部稳定提起，不遮挡结构。",
                        "voiceover_text_target_language": "อย่าเพิ่งคิดว่าเป็นห่วงธรรมดา",
                        "spoken_line_task": "hook",
                        "style_note": "自然软光，不让高光吃掉细节。",
                        "anchor_reference": "小环→透明心形→双翼→流苏",
                    },
                    {
                        "duration": "3s",
                        "shot_content": "半脸耳部佩戴近景，耳饰自然下垂。",
                        "person_action": "轻转头一次，保持耳部无遮挡。",
                        "voiceover_text_target_language": "ใส่แล้วหน้าดูเด่นขึ้นพอดี",
                        "spoken_line_task": "decision",
                        "style_note": "耳部全程无遮挡。",
                        "anchor_reference": "耳部佩戴结果",
                    },
                ],
                "execution_constraints": {
                    "visual_style": "家中窗边自然软光，奶油白背景。",
                    "person_constraints": "泰国日常感女生，至少一侧耳部完整露出。",
                    "styling_constraints": "低饱和纯色上衣，领口干净利落。",
                    "tone_completion_constraints": "轻分享，不做强推销。",
                    "scene_constraints": "限定在窗边近距离分享场景。",
                    "emotion_progression_constraints": "严格执行轻判断→轻发现→轻认同，不做夸张笑容或大动作。",
                    "camera_focus": "按小环→透明心形→双翼→流苏的顺序读取。",
                    "product_priority_principle": "优先保证小环、透明心形、双翼和流苏连续可读。",
                    "realism_principle": "金色保持真实暖调，结构从上到下清楚。",
                },
                "negative_constraints": ["不要让头发遮住耳部。", "不要使用强滤镜。"],
            }
        )
        self.assertTrue(rendered.startswith("【整体】"))
        self.assertIn("脚本ID:003_M1_M", rendered)
        self.assertIn("【商品】", rendered)
        self.assertIn("【镜头1|3s|hook】", rendered)
        self.assertIn("口播:无", render_script({"storyboard": [{"duration": "3s"}], "execution_constraints": {}, "negative_constraints": []}))
        self.assertIn("【情绪】", rendered)
        self.assertIn("【节奏】", rendered)
        self.assertIn("【禁止】", rendered)
        self.assertNotIn("【脚本ID】", rendered)


if __name__ == "__main__":
    unittest.main()
