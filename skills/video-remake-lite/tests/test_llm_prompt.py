#!/usr/bin/env python3
"""Prompt contract tests for video-remake-lite Codex generation."""

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.llm_client import VideoRemakeLLMClient  # noqa: E402


class FourFieldPromptContractTest(unittest.TestCase):
    def test_prompt_allows_chinese_instructions_but_not_spoken_text(self) -> None:
        prompt = VideoRemakeLLMClient._build_four_field_prompt(
            {
                "content_branch_label": "非商品展示型",
                "target_country": "泰国",
                "target_language": "泰语",
                "product_type": "无",
                "store_id": "THBT01",
            },
            task_label="156",
            duration=12.5,
            frame_count=8,
        )

        self.assertIn("提示词本身可以用中文写执行说明", prompt)
        self.assertIn("所有会被视频显示或朗读的文字必须使用目标语言", prompt)
        self.assertIn("不要出现中文口播、不要出现中文字幕、不要出现中文屏幕文字", prompt)

    def test_spoken_text_repair_preserves_chinese_instructions(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        calls = []

        def fake_responses_text(*, prompt, frames):
            calls.append((prompt, list(frames)))
            return "生成一条竖屏短视频。\n镜头1：室内自拍。\n字幕/旁白：สวัสดี\n执行提醒：中文说明可以保留。"

        client._responses_text = fake_responses_text

        output = client._ensure_spoken_text_no_chinese(
            "生成一条竖屏短视频。\n字幕/旁白：你好\n执行提醒：保持自拍感。",
            {"target_language": "泰语"},
        )

        self.assertIn("生成一条竖屏短视频", output)
        self.assertIn("字幕/旁白：สวัสดี", output)
        self.assertEqual(len(calls), 1)
        self.assertIn("只修复会被视频里显示或朗读的内容", calls[0][0])

    def test_spoken_text_repair_skips_no_subtitle_instruction(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "镜头1：室内自拍。\n字幕/旁白：无字幕/无口播\n执行提醒：保持原视频节奏。",
            {"target_language": "泰语"},
        )

        self.assertIn("字幕/旁白：无字幕/无口播", output)


if __name__ == "__main__":
    unittest.main()
