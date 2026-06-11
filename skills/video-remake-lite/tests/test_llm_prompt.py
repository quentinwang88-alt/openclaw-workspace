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

    def test_spoken_text_validator_allows_inline_execution_notes(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "镜头1：搜索页收尾。\n屏幕文字：\"ค้นหา\"、\"THFJ01\"。不要显示其他语言文字。\n字幕/旁白：ไม่มีคำบรรยาย/ไม่มีเสียงพากย์（执行说明，不显示）。",
            {"target_language": "泰语"},
        )

        self.assertIn("ค้นหา", output)
        self.assertIn("执行说明", output)

    def test_spoken_text_validator_handles_multi_shot_no_subtitle_lines(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "镜头1：室内自拍。字幕/旁白：无字幕/无口播（执行说明，不显示）。 | 镜头2：挥手。字幕/旁白：无字幕/无口播（执行说明，不显示）。",
            {"target_language": "泰语"},
        )

        self.assertIn("镜头2", output)

    def test_spoken_text_validator_still_repairs_actual_chinese_spoken_text(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        calls = []

        def fake_responses_text(*, prompt, frames):
            calls.append(prompt)
            return "字幕/旁白：สวัสดี"

        client._responses_text = fake_responses_text

        output = client._ensure_spoken_text_no_chinese(
            "镜头1：室内自拍。字幕/旁白：你好。",
            {"target_language": "泰语"},
        )

        self.assertIn("สวัสดี", output)
        self.assertEqual(len(calls), 1)

    def test_spoken_text_validator_allows_screen_text_position_notes(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "镜头6：尾卡。屏幕文字只显示泰语：顶部“ทุกคนอยู่ที่ TikTok”；搜索框左侧“ค้นหา”；搜索词“ท่าเต้นมือง่ายๆ”。旁白/口播：无口播（执行说明，不朗读）。",
            {"target_language": "泰语"},
        )

        self.assertIn("ท่าเต้น", output)

    def test_spoken_text_validator_allows_no_screen_text_execution_note(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "屏幕文字：无屏幕文字（这是执行说明，不是画面文字）。",
            {"target_language": "泰语"},
        )

        self.assertIn("无屏幕文字", output)

    def test_spoken_text_validator_allows_quoted_screen_text_with_tail_notes(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "镜头7：尾卡。可显示的屏幕文字仅限泰语：顶部显示“ทุกคนกำลังใช้”；搜索框左侧显示“ค้นหา”；搜索词显示“เต้นมือแบบง่าย”。右下角可放装饰图标，不要出现中文或原中文水印。",
            {"target_language": "泰语"},
        )

        self.assertIn("ค้นหา", output)

    def test_spoken_text_validator_allows_screen_text_label_notes(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '镜头7：尾卡。屏幕文字只允许越南语：按钮文字"Tìm kiếm"，搜索框文字"clover"，下方文字"Khám phá thêm nhà sáng tạo"。不要出现任何中文。',
            {"target_language": "越南语"},
        )

        self.assertIn("Tìm kiếm", output)

    def test_spoken_text_validator_allows_platform_fallback_notes(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '镜头7：尾卡。屏幕文字必须只显示越南语和英文："Tìm kiếm trên TikTok"、"clover"、"Khám phá thêm nhà sáng tạo"。如果不能使用真实平台标识，则做成普通深色搜索界面，不放真实logo。无口播。',
            {"target_language": "越南语"},
        )

        self.assertIn("clover", output)

    def test_spoken_text_validator_allows_quoted_text_with_unquoted_chinese_notes(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '10.8-14.3秒：尾卡。屏幕文字只能显示："Tìm kiếm"、"VNPS01"、"Khám phá thêm nhiều nhà sáng tạo"。无口播。停留到结束。',
            {"target_language": "越南语"},
        )

        self.assertIn("VNPS01", output)

    def test_spoken_text_validator_repairs_quoted_chinese_visible_text(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        calls = []

        def fake_responses_text(*, prompt, frames):
            calls.append(prompt)
            return '屏幕文字："Tìm kiếm"'

        client._responses_text = fake_responses_text

        output = client._ensure_spoken_text_no_chinese(
            '屏幕文字："搜索"',
            {"target_language": "越南语"},
        )

        self.assertIn("Tìm kiếm", output)
        self.assertEqual(len(calls), 1)

    def test_spoken_text_validator_does_not_treat_quoted_id_colon_as_field_colon(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '镜头7：尾卡，不出现中文。头像下方显示屏幕文字"ID: 77630547764"。左侧按钮显示屏幕文字"Tìm kiếm TikTok"。旁白/口播：无口播（执行说明，不显示在画面）。',
            {"target_language": "越南语"},
        )

        self.assertIn("ID: 77630547764", output)

    def test_spoken_text_validator_allows_subtitle_timing_notes_with_quoted_thai(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '字幕/口播要求：只有前 0.0-3.4 秒显示泰语字幕："นี่คือความต่างที่คุณอยากเห็นใช่ไหม"。全片无口播。不要添加其他屏幕文字。',
            {"target_language": "泰语"},
        )

        self.assertIn("นี่คือความต่าง", output)

    def test_spoken_text_validator_allows_subtitle_requirement_with_style_notes(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '字幕要求：唯一可显示字幕只使用泰语："นี่คือความต่างที่เธออยากเห็นใช่ไหม"，仅在前半段0.0-3.4秒左右出现，白色简洁字幕，位置在画面下方，不遮挡人物脸。',
            {"target_language": "泰语"},
        )

        self.assertIn("เธออยากเห็น", output)

    def test_spoken_text_validator_allows_subtitle_style_line_with_quoted_thai(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '字幕样式：仅前半段显示泰语字幕“นี่คือความต่างที่เธออยากเห็นใช่ไหม”，白色字体、细黑描边、位于画面下方居中，不遮挡脸部。后半段不显示文字。',
            {"target_language": "泰语"},
        )

        self.assertIn("นี่คือความต่าง", output)

    def test_spoken_text_validator_allows_unquoted_thai_in_requirement_line(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '字幕/旁白要求：只出现一次核心泰语字幕并在前半段持续：นี่คือความต่างที่อยากได้ไหม；全片无口播。',
            {"target_language": "泰语"},
        )

        self.assertIn("นี่คือความต่าง", output)

    def test_spoken_text_validator_allows_subtitle_style_notes_with_quoted_thai(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            '1）0.0-1.4秒：室内暖色台灯光，年轻泰国女性正面中近景，长棕发刘海，戴黑框眼镜，穿浅色居家睡衣或宽松上衣。她右手扶眼镜，左手轻触脸颊，表情平静、略冷。画面左下显示泰语字幕："สวัสดี ขอ 4 วิ"，白色字体，轻微黑色描边。',
            {"target_language": "泰语"},
        )

        self.assertIn("สวัสดี", output)

    def test_spoken_text_validator_allows_subtitle_style_content_marker(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "字幕样式：白色小号无衬线字体，轻微阴影，位置在画面下三分之一、胸口上方，内容只能是：ลองแต่งหน้าสไตล์ญี่ปุ่น...",
            {"target_language": "泰语"},
        )

        self.assertIn("ลองแต่งหน้า", output)

    def test_spoken_text_validator_allows_actual_display_text_marker(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "字幕样式：仅前半段底部居中白色小字，轻微阴影，实际显示文字只能是：ลองแต่งหน้าสไตล์ญี่ปุ่น...",
            {"target_language": "泰语"},
        )

        self.assertIn("ลองแต่งหน้า", output)

    def test_spoken_text_validator_allows_subtitle_requirement_with_style_tail(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        client._responses_text = lambda *, prompt, frames: self.fail("repair should not run")

        output = client._ensure_spoken_text_no_chinese(
            "字幕/旁白要求：全片无口播；只在前半段显示泰语屏幕文字：ลองแต่งหน้าแบบญี่ปุ่น...；后半段不显示文字。字幕为白色小字，居中偏下，简洁不遮脸。",
            {"target_language": "泰语"},
        )

        self.assertIn("ลองแต่งหน้า", output)

    def test_spoken_text_validator_repairs_chinese_after_style_content_marker(self) -> None:
        client = object.__new__(VideoRemakeLLMClient)
        calls = []

        def fake_responses_text(*, prompt, frames):
            calls.append(prompt)
            return "字幕样式：白色小号无衬线字体，内容只能是：ลองแต่งหน้า"

        client._responses_text = fake_responses_text

        output = client._ensure_spoken_text_no_chinese(
            "字幕样式：白色小号无衬线字体，内容只能是：试试日系妆容",
            {"target_language": "泰语"},
        )

        self.assertIn("ลองแต่งหน้า", output)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
