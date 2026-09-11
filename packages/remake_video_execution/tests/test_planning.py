from __future__ import annotations

import unittest

from remake_video_execution.coverage import validate_coverage
from remake_video_execution.parser import parse_source
from remake_video_execution.planner import plan_segments
from remake_video_execution.source import freeze_record


PROMPT = """【视频类型】
41秒竖屏短视频。

【整体画面风格】
自然生活流。

【0-3 秒】
人物近景出现。

【3-10 秒】
人物后退展示全身。

【10-18 秒】
人物连续转身。

【18-24 秒】
纽扣近景。

【24-30 秒】
解开纽扣。

【30-37 秒】
展示背面。

【37-41 秒】
回到正面。"""


def fields(prompt: str = PROMPT) -> dict:
    return {
        "脚本ID": "vs_test", "脚本来源": "视频复刻", "视频时长": 41,
        "视频生成提示词": prompt, "目标国家": "TH", "目标语言": "泰语",
        "发布用途": "养号", "是否挂车": "否",
        "产品图片": [{"file_token": "image-token"}],
    }


class PlanningTests(unittest.TestCase):
    def test_parses_and_covers_timed_prompt_without_rewriting_source_lines(self):
        source = freeze_record("rec_test", fields())
        plan = parse_source(source)
        segments = plan_segments(plan)
        self.assertEqual(len(plan.shots), 7)
        self.assertTrue(all(segment.requested_duration_seconds <= 15 for segment in segments.segments))
        self.assertEqual(validate_coverage(plan, segments), [])
        rendered = "\n".join(segment.prompt for segment in segments.segments)
        for text in ("人物近景出现", "人物连续转身", "回到正面"):
            self.assertIn(text, rendered)

    def test_long_single_shot_is_split_and_marked_continuous(self):
        raw = "【0-20 秒】\n人物持续完成一个不可中断的动作。"
        item = fields(raw)
        item["视频时长"] = 20
        plan = plan_segments(parse_source(freeze_record("rec_long", item)))
        self.assertEqual(
            [(x.global_start_ms, x.global_end_ms) for x in plan.segments],
            [(0, 15000), (15000, 20000)],
        )
        self.assertEqual(plan.segments[1].incoming_boundary, "CONTINUOUS")

    def test_missing_assets_and_business_handoff_are_explicit_blockers(self):
        item = fields()
        item.pop("产品图片")
        item.pop("发布用途")
        item.pop("是否挂车")
        plan = parse_source(freeze_record("rec_missing", item))
        self.assertGreaterEqual(
            {issue.code for issue in plan.issues},
            {"REFERENCE_ASSETS_MISSING", "PUBLISH_PURPOSE_MISSING", "CART_SETTING_MISSING"},
        )

    def test_source_voiceover_is_preserved_and_not_added_to_no_voiceover(self):
        voiced = fields("【0-10 秒】\n人物展示。\n口播/短句：สวัสดีค่ะ")
        voiced["视频时长"] = 10
        self.assertEqual(parse_source(freeze_record("rec_voice", voiced)).audio_mode, "SOURCE_COPY_TTS")
        silent = fields("【0-10 秒】\n人物展示。\n本次不安排口播。")
        silent["视频时长"] = 10
        self.assertEqual(parse_source(freeze_record("rec_silent", silent)).audio_mode, "NO_VOICEOVER")

    def test_visible_chinese_text_blocks_thai_task(self):
        item = fields("【0-10 秒】\n人物挥手。\n屏幕文字：今天就到这里")
        item["视频时长"] = 10
        plan = parse_source(freeze_record("rec_bad_text", item))
        self.assertIn("VISIBLE_TEXT_LANGUAGE_MISMATCH", {issue.code for issue in plan.issues})

    def test_trailing_product_lock_is_global_for_every_segment(self):
        item = fields(
            "【0-10 秒】\n第一段动作。\n\n【10-20 秒】\n第二段动作。"
            "\n\n【目标商品必须看清】\n深棕色短夹克必须保持一致。"
        )
        item["视频时长"] = 20
        plan = plan_segments(parse_source(freeze_record("rec_global", item)))
        self.assertEqual(len(plan.segments), 2)
        self.assertTrue(all("深棕色短夹克必须保持一致" in segment.prompt for segment in plan.segments))
        self.assertNotIn("目标商品必须看清", plan.segments[-1].prompt.split("【本段时间轴】", 1)[1])

    def test_structured_timeline_is_used_when_prompt_has_no_time_headings(self):
        item = fields("自由格式人工说明，以结构化镜头为执行时间轴。")
        source = freeze_record("rec_structured", item, {
            "shots": [
                {"shot_id": "A", "start_ms": 0, "end_ms": 8000, "action": "打开外套"},
                {"shot_id": "B", "start_ms": 8000, "end_ms": 16000, "action": "穿好展示"},
            ]
        })
        plan = parse_source(source)
        self.assertEqual([shot.shot_id for shot in plan.shots], ["A", "B"])
        self.assertNotIn("TIMELINE_UNSTRUCTURED", {issue.code for issue in plan.issues})

    def test_segment_prompt_uses_local_clock_for_detail_lines(self):
        item = fields(
            "【0-10 秒】\n第一段。\n  · 6-10s 走近镜头。"
            "\n\n【10-18 秒】\n第二段。\n  · 10-13s 转向左侧。\n  · 13-15.5s 整理衣领。"
        )
        item["视频时长"] = 18
        plan = plan_segments(parse_source(freeze_record("rec_clock", item)))
        second = plan.segments[1].prompt
        self.assertIn("0-3s 转向左侧", second)
        self.assertIn("3-5.5s 整理衣领", second)
        self.assertNotIn("10-13s", second)

    def test_split_shot_omits_detail_lines_outside_segment(self):
        item = fields(
            "【0-20 秒】\n连续展示。\n  · 0-10s 正面。\n  · 10-20s 背面。"
        )
        item["视频时长"] = 20
        plan = plan_segments(parse_source(freeze_record("rec_split_clock", item)))
        self.assertIn("0-10s 正面", plan.segments[0].prompt)
        self.assertIn("10-15s 背面", plan.segments[0].prompt)
        self.assertNotIn("正面", plan.segments[1].prompt)
        self.assertIn("0-5s 背面", plan.segments[1].prompt)


if __name__ == "__main__":
    unittest.main()
