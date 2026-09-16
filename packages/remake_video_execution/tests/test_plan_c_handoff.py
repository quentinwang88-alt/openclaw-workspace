from __future__ import annotations

import unittest

from remake_video_execution.plan_c_handoff import (
    build_plan_c_handoff,
    strip_visible_text_lines,
)
from remake_video_execution.planner import plan_c_segment_count


PRODUCT_REFERENCE_MANIFEST = {
    "reference_mode": "RAW_PRODUCT",
    "assets": [
        {"role": "PRODUCT_REFERENCE", "local_path": "/tmp/product.png", "sha256": "a" * 64},
    ],
}

PROMPT_30 = """【0-10 秒】
开场正面展示外套。
口播/短句：这是开头。

【10-20 秒】
侧身展示细节。
口播/短句：这是中段。

【20-30 秒】
转身收尾。
口播/短句：这是结尾。"""

PROMPT_30_NEUTRAL = """【0-10 秒】
开场正面展示外套。

【10-20 秒】
侧身展示细节。

【20-30 秒】
转身收尾。"""

PROMPT_41 = """【视频类型】
41秒竖屏短视频。

【0-3 秒】
人物近景出现。

【3-10 秒】
人物后退展示全身。

【10-18 秒】
人物连续转身。

【18-24 秒】
纽扣近景。屏幕文字：金色按扣

【24-30 秒】
解开纽扣。

【30-37 秒】
展示背面。

【37-41 秒】
回到正面。"""


def fields(prompt: str, duration: int, *, voiceover: str = "", silent: bool = False) -> dict:
    item = {
        "脚本ID": "vs_test", "脚本来源": "视频复刻", "视频时长": duration,
        "视频生成提示词": prompt, "目标国家": "TH", "目标语言": "泰语",
        "发布用途": "养号", "是否挂车": "否",
        "产品图片": [{"file_token": "image-token"}],
    }
    if voiceover:
        item["口播_目标语言"] = voiceover
    if silent:
        item["视频生成提示词"] = prompt + "\n\n本次不安排口播。"
    return item


class PlanCSegmentCountTests(unittest.TestCase):
    def test_duration_maps_to_two_or_three_segments(self):
        for duration in (16, 20, 25, 30):
            self.assertEqual(plan_c_segment_count(duration), 2)
        for duration in (31, 35, 41, 45):
            self.assertEqual(plan_c_segment_count(duration), 3)


class PlanCHandoffTests(unittest.TestCase):
    def test_30_second_script_becomes_a_and_b(self):
        handoff = build_plan_c_handoff(
            record_id="rec30", fields=fields(PROMPT_30, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertFalse(handoff["blocked"], handoff["blocking_codes"])
        segments = handoff["plan"]["segments"]
        self.assertEqual([item["segment_id"] for item in segments], ["A", "B"])
        self.assertEqual(sum(item["duration_seconds"] for item in segments), 30)

    def test_41_second_script_becomes_a_b_c(self):
        handoff = build_plan_c_handoff(
            record_id="rec41",
            fields=fields(PROMPT_41, 41, silent=True),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        segments = handoff["plan"]["segments"]
        self.assertEqual([item["segment_id"] for item in segments], ["A", "B", "C"])
        self.assertEqual(sum(item["duration_seconds"] for item in segments), 41)
        self.assertEqual(handoff["audio_mode"], "NO_VOICEOVER")

    def test_every_segment_is_within_the_h3_window(self):
        for prompt, duration in ((PROMPT_30, 30), (PROMPT_41, 41)):
            handoff = build_plan_c_handoff(
                record_id=f"rec{duration}",
                fields=fields(prompt, duration, silent=True),
                frozen_assets=PRODUCT_REFERENCE_MANIFEST,
            )
            for segment in handoff["plan"]["segments"]:
                self.assertGreaterEqual(segment["duration_seconds"], 4)
                self.assertLessEqual(segment["duration_seconds"], 15)

    def test_cut_and_continuous_boundaries_are_converted(self):
        # 30s with a boundary landing inside a shot must be a continuous bridge.
        handoff = build_plan_c_handoff(
            record_id="rec_cont", fields=fields(PROMPT_30, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertEqual(
            handoff["plan"]["segments"][1]["frame_contract"]["incoming_boundary_mode"],
            "CONTINUOUS",
        )
        self.assertEqual(
            handoff["plan"]["bridge_contract"]["boundaries"][0]["boundary_mode"],
            "CONTINUOUS",
        )
        self.assertEqual(handoff["plan"]["segments"][0]["generation_mode"], "first_last")
        self.assertEqual(handoff["plan"]["segments"][1]["generation_mode"], "first_frame")

        # A boundary on a real shot edge is a hard cut with its own entry frame.
        split = """【0-15 秒】
第一段动作。

【15-30 秒】
第二段动作。"""
        cut = build_plan_c_handoff(
            record_id="rec_cut", fields=fields(split, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertEqual(
            cut["plan"]["bridge_contract"]["boundaries"][0]["boundary_mode"],
            "DISCONTINUOUS_CUT",
        )
        self.assertIn("SB_ENTRY", cut["keyframe_package"])

    def test_keyframe_package_is_plan_c_compatible(self):
        handoff = build_plan_c_handoff(
            record_id="rec_kf", fields=fields(PROMPT_41, 41),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        package = handoff["keyframe_package"]
        self.assertIn("K0", package)
        self.assertIn("frozen_reference_assets", package)
        self.assertTrue(package["K0"]["prompt"])
        self.assertEqual(
            package["segment_frames"]["A"]["start"],
            {"key": "K0", "source": "MASTER_OPENING"},
        )
        # Continuous boundaries own a planned tail frame plus an actual tail read.
        planned = [key for key in package if key.endswith("_PLANNED")]
        actual = [key for key in package if key.endswith("_ACTUAL")]
        expected = sum(
            1 for item in handoff["plan"]["bridge_contract"]["boundaries"]
            if item["boundary_mode"] == "CONTINUOUS"
        )
        self.assertEqual(len(planned), expected)
        self.assertEqual(len(actual), expected)
        for key in planned + [item for item in package if item.endswith("_ENTRY")]:
            prompt = package[key]["prompt"]
            # Frozen spoken copy and on-screen copy must never reach the image model.
            for copy_text in ("这是开头", "这是中段", "这是结尾", "金色按扣"):
                self.assertNotIn(copy_text, prompt)
            # ...but the negative visual constraint itself must stay explicit.
            self.assertIn("画面中不出现口播、字幕、屏幕文字或CTA文字", prompt)
            self.assertIn("观众可见文字统一由后期生成", prompt)

    def test_segment_prompt_keeps_frozen_actions_without_new_creative_content(self):
        handoff = build_plan_c_handoff(
            record_id="rec_prompt", fields=fields(PROMPT_41, 41),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        rendered = "\n".join(item["video_prompt"] for item in handoff["plan"]["segments"])
        for text in ("人物近景出现", "人物连续转身", "回到正面"):
            self.assertIn(text, rendered)
        self.assertIn("不增加剧情、卖点、台词、字幕或CTA", rendered)
        # Explicit screen copy must not be handed to the video model.
        self.assertNotIn("金色按扣", rendered)
        self.assertEqual(
            handoff["plan"]["postprocess_contract"]["subtitles"]["mode"],
            "PRESERVE_SOURCE_TIMELINE",
        )
        self.assertTrue(handoff["plan"]["postprocess_contract"]["subtitles"]["items"])

    def test_frozen_voiceover_is_never_rewritable_and_hash_is_stable(self):
        handoff = build_plan_c_handoff(
            record_id="rec_voice", fields=fields(PROMPT_30, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertEqual(handoff["audio_mode"], "PRESERVE_SOURCE_COPY")
        self.assertFalse(handoff["voiceover"]["rewrite_allowed"])
        self.assertFalse(handoff["voiceover"]["revision_allowed"])
        self.assertEqual(len(handoff["voiceover"]["semantic_sections"]), 2)
        digest = handoff["voiceover"]["target_text_sha256"]
        repeat = build_plan_c_handoff(
            record_id="rec_voice", fields=fields(PROMPT_30, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertEqual(repeat["voiceover"]["target_text_sha256"], digest)

    def test_stable_input_produces_stable_job_id_and_revision(self):
        first = build_plan_c_handoff(
            record_id="rec_stable", fields=fields(PROMPT_30, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        second = build_plan_c_handoff(
            record_id="rec_stable", fields=fields(PROMPT_30, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertTrue(first["job_id"].startswith("LFR_"))
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(first["source_revision_hash"], second["source_revision_hash"])

        changed = build_plan_c_handoff(
            record_id="rec_stable",
            fields=fields(PROMPT_30.replace("侧身展示细节", "正面转侧身展示细节"), 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertNotEqual(first["job_id"], changed["job_id"])
        self.assertNotEqual(first["source_revision_hash"], changed["source_revision_hash"])

    def test_unresolved_audio_mode_blocks_before_paid_submission(self):
        handoff = build_plan_c_handoff(
            record_id="rec_audio", fields=fields(PROMPT_30_NEUTRAL, 30),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        # The neutral script carries neither spoken copy nor a silence claim, so
        # the sound mode is genuinely unknown and must not be guessed.
        self.assertEqual(handoff["audio_mode"], "UNSPECIFIED")
        self.assertTrue(handoff["blocked"])
        self.assertIn("AUDIO_MODE_UNRESOLVED", handoff["blocking_codes"])
        for code in ("REMAKE_SEGMENT_COUNT_UNSUPPORTED", "FROZEN_PRODUCT_REFERENCE_MISSING"):
            self.assertNotIn(code, handoff["blocking_codes"])

    def test_missing_frozen_product_reference_blocks(self):
        handoff = build_plan_c_handoff(
            record_id="rec_no_ref", fields=fields(PROMPT_30, 30) | {"产品图片": []},
            frozen_assets={"assets": [{"role": "PERSONA_REFERENCE", "local_path": "/tmp/p.png"}]},
        )
        self.assertIn("FROZEN_PRODUCT_REFERENCE_MISSING", handoff["blocking_codes"])

    def test_over_long_source_is_blocked_not_silently_truncated(self):
        prompt = "\n\n".join(f"【{i}-{i + 10} 秒】\n动作{i}。" for i in range(0, 60, 10))
        handoff = build_plan_c_handoff(
            record_id="rec_60", fields=fields(prompt, 60, silent=True),
            frozen_assets=PRODUCT_REFERENCE_MANIFEST,
        )
        self.assertTrue(handoff["blocked"])
        self.assertIn("REMAKE_SEGMENT_COUNT_UNSUPPORTED", handoff["blocking_codes"])


class VisibleTextTests(unittest.TestCase):
    def test_inline_and_own_line_screen_text_are_removed(self):
        self.assertEqual(
            strip_visible_text_lines("纽扣近景。屏幕文字：金色按扣"),
            "纽扣近景。",
        )
        self.assertEqual(
            strip_visible_text_lines("动作。\n字幕：你好\n更多的动作。"),
            "动作。\n更多的动作。",
        )
        # The word 字幕 alone ("不要字幕") is a visual instruction, not copy.
        self.assertEqual(
            strip_visible_text_lines("不要字幕；不要让人物说话。"),
            "不要字幕；不要让人物说话。",
        )


if __name__ == "__main__":
    unittest.main()
