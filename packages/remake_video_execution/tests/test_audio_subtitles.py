from __future__ import annotations

import unittest

from remake_video_execution.audio import build_frozen_audio_plan
from remake_video_execution.parser import parse_source
from remake_video_execution.source import freeze_record
from remake_video_execution.subtitles import build_subtitle_plan


class AudioSubtitleTests(unittest.TestCase):
    def test_source_copy_and_screen_text_keep_global_timing(self):
        fields = {
            "脚本ID": "vs_audio", "脚本来源": "视频复刻", "视频时长": 10,
            "目标语言": "泰语", "发布用途": "养号", "是否挂车": "否",
            "产品图片": [{"file_token": "img"}],
            "口播_目标语言": "สวัสดีค่ะ",
            "视频生成提示词": "【0-10 秒】\n人物展示。\n口播/短句：สวัสดีค่ะ\n屏幕文字：สวยมาก",
        }
        plan = parse_source(freeze_record("rec", fields))
        audio, issues = build_frozen_audio_plan(plan)
        subtitles = build_subtitle_plan(plan)
        self.assertEqual(issues, [])
        self.assertEqual(audio["target_text"], "สวัสดีค่ะ")
        self.assertFalse(audio["rewrite_allowed"])
        self.assertEqual(audio["timed_lines"][0]["start_ms"], 0)
        self.assertEqual(subtitles["cues"][0]["end_ms"], 10000)

    def test_conflicting_voiceover_sources_block(self):
        fields = {
            "脚本ID": "vs_audio", "脚本来源": "视频复刻", "视频时长": 10,
            "目标语言": "泰语", "发布用途": "养号", "是否挂车": "否",
            "产品图片": [{"file_token": "img"}],
            "口播_目标语言": "版本甲",
            "视频生成提示词": "【0-10 秒】\n口播：版本乙",
        }
        _, issues = build_frozen_audio_plan(parse_source(freeze_record("rec", fields)))
        self.assertEqual([issue.code for issue in issues], ["AUDIO_SOURCE_CONFLICT"])


if __name__ == "__main__":
    unittest.main()
