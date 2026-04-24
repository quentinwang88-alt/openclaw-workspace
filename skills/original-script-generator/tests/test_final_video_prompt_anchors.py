#!/usr/bin/env python3
"""最终视频提示词锚点强化回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.pipeline import OriginalScriptPipeline  # noqa: E402
from core.script_renderer import compress_final_video_prompt_payload, render_video_prompt  # noqa: E402


class FinalVideoPromptAnchorsTest(unittest.TestCase):
    def _build_pipeline(self) -> OriginalScriptPipeline:
        return OriginalScriptPipeline.__new__(OriginalScriptPipeline)

    def _base_prompt(self) -> dict:
        return {
            "video_setup": "家中镜前自然分享；真实轻精致；商品必须是主角；整体氛围自然",
            "execution_boundary": "原生自然；商品保持画面主角；不要强滤镜；不要空镜头",
            "shot_execution": [
                {
                    "shot_no": 1,
                    "duration": "3s",
                    "shot_content": "耳侧近景，先给佩戴前后的第一眼结果，对准耳侧轮廓和耳线位置",
                    "voiceover_text_target_language": "Đeo lên là thấy khác ngay",
                    "voiceover_text_zh": "",
                    "spoken_line_task": "hook",
                    "person_action": "人物轻转头，保持耳侧无遮挡",
                    "style_note": "",
                },
                {
                    "shot_no": 2,
                    "duration": "3s",
                    "shot_content": "中近景继续交代耳侧佩戴状态，让耳线长度和耳垂下方位置看清楚",
                    "voiceover_text_target_language": "Phần dây ngắn nhìn rất gọn",
                    "voiceover_text_zh": "",
                    "spoken_line_task": "proof",
                    "person_action": "人物停住侧脸，手不要遮挡耳侧",
                    "style_note": "",
                },
                {
                    "shot_no": 3,
                    "duration": "3s",
                    "shot_content": "切到正侧结合镜头，继续给佩戴结果，不要只拍氛围",
                    "voiceover_text_target_language": "Không bị vướng cổ nên nhìn sạch hơn",
                    "voiceover_text_zh": "",
                    "spoken_line_task": "proof",
                    "person_action": "人物微转头，保持耳侧与颈侧线条干净",
                    "style_note": "",
                },
                {
                    "shot_no": 4,
                    "duration": "3s",
                    "shot_content": "最后用稳定近景收住耳侧结果和整体精致感",
                    "voiceover_text_target_language": "Đeo hằng ngày rất hợp",
                    "voiceover_text_zh": "",
                    "spoken_line_task": "decision",
                    "person_action": "人物自然点头，保持商品为视觉中心",
                    "style_note": "",
                },
            ],
        }

    def test_reinforce_final_video_prompt_adds_anchor_summary_and_shot_note(self) -> None:
        pipeline = self._build_pipeline()
        anchor_card = {
            "parameter_anchors": [
                {
                    "parameter_name": "参数1",
                    "parameter_value": "耳线整体长度要短，不超过耳垂下方约2-3厘米",
                }
            ],
            "hard_anchors": [
                {
                    "anchor": "整体像精致短耳线，不是长款流苏耳线",
                }
            ],
            "display_anchors": [
                {
                    "anchor": "耳侧佩戴长度结果必须清楚",
                }
            ],
        }

        reinforced = pipeline._reinforce_final_video_prompt_anchors(self._base_prompt(), anchor_card)

        self.assertIn("商品锚点：", reinforced["video_setup"])
        self.assertIn("锚点执行：", reinforced["execution_boundary"])
        self.assertIn("锚点：", reinforced["shot_execution"][1]["style_note"])

    def test_compress_final_video_prompt_preserves_anchor_priority_segments(self) -> None:
        prompt = self._base_prompt()
        prompt["video_setup"] = (
            "商品锚点：短耳线，不超过耳垂下方2-3厘米 / 精致短耳线，不是长流苏；"
            "家中镜前自然分享；真实轻精致；商品必须是主角；整体氛围自然；"
            "镜头语言保持原生；不要过度修饰；结果要先成立"
        )
        prompt["execution_boundary"] = (
            "锚点执行：至少1镜清楚交代短耳线长度 / 非长流苏结果；"
            "原生自然；商品保持画面主角；不要强滤镜；不要空镜头；"
            "不要让头发长期遮挡耳侧；不要把商品拍成泛化情绪片"
        )
        for shot in prompt["shot_execution"]:
            shot["shot_content"] = shot["shot_content"] + "，并持续把耳线长度结果、耳侧轮廓与佩戴落点交代清楚，避免变成只剩氛围的近景描述"
            shot["person_action"] = shot["person_action"] + "，动作幅度保持克制，避免遮挡耳侧和颈侧线条"
        prompt["shot_execution"][1]["style_note"] = "锚点：短耳线长度"

        compressed = compress_final_video_prompt_payload(prompt, preferred_max_chars=260, hard_max_chars=320)
        rendered = render_video_prompt(prompt)

        self.assertIn("商品锚点：", compressed["video_setup"])
        self.assertIn("锚点执行：", compressed["execution_boundary"])
        self.assertIn("商品锚点：", rendered)


if __name__ == "__main__":
    unittest.main()
