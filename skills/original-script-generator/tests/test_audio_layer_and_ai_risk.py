#!/usr/bin/env python3
"""audio_layer 与镜头 AI 可拍性字段回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.json_parser import JSONParseError, validate_script_payload  # noqa: E402
from core.prompts import build_script_prompt, build_script_review_prompt  # noqa: E402
from core.script_brief_builder import _ai_shot_risk_profile_key  # noqa: E402
from core.script_renderer import render_script  # noqa: E402


def _base_script() -> dict:
    storyboard = []
    for index, task in enumerate(["hook", "proof", "proof", "decision"], 1):
        storyboard.append(
            {
                "shot_no": index,
                "duration": "3s",
                "shot_content": "已夹好发型结果镜头，发夹与头发固定关系清楚" if index == 1 else "侧后方结果镜头，头发保持整齐",
                "shot_purpose": "证明发饰夹好后的结果",
                "subtitle_text_target_language": "",
                "subtitle_text_zh": "",
                "voiceover_text_target_language": "Kẹp lên là tóc gọn hơn",
                "voiceover_text_zh": "",
                "spoken_line_task": task,
                "person_action": "人物轻微侧头，发夹位置保持稳定",
                "style_note": "家中镜前自然分享",
                "anchor_reference": "横夹侧边，半扎固定关系",
                "task_type": "attention" if task == "hook" else "proof" if task == "proof" else "bridge",
                "ai_shot_risk": "low",
                "replacement_template_id": "",
            }
        )
    return {
        "script_positioning": {
            "script_title": "发饰固定结果脚本",
            "direction_type": "结果成立型",
            "core_primary_selling_point": "夹好后头发更整齐",
        },
        "opening_design": {
            "opening_mode": "结果先给型",
            "first_frame": "已经夹好后的侧后方发型结果",
            "expression_entry": "夹上后头发更完整",
            "first_line_type": "结果判断",
        },
        "full_15s_flow": [
            {"stage": "opening", "time_range": "0-3s", "task": "hook", "summary": "先给夹好结果"},
            {"stage": "middle", "time_range": "4-9s", "task": "proof", "summary": "证明固定关系"},
            {"stage": "ending", "time_range": "10-12s", "task": "decision", "summary": "轻决策收束"},
        ],
        "storyboard": storyboard,
        "execution_constraints": {
            "visual_style": "家中镜前自然光，真实轻精致",
            "person_constraints": "自然分享状态，动作克制",
            "styling_constraints": "头发和发夹关系清楚",
            "tone_completion_constraints": "干净日常感",
            "scene_constraints": "家中镜前",
            "emotion_progression_constraints": "轻确认到满意",
            "camera_focus": "发夹固定头发后的结果",
            "product_priority_principle": "发饰和头发关系必须清楚",
            "realism_principle": "避免复杂夹发过程",
        },
        "audio_layer": {
            "bgm_style": "干净家居感",
            "bgm_energy": "low",
            "sfx_cues": [
                {
                    "time_range": "0.3-0.6s",
                    "sfx_type": "soft_click",
                    "purpose": "强化发夹夹住头发的固定感",
                    "volume_note": "很轻，不盖过口播",
                }
            ],
            "voiceover_priority": "high",
            "mix_note": "SFX 不盖过口播，画面动作弱时音效克制",
            "audio_negative_constraints": ["不要盖住口播", "不要游戏音效", "不要廉价 bling 效果"],
        },
        "rhythm_checkpoints": {
            "hook_complete_by": "3s",
            "core_proof_start_between": "4-8s",
            "decision_signal_by": "12s",
            "risk_resolution_decision_by": "9s_or_not_applicable",
        },
        "negative_constraints": ["不拍完整夹发过程"],
    }


class AudioLayerAndAiRiskTest(unittest.TestCase):
    def test_script_schema_accepts_audio_layer_and_ai_risk_fields(self) -> None:
        script = _base_script()

        validate_script_payload(script)
        rendered = render_script(script)

        self.assertIn("【音频】", rendered)
        self.assertIn("soft_click", rendered)

    def test_script_schema_backfills_audio_layer_and_ai_risk_for_old_payload(self) -> None:
        script = _base_script()
        script.pop("audio_layer")
        for shot in script["storyboard"]:
            shot.pop("ai_shot_risk")
            shot.pop("replacement_template_id")

        validate_script_payload(script)

        self.assertEqual(script["audio_layer"]["voiceover_priority"], "high")
        self.assertEqual(script["storyboard"][0]["ai_shot_risk"], "low")
        self.assertEqual(script["rhythm_checkpoints"]["core_proof_start_between"], "4-8s")

    def test_audio_layer_rejects_overdense_sfx(self) -> None:
        script = _base_script()
        script["audio_layer"]["sfx_cues"] = [
            {"time_range": "0-1s", "sfx_type": "soft_click", "purpose": "固定感", "volume_note": "轻"},
            {"time_range": "1-2s", "sfx_type": "hair_rustle", "purpose": "头发轻动", "volume_note": "轻"},
            {"time_range": "2-3s", "sfx_type": "small_pop", "purpose": "结果出现", "volume_note": "轻"},
            {"time_range": "3-4s", "sfx_type": "light_tap", "purpose": "细节展示", "volume_note": "轻"},
        ]

        with self.assertRaises(JSONParseError):
            validate_script_payload(script)

    def test_prompts_include_new_execution_rules(self) -> None:
        script_prompt = build_script_prompt("VN", "vi", "发夹", {"focus_control": {"script_role": "result_delivery"}})
        review_prompt = build_script_review_prompt("VN", "发夹", {}, {}, {}, {}, {})

        self.assertNotIn("{{audio_layer_rule}}", script_prompt + review_prompt)
        self.assertIn("audio_layer", script_prompt)
        self.assertIn("ai_shot_risk", script_prompt)
        self.assertIn("BGM 可能盖过口播", review_prompt)
        self.assertIn("前 3 秒看到夹好结果", review_prompt)
        self.assertIn("hair_accessory_subtype", script_prompt)
        self.assertIn("没有明确夹合动作时，不要强行使用 soft_click", script_prompt)
        self.assertIn("rhythm_checkpoints", script_prompt)

    def test_ai_shot_risk_profile_key_splits_ear_and_general_accessory(self) -> None:
        self.assertEqual(_ai_shot_risk_profile_key("耳线"), "ear_accessory")
        self.assertEqual(_ai_shot_risk_profile_key("发夹"), "hair_accessory")
        self.assertEqual(_ai_shot_risk_profile_key("手链"), "general_accessory")


if __name__ == "__main__":
    unittest.main()
