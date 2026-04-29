#!/usr/bin/env python3
"""脚本 15 秒硬节点代码侧兜底测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.business_rules import validate_script_time_nodes  # noqa: E402
from core.json_parser import validate_script_payload  # noqa: E402
from core.pipeline import OriginalScriptPipeline  # noqa: E402


def _late_decision_script() -> dict:
    storyboard = []
    tasks = ["hook", "proof", "proof", "decision"]
    for index, task in enumerate(tasks, 1):
        storyboard.append(
            {
                "shot_no": index,
                "duration": "4s",
                "shot_content": "已夹好发饰结果镜头，头发和发饰关系清楚",
                "shot_purpose": "围绕发饰结果做证明",
                "subtitle_text_target_language": "",
                "subtitle_text_zh": "",
                "voiceover_text_target_language": "Kẹp lên là tóc gọn hơn",
                "voiceover_text_zh": "",
                "spoken_line_task": task,
                "person_action": "人物轻微侧头，发饰保持清楚可见",
                "style_note": "家中镜前自然分享",
                "anchor_reference": "发饰已佩戴结果",
                "task_type": "attention" if task == "hook" else "proof" if task == "proof" else "bridge",
                "ai_shot_risk": "low",
                "replacement_template_id": "",
            }
        )
    return {
        "script_positioning": {
            "script_title": "发饰风险化解脚本",
            "direction_type": "风险化解型",
            "core_primary_selling_point": "小抓夹也能整理局部头发",
        },
        "opening_design": {
            "opening_mode": "轻顾虑冲突型",
            "first_frame": "发饰已夹好的侧后方结果",
            "expression_entry": "担心夹不住也能先看结果",
            "first_line_type": "轻顾虑",
        },
        "full_15s_flow": [
            {"stage": "opening", "time_range": "0-4s", "task": "hook", "summary": "开场"},
            {"stage": "middle", "time_range": "4-12s", "task": "proof", "summary": "证明"},
            {"stage": "ending", "time_range": "12-16s", "task": "decision", "summary": "收束"},
        ],
        "storyboard": storyboard,
        "execution_constraints": {
            "visual_style": "家中自然光",
            "person_constraints": "动作克制",
            "styling_constraints": "头发和发饰关系清楚",
            "tone_completion_constraints": "干净日常感",
            "scene_constraints": "镜前",
            "emotion_progression_constraints": "轻顾虑到确认",
            "camera_focus": "发饰固定结果",
            "product_priority_principle": "发饰是主角",
            "realism_principle": "避免复杂夹发过程",
        },
        "rhythm_checkpoints": {
            "hook_complete_by": "3s",
            "core_proof_start_between": "4-8s",
            "decision_signal_by": "12s",
            "risk_resolution_decision_by": "9s_or_not_applicable",
        },
        "audio_layer": {
            "bgm_style": "清爽生活化",
            "bgm_energy": "low",
            "sfx_cues": [],
            "voiceover_priority": "high",
            "mix_note": "SFX 不盖过口播",
            "audio_negative_constraints": ["不要盖住口播"],
        },
        "negative_constraints": ["不做大幅甩头"],
    }


class ScriptTimingRepairTest(unittest.TestCase):
    def test_code_side_repair_moves_decision_before_risk_deadline(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        script = _late_decision_script()
        strategy = {"strategy_id": "S3", "script_role": "risk_resolution"}

        _, violations = validate_script_time_nodes(strategy, script)
        self.assertTrue(any("decision 信号" in item for item in violations))

        repaired, actions = pipeline._repair_script_timing_if_needed(strategy, script, violations)

        self.assertTrue(actions)
        validate_script_payload(repaired)
        _, repaired_violations = validate_script_time_nodes(strategy, repaired)
        self.assertFalse(any("decision 信号" in item for item in repaired_violations))
        self.assertIn("proof+decision", [shot["spoken_line_task"] for shot in repaired["storyboard"]])


if __name__ == "__main__":
    unittest.main()
