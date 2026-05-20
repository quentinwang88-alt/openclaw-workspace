#!/usr/bin/env python3
"""Q1 三层分流 precheck 回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.pipeline import OriginalScriptPipeline  # noqa: E402
from core.prompts import build_script_review_prompt  # noqa: E402


def _anchor_card() -> dict:
    return {
        "category_execution_contract": {
            "display_family": "hair_accessory",
            "product_subtype": "scrunchie",
            "use_case": "low_bun",
            "placement_zone": "bun_area",
            "hold_scope": "bun",
            "orientation": "wrap_around",
            "primary_visual_result": "低髻佩戴后更柔和完整",
            "operation_policy": "result_first_process_avoid",
            "field_confidence": {
                "product_subtype": "high",
                "use_case": "high",
                "placement_zone": "high",
                "hold_scope": "high",
                "orientation": "high",
                "primary_visual_result": "high",
                "operation_policy": "high",
            },
            "safe_shot_templates": ["已戴好在低髻上的结果镜头"],
            "forbidden_actions": ["完整套入并环绕固定过程"],
            "result_priority": "先证明低髻更完整",
            "audio_policy": {
                "forbidden_sfx": ["soft_click", "clean_clip_click"],
            },
        }
    }


def _persona_pack() -> dict:
    return {
        "human_performance_contract": {
            "gaze_rule": {
                "min_points_required": 3,
                "final_point_options": ["camera", "mirror_full_result"],
            }
        }
    }


def _script() -> dict:
    storyboard = []
    ranges = ["0-3s", "3-6s", "6-10s", "10-15s"]
    gazes = ["mirror", "hair_accessory_position", "camera", "mirror_full_result"]
    for index, time_range in enumerate(ranges, 1):
        storyboard.append(
            {
                "shot_no": index,
                "duration": time_range,
                "shot_content": "已戴好发圈的低髻结果镜头",
                "shot_purpose": "证明低髻更完整",
                "subtitle_text_target_language": "",
                "subtitle_text_zh": "",
                "voiceover_text_target_language": "This bun looks softer now.",
                "voiceover_text_zh": "",
                "spoken_line_task": "hook" if index == 1 else "decision" if index == 4 else "proof",
                "person_action": "人物在镜前轻整理发饰边缘",
                "performance": {
                    "gaze": gazes[index - 1],
                    "expression_or_micro_reaction": "嘴角轻轻放松" if index in {2, 4} else "",
                    "body_language": "肩膀放松",
                    "product_interaction": "轻整理发饰边缘" if index == 2 else "",
                },
                "style_note": "结果先给，不回到未戴状态",
                "anchor_reference": "发圈在低髻外侧",
                "task_type": "attention" if index == 1 else "bridge" if index == 4 else "proof",
                "ai_shot_risk": "low",
                "replacement_template_id": "",
            }
        )
    return {
        "proof_path": "A_result_detail_only",
        "performance_strategy": "镜前轻确认",
        "shot_skeleton": [
            {"shot_index": 1, "time_range": "0-3s", "role": "hook", "shot_purpose": "结果先给", "proof_path": "A_result_detail_only"},
            {"shot_index": 2, "time_range": "3-6s", "role": "proof", "shot_purpose": "发饰细节", "proof_path": "A_result_detail_only"},
            {"shot_index": 3, "time_range": "6-10s", "role": "proof", "shot_purpose": "结果复核", "proof_path": "A_result_detail_only"},
            {"shot_index": 4, "time_range": "10-15s", "role": "decision", "shot_purpose": "整体确认", "proof_path": "A_result_detail_only"},
        ],
        "script_positioning": {
            "script_title": "低髻发圈结果脚本",
            "direction_type": "结果成立型",
            "core_primary_selling_point": "低髻更完整",
        },
        "opening_design": {
            "opening_mode": "结果先给",
            "first_frame": "已戴好发圈的低髻",
            "expression_entry": "轻观察",
            "first_line_type": "结果判断",
        },
        "full_15s_flow": [
            {"stage": "opening", "time_range": "0-3s", "task": "hook", "summary": "结果先给"},
            {"stage": "middle", "time_range": "3-10s", "task": "proof", "summary": "细节和结果复核"},
            {"stage": "ending", "time_range": "10-15s", "task": "decision", "summary": "整体确认"},
        ],
        "storyboard": storyboard,
        "execution_constraints": {
            "visual_style": "镜前自然光",
            "person_constraints": "真实整理状态",
            "styling_constraints": "低髻和发圈关系清楚",
            "tone_completion_constraints": "轻分享",
            "scene_constraints": "镜前",
            "emotion_progression_constraints": "观察到满意",
            "camera_focus": "发圈和低髻",
            "product_priority_principle": "商品是主角",
            "realism_principle": "不拍完整套入过程",
        },
        "rhythm_checkpoints": {
            "hook_complete_by": "3s",
            "core_proof_start_between": "4-8s",
            "decision_signal_by": "12s",
            "risk_resolution_decision_by": "9s_or_not_applicable",
        },
        "audio_layer": {
            "bgm_style": "低存在感生活感 BGM",
            "bgm_energy": "low",
            "sfx_cues": [],
            "voiceover_priority": "high",
            "mix_note": "BGM 不盖过口播",
            "audio_negative_constraints": ["不要 click"],
        },
        "negative_constraints": ["不拍完整套入过程"],
    }


class Q1PrecheckTest(unittest.TestCase):
    def test_precheck_passes_clean_contract_aligned_script(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        result = pipeline._build_q1_precheck_result(
            context={"target_language": "English", "product_type": "发圈"},
            anchor_card=_anchor_card(),
            persona_style_emotion_pack=_persona_pack(),
            final_strategy={"strategy_id": "S1"},
            script_json=_script(),
        )

        self.assertTrue(result["pass"])
        self.assertFalse(result["blocking_major"])

    def test_precheck_blocks_forbidden_sfx_and_full_process(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        script = _script()
        script["audio_layer"]["sfx_cues"] = [
            {"time_range": "1-2s", "sfx_type": "soft_click", "purpose": "click", "volume_note": "light"}
        ]
        script["storyboard"][1]["person_action"] = "人物完整套入并环绕固定过程"

        result = pipeline._build_q1_precheck_result(
            context={"target_language": "English", "product_type": "发圈"},
            anchor_card=_anchor_card(),
            persona_style_emotion_pack=_persona_pack(),
            final_strategy={"strategy_id": "S1"},
            script_json=script,
        )

        self.assertTrue(result["blocking_major"])
        self.assertTrue(any("audio_policy" in item for item in result["l1_major_issues"]))
        self.assertTrue(any("operation_policy" in item for item in result["l1_major_issues"]))

    def test_precheck_ignores_negative_constraints_when_matching_forbidden_terms(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        script = _script()
        script["audio_layer"]["sfx_cues"] = [
            {"time_range": "1-2s", "sfx_type": "柔和转场音", "purpose": "不要使用 soft_click", "volume_note": "light"}
        ]
        script["audio_layer"]["audio_negative_constraints"] = ["不要 soft_click", "不要 clean_clip_click"]
        script["negative_constraints"] = ["不拍完整套入并环绕固定过程"]
        script["shot_skeleton"][1]["shot_purpose"] = "结果复核，不拍完整佩戴。"
        script["storyboard"][1]["performance"]["product_interaction"] = "只轻扶花体外缘一次，不让手、头发和发饰长时间纠缠"

        result = pipeline._build_q1_precheck_result(
            context={"target_language": "English", "product_type": "发圈"},
            anchor_card=_anchor_card(),
            persona_style_emotion_pack=_persona_pack(),
            final_strategy={"strategy_id": "S1"},
            script_json=script,
        )

        self.assertFalse(result["blocking_major"])
        self.assertFalse(any("audio_policy" in item for item in result["l1_major_issues"]))
        self.assertFalse(any("operation_policy" in item for item in result["l1_major_issues"]))

    def test_apparel_accessory_strong_claim_check_ignores_negated_policy_text(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        anchor = _anchor_card()
        anchor["category_execution_contract"].update(
            {
                "display_family": "apparel_accessory",
                "product_subtype": "scarf",
                "use_case": "winter_outing",
                "placement_zone": "neck_shoulder",
                "hold_scope": "winter_outfit_completion",
                "orientation": "wrapped_neck",
                "primary_visual_result": "围巾让冬季上半身搭配更完整",
                "result_priority": "证明冬季穿搭完整度，不承诺强保暖或防风效果",
            }
        )
        script = _script()
        for shot in script["storyboard"]:
            shot["shot_content"] = "已围好围巾的镜前上半身结果镜头"
            shot["person_action"] = "人物轻整理围巾边缘，半步后退看外套搭配"
        script["execution_constraints"]["product_priority_principle"] = "不承诺强保暖、防风、防寒效果"
        script["negative_constraints"] = ["不要写强保暖", "不得承诺防风"]

        result = pipeline._build_q1_precheck_result(
            context={"target_language": "English", "product_type": "围巾"},
            anchor_card=anchor,
            persona_style_emotion_pack=_persona_pack(),
            final_strategy={"strategy_id": "S1"},
            script_json=script,
        )

        self.assertFalse(any("apparel_accessory_winter_check" in item for item in result["l1_major_issues"]))

    def test_precheck_reports_chinese_in_target_language_voiceover(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        script = _script()
        script["storyboard"][0]["voiceover_text_target_language"] = "戴上之后发型更完整"

        result = pipeline._build_q1_precheck_result(
            context={"target_language": "vi", "product_type": "发圈"},
            anchor_card=_anchor_card(),
            persona_style_emotion_pack=_persona_pack(),
            final_strategy={"strategy_id": "S1"},
            script_json=script,
        )

        self.assertTrue(any("target_language_check" in item for item in result["l0_major_issues"]))

    def test_review_prompt_includes_precheck_result(self) -> None:
        prompt = build_script_review_prompt(
            target_country="US",
            product_type="发圈",
            anchor_card_json=_anchor_card(),
            final_strategy_json={},
            expression_plan_json={},
            persona_style_emotion_pack_json=_persona_pack(),
            script_json=_script(),
            target_language="English",
            pre_qc_result={"blocking_major": True, "l1_major_issues": ["x"]},
        )

        self.assertIn("代码侧 Q1 precheck 结果", prompt)
        self.assertIn("不要重新辩论", prompt)

    def test_review_prompt_uses_compact_q1_context(self) -> None:
        prompt = build_script_review_prompt(
            target_country="US",
            product_type="发圈",
            anchor_card_json={
                **_anchor_card(),
                "product_positioning_one_liner": "这段不应作为完整锚点卡传入 Q1",
                "candidate_primary_selling_points": [
                    {"selling_point": "冗余卖点", "how_to_tell": "冗余说明"}
                ],
            },
            final_strategy_json={
                "strategy_id": "S1",
                "script_role": "result_delivery",
                "primary_focus": "低髻更完整",
                "proof_path": "A_result_detail_only",
                "long_unused_field": "这段不应进入 Q1",
            },
            expression_plan_json={
                "opening_expression_task": "中文控制任务",
                "voiceover_intent": "目标语言口播意图",
            },
            persona_style_emotion_pack_json=_persona_pack(),
            script_json=_script(),
            target_language="English",
            pre_qc_result={"blocking_major": False},
        )

        self.assertIn("Q1 精简上下文", prompt)
        self.assertIn("category_execution_contract", prompt)
        self.assertIn("strategy_core", prompt)
        self.assertIn("voiceover_intent", prompt)
        self.assertNotIn("candidate_primary_selling_points", prompt)
        self.assertNotIn("long_unused_field", prompt)
        self.assertNotIn("这段不应作为完整锚点卡传入 Q1", prompt)

    def test_local_repair_removes_forbidden_sfx(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        script = _script()
        script["audio_layer"]["sfx_cues"] = [
            {"time_range": "1-2s", "sfx_type": "soft_click", "purpose": "click", "volume_note": "light"}
        ]
        repaired, actions = pipeline._apply_q1_precheck_local_repairs(
            script_json=script,
            anchor_card=_anchor_card(),
            pre_qc_result={"human_stiffness_check": {}},
        )

        self.assertTrue(actions)
        self.assertEqual(repaired["audio_layer"]["sfx_cues"], [])

    def test_high_maturity_clean_precheck_can_use_local_q1_pass(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        precheck = pipeline._build_q1_precheck_result(
            context={"target_language": "English", "product_type": "发圈", "type_guard": {"review_required": False}},
            anchor_card=_anchor_card(),
            persona_style_emotion_pack=_persona_pack(),
            final_strategy={"strategy_id": "S1"},
            script_json=_script(),
        )

        self.assertTrue(pipeline._should_use_local_q1_pass(
            {"type_guard": {"review_required": False}},
            _anchor_card(),
            precheck,
        ))
        review = pipeline._build_local_q1_pass_review(_script(), precheck)
        self.assertTrue(review["pass"])
        self.assertEqual(review["q1_route"], "local_pass_high_maturity")

    def test_low_maturity_category_does_not_skip_llm_q1(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        anchor = _anchor_card()
        anchor["category_execution_contract"]["display_family"] = "apparel_accessory"
        precheck = {
            "pass": True,
            "blocking_major": False,
            "l0_major_issues": [],
            "l0_minor_issues": [],
            "l1_major_issues": [],
            "l1_minor_issues": [],
            "warnings": [],
            "human_stiffness_check": {"hit_count": 0},
        }

        self.assertFalse(pipeline._should_use_local_q1_pass(
            {"type_guard": {"review_required": False}},
            anchor,
            precheck,
        ))


if __name__ == "__main__":
    unittest.main()
