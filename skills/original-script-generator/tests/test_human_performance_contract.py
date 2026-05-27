#!/usr/bin/env python3
"""human_performance_contract 链路回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.json_parser import (  # noqa: E402
    JSONParseError,
    validate_persona_style_emotion_pack_payload,
    validate_script_payload,
    validate_variant_payload,
    validate_video_prompt_payload,
)
from core.business_rules import validate_script_direction_separation  # noqa: E402
from core.prompts import build_final_video_prompt_prompt, build_script_prompt, build_styling_plan_prompt  # noqa: E402
from core.pipeline import OriginalScriptPipeline  # noqa: E402
from core.script_brief_builder import build_script_brief  # noqa: E402


def _hair_anchor_card() -> dict:
    return {
        "product_positioning_one_liner": "镜前发饰整理",
        "category_execution_contract": {
            "display_family": "hair_accessory",
            "product_subtype": "scrunchie",
            "use_case": "low_bun_or_loose_bun",
            "placement_zone": "bun_area",
            "hold_scope": "bun",
            "orientation": "wrap_around",
            "primary_visual_result": "低髻加上发圈后发型更完整",
            "operation_policy": "result_first_process_avoid",
            "forbidden_actions": ["完整套入并环绕固定过程"],
        },
    }


def _winter_scarf_anchor_card() -> dict:
    return {
        "product_positioning_one_liner": "冬季围巾出门搭配",
        "category_execution_contract": {
            "display_family": "apparel_accessory",
            "product_subtype": "scarf",
            "use_case": "winter_outing",
            "placement_zone": "neck_shoulder",
            "hold_scope": "winter_outfit_completion",
            "orientation": "wrapped_neck",
            "primary_visual_result": "围上后脖颈、肩部和上半身区域更有冬季氛围，和外套或针织上衣搭配后整体更完整",
            "operation_policy": "result_first_process_avoid",
            "field_confidence": {
                "product_subtype": "high",
                "use_case": "medium",
                "placement_zone": "high",
                "hold_scope": "medium",
                "orientation": "medium",
                "primary_visual_result": "high",
                "operation_policy": "high",
            },
            "season_context": {"primary_season": "winter", "weather_signal": "cold"},
            "hat_risk_tier": "unknown",
            "set_relationship": "unknown",
            "co_styling_hint": {"pair_with": ["winter_coat", "knit_sweater"]},
            "safe_shot_templates": ["已围好后的胸口以上中近景，围巾自然覆盖脖颈或肩部"],
            "forbidden_actions": ["完整绕脖多圈并打结的复杂过程", "暗示未确认的强保暖、防寒、防风效果"],
            "result_priority": "优先证明冬季上半身穿搭更完整，而不是证明复杂围法或强保暖功能",
            "audio_policy": {
                "bgm_style": "低存在感冬季出门自检感 BGM",
                "bgm_energy": "low",
                "voiceover_priority": "high",
                "sfx_policy": "少量布料轻响即可，不强化功能测试",
                "allowed_sfx": ["very_light_fabric_rustle"],
                "forbidden_sfx": ["夸张风声", "大片 whoosh", "闪光音"],
                "sfx_timing_rules": ["手轻整理围巾边缘时可使用 very_light_fabric_rustle"],
                "audio_negative_constraints": ["不得用强冷风音效暗示防风防寒"],
            },
        },
    }


def _ear_anchor_card() -> dict:
    return {
        "product_positioning_one_liner": "耳侧细节让脸侧更精致",
        "category_execution_contract": {
            "display_family": "ear_accessory",
            "product_subtype": "unknown",
            "use_case": "unknown",
            "placement_zone": "face_side",
            "hold_scope": "decorative_only",
            "orientation": "unknown",
            "primary_visual_result": "佩戴后耳侧细节更清楚，脸侧线条更完整",
            "operation_policy": "static_result_only",
            "forbidden_actions": ["展示穿耳或扣耳针的复杂过程"],
        },
    }


def _apparel_anchor_card() -> dict:
    return {
        "product_positioning_one_liner": "镜前确认上身版型",
        "category_execution_contract": {
            "display_family": "apparel",
            "product_subtype": "unknown",
            "use_case": "before_going_out",
            "placement_zone": "upper_body",
            "hold_scope": "upper_body_styling",
            "orientation": "unknown",
            "primary_visual_result": "穿上后上半身版型和整体比例更清楚",
            "operation_policy": "result_first_process_avoid",
            "forbidden_actions": ["复杂换装过程", "夸张走秀转圈"],
        },
    }


def _persona_pack() -> dict:
    return {
        "persona_state": "R1 轻分享型",
        "appearance_anchor": "真实顺眼",
        "attractiveness_boundary": "不抢商品",
        "hairstyle_rule": "头发和发饰位置清楚",
        "makeup_rule": "淡妆",
        "clothing_rule": "低饱和上衣",
        "accessory_rule": "不叠加抢眼配饰",
        "emotion_progression": "观察到满意再分享",
        "movement_style": "镜前轻确认",
        "styling_completion_tag": "干净日常感",
        "persona_visual_tone": "轻分享型",
        "styling_key_anchor": "头部区域清爽",
        "emotion_arc_tag": "观察 → 满意 → 分享",
        "anti_template_warnings": ["不要全程固定微笑"],
        "human_performance_contract": {
            "performance_family": "mirror_hair_check",
            "persona_mode": "mirror_self_check_with_soft_friend_share",
            "expression_arc": ["0-3s：轻微观察", "3-8s：满意", "8-15s：分享"],
            "gaze_plan": ["mirror", "hair_accessory_position", "camera", "mirror_full_result"],
            "gaze_rule": {"min_points_required": 3, "final_point_options": ["camera", "mirror_full_result"]},
            "micro_reaction_beats": ["嘴角自然上扬", "轻点头确认"],
            "body_language_beats": ["侧头 15-30 度展示发饰位置"],
            "product_interaction_beats": ["只轻整理发饰边缘或附近头发"],
            "relatable_moment": "出门前整理头发",
            "performance_intensity": "low_to_medium",
            "forbidden_performance": ["全程固定微笑看镜头"],
            "active_micro_reaction_limit": 3,
            "scene_seed_brief": {
                "enabled": True,
                "display_family": "hair_accessory",
                "seed_goal": "让人物像在镜前确认发型完整度",
                "strategy_by_script_role": {
                    "S1": {
                        "seed_mode": "casual_self_check",
                        "moment_bias": "随手检查",
                        "tension_bias": "轻微犹豫",
                        "camera_gaze_bias": "少",
                        "payoff_bias": "自然确认",
                    },
                    "S4": {
                        "seed_mode": "visual_hook_first",
                        "moment_bias": "首镜先给更明显结果",
                        "tension_bias": "第一眼有惊喜但不过度",
                        "camera_gaze_bias": "中高",
                        "payoff_bias": "惊艳感收回自然分享",
                    },
                },
                "moment_hints": ["出门前看镜中发型"],
                "small_tension_hints": ["发髻看起来有点随意"],
                "micro_behavior_boundary": {
                    "safe_behavior_hints": ["轻看发髻边缘"],
                    "risk_boundary": ["不展示完整套入过程"],
                },
                "payoff_direction": "确认发型更完整",
                "anti_template_guidance": ["不要每条都写成固定微笑展示"],
            },
        },
    }


class HumanPerformanceContractTest(unittest.TestCase):
    def test_hair_accessory_p3_prompt_injects_enabled_profile(self) -> None:
        prompt = build_styling_plan_prompt(
            target_country="VN",
            target_language="vi",
            product_type="发圈",
            anchor_card_json=_hair_anchor_card(),
        )

        self.assertIn("performance_profile_json", prompt)
        self.assertIn('"enable_human_performance_contract":true', prompt)
        self.assertIn("mirror_hair_check", prompt)
        self.assertIn("human_performance_contract", prompt)

    def test_ear_accessory_p3_prompt_is_enabled(self) -> None:
        prompt = build_styling_plan_prompt(
            target_country="VN",
            target_language="vi",
            product_type="耳环",
            anchor_card_json=_ear_anchor_card(),
        )

        self.assertIn('"enable_human_performance_contract":true', prompt)
        self.assertIn('"display_family":"ear_accessory"', prompt)
        self.assertIn("face_side_detail_check", prompt)
        self.assertIn("ear_side_detail", prompt)
        self.assertIn("scene_seed_brief", prompt)
        self.assertIn("strategy_by_script_role", prompt)
        self.assertIn("visual_hook_first", prompt)
        self.assertIn("让人物像在镜前确认脸侧细节是否补完整", prompt)

    def test_apparel_p3_prompt_is_enabled(self) -> None:
        prompt = build_styling_plan_prompt(
            target_country="TH",
            target_language="th",
            product_type="上装",
            anchor_card_json=_apparel_anchor_card(),
        )

        self.assertIn('"enable_human_performance_contract":true', prompt)
        self.assertIn('"display_family":"apparel"', prompt)
        self.assertIn("outfit_fit_check", prompt)
        self.assertIn("mirror_full_body", prompt)
        self.assertIn("让人物像在试穿后确认版型和比例", prompt)

    def test_apparel_accessory_p3_prompt_selects_scarf_variant(self) -> None:
        prompt = build_styling_plan_prompt(
            target_country="TH",
            target_language="th",
            product_type="围巾",
            anchor_card_json=_winter_scarf_anchor_card(),
        )

        self.assertIn('"enable_human_performance_contract":true', prompt)
        self.assertIn('"display_family":"apparel_accessory"', prompt)
        self.assertIn('"selected_variant":"scarf_variant"', prompt)
        self.assertIn("winter_outfit_accessory_check", prompt)
        self.assertIn("手轻整理围巾边缘，确认垂感和层次", prompt)
        self.assertIn("让人物像在冬季出门前确认上半身穿搭完整度", prompt)

    def test_persona_validator_and_script_brief_preserve_contract(self) -> None:
        pack = _persona_pack()

        validate_persona_style_emotion_pack_payload(pack)
        brief = build_script_brief(
            product_type="发圈",
            anchor_card=_hair_anchor_card(),
            opening_strategies={},
            persona_style_emotion_pack=pack,
            final_strategy={"primary_selling_point": "发髻更完整"},
            expression_plan={},
        )

        contract = brief["human_performance_contract"]
        self.assertEqual(contract["performance_family"], "mirror_hair_check")
        self.assertEqual(contract["active_micro_reaction_limit"], 3)
        self.assertIn("hair_accessory_position", contract["gaze_plan"])
        self.assertIn("全程固定微笑看镜头", contract["forbidden_performance"])
        self.assertTrue(contract["scene_seed_brief"]["enabled"])
        self.assertIn("出门前看镜中发型", contract["scene_seed_brief"]["moment_hints"])
        self.assertEqual(
            contract["scene_seed_brief"]["strategy_by_script_role"]["S4"]["seed_mode"],
            "visual_hook_first",
        )
        self.assertNotIn("performance_plan_by_shot", brief)

    def test_p7_prompt_receives_compact_scene_seed_brief_and_requires_scene_seed(self) -> None:
        pack = _persona_pack()
        brief = build_script_brief(
            product_type="发圈",
            anchor_card=_hair_anchor_card(),
            opening_strategies={},
            persona_style_emotion_pack=pack,
            final_strategy={"strategy_id": "S1", "primary_selling_point": "发髻更完整", "proof_path": "A_result_detail_only"},
            expression_plan={},
        )

        prompt = build_script_prompt(
            target_country="VN",
            target_language="vi",
            product_type="发圈",
            script_brief_json=brief,
        )

        self.assertIn("scene_seed_brief", prompt)
        self.assertIn("current_script_strategy", prompt)
        self.assertIn("casual_self_check", prompt)
        self.assertIn('"scene_seed"', prompt)
        self.assertIn("moment", prompt)
        self.assertIn("small_tension", prompt)

    def test_direction_separation_flags_s4_scene_seed_too_close_to_s1(self) -> None:
        s1_script = {
            "scene_seed": {
                "moment": "出门前在镜前确认耳侧",
                "small_tension": "脸侧有点空",
                "micro_behavior": "靠近镜子看耳侧，再退半步看整体",
                "payoff_feeling": "自然确认刚好补完整体",
            },
            "opening_design": {"visual": "镜前耳侧结果"},
            "storyboard": [
                {"shot_content": "镜前侧脸佩戴后结果", "person_action": "看镜子", "task_type": "attention"},
                {"shot_content": "脸侧比例结果", "person_action": "小幅转头", "task_type": "proof"},
                {"shot_content": "整体关系", "person_action": "退半步", "task_type": "proof"},
                {"shot_content": "轻分享", "person_action": "看镜头", "task_type": "bridge"},
            ],
        }
        s4_script = {
            "scene_seed": {
                "moment": "出门前在镜中看脸侧",
                "small_tension": "整体太素，脸侧少一点",
                "micro_behavior": "靠近镜子确认耳侧反光，再退半步",
                "payoff_feeling": "自然确认耳侧亮点刚好补上",
            },
            "opening_design": {"visual": "暖光窗边强结果"},
            "storyboard": [
                {"shot_content": "暖光窗边耳饰强结果", "person_action": "轻转头", "task_type": "attention"},
                {"shot_content": "耳侧细节近景", "person_action": "头部小幅转动", "task_type": "proof"},
                {"shot_content": "脸侧整体关系", "person_action": "退半步", "task_type": "proof"},
                {"shot_content": "轻分享", "person_action": "看镜头", "task_type": "bridge"},
            ],
        }

        issue = validate_script_direction_separation(
            final_strategy={"strategy_id": "S4"},
            script_json=s4_script,
            existing_scripts={"S1": s1_script},
        )

        self.assertIn("scene_seed", issue or "")

    def test_scene_seed_liveliness_precheck_flags_static_storyboard(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pack = _persona_pack()
        script = {
            "scene_seed": {
                "moment": "出门前看镜中发型",
                "small_tension": "发髻看起来有点随意",
                "micro_behavior": "轻看发髻边缘",
                "payoff_feeling": "确认发型更完整",
            },
            "storyboard": [
                {
                    "shot_no": 1,
                    "shot_content": "静物发圈细节",
                    "person_action": "人物不入镜",
                    "performance": {"gaze": "none", "expression_or_micro_reaction": "无人物表情", "body_language": "无人物身体动作"},
                },
                {
                    "shot_no": 2,
                    "shot_content": "静物发圈细节",
                    "person_action": "人物不入镜",
                    "performance": {"gaze": "none", "expression_or_micro_reaction": "无人物表情", "body_language": "无人物身体动作"},
                },
                {
                    "shot_no": 3,
                    "shot_content": "静物发圈细节",
                    "person_action": "人物不入镜",
                    "performance": {"gaze": "none", "expression_or_micro_reaction": "无人物表情", "body_language": "无人物身体动作"},
                },
                {
                    "shot_no": 4,
                    "shot_content": "镜中结果",
                    "person_action": "人物看镜子",
                    "performance": {"gaze": "mirror", "expression_or_micro_reaction": "轻确认", "body_language": "肩膀放松"},
                },
            ],
        }

        check, major, minor = pipeline._precheck_scene_seed_liveliness(
            anchor_card=_hair_anchor_card(),
            persona_style_emotion_pack=pack,
            script_json=script,
        )

        self.assertTrue(check["static_or_pose_heavy"])
        self.assertTrue(major)
        self.assertFalse(minor)

    def test_script_validator_backfills_and_accepts_performance_field(self) -> None:
        storyboard = []
        for index, task in enumerate(["hook", "proof", "proof", "decision"], 1):
            storyboard.append(
                {
                    "shot_no": index,
                    "duration": "3s",
                    "shot_content": "已戴好发圈的低髻结果镜头",
                    "shot_purpose": "证明发型更完整",
                    "subtitle_text_target_language": "",
                    "subtitle_text_zh": "",
                    "voiceover_text_target_language": "Tóc nhìn gọn hơn",
                    "voiceover_text_zh": "",
                    "spoken_line_task": task,
                    "person_action": "人物轻微侧头看镜子里的发髻",
                    "performance": "人物先看镜子里的发髻，表情轻微观察，随后嘴角自然上扬",
                    "style_note": "家中镜前自然分享",
                    "anchor_reference": "低髻发圈结果",
                    "task_type": "attention" if task == "hook" else "proof" if task == "proof" else "bridge",
                    "ai_shot_risk": "low",
                    "replacement_template_id": "",
                }
            )
        script = {
            "script_positioning": {"script_title": "发圈结果", "direction_type": "结果成立型", "core_primary_selling_point": "发髻更完整"},
            "opening_design": {"opening_mode": "结果先给", "first_frame": "已佩戴结果", "expression_entry": "发型更完整", "first_line_type": "结果判断"},
            "full_15s_flow": [
                {"stage": "opening", "time_range": "0-3s", "task": "hook", "summary": "结果先给"},
                {"stage": "middle", "time_range": "4-9s", "task": "proof", "summary": "细节复核"},
                {"stage": "ending", "time_range": "10-12s", "task": "decision", "summary": "轻确认"},
            ],
            "storyboard": storyboard,
            "execution_constraints": {
                "visual_style": "家中镜前自然光",
                "person_constraints": "人物真实轻分享",
                "styling_constraints": "头发和发圈位置清楚",
                "tone_completion_constraints": "干净日常感",
                "scene_constraints": "家中镜前",
                "emotion_progression_constraints": "观察到满意",
                "camera_focus": "发圈和低髻结果",
                "product_priority_principle": "发圈是主角",
                "realism_principle": "不拍完整套入过程",
            },
            "rhythm_checkpoints": {
                "hook_complete_by": "3s",
                "core_proof_start_between": "4-8s",
                "decision_signal_by": "12s",
                "risk_resolution_decision_by": "9s_or_not_applicable",
            },
            "audio_layer": {
                "bgm_style": "柔和镜前整理感",
                "bgm_energy": "low",
                "sfx_cues": [],
                "voiceover_priority": "high",
                "mix_note": "不盖过口播",
                "audio_negative_constraints": ["不要 click"],
            },
            "negative_constraints": ["不拍完整套入过程"],
        }

        validate_script_payload(script)

        self.assertEqual(script["proof_path"], "A_result_detail_only")
        self.assertEqual(len(script["shot_skeleton"]), len(script["storyboard"]))
        self.assertIn("performance", script["storyboard"][0])
        self.assertIsInstance(script["storyboard"][0]["performance"], dict)
        self.assertIn("gaze", script["storyboard"][0]["performance"])

    def test_script_validator_rejects_chinese_target_voiceover(self) -> None:
        storyboard = []
        for index, task in enumerate(["hook", "proof", "proof", "decision"], 1):
            storyboard.append(
                {
                    "shot_no": index,
                    "duration": "3s",
                    "shot_content": "已戴好发圈的低髻结果镜头",
                    "shot_purpose": "证明发型更完整",
                    "subtitle_text_target_language": "",
                    "subtitle_text_zh": "",
                    "voiceover_text_target_language": "戴上之后发型更完整",
                    "voiceover_text_zh": "",
                    "spoken_line_task": task,
                    "person_action": "人物轻微侧头看镜子里的发髻",
                    "performance": {
                        "gaze": "mirror",
                        "expression_or_micro_reaction": "嘴角轻轻放松",
                        "body_language": "肩膀放松",
                        "product_interaction": "轻整理发饰边缘",
                    },
                    "style_note": "家中镜前自然分享",
                    "anchor_reference": "低髻发圈结果",
                    "task_type": "attention" if task == "hook" else "proof" if task == "proof" else "bridge",
                    "ai_shot_risk": "low",
                    "replacement_template_id": "",
                }
            )
        script = {
            "proof_path": "A_result_detail_only",
            "performance_strategy": "镜前轻确认",
            "shot_skeleton": [
                {"shot_index": index, "time_range": f"{index}-{index + 1}s", "role": "proof", "shot_purpose": "证明结果", "proof_path": "A_result_detail_only"}
                for index in range(1, 5)
            ],
            "script_positioning": {"script_title": "发圈结果", "direction_type": "结果成立型", "core_primary_selling_point": "发髻更完整"},
            "opening_design": {"opening_mode": "结果先给", "first_frame": "已佩戴结果", "expression_entry": "发型更完整", "first_line_type": "结果判断"},
            "full_15s_flow": [
                {"stage": "opening", "time_range": "0-3s", "task": "hook", "summary": "结果先给"},
                {"stage": "middle", "time_range": "4-9s", "task": "proof", "summary": "细节复核"},
                {"stage": "ending", "time_range": "10-12s", "task": "decision", "summary": "轻确认"},
            ],
            "storyboard": storyboard,
            "execution_constraints": {
                "visual_style": "家中镜前自然光",
                "person_constraints": "人物真实轻分享",
                "styling_constraints": "头发和发圈位置清楚",
                "tone_completion_constraints": "干净日常感",
                "scene_constraints": "家中镜前",
                "emotion_progression_constraints": "观察到满意",
                "camera_focus": "发圈和低髻结果",
                "product_priority_principle": "发圈是主角",
                "realism_principle": "不拍完整套入过程",
            },
            "rhythm_checkpoints": {
                "hook_complete_by": "3s",
                "core_proof_start_between": "4-8s",
                "decision_signal_by": "12s",
                "risk_resolution_decision_by": "9s_or_not_applicable",
            },
            "audio_layer": {
                "bgm_style": "柔和镜前整理感",
                "bgm_energy": "low",
                "sfx_cues": [],
                "voiceover_priority": "high",
                "mix_note": "不盖过口播",
                "audio_negative_constraints": ["不要 click"],
            },
            "negative_constraints": ["不拍完整套入过程"],
        }

        with self.assertRaisesRegex(JSONParseError, "不得包含中文"):
            validate_script_payload(script, target_language="越南语")

    def test_variant_validator_checks_every_shot_voiceover_language(self) -> None:
        payload = {
            "variant_count": 1,
            "variants": [
                {
                    "variant_id": "V1",
                    "variant_no": 1,
                    "variant_strength": "light",
                    "variant_focus": "opening",
                    "source_script_id": "S1",
                    "source_strategy_id": "strategy_s1",
                    "strategy_id": "S1",
                    "strategy_name": "结果成立",
                    "primary_selling_point": "发型更完整",
                    "final_video_script_prompt": {
                        "video_setup": {
                            "video_theme": "镜前结果确认",
                            "product_focus": "低髻发圈结果",
                            "person_final": "真实轻分享",
                            "outfit_final": "低饱和上衣",
                            "scene_final": "家中镜前",
                            "emotion_final": "轻满意",
                            "overall_style": "自然不硬广",
                        },
                        "shot_execution": [
                            {
                                "shot_no": 1,
                                "duration": "0-3s",
                                "visual": "已戴好发圈的低髻结果",
                                "person_action": "人物看镜中发髻",
                                "product_focus": "发圈和低髻关系",
                                "voiceover": "戴上之后发型更完整",
                            },
                            {
                                "shot_no": 2,
                                "duration": "3-6s",
                                "visual": "发圈细节近景",
                                "person_action": "人物轻整理边缘",
                                "product_focus": "褶皱和发髻关系",
                                "voiceover": "Tóc nhìn gọn hơn",
                            },
                            {
                                "shot_no": 3,
                                "duration": "6-10s",
                                "visual": "镜中整体复核",
                                "person_action": "人物半步后退",
                                "product_focus": "整体完成度",
                                "voiceover": "Nhìn mềm hơn một chút",
                            },
                            {
                                "shot_no": 4,
                                "duration": "10-15s",
                                "visual": "出门前轻确认",
                                "person_action": "人物短暂看镜头",
                                "product_focus": "低髻完成度",
                                "voiceover": "Vậy là đủ gọn rồi",
                            },
                        ],
                        "style_boundaries": ["自然镜前，不硬广"],
                    },
                }
            ],
        }

        with self.assertRaisesRegex(JSONParseError, "不得包含中文"):
            validate_variant_payload(payload, expected_count=1, expected_variant_ids=["V1"], target_language="越南语")

    def test_final_video_prompt_preserves_performance(self) -> None:
        prompt = {
            "video_setup": "15 秒家中镜前发圈结果视频",
            "shot_execution": [
                {
                    "shot_no": index,
                    "duration": "3s",
                    "shot_content": "低髻发圈结果镜头",
                    "voiceover_text_target_language": "Tóc nhìn gọn hơn",
                    "voiceover_text_zh": "",
                    "spoken_line_task": task,
                    "person_action": "人物轻微侧头",
                    "performance": "眼神先看镜子里的发髻，再短暂看向镜头像朋友分享",
                    "style_note": "自然镜前",
                }
                for index, task in enumerate(["hook", "proof", "proof", "decision"], 1)
            ],
            "execution_boundary": "保留眼神路径和轻微表情变化",
        }

        validate_video_prompt_payload(prompt)

        self.assertIn("performance", prompt["shot_execution"][0])
        self.assertIsInstance(prompt["shot_execution"][0]["performance"], dict)
        self.assertIn("gaze", prompt["shot_execution"][0]["performance"])


if __name__ == "__main__":
    unittest.main()
