#!/usr/bin/env python3
"""关键视觉防错锚点链路回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.json_parser import validate_anchor_card_payload  # noqa: E402
from core.pipeline import OriginalScriptPipeline  # noqa: E402
from core.prompts import build_anchor_card_prompt, build_script_prompt, build_script_review_prompt  # noqa: E402
from core.script_brief_builder import build_script_brief  # noqa: E402


class KeyVisualConstraintsTest(unittest.TestCase):
    def _base_anchor_card(self) -> dict:
        return {
            "product_positioning_one_liner": "短垂耳饰",
            "hard_anchors": [{"anchor": "短垂耳线", "reason_not_changeable": "商品轮廓可见", "confidence": "high"}],
            "display_anchors": [{"anchor": "耳侧佩戴结果", "why_must_show": "避免被生成成长流苏", "recommended_shot_type": "耳侧近景"}],
            "key_visual_constraints": [
                {"constraint": "末端落点不超过耳垂下方约2-3厘米", "confidence": "high", "basis": "图片中短垂比例可见"},
                {"constraint": "贴耳主体在耳垂附近，不生成长流苏主体", "confidence": "medium", "basis": "耳侧主体比例可观察"},
                {"constraint": "具体金属材质不强写", "confidence": "low", "basis": "图片无法确认材质"},
            ],
            "distortion_alerts": ["不要生成长流苏耳线"],
            "candidate_primary_selling_points": [{"selling_point": "短垂精致", "how_to_tell": "直接说短垂", "how_to_show": "耳侧近景", "risk_if_missed": "变成长款"}],
            "persona_suggestions": [{"persona": "自然分享", "why_fit": "商品小巧"}],
            "scene_suggestions": [{"scene": "家中镜前", "why_fit": "方便展示耳侧", "not_recommended_scene": "远景空镜"}],
            "camera_mandates": [{"stage": "opening", "must_do": "先露耳侧"}],
            "parameter_anchors": [],
            "structure_anchors": [],
            "operation_anchors": [],
            "fixation_result_anchors": [],
            "before_after_result_anchors": [],
            "scene_usage_anchors": [],
        }

    def test_anchor_card_validation_accepts_and_caps_key_visual_constraints(self) -> None:
        card = self._base_anchor_card()
        card["key_visual_constraints"] = [
            {"constraint": f"约束{i}", "confidence": "medium", "basis": "图片可见比例"}
            for i in range(6)
        ]

        validate_anchor_card_payload(card)

        self.assertEqual(len(card["key_visual_constraints"]), 3)

    def test_script_brief_only_includes_high_and_medium_constraints(self) -> None:
        card = self._base_anchor_card()

        brief = build_script_brief(
            product_type="耳饰",
            anchor_card=card,
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy={"primary_selling_point": "短垂精致"},
            expression_plan={},
        )

        constraints = brief["key_visual_constraints"]
        self.assertEqual([item["confidence"] for item in constraints], ["high", "medium"])
        self.assertIn("末端落点", constraints[0]["constraint"])
        self.assertNotIn("具体金属材质不强写", str(constraints))

    def test_prompts_include_key_visual_execution_rules(self) -> None:
        anchor_prompt = build_anchor_card_prompt(
            target_country="VN",
            target_language="vi",
            product_type="耳饰",
        )
        script_prompt = build_script_prompt("VN", "vi", "耳饰", {"key_visual_constraints": []})
        review_prompt = build_script_review_prompt("VN", "耳饰", {}, {}, {}, {}, {})

        self.assertIn("key_visual_constraints", anchor_prompt)
        self.assertIn("短垂比例、末端落点、贴耳主体、避免长流苏", anchor_prompt)
        self.assertIn("P7 必须严格遵守", script_prompt)
        self.assertIn("违反了其中 high / medium 约束", review_prompt)

    def test_final_video_anchor_segments_include_high_medium_constraints(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        card = self._base_anchor_card()

        segments = pipeline._build_final_video_prompt_anchor_segments(card, max_items=5)

        self.assertTrue(any("末端落点" in segment for segment in segments))
        self.assertTrue(any("贴耳主体" in segment for segment in segments))
        self.assertFalse(any("具体金属材质" in segment for segment in segments))

    def test_hair_accessory_profile_flows_into_script_brief(self) -> None:
        card = self._base_anchor_card()
        card.update(
            {
                "hair_accessory_subtype": "headband",
                "placement_zone": "top_head",
                "hold_scope": "decorative_only",
                "orientation": "wear_on_head",
                "primary_result": "more_volume",
            }
        )

        validate_anchor_card_payload(card)
        brief = build_script_brief(
            product_type="发箍",
            anchor_card=card,
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy={"primary_selling_point": "戴上后头顶更有形"},
            expression_plan={},
        )

        self.assertEqual(brief["hair_accessory_profile"]["hair_accessory_subtype"], "headband")
        self.assertEqual(brief["hair_accessory_profile"]["orientation"], "wear_on_head")

    def test_category_execution_contract_flows_into_script_brief(self) -> None:
        card = self._base_anchor_card()
        card["category_execution_contract"] = {
            "display_family": "hair_accessory",
            "product_subtype": "scrunchie",
            "use_case": "low_bun_or_loose_bun",
            "placement_zone": "bun_area",
            "hold_scope": "bun",
            "orientation": "wrap_around",
            "primary_visual_result": "简单低髻或松散盘发加上银灰褶皱发圈后，发型更柔和、更完整",
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
            "safe_shot_templates": [
                {
                    "id": "hair_scrunchie_bun_result_closeup",
                    "desc": "已戴好在低髻或松散发髻上的结果镜头",
                }
            ],
            "forbidden_actions": [
                {"id": "full_wraparound_process", "desc": "完整套入并环绕固定过程"},
                "手掌遮挡转场回到未戴状态",
            ],
            "result_priority": "先证明发髻更柔和、更完整，而不是证明低马尾更集中",
            "audio_policy": {
                "bgm_style": "柔和清爽的镜前日常整理感 BGM",
                "bgm_energy": "low",
                "voiceover_priority": "high",
                "sfx_policy": "音效只辅助布面触感和结果出现，不强化完整套入动作。",
                "allowed_sfx": ["subtle_pop", "soft_fabric_rustle"],
                "forbidden_sfx": ["soft_click", "clean_clip_click"],
                "sfx_timing_rules": ["首镜已佩戴结果出现时，可使用 subtle_pop 做极轻提示。"],
                "audio_negative_constraints": ["不得用 click 暗示不存在的夹合动作"],
            },
        }

        validate_anchor_card_payload(card)
        brief = build_script_brief(
            product_type="发圈",
            anchor_card=card,
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy={"primary_selling_point": "发髻更完整"},
            expression_plan={},
        )

        contract = brief["category_execution_contract"]
        self.assertEqual(contract["product_subtype"], "scrunchie")
        self.assertEqual(contract["placement_zone"], "bun_area")
        self.assertEqual(contract["operation_policy"], "result_first_process_avoid")
        self.assertEqual(contract["field_confidence"]["operation_policy"], "high")
        self.assertEqual(contract["safe_shot_templates"][0]["id"], "hair_scrunchie_bun_result_closeup")
        self.assertEqual(contract["forbidden_actions"][0]["id"], "full_wraparound_process")
        self.assertIn("soft_click", contract["audio_policy"]["forbidden_sfx"])

    def test_apparel_accessory_contract_extra_fields_flow_into_script_brief(self) -> None:
        card = self._base_anchor_card()
        card["category_execution_contract"] = {
            "display_family": "apparel_accessory",
            "product_subtype": "scarf_hat_set",
            "use_case": "winter_travel",
            "placement_zone": "scarf_hat_combo",
            "hold_scope": "winter_outfit_completion",
            "orientation": "full_set_wear",
            "primary_visual_result": "围巾和帽子一起佩戴后，上半身冬季穿搭更完整",
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
            "hat_risk_tier": "medium_risk",
            "set_relationship": "matching_color",
            "co_styling_hint": {"pair_with": ["winter_coat", "knit_sweater"]},
            "safe_shot_templates": ["已戴好帽子和围好围巾的上半身结果镜头"],
            "forbidden_actions": ["同时复杂戴帽和打结围巾", "强保暖、防风、防寒效果承诺"],
            "result_priority": "优先证明套装让冬季上半身穿搭更完整",
            "audio_policy": {
                "bgm_style": "低存在感生活化 BGM",
                "bgm_energy": "low",
                "voiceover_priority": "high",
                "sfx_policy": "以 BGM 为主，SFX 可不加",
                "allowed_sfx": [],
                "forbidden_sfx": ["强风音", "夸张 whoosh"],
                "sfx_timing_rules": [],
                "audio_negative_constraints": ["不得用音效暗示强保暖"],
            },
        }

        validate_anchor_card_payload(card)
        brief = build_script_brief(
            product_type="围巾帽子套装",
            anchor_card=card,
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy={"primary_selling_point": "冬季上半身更完整"},
            expression_plan={},
        )

        contract = brief["category_execution_contract"]
        self.assertEqual(contract["display_family"], "apparel_accessory")
        self.assertEqual(contract["product_subtype"], "scarf_hat_set")
        self.assertEqual(contract["season_context"]["primary_season"], "winter")
        self.assertEqual(contract["hat_risk_tier"], "medium_risk")
        self.assertEqual(contract["set_relationship"], "matching_color")
        self.assertIn("winter_coat", contract["co_styling_hint"]["pair_with"])


if __name__ == "__main__":
    unittest.main()
