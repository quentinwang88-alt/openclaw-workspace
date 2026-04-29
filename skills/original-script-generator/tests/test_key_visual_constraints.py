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

        self.assertEqual(len(card["key_visual_constraints"]), 5)

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


if __name__ == "__main__":
    unittest.main()
