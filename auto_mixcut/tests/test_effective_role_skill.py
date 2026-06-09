from __future__ import annotations

import unittest

from auto_mixcut.skills.ai_segment_factory_config import AISegmentFactoryConfig, SegmentTypeRule
from auto_mixcut.skills.effective_role_skill import _compute_roles
from auto_mixcut.skills.hard_subtitle_policy import classify_text_overlay


def _config() -> AISegmentFactoryConfig:
    return AISegmentFactoryConfig(
        segment_type_rules={
            "tryon_result": SegmentTypeRule(
                risk_level="high",
                default_roles=["result", "hero"],
                possible_roles=["result", "hero", "scene", "ending"],
                core_allowed="yes",
                anchor_strength="strict",
                require_reference_image=True,
                preferred_generation_type="image_to_video",
                batch_friendly="low",
                require_frame_consistency=True,
            )
        }
    )


class EffectiveRoleSkillTest(unittest.TestCase):
    def test_ai_strict_pass_medium_risk_can_keep_core_roles(self):
        roles, reason = _compute_roles(
            {
                "source_type": "ai_generated",
                "segment_type": "tryon_result",
                "anchor_match_level": "strict_pass",
                "frame_consistency_status": "pass",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "high",
                "primary_shot_role": "hero",
                "secondary_roles_json": ["result"],
            },
            _config(),
        )

        self.assertEqual(roles, ["hero", "result"])
        self.assertIn("strict_pass", reason)

    def test_ai_uncertain_anchor_stays_scene_ending_only(self):
        roles, reason = _compute_roles(
            {
                "source_type": "ai_generated",
                "segment_type": "tryon_result",
                "anchor_match_level": "uncertain",
                "frame_consistency_status": "pass",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "high",
                "primary_shot_role": "hero",
            },
            _config(),
        )

        self.assertEqual(roles, ["scene", "ending"])
        self.assertIn("uncertain", reason)

    def test_ai_anchor_fail_blocks_repairable_bottom_subtitle_roles(self):
        roles, reason = _compute_roles(
            {
                "source_type": "ai_generated",
                "segment_type": "tryon_result",
                "anchor_match_level": "fail",
                "frame_consistency_status": "pass",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "needs_processing",
                "product_visibility": "high",
                "confidence": "high",
                "primary_shot_role": "result",
                "text_overlay_risk": "bottom_caption_repairable",
                "reason": "底部字幕可裁剪，但商品锚点已失败",
            },
            _config(),
        )

        self.assertEqual(roles, [])
        self.assertEqual(reason, "ai anchor fail")

    def test_ai_strict_pass_missing_segment_type_infers_roles_from_tag(self):
        roles, reason = _compute_roles(
            {
                "source_type": "ai_generated",
                "anchor_match_level": "strict_pass",
                "frame_consistency_status": "pass",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "high",
                "primary_shot_role": "result",
                "secondary_roles_json": ["hero", "detail"],
            },
            _config(),
        )

        self.assertEqual(roles, ["detail", "ending", "hero", "result", "scene"])
        self.assertIn("missing segment_type", reason)

    def test_trusted_exact_sku_creator_keeps_roles_for_soft_anchor_uncertainty(self):
        roles, reason = _compute_roles(
            {
                "source_type": "authorized_creator",
                "source_trust_level": "high",
                "product_binding_type": "exact_sku",
                "product_match_status": "trusted_by_source",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "medium",
                "primary_shot_role": "hero",
                "secondary_roles_json": ["detail"],
                "reason": "商品锚点不确定需人工确认，但外套上身和细节清楚",
            },
            _config(),
        )

        self.assertEqual(roles, ["detail", "ending", "hero", "scene"])
        self.assertIn("trusted exact_sku", reason)

    def test_trusted_exact_sku_creator_still_blocks_hard_wrong_category(self):
        roles, reason = _compute_roles(
            {
                "source_type": "authorized_creator",
                "source_trust_level": "high",
                "product_binding_type": "exact_sku",
                "product_match_status": "trusted_by_source",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "medium",
                "primary_shot_role": "hero",
                "reason": "商品锚点不确定且错品类，可能不是外套",
            },
            _config(),
        )

        self.assertEqual(roles, [])
        self.assertEqual(reason, "medium/high risk")

    def test_low_trust_exact_sku_soft_anchor_can_enter_without_hero_by_default(self):
        roles, reason = _compute_roles(
            {
                "source_type": "douyin_repost",
                "source_trust_level": "low",
                "product_binding_type": "exact_sku",
                "product_match_status": "uncertain",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "medium",
                "primary_shot_role": "hero",
                "secondary_roles_json": ["detail", "result"],
                "reason": "商品锚点缺失，需人工确认，但商品主体清楚",
            },
            _config(),
        )

        self.assertEqual(roles, ["detail", "ending", "result", "scene"])
        self.assertIn("low trust exact_sku", reason)

    def test_low_trust_exact_sku_allows_hero_only_when_first_slot_confident(self):
        roles, reason = _compute_roles(
            {
                "source_type": "competitor",
                "source_trust_level": "low",
                "product_binding_type": "exact_sku",
                "product_match_status": "uncertain",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "high",
                "primary_shot_role": "hero",
                "reason": "商品锚点不确定需人工确认，但首镜产品完整清晰",
            },
            _config(),
        )

        self.assertEqual(roles, ["ending", "hero", "scene"])
        self.assertIn("low trust exact_sku", reason)

    def test_low_trust_exact_sku_still_blocks_hard_wrong_category(self):
        roles, reason = _compute_roles(
            {
                "source_type": "douyin_repost",
                "source_trust_level": "low",
                "product_binding_type": "exact_sku",
                "product_match_status": "uncertain",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "yes",
                "product_visibility": "high",
                "confidence": "high",
                "primary_shot_role": "hero",
                "reason": "商品锚点不确定且错品类，可能不是外套",
            },
            _config(),
        )

        self.assertEqual(roles, [])
        self.assertEqual(reason, "medium/high risk")

    def test_bottom_subtitle_is_repairable_but_not_blocked(self):
        roles, reason = _compute_roles(
            {
                "source_type": "authorized_creator",
                "source_trust_level": "high",
                "product_binding_type": "exact_sku",
                "product_match_status": "trusted_by_source",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "needs_processing",
                "product_visibility": "high",
                "confidence": "medium",
                "primary_shot_role": "detail",
                "secondary_roles_json": ["result"],
                "text_overlay_risk": "bottom_caption_repairable",
                "reason": "外套细节清楚，底部越南语字幕不遮挡商品",
            },
            _config(),
        )

        self.assertEqual(roles, ["detail", "ending", "result", "scene"])
        self.assertIn("crop before render", reason)

    def test_large_subtitle_is_hard_blocked(self):
        roles, reason = _compute_roles(
            {
                "source_type": "authorized_creator",
                "source_trust_level": "high",
                "product_binding_type": "exact_sku",
                "product_match_status": "trusted_by_source",
            },
            {},
            {
                "risk_level": "medium",
                "mixcut_usability": "needs_processing",
                "product_visibility": "high",
                "confidence": "medium",
                "primary_shot_role": "detail",
                "text_overlay_risk": "large_obstructive_text",
                "reason": "中部大面积外文字幕遮挡商品主体",
            },
            _config(),
        )

        self.assertEqual(roles, [])
        self.assertEqual(reason, "hard subtitle unusable")

    def test_subtitle_policy_infers_bottom_caption_from_reason(self):
        overlay = classify_text_overlay({"reason": "衣服细节清楚，底部文字 Chi tiet dep mat 不遮挡商品"})

        self.assertEqual(overlay["risk"], "bottom_caption_repairable")
        self.assertEqual(overlay["language"], "vietnamese")

    def test_explicit_foreign_caption_is_repairable_when_reason_says_bottom_non_obstructive(self):
        overlay = classify_text_overlay({
            "text_overlay_risk": "foreign_language_caption",
            "reason": "画面底部有越南语字幕 Chi tiet dep mat，不遮挡商品主体",
        })

        self.assertEqual(overlay["risk"], "bottom_caption_repairable")
        self.assertEqual(overlay["language"], "vietnamese")


if __name__ == "__main__":
    unittest.main()
