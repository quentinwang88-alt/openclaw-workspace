from __future__ import annotations

import unittest

from auto_mixcut.skills.ai_anchor_check_skill import _evaluate_anchor_match, _is_missing_anchor_uncertain


class AIAnchorCheckSkillTest(unittest.TestCase):
    def test_forbidden_color_mismatch_blocks_local_strict_fallback(self):
        anchor = {
            "core_visual_points": ["黑色PU皮革材质", "短款宽松", "翻领机车夹克"],
            "forbidden_mismatch": ["禁止出现棕色、白色、彩色等非黑色主商品替代"],
        }
        tag = {
            "primary_shot_role": "result",
            "product_visibility": "high",
            "confidence": "high",
            "risk_level": "low",
            "mixcut_usability": "yes",
            "reason": "模特穿着棕色皮夹克展示上身效果，商品清晰。",
        }
        segment = {"frame_consistency_status": "pass"}
        level, reason, core_roles, soft_roles = _evaluate_anchor_match(anchor, tag, segment, {})

        self.assertEqual(level, "fail")
        self.assertIn("棕色", reason)
        self.assertEqual(core_roles, [])
        self.assertEqual(soft_roles, [])

    def test_missing_name_category_anchor_reason_triggers_local_fallback(self):
        self.assertTrue(_is_missing_anchor_uncertain("uncertain", "未提供商品名称、类目和锚点，无法确认是否匹配具体商品"))

    def test_prompt_package_exact_sku_ai_candidate_can_pass_core_with_medium_confidence(self):
        anchor = {
            "core_visual_points": ["小香风短款女士外套", "轻薄开衫式夹克轮廓"],
            "forbidden_mismatch": ["男装外套或儿童服装", "裤子、裙子、连衣裙等非上衣类商品"],
        }
        tag = {
            "primary_shot_role": "result",
            "secondary_roles_json": ["hero", "detail", "scene"],
            "product_visibility": "high",
            "confidence": "medium",
            "risk_level": "medium",
            "mixcut_usability": "yes",
            "reason": "模特上身展示短款小香风外套，主体清晰，需人工确认具体商品锚点。",
        }
        segment = {
            "source_type": "ai_generated",
            "product_binding_type": "exact_sku",
            "segment_type": "product_display",
            "slot_role": "hero",
            "prompt_package_id": "SPK_TEST",
            "frame_consistency_status": "pass",
        }

        level, reason, core_roles, soft_roles = _evaluate_anchor_match(anchor, tag, segment, {})

        self.assertEqual(level, "strict_pass")
        self.assertIn("Prompt Package exact_sku", reason)
        self.assertIn("hero", core_roles)
        self.assertIn("scene", soft_roles)


if __name__ == "__main__":
    unittest.main()
