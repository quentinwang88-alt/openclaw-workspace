from __future__ import annotations

import unittest

from scripts.run_creative_full_voiceover_stage0 import (
    assemble_selected_candidate,
    build_payload,
    build_selected_voiceover_plan,
    validate_result,
)


class CreativeFullVoiceoverStage0Test(unittest.TestCase):
    def _product(self):
        contract = {
            "schema_version": "voiceover-expression-contract-v2",
            "content_mainline": "短外套的可见细节",
            "argument_contract": {
                "content": {
                    "audience_tension": {"text": "基础穿搭会不会太空？"},
                    "value_proposition": {"text": "用具体细节回应基础穿搭"},
                    "proof_atoms": [
                        {"claim_key": "C1", "fact_text": "短款衣长收在腰线附近"},
                        {"claim_key": "C2", "fact_text": "袖部带有银色椭圆扣"},
                    ],
                }
            },
            "creative_voice_context": {"scene_moment": "下班准备离开"},
            "narrative_anchor_options": [{"source_key": "scene_moment"}],
            "approved_style_references": [{"reference_sample_id": "R1"}],
            "relationship_language_profile": {"enabled": True},
            "forbidden_leaps": ["不得编造材质"],
        }
        return {
            "product_code": "P1",
            "target_country": "泰国",
            "target_language": "泰语",
            "top_category": "女装",
            "product_type": "外套",
            "directions": [{
                "output_slot": "S1",
                "direction_assignment_id": "DIR_1",
                "execution_card_id": "EXEC_1",
                "source_profile_id": "SP_1",
                "cluster_id": 1,
                "cluster_version": "v1",
                "p2_lite": {"primary_observation": "短款衣长收在腰线附近"},
                "content_bundle_brief": {
                    "content_bundle_id": "CB_1",
                    "content_mainline": "下班前发现短款衣长",
                    "claim_atoms": [
                        {"claim_key": "C1", "fact_text": "短款衣长收在腰线附近", "role": "core_result"},
                        {"claim_key": "C2", "fact_text": "袖部带有银色椭圆扣", "role": "supporting"},
                    ],
                    "max_claim_atoms": 3,
                    "max_themes": 2,
                },
                "creative_diversity_contract": {
                    "contract_id": "CDC_1",
                    "structure_family": "HOOK>PROOF",
                    "required_carrier": "WEARER_ACTIVE",
                    "required_presentation_mode": "PERSON_ON_CAMERA",
                },
                "creative_blueprint": {
                    "creative_blueprint_id": "CBP_1",
                    "persona": {
                        "identity": "通勤女性",
                        "appearance": "自然肤质",
                        "hair_makeup": "低马尾淡妆",
                        "styling": "黑色内搭和高腰裤",
                    },
                    "scene": {"location": "办公室衣帽区", "moment": "下班前", "lighting": "窗光"},
                    "event_design": {
                        "start_state": "拿起包",
                        "natural_event": "背包后走向门口",
                        "core_result_moment": "侧身时看到短款衣长",
                        "end_state": "离开",
                    },
                    "macro_visual_passages": [
                        {"narrative_role": "EVENT_ENTRY", "visible_process": "拿起包", "observable_action": "拿起包", "camera_observation": "中景"},
                        {"narrative_role": "EVENT_PROOF", "visible_process": "侧身", "observable_action": "走向门口", "camera_observation": "侧面中景"},
                        {"narrative_role": "EVENT_END", "visible_process": "离开", "observable_action": "走出画面", "camera_observation": "固定中景"},
                    ],
                },
                "visual_plan": {
                    "execution_card_id": "EXEC_1",
                    "shots": [
                        {
                            "shot_no": 1,
                            "duration": "0-7.5s",
                            "shot_content": "人物拿起包",
                            "observable_action": "拿起包准备离开",
                            "framing": "固定中景",
                            "product_visibility": "FULL",
                            "supported_claim_keys": ["C1"],
                            "structure_beat": "HOOK",
                            "carrier_mode": "WEARER_ACTIVE",
                            "continuity_group": "C1",
                            "opening_mechanism": "ACTION_HOOK",
                        },
                        {
                            "shot_no": 2,
                            "duration": "7.5-15s",
                            "shot_content": "人物侧身走向门口",
                            "observable_action": "衣长和袖扣清楚可见",
                            "framing": "侧面中景",
                            "product_visibility": "FULL",
                            "supported_claim_keys": ["C2"],
                            "structure_beat": "PROOF",
                            "carrier_mode": "WEARER_ACTIVE",
                            "continuity_group": "C1",
                            "opening_mechanism": "NONE",
                        },
                    ],
                    "unknowns_preserved": [],
                },
                "voiceover_candidates": [{"expression_contract": contract}],
            }],
        }

    def test_payload_uses_contract_but_not_old_candidate_text(self):
        product = self._product()
        product["directions"][0]["voiceover_candidates"][0]["lines"] = [
            {"voiceover_text_target_language": "旧文案不得进入"}
        ]
        payload = build_payload(product)

        self.assertEqual("P1", payload["product_code"])
        self.assertEqual("WORN_APPAREL", payload["creative_product_profile"])
        self.assertEqual("apparel", payload["display_family"])
        self.assertEqual("女装", payload["top_category"])
        self.assertEqual("外套", payload["product_type"])
        self.assertEqual(["C1", "C2"], [item["claim_key"] for item in payload["verified_facts"]])
        self.assertNotIn("旧文案不得进入", str(payload))

    def test_payload_passes_worn_accessory_context_without_new_constraints(self):
        product = self._product()
        product["top_category"] = "配饰"
        product["product_type"] = "围巾"
        product["directions"][0]["creative_diversity_contract"].update(
            {
                "category": "配饰",
                "product_type": "围巾",
                "creative_product_profile": "WORN_ACCESSORY",
            }
        )

        payload = build_payload(product)

        self.assertEqual("围巾", payload["product_type"])
        self.assertEqual("配饰", payload["top_category"])
        self.assertEqual("WORN_ACCESSORY", payload["creative_product_profile"])
        self.assertEqual("apparel_accessory", payload["display_family"])
        self.assertNotIn("required", payload)

    def test_validation_adds_duration_without_rewriting(self):
        payload = build_payload(self._product())
        generated = {
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "target_text": "นะ" * 65,
                    "chinese_translation": "中文",
                    "used_claim_refs": ["C1"],
                }
                for candidate_id in (
                    "FULL_A_NEED",
                    "FULL_B_LIVED_MOMENT",
                    "FULL_C_DETAIL_DISCOVERY",
                )
            ]
        }
        validation = validate_result(payload, generated)

        self.assertTrue(validation["valid"])
        self.assertEqual(10.0, generated["candidates"][0]["estimated_delivery_seconds"])
        self.assertEqual("READY_FOR_BLIND_REVIEW", generated["candidates"][0]["selection_readiness"])

    def test_complete_candidate_is_mounted_as_one_cross_shot_segment(self):
        product = self._product()
        candidate = {
            "candidate_id": "FULL_B_LIVED_MOMENT",
            "target_text": "ก่อนออกไปทำงาน ฉันหยิบกระเป๋า แล้วเห็นชายเสื้ออยู่ใกล้เอวค่ะ",
            "chinese_translation": "出门前我拿起包，看到衣摆落在腰线附近。",
            "used_claim_refs": ["C1"],
            "estimated_delivery_seconds": 11.5,
            "selection_readiness": "READY_FOR_BLIND_REVIEW",
        }
        plan = build_selected_voiceover_plan(product["directions"][0], candidate)

        self.assertEqual(1, len(plan["lines"]))
        self.assertEqual(1, plan["lines"][0]["shot_no"])
        self.assertEqual(2, plan["lines"][0]["end_shot_no"])
        self.assertEqual(candidate["target_text"], plan["lines"][0]["voiceover_text_target_language"])
        self.assertFalse(plan["engine_provenance"]["downstream_rewritten"])

    def test_downstream_assembly_preserves_complete_candidate_exactly(self):
        product = self._product()
        candidate = {
            "candidate_id": "FULL_B_LIVED_MOMENT",
            "target_text": "ก่อนออกไปทำงาน ฉันหยิบกระเป๋า แล้วเห็นชายเสื้ออยู่ใกล้เอวค่ะ",
            "chinese_translation": "出门前我拿起包，看到衣摆落在腰线附近。",
            "used_claim_refs": ["C1"],
            "estimated_delivery_seconds": 11.5,
            "selection_readiness": "READY_FOR_BLIND_REVIEW",
        }
        assembled = assemble_selected_candidate(
            product,
            {"product_code": "P1", "candidates": [candidate]},
            "FULL_B_LIVED_MOMENT",
        )

        self.assertEqual(
            candidate["target_text"],
            assembled["script"]["continuous_voiceover"]["target_language"],
        )
        self.assertFalse(assembled["downstream_rewritten"])
        self.assertEqual(
            "central_creative_full_script",
            assembled["script"]["voiceover_execution_plan"]["source_kind"],
        )
        self.assertFalse(
            assembled["script"]["voiceover_execution_plan"]["downstream_rewritten"]
        )
        self.assertIn("【视频模型主输入", assembled["final_video_prompt"])


if __name__ == "__main__":
    unittest.main()
