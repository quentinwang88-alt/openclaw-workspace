"""The batch bridge must keep one complete utterance and exact lineage."""
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.complete_voiceover_direct import (
    _expression_with_selected_claims,
    _narrative_anchor_options,
    run_central_complete_voiceover,
)


class CompleteVoiceoverDirectTest(unittest.TestCase):
    def test_claim_selection_uses_whole_video_evidence_not_visual_spoken_choice(self):
        direction = {"content_bundle_brief": {}}
        visual = {"shots": []}
        expression = {
            "claim_atoms": [
                {"claim_key": "C1", "fact_text": "短款到腰线", "supported_shot_nos": [2]},
                {"claim_key": "C2", "fact_text": "未经画面支持", "supported_shot_nos": []},
            ],
            "argument_contract": {"content": {"proof_atoms": []}},
        }
        with patch(
            "core.complete_voiceover_direct.build_voiceover_expression_contract",
            return_value=expression,
        ):
            _, selected = _expression_with_selected_claims(direction, visual)
        self.assertEqual([item["claim_key"] for item in selected], ["C1"])

    def test_selling_argument_may_use_two_related_evidence_facts(self):
        direction = {
            "content_bundle_brief": {"content_mode": "SELLING_ARGUMENT"}
        }
        expression = {
            "claim_atoms": [
                {"claim_key": "C1", "fact_text": "短款到腰线", "supported_shot_nos": [1]},
                {"claim_key": "C2", "fact_text": "前襟排扣", "supported_shot_nos": [2]},
            ],
            "argument_contract": {"content": {"proof_atoms": []}},
        }
        with patch(
            "core.complete_voiceover_direct.build_voiceover_expression_contract",
            return_value=expression,
        ):
            _, selected = _expression_with_selected_claims(direction, {"shots": []})
        self.assertEqual([item["claim_key"] for item in selected], ["C1", "C2"])

    def test_content_first_context_restores_speaker_position_without_action_plot(self):
        anchors = _narrative_anchor_options({
            "grounding_mode": "CONTENT_FIRST_WHOLE_VIDEO",
            "creator_motivation": "分享自己挑短外套时会看的位置",
            "scene_moment": "早晨出门前",
            "event_context": "拿包后突然发现",
        })
        self.assertEqual(
            [item["source"] for item in anchors],
            ["speaker_intent", "scene_moment"],
        )

    def test_available_selling_argument_is_declared_as_voiceover_mainline(self):
        direction = {
            "content_bundle_brief": {
                "content_mode": "SELLING_ARGUMENT",
            }
        }
        visual = {"shots": [{"supported_claim_keys": ["C1"]}]}
        expression = {
            "claim_atoms": [
                {"claim_key": "C1", "fact_text": "前襟五颗扣子", "supported_shot_nos": [1]},
            ],
            "argument_contract": {
                "content": {
                    "value_proposition": {"text": "适合作为降温环境的外搭"},
                    "audience_tension": {"text": ""},
                    "selling_argument": {
                        "argument_id": "ARG_COOLING_LAYER",
                        "status": "AVAILABLE",
                        "core_value": "适合作为降温环境的外搭",
                        "target_need": "频繁进出空调房时需要一层外搭",
                        "allowed_strength": "soft_only",
                        "proof_match_status": "UNMATCHED",
                    },
                }
            },
            "creative_voice_context": {},
            "forbidden_leaps": [],
        }
        captured = {}

        def fake_invoke(_command, payload):
            captured.update(payload)
            return {
                "candidate_id": "GENERAL_PRODUCT_SHARE",
                "hook_id": "GENERAL_PRODUCT_SHARE",
                "target_text": "ตัวนี้เหมาะเอาไว้คลุมตอนเข้าออกห้องแอร์ค่ะ ด้านหน้ามีกระดุมห้าเม็ด",
                "chinese_translation": "这件适合进出空调房时当外搭，前面有五颗扣子。",
                "used_claim_refs": ["C1"],
                "used_selling_argument_id": "ARG_COOLING_LAYER",
                "selling_argument_realization": "ตัวนี้เหมาะเอาไว้คลุมตอนเข้าออกห้องแอร์",
            }

        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[{"hook_id": "GENERAL_PRODUCT_SHARE"}],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(expression, expression["claim_atoms"]),
        ), patch("core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke):
            run_central_complete_voiceover(
                product_code="P1",
                target_country="泰国",
                target_language="泰语",
                top_category="女装",
                product_type="外套",
                direction=direction,
                visual_plan=visual,
                model_command="mock-command",
                candidate_hook_id="GENERAL_PRODUCT_SHARE",
            )
        self.assertEqual(captured["mainline_policy"], "SELLING_ARGUMENT_IS_PRIMARY")
        self.assertEqual(
            captured["selling_argument"]["core_value"],
            "适合作为降温环境的外搭",
        )

    def test_selling_argument_may_generate_without_a_visible_fact(self):
        direction = {"content_bundle_brief": {"content_mode": "SELLING_ARGUMENT"}}
        visual = {"shots": [{"supported_claim_keys": []}]}
        expression = {
            "claim_atoms": [],
            "argument_contract": {
                "content": {
                    "value_proposition": {"text": "适合作为降温环境的外搭"},
                    "audience_tension": {"text": ""},
                    "selling_argument": {
                        "argument_id": "ARG_COOLING_LAYER",
                        "status": "AVAILABLE",
                        "core_value": "适合作为降温环境的外搭",
                        "allowed_strength": "soft_only",
                        "proof_match_status": "UNMATCHED",
                    },
                }
            },
            "creative_voice_context": {},
            "forbidden_leaps": [],
        }

        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[{"hook_id": "GENERAL_PRODUCT_SHARE"}],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(expression, []),
        ), patch(
            "core.complete_voiceover_direct._invoke_model",
            return_value={
                "candidate_id": "GENERAL_PRODUCT_SHARE",
                "hook_id": "GENERAL_PRODUCT_SHARE",
                "target_text": "วันไหนต้องเข้าออกห้องแอร์ทั้งวัน เราจะหยิบตัวนี้มาใส่ค่ะ",
                "chinese_translation": "哪天需要一整天进出空调房，我就会拿这件来穿。",
                "used_claim_refs": [],
                "used_selling_argument_id": "ARG_COOLING_LAYER",
                "selling_argument_realization": "วันไหนต้องเข้าออกห้องแอร์ทั้งวัน",
            },
        ):
            result = run_central_complete_voiceover(
                product_code="P1",
                target_country="泰国",
                target_language="泰语",
                top_category="女装",
                product_type="外套",
                direction=direction,
                visual_plan=visual,
                model_command="mock-command",
                candidate_hook_id="GENERAL_PRODUCT_SHARE",
            )
        self.assertEqual(result["selected_claim_count"], 0)
        self.assertEqual(result["selected_selling_argument_id"], "ARG_COOLING_LAYER")

    def test_builds_one_cross_shot_line_without_rewrite(self):
        direction = {
            "content_bundle_brief": {"eligible_hook_ids": ["DETAIL_SURPRISE"]},
            "creative_blueprint": {},
        }
        visual = {
            "shots": [
                {"supported_claim_keys": ["C1"]},
                {"supported_claim_keys": ["C2"]},
                {"supported_claim_keys": []},
            ]
        }
        expression = {
            "claim_atoms": [
                {"claim_key": "C1", "fact_text": "短款到腰线", "supported_shot_nos": [1]},
                {"claim_key": "C2", "fact_text": "袖部有银色扣", "supported_shot_nos": [2]},
            ],
            "argument_contract": {
                "content": {
                    "value_proposition": {"text": "观察两个可见细节"},
                    "audience_tension": {"text": ""},
                    "proof_atoms": [],
                }
            },
            "creative_voice_context": {"speaker_identity": "朋友式分享者"},
            "forbidden_leaps": [],
        }
        generated = {
            "candidate_id": "DETAIL_SURPRISE",
            "hook_id": "DETAIL_SURPRISE",
            "target_text": "ดูนี่ก่อนนะ ตัวนี้เป็นทรงครอป แล้วตรงแขนมีตัวล็อกสีเงินด้วยค่ะ",
            "chinese_translation": "先看这里，这件是短款，袖部还有银色扣。",
            "used_claim_refs": ["C1", "C2"],
        }
        captured = {}

        def fake_invoke(_command, payload):
            captured.update(payload)
            return generated

        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[{"hook_id": "DETAIL_SURPRISE"}],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(expression, expression["claim_atoms"]),
        ), patch("core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke):
            result = run_central_complete_voiceover(
                product_code="P1",
                target_country="泰国",
                target_language="泰语",
                top_category="女装",
                product_type="外套",
                direction=direction,
                visual_plan=visual,
                model_command="mock-command",
                candidate_hook_id="DETAIL_SURPRISE",
            )
        self.assertEqual(result["hook_id"], "DETAIL_SURPRISE")
        self.assertEqual(len(result["lines"]), 1)
        self.assertEqual(result["lines"][0]["end_shot_no"], 3)
        self.assertEqual(
            result["lines"][0]["voiceover_text_target_language"],
            generated["target_text"],
        )
        self.assertFalse(result["engine_provenance"]["downstream_rewritten"])
        self.assertEqual(captured["content_mode"], "FACTUAL_OBSERVATION")
        self.assertEqual(captured["spoken_duration_preference_seconds"], [7, 11])
        self.assertIn("personal_preference", captured["expression_freedom"]["allowed_without_claim_ref"])

    def test_relationship_device_is_passed_as_soft_voiceover_surface_metadata(self):
        direction = {"content_bundle_brief": {"eligible_hook_ids": ["AUDIENCE_NEED_CALLOUT"]}}
        visual = {"shots": [{"supported_claim_keys": ["C1"]}]}
        expression = {
            "claim_atoms": [{"claim_key": "C1", "fact_text": "短款到腰线", "supported_shot_nos": [1]}],
            "argument_contract": {"content": {"value_proposition": {"text": "短款观察"}, "audience_tension": {}, "proof_atoms": []}},
            "creative_voice_context": {},
            "forbidden_leaps": [],
        }
        captured = {}

        def fake_invoke(_command, payload):
            captured.update(payload)
            return {
                "candidate_id": "AUDIENCE_NEED_CALLOUT",
                "hook_id": "AUDIENCE_NEED_CALLOUT",
                "target_text": "สาวๆ ตัวนี้เป็นทรงครอป ความยาวอยู่แถวเอวนะ",
                "chinese_translation": "姐妹们，这件是短款，长度在腰线附近。",
                "used_claim_refs": ["C1"],
            }

        with patch("core.complete_voiceover_direct.load_active_voiceover_hooks", return_value=[{"hook_id": "AUDIENCE_NEED_CALLOUT"}]), \
             patch("core.complete_voiceover_direct._expression_with_selected_claims", return_value=(expression, expression["claim_atoms"])), \
             patch("core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke):
            result = run_central_complete_voiceover(
                product_code="P1", target_country="泰国", target_language="泰语",
                top_category="女装", product_type="外套", direction=direction,
                visual_plan=visual, model_command="mock-command",
                candidate_hook_id="AUDIENCE_NEED_CALLOUT",
                relationship_device="AUDIENCE_ADDRESS",
            )
        self.assertEqual(captured["relationship_language"]["assigned_device"], "AUDIENCE_ADDRESS")
        self.assertEqual(captured["relationship_language"]["audience_addresses"], ["สาวๆ"])
        self.assertTrue(captured["relationship_language"]["hard_required"] is False)
        self.assertEqual(result["relationship_surface"], {
            "requested": "AUDIENCE_ADDRESS",
            "realized": "AUDIENCE_ADDRESS",
            "surface_text": "สาวๆ",
        })


if __name__ == "__main__":
    unittest.main()
