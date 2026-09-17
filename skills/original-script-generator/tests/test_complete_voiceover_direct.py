"""The batch bridge must keep one complete utterance and exact lineage."""
import json
import tempfile
import unittest
from pathlib import Path
import sys
from datetime import datetime
from unittest.mock import patch

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.complete_voiceover_direct import (
    _approved_style_references,
    _estimated_spoken_seconds,
    _expression_with_selected_claims,
    _invoke_model,
    original_voiceover_route_cache_key,
    _hook_surface_status,
    _narrative_anchor_options,
    _relationship_language_profile,
    _resolve_style_reference_routing,
    _target_language_error,
    run_central_complete_voiceover,
)


class CompleteVoiceoverDirectTest(unittest.TestCase):
    def test_native_surface_keeps_human_approved_rhetoric_anchor(self):
        approved = [{"reference_sample_id": "HUMAN_1"}]
        refs, policy = _resolve_style_reference_routing(
            approved,
            {"native_surface_references": [{"video_id": "NATIVE_1"}]},
            "APPROVED_ONLY",
        )
        self.assertEqual(refs, approved)
        self.assertEqual(
            policy, "HYBRID_NATIVE_SURFACE_AND_APPROVED_RHETORIC"
        )

    def test_model_boundary_stringifies_provider_datetime_metadata(self):
        captured = {}

        def fake_run(command, *, input, **kwargs):
            captured["input"] = input
            captured["timeout"] = kwargs["timeout"]
            return type(
                "Completed",
                (),
                {"returncode": 0, "stdout": '{"ok": true}', "stderr": ""},
            )()

        with patch(
            "core.complete_voiceover_direct.subprocess.run",
            side_effect=fake_run,
        ):
            result = _invoke_model(
                "mock-command",
                {"hook_knowledge": {"created_at": datetime(2026, 8, 27, 1, 2, 3)}},
            )

        self.assertEqual(result, {"ok": True})
        self.assertIn("2026-08-27 01:02:03", captured["input"])
        self.assertEqual(json.loads(captured["input"])["route_scope"], "original_shortform")
        self.assertEqual(captured["timeout"], 600)

    def test_voiceover_route_cache_changes_on_rollback(self):
        with patch.dict('os.environ', {'ORIGINAL_SHORTFORM_VOICEOVER_ASTRA_ENABLED':'1'}):
            enabled = original_voiceover_route_cache_key()
        with patch.dict('os.environ', {'ORIGINAL_SHORTFORM_VOICEOVER_ASTRA_ENABLED':'0'}):
            disabled = original_voiceover_route_cache_key()
        self.assertNotEqual(enabled, disabled)
        self.assertEqual(enabled['scope'], disabled['scope'])

    def test_target_language_guard_rejects_thai_for_vietnamese_and_malay(self):
        thai = "ดูนี่ก่อนนะ ตัวนี้สวยมากค่ะ"
        self.assertIn("越南语", _target_language_error(thai, "越南语"))
        self.assertIn("马来语", _target_language_error(thai, "马来语"))

    def test_target_language_guard_accepts_native_vietnamese_and_malay(self):
        self.assertEqual(
            "",
            _target_language_error(
                "Mình vừa để ý chiếc kẹp này giữ tóc gọn mà nhìn vẫn nhẹ nhàng nhé.",
                "越南语",
            ),
        )
        self.assertEqual(
            "",
            _target_language_error(
                "Tengok ni, gelang ini nampak cantik bila kena cahaya.",
                "马来语",
            ),
        )
        self.assertEqual(
            "",
            _target_language_error(
                "Amigas, vean este detalle porque la verdad se ve muy bonito.",
                "西班牙语",
            ),
        )

    def test_relationship_surfaces_follow_frozen_target_language(self):
        vietnamese = _relationship_language_profile(
            "AUDIENCE_NEED_CALLOUT", target_language="越南语"
        )
        malay = _relationship_language_profile(
            "DETAIL_SURPRISE", target_language="马来语"
        )
        spanish = _relationship_language_profile(
            "AUDIENCE_NEED_CALLOUT", target_language="西班牙语"
        )
        self.assertIn("chị em", vietnamese["audience_addresses"])
        self.assertIn("korang", malay["audience_addresses"])
        self.assertIn("amigas", spanish["audience_addresses"])
        self.assertNotRegex(json.dumps(vietnamese, ensure_ascii=False), r"[\u0E00-\u0E7F]")
        self.assertNotRegex(json.dumps(malay, ensure_ascii=False), r"[\u0E00-\u0E7F]")

    def test_duration_estimate_uses_word_rate_for_space_delimited_languages(self):
        text = "Tengok ni gelang ini nampak cantik bila kena cahaya"
        self.assertEqual(_estimated_spoken_seconds(text, "马来语"), 3.33)

    def test_generation_rejects_wrong_language_before_assembly(self):
        expression = {
            "claim_atoms": [{
                "claim_key": "C1", "fact_text": "发夹固定头发", "supported_shot_nos": [1]
            }],
            "argument_contract": {"content": {"value_proposition": {"text": "固定头发"}}},
            "creative_voice_context": {},
            "forbidden_leaps": [],
        }
        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[{"hook_id": "DETAIL_SURPRISE"}],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(expression, expression["claim_atoms"]),
        ), patch(
            "core.complete_voiceover_direct._invoke_model",
            return_value={
                "candidate_id": "DETAIL_SURPRISE",
                "hook_id": "DETAIL_SURPRISE",
                "target_text": "ดูนี่ก่อนนะ กิ๊บตัวนี้เก็บผมได้ค่ะ",
                "chinese_translation": "先看这里，这只发夹可以固定头发。",
                "used_claim_refs": ["C1"],
            },
        ):
            with self.assertRaisesRegex(ValueError, "目标语言为越南语"):
                run_central_complete_voiceover(
                    product_code="P1",
                    target_country="越南",
                    target_language="越南语",
                    top_category="配饰",
                    product_type="抓夹",
                    direction={"content_bundle_brief": {}},
                    visual_plan={"shots": [{"supported_claim_keys": ["C1"]}]},
                    model_command="mock-command",
                    candidate_hook_id="DETAIL_SURPRISE",
                )

    def test_style_references_never_fall_back_across_hook_archetypes(self):
        snapshot = {
            "examples": [
                {
                    "example_id": "E_MATCHED",
                    "raw_text": "matched rhetoric",
                    "source_authorized": 1,
                    "quality_status": "approved_sample",
                    "country": "TH",
                    "category": "womenswear",
                    "language": "zh",
                },
                {
                    "example_id": "E_OTHER",
                    "raw_text": "other rhetoric",
                    "source_authorized": 1,
                    "quality_status": "approved_sample",
                    "country": "TH",
                    "category": "womenswear",
                    "language": "zh",
                },
            ],
            "assignments": [
                {"example_id": "E_MATCHED", "archetype_id": "DETAIL_SURPRISE"},
                {"example_id": "E_OTHER", "archetype_id": "GENERAL_PRODUCT_SHARE"},
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "snapshot.json"
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            with patch(
                "core.complete_voiceover_direct.VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH",
                path,
            ):
                matched = _approved_style_references(
                    "DETAIL_SURPRISE", target_country="泰国", top_category="女装"
                )
                unavailable = _approved_style_references(
                    "VISUAL_RESULT_DIRECT", target_country="泰国", top_category="女装"
                )
        self.assertEqual(
            [item["reference_sample_id"] for item in matched], ["E_MATCHED"]
        )
        self.assertEqual(unavailable, [])

    def test_style_references_allow_same_hook_fashion_family_without_cross_hook(self):
        snapshot = {
            "examples": [{
                "example_id": "E_WOMENSWEAR",
                "raw_text": "authorised viewer relationship sample",
                "source_authorized": 1,
                "quality_status": "approved_sample",
                "country": "TH",
                "category": "womenswear",
                "language": "zh",
            }],
            "assignments": [{
                "example_id": "E_WOMENSWEAR",
                "archetype_id": "AUDIENCE_NEED_CALLOUT",
            }],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "snapshot.json"
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            with patch(
                "core.complete_voiceover_direct.VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH",
                path,
            ):
                matched = _approved_style_references(
                    "AUDIENCE_NEED_CALLOUT",
                    target_country="泰国",
                    top_category="配饰",
                    product_type="丝巾",
                )
                wrong_hook = _approved_style_references(
                    "DETAIL_SURPRISE",
                    target_country="泰国",
                    top_category="配饰",
                    product_type="丝巾",
                )
        self.assertEqual(matched[0]["match_tier"], "EXPRESSION_FAMILY")
        self.assertEqual(wrong_hook, [])

    def test_hook_surface_status_is_observed_separately_from_lineage(self):
        self.assertEqual(
            _hook_surface_status("AUDIENCE_NEED_CALLOUT", "ใครอยากได้ลุคนี้ไหม?"),
            "REALIZED",
        )
        self.assertEqual(
            _hook_surface_status("AUDIENCE_NEED_CALLOUT", "ตัวนี้สีสวยค่ะ"),
            "WEAK",
        )

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

    def test_selling_argument_keeps_one_supporting_fact(self):
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
        self.assertEqual([item["claim_key"] for item in selected], ["C1"])

    def test_selling_argument_prefers_direct_support_over_unrelated_detail(self):
        direction = {
            "content_bundle_brief": {"content_mode": "SELLING_ARGUMENT"}
        }
        expression = {
            "claim_atoms": [
                {
                    "claim_key": "DETAIL",
                    "fact_text": "表面有光泽",
                    "argument_relation": "OPTIONAL_PRODUCT_DETAIL",
                    "supported_shot_nos": [1],
                },
                {
                    "claim_key": "HAIR",
                    "fact_text": "头巾已佩戴完成",
                    "argument_relation": "DIRECT_SUPPORT",
                    "supported_shot_nos": [2],
                },
            ],
            "argument_contract": {"content": {"proof_atoms": []}},
        }
        with patch(
            "core.complete_voiceover_direct.build_voiceover_expression_contract",
            return_value=expression,
        ):
            _, selected = _expression_with_selected_claims(direction, {"shots": []})
        self.assertEqual([item["claim_key"] for item in selected], ["HAIR"])

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
        self.assertTrue(all(item["anchor_id"].startswith("CTX_") for item in anchors))
        self.assertEqual(len({item["anchor_id"] for item in anchors}), 2)

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
                        "primary_demonstration_mode": "NECK_WORN",
                        "demonstration_policy": "ONE_PRIMARY_MODE_PER_15S",
                        "voiceover_scope_policy": "PRIMARY_DEMONSTRATION_MODE_ONLY",
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
        self.assertEqual(
            captured["mainline_scope"]["policy"],
            "ONE_CORE_ARGUMENT_WITH_SAME_THEME_SUPPORT",
        )
        self.assertEqual(
            captured["spoken_brief"]["core_buying_reason"],
            "适合作为降温环境的外搭",
        )
        self.assertEqual(
            captured["spoken_brief"]["optional_supporting_fact"]["claim_key"],
            "C1",
        )
        self.assertFalse(
            captured["expression_density_contract"]["second_selling_argument_allowed"]
        )
        self.assertFalse(
            captured["expression_density_contract"]["full_input_coverage_required"]
        )
        self.assertEqual(
            captured["selling_argument"]["primary_demonstration_mode"],
            "NECK_WORN",
        )
        self.assertNotIn("instruction", captured["mainline_scope"])

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

    def test_selling_scenario_context_is_passed_and_consumption_is_observable(self):
        direction = {"content_bundle_brief": {"content_mode": "SELLING_ARGUMENT"}}
        visual = {"shots": [{"supported_claim_keys": []}]}
        context_anchor = "CTX_SCENARIO_1"
        expression = {
            "claim_atoms": [],
            "argument_contract": {
                "content": {
                    "value_proposition": {"text": "适配多种日常场景"},
                    "audience_tension": {"text": ""},
                    "selling_argument": {
                        "argument_id": "ARG_MULTI_SCENE",
                        "status": "AVAILABLE",
                        "core_value": "适配多种日常场景",
                        "target_need": "旅行只想带一件，办公室、通勤和休闲都能穿",
                        "operator_expression": "适配多种日常场景",
                        "source_operator_expression": "旅行只想带一件，办公室、通勤和休闲都能穿",
                        "source_scope_concept_count": 2,
                        "audience_need_authority": "APPROVED_SELLING_SCENARIO",
                        "audience_situation": "旅行只想带一件，办公室、通勤和休闲都能穿",
                        "multi_scenario_authorized": True,
                        "allowed_strength": "soft_only",
                        "proof_match_status": "UNMATCHED",
                    },
                }
            },
            "voiceover_context_contract": {
                "status": "AVAILABLE",
                "context_mode": "SELLING_SCENARIO",
                "use_priority": "PREFERRED",
                "audience_situation": {
                    "anchor_id": context_anchor,
                    "text": "旅行只想带一件，办公室、通勤和休闲都能穿",
                    "authority": "APPROVED_SELLING_SCENARIO",
                },
                "current_life_moment": {},
                "scenario_budget": 2,
            },
            "creative_voice_context": {},
            "forbidden_leaps": [],
        }
        captured = {}

        def fake_invoke(_command, payload):
            captured.update(payload)
            return {
                "candidate_id": "AUDIENCE_NEED_CALLOUT",
                "hook_id": "AUDIENCE_NEED_CALLOUT",
                "target_text": "ถ้าไปเที่ยวแล้วอยากพกเสื้อคลุมแค่ตัวเดียว ตัวนี้ใส่ต่อได้ทั้งวันค่ะ",
                "chinese_translation": "旅行只想带一件外套时，这件可以接着穿一整天。",
                "used_claim_refs": [],
                "used_selling_argument_id": "ARG_MULTI_SCENE",
                "selling_argument_realization": "ตัวนี้ใส่ต่อได้ทั้งวัน",
                "selling_argument_realization_zh": "一件外套覆盖当天多种场景。",
                "used_context_anchor": context_anchor,
            }

        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[{"hook_id": "AUDIENCE_NEED_CALLOUT"}],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(expression, []),
        ), patch(
            "core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke,
        ):
            result = run_central_complete_voiceover(
                product_code="P1", target_country="泰国", target_language="泰语",
                top_category="女装", product_type="外套", direction=direction,
                visual_plan=visual, model_command="mock-command",
                candidate_hook_id="AUDIENCE_NEED_CALLOUT",
            )

        self.assertEqual(
            captured["spoken_brief"]["operator_context"],
            "旅行只想带一件，办公室、通勤和休闲都能穿",
        )
        self.assertEqual(captured["voiceover_context_contract"]["use_priority"], "PREFERRED")
        self.assertEqual(result["used_context_anchor"], context_anchor)
        self.assertEqual(result["context_consumption_status"], "USED")
        self.assertEqual(result["voiceover_context_mode"], "SELLING_SCENARIO")

    def test_builds_one_cross_shot_line_without_rewrite(self):
        direction = {
            "content_bundle_brief": {"eligible_hook_ids": ["DETAIL_SURPRISE"]},
            "creative_blueprint": {},
            "category_execution_extension": {
                "domain": "ACCESSORY",
                "profile": {
                    "product_subtype": "earring",
                    "identity_authority": {
                        "pairing_mode": "UNAVAILABLE",
                        "authority_source": "UNAVAILABLE",
                        "must_not_assume": ["耳饰为单只还是成对"],
                    },
                },
            },
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
            return_value=[{
                "hook_id": "DETAIL_SURPRISE",
                "hook_name": "细节惊喜型",
                "core_intent": "放大容易忽略但有价值的细节",
                "attention_mechanisms": ["information_gap"],
                "minimal_structure": ["reveal_detail", "offer_proof", "state_feature"],
                "relation_modes": ["reveal"],
            }],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(expression, expression["claim_atoms"]),
        ), patch("core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke):
            result = run_central_complete_voiceover(
                product_code="P1",
                target_country="泰国",
                target_language="泰语",
                top_category="配饰",
                product_type="耳饰",
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
        self.assertIn("native_rhetoric_contract", captured)
        self.assertNotIn("retrieved_speech_hook_pool", captured)
        self.assertEqual(
            captured["category_identity_authority"]["pairing_mode"],
            "UNAVAILABLE",
        )
        self.assertEqual(captured["spoken_duration_preference_seconds"], [7, 11])
        self.assertNotIn("expression_freedom", captured)
        self.assertIn("spoken_brief", captured)
        self.assertEqual(
            captured["hook_guidance"]["minimal_structure"],
            ["reveal_detail", "offer_proof", "state_feature"],
        )

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
        self.assertEqual(result["hook_lineage_status"], "PINNED")
        self.assertEqual(result["hook_surface_status"], "REALIZED")

    def test_native_provider_cannot_replace_the_governed_hook(self):
        direction = {
            "content_bundle_brief": {
                "eligible_hook_ids": ["USER_ADVOCACY_STANCE"],
            }
        }
        visual = {"shots": [{"supported_claim_keys": ["C1"]}]}
        expression = {
            "claim_atoms": [{
                "claim_key": "C1",
                "fact_text": "蓝橙撞色",
                "supported_shot_nos": [1],
            }],
            "argument_contract": {
                "content": {
                    "value_proposition": {"text": "撞色让造型更醒目"},
                    "audience_tension": {},
                    "proof_atoms": [],
                }
            },
            "creative_voice_context": {},
            "forbidden_leaps": [],
        }
        captured = {}

        def fake_invoke(_command, payload):
            captured.update(payload)
            return {
                "candidate_id": "USER_ADVOCACY_STANCE",
                "hook_id": "USER_ADVOCACY_STANCE",
                "target_text": "สำหรับเรา ผ้าผืนนี้ทำให้ชุดเรียบดูเด่นขึ้นแบบพอดีค่ะ",
                "chinese_translation": "对我来说，这条丝巾让简单穿搭恰到好处地更醒目。",
                "used_claim_refs": ["C1"],
            }

        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[
                {"hook_id": "USER_ADVOCACY_STANCE"},
                {"hook_id": "GENERAL_PRODUCT_SHARE"},
            ],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(expression, expression["claim_atoms"]),
        ), patch(
            "core.complete_voiceover_direct._central_native_rhetoric_contract",
            return_value={
                "status": "UNAVAILABLE",
                "resolved_hook_id": "GENERAL_PRODUCT_SHARE",
                "structural_patterns": [],
                "native_surface_references": [],
            },
        ), patch(
            "core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke
        ):
            result = run_central_complete_voiceover(
                product_code="P1",
                target_country="泰国",
                target_language="泰语",
                top_category="配饰",
                product_type="丝巾",
                direction=direction,
                visual_plan=visual,
                model_command="mock-command",
                candidate_hook_id="USER_ADVOCACY_STANCE",
            )
        self.assertEqual(captured["requested_hook_id"], "USER_ADVOCACY_STANCE")
        self.assertEqual(result["hook_id"], "USER_ADVOCACY_STANCE")
        self.assertEqual(
            result["hook_knowledge_provenance"]["upstream_requested_hook_id"],
            "USER_ADVOCACY_STANCE",
        )


def _accessory_mainline(*, forbidden=("纱",)):
    return {
        "schema_version": "mixed-mainline-contract-v1",
        "status": "FROZEN",
        "audience_question": "适合日常约会、度假拍照、婚礼伴娘发型",
        "core_value": "双层纱质蝴蝶造型，超唯美",
        "core_value_safe": "双层蝴蝶造型，超唯美",
        "fact_basis": {"source_text": "双层纱质蝴蝶造型，超唯美", "fact_type": "APPEARANCE_FACT"},
        "visible_answer": {"shot_ref": "CU_01", "module": "WORN_DETAIL", "text": "能看到造型"},
        "expression_boundary": {
            "allowed_wording": "可描述图片支持的造型、层次、排列；材质只按可见观感表述",
            "forbidden_wording": list(forbidden),
            "experience_authority": "NONE",
            "allowed_strength": "SOFT",
            "max_main_value_count": 1,
            "conflicts": [],
        },
        "observation_tasks": [
            {
                "unit_id": "CU_01",
                "module": "WORN_DETAIL",
                "distinct_observation_key": "HAIR|CLOSE|WORN_DETAIL",
            }
        ],
    }


class VoiceoverExpressionBoundaryTest(unittest.TestCase):
    """表达边界必须作用在 ``run_central_complete_voiceover`` 真正发出的 payload 上。

    真实批次实测漏法：``expression`` 已按口径清洗，但这份 payload 又从 direction
    的原始字段取了一遍原文（``semantic_spine_contract.script_thesis.core_buying_reason``
    与 ``context_bridge_contract``），于是模型照原文写出 ``dáng bướm bằng voan hai lớp``
    —— 刚被口径拒掉的材质断言。清洗只覆盖一份副本，等于没清洗。
    """

    _EXPRESSION = {
        "claim_atoms": [
            {
                "claim_key": "C1",
                "fact_text": "双层纱质蝴蝶造型",
                "argument_relation": "DIRECT_SUPPORT",
                "supported_shot_nos": [1],
            }
        ],
        "argument_contract": {
            "content": {
                "value_proposition": {"text": "双层纱质蝴蝶造型，超唯美"},
                "audience_tension": {"text": ""},
                "selling_argument": {
                    "argument_id": "ARG_BUTTERFLY",
                    "status": "AVAILABLE",
                    "core_value": "双层纱质蝴蝶造型，超唯美",
                    "operator_expression": "双层纱质蝴蝶造型",
                    "source_operator_expression": "双层纱质蝴蝶造型，超唯美",
                    "allowed_strength": "soft_only",
                    "proof_match_status": "UNMATCHED",
                },
            }
        },
        "creative_voice_context": {},
        "voiceover_context_contract": {},
        "forbidden_leaps": [],
    }

    def _direction(self):
        mainline = _accessory_mainline()
        return {
            "content_bundle_brief": {
                "content_mode": "SELLING_ARGUMENT",
                "semantic_spine_contract": {
                    "script_thesis": {
                        "core_buying_reason": "双层纱质蝴蝶造型，超唯美",
                        "selected_source_span": "双层纱质蝴蝶造型，超唯美",
                        "primary_narrative_context": "约会拍照",
                    }
                },
                "context_bridge_contract": {
                    "allowed_spoken_context": ["双层纱质蝴蝶造型，超唯美"],
                    "speaker_context": "约会前整理发型",
                },
            },
            "semantic_spine_contract": {
                "script_thesis": {
                    "core_buying_reason": "双层纱质蝴蝶造型，超唯美",
                    "selected_source_span": "双层纱质蝴蝶造型，超唯美",
                }
            },
            "context_bridge_contract": {
                "allowed_spoken_context": ["双层纱质蝴蝶造型，超唯美"]
            },
            "category_execution_extension": {"mixed_mainline_contract": mainline},
        }

    def _run(self, *, generated_zh, captured, direction=None):
        def fake_invoke(_command, payload):
            captured.update(payload)
            return {
                "candidate_id": "AUDIENCE_NEED_CALLOUT",
                "hook_id": "AUDIENCE_NEED_CALLOUT",
                "target_text": "Mình mê nhất dáng bướm hai lớp.",
                "chinese_translation": generated_zh,
                "used_claim_refs": ["C1"],
                "used_selling_argument_id": "ARG_BUTTERFLY",
                "selling_argument_realization": "Mình mê nhất dáng bướm hai lớp.",
            }

        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[{"hook_id": "AUDIENCE_NEED_CALLOUT"}],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(dict(self._EXPRESSION), list(self._EXPRESSION["claim_atoms"])),
        ), patch(
            "core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke
        ):
            return run_central_complete_voiceover(
                product_code="P1",
                target_country="越南",
                target_language="越南语",
                top_category="配饰",
                product_type="抓夹",
                direction=direction if direction is not None else self._direction(),
                visual_plan={"shots": [{"supported_claim_keys": ["C1"]}]},
                model_command="mock-command",
                candidate_hook_id="AUDIENCE_NEED_CALLOUT",
            )

    def test_the_model_payload_carries_no_banned_wording(self):
        from core.mixed_voiceover_mainline import speakable_forbidden_hits

        captured = {}
        self._run(generated_zh="我最喜欢它的双层蝴蝶造型。", captured=captured)
        # 泄漏点是 semantic_spine_contract 与 context_bridge_contract —— 它们从
        # direction 原始字段取，不受 expression 的清洗影响。
        self.assertEqual(speakable_forbidden_hits(captured, ["纱"]), [])
        self.assertNotIn("纱", captured["content_mainline"])
        self.assertNotIn("纱", captured["spoken_brief"]["core_buying_reason"])
        self.assertNotIn("纱", captured["spoken_brief"]["operator_context"])
        self.assertNotIn("纱", json.dumps(captured["verified_facts"], ensure_ascii=False))

    def test_the_forbidden_layer_is_dispatched_into_the_spoken_brief(self):
        captured = {}
        self._run(generated_zh="我最喜欢它的双层蝴蝶造型。", captured=captured)
        # 禁止层放进写作指令区，而不是在顶层新增键（不改中央命令已知结构）。
        self.assertNotIn("voiceover_expression_boundary", captured)
        self.assertEqual(captured["spoken_brief"]["forbidden_wording"], ["纱"])
        self.assertIn("任何语言", captured["spoken_brief"]["forbidden_wording_rule"])

    def test_a_ceiling_only_boundary_is_still_dispatched(self):
        """没有禁词、只有封顶时，约束仍必须到达写作指令区。

        真实批次：手镯与戒指的边界里 ``forbidden_wording`` 为空，封顶只写在
        ``allowed_wording`` 里。先前把下发条件绑在"有没有禁词"上，整层没下发，
        两条真实稿都因此把三个场景并列说了出来。
        """

        captured = {}
        direction = self._direction()
        mainline = direction["category_execution_extension"]["mixed_mainline_contract"]
        mainline["expression_boundary"]["forbidden_wording"] = []
        mainline["expression_boundary"][
            "allowed_wording"
        ] = "只说一个与实际冻结场景兼容的搭配，不并列多个"
        mainline["expression_boundary"]["conflicts"] = [
            {
                "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                "stacked_scenes": ["日常", "约会", "上班"],
                "allowed_scenarios": 1,
                "resolution": "KEEP_ONE",
            }
        ]
        result = self._run(
            generated_zh="适合上班、约会或日常戴，不挑场合。",
            captured=captured,
            direction=direction,
        )
        brief = captured["spoken_brief"]
        self.assertEqual(brief["forbidden_wording"], [])
        self.assertEqual(
            brief["allowed_wording"], "只说一个与实际冻结场景兼容的搭配，不并列多个"
        )
        self.assertEqual(
            [item["kind"] for item in brief["wording_ceilings"]],
            ["MULTI_SCENARIO_UNAUTHORIZED"],
        )
        self.assertEqual(brief["max_main_value_count"], 1)
        boundary = result["expression_boundary"]
        # 清洗没发生，但约束下发了 —— 两者必须分开记，
        # 否则"约束从没发出"在报告里看起来像"无事可做"。
        self.assertFalse(boundary["applied"])
        self.assertTrue(boundary["layer_present"])
        self.assertEqual(boundary["layer_dispatched_to"], "spoken_brief")
        self.assertIn("wording_ceilings", boundary["declared_constraints"])
        # 成品侧也要查得出来，不再因为"没有禁词"而 NOT_APPLICABLE。
        self.assertEqual(boundary["check"]["status"], "FAIL")
        self.assertEqual(boundary["check"]["reason"], "CEILING_EXCEEDED")

    def test_a_single_scenario_ceiling_is_not_a_violation(self):
        captured = {}
        direction = self._direction()
        mainline = direction["category_execution_extension"]["mixed_mainline_contract"]
        mainline["expression_boundary"]["forbidden_wording"] = []
        mainline["expression_boundary"]["conflicts"] = [
            {
                "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                "stacked_scenes": ["日常", "约会", "上班"],
                "allowed_scenarios": 1,
                "resolution": "KEEP_ONE",
            }
        ]
        result = self._run(
            generated_zh="上班戴这枚就够了。", captured=captured, direction=direction
        )
        self.assertEqual(result["expression_boundary"]["check"]["status"], "PASS")

    def test_no_mainline_leaves_the_payload_structure_untouched(self):
        captured = {}
        direction = self._direction()
        direction.pop("category_execution_extension")
        direction.pop("semantic_spine_contract")
        direction["content_bundle_brief"].pop("semantic_spine_contract")
        direction["content_bundle_brief"].pop("context_bridge_contract")
        direction.pop("context_bridge_contract")

        def fake_invoke(_command, payload):
            captured.update(payload)
            return {
                "candidate_id": "AUDIENCE_NEED_CALLOUT",
                "hook_id": "AUDIENCE_NEED_CALLOUT",
                "target_text": "Mình mê nhất dáng bướm hai lớp.",
                "chinese_translation": "我最喜欢它的双层蝴蝶造型。",
                "used_claim_refs": ["C1"],
                "used_selling_argument_id": "ARG_BUTTERFLY",
                "selling_argument_realization": "Mình mê nhất dáng bướm hai lớp.",
            }

        with patch(
            "core.complete_voiceover_direct.load_active_voiceover_hooks",
            return_value=[{"hook_id": "AUDIENCE_NEED_CALLOUT"}],
        ), patch(
            "core.complete_voiceover_direct._expression_with_selected_claims",
            return_value=(dict(self._EXPRESSION), list(self._EXPRESSION["claim_atoms"])),
        ), patch(
            "core.complete_voiceover_direct._invoke_model", side_effect=fake_invoke
        ):
            run_central_complete_voiceover(
                product_code="P1",
                target_country="越南",
                target_language="越南语",
                top_category="配饰",
                product_type="抓夹",
                direction=direction,
                visual_plan={"shots": [{"supported_claim_keys": ["C1"]}]},
                model_command="mock-command",
                candidate_hook_id="AUDIENCE_NEED_CALLOUT",
            )
        self.assertNotIn("voiceover_expression_boundary", captured)
        self.assertNotIn("forbidden_wording", captured["spoken_brief"])

    def test_the_finished_copy_is_checked_and_reported(self):
        captured = {}
        # 模型仍然说出了被禁断言 —— 兜底必须判 FAIL 并留下证据，而不是静默。
        result = self._run(generated_zh="我最喜欢它的双层纱质蝴蝶造型。", captured=captured)
        boundary = result["expression_boundary"]
        self.assertTrue(boundary["applied"])
        self.assertGreater(boundary["cleaned_field_count"], 0)
        self.assertEqual(boundary["layer_dispatched_to"], "spoken_brief")
        self.assertEqual(boundary["check"]["status"], "FAIL")
        self.assertEqual(boundary["check"]["violations"][0]["term"], "纱")

    def test_a_clean_finished_copy_passes_the_check(self):
        captured = {}
        result = self._run(generated_zh="我最喜欢它的双层蝴蝶造型。", captured=captured)
        self.assertEqual(result["expression_boundary"]["check"]["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
