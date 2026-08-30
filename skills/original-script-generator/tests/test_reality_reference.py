from __future__ import annotations

import unittest
from unittest.mock import patch

from core.reality_reference import (
    ExecutionCard,
    _physical_compatibility,
    _product_family,
    assemble_reality_script,
    authenticity_review,
    build_content_bundle_brief,
    build_p2_lite,
    build_reality_direction_packages,
    compile_execution_card,
    normalize_creator_wear_state_continuity,
    project_event_blueprint_to_visual_plan,
    review_creator_clip_information_gain,
    select_execution_reference,
    validate_visual_adaptation,
    validate_creator_recording_blueprint,
    validate_voiceover_plan,
    validate_voiceover_visual_grounding,
)
from core.reality_reference_prompts import (
    build_complete_script_blueprint_prompt,
    build_visual_adaptation_prompt,
)
from core.reality_voiceover_bridge import (
    _hook_delivery_status,
    _hook_qc_status,
    _preferred_hook_id,
    _selection_readiness,
    _silent_windows,
    build_voiceover_expression_contract,
    build_voiceover_argument_contract,
    build_voiceover_variant_id,
    resolve_voiceover_hook_policy,
    run_central_voiceover,
    select_voiceover_claim_atoms,
)


def observed_row(**overrides):
    row = {
        "profile_id": "SP_VIDEO_1",
        "video_id": "V1",
        "asset_id": "A1",
        "profile_type": "VIDEO_INDEPENDENT",
        "independence_level": "PARTIAL",
        "extractor_version": "extractor-v2",
        "duration_sec": 15.0,
        "measured_shot_count": 2,
        "measured_shots": [
            {"shot_index": 1, "start_sec": 0, "end_sec": 3},
            {"shot_index": 2, "start_sec": 3, "end_sec": 15},
        ],
        "semantic_beats": [
            {
                "coarse_beat": "HOOK",
                "start_sec": 0,
                "end_sec": 3,
                "visual_action": "手把外套前襟移入近景",
                "product_state": "PARTIAL",
                "confidence": 0.95,
            },
            {
                "coarse_beat": "PROOF",
                "start_sec": 3,
                "end_sec": 15,
                "visual_action": "镜头停在按扣和口袋细节",
                "product_state": "RESULT_STATE",
                "confidence": 0.9,
            },
        ],
        "coarse_beat_sequence": ["HOOK", "PROOF"],
        "content_carrier": "HAND_ONLY",
        "continuity_mode": "CONTINUOUS_LOW_CUT",
        "proof_mechanisms": ["DETAIL_MACRO"],
        "visual_hook_type": "PRODUCT_REVEAL",
        "camera_grammar": ["FIXED_CLOSE"],
        "action_chain": ["SHOW_RESULT", "FINAL_HOLD"],
        "audio_mode": "BGM_ONLY",
        "availability_status": "AVAILABLE_CACHED",
        "extraction_confidence": 0.95,
    }
    row.update(overrides)
    return row


def assignment():
    return {
        "direction_assignment_id": "SRA_1",
        "output_slot": "S1",
        "cluster_id": 5,
        "cluster_version": "v1",
        "structure_contract": {
            "hard_constraints": {
                "beat_sequence": ["HOOK", "PROOF"],
                "content_carrier": "HAND_ONLY",
                "continuity_mode": "CONTINUOUS_LOW_CUT",
            }
        },
    }


class RealityReferenceTests(unittest.TestCase):
    def test_local_deterministic_voiceover_is_preview_only(self):
        direction = {
            "direction_assignment_id": "D1",
            "content_bundle_brief": {
                "primary_hook_id": "GENERAL_PRODUCT_SHARE",
                "eligible_hook_ids": ["GENERAL_PRODUCT_SHARE"],
                "claim_atoms": [
                    {"claim_key": "C1", "fact_text": "短款衣长", "role": "core_result"}
                ],
            },
            "p2_lite": {"primary_observation": "短款衣长"},
            "creative_blueprint": {},
        }
        visual = {
            "shots": [
                {
                    "shot_no": 1,
                    "duration": "0-15s",
                    "supported_claim_keys": ["C1"],
                }
            ]
        }
        ready = {
            "hook_id": "GENERAL_PRODUCT_SHARE",
            "selected_claim_count": 1,
            "selected_claim_ids": ["C1"],
            "beats": [
                {
                    "suggested_start_ms": 0,
                    "suggested_end_ms": 8000,
                    "speech_text": "ข้อความ",
                    "chinese_translation": "中文",
                    "role": "HOOK",
                }
            ],
            "voiceover_engine_qc": {
                "duration_estimate": {"estimated_sec": 8, "upper_sec": 9},
                "warnings": [],
            },
            "voiceover_expression_contract": {},
            "voiceover_copy_plan": {},
        }
        with patch(
            "core.reality_voiceover_bridge.load_active_voiceover_hooks",
            return_value=[{"hook_id": "GENERAL_PRODUCT_SHARE"}],
        ), patch(
            "core.reality_voiceover_bridge.run_voiceover_engine_variant",
            return_value=ready,
        ):
            result = run_central_voiceover(
                product_code="P1",
                target_country="泰国",
                target_language="泰语",
                direction=direction,
                visual_plan=visual,
                model_command="",
            )
        self.assertEqual("LOCAL_DETERMINISTIC", result["copy_generation_mode"])
        self.assertFalse(result["selection_readiness"]["auto_selectable"])
        self.assertEqual("PREVIEW_ONLY", result["hook_delivery_status"])
        self.assertEqual("PREVIEW_ONLY", result["hook_qc_status"])

    def test_structure_only_direction_never_fabricates_video_reference(self):
        class EmptyRepository:
            def load_cards_for_assignment(self, _assignment):
                return []

        result = build_reality_direction_packages(
            {"selection_run_id": "SR_1", "assignments": [assignment()]},
            anchor_card={
                "hard_anchors": [{"anchor": "短款衣长"}],
                "display_anchors": [{"anchor": "前襟纽扣"}],
                "category_execution_contract": {"display_family": "apparel"},
            },
            product_type="外套",
            top_category="女装",
            repository=EmptyRepository(),
            selling_point_catalog=[{
                "value_id": "ARG1",
                "primary_selling_point": "穿衣更简单",
                "argument_kind": "SELLING_ARGUMENT",
            }],
            allow_structure_only=True,
        )
        self.assertEqual(result["selected_count"], 1)
        direction = result["directions"][0]
        self.assertEqual(direction["structure_source_mode"], "STRUCTURE_ONLY")
        self.assertEqual(direction["reference_selection"]["status"], "STRUCTURE_ONLY")
        self.assertEqual(direction["execution_reference"]["execution_card_id"], "")
        self.assertEqual(direction["execution_reference"]["action_spine"], [])

    def test_argument_contract_only_allows_rhetorical_conflict_for_authorised_tension(self):
        visual = {"shots": [{"shot_no": 1, "supported_claim_keys": ["C1"]}]}
        base_bundle = {
            "content_bundle_id": "CB1",
            "content_mode": "SELLING_ARGUMENT",
            "value_proposition": {"text": "腰线视觉更清晰", "status": "AVAILABLE"},
            "selling_argument": {
                "argument_id": "ARG1",
                "status": "AVAILABLE",
                "script_readiness": "READY",
                "core_proof_claim_keys": ["C1"],
            },
            "claim_atoms": [{"claim_key": "C1", "fact_text": "短款衣长"}],
        }
        no_tension = dict(base_bundle)
        no_tension["audience_tension"] = {"status": "UNAVAILABLE", "text": ""}
        contract = build_voiceover_argument_contract(
            {"content_bundle_brief": no_tension, "creative_blueprint": {}}, visual
        )
        self.assertFalse(contract["expression_policy"]["rhetorical_conflict_allowed"])
        self.assertEqual(
            contract["expression_policy"]["hook_stance"],
            "PERSONAL_SELECTION_CRITERION",
        )

        with_tension = dict(base_bundle)
        with_tension["audience_tension"] = {
            "status": "AVAILABLE",
            "text": "基础穿搭怎么更利落？",
        }
        contract = build_voiceover_argument_contract(
            {"content_bundle_brief": with_tension, "creative_blueprint": {}}, visual
        )
        self.assertTrue(contract["expression_policy"]["rhetorical_conflict_allowed"])

    def test_expression_contract_separates_selling_scenario_from_visual_location(self):
        direction = {
            "content_bundle_brief": {
                "content_mode": "SELLING_ARGUMENT",
                "audience_need_authority": "APPROVED_SELLING_SCENARIO",
                "audience_situation": "旅行只想带一件能覆盖通勤和休闲的外套",
                "multi_scenario_authorized": True,
                "value_proposition": {
                    "text": "旅行带一件就能覆盖多种日常场景",
                    "status": "AVAILABLE",
                },
                "selling_argument": {
                    "argument_id": "ARG_MULTI",
                    "status": "AVAILABLE",
                    "audience_need_authority": "APPROVED_SELLING_SCENARIO",
                    "audience_situation": "旅行只想带一件能覆盖通勤和休闲的外套",
                    "multi_scenario_authorized": True,
                    "core_proof_claim_keys": ["C1"],
                },
                "audience_tension": {"status": "UNAVAILABLE", "text": ""},
                "claim_atoms": [{"claim_key": "C1", "fact_text": "短款衣长"}],
            },
            "creative_blueprint": {
                "voiceover_grounding_mode": "CONTENT_FIRST_WHOLE_VIDEO",
                "scene": {"location": "酒店客房", "moment": "收拾短途行李时"},
                "event_design": {"natural_event": "把外套放进行李后准备出门"},
            },
        }
        expression = build_voiceover_expression_contract(
            direction,
            {"shots": [{"shot_no": 1, "supported_claim_keys": ["C1"]}]},
        )
        context = expression["voiceover_context_contract"]
        self.assertEqual(context["context_mode"], "SELLING_SCENARIO")
        self.assertEqual(context["scenario_budget"], 2)
        self.assertEqual(
            context["current_life_moment"]["authority"], "CREATIVE_DESIGN"
        )
        self.assertEqual(
            context["visual_location"]["spoken_use_policy"],
            "OPTIONAL_NOT_PRODUCT_PROOF",
        )
        self.assertFalse(context["hard_required"])

    def test_operator_confirmed_argument_keeps_authority_and_reframe_policy(self):
        visual = {"shots": [{"shot_no": 1, "supported_claim_keys": ["C1"]}]}
        bundle = {
            "content_bundle_id": "CB_OPERATOR",
            "content_mode": "SELLING_ARGUMENT",
            "value_proposition": {
                "text": "普通基础穿搭也能呈现更精致的感觉",
                "status": "AVAILABLE",
            },
            "selling_argument": {
                "argument_id": "OPERATOR_S1",
                "status": "AVAILABLE",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                "source_argument_id": "S1",
                "authorization_source": "FEISHU_OPERATOR_CONFIRMED",
                "mapping_status": "UNMAPPED",
                "operator_expression": "运营维护的原始卖点",
                "expression_policy": "SEMANTIC_AUTHORITY_NOT_VERBATIM",
                "respectful_reframe_required": True,
                "primary_demonstration_mode": "NECK_WORN",
                "demonstration_policy": "ONE_PRIMARY_MODE_PER_15S",
                "voiceover_scope_policy": "PRIMARY_DEMONSTRATION_MODE_ONLY",
                "script_readiness": "READY",
                "core_proof_claim_keys": ["C1"],
            },
            "audience_tension": {"status": "UNAVAILABLE", "text": ""},
            "claim_atoms": [{"claim_key": "C1", "fact_text": "直筒版型"}],
        }
        contract = build_voiceover_argument_contract(
            {"content_bundle_brief": bundle, "creative_blueprint": {}}, visual
        )
        selling = contract["content"]["selling_argument"]
        self.assertEqual(selling["source_argument_id"], "S1")
        self.assertEqual(selling["mapping_status"], "UNMAPPED")
        self.assertTrue(selling["respectful_reframe_required"])
        self.assertEqual(selling["primary_demonstration_mode"], "NECK_WORN")
        self.assertEqual(
            selling["voiceover_scope_policy"],
            "PRIMARY_DEMONSTRATION_MODE_ONLY",
        )
        self.assertEqual(
            contract["expression_policy"]["selling_point_authority"],
            "FEISHU_OPERATOR_CONFIRMED",
        )
        self.assertEqual(
            contract["expression_policy"]["operator_expression_policy"],
            "SEMANTIC_AUTHORITY_NOT_VERBATIM",
        )

    def test_worn_accessory_prefers_wearer_or_mixed_reference(self):
        self.assertEqual("WORN_ACCESSORY", _product_family("围巾", "配饰"))
        wearer = ExecutionCard.__new__(ExecutionCard)
        wearer.content_carrier = "WEARER_ACTIVE"
        hand = ExecutionCard.__new__(ExecutionCard)
        hand.content_carrier = "HAND_ONLY"
        self.assertGreater(
            _physical_compatibility(wearer, "围巾", "配饰"),
            _physical_compatibility(hand, "围巾", "配饰"),
        )

    def test_small_accessory_still_prefers_hand_reference(self):
        self.assertEqual("ACCESSORY", _product_family("戒指", "配饰"))
        hand = ExecutionCard.__new__(ExecutionCard)
        hand.content_carrier = "HAND_ONLY"
        wearer = ExecutionCard.__new__(ExecutionCard)
        wearer.content_carrier = "WEARER_ACTIVE"
        self.assertGreater(
            _physical_compatibility(hand, "戒指", "配饰"),
            _physical_compatibility(wearer, "戒指", "配饰"),
        )

    def test_event_blueprint_projects_three_passages_without_new_visual_llm(self):
        direction = {
            "creative_blueprint": {
                "creative_blueprint_id": "CBP_EVENT",
                "event_design": {
                    "natural_event": "穿好外搭后拿起随身包，经过窗边准备离开",
                    "core_result_moment": "拿起包站直时看清完整上身结果",
                },
                "scene": {
                    "location": "客厅窗边",
                    "moment": "出门前",
                    "lighting": "自然窗光",
                },
                "macro_visual_passages": [
                    {
                        "passage_no": 1,
                        "narrative_role": "EVENT_ENTRY",
                        "visible_process": "人物穿好外搭，包放在一旁",
                        "observable_action": "人物完成外搭穿着动作",
                        "camera_observation": "固定半身中景",
                        "product_visibility": "PARTIAL",
                        "supported_claim_keys": ["C1"],
                    },
                    {
                        "passage_no": 2,
                        "narrative_role": "EVENT_PROOF",
                        "visible_process": "人物拿起包站直，完整上身状态可见",
                        "observable_action": "人物伸手拿起随身包",
                        "camera_observation": "固定半身中景",
                        "product_visibility": "FULL",
                        "supported_claim_keys": ["C1", "C2"],
                    },
                    {
                        "passage_no": 3,
                        "narrative_role": "EVENT_END",
                        "visible_process": "人物带包经过窗边",
                        "observable_action": "人物向画面边缘走去",
                        "camera_observation": "固定中景",
                        "product_visibility": "FULL",
                        "supported_claim_keys": ["C2"],
                    },
                ],
            },
            "creative_diversity_contract": {"contract_id": "CDV_EVENT"},
            "content_bundle_brief": {
                "content_bundle_id": "CBR_EVENT",
                "claim_atoms": [
                    {"claim_key": "C1", "fact_text": "短款宽松轮廓"},
                    {"claim_key": "C2", "fact_text": "前襟排扣与两侧口袋"},
                ],
            },
            "execution_reference": {
                "execution_card_id": "EXEC_EVENT",
                "shot_execution_spine": [
                    {"order": 1},
                    {"order": 2},
                    {"order": 3},
                ],
                "unknown_fields": ["location"],
            },
            "structure_execution_plan": {
                "shot_plan": [
                    {
                        "time_range": f"{index * 2.5}-{(index + 1) * 2.5}s",
                        "structure_beat": "HOOK" if index == 0 else "PROOF",
                        "carrier_mode": "WEARER_ACTIVE",
                        "continuity_group": "C1",
                        "opening_mechanism": "PRODUCT_REVEAL" if index == 0 else "",
                    }
                    for index in range(6)
                ]
            },
        }
        visual = project_event_blueprint_to_visual_plan(direction=direction)
        self.assertEqual("DETERMINISTIC_EVENT_PROJECTION", visual["source"])
        self.assertEqual(6, len(visual["shots"]))
        self.assertEqual(
            [1, 1, 2, 2, 3, 3],
            [item["reference_spine_orders"][0] for item in visual["shots"]],
        )
        self.assertEqual(3, len({item["observable_action"] for item in visual["shots"]}))
        validation = validate_visual_adaptation(
            visual,
            execution_plan=direction["structure_execution_plan"],
            execution_reference=direction["execution_reference"],
            content_bundle_brief=direction["content_bundle_brief"],
            creative_blueprint=direction["creative_blueprint"],
            creative_diversity_contract=direction["creative_diversity_contract"],
        )
        self.assertTrue(validation["valid"], validation["issues"])

    def test_wearer_direction_prefers_natural_need_callout(self):
        selected = _preferred_hook_id(
            {
                "execution_reference": {"content_carrier": "WEARER_ACTIVE"},
                "content_bundle_brief": {
                    "audience_tension": {"text": "想找腰线更清晰的短外搭"},
                },
            },
            ["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT", "GENERAL_PRODUCT_SHARE"],
        )
        self.assertEqual("AUDIENCE_NEED_CALLOUT", selected)

    def test_detail_execution_does_not_force_detail_speech_hook(self):
        selected = _preferred_hook_id(
            {
                "execution_reference": {
                    "content_carrier": "WEARER_ACTIVE",
                    "visual_hook_type": "ACTION_HOOK",
                    "proof_mechanisms": ["DETAIL_MACRO"],
                }
            },
            ["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT", "GENERAL_PRODUCT_SHARE"],
        )
        self.assertEqual("GENERAL_PRODUCT_SHARE", selected)

    def test_detail_content_can_still_choose_detail_speech_hook(self):
        selected = _preferred_hook_id(
            {
                "content_bundle_brief": {
                    "claim_atoms": [
                        {"claim_key": "C1", "fact_text": "前襟五颗扣子细节"}
                    ],
                }
            },
            ["DETAIL_SURPRISE", "GENERAL_PRODUCT_SHARE"],
        )
        self.assertEqual("DETAIL_SURPRISE", selected)

    def test_hook_policy_filters_false_pain_and_uses_one_truthful_authority(self):
        direction = {
            "content_bundle_brief": {
                "preferred_hook_angles": ["PAIN_REFRAME", "DETAIL_SURPRISE"],
                "primary_hook_id": "PAIN_REFRAME",
                "audience_tension": {"text": "想快速看清前襟和口袋细节"},
            },
            "execution_reference": {"content_carrier": "STATIC_PRODUCT"},
        }
        policy = resolve_voiceover_hook_policy(
            direction,
            {"PAIN_REFRAME", "DETAIL_SURPRISE", "GENERAL_PRODUCT_SHARE"},
        )

        self.assertEqual(["DETAIL_SURPRISE"], policy["eligible_hook_ids"])
        self.assertEqual("DETAIL_SURPRISE", policy["selected_hook_id"])
        with self.assertRaises(ValueError):
            resolve_voiceover_hook_policy(
                direction,
                {"PAIN_REFRAME", "DETAIL_SURPRISE"},
                requested_hook_id="PAIN_REFRAME",
            )

    def test_voiceover_claim_selection_keeps_two_best_facts(self):
        selected, suppressed = select_voiceover_claim_atoms(
            [
                {"claim_key": "C1", "fact_text": "短款衣长", "role": "core_result"},
                {"claim_key": "C2", "fact_text": "前襟排扣", "role": "visual_proof"},
                {"claim_key": "C3", "fact_text": "两侧口袋", "role": "supporting_fact"},
            ]
        )

        self.assertEqual(["C1", "C2"], [item["claim_key"] for item in selected])
        self.assertEqual(["C3"], [item["claim_key"] for item in suppressed])

    def test_expression_contract_combines_structure_claim_roles_and_supported_shots(self):
        contract = build_voiceover_expression_contract(
            {
                "content_bundle_brief": {
                    "content_mainline": "短款轮廓与前襟细节",
                    "claim_atoms": [
                        {"claim_key": "CLM_LENGTH", "fact_text": "短款衣长", "role": "core_result"},
                        {"claim_key": "CLM_CLOSURE", "fact_text": "前襟扣位", "role": "visual_proof"},
                    ],
                },
                "structure_execution_plan": {
                    "macro_family_key": "HOOK>PROOF>ENDING",
                    "shot_plan": [
                        {
                            "structure_beat": "HOOK",
                            "carrier_mode": "WEARER_ACTIVE",
                            "continuity_group": "C1",
                            "opening_mechanism": "RESULT_REVEAL",
                        },
                        {
                            "structure_beat": "PROOF",
                            "carrier_mode": "HAND_ONLY",
                            "continuity_group": "C2",
                        },
                    ],
                },
                "execution_reference": {
                    "proof_mechanisms": ["DETAIL_MACRO"],
                    "visual_hook_type": "RESULT_REVEAL",
                },
            },
            {
                "shots": [
                    {"supported_claim_keys": ["CLM_LENGTH"]},
                    {"supported_claim_keys": ["CLM_CLOSURE"]},
                    {"supported_claim_keys": []},
                ]
            },
        )
        self.assertEqual("voiceover-expression-contract-v2", contract["schema_version"])
        self.assertEqual("RESULT_REVEAL", contract["structure_context"]["opening_mechanism"])
        self.assertEqual([1], contract["claim_atoms"][0]["supported_shot_nos"])
        self.assertEqual([2], contract["claim_atoms"][1]["supported_shot_nos"])
        self.assertTrue(contract["speech_policy"]["allow_non_claim_rhetoric"])
        self.assertFalse(contract["speech_policy"]["generic_cta_required"])
        self.assertEqual(
            "whole_video_semantic_segment",
            contract["speech_policy"]["alignment_granularity"],
        )
        self.assertFalse(contract["speech_policy"]["opening_must_name_visible_fact"])
        self.assertFalse(contract["audio_policy"]["silence_window_required"])
        self.assertFalse(contract["speech_policy"]["claim_coverage_required"])
        self.assertEqual(2, contract["speech_policy"]["preferred_spoken_claim_count"])
        self.assertFalse(contract["speech_policy"]["soft_warning_polish_enabled"])
        self.assertEqual([], contract["speech_policy"]["targeted_warning_polish_codes"])
        self.assertTrue(
            contract["speech_policy"]["approved_sample_guidance_enabled"]
        )
        self.assertTrue(contract["speech_policy"]["narrative_anchor_preferred"])
        self.assertTrue(contract["speech_policy"]["relationship_language_preferred"])
        self.assertEqual(
            "central-voiceover-v32-relational-language",
            contract["speech_policy"]["voiceover_policy_version"],
        )

    def test_expression_contract_passes_lived_moment_family_to_central_engine(self):
        direction = {
            "creative_diversity_contract": {
                "moment_family_id": "QUICK_ERRAND",
                "scene_motif": "公寓大堂快递柜旁",
                "opening_action": "人物关上快递柜门后走向出口",
                "required_presentation_mode": "WEARER_ACTIVE",
            },
            "creative_blueprint": {
                "scene": {
                    "location": "公寓大堂快递柜旁",
                    "moment": "临时下楼取件",
                },
                "event_design": {
                    "natural_event": "人物取完快递准备离开",
                },
            },
            "content_bundle_brief": {"claim_atoms": []},
            "structure_execution_plan": {"shot_plan": []},
        }

        contract = build_voiceover_expression_contract(direction, {"shots": []})

        top_level = contract["creative_voice_context"]["lived_moment_binding"]
        nested = contract["argument_contract"]["creative_voice_context"][
            "lived_moment_binding"
        ]
        self.assertEqual("QUICK_ERRAND", top_level["moment_family_id"])
        self.assertEqual("人物取完快递准备离开", top_level["speech_anchor_zh"])
        self.assertEqual(top_level, nested)

    def test_hook_delivery_status_keeps_structure_and_surface_separate(self):
        qc = {"warnings": [{"code": "HOOK_DELIVERY_TOO_FLAT"}]}
        self.assertEqual("FLAT_WARNING", _hook_delivery_status(qc))
        self.assertEqual("WEAK", _hook_qc_status("DETAIL_SURPRISE", "DETAIL_SURPRISE", qc))

    def test_selection_readiness_keeps_15_to_18_seconds_as_soft_warning(self):
        readiness = _selection_readiness(
            {"duration_estimate": {"estimated_sec": 17.38, "upper_sec": 19.99}}
        )
        self.assertEqual("READY_WITH_DURATION_WARNING", readiness["status"])
        self.assertTrue(readiness["auto_selectable"])
        self.assertEqual("COPY_DURATION_SOFT_WARNING", readiness["warning_code"])

    def test_selection_readiness_repairs_only_after_18_seconds(self):
        readiness = _selection_readiness(
            {"duration_estimate": {"estimated_sec": 18.2, "upper_sec": 20.1}}
        )
        self.assertEqual("LONG_REPAIR_REQUIRED", readiness["status"])
        self.assertFalse(readiness["auto_selectable"])
        self.assertEqual("COPY_DURATION_ESTIMATE_WARNING", readiness["warning_code"])

    def test_expression_contract_dedupes_contained_facts_with_same_visual_evidence(self):
        contract = build_voiceover_expression_contract(
            {
                "content_bundle_brief": {
                    "claim_atoms": [
                        {
                            "claim_key": "CLM_CROPPED_FULL",
                            "fact_text": "短款衣长收在腰线附近",
                            "role": "core_result",
                        },
                        {
                            "claim_key": "CLM_CROPPED_SHORT",
                            "fact_text": "短款衣长",
                            "role": "visual_proof",
                        },
                        {
                            "claim_key": "CLM_SLEEVE",
                            "fact_text": "袖部抽褶与银色椭圆扣",
                            "role": "visual_proof",
                        },
                    ],
                },
                "structure_execution_plan": {"shot_plan": []},
            },
            {
                "shots": [
                    {
                        "supported_claim_keys": [
                            "CLM_CROPPED_FULL",
                            "CLM_CROPPED_SHORT",
                            "CLM_SLEEVE",
                        ]
                    }
                ]
            },
        )

        self.assertEqual(
            ["短款衣长收在腰线附近", "袖部抽褶与银色椭圆扣"],
            [item["fact_text"] for item in contract["claim_atoms"]],
        )
        self.assertEqual("core_result", contract["claim_atoms"][0]["role"])

    def test_visual_prompt_builds_with_anchor_subset(self):
        prompt = build_visual_adaptation_prompt(
            target_country="泰国",
            product_type="外套",
            anchor_card={"hard_anchors": [{"anchor": "五颗按扣"}]},
            direction={"structure_execution_plan": {}, "execution_reference": {}, "p2_lite": {}},
        )
        self.assertIn("五颗按扣", prompt)
        self.assertIn("visual-adaptation-v2", prompt)
        self.assertIn("supported_claim_keys", prompt)
        self.assertIn("全片唯一人物行为主线", prompt)

    def test_complete_blueprint_prompt_serializes_direction_package(self):
        prompt = build_complete_script_blueprint_prompt(
            target_country="泰国",
            product_type="外套",
            direction={
                "content_bundle_brief": {"content_mainline": "短款衣长"},
                "structure_execution_plan": {"macro_family_key": "HOOK>PROOF"},
                "execution_reference": {
                    "content_carrier": "WEARER_ACTIVE",
                    "unknown_fields": ["location"],
                },
                "creative_diversity_contract": {"contract_id": "CDV_1"},
            },
        )
        self.assertIn("短款衣长", prompt)
        self.assertIn("CDV_1", prompt)
        self.assertIn("CREATIVE_DESIGN", prompt)
        self.assertIn("complete-script-blueprint-v4-carrier", prompt)
        self.assertIn("macro_visual_passages", prompt)
        self.assertIn("retention_hook", prompt)
        self.assertIn("不要求突然停住", prompt)

    def test_direct_share_prompt_and_blueprint_require_real_clip_design(self):
        direction = {
            "content_bundle_brief": {"content_mainline": "短款比例"},
            "structure_execution_plan": {"macro_family_key": "HOOK>PROOF"},
            "execution_reference": {"content_carrier": "WEARER_ACTIVE"},
            "creative_diversity_contract": {"contract_id": "CDV_DIRECT"},
            "creator_recording_profile": {
                "enabled": True,
                "recording_mode": "CREATOR_DIRECT_SHARE",
                "capture_preset": "WORN_DIRECT_SHARE",
            },
        }
        prompt = build_complete_script_blueprint_prompt(
            target_country="泰国",
            product_type="外套",
            direction=direction,
        )
        self.assertIn("CREATOR_DIRECT_SHARE", prompt)
        self.assertIn("recording_context", prompt)
        self.assertIn("clip_design", prompt)
        self.assertIn("不要求生活剧情、自拍比例或情绪表演", prompt)
        self.assertIn("以下不算新关系", prompt)
        self.assertIn("中景改成稍宽中景", prompt)
        self.assertIn("不得为了增加片段而补入原结构没有的USE或ENDING", prompt)
        self.assertNotIn("每两个clip至少", prompt)

        invalid = validate_creator_recording_blueprint(
            {"recording_context": {}, "clip_design": []},
            direction["creator_recording_profile"],
        )
        self.assertFalse(invalid["valid"])

    def test_direct_share_information_gain_is_soft_and_flags_repeated_late_standing(self):
        profile = {
            "enabled": True,
            "recording_mode": "CREATOR_DIRECT_SHARE",
            "capture_preset": "WORN_DIRECT_SHARE",
        }
        blueprint = {
            "clip_design": [
                {
                    "clip_no": 1,
                    "recording_relation": "固定手机正面",
                    "visible_process": "人物穿好外套正面分享整体效果",
                },
                {
                    "clip_no": 2,
                    "recording_relation": "固定手机正面",
                    "framing": "上身近景",
                    "visible_process": "人物站着展示口袋和袖口",
                },
                {
                    "clip_no": 3,
                    "recording_relation": "固定手机正面",
                    "framing": "稍宽中景",
                    "visible_process": "人物继续站着面对手机分享",
                },
            ]
        }
        review = review_creator_clip_information_gain(blueprint, profile)
        self.assertEqual("LOW_INFORMATION_GAIN", review["status"])
        self.assertFalse(review["is_blocking"])
        self.assertEqual([2, 3], review["low_information_gain_pairs"][0]["clip_pair"])

    def test_direct_share_information_gain_accepts_natural_relation_change(self):
        profile = {
            "enabled": True,
            "recording_mode": "CREATOR_DIRECT_SHARE",
            "capture_preset": "WORN_DIRECT_SHARE",
        }
        blueprint = {
            "clip_design": [
                {"clip_no": 1, "visible_process": "人物面对手机分享整体效果"},
                {"clip_no": 2, "visible_process": "人物站着展示在身细节"},
                {"clip_no": 3, "visible_process": "人物坐在窗边座位继续分享穿搭关系"},
            ]
        }
        review = review_creator_clip_information_gain(blueprint, profile)
        self.assertEqual("SUFFICIENT", review["status"])
        self.assertEqual([], review["low_information_gain_pairs"])

    def test_product_first_direction_is_not_subject_to_worn_information_gain_review(self):
        review = review_creator_clip_information_gain(
            {"clip_design": []},
            {
                "enabled": True,
                "recording_mode": "CREATOR_DIRECT_SHARE",
                "capture_preset": "PRODUCT_FIRST_THEN_WORN",
            },
        )
        self.assertEqual("NOT_APPLICABLE", review["status"])

    def test_direct_share_blocks_wear_state_regression_without_style_ratio_gate(self):
        profile = {
            "enabled": True,
            "recording_mode": "CREATOR_DIRECT_SHARE",
            "capture_preset": "PRODUCT_FIRST_THEN_WORN",
        }
        base = {
            "recording_context": {
                "recording_mode": "CREATOR_DIRECT_SHARE",
                "recording_motivation": "直接分享当前外套",
                "viewer_awareness": "知道正在录制",
            },
            "clip_design": [
                {
                    "clip_job": "商品开场",
                    "framing": "商品近景",
                    "visible_process": "外套平铺在长凳上",
                },
                {
                    "clip_job": "穿着结果",
                    "framing": "人物中景",
                    "visible_process": "人物已经穿好外套面对手机",
                },
                {
                    "clip_job": "在身细节",
                    "framing": "上身近景",
                    "visible_process": "外套保持穿着，镜头看清正面细节",
                },
            ],
        }
        valid = validate_creator_recording_blueprint(base, profile)
        self.assertTrue(valid["valid"], valid["issues"])

        regressed = {**base, "clip_design": [dict(item) for item in base["clip_design"]]}
        regressed["clip_design"][2]["visible_process"] = "外套重新平铺在长凳上拍细节"
        invalid = validate_creator_recording_blueprint(regressed, profile)
        self.assertFalse(invalid["valid"])
        self.assertEqual([3], invalid["wear_state_continuity"]["violating_clips"])

    def test_direct_share_normalizer_projects_post_wear_flat_lay_to_on_body_detail(self):
        profile = {
            "enabled": True,
            "recording_mode": "CREATOR_DIRECT_SHARE",
            "capture_preset": "PRODUCT_FIRST_THEN_WORN",
        }
        blueprint = {
            "recording_context": {
                "recording_mode": "CREATOR_DIRECT_SHARE",
                "recording_motivation": "直接分享当前外套",
                "viewer_awareness": "知道正在录制",
            },
            "clip_design": [
                {
                    "clip_no": 1,
                    "clip_job": "商品开场",
                    "framing": "商品近景",
                    "visible_process": "外套平铺在长凳上",
                },
                {
                    "clip_no": 2,
                    "clip_job": "穿着结果",
                    "framing": "人物中景",
                    "visible_process": "人物已经穿好外套面对手机",
                },
                {
                    "clip_no": 3,
                    "clip_job": "口袋细节",
                    "framing": "商品近景",
                    "visible_process": "外套重新平铺在长凳上拍口袋",
                    "supported_claim_keys": ["C1"],
                },
            ]
        }
        normalized = normalize_creator_wear_state_continuity(blueprint, profile)
        self.assertEqual(
            ["PRODUCT_OFF_BODY", "WORN", "WORN_DETAIL"],
            normalized["wear_state_continuity"]["states"],
        )
        self.assertIn("保持穿着", normalized["clip_design"][2]["visible_process"])
        self.assertEqual(["C1"], normalized["clip_design"][2]["supported_claim_keys"])
        validation = validate_creator_recording_blueprint(normalized, profile)
        self.assertTrue(validation["valid"], validation["issues"])

    def test_direct_share_normalizer_does_not_treat_context_phrase_as_physical_placement(self):
        profile = {
            "enabled": True,
            "recording_mode": "CREATOR_DIRECT_SHARE",
            "capture_preset": "WORN_DIRECT_SHARE",
        }
        rhetorical_process = (
            "9至15秒人物自然坐在窗边座位，身体保持舒展，袖子在坐姿中仍保留宽松轮廓，"
            "短外搭与高腰牛仔裤的比例被放回真实咖啡厅空间中观察。"
        )
        blueprint = {
            "recording_context": {
                "recording_mode": "CREATOR_DIRECT_SHARE",
                "recording_motivation": "直接分享当前外套",
                "viewer_awareness": "知道正在录制",
            },
            "clip_design": [
                {
                    "clip_no": 1,
                    "clip_job": "整体开场",
                    "framing": "人物中景",
                    "visible_process": "人物穿着外套面对手机分享",
                },
                {
                    "clip_no": 2,
                    "clip_job": "在身证明",
                    "framing": "上身近景",
                    "visible_process": "人物穿着外套展示在身轮廓",
                },
                {
                    "clip_no": 3,
                    "clip_job": "场景关系",
                    "framing": "窗边座位稍宽中景",
                    "visible_process": rhetorical_process,
                },
            ],
        }
        normalized = normalize_creator_wear_state_continuity(blueprint, profile)
        self.assertEqual("UNCHANGED", normalized["wear_state_continuity"]["status"])
        self.assertEqual(
            rhetorical_process,
            normalized["clip_design"][2]["visible_process"],
        )
        validation = validate_creator_recording_blueprint(normalized, profile)
        self.assertTrue(validation["valid"], validation["issues"])
        self.assertEqual(
            [], validation["wear_state_continuity"]["violating_clips"]
        )

    def test_direct_share_visual_plan_allows_natural_speaking_without_action_text(self):
        result = validate_visual_adaptation(
            {
                "execution_card_id": "E1",
                "creative_blueprint_id": "B1",
                "creative_design_authority": "CREATIVE_DESIGN",
                "creative_diversity_contract_id": "D1",
                "shots": [
                    {
                        "shot_content": "人物穿着外套面对手机自然说话",
                        "observable_action": "",
                        "product_visibility": "FULL",
                        "framing": "普通手机中景",
                        "setting_continuity": "客厅窗边",
                        "action_motivation": "直接分享当前外套",
                        "gaze_and_reaction": "自然看向手机",
                        "audio_hard_constraint": "NONE",
                        "audio_preference": "VOICEOVER_PREFERRED",
                        "structure_beat": "HOOK",
                        "carrier_mode": "WEARER_ACTIVE",
                        "continuity_group": "C1",
                        "opening_mechanism": "DIRECT_SHARE",
                        "reference_spine_orders": [1],
                        "supported_claim_keys": [],
                    }
                ],
            },
            execution_plan={
                "shot_plan": [
                    {
                        "structure_beat": "HOOK",
                        "carrier_mode": "WEARER_ACTIVE",
                        "continuity_group": "C1",
                        "opening_mechanism": "DIRECT_SHARE",
                    }
                ]
            },
            execution_reference={
                "execution_card_id": "E1",
                "shot_execution_spine": [{"order": 1}],
            },
            creative_blueprint={
                "creative_blueprint_id": "B1",
                "presentation_mode": "PERSON_ON_CAMERA",
                "recording_context": {"recording_mode": "CREATOR_DIRECT_SHARE"},
            },
            creative_diversity_contract={
                "contract_id": "D1",
                "required_presentation_mode": "PERSON_ON_CAMERA",
            },
        )
        self.assertTrue(result["valid"], result["issues"])

    def test_voiceover_variant_id_changes_with_visual_execution(self):
        routed = {
            "direction_assignment_id": "SRA_1",
            "execution_reference": {"execution_card_id": "EXEC_1"},
            "creative_diversity_contract": {"contract_id": "CDV_1"},
            "creative_blueprint": {"creative_blueprint_id": "CBP_1"},
        }
        first = build_voiceover_variant_id(
            "P1", routed, {"shots": [{"shot_content": "扣好前襟", "supported_claim_keys": ["C1"]}]}
        )
        second = build_voiceover_variant_id(
            "P1", routed, {"shots": [{"shot_content": "抬起袖口", "supported_claim_keys": ["C2"]}]}
        )
        self.assertNotEqual(first, second)

    def test_compile_requires_independent_video_profile(self):
        self.assertIsNone(
            compile_execution_card(
                observed_row(profile_type="PROMPT_ONLY"),
                cluster_run_id="prompt_only_full",
                cluster_id=5,
                cluster_version="v1",
            )
        )

    def test_compile_card_preserves_unknown_setting(self):
        card = compile_execution_card(
            observed_row(content_carrier="WEARER_ACTIVE"), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        self.assertIsNotNone(card)
        assert card is not None
        self.assertEqual(card.evidence_tier, "VIDEO_OBSERVED")
        self.assertIn("location", card.unknown_fields)
        self.assertEqual(card.shot_execution_spine[0]["observable_action"], "手把外套前襟移入近景")

    def test_selector_prefers_structural_match(self):
        good = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        bad = compile_execution_card(
            observed_row(
                profile_id="SP_VIDEO_2",
                content_carrier="WEARER_ACTIVE",
                continuity_mode="MULTI_CUT",
                coarse_beat_sequence=["HOOK", "USE_PROCESS", "ENDING"],
            ),
            cluster_run_id="prompt_only_full",
            cluster_id=5,
            cluster_version="v1",
        )
        assert good is not None and bad is not None
        result = select_execution_reference(
            assignment(), [bad, good], product_type="外套", top_category="女装", strict=False
        )
        self.assertEqual(result["status"], "SELECTED")
        self.assertEqual(result["selected_card"]["source_profile_id"], "SP_VIDEO_1")

    def test_selector_rejects_hair_only_action_for_apparel(self):
        hair = compile_execution_card(
            observed_row(
                semantic_beats=[
                    {
                        "coarse_beat": "HOOK",
                        "start_sec": 0,
                        "end_sec": 3,
                        "visual_action": "Woman applies the product to her hair",
                        "product_state": "PARTIAL",
                        "confidence": 0.95,
                    }
                ],
                source_cat1="配饰",
                source_cat2="发夹",
            ),
            cluster_run_id="prompt_only_full",
            cluster_id=5,
            cluster_version="v1",
        )
        assert hair is not None
        result = select_execution_reference(
            assignment(), [hair], product_type="外套", top_category="女装", strict=False
        )
        self.assertEqual(result["status"], "REFERENCE_INSUFFICIENT")

    def test_p2_lite_uses_one_primary_anchor(self):
        anchor = {
            "display_anchors": [
                {"anchor": "前襟按扣", "recommended_shot_type": "按扣近景"},
                {"anchor": "双翻盖口袋"},
            ]
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        p2 = build_p2_lite(anchor, card.to_dict())
        self.assertEqual(p2["primary_observation"], "前襟按扣")
        self.assertIn("前襟按扣", p2["primary_proof"])
        self.assertIn("镜头停在按扣和口袋细节", p2["primary_proof"])
        self.assertEqual(p2["secondary_fact"], "双翻盖口袋")

    def test_p2_lite_avoids_reusing_primary_observation(self):
        anchor = {
            "display_anchors": [
                {"anchor": "短款腰线"},
                {"anchor": "袖口银扣细节"},
            ]
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        p2 = build_p2_lite(anchor, card.to_dict(), excluded_observations=["短款腰线"])
        self.assertEqual(p2["primary_observation"], "袖口银扣细节")

    def test_content_bundle_selects_multiple_non_redundant_claim_atoms(self):
        anchor = {
            "display_anchors": [
                {"anchor": "短款高腰上身比例"},
                {"anchor": "画面中腿部线条更修长"},
                {"anchor": "前襟双口袋细节"},
                {"anchor": "米杏白色调"},
            ]
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        bundle = build_content_bundle_brief(anchor, card.to_dict(), product_type="短款外套")
        self.assertEqual(bundle["claim_atom_count"], 3)
        facts = [item["fact_text"] for item in bundle["claim_atoms"]]
        self.assertIn("短款衣长", facts)
        self.assertNotIn("高腰", " ".join(facts))
        groups = [item["semantic_group"] for item in bundle["claim_atoms"]]
        self.assertEqual(len(groups), len(set(groups)))
        self.assertFalse(bundle["single_proof_rule"])
        self.assertGreaterEqual(len(bundle["eligible_hook_ids"]), 2)
        self.assertEqual(bundle["preferred_hook_angles"][0], bundle["primary_hook_id"])

    def test_content_bundle_separates_value_tension_and_visual_proof(self):
        anchor = {
            "display_anchors": [
                {"anchor": "短款衣长"},
                {"anchor": "前襟五颗扣子"},
                {"anchor": "两侧大口袋"},
            ]
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="短款外套",
            selling_point_catalog=[
                {
                    "primary_selling_point": "基础穿搭加上短外搭后更完整、更有出门感",
                    "dominant_user_question": "普通基础穿搭怎么快速变得更完整？",
                    "proof_thesis": "短衣长、排扣和口袋共同形成完整外搭轮廓",
                    "decision_thesis": "适合想快速完成日常穿搭的人",
                    "script_role": "aura_enhancement",
                }
            ],
            product_selling_note="酷飒帅气通勤休闲风",
        )
        self.assertEqual(
            "基础穿搭加上短外搭后更完整、更有出门感",
            bundle["value_proposition"]["text"],
        )
        self.assertEqual(
            "普通基础穿搭怎么快速变得更完整？",
            bundle["audience_tension"]["text"],
        )
        self.assertEqual(bundle["proof_atoms"], bundle["claim_atoms"])
        self.assertIn("PAIN_REFRAME", bundle["preferred_hook_angles"])

    def test_authorized_general_argument_gets_safe_non_tension_hook_choices(self):
        anchor = {
            "display_anchors": [
                {"anchor": "黑色发饰轮廓"},
                {"anchor": "多齿插梳结构"},
            ]
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5,
            cluster_version="v1",
        )
        assert card is not None
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="发饰",
            selling_point_catalog=[{
                "value_id": "ARG_CONFIRMED_GENERAL",
                "primary_selling_point": "整理好发型后更适合日常出门搭配",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                "claim_type": "benefit",
                "visual_dependency": "FLEXIBLE",
            }],
        )

        self.assertEqual(bundle["audience_tension"]["status"], "UNAVAILABLE")
        self.assertIn("DISCOVERY_RESULT_PROMISE", bundle["eligible_hook_ids"])
        self.assertIn("USER_ADVOCACY_STANCE", bundle["eligible_hook_ids"])
        self.assertIn("GENERAL_PRODUCT_SHARE", bundle["eligible_hook_ids"])

    def test_approved_multi_scene_argument_authorizes_need_hook_without_fake_pain(self):
        anchor = {
            "display_anchors": [
                {"anchor": "短款衣长"},
                {"anchor": "前襟金属拉链"},
            ]
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5,
            cluster_version="v1",
        )
        assert card is not None
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="外套",
            selling_point_catalog=[{
                "value_id": "ARG_MULTI_SCENE",
                "primary_selling_point": "适配多种日常场景",
                "operator_expression": "适配多种日常场景",
                "source_operator_expression": "旅行带一件就好，办公室、通勤和休闲都能穿",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                "claim_type": "scenario",
                "claim_theme": "scenario",
                "concept_ids": ["CCP_MULTI_SCENE"],
            }],
        )

        self.assertEqual(bundle["audience_tension"]["status"], "UNAVAILABLE")
        self.assertEqual(
            bundle["audience_need_authority"], "APPROVED_SELLING_SCENARIO"
        )
        self.assertEqual(
            bundle["audience_situation"],
            "旅行带一件就好，办公室、通勤和休闲都能穿",
        )
        self.assertEqual(
            bundle["selling_argument"]["target_need"],
            "旅行带一件就好，办公室、通勤和休闲都能穿",
        )
        self.assertTrue(bundle["multi_scenario_authorized"])
        self.assertEqual(bundle["primary_hook_id"], "AUDIENCE_NEED_CALLOUT")
        self.assertNotIn("PAIN_REFRAME", bundle["eligible_hook_ids"])
        policy = resolve_voiceover_hook_policy(
            {"content_bundle_brief": bundle},
            {"AUDIENCE_NEED_CALLOUT", "GENERAL_PRODUCT_SHARE", "PAIN_REFRAME"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
        )
        self.assertTrue(policy["audience_need_authorized"])

    def test_voiceover_hook_policy_accepts_governed_concept_without_raw_tension(self):
        direction = {
            "content_bundle_brief": {
                "preferred_hook_angles": [
                    "PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT",
                    "GENERAL_PRODUCT_SHARE",
                ],
                "audience_tension": {"status": "UNAVAILABLE", "text": ""},
                "hook_tension_authority": "CENTRAL_CONCEPT",
            }
        }
        policy = resolve_voiceover_hook_policy(
            direction,
            {"PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT", "GENERAL_PRODUCT_SHARE"},
            requested_hook_id="PAIN_REFRAME",
        )
        self.assertEqual("PAIN_REFRAME", policy["selected_hook_id"])
        self.assertTrue(policy["tension_available"])
        self.assertEqual("CENTRAL_CONCEPT", policy["hook_tension_authority"])

    def test_scarf_central_concept_drives_hooks_without_raw_tension_text(self):
        anchor = {
            "display_anchors": [
                {"anchor": "深蓝条纹波点图案"},
                {"anchor": "方形丝巾轮廓"},
            ]
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="丝巾",
            selling_point_catalog=[{
                "value_id": "CENTRAL_HAIR_RESCUE",
                "primary_selling_point": "头发状态不理想时可以快速完成造型",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "argument_theme": "HAIR_RESCUE",
                "preferred_hook_ids": [
                    "PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT",
                    "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE",
                ],
                "hook_tension_authority": "CENTRAL_CONCEPT",
            }],
        )
        self.assertEqual("CENTRAL_CONCEPT", bundle["hook_tension_authority"])
        self.assertEqual("PAIN_REFRAME", bundle["preferred_hook_angles"][0])
        self.assertIn("AUDIENCE_NEED_CALLOUT", bundle["eligible_hook_ids"])

    def test_color_mood_central_hook_mapping_does_not_invent_pain(self):
        anchor = {"display_anchors": [{"anchor": "深蓝条纹波点图案"}]}
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="丝巾",
            selling_point_catalog=[{
                "value_id": "CENTRAL_COLOR_MOOD",
                "primary_selling_point": "深蓝色能带出海边氛围",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "argument_theme": "COLOR_MOOD",
                "preferred_hook_ids": [
                    "VISUAL_RESULT_DIRECT", "DETAIL_SURPRISE",
                    "GENERAL_PRODUCT_SHARE",
                ],
            }],
        )
        self.assertEqual("VISUAL_RESULT_DIRECT", bundle["preferred_hook_angles"][0])
        self.assertNotIn("PAIN_REFRAME", bundle["eligible_hook_ids"])

    def test_multi_use_bundle_consumes_single_demonstration_scope(self):
        anchor = {"display_anchors": [{"anchor": "深蓝条纹波点图案"}]}
        card = compile_execution_card(
            observed_row(content_carrier="WEARER_ACTIVE"),
            cluster_run_id="prompt_only_full",
            cluster_id=5,
            cluster_version="v1",
        )
        assert card is not None
        scoped_value = "一条丝巾可以灵活变化用法，这次用作颈部点缀"
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="丝巾",
            selling_point_catalog=[{
                "value_id": "CENTRAL_MULTI_USE",
                "primary_selling_point": "可以系在颈部、头发或包袋上",
                "voiceover_core_value": scoped_value,
                "scoped_creative_core_value": scoped_value,
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "argument_theme": "MULTI_USE",
                "primary_demonstration_mode": "NECK_WORN",
                "supported_demonstration_modes": [
                    "NECK_WORN", "HAIR_TIE", "BAG_ACCENT",
                ],
                "demonstration_policy": "ONE_PRIMARY_MODE_PER_15S",
                "voiceover_scope_policy": "PRIMARY_DEMONSTRATION_MODE_ONLY",
                "visual_dependency": "WEARER_REQUIRED",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            }],
        )
        self.assertEqual(scoped_value, bundle["content_mainline"])
        self.assertEqual(scoped_value, bundle["selling_argument"]["core_value"])
        self.assertEqual(
            "PRIMARY_DEMONSTRATION_MODE_ONLY",
            bundle["selling_argument"]["voiceover_scope_policy"],
        )
        self.assertNotIn("包袋", bundle["selling_argument"]["core_value"])

    def test_content_bundle_keeps_generation_risk_out_of_consumer_tension(self):
        anchor = {
            "display_anchors": [{"anchor": "短款衣长"}, {"anchor": "前襟五颗扣子"}],
            "candidate_primary_selling_points": [{
                "selling_point": "短款衣长",
                "risk_if_missed": "如果不展示衣长，容易被误生成普通长外套",
            }],
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        bundle = build_content_bundle_brief(anchor, card.to_dict(), product_type="短款外套")
        self.assertEqual(bundle["content_mode"], "FACTUAL_OBSERVATION")
        self.assertEqual(bundle["audience_tension"]["status"], "UNAVAILABLE")
        self.assertEqual(bundle["selling_argument"]["status"], "UNAVAILABLE")
        self.assertEqual(bundle["selling_argument"]["argument_id"], "")

    def test_central_selling_argument_without_direct_proof_remains_authoritative(self):
        anchor = {
            "display_anchors": [{"anchor": "短款衣长"}, {"anchor": "前襟五颗扣子"}],
        }
        card = compile_execution_card(
            observed_row(), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="短款外套",
            selling_point_catalog=[{
                "value_id": "CENTRAL_PCL_BENEFIT",
                "primary_selling_point": "适合作为降温环境的外搭",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "source_claim_ids": ["PCL_BENEFIT"],
                "claim_type": "benefit",
                "allowed_strength": "soft_only",
                "compatible_carriers": ["WEARER_ACTIVE", "WEARER_PASSIVE", "MIXED", "STATIC_PRODUCT", "HAND_ONLY"],
            }],
        )
        self.assertEqual(bundle["content_mainline"], "适合作为降温环境的外搭")
        self.assertEqual(bundle["selling_argument"]["argument_id"], "CENTRAL_PCL_BENEFIT")
        self.assertEqual(bundle["selling_argument"]["status"], "AVAILABLE")
        self.assertEqual(bundle["selling_argument"]["script_readiness"], "READY")
        self.assertEqual(bundle["selling_argument"]["proof_match_status"], "UNMATCHED")
        self.assertEqual(bundle["argument_readiness"], "READY")
        self.assertEqual(bundle["proof_match_status"], "UNMATCHED")
        self.assertEqual(bundle["content_mode"], "SELLING_ARGUMENT")
        self.assertTrue(bundle["original_15s_eligible"])
        self.assertEqual(bundle["selling_argument"]["core_proof_claim_keys"], [])
        self.assertEqual(bundle["recommended_flow"], "ORIGINAL_15S")

    def test_argument_selects_one_semantic_core_proof_and_keeps_details_visual(self):
        anchor = {
            "display_anchors": [
                {"anchor": "短款衣长落在腰线附近"},
                {"anchor": "立领和金属拉链细节"},
                {"anchor": "两侧大口袋"},
            ],
        }
        card = compile_execution_card(
            observed_row(content_carrier="WEARER_ACTIVE"), cluster_run_id="prompt_only_full", cluster_id=5, cluster_version="v1"
        )
        assert card is not None
        bundle = build_content_bundle_brief(
            anchor,
            card.to_dict(),
            product_type="短款外套",
            selling_point_catalog=[{
                "value_id": "CENTRAL_WAIST",
                "primary_selling_point": "腰线视觉更清晰",
                "argument_kind": "SELLING_ARGUMENT",
                "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                "claim_type": "visual_result",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            }],
        )
        core = bundle["selling_argument"]["core_proof_claim_keys"]
        optional = bundle["selling_argument"]["optional_visual_claim_keys"]
        self.assertEqual(1, len(core))
        self.assertGreaterEqual(len(optional), 1)
        core_atom = next(item for item in bundle["claim_atoms"] if item["claim_key"] == core[0])
        self.assertEqual("fit_proportion", core_atom["semantic_group"])
        self.assertEqual("SPOKEN_CORE", core_atom["proof_scope"])
        self.assertTrue(all(
            item["proof_scope"] == "VISUAL_ONLY"
            for item in bundle["claim_atoms"] if item["claim_key"] in optional
        ))
        self.assertNotEqual("DETAIL_SURPRISE", bundle["primary_hook_id"])
        self.assertNotIn("AUDIENCE_NEED_CALLOUT", bundle["eligible_hook_ids"])
        self.assertEqual(bundle["argument_readiness"], "READY")
        self.assertEqual(bundle["proof_match_status"], "MATCHED")
        self.assertTrue(bundle["original_15s_eligible"])
        self.assertEqual(bundle["recommended_flow"], "ORIGINAL_15S")

    def test_visual_validation_requires_every_bundle_claim_to_have_support(self):
        plan = {
            "shot_plan": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "HAND_ONLY",
                    "continuity_group": "A",
                    "opening_mechanism": "PRODUCT_REVEAL",
                },
                {
                    "structure_beat": "PROOF",
                    "carrier_mode": "HAND_ONLY",
                    "continuity_group": "A",
                    "opening_mechanism": "",
                },
            ]
        }
        bundle = {
            "content_bundle_id": "CBR_1",
            "claim_atoms": [
                {"claim_key": "CLM_FIT", "fact_text": "短款高腰比例"},
                {"claim_key": "CLM_DETAIL", "fact_text": "双口袋细节"},
            ],
        }
        base_shot = {
            "shot_content": "手把外套前襟移入近景",
            "observable_action": "手停在衣摆旁",
            "product_visibility": "PARTIAL",
            "framing": "固定近景",
            "carrier_mode": "HAND_ONLY",
            "continuity_group": "A",
            "reference_spine_orders": [1],
        }
        payload = {
            "execution_card_id": "EXEC_1",
            "content_bundle_id": "CBR_1",
            "shots": [
                {
                    **base_shot,
                    "structure_beat": "HOOK",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "supported_claim_keys": ["CLM_FIT"],
                },
                {
                    **base_shot,
                    "structure_beat": "PROOF",
                    "opening_mechanism": "",
                    "supported_claim_keys": [],
                },
            ],
        }
        reference = {
            "execution_card_id": "EXEC_1",
            "shot_execution_spine": [{"order": 1, "observable_action": "手拿起商品"}],
        }
        missing = validate_visual_adaptation(
            payload,
            execution_plan=plan,
            execution_reference=reference,
            content_bundle_brief=bundle,
        )
        self.assertFalse(missing["valid"])
        self.assertTrue(any("CLM_DETAIL" in issue for issue in missing["issues"]))
        payload["shots"][1]["supported_claim_keys"] = ["CLM_DETAIL"]
        complete = validate_visual_adaptation(
            payload,
            execution_plan=plan,
            execution_reference=reference,
            content_bundle_brief=bundle,
        )
        self.assertTrue(complete["valid"], complete["issues"])

    def test_visual_validation_blocks_abstract_and_hand_only_person(self):
        plan = {
            "shot_plan": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "HAND_ONLY",
                    "continuity_group": "A",
                    "opening_mechanism": "PRODUCT_REVEAL",
                }
            ]
        }
        payload = {
            "execution_card_id": "EXEC_1",
            "shots": [
                {
                    "shot_content": "达人低头轻判断",
                    "observable_action": "人物转身",
                    "product_visibility": "PARTIAL",
                    "framing": "近景",
                    "structure_beat": "HOOK",
                    "carrier_mode": "HAND_ONLY",
                    "continuity_group": "A",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "reference_spine_orders": [1],
                }
            ]
        }
        review = validate_visual_adaptation(
            payload,
            execution_plan=plan,
            execution_reference={
                "execution_card_id": "EXEC_1",
                "shot_execution_spine": [{"order": 1, "observable_action": "手拿起商品"}],
            },
        )
        self.assertFalse(review["valid"])
        self.assertTrue(any("HAND_ONLY" in issue for issue in review["issues"]))
        self.assertTrue(any("轻判断" in issue for issue in review["issues"]))

    def test_visual_validation_uses_language_independent_spine_lineage(self):
        plan = {
            "shot_plan": [
                {
                    "structure_beat": "HOOK",
                    "carrier_mode": "WEARER_ACTIVE",
                    "continuity_group": "C1",
                    "opening_mechanism": "PRODUCT_REVEAL",
                },
                {
                    "structure_beat": "PROOF",
                    "carrier_mode": "WEARER_ACTIVE",
                    "continuity_group": "C2",
                    "opening_mechanism": "",
                },
            ]
        }
        payload = {
            "execution_card_id": "EXEC_1",
            "shots": [
                {
                    "shot_content": "袖口先进入画面",
                    "observable_action": "手把袖口移到近景",
                    "product_visibility": "PARTIAL",
                    "framing": "固定近景",
                    "structure_beat": "HOOK",
                    "carrier_mode": "WEARER_ACTIVE",
                    "continuity_group": "C1",
                    "opening_mechanism": "PRODUCT_REVEAL",
                    "reference_spine_orders": [1],
                },
                {
                    "shot_content": "人物抬起手臂，袖部结构保持可见",
                    "observable_action": "手臂抬起后停住",
                    "product_visibility": "FULL",
                    "framing": "中近景",
                    "structure_beat": "PROOF",
                    "carrier_mode": "WEARER_ACTIVE",
                    "continuity_group": "C2",
                    "opening_mechanism": "",
                    "reference_spine_orders": [2],
                },
            ],
        }
        review = validate_visual_adaptation(
            payload,
            execution_plan=plan,
            execution_reference={
                "execution_card_id": "EXEC_1",
                "shot_execution_spine": [
                    {"order": 1, "observable_action": "Sleeve enters frame"},
                    {"order": 2, "observable_action": "Model raises arm"},
                ],
            },
        )
        self.assertTrue(review["valid"], review["issues"])

    def test_voiceover_does_not_require_silent_shot(self):
        result = validate_voiceover_plan(
            {
                "lines": [
                    {"shot_no": 1, "voiceover_text_target_language": "x", "voiceover_text_zh": "甲"},
                    {"shot_no": 2, "voiceover_text_target_language": "y", "voiceover_text_zh": "乙"},
                ],
                "silent_shots": [],
            },
            2,
        )
        self.assertTrue(result["valid"], result["issues"])

    def test_voiceover_accepts_tail_silence_inside_spoken_shot(self):
        lines = [
            {"start_ms": 0, "end_ms": 2500},
            {"start_ms": 2500, "end_ms": 12000},
            {"start_ms": 12000, "end_ms": 14200},
        ]
        windows = _silent_windows(lines, total_duration_ms=15000)
        self.assertEqual(
            [{"start_ms": 14200, "end_ms": 15000, "duration_ms": 800}],
            windows,
        )
        result = validate_voiceover_plan(
            {
                "lines": [
                    {
                        "shot_no": 1,
                        "end_shot_no": 1,
                        "voiceover_text_target_language": "x",
                        "voiceover_text_zh": "甲",
                    },
                    {
                        "shot_no": 2,
                        "end_shot_no": 5,
                        "voiceover_text_target_language": "y",
                        "voiceover_text_zh": "乙",
                    },
                    {
                        "shot_no": 5,
                        "end_shot_no": 6,
                        "voiceover_text_target_language": "z",
                        "voiceover_text_zh": "丙",
                    },
                ],
                "silent_shots": [],
                "silent_windows": windows,
                "minimum_silence_window_ms": 450,
            },
            6,
        )
        self.assertTrue(result["valid"], result["issues"])

    def test_authenticity_accepts_time_window_when_every_shot_has_voiceover(self):
        result = authenticity_review(
            {
                "storyboard": [
                    {"audio_actual": "VOICEOVER", "shot_content": "前襟近景"},
                    {"audio_actual": "VOICEOVER_CONTINUATION", "shot_content": "袖口近景"},
                ],
                "audio_plan": {
                    "silent_windows": [
                        {"start_ms": 14200, "end_ms": 15000, "duration_ms": 800}
                    ],
                    "minimum_silence_window_ms": 450,
                },
            }
        )
        self.assertEqual("PASS", result["result"])
        self.assertEqual(1, result["silent_window_count"])
        self.assertEqual(800, result["max_silent_window_ms"])

    def test_voiceover_first_line_must_be_visually_grounded(self):
        generic = validate_voiceover_visual_grounding(
            {
                "lines": [
                    {
                        "shot_no": 1,
                        "voiceover_text_zh": "姐妹们先看这件，真的很好搭。",
                    }
                ]
            },
            primary_observation="短款高腰上身比例",
            first_shot_content="短款衣摆停在高腰线附近",
        )
        self.assertFalse(generic["valid"])
        grounded = validate_voiceover_visual_grounding(
            {
                "lines": [
                    {
                        "shot_no": 1,
                        "voiceover_text_zh": "这件短款的衣摆刚好停在高腰线。",
                    }
                ]
            },
            primary_observation="短款高腰上身比例",
            first_shot_content="短款衣摆停在高腰线附近",
        )
        self.assertTrue(grounded["valid"], grounded["issues"])

    def test_voiceover_first_line_rejects_punctuated_generic_visual_hook(self):
        result = validate_voiceover_visual_grounding(
            {
                "lines": [
                    {
                        "shot_no": 1,
                        "voiceover_text_zh": "姐妹们，先看镜头效果，版型真的很清楚。",
                    }
                ]
            },
            primary_observation="短款高腰上身比例",
            first_shot_content="短款衣摆停在高腰线附近",
        )
        self.assertFalse(result["valid"])
        self.assertGreaterEqual(len(result["issues"]), 2)

    def test_assembly_keeps_editorial_metadata_out_of_visible_fields(self):
        direction = {
            "direction_assignment_id": "SRA_1",
            "selection_run_id": "SR_1",
            "cluster_id": 5,
            "cluster_version": "v1",
            "p2_lite": {"primary_observation": "前襟按扣"},
            "content_bundle_brief": {
                "content_bundle_id": "CBR_1",
                "content_mainline": "前襟按扣和口袋细节",
                "claim_atoms": [
                    {"claim_key": "CLM_1", "fact_text": "前襟按扣", "role": "core_result"},
                    {"claim_key": "CLM_2", "fact_text": "双口袋", "role": "visual_proof"},
                ],
                "max_claim_atoms": 3,
                "max_themes": 2,
            },
            "execution_reference": {
                "execution_card_id": "EXEC_1",
                "execution_card_schema_version": "v1",
                "source_profile_id": "SP_1",
                "unknown_fields": ["location"],
            },
            "structure_execution_plan": {
                "macro_family_key": "HOOK>PROOF",
                "shot_plan": [
                    {
                        "time_range": "0-3s",
                        "structure_beat": "HOOK",
                        "carrier_mode": "HAND_ONLY",
                        "continuity_group": "A",
                        "opening_mechanism": "PRODUCT_REVEAL",
                        "spoken_task_hint": "hook",
                        "visual_task": "商品进入画面",
                    }
                ],
            },
        }
        visual = {
            "shots": [
                {
                    "shot_content": "手把外套前襟移入近景",
                    "observable_action": "手停在按扣旁",
                    "editorial_purpose": "后台证明说明",
                    "product_visibility": "PARTIAL",
                    "framing": "近景",
                }
            ]
        }
        script = assemble_reality_script(
            direction=direction,
            visual_plan=visual,
            voiceover_plan={"lines": [], "silent_shots": [1]},
        )
        self.assertNotIn("editorial_purpose", script["storyboard"][0])
        self.assertEqual("近景", script["storyboard"][0]["framing"])
        self.assertEqual("手停在按扣旁", script["storyboard"][0]["person_action"])
        self.assertIn("gaze_and_reaction", script["storyboard"][0]["performance"])
        self.assertEqual(
            "VIDEO_MODEL_PRIMARY_INPUT",
            script["video_generation_brief"]["usage"],
        )
        self.assertEqual(
            "production-video-brief-v11-semantic-context",
            script["video_generation_brief"]["schema_version"],
        )
        self.assertEqual(
            "STAGE0_SHARED_VISIBLE_CLIP_COMPILER",
            script["video_generation_brief"]["source"],
        )
        self.assertIn("capture_rhythm_contract", script["video_generation_brief"])
        self.assertIn("capture_units", script["video_generation_brief"])
        self.assertIn(
            "final_information_gain_review", script["video_generation_brief"]
        )
        self.assertIn("product_identity_lock", script["video_generation_brief"])
        self.assertNotIn("internal_structure_note", script["video_generation_brief"])
        self.assertFalse(script["execution_constraints"]["single_proof_rule"])
        self.assertEqual(script["reality_reference_provenance"]["content_bundle_id"], "CBR_1")
        self.assertEqual(script["authenticity_review"]["result"], "PASS")
        self.assertEqual("HANDS_ONLY", script["production_design"]["presentation_mode"])
        self.assertFalse(script["production_design"]["character_setting"]["on_camera"])

    def test_assembly_preserves_voiceover_segment_across_shots(self):
        direction = {
            "direction_assignment_id": "SRA_SPAN",
            "p2_lite": {"primary_observation": "前襟扣位"},
            "creative_blueprint": {
                "persona": {
                    "identity": "曼谷通勤女性",
                    "appearance": "自然肤质",
                    "hair_makeup": "低马尾与淡妆",
                    "styling": "纯色内搭与高腰直筒裤",
                    "speaking_personality": "像朋友边看边说",
                    "performance_intensity": "克制自然",
                },
                "scene": {
                    "location": "办公室衣帽区",
                    "moment": "下班前",
                    "lighting": "侧面自然窗光",
                    "background": "浅色墙面与衣架",
                    "camera_setup": "手机胸口高度固定机位",
                },
                "performance_flow": {
                    "entry_state": "已经穿好外套",
                    "behavior_motivation": "出门前核对前襟",
                    "reaction_points": ["看清扣位后抬眼一次"],
                    "ending_state": "手离开前襟自然站定",
                },
            },
            "content_bundle_brief": {
                "content_bundle_id": "CBR_SPAN",
                "content_mainline": "前襟与衣长",
                "claim_atoms": [
                    {"claim_key": "C1", "fact_text": "前襟扣位"},
                    {"claim_key": "C2", "fact_text": "短款衣长"},
                ],
            },
            "execution_reference": {"execution_card_id": "EXEC_SPAN"},
            "structure_execution_plan": {
                "shot_plan": [
                    {
                        "time_range": f"{index * 3}-{(index + 1) * 3}s",
                        "structure_beat": "HOOK" if index == 0 else "PROOF",
                        "carrier_mode": "WEARER_ACTIVE",
                        "continuity_group": f"C{index + 1}",
                        "opening_mechanism": "PRODUCT_REVEAL" if index == 0 else "",
                    }
                    for index in range(3)
                ]
            },
        }
        visual = {
            "shots": [
                {
                    "shot_content": f"画面{index}",
                    "observable_action": f"动作{index}",
                    "framing": "近景",
                    "product_visibility": "FULL",
                    "supported_claim_keys": ["C1"] if index == 1 else ["C2"],
                }
                for index in range(1, 4)
            ]
        }
        script = assemble_reality_script(
            direction=direction,
            visual_plan=visual,
            voiceover_plan={
                "lines": [
                    {
                        "shot_no": 1,
                        "end_shot_no": 2,
                        "voiceover_text_target_language": "ข้อความต่อเนื่อง",
                        "voiceover_text_zh": "跨镜连续口播",
                    }
                ],
                "silent_shots": [3],
            },
        )
        self.assertEqual(
            ["VOICEOVER", "VOICEOVER_CONTINUATION", "SILENT"],
            [item["audio_actual"] for item in script["storyboard"]],
        )
        self.assertEqual(2, script["authenticity_review"]["spoken_shot_count"])
        self.assertEqual(1, script["authenticity_review"]["silent_shot_count"])
        self.assertEqual("PERSON_ON_CAMERA", script["production_design"]["presentation_mode"])
        self.assertEqual(
            "曼谷通勤女性",
            script["production_design"]["character_setting"]["identity"],
        )
        self.assertEqual(
            "纯色内搭与高腰直筒裤",
            script["production_design"]["outfit_setting"]["styling"],
        )
        self.assertEqual(
            "办公室衣帽区",
            script["production_design"]["scene_setting"]["location"],
        )
        execution_plan = script["voiceover_execution_plan"]
        self.assertEqual(execution_plan["schema_version"], "voiceover-execution-plan-v1")
        self.assertEqual(execution_plan["mode"], "GENERATE_FROM_SOURCE_CONTRACT")
        self.assertEqual(
            execution_plan["generation_policy"],
            "central_engine_must_generate_fresh_copy",
        )
        self.assertTrue(execution_plan["source_copy_audit_present"])
        self.assertEqual(execution_plan["target_text"], "ข้อความต่อเนื่อง")
        self.assertEqual(execution_plan["lines"][0]["end_shot_no"], 2)

    def test_stage0_six_slots_compile_to_four_visible_clips_with_two_setups(self):
        beats = ["HOOK", "HOOK", "PROOF", "PROOF", "ENDING", "ENDING"]
        direction = {
            "direction_assignment_id": "SRA_V5",
            "p2_lite": {"primary_observation": "短款衣长"},
            "content_bundle_brief": {
                "content_bundle_id": "CBR_V5",
                "content_mainline": "短款比例",
                "claim_atoms": [
                    {"claim_key": "C1", "fact_text": "短款衣长", "role": "core_result"}
                ],
            },
            "execution_reference": {
                "execution_card_id": "EXEC_V5",
                "shot_count": 6,
                "available_parts": ["HOOK", "PROOF", "ENDING"],
            },
            "product_truth": {
                "product_identity": "短款外套",
                "identity_anchors": ["单排前襟"],
                "visible_detail_anchors": ["短款衣长"],
                "canonical_product_type": "outerwear",
            },
            "structure_execution_plan": {
                "beat_sequence": ["HOOK", "PROOF", "ENDING"],
                "shot_plan": [
                    {
                        "time_range": f"{index * 2.5:.1f}-{(index + 1) * 2.5:.1f}s",
                        "structure_beat": beat,
                        "carrier_mode": "WEARER_ACTIVE",
                        "continuity_group": f"C{index + 1}",
                        "opening_mechanism": "PRODUCT_REVEAL" if index == 0 else "",
                    }
                    for index, beat in enumerate(beats)
                ],
            },
        }
        visual = {
            "shots": [
                {
                    "shot_content": f"第{index}段不同可见关系",
                    "observable_action": f"完成动作{index}",
                    "framing": "普通手机中景" if index % 2 else "普通手机近景",
                    "anchor_reference": "短款衣长",
                    "supported_claim_keys": ["C1"],
                }
                for index in range(1, 7)
            ]
        }
        script = assemble_reality_script(
            direction=direction,
            visual_plan=visual,
            voiceover_plan={"lines": [], "silent_shots": list(range(1, 7))},
        )
        self.assertEqual(4, len(script["capture_units"]))
        self.assertEqual(2, script["capture_rhythm_contract"]["camera_setup_count"])
        self.assertEqual(
            "production-video-brief-v11-semantic-context",
            script["video_generation_brief"]["schema_version"],
        )
        self.assertTrue(
            script["video_generation_brief"]["product_identity_lock"][
                "reference_image_is_authority"
            ]
        )
        self.assertIn("数字裁切", script["video_generation_brief"]["instruction"])

    def test_direct_share_projection_preserves_four_real_clips_and_mixed_carrier(self):
        carriers = [
            "STATIC_PRODUCT",
            "STATIC_PRODUCT",
            "WEARER_ACTIVE",
            "WEARER_ACTIVE",
            "HAND_ONLY",
            "HAND_ONLY",
        ]
        direction = {
            "direction_assignment_id": "SRA_DIRECT",
            "selection_run_id": "SR_DIRECT",
            "cluster_id": 5,
            "cluster_version": "v1",
            "p2_lite": {"primary_observation": "短款比例"},
            "creator_recording_profile": {
                "enabled": True,
                "recording_mode": "CREATOR_DIRECT_SHARE",
                "capture_preset": "PRODUCT_FIRST_THEN_WORN",
            },
            "creative_diversity_contract": {
                "contract_id": "CDV_DIRECT",
                "required_carrier": "MIXED",
                "required_presentation_mode": "MIXED",
            },
            "content_bundle_brief": {
                "content_bundle_id": "CBR_DIRECT",
                "content_mainline": "短款比例",
                "claim_atoms": [
                    {"claim_key": "C1", "fact_text": "衣长落在腰线附近", "role": "core_result"}
                ],
            },
            "product_truth": {
                "product_identity": "短款外套",
                "identity_anchors": ["单排前襟"],
                "visible_detail_anchors": ["衣长落在腰线附近"],
            },
            "execution_reference": {
                "execution_card_id": "EXEC_DIRECT",
                "shot_execution_spine": [
                    {"order": index, "observable_action": f"来源动作{index}"}
                    for index in range(1, 5)
                ],
            },
            "structure_execution_plan": {
                "beat_sequence": ["HOOK", "PROOF", "ENDING"],
                "shot_plan": [
                    {
                        "time_range": f"{index * 2.5:.1f}-{(index + 1) * 2.5:.1f}s",
                        "structure_beat": "HOOK" if index < 2 else "PROOF" if index < 5 else "ENDING",
                        "carrier_mode": carrier,
                        "continuity_group": f"C{index + 1}",
                        "opening_mechanism": "PRODUCT_REVEAL" if index == 0 else "",
                    }
                    for index, carrier in enumerate(carriers)
                ],
            },
            "creative_blueprint": {
                "creative_blueprint_id": "CBP_DIRECT",
                "creative_thesis": "直接分享短款比例",
                "creator_motivation": "想把刚穿上的比例分享给观众",
                "recording_context": {
                    "recording_mode": "CREATOR_DIRECT_SHARE",
                    "recording_motivation": "出门前直接分享刚穿上的比例",
                    "camera_relationship": "商品先出镜后切自拍",
                    "viewer_awareness": "知道正在面对观众拍摄",
                },
                "persona": {},
                "scene": {"location": "公寓窗边", "moment": "出门前", "lighting": "自然窗光"},
                "performance_flow": {},
                "event_design": {"core_result_moment": "看清短款比例", "natural_event": "主动分享"},
                "retention_hook": {},
                "macro_visual_passages": [
                    {"narrative_role": "EVENT_ENTRY", "visible_process": "商品先进入画面", "observable_action": "拿近商品"},
                    {"narrative_role": "EVENT_PROOF", "visible_process": "穿着结果可见", "observable_action": "直接说话"},
                    {"narrative_role": "EVENT_END", "visible_process": "回到中近景", "observable_action": "说完分享"},
                ],
                "clip_design": [
                    {"clip_no": 1, "clip_job": "商品识别", "recording_relation": "手持商品近景", "framing": "普通手机商品近景", "visible_process": "人物拿着外套让前襟进入画面", "observable_action": "人物拿近外套", "supported_claim_keys": []},
                    {"clip_no": 2, "clip_job": "穿着结果", "recording_relation": "固定手机正面", "framing": "普通手机中景", "visible_process": "人物穿好外套面对手机", "observable_action": "人物直接分享", "supported_claim_keys": ["C1"]},
                    {"clip_no": 3, "clip_job": "比例证明", "recording_relation": "镜面穿搭关系", "framing": "镜面半身中景", "visible_process": "人物与腰线关系同时可见", "observable_action": "人物保持自然站姿", "supported_claim_keys": ["C1"]},
                    {"clip_no": 4, "clip_job": "商品回看", "recording_relation": "手持局部近景", "framing": "普通手机局部近景", "visible_process": "人物把衣摆拿近手机", "observable_action": "人物轻拿衣摆", "supported_claim_keys": ["C1"]},
                ],
            },
        }

        visual = project_event_blueprint_to_visual_plan(direction=direction)
        self.assertEqual(
            ["CU_01", "CU_02", "CU_02", "CU_03", "CU_03", "CU_04"],
            [shot["capture_unit_id"] for shot in visual["shots"]],
        )
        visible_text = " ".join(shot["shot_content"] for shot in visual["shots"])
        self.assertNotIn("按已确认锚点呈现", visible_text)
        self.assertNotIn("维持局部承载观察关系", visible_text)

        script = assemble_reality_script(
            direction=direction,
            visual_plan=visual,
            voiceover_plan={"lines": [], "silent_shots": list(range(1, 7))},
        )
        self.assertEqual("MIXED", script["production_design"]["presentation_mode"])
        self.assertEqual("PASS", script["video_generation_brief"]["carrier_integrity"]["result"])
        self.assertEqual(4, len(script["capture_units"]))
        self.assertTrue(script["video_generation_brief"]["creator_recording_profile"]["enabled"])
        self.assertIn("同一创作者", script["video_generation_brief"]["instruction"])

    def test_authenticity_blocks_complete_legacy_action_chain(self):
        result = authenticity_review(
            {
                "storyboard": [
                    {
                        "shot_content": "人物在全身镜前半步后退，低头看腰线，再转身回到正面",
                        "observable_action": "",
                        "person_action": "",
                        "voiceover_text_zh": "",
                    }
                ]
            }
        )
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(len(result["legacy_action_chain_hits"]), 4)


if __name__ == "__main__":
    unittest.main()
