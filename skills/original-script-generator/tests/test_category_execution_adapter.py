import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.category_execution import (
    ACCESSORY_PROFILE_ENV,
    build_category_blueprint_guidance,
    build_category_video_brief,
    compile_category_execution_extension,
    project_category_capture_rhythm_contract,
    reconcile_anchor_category_contract,
    resolve_category_argument_execution,
    resolve_category_carrier_execution,
    validate_category_execution_identity,
)
from core.category_execution.accessory import (
    SCARF_MULTICLIP_ENV,
    SCARF_MULTICLIP_PROFILE,
    SMALL_ACCESSORY_MOTION_ENV,
    SMALL_ACCESSORY_MULTICLIP_ENV,
    SMALL_ACCESSORY_MULTICLIP_PROFILE,
    _no_face_deep_clean,
    _no_face_safe_text,
)
from core.accessory_mixed_templates import (
    ACCESSORY_MIXED_TEMPLATE_ENV,
    ACCESSORY_MIXED_TEMPLATE_PROFILE,
)
from core.original_batch_allocator import _make_item
from core.production_script_renderer import render_video_generation_prompt
from core.simplified_complete_script import (
    assemble_simplified_complete_script,
    build_simplified_creative_seed,
    build_simplified_script_prompt,
    build_simplified_voiceover_inputs,
    compile_capture_units,
    normalize_simplified_visual_script,
    validate_simplified_complete_script,
    validate_simplified_visual_script,
)


def _contract(carrier="WEARER_ACTIVE"):
    return {
        "direction_identity": {"macro_family_key": "HOOK>PROOF>ENDING"},
        "hard_constraints": {
            "content_carrier": carrier,
            "beat_sequence": ["HOOK", "PROOF", "ENDING"],
        },
    }


def _bundle(fact="耳饰已经佩戴后的耳侧效果", key="C1"):
    return {
        "content_mainline": fact,
        "claim_atoms": [
            {"claim_key": key, "fact_text": fact, "role": "core_result"}
        ],
    }


def _anchor(product_name="金色水滴形耳饰"):
    return {
        "product_positioning_one_liner": product_name,
        "hard_anchors": [{"anchor": product_name}],
        "display_anchors": [{"anchor": "水滴吊坠结构"}],
        "category_execution_contract": {"display_family": "jewelry"},
    }


class CategoryExecutionAdapterTest(unittest.TestCase):
    def test_feature_gate_defaults_to_disabled(self):
        with patch.dict(os.environ, {ACCESSORY_PROFILE_ENV: "0"}, clear=False):
            extension = compile_category_execution_extension(
                product_type="耳环", top_category="配饰", anchor_card=_anchor()
            )
        self.assertEqual(extension, {})

    def test_apparel_is_omitted_even_when_accessory_feature_is_enabled(self):
        extension = compile_category_execution_extension(
            product_type="外套",
            top_category="女装",
            anchor_card={"category_execution_contract": {"display_family": "apparel"}},
            enabled=True,
        )
        self.assertEqual(extension, {})

    def test_three_supported_categories_compile_distinct_physical_profiles(self):
        cases = (
            ("耳环", "EAR", "ALREADY_WORN_EAR_VISIBLE"),
            ("发夹", "HAIR", "ALREADY_STYLED_HAIR_RESULT"),
            ("围巾", "NECK_SHOULDER", "ALREADY_WORN_UPPER_BODY_RESULT"),
        )
        for product_type, zone, result_view in cases:
            with self.subTest(product_type=product_type):
                extension = compile_category_execution_extension(
                    product_type=product_type,
                    top_category="配饰",
                    anchor_card=_anchor(product_type),
                    enabled=True,
                )
                self.assertEqual(extension["domain"], "ACCESSORY")
                self.assertEqual(extension["profile"]["wearing_zone"], zone)
                self.assertEqual(
                    extension["profile"]["required_result_view"], result_view
                )
                self.assertTrue(extension["profile"]["interaction_boundary"])

    def test_wrist_accessory_compiles_worn_result_and_wrist_outfit_role(self):
        extension = compile_category_execution_extension(
            product_type="手镯",
            top_category="配饰",
            anchor_card=_anchor("金色细手镯"),
            enabled=True,
        )
        profile = extension["profile"]
        self.assertEqual("WRIST_FOREARM", profile["wearing_zone"])
        self.assertEqual("ALREADY_WORN_WRIST_RESULT", profile["required_result_view"])
        self.assertEqual(
            "SUPPORTING_OUTFIT_WRIST",
            profile["outfit_context"]["target_role"],
        )
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="PERSON_ON_CAMERA"
        )
        self.assertEqual("WRIST_WORN", carrier["primary_demonstration_mode"])
        self.assertTrue(
            all("领口" not in item for item in carrier["optional_simple_interactions"])
        )
        self.assertIn("手腕", carrier["product_relation_zh"])
        prominence = carrier["product_prominence_contract"]
        self.assertEqual("WRIST_FOREARM_CLOSE", prominence["primary_framing"])
        self.assertFalse(prominence["hard_required"])
        self.assertFalse(prominence["may_trigger_retry"])

    def test_ring_compiles_finger_profile_and_matching_claim_actions(self):
        extension = compile_category_execution_extension(
            product_type="戒指",
            top_category="配饰",
            anchor_card=_anchor("银色开口戒指"),
            enabled=True,
        )
        self.assertEqual("FINGER_HAND", extension["profile"]["wearing_zone"])
        self.assertEqual(
            "ALREADY_WORN_FINGER_RESULT",
            extension["profile"]["required_result_view"],
        )
        resolved = resolve_category_argument_execution(
            extension,
            selling_argument={
                "proof_action_intent": "SIZE_ADJUSTMENT",
                "preferred_action_mode": "ADJUST_THEN_WEAR",
                "required_proof_relation": "调节一次后保留佩戴结果",
            },
        )
        carrier = resolve_category_carrier_execution(
            resolved, presentation_mode="HANDS_ONLY"
        )
        self.assertEqual("FINGER_WORN", carrier["primary_demonstration_mode"])
        self.assertEqual("SIZE_ADJUSTMENT", carrier["proof_action_intent"])
        self.assertEqual(
            "RING_ADJUST_THEN_WEAR",
            carrier["interaction_capabilities"][0]["interaction_id"],
        )
        seed = build_simplified_creative_seed(
            anchor_card=_anchor("银色开口戒指"),
            structure_contract=_contract("HAND_ONLY"),
            content_bundle={
                **_bundle("开口戒指可以小幅调节后稳定佩戴", "R1"),
                "selling_argument": {
                    "argument_id": "RING_ADJUST",
                    "proof_subject": "ON_BODY_RESULT",
                    "proof_action_intent": "SIZE_ADJUSTMENT",
                    "preferred_action_mode": "ADJUST_THEN_WEAR",
                    "required_proof_relation": "调节一次后保留佩戴结果",
                },
            },
            creative_contract={},
            execution_reference={"content_carrier": "HAND_ONLY"},
            requested_hook_id="DETAIL_SURPRISE",
            content_angle_key="RING_ADJUSTMENT",
            product_type="戒指",
            top_category="配饰",
            category_execution_extension=resolved,
        )
        self.assertEqual(
            "ADJUST_THEN_WEAR", seed["action_design"]["primary_action_mode"]
        )
        self.assertEqual(
            "MATCHED",
            seed["action_design"]["claim_action_contract"]["compatibility"],
        )

    def test_wrist_put_on_argument_exposes_one_matching_process_action(self):
        extension = compile_category_execution_extension(
            product_type="手镯",
            top_category="配饰",
            anchor_card=_anchor("金色细手镯"),
            enabled=True,
        )
        resolved = resolve_category_argument_execution(
            extension,
            selling_argument={"preferred_action_mode": "SIMPLE_WEAR_PROCESS"},
        )
        carrier = resolve_category_carrier_execution(
            resolved, presentation_mode="HANDS_ONLY"
        )
        self.assertEqual("SIMPLE_WEAR_PROCESS", carrier["preferred_action_mode"])
        self.assertEqual(1, len(carrier["interaction_capabilities"]))
        self.assertEqual(
            "WRIST_SIMPLE_PUT_ON",
            carrier["interaction_capabilities"][0]["interaction_id"],
        )

    def test_hair_accessory_uses_hair_actions_and_rear_capture_relationship(self):
        extension = compile_category_execution_extension(
            product_type="抓夹",
            top_category="发饰",
            anchor_card=_anchor("棕色抓夹"),
            enabled=True,
        )
        profile = extension["profile"]
        self.assertEqual(
            "SUPPORTING_OUTFIT_HAIR",
            profile["outfit_context"]["target_role"],
        )
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="PERSON_ON_CAMERA"
        )
        self.assertEqual("HAIR_WORN", carrier["primary_demonstration_mode"])
        self.assertIn("侧后方", carrier["capture_relationship"])
        self.assertTrue(
            all("垂端" not in item and "领口" not in item
                for item in carrier["optional_simple_interactions"])
        )
        brief = build_category_video_brief(
            extension, carrier_execution=carrier
        )
        self.assertIn("侧后方", brief["capture_relationship"])
        prominence = brief["product_prominence_contract"]
        self.assertEqual("HAIR_REGION_CLOSE", prominence["primary_framing"])
        self.assertIn("头肩范围", prominence["context_guidance"])
        self.assertEqual(
            "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN",
            prominence["sequence_policy"],
        )
        self.assertEqual(
            [
                "PRODUCT_RESULT_CLOSE",
                "NATURAL_MOTION_RELATION",
                "PRODUCT_REACQUISITION",
            ],
            prominence["capture_arc"],
        )
        self.assertIn(
            "发饰",
            prominence["terminal_visibility"]["ending_guidance"],
        )
        self.assertEqual(
            {
                "HAIR_BODY_ARC_REVEAL",
                "HAIR_MIRROR_TO_REAR_CUT",
                "HAIR_WEIGHT_SHIFT_REFRAME",
            },
            {
                item["interaction_id"]
                for item in carrier["interaction_capabilities"]
            },
        )
        self.assertTrue(
            all(
                item["motion_scope"] == "ONE_CONTINUOUS_CHANGE"
                for item in carrier["interaction_capabilities"]
            )
        )
        self.assertTrue(
            all(
                "第一帧可见" in item["start_state"]
                for item in carrier["interaction_capabilities"]
            )
        )
        self.assertTrue(
            all(
                "最后一瞬" in item["end_state"]
                for item in carrier["interaction_capabilities"]
            )
        )
        guidance = build_category_blueprint_guidance(
            extension, carrier_execution=carrier
        )
        self.assertIn("小商品观察尺度", guidance)
        self.assertIn("中远景不能承担发饰证明", guidance)

    def test_small_accessory_motion_projection_can_be_rolled_back(self):
        with patch.dict(
            os.environ,
            {SMALL_ACCESSORY_MOTION_ENV: "0"},
            clear=False,
        ):
            extension = compile_category_execution_extension(
                product_type="抓夹",
                top_category="发饰",
                anchor_card=_anchor("棕色抓夹"),
                enabled=True,
            )
            carrier = resolve_category_carrier_execution(
                extension, presentation_mode="PERSON_ON_CAMERA"
            )

        self.assertEqual(
            "accessory-execution-profile-v4-small-prominence",
            extension["schema_version"],
        )
        self.assertEqual(
            "ONE_PRODUCT_DOMINANT_VIEW_THEN_CONTEXT_VIEW",
            carrier["product_prominence_contract"]["sequence_policy"],
        )
        self.assertEqual(
            {"HAIR_RESULT_REAR_THREE_QUARTER", "HAIR_RESULT_MIRROR_CONFIRM"},
            {
                item["interaction_id"]
                for item in carrier["interaction_capabilities"]
            },
        )

    def test_scarf_subtypes_are_distinct_and_generic_scarf_remains_compatible(self):
        cases = {
            "围巾": ("scarf", "NECK_SHOULDER", "ALREADY_WORN_UPPER_BODY_RESULT"),
            "秋冬围巾": ("winter_scarf", "NECK_SHOULDER", "ALREADY_WORN_UPPER_BODY_RESULT"),
            "丝巾": ("silk_scarf", "NECK_UPPER_BODY", "ALREADY_STYLED_NECK_RESULT"),
            "头巾": ("headscarf", "HEAD_HAIR", "ALREADY_STYLED_HEAD_RESULT"),
        }
        for product_type, expected in cases.items():
            with self.subTest(product_type=product_type):
                extension = compile_category_execution_extension(
                    product_type=product_type,
                    top_category="配饰",
                    anchor_card=_anchor(product_type),
                    enabled=True,
                )
                profile = extension["profile"]
                self.assertEqual(
                    extension["schema_version"],
                    "accessory-execution-profile-v3-scarf",
                )
                self.assertEqual(profile["product_subtype"], expected[0])
                self.assertEqual(profile["wearing_zone"], expected[1])
                self.assertEqual(profile["required_result_view"], expected[2])
                self.assertTrue(profile["preferred_carriers"])
                self.assertTrue(profile["compatible_proof_subjects"])
                self.assertTrue(profile["outfit_context"])
                self.assertTrue(profile["scene_preferences"])
                self.assertNotIn("product_prominence", profile)

    def test_silk_scarf_and_headscarf_boundaries_do_not_infer_material_or_identity(self):
        silk = compile_category_execution_extension(
            product_type="丝巾", top_category="配饰", anchor_card=_anchor("印花方巾"), enabled=True
        )
        head = compile_category_execution_extension(
            product_type="头巾", top_category="配饰", anchor_card=_anchor("几何图案头巾"), enabled=True
        )
        self.assertIn("真丝", silk["profile"]["interaction_boundary"])
        self.assertIn("冰凉", silk["profile"]["interaction_boundary"])
        self.assertIn("宗教身份", head["profile"]["interaction_boundary"])
        self.assertIn("文化身份", head["profile"]["interaction_boundary"])

    def test_registered_scarf_type_reconciles_legacy_winter_contract(self):
        legacy = {
            "product_positioning_one_liner": "印花方巾",
            "category_execution_contract": {
                "display_family": "winter_scarf",
                "product_subtype": "winter_scarf",
                "operation_policy": "process_required",
                "season_context": {
                    "primary_season": "winter",
                    "weather_signal": "cold",
                },
                "co_styling_hint": {
                    "pair_with": ["winter_coat", "basic_turtleneck", "plain_shirt"]
                },
            },
        }
        reconciled = reconcile_anchor_category_contract(
            legacy,
            product_type="丝巾",
            top_category="配饰",
        )
        contract = reconciled["category_execution_contract"]
        self.assertEqual("silk_scarf", contract["product_subtype"])
        self.assertEqual("result_first_process_avoid", contract["operation_policy"])
        self.assertEqual("unknown", contract["season_context"]["primary_season"])
        self.assertEqual(["plain_shirt"], contract["co_styling_hint"]["pair_with"])
        self.assertEqual("winter_scarf", legacy["category_execution_contract"]["product_subtype"])

    def test_reconcile_is_noop_for_apparel(self):
        anchor = {"category_execution_contract": {"display_family": "apparel"}}
        self.assertIs(
            anchor,
            reconcile_anchor_category_contract(
                anchor,
                product_type="外套",
                top_category="女装",
            ),
        )

    def test_mixed_carrier_keeps_scarf_wearer_result(self):
        extension = compile_category_execution_extension(
            product_type="丝巾",
            top_category="配饰",
            anchor_card=_anchor("印花方巾"),
            enabled=True,
        )
        mixed = resolve_category_carrier_execution(
            extension,
            presentation_mode="MIXED",
        )
        self.assertEqual("ALREADY_STYLED_NECK_RESULT", mixed["required_view"])
        self.assertIn("已经佩戴后的关系", mixed["claim_boundary"])
        self.assertTrue(mixed["optional_simple_interactions"])
        self.assertTrue(mixed["interaction_capabilities"])
        self.assertTrue(
            any(
                item["primary_action_mode"] == "SIMPLE_WEAR_PROCESS"
                for item in mixed["interaction_capabilities"]
            )
        )

        hand = resolve_category_carrier_execution(
            extension,
            presentation_mode="HAND_ONLY",
        )
        self.assertIn("自然展开一次商品", hand["optional_simple_interactions"])
        self.assertEqual(
            {"HANDHELD_PRODUCT", "DETAIL_SHOW"},
            {item["primary_action_mode"] for item in hand["interaction_capabilities"]},
        )

        static = resolve_category_carrier_execution(
            extension,
            presentation_mode="STATIC_PRODUCT",
        )
        self.assertEqual([], static["optional_simple_interactions"])
        self.assertEqual([], static["interaction_capabilities"])

    def test_selected_scarf_action_replaces_optional_action_list_in_guidance(self):
        extension = compile_category_execution_extension(
            product_type="丝巾",
            top_category="配饰",
            anchor_card=_anchor("印花方巾"),
            enabled=True,
        )
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="WEARER_ACTIVE"
        )
        carrier["selected_action_design"] = dict(
            carrier["interaction_capabilities"][-1]
        )
        guidance = build_category_blueprint_guidance(
            extension, carrier_execution=carrier
        )
        self.assertIn("本条核心商品互动", guidance)
        self.assertNotIn("最多自然采用其中一个，也可以不用", guidance)
        brief = build_category_video_brief(extension, carrier_execution=carrier)
        self.assertEqual(
            "accessory-video-handoff-v5-state-aware",
            brief["schema_version"],
        )
        self.assertTrue(brief["selected_action_design"])
        self.assertEqual(
            "IN_PROGRESS",
            brief["wear_state_contract"]["initial_state"],
        )
        self.assertIn("完成上述简单动作后", brief["required_visible_result"])
        self.assertEqual(2, brief["hand_anatomy_guard"]["max_visible_hands"])
        self.assertEqual(
            "SINGLE_PERSON", brief["hand_anatomy_guard"]["hand_owner"]
        )

    def test_scarf_argument_freezes_one_demonstration_mode_per_script(self):
        extension = compile_category_execution_extension(
            product_type="丝巾",
            top_category="配饰",
            anchor_card=_anchor("印花方巾"),
            enabled=True,
        )
        resolved = resolve_category_argument_execution(
            extension,
            selling_argument={
                "argument_theme": "HAIR_RESCUE",
                "primary_demonstration_mode": "HAIR_TIE",
                "supported_demonstration_modes": ["HAIR_TIE", "NECK_WORN", "BAG_ACCENT"],
                "evidence_mode": "VISUAL_RESULT_WITH_AUTHORIZED_VOICEOVER",
            },
        )
        profile = resolved["profile"]
        self.assertEqual("HAIR_TIE", profile["primary_demonstration_mode"])
        self.assertEqual("HEAD_HAIR", profile["wearing_zone"])
        self.assertEqual("ALREADY_STYLED_HAIR_RESULT", profile["required_result_view"])
        self.assertEqual("ONE_PRIMARY_MODE_PER_15S", profile["demonstration_policy"])
        self.assertEqual("NECK_UPPER_BODY", extension["profile"]["wearing_zone"])
        carrier = resolve_category_carrier_execution(
            resolved,
            presentation_mode="WEARER_ACTIVE",
        )
        self.assertIn("轻托一次已经系好的马尾或发尾", carrier["optional_simple_interactions"])
        guidance = build_category_blueprint_guidance(
            resolved,
            carrier_execution=carrier,
        )
        self.assertIn("最多自然采用其中一个，也可以不用", guidance)
        self.assertIn("不得扩写成完整佩戴教程", guidance)

    def test_headscarf_process_is_soft_but_full_wrapping_remains_forbidden(self):
        anchor = _anchor("几何图案头巾")
        extension = compile_category_execution_extension(
            product_type="头巾",
            top_category="配饰",
            anchor_card=anchor,
            enabled=True,
        )
        reconciled = reconcile_anchor_category_contract(
            anchor,
            product_type="头巾",
            top_category="配饰",
        )
        self.assertEqual(
            "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            extension["profile"]["process_policy"],
        )
        self.assertEqual(
            "result_first_process_avoid",
            reconciled["category_execution_contract"]["operation_policy"],
        )
        boundary = "；".join(extension["profile"]["interaction_boundary"])
        self.assertIn("完整包裹", boundary)
        self.assertIn("复杂系结", boundary)

    def test_earring_pairing_authority_is_anchor_only(self):
        unknown = compile_category_execution_extension(
            product_type="耳环", top_category="配饰", anchor_card=_anchor(), enabled=True
        )
        pair = compile_category_execution_extension(
            product_type="耳环", top_category="配饰", anchor_card=_anchor("一对金色水滴耳饰"), enabled=True
        )
        single = compile_category_execution_extension(
            product_type="耳环", top_category="配饰", anchor_card=_anchor("单只金色水滴耳饰"), enabled=True
        )
        self.assertEqual(unknown["profile"]["identity_authority"]["pairing_mode"], "UNAVAILABLE")
        self.assertEqual(pair["profile"]["identity_authority"]["pairing_mode"], "PAIR")
        self.assertEqual(single["profile"]["identity_authority"]["pairing_mode"], "SINGLE")
        self.assertTrue(validate_category_execution_identity(unknown, script={"script_concept": {"one_sentence_idea": "看这对耳饰"}}))
        self.assertFalse(validate_category_execution_identity(unknown, script={"script_concept": {"one_sentence_idea": "看这款耳饰"}}))
        self.assertFalse(validate_category_execution_identity(pair, script={"script_concept": {"one_sentence_idea": "看这对耳饰"}}))
        self.assertFalse(validate_category_execution_identity(single, script={"script_concept": {"one_sentence_idea": "看这只耳饰"}}))

    def test_earring_display_suggestion_cannot_authorize_pairing(self):
        anchor = _anchor()
        anchor["display_anchors"] = [{"anchor": "手持近距离展示成对耳饰"}]
        extension = compile_category_execution_extension(
            product_type="耳环", top_category="配饰", anchor_card=anchor, enabled=True
        )
        self.assertEqual(
            extension["profile"]["identity_authority"]["pairing_mode"],
            "UNAVAILABLE",
        )

    def test_carrier_authority_changes_execution_not_the_routed_carrier(self):
        extension = compile_category_execution_extension(
            product_type="发夹",
            top_category="配饰",
            anchor_card=_anchor("发夹"),
            enabled=True,
        )
        person = resolve_category_carrier_execution(
            extension, presentation_mode="PERSON_ON_CAMERA"
        )
        static = resolve_category_carrier_execution(
            extension, presentation_mode="STATIC_PRODUCT"
        )
        self.assertEqual(person["required_view"], "ALREADY_STYLED_HAIR_RESULT")
        self.assertEqual(static["required_view"], "PRODUCT_DETAIL_ONLY")
        self.assertIn("不把静物或手持画面写成佩戴结果证明", static["claim_boundary"])
        self.assertEqual(
            "PRODUCT_DOMINANT_CLOSE",
            static["product_prominence_contract"]["primary_framing"],
        )

    def test_apparel_seed_and_prompt_are_identical_with_feature_off_or_on(self):
        kwargs = dict(
            anchor_card={
                "product_positioning_one_liner": "米白短款外套",
                "hard_anchors": [{"anchor": "米白短款外套"}],
                "display_anchors": [{"anchor": "单列前襟扣"}],
                "category_execution_contract": {"display_family": "apparel"},
            },
            structure_contract=_contract(),
            content_bundle=_bundle("短款衣长", "C1"),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        with patch.dict(os.environ, {ACCESSORY_PROFILE_ENV: "0"}, clear=False):
            baseline_seed = build_simplified_creative_seed(**kwargs)
        with patch.dict(os.environ, {ACCESSORY_PROFILE_ENV: "1"}, clear=False):
            enabled_seed = build_simplified_creative_seed(**kwargs)
        self.assertEqual(baseline_seed, enabled_seed)
        self.assertNotIn("category_execution_extension", enabled_seed)
        self.assertEqual(
            build_simplified_script_prompt(
                baseline_seed,
                target_country="泰国",
                target_language="泰语",
                duration_seconds=15,
            ),
            build_simplified_script_prompt(
                enabled_seed,
                target_country="泰国",
                target_language="泰语",
                duration_seconds=15,
            ),
        )

    def test_category_multiclip_projection_is_isolated_from_apparel(self):
        base = {
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 4,
            "structure_unit_roles": ["HOOK", "PROOF", "PROOF", "ENDING"],
            "shot_richness_contract": {"planned_visible_clips": 4},
        }
        projected = project_category_capture_rhythm_contract(
            {}, carrier_execution={}, capture_contract=base
        )
        self.assertEqual(base, projected)
        self.assertIsNot(base, projected)
        self.assertNotIn("category_rollout_contract", projected)

    def test_scarf_multiclip_projection_adds_physical_roles_only(self):
        extension = compile_category_execution_extension(
            product_type="丝巾",
            top_category="配饰",
            anchor_card=_anchor("深蓝波点丝巾"),
            enabled=True,
        )
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="PERSON_ON_CAMERA"
        )
        base = {
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 4,
            "macro_structure": ["HOOK", "PROOF", "PROOF", "ENDING"],
            "structure_unit_roles": ["HOOK", "PROOF", "PROOF", "ENDING"],
            "shot_richness_contract": {"planned_visible_clips": 4},
        }
        with patch.dict(
            os.environ, {SCARF_MULTICLIP_ENV: "1"}, clear=False
        ):
            projected = project_category_capture_rhythm_contract(
                extension,
                carrier_execution=carrier,
                capture_contract=base,
            )
        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "ENDING"],
            projected["structure_unit_roles"],
        )
        self.assertEqual(SCARF_MULTICLIP_PROFILE, projected["category_projection"])
        self.assertEqual(4, len(projected["category_unit_roles"]))
        self.assertEqual(4, len(projected["framing_guidance_by_unit"]))
        self.assertTrue(
            projected["category_rollout_contract"]["female_apparel_unchanged"]
        )
        self.assertFalse(
            projected["category_rollout_contract"]["may_trigger_retry"]
        )

    def test_small_accessory_multiclip_projection_has_independent_rollback(self):
        extension = compile_category_execution_extension(
            product_type="手镯",
            top_category="配饰",
            anchor_card=_anchor("金色细手镯"),
            enabled=True,
        )
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="PERSON_ON_CAMERA"
        )
        base = {
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 3,
            "macro_structure": ["HOOK", "PROOF", "ENDING"],
            "structure_unit_roles": ["HOOK", "PROOF", "ENDING"],
            "shot_richness_contract": {"planned_visible_clips": 3},
        }
        with patch.dict(
            os.environ, {SMALL_ACCESSORY_MULTICLIP_ENV: "1"}, clear=False
        ):
            enabled = project_category_capture_rhythm_contract(
                extension,
                carrier_execution=carrier,
                capture_contract=base,
            )
        self.assertEqual(
            SMALL_ACCESSORY_MULTICLIP_PROFILE,
            enabled["category_projection"],
        )
        self.assertEqual("PRODUCT_RESULT_CLOSE", enabled["unit_roles"][0])
        self.assertEqual("PRODUCT_VISIBLE_RESULT", enabled["unit_roles"][-1])
        self.assertIn("清楚可辨", enabled["framing_guidance_by_unit"][-1])
        with patch.dict(
            os.environ, {SMALL_ACCESSORY_MULTICLIP_ENV: "0"}, clear=False
        ):
            disabled = project_category_capture_rhythm_contract(
                extension,
                carrier_execution=carrier,
                capture_contract=base,
            )
        self.assertEqual(base, disabled)

    def test_three_clip_scarf_projection_keeps_final_result_framing(self):
        extension = compile_category_execution_extension(
            product_type="丝巾",
            top_category="配饰",
            anchor_card=_anchor("深蓝波点丝巾"),
            enabled=True,
        )
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="PERSON_ON_CAMERA"
        )
        carrier["preferred_action_mode"] = "RESULT_SHOW"
        carrier["selected_action_design"] = {
            "primary_action_mode": "SIMPLE_WEAR_PROCESS"
        }
        base = {
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 3,
            "macro_structure": ["HOOK", "USE_PROCESS", "ENDING"],
            "structure_unit_roles": ["HOOK", "USE_PROCESS", "ENDING"],
        }
        projected = project_category_capture_rhythm_contract(
            extension,
            carrier_execution=carrier,
            capture_contract=base,
        )
        self.assertEqual(
            [
                "WORN_OR_PRODUCT_OPENING",
                "SIMPLE_WEAR_PROCESS_OR_DETAIL",
                "WORN_OR_CONTEXT_RESULT",
            ],
            projected["unit_roles"],
        )
        self.assertIn("佩戴步骤", projected["framing_guidance_by_unit"][1])
        self.assertIn("清楚结果", projected["framing_guidance_by_unit"][-1])

    def test_small_accessory_motion_is_frozen_by_single_category_projection(self):
        extension = compile_category_execution_extension(
            product_type="抓夹",
            top_category="配饰",
            anchor_card=_anchor("棕色抓夹"),
            enabled=True,
        )
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="PERSON_ON_CAMERA"
        )
        base = {
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 4,
            "macro_structure": ["HOOK", "PROOF", "PROOF", "ENDING"],
            "structure_unit_roles": ["HOOK", "PROOF", "PROOF", "ENDING"],
        }
        projected = project_category_capture_rhythm_contract(
            extension,
            carrier_execution=carrier,
            capture_contract=base,
        )
        self.assertEqual(SMALL_ACCESSORY_MULTICLIP_PROFILE, projected["category_projection"])
        self.assertEqual(
            [
                "PRODUCT_RESULT_CLOSE",
                "NATURAL_MOTION_RELATION",
                "PRODUCT_DETAIL_RELATION",
                "PRODUCT_REACQUISITION",
            ],
            projected["unit_roles"],
        )
        self.assertEqual(
            projected["category_unit_roles"], projected["unit_roles"]
        )
        self.assertEqual(1, projected["unit_roles"].count("NATURAL_MOTION_RELATION"))

    def test_scarf_seed_consumes_category_multiclip_without_new_stage(self):
        kwargs = dict(
            anchor_card={
                "product_positioning_one_liner": "深蓝波点丝巾",
                "hard_anchors": [{"anchor": "深蓝波点丝巾"}],
                "display_anchors": [{"anchor": "波点图案"}],
            },
            structure_contract=_contract(),
            content_bundle=_bundle("丝巾佩戴后的穿搭点缀", "C1"),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            content_angle_key="DETAIL_OBSERVATION",
            product_type="丝巾",
            top_category="配饰",
        )
        with patch.dict(
            os.environ,
            {
                ACCESSORY_PROFILE_ENV: "1",
                SCARF_MULTICLIP_ENV: "1",
            },
            clear=False,
        ):
            seed = build_simplified_creative_seed(**kwargs)
        rhythm = seed["capture_rhythm_contract"]
        self.assertEqual(SCARF_MULTICLIP_PROFILE, rhythm["category_projection"])
        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "ENDING"],
            rhythm["structure_unit_roles"],
        )
        self.assertEqual(
            len(rhythm["category_unit_roles"]), rhythm["capture_unit_count"]
        )
        storyboard = [
            {
                "shot_no": index,
                "narrative_role": role,
                "visual_content": f"丝巾画面{index}",
                "character_action": "自然展示",
            }
            for index, role in enumerate(
                rhythm["structure_unit_roles"], start=1
            )
        ]
        compiled, units = compile_capture_units(storyboard, rhythm)
        self.assertEqual(
            rhythm["structure_unit_roles"],
            [item["structure_role"] for item in units],
        )
        self.assertEqual(
            rhythm["category_unit_roles"],
            [item["unit_role"] for item in units],
        )
        self.assertTrue(all(item["edit_before"] in {"START", "DIRECT_CUT"} for item in units))
        self.assertTrue(all(item.get("capture_unit_role") for item in compiled))

    def test_scarf_seed_passes_final_action_design_into_capture_projection(self):
        structure = _contract()
        structure["hard_constraints"]["beat_sequence"] = [
            "HOOK",
            "USE_PROCESS",
            "PROOF",
            "ENDING",
        ]
        structure["direction_identity"]["macro_family_key"] = (
            "HOOK>USE_PROCESS>PROOF>ENDING"
        )
        with patch.dict(
            os.environ,
            {
                ACCESSORY_PROFILE_ENV: "1",
                SCARF_MULTICLIP_ENV: "1",
            },
            clear=False,
        ):
            seed = build_simplified_creative_seed(
                anchor_card={
                    "product_positioning_one_liner": "深蓝波点丝巾",
                    "hard_anchors": [{"anchor": "深蓝波点丝巾"}],
                    "display_anchors": [{"anchor": "波点图案"}],
                },
                structure_contract=structure,
                content_bundle=_bundle("丝巾佩戴后的穿搭点缀", "C1"),
                creative_contract={},
                execution_reference={"content_carrier": "WEARER_ACTIVE"},
                requested_hook_id="GENERAL_PRODUCT_SHARE",
                content_angle_key="DETAIL_OBSERVATION",
                product_type="丝巾",
                top_category="配饰",
            )
        self.assertEqual(
            seed["action_design"],
            seed["carrier_specific_execution"]["selected_action_design"],
        )
        self.assertEqual(
            "SIMPLE_WEAR_PROCESS",
            seed["action_design"]["primary_action_mode"],
        )
        self.assertIn(
            "SIMPLE_WEAR_PROCESS_OR_DETAIL",
            seed["capture_rhythm_contract"]["unit_roles"],
        )

    def test_accessory_extension_flows_from_plan_to_video_brief(self):
        # Pin the mixed-display gate off.  This test walks the legacy
        # single-carrier hand-off, and its expected framing vocabulary is the
        # the legacy one; leaving the gate ambient would make it depend on
        # whatever the caller exported.
        with patch.dict(
            os.environ,
            {ACCESSORY_PROFILE_ENV: "1", ACCESSORY_MIXED_TEMPLATE_ENV: "0"},
            clear=False,
        ):
            item = _make_item(
                product_code="P_EAR",
                batch_id="B1",
                item_index=1,
                item_role="INITIAL_DIRECTION",
                direction={
                    "direction_assignment_id": "DA1",
                    "selection_run_id": "SR1",
                    "output_slot": "S1",
                    "structure_contract": _contract(),
                    "execution_reference": {"content_carrier": "WEARER_ACTIVE"},
                },
                bundle=_bundle(),
                angle_key="FACT_DISCOVERY",
                hook_id="AUDIENCE_NEED_CALLOUT",
                eligible_hooks=["AUDIENCE_NEED_CALLOUT"],
                creative={},
                visual_signature="人|房间|已佩戴|单耳",
                policy_version="test",
                used_signatures=set(),
                anchor_card=_anchor(),
                product_type="耳环",
                top_category="配饰",
            )
        self.assertIsNotNone(item)
        frozen = json.loads(item.frozen_direction_package_json)
        self.assertIn("category_execution_extension", frozen)
        seed = frozen["simplified_creative_seed"]
        self.assertEqual(
            seed["carrier_specific_execution"]["required_view"],
            "ALREADY_WORN_EAR_VISIBLE",
        )
        prompt = build_simplified_script_prompt(
            seed, target_country="泰国", target_language="泰语", duration_seconds=15
        )
        self.assertIn("类目执行补充", prompt)
        self.assertIn("已经佩戴好的状态", prompt)
        self.assertIn("戴耳环动作", prompt)

        raw = {
            "script_concept": {
                "one_sentence_idea": "已经佩戴好的耳侧分享",
                "viewer_need": "看清耳饰上耳比例",
                "hook_intent": "先给结果",
            },
            "production_design": {
                "presentation_mode": "PERSON_ON_CAMERA",
                "character": {
                    "identity": "日常分享者",
                    "appearance": "二十多岁自然气质",
                    "hair_makeup": "头发已在耳后，淡妆",
                    "speaking_personality": "朋友式分享",
                },
                "outfit": {
                    "base_outfit": "简洁圆领上衣",
                    "product_role": "耳饰作为脸侧细节点",
                    "accessories": "无其他抢眼首饰",
                },
                "scene": {
                    "location": "公寓窗边",
                    "moment": "出门前",
                    "lighting": "自然光",
                    "background": "普通房间",
                },
                "emotion": {
                    "starting_state": "自然",
                    "natural_change": "轻微满意",
                    "ending_state": "保持放松",
                },
            },
            "product_usage": {
                "identity_anchors_preserved": ["金色水滴形耳饰"],
                "selling_points_used": ["C1"],
            },
            "storyboard": [
                {
                    "shot_no": index,
                    "time_range": f"{(index - 1) * 3}-{index * 3}s",
                    "visual_content": "耳饰已经佩戴，耳侧和半脸清楚可见",
                    "character_action": "保持自然小幅呼吸",
                    "natural_emotion": "放松",
                    "camera": "手机固定近景",
                    "product_anchors_visible": ["金色水滴形耳饰"],
                    "supported_claim_keys": ["C1"],
                    "narrative_role": role,
                }
                for index, role in enumerate(
                    ("HOOK", "PROOF", "PROOF", "ENDING"), 1
                )
            ],
            "voiceover_context": {
                "viewer_relationship": "朋友分享",
                "speaking_intent": "展示佩戴效果",
                "desired_tone": "自然",
            },
        }
        normalized = normalize_simplified_visual_script(
            raw, seed, generation_provenance={"model": "test"}
        )
        self.assertEqual(
            normalized["production_design"]["accessory_execution"]["wearing_zone"],
            "EAR",
        )
        assembled = assemble_simplified_complete_script(
            normalized,
            seed,
            {
                "hook_id": "AUDIENCE_NEED_CALLOUT",
                "lines": [
                    {
                        "voiceover_text_target_language": "ดูต่างหูชิ้นนี้ก่อนนะ",
                        "voiceover_text_zh": "先看这款耳饰。",
                    }
                ],
            },
        )
        brief = assembled["video_generation_brief"]
        self.assertIn("category_execution_extension", brief)
        self.assertEqual(
            brief["accessory_execution_brief"]["schema_version"],
            "accessory-video-handoff-v5-small-motion-return",
        )
        prominence = brief["accessory_execution_brief"][
            "product_prominence_contract"
        ]
        self.assertEqual("EAR_HALF_FACE_CLOSE", prominence["primary_framing"])
        self.assertEqual(
            "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN",
            prominence["sequence_policy"],
        )

        direction, _ = build_simplified_voiceover_inputs(
            normalized,
            seed,
            frozen,
        )
        self.assertEqual(
            direction["category_execution_extension"]["profile"]["identity_authority"]["pairing_mode"],
            "UNAVAILABLE",
        )
        visual_validation = validate_simplified_visual_script(normalized, seed)
        self.assertTrue(visual_validation["valid"], visual_validation)
        self.assertTrue(validate_simplified_complete_script(assembled)["valid"])

        rendered_item = SimpleNamespace(
            result_json=json.dumps({"script": assembled}, ensure_ascii=False)
        )
        rendered = render_video_generation_prompt(
            item=rendered_item, duration_seconds=15
        )
        self.assertIn("【配饰佩戴与展示关系】", rendered)
        self.assertIn("商品已经正确佩戴在耳部", rendered)
        self.assertIn("戴耳环动作", rendered)
        self.assertIn("商品观察尺度", rendered)
        self.assertIn("半脸与耳侧近景", rendered)
        self.assertIn("PRODUCT_REACQUISITION", rendered)
        self.assertIn("视线关系：", rendered)
        self.assertIn("自然反应：", rendered)
        self.assertIn("补录半脸耳侧近景", rendered)


class NoFaceFramingAlignmentTest(unittest.TestCase):
    """A frozen NO_FACE contract must not leave face wording in the hand-off.

    The accessory profile authored its small-product framing vocabulary for a
    wearer-on-camera video, so it names the face ("半脸耳侧近景").  When the
    mixed-display template freezes a NO_FACE policy for the same product, every
    code-owned field that reaches the video prompt has to switch to a face-free
    equivalent.  Otherwise the storyboard says NO_FACE while the rendered prompt
    still asks the model for a half-face shot.
    """

    FACE_TERMS = ("半脸", "侧脸", "正脸", "全脸", "镜面头肩", "头肩", "自拍")

    def _face_hits(self, node, path=""):
        hits = []
        if isinstance(node, dict):
            for key, value in node.items():
                hits.extend(self._face_hits(value, f"{path}.{key}"))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                hits.extend(self._face_hits(value, f"{path}[{index}]"))
        elif isinstance(node, str):
            if any(term in node for term in self.FACE_TERMS):
                hits.append((path, node))
        return hits

    def _earring_extension(self, *, face_policy=None):
        extension = compile_category_execution_extension(
            product_type="耳环",
            top_category="配饰",
            anchor_card=_anchor(),
            enabled=True,
        )
        self.assertTrue(extension.get("profile"))
        if face_policy is None:
            return extension
        extension = dict(extension)
        extension["mixed_template_contract"] = {
            "execution_profile": ACCESSORY_MIXED_TEMPLATE_PROFILE,
            "face_policy": face_policy,
        }
        return extension

    def _projections(self, extension):
        carrier = resolve_category_carrier_execution(
            extension, presentation_mode="MIXED"
        )
        brief = build_category_video_brief(extension, carrier_execution=carrier)
        rhythm = project_category_capture_rhythm_contract(
            extension,
            carrier_execution=carrier,
            capture_contract={
                "profile": "NATIVE_MULTI_CLIP_V1",
                "capture_unit_count": 4,
            },
        )
        return carrier, brief, rhythm

    def test_no_face_contract_removes_face_wording(self):
        with patch.dict(
            os.environ,
            {
                ACCESSORY_PROFILE_ENV: "1",
                SMALL_ACCESSORY_MOTION_ENV: "1",
                SMALL_ACCESSORY_MULTICLIP_ENV: "1",
            },
            clear=False,
        ):
            extension = self._earring_extension(face_policy="NO_FACE")
            carrier, brief, rhythm = self._projections(extension)

        for label, node in (("carrier", carrier), ("brief", brief), ("rhythm", rhythm)):
            hits = self._face_hits(node)
            self.assertEqual([], hits, f"{label} 仍含脸部措辞: {hits[:3]}")

        prominence = carrier["product_prominence_contract"]
        self.assertIn("耳侧与耳垂近景", prominence["opening_guidance"])
        terminal = prominence["terminal_visibility"]
        self.assertEqual("耳侧与颈侧近景", terminal["ending_framing"])
        self.assertIn("耳侧与颈侧近景", terminal["ending_guidance"])
        self.assertTrue(
            all(
                "耳侧" in item for item in carrier["optional_simple_interactions"]
            ),
            carrier["optional_simple_interactions"],
        )
        framing = rhythm.get("framing_guidance_by_unit") or []
        neutral_units = [unit for unit in framing if "耳侧与颈侧近景" in unit]
        self.assertEqual(1, len(neutral_units), framing)
        self.assertIs(framing[-1], neutral_units[0], framing)
        self.assertIn("补录耳侧与颈侧近景", neutral_units[0])

    def test_no_face_contract_drops_head_and_shoulders_framing(self):
        """Head-and-shoulders is a face shot by another name.

        The EAR zone forbids ``眼睛入画 / 鼻子入画 / 嘴部入画`` and its
        ``allowed_framing`` has no head-and-shoulders option, yet the authored
        earring profile offers "后续可回到头肩或上半身交代人物与穿搭" as the
        context framing.  A NO_FACE film must not repeat that offer, otherwise
        the rendered prompt asks for a framing its own forbidden list bans.
        """

        with patch.dict(
            os.environ,
            {
                ACCESSORY_PROFILE_ENV: "1",
                SMALL_ACCESSORY_MOTION_ENV: "1",
                SMALL_ACCESSORY_MULTICLIP_ENV: "1",
            },
            clear=False,
        ):
            authored = self._earring_extension()
            self.assertIn(
                "头肩", authored["profile"]["product_prominence"]["context_guidance"]
            )
            extension = self._earring_extension(face_policy="NO_FACE")
            carrier, brief, rhythm = self._projections(extension)

        prominence = carrier["product_prominence_contract"]
        self.assertNotIn("头肩", prominence["context_guidance"])
        self.assertIn("颈部与肩线", prominence["context_guidance"])
        for label, node in (("carrier", carrier), ("brief", brief), ("rhythm", rhythm)):
            hits = self._face_hits(node)
            self.assertEqual([], hits, f"{label} 仍含脸部措辞: {hits[:3]}")

    def test_head_and_shoulders_rewrite_keeps_the_back_of_head_intent(self):
        """The rewrite must be intent-preserving inside the HAIR zone.

        "头肩侧后方近景" is a view from behind the head: the face is not in
        frame, so it is an authorised HAIR framing and must keep its direction.
        A blanket 头肩 -> 颈肩 substitution would silently turn the hair result
        shot into a neck shot, so the specific patterns have to win.
        """

        cleaned = _no_face_deep_clean(
            {
                "end_state": "通过重新构图回到发饰无遮挡的头肩侧后方近景，只在最后一瞬收住",
                "start_state": "发饰已经固定完成并从第一帧可见，人物在头肩或上半身构图内轻微调整自然重心",
                "core_action": "先录一段镜面头肩结果，再重新放置同一部手机补录侧后方发饰近景，两段直接剪切",
                "keyword_list": ["镜面头肩", "头肩关系内", "侧后方近景"],
            }
        )

        self.assertEqual(
            "通过重新构图回到发饰无遮挡的侧后方近景，只在最后一瞬收住",
            cleaned["end_state"],
        )
        self.assertIn("上半身构图内", cleaned["start_state"])
        self.assertIn("同机位重拍结果", cleaned["core_action"])
        self.assertEqual(["同机位重拍", "颈肩关系内", "侧后方近景"], cleaned["keyword_list"])

    def test_creator_self_shot_does_not_become_selfie_wording(self):
        """``CREATOR_SELF_SHOT`` names who owns the camera, not where it points.

        "自拍" was already listed in ``_FACE_TERMS`` but had no entry in the
        rewrite table, so ``_has_face_term`` reported a hit while
        ``_no_face_safe_text`` handed the text straight back -- a silent no-op.
        A model that reads the capture-mode token paraphrases it into
        "人物在同一自拍范围内轻微调整位置" and the face returns through the
        capture mode.  The rewrite must keep the recording-organisation meaning
        and drop only the framing claim.
        """

        cases = (
            (
                "手部与商品在台面柔光范围内完成前两段；人物随后站在鞋柜旁的"
                "自然自拍范围内补录耳侧佩戴画面",
                "自然同机位取景范围内",
            ),
            ("人物在同一自拍范围内轻微调整位置", "同一取景范围内"),
            (
                "手机保持普通自拍或固定近距离记录关系，腕部自然进入画面；",
                "固定近距离记录关系",
            ),
        )
        for source, expected in cases:
            cleaned = _no_face_safe_text(source)
            self.assertIn(expected, cleaned, cleaned)
            self.assertNotIn("自拍", cleaned, cleaned)

    def test_absent_contract_keeps_the_authored_wording(self):
        """No contract means no change: the legacy vocabulary is untouched."""

        with patch.dict(
            os.environ,
            {
                ACCESSORY_PROFILE_ENV: "1",
                SMALL_ACCESSORY_MOTION_ENV: "1",
                SMALL_ACCESSORY_MULTICLIP_ENV: "1",
            },
            clear=False,
        ):
            extension = self._earring_extension()
            carrier, brief, rhythm = self._projections(extension)

        rendered = json.dumps([carrier, brief, rhythm], ensure_ascii=False)
        self.assertIn("半脸与耳侧近景", rendered)
        self.assertIn("补录半脸耳侧近景", rendered)
        self.assertIn("半脸耳侧近景", rendered)

    def test_non_no_face_policy_is_a_no_op(self):
        """A contract that does not forbid faces must not rewrite the prose."""

        with patch.dict(
            os.environ,
            {
                ACCESSORY_PROFILE_ENV: "1",
                SMALL_ACCESSORY_MOTION_ENV: "1",
                SMALL_ACCESSORY_MULTICLIP_ENV: "1",
            },
            clear=False,
        ):
            extension = self._earring_extension(face_policy="FACE_ALLOWED")
            carrier, _, _ = self._projections(extension)

        rendered = json.dumps(carrier, ensure_ascii=False)
        self.assertIn("半脸与耳侧近景", rendered)


if __name__ == "__main__":
    unittest.main()
