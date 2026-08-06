import unittest

from core.simplified_complete_script import (
    CAPTURE_MODE_CREATOR_SELF_SHOT,
    SCRIPT_MODE_SIMPLIFIED,
    assemble_simplified_complete_script,
    build_product_identity_lock,
    build_simplified_creative_seed,
    build_simplified_script_prompt,
    build_simplified_voiceover_inputs,
    normalize_simplified_visual_script,
    validate_simplified_complete_script,
    validate_simplified_visual_script,
)


def _anchor():
    return {
        "product_positioning_one_liner": "近黑色短款圆领排扣外套",
        "hard_anchors": [{"anchor": "短款衣长"}],
        "display_anchors": [
            {"anchor": "五颗前襟纽扣"},
            {"anchor": "圆领"},
        ],
        "category_execution_contract": {"display_family": "apparel"},
    }


def _contract(carrier="STATIC_PRODUCT"):
    return {
        "direction_identity": {"macro_family_key": "HOOK>PROOF>ENDING"},
        "hard_constraints": {
            "content_carrier": carrier,
            "beat_sequence": ["HOOK", "PROOF", "ENDING"],
        },
    }


def _bundle(fact="正面半身上身效果", key="C1"):
    return {
        "content_mainline": fact,
        "eligible_hook_ids": ["AUDIENCE_NEED_CALLOUT"],
        "claim_atoms": [{"claim_key": key, "fact_text": fact, "role": "core_result"}],
    }


def _person_script():
    return {
        "script_concept": {
            "one_sentence_idea": "出门前确认外套带来的上半身层次",
            "viewer_need": "基础穿搭不再空",
            "hook_intent": "用拿包前的停顿制造关注",
        },
        "production_design": {
            "presentation_mode": "PERSON_ON_CAMERA",
            "character": {
                "identity": "准备出门的年轻上班族",
                "appearance": "二十多岁，气质松弛",
                "hair_makeup": "低马尾，淡妆",
                "speaking_personality": "像朋友分享刚发现的搭配",
            },
            "outfit": {
                "base_outfit": "黑色背心与高腰牛仔裤",
                "product_role": "短款外套作为上半身视觉重点",
                "accessories": "深棕色肩包",
            },
            "scene": {
                "location": "公寓玄关",
                "moment": "早晨拿包出门前",
                "lighting": "窗边自然光",
                "background": "浅色墙面和窄边穿衣镜",
            },
            "emotion": {
                "starting_state": "专注整理",
                "natural_change": "看到正面层次后轻微满意",
                "ending_state": "自然拿包离开",
            },
            "life_event": {
                "motivation": "确认今天的出门穿搭",
                "continuous_event": "穿好外套、扣一颗纽扣、拿包离开",
            },
        },
        "product_usage": {
            "identity_anchors_preserved": ["短款衣长"],
            "selling_points_used": ["C1"],
        },
        "storyboard": [
            {
                "shot_no": i,
                "time_range": f"{(i-1)*3}-{i*3}s",
                "visual_content": "人物在玄关完成出门动作，外套正面清楚可见",
                "character_action": action,
                "natural_emotion": "自然专注",
                "camera": "中景轻跟拍",
                "product_anchors_visible": ["短款衣长", "五颗前襟纽扣"],
                "supported_claim_keys": ["C1"] if i in {2, 3} else [],
                "narrative_role": role,
            }
            for i, (action, role) in enumerate(
                [
                    ("从衣架取下外套", "HOOK"),
                    ("穿上后顺手扣一颗纽扣", "PROOF"),
                    ("正面看一眼衣长和前襟", "PROOF"),
                    ("拿起肩包走向门口", "ENDING"),
                ],
                1,
            )
        ],
        "voiceover_context": {
            "viewer_relationship": "像和姐妹分享",
            "speaking_intent": "分享出门前的真实选择",
            "desired_tone": "有轻微发现感的自然口语",
        },
    }


class SimplifiedCompleteScriptTest(unittest.TestCase):
    def test_scarf_identity_lock_uses_only_approved_identity_anchors(self):
        lock = build_product_identity_lock(
            {
                "canonical_product_type": "silk_scarf",
                "product_identity": "米白蓝边印花方巾",
                "identity_anchors": ["米白底色；蓝色边框；方形轮廓"],
                "visible_detail_anchors": ["角落有小型几何印花"],
            }
        )
        self.assertEqual("product-identity-lock-v3-scarf", lock["compiler_version"])
        self.assertEqual("NOT_APPLICABLE", lock["visible_closure_contract"]["status"])
        self.assertIn("米白底色", lock["scarf_identity_contract"]["color_anchors"])
        self.assertIn("蓝色边框", lock["scarf_identity_contract"]["edge_anchors"])
        self.assertIn("方形轮廓", lock["scarf_identity_contract"]["shape_anchors"])
        self.assertIn("不授权真丝", "；".join(lock["must_not_change"]))

    def test_person_seed_freezes_creator_self_shot_capture_relationship(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )

        self.assertEqual(
            seed["creative_direction"]["capture_mode"],
            CAPTURE_MODE_CREATOR_SELF_SHOT,
        )
        self.assertEqual(
            seed["voiceover_surface_contract"]["speaker_position"],
            "CREATOR_TO_CAMERA",
        )
        prompt = build_simplified_script_prompt(
            seed, target_country="泰国", target_language="泰语", duration_seconds=15
        )
        self.assertIn("创作者自己使用手机前置镜头", prompt)
        self.assertIn("结构 Beat 是同一次分享中的内容推进", prompt)
        self.assertIn("不得设计第三人跟拍", prompt)

    def test_capture_mode_is_normalized_from_frozen_seed_without_repair_loop(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        script = _person_script()
        script["production_design"].pop("capture_mode", None)

        normalized = normalize_simplified_visual_script(
            script,
            seed,
            generation_provenance={"model": "test"},
        )

        self.assertEqual(
            normalized["production_design"]["capture_mode"],
            CAPTURE_MODE_CREATOR_SELF_SHOT,
        )

    def test_product_identity_lock_is_compiled_only_from_approved_anchors(self):
        lock = build_product_identity_lock(
            {
                "product_identity": "棕色短款翻领上装",
                "identity_anchors": [
                    "翻领结构与竖向前襟门襟",
                    "正面四颗圆形纹理扣，左右袖口各一颗扣子",
                ],
                "visible_detail_anchors": ["袖口扣与侧面衣身线条"],
            }
        )
        self.assertTrue(lock["reference_image_is_authority"])
        self.assertIn("棕色短款翻领上装", lock["must_preserve"])
        self.assertIn("袖口扣与侧面衣身线条", lock["critical_visible_details"])
        self.assertIn("禁止将参考图中的前襟扣子改成双排扣", lock["must_not_change"])
        self.assertEqual("product-identity-lock-v2", lock["compiler_version"])
        self.assertEqual(
            "SINGLE_VISIBLE_VERTICAL_ROW",
            lock["visible_closure_contract"]["layout"],
        )
        self.assertEqual("四", lock["visible_closure_contract"]["visible_button_count"])

    def test_hidden_snaps_are_compiled_as_one_visible_row_not_double_breasted(self):
        lock = build_product_identity_lock(
            {
                "product_identity": "米白色短款外套",
                "identity_anchors": [
                    "前襟为按扣与暗扣结构，左侧可见五颗圆形扣，右侧对应五颗暗扣",
                ],
                "visible_detail_anchors": ["前襟扣位细节"],
            }
        )
        closure = lock["visible_closure_contract"]
        self.assertEqual("AVAILABLE", closure["status"])
        self.assertEqual("五", closure["visible_button_count"])
        self.assertTrue(closure["hidden_counterpart"])
        self.assertIn("前襟只允许一列五颗可见扣子", lock["must_preserve"])
        self.assertIn("禁止左右对称生成两列可见纽扣", lock["must_not_change"])
        self.assertIn("禁止把隐藏暗扣画成外露纽扣", lock["must_not_change"])

    def test_confirmed_double_breasted_anchor_is_not_rewritten_as_single_row(self):
        lock = build_product_identity_lock(
            {
                "product_identity": "黑色双排扣外套",
                "identity_anchors": ["正面双排六颗纽扣"],
            }
        )
        self.assertEqual("UNAVAILABLE", lock["visible_closure_contract"]["status"])
        self.assertNotIn("禁止将参考图中的前襟扣子改成双排扣", lock["must_not_change"])

    def test_static_cluster_keeps_static_when_detail_fact_is_available(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("STATIC_PRODUCT"),
            content_bundle={
                "content_mainline": "正面半身上身效果",
                "claim_atoms": [
                    {"claim_key": "C1", "fact_text": "正面半身上身效果"},
                    {"claim_key": "C2", "fact_text": "前襟五颗扣子"},
                ],
            },
            creative_contract={},
            execution_reference={"content_carrier": "STATIC_PRODUCT"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        self.assertEqual(seed["creative_direction"]["preferred_presentation"], "STATIC_PRODUCT")
        self.assertEqual(seed["optional_visual_inspiration"]["status"], "AVAILABLE")
        self.assertEqual(
            [item["claim_key"] for item in seed["product_truth"]["approved_claims"]],
            ["C2"],
        )

    def test_static_cluster_is_not_rewritten_by_late_fact_keyword_inference(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("STATIC_PRODUCT"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "STATIC_PRODUCT"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        self.assertEqual(seed["creative_direction"]["preferred_presentation"], "STATIC_PRODUCT")
        self.assertEqual(seed["optional_visual_inspiration"]["status"], "AVAILABLE")

    def test_selling_argument_remains_mainline_when_claims_are_available(self):
        bundle = {
            "content_mainline": "基础穿搭也能有上半身层次",
            "content_mode": "SELLING_ARGUMENT",
            "value_proposition": {
                "status": "AVAILABLE",
                "text": "基础穿搭也能有上半身层次",
                "authority": "SOURCE_AUTHORIZED",
            },
            "selling_argument": {
                "argument_id": "ARG_1",
                "status": "AVAILABLE",
                "core_value": "基础穿搭也能有上半身层次",
                "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
            },
            "claim_atoms": [
                {"claim_key": "C1", "fact_text": "前襟五颗扣子"},
                {"claim_key": "C2", "fact_text": "短款衣长"},
            ],
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=bundle,
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="ARGUMENT_ARG_1",
            product_type="外套",
            top_category="女装",
        )
        self.assertEqual(seed["product_truth"]["content_mainline"], "基础穿搭也能有上半身层次")
        self.assertEqual(seed["product_truth"]["selling_argument"]["argument_id"], "ARG_1")

    def test_static_direction_keeps_authorised_wearing_value_as_mainline(self):
        bundle = {
            "content_mainline": "适合作为降温环境的外搭",
            "content_mode": "SELLING_ARGUMENT",
            "value_proposition": {
                "status": "AVAILABLE",
                "text": "适合作为降温环境的外搭",
                "authority": "SOURCE_AUTHORIZED",
            },
            "selling_argument": {
                "argument_id": "ARG_COOLING_LAYER",
                "status": "AVAILABLE",
                "core_value": "适合作为降温环境的外搭",
                "compatible_carriers": ["WEARER_ACTIVE"],
            },
            "claim_atoms": [{"claim_key": "C1", "fact_text": "前襟五颗扣子"}],
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("STATIC_PRODUCT"),
            content_bundle=bundle,
            creative_contract={},
            execution_reference={"content_carrier": "STATIC_PRODUCT"},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            content_angle_key="ARGUMENT_ARG_COOLING_LAYER",
            product_type="外套",
            top_category="女装",
        )
        truth = seed["product_truth"]
        self.assertEqual(truth["content_mode"], "SELLING_ARGUMENT")
        self.assertEqual(truth["content_mainline"], "适合作为降温环境的外搭")
        self.assertEqual(truth["selling_argument"]["status"], "AVAILABLE")
        self.assertEqual(truth["selling_argument"]["carrier_match_status"], "UNMATCHED")

    def test_selling_argument_overrides_missing_value_proposition_before_fact_fallback(self):
        bundle = {
            "content_mainline": "前襟五颗扣子",
            "content_mode": "FACTUAL_OBSERVATION",
            "value_proposition": {"status": "UNAVAILABLE", "text": ""},
            "selling_argument": {
                "argument_id": "ARG_COOLING_LAYER",
                "status": "AVAILABLE",
                "core_value": "适合作为降温环境的外搭",
                "compatible_carriers": ["WEARER_ACTIVE"],
            },
            "claim_atoms": [{"claim_key": "C1", "fact_text": "前襟五颗扣子"}],
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("STATIC_PRODUCT"),
            content_bundle=bundle,
            creative_contract={},
            execution_reference={"content_carrier": "STATIC_PRODUCT"},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            content_angle_key="ARGUMENT_ARG_COOLING_LAYER",
            product_type="外套",
            top_category="女装",
        )
        truth = seed["product_truth"]
        self.assertEqual(truth["content_mode"], "SELLING_ARGUMENT")
        self.assertEqual(truth["content_mainline"], "适合作为降温环境的外搭")
        self.assertEqual(
            truth["value_proposition"]["authority"],
            "NORMALIZED_CREATIVE_SEMANTICS",
        )

    def test_visual_script_may_use_only_part_of_shared_evidence_pool(self):
        bundle = {
            "content_mainline": "腰线视觉更清晰",
            "value_proposition": {"status": "AVAILABLE", "text": "腰线视觉更清晰"},
            "selling_argument": {
                "argument_id": "ARG_WAIST",
                "status": "AVAILABLE",
                "core_value": "腰线视觉更清晰",
                "compatible_carriers": ["WEARER_ACTIVE"],
                "core_proof_claim_keys": ["C1"],
                "optional_visual_claim_keys": ["C2"],
            },
            "claim_atoms": [
                {"claim_key": "C1", "fact_text": "短款衣长落在腰线附近", "role": "core_proof"},
                {"claim_key": "C2", "fact_text": "立领和金属拉链细节", "role": "optional_visual"},
            ],
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(), structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=bundle, creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="DISCOVERY_RESULT_PROMISE", content_angle_key="ARGUMENT_ARG_WAIST",
            product_type="外套", top_category="女装",
        )
        script = _person_script()
        for shot in script["storyboard"]:
            shot["supported_claim_keys"] = ["C2"]
        result = validate_simplified_visual_script(script, seed)
        self.assertTrue(result["valid"], result)
        self.assertTrue(any("声明使用" in warning for warning in result["warnings"]))

    def test_incompatible_reference_is_skipped_not_rejected(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("STATIC_PRODUCT"),
            content_bundle=_bundle("五颗前襟纽扣清晰可见"),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE", "action_spine": ["转身"]},
            requested_hook_id="DETAIL_SURPRISE",
            content_angle_key="DETAIL_OBSERVATION",
            product_type="外套",
            top_category="女装",
        )
        self.assertEqual(seed["creative_direction"]["preferred_presentation"], "STATIC_PRODUCT")
        self.assertEqual(seed["optional_visual_inspiration"]["status"], "SKIPPED_INCOMPATIBLE")
        self.assertEqual(seed["optional_visual_inspiration"]["action_spine"], [])

    def test_minimum_validator_accepts_complete_person_script(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        result = validate_simplified_visual_script(_person_script(), seed)
        self.assertTrue(result["valid"], result["issues"])

    def test_observational_script_does_not_require_life_event(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={
                "opening_action": "拿包后恰好露出腰线",
                "persona_role": "普通通勤者",
                "scene_motif": "客厅窗边",
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="DETAIL_SURPRISE",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        self.assertNotIn("preferred_opening_action", seed["diversity_context"])
        prompt = build_simplified_script_prompt(
            seed, target_country="泰国", target_language="泰语", duration_seconds=15
        )
        self.assertNotIn("一个连续生活事件和4至6个分镜必须同时成立", prompt)
        self.assertIn("默认采用观察式画面", prompt)
        script = _person_script()
        script["production_design"].pop("life_event", None)
        result = validate_simplified_visual_script(script, seed)
        self.assertTrue(result["valid"], result["issues"])

    def test_body_result_argument_gets_soft_show_result_opening_job(self):
        bundle = _bundle("直筒版型上身显瘦", "C1")
        bundle["selling_argument"] = {
            "status": "AVAILABLE",
            "argument_id": "ARG_BODY",
            "core_value": "适合腰部有赘肉的女生，上身显瘦",
            "operator_expression": "直筒版型遮肉显瘦",
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=bundle,
            creative_contract={"scene_motif": "浅色单色墙面"},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="PAIN_REFRAME",
            content_angle_key="ARGUMENT_ARG_BODY",
            product_type="外套",
            top_category="女装",
        )

        opening = seed["creative_direction"]["opening_visual_job"]
        self.assertEqual("SHOW_RESULT", opening["job"])
        self.assertEqual("SOFT_CREATIVE_GUIDANCE", opening["authority"])
        prompt = build_simplified_script_prompt(
            seed, target_country="泰国", target_language="泰语", duration_seconds=15
        )
        self.assertIn("不得为了执行它强制设计前后对比", prompt)

    def test_photo_argument_gets_scene_first_without_forcing_action(self):
        bundle = _bundle("适合拍照打卡", "C1")
        bundle["selling_argument"] = {
            "status": "AVAILABLE",
            "argument_id": "ARG_PHOTO",
            "core_value": "适合拍照打卡和探店，穿上很上镜",
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=bundle,
            creative_contract={"scene_motif": "咖啡厅窗边"},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="USER_ADVOCACY_STANCE",
            content_angle_key="ARGUMENT_ARG_PHOTO",
            product_type="外套",
            top_category="女装",
        )

        opening = seed["creative_direction"]["opening_visual_job"]
        self.assertEqual("SHOW_RESULT", opening["job"])
        self.assertNotIn("preferred_opening_action", seed["diversity_context"])

    def test_raw_photo_wording_cannot_override_static_structure(self):
        bundle = _bundle("正面四颗纽扣", "C1")
        bundle["selling_argument"] = {
            "status": "AVAILABLE",
            "argument_id": "ARG_PHOTO_STATIC",
            "core_value": "适合拍照打卡和探店，穿上很上镜",
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("STATIC_PRODUCT"),
            content_bundle=bundle,
            creative_contract={"scene_motif": "咖啡厅窗边"},
            execution_reference={"content_carrier": "STATIC_PRODUCT"},
            requested_hook_id="USER_ADVOCACY_STANCE",
            content_angle_key="ARGUMENT_ARG_PHOTO_STATIC",
            product_type="外套",
            top_category="女装",
        )

        self.assertEqual("STATIC_PRODUCT", seed["creative_direction"]["preferred_presentation"])
        self.assertEqual(
            "PRODUCT_FIRST",
            seed["creative_direction"]["opening_visual_job"]["job"],
        )

    def test_operator_wording_is_hidden_from_visual_prompt_but_kept_for_voiceover(self):
        raw_expression = "农村妇女穿上这件衣服都能拥有富婆的气质"
        bundle = {
            "content_mainline": raw_expression,
            "content_mode": "SELLING_ARGUMENT",
            "selling_argument": {
                "argument_id": "ARG_PREMIUM",
                "source_argument_id": "SRC_5",
                "source_claim_ids": ["CLM_5"],
                "status": "AVAILABLE",
                "operator_expression": raw_expression,
                "core_value": raw_expression,
                "creative_core_value": "整体风格更显质感",
                "claim_type": "benefit",
                "claim_theme": "style",
                "allowed_strength": "soft_only",
                "visual_dependency": "FLEXIBLE",
                "compatible_carriers": [],
            },
            "claim_atoms": [{"claim_key": "C1", "fact_text": "短款衣长"}],
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=bundle,
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            content_angle_key="ARGUMENT_ARG_PREMIUM",
            product_type="外套",
            top_category="女装",
        )
        prompt = build_simplified_script_prompt(
            seed,
            target_country="泰国",
            target_language="泰语",
            duration_seconds=15,
        )
        self.assertNotIn(raw_expression, prompt)
        self.assertIn("整体风格更显质感", prompt)
        self.assertEqual("SRC_5", seed["product_truth"]["selling_argument"]["source_argument_id"])

        direction, _ = build_simplified_voiceover_inputs(
            _person_script(),
            seed,
            {
                "structure_contract": _contract("WEARER_ACTIVE"),
                "content_bundle_brief": bundle,
                "execution_reference": {"content_carrier": "WEARER_ACTIVE"},
            },
        )
        self.assertEqual(
            raw_expression,
            direction["content_bundle_brief"]["selling_argument"]["operator_expression"],
        )

    def test_missing_product_anchor_is_a_core_failure(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        script = _person_script()
        script["product_usage"]["identity_anchors_preserved"] = []
        for shot in script["storyboard"]:
            shot["product_anchors_visible"] = []
        result = validate_simplified_visual_script(script, seed)
        self.assertFalse(result["valid"])
        self.assertTrue(any("商品身份锚点" in issue for issue in result["issues"]))

    def test_exact_clause_from_approved_compound_anchor_is_authorized(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        compound = "正面四颗圆形纹理扣，左右袖口各一颗扣子"
        seed["product_truth"]["visible_detail_anchors"].append(compound)
        script = _person_script()
        script["storyboard"][0]["product_anchors_visible"].append(
            "左右袖口各一颗扣子"
        )
        result = validate_simplified_visual_script(script, seed)
        self.assertTrue(result["valid"], result["issues"])

    def test_static_anchor_wording_does_not_create_false_person_conflict(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("STATIC_PRODUCT"),
            content_bundle=_bundle("前襟五颗扣子", "C2"),
            creative_contract={},
            execution_reference={"content_carrier": "STATIC_PRODUCT"},
            requested_hook_id="DETAIL_SURPRISE",
            content_angle_key="DETAIL_OBSERVATION",
            product_type="外套",
            top_category="女装",
        )
        script = _person_script()
        script["production_design"]["presentation_mode"] = "STATIC_PRODUCT"
        script["production_design"]["character"] = {
            "identity": "不适用，无人物出镜",
            "appearance": "不适用",
            "hair_makeup": "不适用",
            "speaking_personality": "不适用",
        }
        script["production_design"]["outfit"] = {
            "base_outfit": "不适用",
            "product_role": "挂在衣钩上的商品",
            "accessories": "无",
        }
        script["production_design"]["emotion"] = {
            "starting_state": "静置",
            "natural_change": "镜头连续观察",
            "ending_state": "静置",
        }
        script["production_design"]["life_event"] = {
            "motivation": "出门前快速确认商品正面结构",
            "continuous_event": "商品挂在衣钩上，镜头从前襟移至口袋后回到整体",
        }
        script["product_usage"] = {
            "identity_anchors_preserved": ["短款衣长"],
            "selling_points_used": ["C2"],
        }
        for shot in script["storyboard"]:
            shot["character_action"] = "无人物动作；商品保持静置，镜头连续观察。"
            shot["visual_content"] = "商品正面静置，五颗扣子和两侧口袋清楚可见，不增加其他填充物。"
            shot["product_anchors_visible"] = ["五颗前襟纽扣"]
            shot["supported_claim_keys"] = ["C2"]
        script["storyboard"][0]["visual_content"] = (
            "商品与基础衣物保持独立陈列，没有人物试穿或额外情节。"
        )
        result = validate_simplified_visual_script(script, seed)
        self.assertTrue(result["valid"], result["issues"])

    def test_voiceover_adapter_and_assembly_preserve_full_design(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        frozen = {
            "structure_contract": _contract("WEARER_ACTIVE"),
            "content_bundle_brief": _bundle(),
            "execution_reference": {"content_carrier": "WEARER_ACTIVE"},
        }
        script = _person_script()
        direction, visual = build_simplified_voiceover_inputs(script, seed, frozen)
        self.assertEqual(direction["creative_blueprint"]["persona"]["identity"], "准备出门的年轻上班族")
        self.assertEqual(direction["creative_blueprint"]["voiceover_grounding_mode"], "CONTENT_FIRST_WHOLE_VIDEO")
        self.assertEqual(
            direction["creative_blueprint"]["creator_motivation"],
            "分享出门前的真实选择",
        )
        self.assertEqual(direction["creative_blueprint"]["event_design"]["natural_event"], "")
        self.assertEqual(direction["creative_blueprint"]["retention_hook"]["opening_event"], "")
        self.assertEqual(
            [item["claim_key"] for item in direction["content_bundle_brief"]["claim_atoms"]],
            ["C1"],
        )
        self.assertIn("C1", {key for shot in visual["shots"] for key in shot["supported_claim_keys"]})
        voiceover = {
            "hook_id": "AUDIENCE_NEED_CALLOUT",
            "selected_claim_ids": ["C1"],
            "selected_selling_argument_id": "ARG_1",
            "selling_argument_realization": "ใส่คลุมเวลาอยู่ในห้องแอร์",
            "selling_argument_realization_zh": "空调房里披上更安心",
            "lines": [{
                "voiceover_text_target_language": "สาวๆ ดูตัวนี้ก่อนนะ",
                "voiceover_text_zh": "姐妹们，先看这件。",
            }],
        }
        assembled = assemble_simplified_complete_script(script, seed, voiceover)
        self.assertEqual(assembled["production_design"], script["production_design"])
        self.assertEqual(assembled["assembly_provenance"]["script_mode"], SCRIPT_MODE_SIMPLIFIED)
        self.assertEqual(
            assembled["continuous_voiceover"]["selling_argument_realization"],
            "ใส่คลุมเวลาอยู่ในห้องแอร์",
        )
        self.assertEqual(
            assembled["continuous_voiceover"]["selling_argument_realization_zh"],
            "空调房里披上更安心",
        )
        self.assertEqual(
            assembled["video_generation_brief"]["render_profile"],
            "UGC_NATIVE_V1",
        )
        self.assertTrue(
            assembled["video_generation_brief"]["product_identity_lock"][
                "reference_image_is_authority"
            ]
        )
        self.assertTrue(validate_simplified_complete_script(assembled)["valid"])

    def test_neutral_comfort_word_in_character_action_is_not_a_product_claim(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        script = _person_script()
        script["storyboard"][0]["character_action"] = "将肩包向后调整到舒适位置"
        result = validate_simplified_visual_script(script, seed)
        self.assertTrue(result["valid"], result["issues"])

    def test_v2_video_brief_requires_product_identity_lock(self):
        script = _person_script()
        script["continuous_voiceover"] = {
            "target_text": "ข้อความทดสอบ",
            "chinese_translation": "测试口播",
        }
        script["video_generation_brief"] = {
            "schema_version": "production-video-brief-v2-ugc-native",
            "render_profile": "UGC_NATIVE_V1",
        }
        result = validate_simplified_complete_script(script)
        self.assertFalse(result["valid"])
        self.assertIn("视频生成简报缺少商品身份锁", result["issues"])
        self.assertIn("视频生成简报缺少商品负向约束", result["issues"])

    def test_unauthorized_product_comfort_claim_is_still_rejected(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        script = _person_script()
        script["storyboard"][0]["visual_content"] = "人物穿上这件外套后感觉舒适"
        result = validate_simplified_visual_script(script, seed)
        self.assertFalse(result["valid"])
        self.assertIn("商品事实冲突：出现未授权效果词=舒适", result["issues"])

    def test_comfortable_sitting_posture_is_not_treated_as_product_claim(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        script = _person_script()
        script["storyboard"][0]["visual_content"] = "人物恢复舒适坐姿，商品正面保持清楚可见"
        result = validate_simplified_visual_script(script, seed)
        self.assertNotIn("商品事实冲突：出现未授权效果词=舒适", result["issues"])

    def test_scene_reference_only_exposes_approved_realism_anchors(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={
                "scene_reference_contract": {
                    "status": "AVAILABLE",
                    "selection_mode": "SOFT_REFERENCE",
                    "scene_family_key": "CAFE_DINING",
                    "prototype_name": "咖啡馆靠窗座位",
                    "approved_realism_anchors": ["靠窗座位与自然侧光", "桌面保留少量真实使用痕迹"],
                    "representative_video_ids": ["should-not-reach-prompt"],
                    "matrix_bonus": 12,
                }
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        scene = seed["diversity_context"]["scene_reference"]
        self.assertEqual(scene["status"], "AVAILABLE")
        self.assertEqual(scene["scene_family_key"], "CAFE_DINING")
        self.assertEqual(scene["realism_anchors"], ["靠窗座位与自然侧光", "桌面保留少量真实使用痕迹"])
        self.assertNotIn("representative_video_ids", scene)
        self.assertNotIn("matrix_bonus", scene)

    def test_scene_execution_card_is_compact_and_keeps_source_ids_out(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={
                "scene_reference_contract": {
                    "status": "SOFT_ONLY",
                    "scene_execution_card": {
                        "status": "AVAILABLE",
                        "source_quality": "STRUCTURE_SCENE_PROTOTYPE",
                        "prototype_name": "咖啡馆靠窗座位",
                        "space": {
                            "location": "咖啡厅靠窗座位",
                            "phone_placement": "手机靠在桌边",
                            "subject_position": "人物距手机一到两步",
                            "background_depth": "局部桌面与座位纵深",
                        },
                        "background_anchors": ["木桌", "咖啡杯", "第三项忽略"],
                        "lived_in_trace": "随手放下的帆布包",
                        "lighting": "窗边自然光",
                        "representative_video_ids": ["must-not-reach-prompt"],
                    },
                }
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        card = seed["diversity_context"]["scene_reference"]["execution_card"]
        self.assertEqual(card["status"], "AVAILABLE")
        self.assertEqual(card["background_anchors"], ["木桌", "咖啡杯"])
        self.assertEqual(card["space"]["phone_placement"], "手机靠在桌边")
        self.assertNotIn("representative_video_ids", card)

    def test_normalization_removes_unsourced_exact_scene_measurements(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        raw = _person_script()
        raw["production_design"]["scene"]["phone_placement"] = "手机放在1.3米高的架子上"
        raw["production_design"]["scene"]["subject_position"] = "人物距离手机55厘米"
        normalized = normalize_simplified_visual_script(
            raw,
            seed,
            generation_provenance={"model": "test"},
        )
        scene = normalized["production_design"]["scene"]
        self.assertNotIn("1.3米", scene["phone_placement"])
        self.assertNotIn("55厘米", scene["subject_position"])


if __name__ == "__main__":
    unittest.main()
