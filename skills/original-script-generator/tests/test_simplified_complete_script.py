import unittest

from core.category_execution import compile_category_execution_extension
from core.simplified_complete_script import (
    CAPTURE_RHYTHM_MULTICLIP,
    CAPTURE_MODE_CREATOR_SELF_SHOT,
    SCRIPT_MODE_SIMPLIFIED,
    assemble_simplified_complete_script,
    build_product_identity_lock,
    build_capture_rhythm_contract,
    build_creator_recording_profile,
    build_simplified_creative_seed,
    build_simplified_script_prompt,
    build_simplified_voiceover_inputs,
    compile_capture_units,
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
    def test_creator_recording_profile_is_narrow_and_honors_three_real_clips(self):
        direct = build_creator_recording_profile(
            top_category="女装",
            product_type="外套",
            content_carrier="WEARER_ACTIVE",
        )
        accessory = build_creator_recording_profile(
            top_category="配饰",
            product_type="发夹",
            content_carrier="WEARER_ACTIVE",
        )
        self.assertTrue(direct["enabled"])
        self.assertEqual("CREATOR_DIRECT_SHARE", direct["recording_mode"])
        self.assertFalse(accessory["enabled"])

        direct["planned_visible_clip_count"] = 3
        contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
            creator_recording_profile=direct,
        )
        self.assertEqual(3, contract["capture_unit_count"])
        self.assertEqual("WORN_DIRECT_SHARE", contract["capture_grammar"])
        self.assertEqual([], contract["observable_change_jobs"])
        self.assertNotIn("framing_guidance_by_unit", contract)

        storyboard = [
            {
                "shot_no": index,
                "narrative_role": "PROOF",
                "visual_content": f"画面{index}",
                "character_action": "自然分享",
            }
            for index in range(1, 4)
        ]
        compiled, units = compile_capture_units(storyboard, contract)
        self.assertTrue(all(not unit.get("observable_change_job") for unit in units))
        self.assertTrue(all(not shot.get("observable_change_job") for shot in compiled))
    def test_public_scene_uses_one_fixed_position_plus_handheld_cutaway(self):
        contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
            scene_context={"scene_motif": "商场连廊靠窗休息区"},
            retrieval_reference={
                "primary_real_case": {
                    "execution_card": {
                        "execution_card_id": "EXEC_PUBLIC",
                        "shot_count": 5,
                        "available_parts": ["opening", "proof", "ending"],
                    }
                }
            },
        )

        self.assertEqual(4, contract["capture_unit_count"])
        self.assertEqual(
            "ONE_PUBLIC_PHONE_POSITION_PLUS_HANDHELD_CUTAWAY",
            contract["capture_setup_mode"],
        )
        self.assertEqual("REAL_EXECUTION_CARD", contract["derivation_source"])
        self.assertEqual(2, contract["camera_setup_count"])
        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "ENDING"],
            contract["structure_unit_roles"],
        )
        self.assertEqual(
            ["OPENING", "PROOF", "ENDING"],
            contract["reference_function_sequence"],
        )
        self.assertEqual(
            4,
            contract["shot_richness_contract"]["preferred_visible_clips"],
        )

    def test_final_information_gain_review_reads_compiled_units(self):
        profile = build_creator_recording_profile(
            top_category="女装",
            product_type="外套",
            content_carrier="WEARER_ACTIVE",
        )
        profile["planned_visible_clip_count"] = 3
        contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
            creator_recording_profile=profile,
        )
        stationary_storyboard = [
            {
                "shot_no": 1,
                "narrative_role": "HOOK",
                "visual_content": "人物穿着外套面对手机分享整体效果",
                "character_action": "自然开口",
            },
            {
                "shot_no": 2,
                "narrative_role": "PROOF",
                "visual_content": "外套前襟区域露出，口袋和下摆同时进入视野",
                "character_action": "人物保持自然说话状态",
            },
            {
                "shot_no": 3,
                "narrative_role": "ENDING",
                "visual_content": "人物继续站着面对手机分享外套",
                "character_action": "保持自然站姿",
            },
        ]
        _, units = compile_capture_units(stationary_storyboard, contract)
        review = contract["final_information_gain_review"]
        self.assertEqual("LOW_INFORMATION_GAIN", review["status"])
        self.assertEqual(
            [2, 3],
            review["low_information_gain_pairs"][0]["capture_unit_pair"],
        )
        self.assertFalse(review["is_blocking"])
        self.assertEqual(
            review,
            contract["shot_richness_contract"]["final_information_gain_review"],
        )
        self.assertEqual(3, len(units))

        seated_contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
            creator_recording_profile=profile,
        )
        seated_storyboard = [dict(item) for item in stationary_storyboard]
        seated_storyboard[2]["visual_content"] = (
            "人物坐在咖啡厅窗边座位，继续展示外套与高腰裤的穿搭关系"
        )
        seated_storyboard[2]["character_action"] = "自然坐下后继续分享"
        compile_capture_units(seated_storyboard, seated_contract)
        self.assertEqual(
            "SUFFICIENT",
            seated_contract["final_information_gain_review"]["status"],
        )

        moving_contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
            creator_recording_profile=profile,
        )
        moving_storyboard = [dict(item) for item in stationary_storyboard]
        moving_storyboard[2]["visual_content"] = (
            "人物走入电梯，外套与电梯厅形成新的空间关系"
        )
        moving_storyboard[2]["character_action"] = "自然走入电梯"
        compile_capture_units(moving_storyboard, moving_contract)
        self.assertEqual(
            "SUFFICIENT",
            moving_contract["final_information_gain_review"]["status"],
        )

    def test_empty_execution_card_is_labeled_generic_fallback(self):
        contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
            retrieval_reference={
                "primary_real_case": {
                    "execution_card": {
                        "execution_card_id": "EXEC_EMPTY",
                        "shot_count": 0,
                        "available_parts": [],
                    }
                }
            },
        )
        self.assertEqual("GENERIC_FALLBACK", contract["derivation_source"])
        self.assertEqual(
            "INSUFFICIENT",
            contract["retrieved_execution_shape"]["evidence_status"],
        )
        self.assertEqual([], contract["reference_function_sequence"])

    def test_blueprint_prompt_hides_unselected_outfit_alternatives(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={
                "outfit_selection_contract": {
                    "source_type": "LIGHTWEIGHT_TEMPLATE",
                    "template_id": "STYLE_EXACT",
                    "template_display_name": "运营可见穿搭标题",
                    "target_role": "TARGET_GARMENT",
                    "style_family": "日常干净",
                    "outfit_recipe": {
                        "top": "黑色吊带",
                        "bottom": "高腰阔腿裤",
                        "footwear": "平底鞋",
                    },
                    "source_outfit_recipe": {
                        "top": "黑色吊带或短袖T恤",
                        "bottom": "高腰阔腿裤或牛仔短裤",
                    },
                    "source_accessory_items": ["小号肩包或细金属耳环"],
                    "accessory_items": ["细金属耳环"],
                    "inner_type": "黑色吊带或短袖T恤",
                    "base_outfit_direction": "黑色吊带或短袖T恤配高腰阔腿裤或牛仔短裤",
                },
                "surface_profile": {
                    "base_outfit_direction": "黑色吊带或短袖T恤配高腰阔腿裤或牛仔短裤",
                },
                "outfit_scene_affinity_contract": {
                    "match_status": "MATCHED",
                    "template_display_name": "运营可见穿搭标题",
                    "ranking_bonus": 30,
                },
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="DETAIL_SURPRISE",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        prompt = build_simplified_script_prompt(
            seed,
            target_country="泰国",
            target_language="泰语",
            duration_seconds=15,
        )
        self.assertIn("上装：黑色吊带", prompt)
        self.assertIn("下装：高腰阔腿裤", prompt)
        self.assertNotIn("source_outfit_recipe", prompt)
        self.assertNotIn("source_accessory_items", prompt)
        self.assertNotIn("小号肩包或细金属耳环", prompt)
        self.assertNotIn("运营可见穿搭标题", prompt)
        self.assertNotIn("outfit_scene_affinity_contract", prompt)
        self.assertNotIn("吊带或短袖", prompt)
        self.assertNotIn("阔腿裤或牛仔短裤", prompt)

    def test_normalization_freezes_one_piece_and_specific_accessory(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={
                "outfit_selection_contract": {
                    "source_type": "LIGHTWEIGHT_TEMPLATE",
                    "template_id": "STYLE_DRESS",
                    "target_role": "TARGET_GARMENT",
                    "outfit_structure": "ONE_PIECE",
                    "outfit_recipe": {
                        "one_piece": "深蓝条纹波点连衣裙",
                        "top": "",
                        "bottom": "",
                        "other_accessories": "深咖色鸭舌帽",
                    },
                    "accessory_policy": "SPECIFIED",
                    "accessory_items": ["深咖色鸭舌帽"],
                },
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="DETAIL_SURPRISE",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        raw = _person_script()
        raw["production_design"]["outfit"] = {
            "base_outfit": "模型随机写的上衣和长裤",
            "product_role": "目标外套",
            "accessories": "无",
        }
        normalized = normalize_simplified_visual_script(
            raw, seed, generation_provenance={"model": "test"}
        )
        outfit = normalized["production_design"]["outfit"]
        self.assertIn("连体单品：深蓝条纹波点连衣裙", outfit["base_outfit"])
        self.assertNotIn("下装", outfit["base_outfit"])
        self.assertEqual("深咖色鸭舌帽", outfit["accessories"])

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

    def test_persona_template_keeps_appearance_but_current_role_follows_scene(self):
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle(),
            creative_contract={
                "persona_role": "出发前整理旅行行李的城市穿搭者",
                "persona_selection_contract": {
                    "availability": "AVAILABLE",
                    "persona_id": "TH_BASE_1",
                    "script_projection": {
                        "identity": "曼谷办公室通勤女性",
                        "appearance": "自然泰国年轻女性",
                        "hair_makeup": "自然长发与淡妆",
                        "speaking_personality": "像朋友自然分享",
                    },
                },
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        persona = seed["diversity_context"]["persona_selection_contract"]
        self.assertEqual(
            persona["script_projection"]["identity"],
            "出发前整理旅行行李的城市穿搭者",
        )
        self.assertEqual(persona["template_identity_text"], "曼谷办公室通勤女性")
        self.assertEqual(
            persona["script_projection"]["appearance"], "自然泰国年轻女性"
        )
        prompt = build_simplified_script_prompt(
            seed, target_country="泰国", target_language="泰语", duration_seconds=15
        )
        self.assertIn("对自己的手机镜头说话的创作者", prompt)
        self.assertIn("分别录制4段简短素材", prompt)
        self.assertIn("片段间普通直接剪切", prompt)
        self.assertIn("不是被摄影团队拍摄的沉默模特", prompt)
        self.assertEqual(
            CAPTURE_RHYTHM_MULTICLIP,
            seed["capture_rhythm_contract"]["profile"],
        )

    def test_capture_units_compile_four_storyboard_passages_into_three_phone_clips(self):
        contract = {
            "profile": CAPTURE_RHYTHM_MULTICLIP,
            "capture_unit_count": 3,
        }
        storyboard, units = compile_capture_units(
            _person_script()["storyboard"], contract
        )

        self.assertEqual(3, len(units))
        self.assertEqual(
            ["CU_01", "CU_02", "CU_02", "CU_03"],
            [item["capture_unit_id"] for item in storyboard],
        )
        self.assertEqual(
            [True, True, False, True],
            [item["starts_new_take"] for item in storyboard],
        )
        self.assertEqual("DIRECT_CUT", storyboard[-1]["edit_before"])

    def test_default_shot_richness_keeps_all_five_routed_beats_when_budget_allows(self):
        contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "USE_PROCESS", "PROOF", "ENDING"],
            scene_context={"scene_motif": "公寓玄关"},
            retrieval_reference={
                "primary_real_case": {
                    "execution_card": {
                        "execution_card_id": "EXEC_RICH",
                        "shot_count": 6,
                        "available_parts": [
                            "opening",
                            "proof",
                            "use_process",
                            "ending",
                        ],
                    }
                }
            },
        )
        storyboard = [
            {
                "shot_no": index + 1,
                "time_range": f"{index * 2}-{(index + 1) * 2}s",
                "narrative_role": role,
                "visual_content": f"画面{index + 1}",
                "character_action": f"动作{index + 1}",
            }
            for index, role in enumerate(
                ["HOOK", "PROOF", "USE_PROCESS", "PROOF", "ENDING"]
            )
        ]

        compiled_storyboard, units = compile_capture_units(storyboard, contract)

        self.assertEqual(5, len(units))
        self.assertEqual(5, len({item["capture_unit_id"] for item in compiled_storyboard}))
        self.assertEqual([1], units[0]["shot_numbers"])
        self.assertEqual([5], units[-1]["shot_numbers"])
        self.assertEqual(
            "PRESERVED",
            contract["shot_richness_contract"]["preservation_status"],
        )
        self.assertEqual(
            ["HOOK", "PROOF", "USE_PROCESS", "PROOF", "ENDING"],
            contract["shot_richness_contract"]["compiled_function_sequence"],
        )

    def test_four_visible_clips_repeat_existing_proof_without_adding_use_or_ending(self):
        ending_contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
        )
        use_contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "USE_PROCESS"],
        )
        source = [
            {
                "shot_no": index,
                "time_range": f"{(index - 1) * 3}-{index * 3}s",
                "narrative_role": role,
                "visual_content": f"画面{index}",
                "character_action": f"动作{index}",
                "capture_unit_id": f"CU_{index:02d}",
                "starts_new_take": True,
            }
            for index, role in enumerate(
                ["HOOK", "PROOF", "USE", "ENDING"], 1
            )
        ]

        ending_storyboard, ending_units = compile_capture_units(
            source, ending_contract
        )
        use_storyboard, use_units = compile_capture_units(source, use_contract)

        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "ENDING"],
            [unit["structure_role"] for unit in ending_units],
        )
        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "ENDING"],
            [shot["narrative_role"] for shot in ending_storyboard],
        )
        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "USE_PROCESS"],
            [unit["structure_role"] for unit in use_units],
        )
        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "USE_PROCESS"],
            [shot["narrative_role"] for shot in use_storyboard],
        )
        self.assertNotIn(
            "ENDING", [unit["structure_role"] for unit in use_units]
        )
        self.assertEqual(
            "PRESERVED",
            use_contract["shot_richness_contract"]["structure_preservation_status"],
        )

    def test_each_visible_clip_receives_a_distinct_observable_viewing_job(self):
        contract = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=["HOOK", "PROOF", "ENDING"],
        )
        storyboard = [
            {
                "shot_no": index,
                "narrative_role": "PROOF",
                "visual_content": f"画面{index}",
                "character_action": f"动作{index}",
            }
            for index in range(1, 5)
        ]

        compiled, units = compile_capture_units(storyboard, contract)

        jobs = [unit["observable_change_job"] for unit in units]
        self.assertEqual(4, len(jobs))
        self.assertEqual(4, len(set(jobs)))
        self.assertIn("PRIMARY_PRODUCT_EVIDENCE", jobs)
        self.assertIn("DISTINCT_PRODUCT_EVIDENCE_2", jobs)
        self.assertTrue(
            all(shot.get("observable_change_job") for shot in compiled)
        )

    def test_capture_units_preserve_category_owned_roles_and_framing(self):
        contract = {
            "profile": CAPTURE_RHYTHM_MULTICLIP,
            "capture_unit_count": 3,
            "unit_roles": [
                "PRODUCT_RESULT_CLOSE",
                "NATURAL_MOTION_RELATION",
                "PRODUCT_REACQUISITION",
            ],
            "framing_guidance_by_unit": [
                "先看清已经佩戴好的商品",
                "一次连续的上半身变化",
                "最后回到商品近景",
            ],
        }

        _, units = compile_capture_units(_person_script()["storyboard"], contract)

        self.assertEqual(
            contract["unit_roles"],
            [item["unit_role"] for item in units],
        )
        self.assertEqual(
            contract["framing_guidance_by_unit"],
            [item["framing_guidance"] for item in units],
        )

    def test_capture_units_preserve_valid_semantic_boundaries_from_visual_script(self):
        contract = {
            "profile": CAPTURE_RHYTHM_MULTICLIP,
            "capture_unit_count": 3,
        }
        source = []
        for index, unit_id in enumerate(
            ["CU_01", "CU_01", "CU_02", "CU_02", "CU_03"], 1
        ):
            source.append({
                "shot_no": index,
                "narrative_role": "HOOK" if index == 1 else "PROOF",
                "capture_unit_id": unit_id,
                "starts_new_take": index in {1, 3, 5},
            })

        storyboard, units = compile_capture_units(source, contract)

        self.assertEqual(
            ["CU_01", "CU_01", "CU_02", "CU_02", "CU_03"],
            [item["capture_unit_id"] for item in storyboard],
        )
        self.assertEqual(
            "MODEL_BOUNDARIES_VALIDATED", units[0]["grouping_source"]
        )
        self.assertEqual([1, 2], units[0]["shot_numbers"])
        self.assertEqual([3, 4], units[1]["shot_numbers"])
        self.assertEqual([5], units[2]["shot_numbers"])

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
        self.assertNotIn("棕色短款翻领上装", lock["must_preserve"])
        self.assertIn("翻领结构与竖向前襟门襟", lock["must_preserve"])
        self.assertIn("袖口扣与侧面衣身线条", lock["critical_visible_details"])
        self.assertIn("禁止将参考图中的前襟扣子改成双排扣", lock["must_not_change"])
        self.assertEqual("product-identity-lock-v2", lock["compiler_version"])
        self.assertEqual(
            "SINGLE_VISIBLE_VERTICAL_ROW",
            lock["visible_closure_contract"]["layout"],
        )
        self.assertEqual("四", lock["visible_closure_contract"]["visible_button_count"])

    def test_wrist_accessory_identity_lock_does_not_inherit_garment_terms(self):
        lock = build_product_identity_lock(
            {
                "canonical_product_type": "bangle",
                "product_identity": "金色细手镯",
                "identity_anchors": ["单圈细环；开放式缺口；金色"],
                "visible_detail_anchors": ["两端圆点结构"],
            }
        )
        self.assertEqual(
            "product-identity-lock-v4-explicit-quantity", lock["compiler_version"]
        )
        self.assertEqual("NOT_APPLICABLE", lock["visible_closure_contract"]["status"])
        self.assertEqual(
            "SINGLE_PRODUCT_DEFAULT",
            lock["display_quantity_contract"]["status"],
        )
        material = "；".join(lock["must_not_change"])
        self.assertIn("手腕位置", material)
        self.assertIn("禁止新增第二只同款商品", material)
        self.assertNotIn("衣长", material)
        self.assertNotIn("袖口结构", material)

    def test_wrist_identity_lock_allows_only_explicit_authorized_stack_count(self):
        lock = build_product_identity_lock(
            {
                "canonical_product_type": "bangle",
                "product_identity": "金色细手镯",
                "identity_anchors": ["单圈细环；金色"],
                "display_quantity_contract": {
                    "status": "AUTHORIZED",
                    "mode": "SAME_SKU_STACK",
                    "min_display_count": 2,
                    "max_display_count": 3,
                    "required_display_count": 3,
                    "continuity": "SAME_COUNT_THROUGHOUT_VIDEO",
                    "authority": "EXPLICIT_OPERATOR_QUANTITY",
                },
            }
        )
        self.assertEqual(3, lock["display_quantity_contract"]["required_display_count"])
        material = "；".join(lock["must_not_change"])
        self.assertIn("始终佩戴3只同款商品", material)
        self.assertIn("不得中途增加、减少", material)
        self.assertNotIn("禁止新增第二只同款商品", material)

    def test_claw_clip_identity_lock_prevents_type_and_duplicate_drift(self):
        lock = build_product_identity_lock(
            {
                "canonical_product_type": "claw_clip",
                "product_identity": "棕色弧形抓夹",
                "identity_anchors": ["弧形夹体；两列齿梳；棕色"],
            }
        )
        material = "；".join(lock["must_not_change"])
        self.assertIn("抓夹", material)
        self.assertIn("第二个同款商品", material)
        self.assertNotIn("衣长", material)

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
        self.assertIn("不同片段不要求分别增加动作、情绪或生活事件", prompt)
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
        self.assertNotIn("opening_visual_job", prompt)
        self.assertNotIn('"source_job": "SHOW_RESULT"', prompt)
        self.assertNotIn("opening_scene_projection", prompt)

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
        governed_bundle = {
            **_bundle(),
            "selling_argument_lineage": {"status": "CONFIRMED"},
            "selling_argument": {
                "argument_id": "ARG_1",
                "source_argument_id": "PCS_HUMAN_1",
                "source_claim_ids": ["C1"],
            },
        }
        seed = build_simplified_creative_seed(
            anchor_card=_anchor(),
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=governed_bundle,
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        frozen = {
            "structure_contract": _contract("WEARER_ACTIVE"),
            "content_bundle_brief": governed_bundle,
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
        self.assertEqual(
            direction["creative_blueprint"]["event_design"]["natural_event"],
            "穿好外套、扣一颗纽扣、拿包离开",
        )
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
            "used_context_anchor": "CTX_SCENARIO_1",
            "context_consumption_status": "USED",
            "voiceover_context_mode": "SELLING_SCENARIO",
            "lines": [{
                "voiceover_text_target_language": "สาวๆ ดูตัวนี้ก่อนนะ",
                "voiceover_text_zh": "姐妹们，先看这件。",
            }],
        }
        assembled = assemble_simplified_complete_script(script, seed, voiceover)
        self.assertEqual(assembled["production_design"], script["production_design"])
        self.assertEqual(assembled["assembly_provenance"]["script_mode"], SCRIPT_MODE_SIMPLIFIED)
        self.assertEqual(
            "PCS_HUMAN_1",
            assembled["assembly_provenance"]["selling_argument_source_argument_id"],
        )
        self.assertEqual(
            "CONFIRMED",
            assembled["assembly_provenance"]["selling_argument_lineage_status"],
        )
        self.assertEqual(
            assembled["continuous_voiceover"]["selling_argument_realization"],
            "ใส่คลุมเวลาอยู่ในห้องแอร์",
        )
        self.assertEqual(
            assembled["continuous_voiceover"]["selling_argument_realization_zh"],
            "空调房里披上更安心",
        )
        self.assertEqual(
            assembled["continuous_voiceover"]["used_context_anchor"],
            "CTX_SCENARIO_1",
        )
        self.assertEqual(
            assembled["continuous_voiceover"]["context_consumption_status"],
            "USED",
        )
        self.assertEqual(
            assembled["continuous_voiceover"]["voiceover_context_mode"],
            "SELLING_SCENARIO",
        )
        self.assertEqual(
            assembled["video_generation_brief"]["render_profile"],
            "UGC_NATIVE_V2_MULTICLIP",
        )
        self.assertEqual(
            CAPTURE_RHYTHM_MULTICLIP,
            assembled["video_generation_brief"]["capture_rhythm_contract"]["profile"],
        )
        self.assertEqual(4, len(assembled["video_generation_brief"]["capture_units"]))
        self.assertEqual(
            {}, assembled["video_generation_brief"]["outfit_prompt_projection"]
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

    def test_operator_argument_explicitly_authorizes_effect_word(self):
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
        seed["semantic_spine_contract"] = {
            "source_argument": {
                "raw_text": "里面可以加针织衫，保暖的同时不会显得太笨重"
            },
            "script_thesis": {
                "core_buying_reason": "里面可以加衣服，保暖但不显笨重"
            },
        }
        script = _person_script()
        script["storyboard"][0]["visual_content"] = (
            "人物穿好外套和针织内搭，呈现保暖但不显笨重的上身结果"
        )
        result = validate_simplified_visual_script(script, seed)
        self.assertNotIn("商品事实冲突：出现未授权效果词=保暖", result["issues"])

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
                    "scene_request": {
                        "canonical_product_type": "headscarf",
                        "presentation_mode": "PERSON_ON_CAMERA",
                        "scene_intent": "DAYTIME_USE",
                        "time_light_need": "DAYLIGHT",
                        "capture_mode": "CREATOR_SELF_SHOT",
                        "country": "泰国",
                    },
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
                        "situation_tags": ["WORK_BREAK"],
                        "aesthetic_anchors": ["暖木与自然侧光"],
                        "visual_scene_recipe": {
                            "space_relationship": "咖啡厅靠窗座位；局部桌面与座位纵深",
                            "material_palette": "暖木桌面与透明玻璃杯",
                            "lighting_texture": "柔和窗边自然光",
                            "lived_in_detail": "随手放下的帆布包",
                        },
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
        self.assertEqual(card["situation_tags"], ["WORK_BREAK"])
        self.assertEqual(
            seed["diversity_context"]["scene_reference"]["scene_request"]["time_light_need"],
            "DAYLIGHT",
        )
        self.assertEqual(
            card["visual_scene_recipe"]["material_palette"],
            "暖木桌面与透明玻璃杯",
        )
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

    def test_scarf_visual_execution_v2_is_soft_and_apparel_stays_unchanged(self):
        scarf_anchor = {
            "product_positioning_one_liner": "深蓝条纹波点丝巾",
            "hard_anchors": [{"anchor": "深蓝底色与方形轮廓"}],
            "display_anchors": [{"anchor": "条纹与波点图案"}],
        }
        extension = compile_category_execution_extension(
            product_type="丝巾",
            top_category="配饰",
            anchor_card=scarf_anchor,
            enabled=True,
        )
        seed = build_simplified_creative_seed(
            anchor_card=scarf_anchor,
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle("条纹与波点图案清楚可见"),
            creative_contract={
                "opening_action": "人物已经搭好丝巾，拿起桌边的小包",
                "action_grammar": "半身结果建立→拿起小包→自然准备离开",
                "outfit_selection_contract": {
                    "silhouette_key": "SILK_SCARF_CREW_NECK",
                    "base_outfit_direction": "合身圆领上衣配高腰半裙",
                    "finish_direction": "城市休闲完成度",
                    "supporting_elements": "一只小包",
                    "grooming_direction": "自然有气色",
                },
                "scene_reference_contract": {
                    "status": "SOFT_ONLY",
                    "scene_execution_card": {
                        "status": "AVAILABLE",
                        "source_quality": "STRUCTURE_SCENE_PROTOTYPE",
                        "scene_family_key": "CAFE_DINING",
                        "situation_tags": ["WORK_BREAK"],
                        "aesthetic_anchors": ["暖木与自然侧光"],
                        "visual_scene_recipe": {
                            "space_relationship": "咖啡厅靠窗座位与局部桌面纵深",
                            "material_palette": "暖木桌面与透明玻璃杯",
                            "lighting_texture": "柔和窗边自然光",
                            "lived_in_detail": "桌边随手放下的小包",
                        },
                        "lived_in_trace": "桌边随手放下的小包",
                        "coherence_key": "PROTOTYPE:scene_v2:21",
                    },
                },
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="丝巾",
            top_category="配饰",
            category_execution_extension=extension,
        )
        visual = seed["visual_execution_contract"]
        self.assertEqual(
            visual["schema_version"], "visual-execution-contract-v4-wearable-saliency"
        )
        self.assertEqual(visual["visual_finish_profile"], "NATIVE_STYLED")
        self.assertEqual(visual["authorities"]["styling_context"], "SOFT")
        self.assertEqual(
            visual["authorities"]["visual_saliency"], "SOFT_FINAL_COMPOSITION"
        )
        self.assertFalse(visual["diagnostics_policy"]["may_block_generation"])
        self.assertEqual(visual["scene_context"]["situation_tags"], ["WORK_BREAK"])
        self.assertEqual(
            visual["scene_context"]["visual_scene_recipe"]["material_palette"],
            "暖木桌面与透明玻璃杯",
        )
        self.assertEqual(
            visual["event_progression"]["suggested_event_flow"],
            "半身结果建立→拿起小包→自然准备离开",
        )
        saliency = visual["visual_saliency"]
        self.assertEqual(saliency["exposure"]["profile"], "BRIGHT_NATIVE")
        self.assertEqual(
            saliency["separation"]["palette_class"],
            "PATTERNED_OR_MULTICOLOR",
        )
        self.assertEqual(
            saliency["opening_focus"]["framing"], "FACE_NECK_UPPER_BODY"
        )
        self.assertIn(
            "不重新系结", saliency["opening_focus"]["natural_change"]
        )
        self.assertFalse(saliency["policy"]["may_block_generation"])
        self.assertEqual("CATEGORY_CAPABILITY", seed["action_design"]["source"])
        self.assertTrue(seed["action_design"]["action_signature"])
        self.assertEqual(
            seed["action_design"],
            seed["carrier_specific_execution"]["selected_action_design"],
        )

        raw = _person_script()
        raw["production_design"].pop("life_event", None)
        raw["product_usage"]["identity_anchors_preserved"] = [
            "深蓝底色与方形轮廓"
        ]
        for shot in raw["storyboard"]:
            shot["product_anchors_visible"] = ["深蓝底色与方形轮廓"]
        normalized = normalize_simplified_visual_script(
            raw, seed, generation_provenance={"model": "test"}
        )
        self.assertEqual(
            normalized["production_design"]["life_event"]["continuous_event"],
            "半身结果建立→拿起小包→自然准备离开",
        )
        self.assertEqual(
            seed["action_design"],
            normalized["production_design"]["action_execution"],
        )
        self.assertEqual(
            seed["action_design"],
            normalized["production_design"]["accessory_execution"]["selected_action_design"],
        )
        self.assertEqual(normalized["visual_execution_diagnostics"]["mode"], "SOFT_ONLY")
        validation = validate_simplified_visual_script(normalized, seed)
        self.assertTrue(validation["valid"], validation)
        self.assertTrue(
            any("动作主线未明显落到分镜" in item for item in validation["warnings"]),
            validation,
        )

        apparel_seed = build_simplified_creative_seed(
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
            "WEARABLE_VISUAL_SALIENCY",
            apparel_seed["visual_execution_contract"]["feature_scope"],
        )
        self.assertEqual(
            "outerwear",
            apparel_seed["product_truth"]["canonical_product_type"],
        )

    def test_apparel_reuses_anchor_actions_and_demotes_scene_action_chain(self):
        anchor = {
            "product_positioning_one_liner": "浅蓝色短款蓬松外套",
            "hard_anchors": [{"anchor": "浅蓝色短款蓬松外套"}],
            "display_anchors": [
                {
                    "anchor": "背面展示横向分隔线和短款轮廓",
                    "recommended_shot_type": "背面中景，人物自然站立",
                }
            ],
            "operation_anchors": ["可轻扶领口进行小幅整理"],
            "category_execution_contract": {
                "display_family": "apparel",
                "safe_shot_templates": [
                    "侧前方轻微转身，保持外套轮廓完整可见"
                ],
            },
        }
        retrieval = {
            "status": "AVAILABLE",
            "primary_execution_card": {
                "execution_card": {
                    "execution_card_id": "EXEC_APPAREL_TEST",
                    "physical_action_type": "WEAR",
                    "shot_count": 5,
                    "available_parts": ["opening", "proof", "ending"],
                    "rhythm_logic": "多个短片段直接剪切",
                }
            },
        }
        seed = build_simplified_creative_seed(
            anchor_card=anchor,
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=_bundle("短款比例清楚可见"),
            creative_contract={
                "contract_id": "CDV_APPAREL_TEST",
                "scene_motif": "酒店房间行李架旁",
                "opening_action": "拿起随身包回到手机前",
                "action_grammar": "拿包→站着展示→补录细节",
            },
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
            retrieval_reference_contract=retrieval,
        )

        action = seed["action_design"]
        self.assertEqual("APPAREL_PRODUCT_ANCHOR_ACTION", action["interaction_id"])
        self.assertEqual("PRODUCT_ANCHOR_WITH_REAL_EXECUTION", action["source"])
        self.assertNotIn("拿包", action["core_action"])
        self.assertEqual("", action["supporting_scene_action"])
        self.assertEqual(
            "EXEC_APPAREL_TEST",
            action["execution_reference"]["execution_card_id"],
        )
        event = seed["visual_execution_contract"]["event_progression"]
        self.assertEqual("", event["suggested_opening_action"])
        self.assertEqual("", event["suggested_event_flow"])

    def test_apparel_soft_ranking_prefers_collar_action_for_collar_argument(self):
        anchor = {
            "product_positioning_one_liner": "浅蓝色短款立领外套",
            "hard_anchors": [{"anchor": "浅蓝色短款立领外套"}],
            "display_anchors": [
                {
                    "anchor": "正面半身展示立领、门襟和短款下摆",
                    "recommended_shot_type": "正面中近景",
                },
                {
                    "anchor": "背面展示横向分隔线和衣身轮廓",
                    "recommended_shot_type": "背面中景",
                },
            ],
            "operation_anchors": [
                "可双手插入口袋做自然站姿",
                "可轻扶领口进行小幅整理",
            ],
            "category_execution_contract": {
                "display_family": "apparel",
                "safe_shot_templates": ["侧前方轻微转身，保持外套轮廓清楚"],
            },
        }
        bundle = _bundle("手插口袋或轻扶领口的日常上身状态", "CLM_COLLAR")
        bundle["selling_argument"] = {
            "status": "AVAILABLE",
            "argument_id": "ARG_COLLAR",
            "creative_core_value": "高领防风",
            "claim_theme": "operator_value",
            "proof_subject": "ON_BODY_RESULT",
            "core_proof_claim_keys": ["CLM_COLLAR"],
        }
        bundle["semantic_spine_contract"] = {
            "source_argument": {
                "raw_text": "高领防风，拉起来更暖，进入室内也可以敞开穿"
            },
            "script_thesis": {
                "core_buying_reason": "冷风里护住领口，进室内可以敞开"
            },
        }
        seed = build_simplified_creative_seed(
            anchor_card=anchor,
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=bundle,
            creative_contract={"contract_id": "CDV_COLLAR", "scene_motif": "酒店房间"},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        action = seed["action_design"]
        self.assertIn("existing-action-design-v3", action["selection_policy"])
        self.assertTrue(
            any(term in action["core_action"] for term in ("领口", "立领", "门襟")),
            action,
        )
        self.assertNotEqual("可双手插入口袋做自然站姿", action["core_action"])

    def test_apparel_soft_ranking_prefers_silhouette_for_layering_argument(self):
        anchor = {
            "product_positioning_one_liner": "浅蓝色宽松短外套",
            "hard_anchors": [{"anchor": "浅蓝色宽松短外套"}],
            "display_anchors": [
                {
                    "anchor": "背面展示衣身宽松轮廓",
                    "recommended_shot_type": "背面中景自然慢走",
                }
            ],
            "operation_anchors": ["可双手插入口袋做自然站姿"],
            "category_execution_contract": {
                "display_family": "apparel",
                "safe_shot_templates": ["侧前方轻微转身，保持衣身轮廓清楚"],
            },
        }
        bundle = _bundle("衣身宽松轮廓清楚可见", "CLM_FIT")
        bundle["selling_argument"] = {
            "status": "AVAILABLE",
            "argument_id": "ARG_LAYERING",
            "creative_core_value": "宽松版型",
            "claim_theme": "fit",
            "proof_subject": "ON_BODY_RESULT",
        }
        bundle["semantic_spine_contract"] = {
            "source_argument": {
                "raw_text": "宽松不臃肿，里面还能叠穿卫衣或针织衫"
            },
            "script_thesis": {
                "core_buying_reason": "可以加内搭但轮廓不会显得笨重"
            },
        }
        seed = build_simplified_creative_seed(
            anchor_card=anchor,
            structure_contract=_contract("WEARER_ACTIVE"),
            content_bundle=bundle,
            creative_contract={"contract_id": "CDV_LAYERING"},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            content_angle_key="FACT_DISCOVERY",
            product_type="外套",
            top_category="女装",
        )
        action = seed["action_design"]
        self.assertIn("existing-action-design-v3", action["selection_policy"])
        self.assertTrue(
            any(term in action["core_action"] for term in ("轮廓", "背面", "侧前")),
            action,
        )
        self.assertNotEqual("可双手插入口袋做自然站姿", action["core_action"])

    def test_hair_motion_actions_rotate_by_frozen_direction_without_state(self):
        hair_anchor = {
            "product_positioning_one_liner": "棕色抓夹",
            "hard_anchors": [{"anchor": "棕色抓夹"}],
            "display_anchors": [{"anchor": "抓夹固定在后脑盘发区域"}],
        }
        extension = compile_category_execution_extension(
            product_type="抓夹",
            top_category="配饰",
            anchor_card=hair_anchor,
            enabled=True,
        )
        action_ids = []
        for contract_id in (
            "CDV_F09E549122FEBE3AAA55DECA",
            "CDV_7065DE0CFFC3D4CFD554C1BA",
            "CDV_0A997758834CB16DD750B285",
        ):
            seed = build_simplified_creative_seed(
                anchor_card=hair_anchor,
                structure_contract=_contract("WEARER_ACTIVE"),
                content_bundle=_bundle("抓夹固定后的盘发结果清楚可见"),
                creative_contract={
                    "contract_id": contract_id,
                    "opening_action": "发饰已经佩戴完成",
                    "action_grammar": "结果建立→自然观察→结束分享",
                },
                execution_reference={"content_carrier": "WEARER_ACTIVE"},
                requested_hook_id="AUDIENCE_NEED_CALLOUT",
                content_angle_key="FACT_DISCOVERY",
                product_type="抓夹",
                top_category="配饰",
                category_execution_extension=extension,
            )
            self.assertEqual(
                "action-variety-v2-direction-rotation",
                seed["action_design"]["selection_policy"],
            )
            action_ids.append(seed["action_design"]["interaction_id"])

        self.assertEqual(3, len(set(action_ids)))

    def test_reference_realization_is_observability_only(self):
        retrieval_contract = {
            "status": "AVAILABLE",
            "selection_mode": "CASE_FIRST_SAME_VIDEO_DIMENSIONS",
            "scene_alignment_status": "STRUCTURE_CARRIER_MATCH_ONLY",
            "usage_boundary": "只借鉴拍摄语法",
            "authority_boundary": {"product_truth": "CURRENT_PRODUCT_ANCHOR_AUTHORITY"},
            "primary_case": {
                "reference_execution_spine": {
                    "schema_version": "reference-execution-spine-v1",
                    "reference_spine_id": "RSP_TEST",
                    "available_parts": ["opening", "proof", "ending"],
                    "parts": {
                        "opening": {"status": "AVAILABLE", "visual_action": "商品结果近景"},
                        "proof": {"status": "AVAILABLE", "visual_action": "人物自然走动"},
                        "ending": {"status": "AVAILABLE", "visual_action": "回到商品结果"},
                    },
                },
                "dimension_references": {},
            },
            "supporting_case": {},
        }
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
            retrieval_reference_contract=retrieval_contract,
        )
        raw = _person_script()
        raw["reference_realization"] = {
            "status": "APPLIED",
            "adopted_parts": ["opening", "proof", "unknown"],
            "adaptation_notes": "借用了结果开场与中段自然动作",
        }
        normalized = normalize_simplified_visual_script(
            raw,
            seed,
            generation_provenance={"model": "test"},
        )
        realization = normalized["reference_realization"]
        self.assertEqual(realization["status"], "APPLIED")
        self.assertEqual(realization["reference_spine_id"], "RSP_TEST")
        self.assertEqual(realization["adopted_parts"], ["opening", "proof"])
        self.assertFalse(realization["hard_required"])
        validation = validate_simplified_visual_script(normalized, seed)
        self.assertTrue(validation["valid"], validation)


if __name__ == "__main__":
    unittest.main()
