from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.complete_script_v3 import (
    COMPLETE_BLUEPRINT_SCHEMA_VERSION,
    EXACT_PRODUCT_OUTFIT_SCENE_MATCH_BONUS,
    FIELD_CONSUMERS,
    SELLING_SCENE_SEMANTIC_MATCH_BONUS,
    assign_audio_actual,
    attach_field_consumers,
    build_creative_diversity_contract,
    creative_product_profile,
    creative_usage_row,
    validate_complete_blueprint,
    validate_complete_script,
    video_prompt_projection,
)
from core.storage import PipelineStorage
from scripts.run_reality_reference_stage0 import (
    _blueprint_cache_matches_model,
    _normalize_blueprint,
)


def direction() -> dict:
    return {
        "direction_assignment_id": "SRA_TEST",
        "output_slot": "S1",
        "cluster_id": 8,
        "execution_reference": {
            "content_carrier": "WEARER_ACTIVE",
            "behavior_chain": ["扣好前襟", "展示扣位"],
            "shot_execution_spine": [],
        },
        "structure_execution_plan": {"macro_family_key": "HOOK>PROOF>ENDING"},
        "content_bundle_brief": {"eligible_hook_ids": ["DETAIL_SURPRISE"]},
    }


def blueprint_for(contract: dict) -> dict:
    opening = contract["opening_action"]
    return attach_field_consumers(
        {
            "schema_version": COMPLETE_BLUEPRINT_SCHEMA_VERSION,
            "authority": "CREATIVE_DESIGN",
            "diversity_contract_id": contract["contract_id"],
            "presentation_mode": contract["required_presentation_mode"],
            "creative_thesis": "从前襟动作进入，再看两个能被画面证明的细节",
            "creator_motivation": "出门前顺手替朋友核对实物细节",
            "viewer_relationship": contract["viewer_relationship"],
            "retention_hook": {
                "opening_event": "人物扣到最后一颗时突然停手并把前襟转向镜头",
                "delayed_answer": "先不露出完整轮廓，切到中景后才看清衣长和口袋关系",
                "payoff_time": "3-5s",
            },
            "persona": {
                "identity": "在曼谷上班的年轻女性",
                "age_presence": "二十多岁",
                "appearance": "自然肤质和日常状态",
                "hair_makeup": "低马尾与淡妆",
                "styling": "简单通勤下装",
                "speaking_personality": "像朋友一样边看边说",
                "performance_intensity": "动作克制，不持续看镜头",
            },
            "scene": {
                "location": contract["scene_motif"],
                "moment": "准备出门前的最后检查",
                "lighting": "侧面自然窗光",
                "background": "无品牌标识的浅色墙面",
                "camera_setup": "手机固定在胸口高度",
                "why_this_scene": "这里本来就是人物整理外套的位置",
            },
            "performance_flow": {
                "entry_state": "人物已经穿好商品，手停在前襟",
                "behavior_motivation": f"{opening}，随后拿好随身物品准备离开",
                "reaction_points": [],
                "ending_state": "人物带着随身物品离开原位置",
            },
            "event_design": {
                "event_motif": "出门前完成穿搭并拿好随身物品",
                "start_state": "人物已经穿好商品，随身物品放在一旁",
                "natural_event": f"{opening}，随后拿好随身物品准备离开",
                "core_result_moment": "人物拿起随身物品站直时看清完整上身结果",
                "end_state": "人物带着随身物品离开原位置",
            },
            "macro_visual_passages": [
                {
                    "passage_no": 1,
                    "narrative_role": "EVENT_ENTRY",
                    "visible_process": "人物正在完成穿着动作",
                    "observable_action": "人物完成穿着动作",
                    "camera_observation": "固定半身中景",
                    "product_visibility": "PARTIAL",
                    "supported_claim_keys": ["C1"],
                },
                {
                    "passage_no": 2,
                    "narrative_role": "EVENT_PROOF",
                    "visible_process": "人物站直并拿起随身物品，完整上身状态可见",
                    "observable_action": "人物拿起随身物品",
                    "camera_observation": "固定半身中景",
                    "product_visibility": "FULL",
                    "supported_claim_keys": ["C1", "C2"],
                },
                {
                    "passage_no": 3,
                    "narrative_role": "EVENT_END",
                    "visible_process": "人物带着随身物品离开原位置",
                    "observable_action": "人物走向画面边缘",
                    "camera_observation": "固定中景",
                    "product_visibility": "FULL",
                    "supported_claim_keys": ["C2"],
                },
            ],
            "visual_language": {
                "image_texture": "普通手机实拍",
                "camera_behavior": "固定机位，仅一次局部切换",
                "framing_bias": "整体和细节交替",
                "editing_rhythm": "动作完成后再切镜",
                "anti_template_rules": list(contract["forbidden_recent_patterns"]),
            },
            "voice_identity": {
                "tone": "自然发现式分享",
                "relationship_mode": "朋友式提醒",
                "particle_density": "每个语义段最多一个自然语气词",
                "sales_pressure": "低",
                "forbidden_tone": ["主播催单", "参数朗读"],
            },
            "audio_direction": {
                "bgm_style": "轻量日常节奏",
                "environment_sound": "保留一次扣合声",
                "voiceover_priority": "口播覆盖主论证，动作声优先时让位",
            },
        }
    )


class CompleteScriptV3Tests(unittest.TestCase):
    def test_soft_product_profiles_cover_apparel_worn_and_small_accessories(self) -> None:
        self.assertEqual("WORN_APPAREL", creative_product_profile("连衣裙", "女装"))
        self.assertEqual("WORN_ACCESSORY", creative_product_profile("围巾", "配饰"))
        self.assertEqual("WORN_ACCESSORY", creative_product_profile("头巾", "配饰"))
        self.assertEqual("HAND_STATIC_ACCESSORY", creative_product_profile("戒指", "配饰"))

    def test_scarf_wearer_contract_uses_worn_accessory_life_event(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="SCARF_1",
            country="泰国",
            category="配饰",
            product_type="围巾",
            direction=direction(),
            recent_usage=[],
        )

        self.assertEqual("WORN_ACCESSORY", contract["creative_product_profile"])
        self.assertIn("佩戴", contract["opening_action"])
        self.assertNotIn("工作台", contract["scene_motif"])

    def test_wrist_and_hair_accessories_receive_role_aware_outfit_contracts(self) -> None:
        wrist = build_creative_diversity_contract(
            product_code="WRIST_1",
            country="泰国",
            category="配饰",
            product_type="手镯",
            direction=direction(),
            recent_usage=[],
        )
        hair = build_creative_diversity_contract(
            product_code="HAIR_1",
            country="泰国",
            category="发饰",
            product_type="抓夹",
            direction=direction(),
            recent_usage=[],
        )
        self.assertEqual("WORN_ACCESSORY", wrist["creative_product_profile"])
        self.assertEqual(
            "SUPPORTING_OUTFIT_WRIST",
            wrist["outfit_selection_contract"]["target_role"],
        )
        self.assertIn("WRIST", wrist["outfit_selection_contract"]["visibility_zones"])
        self.assertEqual("WORN_ACCESSORY", hair["creative_product_profile"])
        self.assertEqual(
            "SUPPORTING_OUTFIT_HAIR",
            hair["outfit_selection_contract"]["target_role"],
        )
        self.assertIn("HAIR", hair["outfit_selection_contract"]["visibility_zones"])
        self.assertNotIn("领口", hair["outfit_selection_contract"]["visibility_requirement"])

    def test_scarf_subtypes_receive_distinct_scene_and_outfit_pools(self) -> None:
        outputs = {}
        for product_type in ("秋冬围巾", "丝巾", "头巾"):
            outputs[product_type] = build_creative_diversity_contract(
                product_code=f"P_{product_type}",
                country="泰国",
                category="配饰",
                product_type=product_type,
                direction=direction(),
                recent_usage=[],
            )
        self.assertEqual(
            3,
            len({item["outfit_selection_contract"]["silhouette_key"] for item in outputs.values()}),
        )
        self.assertTrue(
            all(
                item["outfit_selection_contract"]["contract_version"]
                == "outfit-selection-v10-persona-affinity"
                for item in outputs.values()
            )
        )
        self.assertNotEqual(
            outputs["秋冬围巾"]["scene_motif"],
            outputs["丝巾"]["scene_motif"],
        )
        self.assertNotIn("缠绕", outputs["头巾"]["opening_action"])
        self.assertEqual(
            "SUPPORTING_OUTFIT_HEAD",
            outputs["头巾"]["outfit_selection_contract"]["target_role"],
        )
        self.assertIn(
            outputs["头巾"]["outfit_selection_contract"]["style_family"],
            {
                "Y2K_BOLD_FEMININE",
                "STREET_FEMININE",
                "RESORT_CHIC",
                "CITY_MINIMAL_HEAD_STYLE",
            },
        )
        self.assertNotIn(
            "中长半裙",
            outputs["丝巾"]["outfit_selection_contract"]["base_outfit_direction"],
        )
        self.assertTrue(
            outputs["丝巾"]["outfit_selection_contract"]["outfit_recipe"]
        )

    def test_silk_scarf_hair_mode_switches_supporting_outfit_to_head_role(self) -> None:
        hair_direction = direction()
        hair_direction["content_bundle_brief"] = {
            "selling_argument": {
                "primary_demonstration_mode": "HAIR_TIE",
            }
        }
        contract = build_creative_diversity_contract(
            product_code="P_SILK_HAIR",
            country="泰国",
            category="配饰",
            product_type="丝巾",
            direction=hair_direction,
            recent_usage=[],
        )
        outfit = contract["outfit_selection_contract"]
        self.assertEqual("HAIR_TIE", outfit["demonstration_mode"])
        self.assertEqual("SUPPORTING_OUTFIT_HEAD", outfit["target_role"])
        self.assertIn("HEAD", outfit["visibility_zones"])

    def test_headscarf_sun_shade_compiles_daylight_scene_request(self) -> None:
        sun_direction = direction()
        sun_direction["content_bundle_brief"] = {
            "selling_argument": {
                "argument_theme": "SUN_SHADE",
                "proof_subject": "SCENE_USAGE",
                "primary_demonstration_mode": "HEAD_WORN",
            }
        }
        contract = build_creative_diversity_contract(
            product_code="P_HEAD_SUN",
            country="泰国",
            category="配饰",
            product_type="头巾",
            direction=sun_direction,
            recent_usage=[],
        )
        request = contract["scene_request_contract"]
        self.assertEqual(request["canonical_product_type"], "headscarf")
        self.assertEqual(request["scene_intent"], "DAYTIME_USE")
        self.assertEqual(request["time_light_need"], "DAYLIGHT")
        self.assertEqual(request["capture_mode"], "CREATOR_SELF_SHOT")
        self.assertEqual(
            contract["scene_reference_contract"]["scene_request"], request
        )
        self.assertIn("DAYTIME_USE", contract["scene_affinity_preferences"])
        self.assertIn("DAYTIME_USE", contract["scene_affinity_matches"])
        self.assertGreater(contract["scene_affinity_score"], 0)
        self.assertGreater(contract["scene_proof_environment_score"], 0)
        self.assertTrue(
            any(
                token in contract["scene_motif"]
                for token in ("户外", "室外", "临街", "步道", "楼下", "遮檐")
            )
        )
        self.assertNotIn("客厅", contract["scene_motif"])
        self.assertNotIn("玄关", contract["scene_motif"])

    def test_silk_and_head_scarf_outfits_rotate_structured_silhouettes_in_batch(self) -> None:
        expectations = {"丝巾": 5, "头巾": 4}
        for product_type, minimum_unique in expectations.items():
            recent = []
            silhouettes = set()
            for index in range(5):
                contract = build_creative_diversity_contract(
                    product_code=f"P_{product_type}_{index}",
                    country="泰国",
                    category="配饰",
                    product_type=product_type,
                    direction=direction(),
                    recent_usage=recent,
                )
                outfit = contract["outfit_selection_contract"]
                silhouettes.add(outfit["silhouette_key"])
                self.assertTrue(outfit["outfit_recipe"])
                recent.append({**contract, "_batch_reserved": True})
            self.assertGreaterEqual(len(silhouettes), minimum_unique)

    def test_generic_scarf_no_longer_uses_product_led_outfit_placeholder(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P_GENERIC_SCARF",
            country="泰国",
            category="配饰",
            product_type="围巾",
            direction=direction(),
            recent_usage=[],
        )
        outfit = contract["outfit_selection_contract"]
        self.assertNotEqual("PRODUCT_LED", outfit["silhouette_key"])
        self.assertTrue(outfit["base_outfit_direction"])
        self.assertTrue(outfit["visibility_requirement"])

    def test_category_scene_preferences_softly_rank_existing_scarf_scenes(self) -> None:
        scarf_direction = direction()
        scarf_direction["category_execution_extension"] = {
            "profile": {
                "product_subtype": "silk_scarf",
                "scene_preferences": ["CAFE_DINING", "OFFICE_WORKBREAK"],
            }
        }
        contract = build_creative_diversity_contract(
            product_code="P_SILK_SCENE",
            country="泰国",
            category="配饰",
            product_type="丝巾",
            direction=scarf_direction,
            recent_usage=[],
        )
        self.assertEqual(
            ["CAFE_DINING", "OFFICE_WORKBREAK"],
            contract["category_scene_affinity_preferences"],
        )
        self.assertTrue(contract["scene_affinity_matches"])
        self.assertGreater(contract["scene_affinity_score"], 0)
        self.assertEqual("SOFT_PREFERENCE_ONLY", contract["scene_affinity_policy"])

    def test_small_accessory_static_contract_does_not_use_clothes_hanger(self) -> None:
        static_direction = direction()
        static_direction["execution_reference"]["content_carrier"] = "STATIC_PRODUCT"
        contract = build_creative_diversity_contract(
            product_code="RING_1",
            country="泰国",
            category="配饰",
            product_type="戒指",
            direction=static_direction,
            recent_usage=[],
        )

        self.assertEqual("HAND_STATIC_ACCESSORY", contract["creative_product_profile"])
        self.assertNotIn("衣架", contract["scene_motif"])
        self.assertTrue(any(token in contract["scene_motif"] for token in ("托盘", "台面")))

    def test_recent_exact_combination_is_not_selected_again(self) -> None:
        first = build_creative_diversity_contract(
            product_code="P1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        second = build_creative_diversity_contract(
            product_code="P2",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[first],
        )
        signature = lambda item: (item["persona_role"], item["scene_motif"], item["opening_action"])
        self.assertNotEqual(signature(first), signature(second))
        self.assertTrue(any("＋" in item for item in first["forbidden_recent_patterns"]))

    def test_upper_apparel_rotates_across_eight_unique_life_events(self) -> None:
        recent = []
        signatures = set()
        for index in range(8):
            contract = build_creative_diversity_contract(
                product_code=f"P_ROTATE_{index}",
                country="泰国",
                category="女装",
                product_type="外套",
                direction=direction(),
                recent_usage=recent,
            )
            signatures.add(
                (
                    contract["persona_role"],
                    contract["scene_motif"],
                    contract["opening_action"],
                )
            )
            recent.append(contract)

        self.assertEqual(8, len(signatures))
        self.assertGreaterEqual(
            len({contract["scene_motif"] for contract in recent}),
            8,
        )
        self.assertGreaterEqual(
            len({contract.get("scene_family_key") for contract in recent}),
            3,
        )
        self.assertGreaterEqual(
            len({contract.get("surface_profile", {}).get("surface_profile_key") for contract in recent}),
            3,
        )
        self.assertGreaterEqual(
            len({contract.get("outfit_selection_contract", {}).get("silhouette_key") for contract in recent}),
            3,
        )
        self.assertTrue(
            all(
                contract.get("outfit_selection_contract", {}).get("source_type")
                == "INTERNAL_PROFILE"
                for contract in recent
            )
        )
        self.assertTrue(
            all(
                contract.get("outfit_selection_contract", {}).get("template_id") is None
                for contract in recent
            )
        )
        self.assertTrue(
            all(
                contract.get("moment_family_id")
                in {
                    "READY_TO_LEAVE",
                    "COMMUTE_TRANSITION",
                    "LEISURE_OUTING",
                    "QUICK_ERRAND",
                    "WAITING_IN_TRANSIT",
                }
                for contract in recent
            )
        )
        self.assertGreaterEqual(
            len({contract["moment_family_id"] for contract in recent}),
            3,
        )

    def test_outfit_rotation_reads_persisted_metadata_json(self) -> None:
        first = build_creative_diversity_contract(
            product_code="P_OUTFIT_1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        first_key = first["outfit_selection_contract"]["silhouette_key"]
        persisted = [{
            "persona_role": first["persona_role"],
            "scene_motif": first["scene_motif"],
            "opening_action": first["opening_action"],
            "metadata_json": json.dumps({
                "outfit_selection_contract": first["outfit_selection_contract"],
                "surface_profile": first["surface_profile"],
            }, ensure_ascii=False),
        }]

        second = build_creative_diversity_contract(
            product_code="P_OUTFIT_2",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=persisted,
        )

        self.assertNotEqual(
            first_key,
            second["outfit_selection_contract"]["silhouette_key"],
        )
        self.assertEqual(
            "outfit-selection-v10-persona-affinity",
            second["outfit_selection_contract"]["contract_version"],
        )
        self.assertFalse(second["outfit_selection_contract"]["hard_required"])

    def test_selling_argument_softly_prefers_a_matching_scene(self) -> None:
        premium_direction = direction()
        premium_direction["content_bundle_brief"] = {
            "content_mainline": "预算有限也想穿出复古高级的质感",
            "selling_argument": {
                "status": "AVAILABLE",
                "core_value": "预算有限也想穿出复古高级的质感",
                "operator_expression": "想穿出有钱感，颜色复古",
            },
        }
        contract = build_creative_diversity_contract(
            product_code="P_PREMIUM",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=premium_direction,
            recent_usage=[],
        )

        self.assertEqual("SOFT_PREFERENCE_ONLY", contract["scene_affinity_policy"])
        self.assertIn("PREMIUM_AMBIENCE", contract["scene_affinity_preferences"])
        self.assertIn("PREMIUM_AMBIENCE", contract["scene_affinity_matches"])
        self.assertGreater(contract["scene_affinity_score"], 0)

    def test_photo_argument_can_reach_photo_friendly_scene_without_a_gate(self) -> None:
        photo_direction = direction()
        photo_direction["content_bundle_brief"] = {
            "selling_argument": {
                "status": "AVAILABLE",
                "core_value": "适合拍照打卡和探店，穿上很上镜",
            }
        }
        contract = build_creative_diversity_contract(
            product_code="P_PHOTO",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=photo_direction,
            recent_usage=[],
        )

        self.assertIn("PHOTO_FRIENDLY", contract["scene_affinity_matches"])
        self.assertIn(
            "PHOTO_FRIENDLY", contract["selling_scene_affinity_matches"]
        )
        self.assertEqual(
            SELLING_SCENE_SEMANTIC_MATCH_BONUS,
            contract["selling_scene_semantic_bonus"],
        )
        self.assertGreater(
            contract["selling_scene_semantic_bonus"],
            EXACT_PRODUCT_OUTFIT_SCENE_MATCH_BONUS,
        )
        self.assertTrue(
            any(
                token in contract["scene_motif"]
                for token in ("咖啡", "精品", "酒店", "商场", "展览", "书店")
            )
        )

    def test_photo_argument_outranks_exact_outfit_office_preference(self) -> None:
        photo_direction = direction()
        photo_direction["content_bundle_brief"] = {
            "selling_argument": {
                "status": "AVAILABLE",
                "core_value": "适合拍照打卡和探店，穿上很上镜",
            }
        }
        candidates = [
            {
                "moment_family_id": "OFFICE_BREAK",
                "persona_role": "通勤分享者",
                "viewer_relationship": "像朋友分享",
                "scene_motif": "办公室衣帽区靠窗墙面",
                "opening_action": "人物拿起包准备离开",
                "action_grammar": "整体→细节→整体",
                "visual_tone": "自然记录",
            },
            {
                "moment_family_id": "LEISURE_OUTING",
                "persona_role": "日常分享者",
                "viewer_relationship": "像朋友分享",
                "scene_motif": "咖啡厅靠窗的普通座位区域",
                "opening_action": "人物看向手机准备分享",
                "action_grammar": "整体→细节→整体",
                "visual_tone": "自然记录",
            },
        ]
        exact_office_outfit = {
            "source_type": "LIGHTWEIGHT_TEMPLATE",
            "source_tier": "EXACT_PRODUCT_TEMPLATE",
            "match_scope": "EXACT_PRODUCT_CODE",
            "template_id": "STYLE_OFFICE",
            "scene_families": ["OFFICE_WORKBREAK"],
            "silhouette_key": "OFFICE",
            "style_family": "OFFICE",
            "outfit_recipe": {"top": "简洁内搭", "bottom": "高腰长裤"},
        }
        with patch(
            "core.complete_script_v3._creative_combinations",
            return_value=candidates,
        ), patch(
            "core.complete_script_v3._select_outfit_contract",
            return_value=(exact_office_outfit, 0, 0),
        ):
            contract = build_creative_diversity_contract(
                product_code="P_PHOTO_EXACT_OFFICE",
                country="泰国",
                category="女装",
                product_type="外套",
                direction=photo_direction,
                recent_usage=[],
            )

        self.assertIn("咖啡", contract["scene_motif"])
        self.assertEqual(
            SELLING_SCENE_SEMANTIC_MATCH_BONUS,
            contract["selling_scene_semantic_bonus"],
        )
        self.assertEqual(
            "FALLBACK", contract["outfit_scene_affinity_contract"]["match_status"]
        )

    def test_legacy_creative_carrier_override_cannot_rewrite_structure_carrier(self) -> None:
        hand_direction = direction()
        hand_direction["execution_reference"]["content_carrier"] = "HAND_ONLY"
        hand_direction["_creative_carrier_override"] = "WEARER_ACTIVE"
        hand_direction["content_bundle_brief"] = {
            "selling_argument": {
                "status": "AVAILABLE",
                "core_value": "老钱风穿搭适合多种场合",
            }
        }
        contract = build_creative_diversity_contract(
            product_code="P_OVERRIDE",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=hand_direction,
            recent_usage=[],
        )

        self.assertEqual("HANDS_ONLY", contract["required_presentation_mode"])

    def test_static_premium_argument_uses_quality_hanger_scene(self) -> None:
        static_direction = direction()
        static_direction["execution_reference"]["content_carrier"] = "STATIC_PRODUCT"
        static_direction["content_bundle_brief"] = {
            "selling_argument": {
                "status": "AVAILABLE",
                "core_value": "预算有限也想穿出高级有质感的感觉",
            }
        }
        contract = build_creative_diversity_contract(
            product_code="P_STATIC_PREMIUM",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=static_direction,
            recent_usage=[],
        )

        self.assertEqual("STATIC_PRODUCT", contract["required_presentation_mode"])
        self.assertIn("PREMIUM_AMBIENCE", contract["scene_affinity_matches"])
        self.assertTrue(any(token in contract["scene_motif"] for token in ("精品", "木质", "深木")))

    def test_creative_usage_ledger_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PipelineStorage(Path(temp_dir) / "stage0.sqlite3", database_url="sqlite")
            contract = build_creative_diversity_contract(
                product_code="P1",
                country="泰国",
                category="女装",
                product_type="外套",
                direction=direction(),
                recent_usage=[],
            )
            row = creative_usage_row(
                contract=contract,
                product_code="P1",
                direction=direction(),
                source_run_id=9,
            )
            storage.reserve_creative_pattern(row)
            storage.update_creative_pattern_status(row["usage_id"], "MACHINE_SCREENED")
            recent = storage.list_recent_creative_patterns(country="泰国", category="女装")
            self.assertEqual(1, len(recent))
            self.assertEqual("MACHINE_SCREENED", recent[0]["status"])

    def test_same_product_direction_rotates_after_machine_screened_contract(self) -> None:
        first = build_creative_diversity_contract(
            product_code="P1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        prior = {
            **first,
            "product_code": "P1",
            "direction_id": "SRA_TEST",
            "status": "MACHINE_SCREENED",
        }
        rerun = build_creative_diversity_contract(
            product_code="P1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[prior],
        )
        signature = lambda item: (item["persona_role"], item["scene_motif"], item["opening_action"])
        self.assertNotEqual(signature(first), signature(rerun))
        self.assertFalse(rerun["history_snapshot"]["reused_same_product_direction"])

    def test_blueprint_fields_have_consumers_and_projection_is_render_only(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        blueprint = blueprint_for(contract)
        validation = validate_complete_blueprint(blueprint, contract)
        self.assertTrue(validation["valid"], validation["issues"])
        self.assertEqual(FIELD_CONSUMERS, blueprint["field_consumers"])
        projection = video_prompt_projection(blueprint)
        self.assertIn("persona", projection)
        self.assertIn("scene", projection)
        self.assertIn("retention_hook", projection)
        self.assertIn("opening_event", projection["retention_hook"])
        self.assertNotIn("delayed_answer", projection["retention_hook"])
        self.assertNotIn("creator_motivation", projection)
        self.assertNotIn("voice_identity", projection)

    def test_blueprint_normalization_restores_code_authority_fields(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        raw = blueprint_for(contract)
        raw["viewer_relationship"] = "模型改写后的关系"
        raw["scene"] = {**raw["scene"], "location": "模型扩写后的相似场景"}

        normalized = _normalize_blueprint(raw, contract)

        self.assertEqual(contract["viewer_relationship"], normalized["viewer_relationship"])
        self.assertEqual(contract["scene_motif"], normalized["scene"]["location"])
        validation = validate_complete_blueprint(normalized, contract)
        self.assertTrue(validation["valid"], validation["issues"])

    def test_blueprint_model_provenance_partitions_cache_and_id(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        raw = blueprint_for(contract)
        sol = {
            "stage": "complete_script_blueprint",
            "route": "primary",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
        }
        old = {**sol, "model": "gpt-5.5"}
        sol_blueprint = _normalize_blueprint(raw, contract, sol)
        old_blueprint = _normalize_blueprint(raw, contract, old)

        self.assertTrue(_blueprint_cache_matches_model(sol_blueprint, sol))
        self.assertFalse(_blueprint_cache_matches_model(sol_blueprint, old))
        self.assertFalse(_blueprint_cache_matches_model(raw, sol))
        self.assertNotEqual(
            sol_blueprint["creative_blueprint_id"],
            old_blueprint["creative_blueprint_id"],
        )

    def test_audio_authority_and_release_gate(self) -> None:
        shots = assign_audio_actual(
            [
                {"shot_no": 1, "audio_hard_constraint": "NONE", "audio_preference": "SILENCE_PREFERRED"},
                {"shot_no": 2, "audio_hard_constraint": "MUST_BE_SILENT", "audio_preference": "VOICEOVER_PREFERRED"},
                {"shot_no": 3, "audio_hard_constraint": "NONE", "audio_preference": "AMBIENT_PREFERRED"},
            ],
            spoken_shots=[1, 2],
        )
        self.assertEqual(["VOICEOVER", "SILENT", "AMBIENT"], [item["audio_actual"] for item in shots])
        mixed = assign_audio_actual(
            [{"shot_no": 1, "audio_hard_constraint": "MUST_KEEP_NATURAL_SOUND"}],
            spoken_shots=[1],
        )
        self.assertEqual("VOICEOVER_WITH_NATURAL_SOUND", mixed[0]["audio_actual"])
        contract = build_creative_diversity_contract(
            product_code="P1",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        for index, shot in enumerate(shots, 1):
            shot.update(
                {
                    "shot_content": f"画面{index}",
                    "observable_action": f"动作{index}",
                    "framing": "固定机位",
                    "supported_claim_keys": ["C1"] if index == 1 else (["C2"] if index == 2 else []),
                    "voiceover_text_target_language": "ข้อความ" if index == 1 else "",
                }
            )
        quality = validate_complete_script(
            {
                "creative_blueprint": blueprint_for(contract),
                "creative_diversity_contract": contract,
                "storyboard": shots,
            }
        )
        self.assertTrue(quality["valid"], quality["issues"])
        self.assertEqual("MACHINE_SCREENED_NOT_HUMAN_APPROVED", quality["release_status"])
        self.assertEqual("PENDING", quality["judges"]["thai_native_human"])
        self.assertFalse(quality["retention_review"]["is_blocking"])

    def test_retention_review_is_soft_and_only_checks_plan_signals(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P3",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        blueprint = blueprint_for(contract)
        storyboard = [
            {
                "shot_no": 1,
                "shot_content": "人物扣到最后一颗时停手",
                "observable_action": "扣合后突然停手并把前襟转向镜头",
                "framing": "中近景",
                "audio_actual": "VOICEOVER",
                "supported_claim_keys": ["C1"],
                "carrier_mode": "WEARER_ACTIVE",
                "gaze_and_reaction": "停手后抬眼看镜头一次",
            },
            {
                "shot_no": 2,
                "shot_content": "同一动作继续带出衣长与口袋",
                "observable_action": "手沿前襟滑到口袋后自然松开",
                "framing": "中景",
                "audio_actual": "VOICEOVER_CONTINUATION",
                "supported_claim_keys": ["C1", "C2"],
                "carrier_mode": "WEARER_ACTIVE",
                "gaze_and_reaction": "看到完整轮廓后嘴角自然放松",
            },
        ]
        result = validate_complete_script(
            {
                "creative_blueprint": blueprint,
                "creative_diversity_contract": contract,
                "production_design": {"presentation_mode": "PERSON_ON_CAMERA"},
                "storyboard": storyboard,
            }
        )
        self.assertTrue(result["valid"], result["issues"])
        self.assertEqual(3, result["retention_review"]["passed_count"])
        self.assertEqual(
            "TEXT_PLAN_ONLY_NOT_RENDER_JUDGMENT",
            result["retention_review"]["scope"],
        )
        self.assertEqual([], result["warnings"])

    def test_blueprint_allows_no_forced_reaction_points(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P4",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        blueprint = blueprint_for(contract)
        blueprint["performance_flow"]["reaction_points"] = []
        result = validate_complete_blueprint(blueprint, contract)
        self.assertTrue(result["valid"], result["issues"])

    def test_static_blueprint_allows_explicit_no_person_boundary(self) -> None:
        static_direction = direction()
        static_direction["execution_reference"]["content_carrier"] = "STATIC_PRODUCT"
        contract = build_creative_diversity_contract(
            product_code="P_STATIC",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=static_direction,
            recent_usage=[],
        )
        blueprint = blueprint_for(contract)
        blueprint["retention_hook"] = {
            "opening_event": "商品静置在衣架旁，人物不出镜",
            "delayed_answer": "不含手部，近景后再看前襟细节",
            "payoff_time": "3-5s",
        }
        blueprint["performance_flow"] = {
            "entry_state": "人物不出镜",
            "behavior_motivation": f"{contract['opening_action']}，不含手部，商品保持静置展示",
            "reaction_points": [],
            "ending_state": "静物画面自然结束",
        }
        blueprint["event_design"] = {
            "event_motif": "静物观察",
            "start_state": "人物不出镜，商品已经摆放完成",
            "natural_event": f"{contract['opening_action']}，不含手部，商品在衣架旁保持静置，镜头自然推近前襟",
            "core_result_moment": "近景看清前襟细节",
            "end_state": "静物画面自然结束",
        }
        blueprint["macro_visual_passages"] = [
            {
                "passage_no": index,
                "narrative_role": role,
                "visible_process": "不出现人物或手部，静物保持在衣架旁",
                "observable_action": "镜头自然记录静物细节",
                "camera_observation": "固定近景",
                "product_visibility": "FULL",
                "supported_claim_keys": ["C1"],
            }
            for index, role in enumerate(("EVENT_ENTRY", "EVENT_PROOF", "EVENT_END"), 1)
        ]
        result = validate_complete_blueprint(blueprint, contract)
        self.assertTrue(result["valid"], result["issues"])

    def test_checklist_action_only_warns_and_never_blocks(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P5",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        blueprint = blueprint_for(contract)
        result = validate_complete_script(
            {
                "creative_blueprint": blueprint,
                "creative_diversity_contract": contract,
                "production_design": {"presentation_mode": "PERSON_ON_CAMERA"},
                "storyboard": [
                    {
                        "shot_no": 1,
                        "shot_content": "人物穿着外套",
                        "observable_action": "手指从上到下逐颗指向扣子",
                        "framing": "半身",
                        "audio_actual": "VOICEOVER",
                        "supported_claim_keys": ["C1", "C2"],
                    }
                ],
            }
        )
        self.assertTrue(result["valid"], result["issues"])
        self.assertFalse(result["retention_review"]["no_checklist_action"])
        self.assertTrue(any("逐项" in warning for warning in result["warnings"]))

    def test_complete_script_accepts_real_tail_silence_without_empty_shot(self) -> None:
        contract = build_creative_diversity_contract(
            product_code="P2",
            country="泰国",
            category="女装",
            product_type="外套",
            direction=direction(),
            recent_usage=[],
        )
        storyboard = [
            {
                "shot_no": index,
                "shot_content": f"画面{index}",
                "observable_action": f"动作{index}",
                "framing": "固定机位",
                "audio_actual": "VOICEOVER" if index == 1 else "VOICEOVER_CONTINUATION",
                "supported_claim_keys": ["C1"] if index == 1 else ["C2"],
            }
            for index in range(1, 3)
        ]
        quality = validate_complete_script(
            {
                "creative_blueprint": blueprint_for(contract),
                "creative_diversity_contract": contract,
                "storyboard": storyboard,
                "audio_plan": {
                    "silent_windows": [
                        {"start_ms": 14200, "end_ms": 15000, "duration_ms": 800}
                    ],
                    "minimum_silence_window_ms": 450,
                },
            }
        )
        self.assertTrue(quality["valid"], quality["issues"])


if __name__ == "__main__":
    unittest.main()
