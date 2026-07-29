from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from core.complete_script_v3 import (
    COMPLETE_BLUEPRINT_SCHEMA_VERSION,
    FIELD_CONSUMERS,
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
