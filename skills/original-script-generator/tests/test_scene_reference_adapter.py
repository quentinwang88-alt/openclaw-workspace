import unittest

from core.scene_reference_adapter import (
    build_contexts_from_matrix_rows,
    load_scene_policy,
    scene_family_for_motif,
    scene_reference_contract_for_family,
)


def _direction(direction_id="DA_1", cluster=7):
    return {
        "direction_assignment_id": direction_id,
        "cluster_id": cluster,
        "structure_contract": {
            "provenance": {"source_run_id": "prompt_only_full", "cluster_id": cluster},
        },
    }


class SceneReferenceAdapterTest(unittest.TestCase):
    def test_public_exit_and_walkway_candidates_map_to_street_outing(self):
        self.assertEqual(
            scene_family_for_motif("书店出口旁的暖色书架过道"),
            "STREET_OUTING",
        )
        self.assertEqual(
            scene_family_for_motif("商场连廊靠窗的自然光休息区"),
            "STREET_OUTING",
        )
        self.assertEqual(
            scene_family_for_motif("咖啡厅靠窗的质感座位区域"),
            "CAFE_DINING",
        )

    def test_policy_maps_every_discovered_scene_cluster(self):
        policy = load_scene_policy()
        self.assertEqual(set(policy["clusters"].keys()), {str(index) for index in range(39)})

    def test_active_family_gets_positive_only_bonus_and_compact_contract(self):
        policy = load_scene_policy()
        rows = [
            {"structure_run": "prompt_only_full", "structure_cluster": 7, "scene_cluster": 21, "count": 6, "scene_name": "咖啡店桌边", "sample_video_ids": ["v1", "v2"]},
            {"structure_run": "prompt_only_full", "structure_cluster": 7, "scene_cluster": 10, "count": 1, "scene_name": "居家窗边", "sample_video_ids": ["v3"]},
            {"structure_run": "prompt_only_full", "structure_cluster": 99, "scene_cluster": 10, "count": 40, "scene_name": "居家窗边", "sample_video_ids": []},
        ]
        context = build_contexts_from_matrix_rows([_direction()], rows, policy)["DA_1"]
        cafe = scene_reference_contract_for_family(context, "CAFE_DINING")
        home = scene_reference_contract_for_family(context, "HOME_ROUTINE")
        self.assertEqual(cafe["status"], "AVAILABLE")
        self.assertGreater(cafe["matrix_bonus"], 0)
        self.assertEqual(cafe["approved_realism_anchors"], ["靠窗座位与自然侧光", "桌面保留少量真实使用痕迹"])
        self.assertEqual(home["status"], "SOFT_ONLY")
        self.assertEqual(home["matrix_bonus"], 0)
        self.assertEqual(home["approved_realism_anchors"], [])

    def test_missing_matrix_uses_non_blocking_curated_motif_fallback(self):
        policy = load_scene_policy()
        context = build_contexts_from_matrix_rows([_direction()], [], policy)["DA_1"]
        contract = scene_reference_contract_for_family(context, "OFFICE_WORKBREAK")
        self.assertEqual(contract["status"], "SOFT_ONLY")
        self.assertEqual(contract["selection_mode"], "CURATED_MOTIF_FALLBACK")
        self.assertEqual(contract["approved_realism_anchors"], [])
        self.assertEqual(contract["matrix_bonus"], 0)
        self.assertEqual(contract["scene_execution_card"]["status"], "AVAILABLE")

    def test_unmatched_structure_can_use_global_family_prototype_without_bonus(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 99,
            "scene_cluster": 21,
            "count": 8,
            "scene_name": "咖啡馆靠窗座位",
            "dominant_location": "咖啡馆",
            "sample_video_ids": [],
        }]
        context = build_contexts_from_matrix_rows([_direction()], rows, policy)["DA_1"]
        contract = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅靠窗的质感座位区域",
        )
        self.assertEqual(contract["selection_mode"], "SCENE_FAMILY_PROTOTYPE_FALLBACK")
        self.assertEqual(contract["matrix_bonus"], 0)
        self.assertEqual(
            contract["scene_execution_card"]["source_quality"],
            "CURATED_MOTIF_FALLBACK",
        )

    def test_weak_active_family_never_reaches_generation_prompt(self):
        context = {
            "status": "AVAILABLE",
            "policy_version": "scene-routing-policy-v1",
            "scene_run_id": "scene_v2",
            "source_run_id": "prompt_only_full",
            "structure_cluster": 13,
            "families": {
                "CAFE_DINING": {
                    "routing_status": "ACTIVE",
                    "support_level": "WEAK",
                    "support_count": 1,
                    "lift": 2.04,
                    "matrix_bonus": 0,
                    "approved_realism_anchors": ["靠窗座位与自然侧光"],
                }
            },
        }
        contract = scene_reference_contract_for_family(context, "CAFE_DINING")
        self.assertEqual(contract["status"], "SOFT_ONLY")
        self.assertEqual(contract["matrix_bonus"], 0)
        self.assertEqual(contract["approved_realism_anchors"], [])

    def test_soft_pair_still_exposes_advisory_scene_execution_card(self):
        policy = load_scene_policy()
        rows = [
            {
                "structure_run": "prompt_only_full",
                "structure_cluster": 7,
                "scene_cluster": 21,
                "count": 1,
                "scene_name": "咖啡馆靠窗座位",
                "dominant_location": "咖啡馆",
                "top_props": '["木桌", "咖啡杯", "发夹"]',
                "realism_anchor_pool": '["桌面有一只随手放下的帆布包"]',
                "lighting_distribution": '{"UNKNOWN": 4, "窗边自然光": 3}',
                "sample_video_ids": ["v1"],
            },
        ]
        context = build_contexts_from_matrix_rows([_direction()], rows, policy)["DA_1"]
        contract = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅靠窗的质感座位区域",
            presentation="PERSON_ON_CAMERA",
        )
        self.assertEqual(contract["status"], "SOFT_ONLY")
        card = contract["scene_execution_card"]
        self.assertEqual(card["status"], "AVAILABLE")
        self.assertIn("手机", card["space"]["phone_placement"])
        self.assertEqual(card["source_quality"], "CURATED_MOTIF_FALLBACK")
        self.assertEqual(card["background_anchors"], [])
        self.assertNotIn("木桌", card["visual_scene_recipe"]["material_palette"])
        self.assertEqual(
            card["provenance"]["fallback_reason"],
            "NO_COMPATIBLE_SINGLE_OBSERVATION",
        )

    def test_observed_scene_card_does_not_mix_prototype_anchors(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 7,
            "scene_cluster": 21,
            "count": 4,
            "scene_name": "咖啡馆靠窗座位",
            "dominant_ambience": "原型暖色氛围",
            "top_props": '["原型木桌", "原型咖啡杯"]',
            "realism_anchor_pool": '["原型帆布包"]',
            "lighting_distribution": '{"窗边自然光": 4}',
            "sample_video_ids": ["v1"],
        }]
        observations = {
            "v1": {
                "video_id": "v1",
                "country": "泰国",
                "cat1": "配饰",
                "location": "咖啡馆",
                "lighting": "柔和窗边自然光",
                "ambience": "暖木、安静、有人使用过的桌面",
                "props_normalized": ["观察桌边", "玻璃杯"],
                "realism_anchors": ["椅背搭着一只日常包"],
                "confidence": 0.94,
            }
        }
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy, observations
        )["DA_1"]
        contract = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅靠窗座位",
            country="泰国",
            category="配饰",
        )
        card = contract["scene_execution_card"]
        self.assertEqual(card["background_anchors"], ["观察桌边", "玻璃杯"])
        self.assertNotIn("原型木桌", card["background_anchors"])
        self.assertEqual(card["aesthetic_anchors"], ["暖木、安静、有人使用过的桌面"])
        self.assertEqual(card["situation_tags"], [])
        self.assertIn(
            "暖木、安静、有人使用过的桌面",
            card["visual_scene_recipe"]["material_palette"],
        )
        self.assertEqual(
            card["visual_scene_recipe"]["lighting_texture"], "柔和窗边自然光"
        )
        self.assertEqual(
            card["visual_scene_recipe"]["lived_in_detail"], "椅背搭着一只日常包"
        )
        self.assertEqual(card["coherence_key"], "OBSERVED:v1")

    def test_operational_ambience_label_is_not_exposed_as_aesthetic(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 7,
            "scene_cluster": 21,
            "count": 4,
            "scene_name": "办公室休息角",
            "dominant_location": "办公室",
            "dominant_ambience": "WORK_BREAK",
            "top_props": '["浅木收纳柜", "玻璃水杯"]',
            "realism_anchor_pool": '["矮柜边放着随身包"]',
            "lighting_distribution": '{"NATURAL_DAY": 4}',
            "sample_video_ids": [],
        }]
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy
        )["DA_1"]
        contract = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="办公室公共休息区靠窗位置",
        )
        card = contract["scene_execution_card"]
        self.assertEqual(card["situation_tags"], ["CAFE_DINING"])
        self.assertEqual(card["aesthetic_anchors"], [])
        self.assertNotIn(
            "WORK_BREAK", card["visual_scene_recipe"]["material_palette"]
        )
        self.assertEqual(card["visual_scene_recipe"]["material_palette"], "")
        self.assertIn(
            "办公室公共休息区靠窗位置",
            card["visual_scene_recipe"]["space_relationship"],
        )
        self.assertEqual(
            card["visual_scene_recipe"]["lighting_texture"],
            "现场已有自然光或普通室内光",
        )

    def test_prototype_representative_case_can_supply_same_source_observation(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 7,
            "scene_cluster": 21,
            "count": 4,
            "scene_name": "咖啡馆靠窗座位",
            "scene_description": "咖啡馆普通靠窗座位",
            "representative_cases": '[{"video_id": "prototype-v1", "dist": 0}]',
            "sample_video_ids": [],
        }]
        observations = {
            "prototype-v1": {
                "video_id": "prototype-v1",
                "country": "泰国",
                "cat1": "配饰",
                "location": "咖啡馆",
                "lighting": "柔和窗边自然光",
                "ambience": "WORK_BREAK",
                "props_normalized": ["木桌", "透明玻璃杯"],
                "realism_anchors": ["椅背搭着日常肩包"],
                "confidence": 0.96,
            }
        }
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy, observations
        )["DA_1"]
        card = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅靠窗座位",
            country="泰国",
            category="配饰",
        )["scene_execution_card"]
        self.assertEqual(card["coherence_key"], "OBSERVED:prototype-v1")
        self.assertEqual(card["background_anchors"], ["木桌", "透明玻璃杯"])
        self.assertEqual(
            card["visual_scene_recipe"]["material_palette"],
            "木桌；透明玻璃杯",
        )

    def test_richer_target_observation_beats_empty_higher_confidence_sample(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 7,
            "scene_cluster": 21,
            "count": 4,
            "scene_name": "咖啡馆靠窗座位",
            "sample_video_ids": ["empty-v1", "rich-v2"],
        }]
        observations = {
            "empty-v1": {
                "video_id": "empty-v1", "country": "泰国", "cat1": "配饰",
                "lighting": "OTHER", "ambience": "WORK_BREAK", "confidence": 0.99,
            },
            "rich-v2": {
                "video_id": "rich-v2", "country": "泰国", "cat1": "配饰",
                "lighting": "柔和窗边自然光", "ambience": "WORK_BREAK",
                "props_normalized": ["木桌"],
                "realism_anchors": ["椅背搭着日常肩包"], "confidence": 0.80,
            },
        }
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy, observations
        )["DA_1"]
        card = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅靠窗座位",
            country="泰国",
            category="配饰",
        )["scene_execution_card"]
        self.assertEqual(card["coherence_key"], "OBSERVED:rich-v2")

    def test_daylight_need_rejects_night_observation_and_uses_complete_fallback(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 7,
            "scene_cluster": 21,
            "count": 4,
            "scene_name": "咖啡馆靠窗座位",
            "sample_video_ids": ["night-v1"],
        }]
        observations = {
            "night-v1": {
                "video_id": "night-v1",
                "country": "泰国",
                "cat1": "配饰",
                "cat2": "头巾",
                "location": "咖啡馆",
                "lighting": "NIGHT_AMBIENT",
                "props_normalized": ["深木桌"],
                "realism_anchors": ["桌边放着随身包"],
                "confidence": 0.98,
            }
        }
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy, observations
        )["DA_1"]
        card = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅入口内侧的自然光过渡区",
            country="泰国",
            category="配饰",
            product_type="头巾",
            scene_request={
                "canonical_product_type": "headscarf",
                "scene_intent": "DAYTIME_USE",
                "time_light_need": "DAYLIGHT",
                "capture_mode": "CREATOR_SELF_SHOT",
            },
        )["scene_execution_card"]
        self.assertEqual(card["source_quality"], "CURATED_MOTIF_FALLBACK")
        self.assertTrue(card["coherence_key"].startswith("CURATED:"))
        self.assertNotIn("NIGHT", card["lighting"])
        self.assertEqual(card["scene_request"]["time_light_need"], "DAYLIGHT")

    def test_exact_product_type_observation_wins_within_same_category(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 7,
            "scene_cluster": 21,
            "count": 4,
            "scene_name": "咖啡馆靠窗座位",
            "sample_video_ids": ["generic-v1", "head-v2"],
        }]
        observations = {
            "generic-v1": {
                "video_id": "generic-v1", "country": "泰国",
                "cat1": "配饰", "cat2": "丝巾", "location": "咖啡馆",
                "lighting": "NATURAL_DAY", "confidence": 0.99,
            },
            "head-v2": {
                "video_id": "head-v2", "country": "泰国",
                "cat1": "配饰", "cat2": "头巾", "location": "咖啡馆",
                "lighting": "NATURAL_DAY", "confidence": 0.80,
            },
        }
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy, observations
        )["DA_1"]
        card = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅靠窗座位",
            country="泰国",
            category="配饰",
            product_type="头巾",
        )["scene_execution_card"]
        self.assertEqual(card["coherence_key"], "OBSERVED:head-v2")
        self.assertEqual(
            card["source_quality"], "TARGET_MARKET_PRODUCT_TYPE_SCENE_TAG"
        )

    def test_known_incompatible_subtype_uses_coherent_curated_fallback(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full",
            "structure_cluster": 7,
            "scene_cluster": 21,
            "count": 4,
            "scene_name": "咖啡馆靠窗座位",
            "sample_video_ids": ["earring-v1"],
        }]
        observations = {
            "earring-v1": {
                "video_id": "earring-v1", "country": "泰国",
                "cat1": "配饰", "cat2": "耳饰", "location": "咖啡馆",
                "lighting": "NATURAL_DAY", "confidence": 0.99,
            }
        }
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy, observations
        )["DA_1"]
        card = scene_reference_contract_for_family(
            context,
            "CAFE_DINING",
            scene_motif="咖啡厅靠窗座位",
            country="泰国",
            category="配饰",
            product_type="头巾",
        )["scene_execution_card"]
        self.assertEqual(card["source_quality"], "CURATED_MOTIF_FALLBACK")
        self.assertTrue(card["coherence_key"].startswith("CURATED:"))

    def test_observed_card_uses_observed_location_not_curated_location(self):
        policy = load_scene_policy()
        rows = [{
            "structure_run": "prompt_only_full", "structure_cluster": 7,
            "scene_cluster": 21, "count": 4, "scene_name": "咖啡馆",
            "sample_video_ids": ["v1"],
        }]
        observations = {"v1": {
            "video_id": "v1", "country": "泰国", "cat1": "配饰",
            "location": "咖啡馆", "lighting": "NATURAL_DAY",
            "props_normalized": ["木桌"], "confidence": 0.9,
        }}
        context = build_contexts_from_matrix_rows(
            [_direction()], rows, policy, observations
        )["DA_1"]
        card = scene_reference_contract_for_family(
            context, "CAFE_DINING",
            scene_motif="精品酒店大堂的暖色休息区",
            country="泰国", category="配饰",
        )["scene_execution_card"]
        self.assertEqual(card["space"]["location"], "咖啡馆")
        self.assertNotIn(
            "精品酒店", card["visual_scene_recipe"]["space_relationship"]
        )

    def test_derived_direction_can_use_same_beat_scene_fallback_without_bonus(self):
        policy = load_scene_policy()
        direction = _direction("DA_V2", cluster=2)
        direction["structure_contract"]["provenance"]["source_run_id"] = "v2_final"
        direction["macro_family_key"] = "HOOK>PROOF"
        rows = [
            {
                "structure_run": "prompt_only_full",
                "structure_cluster": 1,
                "dominant_beat_sequence": '["HOOK", "PROOF"]',
                "scene_cluster": 21,
                "count": 6,
                "scene_name": "咖啡馆靠窗座位",
                "sample_video_ids": [],
            }
        ]
        context = build_contexts_from_matrix_rows([direction], rows, policy)["DA_V2"]
        self.assertEqual(context["fallback_mode"], "MACRO_FAMILY_FALLBACK")
        contract = scene_reference_contract_for_family(
            context, "CAFE_DINING", scene_motif="咖啡厅靠窗的质感座位区域"
        )
        self.assertEqual(contract["selection_mode"], "MACRO_FAMILY_FALLBACK")
        self.assertEqual(contract["matrix_bonus"], 0)
        self.assertEqual(contract["scene_execution_card"]["status"], "AVAILABLE")


if __name__ == "__main__":
    unittest.main()
