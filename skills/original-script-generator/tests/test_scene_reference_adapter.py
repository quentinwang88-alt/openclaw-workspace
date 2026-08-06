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
            "SCENE_FAMILY_PROTOTYPE",
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
        self.assertEqual(card["lived_in_trace"], "桌面有一只随手放下的帆布包")
        self.assertEqual(card["background_anchors"], ["木桌", "咖啡杯"])
        self.assertEqual(card["lighting"], "窗边自然光")

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
