import json
import unittest
from collections import Counter
from pathlib import Path
import sys


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.multidim_reference_adapter import (
    legacy_execution_reference_projection,
    model_visible_reference_projection,
    select_retrieval_reference_contract,
)


def _candidate(
    video_id: str,
    *,
    base_score: float,
    carrier: str,
    scene_cluster: str,
    visual_cluster: str = "2",
    rhythm_cluster: str = "3",
    category_status: str = "EXACT_PRODUCT_TYPE",
    storyboard=None,
):
    assignments = {
        "structure": {"run_id": "prompt_only_full", "cluster_id": "7", "schema_status": "AUTHORITY", "measured_available": 1},
        "scene": {"run_id": "scene_v2", "cluster_id": scene_cluster, "schema_status": "AUTHORITY", "measured_available": 1},
        "persona_presentation": {"run_id": "persona_presentation_v1", "cluster_id": "1", "schema_status": "RETRIEVAL_AID", "measured_available": 1},
        "rhythm": {"run_id": "rhythm_v1", "cluster_id": rhythm_cluster, "schema_status": "AUTHORITY", "measured_available": 1},
        "visual_hook": {"run_id": "visual_hook_v2", "cluster_id": visual_cluster, "schema_status": "RETRIEVAL_AID", "measured_available": 1},
    }
    return {
        "video_id": video_id,
        "base_score": base_score,
        "category_match_status": category_status,
        "structure_measured_available": True,
        "case": {
            "content_carrier": carrier,
            "cat1": "女装",
            "cat2": "外套",
            "country": "泰国",
            "profile_type": "PROMPT_ONLY",
            "evidence_tier": "PROMPT_ONLY",
            "opening_logic": f"{video_id} 开场",
            "camera_logic": f"{video_id} 镜头",
            "action_logic": f"{video_id} 动作",
            "rhythm_logic": f"{video_id} 节奏",
        },
        "asset": {
            "asset_id": f"asset-{video_id}",
            "asset_version": 2,
            "review_status": "reviewed",
            "source_structure_summary": "真实结构摘要",
            "source_style_summary": "真实风格摘要",
            "entry_signature": "第一眼商品结果",
            "storyboard_json": json.dumps(storyboard or [
                {
                    "title": "开场结果",
                    "duration": "0-2秒",
                    "visual_content": f"{video_id} 商品先进入近景",
                    "camera_and_editing": "手机近景",
                    "core_function": "建立停留",
                },
                {
                    "title": "中段证明",
                    "duration": "2-8秒",
                    "visual_content": "人物自然走动，商品持续可见",
                    "camera_and_editing": "重新放置手机后拍中景",
                    "core_function": "给出证明",
                },
                {
                    "title": "自然收束",
                    "duration": "8-12秒",
                    "visual_content": "人物换一个近似角度停住，商品结果仍清楚",
                    "camera_and_editing": "直接剪切后的半身近景",
                    "core_function": "回到商品结果",
                },
            ], ensure_ascii=False),
        },
        "assignments": assignments,
        "prototype_projections": {
            "scene": {
                "status": "AVAILABLE",
                "dimension_type": "scene",
                "cluster_id": scene_cluster,
                "schema_status": "AUTHORITY",
                "scene_executability": "AVAILABLE",
                "name": "窗边生活场景",
                "measured_signal": {"camera_motion_active_pct": 95.0},
                "distinctive_anchors": {
                    "space_layout": ["窗边与普通浅色墙形成两层空间"],
                    "color_light_anchors": ["精确6500K商业灯光"],
                    "generic_fillers": ["真实感"],
                },
            },
            "rhythm": {
                "status": "AVAILABLE",
                "dimension_type": "rhythm",
                "cluster_id": rhythm_cluster,
                "production_family_id": "STEADY_MULTI_CLIP",
                "production_family_name": "稳推进",
                "production_brief": "多个手机拍摄单元稳定推进",
                "is_generic_rhythm": False,
                "measured_signal": {"shot_count_median": 2},
            },
            "persona_presentation": {
                "status": "AVAILABLE",
                "dimension_type": "persona_presentation",
                "cluster_id": "1",
                "name": "真人自然分享",
            },
            "visual_hook": {
                "status": "AVAILABLE",
                "dimension_type": "visual_hook",
                "cluster_id": visual_cluster,
                "entry_subject": "RESULT_EFFECT",
            },
        },
    }


def _context():
    return {
        "status": "AVAILABLE",
        "reason": "CASE_FIRST_CANDIDATES",
        "policy_version": "multidim-case-retrieval-v2-lightweight-spine",
        "direction_assignment_id": "DA_1",
        "structure_run_id": "prompt_only_full",
        "structure_cluster_id": "7",
        "active_runs": {
            "structure": "prompt_only_full",
            "scene": "scene_v2",
            "persona_presentation": "persona_presentation_v1",
            "rhythm": "rhythm_v1",
            "visual_hook": "visual_hook_v2",
        },
        "data_snapshot_hash": "snapshot-1",
        "candidates": [
            _candidate("wrong-carrier", base_score=80, carrier="STATIC_PRODUCT", scene_cluster="9"),
            _candidate("matched-a", base_score=45, carrier="WEARER_ACTIVE", scene_cluster="3"),
            _candidate("matched-b", base_score=44, carrier="WEARER_ACTIVE", scene_cluster="3"),
        ],
        "cooccurrence": {},
    }


class MultidimReferenceAdapterTest(unittest.TestCase):
    @staticmethod
    def _with_execution_card(candidate, parts, action="WEAR"):
        candidate["case"]["physical_action_type"] = action
        candidate["execution_card"] = {
            "schema_version": "script-execution-card-v2",
            "execution_card_id": "EXEC_" + candidate["video_id"],
            "video_id": candidate["video_id"],
            "content_carrier": candidate["case"]["content_carrier"],
            "physical_action_type": action,
            "shot_count": 5,
            "parts": {
                name: {
                    "status": "AVAILABLE" if name in parts else "UNAVAILABLE",
                    "part": name,
                    "shots": [],
                }
                for name in ("opening", "proof", "use_process", "ending")
            },
            "available_parts": list(parts),
        }
        return candidate

    def test_scene_usage_prefers_case_with_use_process(self):
        without_use = self._with_execution_card(
            _candidate("no-use", base_score=90, carrier="WEARER_ACTIVE", scene_cluster="3"),
            ("opening", "proof", "ending"),
        )
        with_use = self._with_execution_card(
            _candidate("with-use", base_score=10, carrier="WEARER_ACTIVE", scene_cluster="3"),
            ("opening", "use_process", "ending"),
        )
        context = _context()
        context["candidates"] = [without_use, with_use]
        contract = select_retrieval_reference_contract(
            context,
            creative_contract={"carrier_mode": "WEARER_ACTIVE"},
            content_bundle={
                "proof_execution_intent": {
                    "proof_subject": "SCENE_USAGE",
                    "preferred_parts": ["opening", "use_process"],
                    "required_any_parts": ["use_process"],
                    "preferred_action_tokens": ["WEAR", "WALK"],
                    "hard_required_part_match": True,
                }
            },
            requested_hook_id="GENERAL_PRODUCT_SHARE",
        )
        self.assertEqual("with-use", contract["primary_case"]["video_id"])
        self.assertTrue(
            contract["primary_case"]["proof_execution_match"]["required_part_match"]
        )

    def test_scene_usage_returns_unavailable_when_no_case_has_use_process(self):
        candidate = self._with_execution_card(
            _candidate("no-use", base_score=90, carrier="WEARER_ACTIVE", scene_cluster="3"),
            ("opening", "proof", "ending"),
        )
        context = _context()
        context["candidates"] = [candidate]
        contract = select_retrieval_reference_contract(
            context,
            creative_contract={"carrier_mode": "WEARER_ACTIVE"},
            content_bundle={
                "proof_execution_intent": {
                    "proof_subject": "SCENE_USAGE",
                    "preferred_parts": ["use_process"],
                    "required_any_parts": ["use_process"],
                    "preferred_action_tokens": ["WEAR"],
                    "hard_required_part_match": True,
                }
            },
            requested_hook_id="GENERAL_PRODUCT_SHARE",
        )
        self.assertEqual("UNAVAILABLE", contract["status"])
        self.assertEqual(
            "NO_EXECUTION_CASE_FOR_PROOF_INTENT", contract["fallback_reason"]
        )
    def test_same_macro_family_is_disclosed_support_when_exact_cluster_is_empty(self):
        candidate = _candidate(
            "family-support", base_score=0, carrier="WEARER_ACTIVE",
            scene_cluster="3",
        )
        candidate["assignments"]["structure"] = {
            "run_id": "structure_script_v2",
            "cluster_id": "8",
            "schema_status": "AUTHORITY",
            "measured_available": 1,
        }
        candidate["same_video_dimension_bundle"] = {
            "video_id": "family-support",
            "structure_family": "HOOK>PROOF>ENDING",
        }
        context = {
            "status": "AVAILABLE",
            "direction_assignment_id": "DA_FAMILY",
            "structure_run_id": "structure_script_v2",
            "structure_cluster_id": "99",
            "requested_structure_family": "HOOK>PROOF>ENDING",
            "active_runs": {"structure": "structure_script_v2"},
            "candidates": [candidate],
        }
        contract = select_retrieval_reference_contract(
            context,
            creative_contract={"carrier_mode": "WEARER_ACTIVE"},
            content_bundle={},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
        )

        self.assertEqual("AVAILABLE", contract["status"])
        self.assertEqual(
            "SCRIPT_EXECUTION_CARD_SAME_VIDEO_MACRO_FAMILY_FALLBACK",
            contract["selection_mode"],
        )
        self.assertEqual(
            "MACRO_FAMILY_SUPPORT",
            contract["primary_execution_card"]["eligibility_status"],
        )

    def test_single_take_exact_case_yields_to_shot_rich_same_family_case(self):
        single_take = self._with_execution_card(
            _candidate(
                "single-take-exact", base_score=200,
                carrier="WEARER_ACTIVE", scene_cluster="3",
            ),
            ("opening",),
        )
        single_take["execution_card"]["shot_count"] = 1
        single_take["same_video_dimension_bundle"] = {
            "video_id": "single-take-exact",
            "structure_family": "HOOK>PROOF>ENDING",
            "rhythm_family": "STEADY_SINGLE_TAKE",
        }
        rich_family = self._with_execution_card(
            _candidate(
                "rich-family", base_score=5,
                carrier="WEARER_ACTIVE", scene_cluster="3",
            ),
            ("opening", "proof", "ending"),
        )
        rich_family["assignments"]["structure"] = {
            "run_id": "prompt_only_full",
            "cluster_id": "8",
            "schema_status": "AUTHORITY",
            "measured_available": 1,
        }
        rich_family["execution_card"]["shot_count"] = 5
        rich_family["same_video_dimension_bundle"] = {
            "video_id": "rich-family",
            "structure_family": "HOOK>PROOF>ENDING",
            "rhythm_family": "STEADY_MULTI_CLIP",
        }
        context = _context()
        context["requested_structure_family"] = "HOOK>PROOF>ENDING"
        context["candidates"] = [single_take, rich_family]

        contract = select_retrieval_reference_contract(
            context,
            creative_contract={"carrier_mode": "WEARER_ACTIVE"},
            content_bundle={},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
        )

        self.assertEqual("AVAILABLE", contract["status"])
        self.assertEqual("rich-family", contract["primary_case"]["video_id"])
        self.assertEqual(
            "SCRIPT_EXECUTION_CARD_SAME_VIDEO_MACRO_FAMILY_FALLBACK",
            contract["selection_mode"],
        )
        self.assertEqual(
            "ELIGIBLE",
            contract["primary_case"]["shot_richness_match"]["status"],
        )

    def test_selected_case_projects_to_legacy_execution_reference(self):
        contract = select_retrieval_reference_contract(
            _context(),
            creative_contract={"carrier_mode": "WEARER_ACTIVE"},
            content_bundle={},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
        )

        projected = legacy_execution_reference_projection(contract)

        self.assertEqual("VIDEO_REFERENCED", projected["reference_status"])
        self.assertTrue(projected["execution_card_id"])
        self.assertEqual(
            contract["primary_execution_card"]["video_id"],
            projected["_meta"]["video_id"],
        )
        self.assertTrue(projected["camera_grammar"])
        self.assertTrue(
            any("手机" in item for item in projected["camera_grammar"]),
            projected["camera_grammar"],
        )
        # Source product/story nouns stay outside the legacy execution-only
        # bridge; only measured framing/editing is restored.
        self.assertFalse(
            any("商品先进入近景" in item for item in projected["camera_grammar"])
        )

    def test_v3_accepts_derived_script_and_preserves_same_video_dimensions(self):
        derived = _candidate(
            "derived-complete", base_score=0, carrier="WEARER_ACTIVE",
            scene_cluster="3",
        )
        verified = _candidate(
            "verified-incomplete", base_score=0, carrier="WEARER_ACTIVE",
            scene_cluster="3",
        )
        for candidate, tier, raw_verified, parts in (
            (derived, "VIDEO_DERIVED_SCRIPT", False, ("opening", "proof", "use_process", "ending")),
            (verified, "VIDEO_INDEPENDENT", True, ("opening", "proof")),
        ):
            candidate["case"].update({
                "evidence_tier": tier,
                "semantic_source": "VIDEO_RECONSTRUCTION_ASSET",
                "raw_video_verified": raw_verified,
                "physical_action_type": "WEAR",
                "creator_id": candidate["video_id"],
                "template_group_id": candidate["video_id"],
            })
            candidate["same_video_dimension_bundle"] = {
                "video_id": candidate["video_id"],
                "structure_family": "TRY_ON_PROOF",
                "scene_family": "HOME_LIVING",
                "rhythm_family": "STEADY_MULTI_CLIP",
                "persona_family": "ON_CAMERA_TRY_ON",
                "visual_hook_family": "RESULT_EFFECT",
            }
            card_parts = {
                name: {
                    "status": "AVAILABLE" if name in parts else "UNAVAILABLE",
                    "part": name,
                    "shots": [],
                }
                for name in ("opening", "proof", "use_process", "ending")
            }
            candidate["execution_card"] = {
                "schema_version": "script-execution-card-v2",
                "execution_card_id": "EXEC_" + candidate["video_id"],
                "video_id": candidate["video_id"],
                "content_carrier": "WEARER_ACTIVE",
                "physical_action_type": "WEAR",
                "shot_count": 5,
                "parts": card_parts,
                "available_parts": list(parts),
                "_meta": {
                    "evidence_tier": tier,
                    "semantic_source": "VIDEO_RECONSTRUCTION_ASSET",
                    "raw_video_verified": raw_verified,
                },
            }
        context = {
            "status": "AVAILABLE",
            "reason": "SCRIPT_EXECUTION_CARD_CANDIDATES",
            "direction_assignment_id": "DA_V3",
            "structure_run_id": "legacy-run",
            "structure_cluster_id": "7",
            "active_runs": {
                "structure": "structure_script_v2",
                "scene": "scene_script_v2",
                "rhythm": "rhythm_script_v2",
                "persona_presentation": "persona_presentation_script_v2",
                "visual_hook": "visual_hook_script_v3",
                "script_execution": "script_execution_card_v2",
                "speech_hook": "speech_hook_candidate_v1",
            },
            "data_snapshot_hash": "v3-snapshot",
            "candidates": [verified, derived],
            "speech_hook_pool": [{
                "cluster_id": "4",
                "prototype_name": "DISCOVERY · PROOF",
                "opening_move": "DISCOVERY",
                "relation_mode": "PEER_CHAT",
                "argument_order": "DISCOVERY_PROOF",
                "ending_pattern": "LIGHT_CLOSE",
                "target_language_examples": [{
                    "language": "th",
                    "rhetorical_example": "ตัวอย่างเชิงโวหาร",
                }],
            }],
        }
        contract = select_retrieval_reference_contract(
            context,
            creative_contract={"carrier_mode": "WEARER_ACTIVE"},
            content_bundle={},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
        )
        self.assertEqual(contract["status"], "AVAILABLE")
        self.assertEqual(
            contract["primary_execution_card"]["video_id"],
            "derived-complete",
        )
        self.assertFalse(
            contract["primary_execution_card"]["_meta"]["raw_video_verified"]
        )
        visible = model_visible_reference_projection(contract)
        primary = visible["primary_real_case"]
        self.assertEqual(
            primary["same_video_dimension_bundle"]["video_id"],
            "derived-complete",
        )
        self.assertEqual(
            primary["execution_card"]["parts"]["use_process"]["status"],
            "AVAILABLE",
        )
        self.assertNotIn("speech_hook_pool", visible)
        self.assertEqual(
            contract["authority_boundary"]["speech_hook_pool"],
            "RHETORIC_AND_TARGET_LANGUAGE_EXAMPLE_ONLY",
        )

    def test_case_first_selection_respects_carrier_and_selected_scene(self):
        usage = Counter()
        contract = select_retrieval_reference_contract(
            _context(),
            creative_contract={
                "carrier_mode": "WEARER_ACTIVE",
                "scene_reference_contract": {
                    "primary_scene_cluster_id": 3,
                    "supporting_scene_cluster_ids": [4],
                },
            },
            content_bundle={"content_bundle_id": "CB_1"},
            requested_hook_id="DETAIL_SURPRISE",
            used_video_ids=usage,
        )
        self.assertEqual(contract["status"], "AVAILABLE")
        self.assertEqual(contract["primary_case"]["video_id"], "matched-a")
        self.assertEqual(contract["supporting_case"]["video_id"], "matched-b")
        self.assertTrue(contract["primary_case"]["scene_match"])
        self.assertEqual(
            contract["scene_alignment_status"], "MATCHED_SELECTED_SCENE"
        )
        self.assertEqual(usage["matched-a"], 1)

    def test_usage_rotates_primary_real_case_without_randomness(self):
        usage = Counter({"matched-a": 1})
        contract = select_retrieval_reference_contract(
            _context(),
            creative_contract={
                "carrier_mode": "WEARER_ACTIVE",
                "scene_reference_contract": {"primary_scene_cluster_id": 3},
            },
            content_bundle={},
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            used_video_ids=usage,
        )
        self.assertEqual(contract["primary_case"]["video_id"], "matched-b")

    def test_model_projection_only_exposes_lightweight_execution_spine(self):
        contract = select_retrieval_reference_contract(
            _context(),
            creative_contract={
                "carrier_mode": "WEARER_ACTIVE",
                "scene_reference_contract": {"primary_scene_cluster_id": 3},
            },
            content_bundle={},
            requested_hook_id="DETAIL_SURPRISE",
        )
        visible = model_visible_reference_projection(contract)
        self.assertEqual(visible["status"], "AVAILABLE")
        primary = visible["primary_real_case"]
        self.assertNotIn("storyboard", primary)
        self.assertNotIn("dimension_references", primary)
        spine = primary["reference_execution_spine"]
        self.assertEqual(spine["available_parts"], ["opening", "proof", "ending"])
        self.assertTrue(spine["reference_spine_id"].startswith("RSP_"))
        self.assertEqual(
            spine["semantic_boundary"], "EXECUTION_ONLY_NO_SOURCE_CONTENT"
        )
        self.assertNotIn("visual_action", spine["parts"]["opening"])
        self.assertNotIn("shot_function", spine["parts"]["opening"])
        scene = primary["matched_scene_realism"]
        self.assertEqual(
            scene["distinctive_anchors"]["color_light_anchors"],
            ["精确商业灯光"],
        )
        self.assertNotIn("generic_fillers", scene["distinctive_anchors"])

    def test_known_different_family_cannot_become_primary(self):
        context = _context()
        context["candidates"] = [
            _candidate(
                "wrong-family",
                base_score=200,
                carrier="WEARER_ACTIVE",
                scene_cluster="3",
                category_status="KNOWN_DIFFERENT_FAMILY",
            )
        ]
        contract = select_retrieval_reference_contract(
            context,
            creative_contract={"carrier_mode": "WEARER_ACTIVE"},
            content_bundle={},
            requested_hook_id="DETAIL_SURPRISE",
        )
        self.assertEqual(contract["status"], "UNAVAILABLE")
        self.assertEqual(contract["fallback_reason"], "NO_ELIGIBLE_PRIMARY_CASE")
        self.assertEqual(
            contract["candidate_diagnostics"]["eligibility_distribution"],
            {"QUARANTINED": 1},
        )

    def test_metadata_storyboard_carrier_conflict_is_quarantined(self):
        context = _context()
        context["candidates"] = [
            _candidate(
                "fake-static",
                base_score=200,
                carrier="STATIC_PRODUCT",
                scene_cluster="3",
                storyboard=[
                    {
                        "title": "人物试穿",
                        "visual_content": "女性模特穿上外套后全身走动",
                        "camera_and_editing": "全身中景",
                        "core_function": "上身结果",
                    },
                    {
                        "title": "侧身展示",
                        "visual_content": "人物侧身转身，继续展示穿着结果",
                        "camera_and_editing": "半身中景",
                        "core_function": "穿着证明",
                    },
                ],
            )
        ]
        contract = select_retrieval_reference_contract(
            context,
            creative_contract={"carrier_mode": "STATIC_PRODUCT"},
            content_bundle={},
            requested_hook_id="DETAIL_SURPRISE",
        )
        self.assertEqual(contract["status"], "UNAVAILABLE")
        reasons = contract["candidate_diagnostics"]["reason_distribution"]
        self.assertEqual(
            reasons["SOURCE_METADATA_STORYBOARD_CARRIER_CONFLICT"], 1
        )

    def test_unavailable_context_is_explicit_no_effect(self):
        contract = select_retrieval_reference_contract(
            {"status": "UNAVAILABLE", "reason": "RDS_READ_FAILED"},
            creative_contract={},
            content_bundle={},
            requested_hook_id="",
        )
        self.assertEqual(contract["status"], "UNAVAILABLE")
        self.assertEqual(contract["selection_mode"], "NO_EFFECT")
        self.assertEqual(contract["fallback_reason"], "RDS_READ_FAILED")


if __name__ == "__main__":
    unittest.main()
