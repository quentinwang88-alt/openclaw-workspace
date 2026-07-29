import sys
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.prompts import build_final_strategy_prompt, build_script_prompt  # noqa: E402
from core.pipeline import OriginalScriptPipeline  # noqa: E402
from core.script_brief_builder import build_script_brief  # noqa: E402
from core.structure_execution_compiler import apply_structure_execution_plan  # noqa: E402
from core.structure_router_adapter import (  # noqa: E402
    annotate_artifact_with_structure,
    attach_contract_to_strategy,
    validate_script_contract,
)


def selection_fixture():
    contract = {
        "contract_schema_version": "structure-direction-contract-v1",
        "direction_identity": {
            "macro_family_key": "HOOK>USE_PROCESS>PROOF",
            "visual_archetype_key": "content_carrier=HAND_ONLY|continuity_mode=MULTI_CUT",
        },
        "hard_constraints": {
            "beat_sequence": ["HOOK", "USE_PROCESS", "PROOF"],
            "required_beats": ["HOOK", "USE_PROCESS", "PROOF"],
            "content_carrier": "HAND_ONLY",
            "continuity_mode": "MULTI_CUT",
            "shot_count": "UNAVAILABLE",
        },
        "soft_preferences": {},
        "unknown_constraints": ["shot_count"],
        "execution_translation": {"visual_instruction": "承载方式固定为 HAND_ONLY"},
        "evidence": {"evidence_tier": "BOOTSTRAP"},
        "provenance": {
            "selection_run_id": "SR_TEST",
            "direction_assignment_id": "SRA_TEST",
            "output_slot": "S1",
            "direction_role": "BASELINE",
        },
    }
    return {
        "selection_run_id": "SR_TEST",
        "assignments": [{"output_slot": "S1", "structure_contract": contract}],
    }


class StructureRouterOriginalIntegrationTest(unittest.TestCase):
    def test_contract_is_attached_to_strategy_and_script_brief(self):
        strategy = attach_contract_to_strategy(
            {"strategy_id": "S1", "primary_selling_point": "轻巧"},
            selection_fixture(),
            1,
        )
        brief = build_script_brief(
            product_type="发饰",
            anchor_card={},
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy=strategy,
            expression_plan={},
            structure_contract=strategy["structure_contract"],
        )
        self.assertEqual(brief["structure_contract"]["provenance"]["direction_assignment_id"], "SRA_TEST")
        plan = brief["structure_execution_plan"]
        self.assertTrue(plan["contract_applied"])
        self.assertEqual(plan["beat_sequence"], ["HOOK", "USE_PROCESS", "PROOF"])
        self.assertEqual(plan["shot_count"], 6)
        self.assertTrue(all(shot["carrier_mode"] == "HAND_ONLY" for shot in plan["shot_plan"]))
        prompt = build_script_prompt("TH", "泰语", "发饰", brief)
        self.assertIn("不要因为系统仍用 S1-S4 字段", prompt)
        self.assertIn("不得再使用统一六镜头模板", prompt)

    def test_compiled_plan_is_applied_and_validated_by_explicit_fields(self):
        strategy = attach_contract_to_strategy(
            {"strategy_id": "S1", "primary_selling_point": "轻巧"},
            selection_fixture(),
            1,
        )
        brief = build_script_brief(
            product_type="发饰",
            anchor_card={},
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy=strategy,
            expression_plan={},
            structure_contract=strategy["structure_contract"],
        )
        plan = brief["structure_execution_plan"]
        script = {
            "shot_skeleton": [
                {"shot_index": index, "time_range": "", "role": "proof", "shot_purpose": "", "proof_path": "A_result_detail_only"}
                for index in range(1, plan["shot_count"] + 1)
            ],
            "storyboard": [
                {
                    "shot_no": index,
                    "duration": "",
                    "shot_content": "手部展示并完成商品使用过程",
                    "person_action": "手持商品完成动作",
                    "spoken_line_task": "proof",
                    "task_type": "proof",
                }
                for index in range(1, plan["shot_count"] + 1)
            ],
        }
        apply_structure_execution_plan(script, plan)
        validation = validate_script_contract(script, strategy["structure_contract"])
        self.assertTrue(validation["valid"], validation)
        self.assertEqual(validation["observed"]["beat_authority"], "EXPLICIT")

    def test_surplus_review_shot_is_merged_back_into_authoritative_plan(self):
        plan = {
            "contract_applied": True,
            "shot_count": 4,
            "shot_plan": [
                {"shot_index": 1, "time_range": "0-3s", "structure_beat": "HOOK", "carrier_mode": "STATIC_PRODUCT", "continuity_group": "A", "opening_mechanism": "PRODUCT_REVEAL", "visual_task": "hook", "spoken_task_hint": "hook"},
                {"shot_index": 2, "time_range": "3-6.5s", "structure_beat": "PROOF", "carrier_mode": "STATIC_PRODUCT", "continuity_group": "A", "opening_mechanism": "", "visual_task": "proof one", "spoken_task_hint": "proof"},
                {"shot_index": 3, "time_range": "6.5-10.5s", "structure_beat": "PROOF", "carrier_mode": "STATIC_PRODUCT", "continuity_group": "A", "opening_mechanism": "", "visual_task": "proof two", "spoken_task_hint": "proof"},
                {"shot_index": 4, "time_range": "10.5-15s", "structure_beat": "ENDING", "carrier_mode": "STATIC_PRODUCT", "continuity_group": "A", "opening_mechanism": "", "visual_task": "ending", "spoken_task_hint": "decision"},
            ],
        }
        beats = ["HOOK", "PROOF", "PROOF", "PROOF", "ENDING"]
        script = {
            "proof_path": "A_result_detail_only",
            "shot_skeleton": [
                {"shot_index": index, "role": "proof", "shot_purpose": f"骨架{index}", "proof_path": "A_result_detail_only"}
                for index in range(1, 6)
            ],
            "storyboard": [
                {
                    "shot_no": index,
                    "structure_beat": beat,
                    "shot_content": f"画面{index}",
                    "person_action": "无人物",
                }
                for index, beat in enumerate(beats, 1)
            ],
        }
        apply_structure_execution_plan(script, plan)
        self.assertEqual(len(script["storyboard"]), 4)
        self.assertEqual(len(script["shot_skeleton"]), 4)
        self.assertEqual([item["structure_beat"] for item in script["storyboard"]], ["HOOK", "PROOF", "PROOF", "ENDING"])
        self.assertEqual(script["storyboard"][-1]["shot_content"], "画面5")
        self.assertIn("画面3", script["storyboard"][2]["shot_content"])
        self.assertIn("画面4", script["storyboard"][2]["shot_content"])

    def test_strategy_prompt_states_slots_are_not_fixed_directions(self):
        prompt = build_final_strategy_prompt(
            "TH",
            "泰语",
            "发饰",
            {},
            structure_direction_packages_json=selection_fixture(),
        )
        self.assertIn("S1-S4 是槽位，不是固定叙事方向", prompt)

    def test_artifact_carries_traceable_direction_id(self):
        contract = selection_fixture()["assignments"][0]["structure_contract"]
        artifact = annotate_artifact_with_structure({"content_id": "C1"}, contract)
        self.assertEqual(artifact["_structure_provenance"]["direction_assignment_id"], "SRA_TEST")

    def test_routed_variant_pool_does_not_restore_legacy_slot_direction(self):
        strategy = attach_contract_to_strategy(
            {
                "strategy_id": "S4",
                "script_role": "risk_resolution",
                "opening_angle": "先提出佩戴顾虑",
                "proof_path": "A_result_detail_only",
            },
            selection_fixture(),
            1,
        )
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pool = pipeline._build_direction_allowed_pool(strategy)
        self.assertEqual(pool["script_role"], ["risk_resolution"])
        self.assertNotIn("高惊艳首镜型", pool["opening_mode"])
        self.assertTrue(any("仅为输出槽位" in item for item in pool["extra_boundary"]))

    def test_variant_prompt_receives_authoritative_per_shot_structure(self):
        strategy = attach_contract_to_strategy(
            {"strategy_id": "S1", "script_role": "risk_resolution"},
            selection_fixture(),
            1,
        )
        brief = build_script_brief(
            product_type="发饰",
            anchor_card={},
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy=strategy,
            expression_plan={},
            structure_contract=strategy["structure_contract"],
        )
        payload = {
            "variants": [
                {
                    "variant_id": "V1",
                    "final_video_script_prompt": {
                        "shot_execution": [
                            {
                                "shot_no": index,
                                "duration": "",
                                "visual": "手部展示商品",
                                "person_action": "仅手部完成动作",
                                "product_focus": "商品细节",
                                "voiceover": "",
                            }
                            for index in range(1, brief["structure_execution_plan"]["shot_count"] + 1)
                        ]
                    },
                }
            ]
        }
        OriginalScriptPipeline._apply_structure_plan_to_variant_payload(
            payload,
            brief["structure_execution_plan"],
            strategy["structure_contract"],
        )
        shots = payload["variants"][0]["final_video_script_prompt"]["shot_execution"]
        self.assertEqual([shot["structure_beat"] for shot in shots], ["HOOK", "USE_PROCESS", "PROOF", "PROOF", "PROOF", "PROOF"])
        self.assertTrue(all(shot["carrier_mode"] == "HAND_ONLY" for shot in shots))


if __name__ == "__main__":
    unittest.main()
