import sys
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.structure_execution_compiler import compile_structure_execution_plan  # noqa: E402


def contract(beats, carrier, continuity, opening):
    return {
        "direction_identity": {"macro_family_key": ">".join(beats)},
        "hard_constraints": {
            "beat_sequence": beats,
            "content_carrier": carrier,
            "continuity_mode": continuity,
            "visual_hook_type": opening,
            "shot_count": "UNAVAILABLE",
        },
        "provenance": {"direction_assignment_id": "SRA_TEST"},
    }


class StructureExecutionCompilerTest(unittest.TestCase):
    def test_four_routed_directions_compile_to_distinct_visual_plans(self):
        contracts = [
            contract(["HOOK", "PROOF"], "WEARER_ACTIVE", "MULTI_CUT", "PRODUCT_REVEAL"),
            contract(["HOOK", "PROOF", "USE_PROCESS"], "MIXED", "CONTINUOUS_LOW_CUT", "PRODUCT_REVEAL"),
            contract(["HOOK", "PROOF", "USE_PROCESS", "PROOF"], "MIXED", "MULTI_CUT", "PERSON_REVEAL"),
            contract(["HOOK", "PROOF", "ENDING"], "STATIC_PRODUCT", "CONTINUOUS_LOW_CUT", "PRODUCT_REVEAL"),
        ]
        plans = [
            compile_structure_execution_plan(item, {"operation_policy": "result_first_process_avoid"})
            for item in contracts
        ]
        signatures = {
            (
                tuple(shot["structure_beat"] for shot in plan["shot_plan"]),
                tuple(shot["carrier_mode"] for shot in plan["shot_plan"]),
                tuple(shot["continuity_group"] for shot in plan["shot_plan"]),
                plan["opening_mechanism"],
            )
            for plan in plans
        }
        self.assertEqual(len(signatures), 4)
        self.assertEqual(plans[1]["shot_count"], 4)
        self.assertEqual(plans[2]["shot_count"], 6)
        self.assertGreaterEqual(len({shot["carrier_mode"] for shot in plans[1]["shot_plan"]}), 2)

    def test_process_forbidden_is_reported_instead_of_silently_flattened(self):
        plan = compile_structure_execution_plan(
            contract(["HOOK", "USE_PROCESS", "PROOF"], "HAND_ONLY", "MULTI_CUT", "PROCESS_REVEAL"),
            {"operation_policy": "process_forbidden"},
        )
        self.assertTrue(plan["blocking_conflicts"])
        self.assertIn("USE_PROCESS", plan["blocking_conflicts"][0])


if __name__ == "__main__":
    unittest.main()
