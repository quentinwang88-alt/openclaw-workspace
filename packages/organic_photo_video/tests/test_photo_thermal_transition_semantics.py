"""Deterministic semantic QA verdicts for the daily hot→cold transition line."""
from __future__ import annotations

import copy
from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.photo_layering_qa import (
    LAYER_COUNT_MISMATCH, LAYER_ORDER_MISMATCH, LAYER_SOURCE_CAMERA_MISMATCH,
    LAYER_SOURCE_IDENTITY_MISMATCH, QA_SCHEMA_INCOMPLETE,
)
from services.photo_content_planner import load_planning_policy
from services.photo_theme import resolve_photo_theme
from services.photo_thermal_transition_contract import (
    BASE_OUTFIT_DRIFT, CONTEXT_ORDER_MISMATCH, TEMPERATURE_VALUE_UNSOURCED,
    TRANSITION_NOT_VISIBLE, UNKNOWN_THERMAL_CONTEXT,
)
from services.photo_thermal_transition_flow import build_thermal_transition_content_plan
from services.photo_thermal_transition_qa import (
    THERMAL_TRANSITION_QA_SCHEMA, ThermalTransitionQAError,
    evaluate_thermal_transition_qa, failed_roles_from_thermal_qa,
    thermal_qa_as_alignment, thermal_transition_qa_markdown,
    thermal_transition_qa_note_zh,
)
from config.loader import load_content_recipes


RECIPE_ID = "PHOTO_TH_THERMAL_TRANSITION_V1"
ROLES = ("base", "mid", "outer")
CONTEXTS = {"base": "outdoor_hot", "mid": "transit_cool", "outer": "office_cold"}
STACKS = {
    "base": ["core"],
    "mid": ["core", "shirt"],
    "outer": ["core", "shirt", "blazer"],
}
ITEMS = {
    "base": ["breathable_top", "trousers", "loafer"],
    "mid": ["breathable_top", "cotton_shirt", "trousers", "loafer"],
    "outer": ["breathable_top", "cotton_shirt", "light_blazer", "trousers", "loafer"],
}
VARIABLES = {
    "transition_key": "outdoor_bts_office", "thermal_sensitivity": "normal",
    "dress_code": "office", "style_series": "minimal_city",
    "temperature_label_mode": "QUALITATIVE",
}


def _fixture():
    recipe = next(
        item for item in load_content_recipes() if item.recipe_id == RECIPE_ID
    )
    spec = dict(recipe.recipe_spec_json or {})
    plan = build_thermal_transition_content_plan(
        record_id="thermal-qa", recipe_id=RECIPE_ID, recipe_spec=spec,
        policy=load_planning_policy(RECIPE_ID),
        theme=resolve_photo_theme("冷热切换"), reference_mode="COMPLETE_LOOK",
        variables=dict(VARIABLES),
    )
    return spec, plan["items"][0]


def _observation(**overrides):
    pages = [{
        "role": role,
        "observed_thermal_context": CONTEXTS[role],
        "visible_layer_count": len(STACKS[role]),
        "visible_layer_stack": list(STACKS[role]),
        "observed_item_types": list(ITEMS[role]),
        "layer_evidence": ["层次清楚"],
        "identity_id": "person_1",
        "camera_signature": "camera_1",
        "full_body": True,
        "collar_compatible": True,
        "sleeve_conflict": False,
        "hem_conflict": False,
        "temperature_claim_matches": True,
        "temperature_claim_text": "",
        "observed_base_signature": "core_base",
        "observed_bottom_signature": "bottom_main",
        "observed_shoes_signature": "shoes_main",
        "added_garment_visible": True,
        "person_flags": {
            "face_or_limb_deformity": False, "obvious_unnatural_tilt": False,
        },
        "repair_instruction": "",
    } for role in ROLES]
    for path, value in overrides.items():
        role, field = path.split("__")
        page = next(item for item in pages if item["role"] == role)
        page[field] = copy.deepcopy(value)
    return {"thermal_index": 0, "notes": "同人同机位", "pages": pages}


def _evaluate(observation, spec=None, post=None):
    spec = spec if spec is not None else _fixture()[0]
    post = post if post is not None else _fixture()[1]
    return evaluate_thermal_transition_qa(
        observation, look_plans=post["looks"],
        profile_binding=post["profile_binding"],
        thermal_transition_contract=spec["thermal_transition_contract"],
    )


def _failed_roles(report):
    return {
        item["role"]: sorted(item["failure_codes"])
        for item in report["roles"] if not item["passed"]
    }


class ThermalTransitionSemanticsTest(unittest.TestCase):
    def test_clean_observation_passes_with_frozen_contexts(self):
        spec, post = _fixture()
        report = _evaluate(_observation(), spec, post)
        self.assertTrue(report["passed"])
        self.assertEqual(report["schema_version"], THERMAL_TRANSITION_QA_SCHEMA)
        self.assertEqual(
            report["thermal_contexts"],
            {
                "expected": [CONTEXTS[role] for role in ROLES],
                "observed": {role: CONTEXTS[role] for role in ROLES},
            },
        )
        self.assertEqual(
            [item["visible_layer_count"] for item in report["roles"]], [1, 2, 3]
        )
        self.assertEqual(failed_roles_from_thermal_qa(report), [])
        self.assertIn("冷热切换 QA 通过", thermal_transition_qa_note_zh(report))
        self.assertIn("冷热切换 Canary QA", thermal_transition_qa_markdown(report))
        alignment = thermal_qa_as_alignment(report)
        self.assertTrue(alignment["passed"])
        self.assertEqual(alignment["scope"], "THERMAL_TRANSITION_QA")

    def test_unknown_thermal_context_is_flagged(self):
        report = _evaluate(_observation(outer__observed_thermal_context="sauna_hot"))
        self.assertFalse(report["passed"])
        self.assertEqual(_failed_roles(report), {"outer": [UNKNOWN_THERMAL_CONTEXT]})

    def test_context_order_mismatch_is_flagged_on_the_swapped_pages(self):
        report = _evaluate(_observation(**{
            "base__observed_thermal_context": "transit_cool",
            "mid__observed_thermal_context": "outdoor_hot",
        }))
        self.assertFalse(report["passed"])
        self.assertEqual(_failed_roles(report), {
            "base": [CONTEXT_ORDER_MISMATCH],
            "mid": [CONTEXT_ORDER_MISMATCH],
        })

    def test_base_outfit_drift_fails_both_carrier_and_drifter(self):
        report = _evaluate(_observation(outer__observed_bottom_signature="other_bottom"))
        self.assertFalse(report["passed"])
        self.assertEqual(_failed_roles(report), {
            "base": [BASE_OUTFIT_DRIFT], "outer": [BASE_OUTFIT_DRIFT],
        })

    def test_invisible_transition_is_flagged(self):
        report = _evaluate(_observation(mid__added_garment_visible=False))
        self.assertFalse(report["passed"])
        self.assertEqual(_failed_roles(report), {"mid": [TRANSITION_NOT_VISIBLE]})

    def test_structural_verdicts_are_inherited(self):
        report = _evaluate(_observation(mid__visible_layer_count=3))
        self.assertIn(LAYER_COUNT_MISMATCH, _failed_roles(report)["mid"])
        report = _evaluate(_observation(outer__visible_layer_stack=["core", "blazer", "shirt"]))
        self.assertTrue(all(
            LAYER_ORDER_MISMATCH in item["failure_codes"]
            for item in report["roles"]
        ))
        report = _evaluate(_observation(outer__identity_id="person_2"))
        self.assertEqual(
            _failed_roles(report),
            {role: [LAYER_SOURCE_IDENTITY_MISMATCH] for role in ROLES},
        )
        report = _evaluate(_observation(outer__camera_signature="camera_2"))
        self.assertEqual(
            _failed_roles(report),
            {role: [LAYER_SOURCE_CAMERA_MISMATCH] for role in ROLES},
        )

    def test_qualitative_copy_rejects_any_concrete_temperature_claim(self):
        report = _evaluate(_observation(mid__temperature_claim_text="ข้างนอก 32°C"))
        self.assertFalse(report["passed"])
        self.assertEqual(_failed_roles(report), {"mid": [TEMPERATURE_VALUE_UNSOURCED]})

    def test_numeric_claim_needs_a_declared_source(self):
        spec, post = _fixture()
        binding = dict(post["profile_binding"])
        binding["temperature_label_mode"] = "EXPLICIT_INPUT"
        # No declared source ⇒ still rejected.
        report = evaluate_thermal_transition_qa(
            _observation(mid__temperature_claim_text="ข้างนอก 32°C"),
            look_plans=post["looks"], profile_binding=binding,
            thermal_transition_contract=spec["thermal_transition_contract"],
        )
        self.assertEqual(_failed_roles(report), {"mid": [TEMPERATURE_VALUE_UNSOURCED]})
        # Declared source ⇒ accepted.
        binding["sourced_temperature_values"] = ["32°C"]
        report = evaluate_thermal_transition_qa(
            _observation(mid__temperature_claim_text="ข้างนอก 32°C"),
            look_plans=post["looks"], profile_binding=binding,
            thermal_transition_contract=spec["thermal_transition_contract"],
        )
        self.assertTrue(report["passed"])

    def test_incomplete_observation_raises_schema_incomplete(self):
        broken = _observation()
        del broken["pages"][1]["observed_shoes_signature"]
        with self.assertRaisesRegex(Exception, QA_SCHEMA_INCOMPLETE):
            _evaluate(broken)

    def test_duplicate_and_missing_pages_are_rejected(self):
        duplicated = _observation()
        duplicated["pages"].append(copy.deepcopy(duplicated["pages"][0]))
        with self.assertRaises(ThermalTransitionQAError):
            _evaluate(duplicated)
        truncated = _observation()
        truncated["pages"] = truncated["pages"][:2]
        with self.assertRaises(ThermalTransitionQAError):
            _evaluate(truncated)

    def test_markdown_and_note_name_every_failure_code(self):
        report = _evaluate(_observation(
            mid__added_garment_visible=False,
            outer__observed_thermal_context="sauna_hot",
        ))
        markdown = thermal_transition_qa_markdown(report)
        note = thermal_transition_qa_note_zh(report)
        for code in (TRANSITION_NOT_VISIBLE, UNKNOWN_THERMAL_CONTEXT):
            self.assertIn(code, markdown)
            self.assertIn(code, note)


if __name__ == "__main__":
    unittest.main()
