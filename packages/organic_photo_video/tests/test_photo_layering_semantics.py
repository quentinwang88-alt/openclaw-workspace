from __future__ import annotations

import copy
import unittest

from services.photo_layering_qa import (
    BASE_INCONSISTENT,
    CROSS_BAND_LAYER_COUNT_REGRESSION,
    CROSS_BAND_THERMAL_INDEX_REGRESSION,
    FORBIDDEN_ITEM_PRESENT,
    LAYER_COUNT_MISMATCH,
    LAYER_ORDER_MISMATCH,
    LAYER_SOURCE_CAMERA_MISMATCH,
    LAYER_SOURCE_IDENTITY_MISMATCH,
    NOT_STACKABLE,
    LayeringSemanticQAError,
    evaluate_cross_band_layering,
    failed_roles_from_layering_qa,
    layering_qa_as_alignment,
    normalize_layering_qa,
)


def contract():
    return {
        "source_roles": ["base", "mid", "outer"],
        "bands": [{
            "key": "t15",
            "final_visible_layer_bounds": {"min": 3, "max": 3},
            "allowed_item_types": ["base_top", "cardigan", "trench", "trousers"],
            "forbidden_item_types": ["puffer_jacket"],
        }],
        "cross_band_rules": {
            "temperature_order": ["t15", "t10", "t5", "t0"],
            "final_layer_count_non_decreasing": True,
            "thermal_index_non_decreasing": True,
            "core_base_consistency": "WITHIN_SERIES",
        },
    }


def plans():
    return [
        {"role": "base", "expected_visible_layer_count": 1},
        {"role": "mid", "expected_visible_layer_count": 2},
        {"role": "outer", "expected_visible_layer_count": 3},
    ]


def binding(**overrides):
    value = {
        "band_key": "t15",
        "thermal_sensitivity": "normal",
        "scene": "Commute",
        "series_id": "minimal_city",
        "family_key": "minimal_city_t15",
    }
    value.update(overrides)
    return value


def raw_qa():
    definitions = [
        ("base", 1, ["core-base"], ["base_top", "trousers"]),
        ("mid", 2, ["core-base", "cardigan"], ["base_top", "cardigan", "trousers"]),
        ("outer", 3, ["core-base", "cardigan", "trench"],
         ["base_top", "cardigan", "trench", "trousers"]),
    ]
    pages = []
    for role, count, stack, items in definitions:
        pages.append({
            "role": role,
            "observed_band": "t15",
            "visible_layer_count": count,
            "visible_layer_stack": stack,
            "observed_item_types": items,
            "layer_evidence": [f"{role} 可见累计层栈"],
            "identity_id": "person-1",
            "camera_signature": "camera-1",
            "full_body": True,
            "collar_compatible": True,
            "sleeve_conflict": False,
            "hem_conflict": False,
            "temperature_claim_matches": True,
            "person_flags": {
                "face_or_limb_deformity": False,
                "obvious_unnatural_tilt": False,
            },
            "repair_instruction": "",
        })
    return {"pages": pages, "thermal_index": 2, "notes": ""}


def normalized(raw=None, *, bind=None):
    return normalize_layering_qa(
        raw or raw_qa(),
        look_plans=plans(),
        profile_binding=bind or binding(),
        layering_contract=contract(),
    )


class LayeringSemanticQATest(unittest.TestCase):
    def test_valid_progression_passes_and_projects_to_alignment(self):
        qa = normalized()
        self.assertTrue(qa["passed"])
        self.assertEqual(qa["summary"]["core_base_garment_id"], "core-base")
        self.assertEqual(failed_roles_from_layering_qa(qa), [])
        alignment = layering_qa_as_alignment(qa)
        self.assertTrue(alignment["passed"])
        self.assertEqual(alignment["scope"], "LAYERING_SEMANTIC_QA")

    def test_observation_pages_are_role_keyed_not_position_bound(self):
        raw = raw_qa()
        raw["pages"].reverse()
        self.assertTrue(normalized(raw)["passed"])

    def test_missing_or_mistyped_field_never_defaults_to_pass(self):
        raw = raw_qa()
        del raw["pages"][0]["camera_signature"]
        with self.assertRaisesRegex(LayeringSemanticQAError, "QA_SCHEMA_INCOMPLETE"):
            normalized(raw)
        raw = raw_qa()
        raw["pages"][1]["sleeve_conflict"] = "false"
        with self.assertRaisesRegex(LayeringSemanticQAError, "sleeve_conflict"):
            normalized(raw)

    def test_pants_swap_with_add_layer_claim_is_rejected(self):
        raw = raw_qa()
        mid = raw["pages"][1]
        mid["visible_layer_count"] = 1
        mid["visible_layer_stack"] = ["core-base"]
        mid["observed_item_types"] = ["base_top", "trousers"]
        mid["layer_evidence"] = ["只换了裤子，上身层数没有增加"]
        qa = normalized(raw)
        self.assertFalse(qa["passed"])
        mid_result = next(item for item in qa["roles"] if item["role"] == "mid")
        self.assertIn(LAYER_COUNT_MISMATCH, mid_result["failure_codes"])
        self.assertTrue(all(LAYER_ORDER_MISMATCH in item["failure_codes"] for item in qa["roles"]))

    def test_identity_and_camera_mismatch_fail_the_group(self):
        raw = raw_qa()
        raw["pages"][1]["identity_id"] = "person-2"
        raw["pages"][2]["camera_signature"] = "camera-2"
        qa = normalized(raw)
        self.assertFalse(qa["passed"])
        for result in qa["roles"]:
            self.assertIn(LAYER_SOURCE_IDENTITY_MISMATCH, result["failure_codes"])
            self.assertIn(LAYER_SOURCE_CAMERA_MISMATCH, result["failure_codes"])

    def test_stackability_flags_are_rederived_in_code(self):
        raw = raw_qa()
        raw["pages"][1]["sleeve_conflict"] = True
        raw["pages"][1]["repair_instruction"] = "中层袖笼太宽，无法塞入外层"
        qa = normalized(raw)
        mid = next(item for item in qa["roles"] if item["role"] == "mid")
        self.assertFalse(mid["passed"])
        self.assertIn(NOT_STACKABLE, mid["failure_codes"])

    def test_forbidden_item_is_rejected_even_when_claim_matches(self):
        raw = raw_qa()
        raw["pages"][2]["observed_item_types"].append("puffer_jacket")
        qa = normalized(raw)
        outer = next(item for item in qa["roles"] if item["role"] == "outer")
        self.assertIn(FORBIDDEN_ITEM_PRESENT, outer["failure_codes"])

    def test_base_drift_breaks_both_stack_and_base_consistency(self):
        raw = raw_qa()
        raw["pages"][1]["visible_layer_stack"] = ["different-base", "cardigan"]
        raw["pages"][2]["visible_layer_stack"] = ["different-base", "cardigan", "trench"]
        qa = normalized(raw)
        self.assertFalse(qa["passed"])
        codes = {code for item in qa["roles"] for code in item["failure_codes"]}
        self.assertIn(LAYER_ORDER_MISMATCH, codes)
        self.assertIn(BASE_INCONSISTENT, codes)

    def test_sensitivity_must_be_scalar(self):
        with self.assertRaisesRegex(LayeringSemanticQAError, "单一值"):
            normalized(bind=binding(thermal_sensitivity=["normal"]))


def report(*, band, count, thermal, core="core-base", sensitivity="normal",
           scene="Commute", series="minimal_city"):
    return {
        "profile_binding": {
            "band_key": band,
            "thermal_sensitivity": sensitivity,
            "scene": scene,
            "series_id": series,
        },
        "summary": {
            "final_visible_layer_count": count,
            "thermal_index": thermal,
            "core_base_garment_id": core,
        },
    }


class CrossBandLayeringQATest(unittest.TestCase):
    def test_layer_count_thermal_index_and_core_base_must_not_regress(self):
        result = evaluate_cross_band_layering([
            report(band="t15", count=3, thermal=3),
            report(band="t10", count=2, thermal=2, core="other-base"),
        ], layering_contract=contract())
        self.assertFalse(result["passed"])
        codes = {item["failure_code"] for item in result["failures"]}
        self.assertEqual(codes, {
            CROSS_BAND_LAYER_COUNT_REGRESSION,
            CROSS_BAND_THERMAL_INDEX_REGRESSION,
            BASE_INCONSISTENT,
        })

    def test_different_sensitivity_scene_or_series_are_not_compared(self):
        result = evaluate_cross_band_layering([
            report(band="t15", count=4, thermal=4),
            report(band="t10", count=2, thermal=1, sensitivity="feels_cold"),
            report(band="t5", count=1, thermal=0, scene="Indoor"),
            report(band="t0", count=1, thermal=0, series="soft_city"),
        ], layering_contract=contract())
        self.assertTrue(result["passed"])
        self.assertEqual(len(result["groups"]), 4)
        self.assertTrue(all(not group["comparisons"] for group in result["groups"]))

    def test_valid_same_group_progression_passes(self):
        result = evaluate_cross_band_layering([
            report(band="t0", count=4, thermal=5),
            report(band="t15", count=3, thermal=2),
            report(band="t5", count=4, thermal=4),
            report(band="t10", count=3, thermal=3),
        ], layering_contract=contract())
        self.assertTrue(result["passed"])
        self.assertEqual(result["groups"][0]["bands"], ["t15", "t10", "t5", "t0"])


if __name__ == "__main__":
    unittest.main()
