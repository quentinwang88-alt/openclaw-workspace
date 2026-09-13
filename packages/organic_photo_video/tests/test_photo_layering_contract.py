from __future__ import annotations

import copy
import unittest

from services.photo_layering_contract import (
    LayeringPlanContractError,
    PlanContractValidator,
)


def layering_contract():
    return {
        "source_roles": ["base", "mid", "outer"],
        "bands": [
            {
                "key": "t15",
                "final_visible_layer_bounds": {"min": 3, "max": 4},
                "allowed_item_types": ["base_top", "cardigan", "trench", "wool_coat"],
                "forbidden_item_types": ["puffer_jacket"],
            },
            {
                "key": "t10",
                "final_visible_layer_bounds": {"min": 3, "max": 4},
                "allowed_item_types": ["base_top", "cardigan", "trench", "wool_coat"],
                "forbidden_item_types": ["shorts"],
            },
        ],
        "sensitivity_rules": {
            "feels_cold": {"layer_choice": "UPPER_BOUND"},
            "normal": {"layer_choice": "DEFAULT"},
            "feels_warm": {"layer_choice": "LOWER_BOUND"},
        },
        "scene_rules": {"may_change_layer_stack": False},
        "cross_band_rules": {
            "temperature_order": ["t15", "t10", "t5", "t0"],
            "final_layer_count_non_decreasing": True,
            "thermal_index_non_decreasing": True,
            "core_base_consistency": "WITHIN_SERIES",
        },
    }


def look(role, count, garments, added=""):
    item_types = {
        "core-base": "base_top",
        "cardigan": "cardigan",
        "trench": "trench",
        "wool-coat": "wool_coat",
        "puffer": "puffer_jacket",
        "other-base": "base_top",
    }
    payload = {
        "role": role,
        "expected_visible_layer_count": count,
        "expected_layer_stack": [
            {"garment_id": garment, "item_type": item_types[garment]}
            for garment in garments
        ],
    }
    if added:
        payload["added_garment_id"] = added
    return payload


def post(*, band="t15", sensitivity="normal", scene="Commute",
         series="minimal_city", core="core-base", thermal=2, outer="trench"):
    return {
        "profile_binding": {
            "band_key": band,
            "thermal_sensitivity": sensitivity,
            "scene": scene,
            "family_key": f"{series}_{band}",
            "series_id": series,
            "core_base_garment_id": core,
            "thermal_index": thermal,
        },
        "presentation_order": ["base", "mid", "outer"],
        "generation_order": ["outer", "mid", "base"],
        "scene_modifier": {"footwear_type": "sneaker", "outerwear_state": "open"},
        "looks": [
            look("base", 1, [core]),
            look("mid", 2, [core, "cardigan"], "cardigan"),
            look("outer", 3, [core, "cardigan", outer], outer),
        ],
    }


def plan(*posts):
    return {"flow": "layering_two_step", "posts": list(posts or [post()])}


class PlanContractValidatorTest(unittest.TestCase):
    def setUp(self):
        self.contract = layering_contract()
        self.validator = PlanContractValidator()

    def test_valid_plan_returns_defensive_copy(self):
        source = plan(post())
        frozen = self.validator.validate(source, layering_contract=self.contract)
        self.assertEqual(frozen, source)
        self.assertIsNot(frozen, source)
        frozen["posts"][0]["looks"][0]["role"] = "changed"
        self.assertEqual(source["posts"][0]["looks"][0]["role"], "base")

    def test_looks_are_role_keyed_not_position_bound(self):
        source = plan(post())
        source["posts"][0]["looks"].reverse()
        self.validator.validate(source, layering_contract=self.contract)

    def test_sensitivity_must_freeze_one_declared_value(self):
        source = plan(post())
        source["posts"][0]["profile_binding"]["thermal_sensitivity"] = ["normal"]
        with self.assertRaisesRegex(LayeringPlanContractError, "exactly one"):
            self.validator.validate(source, layering_contract=self.contract)
        source = plan(post(sensitivity="unknown"))
        with self.assertRaisesRegex(LayeringPlanContractError, "not declared"):
            self.validator.validate(source, layering_contract=self.contract)

    def test_sensitivity_layer_choice_cannot_escape_the_band_bounds(self):
        source = plan(post(sensitivity="feels_cold"))
        with self.assertRaisesRegex(LayeringPlanContractError, "does not satisfy sensitivity"):
            self.validator.validate(source, layering_contract=self.contract)

    def test_stack_must_be_strict_inclusion_not_an_outfit_swap(self):
        source = plan(post())
        source["posts"][0]["looks"][1] = look(
            "mid", 2, ["other-base", "cardigan"], "cardigan"
        )
        with self.assertRaisesRegex(LayeringPlanContractError, "strict stack inclusion"):
            self.validator.validate(source, layering_contract=self.contract)

    def test_final_visible_layer_bounds_are_enforced(self):
        source = plan(post())
        source["posts"][0]["looks"].append({})
        source["posts"][0]["looks"] = [
            look("base", 2, ["core-base", "cardigan"]),
            look("mid", 3, ["core-base", "cardigan", "trench"], "trench"),
            look("outer", 4, ["core-base", "cardigan", "trench", "wool-coat"], "wool-coat"),
        ]
        self.validator.validate(source, layering_contract=self.contract)
        self.contract["bands"][0]["final_visible_layer_bounds"] = {"min": 3, "max": 3}
        with self.assertRaisesRegex(LayeringPlanContractError, "outside.*bounds"):
            self.validator.validate(source, layering_contract=self.contract)

    def test_scene_modifier_cannot_change_layer_stack(self):
        source = plan(post())
        source["posts"][0]["scene_modifier"]["add_items"] = ["scarf"]
        with self.assertRaisesRegex(LayeringPlanContractError, "may not change"):
            self.validator.validate(source, layering_contract=self.contract)

    def test_forbidden_and_unknown_item_types_fail(self):
        source = plan(post(outer="puffer"))
        with self.assertRaisesRegex(LayeringPlanContractError, "forbidden"):
            self.validator.validate(source, layering_contract=self.contract)
        source = plan(post())
        source["posts"][0]["looks"][2]["expected_layer_stack"][2]["item_type"] = "cape"
        with self.assertRaisesRegex(LayeringPlanContractError, "outside allowlist"):
            self.validator.validate(source, layering_contract=self.contract)

    def test_cross_band_guards_compare_only_matching_series_sensitivity_scene(self):
        warmer = post(band="t15", thermal=3)
        colder = post(band="t10", thermal=2)
        with self.assertRaisesRegex(LayeringPlanContractError, "thermal_index regressed"):
            self.validator.validate(plan(warmer, colder), layering_contract=self.contract)

        colder["profile_binding"]["scene"] = "Outdoor"
        self.validator.validate(plan(warmer, colder), layering_contract=self.contract)

    def test_cross_band_core_base_must_stay_stable_within_series(self):
        warmer = post(band="t15", thermal=2)
        colder = post(band="t10", core="other-base", thermal=3)
        with self.assertRaisesRegex(LayeringPlanContractError, "core base changed"):
            self.validator.validate(plan(warmer, colder), layering_contract=self.contract)


if __name__ == "__main__":
    unittest.main()
