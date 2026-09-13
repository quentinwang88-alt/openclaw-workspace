from __future__ import annotations

import unittest

from services.photo_content_check import validate_prepared_sources
from services.photo_content_planner import validate_batch_plan
from services.photo_flow_registry import (
    DEFAULT_TRAVEL_SOURCE_ROLES, LAYERING_TWO_STEP_FLOW,
    PhotoFlowRegistryError, get_photo_flow_handler, resolve_required_roles,
)
from services.photo_reference import resolve_reference_mode
from services.photo_reference_vision import PhotoReferenceVisionService
from services.photo_theme import style_look_specs


LAYER_ROLES = ("base", "mid", "outer")


def layer_looks():
    return [
        {
            "role": role,
            "outerwear": f"outer-{role}",
            "top_inner": f"top-{role}",
            "bottom": f"bottom-{role}",
            "shoes": f"shoes-{role}",
        }
        for role in LAYER_ROLES
    ]


class PhotoFlowRegistryTest(unittest.TestCase):
    def test_travel_and_legacy_flows_keep_four_role_default(self):
        self.assertEqual(
            resolve_required_roles(planning_flow="travel_two_step"),
            DEFAULT_TRAVEL_SOURCE_ROLES,
        )
        self.assertEqual(
            resolve_required_roles(planning_flow=""),
            DEFAULT_TRAVEL_SOURCE_ROLES,
        )
        self.assertTrue(get_photo_flow_handler("travel_two_step").travel_semantics)

    def test_layering_flow_requires_recipe_roles(self):
        with self.assertRaisesRegex(PhotoFlowRegistryError, "required_roles"):
            resolve_required_roles(planning_flow=LAYERING_TWO_STEP_FLOW)
        self.assertEqual(
            resolve_required_roles(
                planning_flow=LAYERING_TWO_STEP_FLOW,
                required_roles=LAYER_ROLES,
            ),
            LAYER_ROLES,
        )

    def test_planner_and_theme_validate_the_same_layer_roles(self):
        item = {
            "index": 1,
            "angle_zh": "逐层穿搭",
            "copy": {"title": "x"},
            "family_id": "t15_city",
            "looks": layer_looks(),
            "planning_flow": LAYERING_TWO_STEP_FLOW,
            "required_roles": list(LAYER_ROLES),
            "difference_axes": {"family": "t15_city"},
        }
        plan = {
            "count": 1,
            "reference_mode": "STYLE",
            "planning_flow": LAYERING_TWO_STEP_FLOW,
            "required_roles": list(LAYER_ROLES),
            "items": [item],
        }
        validate_batch_plan(plan)
        self.assertEqual(
            [look["role"] for look in style_look_specs({}, item)],
            list(LAYER_ROLES),
        )

    def test_source_check_uses_frozen_layer_roles_and_count(self):
        plan_item = {
            "planning_flow": LAYERING_TWO_STEP_FLOW,
            "required_roles": list(LAYER_ROLES),
            "looks": [],
        }
        sources = [
            {"role": role, "sha256": f"sha-{role}"}
            for role in LAYER_ROLES
        ]
        validate_prepared_sources(plan_item, sources)

    def test_complete_look_error_can_name_non_travel_roles(self):
        with self.assertRaisesRegex(ValueError, "base/mid/outer"):
            resolve_reference_mode(
                selected_type="完整穿搭", attachments=[1, 2], product_id="",
                required_role_count=3, requested_count=1,
                required_roles=LAYER_ROLES,
            )

    def test_generic_vision_prompt_uses_explicit_roles(self):
        prompt = PhotoReferenceVisionService._analysis_prompt(
            theme={"theme_key": "TEMPERATURE_DRESSING"},
            category_key="womenswear", content_requirement="", count=1,
            required_roles=LAYER_ROLES,
        )
        self.assertIn("base/mid/outer", prompt)
        self.assertIn('"role": "outer"', prompt)
        self.assertIn("LAYER_PROGRESSION", prompt)


if __name__ == "__main__":
    unittest.main()
