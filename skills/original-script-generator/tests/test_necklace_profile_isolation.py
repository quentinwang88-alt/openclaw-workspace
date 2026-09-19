"""Isolation contracts for the necklace mixed profile.

The necklace profile adds a template and a recipe that the four existing
families must never reach, and it must never let its own branch reach theirs.
Two failure modes are covered here:

* **Shared-state pollution** -- the overlay is built on top of an
  ``lru_cache``d definition that every caller shares, so "just mutate it for
  this one compile" would change what a concurrent earring batch compiles
  against.
* **Rotation leakage** -- the new template must not be appended to the shared
  A/B/C list.  Appending it is the obvious implementation and the wrong one:
  every existing category rotates by batch index, so an earring batch would
  eventually be handed a necklace montage.

Every case runs with the necklace switch in both states, because the isolation
must hold whether or not a necklace request is in flight.
"""

import copy
import json
import os
import unittest
from unittest import mock

from core.accessory_mixed_templates import (
    compile_mixed_template_contract,
    get_environment_recipe,
    get_mixed_template,
    list_mixed_templates,
    load_mixed_template_definition,
    mixed_supported_canonical_types,
    mixed_template_ids,
    resolve_mixed_zone,
    select_environment_recipe_id,
    select_template_id,
)
from core.necklace_mixed_profile import (
    NECKLACE_MIXED_V1_ENV,
    NECKLACE_MIXED_V1_RECIPE_ID,
    NECKLACE_MIXED_V1_TEMPLATE_ID,
    build_necklace_profile_overlay,
    select_necklace_environment_recipe_id,
    select_necklace_template_id,
)


_SHIPPED_TEMPLATE_IDS = ["AMX_A_WORN_FIRST", "AMX_B_FORM_FIRST", "AMX_C_DETAIL_FIRST"]
_SHIPPED_RECIPE_IDS = [
    "WINDOW_LIGHT_WOOD",
    "WARM_WOOD_NEUTRAL_SUBJECT",
    "MATTE_GREY_DETAIL",
]
_SHIPPED_ZONES = {
    "earring": "EAR",
    "bracelet": "WRIST",
    "bangle": "WRIST",
    "slim_bangle": "WRIST",
    "ring": "FINGER",
    "claw_clip": "HAIR",
    "hair_clip": "HAIR",
    "headband": "HAIR",
    "scrunchie": "HAIR",
    "hair_tie": "HAIR",
    "ribbon": "HAIR",
    "hair_pin": "HAIR",
    "hair_accessory_generic": "HAIR",
}

_SHARED_THEME = {
    "theme_id": "TH_ISO",
    "candidate_role": "PRIMARY",
    "thesis": "在锁骨上给出一个可验证的落点",
    "approved_claim_refs": ["C1"],
    "evidence_refs": ["C1"],
}


def _definition_snapshot():
    return json.dumps(
        load_mixed_template_definition(), ensure_ascii=False, sort_keys=True
    )


class SharedDefinitionImmutableTest(unittest.TestCase):
    """Building and using an overlay must leave the shared object untouched."""

    def test_building_the_overlay_does_not_mutate_the_shared_definition(self):
        before = _definition_snapshot()
        for _ in range(3):
            build_necklace_profile_overlay()
        self.assertEqual(_definition_snapshot(), before)

    def test_the_overlay_is_a_copy_not_a_reference(self):
        overlay = build_necklace_profile_overlay()
        self.assertIsNot(overlay, load_mixed_template_definition())
        overlay["templates"][0]["template_id"] = "MUTATED"
        self.assertNotIn("MUTATED", mixed_template_ids())

    def test_compiling_through_the_overlay_does_not_mutate_the_shared_definition(self):
        before = _definition_snapshot()
        overlay = build_necklace_profile_overlay()
        compile_mixed_template_contract(
            product_type="项链",
            top_category="首饰",
            template_id=select_necklace_template_id(),
            environment_recipe_id=select_necklace_environment_recipe_id(),
            content_theme=dict(_SHARED_THEME),
            definition=overlay,
        )
        self.assertEqual(_definition_snapshot(), before)

    def test_the_shared_definition_is_stable_across_repeated_reads(self):
        first = _definition_snapshot()
        build_necklace_profile_overlay()
        self.assertEqual(_definition_snapshot(), first)
        self.assertIs(load_mixed_template_definition(), load_mixed_template_definition())


class ShippedRotationUnchangedTest(unittest.TestCase):
    """The A/B/C list, its order and the recipe rotation stay byte for byte."""

    def _assert_shipped_rotation(self):
        self.assertEqual(mixed_template_ids(), _SHIPPED_TEMPLATE_IDS)
        self.assertEqual(
            [select_template_id(index) for index in range(6)],
            [_SHIPPED_TEMPLATE_IDS[index % 3] for index in range(6)],
        )
        self.assertEqual(
            [select_environment_recipe_id(index) for index in range(6)],
            [_SHIPPED_RECIPE_IDS[index % 3] for index in range(6)],
        )
        self.assertNotIn(NECKLACE_MIXED_V1_TEMPLATE_ID, mixed_template_ids())
        self.assertNotIn(NECKLACE_MIXED_V1_RECIPE_ID, _SHIPPED_RECIPE_IDS)

    def test_the_rotation_is_unchanged_with_the_switch_off(self):
        with mock.patch.dict(os.environ, {NECKLACE_MIXED_V1_ENV: "0"}):
            self._assert_shipped_rotation()

    def test_the_rotation_is_unchanged_with_the_switch_on(self):
        with mock.patch.dict(os.environ, {NECKLACE_MIXED_V1_ENV: "1"}):
            self._assert_shipped_rotation()

    def test_the_rotation_is_unchanged_after_a_necklace_compile(self):
        overlay = build_necklace_profile_overlay()
        compile_mixed_template_contract(
            product_type="项链",
            top_category="首饰",
            template_id=select_necklace_template_id(),
            environment_recipe_id=select_necklace_environment_recipe_id(),
            content_theme=dict(_SHARED_THEME),
            definition=overlay,
        )
        self._assert_shipped_rotation()

    def test_the_shipped_zones_are_unchanged_in_both_contexts(self):
        overlay = build_necklace_profile_overlay()
        for canonical, zone in _SHIPPED_ZONES.items():
            self.assertEqual(resolve_mixed_zone(canonical, ""), (zone, canonical), canonical)
            self.assertEqual(
                resolve_mixed_zone(canonical, "", definition=overlay),
                (zone, canonical),
                canonical,
            )

    def test_the_shipped_supported_set_is_untouched_by_the_overlay(self):
        shipped = mixed_supported_canonical_types()
        overlay_supported = mixed_supported_canonical_types(definition=build_necklace_profile_overlay())
        self.assertEqual(shipped, set(_SHIPPED_ZONES))
        self.assertEqual(overlay_supported - shipped, {"necklace"})
        self.assertEqual(shipped & {"necklace"}, set())


class TemplatePoolSeparationTest(unittest.TestCase):
    """Neither side may reach into the other's template or recipe pool."""

    def test_the_necklace_overlay_exposes_exactly_one_template(self):
        overlay = build_necklace_profile_overlay()
        self.assertEqual(mixed_template_ids(definition=overlay), [NECKLACE_MIXED_V1_TEMPLATE_ID])
        self.assertEqual(len(list_mixed_templates(definition=overlay)), 1)

    def test_the_necklace_overlay_does_not_rotate(self):
        overlay = build_necklace_profile_overlay()
        self.assertEqual(
            {select_template_id(index, definition=overlay) for index in range(9)},
            {NECKLACE_MIXED_V1_TEMPLATE_ID},
        )
        self.assertEqual(
            {
                select_environment_recipe_id(index, definition=overlay)
                for index in range(9)
            },
            {NECKLACE_MIXED_V1_RECIPE_ID},
        )

    def test_the_necklace_branch_cannot_select_an_amx_template(self):
        overlay = build_necklace_profile_overlay()
        for template_id in _SHIPPED_TEMPLATE_IDS:
            with self.assertRaises(ValueError, msg=template_id):
                get_mixed_template(template_id, definition=overlay)
        for recipe_id in _SHIPPED_RECIPE_IDS:
            with self.assertRaises(ValueError, msg=recipe_id):
                get_environment_recipe(recipe_id, definition=overlay)

    def test_an_existing_category_cannot_obtain_the_necklace_template(self):
        with self.assertRaises(ValueError):
            get_mixed_template(NECKLACE_MIXED_V1_TEMPLATE_ID)
        with self.assertRaises(ValueError):
            get_environment_recipe(NECKLACE_MIXED_V1_RECIPE_ID)

    def test_compiling_an_amx_template_through_the_overlay_is_refused(self):
        overlay = build_necklace_profile_overlay()
        with self.assertRaises(ValueError):
            compile_mixed_template_contract(
                product_type="项链",
                top_category="首饰",
                template_id="AMX_A_WORN_FIRST",
                content_theme=dict(_SHARED_THEME),
                definition=overlay,
            )

    def test_compiling_a_necklace_without_the_context_is_refused(self):
        # Without the overlay there is no NECK zone, so the shared compiler
        # must refuse rather than fall back to some other category's rule.
        with self.assertRaises(ValueError):
            compile_mixed_template_contract(
                product_type="项链",
                top_category="首饰",
                template_id=NECKLACE_MIXED_V1_TEMPLATE_ID,
                content_theme=dict(_SHARED_THEME),
            )


class InterleavedCompileTest(unittest.TestCase):
    """Alternating categories must give identical results every time."""

    def _compile_earring(self):
        return compile_mixed_template_contract(
            product_type="耳饰",
            top_category="耳饰",
            template_id="AMX_A_WORN_FIRST",
            content_theme=dict(_SHARED_THEME),
        )

    def _compile_necklace(self):
        return compile_mixed_template_contract(
            product_type="项链",
            top_category="首饰",
            template_id=select_necklace_template_id(),
            environment_recipe_id=select_necklace_environment_recipe_id(),
            content_theme=dict(_SHARED_THEME),
            definition=build_necklace_profile_overlay(),
        )

    def test_earring_necklace_earring_gives_the_same_earring_contract(self):
        before = _definition_snapshot()
        first = self._compile_earring()
        self._compile_necklace()
        second = self._compile_earring()
        self.assertEqual(first, second)
        self.assertEqual(_definition_snapshot(), before)

    def test_necklace_earring_necklace_gives_the_same_necklace_contract(self):
        before = _definition_snapshot()
        first = self._compile_necklace()
        self._compile_earring()
        second = self._compile_necklace()
        self.assertEqual(first, second)
        self.assertEqual(_definition_snapshot(), before)

    def test_a_long_interleaving_is_stable_and_keeps_the_pools_apart(self):
        before = _definition_snapshot()
        earrings, necklaces = [], []
        for _ in range(4):
            earrings.append(self._compile_earring())
            necklaces.append(self._compile_necklace())
        self.assertEqual(len({json.dumps(item, sort_keys=True) for item in earrings}), 1)
        self.assertEqual(len({json.dumps(item, sort_keys=True) for item in necklaces}), 1)
        self.assertEqual(earrings[0]["template_id"], "AMX_A_WORN_FIRST")
        self.assertEqual(earrings[0]["category_zone"], "EAR")
        self.assertEqual(necklaces[0]["template_id"], NECKLACE_MIXED_V1_TEMPLATE_ID)
        self.assertEqual(necklaces[0]["category_zone"], "NECK")
        self.assertEqual(_definition_snapshot(), before)
        self.assertEqual(mixed_template_ids(), _SHIPPED_TEMPLATE_IDS)

    def test_the_earring_contract_never_carries_a_necklace_namespace(self):
        self.assertNotIn("necklace_contract", self._compile_earring())
        self.assertNotIn("feature_profile", self._compile_earring())


class OverlayContentTest(unittest.TestCase):
    """The overlay inherits the shared blocks and replaces only the owned ones."""

    def test_the_overlay_inherits_shared_blocks_unchanged(self):
        overlay = build_necklace_profile_overlay()
        shared = load_mixed_template_definition()
        for key in (
            "structural",
            "module_framing_rules",
            "part_evidence_terms",
            "evidence_uncertainty_terms",
        ):
            self.assertEqual(overlay[key], shared[key], key)

    def test_the_overlay_adds_the_neck_rule_without_dropping_the_shipped_ones(self):
        overlay = build_necklace_profile_overlay()
        inherited = set((load_mixed_template_definition().get("category_rules") or {}))
        self.assertEqual(
            set(overlay["category_rules"]) - inherited,
            {"NECK"},
        )
        self.assertEqual(inherited, {"EAR", "WRIST", "FINGER", "HAIR"})

    def test_the_overlay_adds_the_necklace_subtype_without_dropping_the_shipped_ones(self):
        overlay = build_necklace_profile_overlay()
        shipped = set(load_mixed_template_definition().get("physical_subtype_rules") or {})
        self.assertEqual(set(overlay["physical_subtype_rules"]) - shipped, {"necklace"})
        self.assertEqual(len(shipped), 13)

    def test_the_overlay_marker_records_the_profile_and_the_config_hash(self):
        overlay = build_necklace_profile_overlay()
        marker = overlay["necklace_profile_overlay"]
        self.assertEqual(marker["feature_profile"], "NECKLACE_MIXED_V1")
        self.assertEqual(marker["canonical_type"], "necklace")
        self.assertEqual(marker["zone"], "NECK")
        self.assertEqual(marker["subtype"], "SINGLE_LAYER_SINGLE_PENDANT")
        self.assertEqual(marker["interaction_mode"], "NONE")
        self.assertTrue(marker["profile_config_hash"])


if __name__ == "__main__":
    unittest.main()
