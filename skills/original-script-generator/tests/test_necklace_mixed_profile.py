"""Contracts for the necklace single-layer / single-pendant mixed profile (V1).

Additive on purpose: no database, no model, no network.  Every case here is a
pure-function case, and the whole file must pass with the necklace switch in
its shipped state (`0`) so a default-off increment cannot hide a regression
behind an environment change.
"""

import copy
import json
import os
import unittest
from unittest import mock

from core.accessory_mixed_templates import (
    EVIDENCE_ABSENT,
    EVIDENCE_UNKNOWN,
    EVIDENCE_VERIFIED,
    compile_mixed_template_contract,
    contains_face_wording,
    get_mixed_template,
    project_module_framing,
    resolve_mixed_zone,
    validate_mixed_template_contract,
)
from core.necklace_mixed_profile import (
    NECKLACE_CONTRACT_KEY,
    NECKLACE_EVIDENCE_INCOMPLETE,
    NECKLACE_MAINLINE_UNAVAILABLE,
    NECKLACE_MIXED_V1_CANONICAL,
    NECKLACE_MIXED_V1_ENV,
    NECKLACE_MIXED_V1_FEATURE_VERSION,
    NECKLACE_MIXED_V1_INTERACTION_MODE,
    NECKLACE_MIXED_V1_PROFILE,
    NECKLACE_MIXED_V1_RECIPE_ID,
    NECKLACE_MIXED_V1_SUBTYPE,
    NECKLACE_MIXED_V1_TEMPLATE_ID,
    NECKLACE_MIXED_V1_ZONE,
    NECKLACE_SCOPE_UNSUPPORTED,
    NECKLACE_V1_ELIGIBLE,
    NECKLACE_V1_SHOT_ORDER,
    attach_necklace_contract,
    build_necklace_contract_block,
    build_necklace_profile_overlay,
    frozen_necklace_contract,
    judge_necklace_eligibility,
    load_necklace_v1_definition,
    necklace_mixed_v1_enabled,
    necklace_profile_config_hash,
    necklace_v1_subtype_rule,
    necklace_v1_zone_rule,
    resolve_necklace_v1_scope,
    select_necklace_environment_recipe_id,
    select_necklace_template_id,
    validate_necklace_v1_contract,
)


_VERIFIED_EVIDENCE = {
    "has_chain": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
    "has_pendant": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
}
_SINGLETON_COUNTS = {"layer_count": 1, "pendant_count": 1}
_SHORT_SCOPE = {
    "task_branch": "SHORT_VIDEO_ORIGINAL",
    "target_duration_seconds": 15.0,
    "is_new_plan": True,
    "script_mode": "simplified_v1",
}
_MAINLINE = {"core_value": "链条弧度与吊坠在锁骨上的落点"}


def _compile_necklace(**overrides):
    """Compile one real necklace contract through the shared compiler."""

    eligibility = judge_necklace_eligibility(
        part_evidence=_VERIFIED_EVIDENCE,
        counts=_SINGLETON_COUNTS,
        evidence_ref="PC_TEST",
    )
    overlay = build_necklace_profile_overlay(eligibility)
    kwargs = {
        "product_type": "项链",
        "top_category": "首饰",
        "template_id": select_necklace_template_id(),
        "environment_recipe_id": select_necklace_environment_recipe_id(),
        "product_identity_ref": "PC_TEST",
        "content_theme": {
            "theme_id": "TH_NECK",
            "candidate_role": "PRIMARY",
            "thesis": "单层链与吊坠停在锁骨上的实际落点",
            "approved_claim_refs": ["C1"],
            "evidence_refs": ["C1"],
        },
        "definition": overlay,
    }
    kwargs.update(overrides)
    return compile_mixed_template_contract(**kwargs)


def _scope_context(**overrides):
    context = {
        "product_type": "项链",
        "top_category": "首饰",
        "product_code": "PC_TEST",
        "part_evidence": _VERIFIED_EVIDENCE,
        "counts": _SINGLETON_COUNTS,
        "mainline": _MAINLINE,
    }
    context.update(overrides)
    return context


class NecklaceSwitchTest(unittest.TestCase):
    """The new profile must be opt-in, and off by default."""

    def test_the_switch_defaults_to_off(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(NECKLACE_MIXED_V1_ENV, None)
            self.assertFalse(necklace_mixed_v1_enabled())

    def test_the_switch_reads_the_documented_tokens(self):
        for token in ("1", "true", "YES", "on"):
            with mock.patch.dict(os.environ, {NECKLACE_MIXED_V1_ENV: token}):
                self.assertTrue(necklace_mixed_v1_enabled(), token)
        for token in ("0", "false", "", "off"):
            with mock.patch.dict(os.environ, {NECKLACE_MIXED_V1_ENV: token}):
                self.assertFalse(necklace_mixed_v1_enabled(), token)

    def test_an_explicit_override_beats_the_environment(self):
        with mock.patch.dict(os.environ, {NECKLACE_MIXED_V1_ENV: "1"}):
            self.assertFalse(necklace_mixed_v1_enabled(False))
        with mock.patch.dict(os.environ, {NECKLACE_MIXED_V1_ENV: "0"}):
            self.assertTrue(necklace_mixed_v1_enabled(True))


class NecklaceAuthoredTextTest(unittest.TestCase):
    """The authored contract must not instruct what the policy forbids."""

    def test_no_authored_instruction_names_the_face(self):
        # The project has already paid for this once: the EAR zone's
        # "小幅头肩变化" was pasted verbatim into a blueprint prompt whose own
        # ban line forbade the wording.  The necklace text must be clean at
        # source, not merely scrubbed on the way out.
        zone = necklace_v1_zone_rule()
        subtype = necklace_v1_subtype_rule()
        blobs = []
        blobs.extend(zone.get("allowed_framing") or [])
        blobs.extend(zone.get("module_framing", {}).get("WORN_DETAIL") or [])
        blobs.extend(zone.get("module_framing", {}).get("WORN_RELATION") or [])
        blobs.append(zone.get("base_action") or "")
        blobs.extend((zone.get("action_by_module") or {}).values())
        blobs.extend((subtype.get("action_by_module") or {}).values())
        blobs.extend((subtype.get("distinct_jobs") or {}).values())
        blobs.extend(subtype.get("observation_focus") or [])
        blobs.append(subtype.get("fallback_observation") or "")
        blobs.extend(
            item.get("observation_job") or ""
            for item in (load_necklace_v1_definition().get("template") or {}).get(
                "sequence"
            )
            or []
        )
        offenders = [text for text in blobs if contains_face_wording(text)]
        self.assertEqual(offenders, [])

    def test_the_face_ban_is_still_expressed_in_the_zone_rule(self):
        # Cleaning the action text must not have removed the prohibition.
        forbidden = " ".join(necklace_v1_zone_rule().get("forbidden_framing") or [])
        for banned in ("眼睛", "鼻子", "嘴部"):
            self.assertIn(banned, forbidden)


class NecklaceEligibilityTest(unittest.TestCase):
    """Single layer / single pendant is instance evidence, never a title."""

    def test_complete_evidence_is_eligible(self):
        verdict = judge_necklace_eligibility(
            part_evidence=_VERIFIED_EVIDENCE, counts=_SINGLETON_COUNTS
        )
        self.assertTrue(verdict["eligible"])
        self.assertEqual(verdict["reason"], NECKLACE_V1_ELIGIBLE)

    def test_an_unconfirmed_pendant_refuses(self):
        evidence = dict(_VERIFIED_EVIDENCE)
        evidence["has_pendant"] = {"state": EVIDENCE_UNKNOWN}
        verdict = judge_necklace_eligibility(
            part_evidence=evidence, counts=_SINGLETON_COUNTS
        )
        self.assertFalse(verdict["eligible"])
        self.assertTrue(verdict["reason"].startswith(NECKLACE_EVIDENCE_INCOMPLETE))
        self.assertIn("has_pendant:UNKNOWN", verdict["reason"])

    def test_a_denied_chain_refuses(self):
        evidence = dict(_VERIFIED_EVIDENCE)
        evidence["has_chain"] = {"state": EVIDENCE_ABSENT}
        verdict = judge_necklace_eligibility(
            part_evidence=evidence, counts=_SINGLETON_COUNTS
        )
        self.assertFalse(verdict["eligible"])
        self.assertIn("has_chain:ABSENT", verdict["reason"])

    def test_an_unstated_layer_count_refuses_rather_than_defaulting_to_one(self):
        # "nobody said how many layers" must never be read as "one layer".
        verdict = judge_necklace_eligibility(
            part_evidence=_VERIFIED_EVIDENCE, counts={"pendant_count": 1}
        )
        self.assertFalse(verdict["eligible"])
        self.assertIn("layer_count:UNKNOWN", verdict["reason"])
        self.assertIsNone(verdict["layer_count"])

    def test_two_layers_refuses(self):
        verdict = judge_necklace_eligibility(
            part_evidence=_VERIFIED_EVIDENCE,
            counts={"layer_count": 2, "pendant_count": 1},
        )
        self.assertFalse(verdict["eligible"])
        self.assertIn("layer_count:2", verdict["reason"])

    def test_every_verdict_records_its_evidence_source(self):
        verdict = judge_necklace_eligibility(
            part_evidence=_VERIFIED_EVIDENCE,
            counts=_SINGLETON_COUNTS,
            evidence_ref="PC_TEST",
        )
        parts = {item["part"] for item in verdict["evidence_refs"]}
        self.assertLessEqual({"has_chain", "has_pendant", "layer_count", "pendant_count"}, parts)
        self.assertTrue(all(item["ref"] == "PC_TEST" for item in verdict["evidence_refs"]))

    def test_a_bare_string_evidence_map_is_tolerated(self):
        verdict = judge_necklace_eligibility(
            part_evidence={"has_chain": "VERIFIED", "has_pendant": "VERIFIED"},
            counts=_SINGLETON_COUNTS,
        )
        self.assertTrue(verdict["eligible"])

    def test_an_unknown_shape_is_never_read_as_proof(self):
        verdict = judge_necklace_eligibility(part_evidence=None, counts=None)
        self.assertFalse(verdict["eligible"])
        self.assertEqual(verdict["unresolved"], [
            "has_chain:UNKNOWN",
            "has_pendant:UNKNOWN",
            "layer_count:UNKNOWN",
            "pendant_count:UNKNOWN",
        ])


class NecklaceScopeTest(unittest.TestCase):
    """``applicable`` and ``eligible`` answer different questions."""

    def test_a_non_necklace_is_not_applicable(self):
        decision = resolve_necklace_v1_scope(
            _scope_context(product_type="耳饰"), _SHORT_SCOPE, enabled=True
        )
        self.assertFalse(decision["applicable"])
        self.assertFalse(decision["eligible"])
        self.assertEqual(decision["reason"], f"{NECKLACE_SCOPE_UNSUPPORTED}:not_necklace")

    def test_a_choker_is_not_applicable_despite_sharing_the_neck_slot(self):
        # choker is a registered canonical type with slot=neck and
        # family=jewelry.  A slot/family test would let it in; a canonical
        # test must not.
        for raw in ("项圈", "颈圈", "choker"):
            decision = resolve_necklace_v1_scope(
                _scope_context(product_type=raw, top_category="首饰"),
                _SHORT_SCOPE,
                enabled=True,
            )
            self.assertFalse(decision["applicable"], raw)

    def test_a_sibling_accessory_is_not_applicable(self):
        for raw in ("手链", "手镯", "戒指", "耳饰", "抓夹"):
            decision = resolve_necklace_v1_scope(
                _scope_context(product_type=raw, top_category=raw),
                _SHORT_SCOPE,
                enabled=True,
            )
            self.assertFalse(decision["applicable"], raw)

    def test_an_already_canonical_value_is_accepted(self):
        decision = resolve_necklace_v1_scope(
            _scope_context(canonical_type=NECKLACE_MIXED_V1_CANONICAL),
            _SHORT_SCOPE,
            enabled=True,
        )
        self.assertTrue(decision["applicable"])
        self.assertTrue(decision["eligible"])

    def test_a_bare_english_canonical_is_accepted(self):
        # Regression guard for the registry trap: `normalize_product_type`
        # does not treat "necklace" as an alias and would answer `womenwear`,
        # so a resolver that only ran the shared normaliser would refuse a
        # perfectly valid necklace request.
        context = _scope_context(product_type="necklace")
        decision = resolve_necklace_v1_scope(context, _SHORT_SCOPE, enabled=True)
        self.assertEqual(decision["canonical_type"], NECKLACE_MIXED_V1_CANONICAL)
        self.assertTrue(decision["applicable"])
        self.assertTrue(decision["eligible"])

    def test_an_unrecognised_value_is_not_read_as_a_necklace(self):
        # The mirror image of the trap: an unknown accessory also falls back to
        # `womenwear`, and that fallback must never be promoted into an answer.
        # It must also not be read as "necklace" merely because it failed.
        decision = resolve_necklace_v1_scope(
            _scope_context(product_type="某种全新饰品", top_category=""),
            _SHORT_SCOPE,
            enabled=True,
        )
        self.assertEqual(decision["canonical_type"], "")
        self.assertFalse(decision["applicable"])

    def test_a_necklace_with_the_switch_off_is_applicable_but_not_eligible(self):
        decision = resolve_necklace_v1_scope(_scope_context(), _SHORT_SCOPE, enabled=False)
        self.assertTrue(decision["applicable"])
        self.assertFalse(decision["eligible"])
        self.assertEqual(decision["reason"], f"{NECKLACE_SCOPE_UNSUPPORTED}:switch=off")

    def test_a_qualified_request_is_eligible(self):
        decision = resolve_necklace_v1_scope(_scope_context(), _SHORT_SCOPE, enabled=True)
        self.assertTrue(decision["applicable"])
        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["reason"], NECKLACE_V1_ELIGIBLE)
        self.assertEqual(decision["template_id"], NECKLACE_MIXED_V1_TEMPLATE_ID)
        self.assertEqual(decision["environment_recipe_id"], NECKLACE_MIXED_V1_RECIPE_ID)

    def test_a_necklace_outside_the_short_original_scope_is_refused(self):
        for scope, expected in (
            ({**_SHORT_SCOPE, "target_duration_seconds": 30.0}, "duration=30"),
            ({**_SHORT_SCOPE, "task_branch": "LONGFORM"}, "branch=LONGFORM"),
            ({**_SHORT_SCOPE, "is_new_plan": False}, "not_new_plan"),
            ({**_SHORT_SCOPE, "script_mode": "legacy_v2"}, "script_mode=legacy_v2"),
        ):
            decision = resolve_necklace_v1_scope(_scope_context(), scope, enabled=True)
            self.assertTrue(decision["applicable"])
            self.assertFalse(decision["eligible"])
            self.assertTrue(
                decision["reason"].startswith(NECKLACE_SCOPE_UNSUPPORTED), decision["reason"]
            )
            self.assertIn(expected, decision["reason"])

    def test_incomplete_evidence_refuses_with_a_traceable_reason(self):
        decision = resolve_necklace_v1_scope(
            _scope_context(part_evidence={"has_chain": {"state": EVIDENCE_UNKNOWN}}),
            _SHORT_SCOPE,
            enabled=True,
        )
        self.assertTrue(decision["applicable"])
        self.assertFalse(decision["eligible"])
        self.assertTrue(decision["reason"].startswith(NECKLACE_EVIDENCE_INCOMPLETE))
        self.assertTrue(decision["evidence_refs"])

    def test_a_missing_mainline_refuses(self):
        decision = resolve_necklace_v1_scope(
            _scope_context(mainline={}), _SHORT_SCOPE, enabled=True
        )
        self.assertTrue(decision["applicable"])
        self.assertFalse(decision["eligible"])
        self.assertTrue(decision["reason"].startswith(NECKLACE_MAINLINE_UNAVAILABLE))


class NecklaceContractNamespaceTest(unittest.TestCase):
    """The namespace is *additional*: shared keys keep their shape."""

    def test_the_block_carries_the_documented_minimum(self):
        eligibility = judge_necklace_eligibility(
            part_evidence=_VERIFIED_EVIDENCE, counts=_SINGLETON_COUNTS, evidence_ref="PC"
        )
        block = build_necklace_contract_block(
            eligibility=eligibility,
            chain_identity={"ref": "anchor:chain"},
            pendant_identity={"ref": "anchor:pendant"},
            wearing_relation={"ref": "outfit:neckline"},
        )
        self.assertEqual(block["subtype"], NECKLACE_MIXED_V1_SUBTYPE)
        self.assertEqual(block["interaction_mode"], NECKLACE_MIXED_V1_INTERACTION_MODE)
        self.assertEqual(block["feature_profile"], NECKLACE_MIXED_V1_PROFILE)
        self.assertEqual(block["feature_version"], NECKLACE_MIXED_V1_FEATURE_VERSION)
        self.assertEqual(block["chain_identity"]["ref"], "anchor:chain")
        self.assertEqual(block["pendant_identity"]["ref"], "anchor:pendant")
        self.assertEqual(block["wearing_relation"]["ref"], "outfit:neckline")
        self.assertTrue(block["eligibility_evidence_refs"])
        self.assertEqual(block["profile_config_hash"], necklace_profile_config_hash())

    def test_attaching_adds_a_namespace_without_rewriting_shared_values(self):
        contract = _compile_necklace()
        before = copy.deepcopy(contract)
        eligibility = judge_necklace_eligibility(
            part_evidence=_VERIFIED_EVIDENCE, counts=_SINGLETON_COUNTS
        )
        enriched = attach_necklace_contract(contract, eligibility=eligibility)
        for key, value in before.items():
            self.assertEqual(enriched[key], value, key)
        self.assertIn(NECKLACE_CONTRACT_KEY, enriched)
        self.assertEqual(enriched["feature_profile"], NECKLACE_MIXED_V1_PROFILE)

    def test_the_namespace_is_readable_from_either_container_level(self):
        enriched = attach_necklace_contract(_compile_necklace())
        self.assertEqual(
            frozen_necklace_contract({"mixed_template_contract": enriched})["subtype"],
            NECKLACE_MIXED_V1_SUBTYPE,
        )
        self.assertEqual(
            frozen_necklace_contract(
                {"category_execution_extension": {"mixed_template_contract": enriched}}
            )["subtype"],
            NECKLACE_MIXED_V1_SUBTYPE,
        )

    def test_a_plain_contract_has_no_necklace_namespace(self):
        earring = compile_mixed_template_contract(
            product_type="耳饰", top_category="耳饰", template_id="AMX_A_WORN_FIRST"
        )
        self.assertEqual(frozen_necklace_contract({"mixed_template_contract": earring}), {})

    def test_the_config_hash_tracks_the_delta_only(self):
        baseline = necklace_profile_config_hash()
        mutated = copy.deepcopy(load_necklace_v1_definition())
        mutated["template"]["sequence"][0]["duration_seconds"] = 5
        self.assertNotEqual(baseline, necklace_profile_config_hash(mutated))


class NecklaceContractValidationTest(unittest.TestCase):
    """A real compiled contract passes; each mutation must be caught."""

    def setUp(self):
        self.contract = attach_necklace_contract(_compile_necklace())

    def test_the_compiled_contract_is_clean(self):
        self.assertEqual(validate_necklace_v1_contract(self.contract), [])

    def test_the_shared_validator_also_accepts_it(self):
        self.assertEqual(
            validate_mixed_template_contract(
                self.contract, definition=build_necklace_profile_overlay()
            ),
            [],
        )

    def test_the_shot_order_and_timeline_are_frozen_as_specified(self):
        units = self.contract["capture_units"]
        self.assertEqual(
            [(unit["module"], unit["duration_seconds"]) for unit in units],
            list(NECKLACE_V1_SHOT_ORDER),
        )
        self.assertEqual(self.contract["total_duration_seconds"], 15)

    def test_only_the_two_worn_shots_carry_the_neck_zone(self):
        units = self.contract["capture_units"]
        self.assertEqual(units[0]["body_zone"], NECKLACE_MIXED_V1_ZONE)
        self.assertEqual(units[1]["body_zone"], NECKLACE_MIXED_V1_ZONE)
        self.assertEqual(units[2]["body_zone"], "")
        self.assertEqual(units[3]["body_zone"], "")

    def test_the_handheld_shot_is_hand_only_and_the_last_shot_is_a_still(self):
        units = self.contract["capture_units"]
        self.assertEqual(units[2]["carrier_mode"], "HAND_ONLY")
        self.assertEqual(units[2]["product_state"], "HELD")
        self.assertEqual(units[3]["carrier_mode"], "STATIC_PRODUCT")
        self.assertEqual(units[3]["product_state"], "RESTING_ON_SURFACE")

    def test_a_foreign_zone_action_is_caught(self):
        # Mutation: the defect this profile exists to prevent -- a necklace
        # shot that asks for an ear or a wrist.
        for term in ("耳侧", "手腕", "发梢"):
            mutated = copy.deepcopy(self.contract)
            mutated["capture_units"][0]["action"] = f"{term}小幅自然变化"
            errors = validate_necklace_v1_contract(mutated)
            self.assertTrue(
                any("NECKLACE_CONTRACT_FOREIGN_ZONE_ACTION" in item for item in errors),
                term,
            )

    def test_a_generic_observation_job_is_caught(self):
        mutated = copy.deepcopy(self.contract)
        mutated["capture_units"][2]["observation_job"] = "展示核心卖点"
        errors = validate_necklace_v1_contract(mutated)
        self.assertTrue(
            any("NECKLACE_CONTRACT_GENERIC_OBSERVATION" in item for item in errors)
        )

    def test_a_changed_shot_duration_is_caught(self):
        mutated = copy.deepcopy(self.contract)
        mutated["capture_units"][1]["duration_seconds"] = 4
        self.assertTrue(
            any("NECKLACE_CONTRACT_SHOT_DURATION" in item
                for item in validate_necklace_v1_contract(mutated))
        )

    def test_a_reordered_montage_is_caught(self):
        mutated = copy.deepcopy(self.contract)
        mutated["capture_units"][2]["module"] = "WORN_DETAIL"
        self.assertTrue(
            any("NECKLACE_CONTRACT_SHOT_ORDER" in item
                for item in validate_necklace_v1_contract(mutated))
        )

    def test_a_wrong_template_or_recipe_is_caught(self):
        for field, value in (
            ("template_id", "AMX_A_WORN_FIRST"),
            ("environment_recipe_id", "WINDOW_LIGHT_WOOD"),
            ("canonical_product_type", "earring"),
            ("category_zone", "EAR"),
        ):
            mutated = copy.deepcopy(self.contract)
            mutated[field] = value
            errors = validate_necklace_v1_contract(mutated)
            self.assertTrue(
                any("MISMATCH" in item for item in errors), (field, errors)
            )

    def test_a_missing_namespace_is_caught(self):
        mutated = copy.deepcopy(self.contract)
        mutated.pop(NECKLACE_CONTRACT_KEY)
        self.assertTrue(
            any("necklace_contract_missing" in item
                for item in validate_necklace_v1_contract(mutated))
        )

    def test_a_wrong_subtype_or_interaction_mode_is_caught(self):
        for field, value in (("subtype", "MULTI_LAYER"), ("interaction_mode", "CHAIN_PULL")):
            mutated = copy.deepcopy(self.contract)
            mutated[NECKLACE_CONTRACT_KEY][field] = value
            self.assertTrue(
                any("MISMATCH" in item for item in validate_necklace_v1_contract(mutated)),
                field,
            )

    def test_an_empty_contract_reports_missing(self):
        self.assertEqual(validate_necklace_v1_contract({}), ["NECKLACE_CONTRACT_MISSING"])
        self.assertEqual(validate_necklace_v1_contract(None), ["NECKLACE_CONTRACT_MISSING"])


class NecklaceFramingProjectionTest(unittest.TestCase):
    """Framing must be projected from the same context the contract used."""

    def test_the_overlay_projects_the_neck_zone_for_both_worn_shots(self):
        overlay = build_necklace_profile_overlay()
        for module in ("WORN_DETAIL", "WORN_RELATION"):
            framing = project_module_framing(
                NECKLACE_MIXED_V1_CANONICAL, module, definition=overlay
            )
            self.assertEqual(framing.get("body_zone"), NECKLACE_MIXED_V1_ZONE, module)
            self.assertEqual(framing.get("framing_source"), "CATEGORY_ZONE", module)
            self.assertTrue(framing.get("allowed_framing"), module)

    def test_the_shared_definition_has_no_necklace_zone(self):
        # The whole point of the overlay: without the context, necklace is not
        # a mixed type and nothing can accidentally route it into a NECK rule.
        # Note the canonical that comes back: `normalize_product_type` does not
        # recognise a bare English canonical name and falls back to
        # `womenwear`, so the shared resolver's second element is NOT a safe
        # source of truth here -- only the zone being None is.
        zone, _canonical = resolve_mixed_zone(NECKLACE_MIXED_V1_CANONICAL, "")
        self.assertIsNone(zone)
        self.assertEqual(project_module_framing(NECKLACE_MIXED_V1_CANONICAL, "WORN_DETAIL"), {})
        with self.assertRaises(ValueError):
            get_mixed_template(NECKLACE_MIXED_V1_TEMPLATE_ID)

    def test_the_overlay_keeps_the_shared_module_framing_for_handheld_and_static(self):
        overlay = build_necklace_profile_overlay()
        for module, scope in (
            ("HANDHELD_PRODUCT", "HAND_AND_PRODUCT"),
            ("STATIC_PRODUCT", "PRODUCT_AND_SURFACE"),
        ):
            framing = project_module_framing(
                NECKLACE_MIXED_V1_CANONICAL, module, definition=overlay
            )
            self.assertEqual(framing.get("view_scope"), scope, module)
            self.assertEqual(framing.get("framing_source"), "MODULE", module)


class NecklaceDefinitionShapeTest(unittest.TestCase):
    """The delta file stays a delta."""

    def test_the_delta_does_not_carry_shared_configuration(self):
        delta = load_necklace_v1_definition()
        for shared_key in (
            "structural",
            "module_framing_rules",
            "part_evidence_terms",
            "category_rules",
            "physical_subtype_rules",
            "environment_recipes",
            "templates",
        ):
            self.assertNotIn(shared_key, delta, shared_key)

    def test_the_delta_declares_one_template_and_one_recipe(self):
        delta = load_necklace_v1_definition()
        self.assertEqual(delta["template"]["template_id"], NECKLACE_MIXED_V1_TEMPLATE_ID)
        self.assertEqual(
            delta["environment_recipe_id"], NECKLACE_MIXED_V1_RECIPE_ID
        )
        self.assertEqual(len(delta["template"]["sequence"]), 4)

    def test_the_registry_does_not_promote_the_title_into_verified_facts(self):
        # The guard rail the plan calls out explicitly: "不能靠标题'项链'把两者
        # 强制设置为 VERIFIED".
        facts = necklace_v1_subtype_rule()["structure_facts"]
        self.assertEqual(facts["has_chain"], EVIDENCE_UNKNOWN)
        self.assertEqual(facts["has_pendant"], EVIDENCE_UNKNOWN)
        self.assertEqual(facts["has_clasp"], EVIDENCE_UNKNOWN)

    def test_parts_a_necklace_cannot_have_are_absent_so_their_vocabulary_is_blocked(self):
        facts = necklace_v1_subtype_rule()["structure_facts"]
        for part in (
            "has_ear_clip",
            "has_piercing_post",
            "has_teeth_or_jaw",
            "has_band_arch",
            "has_ribbon_bow",
            "has_rigid_ring",
        ):
            self.assertEqual(facts[part], EVIDENCE_ABSENT, part)


if __name__ == "__main__":
    unittest.main()
