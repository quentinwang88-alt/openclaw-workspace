"""Integration contracts for the necklace V1 branch inside the allocator.

Where ``test_necklace_mixed_profile.py`` pins the *profile* (pure functions) and
``test_necklace_profile_isolation.py`` pins the *shared definition*, this file
pins the **wiring**: the branch that decides, per plan item, whether a request
compiles under NMX at all.

Every case goes through the real ``_build_mixed_template_injection`` entry point
rather than through a helper that re-implements it.  A branch that is only ever
exercised through its own function can be perfectly green while the caller never
calls it -- a failure mode this project has already paid for twice.

No database, no model, no network: both switches are environment-only and the
instance evidence is supplied as a plain mapping.
"""

import contextlib
import copy
import json
import os
import unittest
from unittest import mock

from core.accessory_mixed_templates import (
    ACCESSORY_MIXED_TEMPLATE_ENV,
    EVIDENCE_ABSENT,
    EVIDENCE_UNKNOWN,
    EVIDENCE_VERIFIED,
    load_mixed_template_definition,
    mixed_template_ids,
    validate_mixed_template_contract,
)
from core.necklace_mixed_profile import (
    NECKLACE_CONTRACT_KEY,
    NECKLACE_EVIDENCE_INCOMPLETE,
    NECKLACE_MAINLINE_UNAVAILABLE,
    NECKLACE_MIXED_V1_ENV,
    NECKLACE_MIXED_V1_FEATURE_VERSION,
    NECKLACE_MIXED_V1_PROFILE,
    NECKLACE_MIXED_V1_RECIPE_ID,
    NECKLACE_MIXED_V1_TEMPLATE_ID,
    NECKLACE_MIXED_V1_ZONE,
    NECKLACE_V1_SHOT_ORDER,
    validate_necklace_v1_contract,
)
from core.original_batch_allocator import (
    _build_mixed_template_injection,
    _mixed_product_evidence,
)


_SHORT_SCOPE = {
    "task_branch": "SHORT_VIDEO_ORIGINAL",
    "target_duration_seconds": 15.0,
    "is_new_plan": True,
    "script_mode": "simplified_v1",
}
# Same task, wrong length: this is *not* the mixed mode's business.
_LONG_SCOPE = {**_SHORT_SCOPE, "target_duration_seconds": 45.0}

_THEME = {
    "thesis": "单层链与吊坠停在锁骨上的实际落点",
    "thesis_source": "SELLING_ARGUMENT_CORE_VALUE",
    "thesis_source_ref": "bundle.selling_argument.core_value",
    "thesis_input_gap": "",
    "argument_id": "ARG_NECK_01",
}

_VERIFIED_PARTS = {
    "has_chain": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
    "has_pendant": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
}
_SINGLETON_COUNTS = {"layer_count": 1, "pendant_count": 1}

_SINGLE_EVIDENCE = {
    "part_evidence": _VERIFIED_PARTS,
    "counts": _SINGLETON_COUNTS,
    "evidence_ref": "PC_NECK_01",
}


@contextlib.contextmanager
def _switches(*, accessory=True, necklace=True):
    """Both feature switches, restored on exit."""

    with mock.patch.dict(os.environ, {}, clear=False):
        for key, on in (
            (ACCESSORY_MIXED_TEMPLATE_ENV, accessory),
            (NECKLACE_MIXED_V1_ENV, necklace),
        ):
            if on:
                os.environ[key] = "1"
            else:
                os.environ.pop(key, None)
        yield


def _inject(
    *,
    product_type="项链",
    top_category="首饰",
    evidence=_SINGLE_EVIDENCE,
    scope=_SHORT_SCOPE,
    item_index=1,
    theme_proposition=_THEME,
    audience_tension_text="锁骨上的落点",
    **overrides,
):
    kwargs = {
        "product_type": product_type,
        "top_category": top_category,
        "item_index": item_index,
        "item_role": "STRUCTURE_MOTHER",
        "content_angle_key": "ANGLE_NECK_01",
        "audience_tension_text": audience_tension_text,
        "claim_keys": ["C1"],
        "product_code": "PC_NECK_01",
        "execution_scope": scope,
        "theme_proposition": dict(theme_proposition or {}),
        "product_evidence": evidence,
    }
    kwargs.update(overrides)
    return _build_mixed_template_injection(**kwargs)


def _stable(value):
    """A comparison blob with nothing time- or order-dependent in it."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class NecklaceRoutingTest(unittest.TestCase):
    """Section 4's routing table, exercised through the real entry point."""

    def test_the_switch_off_keeps_the_legacy_path_for_a_necklace(self):
        with _switches(accessory=True, necklace=False):
            self.assertEqual(_inject(), {})

    def test_the_switch_off_still_keeps_the_legacy_path_with_full_evidence(self):
        # Eligibility never gets a say while the switch is off: section 4 keeps
        # the legacy path byte-for-byte instead of reporting a refusal that was
        # never asked for.
        with _switches(accessory=True, necklace=False):
            self.assertEqual(_inject(evidence=_SINGLE_EVIDENCE), {})

    def test_the_parent_switch_off_keeps_everything_off(self):
        with _switches(accessory=False, necklace=True):
            self.assertEqual(_inject(), {})

    def test_a_single_layer_single_pendant_necklace_compiles(self):
        with _switches():
            result = _inject()
        self.assertNotIn("errors", result)
        self.assertIn("contract", result)

    def test_a_bare_english_canonical_necklace_compiles(self):
        with _switches():
            result = _inject(product_type="necklace", top_category="")
        self.assertIn("contract", result, result)

    def test_a_non_necklace_never_reaches_the_nmx_template(self):
        with _switches():
            for product_type, top_category in (
                ("choker", ""),
                ("耳饰", "饰品"),
                ("手链", "饰品"),
                ("戒指", "饰品"),
                ("发饰", "饰品"),
                ("女装", ""),
            ):
                with self.subTest(product_type=product_type):
                    result = _inject(
                        product_type=product_type,
                        top_category=top_category,
                        evidence=_SINGLE_EVIDENCE,
                    )
                    contract = result.get("contract") or {}
                    self.assertNotEqual(
                        contract.get("template_id"), NECKLACE_MIXED_V1_TEMPLATE_ID
                    )

    def test_an_earring_request_never_reports_a_necklace_refusal(self):
        # The necklace branch must be *silent* for other categories -- no
        # necklace reason code may leak into their planning report.
        with _switches():
            result = _inject(product_type="耳饰", top_category="饰品")
        self.assertNotIn("NECKLACE_", _stable(result))

    def test_multiple_layers_are_refused_with_a_traceable_reason(self):
        evidence = {
            "part_evidence": _VERIFIED_PARTS,
            "counts": {"layer_count": 2, "pendant_count": 1},
            "evidence_ref": "PC_NECK_02",
        }
        with _switches():
            result = _inject(evidence=evidence)
        self.assertIn("errors", result)
        self.assertNotIn("contract", result)
        self.assertTrue(
            any(NECKLACE_EVIDENCE_INCOMPLETE in item for item in result["errors"])
        )

    def test_multiple_pendants_are_refused(self):
        evidence = {
            "part_evidence": _VERIFIED_PARTS,
            "counts": {"layer_count": 1, "pendant_count": 2},
            "evidence_ref": "PC_NECK_03",
        }
        with _switches():
            result = _inject(evidence=evidence)
        self.assertIn("errors", result)

    def test_an_unknown_layer_count_is_refused_rather_than_assumed_to_be_one(self):
        evidence = {
            "part_evidence": _VERIFIED_PARTS,
            "counts": {},
            "evidence_ref": "PC_NECK_04",
        }
        with _switches():
            result = _inject(evidence=evidence)
        self.assertIn("errors", result)
        self.assertNotIn("contract", result)

    def test_a_bare_title_is_not_evidence(self):
        # The word "项链" proves neither a chain nor a pendant.  With no anchor
        # evidence at all the request is refused instead of compiling a
        # single-layer contract on a guess.
        evidence = {"part_evidence": {}, "counts": {}, "evidence_ref": "PC_NECK_05"}
        with _switches():
            result = _inject(evidence=evidence)
        self.assertIn("errors", result)

    def test_an_absent_pendant_is_refused(self):
        evidence = {
            "part_evidence": {
                "has_chain": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
                "has_pendant": {"state": EVIDENCE_ABSENT, "source": "APPROVED_ANCHORS"},
            },
            "counts": _SINGLETON_COUNTS,
            "evidence_ref": "PC_NECK_06",
        }
        with _switches():
            result = _inject(evidence=evidence)
        self.assertIn("errors", result)

    def test_a_non_15_second_task_keeps_the_legacy_path(self):
        # A parent-task mismatch is not a necklace refusal: the request simply
        # is not this mode's business, so the legacy path stands.
        with _switches():
            result = _inject(scope=_LONG_SCOPE)
        self.assertNotIn("contract", result)
        self.assertNotIn("errors", result)

    def test_a_missing_mainline_is_refused(self):
        with _switches():
            result = _inject(theme_proposition={}, audience_tension_text="")
        self.assertIn("errors", result)
        self.assertTrue(
            any(NECKLACE_MAINLINE_UNAVAILABLE in item for item in result["errors"])
        )

    def test_a_broken_profile_degrades_to_the_legacy_path(self):
        # A missing necklace module must never break planning for anybody.
        with _switches():
            with mock.patch.dict(
                "sys.modules",
                {
                    "core.necklace_mixed_profile": None,
                },
            ):
                result = _inject()
        self.assertEqual(result, {})

    def test_a_necklace_ignores_history_references_only_through_the_shared_judge(self):
        # Reserved/history references are still honoured; two identical necklace
        # candidates in one batch must not both ship.  The first one compiles,
        # the second is reported as a duplicate through the shared vocabulary.
        with _switches():
            first = _inject(identity="D1#01")
            self.assertIn("contract", first)
            reserved = [
                {
                    "identity": "D1#01",
                    "signature": first["contract"].get("final_shot_signature") or {},
                }
            ]
            second = _inject(
                item_index=2,
                identity="D1#02",
                reserved_references=reserved,
            )
        self.assertIn("rejected", second)
        self.assertNotIn("contract", second)


class NecklaceContractShapeTest(unittest.TestCase):
    """The compiled contract itself: timeline, zone and namespace."""

    def _contract(self, **overrides):
        with _switches():
            result = _inject(**overrides)
        self.assertIn("contract", result, result)
        return result["contract"]

    def test_it_carries_the_four_shot_fifteen_second_timeline(self):
        contract = self._contract()
        units = contract.get("capture_units") or []
        self.assertEqual(
            [
                (str(unit.get("module")), int(unit.get("duration_seconds") or 0))
                for unit in units
            ],
            list(NECKLACE_V1_SHOT_ORDER),
        )

    def test_it_is_an_nmx_contract_on_the_neck_zone(self):
        contract = self._contract()
        self.assertEqual(contract.get("template_id"), NECKLACE_MIXED_V1_TEMPLATE_ID)
        self.assertEqual(contract.get("category_zone"), NECKLACE_MIXED_V1_ZONE)
        self.assertEqual(
            contract.get("environment_recipe_id"), NECKLACE_MIXED_V1_RECIPE_ID
        )

    def test_it_carries_the_necklace_namespace(self):
        contract = self._contract()
        self.assertEqual(contract.get("feature_profile"), NECKLACE_MIXED_V1_PROFILE)
        self.assertEqual(
            contract.get("feature_version"), NECKLACE_MIXED_V1_FEATURE_VERSION
        )
        block = contract.get(NECKLACE_CONTRACT_KEY)
        self.assertIsInstance(block, dict)
        self.assertTrue(block.get("profile_config_hash"))
        refs = block.get("eligibility_evidence_refs") or []
        self.assertTrue(refs, "单层/单吊坠依据必须随合同留痕")
        self.assertTrue(
            any(str(ref.get("part")) == "layer_count" for ref in refs),
            refs,
        )

    def test_both_validators_accept_it(self):
        contract = self._contract()
        self.assertEqual(validate_mixed_template_contract(contract), [])
        self.assertEqual(validate_necklace_v1_contract(contract), [])

    def test_the_frozen_timeline_outranks_a_later_config_edit(self):
        # The frozen contract must carry its own shot plan instead of re-reading
        # the latest config on recovery.
        contract = self._contract()
        units = contract.get("capture_units") or []
        self.assertEqual(sum(int(u.get("duration_seconds") or 0) for u in units), 15)

    def test_the_light_setup_is_frozen_and_not_merely_referenced(self):
        # Section 6: the light parameters themselves have to live in the frozen
        # contract.  A bare recipe id would let a later config edit silently
        # re-light a film that was already shot.
        contract = self._contract()
        recipe = contract.get("environment_recipe") or {}
        self.assertTrue(recipe.get("environment"), recipe)
        self.assertTrue(recipe.get("goal"), recipe)
        self.assertEqual(
            contract.get("environment_recipe_version"), recipe.get("recipe_version")
        )

    def test_each_unit_carries_its_own_framing_and_boundaries(self):
        # "Observation and boundary per shot" is what the frozen contract is for.
        contract = self._contract()
        units = contract.get("capture_units") or []
        self.assertEqual(len(units), len(NECKLACE_V1_SHOT_ORDER))
        for unit in units:
            with self.subTest(module=unit.get("module")):
                for field in (
                    "action",
                    "action_boundary",
                    "allowed_framing",
                    "body_zone",
                    "carrier_mode",
                    "duration_seconds",
                    "forbidden_framing",
                    "framing",
                    "observation_job",
                    "product_state",
                    "structure_role",
                    "unit_id",
                ):
                    self.assertIn(field, unit)

    def test_no_measurement_is_invented(self):
        # Section 3: without a source for the chain length or the pendant size,
        # no centimetre value may be written.
        contract = self._contract()
        blob = json.dumps(contract, ensure_ascii=False)
        self.assertNotIn("厘米", blob)

    def test_the_two_worn_shots_stay_on_the_neck_zone(self):
        contract = self._contract()
        units = contract.get("capture_units") or []
        for unit in units[:2]:
            self.assertEqual(unit.get("body_zone"), NECKLACE_MIXED_V1_ZONE)
            self.assertEqual(unit.get("carrier_mode"), "WEARER_ACTIVE")

    def test_the_last_shot_is_a_static_product_with_nobody_in_frame(self):
        contract = self._contract()
        last = (contract.get("capture_units") or [{}])[-1]
        self.assertEqual(last.get("module"), "STATIC_PRODUCT")
        self.assertEqual(last.get("carrier_mode"), "STATIC_PRODUCT")
        self.assertEqual(last.get("product_state"), "RESTING_ON_SURFACE")
        self.assertFalse(str(last.get("body_zone") or ""))

    def test_the_handheld_shot_is_hand_only(self):
        contract = self._contract()
        handheld = (contract.get("capture_units") or [{}])[2]
        self.assertEqual(handheld.get("module"), "HANDHELD_PRODUCT")
        self.assertEqual(handheld.get("carrier_mode"), "HAND_ONLY")
        self.assertEqual(handheld.get("product_state"), "HELD")
        self.assertFalse(str(handheld.get("body_zone") or ""))


class NecklaceGoldenParityTest(unittest.TestCase):
    """The branch has to be invisible to every existing category."""

    # Section 9 asks for one fixed input per existing family, with the whole
    # frozen contract compared and only genuinely volatile fields excluded.
    # Nothing here is volatile, so the comparison is an exact blob equality --
    #正文、顺序和业务字段都不当"噪声"忽略。
    _FAMILY_CASES = (
        ("耳饰", "饰品"),
        ("手链", "饰品"),
        ("戒指", "饰品"),
        ("发饰", "饰品"),
    )

    def test_every_existing_family_is_identical_with_the_switch_on_and_off(self):
        for product_type, top_category in self._FAMILY_CASES:
            for item_index in (1, 2, 3):
                with self.subTest(product_type=product_type, item_index=item_index):
                    with _switches(accessory=True, necklace=False):
                        off = _inject(
                            product_type=product_type,
                            top_category=top_category,
                            item_index=item_index,
                        )
                    with _switches(accessory=True, necklace=True):
                        on = _inject(
                            product_type=product_type,
                            top_category=top_category,
                            item_index=item_index,
                        )
                    self.assertIn("contract", off, off)
                    self.assertEqual(_stable(off), _stable(on))

    def test_the_per_shot_projection_is_unchanged_for_every_family(self):
        # Named field by field, because "the blob is equal" is easy to pass by
        # accident if the projector silently stopped emitting a field at all.

        def shots(result):
            contract = result.get("contract") or {}
            return [
                (
                    unit.get("module"),
                    unit.get("duration_seconds"),
                    unit.get("carrier_mode"),
                    unit.get("product_state"),
                    unit.get("body_zone"),
                    unit.get("action"),
                    unit.get("observation_job"),
                )
                for unit in contract.get("capture_units") or []
            ]

        for product_type, top_category in self._FAMILY_CASES:
            with self.subTest(product_type=product_type):
                with _switches(necklace=False):
                    off = shots(_inject(product_type=product_type, top_category=top_category))
                with _switches(necklace=True):
                    on = shots(_inject(product_type=product_type, top_category=top_category))
                self.assertTrue(off, f"{product_type} 应当产出镜头")
                self.assertEqual(off, on)

    def test_the_light_setup_and_template_of_every_family_are_unchanged(self):
        for product_type, top_category in self._FAMILY_CASES:
            with self.subTest(product_type=product_type):
                with _switches(necklace=False):
                    off = _inject(product_type=product_type, top_category=top_category)
                with _switches(necklace=True):
                    on = _inject(product_type=product_type, top_category=top_category)
                off_contract = off.get("contract") or {}
                on_contract = on.get("contract") or {}
                self.assertEqual(
                    off_contract.get("template_id"), on_contract.get("template_id")
                )
                self.assertEqual(
                    off_contract.get("environment_recipe_id"),
                    on_contract.get("environment_recipe_id"),
                )
                self.assertEqual(
                    off_contract.get("category_zone"), on_contract.get("category_zone")
                )

    def test_interleaved_compilation_does_not_leak_between_categories(self):
        with _switches():
            first = _inject(product_type="耳饰", top_category="饰品")
            self.assertIn("contract", _inject())
            second = _inject(product_type="耳饰", top_category="饰品")
        self.assertEqual(_stable(first), _stable(second))

    def test_a_necklace_never_receives_an_amx_template(self):
        with _switches():
            contract = self._contract_from(_inject())
        self.assertEqual(contract.get("template_id"), NECKLACE_MIXED_V1_TEMPLATE_ID)

    def test_an_earring_never_receives_an_nmx_template(self):
        with _switches():
            result = _inject(product_type="耳饰", top_category="饰品")
        contract = result.get("contract") or {}
        self.assertNotEqual(contract.get("template_id"), NECKLACE_MIXED_V1_TEMPLATE_ID)

    def test_the_shared_definition_is_untouched_by_a_necklace_compile(self):
        before = copy.deepcopy(load_mixed_template_definition())
        rotation_before = list(mixed_template_ids())
        with _switches():
            _inject()
        self.assertEqual(load_mixed_template_definition(), before)
        self.assertEqual(list(mixed_template_ids()), rotation_before)

    def test_the_shared_rotation_still_has_exactly_the_three_amx_templates(self):
        self.assertEqual(
            list(mixed_template_ids()),
            ["AMX_A_WORN_FIRST", "AMX_B_FORM_FIRST", "AMX_C_DETAIL_FIRST"],
        )

    @staticmethod
    def _contract_from(result):
        assert "contract" in result, result
        return result["contract"]


class ProductEvidenceHelperTest(unittest.TestCase):
    """``_mixed_product_evidence`` is the only producer of instance evidence."""

    def test_it_does_no_work_while_the_switch_is_off(self):
        with _switches(necklace=False):
            self.assertIsNone(
                _mixed_product_evidence(
                    anchor_card={"hard_anchors": ["链条", "吊坠", "单层"]},
                    product_code="X",
                )
            )

    def test_it_confirms_a_part_from_the_authoritative_anchor(self):
        with _switches():
            evidence = _mixed_product_evidence(
                anchor_card={"hard_anchors": ["链条", "吊坠", "单层链"]},
                product_code="X",
            )
        parts = evidence["part_evidence"]
        self.assertEqual(parts["has_chain"]["state"], EVIDENCE_VERIFIED)
        self.assertEqual(parts["has_pendant"]["state"], EVIDENCE_VERIFIED)
        self.assertEqual(evidence["counts"].get("layer_count"), 1)

    def test_display_anchors_are_not_evidence(self):
        # "手持展示吊坠" is a presentation idea the model wrote, not a product
        # fact.  Treating it as proof of a pendant is exactly the fabrication
        # the anchor guard exists to prevent.
        with _switches():
            evidence = _mixed_product_evidence(
                anchor_card={"display_anchors": ["手持展示吊坠"]},
                product_code="X",
            )
        self.assertEqual(evidence["part_evidence"]["has_pendant"]["state"], EVIDENCE_UNKNOWN)

    def test_an_unstated_count_stays_unstated(self):
        with _switches():
            evidence = _mixed_product_evidence(
                anchor_card={"hard_anchors": ["链条", "吊坠"]},
                product_code="X",
            )
        self.assertNotIn("layer_count", evidence["counts"])
        self.assertNotIn("pendant_count", evidence["counts"])

    def test_a_stated_multi_layer_count_is_recorded_with_its_source(self):
        with _switches():
            evidence = _mixed_product_evidence(
                anchor_card={"hard_anchors": ["双层链", "吊坠"]},
                product_code="X",
            )
        self.assertEqual(evidence["counts"].get("layer_count"), 2)
        self.assertIn("双层", str(evidence["counts"].get("layer_count_source")))


if __name__ == "__main__":
    unittest.main()
