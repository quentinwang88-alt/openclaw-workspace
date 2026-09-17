"""Contracts for the authored mixed-display accessory template (WP1).

These tests are deliberately additive: they never touch the database, never
call a model, and assert that the new mode defaults to off so existing
apparel / scarf / long-form behaviour stays unchanged.
"""

import json
import os
import unittest
from unittest import mock

from core.accessory_mixed_templates import (
    ACCESSORY_MIXED_TEMPLATE_ENV,
    ACCESSORY_MIXED_TEMPLATE_PROFILE,
    ACCESSORY_MIXED_TEMPLATE_SCHEMA,
    ERR_MIXED_THEME_INPUT_GAP,
    EVIDENCE_ABSENT,
    EVIDENCE_STATES,
    EVIDENCE_UNKNOWN,
    EVIDENCE_VERIFIED,
    MIXED_THESIS_INPUT_GAP_READABILITY,
    PAIRING_OUTPUT_TERMS,
    SINGLE_OUTPUT_TERMS,
    accessory_mixed_template_enabled,
    compile_mixed_template_contract,
    contains_face_wording,
    contains_pairing_claim,
    get_environment_recipe,
    is_accessory_mixed_type,
    is_readable_theme_proposition,
    load_mixed_template_definition,
    mixed_product_fact_signature,
    mixed_semantic_signature,
    mixed_supported_canonical_types,
    mixed_template_ids,
    module_framing_projection,
    physical_subtype_rule,
    project_module_framing,
    render_mixed_blueprint_guidance,
    resolve_mixed_zone,
    resolve_part_gated_actions,
    resolve_theme_proposition,
    sanitize_pairing_claims,
    scrub_no_face_prose_in_place,
    select_environment_recipe_id,
    subtype_structure_facts,
    summarize_mixed_contract,
    theme_proposition,
    unit_carrier_map,
    validate_mixed_template_contract,
    worn_body_framing,
)
from core.original_batch_allocator import (
    _build_mixed_template_injection,
    _mixed_theme_proposition,
)

# One representative product per supported family, expressed as ordinary
# operator input (alias) rather than canonical identifiers.
SAMPLES = (
    ("耳饰", "耳饰", "earring"),
    ("手链", "手链", "bracelet"),
    ("手镯", "手镯", "bangle"),
    ("细手镯", "细手镯", "slim_bangle"),
    ("戒指", "戒指", "ring"),
    ("发夹", "发夹", "hair_clip"),
    ("抓夹", "抓夹", "claw_clip"),
    ("发簪", "发簪", "hair_pin"),
    ("发圈", "大肠发圈", "scrunchie"),
    ("发箍", "发箍", "headband"),
)

REQUIRED_MODULES = (
    "WORN_DETAIL",
    "HANDHELD_PRODUCT",
    "STATIC_PRODUCT",
    "WORN_RELATION",
)

_PUNCTUATION = "、，。；：（）() "


def _character_bigrams(texts):
    """Character bigrams of a framing vocabulary, punctuation removed.

    Used to show that a per-module frame is still worded in its own category's
    vocabulary: leaking "耳侧" into a wrist rule shares no bigram with wrist
    wording, while restating "耳侧与颈侧关系" as "耳侧与耳垂的关系" still does.
    """

    grams = set()
    for text in texts or ():
        cleaned = "".join(ch for ch in str(text) if ch not in _PUNCTUATION)
        grams.update(cleaned[i : i + 2] for i in range(len(cleaned) - 1))
    return grams


def _compile(product_type, top_category, template_id):
    return compile_mixed_template_contract(
        product_type=product_type,
        top_category=top_category,
        template_id=template_id,
        content_theme={
            "theme_id": "TH_TEST",
            "thesis": "验证用主题",
            "candidate_role": "PRIMARY",
            "evidence_refs": ["EVIDENCE_1"],
        },
        product_identity_ref="LOCK_TEST",
    )


class MixedTemplateGateTest(unittest.TestCase):
    def test_mode_is_off_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(ACCESSORY_MIXED_TEMPLATE_ENV, None)
            self.assertFalse(accessory_mixed_template_enabled())

    def test_explicit_override_wins(self):
        self.assertTrue(accessory_mixed_template_enabled(True))
        self.assertFalse(accessory_mixed_template_enabled(False))


class MixedTemplateTypeResolutionTest(unittest.TestCase):
    def test_registry_aliases_resolve_to_expected_zone(self):
        for label, text, expected in SAMPLES:
            with self.subTest(label=label):
                zone, canonical = resolve_mixed_zone(text, text)
                self.assertEqual(canonical, expected)
                self.assertIsNotNone(zone)
                self.assertTrue(is_accessory_mixed_type(text, text))

    def test_non_accessory_types_are_rejected(self):
        for text in ("外套", "T恤", "连衣裙", "围巾", "丝巾", "头巾", "帽子"):
            with self.subTest(text=text):
                zone, _ = resolve_mixed_zone(text, "")
                self.assertIsNone(zone)

    def test_unknown_type_is_rejected_not_guessed(self):
        # Conservative: an unregistered string must not be coerced into an
        # accessory family, because that would silently enable face-free
        # framing for the wrong merchandise.
        zone, _ = resolve_mixed_zone("某种全新饰品", "")
        self.assertIsNone(zone)

    def test_supported_canonical_set_is_the_registry_subset(self):
        supported = mixed_supported_canonical_types()
        self.assertIn("earring", supported)
        self.assertIn("hair_pin", supported)
        # Scarf and apparel must never enter the accessory mixed mode.
        self.assertNotIn("silk_scarf", supported)
        self.assertNotIn("outerwear", supported)


class MixedTemplateCompileTest(unittest.TestCase):
    def test_every_family_and_template_compiles_to_four_units_in_fifteen_seconds(self):
        for label, text, _canonical in SAMPLES:
            for template_id in mixed_template_ids():
                with self.subTest(label=label, template=template_id):
                    contract = _compile(text, text, template_id)
                    summary = summarize_mixed_contract(contract)
                    self.assertEqual(validate_mixed_template_contract(contract), [])
                    self.assertEqual(summary["total_duration_seconds"], 15)
                    self.assertEqual(len(summary["modules"]), 4)
                    self.assertEqual(set(summary["modules"]), set(REQUIRED_MODULES))

    def test_all_three_display_methods_are_present_in_every_contract(self):
        for template_id in mixed_template_ids():
            with self.subTest(template=template_id):
                contract = _compile("耳饰", "耳饰", template_id)
                modules = {unit["module"] for unit in contract["capture_units"]}
                self.assertTrue({"WORN_DETAIL", "WORN_RELATION"} & modules)
                self.assertIn("HANDHELD_PRODUCT", modules)
                self.assertIn("STATIC_PRODUCT", modules)

    def test_worn_units_are_face_free_and_prove_wearing(self):
        contract = _compile("耳饰", "耳饰", mixed_template_ids()[0])
        worn = [
            unit for unit in contract["capture_units"]
            if unit["module"] in {"WORN_DETAIL", "WORN_RELATION"}
        ]
        self.assertEqual(len(worn), 2)
        for unit in worn:
            self.assertEqual(unit["carrier_mode"], "WEARER_ACTIVE")
            self.assertEqual(unit["product_state"], "ALREADY_WORN")
            self.assertEqual(unit["face_policy"], "NO_FACE")
            for banned in ("眼睛入画", "鼻子入画", "嘴部入画"):
                self.assertIn(banned, unit["forbidden_framing"])

    def test_the_two_worn_units_have_different_observation_jobs(self):
        for template_id in mixed_template_ids():
            with self.subTest(template=template_id):
                contract = _compile("戒指", "戒指", template_id)
                jobs = [
                    unit["observation_job"]
                    for unit in contract["capture_units"]
                    if unit["module"] in {"WORN_DETAIL", "WORN_RELATION"}
                ]
                self.assertEqual(len(jobs), 2)
                self.assertNotEqual(jobs[0], jobs[1])

    def test_handheld_does_not_ask_rigid_jewellery_to_unfold(self):
        # A rigid earring must never be instructed to "unfold" itself.
        contract = _compile("耳饰", "耳饰", mixed_template_ids()[0])
        handheld = next(
            unit for unit in contract["capture_units"]
            if unit["module"] == "HANDHELD_PRODUCT"
        )
        self.assertNotIn("展开", handheld["action"])
        self.assertIn("对刚性装饰做展开动作", handheld["action_boundary"])

    def test_direct_cut_between_states_is_allowed_but_in_shot_transfer_is_not(self):
        contract = _compile("手链", "手链", mixed_template_ids()[0])
        policy = contract["cut_policy"]
        self.assertTrue(policy["allow_direct_cut_between_states"])
        self.assertFalse(policy["allow_in_shot_state_change"])
        edits = [unit["edit_before"] for unit in contract["capture_units"]]
        self.assertEqual(edits[0], "START")
        self.assertTrue(all(item == "DIRECT_CUT" for item in edits[1:]))

    def test_contract_records_authored_source_never_real_observation(self):
        contract = _compile("发夹", "发夹", mixed_template_ids()[0])
        self.assertEqual(contract["source_mode"], "AUTHORED_TEMPLATE")
        self.assertEqual(contract["schema_version"], ACCESSORY_MIXED_TEMPLATE_SCHEMA)
        self.assertEqual(contract["execution_profile"], ACCESSORY_MIXED_TEMPLATE_PROFILE)
        self.assertNotIn("video_id", contract)
        self.assertNotIn("cluster_id", contract)

    def test_unsupported_product_raises_instead_of_silently_falling_back(self):
        with self.assertRaises(ValueError):
            _compile("外套", "外套", mixed_template_ids()[0])
        with self.assertRaises(ValueError):
            _compile("围巾", "围巾", mixed_template_ids()[0])

    def test_unknown_template_id_raises(self):
        with self.assertRaises(ValueError):
            _compile("戒指", "戒指", "AMX_NOT_A_TEMPLATE")


class MixedTemplateValidationTest(unittest.TestCase):
    def setUp(self):
        self.base = _compile("耳饰", "耳饰", mixed_template_ids()[0])

    def _mutated(self, **overrides):
        data = dict(self.base)
        data.update(overrides)
        return data

    def test_empty_contract_is_blocked(self):
        self.assertEqual(validate_mixed_template_contract({}), ["MIXED_CONTRACT_MISSING"])

    def test_face_policy_violation_is_blocked(self):
        errors = validate_mixed_template_contract(self._mutated(face_policy="FACE_OK"))
        self.assertIn("MIXED_CONTRACT_FACE_POLICY_NOT_NO_FACE", errors)

    def test_non_mixed_global_carrier_is_blocked(self):
        errors = validate_mixed_template_contract(
            self._mutated(global_carrier="WEARER_ACTIVE")
        )
        self.assertIn("MIXED_CONTRACT_GLOBAL_CARRIER_NOT_MIXED", errors)

    def test_schema_and_profile_mismatch_are_blocked(self):
        errors = validate_mixed_template_contract(self._mutated(schema_version="x"))
        self.assertIn("MIXED_CONTRACT_SCHEMA_MISMATCH", errors)
        errors = validate_mixed_template_contract(
            self._mutated(execution_profile="OTHER")
        )
        self.assertIn("MIXED_CONTRACT_PROFILE_MISMATCH", errors)

    def test_missing_module_is_blocked(self):
        data = self._mutated(capture_units=self.base["capture_units"][:2])
        errors = validate_mixed_template_contract(data)
        self.assertIn("MIXED_CONTRACT_MODULE_MISSING:STATIC_PRODUCT", errors)
        self.assertIn("MIXED_CONTRACT_TIMELINE_MISMATCH:6!=15", errors)

    def test_missing_display_method_is_blocked(self):
        units = [
            dict(unit) for unit in self.base["capture_units"]
            if unit["module"] != "STATIC_PRODUCT"
        ]
        errors = validate_mixed_template_contract(self._mutated(capture_units=units))
        self.assertIn("MIXED_CONTRACT_DISPLAY_METHOD_MISSING:STATIC", errors)

    def test_unit_carrier_conflict_is_blocked(self):
        units = [dict(unit) for unit in self.base["capture_units"]]
        units[1]["carrier_mode"] = "WEARER_ACTIVE"
        errors = validate_mixed_template_contract(self._mutated(capture_units=units))
        self.assertTrue(any("UNIT_CARRIER_CONFLICT" in item for item in errors))

    def test_duplicate_unit_id_is_blocked(self):
        units = [dict(unit) for unit in self.base["capture_units"]]
        units[1]["unit_id"] = units[0]["unit_id"]
        errors = validate_mixed_template_contract(self._mutated(capture_units=units))
        self.assertTrue(any("UNIT_ID_DUPLICATE" in item for item in errors))

    def test_theme_required_fields_are_enforced(self):
        data = self._mutated(
            content_theme={"theme_id": "", "candidate_role": "PRIMARY", "thesis": ""}
        )
        errors = validate_mixed_template_contract(data)
        self.assertIn("MIXED_CONTRACT_THEME_FIELD_MISSING:theme_id", errors)
        self.assertIn("MIXED_CONTRACT_THEME_FIELD_MISSING:thesis", errors)

    def test_unknown_module_is_blocked(self):
        units = [dict(unit) for unit in self.base["capture_units"]]
        units[0]["module"] = "NOT_A_MODULE"
        errors = validate_mixed_template_contract(self._mutated(capture_units=units))
        self.assertTrue(any("UNKNOWN_MODULE" in item for item in errors))


class MixedTemplateEnvironmentTest(unittest.TestCase):
    def test_recipes_rotate_deterministically(self):
        first = select_environment_recipe_id(0)
        self.assertEqual(first, select_environment_recipe_id(0))
        self.assertNotEqual(first, select_environment_recipe_id(1))
        self.assertNotEqual(
            select_environment_recipe_id(1), select_environment_recipe_id(2)
        )
        for index in range(3):
            recipe = get_environment_recipe(select_environment_recipe_id(index))
            self.assertTrue(recipe["environment"])
            self.assertTrue(recipe["goal"])

    def test_templates_rotate_deterministically(self):
        ids = mixed_template_ids()
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids[0], "AMX_A_WORN_FIRST")
        self.assertEqual(ids[1], "AMX_B_FORM_FIRST")
        self.assertEqual(ids[2], "AMX_C_DETAIL_FIRST")


class MixedTemplateProjectionTest(unittest.TestCase):
    def test_unit_carrier_map_projects_one_entry_per_unit(self):
        contract = _compile("手镯", "手镯", mixed_template_ids()[1])
        mapping = unit_carrier_map(contract)
        self.assertEqual(len(mapping), 4)
        self.assertEqual(mapping["CU_02"], "STATIC_PRODUCT")

    def test_summary_exposes_review_fields(self):
        contract = _compile("耳饰", "耳饰", mixed_template_ids()[2])
        summary = summarize_mixed_contract(contract)
        self.assertEqual(summary["template_id"], "AMX_C_DETAIL_FIRST")
        self.assertEqual(summary["candidate_role"], "PRIMARY")
        self.assertEqual(len(summary["units"]), 4)
        self.assertTrue(summary["environment_recipe_id"])


class MixedTemplateInjectionTest(unittest.TestCase):
    """The allocator must freeze the contract only when the gate is on."""

    def _call(self, **overrides):
        kwargs = dict(
            product_type="耳饰",
            top_category="耳饰",
            item_index=1,
            item_role="STRUCTURE_MOTHER",
            content_angle_key="ARG_1",
            audience_tension_text="不知道耳饰怎么搭",
            claim_keys=["CLM_1"],
            product_code="P1",
        )
        kwargs.update(overrides)
        return _build_mixed_template_injection(**kwargs)

    def test_gate_off_injects_nothing(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "0"}, clear=False):
            self.assertEqual(self._call(), {})

    def test_gate_on_injects_a_valid_contract(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            result = self._call()
        self.assertIn("contract", result)
        self.assertNotIn("errors", result)
        self.assertEqual(validate_mixed_template_contract(result["contract"]), [])

    def test_non_accessory_injects_nothing_even_when_gate_is_on(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            for text in ("外套", "T恤", "围巾", "丝巾", "头巾"):
                with self.subTest(text=text):
                    self.assertEqual(
                        self._call(product_type=text, top_category=text), {}
                    )

    def test_item_role_maps_to_candidate_role(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            primary = self._call(item_role="STRUCTURE_MOTHER")
            variant = self._call(item_role="CONTENT_VARIANT")
            hook_variant = self._call(item_role="HOOK_VARIANT")
        self.assertEqual(
            primary["contract"]["content_theme"]["candidate_role"], "PRIMARY"
        )
        self.assertEqual(
            variant["contract"]["content_theme"]["candidate_role"], "EXECUTION_VARIANT"
        )
        self.assertEqual(
            hook_variant["contract"]["content_theme"]["candidate_role"],
            "EXECUTION_VARIANT",
        )

    def test_templates_rotate_across_items(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            ids = [
                self._call(item_index=index)["contract"]["template_id"]
                for index in (1, 2, 3, 4)
            ]
        self.assertEqual(
            ids,
            [
                "AMX_A_WORN_FIRST",
                "AMX_B_FORM_FIRST",
                "AMX_C_DETAIL_FIRST",
                "AMX_A_WORN_FIRST",
            ],
        )

    def test_theme_reuses_existing_content_semantics(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            theme = self._call()["contract"]["content_theme"]
        self.assertEqual(theme["theme_id"], "ARG_1")
        self.assertEqual(theme["parent_theme_id"], "ARG_1")
        self.assertEqual(theme["thesis"], "不知道耳饰怎么搭")
        self.assertEqual(theme["approved_claim_refs"], ["CLM_1"])
        self.assertEqual(theme["evidence_refs"], ["CLM_1"])

    def test_contract_travels_inside_the_existing_extension_slot(self):
        # The contract must live under category_execution_extension so the
        # existing frozen package stays the single authority.
        contract = None
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            contract = self._call()["contract"]
        self.assertEqual(contract["source_mode"], "AUTHORED_TEMPLATE")
        self.assertNotIn("video_id", contract)
        self.assertNotIn("cluster_id", contract)
        self.assertEqual(contract["product_identity_ref"], "P1")


class PairingLeakGuardTest(unittest.TestCase):
    """Blueprint guidance must never leak an unauthorised pairing claim.

    The accessory identity gate hard-fails an earring script whose authored
    content names a pairing relation the anchor card never granted.  Guidance is
    fed to the model as prose, so a guard sentence that spells those terms out
    gets paraphrased into the storyboard and destroys an otherwise valid,
    already-paid generation.
    """

    PAIRING_TERMS = PAIRING_OUTPUT_TERMS + SINGLE_OUTPUT_TERMS

    def _contract(self):
        return compile_mixed_template_contract(
            product_type="耳饰",
            top_category="耳饰",
            template_id="AMX_A_WORN_FIRST",
            environment_recipe_id="WINDOW_LIGHT_WOOD",
            product_identity_ref="P1",
            content_theme={
                "theme_id": "ARG_1",
                "parent_theme_id": "ARG_1",
                "candidate_role": "PRIMARY",
                "thesis": "不知道耳饰怎么搭",
                "approved_claim_refs": ["CLM_1"],
                "evidence_refs": ["CLM_1"],
            },
        )

    def test_unauthorised_anchor_produces_no_pairing_wording(self):
        lines = render_mixed_blueprint_guidance(
            self._contract(), identity_authority={"pairing_mode": "UNAVAILABLE"}
        )
        self.assertTrue(lines)
        for line in lines:
            with self.subTest(line=line):
                self.assertEqual([t for t in self.PAIRING_TERMS if t in line], [])

    def test_missing_authority_behaves_like_unauthorised(self):
        lines = render_mixed_blueprint_guidance(self._contract())
        for line in lines:
            self.assertEqual([t for t in self.PAIRING_TERMS if t in line], [])

    def test_authorised_relations_are_stated_declaratively(self):
        paired = "\n".join(
            render_mixed_blueprint_guidance(
                self._contract(), identity_authority={"pairing_mode": "PAIR"}
            )
        )
        single = "\n".join(
            render_mixed_blueprint_guidance(
                self._contract(), identity_authority={"pairing_mode": "SINGLE"}
            )
        )
        self.assertIn("授权锚点已明确成对关系", paired)
        self.assertIn("授权锚点已明确单件关系", single)
        # An authorised PAIR script may not assert the opposite relation.
        self.assertEqual([t for t in SINGLE_OUTPUT_TERMS if t in paired], [])
        self.assertEqual([t for t in PAIRING_OUTPUT_TERMS if t in single], [])

    def test_stale_contract_text_is_sanitized_before_it_reaches_the_model(self):
        """Contracts frozen before the template text was neutralised still work."""

        contract = self._contract()
        for unit in contract["capture_units"]:
            unit["action"] = "手指稳定承托单只耳饰，小幅转动展示正面与侧面轮廓"
            unit["quantity_rule"] = "一对耳饰的耳侧镜头只看到一只，不等于商品数量变成单只。"
        lines = render_mixed_blueprint_guidance(contract)
        for line in lines:
            with self.subTest(line=line):
                self.assertEqual([t for t in self.PAIRING_TERMS if t in line], [])
        joined = "\n".join(lines)
        self.assertIn("手指稳定承托耳饰", joined)
        self.assertNotIn("一对耳饰的耳侧镜头", joined)

    def test_rule_without_pairing_wording_is_still_delivered(self):
        contract = self._contract()
        for unit in contract["capture_units"]:
            unit["quantity_rule"] = "细链手链与刚性手镯不得互相冒充。"
        joined = "\n".join(render_mixed_blueprint_guidance(contract))
        self.assertIn("细链手链与刚性手镯不得互相冒充", joined)

    def test_helpers_are_consistent(self):
        self.assertTrue(contains_pairing_claim("承托单只耳饰"))
        self.assertFalse(contains_pairing_claim("承托耳饰本体"))
        self.assertEqual(sanitize_pairing_claims("承托单只耳饰"), "承托耳饰")
        self.assertEqual(sanitize_pairing_claims("承托耳饰本体"), "承托耳饰本体")


class ShippedTemplateTextIsNeutralTest(unittest.TestCase):
    """The authored config must not seed a pairing claim in the first place.

    Sanitising at render time keeps old frozen contracts usable, but the root
    cause was authored text: the earring handheld action said "单只耳饰", which
    the model copied verbatim into the storyboard and tripped the identity gate.
    """

    def test_no_shipped_string_names_a_pairing_relation_in_ear_guidance(self):
        definition = load_mixed_template_definition()
        ear = (definition.get("category_rules") or {}).get("EAR") or {}
        self.assertTrue(ear)
        watched = [
            (f"action_by_module.{key}", value)
            for key, value in (ear.get("action_by_module") or {}).items()
        ]
        watched.append(("base_action", ear.get("base_action")))
        watched.append(("quantity_rule", ear.get("quantity_rule")))
        for path, value in watched:
            with self.subTest(path=path):
                self.assertEqual(
                    contains_pairing_claim(value),
                    False,
                    f"{path} 不得出现单双措辞：{value}",
                )

    def test_every_shipped_module_action_is_neutral(self):
        """A zone action is copied into the storyboard, so none may claim a pairing."""

        definition = load_mixed_template_definition()
        for zone, rules in (definition.get("category_rules") or {}).items():
            for key, value in (rules.get("action_by_module") or {}).items():
                with self.subTest(zone=zone, module=key):
                    self.assertFalse(
                        contains_pairing_claim(value),
                        f"{zone}.{key} 不得出现单双措辞：{value}",
                    )


class NoFaceContractDoesNotInstructTheForbiddenTest(unittest.TestCase):
    """A NO_FACE contract must not instruct what it forbids.

    ``render_mixed_blueprint_guidance`` copies each unit's authored ``action``
    into the blueprint prompt verbatim.  The EAR zone forbids eyes / nose /
    mouth in frame, yet its WORN_RELATION action used to read "小幅头肩变化"
    -- so one prompt told the model to shoot head-and-shoulders *and* banned
    the wording for it.  Root cause is the authored text, exactly like the
    pairing wording above; the render path scrubs it so contracts frozen
    before the fix stay usable.
    """

    def _contract(self):
        return compile_mixed_template_contract(
            product_type="耳饰",
            top_category="耳饰",
            template_id="AMX_A_WORN_FIRST",
            environment_recipe_id="WINDOW_LIGHT_WOOD",
            product_identity_ref="P1",
            content_theme={
                "theme_id": "ARG_1",
                "parent_theme_id": "ARG_1",
                "candidate_role": "PRIMARY",
                "thesis": "不知道耳饰怎么搭",
                "approved_claim_refs": ["CLM_1"],
                "evidence_refs": ["CLM_1"],
            },
        )

    def test_shipped_ear_guidance_names_no_part_of_the_face(self):
        ear = (load_mixed_template_definition().get("category_rules") or {}).get("EAR") or {}
        self.assertTrue(ear)
        watched = [
            (f"action_by_module.{key}", value)
            for key, value in (ear.get("action_by_module") or {}).items()
        ]
        watched.append(("base_action", ear.get("base_action")))
        watched.append(("quantity_rule", ear.get("quantity_rule")))
        for key, value in (ear.get("distinct_jobs") or {}).items():
            watched.append((f"distinct_jobs.{key}", value))
        for index, value in enumerate(ear.get("allowed_framing") or []):
            watched.append((f"allowed_framing[{index}]", value))
        # ``forbidden_framing`` is deliberately excluded: those entries *are*
        # the ban, so they have to name what is banned.
        for path, value in watched:
            with self.subTest(path=path):
                self.assertFalse(
                    contains_face_wording(value),
                    f"{path} 不得把脸写进画面：{value}",
                )

    def test_stale_contract_action_is_scrubbed_before_it_reaches_the_model(self):
        contract = self._contract()
        self.assertEqual(contract.get("face_policy"), "NO_FACE")
        for unit in contract["capture_units"]:
            if unit.get("module") == "WORN_RELATION":
                unit["action"] = "小幅头肩变化，展示耳饰与颈侧、领口的搭配关系"

        lines = render_mixed_blueprint_guidance(contract)
        per_shot = [line for line in lines if line.startswith("- 镜头")]
        self.assertTrue(per_shot)
        for line in per_shot:
            with self.subTest(line=line):
                self.assertFalse(contains_face_wording(line), line)
        joined = "\n".join(lines)
        self.assertNotIn("小幅头肩变化", joined)
        self.assertIn("小幅颈肩变化", joined)

    def test_a_non_no_face_policy_keeps_the_authored_wording(self):
        """The scrub must be scoped to NO_FACE, not applied blanket."""

        contract = self._contract()
        for unit in contract["capture_units"]:
            if unit.get("module") == "WORN_RELATION":
                unit["action"] = "小幅头肩变化，展示耳饰与颈侧、领口的搭配关系"
        contract["face_policy"] = "PARTIAL_FACE"

        joined = "\n".join(render_mixed_blueprint_guidance(contract))
        self.assertIn("小幅头肩变化", joined)

    def test_guidance_bans_selfie_wording_and_explains_the_capture_mode(self):
        """The ban has to name "自拍" *and* explain ``CREATOR_SELF_SHOT``.

        A bare wording ban is not enough here: the token name itself reads like
        "selfie", so the prompt has to say what the token actually means
        (who owns the camera / how the takes are organised), or the model
        paraphrases it back into a framing claim.
        """

        lines = render_mixed_blueprint_guidance(self._contract())
        joined = "\n".join(lines)
        self.assertIn("“自拍”", joined)
        self.assertIn("CREATOR_SELF_SHOT", joined)
        self.assertIn("不是取景方式", joined)
        # The existing bans must survive the edit.
        self.assertIn("“半脸”", joined)
        self.assertIn("全片不露脸", joined)

    def test_scrub_rewrites_model_prose_without_dropping_anything(self):
        """The assembled-script boundary rewrites text leaves only.

        ``_no_face_deep_clean`` (the adapter guard) may drop a list entry that
        still names the face.  A compiled script cannot afford that: it has a
        fixed schema and the renderer walks it positionally.  Every key and
        every list slot must survive the scrub, only the prose may change.
        """

        script = {
            "production_design": {
                "capture_mode": "CREATOR_SELF_SHOT",
                "scene": {
                    "subject_position": (
                        "手部与商品在台面柔光范围内完成前两段；"
                        "人物随后站在鞋柜旁的自然自拍范围内补录耳侧佩戴画面"
                    ),
                    "phone_placement": "两部手机分别固定在台面与鞋柜旁",
                },
            },
            "storyboard": [
                {"shot_no": 1, "module": "HANDHELD_PRODUCT", "camera": "固定近景"},
                {
                    "shot_no": 4,
                    "module": "WORN_RELATION",
                    "camera": "沿用第二手机布置重新开录；人物在同一自拍范围内轻微调整位置，使颈侧和方领自然进入构图。",
                    "visual_content": "耳侧与颈侧近景，只保留少量下颌边缘",
                },
            ],
        }
        scrubbed = json.loads(json.dumps(script, ensure_ascii=False))
        scrub_no_face_prose_in_place(scrubbed)

        self.assertEqual(
            "手部与商品在台面柔光范围内完成前两段；"
            "人物随后站在鞋柜旁的自然同机位取景范围内补录耳侧佩戴画面",
            scrubbed["production_design"]["scene"]["subject_position"],
        )
        self.assertIn(
            "同一取景范围内",
            scrubbed["storyboard"][1]["camera"],
        )
        self.assertIn("少量下颌边缘", scrubbed["storyboard"][1]["visual_content"])
        # Structure is untouched: same keys, same shot count, same enum tokens.
        self.assertEqual(len(script["storyboard"]), len(scrubbed["storyboard"]))
        self.assertEqual(
            set(script["production_design"]), set(scrubbed["production_design"])
        )
        self.assertEqual(
            "CREATOR_SELF_SHOT", scrubbed["production_design"]["capture_mode"]
        )
        self.assertEqual("HANDHELD_PRODUCT", scrubbed["storyboard"][0]["module"])

    def test_scrub_mutates_in_place_so_the_video_brief_stays_in_sync(self):
        """``video_generation_brief`` holds these objects *by reference*.

        Returning a cleaned copy instead of mutating would leave the brief -- 
        which is built after the scrub and copied straight into the prompt --
        holding the pre-scrub text.
        """

        production_design = {
            "scene": {"subject_position": "人物站在自然自拍范围内补录"}
        }
        # Mirrors how the brief is built: a reference, never a copy.
        video_brief = {"production_design": production_design}

        scrub_no_face_prose_in_place(production_design)

        position = video_brief["production_design"]["scene"]["subject_position"]
        self.assertNotIn("自拍", position)
        self.assertIn("同机位取景范围", position)


class ShippedModuleFramingIsCompleteTest(unittest.TestCase):
    """Every category x module pair must be answerable from the config alone.

    Review #5: framing used to be one body-zone list copied onto all four
    modules, so a static shot inherited the wrist zone and every category's
    shared prose was written in ear words.
    """

    def test_every_module_has_a_framing_rule(self):
        rules = load_mixed_template_definition()["module_framing_rules"]
        self.assertEqual(set(rules), set(REQUIRED_MODULES))
        categories = load_mixed_template_definition()["category_rules"]
        for module, spec in rules.items():
            with self.subTest(module=module):
                self.assertTrue(spec["view_scope"])
                self.assertTrue(spec["view_label"])
                self.assertTrue(spec["state_boundary"])
                self.assertIn(spec["framing_source"], {"MODULE", "CATEGORY_ZONE"})
                if spec["framing_source"] == "MODULE":
                    # A module-sourced shot owns its frames outright: it must
                    # state them here, because no category can answer for it.
                    self.assertTrue(spec["allowed_framing"])
                    self.assertTrue(spec["forbidden_framing"])
                else:
                    # A category-sourced shot borrows the zone's frames, so the
                    # config must instead name the key every category answers.
                    key = spec["framing_key"]
                    self.assertIn(key, {"WORN_DETAIL", "WORN_RELATION"})
                    for zone, category in categories.items():
                        with self.subTest(module=module, zone=zone):
                            self.assertTrue(category["module_framing"][key])
                            self.assertTrue(category["forbidden_framing"])

    def test_every_category_has_both_worn_frames_and_a_crop_rule(self):
        for zone, rules in load_mixed_template_definition()["category_rules"].items():
            with self.subTest(zone=zone):
                per_module = rules["module_framing"]
                self.assertEqual(
                    set(per_module), {"WORN_DETAIL", "WORN_RELATION"}
                )
                for module, allowed in per_module.items():
                    self.assertTrue(allowed)
                    # The category frames must be drawn from the zone's own
                    # vocabulary, not invented per module.  Exact equality is
                    # the wrong bar -- a relation shot legitimately restates a
                    # zone frame in its own words -- so require that every
                    # frame still shares wording with the zone it belongs to.
                    zone_words = _character_bigrams(rules["allowed_framing"])
                    for frame in allowed:
                        with self.subTest(zone=zone, module=module, frame=frame):
                            self.assertTrue(
                                _character_bigrams([frame]) & zone_words,
                                f"{zone}/{module} 的取景用语不属于该类目区域词表：{frame}",
                            )
                self.assertTrue(rules["visible_quantity_rule"])
                self.assertFalse(contains_pairing_claim(rules["visible_quantity_rule"]))
                self.assertFalse(contains_face_wording(rules["visible_quantity_rule"]))

    def test_project_module_framing_is_total_over_the_config(self):
        definition = load_mixed_template_definition()
        for canonical in definition["canonical_type_to_zone"]:
            for module in REQUIRED_MODULES:
                with self.subTest(canonical=canonical, module=module):
                    framing = project_module_framing(canonical, module)
                    self.assertTrue(framing["allowed_framing"])
                    self.assertTrue(framing["forbidden_framing"])
                    self.assertTrue(framing["state_boundary"])
                    if module in {"WORN_DETAIL", "WORN_RELATION"}:
                        self.assertTrue(framing["body_zone"])
                    else:
                        self.assertEqual(framing["body_zone"], "")

    def test_unknown_module_and_type_are_reported_not_guessed(self):
        self.assertEqual(project_module_framing("手链", "NOT_A_MODULE"), {})
        self.assertEqual(project_module_framing("不是饰品", "WORN_DETAIL"), {})

    def test_module_bans_are_about_framing_not_about_control(self):
        """A static shot may not show a hand; that is not a soft preference."""

        static = load_mixed_template_definition()["module_framing_rules"][
            "STATIC_PRODUCT"
        ]
        for required in ("人物、脸与身体入画", "手或手臂入画", "佩戴部位入画"):
            with self.subTest(required=required):
                self.assertIn(required, static["forbidden_framing"])


class ShippedSubtypeRulesAreCompleteTest(unittest.TestCase):
    """Review #6: actions must come from the physical subtype, not the zone."""

    def test_every_supported_canonical_type_has_a_subtype_rule(self):
        definition = load_mixed_template_definition()
        zone_map = definition["canonical_type_to_zone"]
        rules = definition["physical_subtype_rules"]
        self.assertEqual(set(rules), set(zone_map))
        for canonical, rule in rules.items():
            with self.subTest(canonical=canonical):
                self.assertTrue(rule["family"])
                self.assertTrue(rule["observation_focus"])
                self.assertTrue(rule["fallback_observation"])
                self.assertEqual(
                    set(rule["action_by_module"]), set(REQUIRED_MODULES)
                )
                self.assertEqual(
                    set(rule["distinct_jobs"]), {"WORN_DETAIL", "WORN_RELATION"}
                )

    def test_structure_facts_only_use_the_three_evidence_states(self):
        for canonical, rule in load_mixed_template_definition()[
            "physical_subtype_rules"
        ].items():
            for part, state in (rule["structure_facts"] or {}).items():
                with self.subTest(canonical=canonical, part=part):
                    self.assertIn(state, EVIDENCE_STATES)

    def test_no_shipped_subtype_action_names_a_part_it_excludes(self):
        """The config itself must be self-consistent.

        This is the root-cause guard: the wrist zone used to hand a solid bangle
        the same chain/pendant/clasp action as a fine chain bracelet.
        """

        definition = load_mixed_template_definition()
        terms = definition["part_evidence_terms"]
        for canonical, rule in definition["physical_subtype_rules"].items():
            blocked = []
            for part, state in (rule["structure_facts"] or {}).items():
                if state != EVIDENCE_ABSENT:
                    continue
                blocked.extend(terms.get(part, {}).get("positive_terms") or [])
            if not blocked:
                continue
            for module, action in rule["action_by_module"].items():
                for term in blocked:
                    with self.subTest(canonical=canonical, module=module, term=term):
                        self.assertNotIn(term, action, action)
            for candidate in rule["optional_actions"]:
                for term in blocked:
                    with self.subTest(canonical=canonical, term=term):
                        self.assertNotIn(term, candidate["action"])

    def test_no_shipped_category_prose_names_a_structure_it_cannot_know(self):
        """The category layer must stay structure-blind.

        Review #6: the category layer used to write "链节与吊坠垂落" for every
        wrist product, and the hair rule banned "参考图未提供的背面结构".
        Both are structural claims the category layer has no way to verify --
        the subtype registry and the frozen evidence own that, not the zone.

        ``forbidden_actions`` is deliberately out of scope: "扣细搭扣" is a
        prohibition, not a claim that a clasp exists, so it cannot fabricate
        structure.
        """

        definition = load_mixed_template_definition()
        part_terms = {
            term
            for spec in definition["part_evidence_terms"].values()
            for term in (spec.get("positive_terms") or [])
        }
        for zone, rules in definition["category_rules"].items():
            watched = [
                ("base_action", rules["base_action"]),
                ("quantity_rule", rules["quantity_rule"]),
                ("visible_quantity_rule", rules["visible_quantity_rule"]),
            ]
            watched.extend(
                (f"action_by_module.{key}", value)
                for key, value in rules["action_by_module"].items()
            )
            watched.extend(
                (f"distinct_jobs.{key}", value)
                for key, value in rules["distinct_jobs"].items()
            )
            for path, value in watched:
                hits = sorted(term for term in part_terms if term in value)
                with self.subTest(zone=zone, path=path):
                    self.assertEqual(
                        hits, [], f"{zone}/{path} 写死了结构部件 {hits}：{value}"
                    )

    def test_every_subtype_action_is_face_free_and_pairing_neutral(self):
        for canonical, rule in load_mixed_template_definition()[
            "physical_subtype_rules"
        ].items():
            watched = [
                (f"action_by_module.{key}", value)
                for key, value in rule["action_by_module"].items()
            ]
            watched.extend(
                (f"optional_actions[{index}].action", candidate["action"])
                for index, candidate in enumerate(rule["optional_actions"])
            )
            watched.extend(
                (f"distinct_jobs.{key}", value)
                for key, value in rule["distinct_jobs"].items()
            )
            watched.append(("fallback_observation", rule["fallback_observation"]))
            for path, value in watched:
                with self.subTest(canonical=canonical, path=path):
                    self.assertFalse(contains_face_wording(value), value)
                    self.assertFalse(contains_pairing_claim(value), value)

    def test_optional_actions_are_declared_in_the_contract_but_gated(self):
        sample_by_canonical = {
            canonical: (label, top) for label, top, canonical in SAMPLES
        }
        for canonical in load_mixed_template_definition()["canonical_type_to_zone"]:
            label, top = sample_by_canonical.get(canonical, (canonical, canonical))
            with self.subTest(canonical=canonical):
                contract = _compile(label, top, mixed_template_ids()[0])
                declared = contract["part_gated_actions"]
                expected = physical_subtype_rule(canonical)["optional_actions"]
                self.assertEqual(len(declared), len(expected))
                # Declared is not issued: without evidence nothing fires.
                self.assertEqual(resolve_part_gated_actions(canonical, evidence={}), [])

    def test_landing_actions_are_actually_used_by_the_contract(self):
        contract = _compile("手镯", "手镯", mixed_template_ids()[0])
        bangle = physical_subtype_rule("bangle")
        for unit in contract["capture_units"]:
            with self.subTest(module=unit["module"]):
                self.assertEqual(
                    unit["action"], bangle["action_by_module"][unit["module"]]
                )


class ModuleProjectionReadsBackConsistentlyTest(unittest.TestCase):
    def test_projection_reads_the_frozen_units_not_the_live_config(self):
        contract = _compile("手链", "手链", mixed_template_ids()[0])
        projection = module_framing_projection(contract)
        self.assertEqual(set(projection), set(REQUIRED_MODULES))
        for module, item in projection.items():
            unit = next(
                u for u in contract["capture_units"] if u["module"] == module
            )
            with self.subTest(module=module):
                self.assertEqual(item["allowed_framing"], unit["allowed_framing"])
                self.assertEqual(item["forbidden_framing"], unit["forbidden_framing"])

    def test_worn_body_framing_is_the_two_worn_modules_union(self):
        contract = _compile("耳饰", "耳饰", mixed_template_ids()[0])
        worn = worn_body_framing(contract)
        self.assertIn("耳廓与耳垂近景", worn)
        self.assertIn("少量下颌边缘", worn)
        # A handheld or static allowance must never enter the body vocabulary.
        static = next(
            unit
            for unit in contract["capture_units"]
            if unit["module"] == "STATIC_PRODUCT"
        )
        for value in static["allowed_framing"]:
            self.assertNotIn(value, worn)


class PairedEarringCropRuleTest(unittest.TestCase):
    """T07 -- a paired product is not required to show both in one crop."""

    def test_authorised_pair_is_kept_while_the_crop_rule_does_not_demand_both(self):
        contract = _compile("耳饰", "耳饰", mixed_template_ids()[0])
        detail = next(
            unit
            for unit in contract["capture_units"]
            if unit["module"] == "WORN_DETAIL"
        )
        cue = detail["visible_quantity_rule"]
        self.assertTrue(cue)
        # The crop rule must not itself state a pairing relation: the relation
        # is owned by the frozen authority, and a rule that restated it would
        # become a second, unauthorised source.
        self.assertFalse(contains_pairing_claim(cue), cue)
        self.assertNotIn("两只", cue)
        self.assertNotIn("单只", cue)

        joined = "\n".join(
            render_mixed_blueprint_guidance(
                contract, identity_authority={"pairing_mode": "PAIR"}
            )
        )
        self.assertIn("全片保持成对出现", joined)
        self.assertIn("不要求一个近景内出现全部件数", joined)

    def test_unauthorised_pairing_produces_no_claim_and_no_crop_demand(self):
        contract = _compile("耳饰", "耳饰", mixed_template_ids()[0])
        joined = "\n".join(render_mixed_blueprint_guidance(contract))
        for term in (*PAIRING_OUTPUT_TERMS, *SINGLE_OUTPUT_TERMS):
            with self.subTest(term=term):
                self.assertNotIn(term, joined)


class MixedThemePropositionTest(unittest.TestCase):
    """Package B3: the theme must be a readable claim, never an operator ID.

    Measured on the four real scripts of 2026-09-17, every plan wrote
    ``"thesis": "ARGUMENT_OPERATOR_PCS_..._PCL_..."``.  Two different buying
    reasons then looked identical to a reader *and* to the difference judge
    (Review R4).  These tests pin the replacement: a readable proposition plus
    the source field it was copied from, with the internal ID kept only as
    provenance.
    """

    ANGLE = "ARGUMENT_OPERATOR_PCS_EB7F5A7B62C24DDF_PCL_B0821D5D7D8D4BE3"

    def _bundle(self, **overrides):
        bundle = {
            "content_angle_key": self.ANGLE,
            "semantic_spine_contract": {
                "script_thesis": {"core_buying_reason": "双层纱质蝴蝶造型，超唯美"}
            },
            "content_mainline": "适合日常穿搭场景",
            "selling_argument": {
                "core_value": "适合日常穿搭场景",
                "argument_id": "OPERATOR_PCS_EB7F5A7B62C24DDF",
            },
        }
        bundle.update(overrides)
        return bundle

    def test_the_spine_buying_reason_is_the_first_source(self):
        resolved = _mixed_theme_proposition(
            self._bundle(), fallback_theme_id=self.ANGLE
        )
        self.assertEqual(resolved["thesis"], "双层纱质蝴蝶造型，超唯美")
        self.assertEqual(resolved["thesis_source"], "SPINE_CORE_BUYING_REASON")
        self.assertEqual(
            resolved["thesis_source_ref"],
            "bundle.semantic_spine_contract.script_thesis.core_buying_reason",
        )
        self.assertEqual(resolved["argument_id"], "OPERATOR_PCS_EB7F5A7B62C24DDF")
        self.assertEqual(resolved["thesis_input_gap"], "")

    def test_selling_argument_core_value_outranks_content_mainline(self):
        resolved = _mixed_theme_proposition(
            self._bundle(semantic_spine_contract={}, content_mainline="适合日常穿搭场景"),
            fallback_theme_id=self.ANGLE,
        )
        self.assertEqual(resolved["thesis"], "适合日常穿搭场景")
        self.assertEqual(resolved["thesis_source"], "SELLING_ARGUMENT_CORE_VALUE")

    def test_content_mainline_is_the_last_resort(self):
        resolved = _mixed_theme_proposition(
            self._bundle(semantic_spine_contract={}, selling_argument={}),
            fallback_theme_id=self.ANGLE,
        )
        self.assertEqual(resolved["thesis"], "适合日常穿搭场景")
        self.assertEqual(resolved["thesis_source"], "CONTENT_MAINLINE")
        self.assertEqual(resolved["thesis_source_ref"], "bundle.content_mainline")

    def test_the_angle_key_never_becomes_the_theme(self):
        resolved = _mixed_theme_proposition(
            self._bundle(
                semantic_spine_contract={}, selling_argument={}, content_mainline=""
            ),
            fallback_theme_id=self.ANGLE,
        )
        self.assertEqual(resolved["thesis"], "")
        self.assertNotEqual(resolved["thesis"], self.ANGLE)
        self.assertEqual(
            resolved["thesis_input_gap"], MIXED_THESIS_INPUT_GAP_READABILITY
        )

    def test_the_uuid_family_of_id_shapes_is_not_a_proposition(self):
        for shape in (
            "ARGUMENT_OPERATOR_PCS_X",
            "OPERATOR_PCS_X",
            "TH_ITEM_1",
            "CLM_abc",
            "PCS_abc",
        ):
            with self.subTest(shape=shape):
                self.assertFalse(is_readable_theme_proposition(shape))

    def test_the_unavailable_sentinel_is_not_a_proposition(self):
        for sentinel in ("UNAVAILABLE", "unavailable", "N/A", "NULL"):
            with self.subTest(sentinel=sentinel):
                self.assertFalse(is_readable_theme_proposition(sentinel))

    def test_resolve_declares_a_gap_when_nothing_is_readable(self):
        resolved = resolve_theme_proposition(
            [
                {"source": "A", "ref": "bundle.a", "text": ""},
                {"source": "B", "ref": "bundle.b", "text": "UNAVAILABLE"},
                {"source": "C", "ref": "bundle.c", "text": "ARGUMENT_OPERATOR_PCS_X"},
            ]
        )
        self.assertEqual(resolved["thesis"], "")
        self.assertEqual(
            resolved["thesis_input_gap"], MIXED_THESIS_INPUT_GAP_READABILITY
        )

    def test_the_writing_and_reading_sides_share_one_rule(self):
        self.assertEqual(
            theme_proposition({"theme_id": "TH_1", "thesis": self.ANGLE}),
            ("", MIXED_THESIS_INPUT_GAP_READABILITY),
        )
        self.assertEqual(
            theme_proposition({"theme_id": "TH_1", "thesis": "双层纱质蝴蝶造型，超唯美"}),
            ("双层纱质蝴蝶造型，超唯美", ""),
        )

    def test_the_injection_carries_proposition_and_keeps_the_ids(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            result = _build_mixed_template_injection(
                product_type="耳饰",
                top_category="耳饰",
                item_index=1,
                item_role="STRUCTURE_MOTHER",
                content_angle_key=self.ANGLE,
                audience_tension_text="不知道耳饰怎么搭",
                claim_keys=["CLM_1"],
                product_code="P1",
                theme_proposition=_mixed_theme_proposition(
                    self._bundle(), fallback_theme_id=self.ANGLE
                ),
            )
        theme = result["contract"]["content_theme"]
        self.assertEqual(theme["thesis"], "双层纱质蝴蝶造型，超唯美")
        self.assertEqual(theme["thesis_source"], "SPINE_CORE_BUYING_REASON")
        self.assertEqual(theme["argument_id"], "OPERATOR_PCS_EB7F5A7B62C24DDF")
        # The old theme id is still there to audit against.
        self.assertEqual(theme["theme_id"], self.ANGLE)
        self.assertEqual(validate_mixed_template_contract(result["contract"]), [])

    def test_the_injection_refuses_a_theme_with_no_readable_proposition(self):
        with mock.patch.dict(os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}, clear=False):
            result = _build_mixed_template_injection(
                product_type="耳饰",
                top_category="耳饰",
                item_index=1,
                item_role="STRUCTURE_MOTHER",
                content_angle_key=self.ANGLE,
                audience_tension_text="",
                claim_keys=["CLM_1"],
                product_code="P1",
            )
        self.assertNotIn("contract", result)
        self.assertIn(
            f"{ERR_MIXED_THEME_INPUT_GAP}:{MIXED_THESIS_INPUT_GAP_READABILITY}",
            result["errors"],
        )

    def test_a_declared_gap_is_reported_by_name(self):
        contract = compile_mixed_template_contract(
            product_type="耳饰",
            top_category="耳饰",
            template_id=mixed_template_ids()[0],
            content_theme={
                "theme_id": "TH_1",
                "candidate_role": "PRIMARY",
                "thesis": "",
                "thesis_input_gap": MIXED_THESIS_INPUT_GAP_READABILITY,
            },
        )
        errors = validate_mixed_template_contract(contract)
        self.assertIn(
            f"{ERR_MIXED_THEME_INPUT_GAP}:{MIXED_THESIS_INPUT_GAP_READABILITY}", errors
        )

    def test_product_facts_and_theme_meaning_are_separate_axes(self):
        """Two buying reasons quoting the same product facts are two themes."""

        def _theme(theme_id: str, thesis: str):
            return {
                "theme_id": theme_id,
                "parent_theme_id": theme_id,
                "candidate_role": "PRIMARY",
                "thesis": thesis,
                "approved_claim_refs": ["CLM_shared_a", "CLM_shared_b"],
                "evidence_refs": ["CLM_shared_a", "CLM_shared_b"],
            }

        left = _theme("TH_A", "雪纺花朵抓夹，仙气感十足")
        right = _theme("TH_B", "无需繁琐步骤，随手一夹就能打造高颅顶或蓬松丸子头")
        facts = [mixed_product_fact_signature({"content_theme": theme}) for theme in (left, right)]
        meanings = [mixed_semantic_signature({"content_theme": theme}) for theme in (left, right)]
        self.assertEqual(facts[0]["digest"], facts[1]["digest"])
        self.assertNotEqual(meanings[0]["digest"], meanings[1]["digest"])
        self.assertEqual(meanings[0]["proposition"], "雪纺花朵抓夹，仙气感十足")
        self.assertNotIn("CLM_shared_a", json.dumps(meanings[0]["digest"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
