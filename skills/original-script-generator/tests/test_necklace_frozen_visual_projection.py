"""F2: a frozen necklace reads its lighting and framing off itself.

Review #7 found the necklace visual contract quoting a recipe id with **no**
environment, goal or label text, while the blueprint two fields away quoted the
same recipe verbatim -- one model input, two surfaces and two light directions.
The shipped artifact shows it directly::

    $ python -c '...' run_6/necklace_v1_live/out/dump_68A58E/item1_result.json
    recipe_id  'NMX_WARM_NEUTRAL_WINDOW_V1'
    label      ''
    allowed    []
    forbidden  []
    zone_label ''

The cause was that this branch answered from the *live* configuration:

* ``NMX_WARM_NEUTRAL_WINDOW_V1`` does not exist in the shared
  ``accessory_mixed_templates.json`` (it holds three AMX recipes), so
  ``get_environment_recipe`` raised and the projection fell back to ``{}``;
* ``category_rules`` has no ``NECK`` entry, so the zone's framing came out empty
  even though the frozen contract carries a resolved boundary per shot;
* ``resolve_mixed_zone("necklace", "")`` returns ``(None, 'womenwear')``, so the
  frozen ``category_zone`` is the only thing that can name this zone at all.

These tests pin the frozen-only behaviour, and pin that it stops at the necklace:
freezing is a *no-op* for the four legacy accessory families, whose authority is
still the live configuration.

The fixtures are real frozen contracts lifted verbatim from the isolated
production copy; see ``run_6/necklace_v1_c5_evidence/extract_fixtures.py`` for
the extraction and ``tests/fixtures/accessory_frozen_contracts.json`` for the
row ids the four legacy families came from.
"""
import copy
import json
import os
import pathlib
import unittest
from unittest import mock

from core.accessory_mixed_templates import (
    ACCESSORY_MIXED_TEMPLATE_ENV,
    get_environment_recipe,
    load_mixed_template_definition,
    resolve_mixed_zone,
    worn_body_forbidden_framing,
    worn_body_framing,
)
from core.visual_execution_contract import (
    FROZEN_PROJECTION_STATUS_COMPLETE,
    FROZEN_PROJECTION_STATUS_INCOMPLETE,
    NECKLACE_V1_FEATURE_PROFILE,
    build_accessory_mixed_visual_contract,
)

_FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"

#: Real canonical types for the four legacy families (``canonical_type_to_zone``
#: keys, not the operator-facing Chinese labels).
_LEGACY_CANONICAL = {
    "EAR": "earring",
    "WRIST": "bangle",
    "FINGER": "ring",
    "HAIR": "claw_clip",
}

#: The two worn shots of the real necklace contract.  The neck requirement below
#: may only ever be their union -- never a union that also swallowed the
#: hand-held or static shots.
_WORN_UNITS = ("CU_01", "CU_02")
_OTHER_UNITS = ("CU_03", "CU_04")

_NECKLACE_RECIPE_ID = "NMX_WARM_NEUTRAL_WINDOW_V1"


def frozen_necklace_contract():
    return json.loads(
        (_FIXTURES / "necklace_v1_frozen_contract.json").read_text(encoding="utf-8")
    )


def legacy_frozen_contracts():
    payload = json.loads(
        (_FIXTURES / "accessory_frozen_contracts.json").read_text(encoding="utf-8")
    )
    return payload["contracts"]


def necklace_projection(contract=None, **overrides):
    """Project one frozen necklace contract, with the real one by default."""

    kwargs = dict(
        canonical_product_type="necklace",
        presentation_mode="PERSON_ON_CAMERA",
        capture_mode="CREATOR_SELF_SHOT",
        environment_recipe_id=_NECKLACE_RECIPE_ID,
        frozen_contract=(
            frozen_necklace_contract() if contract is None else contract
        ),
    )
    kwargs.update(overrides)
    return build_accessory_mixed_visual_contract(**kwargs)


def legacy_projection(zone, contract=None, *, frozen=True, **overrides):
    kwargs = dict(
        canonical_product_type=_LEGACY_CANONICAL[zone],
        presentation_mode="PERSON_ON_CAMERA",
        capture_mode="CREATOR_SELF_SHOT",
        environment_recipe_id=(contract or {}).get("environment_recipe_id", ""),
        frozen_contract=contract if frozen else None,
    )
    kwargs.update(overrides)
    return build_accessory_mixed_visual_contract(**kwargs)


def units_by_id(projection):
    return {
        unit["unit_id"]: unit for unit in projection["framing_zone"]["unit_framing"]
    }


def union_of(units, key):
    out = []
    for unit in units:
        for value in unit.get(key) or []:
            if value not in out:
                out.append(value)
    return out


def poisoned_definition(*, neck_rule=None, recipe=None):
    """The live definition with a necklace answer bolted on.

    Only the two places the necklace branch must *not* read are changed; the
    consistency and authenticity rules stay byte-identical, so any output
    movement is attributable to the poison and nothing else.
    """

    definition = copy.deepcopy(load_mixed_template_definition())
    if neck_rule is not None:
        (definition.setdefault("category_rules", {}))["NECK"] = neck_rule
    if recipe is not None:
        (
            definition.setdefault("environment_recipes", {})
        )[_NECKLACE_RECIPE_ID] = recipe
    return definition


class TheFrozenRecipeIsProjectedVerbatim(unittest.TestCase):
    """Every lighting field comes off the frozen document, not today's config."""

    def test_every_lighting_field_matches_the_frozen_recipe(self):
        frozen = frozen_necklace_contract()
        recipe = frozen["environment_recipe"]
        projected = necklace_projection(frozen)["lighting_recipe"]
        for field in ("label", "environment", "goal"):
            with self.subTest(field=field):
                self.assertTrue(recipe[field])
                self.assertEqual(recipe[field], projected[field])
        self.assertEqual(frozen["environment_recipe_id"], projected["recipe_id"])
        self.assertEqual(recipe["recipe_version"], projected["recipe_version"])

    def test_the_recipe_id_carries_its_version_next_to_it(self):
        # A recipe id without its revision is exactly what the defect shipped:
        # a name the reader cannot resolve to any document.
        projected = necklace_projection()["lighting_recipe"]
        self.assertEqual(_NECKLACE_RECIPE_ID, projected["recipe_id"])
        self.assertEqual(1, projected["recipe_version"])

    def test_the_frozen_recipe_is_the_only_authority_for_lighting(self):
        authorities = necklace_projection()["authorities"]
        self.assertEqual("FROZEN_PER_VIDEO", authorities["lighting_recipe"])
        # The framing authority moved for necklaces only: a zone rule read from
        # the live config cannot describe a per-shot boundary.
        self.assertEqual("FROZEN_PER_SHOT", authorities["framing_zone"])
        self.assertEqual("HARD_EXISTING_IDENTITY_LOCK", authorities["product_integrity"])

    def test_the_shared_configuration_could_not_have_answered_this(self):
        # This is the *reason* the branch exists, recorded so the next reader
        # does not "simplify" it back into a live-config lookup.  If necklaces
        # ever do enter the shared table the two assertions below will fail --
        # update this note then, but the frozen-only behaviour must keep being
        # held by the other cases in this file, which do not depend on the
        # shape of the shared config.
        definition = load_mixed_template_definition()
        self.assertNotIn(
            "NECK",
            definition.get("category_rules") or {},
            "项链已进入共享 category_rules —— 请更新本测试说明",
        )
        with self.assertRaises(ValueError):
            get_environment_recipe(_NECKLACE_RECIPE_ID)
        self.assertEqual((None, "womenwear"), resolve_mixed_zone("necklace", ""))

    def test_without_the_frozen_zone_the_projection_is_empty(self):
        # The zone is unreachable by type resolution, so dropping the frozen
        # contract does not merely lose the recipe -- it loses the whole block.
        self.assertEqual({}, necklace_projection(frozen_contract=None))

    def test_the_live_configuration_path_would_have_produced_an_empty_recipe(self):
        # Reconstruct the pre-F2 behaviour on the same real input by hiding the
        # feature profile, which is what selected the frozen-only branch.
        # Everything else is held equal, so this is the defect, not a new input.
        blind = frozen_necklace_contract()
        blind.pop("feature_profile")
        projected = necklace_projection(blind)
        self.assertEqual("", projected["lighting_recipe"]["label"])
        self.assertEqual("", projected["lighting_recipe"]["environment"])
        self.assertEqual("", projected["lighting_recipe"]["goal"])
        self.assertEqual([], projected["framing_zone"]["allowed_framing"])
        self.assertEqual([], projected["framing_zone"]["forbidden_framing"])
        self.assertNotIn("frozen_projection", projected)
        # ...which is exactly the artifact the review found.
        self.assertEqual(_NECKLACE_RECIPE_ID, projected["lighting_recipe"]["recipe_id"])

    def test_a_caller_supplied_recipe_id_cannot_override_the_frozen_one(self):
        projected = necklace_projection(
            environment_recipe_id="WARM_WOOD_NEUTRAL_SUBJECT"
        )
        self.assertEqual(_NECKLACE_RECIPE_ID, projected["lighting_recipe"]["recipe_id"])
        self.assertTrue(projected["lighting_recipe"]["environment"])
        notes = " ".join(projected["frozen_projection"]["notes"])
        self.assertIn("WARM_WOOD_NEUTRAL_SUBJECT", notes)
        self.assertIn(_NECKLACE_RECIPE_ID, notes)


class TheNeckFramingIsReadPerShot(unittest.TestCase):
    """Only the worn shots carry the neck requirement."""

    def test_the_neck_boundaries_are_the_union_of_the_worn_shots(self):
        frozen = frozen_necklace_contract()
        projected = necklace_projection(frozen)["framing_zone"]
        self.assertEqual(worn_body_framing(frozen), projected["allowed_framing"])
        self.assertEqual(
            worn_body_forbidden_framing(frozen), projected["forbidden_framing"]
        )
        self.assertTrue(projected["allowed_framing"])
        self.assertTrue(projected["forbidden_framing"])

    def test_the_other_shots_are_genuinely_left_out_of_that_union(self):
        # Guards against the union silently widening to every unit: the
        # hand-held and static shots frame parts a necklace shot must never use.
        frozen = frozen_necklace_contract()
        projected = necklace_projection()["framing_zone"]
        allowed_all = union_of(frozen["capture_units"], "allowed_framing")
        forbidden_all = union_of(frozen["capture_units"], "forbidden_framing")
        self.assertNotEqual(allowed_all, projected["allowed_framing"])
        self.assertNotEqual(forbidden_all, projected["forbidden_framing"])
        self.assertIn("手指与商品本体的承托关系", allowed_all)
        self.assertNotIn("手指与商品本体的承托关系", projected["allowed_framing"])

    def test_every_shot_keeps_its_own_boundaries(self):
        frozen = frozen_necklace_contract()
        framing = necklace_projection()["framing_zone"]
        projected = {unit["unit_id"]: unit for unit in framing["unit_framing"]}
        self.assertEqual(
            [unit["unit_id"] for unit in frozen["capture_units"]],
            [unit["unit_id"] for unit in framing["unit_framing"]],
        )
        for unit in frozen["capture_units"]:
            unit_id = unit["unit_id"]
            with self.subTest(unit=unit_id):
                self.assertEqual(
                    unit["allowed_framing"], projected[unit_id]["allowed_framing"]
                )
                self.assertEqual(
                    unit["forbidden_framing"], projected[unit_id]["forbidden_framing"]
                )
                self.assertEqual(unit["module"], projected[unit_id]["module"])
                self.assertEqual(unit["carrier_mode"], projected[unit_id]["carrier_mode"])

    def test_the_neck_requirement_is_not_promoted_to_the_other_shots(self):
        # The worn shots name the face parts a neck close-up could stray into.
        # The hand-held and static shots have their own vocabulary -- the
        # static shot bans hands and arms, the hand-held shot bans the wearing
        # site -- and folding the sets together is the mistake this guards.
        # (``正面全脸`` genuinely appears in both the worn list and the
        # hand-held list; the frozen document's own overlap is not the thing
        # under test, so the assertions below use the lists' distinctive items.)
        projected = units_by_id(necklace_projection())
        for unit_id in _WORN_UNITS:
            with self.subTest(unit=unit_id):
                for face_part in ("嘴部入画", "鼻子入画", "眼睛入画"):
                    self.assertIn(face_part, projected[unit_id]["forbidden_framing"])
                self.assertNotIn("佩戴部位入画", projected[unit_id]["forbidden_framing"])
                self.assertNotIn("手或手臂入画", projected[unit_id]["forbidden_framing"])
        for unit_id in _OTHER_UNITS:
            with self.subTest(unit=unit_id):
                for face_part in ("嘴部入画", "鼻子入画", "眼睛入画"):
                    self.assertNotIn(face_part, projected[unit_id]["forbidden_framing"])
        self.assertIn(
            "佩戴部位入画", projected["CU_03"]["forbidden_framing"]
        )
        self.assertIn("手或手臂入画", projected["CU_04"]["forbidden_framing"])

    def test_the_neck_union_excludes_what_the_other_shots_ban(self):
        # The other direction of the same guard: a static shot's "手或手臂入画"
        # is not a neck rule and must not be promoted into the worn union.
        framing = necklace_projection()["framing_zone"]
        for unrelated in ("手或手臂入画", "佩戴部位入画", "全身穿搭展示"):
            with self.subTest(ban=unrelated):
                self.assertNotIn(unrelated, framing["forbidden_framing"])
        for unrelated in ("手指与商品本体的承托关系", "克制的同侧机位变化"):
            with self.subTest(framing=unrelated):
                self.assertNotIn(unrelated, framing["allowed_framing"])

    def test_the_projection_says_which_shots_the_neck_arrays_describe(self):
        framing = necklace_projection()["framing_zone"]
        self.assertIn("佩戴镜", framing["applies_to"])
        self.assertIn("WORN_DETAIL", framing["applies_to"])
        self.assertIn("WORN_RELATION", framing["applies_to"])
        self.assertEqual(["CU_01", "CU_02"], necklace_projection()["frozen_projection"]["worn_unit_ids"])

    def test_a_contract_that_omits_the_body_zone_still_finds_its_worn_shots(self):
        # ``body_zone`` is the primary signal; ``carrier_mode`` is the fallback,
        # so a contract written before the zone field existed is not left with
        # an empty neck boundary.
        blind = frozen_necklace_contract()
        for unit in blind["capture_units"]:
            unit.pop("body_zone", None)
        projected = necklace_projection(blind)
        self.assertEqual(["CU_01", "CU_02"], projected["frozen_projection"]["worn_unit_ids"])
        self.assertEqual(worn_body_framing(blind), projected["framing_zone"]["allowed_framing"])
        self.assertEqual(FROZEN_PROJECTION_STATUS_COMPLETE, projected["frozen_projection"]["status"])

    def test_the_no_face_rule_does_not_need_the_display_label(self):
        # ``zone_label`` is only a display string and the frozen contract does
        # not carry one.  The no-face block keys off the execution profile and
        # the face policy, so its absence must not change anything.
        from core.production_script_renderer import _is_face_free_contract

        frozen = frozen_necklace_contract()
        self.assertFalse(frozen.get("zone_label"))
        self.assertEqual("", necklace_projection()["framing_zone"]["zone_label"])
        self.assertTrue(
            _is_face_free_contract({"mixed_template_contract": frozen})
        )


class AFrozenGapIsNamedNotBackfilled(unittest.TestCase):
    """Missing frozen data is reported by name; it is never filled in."""

    def test_the_real_contract_is_complete(self):
        projected = necklace_projection()["frozen_projection"]
        self.assertEqual(FROZEN_PROJECTION_STATUS_COMPLETE, projected["status"])
        self.assertEqual([], projected["gaps"])

    def test_the_projection_records_which_document_it_read(self):
        frozen = frozen_necklace_contract()
        projected = necklace_projection()["frozen_projection"]
        self.assertEqual(NECKLACE_V1_FEATURE_PROFILE, projected["feature_profile"])
        self.assertEqual(frozen["template_id"], projected["template_id"])
        self.assertEqual(frozen["template_version"], projected["template_version"])
        self.assertEqual(frozen["feature_version"], projected["feature_version"])
        self.assertEqual(
            frozen["necklace_contract"]["profile_config_hash"],
            projected["profile_config_hash"],
        )

    def _assert_incomplete(self, projected):
        self.assertEqual(FROZEN_PROJECTION_STATUS_INCOMPLETE, projected["frozen_projection"]["status"])
        self.assertTrue(projected["frozen_projection"]["gaps"])
        return " ".join(gap["reason"] for gap in projected["frozen_projection"]["gaps"])

    def test_a_recipe_without_environment_or_goal_is_a_named_gap(self):
        stripped = frozen_necklace_contract()
        stripped["environment_recipe"] = {"label": "只剩标签", "recipe_version": 1}
        reasons = self._assert_incomplete(necklace_projection(stripped))
        self.assertIn("environment/goal", reasons)

    def test_the_missing_recipe_is_never_backfilled_from_today(self):
        stripped = frozen_necklace_contract()
        stripped.pop("environment_recipe")
        poisoned = poisoned_definition(
            recipe={
                "label": "今天的光",
                "environment": "今天的环境",
                "goal": "今天的目标",
                "recipe_version": 99,
            }
        )
        with mock.patch(
            "core.accessory_mixed_templates.load_mixed_template_definition",
            lambda: poisoned,
        ):
            projected = necklace_projection(stripped)
        recipe = projected["lighting_recipe"]
        self.assertEqual("", recipe["label"])
        self.assertEqual("", recipe["environment"])
        self.assertEqual("", recipe["goal"])
        self.assertNotIn("今天", json.dumps(projected, ensure_ascii=False))
        self.assertEqual(
            FROZEN_PROJECTION_STATUS_INCOMPLETE, projected["frozen_projection"]["status"]
        )

    def test_a_contract_without_capture_units_is_a_named_gap(self):
        stripped = frozen_necklace_contract()
        stripped.pop("capture_units")
        reasons = self._assert_incomplete(necklace_projection(stripped))
        self.assertIn("逐镜执行单元", reasons)

    def test_a_contract_without_a_worn_shot_is_a_named_gap(self):
        stripped = frozen_necklace_contract()
        stripped["capture_units"] = [
            unit
            for unit in stripped["capture_units"]
            if unit["module"] not in {"WORN_DETAIL", "WORN_RELATION"}
        ]
        reasons = self._assert_incomplete(necklace_projection(stripped))
        self.assertIn("没有佩戴镜", reasons)

    def test_a_worn_shot_without_boundaries_is_a_named_gap(self):
        stripped = frozen_necklace_contract()
        for unit in stripped["capture_units"]:
            if unit["module"] in {"WORN_DETAIL", "WORN_RELATION"}:
                unit["allowed_framing"] = []
        reasons = self._assert_incomplete(necklace_projection(stripped))
        self.assertIn("取景边界为空", reasons)

    def test_a_missing_recipe_id_is_a_named_gap(self):
        stripped = frozen_necklace_contract()
        stripped.pop("environment_recipe_id")
        reasons = self._assert_incomplete(necklace_projection(stripped))
        self.assertIn("光影配方 id", reasons)

    def test_a_missing_config_hash_is_a_named_gap(self):
        stripped = frozen_necklace_contract()
        stripped["necklace_contract"].pop("profile_config_hash")
        reasons = self._assert_incomplete(necklace_projection(stripped))
        self.assertIn("profile_config_hash", reasons)

    def test_every_gap_names_the_field_it_is_about(self):
        stripped = frozen_necklace_contract()
        stripped.pop("environment_recipe")
        stripped.pop("capture_units")
        stripped.pop("environment_recipe_id")
        stripped["necklace_contract"].pop("profile_config_hash")
        gaps = necklace_projection(stripped)["frozen_projection"]["gaps"]
        self.assertGreaterEqual(len(gaps), 4)
        for gap in gaps:
            with self.subTest(field=gap.get("field")):
                self.assertTrue(gap.get("field"))
                self.assertTrue(gap.get("reason"))

    def test_a_missing_display_label_is_a_note_and_not_a_gap(self):
        # ``zone_label`` is presentation only.  Calling it a gap would report a
        # complete contract as damaged.
        projected = necklace_projection()["frozen_projection"]
        self.assertEqual([], projected["gaps"])
        self.assertEqual(FROZEN_PROJECTION_STATUS_COMPLETE, projected["status"])
        self.assertTrue(projected["notes"])
        self.assertIn("zone_label", " ".join(projected["notes"]))


class ATodayEditCannotMoveAFrozenTask(unittest.TestCase):
    """A finished task keeps describing the document it was frozen against."""

    def test_changing_the_live_lighting_recipe_moves_nothing(self):
        before = necklace_projection()
        poisoned = poisoned_definition(
            recipe={
                "label": "今天的光",
                "environment": "今天的环境",
                "goal": "今天的目标",
                "recipe_version": 99,
            }
        )
        with mock.patch(
            "core.accessory_mixed_templates.load_mixed_template_definition",
            lambda: poisoned,
        ):
            after = necklace_projection()
        self.assertEqual(before, after)

    def test_adding_a_neck_zone_rule_to_the_live_config_moves_nothing(self):
        before = necklace_projection()
        poisoned = poisoned_definition(
            neck_rule={
                "zone_label": "伪造颈部",
                "allowed_framing": ["正面全脸特写"],
                "forbidden_framing": [],
            }
        )
        with mock.patch(
            "core.accessory_mixed_templates.load_mixed_template_definition",
            lambda: poisoned,
        ):
            after = necklace_projection()
        self.assertEqual(before, after)
        self.assertNotIn("正面全脸特写", after["framing_zone"]["allowed_framing"])
        self.assertNotIn("伪造颈部", after["framing_zone"]["zone_label"])

    def test_both_edits_at_once_move_nothing(self):
        before = necklace_projection()
        poisoned = poisoned_definition(
            neck_rule={
                "zone_label": "伪造颈部",
                "allowed_framing": ["正面全脸特写"],
                "forbidden_framing": [],
            },
            recipe={"label": "今天的光", "environment": "今天的环境", "goal": "今天的目标", "recipe_version": 99},
        )
        with mock.patch(
            "core.accessory_mixed_templates.load_mixed_template_definition",
            lambda: poisoned,
        ):
            after = necklace_projection()
        self.assertEqual(before, after)

    def test_switching_the_gate_off_does_not_unfreeze_a_frozen_task(self):
        # The switch decides whether the mixed mode starts at all; it must never
        # be able to move aside a contract a task already carries.
        on = necklace_projection()
        for value in ("0", ""):
            with self.subTest(switched=value):
                with mock.patch.dict(
                    os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: value}
                ):
                    self.assertEqual(on, necklace_projection())

    def test_the_gate_still_governs_a_task_that_is_not_frozen_yet(self):
        with mock.patch.dict(
            os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "0"}
        ):
            self.assertEqual({}, legacy_projection("EAR", frozen=False))


class FreezingIsStillANoOpForTheOtherFourFamilies(unittest.TestCase):
    """The four legacy families keep answering from the live configuration."""

    def test_every_legacy_family_projects_the_same_frozen_and_unfrozen(self):
        # Their zone boundaries and recipe still live in the shared config, so
        # carrying a frozen contract must change nothing at all.  If the
        # necklace branch ever widens, this is what breaks first.
        with mock.patch.dict(
            os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}
        ):
            for zone, contract in legacy_frozen_contracts().items():
                with self.subTest(zone=zone):
                    frozen = legacy_projection(zone, contract, frozen=True)
                    live = legacy_projection(zone, contract, frozen=False)
                    self.assertTrue(frozen, f"{zone} 应当产出合同")
                    self.assertEqual(live, frozen)

    def test_no_legacy_projection_gains_the_necklace_bookkeeping(self):
        with mock.patch.dict(
            os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}
        ):
            for zone, contract in legacy_frozen_contracts().items():
                with self.subTest(zone=zone):
                    projected = legacy_projection(zone, contract)
                    self.assertNotIn("frozen_projection", projected)
                    self.assertEqual(
                        "CATEGORY_EXTENSION", projected["authorities"]["framing_zone"]
                    )

    def test_the_legacy_families_still_read_their_own_live_boundaries(self):
        definition = load_mixed_template_definition()
        rules = definition["category_rules"]
        with mock.patch.dict(
            os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}
        ):
            for zone, contract in legacy_frozen_contracts().items():
                with self.subTest(zone=zone):
                    projected = legacy_projection(zone, contract)
                    self.assertEqual(zone, projected["framing_zone"]["zone"])
                    self.assertEqual(
                        rules[zone]["allowed_framing"],
                        projected["framing_zone"]["allowed_framing"],
                    )
                    self.assertEqual(
                        rules[zone]["forbidden_framing"],
                        projected["framing_zone"]["forbidden_framing"],
                    )
                    self.assertEqual(
                        rules[zone]["zone_label"],
                        projected["framing_zone"]["zone_label"],
                    )

    def test_the_legacy_projection_is_not_widened_by_the_necklace_fixture(self):
        # Interleaved calls: the frozen necklace must not leak a zone, a recipe
        # or an authority into a family projected straight after it.
        with mock.patch.dict(
            os.environ, {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}
        ):
            first = legacy_projection("EAR", legacy_frozen_contracts()["EAR"])
            necklace_projection()
            second = legacy_projection("EAR", legacy_frozen_contracts()["EAR"])
        self.assertEqual(first, second)
        self.assertEqual("EAR", second["framing_zone"]["zone"])


class TheWriterReceivesTheRecipeAndNotTheBookkeeping(unittest.TestCase):
    """The frozen data has to reach the model; the gap report has to not."""

    @staticmethod
    def _seed(contract):
        from core.simplified_complete_script import build_simplified_creative_seed

        return build_simplified_creative_seed(
            anchor_card={
                "product_positioning_one_liner": "单层链圆形吊坠项链",
                "hard_anchors": [{"anchor": "单层链条"}],
                "display_anchors": [{"anchor": "金属光泽"}],
                "category_execution_contract": {"display_family": "accessory"},
            },
            structure_contract={
                "direction_identity": {"macro_family_key": "HOOK>PROOF>ENDING"},
                "hard_constraints": {
                    "content_carrier": "WEARER_ACTIVE",
                    "beat_sequence": ["HOOK", "PROOF", "ENDING"],
                },
            },
            content_bundle={
                "content_mainline": "吊坠正面贴合锁骨的位置",
                "eligible_hook_ids": ["AUDIENCE_NEED_CALLOUT"],
                "claim_atoms": [
                    {
                        "claim_key": "C1",
                        "fact_text": "吊坠正面贴合锁骨",
                        "role": "core_result",
                    }
                ],
            },
            creative_contract={},
            execution_reference={"content_carrier": "WEARER_ACTIVE"},
            requested_hook_id="AUDIENCE_NEED_CALLOUT",
            content_angle_key="FACT_DISCOVERY",
            product_type="项链",
            top_category="饰品",
            category_execution_extension={"mixed_template_contract": contract},
        )

    def _payload(self, contract):
        from core.simplified_complete_script import build_simplified_script_prompt

        seed = self._seed(contract)
        prompt = build_simplified_script_prompt(
            seed,
            target_country="泰国",
            target_language="泰语",
            duration_seconds=15,
        )
        return seed, prompt

    def test_the_seed_carries_the_frozen_recipe(self):
        frozen = frozen_necklace_contract()
        seed = self._seed(frozen)
        projected = seed["visual_execution_contract"]["lighting_recipe"]
        self.assertEqual(frozen["environment_recipe"]["label"], projected["label"])
        self.assertEqual(
            frozen["environment_recipe"]["environment"], projected["environment"]
        )
        self.assertEqual(frozen["environment_recipe"]["goal"], projected["goal"])

    def test_the_writer_payload_carries_the_recipe_text(self):
        frozen = frozen_necklace_contract()
        _seed, prompt = self._payload(frozen)
        for field in ("label", "environment", "goal"):
            with self.subTest(field=field):
                self.assertTrue(frozen["environment_recipe"][field])
                self.assertIn(frozen["environment_recipe"][field], prompt)

    def test_the_writer_payload_carries_both_framing_directions(self):
        _seed, prompt = self._payload(frozen_necklace_contract())
        self.assertIn("颈部下段与锁骨近景", prompt)
        self.assertIn("正面全脸", prompt)
        self.assertIn("FROZEN_PER_SHOT", prompt)

    def test_the_writer_payload_never_carries_the_gap_report(self):
        # ``frozen_projection`` is review bookkeeping: a template revision, a
        # config hash, and a ``gaps`` list.  A gap reads "佩戴镜的取景边界为空，
        # 等于没有约束", which as *writer* input would invite the model to invent
        # exactly the framing the missing data could not constrain.
        _seed, prompt = self._payload(frozen_necklace_contract())
        self.assertNotIn("frozen_projection", prompt)
        self.assertNotIn("冻结合同未记录 zone_label", prompt)

    def test_the_gap_report_survives_where_a_reviewer_can_read_it(self):
        # Removing it from the payload must not remove it from the record.
        seed = self._seed(frozen_necklace_contract())
        self.assertIn("frozen_projection", seed["visual_execution_contract"])
        self.assertEqual(
            FROZEN_PROJECTION_STATUS_COMPLETE,
            seed["visual_execution_contract"]["frozen_projection"]["status"],
        )

    def test_an_incomplete_frozen_contract_does_not_soften_the_payload(self):
        # Even when the frozen data is thin, what reaches the writer is the
        # frozen text and the frozen boundaries -- never an admission that the
        # boundary is missing.
        stripped = frozen_necklace_contract()
        stripped.pop("capture_units")
        _seed, prompt = self._payload(stripped)
        self.assertIn("正面全脸", prompt)
        self.assertNotIn("frozen_projection", prompt)
        self.assertNotIn("取景边界为空", prompt)
        self.assertNotIn("无法还原任何一镜", prompt)


if __name__ == "__main__":
    unittest.main()
