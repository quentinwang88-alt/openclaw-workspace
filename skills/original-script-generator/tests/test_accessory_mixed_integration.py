"""WP1 integration: the accessory mixed mode must not leak into legacy paths.

Every assertion here runs twice — once with the feature gate off (legacy
behaviour must be byte-for-byte identical) and once with it on (the new
face-free accessory behaviour must apply).
"""

import copy
import json
import os
import unittest
from types import SimpleNamespace
from unittest import mock

from core.accessory_mixed_templates import (
    ACCESSORY_MIXED_TEMPLATE_ENV,
    MIXED_HISTORY_METADATA_KEY,
    MIXED_TEMPLATE_CONTRACT_KEY,
    ERR_MIXED_PART_CONTRADICTION,
    ERR_MIXED_SCOPE_UNSUPPORTED,
    ERR_MIXED_STRUCTURE_INCOMPATIBLE,
    ERR_MIXED_TIMELINE_MISMATCH,
    ERR_MIXED_UNIT_ID_MISMATCH,
    EVIDENCE_ABSENT,
    EVIDENCE_UNKNOWN,
    EVIDENCE_VERIFIED,
    PROJECTION_STATUS_APPLIED,
    PROJECTION_STATUS_MISMATCH,
    _signature_atom,
    anchor_evidence_texts,
    attach_mixed_part_evidence,
    compile_mixed_template_contract,
    frozen_unit_timeline,
    judge_mixed_candidate,
    load_mixed_template_definition,
    map_structure_beats_to_units,
    mixed_reference_signature,
    mixed_scope_decision,
    mixed_signature_bundle,
    mixed_template_ids,
    mixed_template_unit_count,
    mixed_visual_signature,
    module_framing_legend_lines,
    project_mixed_template_onto_units,
    render_mixed_blueprint_guidance,
    resolve_part_evidence,
    validate_mixed_template_contract,
)
from core.category_execution import (
    build_category_blueprint_guidance,
    compile_category_execution_extension,
)
from core.first_frame_contract import (
    _mixed_accessory_frame_line,
    build_first_frame_contract,
    render_first_frame_prompt,
)
from core.original_batch_allocator import (
    _build_mixed_template_injection,
    _mixed_structure_rejection,
    allocate_batch_items,
)
from core.original_batch_executor import (
    _mixed_preflight_error,
    resolve_planning_execution_scope,
)
from core.original_batch_models import BatchRequest
from core.production_script_renderer import render_video_generation_prompt
from core.simplified_complete_script import (
    CAPTURE_MODE_CREATOR_SELF_SHOT,
    _project_mixed_template_shot_contract,
    build_capture_rhythm_contract,
    compile_capture_units,
    validate_simplified_visual_script,
)
from core.visual_execution_contract import build_visual_execution_contract

_GATE_OFF = {ACCESSORY_MIXED_TEMPLATE_ENV: "0"}
_GATE_ON = {ACCESSORY_MIXED_TEMPLATE_ENV: "1"}

_SHORT_SCOPE = {
    "task_branch": "SHORT_VIDEO_ORIGINAL",
    "target_duration_seconds": 15.0,
    "is_new_plan": True,
    "execution_mode": "PLAN_ONLY",
    "script_mode": "simplified_v1",
}


def _shots(beats):
    """Minimal storyboard rows the real compile step can group."""

    return [
        {
            "shot_no": index + 1,
            "narrative_role": beat,
            "visual_content": f"画面{index + 1}",
            "character_action": f"动作{index + 1}",
            "natural_emotion": "自然",
            "camera": "手持手机",
            "time_range": f"{index * 3}-{(index + 1) * 3}秒",
        }
        for index, beat in enumerate(beats)
    ]


def _contract_for(template_id, product_type="耳饰"):
    return compile_mixed_template_contract(
        product_type=product_type,
        top_category="饰品",
        template_id=template_id,
        content_theme={
            "theme_id": "TH_1",
            "candidate_role": "PRIMARY",
            "thesis": "主题",
        },
    )


def _accessory_extension(product_type, anchor_card=None):
    """A real compiled accessory extension for the mixed-template tests."""

    return compile_category_execution_extension(
        product_type=product_type,
        top_category="饰品",
        anchor_card=anchor_card or {"hard_anchors": []},
        enabled=True,
    )


def _contract(canonical_type, carrier):
    return {
        "product_truth": {"canonical_product_type": canonical_type},
        "opening_contract": {
            "carrier_mode": carrier,
            "visual_content": "商品在画面中的状态",
            "character_action": "自然展示商品",
        },
        "presentation_mode": "MIXED",
        "capture_mode": "MIXED_MODULES",
        "product_identity_lock": {},
        "persona_contract": {},
        "outfit_contract": {},
        "outfit_prompt_projection": {},
        "scene_contract": {},
        "visual_saliency": {},
        "opening_scene_projection": {},
        "body_proportion_authority": {},
        "persona_reference_assets": [],
    }


class MixedFrameLineGateTest(unittest.TestCase):
    """Review #4: the gate owns *new planning*, the contract owns a *frozen task*.

    These used to assert the opposite -- that the line appears or disappears with
    ``ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED``.  That is the defect:
    an async first-frame worker without the variable (or a retry after the switch
    was turned off) would silently restore the legacy half-face framing onto a
    task that was frozen face-free, and turning the switch on would change legacy
    accessory tasks that carry no contract at all.
    """

    NON_ACCESSORY = ("outerwear", "silk_scarf", "winter_scarf", "womenwear")
    ACCESSORY = ("earring", "bracelet", "ring", "hair_clip", "claw_clip")

    def test_no_frozen_contract_means_no_mixed_line(self):
        for gate in (_GATE_OFF, _GATE_ON):
            with mock.patch.dict(os.environ, gate, clear=False):
                for canonical in self.ACCESSORY + self.NON_ACCESSORY:
                    with self.subTest(gate=gate, canonical=canonical):
                        # The line no longer takes the product type at all: without
                        # a frozen contract there is nothing authorised to say.
                        self.assertEqual(_mixed_accessory_frame_line(None), "")
                        self.assertEqual(_mixed_accessory_frame_line({}), "")

    def test_the_switch_cannot_change_a_frozen_task(self):
        contract = _contract_for("AMX_C_DETAIL_FIRST", "耳饰")
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            on = _mixed_accessory_frame_line(contract)
        with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
            off = _mixed_accessory_frame_line(contract)
        self.assertTrue(on)
        self.assertEqual(on, off)

    def test_the_frozen_opening_shot_decides_the_line(self):
        """A static-first template must never be described as a worn close-up."""

        expected = {
            "AMX_A_WORN_FIRST": ("佩戴近景首帧", "耳廓与耳垂近景", "眼睛入画"),
            "AMX_B_FORM_FIRST": ("手与商品首帧", "手指与商品本体的承托关系", "脸与头部入画"),
            "AMX_C_DETAIL_FIRST": ("商品与台面首帧", "商品与冻结台面的接触关系", "手或手臂入画"),
        }
        for template_id, (label, anchor, ban) in expected.items():
            with self.subTest(template_id=template_id):
                line = _mixed_accessory_frame_line(
                    _contract_for(template_id, "耳饰")
                )
                self.assertIn(label, line)
                self.assertIn(anchor, line)
                self.assertIn(ban, line)
                self.assertNotIn("半脸", line)
                self.assertNotIn("头肩", line)

    def test_unknown_opening_scope_stays_conservatively_face_free(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "耳饰")
        contract["capture_units"] = [
            {"unit_id": "CU_01", "view_scope": "NOT_A_SCOPE", "view_label": ""}
        ]
        line = _mixed_accessory_frame_line(contract)
        self.assertIn("保持不露脸", line)
        self.assertNotIn("半脸", line)


class MixedFirstFramePromptTest(unittest.TestCase):
    def _prompt_contract(self, canonical_type, template_id):
        contract = _contract(canonical_type, "MIXED")
        contract["mixed_template_contract"] = _contract_for(
            template_id, canonical_type
        )
        return contract

    def test_legacy_half_face_guidance_survives_without_a_contract(self):
        for gate in (_GATE_OFF, _GATE_ON):
            with mock.patch.dict(os.environ, gate, clear=False):
                prompt = render_first_frame_prompt(
                    _contract("earring", "WEARER_ACTIVE")
                )
            # Pre-existing behaviour for earrings must remain unchanged -- and the
            # switch must not be able to alter it either way.
            self.assertIn("半脸耳侧近景", prompt)

    def test_frozen_contract_removes_half_face_even_with_the_gate_off(self):
        with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
            prompt = render_first_frame_prompt(
                self._prompt_contract("earring", "AMX_A_WORN_FIRST")
            )
        self.assertNotIn("半脸", prompt)
        self.assertIn("佩戴近景首帧", prompt)
        self.assertIn("眼睛入画", prompt)

    def test_frozen_static_first_template_never_mentions_a_person(self):
        with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
            prompt = render_first_frame_prompt(
                self._prompt_contract("earring", "AMX_C_DETAIL_FIRST")
            )
        self.assertIn("商品与台面首帧", prompt)
        self.assertNotIn("半脸", prompt)
        self.assertNotIn("头肩", prompt)

    def test_frozen_handheld_first_template_forbids_unfolding_rigid_forms(self):
        with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
            prompt = render_first_frame_prompt(
                self._prompt_contract("earring", "AMX_B_FORM_FIRST")
            )
        self.assertIn("手与商品首帧", prompt)
        # The generic "naturally unfold the product" fallback must not appear;
        # the line instead explicitly forbids unfolding rigid jewellery.
        self.assertNotIn("自然展开一次商品", prompt)
        self.assertIn("不对刚性结构做多余展开", prompt)

    def test_frozen_contract_does_not_change_scarf_or_apparel_prompts(self):
        off_prompt = None
        with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
            off_prompt = render_first_frame_prompt(_contract("silk_scarf", "WEARER_ACTIVE"))
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            on_prompt = render_first_frame_prompt(_contract("silk_scarf", "WEARER_ACTIVE"))
        self.assertEqual(off_prompt, on_prompt)
        self.assertIn("丝巾/围巾", on_prompt)


class MixedFirstFrameFingerprintTest(unittest.TestCase):
    """T08-T10 (Review #4): the frozen contract decides, the switch never does.

    The first frame is routinely rendered later, on a worker that never saw the
    planning-time environment.  So a switch flip must leave both the rendered
    prompt *and* the cache fingerprint of a frozen task untouched, a legacy task
    must not start rendering differently just because the switch went on, and the
    fingerprint must depend on the opening shot only.
    """

    def _script(self, contract=None, canonical="earring"):
        brief = {
            "product_identity_lock": {},
            "product_truth": {"canonical_product_type": canonical},
            "production_design": {
                "presentation_mode": "MIXED",
                "capture_mode": "MIXED_MODULES",
            },
        }
        if contract is not None:
            brief["category_execution_extension"] = {
                MIXED_TEMPLATE_CONTRACT_KEY: copy.deepcopy(contract)
            }
        return {"script_id": "S1", "video_generation_brief": brief}

    def _build(self, script):
        return build_first_frame_contract(
            script_id="S1",
            product_code="P1",
            product_images=[{"asset_id": "A1", "url": "u"}],
            script=script,
        )

    def test_T08_the_switch_never_changes_a_frozen_first_frame(self):
        for template_id in (
            "AMX_A_WORN_FIRST",
            "AMX_B_FORM_FIRST",
            "AMX_C_DETAIL_FIRST",
        ):
            with self.subTest(template_id=template_id):
                script = self._script(_contract_for(template_id, "耳饰"))
                with mock.patch.dict(os.environ, _GATE_ON, clear=False):
                    on = self._build(script)
                with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
                    off = self._build(script)
                self.assertTrue(on["mixed_first_frame_facts"])
                self.assertEqual(on["asset_fingerprint"], off["asset_fingerprint"])
                self.assertEqual(on["contract_id"], off["contract_id"])
                self.assertEqual(
                    render_first_frame_prompt(on), render_first_frame_prompt(off)
                )

    def test_T09_legacy_task_is_untouched_by_the_switch(self):
        # Legacy accessory framings that the mixed mode must not steal.
        legacy_framing = {
            "earring": "半脸耳侧近景",
            "bracelet": "手腕前臂近景",
        }
        for canonical in ("earring", "bracelet", "ring", "hair_clip"):
            with self.subTest(canonical=canonical):
                script = self._script(None, canonical)
                with mock.patch.dict(os.environ, _GATE_ON, clear=False):
                    on = self._build(script)
                with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
                    off = self._build(script)
                # No contract means the mixed mode owns nothing here.
                self.assertEqual(on["mixed_template_contract"], {})
                self.assertEqual(on["mixed_first_frame_facts"], {})
                self.assertEqual(on["asset_fingerprint"], off["asset_fingerprint"])
                self.assertEqual(on["contract_id"], off["contract_id"])
                on_prompt = render_first_frame_prompt(on)
                self.assertEqual(on_prompt, render_first_frame_prompt(off))
                # The legacy accessory framing must survive verbatim.
                expected = legacy_framing.get(canonical)
                if expected:
                    self.assertIn(expected, on_prompt)
                # No mixed-mode framing line leaked in.
                for label in ("佩戴近景首帧", "手与商品首帧", "商品与台面首帧"):
                    self.assertNotIn(label, on_prompt)

    def test_T10_only_the_opening_shot_feeds_the_fingerprint(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "耳饰")
        self.assertTrue(contract["capture_units"])
        baseline = self._build(self._script(contract))

        # The facts really are the opening unit and nothing else.
        self.assertEqual(
            set(baseline["mixed_first_frame_facts"]),
            {"template_id", "face_policy", "environment_recipe_id", "opening_unit"},
        )
        self.assertEqual(
            baseline["mixed_first_frame_facts"]["opening_unit"]["unit_id"],
            contract["capture_units"][0]["unit_id"],
        )

        # Rewriting the *last* shot must not invalidate a cached first frame.
        later = copy.deepcopy(contract)
        later["capture_units"][-1]["observation_job"] = "完全改写的后段观察任务"
        later["capture_units"][-1]["view_label"] = "改写后的后段取景"
        same = self._build(self._script(later))
        self.assertEqual(baseline["asset_fingerprint"], same["asset_fingerprint"])
        self.assertEqual(baseline["contract_id"], same["contract_id"])

        # Changing the opening shot must invalidate it.
        opening = copy.deepcopy(contract)
        opening["capture_units"][0]["view_scope"] = "PRODUCT_AND_SURFACE"
        changed = self._build(self._script(opening))
        self.assertNotEqual(
            baseline["asset_fingerprint"], changed["asset_fingerprint"]
        )


class MixedVisualContractTest(unittest.TestCase):
    def _build(self, canonical_type, recipe_id=""):
        return build_visual_execution_contract(
            canonical_product_type=canonical_type,
            presentation_mode="MIXED",
            capture_mode="MIXED_MODULES",
            outfit_contract={},
            scene_reference={},
            accessory_environment_recipe_id=recipe_id,
        )

    def test_accessories_still_return_empty_contract_when_gate_is_off(self):
        with mock.patch.dict(os.environ, _GATE_OFF, clear=False):
            for canonical in ("earring", "bracelet", "ring", "hair_clip"):
                with self.subTest(canonical=canonical):
                    self.assertEqual(self._build(canonical), {})

    def test_gate_on_returns_accessory_specific_contract(self):
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            contract = self._build("earring", recipe_id="MATTE_GREY_DETAIL")
        self.assertEqual(contract["schema_version"], "accessory-mixed-visual-contract-v1")
        self.assertEqual(contract["feature_scope"], "ACCESSORY_MIXED_TEMPLATE")
        self.assertEqual(contract["lighting_recipe"]["recipe_id"], "MATTE_GREY_DETAIL")
        self.assertEqual(contract["framing_zone"]["zone"], "EAR")
        self.assertIn("眼睛入画", contract["framing_zone"]["forbidden_framing"])
        self.assertTrue(contract["lighting_recipe"]["environment"])

    def test_gate_on_keeps_scarf_on_the_legacy_contract(self):
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            contract = self._build("silk_scarf")
        self.assertEqual(
            contract["schema_version"], "visual-execution-contract-v4-wearable-saliency"
        )
        self.assertNotEqual(contract.get("feature_scope"), "ACCESSORY_MIXED_TEMPLATE")

    def test_gate_on_does_not_widen_the_legacy_supported_set(self):
        # A non-accessory, non-scarf, non-apparel type stays empty either way.
        for gate in (_GATE_OFF, _GATE_ON):
            with mock.patch.dict(os.environ, gate, clear=False):
                with self.subTest(gate=gate):
                    self.assertEqual(self._build("necklace"), {})


class FiveBeatStructureRejectedTest(unittest.TestCase):
    """T01 (Review #1): a five-beat structure must never validate against a
    four-shot template.  The candidate is excluded while structures are still
    being chosen, and a compiled five-clip script may not come out valid.
    """

    FIVE = ["HOOK", "PROOF", "PROOF", "USE_PROCESS", "ENDING"]
    FOUR = ["HOOK", "PROOF", "USE_PROCESS", "ENDING"]

    def test_mapping_refuses_more_beats_than_shots(self):
        result = map_structure_beats_to_units(self.FIVE, 4)
        self.assertFalse(result["compatible"])
        self.assertIn(ERR_MIXED_STRUCTURE_INCOMPATIBLE, result["reason"])

    def test_shorter_or_equal_structure_still_maps(self):
        for beats in (self.FOUR, ["HOOK", "PROOF", "ENDING"], ["HOOK"]):
            with self.subTest(beats=beats):
                self.assertTrue(map_structure_beats_to_units(beats, 4)["compatible"])

    def test_planner_excludes_five_beat_direction_with_a_reason(self):
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            rejection = _mixed_structure_rejection(
                direction={
                    "direction_assignment_id": "DA_5",
                    "output_slot": "S1",
                    "structure_contract": {
                        "hard_constraints": {"beat_sequence": self.FIVE}
                    },
                },
                product_type="耳饰",
                top_category="饰品",
                execution_scope=_SHORT_SCOPE,
            )
        self.assertIn(ERR_MIXED_STRUCTURE_INCOMPATIBLE, rejection)

    def test_planner_keeps_four_beat_direction(self):
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            rejection = _mixed_structure_rejection(
                direction={
                    "direction_assignment_id": "DA_4",
                    "structure_contract": {
                        "hard_constraints": {"beat_sequence": self.FOUR}
                    },
                },
                product_type="耳饰",
                top_category="饰品",
                execution_scope=_SHORT_SCOPE,
            )
        self.assertEqual(rejection, "")

    def test_compiled_five_shot_script_is_not_valid(self):
        contract = _contract_for("AMX_A_WORN_FIRST")
        rhythm = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=self.FIVE,
        )
        self.assertEqual(rhythm["capture_unit_count"], 5)
        storyboard, units = compile_capture_units(_shots(self.FIVE), rhythm)
        self.assertEqual(len(units), 5)
        result = {"capture_units": units}
        _project_mixed_template_shot_contract(
            {"category_execution_extension": {"mixed_template_contract": contract}},
            storyboard,
            units,
            result,
        )
        # A half-applied contract is never recorded as an applied one.
        self.assertIn("mixed_template_shot_projection_errors", result)
        self.assertNotIn("mixed_template_shot_projection", result)
        verdict = validate_simplified_visual_script(result, {"product_truth": {}})
        self.assertFalse(verdict["valid"])
        self.assertTrue(
            any("混合模板" in issue for issue in verdict["issues"]),
            verdict["issues"],
        )


class MixedPreflightStopsBeforeModelTest(unittest.TestCase):
    """T02 (Review #2): a missing, rejected or illegal contract must fail before
    the first model call -- never as a silent fallback to the legacy mode.
    """

    def _frozen(self, *, status="FROZEN", contract=None):
        frozen = {"schema_version": "original-frozen-direction-package-v1"}
        extension = {}
        if status:
            frozen["mixed_template_contract_status"] = status
        if contract is not None:
            extension["mixed_template_contract"] = contract
        if extension:
            frozen["category_execution_extension"] = extension
        return frozen

    def _tampered(self, mutate):
        contract = _contract_for("AMX_A_WORN_FIRST")
        mutate(contract)
        return contract

    def test_no_mixed_declaration_keeps_the_legacy_path(self):
        self.assertEqual(_mixed_preflight_error({}), "")
        self.assertEqual(_mixed_preflight_error(self._frozen(status="")), "")

    def test_rejected_contract_blocks(self):
        frozen = dict(self._frozen(contract=_contract_for("AMX_A_WORN_FIRST")))
        frozen["mixed_template_contract_status"] = "REJECTED"
        self.assertEqual(_mixed_preflight_error(frozen), "MIXED_CONTRACT_REJECTED")

    def test_missing_contract_blocks(self):
        self.assertEqual(
            _mixed_preflight_error(self._frozen(status="FROZEN")),
            "MIXED_CONTRACT_MISSING",
        )

    def test_sixteen_second_contract_blocks(self):
        def _inflate(contract):
            contract["capture_units"][0]["duration_seconds"] += 1

        error = _mixed_preflight_error(self._frozen(contract=self._tampered(_inflate)))
        self.assertTrue(error.startswith("MIXED_CONTRACT_INVALID:"), error)
        self.assertIn("MIXED_CONTRACT_TIMELINE_MISMATCH", error)

    def test_duplicate_unit_id_blocks(self):
        def _duplicate(contract):
            contract["capture_units"][1]["unit_id"] = contract["capture_units"][0][
                "unit_id"
            ]

        error = _mixed_preflight_error(self._frozen(contract=self._tampered(_duplicate)))
        self.assertIn("MIXED_CONTRACT_UNIT_ID_DUPLICATE", error)

    def test_dropped_shot_blocks(self):
        def _drop(contract):
            contract["capture_units"].pop()

        error = _mixed_preflight_error(self._frozen(contract=self._tampered(_drop)))
        self.assertTrue(error.startswith("MIXED_CONTRACT_INVALID:"), error)

    def test_legal_contract_passes(self):
        self.assertEqual(
            _mixed_preflight_error(self._frozen(contract=_contract_for("AMX_C_DETAIL_FIRST"))), ""
        )

    def test_running_a_blocked_item_calls_no_model(self):
        """The preflight sits ahead of ``_execute_single_item_with_timeout``, so
        a blocked item must leave the model call count at zero."""

        from core import original_batch_executor as executor

        frozen = dict(self._frozen(contract=_contract_for("AMX_A_WORN_FIRST")))
        frozen["mixed_template_contract_status"] = "REJECTED"
        item = SimpleNamespace(
            batch_item_id="BI_1",
            batch_id="B_1",
            item_index=1,
            item_role="STRUCTURE_MOTHER",
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            status="PLANNED",
            attempt_count=0,
            frozen_direction_package_json=json.dumps(frozen, ensure_ascii=False),
        )
        batch = SimpleNamespace(
            batch_id="B_1", requested_count=1, status="PLANNED", planned_count=1
        )
        storage = mock.MagicMock()
        storage.get_batch.return_value = batch
        storage.get_items.return_value = [item]

        model_calls = []

        def _spy(**kwargs):
            model_calls.append(kwargs)
            return {"status": "SUCCESS"}

        with mock.patch.object(executor, "BatchStorage", return_value=storage):
            with mock.patch.object(
                executor, "_execute_single_item_with_timeout", side_effect=_spy
            ):
                executor.run_script_only("B_1", resume=False)

        self.assertEqual(model_calls, [])
        statuses = [call.args[1] for call in storage.update_item_status.call_args_list]
        self.assertIn("SCRIPT_FAILED", statuses)
        self.assertNotIn("SCRIPT_RUNNING", statuses)

    def test_running_a_legal_item_still_reaches_the_model(self):
        """The guard must not block a valid mixed item."""

        from core import original_batch_executor as executor

        item = SimpleNamespace(
            batch_item_id="BI_2",
            batch_id="B_2",
            item_index=1,
            item_role="STRUCTURE_MOTHER",
            requested_hook_id="GENERAL_PRODUCT_SHARE",
            status="PLANNED",
            attempt_count=0,
            frozen_direction_package_json=json.dumps(
                self._frozen(contract=_contract_for("AMX_B_FORM_FIRST")), ensure_ascii=False
            ),
        )
        batch = SimpleNamespace(
            batch_id="B_2", requested_count=1, status="PLANNED", planned_count=1
        )
        storage = mock.MagicMock()
        storage.get_batch.return_value = batch
        storage.get_items.return_value = [item]

        model_calls = []

        def _spy(**kwargs):
            model_calls.append(kwargs)
            return {"status": "SUCCESS", "script_id": "SCRIPT_X"}

        with mock.patch.object(executor, "BatchStorage", return_value=storage):
            with mock.patch.object(
                executor, "_execute_single_item_with_timeout", side_effect=_spy
            ):
                executor.run_script_only("B_2", resume=False)

        self.assertEqual(len(model_calls), 1)
        statuses = [call.args[1] for call in storage.update_item_status.call_args_list]
        self.assertIn("SCRIPT_RUNNING", statuses)


class ExecutionScopeIsolationTest(unittest.TestCase):
    """T18 (part 1): the mixed mode may not be switched on by the flag alone.
    The long-form direct-product builder borrows this planner with a 15-second
    duration, so the *declared parent task* has to decide.
    """

    def test_longform_scope_is_refused(self):
        scope = resolve_planning_execution_scope(
            BatchRequest(
                request_id="R",
                product_code="P",
                requested_count=1,
                test_phase="T",
                duration_seconds=15.0,
                script_mode="simplified_v1",
            ),
            {"longform_prefer_persona_pack": True},
        )
        self.assertEqual(scope["task_branch"], "LONGFORM")
        decision = mixed_scope_decision(scope)
        self.assertFalse(decision["eligible"])
        self.assertIn(ERR_MIXED_SCOPE_UNSUPPORTED, decision["reason"])

    def test_short_video_scope_is_eligible(self):
        scope = resolve_planning_execution_scope(
            BatchRequest(
                request_id="R",
                product_code="P",
                requested_count=1,
                test_phase="T",
                duration_seconds=15.0,
                script_mode="simplified_v1",
            ),
            {},
        )
        self.assertEqual(scope["task_branch"], "SHORT_VIDEO_ORIGINAL")
        self.assertTrue(mixed_scope_decision(scope)["eligible"])

    def test_wrong_duration_and_resume_are_refused(self):
        base = dict(_SHORT_SCOPE)
        for override in (
            {"target_duration_seconds": 30.0},
            {"is_new_plan": False},
            {"script_mode": "legacy"},
        ):
            with self.subTest(override=override):
                scope = {**base, **override}
                decision = mixed_scope_decision(scope)
                self.assertFalse(decision["eligible"], scope)
                self.assertIn(ERR_MIXED_SCOPE_UNSUPPORTED, decision["reason"])

    def test_undeclared_scope_keeps_the_historical_behaviour(self):
        self.assertTrue(mixed_scope_decision(None)["eligible"])

    def test_longform_scope_never_builds_a_contract(self):
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            injection = _build_mixed_template_injection(
                product_type="耳饰",
                top_category="饰品",
                item_index=1,
                item_role="STRUCTURE_MOTHER",
                content_angle_key="FACT_DISCOVERY",
                audience_tension_text="主题",
                claim_keys=[],
                product_code="P",
                execution_scope={
                    "task_branch": "LONGFORM",
                    "target_duration_seconds": 15.0,
                    "is_new_plan": True,
                    "script_mode": "simplified_v1",
                },
            )
        self.assertNotIn("contract", injection)
        self.assertNotIn("errors", injection)
        self.assertIn("scope_rejected", injection)


class LegalTemplateProjectionTest(unittest.TestCase):
    """T03: a legal A/B/C contract projects onto four stable shot ids with one
    contiguous 15-second timeline and all three display methods present.
    """

    BEATS = ["HOOK", "PROOF", "USE_PROCESS", "ENDING"]
    TEMPLATES = ("AMX_A_WORN_FIRST", "AMX_B_FORM_FIRST", "AMX_C_DETAIL_FIRST")

    def _project(self, template_id):
        contract = _contract_for(template_id)
        rhythm = build_capture_rhythm_contract(
            capture_mode=CAPTURE_MODE_CREATOR_SELF_SHOT,
            macro_structure=self.BEATS,
        )
        self.assertEqual(rhythm["capture_unit_count"], 4)
        storyboard, units = compile_capture_units(_shots(self.BEATS), rhythm)
        result = {"capture_units": units}
        _project_mixed_template_shot_contract(
            {"category_execution_extension": {"mixed_template_contract": contract}},
            storyboard,
            units,
            result,
        )
        return contract, storyboard, units, result

    def test_four_stable_ids_and_applied_projection(self):
        for template_id in self.TEMPLATES:
            with self.subTest(template=template_id):
                contract, _, units, result = self._project(template_id)
                self.assertEqual(mixed_template_unit_count(contract), 4)
                self.assertEqual(
                    [unit["capture_unit_id"] for unit in units],
                    ["CU_01", "CU_02", "CU_03", "CU_04"],
                )
                self.assertEqual(
                    result["mixed_template_shot_projection"]["status"],
                    PROJECTION_STATUS_APPLIED,
                )
                self.assertNotIn("mixed_template_shot_projection_errors", result)

    def test_timeline_is_contiguous_and_totals_fifteen_seconds(self):
        for template_id in self.TEMPLATES:
            with self.subTest(template=template_id):
                contract, _, units, _ = self._project(template_id)
                timeline, total = frozen_unit_timeline(contract)
                self.assertEqual(total, 15)
                spans = list(timeline.values())
                self.assertEqual(spans[0]["start_seconds"], 0)
                self.assertEqual(spans[-1]["end_seconds"], 15)
                for previous, current in zip(spans, spans[1:]):
                    self.assertEqual(current["start_seconds"], previous["end_seconds"])
                projected = [
                    unit["time_range"] for unit in units if unit.get("time_range")
                ]
                self.assertEqual(len(projected), 4)
                self.assertEqual(projected[-1]["end_seconds"], 15)

    def test_all_three_display_methods_are_present(self):
        for template_id in self.TEMPLATES:
            with self.subTest(template=template_id):
                _, _, units, _ = self._project(template_id)
                modules = {unit["module"] for unit in units}
                self.assertTrue(modules & {"WORN_DETAIL", "WORN_RELATION"})
                self.assertIn("HANDHELD_PRODUCT", modules)
                self.assertIn("STATIC_PRODUCT", modules)
                carriers = {unit["carrier_mode"] for unit in units}
                self.assertEqual(
                    carriers, {"WEARER_ACTIVE", "HAND_ONLY", "STATIC_PRODUCT"}
                )

    def test_projection_no_longer_guesses_by_position(self):
        """Rebuilt with shuffled ids: the old code fell back to the n-th
        contract shot.  Identity is now by id only."""

        contract, storyboard, units, _ = self._project("AMX_A_WORN_FIRST")
        broken = [dict(unit) for unit in units]
        broken[1]["capture_unit_id"] = "CU_99"
        result = project_mixed_template_onto_units(broken, storyboard, contract)
        self.assertEqual(result["status"], PROJECTION_STATUS_MISMATCH)
        self.assertTrue(
            any(ERR_MIXED_UNIT_ID_MISMATCH in code for code in result["errors"]),
            result["errors"],
        )


class ModuleFramingPerShotTest(unittest.TestCase):
    """T05 -- framing is decided per (category, shot module) (Review #5).

    Before the split, every unit of every category carried the category's whole
    ``allowed_framing`` list, so a bracelet's static shot still advertised the
    wrist zone, and the shared blueprint prose told every category to describe
    itself in ear words.
    """

    CATEGORIES = (
        ("耳饰", "EAR"),
        ("手链", "WRIST"),
        ("戒指", "FINGER"),
        ("抓夹", "HAIR"),
    )

    def test_every_category_and_module_has_a_coherent_frame(self):
        for product_type, zone in self.CATEGORIES:
            for template_id in mixed_template_ids():
                with self.subTest(product=product_type, template=template_id):
                    contract = _contract_for(template_id, product_type)
                    self.assertEqual(validate_mixed_template_contract(contract), [])
                    units = contract["capture_units"]
                    self.assertEqual(len(units), 4)
                    by_module = {unit["module"]: unit for unit in units}

                    for module in ("WORN_DETAIL", "WORN_RELATION"):
                        unit = by_module[module]
                        self.assertEqual(unit["body_zone"], zone)
                        self.assertTrue(unit["allowed_framing"])
                        # The worn frame is the category's own ban list, never a
                        # generic one.
                        self.assertEqual(
                            unit["forbidden_framing"],
                            load_mixed_template_definition()["category_rules"][zone][
                                "forbidden_framing"
                            ],
                        )

                    # A handheld shot is hand-and-product: it may show a hand,
                    # but never a body zone.
                    handheld = by_module["HANDHELD_PRODUCT"]
                    self.assertEqual(handheld["body_zone"], "")
                    self.assertTrue(handheld["view_scope"].startswith("HAND"))
                    for value in handheld["allowed_framing"]:
                        with self.subTest(value=value):
                            self.assertFalse(
                                any(
                                    zone_word in value
                                    for zone_word in ("耳", "手腕", "后脑", "发束")
                                ),
                                f"手持镜不得继承佩戴部位取景：{value}",
                            )

                    # A static shot is product-and-surface: no body at all.
                    static = by_module["STATIC_PRODUCT"]
                    self.assertEqual(static["body_zone"], "")
                    self.assertTrue(static["view_scope"].startswith("PRODUCT"))
                    for value in static["allowed_framing"]:
                        with self.subTest(value=value):
                            self.assertFalse(
                                any(
                                    zone_word in value
                                    for zone_word in (
                                        "耳", "手腕", "手指", "后脑", "发束", "手",
                                    )
                                ),
                                f"静物镜不得继承佩戴部位取景：{value}",
                            )

    def test_static_shot_excludes_every_part_of_the_body(self):
        for product_type, _zone in self.CATEGORIES:
            with self.subTest(product=product_type):
                contract = _contract_for("AMX_C_DETAIL_FIRST", product_type)
                static = next(
                    unit
                    for unit in contract["capture_units"]
                    if unit["module"] == "STATIC_PRODUCT"
                )
                banned = "、".join(static["forbidden_framing"])
                for required in ("人物", "手或手臂", "佩戴部位"):
                    self.assertIn(required, banned)

    def test_a_non_ear_category_is_never_told_to_describe_itself_as_an_ear(self):
        rules = load_mixed_template_definition()["category_rules"]
        for product_type, zone in (("手链", "WRIST"), ("戒指", "FINGER"), ("抓夹", "HAIR")):
            with self.subTest(product=product_type):
                contract = _contract_for("AMX_A_WORN_FIRST", product_type)
                joined = "\n".join(render_mixed_blueprint_guidance(contract))
                # The legend names this category's own body zone...
                self.assertIn(f"（{rules[zone]['zone_label']}）", joined)
                # ...and the old shared sentence (which hard-coded the ear
                # vocabulary for all four categories) is gone.
                self.assertNotIn("佩戴关系只用", joined)
                self.assertNotIn("耳廓与耳垂近景", joined)
                self.assertNotIn("耳侧、耳廓、耳垂、颈侧", joined)

    def test_module_legend_states_each_module_with_its_own_range_and_bans(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "手链")
        legend = module_framing_legend_lines(contract)
        self.assertEqual(len(legend), 1)
        line = legend[0]
        for module in ("WORN_DETAIL", "WORN_RELATION", "HANDHELD_PRODUCT", "STATIC_PRODUCT"):
            label = next(
                unit["view_label"]
                for unit in contract["capture_units"]
                if unit["module"] == module
            )
            with self.subTest(module=module):
                self.assertIn(label, line)
        self.assertIn("不得出现人物、脸与身体入画", line)
        # The shot lines must use the same labels, or 镜头 N cannot be mapped
        # onto its legend row.
        for shot_line in render_mixed_blueprint_guidance(contract):
            if not shot_line.startswith("- 镜头"):
                continue
            label = shot_line.split("）", 1)[1].split("｜", 1)[0]
            with self.subTest(shot=shot_line[:20]):
                self.assertIn(label, line)

    def test_visible_quantity_is_separate_from_the_authorised_quantity(self):
        """A paired product must not be required to show both in one crop."""

        contract = _contract_for("AMX_A_WORN_FIRST", "耳饰")
        detail = next(
            unit
            for unit in contract["capture_units"]
            if unit["module"] == "WORN_DETAIL"
        )
        self.assertTrue(detail["visible_quantity_rule"])
        self.assertTrue(detail["quantity_rule"])

        joined = "\n".join(
            render_mixed_blueprint_guidance(
                contract, identity_authority={"pairing_mode": "PAIR"}
            )
        )
        self.assertIn("本镜可见数量", joined)
        # The category's own crop rule wins over the shared module default.
        self.assertIn(
            "不要求一个近景内出现全部件数", joined
        )
        self.assertIn("授权数量与佩戴位置不因裁切改变", joined)
        # The pairing relation itself is still stated only by the authority.
        self.assertIn("全片保持成对出现", joined)


class PartEvidenceGatingTest(unittest.TestCase):
    """T06 -- no action may display a part the product does not have (Review #6)."""

    def test_solid_bangle_never_mentions_chain_pendant_or_clasp(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "手镯")
        self.assertEqual(contract["physical_family"], "RIGID_WRIST_RING")
        self.assertEqual(contract["structure_facts"]["has_chain"], "ABSENT")
        joined = "；".join(unit["action"] for unit in contract["capture_units"])
        for forbidden in ("链节", "链条", "吊坠", "搭扣", "扣环"):
            with self.subTest(term=forbidden):
                self.assertNotIn(forbidden, joined)
        self.assertIn("圈口轮廓", joined)

    def test_bracelet_keeps_its_chain_but_not_an_unverified_pendant(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "手链")
        self.assertEqual(contract["structure_facts"]["has_chain"], "VERIFIED")
        self.assertEqual(contract["structure_facts"]["has_pendant"], "UNKNOWN")
        joined = "；".join(unit["action"] for unit in contract["capture_units"])
        self.assertIn("链节", joined)
        self.assertNotIn("吊坠", joined)
        self.assertNotIn("搭扣", joined)

    def test_unknown_subtype_gets_no_clip_body_action(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "发饰")
        self.assertEqual(contract["physical_family"], "HAIR_GENERIC")
        self.assertEqual(contract["structure_facts"], {})
        joined = "；".join(unit["action"] for unit in contract["capture_units"])
        for forbidden in ("夹体", "齿口", "背面结构", "夹持部位"):
            with self.subTest(term=forbidden):
                self.assertNotIn(forbidden, joined)

    def test_claw_clip_is_allowed_its_own_jaw_but_not_a_chain(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "抓夹")
        self.assertEqual(contract["structure_facts"]["has_teeth_or_jaw"], "VERIFIED")
        joined = "；".join(unit["action"] for unit in contract["capture_units"])
        self.assertIn("夹体", joined)
        self.assertNotIn("链节", joined)

    def test_a_contradicting_action_is_a_hard_validation_error(self):
        """The registry, not the anchors, decides what form a product has.

        "手链与手镯不得互相冒充" was only a negative rule, so it could not
        cancel a positive instruction to shoot a solid bangle's chain links.
        """

        contract = _contract_for("AMX_A_WORN_FIRST", "手镯")
        self.assertEqual(validate_mixed_template_contract(contract), [])
        bangle_unit = next(
            unit
            for unit in contract["capture_units"]
            if unit["module"] == "WORN_DETAIL"
        )
        bangle_unit["action"] = "手腕小幅转动，展示链节走向与吊坠垂落"

        errors = validate_mixed_template_contract(contract)
        self.assertTrue(
            any(ERR_MIXED_PART_CONTRADICTION in code for code in errors), errors
        )
        self.assertTrue(any("链节" in code for code in errors), errors)

    def test_evidence_is_tri_state_and_unknown_is_not_absent(self):
        facts = {"has_clasp": "UNKNOWN"}
        self.assertEqual(
            resolve_part_evidence(
                "has_clasp", structure_facts=facts, anchor_texts=["玫瑰金手镯"]
            )["state"],
            EVIDENCE_UNKNOWN,
        )
        self.assertEqual(
            resolve_part_evidence(
                "has_clasp", structure_facts={}, anchor_texts=["带搭扣的手链"]
            )["state"],
            EVIDENCE_VERIFIED,
        )
        # "无搭扣" contains "搭扣", so the negative scan has to win.
        self.assertEqual(
            resolve_part_evidence(
                "has_clasp", structure_facts={}, anchor_texts=["实心无搭扣手镯"]
            )["state"],
            EVIDENCE_ABSENT,
        )
        # The registry outranks anchor prose entirely.
        self.assertEqual(
            resolve_part_evidence(
                "has_chain",
                structure_facts={"has_chain": "ABSENT"},
                anchor_texts=["链节明显"],
            )["state"],
            EVIDENCE_ABSENT,
        )

    def test_gated_action_only_fires_on_verified_evidence(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "手链")
        gated = contract["part_gated_actions"]
        self.assertTrue(gated)
        required_parts = {
            part for item in gated for part in (item.get("requires") or {})
        }
        self.assertEqual(required_parts, {"has_pendant", "has_clasp"})
        for item in gated:
            self.assertEqual(set(item["requires"].values()), {"VERIFIED"})

        # UNKNOWN (no anchors at all) withholds the candidate.
        withheld = render_mixed_blueprint_guidance(contract, evidence={})
        self.assertFalse(any("同时交代已确认的吊坠" in line for line in withheld))
        self.assertTrue(any("未确认部件不要展示" in line for line in withheld))

        verified = render_mixed_blueprint_guidance(
            contract,
            evidence={
                "has_pendant": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
                "has_clasp": {"state": EVIDENCE_UNKNOWN, "source": "NO_EVIDENCE"},
            },
        )
        self.assertTrue(any("同时交代已确认的吊坠" in line for line in verified))
        # The clasp stayed UNKNOWN, so that candidate must still be withheld.
        self.assertFalse(any("已确认的搭扣外观" in line for line in verified))
        joined = "\n".join(verified)
        self.assertIn("本片可观察重点", joined)

    def test_absent_evidence_can_never_enable_a_part_action(self):
        contract = _contract_for("AMX_A_WORN_FIRST", "手链")
        lines = render_mixed_blueprint_guidance(
            contract,
            evidence={"has_pendant": {"state": EVIDENCE_ABSENT}},
        )
        # No *action* may be issued for the part.  The fallback line names it
        # only to say it must not be displayed.
        self.assertFalse(any("同时交代已确认的吊坠" in line for line in lines))
        self.assertTrue(any("未确认部件不要展示" in line for line in lines))

    def test_unconfirmed_structure_is_banned_even_without_gated_actions(self):
        """Review #6: a subtype with nothing confirmed must be told to stop.

        ``withheld`` used to count only the gated candidates the evidence
        refused to issue, so a subtype that declares no optional action at all
        -- a generic hair accessory, or a clip whose jaw and hinge are both
        ``UNKNOWN`` -- got no instruction and quietly invited the model to
        invent a back-of-clip detail.
        """

        for product_type, canonical in (
            ("发夹", "hair_clip"),
            ("hair_accessory_generic", "hair_accessory_generic"),
        ):
            with self.subTest(canonical=canonical):
                contract = _contract_for("AMX_A_WORN_FIRST", product_type)
                facts = contract["structure_facts"]
                self.assertTrue(
                    not facts
                    or any(state == EVIDENCE_UNKNOWN for state in facts.values())
                )
                lines = render_mixed_blueprint_guidance(contract, evidence={})
                self.assertTrue(
                    any("未确认部件不要展示" in line for line in lines),
                    f"{canonical} 未收到未确认结构禁令：{lines}",
                )

    def test_no_shipped_instruction_requires_an_unconfirmed_back_side(self):
        """A ban on the back side is correct; a demand for it is not."""

        ban_markers = ("不得", "不要", "禁止", "未确认", "不生成", "不要求")
        for product_type in ("发夹", "hair_accessory_generic", "手链"):
            with self.subTest(product_type=product_type):
                contract = _contract_for("AMX_A_WORN_FIRST", product_type)
                offending = [
                    line
                    for line in render_mixed_blueprint_guidance(contract, evidence={})
                    if any(word in line for word in ("背面", "内侧", "反面"))
                    and not any(marker in line for marker in ban_markers)
                ]
                self.assertEqual(offending, [])

    def test_anchor_evidence_reads_hard_anchors_only(self):
        """A display suggestion is not proof that a part exists."""

        self.assertEqual(
            anchor_evidence_texts({"display_anchors": [{"anchor": "手持展示吊坠"}]}),
            [],
        )
        self.assertEqual(
            anchor_evidence_texts({"hard_anchors": [{"anchor": "细链手链"}]}),
            ["细链手链"],
        )
        self.assertEqual(
            resolve_part_evidence(
                "has_pendant",
                structure_facts={},
                anchor_texts=anchor_evidence_texts(
                    {"display_anchors": [{"anchor": "展示吊坠"}]}
                ),
            )["state"],
            EVIDENCE_UNKNOWN,
        )

    def test_evidence_attachment_is_a_no_op_without_a_contract(self):
        for extension in ({}, {"profile": {}}, None):
            with self.subTest(extension=extension):
                self.assertEqual(attach_mixed_part_evidence(extension), {})
        legacy = {"domain": "ACCESSORY", "profile": {"product_subtype": "earring"}}
        attach_mixed_part_evidence(legacy, anchor_texts=["耳针式耳饰"])
        self.assertEqual(legacy, {"domain": "ACCESSORY", "profile": {"product_subtype": "earring"}})

    def test_attached_evidence_reaches_the_blueprint_guidance(self):
        extension = _accessory_extension("手链")
        contract = _contract_for("AMX_A_WORN_FIRST", "手链")
        extension["mixed_template_contract"] = contract

        attach_mixed_part_evidence(
            extension, anchor_texts=["细链手链，带吊坠"]
        )
        self.assertEqual(
            extension["part_evidence"]["has_pendant"]["state"], EVIDENCE_VERIFIED
        )
        guidance = build_category_blueprint_guidance(
            extension, carrier_execution={"product_relation_zh": ""}
        )
        self.assertIn("同时交代已确认的吊坠", guidance)


class RendererFramingPerCategoryTest(unittest.TestCase):
    """The final video prompt must not describe a bracelet in ear words."""

    def _item(self, product_type):
        contract = _contract_for("AMX_A_WORN_FIRST", product_type)
        script = {
            "production_design": {
                "presentation_mode": "MIXED",
                "capture_mode": "CREATOR_SELF_SHOT",
            },
            "video_generation_brief": {
                "render_profile": "ugc_native_v1",
                "category_execution_extension": {"mixed_template_contract": contract},
                "storyboard": [],
                "capture_units": [],
            },
        }
        return SimpleNamespace(
            result_json=json.dumps({"script": script}, ensure_ascii=False),
            content_bundle_json="",
        )

    def test_renderer_no_face_block_uses_the_categorys_own_framing(self):
        wrist = render_video_generation_prompt(
            item=self._item("手链"), duration_seconds=15
        )
        self.assertIn("【全片不露脸｜硬约束】", wrist)
        self.assertIn("同一手腕近景", wrist)
        self.assertNotIn("耳廓与耳垂近景", wrist)
        self.assertIn("手持与静物镜头另有模块禁令", wrist)
        self.assertIn("手或手臂入画", wrist)
        # The "出镜方式" line must be category-correct too.
        self.assertIn("同一手腕近景、少量前臂、少量袖口", wrist)

    def test_earring_still_gets_its_own_framing(self):
        ear = render_video_generation_prompt(
            item=self._item("耳饰"), duration_seconds=15
        )
        self.assertIn("耳廓与耳垂近景", ear)
        self.assertNotIn("同一手腕近景", ear)


# ── Stage E: final-shot difference judgement (Review #3) ───────────────


def _mixed_contract_of(item):
    """The frozen mixed contract an accepted plan item carries."""

    frozen = json.loads(item.frozen_direction_package_json or "{}")
    extension = frozen.get("category_execution_extension") or {}
    return extension.get("mixed_template_contract") or {}


def _mixed_direction(da_id, slot, carrier="WEARER_ACTIVE"):
    return {
        "direction_assignment_id": da_id,
        "output_slot": slot,
        "selection_run_id": "SR_E",
        "cluster_id": 1,
        "cluster_version": "v1",
        "evidence_tier": "BOOTSTRAP",
        "structure_contract": {
            "direction_identity": {"macro_family_key": "HOOK>PROOF>RESULT"},
            "hard_constraints": {
                "content_carrier": carrier,
                "continuity_mode": "MULTI_CUT",
            },
            "evidence": {"evidence_tier": "BOOTSTRAP"},
        },
        "execution_reference": {
            "execution_card_id": f"EC_{da_id}",
            "source_video_id": "V_E",
            "content_carrier": carrier,
        },
        "country": "泰国",
        "category": "配饰",
    }


def _mixed_anchor_card():
    return {
        "hard_anchors": [{"anchor": "蝴蝶造型", "why_must_show": "商品主体造型"}],
        "display_anchors": [
            {"anchor": "镂空轮廓", "why_must_show": "耳饰外轮廓"},
            {"anchor": "耳线长度比例", "why_must_show": "佩戴比例"},
        ],
        "category_execution_contract": {"display_family": "accessory"},
    }


def _mixed_selling_catalog():
    return [
        {
            "value_id": "ARG_HOLLOW",
            "primary_selling_point": "镂空轮廓让耳饰边缘更清楚",
            "proof_thesis": "镂空轮廓在近景里可以看清边缘",
            "truth_status": "VERIFIED",
            "visual_dependency": "WEARER_REQUIRED",
            "argument_kind": "SELLING_ARGUMENT",
        },
        {
            "value_id": "ARG_LENGTH",
            "primary_selling_point": "耳线长度比例修饰脸型",
            "proof_thesis": "耳线长度比例在侧脸关系里可见",
            "truth_status": "VERIFIED",
            "visual_dependency": "WEARER_REQUIRED",
            "argument_kind": "SELLING_ARGUMENT",
        },
    ]


def _allocate_mixed(requested_count, *, recent_usage=None, product_type="耳饰",
                    top_category="配饰", execution_scope=None, seed=42):
    with mock.patch.dict(os.environ, _GATE_ON, clear=False):
        return allocate_batch_items(
            product_code="P_E",
            requested_count=requested_count,
            directions=[_mixed_direction("DA_E", "S1"), _mixed_direction("DA_E2", "S2")],
            anchor_card=_mixed_anchor_card(),
            active_hook_ids=["DETAIL_SURPRISE", "AUDIENCE_NEED_CALLOUT"],
            creative_policy_version="test-v1",
            random_seed=seed,
            recent_creative_usage=recent_usage,
            selling_point_catalog=_mixed_selling_catalog(),
            product_type=product_type,
            top_category=top_category,
            execution_scope=execution_scope,
        )


def _mixed_history_row(contract, *, legacy_signature="OLD_SCENE_SIG|WHICH|MUST|NOT|BE|REUSED",
                       identity=""):
    """One historical usage row that already owns a final-shot signature."""

    return {
        "usage_id": identity or f"CPU_{contract.get('template_id')}",
        "visual_signature": legacy_signature,
        "scene_motif": "OLD_SCENE",
        "persona_role": "OLD_PERSONA",
        "metadata": {
            "batch_item_id": f"HIST_{contract.get('template_id')}",
            MIXED_HISTORY_METADATA_KEY: mixed_signature_bundle(contract),
        },
    }


def _deferred_with(report_status, summary):
    return [
        row
        for row in summary.get("deferred_content") or []
        if (row.get("difference_report") or {}).get("review_status") == report_status
    ]


class MixedFinalShotComparisonTest(unittest.TestCase):
    """T14: a repeated montage is refused even when the old signature differs.

    Review #3's field case: candidates 1 and 4 compiled to the *same* four final
    shots while their legacy ``visual_signature`` differed (the scene motif
    rotates per item index).  The old axis therefore reported "different" for a
    montage the viewer would receive twice.  The comparison now runs on the
    final shots, and a repeated candidate is dropped instead of being delivered
    as a synonym.
    """

    def test_a_repeated_montage_is_refused_and_reported(self):
        items, summary = _allocate_mixed(4)
        by_template = {_mixed_contract_of(item)["template_id"] for item in items}

        self.assertEqual(len(items), 3, "请求 4 条，其中 1 条最终镜头重复")
        self.assertEqual(summary["planned_count"], len(items))
        self.assertEqual(summary["mixed_duplicate_rejected_count"], 1)
        self.assertEqual(len(by_template), 3, "交付的三条必须各自是不同的最终镜头")

        duplicates = [
            row
            for row in summary.get("deferred_content") or []
            if row.get("downgrade_reason") == "MIXED_DUPLICATE_CANDIDATE"
        ]
        self.assertTrue(duplicates, "重复候选必须留下可核查的剔除记录")
        report = duplicates[0]["difference_report"]
        self.assertEqual(report["review_status"], "EXACT_DUPLICATE")
        self.assertEqual(report["comparison_scope"], "BATCH_AND_HISTORY")
        self.assertFalse(report["counts_as_independent"])
        self.assertIn("FINAL_SHOTS_IDENTICAL", report["difference_dimensions"])
        self.assertTrue(report["nearest_script_id"], "必须指出跟谁重复")
        self.assertTrue(report["difference_summary"], "必须给出可核查的比较依据")
        self.assertEqual(duplicates[0]["recommended_flow"], "REPLAN_MIXED_THEME")
        self.assertEqual(summary["mixed_shortage_reason"], "DIFFERENCE_INSUFFICIENT")
        self.assertEqual(summary["allocation_status"], "PARTIAL_CONTENT_CAPACITY")

    def test_the_legacy_scene_axis_cannot_mask_a_repeated_montage(self):
        contract = _contract_for("AMX_A_WORN_FIRST")
        first = _mixed_history_row(contract, legacy_signature="SCENE_ONE|A|B|C|D|E")
        second = _mixed_history_row(contract, legacy_signature="SCENE_TWO|A|B|C|D|E")

        self.assertNotEqual(
            first["visual_signature"], second["visual_signature"],
            "夹具：两条历史的旧场景签名必须不同",
        )
        reference_a = mixed_reference_signature(first)
        reference_b = mixed_reference_signature(second)
        self.assertTrue(reference_a["complete"])
        self.assertTrue(reference_b["complete"])
        self.assertEqual(
            reference_a["signature"], reference_b["signature"],
            "旧场景签名不参与最终镜头比较",
        )

        for reference in (reference_a, reference_b):
            with self.subTest(identity=reference["identity"]):
                report = judge_mixed_candidate(copy.deepcopy(contract), [reference])
                self.assertEqual(
                    report["review_status"], "EXACT_DUPLICATE",
                    "旧场景签名不同不能救回一个重复的蒙太奇",
                )
                self.assertFalse(report["counts_as_independent"])

    def test_a_rewritten_theme_label_does_not_move_the_final_shots(self):
        contract = _contract_for("AMX_A_WORN_FIRST")
        rewritten = copy.deepcopy(contract)
        rewritten["content_theme"]["theme_id"] = "TH_PARAPHRASE"
        rewritten["content_theme"]["thesis"] = "换一种说法的同一件事"

        self.assertEqual(
            mixed_visual_signature(contract)["digest"],
            mixed_visual_signature(rewritten)["digest"],
            "主题文案轮换不得改变最终镜头签名",
        )
        report = judge_mixed_candidate(
            rewritten,
            [{"identity": "ITEM_1", "signature": mixed_signature_bundle(contract)}],
        )
        self.assertEqual(report["review_status"], "EXACT_DUPLICATE")


class MixedSurfaceOnlyAndVariantTest(unittest.TestCase):
    """T15: only a demonstrated, evidenced difference counts as new content.

    Two things must never be confused: changing the tabletop/light recipe is
    scheduling variety, while adding an observation point the authored
    vocabulary supports is a real execution variant.  The first must not be
    counted as an independent script, the second must be labelled as one.
    """

    def test_environment_change_alone_is_not_independent_content(self):
        # 三种模板都已被历史占用，且候选只能靠换台面取胜 —— 正是"只换背景"。
        history = [
            _mixed_history_row(_contract_with_recipe(template_id, "MATTE_GREY_DETAIL"))
            for template_id in mixed_template_ids()
        ]
        items, summary = _allocate_mixed(1, recent_usage=history)

        self.assertEqual(items, [], "只换台面不得产出新脚本")
        self.assertEqual(summary["planned_count"], 0)
        surface = _deferred_with("SURFACE_ONLY", summary)
        self.assertTrue(surface, "只换台面的候选必须被识别为 SURFACE_ONLY")
        for row in surface:
            report = row["difference_report"]
            self.assertFalse(report["counts_as_independent"])
            self.assertIn("SURFACE_DIFFERENT", report["difference_dimensions"])
            self.assertIn("FINAL_SHOTS_IDENTICAL", report["difference_dimensions"])
        self.assertEqual(summary["mixed_usable_count"], 0)
        self.assertEqual(summary["mixed_shortage_reason"], "DIFFERENCE_INSUFFICIENT")
        self.assertEqual(
            (summary["mixed_history_coverage"] or {}).get("history_compared"), 3,
            "批次报告必须声明实际参与比较的历史条数",
        )

    def test_a_new_evidenced_observation_point_is_labelled_an_execution_variant(self):
        reference = _contract_for("AMX_A_WORN_FIRST")
        candidate = _contract_for("AMX_B_FORM_FIRST")

        report = judge_mixed_candidate(
            candidate,
            [{"identity": "ITEM_1", "signature": mixed_signature_bundle(reference)}],
        )
        self.assertEqual(report["review_status"], "EXECUTION_VARIANT")
        self.assertTrue(report["counts_as_independent"])
        self.assertIn("NEW_OBSERVATION_POINT", report["difference_dimensions"])
        self.assertIn("SEMANTIC_IDENTICAL", report["difference_dimensions"])
        self.assertEqual(report["nearest_script_id"], "ITEM_1")

        reference_jobs = {
            shot["observation_job"]
            for shot in mixed_visual_signature(reference)["shots"]
        }
        candidate_jobs = {
            shot["observation_job"]
            for shot in mixed_visual_signature(candidate)["shots"]
        }
        new_jobs = candidate_jobs - reference_jobs
        self.assertTrue(new_jobs, "夹具：B 模板必须带来 A 没有的观察重点")

        # 新观察点必须真的落在实际镜头上，而不只是出现在比较说明里。
        shot_jobs = {
            _signature_atom(unit.get("observation_job"))
            for unit in candidate.get("capture_units") or []
        }
        for job in new_jobs:
            self.assertIn(job, shot_jobs, f"新观察点未进入最终镜头：{job}")

    def test_an_invented_observation_point_is_not_accepted(self):
        reference = _contract_for("AMX_A_WORN_FIRST")
        candidate = copy.deepcopy(reference)
        candidate["template_id"] = "AMX_B_FORM_FIRST"
        for unit in candidate["capture_units"]:
            unit["observation_job"] = "展示商品内部从未验证过的隐藏结构"

        report = judge_mixed_candidate(
            candidate,
            [{"identity": "ITEM_1", "signature": mixed_signature_bundle(reference)}],
        )
        self.assertEqual(
            report["review_status"], "NEEDS_REVIEW",
            "凭空发明的观察点不得被当成有证据的新观察点",
        )
        self.assertIn("UNVERIFIED_NEW_OBSERVATION", report["difference_dimensions"])
        self.assertFalse(
            report["counts_as_independent"],
            "无法验证的差异不得宣称已通过去重",
        )


def _contract_with_recipe(template_id, recipe_id):
    return compile_mixed_template_contract(
        product_type="耳饰",
        top_category="饰品",
        template_id=template_id,
        environment_recipe_id=recipe_id,
        content_theme={
            "theme_id": "TH_1",
            "thesis": "怕买了不会戴",
            "approved_claim_refs": ["C1"],
            "evidence_refs": ["C1"],
        },
    )


class MixedOtherBranchesNeverEnterTest(unittest.TestCase):
    """T18: the mixed mode belongs to the 15-second accessory short original.

    The flag is a feature switch, not an authorisation: a long-form source
    build borrowing this planner, a remake, a resumed plan and a non-accessory
    category must all stay on their own path.
    """

    def _injection(self, **overrides):
        payload = {
            "product_type": "耳饰",
            "top_category": "饰品",
            "item_index": 1,
            "item_role": "STRUCTURE_MOTHER",
            "content_angle_key": "FACT_DISCOVERY",
            "audience_tension_text": "主题",
            "claim_keys": [],
            "product_code": "P",
        }
        payload.update(overrides)
        with mock.patch.dict(os.environ, _GATE_ON, clear=False):
            return _build_mixed_template_injection(**payload)

    def test_remake_and_resume_scopes_are_refused(self):
        for override in (
            {"task_branch": "REMAKE", "target_duration_seconds": 15.0,
             "is_new_plan": True, "script_mode": "simplified_v1"},
            {"task_branch": "SHORT_VIDEO_ORIGINAL", "target_duration_seconds": 15.0,
             "is_new_plan": False, "script_mode": "simplified_v1"},
            {"task_branch": "SHORT_VIDEO_ORIGINAL", "target_duration_seconds": 30.0,
             "is_new_plan": True, "script_mode": "simplified_v1"},
        ):
            with self.subTest(scope=override):
                injection = self._injection(execution_scope=override)
                self.assertNotIn("contract", injection)
                self.assertNotIn("errors", injection)
                self.assertIn("scope_rejected", injection)

    def test_womenswear_never_builds_an_accessory_contract(self):
        for product_type, top_category in (("外套", "女装"), ("针织", "女装")):
            with self.subTest(product_type=product_type):
                injection = self._injection(
                    product_type=product_type,
                    top_category=top_category,
                    execution_scope=_SHORT_SCOPE,
                )
                self.assertEqual(
                    injection, {},
                    "全局开关不得让女装进入 15 秒配饰混合模式",
                )

    def test_an_eligible_short_original_still_gets_the_contract(self):
        injection = self._injection(execution_scope=_SHORT_SCOPE)
        self.assertIn("contract", injection)
        self.assertTrue(
            (injection["contract"].get("difference_report") or {}).get("review_status"),
            "通过的候选必须带可核查的比较报告",
        )

    def test_longform_and_womenswear_plans_stay_legacy(self):
        # 借用同一入口的长视频规划：既不注入合同，也不报错。
        longform, longform_summary = _allocate_mixed(
            1, execution_scope={"task_branch": "LONGFORM",
                                "target_duration_seconds": 15.0,
                                "is_new_plan": True,
                                "script_mode": "simplified_v1"},
        )
        for item in longform:
            self.assertEqual(_mixed_contract_of(item), {})
        self.assertEqual(longform_summary["mixed_usable_count"], 0)
        self.assertEqual(longform_summary["mixed_duplicate_rejected_count"], 0)

        apparel, apparel_summary = _allocate_mixed(
            1, product_type="外套", top_category="女装", execution_scope=_SHORT_SCOPE,
        )
        for item in apparel:
            self.assertEqual(_mixed_contract_of(item), {})
        self.assertEqual(apparel_summary["mixed_usable_count"], 0)
        self.assertEqual(
            (apparel_summary["mixed_history_coverage"] or {}).get("scope_status"),
            "OUT_OF_SCOPE",
            "女装批次必须声明配饰混合模式不适用，零覆盖不等于去重通过",
        )
        self.assertEqual(
            (longform_summary["mixed_history_coverage"] or {}).get("scope_status"),
            "SCOPE_UNSUPPORTED",
            "借用同一入口的长视频规划必须声明该模式不适用",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
