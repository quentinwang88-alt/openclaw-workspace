"""C2 定向测试：一条主线冻结 —— 观察任务与中央口播共用同一份。

方案原文（C2）：冻结观众问题、核心价值、事实依据、可见回答、表达边界；主线同时进入
观察任务与中央口播；各模块任务分别对齐；**不把重复裁切自动算作第二个观察**。
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.mixed_mainline_contract import (  # noqa: E402
    MAINLINE_CONTRACT_ENV,
    MAINLINE_CONTRACT_SCHEMA,
    MAINLINE_STATUS_FROZEN,
    MAINLINE_STATUS_INCOMPLETE,
    MODULE_MAINLINE_LINK,
    build_mixed_mainline_contract,
    distinct_observation_count,
    mixed_mainline_enabled,
    observation_tasks,
    rebuild_mainline_for_frozen,
    repeated_observations,
    validate_mainline_contract,
)


def _unit(unit_id, module, view_scope, *, body_zone="", job="看清一点东西", index=None):
    return {
        "unit_id": unit_id,
        "unit_index": index,
        "module": module,
        "module_label": module,
        "view_scope": view_scope,
        "view_label": view_scope,
        "body_zone": body_zone,
        "observation_job": job,
        "product_state": "ALREADY_WORN" if module.startswith("WORN") else "HELD",
    }


def _mixed_contract(units):
    return {
        "template_id": "AMX_A_WORN_FIRST",
        "execution_profile": "ACCESSORY_MIXED_TEMPLATE_V1",
        "total_duration_seconds": 15,
        "capture_units": units,
    }


FOUR_UNITS = [
    _unit("CU_01", "WORN_DETAIL", "BODY_ZONE_CLOSE", body_zone="HAIR", job="发夹与固定发束的关系"),
    _unit("CU_02", "HANDHELD_PRODUCT", "HAND_AND_PRODUCT", job="看清商品本体造型"),
    _unit("CU_03", "STATIC_PRODUCT", "PRODUCT_AND_SURFACE", job="稳定停住看清细节"),
    _unit("CU_04", "WORN_RELATION", "BODY_ZONE_RELATION", body_zone="HAIR", job="与整体发型轮廓的关系"),
]


def _facts(**adaptation):
    merged = {
        "verdict": "ADAPTED_WITH_CEILING",
        "eligible": True,
        "allowed_wording": "只描述可见的造型、层次与排列",
        "forbidden_wording": ["纱"],
        "experience_authority": "NONE",
        "conflicts": [],
        **(adaptation or {}),
    }
    return {
        "schema_version": "selling-fact-evidence-v1",
        "source_text": "双层纱质蝴蝶造型，超唯美",
        "source_text_field": "source_operator_expression",
        "source": {"source_argument_id": "PCS_1"},
        "product_image_version": {"status": "UNKNOWN"},
        "fact_type": {"tier": "APPEARANCE_FACT", "tier_label_zh": "外观与装饰事实", "requires": "PRODUCT_IMAGE_OR_SPEC"},
        "adaptation": merged,
    }


class SwitchTest(unittest.TestCase):
    def test_the_switch_defaults_to_off(self):
        saved = os.environ.pop(MAINLINE_CONTRACT_ENV, None)
        try:
            self.assertFalse(mixed_mainline_enabled())
        finally:
            if saved is not None:
                os.environ[MAINLINE_CONTRACT_ENV] = saved

    def test_the_switch_reads_the_environment(self):
        saved = os.environ.get(MAINLINE_CONTRACT_ENV)
        try:
            os.environ[MAINLINE_CONTRACT_ENV] = "1"
            self.assertTrue(mixed_mainline_enabled())
            os.environ[MAINLINE_CONTRACT_ENV] = "0"
            self.assertFalse(mixed_mainline_enabled())
        finally:
            if saved is None:
                os.environ.pop(MAINLINE_CONTRACT_ENV, None)
            else:
                os.environ[MAINLINE_CONTRACT_ENV] = saved


class MainlineFieldTest(unittest.TestCase):
    """方案点名的五个字段，一个来源一个，缺了记缺口。"""

    def test_a_complete_input_freezes_all_five_fields(self):
        contract = build_mixed_mainline_contract(
            selling_argument={
                "core_value": "适合日常穿搭场景",
                "audience_situation": "可以在通勤和约会的穿搭里用",
                "allowed_strength": "factual",
            },
            fact_evidence=_facts(),
            semantic_spine={"script_thesis": {"core_buying_reason": "适合日常穿搭场景"}},
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertEqual(contract["schema_version"], MAINLINE_CONTRACT_SCHEMA)
        self.assertEqual(contract["status"], MAINLINE_STATUS_FROZEN)
        self.assertEqual(contract["input_gap"], [])
        self.assertEqual(contract["audience_question"], "可以在通勤和约会的穿搭里用")
        self.assertEqual(contract["core_value"], "适合日常穿搭场景")
        self.assertEqual(validate_mainline_contract(contract), [])

    def test_a_missing_audience_question_is_recorded_as_an_input_gap(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "适合日常穿搭场景"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertEqual(contract["status"], MAINLINE_STATUS_INCOMPLETE)
        self.assertEqual(contract["audience_question"], "")
        self.assertIn("AUDIENCE_QUESTION_UNAVAILABLE", contract["input_gap"])

    def test_an_internal_identifier_is_not_accepted_as_a_mainline(self):
        # R4 的口子：thesis 在规划期会退化成 ARGUMENT_OPERATOR_*。它不是文案，
        # 所以不能顶替观众问题，也不能顶替核心价值。
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "适合日常穿搭场景"},
            fact_evidence=_facts(),
            semantic_spine={
                "script_thesis": {
                    "core_buying_reason": "ARGUMENT_OPERATOR_PCS_ABCD_PCL_1234",
                    "primary_narrative_context": "ARGUMENT_OPERATOR_PCS_ABCD_PCL_1234",
                }
            },
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertEqual(contract["core_value"], "适合日常穿搭场景")
        self.assertNotIn("ARGUMENT_OPERATOR_", contract["audience_question"])
        errors = validate_mainline_contract({**contract, "core_value": "ARGUMENT_OPERATOR_X"})
        self.assertIn("MAINLINE_FIELD_NOT_READABLE:core_value", errors)

    def test_the_spine_buying_reason_wins_over_the_normalized_label(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "适合日常穿搭场景", "audience_situation": "通勤用"},
            fact_evidence=_facts(),
            semantic_spine={"script_thesis": {"core_buying_reason": "双层纱质蝴蝶造型，超唯美"}},
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertEqual(contract["core_value"], "双层纱质蝴蝶造型，超唯美")
        self.assertEqual(
            contract["source_refs"]["core_value"],
            "semantic_spine.script_thesis.core_buying_reason",
        )

    def test_the_fact_basis_carries_text_type_and_verdict(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "价值"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        basis = contract["fact_basis"]
        self.assertEqual(basis["source_text"], "双层纱质蝴蝶造型，超唯美")
        self.assertEqual(basis["fact_type"], "APPEARANCE_FACT")
        self.assertEqual(basis["verdict"], "ADAPTED_WITH_CEILING")
        self.assertEqual(basis["evidence_requirement"], "PRODUCT_IMAGE_OR_SPEC")
        self.assertEqual(basis["product_image_version_status"], "UNKNOWN")

    def test_a_missing_fact_record_is_a_gap_not_an_invention(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "价值", "audience_situation": "问题"},
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertIn("FACT_BASIS_UNAVAILABLE", contract["input_gap"])
        self.assertEqual(contract["fact_basis"]["source_text"], "")

    def test_the_expression_boundary_allows_one_main_value(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "价值", "audience_situation": "问题", "allowed_strength": "soft_only"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        boundary = contract["expression_boundary"]
        self.assertEqual(boundary["max_main_value_count"], 1)
        self.assertEqual(boundary["allowed_strength"], "soft_only")
        self.assertEqual(boundary["forbidden_wording"], ["纱"])
        self.assertEqual(boundary["experience_authority"], "NONE")

    def test_stacked_scene_ceilings_travel_into_the_boundary(self):
        conflict = {
            "kind": "MULTI_SCENARIO_UNAUTHORIZED",
            "stacked_scenes": ["约会", "度假"],
            "allowed_scenarios": 1,
            "resolution": "KEEP_ONE",
        }
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "价值", "audience_situation": "问题"},
            fact_evidence=_facts(conflicts=[conflict], allowed_wording="只说一个搭配"),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertEqual(contract["expression_boundary"]["conflicts"], [conflict])


class ObservationTaskTest(unittest.TestCase):
    """方案的各模块任务，与"重复裁切不算第二个观察"。"""

    def _contract(self, units):
        return build_mixed_mainline_contract(
            selling_argument={"core_value": "价值", "audience_situation": "问题"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(units),
        )

    def test_every_unit_gets_one_task_with_the_module_task_wording(self):
        tasks = observation_tasks(self._contract(FOUR_UNITS))
        self.assertEqual([task["unit_id"] for task in tasks], ["CU_01", "CU_02", "CU_03", "CU_04"])
        self.assertEqual(tasks[0]["task_zh"], "直接展示主线相关的结果（佩戴／造型开场）")
        self.assertEqual(tasks[1]["task_zh"], "看清商品本体轮廓、排列与已确认结构")
        self.assertEqual(tasks[2]["task_zh"], "集中看一个需要稳定画面的细节")
        self.assertEqual(tasks[3]["task_zh"], "解释佩戴关系与比例搭配关系")

    def test_each_module_maps_to_its_mainline_link(self):
        tasks = observation_tasks(self._contract(FOUR_UNITS))
        for task in tasks:
            self.assertEqual(task["mainline_link"], MODULE_MAINLINE_LINK[task["module"]])

    def test_the_visible_answer_prefers_the_worn_opening_shot(self):
        contract = self._contract(FOUR_UNITS)
        self.assertEqual(contract["visible_answer"]["shot_ref"], "CU_01")
        self.assertEqual(contract["visible_answer"]["module"], "WORN_DETAIL")
        self.assertIn("价值", contract["visible_answer"]["text"])

    def test_the_visible_answer_falls_back_when_there_is_no_worn_shot(self):
        units = [
            _unit("CU_01", "HANDHELD_PRODUCT", "HAND_AND_PRODUCT", job="看清本体"),
            _unit("CU_02", "STATIC_PRODUCT", "PRODUCT_AND_SURFACE", job="看清细节"),
        ]
        contract = self._contract(units)
        self.assertEqual(contract["visible_answer"]["shot_ref"], "CU_01")
        self.assertEqual(contract["visible_answer"]["module"], "HANDHELD_PRODUCT")

    def test_reframing_the_same_view_is_not_a_second_observation(self):
        units = [
            _unit("CU_01", "WORN_DETAIL", "BODY_ZONE_CLOSE", body_zone="HAIR", job="发夹与发束"),
            _unit("CU_02", "WORN_DETAIL", "BODY_ZONE_CLOSE", body_zone="HAIR", job="同一处再裁一次"),
        ]
        contract = self._contract(units)
        tasks = observation_tasks(contract)
        self.assertTrue(tasks[0]["counts_as_new_observation"])
        self.assertFalse(tasks[1]["counts_as_new_observation"])
        self.assertEqual(tasks[1]["same_observation_as"], "CU_01")
        self.assertEqual(distinct_observation_count(contract), 1)
        self.assertEqual(
            repeated_observations(contract),
            [
                {
                    "distinct_observation_key": tasks[0]["distinct_observation_key"],
                    "unit_ids": ["CU_01", "CU_02"],
                }
            ],
        )
        self.assertEqual(contract["repeated_observation_units"], ["CU_02"])

    def test_first_and_last_worn_shots_stay_two_observations(self):
        # 方案：首尾可同一佩戴状态，但要有细节与整体关系的区别。
        units = [
            _unit("CU_01", "WORN_DETAIL", "BODY_ZONE_CLOSE", body_zone="HAIR", job="发束关系"),
            _unit("CU_04", "WORN_RELATION", "BODY_ZONE_RELATION", body_zone="HAIR", job="整体轮廓关系"),
        ]
        contract = self._contract(units)
        self.assertEqual(distinct_observation_count(contract), 2)
        self.assertEqual(repeated_observations(contract), [])

    def test_the_same_view_scope_on_a_different_body_zone_is_distinct(self):
        units = [
            _unit("CU_01", "WORN_DETAIL", "BODY_ZONE_CLOSE", body_zone="HAIR"),
            _unit("CU_02", "WORN_DETAIL", "BODY_ZONE_CLOSE", body_zone="WRIST"),
        ]
        self.assertEqual(distinct_observation_count(self._contract(units)), 2)


class RebuildForFrozenTest(unittest.TestCase):
    """历史冻结包没有主线合同时，离线重建用于审阅，不改动老方案的行为。"""

    def _frozen(self):
        return {
            "requested_hook_id": "AUDIENCE_NEED_CALLOUT",
            "content_bundle_brief": {
                "selling_argument": {
                    "core_value": "适合日常穿搭场景",
                    "audience_situation": "可以在通勤时用",
                    "fact_evidence": _facts(),
                },
                "audience_tension_text": "",
            },
            "semantic_spine_contract": {"script_thesis": {"core_buying_reason": "适合日常穿搭场景"}},
            "category_execution_extension": {"mixed_template_contract": _mixed_contract(FOUR_UNITS)},
        }

    def test_rebuild_reads_the_package_own_data(self):
        contract = rebuild_mainline_for_frozen(self._frozen())
        self.assertEqual(contract["status"], MAINLINE_STATUS_FROZEN)
        self.assertEqual(contract["core_value"], "适合日常穿搭场景")
        self.assertEqual(contract["requested_hook_id"], "AUDIENCE_NEED_CALLOUT")
        self.assertEqual(distinct_observation_count(contract), 4)

    def test_rebuild_without_a_mixed_contract_reports_the_gap(self):
        frozen = self._frozen()
        frozen["category_execution_extension"] = {}
        contract = rebuild_mainline_for_frozen(frozen)
        self.assertIn("VISIBLE_ANSWER_UNAVAILABLE", contract["input_gap"])
        self.assertEqual(observation_tasks(contract), [])

    def test_rebuild_of_an_empty_package_is_empty(self):
        self.assertEqual(rebuild_mainline_for_frozen({}), {})
        self.assertEqual(rebuild_mainline_for_frozen(None), {})

    def test_an_explicit_fact_record_overrides_the_package_one(self):
        frozen = self._frozen()
        frozen["content_bundle_brief"]["selling_argument"].pop("fact_evidence")
        contract = rebuild_mainline_for_frozen(frozen, fact_evidence=_facts())
        self.assertEqual(contract["fact_basis"]["source_text"], "双层纱质蝴蝶造型，超唯美")


class CoreValueWordingTest(unittest.TestCase):
    """核心价值不得违反自己那份表达边界（实测：原文含本合同禁的词）。"""

    def test_a_banned_word_in_the_quoted_sentence_is_stripped_for_speaking(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"creative_core_value": "双层纱质蝴蝶造型，超唯美"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        # 原文保留供审计，口播只能拿到剔除后的那一句。
        self.assertEqual(contract["core_value"], "双层纱质蝴蝶造型，超唯美")
        self.assertEqual(contract["core_value_safe"], "双层蝴蝶造型，超唯美")
        self.assertTrue(contract["expression_boundary"]["core_value_wording_constrained"])
        self.assertEqual(
            contract["expression_boundary"]["core_value_forbidden_matched"], ["纱"]
        )

    def test_a_clean_core_value_is_left_alone(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"creative_core_value": "适合日常穿搭场景"},
            fact_evidence=_facts(verdict="ADAPTED", forbidden_wording=[]),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertEqual(contract["core_value_safe"], "适合日常穿搭场景")
        self.assertFalse(contract["expression_boundary"]["core_value_wording_constrained"])

    def test_the_visible_answer_uses_the_speakable_value(self):
        # 可见回答是从核心价值派生的，也必须用可说口径 —— 否则刚被表达边界收窄
        # 掉的措辞会从这条通路重新流回下游。
        contract = build_mixed_mainline_contract(
            selling_argument={"creative_core_value": "双层纱质蝴蝶造型，超唯美"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertNotIn("纱", contract["visible_answer"]["text"])
        self.assertTrue(
            contract["visible_answer"]["text"].startswith("双层蝴蝶造型"),
            contract["visible_answer"]["text"],
        )

    def test_a_core_value_that_disappears_entirely_is_an_input_gap(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"creative_core_value": "雪纺"},
            fact_evidence=_facts(forbidden_wording=["雪纺"]),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        self.assertEqual(contract["core_value_safe"], "")
        self.assertIn("CORE_VALUE_WORDING_UNAVAILABLE", contract["input_gap"])
        self.assertEqual(contract["status"], MAINLINE_STATUS_INCOMPLETE)


class ValidationTest(unittest.TestCase):
    def test_a_missing_contract_is_an_error(self):
        self.assertEqual(validate_mainline_contract({}), ["MAINLINE_CONTRACT_MISSING"])
        self.assertEqual(validate_mainline_contract(None), ["MAINLINE_CONTRACT_MISSING"])

    def test_each_missing_field_is_named(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "价值", "audience_situation": "问题"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        broken = dict(contract)
        broken["visible_answer"] = {}
        errors = validate_mainline_contract(broken)
        self.assertIn("MAINLINE_FIELD_MISSING:visible_answer", errors)

    def test_a_visible_answer_without_a_shot_is_an_error(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "价值", "audience_situation": "问题"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract(FOUR_UNITS),
        )
        broken = dict(contract)
        broken["visible_answer"] = {"text": "说了要看到但没指镜头"}
        self.assertIn("MAINLINE_VISIBLE_ANSWER_WITHOUT_SHOT", validate_mainline_contract(broken))

    def test_an_observation_free_contract_is_an_error(self):
        contract = build_mixed_mainline_contract(
            selling_argument={"core_value": "价值", "audience_situation": "问题"},
            fact_evidence=_facts(),
            mixed_contract=_mixed_contract([]),
        )
        self.assertIn("MAINLINE_OBSERVATION_TASKS_EMPTY", validate_mainline_contract(contract))
