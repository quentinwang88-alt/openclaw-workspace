"""C3 定向测试：口播三类核对、载荷白名单、语种标签。

方案原文（C3）：成稿核对分三类 —— 可见结果落在具体镜头；规格事实核对来源但不强求每句
镜头证明；建议与审美不伪装为实测。口播只接收目标语言、一个主价值、必要事实、实际可见
回答、可选语境及体验边界。交付文档语言标签动态读取 ``target_language``。
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.mixed_voiceover_mainline import (  # noqa: E402
    CHECK_ATTENTION,
    CHECK_FAIL,
    CHECK_NOT_APPLICABLE,
    CHECK_PASS,
    FORBIDDEN_PAYLOAD_KEYS,
    reduce_voiceover_payload,
    resolve_language_label,
    review_voiceover_against_mainline,
    voiceover_payload_reduction,
)


def _mainline(
    *,
    tier="APPEARANCE_FACT",
    verdict="ADAPTED_WITH_CEILING",
    image="UNKNOWN",
    conflicts=None,
    forbidden=None,
    shot_ref="CU_01",
):
    return {
        "schema_version": "mixed-mainline-contract-v1",
        "status": "FROZEN",
        "audience_question": "问题",
        "core_value": "双层纱质蝴蝶造型，超唯美",
        "fact_basis": {
            "source_text": "双层纱质蝴蝶造型，超唯美",
            "fact_type": tier,
            "verdict": verdict,
            "product_image_version_status": image,
        },
        "visible_answer": {"shot_ref": shot_ref, "module": "WORN_DETAIL", "text": "能看到什么"},
        "expression_boundary": {
            "allowed_wording": "只描述可见的造型、层次与排列",
            # ``forbidden`` 显式传空列表 = 只有封顶、没有禁词的边界（真实批次里
            # 多场景封顶就是这个形状）。默认仍是 ["纱"]，其余用例不受影响。
            "forbidden_wording": ["纱"] if forbidden is None else list(forbidden),
            "experience_authority": "NONE",
            "max_main_value_count": 1,
            "conflicts": list(conflicts or []),
        },
        "observation_tasks": [
            {
                "unit_id": "CU_01",
                "module": "WORN_DETAIL",
                "distinct_observation_key": "BODY_ZONE_CLOSE|HAIR|WORN_DETAIL",
                "counts_as_new_observation": True,
            }
        ],
    }


def _script(*units, voice=None, lines=None):
    return {
        "continuous_voiceover": {
            "target_language": "越南语",
            "target_text": "Mình vừa tìm được chiếc kẹp này",
            "chinese_translation": "我刚发现这款抓夹",
            **(voice or {}),
        },
        "storyboard": [
            {"capture_unit_id": unit, "visual_content": "画面"}
            for unit in (units or ("CU_01",))
        ],
        **(lines or {}),
    }


class VisibleResultBucketTest(unittest.TestCase):
    """可见结果必须落在具体镜头上。"""

    def test_a_visible_answer_landing_on_a_real_shot_passes(self):
        review = review_voiceover_against_mainline(_script("CU_01", "CU_02"), _mainline())
        bucket = review["buckets"]["visible_result"]
        self.assertEqual(bucket["status"], CHECK_PASS)
        self.assertEqual(bucket["shot_ref"], "CU_01")
        self.assertEqual(bucket["storyboard_units"], ["CU_01", "CU_02"])

    def test_a_visible_answer_without_its_shot_fails(self):
        review = review_voiceover_against_mainline(_script("CU_02"), _mainline())
        bucket = review["buckets"]["visible_result"]
        self.assertEqual(bucket["status"], CHECK_FAIL)
        self.assertEqual(review["status"], CHECK_FAIL)

    def test_a_script_without_capture_units_fails_rather_than_passes_silently(self):
        script = {
            "continuous_voiceover": {"target_language": "越南语"},
            "storyboard": [],
        }
        review = review_voiceover_against_mainline(script, _mainline())
        self.assertEqual(review["buckets"]["visible_result"]["status"], CHECK_FAIL)

    def test_no_mainline_contract_is_not_applicable(self):
        review = review_voiceover_against_mainline(_script("CU_01"), {})
        self.assertEqual(review["status"], CHECK_NOT_APPLICABLE)
        self.assertEqual(review["reason"], "NO_MAINLINE_CONTRACT")
        self.assertEqual(review["buckets"], {})


class SpecFactBucketTest(unittest.TestCase):
    """核实来源，但不强求每句镜头证明。"""

    def test_an_unsourced_performance_claim_needs_attention(self):
        review = review_voiceover_against_mainline(
            _script("CU_01"), _mainline(tier="PRODUCT_PERFORMANCE", verdict="NEEDS_SOURCE")
        )
        bucket = review["buckets"]["spec_fact"]
        self.assertEqual(bucket["status"], CHECK_ATTENTION)
        self.assertFalse(bucket["shot_proof_required"])

    def test_a_sourced_performance_claim_passes(self):
        review = review_voiceover_against_mainline(
            _script("CU_01"),
            _mainline(tier="PERSONAL_EXPERIENCE", verdict="ADAPTED"),
        )
        self.assertEqual(review["buckets"]["spec_fact"]["status"], CHECK_PASS)

    def test_an_appearance_claim_without_an_image_version_needs_attention(self):
        review = review_voiceover_against_mainline(
            _script("CU_01"), _mainline(tier="APPEARANCE_FACT", image="UNKNOWN")
        )
        bucket = review["buckets"]["spec_fact"]
        self.assertEqual(bucket["status"], CHECK_ATTENTION)
        self.assertIn("图片版本", bucket["detail"])

    def test_an_appearance_claim_with_an_image_version_passes(self):
        review = review_voiceover_against_mainline(
            _script("CU_01"), _mainline(tier="APPEARANCE_FACT", image="AVAILABLE")
        )
        self.assertEqual(review["buckets"]["spec_fact"]["status"], CHECK_PASS)

    def test_a_styling_claim_does_not_require_a_spec_source(self):
        review = review_voiceover_against_mainline(
            _script("CU_01"), _mainline(tier="STYLING_SUGGESTION", verdict="ADAPTED_WITH_CEILING")
        )
        bucket = review["buckets"]["spec_fact"]
        self.assertEqual(bucket["status"], CHECK_PASS)
        self.assertIn("不依赖规格来源", bucket["detail"])


class SuggestionBucketTest(unittest.TestCase):
    """建议与审美不伪装为实测。"""

    def test_unauthorised_experience_wording_in_the_finished_copy_is_flagged(self):
        review = review_voiceover_against_mainline(
            _script("CU_01", voice={"chinese_translation": "我试了一天，随手一夹就完成"}),
            _mainline(),
        )
        bucket = review["buckets"]["suggestion_and_aesthetic"]
        self.assertEqual(bucket["status"], CHECK_ATTENTION)
        self.assertIn("随手一夹", bucket["unsourced_experience_wording"])
        self.assertEqual(review["status"], CHECK_ATTENTION)

    def test_the_same_wording_is_fine_once_a_human_recorded_it(self):
        contract = _mainline()
        contract["expression_boundary"]["experience_authority"] = "OPERATOR_CONFIRMED_HISTORY"
        review = review_voiceover_against_mainline(
            _script("CU_01", voice={"chinese_translation": "我试了一天，随手一夹就完成"}),
            contract,
        )
        self.assertEqual(review["buckets"]["suggestion_and_aesthetic"]["status"], CHECK_PASS)

    def test_aesthetic_wording_alone_is_not_a_pretend_test(self):
        review = review_voiceover_against_mainline(
            _script("CU_01", voice={"chinese_translation": "我喜欢这个层次，看着柔美"}),
            _mainline(),
        )
        self.assertEqual(review["buckets"]["suggestion_and_aesthetic"]["status"], CHECK_PASS)

    def test_stacking_ceilings_travel_into_the_bucket(self):
        conflict = {"kind": "MULTI_SCENARIO_UNAUTHORIZED", "allowed_scenarios": 1, "resolution": "KEEP_ONE"}
        review = review_voiceover_against_mainline(
            _script("CU_01"), _mainline(tier="STYLING_SUGGESTION", conflicts=[conflict])
        )
        bucket = review["buckets"]["suggestion_and_aesthetic"]
        self.assertEqual(bucket["wording_ceilings"], ["MULTI_SCENARIO_UNAUTHORIZED"])
        self.assertIn("MULTI_SCENARIO_UNAUTHORIZED", bucket["stacking_note"])

    def test_the_review_reports_the_distinct_observation_count(self):
        review = review_voiceover_against_mainline(_script("CU_01"), _mainline())
        self.assertEqual(review["distinct_observation_count"], 1)


class PayloadReductionTest(unittest.TestCase):
    """口播只接收该接收的；不许送的东西要能被查出来。"""

    def test_a_clean_payload_reports_nothing_forbidden(self):
        reduction = voiceover_payload_reduction(
            {"target_language": "越南语", "content_mainline": "价值", "mixed_mainline_contract": {}}
        )
        self.assertTrue(reduction["reduction_ok"])
        self.assertEqual(reduction["forbidden_keys_present"], [])
        self.assertEqual(reduction["required_keys_missing"], [])

    def test_forbidden_blocks_are_named(self):
        reduction = voiceover_payload_reduction(
            {"selling_point_catalog": [1, 2, 3], "action_library": {}, "error_code": "X"}
        )
        self.assertFalse(reduction["reduction_ok"])
        self.assertEqual(
            reduction["forbidden_keys_present"],
            ["selling_point_catalog", "action_library", "error_code"],
        )

    def test_the_reducer_drops_them_and_keeps_everything_else(self):
        reduced = reduce_voiceover_payload(
            {"selling_point_catalog": [1], "content_mainline": "价值", "error_code": "X"}
        )
        self.assertEqual(reduced, {"content_mainline": "价值"})

    def test_the_reducer_is_a_noop_on_a_clean_payload(self):
        clean = {"content_mainline": "价值", "claim_atoms": []}
        self.assertEqual(reduce_voiceover_payload(clean), clean)

    def test_the_named_forbidden_keys_cover_the_plan_three(self):
        for key in ("selling_point_catalog", "action_library", "error_code"):
            self.assertIn(key, FORBIDDEN_PAYLOAD_KEYS)

    def test_the_production_entry_never_ships_a_forbidden_block(self):
        # 白名单必须长在中央引擎真正收到的那份 contract 上。曾经它只存在于一个
        # 没人调用的平行函数里，于是「没有投喂全部候选卖点」只是测试里成立。
        from core.reality_voiceover_bridge import build_voiceover_expression_contract

        direction = {
            "content_bundle_brief": {
                "selling_point_catalog": [{"value_id": "V1"}],
                "action_library": {"templates": []},
                "error_code": "PLAN_REJECTED",
                "content_mainline": "价值",
                "mixed_mainline_contract": {"core_value_safe": "价值"},
            }
        }
        contract = build_voiceover_expression_contract(direction, {"shots": []})
        reduction = voiceover_payload_reduction(contract)
        self.assertTrue(reduction["reduction_ok"], reduction["forbidden_keys_present"])
        self.assertEqual(contract["mixed_mainline_contract"]["core_value_safe"], "价值")

    def test_the_speakable_core_value_wins_for_the_voiceover(self):
        # 主线的 core_value 是运营原文，可能带着本合同自己禁的词；口播要拿的是
        # 剔除之后的那一句，否则模型会照写刚被口径拒掉的材质断言。
        from core.reality_voiceover_bridge import build_voiceover_expression_contract

        direction = {
            "content_bundle_brief": {
                "mixed_mainline_contract": {
                    "core_value": "双层纱质蝴蝶造型，超唯美",
                    "core_value_safe": "双层蝴蝶造型，超唯美",
                }
            }
        }
        contract = build_voiceover_expression_contract(direction, {"shots": []})
        self.assertEqual(contract["content_mainline"], "双层蝴蝶造型，超唯美")

    def test_an_older_package_without_the_safe_value_still_works(self):
        # 早于该字段冻结的包没有 core_value_safe，退回 core_value，行为不变。
        from core.reality_voiceover_bridge import build_voiceover_expression_contract

        direction = {
            "content_bundle_brief": {"mixed_mainline_contract": {"core_value": "旧包的价值"}}
        }
        contract = build_voiceover_expression_contract(direction, {"shots": []})
        self.assertEqual(contract["content_mainline"], "旧包的价值")

    def test_the_named_reduction_is_the_same_code_path(self):
        # 别名的意义在于：不存在第二条没人走的路径。
        from core.reality_voiceover_bridge import (
            build_voiceover_expression_contract,
            build_voiceover_expression_contract_reduced,
        )

        direction = {"content_bundle_brief": {"content_mainline": "价值"}}
        self.assertEqual(
            build_voiceover_expression_contract_reduced(direction, {"shots": []}),
            build_voiceover_expression_contract(direction, {"shots": []}),
        )

    def test_the_real_contract_reports_no_missing_required_key(self):
        # 必需键清单要描述真实的 contract：目标语言由口播入口的调用参数携带，
        # 不在 contract 里，把它列进来会让每次检查都报一条修不掉的缺件。
        from core.reality_voiceover_bridge import build_voiceover_expression_contract

        direction = {
            "content_bundle_brief": {
                "content_mainline": "价值",
                "mixed_mainline_contract": {"core_value_safe": "价值"},
            }
        }
        contract = build_voiceover_expression_contract(direction, {"shots": []})
        self.assertEqual(voiceover_payload_reduction(contract)["required_keys_missing"], [])


class LanguageLabelTest(unittest.TestCase):
    """交付文档的语种标签只认 target_language。"""

    def test_the_script_language_wins_and_is_named(self):
        resolved = resolve_language_label(_script("CU_01"), batch={"target_language": "越南语"})
        self.assertEqual(resolved["label"], "越南语")
        self.assertEqual(
            resolved["label_source"], "script.continuous_voiceover.target_language"
        )
        self.assertFalse(resolved["mismatch"])

    def test_the_utterance_is_never_used_as_the_label(self):
        resolved = resolve_language_label({"continuous_voiceover": {"target_text": "Xin chào"}})
        self.assertEqual(resolved["label"], "")
        self.assertEqual(resolved["label_source"], "UNAVAILABLE")
        self.assertEqual(resolved["utterance"], "Xin chào")
        self.assertTrue(resolved["utterance_is_not_a_label"])

    def test_the_batch_language_is_the_second_choice(self):
        resolved = resolve_language_label({}, batch={"target_language": "马来语"})
        self.assertEqual(resolved["label"], "马来语")
        self.assertEqual(resolved["label_source"], "batch.target_language")

    def test_a_disagreement_is_reported_instead_of_hidden(self):
        resolved = resolve_language_label(
            _script("CU_01", voice={"target_language": "马来语"}), batch={"target_language": "越南语"}
        )
        self.assertTrue(resolved["mismatch"])
        self.assertEqual(resolved["script_language"], "马来语")
        self.assertEqual(resolved["batch_language"], "越南语")


class RendererLabelTest(unittest.TestCase):
    """渲染器把语种标签与正文分开（Review R3）。"""

    def test_the_stage0_prompt_labels_the_language_not_the_utterance(self):
        from core.production_script_renderer import render_stage0_video_generation_prompt

        text = render_stage0_video_generation_prompt(
            script={
                "continuous_voiceover": {
                    "target_language": "越南语",
                    "target_text": "Xin chào các bạn",
                },
                "capture_units": [{"capture_unit_id": "CU_01", "structure_role": "MOMENT"}],
            },
            duration_seconds=15,
        )
        self.assertIn("【连续口播｜必须原样使用目标语言】", text)
        self.assertIn("越南语", text)

    def test_the_language_label_does_not_come_from_the_utterance(self):
        from core.production_script_renderer import render_stage0_video_generation_prompt

        text = render_stage0_video_generation_prompt(
            script={
                "continuous_voiceover": {"target_text": "Xin chào các bạn"},
                "capture_units": [{"capture_unit_id": "CU_01", "structure_role": "MOMENT"}],
            },
            duration_seconds=15,
        )
        # 没有 target_language 时退回到正文（原来就是这个行为），但不能凭空编一个语种。
        self.assertNotIn("马来语", text)
        self.assertNotIn("泰语", text)

    def test_the_production_script_labels_language_and_text_separately(self):
        from core.production_script_renderer import render_complete_production_script

        class _Item:
            macro_family_key = ""
            carrier_mode = "MIXED"
            actual_hook_id = ""
            requested_hook_id = ""
            batch_item_id = "I1"
            item_index = 1

            def __init__(self, payload):
                self.result_json = json.dumps(payload, ensure_ascii=False)

        item = _Item(
            {
                "script": {
                    "continuous_voiceover": {
                        "target_language": "越南语",
                        "target_text": "Xin chào các bạn",
                        "chinese_translation": "大家好",
                    },
                    "capture_units": [{"capture_unit_id": "CU_01", "structure_role": "MOMENT"}],
                }
            }
        )
        text = render_complete_production_script(item=item, duration_seconds=15)
        self.assertIn("目标语言：越南语", text)
        self.assertIn("目标语言正文：Xin chào các bạn", text)
        self.assertNotIn("目标语言：Xin chào", text)


class _BoundaryPayloadMixin:
    """真实批次里含「纱」的每一类来源路径，用来验证清洗覆盖面。"""

    def _payload(self):
        return {
            "content_mainline": "双层蝴蝶造型，超唯美",
            "mixed_mainline_contract": _mainline(),
            "argument_contract": {
                "content": {
                    "value_proposition": {"text": "双层纱质蝴蝶造型，超唯美"},
                    "audience_tension": {"text": "纱质发饰总显廉价"},
                    "selling_argument": {
                        "operator_expression": "双层纱质蝴蝶造型",
                        "core_value": "双层纱质蝴蝶造型，超唯美",
                    },
                    "proof_atoms": [{"claim_key": "C1", "fact_text": "纱质双层结构"}],
                    "argument_context_alignment": {"allowed_spoken_context": "双层纱质蝴蝶造型"},
                },
                "forbidden_claims": ["未经授权的舒适、保暖或材质性能"],
            },
            "claim_atoms": [{"claim_key": "C1", "fact_text": "双层纱质蝴蝶造型"}],
            "context_bridge_contract": {"allowed_spoken_context": "双层纱质蝴蝶造型"},
        }


class ExpressionBoundaryTest(_BoundaryPayloadMixin, unittest.TestCase):
    """表达边界的三个口子：清洗、禁止层、成品兜底。

    真实批次暴露过：主线把核心价值清洗成「双层蝴蝶造型」，同一条片子的口播却说出
    「双层纱质蝴蝶造型」（目标语言 ``dáng bướm bằng voan hai lớp``）。清洗只落在
    ``content_mainline`` 一个字段，其余正面授权字段仍是原文 —— 禁止的措辞从来没有
    变成过约束。下面的用例逐条钉住这几件事。
    """

    def test_an_empty_boundary_leaves_the_payload_untouched(self):
        from core.mixed_voiceover_mainline import apply_expression_boundary_layer

        payload = self._payload()
        before = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        # 边界存在但**什么都没声明**：没有禁词、没有允许口径、没有封顶。
        # 这种包必须逐字不变 —— 旧契约的错在于把"逐字不变"扩大到了所有无禁词的包。
        mainline = {"expression_boundary": {"forbidden_wording": []}}
        applied, report = apply_expression_boundary_layer(payload, mainline)
        self.assertFalse(report["applied"])
        self.assertFalse(report["layer_present"])
        self.assertEqual(report["declared_constraints"], [])
        self.assertEqual(json.dumps(applied, ensure_ascii=False, sort_keys=True), before)

    def test_a_ceiling_only_boundary_still_ships_its_constraint(self):
        """没有禁词、但有封顶的包，约束必须**下发**。

        真实批次暴露过：多场景封顶刻意不走 ``forbidden_wording``（单个场景词是对的，
        错的只是并列堆叠），它只存在于 ``allowed_wording`` / ``conflicts`` 里。先前把
        下发条件绑在"有没有禁词"上，于是手镯／戒指两条真实稿把三个场景并列说出来，
        而 ``applied=False``、全链路都报 PASS。
        """

        from core.mixed_voiceover_mainline import BOUNDARY_LAYER_KEY, apply_expression_boundary_layer

        payload = self._payload()
        mainline = {
            "expression_boundary": {
                "forbidden_wording": [],
                "allowed_wording": "只说一个与实际冻结场景兼容的搭配，不并列多个",
                "allowed_strength": "factual",
                "max_main_value_count": 1,
                "conflicts": [
                    {
                        "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                        "stacked_scenes": ["日常", "约会", "上班"],
                        "allowed_scenarios": 1,
                        "resolution": "KEEP_ONE",
                    }
                ],
            }
        }
        applied, report = apply_expression_boundary_layer(payload, mainline)
        # 清洗没发生（本来就没有禁词），但约束下发了 —— 两件事必须分开报。
        self.assertFalse(report["applied"])
        self.assertTrue(report["layer_present"])
        self.assertEqual(
            report["declared_constraints"],
            ["allowed_wording", "wording_ceilings", "allowed_strength"],
        )
        layer = applied[BOUNDARY_LAYER_KEY]
        self.assertEqual(layer["allowed_wording"], "只说一个与实际冻结场景兼容的搭配，不并列多个")
        self.assertEqual(
            [item["kind"] for item in layer["wording_ceilings"]],
            ["MULTI_SCENARIO_UNAUTHORIZED"],
        )

    def test_a_scenario_ceiling_ships_its_numeric_limit(self):
        """封顶的**数值**必须跟着类型一起下发。

        编译 ceilings 时曾写死只搬 ``allowed_looks``，而多场景那条用的是
        ``allowed_scenarios`` —— 实测产出 ``{"kind": ..., "allowed_looks": null}``：
        模型只收到一个类型名，不知道"最多几个"。约束说"不许并列"，却没说上限是 1。
        """

        from core.mixed_voiceover_mainline import BOUNDARY_LAYER_KEY, apply_expression_boundary_layer

        payload = self._payload()
        mainline = {
            "expression_boundary": {
                "forbidden_wording": [],
                "allowed_wording": "只说一个与实际冻结场景兼容的搭配，不并列多个",
                "conflicts": [
                    {
                        "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                        "stacked_scenes": ["日常", "约会", "上班"],
                        "allowed_scenarios": 1,
                        "resolution": "KEEP_ONE",
                    }
                ],
            }
        }
        applied, _ = apply_expression_boundary_layer(payload, mainline)
        ceiling = applied[BOUNDARY_LAYER_KEY]["wording_ceilings"][0]
        self.assertEqual(ceiling["limit_field"], "allowed_scenarios")
        self.assertEqual(ceiling["max_allowed"], 1)
        self.assertEqual(ceiling["allowed_scenarios"], 1)
        self.assertEqual(ceiling["resolution"], "KEEP_ONE")

    def test_a_look_ceiling_reads_its_own_limit_field(self):
        """多造型那条走 ``allowed_looks``：限值字段按 kind 取，不能写死单字段名。"""

        from core.mixed_voiceover_mainline import BOUNDARY_LAYER_KEY, apply_expression_boundary_layer

        payload = self._payload()
        mainline = {
            "expression_boundary": {
                "forbidden_wording": [],
                "allowed_wording": "只说一个造型",
                "conflicts": [
                    {
                        "kind": "MULTI_LOOK_UNAUTHORIZED",
                        "stacked_looks": ["造型甲", "造型乙"],
                        "allowed_looks": 1,
                        "resolution": "KEEP_ONE",
                    }
                ],
            }
        }
        applied, _ = apply_expression_boundary_layer(payload, mainline)
        ceiling = applied[BOUNDARY_LAYER_KEY]["wording_ceilings"][0]
        self.assertEqual(ceiling["limit_field"], "allowed_looks")
        self.assertEqual(ceiling["max_allowed"], 1)
        self.assertIsNone(ceiling["allowed_scenarios"])

    def test_no_mainline_contract_leaves_the_payload_untouched(self):
        from core.mixed_voiceover_mainline import apply_expression_boundary_layer

        payload = self._payload()
        before = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        applied, report = apply_expression_boundary_layer(payload, {})
        self.assertFalse(report["applied"])
        self.assertFalse(report["layer_present"])
        self.assertEqual(json.dumps(applied, ensure_ascii=False, sort_keys=True), before)

    def test_banned_wording_is_removed_from_every_speakable_field(self):
        from core.mixed_voiceover_mainline import (
            apply_expression_boundary_layer,
            speakable_forbidden_hits,
        )

        applied, report = apply_expression_boundary_layer(self._payload(), _mainline())
        # fail-closed：默认清洗全部可讲字段，而不是维护一张"值得清洗的字段"白名单。
        self.assertGreaterEqual(report["cleaned_field_count"], 6, report["cleaned_paths"])
        self.assertEqual(speakable_forbidden_hits(applied, ["纱"]), [])
        self.assertNotIn("纱", json.dumps(applied["claim_atoms"], ensure_ascii=False))
        self.assertNotIn(
            "纱", applied["context_bridge_contract"]["allowed_spoken_context"]
        )

    def test_the_boundary_namespace_keeps_the_banned_terms(self):
        from core.mixed_voiceover_mainline import apply_expression_boundary_layer

        applied, _ = apply_expression_boundary_layer(self._payload(), _mainline())
        # 把"不许说什么"本身清掉，约束就没有内容了。
        self.assertEqual(
            applied["mixed_mainline_contract"]["expression_boundary"]["forbidden_wording"],
            ["纱"],
        )
        self.assertEqual(
            applied["argument_contract"]["forbidden_claims"],
            ["未经授权的舒适、保暖或材质性能"],
        )

    def test_the_payload_carries_an_explicit_forbidden_layer(self):
        from core.mixed_voiceover_mainline import BOUNDARY_LAYER_KEY, apply_expression_boundary_layer

        applied, report = apply_expression_boundary_layer(self._payload(), _mainline())
        layer = applied[BOUNDARY_LAYER_KEY]
        self.assertTrue(report["boundary_layer_present"])
        self.assertEqual(layer["forbidden_wording"], ["纱"])
        self.assertEqual(layer["rule_scope"], "ANY_LANGUAGE_INCLUDING_TARGET_TEXT")
        self.assertTrue(layer["is_constraint_not_material"])

    def test_the_forbidden_layer_rule_carries_no_category_terms(self):
        # 喂给模型的指引文案不得包含会被下游硬门禁枪毙的类目术语。
        from core.mixed_voiceover_mainline import compile_expression_boundary_layer

        rule = compile_expression_boundary_layer(_mainline())["forbidden_wording_rule"]
        self.assertIn("任何语言", rule)
        for term in ("单只", "一对"):
            self.assertNotIn(term, rule)

    def test_the_cleaning_report_keeps_the_original_wording(self):
        from core.mixed_voiceover_mainline import apply_expression_boundary_layer

        _, report = apply_expression_boundary_layer(self._payload(), _mainline())
        originals = report["cleaned_originals"]
        # 清洗把「双层纱质蝴蝶造型，超唯美」从模型输入里删掉是对的；审计不能因此丢掉原句。
        self.assertEqual(
            originals["mixed_mainline_contract.core_value"], "双层纱质蝴蝶造型，超唯美"
        )
        self.assertTrue(all("纱" in value for value in originals.values()))

    def test_the_production_entry_applies_it_not_a_parallel_function(self):
        # 「声明了合同 ≠ 合同生效」：禁止层必须长在中央引擎真正收到的那份 contract 上。
        from core.reality_voiceover_bridge import build_voiceover_expression_contract

        direction = {
            "content_bundle_brief": {
                "content_mainline": "双层蝴蝶造型，超唯美",
                "claim_atoms": [{"claim_key": "C1", "fact_text": "双层纱质蝴蝶造型"}],
                "mixed_mainline_contract": _mainline(),
            }
        }
        contract = build_voiceover_expression_contract(direction, {"shots": []})
        self.assertIn("voiceover_expression_boundary", contract)
        self.assertEqual(
            contract["voiceover_expression_boundary"]["forbidden_wording"], ["纱"]
        )
        self.assertNotIn("纱", contract["claim_atoms"][0]["fact_text"])

    def test_the_boundary_terms_are_read_from_the_frozen_mainline(self):
        from core.reality_voiceover_bridge import (
            _boundary_forbidden_terms,
            _strip_banned_terms,
        )

        direction = {"mixed_mainline_contract": _mainline()}
        self.assertEqual(_boundary_forbidden_terms(direction), ["纱"])
        self.assertEqual(_strip_banned_terms("双层纱质蝴蝶造型", ["纱"]), "双层蝴蝶造型")
        # 没有禁词时逐字不改，包括空值。
        self.assertEqual(_strip_banned_terms("双层纱质蝴蝶造型", []), "双层纱质蝴蝶造型")

    def test_the_engine_request_never_carries_the_banned_claim(self):
        # 契约不是唯一一条投喂通路：core_selling_points / visual_fact_inputs /
        # primary_selling_point 是引擎请求上的独立字段，带着同一批原句。
        from unittest import mock

        from core import reality_voiceover_bridge as bridge

        mainline = _mainline()
        direction = {
            "content_bundle_brief": {
                "content_mainline": "双层蝴蝶造型，超唯美",
                "selling_argument": {
                    "core_proof_claim_keys": ["C1"],
                    "operator_expression": "双层纱质蝴蝶造型",
                    "core_value": "双层纱质蝴蝶造型，超唯美",
                },
                "proof_atoms": [{"claim_key": "C1", "fact_text": "双层纱质蝴蝶造型"}],
                "mixed_mainline_contract": mainline,
            },
            "mixed_mainline_contract": mainline,
        }
        visual_plan = {"shots": [{"shot_no": 1, "supported_claim_keys": ["C1"]}]}
        captured = {}

        def _fake_engine_variant(**kwargs):
            captured.update(kwargs)
            return {
                "hook_id": "H1",
                "selected_claim_count": 1,
                "selected_claim_ids": ["C1"],
                "beats": [
                    {
                        "suggested_start_ms": 0,
                        "suggested_end_ms": 1500,
                        "speech_text": "dang buom bang voan hai lop",
                        "chinese_translation": "双层纱质蝴蝶造型",
                        "role": "VALUE",
                    }
                ],
            }

        with mock.patch.object(
            bridge,
            "load_active_voiceover_hooks",
            return_value=[{"hook_id": "H1", "status": "ACTIVE"}],
        ), mock.patch.object(
            bridge, "run_voiceover_engine_variant", side_effect=_fake_engine_variant
        ):
            result = bridge.run_central_voiceover(
                product_code="P1",
                target_country="VN",
                target_language="越南语",
                direction=direction,
                visual_plan=visual_plan,
            )

        product = captured["product"]
        strategy = captured["strategy"]
        self.assertTrue(product["core_selling_points"])
        self.assertTrue(all("纱" not in item for item in product["core_selling_points"]))
        self.assertNotIn("纱", strategy["primary_selling_point"])
        self.assertTrue(all("纱" not in item["fact_text"] for item in product["visual_fact_inputs"]))
        self.assertEqual(
            strategy["expression_contract"]["voiceover_expression_boundary"]["forbidden_wording"],
            ["纱"],
        )
        self.assertTrue(result["expression_boundary"]["layer_present_in_payload"])
        self.assertEqual(result["expression_boundary"]["speakable_leaks_in_payload"], [])
        # 兜底改成"查出来"：引擎这次确实把被禁断言说出来了，必须被判定为未通过。
        self.assertEqual(result["expression_boundary"]["check"]["status"], CHECK_FAIL)


class BoundaryCheckTest(unittest.TestCase):
    """成品兜底：真说出来了要能被检出，而不是指望它没写。"""

    def test_the_chinese_translation_is_the_blocking_basis(self):
        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        check = check_voiceover_target_against_boundary(
            voice={
                "target_text": "dang buom bang voan hai lop",
                "chinese_translation": "双层纱质蝴蝶造型",
            },
            mainline=_mainline(),
        )
        self.assertEqual(check["status"], CHECK_FAIL)
        self.assertEqual(check["violations"][0]["term"], "纱")
        self.assertEqual(check["violations"][0]["scope"], "chinese_translation")

    def test_the_detection_scope_is_reported_not_overclaimed(self):
        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        check = check_voiceover_target_against_boundary(
            voice={"chinese_translation": "双层纱质蝴蝶造型"}, mainline=_mainline()
        )
        self.assertEqual(check["blocking_basis"], "chinese_translation")
        self.assertEqual(check["target_language_detection"], "SHARED_SCRIPT_TERMS_ONLY")
        self.assertTrue(check["residual_risk"])

    def test_a_clean_utterance_passes(self):
        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        check = check_voiceover_target_against_boundary(
            voice={"chinese_translation": "双层蝴蝶造型，夹得很稳"},
            mainline=_mainline(),
        )
        self.assertEqual(check["status"], CHECK_PASS)
        self.assertEqual(check["violations"], [])

    def test_without_banned_wording_it_is_not_applicable(self):
        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        check = check_voiceover_target_against_boundary(
            voice={"chinese_translation": "双层纱质蝴蝶造型"},
            mainline={"expression_boundary": {"forbidden_wording": []}},
        )
        self.assertEqual(check["status"], CHECK_NOT_APPLICABLE)
        self.assertEqual(check["reason"], "NO_FORBIDDEN_WORDING")

    def test_a_ceiling_only_boundary_is_judged_rather_than_skipped(self):
        """没有禁词不等于没有边界可查。

        封顶约束不走 ``forbidden_wording``，所以成品侧必须另有一条判定；否则
        "不并列多个"永远没有检测，模型说三个场景也没人发现。
        """

        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        mainline = _mainline(
            conflicts=[
                {
                    "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                    "stacked_scenes": ["日常", "约会", "上班"],
                    "allowed_scenarios": 1,
                    "resolution": "KEEP_ONE",
                }
            ],
            forbidden=[],
        )
        stacked = check_voiceover_target_against_boundary(
            voice={"chinese_translation": "适合上班、约会或日常戴，不挑场合"}, mainline=mainline
        )
        self.assertEqual(stacked["status"], CHECK_FAIL)
        self.assertEqual(stacked["reason"], "CEILING_EXCEEDED")
        self.assertEqual(len(stacked["ceiling_violations"]), 1)
        self.assertEqual(stacked["ceiling_violations"][0]["spoken_count"], 3)
        self.assertEqual(stacked["ceiling_violations"][0]["allowed"], 1)

        one_only = check_voiceover_target_against_boundary(
            voice={"chinese_translation": "上班戴这枚就够了，不用换来换去"}, mainline=mainline
        )
        self.assertEqual(one_only["status"], CHECK_PASS)
        self.assertEqual(one_only["ceiling_violations"], [])

    def test_a_ceiling_without_a_chinese_translation_is_not_applicable(self):
        # 中译缺失时不能假装查过：判据在中译侧，没有中译就是"没有依据"。
        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        check = check_voiceover_target_against_boundary(
            voice={"target_text": "Kalau korang cari cincin..."},
            mainline=_mainline(
                conflicts=[
                    {
                        "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                        "stacked_scenes": ["日常"],
                        "allowed_scenarios": 1,
                        "resolution": "KEEP_ONE",
                    }
                ],
                forbidden=[],
            ),
        )
        self.assertEqual(check["status"], CHECK_NOT_APPLICABLE)
        self.assertEqual(check["reason"], "NO_CHINESE_TRANSLATION")

    def test_a_ceiling_that_allows_the_stacked_scenes_is_not_a_violation(self):
        # ``allowed_scenarios`` 说了算：授权了多场景就不是违规。
        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        check = check_voiceover_target_against_boundary(
            voice={"chinese_translation": "上班、约会、日常都能戴"},
            mainline=_mainline(
                conflicts=[
                    {
                        "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                        "stacked_scenes": ["日常", "约会", "上班"],
                        "allowed_scenarios": 3,
                        "resolution": "KEEP_ONE",
                    }
                ],
                forbidden=[],
            ),
        )
        self.assertEqual(check["status"], CHECK_PASS)

    def test_each_line_is_reported_with_its_index(self):
        from core.mixed_voiceover_mainline import check_voiceover_target_against_boundary

        check = check_voiceover_target_against_boundary(
            mainline=_mainline(),
            lines=[
                {"voiceover_text_zh": "第一句很干净"},
                {"voiceover_text_zh": "第二句提到纱"},
            ],
        )
        self.assertEqual(check["status"], CHECK_FAIL)
        self.assertEqual([item["index"] for item in check["violations"]], [2])


class DeliveredVoiceoverReviewTest(unittest.TestCase):
    """两份核对要真的产出结论，而不是只在测试里被调用。"""

    class _Item:
        macro_family_key = ""
        carrier_mode = "MIXED"
        actual_hook_id = ""
        requested_hook_id = ""
        batch_item_id = "I1"
        item_index = 1

        def __init__(self, payload):
            self.result_json = json.dumps(payload, ensure_ascii=False)

    def _script(self, chinese):
        return {
            "continuous_voiceover": {
                "target_language": "越南语",
                "target_text": "dang buom",
                "chinese_translation": chinese,
            },
            "storyboard": [{"capture_unit_id": "CU_01", "visual_content": "画面"}],
            "capture_units": [{"capture_unit_id": "CU_01", "structure_role": "MOMENT"}],
        }

    def test_without_a_mainline_the_delivery_text_is_unchanged(self):
        from core.production_script_renderer import render_complete_production_script

        text = render_complete_production_script(
            item=self._Item({"script": self._script("双层蝴蝶造型")}), duration_seconds=15
        )
        self.assertNotIn("口播口径核对", text)
        self.assertNotIn("主线三类核对", text)

    def test_a_delivered_script_with_a_mainline_reports_both_checks(self):
        from core.production_script_renderer import render_complete_production_script

        script = self._script("双层蝴蝶造型")
        script["mixed_mainline_contract"] = _mainline()
        text = render_complete_production_script(
            item=self._Item({"script": script}), duration_seconds=15
        )
        self.assertIn("口播口径核对：通过", text)
        self.assertIn("主线三类核对", text)

    def test_a_violation_in_the_finished_copy_surfaces_in_the_delivery_text(self):
        from core.production_script_renderer import render_complete_production_script

        script = self._script("双层纱质蝴蝶造型")
        script["mixed_mainline_contract"] = _mainline()
        text = render_complete_production_script(
            item=self._Item({"script": script}), duration_seconds=15
        )
        self.assertIn("口播口径核对：未通过", text)
        self.assertIn("纱", text)

    def test_the_projection_carries_the_review_as_structured_data(self):
        from core.production_script_renderer import build_production_projection

        script = self._script("双层纱质蝴蝶造型")
        script["mixed_mainline_contract"] = _mainline()

        class _Batch:
            duration_seconds = 15
            batch_id = "B1"
            product_code = "P1"
            target_country = "VN"
            target_language = "越南语"
            top_category = ""
            product_type = ""

        projection = build_production_projection(
            batch=_Batch(), item=self._Item({"script": script})
        )
        self.assertEqual(projection["voiceover_review_status"], CHECK_FAIL)
        self.assertEqual(
            projection["voiceover_review"]["boundary_check"]["violations"][0]["term"], "纱"
        )
