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


def _mainline(*, tier="APPEARANCE_FACT", verdict="ADAPTED_WITH_CEILING", image="UNKNOWN", conflicts=None, shot_ref="CU_01"):
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
            "forbidden_wording": ["纱"],
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
