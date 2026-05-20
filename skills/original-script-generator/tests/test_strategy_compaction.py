#!/usr/bin/env python3
"""P4/P5 紧凑策略卡与 P6 控制字段清洗回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.json_parser import validate_expression_plan_payload, validate_strategy_payload  # noqa: E402
from core.business_rules import validate_strategy_distribution  # noqa: E402
from core.pipeline import OriginalScriptPipeline  # noqa: E402
from core.script_brief_builder import build_script_brief, _should_skip_control_layer_warning  # noqa: E402


class StrategyCompactionTest(unittest.TestCase):
    def test_compact_strategy_payload_is_normalized_for_downstream(self) -> None:
        payload = {
            "contract_conflict_warning": "",
            "strategies": [
                {
                    "strategy_id": "S1",
                    "script_role": "cognitive_reframing",
                    "primary_focus": "发髻外侧更饱满",
                    "secondary_focus": "",
                    "opening_angle": "先纠正常见误判",
                    "proof_path": "A_result_detail_only",
                    "performance_bias": "镜前轻观察",
                    "risk_note": "不拍完整固定过程",
                },
                {
                    "strategy_id": "S2",
                    "script_role": "result_delivery",
                    "primary_focus": "低髻结果更完整",
                    "secondary_focus": "",
                    "opening_angle": "结果先给",
                    "proof_path": "B_result_with_light_compare",
                    "performance_bias": "轻分享",
                    "risk_note": "对比只出现一次",
                },
                {
                    "strategy_id": "S3",
                    "script_role": "risk_resolution",
                    "primary_focus": "大花不显夸张",
                    "secondary_focus": "",
                    "opening_angle": "先提出顾虑",
                    "proof_path": "C_result_with_short_process",
                    "performance_bias": "轻确认",
                    "risk_note": "不让人物硬推销",
                },
                {
                    "strategy_id": "S4",
                    "script_role": "aura_enhancement",
                    "primary_focus": "整体氛围更完整",
                    "secondary_focus": "",
                    "opening_angle": "高惊艳结果首镜",
                    "proof_path": "D_result_with_light_compare_and_short_process",
                    "performance_bias": "轻惊喜后收住",
                    "risk_note": "不夸张演戏",
                },
            ],
        }

        validate_strategy_payload(payload)

        first = payload["strategies"][0]
        self.assertEqual(first["final_strategy_id"], "Final_S1")
        self.assertEqual(first["primary_selling_point"], "发髻外侧更饱满")
        self.assertIn("A_result_detail_only", first["proof_thesis"])
        self.assertTrue(first["opening_mode"])
        self.assertTrue(first["core_proof_method"])
        self.assertTrue(first["forbidden_patterns"])
        self.assertIsNone(validate_strategy_distribution("US", payload["strategies"]))
        self.assertGreaterEqual(len({item["ending_mode"] for item in payload["strategies"]}), 3)

    def test_expression_plan_separates_voiceover_intent_and_cleans_target_language_quotes(self) -> None:
        expression_plan = {
            "exp_id": "EXP_S1",
            "main_expression_pattern": "轻吐槽到结果确认",
            "aux_expression_pattern": "朋友式补充",
            "native_expression_entry": "像真实分享者顺手说出的第一反应",
            "opening_expression_task": "0-2秒先看镜中后脑；越南语口播/字幕：“Búi tóc rồi mà phía sau vẫn thiếu một điểm nhấn.”",
            "middle_expression_task": "2-9秒用结果复核证明发髻更完整。",
            "ending_expression_task": "9-15秒轻确认可以出门。",
            "human_touch_focus_point": "保留真实镜前确认感",
            "most_likely_empty_point": "中段容易只剩氛围",
            "expression_weight_control": "表达服务商品 proof",
            "voiceover_intent": "开头轻吐槽，中段确认变化，结尾朋友式轻分享",
            "voiceover_language_requirement": "P7 生成口播时必须使用目标语言 vi，不得使用中文",
        }
        validate_expression_plan_payload(expression_plan)

        brief = build_script_brief(
            product_type="发饰",
            anchor_card={},
            opening_strategies={},
            persona_style_emotion_pack={},
            final_strategy={"strategy_id": "S1", "primary_selling_point": "发髻更完整"},
            expression_plan=expression_plan,
        )

        plan = brief["expression_plan"]
        self.assertNotIn("Búi tóc", plan["opening_expression_task"])
        self.assertIn("voiceover_intent", plan)
        self.assertIn("目标语言", plan["voiceover_language_requirement"])

    def test_strategy_proof_path_aligns_with_operation_policy(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        cards = {
            "strategies": [
                {"strategy_id": "S1", "proof_path": "D_result_with_light_compare_and_short_process"},
                {"strategy_id": "S2", "proof_path": "B_result_with_light_compare"},
                {"strategy_id": "S3", "proof_path": "C_result_with_short_process"},
                {"strategy_id": "S4", "proof_path": "A_result_detail_only"},
            ]
        }
        anchor_card = {
            "category_execution_contract": {
                "operation_policy": "result_first_process_avoid",
            }
        }

        aligned = pipeline._align_strategy_cards_with_contract(cards, anchor_card)
        paths = [item["proof_path"] for item in aligned["strategies"]]

        self.assertEqual(
            paths,
            [
                "B_result_with_light_compare",
                "B_result_with_light_compare",
                "A_result_detail_only",
                "A_result_detail_only",
            ],
        )
        self.assertIn("operation_policy", aligned["contract_conflict_warning"])

    def test_control_layer_warning_ignores_embedded_engineering_tokens(self) -> None:
        self.assertTrue(_should_skip_control_layer_warning(
            ["final_strategy", "risk_note"],
            "已按 operation_policy=result_first_process_avoid 从 C_result_with_short_process 收敛为 A_result_detail_only。",
        ))


if __name__ == "__main__":
    unittest.main()
