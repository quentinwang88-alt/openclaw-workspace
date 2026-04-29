import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.hermes import build_hermes_input, run_hermes_batch  # noqa: E402
from scripts.run_hermes_picker import _extract_response_text  # noqa: E402


def sample_selection_rows():
    return [
        {
            "work_id": "batch_001_1734141996809291509",
            "batch_id": "batch_001",
            "product_id": "1734141996809291509",
            "country": "VN",
            "category": "时尚配件",
            "product_name": "Cloud Ring",
            "listing_days": 74,
            "sales_7d": 27327,
            "total_sales": 130228,
            "avg_price_7d_rmb": 4.49,
            "creator_count": 499,
            "video_count": 0,
            "live_count": 625,
            "commission_rate": 0.10,
            "pool_type": "新品池",
            "competition_maturity": "中",
            "source_rule_score": 82,
            "rule_score": 82,
            "rule_pass_reason": "新品启动强，竞争密度可接受",
            "procurement_price_rmb": 2.85,
            "gross_margin_rate": 0.365,
            "distribution_margin_rate": 0.265,
        }
    ]


class HermesPromptTest(unittest.TestCase):
    def test_extract_response_text_strips_session_id_footer(self):
        self.assertEqual(
            _extract_response_text('{"ok": true}\n\nsession_id: abc123\n'),
            '{"ok": true}',
        )

    def test_extract_response_text_strips_cli_banner(self):
        self.assertEqual(
            _extract_response_text(
                '╭─ ⚕ Hermes ───────────────────────────────────────────────────────────────────╮\n'
                '{"ok": true}\n'
            ),
            '{"ok": true}',
        )

    def test_build_hermes_input_contains_prompt_bundle(self):
        payload = build_hermes_input("batch_001", sample_selection_rows())

        self.assertEqual(payload["profile"], "picker")
        self.assertEqual(payload["batch_id"], "batch_001")
        self.assertEqual(payload["country"], "VN")
        self.assertEqual(payload["category"], "时尚配件")
        self.assertEqual(payload["shortlist_count"], 1)
        self.assertIn("你是一个面向 TikTok 内容驱动型跨境电商团队的选品最终判断助手", payload["picker_system_prompt"])
        self.assertIn("batch_id: batch_001", payload["picker_user_prompt"])
        self.assertIn("items 数量必须与输入商品数量一致", payload["picker_json_output_constraint"])
        self.assertEqual(
            payload["items"][0],
            {
                "work_id": "batch_001_1734141996809291509",
                "product_id": "1734141996809291509",
                "product_name": "Cloud Ring",
                "country": "VN",
                "category": "时尚配件",
                "listing_days": 74,
                "sales_7d": 27327,
                "sales_total": 130228,
                "avg_price_7d_rmb": 4.49,
                "creator_count": 499,
                "video_count": 0,
                "live_count": 625,
                "commission_rate": 0.1,
                "pool_type": "新品池",
                "competition_maturity": "中",
                "source_rule_score": 82.0,
                "rule_reason": "新品启动强，竞争密度可接受",
                "procurement_price_rmb": 2.85,
                "gross_margin": 0.365,
                "gross_margin_after_commission": 0.265,
            },
        )

    @patch("app.hermes.subprocess.run")
    def test_run_hermes_batch_repairs_invalid_json_once(self, mock_run):
        mock_run.side_effect = [
            type("Completed", (), {"returncode": 0, "stdout": "not valid json", "stderr": ""})(),
            type(
                "Completed",
                (),
                {
                    "returncode": 0,
                    "stdout": json.dumps(
                        {
                            "batch_id": "batch_001",
                            "items": [
                                {
                                    "work_id": "batch_001_1734141996809291509",
                                    "strategy_suggestion": "自然流",
                                    "recommended_action": "观察",
                                    "recommendation_reason": "内容表达直观，毛利尚可，但同质化不低。",
                                    "risk_warning": "供货稳定性和同质化竞争仍需继续观察。",
                                }
                            ],
                        },
                        ensure_ascii=False,
                    ),
                    "stderr": "",
                },
            )(),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            result = run_hermes_batch(
                "batch_001",
                sample_selection_rows(),
                Path(temp_dir),
                "fake-picker --profile {profile} --system {system_prompt} --prompt {user_prompt} --input {input} --output {output}",
                timeout_seconds=30,
            )

            self.assertEqual(result.status, "success")
            self.assertEqual(result.items["batch_001_1734141996809291509"]["strategy_suggestion"], "自然流")
            self.assertEqual(mock_run.call_count, 2)

            first_command = mock_run.call_args_list[0].args[0]
            second_command = mock_run.call_args_list[1].args[0]
            self.assertTrue(any("hermes_user_prompt.txt" in part for part in first_command))
            self.assertTrue(any("hermes_repair_prompt.txt" in part for part in second_command))
            self.assertTrue(any("hermes_repair_input.json" in part for part in second_command))

            output_payload = json.loads(Path(result.output_path).read_text(encoding="utf-8"))
            self.assertEqual(
                output_payload,
                {
                    "batch_id": "batch_001",
                    "items": [
                        {
                            "work_id": "batch_001_1734141996809291509",
                            "strategy_suggestion": "自然流",
                            "recommended_action": "观察",
                            "recommendation_reason": "内容表达直观，毛利尚可，但同质化不低。",
                            "risk_warning": "供货稳定性和同质化竞争仍需继续观察。",
                        }
                    ],
                },
            )


if __name__ == "__main__":
    unittest.main()
