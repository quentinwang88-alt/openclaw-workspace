#!/usr/bin/env python3
"""全流程重跑时的前台输出清空回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.pipeline import OriginalScriptPipeline  # noqa: E402


class PipelineRerunResetTest(unittest.TestCase):
    def test_full_flow_rerun_clear_values_cover_primary_outputs(self) -> None:
        values = OriginalScriptPipeline._build_full_flow_rerun_clear_values()

        required_fields = [
            "output_summary",
            "anchor_card_json",
            "opening_strategy_json",
            "styling_plan_json",
            "three_strategies_json",
            "script_s1_json",
            "script_s1",
            "review_s1_json",
            "video_prompt_s1_json",
            "video_prompt_s1",
            "variant_s1_json",
            "script_1_variant_1",
            "script_s4_json",
            "script_s4",
            "review_s4_json",
            "video_prompt_s4_json",
            "video_prompt_s4",
            "variant_s4_json",
            "script_4_variant_5",
        ]

        for field in required_fields:
            self.assertIn(field, values)
            self.assertEqual(values[field], "")


if __name__ == "__main__":
    unittest.main()
