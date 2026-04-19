#!/usr/bin/env python3
"""统一脚本 ID 测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.script_ids import (  # noqa: E402
    build_script_id_from_context,
    build_script_id_from_fields,
    extract_unified_id_from_text,
    is_valid_unified_id,
    parse_slot_from_logical_name,
    prepend_unified_id,
)
from core.script_renderer import render_script  # noqa: E402


class ScriptIdsTest(unittest.TestCase):
    def test_build_script_id_from_context_uses_task_no_and_parent_slot(self) -> None:
        context = {
            "task_no": "003",
            "product_code": "BH001",
            "parent_slot_1": "M1",
            "parent_slot_2": "M2",
        }
        self.assertEqual(build_script_id_from_context(context, 1, None, record_id="rec123"), "003_M1_M")
        self.assertEqual(build_script_id_from_context(context, 2, 3, record_id="rec123"), "003_M2_V3")

    def test_build_script_id_from_fields_supports_fallback_parent_slot(self) -> None:
        fields = {
            "任务编号": "003",
            "产品编码": "BH001",
        }
        mapping = {
            "task_no": "任务编号",
            "product_code": "产品编码",
            "product_id": None,
            "parent_slot_1": None,
            "parent_slot_2": None,
            "parent_slot_3": None,
            "parent_slot_4": None,
        }
        self.assertEqual(build_script_id_from_fields(fields, mapping, 1, None, record_id="rec123"), "003_M1_M")
        self.assertEqual(build_script_id_from_fields(fields, mapping, 4, 5, record_id="rec123"), "003_M4_V5")

    def test_parse_slot_from_logical_name(self) -> None:
        self.assertEqual(parse_slot_from_logical_name("script_s1"), (1, None))
        self.assertEqual(parse_slot_from_logical_name("video_prompt_s3"), (3, None))
        self.assertEqual(parse_slot_from_logical_name("script_2_variant_4"), (2, 4))

    def test_unified_id_helpers_support_legacy_and_structured_values(self) -> None:
        self.assertTrue(is_valid_unified_id("123456"))
        self.assertTrue(is_valid_unified_id("003_M1_V2"))
        text = prepend_unified_id("这里是正文", "003_M1_V2")
        self.assertEqual(extract_unified_id_from_text(text), "003_M1_V2")

    def test_render_script_uses_script_id_header(self) -> None:
        rendered = render_script(
            {
                "content_id": "003_M1_M",
                "script_positioning": {},
                "storyboard": [],
                "execution_constraints": {},
                "negative_constraints": [],
            }
        )
        self.assertIn("【脚本ID】", rendered)
        self.assertIn("003_M1_M", rendered)


if __name__ == "__main__":
    unittest.main()
