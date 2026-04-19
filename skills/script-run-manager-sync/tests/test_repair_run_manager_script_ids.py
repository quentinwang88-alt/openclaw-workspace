#!/usr/bin/env python3
"""脚本ID修复工具测试。"""

import unittest

from repair_run_manager_script_ids import (
    extract_script_id_from_prompt,
    is_structured_script_id,
    parse_task_name,
)


class RepairRunManagerScriptIdsTest(unittest.TestCase):
    def test_parse_task_name_supports_slot_suffix(self) -> None:
        self.assertEqual(parse_task_name("ABC001.S2V4"), ("ABC001", "S2V4"))

    def test_extract_script_id_from_prompt_prefers_script_header(self) -> None:
        prompt = "【脚本ID】\n- 010_M2_V4\n\n正文内容"
        self.assertEqual(extract_script_id_from_prompt(prompt), "010_M2_V4")

    def test_extract_script_id_from_prompt_keeps_legacy_content_header_compatible(self) -> None:
        prompt = "【内容ID】\n- 002_M1_V2\n\n正文内容"
        self.assertEqual(extract_script_id_from_prompt(prompt), "002_M1_V2")

    def test_extract_script_id_from_prompt_returns_empty_when_missing(self) -> None:
        self.assertEqual(extract_script_id_from_prompt("没有头部"), "")

    def test_is_structured_script_id_rejects_legacy_numeric_content_id(self) -> None:
        self.assertTrue(is_structured_script_id("010_M2_V4"))
        self.assertFalse(is_structured_script_id("462810"))


if __name__ == "__main__":
    unittest.main()
