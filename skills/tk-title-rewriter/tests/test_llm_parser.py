#!/usr/bin/env python3
"""Tests for parsing and aligning LLM output."""

from __future__ import annotations

import unittest

from core.llm import RewriteInput, align_rewrites, parse_rewrite_output


class LLMParserTests(unittest.TestCase):
    def test_parse_rewrite_output(self) -> None:
        raw_text = """
---
原标题：精致抓夹大号发量多发夹女高级感后脑勺盘发鲨鱼夹子
TK标题：Kep cang cua size lon phong cach thanh lich cho nu hop toc day de bui toc sau dau
字符数：93 | 提取属性：kep cang cua, size lon, thanh lich, cho nu, hop toc day
---
原标题：V领冰丝针织开衫女短款长袖百搭薄外套
TK标题：เสื้อคาร์ดิแกนถักคอวี เนื้อบางลื่น แขนยาว ทรงสั้น ใส่ง่าย สำหรับผู้หญิง
字符数：88 | 提取属性：เสื้อคาร์ดิแกนถัก, คอวี, เนื้อบางลื่น, แขนยาว, สำหรับผู้หญิง
---
"""
        parsed = parse_rewrite_output(raw_text)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].character_count, 93)
        self.assertEqual(parsed[1].original_title, "V领冰丝针织开衫女短款长袖百搭薄外套")

    def test_align_rewrites_by_order_when_counts_match(self) -> None:
        requested = [
            RewriteInput(record_id="rec1", category="发夹", original_title="标题1"),
            RewriteInput(record_id="rec2", category="发夹", original_title="标题2"),
        ]
        parsed = parse_rewrite_output(
            """
---
原标题：标题1
TK标题：A
字符数：1 | 提取属性：x
---
原标题：标题2
TK标题：B
字符数：1 | 提取属性：y
---
"""
        )
        successes, failures = align_rewrites(requested, parsed)
        self.assertEqual(len(successes), 2)
        self.assertEqual(len(failures), 0)
        self.assertEqual(successes[1][1].tk_title, "B")

    def test_align_rewrites_by_original_title_when_counts_differ(self) -> None:
        requested = [
            RewriteInput(record_id="rec1", category="发夹", original_title="标题1"),
            RewriteInput(record_id="rec2", category="发夹", original_title="标题2"),
        ]
        parsed = parse_rewrite_output(
            """
---
原标题：标题2
TK标题：B
字符数：1 | 提取属性：y
---
"""
        )
        successes, failures = align_rewrites(requested, parsed)
        self.assertEqual(len(successes), 1)
        self.assertEqual(successes[0][0].record_id, "rec2")
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].record_id, "rec1")


if __name__ == "__main__":
    unittest.main()
