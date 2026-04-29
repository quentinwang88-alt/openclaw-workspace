#!/usr/bin/env python3
"""Tests for field-name resolution helpers."""

from __future__ import annotations

import unittest

from core.mapping import normalize_name, resolve_field_name


class MappingTests(unittest.TestCase):
    def test_normalize_name_ignores_spacing_and_brackets(self) -> None:
        self.assertEqual(normalize_name(" 优化后的标题（中文） "), "优化后的标题中文")

    def test_resolve_field_name_prefers_explicit_request(self) -> None:
        field_names = ["产品名称", "类目", "TK 标题"]
        self.assertEqual(resolve_field_name(field_names, "TK标题", ["TK标题"]), "TK 标题")

    def test_resolve_field_name_uses_fallback_candidates(self) -> None:
        field_names = ["产品标题", "产品类目", "TK标题"]
        self.assertEqual(resolve_field_name(field_names, None, ["原始标题", "产品标题"]), "产品标题")


if __name__ == "__main__":
    unittest.main()
