#!/usr/bin/env python3
"""Tests for prompt template loading and matching."""

from __future__ import annotations

import unittest
from pathlib import Path

from core.prompt_loader import load_prompt_templates, match_prompt_template


class PromptLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prompts_dir = Path(__file__).resolve().parents[1] / "prompts"
        self.templates = load_prompt_templates(self.prompts_dir)

    def test_load_prompt_templates(self) -> None:
        names = {template.category_name for template in self.templates}
        self.assertIn("发夹", names)
        self.assertIn("针织开衫", names)

    def test_match_prompt_template_by_exact_name(self) -> None:
        template, mode = match_prompt_template("发夹", self.templates)
        self.assertIsNotNone(template)
        self.assertEqual(template.category_name, "发夹")
        self.assertEqual(mode, "exact")

    def test_match_prompt_template_by_alias(self) -> None:
        template, mode = match_prompt_template("抓夹", self.templates)
        self.assertIsNotNone(template)
        self.assertEqual(template.category_name, "发夹")
        self.assertEqual(mode, "alias")


if __name__ == "__main__":
    unittest.main()
