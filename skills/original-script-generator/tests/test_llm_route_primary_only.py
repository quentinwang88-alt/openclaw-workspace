#!/usr/bin/env python3
"""LLM 单线路兼容回归测试。"""

from __future__ import annotations

import tempfile
from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.llm_client import PRIMARY_ROUTE, normalize_route, normalize_route_order  # noqa: E402
from core.runtime_config import load_runtime_config, save_runtime_config  # noqa: E402


class LLMRoutePrimaryOnlyTest(unittest.TestCase):
    def test_only_primary_route_is_accepted(self) -> None:
        self.assertEqual(normalize_route("primary"), PRIMARY_ROUTE)
        self.assertEqual(normalize_route(None), PRIMARY_ROUTE)
        for route in ("auto", "backup", "gemini"):
            with self.assertRaises(ValueError):
                normalize_route(route)

    def test_route_order_accepts_only_primary(self) -> None:
        self.assertEqual(normalize_route_order("primary"), [PRIMARY_ROUTE])
        self.assertEqual(normalize_route_order(["primary"]), [PRIMARY_ROUTE])
        self.assertIsNone(normalize_route_order(""))
        for route_order in ("gemini,primary", ["backup", "primary"]):
            with self.assertRaises(ValueError):
                normalize_route_order(route_order)

    def test_runtime_config_loads_legacy_route_as_primary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_path.write_text('{"llm_route":"gemini"}\n', encoding="utf-8")

            payload = load_runtime_config(config_path=config_path)

        self.assertEqual(payload["llm_route"], PRIMARY_ROUTE)

    def test_runtime_config_rejects_non_primary_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            with self.assertRaises(ValueError):
                save_runtime_config({"llm_route": "backup"}, config_path=config_path)


if __name__ == "__main__":
    unittest.main()
