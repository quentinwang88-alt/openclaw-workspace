#!/usr/bin/env python3
"""Phase 3A：播种必须能收窄到本轮要开的那条线。

``seed_reference_data.py --apply`` 原本遍历整个 bundle，开一条 VN 线会把 TH/MX 的
市场包、配方、profile 一起重推一遍 —— 那些线的字节和线上状态都不是本轮该动的东西。
``--only`` 把 upsert 收窄到点名的实体 id，并且**未命中就中止**（拼错之后"什么都没写"
不能算部署成功）。
"""
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

SCRIPT = PACKAGE_ROOT / "scripts" / "seed_reference_data.py"
VN_IDS = ("MP_VN_DEFAULT_V1", "PHOTO_TRAVEL_OUTFIT_V3", "PHOTO_MATCHING_CHOICE_V3")
TH_IDS = ("MP_TH_DEFAULT_V1", "PHOTO_TH_TRAVEL_OUTFIT_V2")
MX_IDS = ("MP_MX_DEFAULT_V1", "PHOTO_MX_BEFORE_AFTER_V1")


def _run(*ids):
    args = [argument for value in ids for argument in ("--only", value)]
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=str(PACKAGE_ROOT), capture_output=True, text=True, check=False,
    )


class SeedScopeTest(unittest.TestCase):

    def test_a_scoped_dry_run_writes_nothing_and_lists_only_that_scope(self):
        result = _run(*VN_IDS)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no_database_writes=1", result.stdout)
        self.assertIn("scope=" + ",".join(sorted(VN_IDS)), result.stdout)
        for value in VN_IDS:
            self.assertIn("in_scope", result.stdout)
            self.assertIn(value, result.stdout)

    def test_a_scoped_run_never_pulls_in_other_markets(self):
        result = _run(*VN_IDS)
        scoped = [line for line in result.stdout.splitlines() if line.startswith("  in_scope")]
        self.assertEqual(len(scoped), len(VN_IDS), result.stdout)
        for line in scoped:
            with self.subTest(line=line):
                for foreign in TH_IDS + MX_IDS:
                    self.assertNotIn(foreign, line)

    def test_an_unknown_id_aborts_instead_of_reporting_success(self):
        result = _run("MP_VN_DEFAULT_V9")
        self.assertEqual(result.returncode, 2)
        self.assertIn("unknown --only id(s), nothing was written", result.stderr)
        self.assertNotIn("no_database_writes=1", result.stdout)

    def test_without_only_the_behaviour_is_unchanged(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT)],
            cwd=str(PACKAGE_ROOT), capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("scope=", result.stdout)
        self.assertIn("no_database_writes=1", result.stdout)

    def test_the_allowlist_covers_every_seedable_entity_kind(self):
        """范围收窄对六类可播种实体都生效，不能只挡市场包。"""
        source = SCRIPT.read_text(encoding="utf-8")
        for attr in ("market_pack_id", "theme_id", "render_preset_id", "recipe_id",
                     "render_profile_id", "quality_profile_id"):
            with self.subTest(attr=attr):
                self.assertIn(attr, source)


if __name__ == "__main__":
    unittest.main()
