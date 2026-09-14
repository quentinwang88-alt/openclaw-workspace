#!/usr/bin/env python3
"""Phase 3A：预检必须能按运营入口圈范围，且不得把「没查到东西」当就绪。

要守住的三条：
1. ``--preset-name`` 真能解析预设（配方 / 市场 / 语言），而不是只比字符串；
2. 未绑定市场的通用 Recipe（两条 VN 围巾线）在 ``--require-ready`` 下**不能**因为
   ``needs_asset`` 恰好为空就报就绪 —— 这是本轮要消掉的假就绪；
3. 素材按任务动态输入的现役产线（TH 旅行）不该被当成未就绪，只出说明。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from scripts.preflight_native_photo import (  # noqa: E402
    preflight, readiness_blockers, readiness_notes,
)

CONFIG_DIR = PACKAGE_ROOT / "config"
VN_MATCHING = "图文｜VN｜围巾搭配四选一"
VN_TRAVEL = "图文｜VN｜围巾旅行"
TH_TRAVEL = "图文｜TH｜旅行穿搭"
LEGACY_VIDEO = "TH｜五套穿搭｜拆解首图"


class PresetScopedPreflightTest(unittest.TestCase):

    def test_a_preset_scopes_the_check_to_its_native_photo_recipes(self):
        result = preflight(CONFIG_DIR, preset_names=[VN_MATCHING])
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["recipe_count"], 1)
        self.assertEqual([r["recipe_id"] for r in result["recipes"]],
                         ["PHOTO_MATCHING_CHOICE_V3"])
        row = result["presets"][0]
        self.assertEqual(row["name"], VN_MATCHING)
        self.assertEqual(row["markets"], ["VN"])
        self.assertEqual(row["languages"], ["vi-VN"])
        self.assertEqual(row["native_photo_recipe_ids"], ["PHOTO_MATCHING_CHOICE_V3"])
        self.assertEqual(row["entry_group"], "trial")

    def test_an_unbound_recipe_is_not_ready_just_because_needs_asset_is_empty(self):
        """本轮的靶心：needs_asset 为空 ≠ 就绪。"""
        result = preflight(CONFIG_DIR, preset_names=[VN_MATCHING],
                           require_ready=True)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["needs_asset"], [],
                         "这条线本来就不在 needs_asset 桶里")
        self.assertIn("PHOTO_MATCHING_CHOICE_V3", result["canary_market_unbound"])
        self.assertIn("PHOTO_MATCHING_CHOICE_V3", result["not_ready"])
        self.assertFalse(result["ready"])
        self.assertEqual(result["check"], "production_ready")
        self.assertTrue(any("未绑定市场" in item for item in result["ready_blockers"]))
        self.assertTrue(any("仍为 disabled" in item for item in result["ready_blockers"]))

    def test_both_vn_lines_are_blocked_for_their_own_reasons(self):
        result = preflight(CONFIG_DIR, preset_names=[VN_MATCHING, VN_TRAVEL],
                           require_ready=True)
        self.assertEqual(result["errors"], [])
        self.assertEqual(sorted(r["recipe_id"] for r in result["recipes"]),
                         ["PHOTO_MATCHING_CHOICE_V3", "PHOTO_TRAVEL_OUTFIT_V3"])
        self.assertEqual(result["not_ready"],
                         ["PHOTO_MATCHING_CHOICE_V3", "PHOTO_TRAVEL_OUTFIT_V3"])
        self.assertFalse(result["ready"])
        self.assertEqual(len(result["presets"]), 2)

    def test_a_dynamic_input_line_is_ready_with_a_note_not_a_blocker(self):
        """TH 旅行是现役产线：素材按任务补，不该被判未就绪。"""
        result = preflight(CONFIG_DIR, preset_names=[TH_TRAVEL], require_ready=True)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["ready_blockers"], [])
        self.assertTrue(result["ready"])
        self.assertTrue(any("动态输入" in item for item in result["ready_notes"]))
        self.assertIn("PHOTO_TH_TRAVEL_OUTFIT_V2", result["dynamic_input_required"])
        self.assertNotIn("PHOTO_TH_TRAVEL_OUTFIT_V2", result["not_ready"])

    def test_an_unknown_preset_name_fails_loudly(self):
        result = preflight(CONFIG_DIR, preset_names=["图文｜VN｜不存在的线"])
        self.assertTrue(any("未知生产预设" in item for item in result["errors"]))
        self.assertEqual(result["recipe_count"], 0)

    def test_a_legacy_video_preset_is_reported_instead_of_silently_passing(self):
        """点到原生图文范围外的入口：报错，而不是"检查了 0 条 ⇒ 就绪"。"""
        result = preflight(CONFIG_DIR, preset_names=[LEGACY_VIDEO],
                           require_ready=True)
        self.assertEqual(result["recipe_count"], 0)
        self.assertTrue(any("不在本预检范围内" in item for item in result["errors"]))
        self.assertFalse(result["ready"])

    def test_preset_and_recipe_filters_intersect(self):
        both = preflight(CONFIG_DIR, preset_names=[VN_MATCHING],
                         recipe_ids=["PHOTO_MATCHING_CHOICE_V3"])
        self.assertEqual(both["errors"], [])
        self.assertEqual(both["recipe_count"], 1)
        empty = preflight(CONFIG_DIR, preset_names=[VN_MATCHING],
                          recipe_ids=["PHOTO_TH_TRAVEL_OUTFIT_V2"])
        self.assertEqual(empty["recipe_count"], 0)

    def test_the_default_run_is_a_plain_config_check(self):
        result = preflight(CONFIG_DIR)
        self.assertEqual(result["check"], "config")
        self.assertEqual(result["errors"], [])
        self.assertFalse(result["ready"], "全目录还有静态素材缺口")
        self.assertIn("PHOTO_MX_BEFORE_AFTER_V1", result["needs_asset"])
        self.assertIn("PHOTO_TRAVEL_OUTFIT_V3", result["canary_market_unbound"])

    def test_blockers_and_notes_never_overlap(self):
        result = preflight(CONFIG_DIR)
        self.assertEqual(set(result["not_ready"]),
                         set(result["needs_asset"]) | set(result["canary_market_unbound"]))
        self.assertEqual(result["ready"], not result["ready_blockers"])
        notes = " ".join(readiness_notes(result))
        blockers = " ".join(readiness_blockers(result, result["presets"]))
        self.assertIn("动态输入", notes)
        self.assertNotIn("动态输入", blockers)

    def test_scoping_never_silently_widens_the_scope(self):
        """圈定 VN 时绝不能顺手把其它市场的配方也带进来。"""
        result = preflight(CONFIG_DIR, preset_names=[VN_TRAVEL])
        ids = {r["recipe_id"] for r in result["recipes"]}
        self.assertEqual(ids, {"PHOTO_TRAVEL_OUTFIT_V3"})
        self.assertNotIn("PHOTO_TH_TRAVEL_OUTFIT_V2", ids)


if __name__ == "__main__":
    unittest.main()
