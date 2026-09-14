#!/usr/bin/env python3
"""Phase 2A/2B：预设「展示分组」只控制目录，以及入口清单必须可复现。

Phase 2 不加任何生产力：它把运营的日常入口整理清楚。
- `entry_group` 只是目录/视图属性，**启用语义仍归 `status`**：把一条 disabled
  的预设标进 `production` 组，它照样 resolve 不了；把它标进 `legacy` 组，只要
  status 是 active 就照常可用。
- 飞书「生产预设」下拉的选项不受分组影响（仍等于 active 预设集合）。
- 入口清单（预设→配方→流程→主题→语言→排版→输出）由脚本从 config 派生，
  因而必须与随仓提交的名单逐字一致，不能悄悄漂移。
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from scripts.photo_entry_catalog import build_payload, render_markdown  # noqa: E402
from services.feishu_workflow import (  # noqa: E402
    PRESET_ENTRY_GROUPS, FeishuWorkflowError, ProductionPresetCatalog,
    preset_entry_group, theme_is_optional,
)
from services.photo_content_planner import get_planning_flow  # noqa: E402
from services.photo_flow_registry import (  # noqa: E402
    is_layered_progression_flow, is_thermal_transition_flow,
)

CONFIG_DIR = PACKAGE_ROOT / "config"
PRESETS_PATH = CONFIG_DIR / "feishu_production_presets.json"
CATALOG_DOC = PACKAGE_ROOT / "docs" / "OPV_ENTRY_CATALOG_20260914.md"


def _preset(name, **overrides):
    task = {
        "account_id": "OPV_TH_TEST_001", "market": "TH", "language": "th-TH",
        "recipe_id": "PHOTO_TH_PICK_YOUR_LOOK_V3", "theme_id": "",
        "hook_strategy": "pick_one_of_four", "persona_ref": "", "look_ref": "",
        "scene_ref": "",
    }
    return {"name": name, "tasks": [task], **overrides}


class EntryGroupCatalogTest(unittest.TestCase):
    """分组是展示属性：不改变启用语义，也不改变下拉选项。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def _catalog(self, presets):
        path = self.root / "presets.json"
        path.write_text(json.dumps(
            {"schema_version": "opv-feishu-production-presets-v1", "presets": presets},
            ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        return ProductionPresetCatalog(path)

    def test_a_disabled_preset_is_unusable_even_inside_the_production_group(self):
        catalog = self._catalog([
            _preset("图文｜VN｜围巾旅行", entry_group="production", status="disabled",
                    disabled_reason="待验收"),
            _preset("图文｜TH｜四选一穿搭", entry_group="production", status="active"),
        ])
        self.assertEqual(catalog.entry_group("图文｜VN｜围巾旅行"), "production")
        self.assertEqual(catalog.names, ["图文｜TH｜四选一穿搭"])
        with self.assertRaisesRegex(FeishuWorkflowError, "已停用"):
            catalog.resolve_batch("图文｜VN｜围巾旅行", "rec1", 1)

    def test_an_active_preset_stays_usable_inside_the_legacy_group(self):
        catalog = self._catalog([
            _preset("TH｜五套穿搭｜拆解首图", entry_group="legacy", status="active"),
        ])
        self.assertEqual(catalog.entry_group("TH｜五套穿搭｜拆解首图"), "legacy")
        self.assertIn("TH｜五套穿搭｜拆解首图", catalog.names)
        self.assertEqual(len(catalog.resolve_batch("TH｜五套穿搭｜拆解首图", "rec1", 1)), 1)

    def test_an_unknown_entry_group_is_rejected_loudly(self):
        with self.assertRaisesRegex(FeishuWorkflowError, "未知的预设展示分组"):
            preset_entry_group({"name": "x", "entry_group": "archived"})
        catalog = self._catalog([_preset("图文｜TH｜四选一穿搭", entry_group="archived")])
        with self.assertRaisesRegex(FeishuWorkflowError, "未知的预设展示分组"):
            catalog.entry_group("图文｜TH｜四选一穿搭")

    def test_a_preset_without_the_attribute_falls_into_the_derived_group(self):
        self.assertEqual(preset_entry_group({"status": "disabled"}), "trial")
        self.assertEqual(preset_entry_group({"media_kind": "native_photo"}), "production")
        self.assertEqual(preset_entry_group({}), "legacy")

    def test_entries_is_a_read_only_view_that_can_filter_by_group(self):
        catalog = ProductionPresetCatalog(PRESETS_PATH)
        every = catalog.entries()
        shipped = json.loads(PRESETS_PATH.read_text(encoding="utf-8"))["presets"]
        self.assertEqual(len(every), len(shipped), "目录必须列全每一条预设")
        self.assertEqual([row["name"] for row in catalog.entries("production")],
                         [row["name"] for row in every if row["entry_group"] == "production"])
        for group in PRESET_ENTRY_GROUPS:
            with self.subTest(group=group):
                self.assertTrue(catalog.entries(group), f"{group} 组不应为空")
        with self.assertRaisesRegex(FeishuWorkflowError, "未知的预设展示分组"):
            catalog.entries("archived")

    def test_grouping_does_not_touch_the_feishu_preset_options(self):
        from scripts.ensure_feishu_task_table import task_field_specs
        from services.feishu_workflow import FIELD_PRESET

        catalog = ProductionPresetCatalog(PRESETS_PATH)
        specs = {name: prop for name, _type, _ui, prop in task_field_specs(catalog)}
        options = [item["name"] for item in specs[FIELD_PRESET]["options"]]
        self.assertEqual(options, catalog.names)
        self.assertNotIn("图文｜VN｜围巾旅行", options, "disabled 预设不得进入下拉")

    def test_every_shipped_preset_declares_a_group(self):
        payload = json.loads(PRESETS_PATH.read_text(encoding="utf-8"))
        for item in payload["presets"]:
            with self.subTest(preset=item["name"]):
                self.assertIn(item.get("entry_group"), PRESET_ENTRY_GROUPS)
                # 显式声明必须与推导一致，否则目录与真实入口性质会分叉。
                self.assertEqual(item["entry_group"], preset_entry_group(
                    {key: value for key, value in item.items() if key != "entry_group"}))


class ThemeDefaultOwnershipTest(unittest.TestCase):
    """「主题缺省规则」只有一处实现：工作流与入口清单共用它。"""

    def _recipe_spec(self, recipe_id):
        path = CONFIG_DIR / "recipes" / f"{recipe_id}.json"
        return json.loads(path.read_text(encoding="utf-8")).get("recipe_spec") or {}

    def _optional(self, recipe_id):
        flow = get_planning_flow(recipe_id)
        packs = dict(self._recipe_spec(recipe_id).get("locale_copy_packs") or {})
        return theme_is_optional(
            locale_pack=(packs or None),
            layering_flow=is_layered_progression_flow(flow),
            thermal_transition_flow=is_thermal_transition_flow(flow),
            planning_flow=flow,
        )

    def test_country_agnostic_matching_line_allows_an_empty_theme(self):
        self.assertTrue(self._optional("PHOTO_MATCHING_CHOICE_V3"))

    def test_travel_lines_still_require_a_theme(self):
        self.assertFalse(self._optional("PHOTO_TRAVEL_OUTFIT_V3"))
        self.assertFalse(self._optional("PHOTO_TH_TRAVEL_OUTFIT_V2"))

    def test_layering_and_thermal_lines_still_require_a_theme(self):
        self.assertFalse(self._optional("PHOTO_TH_THERMAL_TRANSITION_V1"))

    def test_v1_recipe_without_locale_packs_still_requires_a_theme(self):
        self.assertFalse(self._optional("PHOTO_TH_PICK_YOUR_LOOK_V3"))


class EntryCatalogDocumentTest(unittest.TestCase):
    """随仓的入口清单必须与当前配置逐字一致（防止清单与配置分叉）。"""

    def test_committed_catalog_matches_the_shipped_config(self):
        self.assertTrue(CATALOG_DOC.is_file(), f"缺少入口清单：{CATALOG_DOC}")
        self.assertEqual(
            CATALOG_DOC.read_text(encoding="utf-8"), render_markdown(build_payload()),
            "入口清单与配置不一致；重新生成：PYTHONPATH=. /usr/bin/python3 "
            "scripts/photo_entry_catalog.py --output docs/OPV_ENTRY_CATALOG_20260914.md",
        )

    def test_every_vn_preset_row_shows_its_language_pack_and_vn_store(self):
        payload = build_payload()
        vn = [item for item in payload["presets"]
              if item["entry_group"] == "trial" and item["category_key"] == "scarf"]
        self.assertEqual(len(vn), 2)
        for item in vn:
            task = item["tasks"][0]
            with self.subTest(preset=item["name"]):
                self.assertEqual(task["language"], "vi-VN")
                # 该配方的语言包必须覆盖 vi-VN；旅行线同时也服务 th-TH，属正常。
                self.assertIn("vi-VN", task["recipe"]["locale_packs"])
                self.assertNotIn("th-TH", item["name"])
                # 2026-09-14：VN 店铺路由已补（方案 §3B），清单必须如实写出
                # 实际店铺，不能再写「未配置路由」。
                self.assertEqual(task["store_id"], "VNPS01")


class PresetMetadataTest(unittest.TestCase):
    """整理预设 metadata 时不得动名称存储值或任务指向。"""

    def test_names_and_recipe_ids_are_untouched_by_the_grouping_edit(self):
        payload = json.loads(PRESETS_PATH.read_text(encoding="utf-8"))
        names = [item["name"] for item in payload["presets"]]
        self.assertEqual(len(names), len(set(names)), "预设名不得重复")
        # 2026-09-14 之前提交里的名称与配方指向必须逐字保留。
        expected = {
            "图文｜TH｜旅行穿搭": "PHOTO_TH_TRAVEL_OUTFIT_V2",
            "图文｜TH｜四选一穿搭": "PHOTO_TH_PICK_YOUR_LOOK_V3",
            "图文｜VN｜围巾旅行": "PHOTO_TRAVEL_OUTFIT_V3",
            "图文｜VN｜围巾搭配四选一": "PHOTO_MATCHING_CHOICE_V3",
            "TH｜随机养号组合｜轻文字": "RECIPE_PAIN_POINT_SOLUTION_V1",
        }
        by_name = {item["name"]: item for item in payload["presets"]}
        for name, recipe_id in expected.items():
            with self.subTest(preset=name):
                tasks = by_name[name].get("tasks") or []
                if tasks:
                    self.assertEqual(tasks[0]["recipe_id"], recipe_id)
                else:
                    self.assertTrue(by_name[name]["tasks_from"])

    def test_no_preset_was_dropped_for_hiding_an_old_entry(self):
        """旧入口可以标 legacy，但不得因为"看着旧"而被删除。"""
        payload = json.loads(PRESETS_PATH.read_text(encoding="utf-8"))
        self.assertEqual(len(payload["presets"]), 18)


if __name__ == "__main__":
    unittest.main()
