#!/usr/bin/env python3

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from backfill_script_types import build_source_script_type_index, plan_backfill, plan_source_type_repairs
from core.bitable import TableRecord
from core.sync import SOURCE_FIELD_ALIASES, resolve_field_mapping


class BackfillScriptTypesTest(unittest.TestCase):
    def test_all_five_run_manager_types_are_classified_without_guessing(self) -> None:
        mapping = resolve_field_mapping(
            [
                "任务编号", "产品编码", "脚本方向一", "所属母版1",
                "脚本来源", "脚本类型", "发布用途", "内容分支",
                "源复刻任务ID",
            ],
            SOURCE_FIELD_ALIASES,
        )
        sources = [
            TableRecord("src_original", {"任务编号": "001", "产品编码": "A", "脚本方向一": "原创"}),
            TableRecord("src_remake", {
                "任务编号": "002", "产品编码": "B", "脚本方向一": "复刻",
                "脚本类型": "短视频复刻", "源复刻任务ID": "remake_002",
            }),
            TableRecord("src_nurture", {"任务编号": "003", "产品编码": "C", "脚本方向一": "养号", "发布用途": "养号"}),
        ]
        index = build_source_script_type_index(sources, mapping)
        run_records = [
            TableRecord("run_original", {"脚本ID": "001_M1_M"}),
            TableRecord("run_remake", {"脚本ID": "002_M1_M"}),
            TableRecord("run_nurture", {"脚本ID": "003_M1_M"}),
            TableRecord("run_light", {"脚本ID": "light_1", "任务来源": "轻量试穿视频"}),
            TableRecord("run_supplement", {"脚本ID": "sup_1", "任务来源": "口播增强补充镜头"}),
        ]

        updates, summary = plan_backfill(run_records, index)

        self.assertEqual(summary["error_count"], 0)
        self.assertEqual(summary["planned_updates"], 5)
        self.assertEqual(
            summary["type_counts"],
            {
                "原创脚本": 1,
                "短视频复刻脚本": 1,
                "养号脚本": 1,
                "轻视频脚本": 1,
                "轻视频补素材脚本": 1,
            },
        )
        self.assertEqual(
            [item["fields"]["脚本类型"] for item in updates],
            ["原创脚本", "短视频复刻脚本", "养号脚本", "轻视频脚本", "轻视频补素材脚本"],
        )

    def test_existing_type_is_preserved_and_unknown_record_blocks_apply(self) -> None:
        records = [
            TableRecord("existing", {"脚本ID": "known", "脚本类型": "原创脚本"}),
            TableRecord("unknown", {"脚本ID": "missing"}),
        ]

        updates, summary = plan_backfill(records, {})

        self.assertEqual(updates, [])
        self.assertEqual(summary["error_count"], 1)
        self.assertEqual(summary["type_counts"]["原创脚本"], 1)
        self.assertEqual(summary["type_counts"]["无法分类"], 1)

    def test_type_only_remake_label_is_repaired_as_original(self) -> None:
        mapping = resolve_field_mapping(
            ["任务编号", "脚本方向一", "脚本类型", "脚本来源", "源复刻任务ID", "发布用途"],
            SOURCE_FIELD_ALIASES,
        )
        record = TableRecord(
            "src_bad_copy",
            {"任务编号": "2712", "脚本方向一": "原创一", "脚本类型": "短视频复刻"},
        )

        index = build_source_script_type_index([record], mapping)
        repairs, summary = plan_source_type_repairs([record], mapping)

        self.assertEqual(index["2712_M1_M"], {"原创脚本"})
        self.assertEqual(summary["planned_updates"], 1)
        self.assertEqual(repairs[0]["fields"], {"脚本类型": "原创脚本"})


if __name__ == "__main__":
    unittest.main()
