#!/usr/bin/env python3
"""人工短视频脚本库解析测试。"""

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import TableRecord
from core.manual_source import build_manual_sync_tasks, manual_script_id, resolve_manual_field_mapping
from core.sync import SCRIPT_TYPE_NURTURE, SCRIPT_TYPE_ORIGINAL, build_target_fields, task_script_type


class ManualSourceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source_mapping = resolve_manual_field_mapping(
            ["脚本ID", "脚本", "用途", "产品ID", "店铺", "图片", "同步", "语言", "时长", "状态", "时间"]
        )
        self.target_mapping = {
            "task_name": "任务名",
            "prompt": "提示词",
            "reference_images": "参考图",
            "script_id": "脚本ID",
            "internal_script_key": "内部脚本键",
            "reference_free": "免参考图",
            "script_type": "脚本类型",
        }

    def test_sales_script_generates_stable_id_and_sale_metadata(self) -> None:
        record = TableRecord(
            "rec_manual_001",
            {"脚本": "泰语带货正文", "用途": "带货", "产品ID": "P001", "店铺": "THPS01", "同步": True},
        )
        result = build_manual_sync_tasks([record], self.source_mapping)

        self.assertEqual(result.errors, {})
        self.assertEqual(result.script_ids[record.record_id], manual_script_id(record.record_id))
        task = result.tasks[0]
        self.assertEqual(task.script_id, manual_script_id(record.record_id))
        self.assertEqual(task.task_name, f"P001.{task.script_id}")
        self.assertEqual(task.publish_purpose, "带货")
        self.assertEqual(task.cart_enabled, "是")
        self.assertEqual(task_script_type(task), SCRIPT_TYPE_ORIGINAL)

    def test_non_sales_script_can_be_reference_free_nurture(self) -> None:
        record = TableRecord(
            "rec_manual_002",
            {"脚本": "日常养号正文", "用途": "非带货", "店铺": "VNPS01", "同步": True},
        )
        result = build_manual_sync_tasks([record], self.source_mapping)

        task = result.tasks[0]
        self.assertEqual(task_script_type(task), SCRIPT_TYPE_NURTURE)
        fields = build_target_fields(task, self.target_mapping)
        self.assertEqual(fields["免参考图"], "是")
        self.assertEqual(fields["脚本类型"], SCRIPT_TYPE_NURTURE)

    def test_invalid_sales_row_is_not_emitted(self) -> None:
        record = TableRecord(
            "rec_manual_003",
            {"脚本": "缺产品的带货脚本", "用途": "带货", "店铺": "THPS01", "同步": True},
        )
        result = build_manual_sync_tasks([record], self.source_mapping)

        self.assertEqual(result.tasks, [])
        self.assertIn("产品ID", result.errors[record.record_id])


if __name__ == "__main__":
    unittest.main()
