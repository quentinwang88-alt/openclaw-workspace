#!/usr/bin/env python3
"""同步逻辑单元测试。"""

import unittest

from core.bitable import TableRecord
from core.sync import (
    SOURCE_FIELD_ALIASES,
    SCRIPT_FIELD_SPECS,
    TARGET_FIELD_ALIASES,
    build_prompt_with_anchor,
    build_target_fields,
    build_source_failure_fields,
    build_source_success_fields,
    build_sync_tasks,
    now_text,
    resolve_field_mapping,
)


class ScriptRunManagerSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        source_field_names = [
            "产品编码",
            "产品类型",
            "一级类目",
            "产品参数信息",
            "任务编号",
            "产品图片",
            "是否可同步",
            "同步状态",
            "同步时间",
            "所属母版1",
            "所属母版2",
            "母版方向1",
            "母版方向2",
            "脚本方向一",
            "脚本1变体1",
            "脚本方向二",
            "脚本4变体5",
        ]
        target_field_names = ["任务名", "提示词", "参考图", "脚本ID"]
        self.mapping = resolve_field_mapping(source_field_names, SOURCE_FIELD_ALIASES)
        self.target_mapping = resolve_field_mapping(target_field_names, TARGET_FIELD_ALIASES)

    def test_script_specs_cover_24_slots(self) -> None:
        self.assertEqual(len(SCRIPT_FIELD_SPECS), 24)

    def test_build_sync_tasks_creates_one_task_per_non_empty_script(self) -> None:
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "产品编码": "ABC001",
                    "产品类型": "手镯",
                    "一级类目": "配饰",
                    "产品参数信息": "细手圈，内径约56mm，圈宽2mm，开口可微调",
                    "任务编号": "003",
                    "是否可同步": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "所属母版1": "M1",
                    "所属母版2": "M2",
                    "母版方向1": "日常轻分享流",
                    "母版方向2": "问题解决流",
                    "脚本方向一": "script one",
                    "脚本1变体1": "script one v1",
                    "脚本方向二": "script two",
                    "脚本4变体5": "script four v5",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual([task.task_name for task in tasks], ["ABC001.S1", "ABC001.S1V1", "ABC001.S2", "ABC001.S4V5"])
        self.assertEqual(tasks[0].reference_images, [{"file_token": "file_1"}])
        self.assertEqual(tasks[0].product_type, "手镯")
        self.assertEqual(tasks[0].business_category, "配饰")
        self.assertEqual(tasks[0].product_params, "细手圈，内径约56mm，圈宽2mm，开口可微调")
        self.assertEqual(tasks[0].script_id, "003_M1_M")
        self.assertEqual(tasks[1].script_id, "003_M1_V1")
        self.assertEqual(tasks[2].script_id, "003_M2_M")
        self.assertEqual(tasks[3].script_id, "003_M4_V5")

    def test_build_sync_tasks_respects_checkbox(self) -> None:
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "产品编码": "ABC001",
                    "是否可同步": False,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "script one",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)
        self.assertEqual(tasks, [])

    def test_build_target_fields_includes_script_id(self) -> None:
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "产品编码": "ABC001",
                    "产品类型": "手镯",
                    "一级类目": "配饰",
                    "产品参数信息": "细手圈，内径约56mm，圈宽2mm，开口可微调",
                    "任务编号": "003",
                    "是否可同步": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "所属母版1": "M1",
                    "母版方向1": "日常轻分享流",
                    "脚本方向一": "script one",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)
        fields = build_target_fields(tasks[0], self.target_mapping)

        self.assertEqual(fields["任务名"], "ABC001.S1")
        self.assertIn("【产品锚点】", fields["提示词"])
        self.assertIn("产品类型：手镯", fields["提示词"])
        self.assertIn("一级类目：配饰", fields["提示词"])
        self.assertIn("产品参数信息：细手圈，内径约56mm，圈宽2mm，开口可微调", fields["提示词"])
        self.assertIn("【脚本内容】\nscript one", fields["提示词"])
        self.assertEqual(fields["脚本ID"], "003_M1_M")

    def test_build_prompt_with_anchor_falls_back_to_raw_script_without_anchor_fields(self) -> None:
        prompt = build_prompt_with_anchor(
            build_sync_tasks(
                [
                    TableRecord(
                        record_id="rec_2",
                        fields={
                            "产品编码": "ABC002",
                            "任务编号": "004",
                            "是否可同步": True,
                            "产品图片": [{"file_token": "file_2"}],
                            "脚本方向一": "plain script",
                        },
                    ),
                ],
                self.mapping,
            )[0]
        )

        self.assertEqual(prompt, "plain script")

    def test_source_backwrite_fields(self) -> None:
        ts = now_text()
        success_fields = build_source_success_fields(self.mapping, synced_count=24, synced_at=ts)
        failure_fields = build_source_failure_fields(self.mapping, error_message="boom", synced_at=ts)

        self.assertFalse(success_fields["是否可同步"])
        self.assertIn("新增 24 条", success_fields["同步状态"])
        self.assertEqual(success_fields["同步时间"], ts)
        self.assertIn("同步失败", failure_fields["同步状态"])


if __name__ == "__main__":
    unittest.main()
