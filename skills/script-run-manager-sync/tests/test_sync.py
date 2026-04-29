#!/usr/bin/env python3
"""同步逻辑单元测试。"""

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import TableRecord
from core.sync import (
    SOURCE_FIELD_ALIASES,
    SCRIPT_FIELD_SPECS,
    TARGET_FIELD_ALIASES,
    compact_anchor_text,
    has_any_sync_enabled,
    is_variant_slot,
    build_prompt_with_anchor,
    build_target_fields,
    build_source_failure_fields,
    build_source_success_fields,
    build_sync_tasks,
    now_text,
    prepend_script_id_header,
    prompt_has_script_id_header,
    resolve_field_mapping,
    summarize_sync_scope,
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
            "是否可同步母版",
            "是否可同步子变体",
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
        target_field_names = ["任务名", "提示词", "参考图", "脚本ID", "脚本来源", "发布用途", "是否挂车", "内容分支", "免参考图"]
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

    def test_build_sync_tasks_uses_original_script_for_master_slot(self) -> None:
        records = [
            TableRecord(
                record_id="rec_master_prompt",
                fields={
                    "产品编码": "ABC099",
                    "任务编号": "099",
                    "是否可同步": True,
                    "脚本方向一": "original script one",
                    "脚本1变体1": "variant one",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual(tasks[0].task_name, "ABC099.S1")
        self.assertEqual(tasks[0].prompt_text, "original script one")
        self.assertEqual(tasks[1].prompt_text, "variant one")

    def test_build_sync_tasks_falls_back_to_task_no_when_product_code_missing(self) -> None:
        records = [
            TableRecord(
                record_id="rec_missing_code",
                fields={
                    "任务编号": "053",
                    "是否可同步母版": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "master one",
                    "脚本方向二": "master two",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual([task.task_name for task in tasks], ["053.S1", "053.S2"])
        self.assertTrue(all(task.product_code == "053" for task in tasks))
        self.assertEqual(tasks[0].script_id, "053_M1_M")

    def test_split_sync_checkboxes_only_sync_master_slots_when_master_checked(self) -> None:
        records = [
            TableRecord(
                record_id="rec_master",
                fields={
                    "产品编码": "ABC010",
                    "是否可同步母版": True,
                    "是否可同步子变体": False,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "master one",
                    "脚本1变体1": "variant one",
                    "脚本方向二": "master two",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual([task.task_name for task in tasks], ["ABC010.S1", "ABC010.S2"])

    def test_split_sync_checkboxes_only_sync_variant_slots_when_variant_checked(self) -> None:
        records = [
            TableRecord(
                record_id="rec_variant",
                fields={
                    "产品编码": "ABC011",
                    "是否可同步母版": False,
                    "是否可同步子变体": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "master one",
                    "脚本1变体1": "variant one",
                    "脚本4变体5": "variant four v5",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual([task.task_name for task in tasks], ["ABC011.S1V1", "ABC011.S4V5"])

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

    def test_has_any_sync_enabled_supports_split_fields(self) -> None:
        fields = {"是否可同步母版": True}
        self.assertTrue(has_any_sync_enabled(fields, self.mapping))

    def test_is_variant_slot(self) -> None:
        self.assertFalse(is_variant_slot("S1"))
        self.assertTrue(is_variant_slot("S1V1"))

    def test_summarize_sync_scope(self) -> None:
        tasks = build_sync_tasks(
            [
                TableRecord(
                    record_id="rec_scope",
                    fields={
                        "产品编码": "ABC012",
                        "是否可同步": True,
                        "产品图片": [{"file_token": "file_1"}],
                        "脚本方向一": "master one",
                        "脚本1变体1": "variant one",
                        "脚本4变体5": "variant four v5",
                    },
                ),
            ],
            self.mapping,
        )

        self.assertEqual(summarize_sync_scope(tasks), "母版+子变体（母版 1 条，子变体 2 条）")

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
        self.assertTrue(fields["提示词"].startswith("【脚本ID】\n- 003_M1_M\n\n产品锚点：手镯｜细手圈，内径约56mm，圈宽2mm，开口可微调\n"))
        self.assertTrue(fields["提示词"].endswith("script one"))
        self.assertEqual(fields["脚本ID"], "003_M1_M")

    def test_prepend_script_id_header_does_not_duplicate_standard_header(self) -> None:
        prompt = prepend_script_id_header("【脚本ID】\n- old_id\n\n正文内容", "003_M1_V1")

        self.assertEqual(prompt.count("【脚本ID】"), 1)
        self.assertTrue(prompt.startswith("【脚本ID】\n- 003_M1_V1\n\n正文内容"))
        self.assertTrue(prompt_has_script_id_header(prompt))

    def test_prepend_script_id_header_keeps_legacy_content_id_below_script_id(self) -> None:
        prompt = prepend_script_id_header("【内容ID】\n- 123456\n\n正文内容", "003_M1_V1")

        self.assertTrue(prompt.startswith("【脚本ID】\n- 003_M1_V1\n\n【内容ID】\n- 123456"))

    def test_nurture_task_sets_reference_free_flag(self) -> None:
        source_field_names = [
            "产品编码",
            "任务编号",
            "是否可同步母版",
            "脚本方向一",
            "脚本来源",
            "发布用途",
            "是否挂车",
            "内容分支",
        ]
        mapping = resolve_field_mapping(source_field_names, SOURCE_FIELD_ALIASES)
        tasks = build_sync_tasks(
            [
                TableRecord(
                    record_id="rec_nurture",
                    fields={
                        "产品编码": "YR028",
                        "任务编号": "028",
                        "是否可同步母版": True,
                        "脚本方向一": "final storyboard",
                        "脚本来源": "养号复刻",
                        "发布用途": "养号",
                        "是否挂车": "否",
                        "内容分支": "非商品展示型",
                    },
                ),
            ],
            mapping,
        )

        fields = build_target_fields(tasks[0], self.target_mapping)

        self.assertEqual(fields["脚本来源"], "养号复刻")
        self.assertEqual(fields["发布用途"], "养号")
        self.assertEqual(fields["是否挂车"], "否")
        self.assertEqual(fields["内容分支"], "非商品展示型")
        self.assertEqual(fields["免参考图"], "是")

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

        self.assertIn("【口播/字幕语言强制约束】", prompt)
        self.assertTrue(prompt.startswith("【脚本ID】\n- 004_M1_M\n\n"))
        self.assertIn("plain script", prompt)

    def test_compact_anchor_text_flattens_multiline_and_truncates(self) -> None:
        text = compact_anchor_text(
            "细手圈\n内径约56mm\n圈宽2mm\n开口可微调\n适合日常轻佩戴\n避免过度拉伸",
            max_length=24,
        )

        self.assertIn("细手圈；内径约56mm", text)
        self.assertTrue(text.endswith("…"))

    def test_source_backwrite_fields(self) -> None:
        ts = now_text()
        success_fields = build_source_success_fields(
            self.mapping,
            synced_count=24,
            synced_at=ts,
            sync_scope="母版（4 条）",
            cleared_legacy=True,
            cleared_master=True,
            cleared_variant=True,
        )
        failure_fields = build_source_failure_fields(
            self.mapping,
            error_message="boom",
            synced_at=ts,
            sync_scope="子变体（20 条）",
        )

        self.assertFalse(success_fields["是否可同步"])
        self.assertFalse(success_fields["是否可同步母版"])
        self.assertFalse(success_fields["是否可同步子变体"])
        self.assertIn("母版（4 条）", success_fields["同步状态"])
        self.assertIn("新增 24 条", success_fields["同步状态"])
        self.assertEqual(success_fields["同步时间"], ts)
        self.assertIn("子变体（20 条）", failure_fields["同步状态"])
        self.assertIn("同步失败", failure_fields["同步状态"])


if __name__ == "__main__":
    unittest.main()
