#!/usr/bin/env python3
"""生成变体触发策略回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import TaskRecord  # noqa: E402
from core.pipeline import (  # noqa: E402
    OriginalScriptPipeline,
    STATUS_DONE,
    STATUS_PENDING_VARIANTS,
    load_pending_records,
)


class _FakeClient:
    def __init__(self, records):
        self._records = records

    def list_records(self, page_size: int = 100):
        return list(self._records)


class VariantTriggerFlowTest(unittest.TestCase):
    def test_context_defaults_to_generate_variants_when_checkbox_field_missing(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pipeline.mapping = {
            "status": "任务状态",
            "product_images": "产品图片",
            "product_code": "产品编码",
            "product_id": "商品ID",
            "parent_slot_1": "所属母版1",
            "parent_slot_2": "所属母版2",
            "parent_slot_3": "所属母版3",
            "parent_slot_4": "所属母版4",
            "top_category": "一级类目",
            "target_country": "目标国家",
            "target_language": "目标语言",
            "product_type": "产品类型",
            "product_selling_note": "产品卖点说明",
        }
        pipeline.llm_route = "primary"

        context = pipeline._build_context(
            TaskRecord(
                record_id="rec_legacy",
                fields={
                    "任务状态": "待执行-全流程",
                    "产品编码": "046",
                    "一级类目": "配饰",
                    "目标国家": "MY",
                    "目标语言": "ms",
                    "产品类型": "耳线",
                },
            )
        )

        self.assertTrue(context["generate_variants_requested"])

    def test_context_reads_generate_variants_checkbox(self) -> None:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pipeline.mapping = {
            "status": "任务状态",
            "generate_variants": "生成变体",
            "product_images": "产品图片",
            "product_code": "产品编码",
            "product_id": "商品ID",
            "parent_slot_1": "所属母版1",
            "parent_slot_2": "所属母版2",
            "parent_slot_3": "所属母版3",
            "parent_slot_4": "所属母版4",
            "top_category": "一级类目",
            "target_country": "目标国家",
            "target_language": "目标语言",
            "product_type": "产品类型",
            "product_selling_note": "产品卖点说明",
        }
        pipeline.llm_route = "primary"

        checked = pipeline._build_context(
            TaskRecord(
                record_id="rec_checked",
                fields={
                    "任务状态": "待执行-全流程",
                    "生成变体": True,
                },
            )
        )
        unchecked = pipeline._build_context(
            TaskRecord(
                record_id="rec_unchecked",
                fields={
                    "任务状态": "待执行-全流程",
                    "生成变体": False,
                },
            )
        )

        self.assertTrue(checked["generate_variants_requested"])
        self.assertFalse(unchecked["generate_variants_requested"])

    def test_load_pending_records_auto_enqueues_completed_checked_record_for_variants(self) -> None:
        mapping = {
            "status": "任务状态",
            "generate_variants": "生成变体",
            "review_s1_json": "脚本_S1_质检_JSON",
            "variant_s1_json": "变体_S1_JSON",
        }
        client = _FakeClient(
            [
                TaskRecord(
                    record_id="rec_done",
                    fields={
                        "任务状态": STATUS_DONE,
                        "生成变体": True,
                        "脚本_S1_质检_JSON": '{"pass": true}',
                        "变体_S1_JSON": "",
                    },
                )
            ]
        )

        records = load_pending_records(client, mapping)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].record_id, "rec_done")
        self.assertEqual(records[0].fields["任务状态"], STATUS_PENDING_VARIANTS)

    def test_load_pending_records_skips_completed_checked_record_when_variants_already_exist(self) -> None:
        mapping = {
            "status": "任务状态",
            "generate_variants": "生成变体",
            "review_s1_json": "脚本_S1_质检_JSON",
            "variant_s1_json": "变体_S1_JSON",
        }
        client = _FakeClient(
            [
                TaskRecord(
                    record_id="rec_done",
                    fields={
                        "任务状态": STATUS_DONE,
                        "生成变体": True,
                        "脚本_S1_质检_JSON": '{"pass": true}',
                        "变体_S1_JSON": '{"variants":[{"variant_id":"V1"}]}',
                    },
                )
            ]
        )

        records = load_pending_records(client, mapping)

        self.assertEqual(records, [])

    def test_load_pending_records_skips_completed_checked_record_without_qc_pass(self) -> None:
        mapping = {
            "status": "任务状态",
            "generate_variants": "生成变体",
            "review_s1_json": "脚本_S1_质检_JSON",
            "variant_s1_json": "变体_S1_JSON",
        }
        client = _FakeClient(
            [
                TaskRecord(
                    record_id="rec_done",
                    fields={
                        "任务状态": "已完成-含质检失败脚本",
                        "生成变体": True,
                        "脚本_S1_质检_JSON": '{"pass": false}',
                        "变体_S1_JSON": "",
                    },
                )
            ]
        )

        records = load_pending_records(client, mapping)

        self.assertEqual(records, [])


if __name__ == "__main__":
    unittest.main()
