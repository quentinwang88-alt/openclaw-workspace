#!/usr/bin/env python3
"""产品参数信息接入锚点链路回归测试。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import TaskRecord  # noqa: E402
from core.pipeline import OriginalScriptPipeline  # noqa: E402
from core.prompts import build_anchor_card_prompt  # noqa: E402


class ProductParamsAnchorMergeTest(unittest.TestCase):
    def _build_pipeline(self) -> OriginalScriptPipeline:
        pipeline = OriginalScriptPipeline.__new__(OriginalScriptPipeline)
        pipeline.mapping = {
            "status": "任务状态",
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
            "product_params": "产品参数信息",
        }
        pipeline.llm_route = "primary"
        return pipeline

    def test_context_reads_product_params_field(self) -> None:
        pipeline = self._build_pipeline()

        context = pipeline._build_context(
            TaskRecord(
                record_id="rec_params",
                fields={
                    "任务状态": "待执行-全流程",
                    "产品编码": "A-01",
                    "目标国家": "MY",
                    "目标语言": "ms",
                    "产品类型": "手环",
                    "产品参数信息": "内径约56mm，圈宽2mm，开口可微调",
                },
            )
        )

        self.assertEqual(context["product_params"], "内径约56mm，圈宽2mm，开口可微调")

    def test_input_hash_changes_when_product_params_change(self) -> None:
        pipeline = self._build_pipeline()
        attachments = [{"file_token": "file_1"}]
        base_context = {
            "top_category": "配饰",
            "target_country": "MY",
            "target_language": "ms",
            "product_type": "手环",
            "product_selling_note": "轻礼物感",
            "product_params": "内径约56mm",
        }

        hash_a = pipeline._build_input_hash(attachments, base_context)
        hash_b = pipeline._build_input_hash(
            attachments,
            {
                **base_context,
                "product_params": "内径约58mm",
            },
        )

        self.assertNotEqual(hash_a, hash_b)

    def test_merge_product_params_into_anchor_card_prioritizes_manual_facts(self) -> None:
        pipeline = self._build_pipeline()
        anchor_card = {
            "parameter_anchors": [
                {
                    "parameter_name": "内径",
                    "parameter_value": "约56mm",
                    "why_must_preserve": "图片可见",
                    "execution_note": "",
                    "confidence": "medium",
                },
                {
                    "parameter_name": "材质",
                    "parameter_value": "铜镀金",
                    "why_must_preserve": "图片文案可见",
                    "execution_note": "",
                    "confidence": "medium",
                },
            ]
        }

        merged = pipeline._merge_product_params_into_anchor_card(
            anchor_card,
            "内径约56mm，圈宽2mm，开口可微调",
        )

        self.assertEqual(len(merged["parameter_anchors"]), 4)
        self.assertEqual(merged["parameter_anchors"][0]["parameter_name"], "内径")
        self.assertEqual(merged["parameter_anchors"][0]["parameter_value"], "约56mm")
        self.assertEqual(merged["parameter_anchors"][1]["parameter_name"], "圈宽")
        self.assertEqual(merged["parameter_anchors"][1]["parameter_value"], "2mm")
        self.assertEqual(merged["parameter_anchors"][2]["parameter_value"], "开口可微调")
        self.assertEqual(merged["parameter_anchors"][3]["parameter_value"], "铜镀金")
        self.assertEqual(merged["parameter_anchors"][0]["confidence"], "high")

    def test_anchor_prompt_includes_product_parameter_info(self) -> None:
        prompt = build_anchor_card_prompt(
            target_country="MY",
            target_language="ms",
            product_type="手环",
            product_selling_note="轻礼物感",
            product_parameter_info="内径约56mm，圈宽2mm",
        )

        self.assertIn("product_parameter_info: 内径约56mm，圈宽2mm", prompt)
        self.assertIn("可将其中人工确认的参数事实补入 parameter_anchors", prompt)


if __name__ == "__main__":
    unittest.main()
