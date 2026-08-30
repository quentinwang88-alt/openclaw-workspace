from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.organic_seeding_feishu import (
    SEED_OPERATION_FIELDS, SEED_SCRIPT_FIELDS, operation_record_values, result_to_feishu_fields,
)
from scripts.run_feishu_seeding_tasks import _allowed_task_statuses


class OrganicSeedingFeishuTest(unittest.TestCase):
    def test_compact_chinese_fields_map_to_internal_contracts(self):
        task = operation_record_values(
            SimpleNamespace(
                record_id="rec1",
                fields={
                    "任务ID": "T1", "产品编码": "P1", "生成数": 2,
                    "种草目标": "风格记忆", "内容要求": "通勤造型记忆",
                    "体验权限": "无历史体验",
                    "产品事实": "细密纹理\n边缘平整", "任务状态": "待执行",
                },
            )
        )
        self.assertEqual(task["theme"]["objective"], "STYLE_MEMORY")
        self.assertNotIn("product_role", task["theme"])
        self.assertNotIn("product_prominence", task["theme"])
        self.assertEqual(task["theme"]["experience_authority"], "NONE")
        self.assertEqual(task["theme"]["lived_context"], "通勤造型记忆")
        self.assertEqual(task["duration_seconds"], 15)
        self.assertEqual(task["facts"], ["细密纹理", "边缘平整"])

    def test_projection_keeps_review_fields_compact_and_chinese(self):
        fields = result_to_feishu_fields(
            result={
                "script_id": "S1", "item_id": "I1", "item_index": 1,
                "seed_theme": {
                    "theme_id": "T1", "objective": "STYLE_MEMORY",
                    "viewer_payoff": "搭配参考", "product_role": "SUPPORTING",
                    "product_prominence": "MID", "experience_authority": "NONE",
                    "memory_residue": "造型记忆",
                },
                "voiceover": {"target_text": "เสียง", "chinese_translation": "中文"},
                "quality": {"content_value_score": 90, "adness_score": 0, "fake_experience_check": "PASS"},
                "visual_blueprint": {"script_title": "标题"},
                "complete_script": "完整", "video_generation_prompt": "提示词",
            },
            run={"run_id": "R1", "input_hash": "H1", "versions": {}},
            task={
                "product_code": "P1", "store_id": "SHOP", "duration_seconds": 15,
                "top_category": "配饰", "product_type": "围巾", "target_country": "泰国",
                "target_language": "泰语",
            },
            product_images=[],
        )
        self.assertEqual(fields["种草目标"], "风格记忆")
        self.assertEqual(fields["发布策略"], "种草不挂车")
        self.assertEqual(fields["视频提示词"], "提示词")
        self.assertEqual(
            fields["质检结果"],
            "安全通过｜事实✓｜不挂车✓｜执行待评｜创意待审｜批次待评｜泰语待母语审",
        )
        self.assertNotIn("内容分支", fields)

    def test_schema_is_reduced_to_operator_essentials(self):
        self.assertEqual(len(SEED_OPERATION_FIELDS) + 1, 18)
        self.assertEqual(len(SEED_SCRIPT_FIELDS) + 1, 21)

    def test_creative_grade_reuses_existing_qc_text_field(self):
        fields = result_to_feishu_fields(
            result={
                "script_id": "S1", "item_id": "I1", "item_index": 1,
                "seed_theme": {
                    "theme_id": "T1", "objective": "STYLE_MEMORY",
                    "viewer_payoff": "搭配参考", "product_role": "SUPPORTING",
                    "product_prominence": "MID", "experience_authority": "NONE",
                    "memory_residue": "造型记忆",
                },
                "voiceover": {"target_text": "เสียง", "chinese_translation": "中文"},
                "quality": {
                    "passed": True,
                    "quality_dimensions": {
                        "fact_integrity": "PASS", "commerce_safety": "PASS",
                        "execution_grade": "A", "creative_quality": "B",
                        "opening_grade": "A", "distinctness_grade": "A",
                        "language_review": "MACHINE_SCREENED_NATIVE_REVIEW_PENDING",
                    },
                },
                "visual_blueprint": {"script_title": "标题"},
                "complete_script": "完整", "video_generation_prompt": "提示词",
            },
            run={"run_id": "R1", "input_hash": "H1", "versions": {}},
            task={
                "product_code": "P1", "store_id": "SHOP", "duration_seconds": 15,
                "top_category": "女装", "product_type": "外套", "target_country": "泰国",
                "target_language": "泰语",
            },
            product_images=[],
        )
        self.assertIn("创意B｜前3秒A", fields["质检结果"])
        self.assertEqual(len(SEED_SCRIPT_FIELDS) + 1, 21)

    def test_completed_task_requires_explicit_record_scoped_rerun(self):
        self.assertEqual(
            _allowed_task_statuses(
                record_id=None, item_indices=(), rerun_completed=False
            ),
            {"待执行"},
        )
        self.assertIn(
            "已完成",
            _allowed_task_statuses(
                record_id="rec1", item_indices=(), rerun_completed=True
            ),
        )


if __name__ == "__main__":
    unittest.main()
