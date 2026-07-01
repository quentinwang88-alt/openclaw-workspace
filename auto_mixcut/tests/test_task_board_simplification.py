from __future__ import annotations

from types import SimpleNamespace
import unittest

from auto_mixcut.skills.feishu_review_skill import _product_task_summary_fields
from scripts.run_mixcut_task_scanner import _task_state
from scripts.run_ads_mixcut_unattended import plan_ads_mixcut
from scripts.sync_prompt_package_workbench_from_tasks import (
    _brief_with_hook_direction_context,
    _gap_count,
    _hook_direction_context,
)


class _Repo:
    def __init__(self, task: dict):
        self.task = task

    def list_where(self, _table: str, _where: str, _params: tuple):
        return [self.task] if self.task else []


class TaskBoardSimplificationTest(unittest.TestCase):
    def test_scanner_reads_new_task_state_first(self):
        fields = {"任务状态": "待开始", "混剪任务状态": "已完成"}

        self.assertEqual(_task_state(fields), "待开始")

    def test_gap_count_can_fallback_to_rds_task(self):
        ctx = SimpleNamespace(
            repo=_Repo(
                {
                    "blocked_reason": "AI补素材：首镜 4 个钩子",
                    "requested_variant_count": 40,
                    "actual_variant_count": 12,
                    "material_status": "blocked",
                }
            )
        )

        self.assertEqual(_gap_count({}, ctx, "PROD_GAP", max_packages=6), 4)

    def test_hook_direction_context_prefers_manual_then_voc_then_default(self):
        context = _hook_direction_context(
            {
                "钩子方向备注": "突出出门前快速整理",
                "VOC参考摘要": "镜前转头展示造型变化",
                "是否启用VOC参考": "启用",
            },
            "hair_accessories",
        )

        self.assertEqual(context["source"], "manual+voc")
        self.assertEqual(context["final_directions"][0], "突出出门前快速整理")
        self.assertIn("镜前转头展示造型变化", context["final_directions"])

    def test_hook_direction_context_can_disable_voc_reference(self):
        context = _hook_direction_context(
            {
                "VOC参考摘要": "镜前转头展示造型变化",
                "是否启用VOC参考": "关闭",
            },
            "hair_accessories",
        )

        self.assertEqual(context["source"], "category_default")
        self.assertNotIn("镜前转头展示造型变化", context["final_directions"])

    def test_hook_direction_context_injects_into_anchor_brief(self):
        brief = {
            "material_anchor_brief": {
                "product_id": "PROD_HOOK",
                "category": "hair_accessories",
                "must_show": ["商品主体清楚"],
                "safe_micro_actions": ["自然手持展示"],
            }
        }

        updated = _brief_with_hook_direction_context(
            brief,
            {"钩子方向备注": "夹住侧边头发后轻转头"},
            "hair_accessories",
        )
        material = updated["material_anchor_brief"]

        self.assertEqual(material["hook_direction_context"]["source"], "manual")
        self.assertIn("夹住侧边头发后轻转头", material["safe_micro_actions"])

    def test_summary_fields_hide_process_detail(self):
        fields = _product_task_summary_fields(
            {
                "pipeline_status": "WAITING_AI_RETURN",
                "next_action": "CHECK_AI_RETURN_THEN_CONTINUE",
                "requested_variant_count": 20,
                "actual_variant_count": 8,
            }
        )

        self.assertEqual(fields["任务状态"], "生产中")
        self.assertEqual(fields["工厂状态"], "等待AI回流")
        self.assertEqual(fields["成片进度"], "8/20")
        self.assertEqual(fields["异常等级"], "提醒")

    def test_unconfirmed_voc_package_does_not_block_ads_fast(self):
        plan = plan_ads_mixcut(
            "PROD_UNCONFIRMED_VOC",
            {"product_id": "PROD_UNCONFIRMED_VOC", "task_type": "ADS_FAST", "task_status": "running"},
            {
                "total": 2,
                "by_core_role": {"hero": 0, "result": 0, "detail": 0, "scene": 0, "ending": 0},
                "hook_segments": 0,
                "voc_segments": {"total": 0, "usable": 0, "unusable": 0},
            },
            {"total_outputs": 0, "good_outputs": 0, "strict_good_outputs_with_voc_segments": 0},
            {
                "package_id": "VOC_UNCONFIRMED",
                "readiness_status": "smoke_ready_unconfirmed",
                "confirmed": False,
                "candidates": [{"candidate_id": "C1"}],
            },
            {},
            target=20,
            use_voc_hooks=True,
            max_hook=6,
            max_support=12,
        )

        self.assertNotEqual(plan["status"], "needs_manual_confirmation")
        self.assertNotIn("voc_manual_confirmation_required", plan["blockers"])
        self.assertEqual(plan["flow_summary"]["voc_participation"]["mode"], "voc_unconfirmed_ignored")


if __name__ == "__main__":
    unittest.main()
