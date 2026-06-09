from __future__ import annotations

from datetime import datetime

from auto_mixcut.core.result import Result

from .context import SkillContext
from .bgm_usage_skill import BgmUsageSkill
from .feishu_review_skill import sync_product_task_best_effort
from .usage_counter_skill import is_good_rendered_output, reconcile_product_segment_usage


class BatchControlSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def abort_batch(self, batch_id: str, reason: str = "operator_abort") -> Result:
        batch = self.ctx.repo.get("mixcut_batches", "batch_id", batch_id)
        if not batch:
            return Result.fail("BATCH_NOT_FOUND", "batch not found", {"batch_id": batch_id})
        product_id = str(batch.get("product_id") or "")
        now = datetime.utcnow().isoformat(timespec="seconds")
        outputs = self.ctx.repo.list_where("outputs", "batch_id=?", (batch_id,))
        plans = self.ctx.repo.list_where("render_plans", "batch_id=?", (batch_id,))
        aborted_outputs = []
        for output in outputs:
            self.ctx.repo.update(
                "outputs",
                "output_id",
                output["output_id"],
                {
                    "human_quality_status": "rejected",
                    "human_feedback_reason": f"aborted:{reason}",
                },
            )
            BgmUsageSkill(self.ctx).record_output_feedback(output["output_id"], "aborted", f"aborted:{reason}")
            aborted_outputs.append(output["output_id"])
        aborted_plans = []
        for plan in plans:
            self.ctx.repo.update(
                "render_plans",
                "render_plan_id",
                plan["render_plan_id"],
                {
                    "render_status": "aborted",
                    "quality_gate_status": "aborted",
                },
            )
            aborted_plans.append(plan["render_plan_id"])
        self.ctx.repo.update(
            "mixcut_batches",
            "batch_id",
            batch_id,
            {"batch_status": "aborted_by_operator", "updated_at": now},
        )
        usage = reconcile_product_segment_usage(self.ctx, product_id) if product_id else {}
        actual = _actual_good_outputs(self.ctx, product_id)
        task_sync = {}
        tasks = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,)) if product_id else []
        if tasks:
            self.ctx.repo.update("content_tasks", "task_id", tasks[0]["task_id"], {"actual_variant_count": actual})
            task_sync = sync_product_task_best_effort(self.ctx, product_id)
        return Result.ok(
            {
                "batch_id": batch_id,
                "product_id": product_id,
                "aborted_outputs": aborted_outputs,
                "aborted_plans": aborted_plans,
                "actual_variant_count": actual,
                "usage": usage,
                "task_sync": task_sync,
            }
        )


def _actual_good_outputs(ctx: SkillContext, product_id: str) -> int:
    if not product_id:
        return 0
    return sum(1 for output in ctx.repo.list_where("outputs", "product_id=?", (product_id,)) if is_good_rendered_output(output))
