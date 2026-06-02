from __future__ import annotations

from datetime import datetime

from auto_mixcut.core.result import Result

from .context import SkillContext
from .feishu_review_skill import FeishuReviewSkill
from .final_video_qc_skill import FinalVideoQCSkill
from .quality_gate_skill import QualityGateSkill
from .render_plan_skill import RenderPlanSkill
from .render_skill import RenderSkill


class RemixSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def execute_pending(self, batch_id: str | None = None, limit: int = 10) -> Result:
        where = "remix_plan_json IS NOT NULL"
        params: tuple = ()
        if batch_id:
            where += " AND batch_id=?"
            params = (batch_id,)
        outputs = self.ctx.repo.list_where("outputs", where, params)
        results = []
        for output in outputs:
            plan = output.get("remix_plan_json") or {}
            if plan.get("status") != "planned":
                continue
            res = self.execute_output(output["output_id"])
            results.append(res.to_dict())
            if not res.success:
                return res
            if len(results) >= limit:
                break
        return Result.ok({"batch_id": batch_id, "processed": len(results), "results": results})

    def execute_output(self, output_id: str) -> Result:
        source = self.ctx.repo.get("outputs", "output_id", output_id)
        if not source:
            return Result.fail("OUTPUT_NOT_FOUND", "source output not found", {"output_id": output_id})
        remix_plan = source.get("remix_plan_json") or {}
        if remix_plan.get("status") not in {"planned", "failed"}:
            return Result.ok({"output_id": output_id, "skipped": True, "status": remix_plan.get("status")})

        planned = RenderPlanSkill(self.ctx).create_remix_plan(source, remix_plan)
        if not planned.success:
            self._mark_source(source, remix_plan, "failed", {"error": planned.to_dict()})
            return planned
        batch_id = planned.data["batch_id"]

        rendered = RenderSkill(self.ctx).render_batch(batch_id)
        if not rendered.success:
            self._mark_source(source, remix_plan, "failed", {"batch_id": batch_id, "error": rendered.to_dict()})
            return rendered
        qc = QualityGateSkill(self.ctx).check_batch(batch_id)
        if not qc.success:
            self._mark_source(source, remix_plan, "failed", {"batch_id": batch_id, "error": qc.to_dict()})
            return qc
        new_outputs = self.ctx.repo.list_where("outputs", "batch_id=? ORDER BY id", (batch_id,))
        if not any(row.get("machine_quality_status") in _usable_quality_statuses() for row in new_outputs):
            detail = {"batch_id": batch_id, "qc": qc.to_dict(), "output_ids": [row["output_id"] for row in new_outputs]}
            self._mark_source(source, remix_plan, "failed", detail)
            return Result.fail("REMIX_QC_FAILED", "remix rendered but no output passed machine QC", detail)

        final_qc = FinalVideoQCSkill(self.ctx).check_batch(batch_id)
        if not final_qc.success:
            self._mark_source(source, remix_plan, "failed", {"batch_id": batch_id, "error": final_qc.to_dict()})
            return final_qc
        FeishuReviewSkill(self.ctx).sync_output_qc(batch_id)

        refreshed = self.ctx.repo.list_where("outputs", "batch_id=? ORDER BY id", (batch_id,))
        new_output_ids = [row["output_id"] for row in refreshed]
        self._mark_source(
            source,
            remix_plan,
            "executed",
            {
                "remix_batch_id": batch_id,
                "remix_output_ids": new_output_ids,
                "executed_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
        )
        return Result.ok({"source_output_id": output_id, "remix_batch_id": batch_id, "remix_output_ids": new_output_ids})

    def _mark_source(self, source: dict, remix_plan: dict, status: str, extra: dict) -> None:
        updated = dict(remix_plan)
        updated["status"] = status
        updated.update(extra)
        self.ctx.repo.update("outputs", "output_id", source["output_id"], {"remix_plan_json": updated})


def _usable_quality_statuses() -> set[str]:
    return {"publish_ready", "needs_review", "passed", "passed_with_warning"}
