from __future__ import annotations

from auto_mixcut.core.result import Result

from .context import SkillContext
from .llm_router_skill import LLMRouterSkill


class AIGeneratedConsistencySkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self.router = LLMRouterSkill(ctx)

    def check_product(self, product_id: str) -> Result:
        segments = self.ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (product_id,))
        for segment in segments:
            res = self.check_segment(segment["segment_id"])
            if not res.success:
                return res
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "CONSISTENCY_CHECKED"})
        return Result.ok({"checked_segments": len(segments)})

    def check_segment(self, segment_id: str) -> Result:
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        if not segment:
            return Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": segment_id})
        if segment["source_type"] != "ai_generated":
            return Result.ok({"segment_id": segment_id, "skipped": True})
        call = self.router.call("ai_generated_consistency_check", {"segment_id": segment_id}, product_id=segment["product_id"], segment_id=segment_id, asset_id=segment["asset_id"])
        if not call.success:
            return call
        data = call.data["response"]
        self.ctx.repo.update(
            "segments",
            "segment_id",
            segment_id,
            {
                "frame_consistency_score": data["frame_consistency_score"],
                "frame_consistency_status": data["frame_consistency_status"],
                "frame_consistency_reason": data["frame_consistency_reason"],
            },
        )
        return Result.ok({"segment_id": segment_id, **data})
