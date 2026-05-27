from __future__ import annotations

import os

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext
from .llm_router_skill import LLMRouterSkill


class AITaggingSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self.router = LLMRouterSkill(ctx)

    def submit_batch(self, product_id: str, prompt_version: str = "v1.0") -> Result:
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        segments = _limit_segments(segments)
        batch_id = new_id("AIBATCH")
        self.ctx.repo.upsert(
            "ai_batches",
            "ai_batch_id",
            {
                "ai_batch_id": batch_id,
                "product_id": product_id,
                "batch_type": "segment_tagging",
                "status": "submitted",
                "total_segments": len(segments),
                "model_tier": "medium_vision",
                "prompt_version": prompt_version,
            },
        )
        return Result.ok({"ai_batch_id": batch_id, "total_segments": len(segments)})

    def poll_results(self, product_id: str, prompt_version: str = "v1.0") -> Result:
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        segments = _limit_segments(segments)
        completed = 0
        for idx, segment in enumerate(segments):
            call = self.router.call(
                "segment_tagging_default",
                {"segment_id": segment["segment_id"], "index": idx, "prompt_version": prompt_version, "image_count": _frame_count(self.ctx, segment["segment_id"])},
                product_id=product_id,
                segment_id=segment["segment_id"],
                asset_id=segment["asset_id"],
            )
            if not call.success:
                continue
            tag = call.data["response"]
            review = _needs_review(segment, tag)
            tag["needs_human_review"] = bool(tag.get("needs_human_review") or review)
            self.ctx.repo.insert(
                "ai_tag_runs",
                {
                    "tag_run_id": new_id("TAGRUN"),
                    "segment_id": segment["segment_id"],
                    "model_tier": call.data["route"]["model_tier"],
                    "model_name": call.data["route"]["model_name"],
                    "prompt_version": prompt_version,
                    "run_type": "segment_tagging",
                    "temperature": 0.0,
                    "raw_response": tag,
                    "parsed_success": 1,
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment["segment_id"],
                    "tag_source": "ai",
                    "primary_shot_role": tag["primary_shot_role"],
                    "secondary_roles_json": tag["secondary_roles"],
                    "product_visibility": tag["product_visibility"],
                    "hook_strength": tag["hook_strength"],
                    "mixcut_usability": tag["mixcut_usability"],
                    "risk_level": tag["risk_level"],
                    "confidence": tag["confidence"],
                    "needs_human_review": int(tag["needs_human_review"]),
                    "reason": tag["reason"],
                },
            )
            completed += 1
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "AI_TAGGED"})
        return Result.ok({"completed_segments": completed})

    def retry_failed(self, product_id: str) -> Result:
        return self.poll_results(product_id)


def _frame_count(ctx: SkillContext, segment_id: str) -> int:
    return len(ctx.repo.list_where("segment_frames", "segment_id=?", (segment_id,)))


def _needs_review(segment, tag) -> bool:
    return (
        tag.get("confidence") == "low"
        or tag.get("risk_level") in {"medium", "high"}
        or segment.get("product_match_status") == "uncertain"
        or tag.get("mixcut_usability") in {"needs_processing", "no"}
    )


def _limit_segments(segments):
    limit = int(os.environ.get("AUTO_MIXCUT_TAG_LIMIT", "0") or "0")
    if limit > 0:
        return segments[:limit]
    return segments
