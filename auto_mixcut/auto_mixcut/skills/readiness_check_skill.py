from __future__ import annotations

from auto_mixcut.core.result import Result

from .context import SkillContext


class ReadinessCheckSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_product(self, product_id: str, requested_count: int | None = None) -> Result:
        task = _task(self.ctx, product_id)
        requested = requested_count if requested_count is not None else int((task or {}).get("requested_variant_count") or 0)
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        counts = {"hero": 0, "detail": 0, "result": 0, "scene": 0, "ending": 0}
        total_usable = 0
        for seg in segments:
            roles = seg.get("effective_roles_json") or []
            if roles:
                total_usable += 1
            for role in counts:
                if role in roles:
                    counts[role] += 1
        tier, max_variants, gaps = _tier(counts, total_usable)
        allowed = min(requested or max_variants, max_variants)
        material_status = "ready" if allowed > 0 else "not_ready"
        self.ctx.repo.update(
            "content_tasks",
            "product_id",
            product_id,
            {
                "material_tier": tier,
                "material_status": material_status,
                "allowed_variant_count": allowed,
                "blocked_reason": "; ".join(gaps),
                "task_status": "READINESS_CHECKED",
            },
        )
        return Result.ok({"product_id": product_id, "tier": tier, "allowed_variant_count": allowed, "counts": counts, "total_usable": total_usable, "gaps": gaps})


def _task(ctx: SkillContext, product_id: str):
    tasks = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return tasks[0] if tasks else None


def _tier(counts, total):
    if counts["hero"] >= 3 and counts["detail"] >= 3 and counts["result"] >= 3 and counts["scene"] >= 1 and total >= 12:
        return "tier_3_full", 10, []
    if counts["hero"] >= 2 and counts["detail"] >= 2 and counts["result"] >= 2 and counts["scene"] >= 1 and total >= 7:
        return "tier_2_standard", 5, []
    gaps = []
    if counts["hero"] < 1:
        gaps.append("need at least 1 hero-capable segment")
    if counts["detail"] < 1:
        gaps.append("need at least 1 detail-capable segment")
    if counts["result"] < 1:
        gaps.append("need at least 1 result-capable segment")
    if total < 4:
        gaps.append("need at least 4 usable segments")
    if not gaps:
        return "tier_1_minimum", 2, []
    return "tier_0_not_ready", 0, gaps
