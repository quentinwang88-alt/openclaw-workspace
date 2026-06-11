from __future__ import annotations

from auto_mixcut.core.result import Result

from .context import SkillContext
from .render_plan_skill import estimate_render_plan_capacity
from .usage_counter_skill import is_good_rendered_output


DEFAULT_EXTRA_CAPACITY_PROBE = 10
MAX_HEAVY_RENDER_PLAN_ESTIMATE_SEGMENTS = 80
AVG_SEGMENTS_PER_OUTPUT = 5
SUPPORT_SEGMENT_REUSE_CAP = 3
FIRST_SLOT_REUSE_CAP = 2


class CapacityCounterSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def refresh_product(self, product_id: str, extra_probe_count: int = DEFAULT_EXTRA_CAPACITY_PROBE) -> Result:
        product_id = str(product_id or "").strip()
        if not product_id:
            return Result.fail("PRODUCT_ID_REQUIRED", "product_id is required")
        tasks = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
        if not tasks:
            return Result.fail("TASK_NOT_FOUND", "task not found", {"product_id": product_id})
        task = tasks[0]
        target = int(task.get("requested_variant_count") or task.get("allowed_variant_count") or 0)
        actual = _actual_good_outputs(self.ctx, product_id)
        target_remaining = max(0, target - actual)
        probe_count = max(0, int(extra_probe_count or DEFAULT_EXTRA_CAPACITY_PROBE))
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        if probe_count and len(segments) <= MAX_HEAVY_RENDER_PLAN_ESTIMATE_SEGMENTS:
            estimate = estimate_render_plan_capacity(self.ctx, product_id, probe_count, allow_fill_mode=True)
        else:
            estimate = _lightweight_capacity_estimate(segments, probe_count)
        raw_pool_extra = int(estimate.get("planned_count") or 0)
        skipped = int(estimate.get("skipped_count") or 0)
        first_slot_remaining = max(0, int(estimate.get("first_slot_capacity") or 0) - actual)
        pool_extra = raw_pool_extra
        bottleneck = _bottleneck_text(estimate)
        note = _capacity_note(target, actual, target_remaining, pool_extra, skipped, estimate, first_slot_remaining, bottleneck)
        patch = {
            "actual_variant_count": actual,
            "target_remaining_variant_count": target_remaining,
            "material_pool_extra_capacity": pool_extra,
            "first_slot_remaining_capacity": first_slot_remaining,
            "current_bottleneck": bottleneck,
            "capacity_note": note,
        }
        write = self.ctx.repo.update("content_tasks", "task_id", task["task_id"], patch)
        if not write.success:
            return write
        return Result.ok({"product_id": product_id, **patch, "estimate": {**estimate, "raw_material_pool_extra_capacity": raw_pool_extra}})


def _actual_good_outputs(ctx: SkillContext, product_id: str) -> int:
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    return sum(1 for output in outputs if is_good_rendered_output(output))


def _lightweight_capacity_estimate(segments: list[dict], probe_count: int) -> dict:
    usable = [segment for segment in segments if segment.get("effective_roles_json")]
    first_slot = [segment for segment in usable if "hero" in (segment.get("effective_roles_json") or [])]
    segment_capacity = int(len(usable) * SUPPORT_SEGMENT_REUSE_CAP / AVG_SEGMENTS_PER_OUTPUT)
    first_slot_capacity = len(first_slot) * FIRST_SLOT_REUSE_CAP
    planned = max(0, min(int(probe_count or 0), segment_capacity, first_slot_capacity))
    bottlenecks = []
    if planned == segment_capacity:
        bottlenecks.append("unique_segment_capacity")
    if planned == first_slot_capacity:
        bottlenecks.append("first_slot_capacity")
    return {
        "planned_count": planned,
        "skipped_count": 0,
        "estimate_mode": "lightweight_capacity",
        "segment_count": len(segments),
        "usable_segment_count": len(usable),
        "first_slot_candidates": len(first_slot),
        "first_slot_capacity": first_slot_capacity,
        "segment_capacity": segment_capacity,
        "bottlenecks": bottlenecks,
    }


def _capacity_note(target: int, actual: int, target_remaining: int, pool_extra: int, skipped: int, estimate: dict, first_slot_remaining: int, bottleneck: str) -> str:
    parts = [
        f"目标={target}",
        f"已有效={actual}",
        f"目标缺口={target_remaining}",
        f"素材池额外容量={pool_extra}",
        f"首镜剩余容量={first_slot_remaining}",
    ]
    if skipped:
        parts.append(f"规划跳过={skipped}")
    if bottleneck:
        parts.append(f"瓶颈={bottleneck}")
    if target_remaining == 0 and pool_extra > 0:
        parts.append("可继续扩量，但当前目标已满")
    elif target_remaining > 0 and pool_extra == 0:
        parts.append("当前素材池无法继续补齐，需补素材")
    return "；".join(parts)


def _bottleneck_text(estimate: dict) -> str:
    if estimate.get("error"):
        return str(estimate.get("error"))
    bottlenecks = set(estimate.get("bottlenecks") or [])
    if "first_slot_capacity" in bottlenecks:
        return "首镜容量不足"
    if "unique_segment_capacity" in bottlenecks:
        return "可用切片数量不足"
    if not int(estimate.get("planned_count") or 0) and int(estimate.get("skipped_count") or 0):
        return "素材质量/去重/模板约束不足"
    reuse_mode = str(estimate.get("reuse_mode") or "")
    if reuse_mode == "strict":
        return ""
    return f"已进入{reuse_mode}复用模式"
