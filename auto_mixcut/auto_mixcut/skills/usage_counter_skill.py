from __future__ import annotations

from collections import Counter

from .context import SkillContext


GOOD_MACHINE_OUTPUT_STATUSES = {"passed", "passed_with_warning", "needs_review", "publish_ready"}
REJECTED_HUMAN_OUTPUT_STATUSES = {"rejected", "discard", "不可发布", "废弃", "不要", "不使用"}


def is_human_rejected_output(output: dict) -> bool:
    return str(output.get("human_quality_status") or output.get("human_review_status") or "").strip() in REJECTED_HUMAN_OUTPUT_STATUSES


def is_good_rendered_output(output: dict) -> bool:
    return (
        output.get("render_status") == "rendered"
        and output.get("machine_quality_status") in GOOD_MACHINE_OUTPUT_STATUSES
        and not is_human_rejected_output(output)
    )


def is_rejected_rendered_output(output: dict) -> bool:
    return output.get("render_status") == "rendered" and is_human_rejected_output(output)


def refresh_output_segment_usage(ctx: SkillContext, output_id: str) -> None:
    if not output_id:
        return
    rows = ctx.repo.list_where("output_segments", "output_id=?", (output_id,))
    for segment_id in {str(row.get("segment_id") or "") for row in rows if row.get("segment_id")}:
        refresh_segment_usage(ctx, segment_id)


def refresh_segment_usage(ctx: SkillContext, segment_id: str) -> None:
    if not segment_id:
        return
    rows = ctx.repo.list_where("output_segments", "segment_id=?", (segment_id,))
    good_output_ids: set[str] = set()
    rejected_output_ids: set[str] = set()
    for row in rows:
        output_id = str(row.get("output_id") or "")
        if not output_id or output_id in good_output_ids or output_id in rejected_output_ids:
            continue
        output = ctx.repo.get("outputs", "output_id", output_id) or {}
        if is_rejected_rendered_output(output):
            rejected_output_ids.add(output_id)
        elif is_good_rendered_output(output):
            good_output_ids.add(output_id)
    ctx.repo.update(
        "segments",
        "segment_id",
        segment_id,
        {
            "used_in_outputs_count": len(good_output_ids),
            "used_in_rejected_outputs_count": len(rejected_output_ids),
        },
    )


def reconcile_product_segment_usage(ctx: SkillContext, product_id: str) -> dict:
    if not product_id:
        return {"product_id": product_id, "segments_touched": 0, "segments_with_usage": 0, "segments_with_rejected_usage": 0}
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    good_ids = [output["output_id"] for output in outputs if is_good_rendered_output(output)]
    rejected_ids = [output["output_id"] for output in outputs if is_rejected_rendered_output(output)]
    normal_counts = _segment_counts_for_outputs(ctx, good_ids)
    rejected_counts = _segment_counts_for_outputs(ctx, rejected_ids)
    touched = 0
    for segment in ctx.repo.list_where("segments", "product_id=?", (product_id,)):
        segment_id = str(segment.get("segment_id") or "")
        ctx.repo.update(
            "segments",
            "segment_id",
            segment_id,
            {
                "used_in_outputs_count": int(normal_counts.get(segment_id, 0)),
                "used_in_rejected_outputs_count": int(rejected_counts.get(segment_id, 0)),
            },
        )
        touched += 1
    return {
        "product_id": product_id,
        "outputs_seen": len(outputs),
        "good_rendered_outputs": len(good_ids),
        "rejected_rendered_outputs": len(rejected_ids),
        "segments_touched": touched,
        "segments_with_usage": len(normal_counts),
        "segments_with_rejected_usage": len(rejected_counts),
    }


def _segment_counts_for_outputs(ctx: SkillContext, output_ids: list[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not output_ids:
        return counts
    placeholders = ",".join(["?"] * len(output_ids))
    rows = ctx.repo.list_where("output_segments", f"output_id IN ({placeholders})", tuple(output_ids))
    counts.update(str(row.get("segment_id") or "") for row in rows if row.get("segment_id"))
    return counts
