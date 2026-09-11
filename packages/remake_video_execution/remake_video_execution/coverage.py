from __future__ import annotations

from .contracts import ExecutionPlan, Issue, SegmentPlan


def validate_coverage(plan: ExecutionPlan, segments: SegmentPlan) -> list[Issue]:
    issues: list[Issue] = []
    coverage = {shot.shot_id: 0 for shot in plan.shots}
    for segment in segments.segments:
        for item in segment.source_shot_slices:
            coverage[item["shot_id"]] += item["source_end_ms"] - item["source_start_ms"]
    for shot in plan.shots:
        expected = shot.end_ms - shot.start_ms
        if coverage[shot.shot_id] != expected:
            issues.append(Issue(
                "SHOT_COVERAGE_MISMATCH", "BLOCK",
                f"{shot.shot_id} 覆盖 {coverage[shot.shot_id]}ms，原稿为 {expected}ms",
                shot.source_span,
            ))
    if segments.segments:
        if segments.segments[0].global_start_ms != 0 or segments.segments[-1].global_end_ms != plan.duration_ms:
            issues.append(Issue("TIMELINE_COVERAGE_MISMATCH", "BLOCK", "片段没有覆盖完整时间轴"))
    return issues

