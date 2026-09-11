from __future__ import annotations

import math

from .capabilities import ProviderCapabilities
from .contracts import ExecutionPlan, GenerationSegment, Issue, SegmentPlan, Shot
from .renderer import render_segment_prompt


def _slice_boundaries(plan: ExecutionPlan, max_ms: int) -> list[int]:
    boundaries = [0]
    cursor = 0
    for shot in plan.shots:
        if shot.start_ms != cursor:
            cursor = shot.start_ms
        while shot.end_ms - cursor > max_ms:
            cursor += max_ms
            boundaries.append(cursor)
        if shot.end_ms - boundaries[-1] > max_ms:
            boundaries.append(cursor)
        cursor = shot.end_ms
        if cursor - boundaries[-1] >= max_ms * 0.60:
            boundaries.append(cursor)
    if plan.duration_ms and boundaries[-1] != plan.duration_ms:
        boundaries.append(plan.duration_ms)
    return sorted(set(boundaries))


def _pack_shots(plan: ExecutionPlan, max_ms: int) -> list[tuple[int, int]]:
    if not plan.shots:
        return []
    candidates = sorted({0, plan.duration_ms, *(shot.start_ms for shot in plan.shots), *(shot.end_ms for shot in plan.shots)})
    n = len(candidates)
    best: list[tuple[float, list[int]] | None] = [None] * n
    best[0] = (0.0, [0])
    for i in range(n):
        if best[i] is None:
            continue
        for j in range(i + 1, n):
            duration = candidates[j] - candidates[i]
            if duration > max_ms:
                break
            if duration <= 0:
                continue
            cost = best[i][0] + 1000 + abs(max_ms * 0.85 - duration) / max_ms
            if best[j] is None or cost < best[j][0]:
                best[j] = (cost, best[i][1] + [j])
    if best[-1] is not None:
        path = best[-1][1]
        return [(candidates[path[i]], candidates[path[i + 1]]) for i in range(len(path) - 1)]
    boundaries = _slice_boundaries(plan, max_ms)
    return [(boundaries[i], boundaries[i + 1]) for i in range(len(boundaries) - 1)]


def plan_segments(plan: ExecutionPlan, capabilities: ProviderCapabilities | None = None) -> SegmentPlan:
    caps = capabilities or ProviderCapabilities()
    max_ms = caps.max_segment_seconds * 1000
    ranges = _pack_shots(plan, max_ms)
    issues = list(plan.issues)
    if len(ranges) > caps.max_segments:
        issues.append(Issue(
            "SEGMENT_LIMIT_EXCEEDED", "BLOCK",
            f"需要 {len(ranges)} 个片段，渠道配置最多 {caps.max_segments} 个",
            affected_stage="PREFLIGHT",
        ))
    segments: list[GenerationSegment] = []
    for ordinal, (start_ms, end_ms) in enumerate(ranges, start=1):
        selected = [shot for shot in plan.shots if shot.end_ms > start_ms and shot.start_ms < end_ms]
        slices = [{
            "shot_id": shot.shot_id,
            "source_start_ms": max(start_ms, shot.start_ms),
            "source_end_ms": min(end_ms, shot.end_ms),
        } for shot in selected]
        split_inside = any(shot.start_ms < start_ms < shot.end_ms for shot in plan.shots)
        boundary = "START" if ordinal == 1 else ("CONTINUOUS" if split_inside else "CUT")
        requested = math.ceil((end_ms - start_ms) / 1000)
        segments.append(GenerationSegment(
            segment_id=f"SEG_{ordinal:02d}", ordinal=ordinal,
            global_start_ms=start_ms, global_end_ms=end_ms,
            requested_duration_seconds=requested,
            source_shot_slices=slices, incoming_boundary=boundary,
            prompt=render_segment_prompt(plan, selected, start_ms, end_ms),
        ))
    return SegmentPlan(
        source_revision_hash=plan.source.source_revision_hash,
        duration_ms=plan.duration_ms,
        segments=segments,
        issues=issues,
    )

