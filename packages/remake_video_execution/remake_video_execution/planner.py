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


def _segments_from_ranges(
    plan: ExecutionPlan, ranges: list[tuple[int, int]], issues: list[Issue]
) -> SegmentPlan:
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
    return _segments_from_ranges(plan, ranges, issues)


# Plan C's media executor only understands two or three ordered segments.  A
# raw minimum-segment pack can produce four pieces for a 31-45s remake because
# it optimises for near-15s pieces rather than for the 2-3 segment contract.
MIN_SEGMENT_MS = 4000


def plan_c_segment_count(total_seconds: int) -> int:
    """Two segments up to 30s, three up to 45s, matching the shared A/B/C cap."""

    return max(2, (int(total_seconds) + 14) // 15)


def _paced_ranges(plan: ExecutionPlan, max_ms: int, count: int) -> list[tuple[int, int]]:
    """Split the frozen timeline into the fewest <=15s pieces Plan C accepts.

    Boundaries are integer seconds so the requested H3 durations add up to the
    frozen source duration.  Interior boundaries snap to a nearby shot boundary
    so a cut lands on a real edit when possible; a boundary that stays inside a
    shot is a legitimate continuous bridge, not an error.
    """

    total_ms = int(plan.duration_ms)
    total_seconds = int(round(total_ms / 1000))
    if total_ms <= 0 or total_seconds <= 0 or count < 2:
        return []
    base, remainder = divmod(total_seconds, count)
    durations = [base + 1] * remainder + [base] * (count - remainder)
    raw_bounds = [0]
    cursor = 0
    for duration in durations[:-1]:
        cursor += duration
        raw_bounds.append(cursor * 1000)
    raw_bounds.append(total_ms)

    tolerance = max_ms // 5
    snapped = [0]
    cursor = 0
    for duration in durations[:-1]:
        cursor += duration
        target_ms = cursor * 1000
        candidates = [
            value for shot in plan.shots
            for value in (shot.start_ms, shot.end_ms)
            if 0 < value < total_ms and abs(value - target_ms) <= tolerance
        ]
        snapped.append(min(candidates, key=lambda value: abs(value - target_ms)) if candidates else target_ms)
    snapped.append(total_ms)

    def valid(bounds: list[int]) -> bool:
        if len(set(bounds)) != len(bounds) or bounds[0] != 0 or bounds[-1] != total_ms:
            return False
        return all(
            MIN_SEGMENT_MS <= bounds[index + 1] - bounds[index] <= max_ms
            for index in range(len(bounds) - 1)
        )

    bounds = snapped if valid(snapped) else raw_bounds
    if not valid(bounds):
        return []
    return [(bounds[index], bounds[index + 1]) for index in range(len(bounds) - 1)]


def plan_segments_for_plan_c(
    plan: ExecutionPlan, capabilities: ProviderCapabilities | None = None
) -> SegmentPlan:
    """Plan exactly 2-3 segments for the shared Plan C executor.

    The count is fixed by the 15s per-segment cap (2 up to 30s, 3 up to 45s), so
    a 30s remake is A/B and a 41s remake is A/B/C even when the minimum-segment
    pack would prefer three near-equal 10s pieces.
    """

    caps = capabilities or ProviderCapabilities()
    max_ms = caps.max_segment_seconds * 1000
    total_seconds = int(round(plan.duration_ms / 1000))
    expected = plan_c_segment_count(total_seconds)
    if total_seconds > 15 and 2 <= expected <= 3:
        ranges = _paced_ranges(plan, max_ms, expected)
        if ranges:
            return _segments_from_ranges(plan, ranges, list(plan.issues))
    base = plan_segments(plan, caps)
    if 2 <= len(base.segments) <= 3:
        return base
    ranges = _paced_ranges(plan, max_ms, max(2, expected))
    issues = list(plan.issues)
    if not 2 <= len(ranges) <= 3:
        issues.append(Issue(
            "REMAKE_SEGMENT_COUNT_UNSUPPORTED", "BLOCK",
            f"复刻时长 {plan.duration_ms / 1000:g}s 需要 {len(ranges)} 段，"
            "Plan C 媒体执行器只支持 2 至 3 段（约16-45秒）",
            affected_stage="PLAN",
        ))
    return _segments_from_ranges(plan, ranges, issues)

