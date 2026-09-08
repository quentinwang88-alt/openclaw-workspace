"""Overall color consistency (全图调色一致性): program-side stats + verdict.

Free, model-free group check that runs before any paid vision QA: four images
from the same piece must share skin-tone/color-temperature/saturation
characteristics (the "one shot looks whiter" failure mode). Visual
cross-checks complement this but never replace the deterministic floor.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from PIL import Image

SCHEMA_VERSION = "opv-photo-color-consistency-v1"

# 组内互比阈值（初版经验值，随真实样片校准）。
MAX_RB_RATIO_RANGE = 0.18          # 色温代理：R/B 比极差
MAX_SATURATION_STD = 0.06          # 平均饱和度标准差（0-1）
MAX_BRIGHTNESS_RANGE = 28.0        # 亮度极差（0-255）


def color_stats(path: str) -> Dict[str, float]:
    """Downsampled color statistics for one image."""
    with Image.open(path) as image:
        small = image.convert("RGB").resize((160, 160), Image.Resampling.BILINEAR)
        pixels = list(small.getdata())
    total = len(pixels)
    red = blue = brightness = 0.0
    saturation_sum = 0.0
    for r, g, b in pixels:
        red += r
        blue += b
        brightness += 0.2126 * r + 0.7152 * g + 0.0722 * b
        max_c, min_c = max(r, g, b), min(r, g, b)
        saturation_sum += 0.0 if max_c == 0 else (max_c - min_c) / max_c
    mean_red, mean_blue = red / total, blue / total
    return {
        "rb_ratio": mean_red / mean_blue if mean_blue else 1.0,
        "brightness": brightness / total,
        "saturation": saturation_sum / total,
    }


def evaluate_group_consistency(paths: Sequence[str]) -> Dict[str, Any]:
    """Deterministic group verdict; single-image groups never fail."""
    path_list = [str(value) for value in paths]
    metrics = {path: color_stats(path) for path in path_list}
    issues: List[str] = []
    failed_indices: List[int] = []
    if len(path_list) > 1:
        rb_values = [metrics[path]["rb_ratio"] for path in path_list]
        sat_values = [metrics[path]["saturation"] for path in path_list]
        bright_values = [metrics[path]["brightness"] for path in path_list]
        rb_range = max(rb_values) - min(rb_values)
        sat_deviation = _stddev(sat_values)
        bright_range = max(bright_values) - min(bright_values)
        if rb_range > MAX_RB_RATIO_RANGE:
            issues.append(f"色温不一致：R/B 比极差 {rb_range:.2f} > {MAX_RB_RATIO_RANGE}")
        if sat_deviation > MAX_SATURATION_STD:
            issues.append(
                f"饱和度不一致：标准差 {sat_deviation:.3f} > {MAX_SATURATION_STD}"
            )
        if bright_range > MAX_BRIGHTNESS_RANGE:
            issues.append(f"亮度不一致：极差 {bright_range:.0f} > {MAX_BRIGHTNESS_RANGE:.0f}")
        if issues:
            failed_indices = _outlier_indices(
                path_list, metrics, rb_values, sat_values, bright_values
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": not issues,
        "failed_roles": failed_indices,
        "metrics": {path: {key: round(value, 4) for key, value in stats.items()}
                    for path, stats in metrics.items()},
        "issues": issues,
    }


def evaluate_visual_consistency(
    review: Mapping[str, Any], role_order: Sequence[str], *,
    role_paths: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Deterministic verdict over visual group-consistency observations.

    The model only reports per-image match scores; any dimension below 85
    fails that role regardless of any model-side pass suggestion.
    """
    raw_roles = review.get("roles") if isinstance(review, Mapping) else None
    roles = raw_roles if isinstance(raw_roles, list) else []
    by_role: Dict[str, Dict[str, Any]] = {}
    failed_roles: List[str] = []
    issues: List[str] = []
    allowed = [str(value) for value in role_order]
    for entry in roles:
        if not isinstance(entry, Mapping):
            continue
        role = str(entry.get("role") or "")
        if role not in allowed:
            continue
        scores: Dict[str, float] = {}
        for key in ("skin_tone_match", "color_grading_match", "lighting_match"):
            try:
                scores[key] = max(0.0, min(100.0, float(entry.get(key) or 0)))
            except (TypeError, ValueError):
                scores[key] = 0.0
        low = [f"{key}={scores[key]:.0f}<85" for key in scores if scores[key] < 85]
        passed = not low
        if not passed:
            failed_roles.append(role)
            path_hint = str((role_paths or {}).get(role) or "")
            issues.append(f"{role} 跨图一致性低分（{'，'.join(low)}）{path_hint}")
        by_role[role] = {
            "passed": passed, "scores": scores,
            "drift_note_zh": str(entry.get("drift_note_zh") or ""),
            "issues": low,
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "visual_group_consistency",
        "passed": not failed_roles,
        "roles": by_role,
        "failed_roles": failed_roles,
        "issues": issues,
        "model_notes": str(review.get("notes") or "") if isinstance(review, Mapping) else "",
    }


def _outlier_indices(path_list: Sequence[str], metrics: Mapping[str, Mapping[str, float]],
                     rb_values: Sequence[float], sat_values: Sequence[float],
                     bright_values: Sequence[float]) -> List[int]:
    """Attribute failures to images deviating beyond a threshold from medians."""
    med_rb, med_sat, med_bright = (
        _median(rb_values), _median(sat_values), _median(bright_values),
    )
    failed: set = set()
    for index in range(len(path_list)):
        if abs(rb_values[index] - med_rb) > MAX_RB_RATIO_RANGE:
            failed.add(index)
        if abs(sat_values[index] - med_sat) > MAX_SATURATION_STD:
            failed.add(index)
        if abs(bright_values[index] - med_bright) > MAX_BRIGHTNESS_RANGE:
            failed.add(index)
    if failed:
        return sorted(failed)
    # 无法逐张归因（整组均匀偏差）时退化为偏离分最大的一张。
    def deviation(index: float) -> float:
        return (
            abs(rb_values[int(index)] - med_rb) / MAX_RB_RATIO_RANGE
            + abs(sat_values[int(index)] - med_sat) / MAX_SATURATION_STD
            + abs(bright_values[int(index)] - med_bright) / MAX_BRIGHTNESS_RANGE
        )
    return [max(range(len(path_list)), key=deviation)]


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _stddev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return variance ** 0.5
