from __future__ import annotations

import math
from typing import Any, Callable


MATCH_CORE_VERSION = "voiceover-visual-match-core-v2-key-evidence"


SELLING_POINT_ROLE_TERMS = (
    ("detail_closure", ("拉链", "按扣", "门襟", "口袋")),
    ("detail_neckline", ("领口", "立领", "领型")),
    ("detail_fabric", ("面料", "材质", "质感", "表面", "哑光")),
    ("detail_sleeve", ("袖口", "袖型", "袖子", "衣袖")),
    ("detail_waistline", ("短款", "衣摆", "腰线", "比例")),
    ("fit_turn", ("版型", "显瘦", "合身")),
    ("color_upper", ("颜色", "米白", "米杏", "色调")),
)


def normalized_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.replace("，", ",").split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def apply_key_match_policy(
    lines: list[dict[str, Any]],
    *,
    beat_plan: list[dict[str, Any]] | dict[str, Any] | None = None,
    primary_selling_point: str = "",
) -> list[dict[str, Any]]:
    """Keep hard visual evidence on the hook and one key selling-point beat only."""

    beats = beat_plan or []
    if isinstance(beats, dict):
        beats = beats.get("beats") or beats.get("placements") or beats.get("lines") or []
    beat_by_id = {
        str(row.get("beat_id") or row.get("block_id") or ""): row
        for row in beats
        if isinstance(row, dict)
    }
    merged: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        beat_id = str(line.get("block_id") or line.get("beat_id") or (line.get("beat_ids") or [f"B{index + 1}"])[0])
        beat = beat_by_id.get(beat_id) or {}
        row = {**beat, **line, "block_id": beat_id}
        row["original_required_shot_roles"] = normalized_list(
            line.get("original_required_shot_roles")
            or line.get("required_shot_roles")
            or beat.get("required_shot_roles")
        )
        merged.append(row)

    key_index = _key_beat_index(merged, primary_selling_point)
    output = []
    for index, row in enumerate(merged):
        role = str(row.get("role") or "").lower()
        if index == 0 or role == "hook":
            priority = "hook"
            required_roles = ["main_wear_upper"]
            critical = True
        elif index == key_index:
            priority = "key"
            required_roles = list(row["original_required_shot_roles"])
            critical = bool(required_roles)
        elif role in {"cta", "decision", "ending"}:
            priority = "ending"
            required_roles = []
            critical = False
        else:
            priority = "normal"
            required_roles = []
            critical = False
        output.append(
            {
                **row,
                "match_priority": priority,
                "required_shot_roles": required_roles,
                "critical_evidence": critical,
            }
        )
    return output


def _key_beat_index(lines: list[dict[str, Any]], primary_selling_point: str) -> int | None:
    eligible = [index for index, row in enumerate(lines) if index > 0 and str(row.get("role") or "").lower() not in {"cta", "decision", "ending"}]
    for index in eligible:
        if str(lines[index].get("match_priority") or "").lower() == "key":
            return index

    inferred_roles = {
        role
        for role, terms in SELLING_POINT_ROLE_TERMS
        if any(term in str(primary_selling_point or "") for term in terms)
    }
    if inferred_roles:
        for index in eligible:
            if inferred_roles.intersection(normalized_list(lines[index].get("original_required_shot_roles"))):
                return index

    for index in eligible:
        if normalized_list(lines[index].get("original_required_shot_roles")):
            return index
    return None


def voiceover_intervals(
    lines: list[dict[str, Any]],
    target_ms: int,
    candidates: list[dict[str, Any]],
    *,
    role_resolver: Callable[[dict[str, Any], int], str] | None = None,
) -> list[dict[str, Any]]:
    if not candidates:
        return []
    resolve_role = role_resolver or desired_role_for_line
    max_source_ms = max(int(row["end_ms"]) - int(row["start_ms"]) for row in candidates)
    intervals: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        start_ms = 0 if index == 0 else int(line.get("start_ms") or 0)
        end_ms = int(lines[index + 1].get("start_ms") or 0) if index + 1 < len(lines) else int(line.get("end_ms") or 0)
        beat_ids = line.get("beat_ids") or [f"B{index + 1}"]
        required_roles = normalized_list(line.get("required_shot_roles"))
        line_max_source_ms = _max_source_duration_for_roles(candidates, required_roles, max_source_ms)
        intervals.extend(
            split_voice_interval(
                start_ms,
                end_ms,
                line_max_source_ms,
                beat_id=str(line.get("block_id") or line.get("beat_id") or beat_ids[0]),
                speech_text=str(line.get("speech_text") or ""),
                chinese_translation=str(line.get("chinese_translation") or ""),
                desired_role=resolve_role(line, index),
                required_shot_roles=required_roles,
                original_required_shot_roles=normalized_list(line.get("original_required_shot_roles")),
                match_priority=str(line.get("match_priority") or "normal"),
                critical_evidence=bool(line.get("critical_evidence")),
            )
        )
    spoken_end = int(lines[-1].get("end_ms") or 0)
    if spoken_end < target_ms:
        intervals.extend(
            split_voice_interval(
                spoken_end,
                target_ms,
                max_source_ms,
                beat_id="ENDING",
                speech_text="",
                chinese_translation="",
                desired_role="ending",
                required_shot_roles=[],
            )
        )
    return intervals


def _max_source_duration_for_roles(candidates: list[dict[str, Any]], required_roles: list[str], fallback_ms: int) -> int:
    required = set(required_roles)
    if not required:
        return fallback_ms
    durations = [
        int(row["end_ms"]) - int(row["start_ms"])
        for row in candidates
        if required.intersection(candidate_supported_roles(row))
    ]
    return max(durations) if durations else fallback_ms


def split_voice_interval(start_ms: int, end_ms: int, max_duration_ms: int, **payload: Any) -> list[dict[str, Any]]:
    total_ms = max(0, int(end_ms) - int(start_ms))
    if total_ms <= 0:
        return []
    count = max(1, math.ceil(total_ms / max(1, int(max_duration_ms))))
    base = total_ms // count
    remainder = total_ms - base * count
    output = []
    cursor = int(start_ms)
    for index in range(count):
        duration_ms = base + (1 if index < remainder else 0)
        chunk_end = cursor + duration_ms
        output.append({**payload, "start_ms": cursor, "end_ms": chunk_end, "duration_ms": duration_ms})
        cursor = chunk_end
    return output


def desired_role_for_line(line: dict[str, Any], index: int) -> str:
    if index == 0 or str(line.get("role") or "") == "hook":
        return "hero"
    text = str(line.get("chinese_translation") or "") + str(line.get("speech_text") or "")
    if any(term in text for term in ("拉链", "按扣", "领口", "立领", "面料", "袖口", "口袋")):
        return "detail"
    if any(term in text for term in ("短款", "衣摆", "腰线", "比例", "版型")):
        return "result"
    if str(line.get("role") or "") in {"cta", "decision"}:
        return "ending"
    return "result"


def candidate_supported_roles(candidate: dict[str, Any]) -> set[str]:
    primary = str(candidate.get("primary_shot_role") or "")
    roles = {primary, *normalized_list(candidate.get("secondary_roles"))}
    if primary in {"hero", "result", "ending"}:
        roles.add("main_wear_upper")
    reason = str(candidate.get("reason") or "")
    if primary == "detail" and any(term in reason for term in ("拉链", "按扣", "门襟", "口袋")):
        roles.add("detail_closure")
    if primary == "detail" and any(term in reason for term in ("领口", "立领", "领型")):
        roles.add("detail_neckline")
    if primary == "detail" and any(term in reason for term in ("面料", "材质", "质感", "表面", "哑光", "缝线")):
        roles.add("detail_fabric")
    if primary == "detail" and any(term in reason for term in ("袖口", "袖子", "衣袖", "手臂")):
        roles.add("detail_sleeve")
    if any(term in reason for term in ("短款", "衣摆", "腰线", "裤腰")):
        roles.update({"detail_waistline", "fit_turn"})
    if any(term in reason for term in ("颜色", "米白", "米杏", "色调")):
        roles.add("color_upper")
    return {role for role in roles if role}


def select_voice_candidate(
    candidates: list[dict[str, Any]],
    *,
    desired_role: str,
    duration_ms: int,
    required_roles: list[str],
    speech_text: str,
    used: set[str],
    used_assets: set[str],
    previous_segment_key: str,
    usage: dict[str, Any],
    sequence_no: int,
    current_sequence: list[str],
) -> tuple[dict[str, Any] | None, bool]:
    eligible = [
        row
        for row in candidates
        if int(row["end_ms"]) - int(row["start_ms"]) >= duration_ms
        and str(row.get("duplicate_group_id") or row.get("asset_id") or "") not in used_assets
        and str(row.get("asset_id") or "") not in used_assets
    ]
    if not eligible:
        return None, False
    required = set(required_roles)
    ranked = []
    for row in eligible:
        supported = candidate_supported_roles(row)
        evidence_match = not required or bool(required & supported)
        keyword_score = sum(
            1
            for term in ("拉链", "按扣", "立领", "领口", "短款", "衣摆", "腰线", "米白", "颜色")
            if term in speech_text and term in str(row.get("reason") or "")
        )
        action_score = 2 if "detail_closure" in required and str(row.get("hook_visual_type") or "") == "action" else 0
        role_fit = (
            3
            if row.get("primary_shot_role") == desired_role
            else 2
            if desired_role == "ending" and row.get("primary_shot_role") in {"result", "hero"}
            else 1
            if desired_role in (row.get("secondary_roles") or [])
            else 0
        )
        asset_id = str(row.get("duplicate_group_id") or row.get("asset_id") or "")
        proposed_sequence = tuple([*current_sequence, asset_id])
        prefix_reuse = sum(1 for sequence in usage.get("sequences") or [] if tuple(sequence[: len(proposed_sequence)]) == proposed_sequence)
        score = (
            12 if evidence_match else 0,
            role_fit,
            -int((usage.get("first_asset_use") or {}).get(asset_id, 0)) if sequence_no == 1 else 0,
            -int((usage.get("asset_use") or {}).get(asset_id, 0)),
            -prefix_reuse,
            2 if row["segment_key"] != previous_segment_key else 0,
            5 if row["segment_key"] not in used else 0,
            keyword_score,
            action_score,
            1 if row.get("product_visibility") == "high" else 0,
            int(row["end_ms"]) - int(row["start_ms"]),
            row["segment_key"],
        )
        ranked.append((score, row, evidence_match))
    _, candidate, matched = max(ranked, key=lambda item: item[0])
    return candidate, matched
