from __future__ import annotations

import math
import subprocess
from pathlib import Path
from typing import Any

from .database import LightTryonDB
from .diversity import assess_product_asset_capacity, evaluate_product_diversity, product_plan_usage
from .utils import normalized_list, stable_hash
from .voiceover_visual_match_core import (
    apply_key_match_policy as _apply_key_match_policy,
    candidate_supported_roles as _shared_candidate_supported_roles,
    desired_role_for_line as _shared_desired_role_for_line,
    select_voice_candidate as _shared_select_voice_candidate,
    split_voice_interval as _shared_split_voice_interval,
    voiceover_intervals as _shared_voiceover_intervals,
)


ASSEMBLY_PLAN_VERSION = "narrative-roughcut-v2-diversity"
VOICEOVER_CUT_PLAN_VERSION = "narrative-voiceover-cut-v4-key-evidence"
ROLE_SEQUENCE_BY_FOCUS = {
    "detail": ["hero", "detail", "result", "detail", "ending"],
    "color": ["hero", "result", "detail", "scene", "ending"],
    "scenario": ["hero", "scene", "result", "scene", "ending"],
    "fit": ["hero", "result", "detail", "result", "ending"],
}


def plan_variant_rough_cut(db: LightTryonDB, variant_id: str) -> dict[str, Any]:
    variant = db.get_narrative_variant(variant_id)
    if not variant:
        raise KeyError(f"找不到增强型内容变体: {variant_id}")
    strategy = db.get_content_strategy(str(variant.get("strategy_group_id") or "")) or {}
    candidates = _asset_segment_candidates(db, str(variant.get("product_id") or ""))
    if not candidates:
        raise ValueError("没有已完成实际打标且可用于混剪的轻视频素材")
    target_ms = int(variant.get("target_duration_seconds") or 22) * 1000
    longest_segment_ms = max(int(row["end_ms"]) - int(row["start_ms"]) for row in candidates)
    clip_count = max(5, min(8, math.ceil(target_ms / max(2500, longest_segment_ms))))
    durations = _balanced_durations(target_ms, clip_count)
    focus = str(strategy.get("visual_focus") or "fit")
    desired_roles = ROLE_SEQUENCE_BY_FOCUS.get(focus, ROLE_SEQUENCE_BY_FOCUS["fit"])
    usage = product_plan_usage(db, str(variant.get("product_id") or ""), exclude_variant_id=variant_id)
    selected: list[dict[str, Any]] = []
    used_segments: set[str] = set()
    used_assets: set[str] = set()
    for index, duration_ms in enumerate(durations):
        role = desired_roles[index % len(desired_roles)]
        candidate = _select_candidate(
            candidates, role, duration_ms, used_segments, used_assets,
            usage=usage, sequence_no=index + 1,
            current_sequence=[str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in selected],
        )
        if not candidate:
            raise ValueError(f"可用实际素材不足，无法为 {role} 分配 {duration_ms}ms 镜头")
        used_segments.add(candidate["segment_key"])
        used_assets.add(str(candidate.get("duplicate_group_id") or candidate["asset_id"]))
        source_start = int(candidate["start_ms"])
        max_start = max(source_start, int(candidate["end_ms"]) - duration_ms)
        offset_room = max(0, max_start - source_start)
        deterministic_offset = int(stable_hash(variant_id, index, candidate["segment_key"], length=8), 16)
        trim_start = source_start + (deterministic_offset % (offset_room + 1) if offset_room else 0)
        selected.append({
            "sequence_no": index + 1,
            "role": role,
            "asset_id": candidate["asset_id"],
            "duplicate_group_id": candidate.get("duplicate_group_id") or candidate["asset_id"],
            "segment_id": candidate.get("segment_id") or "",
            "source_file": candidate["file_path"],
            "source_start_ms": trim_start,
            "source_end_ms": trim_start + duration_ms,
            "duration_ms": duration_ms,
            "primary_shot_role": candidate.get("primary_shot_role") or "",
            "secondary_roles": candidate.get("secondary_roles") or [],
            "hook_visual_type": candidate.get("hook_visual_type") or "none",
            "product_visibility": candidate.get("product_visibility") or "medium",
            "tag_reason": candidate.get("reason") or "",
        })
    cursor = 0
    visual_slots: list[dict[str, Any]] = []
    for clip in selected:
        end = cursor + int(clip["duration_ms"])
        visual_slots.append(_visual_slot(clip, cursor, end))
        cursor = end
    plan = {
        "plan_version": ASSEMBLY_PLAN_VERSION,
        "variant_id": variant_id,
        "product_id": variant.get("product_id"),
        "target_duration_ms": target_ms,
        "visual_focus": focus,
        "capacity_assessment": assess_product_asset_capacity(
            db,
            str(variant.get("product_id") or ""),
            target_count=len(db.list_narrative_variants(str(variant.get("product_id") or ""))),
        ),
        "clips": selected,
        "visual_timeline": {
            "duration_ms": target_ms,
            "mainline_summary": _mainline_summary(selected),
            "overall_confidence": _overall_confidence(candidates, selected),
            "uncertainties": ["口播只能使用 visual_slots 中有正面证据支持的卖点"],
            "visual_slots": visual_slots,
        },
    }
    plan["plan_fingerprint"] = stable_hash(plan, length=24)
    db.update_narrative_variant(
        variant_id,
        workflow_state="visual_roughcut_planned",
        assembly_plan=plan,
        last_error="",
    )
    evaluate_product_diversity(db, str(variant.get("product_id") or ""), persist=True)
    return db.get_narrative_variant(variant_id).get("assembly_plan") or plan


def plan_variant_voiceover_cut(db: LightTryonDB, variant_id: str) -> dict[str, Any]:
    """Re-plan visuals from measured spoken-line timing after continuous TTS."""

    variant = db.get_narrative_variant(variant_id)
    if not variant:
        raise KeyError(f"找不到增强型内容变体: {variant_id}")
    strategy = db.get_content_strategy(str(variant.get("strategy_group_id") or "")) or {}
    raw_lines = sorted(
        [row for row in (variant.get("tts_timeline") or []) if str(row.get("speech_text") or "").strip()],
        key=lambda row: int(row.get("start_ms") or 0),
    )
    if not raw_lines:
        raise ValueError("口播驱动重剪必须先完成连续 TTS 和逐句时间对齐")
    lines = _apply_key_match_policy(
        raw_lines,
        beat_plan=variant.get("beat_plan") or (variant.get("voiceover_response") or {}).get("beats") or [],
        primary_selling_point=str(strategy.get("primary_selling_point") or ""),
    )
    candidates = _asset_segment_candidates(db, str(variant.get("product_id") or ""))
    if not candidates:
        raise ValueError("没有已完成实际打标且可用于口播重剪的素材")
    target_ms = int(variant.get("target_duration_seconds") or 22) * 1000
    intervals = _voiceover_intervals(lines, target_ms, candidates)
    usage = product_plan_usage(db, str(variant.get("product_id") or ""), exclude_variant_id=variant_id)
    selected: list[dict[str, Any]] = []
    evidence_gaps: list[dict[str, Any]] = []
    match_warnings: list[dict[str, Any]] = []
    used_segments: set[str] = set()
    used_assets: set[str] = set()
    previous_segment_key = ""
    for sequence_no, interval in enumerate(intervals, start=1):
        required_roles = normalized_list(interval.get("required_shot_roles"))
        critical_evidence = bool(interval.get("critical_evidence"))
        candidate, evidence_match = _select_voice_candidate(
            candidates,
            desired_role=str(interval.get("desired_role") or "result"),
            duration_ms=int(interval["duration_ms"]),
            required_roles=required_roles,
            speech_text=(
                str(interval.get("chinese_translation") or "")
                + str(interval.get("speech_text") or "")
            ),
            used=used_segments,
            used_assets=used_assets,
            previous_segment_key=previous_segment_key,
            usage=usage,
            sequence_no=sequence_no,
            current_sequence=[str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in selected],
        )
        used_reuse_fallback = False
        if not candidate:
            fallback_candidates = _non_adjacent_candidates(candidates, selected) or candidates
            candidate, evidence_match = _select_voice_candidate(
                fallback_candidates,
                desired_role=str(interval.get("desired_role") or "result"),
                duration_ms=int(interval["duration_ms"]),
                required_roles=required_roles if critical_evidence else [],
                speech_text=str(interval.get("chinese_translation") or "") + str(interval.get("speech_text") or ""),
                used=used_segments,
                used_assets=set(),
                previous_segment_key=previous_segment_key,
                usage=usage,
                sequence_no=sequence_no,
                current_sequence=[str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in selected],
            )
            used_reuse_fallback = bool(candidate)
        if not candidate:
            evidence_gaps.append({
                "beat_id": interval.get("beat_id"),
                "speech_text": interval.get("speech_text"),
                "required_shot_roles": required_roles,
                "start_ms": interval.get("start_ms"),
                "end_ms": interval.get("end_ms"),
                "match_priority": interval.get("match_priority"),
                "critical_evidence": True,
                "reason": "没有时长足够且完成实际打标的素材",
            })
            continue
        if critical_evidence and required_roles and not evidence_match:
            evidence_gaps.append({
                "beat_id": interval.get("beat_id"),
                "speech_text": interval.get("speech_text"),
                "required_shot_roles": required_roles,
                "start_ms": interval.get("start_ms"),
                "end_ms": interval.get("end_ms"),
                "match_priority": interval.get("match_priority"),
                "critical_evidence": True,
                "fallback_asset_id": candidate.get("asset_id"),
                "reason": "只有普通展示镜头，没有专属证据镜头",
            })
            continue
        original_roles = normalized_list(interval.get("original_required_shot_roles"))
        if used_reuse_fallback:
            match_warnings.append({
                "beat_id": interval.get("beat_id"),
                "reason": "material_reuse_fallback",
                "asset_id": candidate.get("asset_id"),
            })
        if not critical_evidence and original_roles and not set(original_roles).intersection(_candidate_supported_roles(candidate)):
            match_warnings.append({
                "beat_id": interval.get("beat_id"),
                "reason": "soft_evidence_unmatched",
                "original_required_shot_roles": original_roles,
                "asset_id": candidate.get("asset_id"),
            })
        used_segments.add(candidate["segment_key"])
        used_assets.add(str(candidate.get("duplicate_group_id") or candidate["asset_id"]))
        previous_segment_key = candidate["segment_key"]
        duration_ms = int(interval["duration_ms"])
        source_start = int(candidate["start_ms"])
        max_start = max(source_start, int(candidate["end_ms"]) - duration_ms)
        offset_room = max(0, max_start - source_start)
        deterministic_offset = int(
            stable_hash(variant_id, sequence_no, candidate["segment_key"], interval.get("beat_id"), length=8),
            16,
        )
        trim_start = source_start + (deterministic_offset % (offset_room + 1) if offset_room else 0)
        selected.append({
            "sequence_no": sequence_no,
            "role": interval.get("desired_role"),
            "beat_id": interval.get("beat_id"),
            "speech_text": interval.get("speech_text"),
            "timeline_start_ms": interval.get("start_ms"),
            "timeline_end_ms": interval.get("end_ms"),
            "required_shot_roles": required_roles,
            "original_required_shot_roles": original_roles,
            "match_priority": interval.get("match_priority"),
            "critical_evidence": critical_evidence,
            "evidence_match": evidence_match,
            "asset_id": candidate["asset_id"],
            "duplicate_group_id": candidate.get("duplicate_group_id") or candidate["asset_id"],
            "segment_id": candidate.get("segment_id") or "",
            "source_file": candidate["file_path"],
            "source_start_ms": trim_start,
            "source_end_ms": trim_start + duration_ms,
            "duration_ms": duration_ms,
            "primary_shot_role": candidate.get("primary_shot_role") or "",
            "secondary_roles": candidate.get("secondary_roles") or [],
            "hook_visual_type": candidate.get("hook_visual_type") or "none",
            "product_visibility": candidate.get("product_visibility") or "medium",
            "tag_reason": candidate.get("reason") or "",
        })
    existing = variant.get("assembly_plan") or {}
    plan = {
        **existing,
        "plan_version": VOICEOVER_CUT_PLAN_VERSION,
        "variant_id": variant_id,
        "product_id": variant.get("product_id"),
        "target_duration_ms": target_ms,
        "capacity_assessment": assess_product_asset_capacity(
            db,
            str(variant.get("product_id") or ""),
            target_count=len(db.list_narrative_variants(str(variant.get("product_id") or ""))),
        ),
        "clips": selected,
        "evidence_gaps": evidence_gaps,
        "match_warnings": match_warnings,
        "beat_alignment": [
            {
                "beat_id": row.get("block_id") or (row.get("beat_ids") or [""])[0],
                "speech_text": row.get("speech_text"),
                "start_ms": row.get("start_ms"),
                "end_ms": row.get("end_ms"),
                "required_shot_roles": row.get("required_shot_roles") or [],
                "original_required_shot_roles": row.get("original_required_shot_roles") or [],
                "match_priority": row.get("match_priority") or "normal",
                "critical_evidence": bool(row.get("critical_evidence")),
            }
            for row in lines
        ],
    }
    cursor = 0
    visual_slots = []
    for clip in selected:
        end = cursor + int(clip["duration_ms"])
        visual_slots.append(_visual_slot(clip, cursor, end))
        cursor = end
    plan["visual_timeline"] = {
        "duration_ms": target_ms,
        "mainline_summary": "按完整口播逐句时间重新选择并排列证据镜头",
        "overall_confidence": _overall_confidence(candidates, selected),
        "uncertainties": [gap["reason"] for gap in evidence_gaps],
        "visual_slots": visual_slots,
    }
    # Key evidence gaps block only this variant. They no longer create supplement jobs.
    plan["supplement_requirements"] = []
    plan["plan_fingerprint"] = stable_hash(plan, length=24)
    state = "voiceover_key_evidence_missing" if evidence_gaps else "voiceover_cut_planned"
    db.update_narrative_variant(
        variant_id,
        workflow_state=state,
        assembly_plan=plan,
        last_error="" if not evidence_gaps else f"VOICEOVER_KEY_EVIDENCE_MISSING:{len(evidence_gaps)}",
    )
    evaluate_product_diversity(db, str(variant.get("product_id") or ""), persist=True)
    return db.get_narrative_variant(variant_id).get("assembly_plan") or plan


def _voiceover_intervals(
    lines: list[dict[str, Any]], target_ms: int, candidates: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    return _shared_voiceover_intervals(lines, target_ms, candidates, role_resolver=_desired_role_for_line)


def _split_voice_interval(
    start_ms: int,
    end_ms: int,
    max_duration_ms: int,
    **payload: Any,
) -> list[dict[str, Any]]:
    return _shared_split_voice_interval(start_ms, end_ms, max_duration_ms, **payload)


def _desired_role_for_line(line: dict[str, Any], index: int) -> str:
    return _shared_desired_role_for_line(line, index)


def _select_voice_candidate(
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
    return _shared_select_voice_candidate(
        candidates,
        desired_role=desired_role,
        duration_ms=duration_ms,
        required_roles=required_roles,
        speech_text=speech_text,
        used=used,
        used_assets=used_assets,
        previous_segment_key=previous_segment_key,
        usage=usage,
        sequence_no=sequence_no,
        current_sequence=current_sequence,
    )


def _candidate_supported_roles(candidate: dict[str, Any]) -> set[str]:
    return _shared_candidate_supported_roles(candidate)


def _non_adjacent_candidates(candidates: list[dict[str, Any]], selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not selected:
        return candidates
    previous = selected[-1]
    previous_asset = str(previous.get("asset_id") or "")
    previous_group = str(previous.get("duplicate_group_id") or previous_asset)
    return [
        row
        for row in candidates
        if str(row.get("asset_id") or "") != previous_asset
        and str(row.get("duplicate_group_id") or row.get("asset_id") or "") != previous_group
    ]


def render_variant_rough_cut(
    db: LightTryonDB,
    variant_id: str,
    output_path: str | Path,
    *,
    ffmpeg_bin: str = "ffmpeg",
) -> dict[str, Any]:
    variant = db.get_narrative_variant(variant_id)
    if not variant:
        raise KeyError(f"找不到增强型内容变体: {variant_id}")
    plan = variant.get("assembly_plan") or {}
    if not plan.get("clips"):
        plan = plan_variant_rough_cut(db, variant_id)
    if plan.get("evidence_gaps"):
        raise ValueError(
            f"口播重剪仍有 {len(plan['evidence_gaps'])} 个关键证据镜头缺口；当前版本不会自动补素材"
        )
    planned_duration = sum(int(row.get("duration_ms") or 0) for row in plan.get("clips") or [])
    if planned_duration != int(plan.get("target_duration_ms") or 0):
        raise ValueError(
            f"镜头计划总时长不等于目标时长: {planned_duration}!={plan.get('target_duration_ms')}"
        )
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    command: list[str] = [ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error"]
    clips = list(plan["clips"])
    for clip in clips:
        command.extend(["-i", str(clip["source_file"])])
    filters: list[str] = []
    labels: list[str] = []
    for index, clip in enumerate(clips):
        start = float(clip["source_start_ms"]) / 1000
        end = float(clip["source_end_ms"]) / 1000
        label = f"v{index}"
        labels.append(f"[{label}]")
        filters.append(
            f"[{index}:v]trim=start={start:.3f}:end={end:.3f},setpts=PTS-STARTPTS,"
            "scale=1080:1920:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,fps=30,setsar=1"
            f"[{label}]"
        )
    target_seconds = float(plan.get("target_duration_ms") or 0) / 1000
    filters.append(
        "".join(labels)
        + f"concat=n={len(clips)}:v=1:a=0,"
        + f"tpad=stop_mode=clone:stop_duration=1,trim=duration={target_seconds:.3f},setpts=PTS-STARTPTS[outv]"
    )
    command.extend([
        "-filter_complex", ";".join(filters),
        "-map", "[outv]",
        "-an",
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "19",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        str(output),
    ])
    completed = subprocess.run(command, capture_output=True, text=True, timeout=1800, check=False)
    if completed.returncode != 0 or not output.is_file() or output.stat().st_size <= 0:
        raise RuntimeError(f"增强型视觉粗剪失败: {completed.stderr[-2000:]}")
    updated_plan = {
        **plan,
        "roughcut_path": str(output),
        "source_object_ref": str(output),
        "render_status": "success",
    }
    timeline = {**(updated_plan.get("visual_timeline") or {}), "source_object_ref": str(output)}
    updated_plan["visual_timeline"] = timeline
    db.update_narrative_variant(
        variant_id,
        workflow_state="waiting_voiceover",
        assembly_plan=updated_plan,
        last_error="",
    )
    return {
        "variant_id": variant_id,
        "output_path": str(output),
        "file_size": output.stat().st_size,
        "assembly_plan": updated_plan,
        "visual_timeline": timeline,
    }


def _asset_segment_candidates(db: LightTryonDB, product_id: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for asset in db.list_media_assets(product_id):
        if asset.get("tag_status") != "completed" or asset.get("asset_status") != "ready":
            continue
        path = Path(str(asset.get("file_path") or ""))
        if not path.is_file():
            continue
        observed = asset.get("observed_tags") if isinstance(asset.get("observed_tags"), dict) else {}
        qc = asset.get("qc_result") if isinstance(asset.get("qc_result"), dict) else {}
        duplicate_group_id = str(qc.get("duplicate_group_id") or asset.get("asset_id") or "")
        for index, segment in enumerate(observed.get("segments") or []):
            if not isinstance(segment, dict):
                continue
            start_ms = int(segment.get("start_ms") or 0)
            end_ms = int(segment.get("end_ms") or 0)
            if end_ms - start_ms < 2500 or segment.get("mixcut_usability") == "no":
                continue
            output.append({
                "segment_key": str(segment.get("segment_id") or f"{asset['asset_id']}:{index}"),
                "segment_id": str(segment.get("segment_id") or ""),
                "asset_id": asset["asset_id"],
                "duplicate_group_id": duplicate_group_id,
                "file_path": str(path.resolve()),
                "start_ms": start_ms,
                "end_ms": end_ms,
                "primary_shot_role": str(segment.get("primary_shot_role") or ""),
                "secondary_roles": normalized_list(segment.get("secondary_roles")),
                "hook_visual_type": str(segment.get("hook_visual_type") or "none"),
                "product_visibility": str(segment.get("product_visibility") or "medium"),
                "confidence": segment.get("confidence"),
                "reason": str(segment.get("reason") or ""),
            })
    return output


def _balanced_durations(target_ms: int, count: int) -> list[int]:
    base = target_ms // count
    durations = [base] * count
    for index in range(target_ms - base * count):
        durations[index % count] += 1
    return durations


def _select_candidate(
    candidates: list[dict[str, Any]],
    desired_role: str,
    duration_ms: int,
    used: set[str],
    used_assets: set[str],
    *,
    usage: dict[str, Any],
    sequence_no: int,
    current_sequence: list[str],
) -> dict[str, Any] | None:
    eligible = [
        row for row in candidates
        if row["segment_key"] not in used
        and str(row.get("duplicate_group_id") or row.get("asset_id") or "") not in used_assets
        and int(row["end_ms"]) - int(row["start_ms"]) >= duration_ms
    ]
    if not eligible:
        return None
    def score(row: dict[str, Any]) -> tuple[Any, ...]:
        asset_id = str(row.get("duplicate_group_id") or row.get("asset_id") or "")
        proposed_sequence = tuple([*current_sequence, asset_id])
        prefix_reuse = sum(
            1 for sequence in usage.get("sequences") or []
            if tuple(sequence[: len(proposed_sequence)]) == proposed_sequence
        )
        return (
            3 if row.get("primary_shot_role") == desired_role else 0,
            1 if desired_role in (row.get("secondary_roles") or []) else 0,
            -int((usage.get("first_asset_use") or {}).get(asset_id, 0)) if sequence_no == 1 else 0,
            -int((usage.get("asset_use") or {}).get(asset_id, 0)),
            -prefix_reuse,
            1 if row.get("product_visibility") == "high" else 0,
            int(row["end_ms"]) - int(row["start_ms"]),
            row["segment_key"],
        )

    return max(eligible, key=score)


def _visual_slot(clip: dict[str, Any], start_ms: int, end_ms: int) -> dict[str, Any]:
    primary = str(clip.get("primary_shot_role") or clip.get("role") or "scene")
    descriptions = {
        "hero": "商品主体清楚的整体上身展示",
        "detail": "商品局部结构或表面细节展示",
        "result": "人物穿着后的整体效果展示",
        "scene": "商品处于真实日常穿搭场景",
        "ending": "人物与商品稳定停留并自然收尾",
    }
    speakable = {
        "hero": ["商品类型", "整体版型"],
        "detail": ["画面中清楚可见的商品局部细节"],
        "result": ["实际上身效果"],
        "scene": ["实际出现的穿搭场景"],
        "ending": ["完整穿搭效果"],
    }
    reason = str(clip.get("tag_reason") or "").strip()
    return {
        "slot_id": f"LTVS_{int(clip['sequence_no']):02d}",
        "start_ms": start_ms,
        "end_ms": end_ms,
        "visual_event": reason or descriptions.get(primary, descriptions["scene"]),
        "observations": [descriptions.get(primary, descriptions["scene"])],
        "event_tags": [
            primary,
            *normalized_list(clip.get("secondary_roles")),
            str(clip.get("hook_visual_type") or "none"),
        ],
        "speakable_facts": speakable.get(primary, []),
        "recommended_line_function": "hook" if int(clip["sequence_no"]) == 1 else ("result_and_cta" if primary == "ending" else "proof"),
        "product_visibility": str(clip.get("product_visibility") or "medium"),
        "confidence": 0.86 if clip.get("product_visibility") == "high" else 0.75,
        "source_frame_indexes": [],
        "source_asset_id": clip.get("asset_id"),
        "source_segment_id": clip.get("segment_id"),
    }


def _mainline_summary(clips: list[dict[str, Any]]) -> str:
    roles = [str(clip.get("role") or "") for clip in clips]
    return "轻量试穿素材按“" + " → ".join(roles) + "”组合，商品始终为同一 SKU"


def _overall_confidence(candidates: list[dict[str, Any]], selected: list[dict[str, Any]]) -> float:
    selected_ids = {str(row.get("segment_id") or "") for row in selected}
    rows = [row for row in candidates if str(row.get("segment_id") or "") in selected_ids]
    if not rows:
        return 0.75
    score_map = {"high": 0.92, "medium": 0.78, "low": 0.6}
    return round(sum(score_map.get(str(row.get("confidence") or "medium"), 0.75) for row in rows) / len(rows), 3)
