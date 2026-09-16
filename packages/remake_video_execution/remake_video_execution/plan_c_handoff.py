"""Compile a frozen remake script into a Plan C execution contract.

This module is the single seam between the remake compiler and the shared Plan C
media executor.  It deliberately does **not** import the original planning
package: a remake script is already frozen human copy, so it must never be sent
through ``validate_master_contract`` or ``compile_longform_plan`` (no selling
points, no capture units, no unique-hook authoring, no voiceover rewriting).

From the moment a Plan C job exists, both original and remake rows share the
same reference freezing, K0/entry frames, H3 submission, remote resume, TTS or
silent track, merge, final validation and Feishu write-back.

Only the two source differences survive here:

* the frozen remake prompt is translated verbatim into Plan C segment prompts;
* the audio contract is inherited from the source script, never rewritten.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Mapping

from .audio import build_frozen_audio_plan
from .contracts import ExecutionPlan, GenerationSegment, Issue, SegmentPlan, SourceSnapshot
from .coverage import validate_coverage
from .parser import parse_source
from .planner import plan_segments_for_plan_c
from .source import freeze_record
from .subtitles import build_subtitle_plan


SCHEMA_VERSION = "remake-plan-c-handoff-v1"
SOURCE_KIND_REMAKE = "REMAKE_SEGMENTED"
SEGMENT_IDS = ("A", "B", "C")

# Plan C's own keyframe/scene code is most reliable with A/B/C and with the
# 4-15s integer H3 window.  Anything else must block before paid submission.
MIN_SEGMENT_SECONDS = 4
MAX_SEGMENT_SECONDS = 15

# Explicit on-screen copy is removed from the visual prompt wherever it appears
# (its own line or inline after a sentence) and reproduced in post production.
VISIBLE_TEXT_INLINE = re.compile(r"(?:屏幕文字|字幕|评论气泡)\s*[：:][^\n]*")
# A screen-text line is removed *together with* one of its newlines, otherwise
# deleting it would leave an orphan blank line in the middle of a shot block.
VISIBLE_TEXT_LINE = re.compile(r"(?m)^[ \t]*(?:屏幕文字|字幕|评论气泡)\s*[：:][^\n]*(?:\n|$)")


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(_text(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or "").strip()
    return str(value).strip()


def _issue(code: str, message: str, stage: str = "HANDOFF") -> Issue:
    return Issue(code, "BLOCK", message, affected_stage=stage)


def strip_visible_text_lines(prompt: str) -> str:
    """Remove explicit screen-text lines from the visual prompt.

    A frozen remake may name on-screen copy.  That copy is reproduced in post
    production from the subtitle contract; the video model must never draw it.
    """

    cleaned = VISIBLE_TEXT_LINE.sub("", str(prompt or ""))
    cleaned = VISIBLE_TEXT_INLINE.sub("", cleaned)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _segment_id(ordinal: int) -> str:
    return SEGMENT_IDS[ordinal - 1] if 0 < ordinal <= len(SEGMENT_IDS) else f"SEG_{ordinal:02d}"


def _shot_dict(shot: Any) -> Dict[str, Any]:
    if hasattr(shot, "to_dict"):
        return dict(shot.to_dict())
    return dict(shot or {})


def _shot_visual_text(shot: Mapping[str, Any]) -> str:
    body = strip_visible_text_lines(str(shot.get("body") or ""))
    lines = [line.strip(" ·•-\t") for line in body.splitlines() if line.strip()]
    return "；".join(lines[:2])


def _plan_c_job_id(record_id: str, source_revision_hash: str) -> str:
    material = json.dumps(
        {"record_id": str(record_id or ""), "source_revision_hash": str(source_revision_hash or "")},
        ensure_ascii=False, sort_keys=True,
    )
    return "LFR_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:20].upper()


def _frozen_reference_manifest(frozen_assets: Mapping[str, Any] | None) -> Dict[str, Any]:
    manifest = dict(frozen_assets or {})
    if "assets" in manifest or "reference_mode" in manifest:
        return manifest
    return {"schema_version": "longform-frozen-reference-assets-v3-persona-pack",
            "status": "UNAVAILABLE", "assets": []}


def _product_authority_present(manifest: Mapping[str, Any]) -> bool:
    roles = {str(item.get("role") or "") for item in manifest.get("assets") or []
             if isinstance(item, Mapping)}
    return bool(roles & {"PRODUCT_REFERENCE", "COMPOSITE_FIRST_FRAME"})


# --------------------------------------------------------------------------- #
# Audio contract
# --------------------------------------------------------------------------- #

def _semantic_sections(
    execution: ExecutionPlan, segments: SegmentPlan, audio_plan: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    """Group per-shot spoken lines into one semantic section per Plan C segment.

    A whole-paragraph source voiceover intentionally produces no sections, which
    keeps the shared executor on its single continuous TTS layout.
    """

    timed = [item for item in audio_plan.get("timed_lines") or [] if isinstance(item, Mapping)]
    if not timed:
        return []
    sections: List[Dict[str, Any]] = []
    for ordinal, segment in enumerate(segments.segments, 1):
        lines = [
            _text(item.get("text"))
            for item in timed
            if int(item.get("end_ms") or 0) > segment.global_start_ms
            and int(item.get("start_ms") or 0) < segment.global_end_ms
        ]
        section_text = " ".join(text for text in lines if text).strip()
        sections.append({
            "segment_id": _segment_id(ordinal),
            "duration_seconds": segment.requested_duration_seconds,
            "target_text": section_text,
        })
    if not all(section["target_text"] for section in sections):
        # Incomplete per-segment copy would silently drop speech; a whole
        # paragraph read is safer and is explicitly allowed by the plan.
        return []
    return sections


def _voiceover_contract(
    execution: ExecutionPlan, segments: SegmentPlan, audio_plan: Mapping[str, Any]
) -> Dict[str, Any]:
    mode = str(audio_plan.get("mode") or "UNSPECIFIED").upper()
    target_text = _text(audio_plan.get("target_text"))
    contract: Dict[str, Any] = {
        "schema_version": "remake-plan-c-voiceover-v1",
        "mode": mode,
        "rewrite_allowed": False,
        "revision_allowed": False,
        "source_mode": "REMAKE_FROZEN_SOURCE",
        "target_language": execution.source.target_language,
        "target_text": target_text,
        "target_text_sha256": (str(audio_plan.get("target_text_sha256") or "")
                               or (hashlib.sha256(target_text.encode("utf-8")).hexdigest()
                                   if target_text else "")),
        "timed_lines": list(audio_plan.get("timed_lines") or []),
        "semantic_sections": [],
        "bgm_policy": str(audio_plan.get("bgm_policy") or "FOLLOW_SOURCE_REQUIREMENTS"),
    }
    if mode == "PRESERVE_SOURCE_COPY":
        contract["semantic_sections"] = _semantic_sections(execution, segments, audio_plan)
    return contract


# --------------------------------------------------------------------------- #
# Plan C segments
# --------------------------------------------------------------------------- #

def _boundary_mode(incoming_boundary: str) -> str:
    return "CONTINUOUS" if str(incoming_boundary).upper() == "CONTINUOUS" else "DISCONTINUOUS_CUT"


def _bridge_state(shot: Mapping[str, Any]) -> Dict[str, Any]:
    """A minimal, honest continuation state for the frozen last shot.

    The remake source does not own the original production world, so the state
    is described only from the frozen copy instead of inventing person, outfit
    or lighting facts.
    """

    tail = _shot_visual_text(shot)
    return {
        "person_state": tail,
        "product_wear_state": tail,
        "pose_or_action_state": tail,
        "camera_state": tail,
        "scene_state": tail,
        "outfit_state": tail,
        "lighting_state": tail,
    }


def _plan_c_segment(
    execution: ExecutionPlan, segment: GenerationSegment, ordinal: int,
    *, incoming_mode: str, outgoing_mode: str,
) -> Dict[str, Any]:
    segment_id = _segment_id(ordinal)
    shots = [
        _shot_dict(shot) for shot in execution.shots
        if shot.end_ms > segment.global_start_ms and shot.start_ms < segment.global_end_ms
    ]
    units = [
        {
            "unit_id": f"{segment_id}{index:02d}",
            "beat": "ACTION",
            "visual_role": "FROZEN_SOURCE_SHOT",
            "shot_and_camera": "",
            "visible_action": _shot_visual_text(shot),
            "visual_result": _shot_visual_text(shot),
            "product_anchors": [],
            "edit_before": "START_FRAME" if index == 1 else "DIRECT_CUT",
        }
        for index, shot in enumerate(shots, 1)
    ]
    if ordinal == 1:
        start_keyframe = "K0"
        start_source = "MASTER_OPENING"
        entry_frame_role = "MASTER_OPENING"
    elif incoming_mode == "CONTINUOUS":
        start_keyframe = f"K{ordinal - 1}_ACTUAL"
        start_source = "PREVIOUS_ACTUAL_TAIL"
        entry_frame_role = "ACTUAL_TAIL"
    else:
        start_keyframe = f"S{segment_id}_ENTRY"
        start_source = "SCENE_ENTRY_GENERATED"
        entry_frame_role = "SETUP_ENTRY"
    return {
        "segment_id": segment_id,
        "scene_id": "SCENE_1",
        "scene_block": {"scene_id": "SCENE_1", "location": "", "source": "FROZEN_REMAKE_COPY"},
        "duration_seconds": int(segment.requested_duration_seconds),
        "segment_visual_role": "FROZEN_REMAKE_SOURCE",
        "generation_mode": "first_last" if outgoing_mode == "CONTINUOUS" else "first_frame",
        "start_keyframe": start_keyframe,
        "frame_contract": {
            "start_source": start_source,
            "end_source": "PLANNED_SAME_SCENE" if outgoing_mode == "CONTINUOUS" else "NONE",
            "incoming_boundary_mode": "MASTER_OPENING" if ordinal == 1 else incoming_mode,
            "outgoing_boundary_mode": outgoing_mode,
            "entry_frame_role": entry_frame_role,
        },
        "capture_units": units,
        "execution_units": units,
        "source_timeline": {
            "global_start_ms": segment.global_start_ms,
            "global_end_ms": segment.global_end_ms,
            "source_shot_slices": [dict(item) for item in segment.source_shot_slices],
            "source_segment_id": segment.segment_id,
        },
        "video_prompt": strip_visible_text_lines(segment.prompt),
    }


def _bridge_contract(
    execution: ExecutionPlan, plan_c_segments: List[Dict[str, Any]],
    remake_segments: List[GenerationSegment],
) -> Dict[str, Any]:
    boundaries: List[Dict[str, Any]] = []
    for index in range(1, len(plan_c_segments)):
        incoming = remake_segments[index]
        mode = _boundary_mode(incoming.incoming_boundary)
        previous = remake_segments[index - 1]
        tail_shot = next(
            (_shot_dict(shot) for shot in reversed(execution.shots)
             if shot.end_ms <= previous.global_end_ms),
            _shot_dict(execution.shots[-1]) if execution.shots else {},
        )
        boundaries.append({
            "boundary_id": f"K{index}",
            "from_segment": plan_c_segments[index - 1]["segment_id"],
            "to_segment": plan_c_segments[index]["segment_id"],
            "boundary_mode": mode,
            "entry_frame_role": "ACTUAL_TAIL" if mode == "CONTINUOUS" else "SETUP_ENTRY",
            "planned_state": _bridge_state(tail_shot) if mode == "CONTINUOUS" else {},
            "tail_extraction_required": mode == "CONTINUOUS",
        })
    return {
        "schema_version": "longform-bridge-v4-boundary-aware",
        "authority": "BOUNDARY_MODE_OWNS_FRAME_LINEAGE",
        "boundaries": boundaries,
        "actual_bridge_frame_required": any(
            item["boundary_mode"] == "CONTINUOUS" for item in boundaries
        ),
        "selection_window_seconds": 0.8,
    }


# --------------------------------------------------------------------------- #
# Keyframe package
# --------------------------------------------------------------------------- #

def build_remake_keyframe_package(
    plan_c_segments: List[Dict[str, Any]], plan: Mapping[str, Any],
    frozen_manifest: Mapping[str, Any],
) -> Dict[str, Any]:
    """Build a Plan C compatible keyframe package without the original master."""

    boundaries = list(dict(plan.get("bridge_contract") or {}).get("boundaries") or [])
    first_unit = (plan_c_segments[0].get("execution_units") or [{}])[0]
    package: Dict[str, Any] = {
        "schema_version": "longform-keyframe-package-v8-shared-visibility",
        "frozen_reference_assets": dict(frozen_manifest),
        "segment_frames": {},
        "K0": {
            "role": "MASTER_FIRST_FRAME",
            "source": "SYSTEM_GENERATED",
            "prompt": _frame_prompt(
                "复刻长视频的开场画面",
                str(first_unit.get("visual_result") or ""),
            ),
            "reference_policy": {
                "primary": "PRODUCT_AND_APPROVED_PERSONA_REFERENCES",
                "composite_purpose": "COMPOSITION_ONLY",
            },
            "failure_policy": "CONTINUE_WITH_GENERATED_RESULT",
            "human_confirmation_required": False,
        },
    }
    package["segment_frames"][plan_c_segments[0]["segment_id"]] = {
        "start": {"key": "K0", "source": "MASTER_OPENING"},
        "end": {"key": "", "source": "NONE"},
    }
    for index, boundary in enumerate(boundaries, 1):
        from_id = str(boundary["from_segment"])
        to_id = str(boundary["to_segment"])
        target = plan_c_segments[index]
        unit = (target.get("execution_units") or [{}])[0]
        if str(boundary.get("boundary_mode") or "CONTINUOUS") == "DISCONTINUOUS_CUT":
            entry_key = f"S{to_id}_ENTRY"
            package[entry_key] = {
                "role": "SETUP_ENTRY",
                "source": "SYSTEM_GENERATED",
                "prompt": _frame_prompt(
                    f"复刻长视频片段{to_id}的切镜进入画面",
                    str(unit.get("visual_result") or ""),
                ),
                "generation_strategy": "NEW_SETUP_WITH_FROZEN_PERSON_PRODUCT_OUTFIT",
                "reference_policy": {
                    "primary": "PRODUCT_AND_APPROVED_PERSONA_REFERENCES",
                    "continuity_locks": ["SAME_PERSONA", "SAME_PRODUCT"],
                    "scene_authority": "FROZEN_REMAKE_SAME_SCENE_NEW_CAMERA_RELATION",
                },
                "failure_policy": "CONTINUE_WITH_GENERATED_RESULT",
                "human_confirmation_required": False,
            }
            package["segment_frames"][from_id]["end"] = {"key": "", "source": "NONE"}
            package["segment_frames"][to_id] = {
                "start": {"key": entry_key, "source": "SCENE_ENTRY_GENERATED"},
                "end": {"key": "", "source": "NONE"},
            }
            continue
        previous_unit = (plan_c_segments[index - 1].get("execution_units") or [{}])[-1]
        planned_key = f"K{index}_PLANNED"
        actual_key = f"K{index}_ACTUAL"
        package[planned_key] = {
            "role": "SEGMENT_BRIDGE_REFERENCE",
            "source": "SYSTEM_GENERATED",
            "prompt": _frame_prompt(
                f"续接片段{from_id}尾帧的桥接画面",
                str(previous_unit.get("visual_result") or ""),
                continuity=True,
            ),
            "generation_strategy": f"CONTINUATION_EDIT_FROM_{from_id}",
            "reference_policy": {
                "primary": f"GENERATED_{from_id}_TAIL",
                "secondary": "ORIGINAL_PRODUCT_REFERENCES",
                "secondary_purpose": "CORRECT_SAME_PRODUCT_ONLY",
            },
            "failure_policy": "CONTINUE_WITH_GENERATED_RESULT",
            "human_confirmation_required": False,
        }
        package[actual_key] = {
            "role": f"SEGMENT_{to_id}_FIRST_FRAME",
            "source": f"EXTRACT_FROM_SEGMENT_{from_id}_LAST_0_8S",
            "required_before_next_submit": True,
            "selection_policy": "CLARITY_EXPOSURE_LOW_MOTION_WITH_SOFT_PRODUCT_READABILITY",
            "failure_policy": "CONTINUE_WITH_BEST_AVAILABLE_FRAME",
            "human_confirmation_required": False,
        }
        package["segment_frames"][from_id]["end"] = {"key": planned_key, "source": "PLANNED_SAME_SCENE"}
        package["segment_frames"].setdefault(to_id, {
            "start": {"key": actual_key, "source": "PREVIOUS_ACTUAL_TAIL"},
            "end": {"key": "", "source": "NONE"},
        })
    package["keyframe_package_id"] = "LFK_" + hashlib.sha256(
        json.dumps(package, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:20].upper()
    return package


def _frame_prompt(role_text: str, action_text: str, *, continuity: bool = False) -> str:
    lines = [
        f"生成{role_text}，9:16竖屏真实手机质感。",
        "商品参考图是商品外观与结构的唯一权威；人物参考图只控制同一人物身份，不复制其衣服、姿势和背景。",
        "已有统一首帧只能作为构图参考，不得覆盖商品原图权威。",
    ]
    if continuity:
        lines.append("这不是重新开场；以已生成的上一段尾帧在同一生活时刻继续，延续同一人物、商品与穿搭。")
    if action_text:
        lines.append(f"本画面只执行冻结原稿的首个画面动作：{action_text}")
    lines.append("画面中不出现口播、字幕、屏幕文字或CTA文字；观众可见文字统一由后期生成。")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def build_plan_c_handoff(
    *,
    record_id: str,
    fields: Mapping[str, Any],
    frozen_assets: Mapping[str, Any] | None,
    structured_source: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Translate one frozen remake row into a Plan C execution contract.

    Returns a payload with ``blocked=True`` when paid submission must not start.
    No original planning function is called: a remake script is never rewritten,
    re-planned or expanded with new selling points.
    """

    source: SourceSnapshot = freeze_record(record_id, dict(fields), dict(structured_source or {}))
    execution = parse_source(source)
    segment_plan = plan_segments_for_plan_c(execution)
    audio_plan, audio_issues = build_frozen_audio_plan(execution)
    subtitle_plan = build_subtitle_plan(execution)

    issues: List[Issue] = list(execution.issues)
    issues.extend(validate_coverage(execution, segment_plan))
    issues.extend(audio_issues)

    manifest = _frozen_reference_manifest(frozen_assets)
    if not _product_authority_present(manifest):
        issues.append(_issue(
            "FROZEN_PRODUCT_REFERENCE_MISSING",
            "复刻任务缺少已冻结的商品权威参考图（PRODUCT_REFERENCE/COMPOSITE_FIRST_FRAME）",
            "REFERENCES",
        ))

    remake_segments = list(segment_plan.segments)
    segment_count_supported = 2 <= len(remake_segments) <= 3
    if not segment_count_supported:
        issues.append(_issue(
            "REMAKE_SEGMENT_COUNT_UNSUPPORTED",
            f"复刻稿 {execution.duration_ms / 1000:g}s 拆成 {len(remake_segments)} 段，"
            "Plan C 媒体执行器只支持 2 至 3 段（约16-45秒）",
            "PLAN",
        ))
    for index, segment in enumerate(remake_segments, 1):
        if not MIN_SEGMENT_SECONDS <= segment.requested_duration_seconds <= MAX_SEGMENT_SECONDS:
            issues.append(_issue(
                "REMAKE_SEGMENT_DURATION_UNSUPPORTED",
                f"复刻片段{_segment_id(index)}时长{segment.requested_duration_seconds}s，"
                f"必须落在 {MIN_SEGMENT_SECONDS}-{MAX_SEGMENT_SECONDS} 秒的 H3 整数窗口内",
                "PLAN",
            ))

    voiceover = _voiceover_contract(execution, segment_plan, audio_plan)
    audio_mode = str(voiceover.get("mode") or "").upper()
    if audio_mode in {"UNSPECIFIED", "DIALOGUE_REQUIRES_PROVIDER"}:
        issues.append(_issue(
            "AUDIO_MODE_UNRESOLVED",
            "复刻稿没有明确声音模式或要求多人对白/对口型；禁止猜测为无口播后直接生成",
            "AUDIO",
        ))

    plan_c_segments: List[Dict[str, Any]] = []
    if segment_count_supported:
        for ordinal, segment in enumerate(remake_segments, 1):
            incoming = "MASTER_OPENING" if ordinal == 1 else _boundary_mode(segment.incoming_boundary)
            outgoing = "NONE"
            if ordinal < len(remake_segments):
                outgoing = _boundary_mode(remake_segments[ordinal].incoming_boundary)
            plan_c_segments.append(_plan_c_segment(
                execution, segment, ordinal,
                incoming_mode=incoming, outgoing_mode=outgoing,
            ))

    bridge_contract = _bridge_contract(execution, plan_c_segments, remake_segments)
    total_seconds = int(round(execution.duration_ms / 1000)) or sum(
        item["duration_seconds"] for item in plan_c_segments
    )
    plan: Dict[str, Any] = {
        "schema_version": "longform-original-plan-v7-shared-visibility",
        "source_kind": SOURCE_KIND_REMAKE,
        "source_revision_hash": source.source_revision_hash,
        "product_code": source.product_id or source.script_id,
        "target_duration_seconds": total_seconds,
        "target_country": source.target_country,
        "target_language": source.target_language,
        "scene_mode": "single",
        "scene_blocks": [{"scene_id": "SCENE_1", "location": "", "source": "FROZEN_REMAKE_COPY"}],
        "bridge_contract": bridge_contract,
        "segments": plan_c_segments,
        "audio_contract": {
            "schema_version": "remake-plan-c-audio-contract-v1",
            "mode": audio_mode,
            "rewrite_allowed": False,
            "target_text_sha256": voiceover.get("target_text_sha256") or "",
        },
        "postprocess_contract": {
            "subtitles": {
                "mode": "PRESERVE_SOURCE_TIMELINE" if subtitle_plan.get("cues") else "NONE",
                "items": list(subtitle_plan.get("cues") or []),
            },
        },
        "merge_contract": {
            "transition": "HARD_CUT",
            "strip_source_audio": True,
            "normalize_before_concat": True,
            "semantic_section_tts_after_concat": bool(voiceover.get("semantic_sections")),
            "silent_track_required": audio_mode == "NO_VOICEOVER",
            "subtitles": bool(subtitle_plan.get("cues")),
        },
    }
    plan["plan_id"] = "LFP_" + hashlib.sha256(
        json.dumps({"source": source.source_revision_hash, "segments": [
            {"id": item["segment_id"], "d": item["duration_seconds"]} for item in plan_c_segments
        ]}, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:20].upper()

    keyframe_package = (
        build_remake_keyframe_package(plan_c_segments, plan, manifest)
        if plan_c_segments
        else {
            "schema_version": "longform-keyframe-package-v8-shared-visibility",
            "frozen_reference_assets": dict(manifest),
            "segment_frames": {},
            "K0": {"role": "MASTER_FIRST_FRAME", "prompt": "", "source": "BLOCKED"},
        }
    )

    blocked_issues = [issue for issue in issues if issue.severity == "BLOCK"]
    return {
        "schema_version": SCHEMA_VERSION,
        "job_id": _plan_c_job_id(record_id, source.source_revision_hash),
        "source_kind": SOURCE_KIND_REMAKE,
        "source_record_id": record_id,
        "source_script_id": source.script_id,
        "source_revision_hash": source.source_revision_hash,
        "product_code": plan["product_code"],
        "target_country": source.target_country,
        "target_language": source.target_language,
        "audio_mode": audio_mode,
        "blocked": bool(blocked_issues),
        "issues": [issue.__dict__ for issue in issues],
        "blocking_codes": [issue.code for issue in blocked_issues],
        "master_contract": {
            "product_code": plan["product_code"],
            "source_kind": SOURCE_KIND_REMAKE,
            "source_record_id": record_id,
            "target_country": source.target_country,
            "target_language": source.target_language,
            "target_duration_seconds": total_seconds,
            "workbench_request": {"source_kind": "REMAKE_FROZEN_COPY"},
        },
        "plan": plan,
        "keyframe_package": keyframe_package,
        "voiceover": voiceover,
        "subtitle_plan": subtitle_plan,
        "execution_plan": execution.to_dict(),
        "segment_plan": segment_plan.to_dict(),
    }
