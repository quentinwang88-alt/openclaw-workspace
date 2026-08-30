from __future__ import annotations

import json
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, Mapping

from .contracts import VOICEOVER_SCHEMA_VERSION, text
from .audio import synthesize_segment_preflight, voiceover_text_hash


DEFAULT_MODEL_COMMAND = (
    "python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py"
)


def _closure_language_contract(master: Mapping[str, Any]) -> Dict[str, Any]:
    identity = dict(master.get("product_identity_lock") or {})
    visible = dict(identity.get("visible_closure_contract") or {})
    evidence = json.dumps(identity, ensure_ascii=False).lower()
    status = text(visible.get("status")).upper()
    if status == "AVAILABLE" and any(token in evidence for token in ("拉链", "zipper", "zip_closure")):
        mode = "ZIPPER_VERIFIED"
        guidance = "仅可使用拉开/拉上拉链等拉链动作词。"
    elif status == "AVAILABLE" and any(token in evidence for token in ("按扣", "snap")):
        mode = "SNAP_VERIFIED"
        guidance = "仅可使用按上/解开按扣等按扣动作词。"
    elif status == "AVAILABLE" and any(
        token in evidence for token in ("纽扣", "圆扣", "扣子", "button")
    ):
        mode = "BUTTON_VERIFIED"
        guidance = "仅可使用扣上/解开纽扣等纽扣动作词。"
    else:
        mode = "NEUTRAL_CLOSURE_LANGUAGE"
        guidance = "闭合方式未核实，只能中性表达合上、敞开或调整前襟，不点名拉链、按扣或纽扣。"
    return {
        "schema_version": "longform-closure-language-v1",
        "mode": mode,
        "visible_closure_contract": visible or {"status": "UNAVAILABLE"},
        "guidance_zh": guidance,
        "hard_blocking": False,
    }


def _spoken_target_range(duration_seconds: Any) -> list[float]:
    """Leave enough tail room for the fixed 350ms segment opening delay."""

    duration = float(duration_seconds or 0)
    minimum = round(duration * 0.90, 1)
    precision = 2 if duration <= 11 else 1
    maximum = round(
        min(duration * 0.96, max(0.0, duration - 0.45)), precision
    )
    return [minimum, maximum]


def build_longform_voiceover_payload(master: Mapping[str, Any], plan: Mapping[str, Any]) -> Dict[str, Any]:
    semantic = dict(master.get("semantic_spine") or {})
    argument_bundle = dict(master.get("longform_argument_bundle") or {})
    primary_argument = dict(argument_bundle.get("primary_argument") or semantic.get("selling_argument") or {})
    supporting_arguments = list(argument_bundle.get("supporting_arguments") or [])
    return {
        "schema_version": "longform-voiceover-input-v2-content-capacity",
        "product_code": text(master.get("product_code")),
        "target_country": text(master.get("target_country")),
        "target_language": text(master.get("target_language")),
        "target_duration_seconds": int(master.get("target_duration_seconds") or 0),
        "requested_hook_id": text(semantic.get("hook_id") or master.get("requested_hook_id")),
        "primary_narrative_context": text(semantic.get("primary_narrative_context")),
        "core_buying_reason": text(semantic.get("core_buying_reason")),
        "selling_argument": primary_argument,
        "approved_supporting_arguments": supporting_arguments,
        "longform_argument_bundle": argument_bundle,
        "verified_facts": list(master.get("verified_facts") or []),
        "relationship_language": dict(master.get("relationship_language") or {}),
        "approved_style_references": list(master.get("approved_style_references") or []),
        "native_rhetoric_contract": dict(master.get("native_rhetoric_contract") or {}),
        "production_world": dict(master.get("production_world") or {}),
        "closure_language_contract": _closure_language_contract(master),
        "semantic_sections": [
            {
                "segment_id": segment["segment_id"],
                "duration_seconds": segment["duration_seconds"],
                "target_spoken_seconds_range": _spoken_target_range(
                    segment["duration_seconds"]
                ),
                "scene_id": segment.get("scene_id"),
                "scene_narrative_role": dict(segment.get("scene_block") or {}).get("narrative_role"),
                "capture_units": [
                    {"unit_id": unit["unit_id"], "beat": unit["beat"],
                     "information_gain": unit["information_gain"]}
                    for unit in segment["capture_units"]
                ],
            }
            for segment in plan.get("segments") or []
        ],
        "speech_policy": {
            "one_continuous_thought": True,
            "no_rehook_after_first_segment": True,
            "no_sentence_to_shot_lock": True,
            "tts_after_video_merge": True,
            "tts_layout": "SEMANTIC_SECTION_AT_SEGMENT_START",
            "target_coverage_ratio": [0.90, 0.96],
            "natural_rate_first": True,
        },
    }


def _estimated_spoken_seconds(target_text: str, target_language: str) -> float:
    chars = len(re.sub(r"\s+", "", text(target_text)))
    normalized = text(target_language).lower()
    if any(token in normalized for token in ("泰", "thai", "th-th")):
        cps = 13.0
    elif any(token in normalized for token in ("中文", "汉语", "chinese", "zh-")):
        cps = 4.2
    else:
        cps = 9.0
    return round(chars / cps, 2) if chars else 0.0


def _invoke_voiceover_model(request: Dict[str, Any], model_command: str) -> Dict[str, Any]:
    completed = subprocess.run(
        shlex.split(model_command), input=json.dumps(request, ensure_ascii=False),
        text=True, capture_output=True, timeout=600, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"长视频中央口播失败: {(completed.stderr or completed.stdout)[-1000:]}")
    result = json.loads(completed.stdout)
    result.pop("_model_provenance", None)
    return result


def _section_duration_fit(result: Mapping[str, Any], payload: Mapping[str, Any]) -> list[Dict[str, Any]]:
    by_id = {
        text(item.get("segment_id")).upper(): item
        for item in result.get("semantic_sections") or [] if isinstance(item, Mapping)
    }
    report = []
    for planned in payload.get("semantic_sections") or []:
        if not isinstance(planned, Mapping):
            continue
        segment_id = text(planned.get("segment_id")).upper()
        actual = by_id.get(segment_id, {})
        estimate = _estimated_spoken_seconds(
            actual.get("target_text") or "", payload.get("target_language") or ""
        )
        target_range = list(planned.get("target_spoken_seconds_range") or [])
        minimum = float(target_range[0]) if len(target_range) > 1 else 0.0
        maximum = float(target_range[1]) if len(target_range) > 1 else float(
            planned.get("duration_seconds") or 0
        )
        segment_duration = float(planned.get("duration_seconds") or 0)
        soft_minimum = round(segment_duration * 0.75, 1)
        soft_maximum = round(segment_duration * 0.98, 1)
        if estimate and minimum <= estimate <= maximum:
            status = "FIT"
        elif estimate and soft_minimum <= estimate <= soft_maximum:
            status = "SOFT_FIT"
        else:
            status = "OUT_OF_RANGE"
        report.append({
            "segment_id": segment_id,
            "estimated_seconds": estimate,
            "target_seconds_range": [minimum, maximum],
            "soft_acceptable_seconds_range": [soft_minimum, soft_maximum],
            "status": status,
        })
    return report


def _duration_fit_penalty(total_estimate: float, target_duration: int,
                          sections: list[Mapping[str, Any]]) -> float:
    penalty = abs(total_estimate - target_duration * 0.90)
    for item in sections:
        estimate = float(item.get("estimated_seconds") or 0)
        bounds = list(item.get("target_seconds_range") or [0, 0])
        minimum, maximum = float(bounds[0]), float(bounds[1])
        if estimate < minimum:
            penalty += (minimum - estimate) * 1.5
        elif estimate > maximum:
            penalty += (estimate - maximum) * 3.0
    return penalty


def run_longform_voiceover(master: Mapping[str, Any], plan: Mapping[str, Any],
                           model_command: str = DEFAULT_MODEL_COMMAND) -> Dict[str, Any]:
    payload = build_longform_voiceover_payload(master, plan)
    request = {
        "contract_name": "creative_longform_single_v1",
        "payload": payload,
    }
    result = _invoke_voiceover_model(request, model_command)
    target_duration = int(master.get("target_duration_seconds") or 0)
    estimate = _estimated_spoken_seconds(
        result.get("target_text") or "", master.get("target_language") or ""
    )
    minimum = target_duration * 0.80
    maximum = target_duration * 0.98
    section_fit = _section_duration_fit(result, payload)
    required = ("target_text", "chinese_translation", "estimated_seconds", "semantic_sections")
    missing = [field for field in required if not result.get(field)]
    if missing:
        raise ValueError("长视频中央口播缺少字段: " + ", ".join(missing))
    realization = text(result.get("selling_argument_realization"))
    if realization and realization not in text(result.get("target_text")):
        raise ValueError("长视频中央口播 selling_argument_realization 必须是目标语言原文片段")
    result["estimated_seconds"] = estimate
    result["duration_fit"] = {
        "method": "LOCALE_NONSPACE_CHAR_RATE_V1",
        "target_duration_seconds": target_duration,
        "estimated_seconds": estimate,
        "coverage_ratio": round(estimate / target_duration, 3) if target_duration else 0,
        "revision_used": False,
        "revision_limit": 0,
        "semantic_sections": section_fit,
        "audio_readiness": "PENDING_EDGE_TTS_PREFLIGHT",
        "authority": "TEXT_ESTIMATE_ADVISORY_ONLY",
    }
    result["schema_version"] = VOICEOVER_SCHEMA_VERSION
    if not isinstance(result.get("argument_usage"), list):
        usage = []
        primary_id = text(payload.get("selling_argument", {}).get("argument_id"))
        if primary_id:
            usage.append({"argument_id": primary_id, "role": "PRIMARY_OUTCOME"})
        roles = {
            text(item.get("argument_id")): text(item.get("argument_role")) or "PRODUCT_REASON"
            for item in payload.get("approved_supporting_arguments") or []
            if isinstance(item, Mapping)
        }
        usage.extend({"argument_id": value, "role": roles.get(value, "PRODUCT_REASON")}
                     for value in result.get("used_supporting_argument_ids") or [])
        result["argument_usage"] = usage
    return result


def _actual_fit_penalty(preflight: Mapping[str, Any]) -> float:
    penalty = 0.0
    for item in preflight.get("sections") or []:
        ratio = float(item.get("coverage_ratio") or 0)
        penalty += abs(ratio - 0.93)
        if ratio < 0.86:
            penalty += (0.86 - ratio) * 4
        elif ratio > 1.0:
            penalty += (ratio - 1.0) * 7
    return penalty


def _preflight_reusable(voiceover: Mapping[str, Any]) -> bool:
    preflight = voiceover.get("tts_preflight") or {}
    sections = [item for item in voiceover.get("semantic_sections") or [] if isinstance(item, Mapping)]
    measured = [item for item in preflight.get("sections") or [] if isinstance(item, Mapping)]
    if len(sections) != len(measured) or not sections:
        return False
    by_id = {text(item.get("segment_id")).upper(): item for item in measured}
    for section in sections:
        item = by_id.get(text(section.get("segment_id")).upper()) or {}
        path = Path(text(item.get("audio_path")))
        if (
            text(item.get("text_sha256"))
            != voiceover_text_hash(text(section.get("target_text")))
            or not path.is_file()
        ):
            return False
    return True


def calibrate_longform_voiceover_with_edge(
    master: Mapping[str, Any],
    plan: Mapping[str, Any],
    voiceover: Mapping[str, Any],
    output_dir: str | Path,
    *,
    model_command: str = DEFAULT_MODEL_COMMAND,
    voice_id: str = "th-TH-PremwadeeNeural",
) -> Dict[str, Any]:
    """Use real Edge duration as the only revision authority.

    One targeted revision is permitted.  A merely non-ideal but usable read is
    accepted; the pipeline does not loop or invent a second selling point just
    to fill time.
    """

    result = dict(voiceover)
    if _preflight_reusable(result):
        return result
    sections = [dict(item) for item in result.get("semantic_sections") or [] if isinstance(item, Mapping)]
    preflight = synthesize_segment_preflight(
        sections, plan.get("segments") or [], output_dir, voice_id=voice_id,
    )
    needs_revision = any(
        item.get("status") in {"TOO_SHORT", "TOO_LONG"}
        for item in preflight.get("sections") or []
    )
    selected = result
    selected_preflight = preflight
    revision_attempted = False
    revision_selected = False
    if needs_revision:
        revision_attempted = True
        payload = build_longform_voiceover_payload(master, plan)
        payload["actual_tts_revision"] = {
            "schema_version": "longform-edge-duration-revision-v1",
            "revision_limit": 1,
            "previous_draft": {
                "target_text": result.get("target_text") or "",
                "chinese_translation": result.get("chinese_translation") or "",
                "semantic_sections": sections,
            },
            "measured_sections": list(preflight.get("sections") or []),
            "instruction": (
                "只调整实测过短或过长的语义段，使Edge自然语速实测尽量达到对应视频段的90%-96%。"
                "过短时补充同一已批准卖点下的具体处境、理由或个人判断；过长时删除重复表达。"
                "保持一条连续思想，不新增卖点、商品事实、清单或第二次钩子，不改画面。"
            ),
        }
        revised = _invoke_voiceover_model(
            {"contract_name": "creative_longform_single_v1", "payload": payload},
            model_command,
        )
        revised_sections = [
            dict(item) for item in revised.get("semantic_sections") or []
            if isinstance(item, Mapping)
        ]
        if len(revised_sections) == len(sections) and all(
            text(item.get("target_text")) for item in revised_sections
        ):
            revised_preflight = synthesize_segment_preflight(
                revised_sections, plan.get("segments") or [], output_dir,
                voice_id=voice_id,
            )
            if _actual_fit_penalty(revised_preflight) < _actual_fit_penalty(preflight):
                selected = {**result, **dict(revised)}
                selected_preflight = revised_preflight
                revision_selected = True
    selected["tts_preflight"] = {
        **selected_preflight,
        "revision_attempted": revision_attempted,
        "revision_selected": revision_selected,
        "revision_limit": 1,
        "readiness": (
            "READY"
            if selected_preflight.get("all_acceptable")
            else "READY_WITH_SOFT_DURATION_WARNING"
        ),
    }
    duration_fit = dict(selected.get("duration_fit") or {})
    duration_fit.update({
        "method": "EDGE_TTS_ACTUAL_V1",
        "authority": "ACTUAL_AUDIO",
        "audio_readiness": selected["tts_preflight"]["readiness"],
        "revision_used": revision_selected,
        "revision_limit": 1,
        "actual_tts_seconds": selected_preflight.get("actual_tts_seconds_total"),
    })
    selected["duration_fit"] = duration_fit
    selected["schema_version"] = VOICEOVER_SCHEMA_VERSION
    return selected
