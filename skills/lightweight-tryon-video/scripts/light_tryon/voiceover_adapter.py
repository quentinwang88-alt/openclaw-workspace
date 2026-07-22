from __future__ import annotations

import re
from typing import Any, Iterable

from .utils import normalized_list, stable_hash
from .workers import run_json_command


VOICEOVER_ADAPTER_VERSION = "current-voiceover-adapter-v1"
ROLE_SEQUENCE = ("hook", "proof", "proof", "decision", "cta")


def build_voiceover_request(
    product: dict[str, Any],
    strategy: dict[str, Any],
    variant: dict[str, Any],
    *,
    available_evidence: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Build a narrow contract for the existing voiceover flow.

    The adapter deliberately carries no replacement writing style prompt. The
    upstream flow remains the sole writer and must return its normal wording.
    """

    return {
        "schema_version": "1.0",
        "adapter_version": VOICEOVER_ADAPTER_VERSION,
        "policy": {
            "generator": "existing_voiceover_flow_only",
            "preserve_current_tone": True,
            "allow_downstream_rewrite": False,
            "hook_source": "existing_hook_library",
        },
        "product": {
            "product_id": product.get("product_id"),
            "product_name": product.get("product_name"),
            "product_title": product.get("product_title"),
            "market": product.get("market"),
            "language": product.get("language"),
            "category": product.get("category"),
            "verified_selling_points": normalized_list(product.get("core_selling_points")),
        },
        "content_combination": {
            "strategy_group_id": strategy.get("strategy_group_id"),
            "hook_id": strategy.get("hook_id"),
            "hook_name": strategy.get("hook_name"),
            "hook_type": strategy.get("hook_type"),
            "primary_selling_point": strategy.get("primary_selling_point"),
            "secondary_selling_points": normalized_list(strategy.get("secondary_selling_points")),
            "visual_focus": strategy.get("visual_focus"),
        },
        "execution": {
            "variant_id": variant.get("variant_id"),
            "target_duration_seconds": int(variant.get("target_duration_seconds") or 22),
            "available_evidence": normalized_list(available_evidence),
            "return_beats_if_supported": True,
        },
    }


def run_existing_voiceover_flow(command: str, request: dict[str, Any], *, timeout: int = 600) -> dict[str, Any]:
    response = run_json_command(command, request, timeout=timeout)
    normalized = normalize_voiceover_response(response)
    normalized["request_fingerprint"] = stable_hash(request, length=20)
    normalized["adapter_version"] = VOICEOVER_ADAPTER_VERSION
    return normalized


def normalize_voiceover_response(response: dict[str, Any]) -> dict[str, Any]:
    beats = response.get("beats") or response.get("storyboard") or []
    if beats and not isinstance(beats, list):
        raise ValueError("现有口播流程返回的 beats/storyboard 必须是数组")
    voiceover_text = str(
        response.get("voiceover_text")
        or response.get("spoken_text")
        or response.get("script")
        or ""
    ).strip()
    normalized_beats = _normalize_beats(beats) if beats else _split_without_rewriting(voiceover_text)
    if not voiceover_text:
        voiceover_text = " ".join(str(item.get("speech_text") or "").strip() for item in normalized_beats).strip()
    if not voiceover_text:
        raise ValueError("现有口播流程没有返回正式口播")
    return {
        **response,
        "voiceover_text": voiceover_text,
        "beats": normalized_beats,
        "downstream_rewritten": False,
    }


def _normalize_beats(rows: list[Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for index, raw in enumerate(rows, start=1):
        row = raw if isinstance(raw, dict) else {"speech_text": str(raw or "")}
        speech = str(
            row.get("speech_text")
            or row.get("voiceover_text_target_language")
            or row.get("voiceover")
            or row.get("spoken_text")
            or ""
        ).strip()
        if not speech:
            continue
        role = str(row.get("role") or row.get("spoken_line_task") or "").strip().lower()
        if role not in {"hook", "fit", "proof", "detail", "color", "scenario", "decision", "cta"}:
            role = ROLE_SEQUENCE[min(index - 1, len(ROLE_SEQUENCE) - 1)]
        result.append({
            "beat_id": str(row.get("beat_id") or f"B{index}"),
            "role": role,
            "speech_text": speech,
            "visual_intent": str(row.get("visual_intent") or row.get("visual_task") or "").strip(),
            "required_shot_roles": normalized_list(row.get("required_shot_roles") or row.get("shot_roles")),
            "required_evidence": normalized_list(row.get("required_evidence") or row.get("evidence_tags")),
            "priority": str(row.get("priority") or "required").strip().lower(),
        })
    if not result:
        raise ValueError("现有口播流程返回的 Beat 没有可用口播文本")
    return result


def _split_without_rewriting(text: str) -> list[dict[str, Any]]:
    """Fallback segmentation preserves every character and only adds structure."""

    parts = [part.strip() for part in re.split(r"(?<=[。！？!?])\s*|\n+", text) if part.strip()]
    if not parts and text.strip():
        parts = [text.strip()]
    return [
        {
            "beat_id": f"B{index}",
            "role": ROLE_SEQUENCE[min(index - 1, len(ROLE_SEQUENCE) - 1)],
            "speech_text": part,
            "visual_intent": "",
            "required_shot_roles": [],
            "required_evidence": [],
            "priority": "required",
        }
        for index, part in enumerate(parts, start=1)
    ]


def build_tts_timeline(beats: list[dict[str, Any]], durations_seconds: list[float], pause_seconds: float = 0.18) -> list[dict[str, Any]]:
    if len(beats) != len(durations_seconds):
        raise ValueError("TTS 实际时长数量必须与 Beat 数量一致")
    cursor = 0.0
    timeline: list[dict[str, Any]] = []
    for index, (beat, duration) in enumerate(zip(beats, durations_seconds)):
        actual = max(0.01, float(duration))
        start = cursor
        end = start + actual
        timeline.append({
            "beat_id": beat.get("beat_id") or f"B{index + 1}",
            "speech_text": beat.get("speech_text") or "",
            "start_seconds": round(start, 3),
            "end_seconds": round(end, 3),
            "duration_seconds": round(actual, 3),
        })
        cursor = end + (pause_seconds if index < len(beats) - 1 else 0.0)
    return timeline
