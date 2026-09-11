from __future__ import annotations

import hashlib
from typing import Any, Dict

from .contracts import ExecutionPlan, Issue


def build_frozen_audio_plan(plan: ExecutionPlan) -> tuple[Dict[str, Any], list[Issue]]:
    source = plan.source
    issues: list[Issue] = []
    timed_lines = [
        {
            "start_ms": shot.start_ms,
            "end_ms": shot.end_ms,
            "text": line,
            "shot_id": shot.shot_id,
        }
        for shot in plan.shots
        for line in shot.spoken_lines
    ]
    if plan.audio_mode == "SOURCE_COPY_TTS":
        target_text = source.source_voiceover or "\n".join(item["text"] for item in timed_lines)
        if source.source_voiceover and timed_lines:
            compact_field = "".join(source.source_voiceover.split())
            compact_timeline = "".join(item["text"] for item in timed_lines for _ in (0,))
            compact_timeline = "".join(compact_timeline.split())
            if compact_field != compact_timeline:
                issues.append(Issue(
                    "AUDIO_SOURCE_CONFLICT", "BLOCK",
                    "口播_目标语言与逐镜口播不一致，不能选择一个版本静默执行",
                    affected_stage="AUDIO",
                ))
        return ({
            "schema_version": "remake-audio-plan-v1",
            "mode": "PRESERVE_SOURCE_COPY",
            "rewrite_allowed": False,
            "target_language": source.target_language,
            "target_text": target_text,
            "target_text_sha256": hashlib.sha256(target_text.encode("utf-8")).hexdigest(),
            "timed_lines": timed_lines,
            "bgm_policy": "FOLLOW_SOURCE_REQUIREMENTS",
        }, issues)
    return ({
        "schema_version": "remake-audio-plan-v1",
        "mode": "NO_VOICEOVER" if plan.audio_mode == "NO_VOICEOVER" else "UNSPECIFIED",
        "rewrite_allowed": False,
        "target_language": source.target_language,
        "target_text": "",
        "timed_lines": [],
        "bgm_policy": "FOLLOW_SOURCE_REQUIREMENTS",
    }, issues)

