from __future__ import annotations

from typing import Any, Dict

from .contracts import ExecutionPlan


def build_subtitle_plan(plan: ExecutionPlan) -> Dict[str, Any]:
    cues = []
    for shot in plan.shots:
        for text in shot.screen_texts:
            cues.append({
                "cue_id": f"CUE_{len(cues) + 1:02d}",
                "start_ms": shot.start_ms,
                "end_ms": shot.end_ms,
                "text": text,
                "source_shot_id": shot.shot_id,
                "render_mode": "POST_PROCESS",
            })
    return {
        "schema_version": "remake-subtitle-plan-v1",
        "target_language": plan.source.target_language,
        "cues": cues,
    }

