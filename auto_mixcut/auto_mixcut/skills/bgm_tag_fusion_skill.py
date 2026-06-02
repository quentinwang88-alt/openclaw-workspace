from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from auto_mixcut.core.result import Result

from .context import SkillContext


class BgmTagFusionSkill:
    """Fuse metadata LLM tags with audio-only technical analysis."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def fuse_track(self, bgm_id: str) -> Result:
        track = self.ctx.repo.get("bgm_tracks", "bgm_id", bgm_id)
        if not track:
            return Result.fail("BGM_NOT_FOUND", "BGM track not found", {"bgm_id": bgm_id})
        updates = _fusion_updates(track)
        if not updates:
            return Result.ok({"bgm_id": bgm_id, "changed": False})
        write = self.ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, updates)
        if not write.success:
            return write
        return Result.ok({"bgm_id": bgm_id, "changed": True, "updates": updates})

    def fuse_all(self, limit: int | None = None) -> Result:
        tracks = [t for t in self.ctx.repo.list_where("bgm_tracks") if t.get("audio_analysis_json") or t.get("ai_suggested_tags_json")]
        if limit:
            tracks = tracks[:limit]
        results = [self.fuse_track(t["bgm_id"]).to_dict() for t in tracks]
        return Result.ok({"count": len(results), "results": results})


def _fusion_updates(track: dict[str, Any]) -> dict[str, Any]:
    text_tags = _jsonish(track.get("ai_suggested_tags_json")) or {}
    audio = track.get("audio_analysis_json") or {}
    audio_tags = audio.get("audio_suggested_tags") or {}
    mix = audio.get("mix_suggestions") or {}

    mood_tags = _merge_tags(text_tags.get("mood_tags") or track.get("mood_tags_json"), audio_tags.get("mood_tags"), 3)
    category_tags = _list(text_tags.get("category_tags")) or _list(track.get("category_tags_json")) or ["generic_fashion"]
    template_tags = _list(text_tags.get("template_tags")) or _list(track.get("template_tags_json"))

    updates: dict[str, Any] = {
        "mood_tags_json": json.dumps(mood_tags or ["daily_clean"], ensure_ascii=False),
        "category_tags_json": json.dumps(category_tags[:3], ensure_ascii=False),
        "template_tags_json": json.dumps(template_tags[:3], ensure_ascii=False),
        "energy_level": audio_tags.get("energy_level") or text_tags.get("energy_level") or track.get("energy_level") or "medium",
        "vocal_type": audio_tags.get("vocal_type") or text_tags.get("vocal_type") or track.get("vocal_type") or "unknown",
        "tag_confidence": _fused_confidence(track, audio),
        "mix_constraints_json": {
            **(_jsonish(track.get("mix_constraints_json")) or {}),
            "tag_fusion": {
                "source": "metadata_llm_plus_audio_analysis",
                "mood_source": "metadata_llm_primary_audio_aux",
                "energy_source": "audio",
                "vocal_source": "audio",
                "fused_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
    }
    for key in ["recommended_start_sec", "default_volume", "fade_in_ms", "fade_out_ms", "suitable_for_intro", "loop_friendly", "voiceover_friendly"]:
        if key in mix:
            value = mix[key]
            if isinstance(value, bool):
                value = 1 if value else 0
            updates[key] = value
    return updates


def _merge_tags(primary: Any, secondary: Any, limit: int) -> list[str]:
    merged = []
    for value in [*_list(primary), *_list(secondary)]:
        if value and value not in merged:
            merged.append(value)
    return merged[:limit]


def _list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        parsed = _jsonish(value)
        if isinstance(parsed, list):
            return [str(v).strip() for v in parsed if str(v).strip()]
    return [str(value).strip()]


def _fused_confidence(track: dict[str, Any], audio: dict[str, Any]) -> str:
    audio_conf = str(audio.get("tag_confidence") or track.get("audio_tag_confidence") or "").lower()
    text_tags = _jsonish(track.get("ai_suggested_tags_json")) or {}
    text_has_mood = bool(_list(text_tags.get("mood_tags")))
    if audio_conf == "high" and text_has_mood:
        return "high"
    if audio_conf in {"high", "medium"}:
        return "medium"
    return track.get("tag_confidence") or "low"


def _jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return None
    return None
