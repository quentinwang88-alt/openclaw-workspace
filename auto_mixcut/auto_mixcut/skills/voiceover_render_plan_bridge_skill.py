from __future__ import annotations

from typing import Any

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class VoiceoverRenderPlanBridgeSkill:
    """Converts a voiceover matcher result to the existing render-plan contract."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def create(
        self,
        *,
        batch_id: str,
        product_id: str,
        variant_no: int,
        voiceover_variant_id: str,
        voiceover_oss_object_id: str,
        tts_timeline: list[dict[str, Any]],
        match_result: dict[str, Any],
        beat_plan: list[dict[str, Any]] | dict[str, Any],
        hook_id: str = "",
        primary_selling_point: str = "",
        template_id: str = "voiceover_evidence_v1",
        audio_policy: dict[str, Any] | None = None,
    ) -> Result:
        gaps = list(match_result.get("evidence_gaps") or [])
        clips = list(match_result.get("clips") or [])
        if gaps:
            return Result.fail("VOICEOVER_EVIDENCE_GAPS", "voiceover plan still has evidence gaps", {"evidence_gaps": gaps})
        if not clips:
            return Result.fail("VOICEOVER_CLIPS_MISSING", "voiceover plan has no clips")
        segments = []
        for index, clip in enumerate(clips, start=1):
            start_ms = int(clip.get("timeline_start_ms") or 0)
            end_ms = int(clip.get("timeline_end_ms") or (start_ms + int(clip.get("duration_ms") or 0)))
            segments.append(
                {
                    "slot": index,
                    "segment_id": clip["segment_id"],
                    "asset_id": clip["asset_id"],
                    "role": clip.get("role") or clip.get("primary_shot_role") or "scene",
                    "start_ms_in_output": start_ms,
                    "end_ms_in_output": end_ms,
                    "beat_id": clip.get("beat_id"),
                    "speech_text": clip.get("speech_text"),
                    "required_shot_roles": clip.get("required_shot_roles") or [],
                    "original_required_shot_roles": clip.get("original_required_shot_roles") or [],
                    "match_priority": clip.get("match_priority") or "normal",
                    "critical_evidence": bool(clip.get("critical_evidence")),
                    "evidence_match": bool(clip.get("evidence_match")),
                }
            )
        render_plan_id = new_id("PLAN")
        target_duration_ms = int(match_result.get("target_duration_ms") or max(row["end_ms_in_output"] for row in segments))
        row = {
            "render_plan_id": render_plan_id,
            "batch_id": batch_id,
            "product_id": product_id,
            "variant_no": variant_no,
            "template_id": template_id,
            "planned_duration_ms": target_duration_ms,
            "plan_json": {
                "segments": segments,
                "template": {"template_id": template_id, "content_mode": "voiceover"},
                "match_warnings": match_result.get("match_warnings") or [],
                "key_beat_id": match_result.get("key_beat_id") or "",
            },
            "quality_gate_status": "pending",
            "render_status": "planned",
            "content_mode": "voiceover",
            "voiceover_variant_id": voiceover_variant_id,
            "voiceover_oss_object_id": voiceover_oss_object_id,
            "hook_id": hook_id,
            "primary_selling_point": primary_selling_point,
            "beat_plan_json": beat_plan,
            "tts_timeline_json": tts_timeline,
            "evidence_gap_json": gaps,
            "audio_policy_json": audio_policy or {"source_audio": "mute", "voiceover": "continuous", "bgm": "disabled"},
            "match_plan_version": match_result.get("match_plan_version") or "voiceover-visual-match-core-v2-key-evidence",
        }
        write = self.ctx.repo.upsert("render_plans", "render_plan_id", row)
        return write if not write.success else Result.ok(row)
