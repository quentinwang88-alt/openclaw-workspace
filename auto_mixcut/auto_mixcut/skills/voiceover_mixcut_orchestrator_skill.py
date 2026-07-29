from __future__ import annotations

from typing import Any

from auto_mixcut.core.result import Result

from .context import SkillContext
from .voiceover_material_adapter_skill import VoiceoverMaterialAdapterSkill
from .voiceover_render_plan_bridge_skill import VoiceoverRenderPlanBridgeSkill


class VoiceoverMixcutOrchestratorSkill:
    """Small coordinator; generation/TTS remain owned by the existing voiceover flow."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def prepare_render_plan(
        self,
        *,
        task_id: str,
        batch_id: str,
        variant_no: int,
        voiceover_variant_id: str,
        voiceover_oss_object_id: str,
        tts_timeline: list[dict[str, Any]],
        beat_plan: list[dict[str, Any]] | dict[str, Any],
        hook_id: str = "",
        primary_selling_point: str = "",
    ) -> Result:
        task = self.ctx.repo.get("content_tasks", "task_id", task_id)
        if not task:
            return Result.fail("TASK_NOT_FOUND", "content task not found", {"task_id": task_id})
        product_id = str(task.get("product_id") or "")
        if not product_id:
            return Result.fail("PRODUCT_ID_MISSING", "content task has no product_id", {"task_id": task_id})
        if str(task.get("content_mode") or "auto") not in {"auto", "voiceover"}:
            return Result.fail("CONTENT_MODE_MISMATCH", "task is not configured for voiceover", {"task_id": task_id})
        target_duration_ms = int(task.get("target_duration_ms") or _timeline_end(tts_timeline) or 15000)
        self.ctx.repo.update(
            "content_tasks",
            "task_id",
            task_id,
            {
                "voiceover_variant_id": voiceover_variant_id,
                "voiceover_status": "matching",
                "narrative_failure_reason": "",
            },
        )
        matched = VoiceoverMaterialAdapterSkill(self.ctx).match(
            product_id,
            tts_timeline,
            target_duration_ms,
            beat_plan=beat_plan,
            primary_selling_point=primary_selling_point,
        )
        if not matched.success:
            self._fail(task_id, matched)
            return matched
        gaps = matched.data.get("evidence_gaps") or []
        if gaps:
            result = Result.fail("VOICEOVER_EVIDENCE_GAPS", "voiceover material matching has evidence gaps", {"evidence_gaps": gaps, "match": matched.data})
            self._fail(task_id, result)
            return result
        bridged = VoiceoverRenderPlanBridgeSkill(self.ctx).create(
            batch_id=batch_id,
            product_id=product_id,
            variant_no=variant_no,
            voiceover_variant_id=voiceover_variant_id,
            voiceover_oss_object_id=voiceover_oss_object_id,
            tts_timeline=tts_timeline,
            match_result=matched.data,
            beat_plan=beat_plan,
            hook_id=hook_id,
            primary_selling_point=primary_selling_point,
        )
        if not bridged.success:
            self._fail(task_id, bridged)
            return bridged
        self.ctx.repo.update(
            "content_tasks",
            "task_id",
            task_id,
            {
                "content_mode": "voiceover",
                "voiceover_status": "render_plan_ready",
                "narrative_failure_reason": "",
                "last_batch_id": batch_id,
            },
        )
        return Result.ok({"task_id": task_id, "match": matched.data, "render_plan": bridged.data})

    def _fail(self, task_id: str, result: Result) -> None:
        error = result.error.to_dict() if result.error else {}
        self.ctx.repo.update(
            "content_tasks",
            "task_id",
            task_id,
            {"voiceover_status": "blocked", "narrative_failure_reason": str(error.get("message") or error.get("code") or "voiceover preparation failed")},
        )


def _timeline_end(rows: list[dict[str, Any]]) -> int:
    values = []
    for row in rows:
        try:
            values.append(int(row.get("end_ms") or round(float(row.get("end_seconds") or 0) * 1000)))
        except (TypeError, ValueError):
            continue
    return max(values, default=0)
