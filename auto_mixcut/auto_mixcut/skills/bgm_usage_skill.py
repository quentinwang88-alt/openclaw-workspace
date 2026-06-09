from __future__ import annotations

import json

from auto_mixcut.core.result import Result

from .context import SkillContext


REJECTED_BGM_FEEDBACK = {"human_rejected", "final_qc_fail", "aborted", "rejected"}
GOOD_BGM_FEEDBACK = {"human_passed", "final_qc_pass", "passed", "publish_ready"}


class BgmUsageSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def record_output_feedback(self, output_id: str, quality_status: str, reason: str = "") -> Result:
        output = self.ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            return Result.fail("OUTPUT_NOT_FOUND", "output not found", {"output_id": output_id})
        bgm_id = _bgm_id_from_output(output)
        if not bgm_id:
            return Result.ok({"output_id": output_id, "bgm_id": "", "updated_events": 0, "reason": "no_bgm_id"})
        events = self.ctx.repo.list_where("bgm_usage_events", "output_id=? AND bgm_id=?", (output_id, bgm_id))
        updated = 0
        for event in events:
            res = self.ctx.repo.update(
                "bgm_usage_events",
                "event_id",
                event["event_id"],
                {"quality_status": quality_status, "reason": reason or quality_status},
            )
            if res.success:
                updated += 1
        counters = refresh_bgm_track_usage(self.ctx, bgm_id, last_feedback_status=quality_status)
        return Result.ok({"output_id": output_id, "bgm_id": bgm_id, "updated_events": updated, "counters": counters})


def refresh_bgm_track_usage(ctx: SkillContext, bgm_id: str, last_feedback_status: str = "") -> dict:
    bgm_id = str(bgm_id or "").strip()
    if not bgm_id:
        return {"bgm_id": bgm_id, "rendered_usage_count": 0, "rejected_usage_count": 0}
    events = ctx.repo.list_where("bgm_usage_events", "bgm_id=? AND usage_status='rendered'", (bgm_id,))
    rendered = len(events)
    rejected = sum(1 for event in events if str(event.get("quality_status") or "") in REJECTED_BGM_FEEDBACK)
    values = {"usage_count": rendered, "rejected_usage_count": rejected}
    if last_feedback_status:
        values["last_feedback_status"] = last_feedback_status
    ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, values)
    return {"bgm_id": bgm_id, "rendered_usage_count": rendered, "rejected_usage_count": rejected}


def _bgm_id_from_output(output: dict) -> str:
    plan = output.get("bgm_plan_json") or {}
    if isinstance(plan, str):
        try:
            plan = json.loads(plan)
        except json.JSONDecodeError:
            plan = {}
    if not isinstance(plan, dict):
        return ""
    return str(plan.get("bgm_id") or "").strip()
