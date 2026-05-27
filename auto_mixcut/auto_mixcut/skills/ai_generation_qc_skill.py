from __future__ import annotations

from typing import Any, Dict, List, Optional

from auto_mixcut.core.result import Result

from .ai_segment_factory_config import AISegmentFactoryConfig, get_config
from .context import SkillContext


class AIGenerationQCSkill:
    def __init__(self, ctx: SkillContext, config: Optional[AISegmentFactoryConfig] = None):
        self.ctx = ctx
        self.config = config or get_config()

    def check_job(self, job_id: str) -> Result:
        job = self.ctx.repo.get("ai_generation_jobs", "job_id", job_id)
        if not job:
            return Result.fail("JOB_NOT_FOUND", "ai_generation_jobs not found", {"job_id": job_id})

        segments = self.ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (job["product_id"],))
        total = len(segments)
        passed = 0
        failed = 0
        for s in segments:
            ok, issues = _basic_qc(s)
            self.ctx.repo.update("segments", "segment_id", s["segment_id"], {"segment_status": "qc_passed" if ok else "qc_failed"})
            if ok:
                passed += 1
            else:
                failed += 1

        self.ctx.repo.update("ai_generation_jobs", "job_id", job_id, {"status": "QC_COMPLETED", "accepted_count": passed})
        return Result.ok({"job_id": job_id, "checked": total, "passed": passed, "failed": failed})

def _basic_qc(segment: Dict[str, Any]) -> tuple[bool, List[str]]:
    issues = []
    duration = int(segment.get("duration_ms") or 0)
    width = int(segment.get("width") or 0)
    height = int(segment.get("height") or 0)
    if duration < 1500 or duration > 6000:
        issues.append(f"时长异常: {duration}ms")
    if width != 1080 or height != 1920:
        issues.append(f"分辨率异常: {width}x{height}")
    if segment.get("source_type") != "ai_generated":
        issues.append("非AI生成素材")
    return len(issues) == 0, issues


def _latest_tag(ctx: SkillContext, segment_id: str) -> Dict[str, Any]:
    rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
    return rows[0] if rows else {}
