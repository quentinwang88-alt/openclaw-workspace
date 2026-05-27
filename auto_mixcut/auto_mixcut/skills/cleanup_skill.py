from __future__ import annotations

import shutil

from auto_mixcut.core.result import Result

from .context import SkillContext
from .feishu_review_skill import FeishuReviewSkill


class CleanupSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def cleanup_task(self, task_id: str | None = None) -> Result:
        cleaned_local = False
        if self.ctx.settings.temp_root.exists():
            shutil.rmtree(self.ctx.settings.temp_root)
            cleaned_local = True
        feishu = FeishuReviewSkill(self.ctx).cleanup_expired_previews()
        if task_id:
            self.ctx.repo.update("content_tasks", "task_id", task_id, {"task_status": "CLEANED"})
        return Result.ok({"local_temp_cleaned": cleaned_local, "feishu": feishu.to_dict()})
