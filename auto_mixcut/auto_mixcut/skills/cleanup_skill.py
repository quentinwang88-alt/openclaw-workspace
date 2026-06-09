from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from pathlib import Path

from auto_mixcut.core.result import Result

from .context import SkillContext
from .feishu_review_skill import FeishuReviewSkill


class CleanupSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def cleanup_task(self, task_id: str | None = None) -> Result:
        cleaned_local = False
        pruned_upload_backups = 0
        if getattr(self.ctx.settings, "local_upload_backup_days", 0) > 0:
            pruned_upload_backups = _prune_old_upload_backups(self.ctx.settings.temp_root / "feishu_uploads", self.ctx.settings.local_upload_backup_days)
        if self.ctx.settings.temp_root.exists():
            if getattr(self.ctx.settings, "local_upload_backup_days", 0) > 0:
                _clean_temp_root_preserving_upload_backups(self.ctx.settings.temp_root)
            else:
                shutil.rmtree(self.ctx.settings.temp_root)
            cleaned_local = True
        feishu_review = FeishuReviewSkill(self.ctx)
        feishu = feishu_review.cleanup_expired_previews()
        output_files = feishu_review.cleanup_output_attachments()
        if task_id:
            self.ctx.repo.update("content_tasks", "task_id", task_id, {"task_status": "CLEANED"})
        return Result.ok({"local_temp_cleaned": cleaned_local, "pruned_upload_backups": pruned_upload_backups, "feishu": feishu.to_dict(), "output_files": output_files.to_dict()})


def _prune_old_upload_backups(root: Path, keep_days: int) -> int:
    if keep_days <= 0 or not root.exists():
        return 0
    cutoff = datetime.utcnow() - timedelta(days=keep_days)
    cleaned = 0
    for path in sorted(root.rglob("*"), reverse=True):
        if not path.is_file():
            continue
        try:
            modified = datetime.utcfromtimestamp(path.stat().st_mtime)
            if modified < cutoff:
                path.unlink()
                cleaned += 1
        except OSError:
            continue
    for path in sorted((p for p in root.rglob("*") if p.is_dir()), reverse=True):
        try:
            path.rmdir()
        except OSError:
            pass
    return cleaned


def _clean_temp_root_preserving_upload_backups(root: Path) -> None:
    upload_backups = (root / "feishu_uploads").resolve()
    for child in root.iterdir():
        try:
            if child.resolve() == upload_backups:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        except OSError:
            continue
