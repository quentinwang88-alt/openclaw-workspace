from __future__ import annotations

from auto_mixcut.adapters.oss import LocalOSS
from auto_mixcut.adapters.repository import SQLiteRepository
from auto_mixcut.core.config import Settings
from auto_mixcut.core.ffmpeg import FFmpeg
from auto_mixcut.skills.context import SkillContext


def build_context(config_path: str | None = None) -> SkillContext:
    settings = Settings.load(config_path)
    return SkillContext(settings=settings, repo=SQLiteRepository(settings.db_path), oss=LocalOSS(settings.oss_root, settings.bucket), ffmpeg=FFmpeg(mock=settings.mock_ffmpeg))
