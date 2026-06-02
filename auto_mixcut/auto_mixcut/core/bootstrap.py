from __future__ import annotations

from auto_mixcut.adapters.oss import build_oss
from auto_mixcut.adapters.repository import MySQLRepository, SQLiteRepository
from auto_mixcut.core.config import Settings
from auto_mixcut.core.ffmpeg import FFmpeg
from auto_mixcut.skills.context import SkillContext


def build_context(config_path: str | None = None) -> SkillContext:
    settings = Settings.load(config_path)
    if settings.db_provider in {"mysql", "rds"}:
        if not settings.database_url:
            raise ValueError("AUTO_MIXCUT_DATABASE_URL or LIKEU_AI_DATABASE_URL is required when AUTO_MIXCUT_DB_PROVIDER=mysql")
        repo = MySQLRepository.from_url(settings.database_url)
    else:
        repo = SQLiteRepository(settings.db_path)
    return SkillContext(settings=settings, repo=repo, oss=build_oss(settings), ffmpeg=FFmpeg(mock=settings.mock_ffmpeg))
