from __future__ import annotations

from dataclasses import dataclass

from auto_mixcut.adapters.repository import SQLiteRepository
from auto_mixcut.core.config import Settings
from auto_mixcut.core.ffmpeg import FFmpeg


@dataclass
class SkillContext:
    settings: Settings
    repo: SQLiteRepository
    oss: object
    ffmpeg: FFmpeg
