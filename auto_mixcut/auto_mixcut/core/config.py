from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass
class Settings:
    root_dir: Path
    db_path: Path
    oss_root: Path
    temp_root: Path
    bucket: str
    mock_llm: bool = True
    mock_ffmpeg: bool = False
    feishu_enabled: bool = False

    @classmethod
    def load(cls, config_path: str | None = None) -> "Settings":
        root = Path(os.environ.get("AUTO_MIXCUT_ROOT", Path.cwd())).resolve()
        data: Dict[str, Any] = {}
        if config_path:
            with open(config_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        return cls(
            root_dir=root,
            db_path=Path(data.get("db_path") or os.environ.get("AUTO_MIXCUT_DB", root / "var" / "auto_mixcut.sqlite")).resolve(),
            oss_root=Path(data.get("oss_root") or os.environ.get("AUTO_MIXCUT_OSS_ROOT", root / "var" / "oss")).resolve(),
            temp_root=Path(data.get("temp_root") or os.environ.get("AUTO_MIXCUT_TEMP_ROOT", "/tmp/auto_mixcut")).resolve(),
            bucket=data.get("bucket") or os.environ.get("AUTO_MIXCUT_BUCKET", "local-auto-mixcut"),
            mock_llm=str(data.get("mock_llm", os.environ.get("AUTO_MIXCUT_MOCK_LLM", "1"))).lower() in {"1", "true", "yes"},
            mock_ffmpeg=str(data.get("mock_ffmpeg", os.environ.get("AUTO_MIXCUT_MOCK_FFMPEG", "0"))).lower() in {"1", "true", "yes"},
            feishu_enabled=str(data.get("feishu_enabled", os.environ.get("AUTO_MIXCUT_FEISHU_ENABLED", "0"))).lower() in {"1", "true", "yes"},
        )
