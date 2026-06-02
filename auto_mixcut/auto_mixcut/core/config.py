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
    oss_cache_root: Path
    oss_provider: str
    temp_root: Path
    bucket: str
    db_provider: str = "sqlite"
    database_url: str = ""
    aliyun_oss_endpoint: str = ""
    aliyun_access_key_id: str = ""
    aliyun_access_key_secret: str = ""
    aliyun_security_token: str = ""
    aliyun_public_base_url: str = ""
    mock_llm: bool = True
    mock_ffmpeg: bool = False
    feishu_enabled: bool = False
    local_upload_backup_days: int = 0

    @classmethod
    def load(cls, config_path: str | None = None) -> "Settings":
        root = Path(os.environ.get("AUTO_MIXCUT_ROOT", Path.cwd())).resolve()
        _load_dotenv(root / ".env")
        data: Dict[str, Any] = {}
        if config_path:
            with open(config_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        temp_root = Path(data.get("temp_root") or os.environ.get("AUTO_MIXCUT_TEMP_ROOT", "/tmp/auto_mixcut")).resolve()
        return cls(
            root_dir=root,
            db_path=Path(data.get("db_path") or os.environ.get("AUTO_MIXCUT_DB", root / "var" / "auto_mixcut.sqlite")).resolve(),
            oss_root=Path(data.get("oss_root") or os.environ.get("AUTO_MIXCUT_OSS_ROOT", root / "var" / "oss")).resolve(),
            oss_cache_root=Path(data.get("oss_cache_root") or os.environ.get("AUTO_MIXCUT_OSS_CACHE_ROOT", temp_root / "oss_cache")).resolve(),
            oss_provider=str(data.get("oss_provider") or os.environ.get("AUTO_MIXCUT_OSS_PROVIDER", "local")).strip().lower(),
            temp_root=temp_root,
            bucket=data.get("bucket") or _env_first("AUTO_MIXCUT_OSS_BUCKET", "ALIYUN_OSS_BUCKET", "AUTO_MIXCUT_BUCKET", default="local-auto-mixcut"),
            db_provider=str(data.get("db_provider") or os.environ.get("AUTO_MIXCUT_DB_PROVIDER", "sqlite")).strip().lower(),
            database_url=str(data.get("database_url") or _env_first("AUTO_MIXCUT_DATABASE_URL", "LIKEU_AI_DATABASE_URL")).strip(),
            aliyun_oss_endpoint=str(data.get("aliyun_oss_endpoint") or _env_first("AUTO_MIXCUT_ALIYUN_OSS_ENDPOINT", "ALIYUN_OSS_ENDPOINT")).strip(),
            aliyun_access_key_id=str(data.get("aliyun_access_key_id") or data.get("aliyun_oss_access_key_id") or _env_first("AUTO_MIXCUT_ALIYUN_ACCESS_KEY_ID", "ALIYUN_OSS_ACCESS_KEY_ID")).strip(),
            aliyun_access_key_secret=str(data.get("aliyun_access_key_secret") or data.get("aliyun_oss_access_key_secret") or _env_first("AUTO_MIXCUT_ALIYUN_ACCESS_KEY_SECRET", "ALIYUN_OSS_ACCESS_KEY_SECRET")).strip(),
            aliyun_security_token=str(data.get("aliyun_security_token") or _env_first("AUTO_MIXCUT_ALIYUN_SECURITY_TOKEN", "ALIYUN_OSS_SECURITY_TOKEN")).strip(),
            aliyun_public_base_url=str(data.get("aliyun_public_base_url") or _env_first("AUTO_MIXCUT_ALIYUN_OSS_PUBLIC_BASE_URL", "ALIYUN_OSS_PUBLIC_BASE_URL", "AUTO_MIXCUT_PREVIEW_BASE_URL")).strip(),
            mock_llm=str(data.get("mock_llm", os.environ.get("AUTO_MIXCUT_MOCK_LLM", "1"))).lower() in {"1", "true", "yes"},
            mock_ffmpeg=str(data.get("mock_ffmpeg", os.environ.get("AUTO_MIXCUT_MOCK_FFMPEG", "0"))).lower() in {"1", "true", "yes"},
            feishu_enabled=str(data.get("feishu_enabled", os.environ.get("AUTO_MIXCUT_FEISHU_ENABLED", "0"))).lower() in {"1", "true", "yes"},
            local_upload_backup_days=max(0, int(data.get("local_upload_backup_days", os.environ.get("AUTO_MIXCUT_LOCAL_UPLOAD_BACKUP_DAYS", "0")) or 0)),
        )


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _env_first(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.environ.get(key)
        if value:
            return value
    return default
