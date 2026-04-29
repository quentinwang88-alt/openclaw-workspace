#!/usr/bin/env python3
"""运行配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _load_local_env() -> None:
    env_candidates = [
        Path(__file__).resolve().parents[1] / ".env.local",
        Path(__file__).resolve().parents[1] / ".env",
    ]
    for env_path in env_candidates:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value
        break


_load_local_env()


def _default_data_root() -> Path:
    raw = os.environ.get("FASTMOSS_B_DATA_ROOT", "").strip()
    root = Path(raw).expanduser() if raw else Path.home() / ".openclaw" / "shared" / "data" / "fastmoss_selection_b"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _default_database_url() -> str:
    return "sqlite:///{path}".format(path=_default_data_root() / "fastmoss_selection_b.sqlite3")


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_DATABASE_URL", _default_database_url()))
    archive_root: Path = field(
        default_factory=lambda: Path(
            os.environ.get("FASTMOSS_B_ARCHIVE_ROOT", str(_default_data_root() / "runs"))
        ).expanduser()
    )
    download_root: Path = field(
        default_factory=lambda: Path(
            os.environ.get("FASTMOSS_B_DOWNLOAD_ROOT", str(_default_data_root() / "downloads"))
        ).expanduser()
    )
    config_table_url: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_CONFIG_TABLE_URL", "").strip())
    batch_table_url: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_BATCH_TABLE_URL", "").strip())
    workspace_table_url: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_WORKSPACE_TABLE_URL", "").strip())
    followup_table_url: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_FOLLOWUP_TABLE_URL", "").strip())
    feishu_read_page_size: int = field(default_factory=lambda: int(os.environ.get("FASTMOSS_B_FEISHU_READ_PAGE_SIZE", "100")))
    feishu_write_batch_size: int = field(default_factory=lambda: int(os.environ.get("FASTMOSS_B_FEISHU_WRITE_BATCH_SIZE", "50")))
    download_retry_limit: int = field(default_factory=lambda: int(os.environ.get("FASTMOSS_B_DOWNLOAD_RETRY_LIMIT", "3")))
    import_retry_limit: int = field(default_factory=lambda: int(os.environ.get("FASTMOSS_B_IMPORT_RETRY_LIMIT", "3")))
    hermes_retry_limit: int = field(default_factory=lambda: int(os.environ.get("FASTMOSS_B_HERMES_RETRY_LIMIT", "2")))
    accio_bot_open_id: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_ACCIO_BOT_OPEN_ID", "").strip())
    accio_bot_name: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_ACCIO_BOT_NAME", "ACCIO选品专员").strip())
    accio_timeout_hours: int = field(default_factory=lambda: int(os.environ.get("FASTMOSS_B_ACCIO_TIMEOUT_HOURS", "2")))
    archive_retention_days: int = field(
        default_factory=lambda: int(os.environ.get("FASTMOSS_B_ARCHIVE_RETENTION_DAYS", "45"))
    )
    pending_batch_status: str = field(default_factory=lambda: os.environ.get("FASTMOSS_B_PENDING_BATCH_STATUS", "待B下载"))
    hermes_command_template: str = field(
        default_factory=lambda: os.environ.get("FASTMOSS_B_HERMES_COMMAND", "").strip()
    )
    hermes_timeout_seconds: int = field(
        default_factory=lambda: int(os.environ.get("FASTMOSS_B_HERMES_TIMEOUT_SECONDS", "300"))
    )
    hermes_chunk_size: int = field(
        default_factory=lambda: int(os.environ.get("FASTMOSS_B_HERMES_CHUNK_SIZE", "8"))
    )


def get_settings() -> Settings:
    settings = Settings()
    settings.archive_root.mkdir(parents=True, exist_ok=True)
    settings.download_root.mkdir(parents=True, exist_ok=True)
    return settings
