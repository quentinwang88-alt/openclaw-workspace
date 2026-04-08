#!/usr/bin/env python3
"""Repository-local environment and path helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_OPENCLAW_HOME = Path.home() / ".openclaw"


def _strip_wrapping_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def load_repo_env(candidates: Iterable[Path] | None = None) -> None:
    """Load repo-local env files without overriding already exported vars."""
    env_candidates = list(candidates or [REPO_ROOT / ".env", REPO_ROOT / ".env.local"])
    bootstrap = {
        "REPO_ROOT": str(REPO_ROOT),
        "OPENCLAW_HOME": os.environ.get("OPENCLAW_HOME", str(DEFAULT_OPENCLAW_HOME)),
    }

    for key, value in bootstrap.items():
        os.environ.setdefault(key, value)

    for candidate in env_candidates:
        if not candidate.exists():
            continue

        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, raw_value = line.split("=", 1)
            key = key.strip()
            value = _strip_wrapping_quotes(raw_value.strip())
            expanded = os.path.expandvars(os.path.expanduser(value))

            if key and key not in os.environ:
                os.environ[key] = expanded


def get_openclaw_home() -> Path:
    return Path(os.environ.get("OPENCLAW_HOME", str(DEFAULT_OPENCLAW_HOME))).expanduser()


def get_repo_root() -> Path:
    return REPO_ROOT


def get_openclaw_workspace_root() -> Path:
    raw = os.environ.get("OPENCLAW_WORKSPACE_ROOT", "").strip()
    return Path(raw).expanduser() if raw else REPO_ROOT


def get_shared_data_dir() -> Path:
    raw = os.environ.get("OPENCLAW_SHARED_DATA_DIR", "").strip()
    path = Path(raw).expanduser() if raw else get_openclaw_home() / "shared" / "data"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_shared_sqlite_path(db_name: str) -> Path:
    filename = db_name if db_name.endswith((".db", ".sqlite", ".sqlite3")) else f"{db_name}.sqlite3"
    return get_shared_data_dir() / filename


def get_shared_sqlite_url(db_name: str) -> str:
    return f"sqlite:///{get_shared_sqlite_path(db_name)}"


def get_media_inbound_dir() -> Path:
    raw = os.environ.get("OPENCLAW_MEDIA_INBOUND_DIR", "").strip()
    return Path(raw).expanduser() if raw else get_openclaw_home() / "media" / "inbound"


load_repo_env()
