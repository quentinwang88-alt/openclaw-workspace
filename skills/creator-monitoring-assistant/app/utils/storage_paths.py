#!/usr/bin/env python3
"""Shared storage path helpers for creator-monitoring."""

from __future__ import annotations

import os
from pathlib import Path


DEFAULT_SHARED_DATA_DIR = Path.home() / ".openclaw" / "shared" / "data"
CREATOR_MONITORING_DB_NAME = "creator_monitoring.sqlite3"


def get_shared_data_dir() -> Path:
    raw = os.environ.get("OPENCLAW_SHARED_DATA_DIR", "").strip()
    path = Path(raw).expanduser() if raw else DEFAULT_SHARED_DATA_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_creator_monitoring_db_path() -> Path:
    return get_shared_data_dir() / CREATOR_MONITORING_DB_NAME


def get_creator_monitoring_database_url() -> str:
    return f"sqlite:///{get_creator_monitoring_db_path()}"
