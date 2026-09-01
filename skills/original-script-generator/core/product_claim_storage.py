"""Storage boundary for the shared governed product-claim catalog.

RDS is the durable authority in production.  The historical voiceover SQLite
database remains an explicit local/test fallback and a rebuildable cache; it is
never selected after an RDS connection failure unless the caller opts in.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict

from core.db_compat import connect_sqlite_or_mysql, is_mysql_url


CLAIM_TABLE_PREFIX = "cc__"
CLAIM_TABLE_NAMES = (
    "claim_concepts",
    "product_claim_sources",
    "product_claims",
    "claim_video_evidence",
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _database_url() -> str:
    return _text(
        os.environ.get("PRODUCT_CLAIMS_DATABASE_URL")
        or os.environ.get("LIKEU_AI_DATABASE_URL")
    )


def _storage_mode() -> str:
    value = _text(os.environ.get("PRODUCT_CLAIMS_STORAGE_MODE") or "auto").lower()
    return value if value in {"auto", "rds", "sqlite"} else "auto"


def claim_store_descriptor(
    sqlite_path: str | Path,
    *,
    explicit_sqlite: bool = False,
) -> Dict[str, Any]:
    """Resolve one authority without silently creating a dual-read system."""

    path = Path(sqlite_path).expanduser()
    mode = _storage_mode()
    database_url = _database_url()
    if explicit_sqlite or mode == "sqlite":
        return {
            "backend": "sqlite",
            "provider": "CENTRAL_CLAIM_SQLITE_EXPLICIT",
            "sqlite_path": path,
            "database_url": "",
            "table_prefix": "",
        }
    if database_url and is_mysql_url(database_url):
        return {
            "backend": "rds",
            "provider": "CENTRAL_CLAIM_RDS",
            "sqlite_path": path,
            "database_url": database_url,
            "table_prefix": CLAIM_TABLE_PREFIX,
        }
    if mode == "rds":
        return {
            "backend": "unavailable",
            "provider": "CENTRAL_CLAIM_RDS",
            "sqlite_path": path,
            "database_url": "",
            "table_prefix": CLAIM_TABLE_PREFIX,
            "reason": "PRODUCT_CLAIMS_DATABASE_URL_REQUIRED",
        }
    return {
        "backend": "sqlite",
        "provider": "CENTRAL_CLAIM_SQLITE_FALLBACK",
        "sqlite_path": path,
        "database_url": "",
        "table_prefix": "",
    }


def connect_claim_store(
    descriptor: Dict[str, Any],
    *,
    timeout: int = 12,
):
    backend = _text(descriptor.get("backend"))
    if backend == "unavailable":
        raise RuntimeError(_text(descriptor.get("reason")) or "CLAIM_STORE_UNAVAILABLE")
    path = Path(descriptor["sqlite_path"])
    if backend == "sqlite" and not path.is_file():
        raise FileNotFoundError(str(path))
    if backend == "sqlite":
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        return connection
    return connect_sqlite_or_mysql(
        path,
        database_url=_text(descriptor.get("database_url")),
        table_prefix=_text(descriptor.get("table_prefix")),
        table_names=CLAIM_TABLE_NAMES,
        timeout=timeout,
    )


def table_columns(connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
