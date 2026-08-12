"""SQLite persistence for generated first-frame assets and script bindings."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from core.storage import default_db_path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class FirstFrameStorage:
    def __init__(self, db_path: Optional[str | Path] = None):
        self.db_path = Path(db_path or default_db_path()).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS original_first_frame_asset (
                    asset_fingerprint TEXT PRIMARY KEY,
                    asset_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    model TEXT,
                    prompt_version TEXT,
                    prompt_text TEXT,
                    contract_json TEXT,
                    local_path TEXT,
                    feishu_attachment_json TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS original_first_frame_binding (
                    script_id TEXT PRIMARY KEY,
                    source_record_id TEXT,
                    asset_fingerprint TEXT,
                    asset_id TEXT,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"""
            )

    def get_ready_asset(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM original_first_frame_asset WHERE asset_fingerprint=? AND status='READY'",
                (fingerprint,),
            ).fetchone()
        return dict(row) if row else None

    def recover_stale_generating(self, *, stale_after_seconds: int) -> list[Dict[str, Any]]:
        """Close assets left GENERATING by an interrupted runner.

        The binding is written before image generation starts, so a later run
        can also restore the corresponding Feishu row to a terminal state.
        """
        self.ensure_schema()
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(60, stale_after_seconds))
        ).strftime("%Y-%m-%d %H:%M:%S")
        message = "STALE_GENERATING_RECOVERED: 上次首帧进程中断或超时，已自动收口，可重新执行"
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT a.asset_fingerprint, a.asset_id, a.updated_at,
                          b.script_id, b.source_record_id
                   FROM original_first_frame_asset a
                   LEFT JOIN original_first_frame_binding b
                     ON b.asset_fingerprint=a.asset_fingerprint
                   WHERE a.status='GENERATING' AND a.updated_at<=?
                   ORDER BY a.updated_at""",
                (cutoff,),
            ).fetchall()
            fingerprints = sorted({str(row["asset_fingerprint"]) for row in rows})
            now = _now()
            for fingerprint in fingerprints:
                conn.execute(
                    """UPDATE original_first_frame_asset
                       SET status='FAILED', error_message=?, updated_at=?
                       WHERE asset_fingerprint=? AND status='GENERATING'""",
                    (message, now, fingerprint),
                )
                conn.execute(
                    """UPDATE original_first_frame_binding
                       SET status='FAILED', updated_at=?
                       WHERE asset_fingerprint=? AND status='GENERATING'""",
                    (now, fingerprint),
                )
        return [dict(row) for row in rows]

    def upsert_asset(self, *, fingerprint: str, asset_id: str, status: str, **values: Any) -> None:
        self.ensure_schema()
        allowed = {
            "model", "prompt_version", "prompt_text", "contract_json", "local_path",
            "feishu_attachment_json", "error_message",
        }
        payload = {key: values.get(key) for key in allowed}
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO original_first_frame_asset (
                    asset_fingerprint, asset_id, status, model, prompt_version,
                    prompt_text, contract_json, local_path, feishu_attachment_json,
                    error_message, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(asset_fingerprint) DO UPDATE SET
                    asset_id=excluded.asset_id, status=excluded.status,
                    model=excluded.model, prompt_version=excluded.prompt_version,
                    prompt_text=excluded.prompt_text, contract_json=excluded.contract_json,
                    local_path=excluded.local_path,
                    feishu_attachment_json=excluded.feishu_attachment_json,
                    error_message=excluded.error_message, updated_at=excluded.updated_at""",
                (
                    fingerprint, asset_id, status, payload["model"], payload["prompt_version"],
                    payload["prompt_text"], payload["contract_json"], payload["local_path"],
                    payload["feishu_attachment_json"], payload["error_message"], _now(), _now(),
                ),
            )

    def bind(self, *, script_id: str, source_record_id: str, fingerprint: str, asset_id: str, status: str) -> None:
        self.ensure_schema()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO original_first_frame_binding (
                    script_id, source_record_id, asset_fingerprint, asset_id, status, updated_at
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(script_id) DO UPDATE SET
                    source_record_id=excluded.source_record_id,
                    asset_fingerprint=excluded.asset_fingerprint,
                    asset_id=excluded.asset_id, status=excluded.status,
                    updated_at=excluded.updated_at""",
                (script_id, source_record_id, fingerprint, asset_id, status, _now()),
            )
