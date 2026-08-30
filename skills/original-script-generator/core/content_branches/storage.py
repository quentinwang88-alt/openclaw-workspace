"""Branch-neutral artifact storage with an injected, branch-owned DB path."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class BranchArtifactStorage:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS branch_run (
                    run_id TEXT PRIMARY KEY,
                    branch_key TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    product_code TEXT NOT NULL,
                    input_hash TEXT NOT NULL,
                    shared_kernel_version TEXT NOT NULL,
                    branch_policy_version TEXT NOT NULL,
                    branch_prompt_version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    result_json TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS branch_item (
                    item_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    branch_key TEXT NOT NULL,
                    item_index INTEGER NOT NULL,
                    intent_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    intent_json TEXT NOT NULL,
                    result_json TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(run_id, item_index),
                    FOREIGN KEY(run_id) REFERENCES branch_run(run_id)
                );
                CREATE TABLE IF NOT EXISTS branch_stage_artifact (
                    artifact_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    item_index INTEGER NOT NULL,
                    stage_key TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(item_id, stage_key, attempt),
                    FOREIGN KEY(run_id) REFERENCES branch_run(run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_branch_run_product
                ON branch_run(branch_key, product_code, created_at);
                CREATE INDEX IF NOT EXISTS idx_branch_item_run
                ON branch_item(run_id, item_index);
                CREATE INDEX IF NOT EXISTS idx_branch_stage_run
                ON branch_stage_artifact(run_id, item_index, stage_key, attempt);
                """
            )

    def start_run(self, row: Dict[str, Any]) -> None:
        now = _now()
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO branch_run (
                run_id, branch_key, request_id, product_code, input_hash,
                shared_kernel_version, branch_policy_version, branch_prompt_version,
                status, request_json, result_json, error_message, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["run_id"], row["branch_key"], row["request_id"],
                    row["product_code"], row["input_hash"], row["shared_kernel_version"],
                    row["branch_policy_version"], row["branch_prompt_version"],
                    row.get("status", "RUNNING"),
                    json.dumps(row.get("request") or {}, ensure_ascii=False, default=str),
                    None, "", now, now,
                ),
            )

    def save_item(self, row: Dict[str, Any]) -> None:
        now = _now()
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO branch_item (
                item_id, run_id, branch_key, item_index, intent_id, status,
                intent_json, result_json, error_code, error_message, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["item_id"], row["run_id"], row["branch_key"],
                    int(row["item_index"]), row["intent_id"], row["status"],
                    json.dumps(row.get("intent") or {}, ensure_ascii=False, default=str),
                    json.dumps(row.get("result") or {}, ensure_ascii=False, default=str)
                    if row.get("result") is not None else None,
                    row.get("error_code", ""), row.get("error_message", ""), now, now,
                ),
            )

    def save_stage_artifact(self, row: Dict[str, Any]) -> None:
        """Persist one immutable-addressed pipeline checkpoint for audit and retry."""
        now = _now()
        attempt = int(row.get("attempt") or 1)
        artifact_id = str(
            row.get("artifact_id")
            or f"{row['item_id']}:{row['stage_key']}:{attempt}"
        )
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO branch_stage_artifact (
                artifact_id, run_id, item_id, item_index, stage_key, attempt,
                status, payload_json, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(item_id, stage_key, attempt) DO UPDATE SET
                status=excluded.status,
                payload_json=excluded.payload_json,
                updated_at=excluded.updated_at""",
                (
                    artifact_id, row["run_id"], row["item_id"],
                    int(row["item_index"]), row["stage_key"], attempt,
                    row.get("status", "READY"),
                    json.dumps(row.get("payload") or {}, ensure_ascii=False, default=str),
                    now, now,
                ),
            )

    def finish_run(self, run_id: str, status: str, result: Dict[str, Any], error: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE branch_run SET status=?, result_json=?, error_message=?, updated_at=? WHERE run_id=?",
                (status, json.dumps(result, ensure_ascii=False, default=str), error, _now(), run_id),
            )

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM branch_run WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def list_items(self, run_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM branch_item WHERE run_id=? ORDER BY item_index", (run_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def list_stage_artifacts(self, run_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM branch_stage_artifact
                WHERE run_id=? ORDER BY item_index, stage_key, attempt""",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]
