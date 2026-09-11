from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Tuple


DEFAULT_DB = Path("/Users/likeu3/.openclaw/shared/data/remake_video_execution.sqlite3")


class RemakeExecutionRepository:
    def __init__(self, db_path: str | Path = DEFAULT_DB):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def ensure_schema(self) -> None:
        with self.connect() as connection:
            connection.executescript("""
            CREATE TABLE IF NOT EXISTS remake_job (
              job_id TEXT PRIMARY KEY, source_record_id TEXT NOT NULL,
              script_id TEXT NOT NULL, source_revision_hash TEXT NOT NULL,
              status TEXT NOT NULL, source_json TEXT NOT NULL, plan_json TEXT NOT NULL,
              final_video_path TEXT NOT NULL DEFAULT '', error_json TEXT NOT NULL DEFAULT '{}',
              created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              UNIQUE(source_record_id, source_revision_hash)
            );
            CREATE TABLE IF NOT EXISTS remake_segment (
              job_id TEXT NOT NULL, segment_id TEXT NOT NULL, status TEXT NOT NULL,
              request_json TEXT NOT NULL, submit_fingerprint TEXT NOT NULL DEFAULT '',
              platform_task_id TEXT NOT NULL DEFAULT '', output_video_path TEXT NOT NULL DEFAULT '',
              error_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL,
              PRIMARY KEY(job_id, segment_id)
            );
            CREATE TABLE IF NOT EXISTS submission_attempt (
              attempt_id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
              segment_id TEXT NOT NULL, fingerprint TEXT NOT NULL, status TEXT NOT NULL,
              platform_task_id TEXT NOT NULL DEFAULT '', response_json TEXT NOT NULL DEFAULT '{}',
              created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              UNIQUE(job_id, segment_id, fingerprint)
            );
            CREATE TABLE IF NOT EXISTS writeback_outbox (
              outbox_id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
              payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
              error_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
            );
            """)

    def save_plan(self, job_id: str, source: Dict[str, Any], plan: Dict[str, Any]) -> str:
        now = int(time.time())
        status = "NEEDS_INPUT" if any(
            issue.get("severity") == "BLOCK" for issue in plan.get("issues", [])
        ) else "PLANNED"
        with self.connect() as connection:
            connection.execute(
                "UPDATE remake_job SET status='SUPERSEDED',updated_at=? "
                "WHERE source_record_id=? AND source_revision_hash<>? AND status IN ('PLANNED','NEEDS_INPUT')",
                (now, source["record_id"], source["source_revision_hash"]),
            )
            existing = connection.execute(
                "SELECT job_id FROM remake_job WHERE source_record_id=? AND source_revision_hash=?",
                (source["record_id"], source["source_revision_hash"]),
            ).fetchone()
            if existing:
                connection.execute(
                    "UPDATE remake_job SET status=?,plan_json=?,updated_at=? WHERE job_id=?",
                    (status, json.dumps(plan, ensure_ascii=False), now, existing["job_id"]),
                )
                return str(existing["job_id"])
            connection.execute(
                "INSERT INTO remake_job(job_id,source_record_id,script_id,source_revision_hash,status,source_json,plan_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (job_id, source["record_id"], source["script_id"], source["source_revision_hash"],
                 status, json.dumps(source, ensure_ascii=False), json.dumps(plan, ensure_ascii=False), now, now),
            )
            for segment in plan.get("segments", []):
                connection.execute(
                    "INSERT INTO remake_segment(job_id,segment_id,status,request_json,updated_at) VALUES(?,?,?,?,?)",
                    (job_id, segment["segment_id"], "PLANNED", json.dumps(segment, ensure_ascii=False), now),
                )
        return job_id

    def reserve_submission(self, job_id: str, segment_id: str, fingerprint: str) -> str:
        """Reserve one paid request; an existing uncertain attempt is never retried."""
        status, _created = self.claim_submission(job_id, segment_id, fingerprint)
        return status

    def claim_submission(self, job_id: str, segment_id: str, fingerprint: str) -> Tuple[str, bool]:
        """Atomically claim a request and say whether this caller created the claim."""
        now = int(time.time())
        with self.connect() as connection:
            row = connection.execute(
                "SELECT status FROM submission_attempt WHERE job_id=? AND segment_id=? AND fingerprint=?",
                (job_id, segment_id, fingerprint),
            ).fetchone()
            if row:
                return str(row["status"]), False
            connection.execute(
                "INSERT INTO submission_attempt(job_id,segment_id,fingerprint,status,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (job_id, segment_id, fingerprint, "SUBMITTING", now, now),
            )
        return "SUBMITTING", True

    def mark_submission(
        self, job_id: str, segment_id: str, fingerprint: str, *,
        status: str, platform_task_id: str = "", response: Dict[str, Any] | None = None,
    ) -> None:
        if status not in {"SUBMITTED", "SUBMISSION_UNKNOWN", "FAILED_BEFORE_SUBMIT"}:
            raise ValueError(f"不支持的提交状态: {status}")
        with self.connect() as connection:
            connection.execute(
                "UPDATE submission_attempt SET status=?,platform_task_id=?,response_json=?,updated_at=? WHERE job_id=? AND segment_id=? AND fingerprint=?",
                (status, platform_task_id, json.dumps(response or {}, ensure_ascii=False), int(time.time()),
                 job_id, segment_id, fingerprint),
            )
            segment_status = {
                "SUBMITTED": "SUBMITTED",
                "SUBMISSION_UNKNOWN": "SUBMISSION_UNKNOWN",
                "FAILED_BEFORE_SUBMIT": "PLANNED",
            }[status]
            connection.execute(
                "UPDATE remake_segment SET status=?,submit_fingerprint=?,platform_task_id=?,error_json=?,updated_at=? "
                "WHERE job_id=? AND segment_id=?",
                (segment_status, fingerprint, platform_task_id,
                 json.dumps(response or {}, ensure_ascii=False) if status != "SUBMITTED" else "{}",
                 int(time.time()), job_id, segment_id),
            )
