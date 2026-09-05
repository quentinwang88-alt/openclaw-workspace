from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_DB_PATH = Path("/Users/likeu3/.openclaw/shared/data/longform_original_video.sqlite3")
DEFAULT_ASSET_ROOT = Path("/Users/likeu3/.openclaw/shared/data/longform_original_video")


class LongformStorage:
    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS longform_job (
              job_id TEXT PRIMARY KEY,
              product_code TEXT NOT NULL,
              status TEXT NOT NULL,
              master_contract_json TEXT NOT NULL,
              plan_json TEXT NOT NULL,
              keyframe_package_json TEXT NOT NULL,
              voiceover_json TEXT NOT NULL DEFAULT '{}',
              merged_video_path TEXT NOT NULL DEFAULT '',
              final_video_path TEXT NOT NULL DEFAULT '',
              execution_report_json TEXT NOT NULL DEFAULT '{}',
              error_json TEXT NOT NULL DEFAULT '{}',
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS longform_segment (
              job_id TEXT NOT NULL,
              segment_id TEXT NOT NULL,
              status TEXT NOT NULL,
              duration_seconds INTEGER NOT NULL,
              generation_mode TEXT NOT NULL,
              prompt TEXT NOT NULL,
              start_frame_path TEXT NOT NULL DEFAULT '',
              end_frame_path TEXT NOT NULL DEFAULT '',
              platform_task_id TEXT NOT NULL DEFAULT '',
              submit_fingerprint TEXT NOT NULL DEFAULT '',
              output_video_path TEXT NOT NULL DEFAULT '',
              platform_response_json TEXT NOT NULL DEFAULT '{}',
              error_json TEXT NOT NULL DEFAULT '{}',
              updated_at INTEGER NOT NULL,
              PRIMARY KEY (job_id, segment_id),
              FOREIGN KEY (job_id) REFERENCES longform_job(job_id)
            );
            """)
            columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(longform_job)")
            }
            if "execution_report_json" not in columns:
                conn.execute(
                    "ALTER TABLE longform_job ADD COLUMN execution_report_json TEXT NOT NULL DEFAULT '{}'"
                )

    def save_plan(self, job_id: str, master: Dict[str, Any], plan: Dict[str, Any],
                  keyframes: Dict[str, Any]) -> None:
        now = int(time.time())
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO longform_job
                (job_id, product_code, status, master_contract_json, plan_json,
                 keyframe_package_json, created_at, updated_at)
                VALUES (?, ?, 'PLANNED', ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET master_contract_json=excluded.master_contract_json,
                  plan_json=excluded.plan_json, keyframe_package_json=excluded.keyframe_package_json,
                  updated_at=excluded.updated_at""",
                (job_id, master["product_code"], json.dumps(master, ensure_ascii=False),
                 json.dumps(plan, ensure_ascii=False), json.dumps(keyframes, ensure_ascii=False), now, now),
            )
            for segment in plan["segments"]:
                conn.execute(
                    """INSERT INTO longform_segment
                    (job_id, segment_id, status, duration_seconds, generation_mode, prompt, updated_at)
                    VALUES (?, ?, 'PLANNED', ?, ?, ?, ?)
                    ON CONFLICT(job_id, segment_id) DO UPDATE SET
                      duration_seconds=excluded.duration_seconds,
                      generation_mode=excluded.generation_mode,
                      prompt=excluded.prompt, updated_at=excluded.updated_at""",
                    (job_id, segment["segment_id"], segment["duration_seconds"],
                     segment["generation_mode"], segment["video_prompt"], now),
                )

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM longform_job WHERE job_id=?", (job_id,)).fetchone()
            if not row:
                return None
            result = dict(row)
            result["segments"] = [dict(item) for item in conn.execute(
                "SELECT * FROM longform_segment WHERE job_id=? ORDER BY segment_id", (job_id,)
            )]
            return result

    def list_batch_jobs(self, batch_id: str) -> list[Dict[str, Any]]:
        """Resume the frozen jobs, never rediscover sources by product recency."""
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT * FROM longform_job
                   WHERE json_extract(master_contract_json, '$.workbench_request.batch_id')=?
                   ORDER BY CAST(json_extract(master_contract_json,
                     '$.workbench_request.item_index') AS INTEGER)""", (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_segment(self, job_id: str, segment_id: str, **fields: Any) -> None:
        allowed = {
            "status", "start_frame_path", "end_frame_path", "platform_task_id",
            "submit_fingerprint", "output_video_path", "platform_response_json", "error_json",
        }
        chosen = {key: value for key, value in fields.items() if key in allowed}
        if not chosen:
            return
        chosen["updated_at"] = int(time.time())
        assignments = ", ".join(f"{key}=?" for key in chosen)
        with self.connect() as conn:
            conn.execute(
                f"UPDATE longform_segment SET {assignments} WHERE job_id=? AND segment_id=?",
                (*chosen.values(), job_id, segment_id),
            )

    def update_job(self, job_id: str, status: str, **fields: Any) -> None:
        allowed = {
            "voiceover_json", "merged_video_path", "final_video_path",
            "execution_report_json", "error_json",
        }
        chosen = {key: value for key, value in fields.items() if key in allowed}
        chosen.update(status=status, updated_at=int(time.time()))
        assignments = ", ".join(f"{key}=?" for key in chosen)
        with self.connect() as conn:
            conn.execute(f"UPDATE longform_job SET {assignments} WHERE job_id=?", (*chosen.values(), job_id))
