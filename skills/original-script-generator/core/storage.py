#!/usr/bin/env python3
"""
原创脚本生成流水线的 SQLite 持久化层。
"""

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


def _shared_data_dir() -> Path:
    root = os.environ.get("OPENCLAW_SHARED_DATA_DIR", str(Path.home() / ".openclaw" / "shared" / "data"))
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_db_path() -> Path:
    override = os.environ.get("ORIGINAL_SCRIPT_GENERATOR_DB_PATH")
    if override:
        return Path(override)
    return _shared_data_dir() / "original_script_generator.sqlite3"


class PipelineStorage:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path) if db_path else default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id TEXT NOT NULL,
                    product_code TEXT,
                    top_category TEXT,
                    input_hash TEXT,
                    target_country TEXT,
                    target_language TEXT,
                    product_type TEXT,
                    request_status TEXT,
                    runtime_status TEXT,
                    error_message TEXT,
                    anchor_card_json TEXT,
                    strategy_cards_json TEXT,
                    content_ids_json TEXT,
                    exp_s1_json TEXT,
                    exp_s2_json TEXT,
                    exp_s3_json TEXT,
                    exp_s4_json TEXT,
                    raw_record_fields_json TEXT,
                    stage_durations_json TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS stage_results (
                    stage_result_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    record_id TEXT NOT NULL,
                    product_code TEXT,
                    stage_name TEXT NOT NULL,
                    stage_order INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    prompt_text TEXT,
                    input_context_json TEXT,
                    image_paths_json TEXT,
                    output_json TEXT,
                    rendered_text TEXT,
                    duration_seconds REAL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_record_id
                ON pipeline_runs(record_id);

                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_product_code
                ON pipeline_runs(product_code);

                CREATE INDEX IF NOT EXISTS idx_stage_results_run_id
                ON stage_results(run_id);

                CREATE INDEX IF NOT EXISTS idx_stage_results_product_code
                ON stage_results(product_code);

                CREATE INDEX IF NOT EXISTS idx_stage_results_stage_name
                ON stage_results(stage_name);
                """
            )
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(pipeline_runs)")}
            if "top_category" not in columns:
                conn.execute("ALTER TABLE pipeline_runs ADD COLUMN top_category TEXT")
            if "anchor_card_json" not in columns:
                conn.execute("ALTER TABLE pipeline_runs ADD COLUMN anchor_card_json TEXT")
            if "strategy_cards_json" not in columns:
                conn.execute("ALTER TABLE pipeline_runs ADD COLUMN strategy_cards_json TEXT")
            if "content_ids_json" not in columns:
                conn.execute("ALTER TABLE pipeline_runs ADD COLUMN content_ids_json TEXT")
            for column in ("exp_s1_json", "exp_s2_json", "exp_s3_json", "exp_s4_json"):
                if column not in columns:
                    conn.execute(f"ALTER TABLE pipeline_runs ADD COLUMN {column} TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_top_category ON pipeline_runs(top_category)"
            )

    @staticmethod
    def _now_string() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _json_dump(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False)

    def create_run(
        self,
        record_id: str,
        product_code: str,
        input_hash: str,
        context: Dict[str, Any],
        raw_record_fields: Dict[str, Any],
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO pipeline_runs (
                    record_id,
                    product_code,
                    top_category,
                    input_hash,
                    target_country,
                    target_language,
                    product_type,
                    request_status,
                    runtime_status,
                    raw_record_fields_json,
                    started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id,
                    product_code or None,
                    context.get("top_category", "") or None,
                    input_hash,
                    context.get("target_country", ""),
                    context.get("target_language", ""),
                    context.get("product_type", ""),
                    context.get("request_status", ""),
                    "started",
                    self._json_dump(raw_record_fields),
                    self._now_string(),
                ),
            )
            return int(cursor.lastrowid)

    def update_run_status(
        self,
        run_id: int,
        runtime_status: str,
        error_message: str = "",
        stage_durations: Optional[Dict[str, float]] = None,
        completed: bool = False,
    ) -> None:
        completed_at = self._now_string() if completed else None
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE pipeline_runs
                SET runtime_status = ?,
                    error_message = ?,
                    stage_durations_json = ?,
                    completed_at = COALESCE(?, completed_at)
                WHERE run_id = ?
                """,
                (
                    runtime_status,
                    error_message or None,
                    self._json_dump(stage_durations or {}),
                    completed_at,
                    run_id,
                ),
            )

    def update_run_artifacts(
        self,
        run_id: int,
        anchor_card: Optional[Dict[str, Any]] = None,
        strategy_cards: Optional[Dict[str, Any]] = None,
        expression_plans: Optional[Dict[str, Dict[str, Any]]] = None,
        content_ids: Optional[Dict[str, Any]] = None,
    ) -> None:
        updates: List[str] = []
        values: List[Any] = []
        if anchor_card is not None:
            updates.append("anchor_card_json = ?")
            values.append(self._json_dump(anchor_card))
        if strategy_cards is not None:
            updates.append("strategy_cards_json = ?")
            values.append(self._json_dump(strategy_cards))
        if expression_plans:
            for key in ("exp_s1_json", "exp_s2_json", "exp_s3_json", "exp_s4_json"):
                value = expression_plans.get(key)
                if value is not None:
                    updates.append(f"{key} = ?")
                    values.append(self._json_dump(value))
        if content_ids is not None:
            merged_content_ids = dict(self.get_run_content_ids(run_id) or {})
            merged_content_ids.update(content_ids)
            updates.append("content_ids_json = ?")
            values.append(self._json_dump(merged_content_ids))
        if not updates:
            return

        values.append(run_id)
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE pipeline_runs
                SET {', '.join(updates)}
                WHERE run_id = ?
                """,
                tuple(values),
            )

    def record_stage_result(
        self,
        run_id: int,
        record_id: str,
        product_code: str,
        stage_name: str,
        stage_order: int,
        status: str,
        prompt_text: str,
        input_context: Dict[str, Any],
        image_paths: List[str],
        output_json: Optional[Dict[str, Any]] = None,
        rendered_text: Optional[str] = None,
        duration_seconds: Optional[float] = None,
        error_message: str = "",
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO stage_results (
                    run_id,
                    record_id,
                    product_code,
                    stage_name,
                    stage_order,
                    status,
                    prompt_text,
                    input_context_json,
                    image_paths_json,
                    output_json,
                    rendered_text,
                    duration_seconds,
                    error_message,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    record_id,
                    product_code or None,
                    stage_name,
                    stage_order,
                    status,
                    prompt_text,
                    self._json_dump(input_context),
                    self._json_dump(image_paths),
                    self._json_dump(output_json) if output_json is not None else None,
                    rendered_text,
                    duration_seconds,
                    error_message or None,
                    self._now_string(),
                ),
            )

    def query_runs_by_product_code(self, product_code: str, limit: int = 20) -> List[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT *
                FROM pipeline_runs
                WHERE product_code = ?
                ORDER BY run_id DESC
                LIMIT ?
                """,
                (product_code, limit),
            )
            return cursor.fetchall()

    def query_runs_by_record_id(self, record_id: str, limit: int = 20) -> List[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT *
                FROM pipeline_runs
                WHERE record_id = ?
                ORDER BY run_id DESC
                LIMIT ?
                """,
                (record_id, limit),
            )
            return cursor.fetchall()

    def get_run_content_ids(self, run_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT content_ids_json
                FROM pipeline_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            if not row or not row["content_ids_json"]:
                return {}
            try:
                value = json.loads(row["content_ids_json"])
            except json.JSONDecodeError:
                return {}
            return value if isinstance(value, dict) else {}

    def query_stage_results(self, run_id: int) -> List[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT *
                FROM stage_results
                WHERE run_id = ?
                ORDER BY stage_order ASC, stage_result_id ASC
                """,
                (run_id,),
            )
            return cursor.fetchall()

    def get_latest_stage_output_json(
        self,
        record_id: str,
        stage_name: str,
        product_code: str = "",
    ) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT sr.output_json
                FROM stage_results sr
                JOIN pipeline_runs pr ON pr.run_id = sr.run_id
                WHERE sr.stage_name = ?
                  AND sr.status = 'success'
                  AND sr.output_json IS NOT NULL
                  AND (
                    sr.record_id = ?
                    OR (? <> '' AND sr.product_code = ?)
                  )
                ORDER BY sr.stage_result_id DESC
                LIMIT 1
                """,
                (stage_name, record_id, product_code, product_code),
            )
            row = cursor.fetchone()
            if not row or not row["output_json"]:
                return None
            return json.loads(row["output_json"])

    def get_latest_stage_output_json_for_input(
        self,
        record_id: str,
        stage_name: str,
        input_hash: str,
        product_code: str = "",
    ) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT sr.output_json
                FROM stage_results sr
                JOIN pipeline_runs pr ON pr.run_id = sr.run_id
                WHERE sr.stage_name = ?
                  AND sr.status = 'success'
                  AND sr.output_json IS NOT NULL
                  AND pr.input_hash = ?
                  AND (
                    sr.record_id = ?
                    OR (? <> '' AND sr.product_code = ?)
                  )
                ORDER BY sr.stage_result_id DESC
                LIMIT 1
                """,
                (stage_name, input_hash, record_id, product_code, product_code),
            )
            row = cursor.fetchone()
            if not row or not row["output_json"]:
                return None
            return json.loads(row["output_json"])
