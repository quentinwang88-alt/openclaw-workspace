from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

from auto_mixcut.core.result import Result


JSON_FIELDS = {
    "product_anchor_json",
    "preferred_mood",
    "probe_json",
    "effective_roles_json",
    "secondary_roles_json",
    "raw_response",
    "response_json",
    "template_pool_json",
    "plan_json",
    "bgm_plan_json",
    "remix_plan_json",
    "final_qc_json",
    "available_markets",
    "available_placements",
    "mood_tags",
    "mood_tags_json",
    "genre_tags",
    "genre_tags_json",
    "category_tags",
    "category_tags_json",
    "template_tags",
    "template_tags_json",
    "ai_suggested_tags",
    "ai_suggested_tags_json",
    "tag_diff_json",
    "secondary_moods",
    "official_tags_json",
    "existing_human_tags_json",
    "allowed_labels_json",
    "performance_tags_json",
    "mix_constraints_json",
    "audio_attachment_json",
    "license_attachment_json",
    "audio_analysis_json",
    "allowed_core_roles_json",
    "allowed_soft_roles_json",
    "reference_asset_ids",
    "reference_image_oss_ids",
    "prompt_package_json",
    "anchor_ref_json",
    "perturbation_seed_json",
    "budget_json",
    "alert_json",
}


class SQLiteRepository:
    """Local RDS-compatible repository for development and tests."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def migrate(self, sql_path: Path) -> Result:
        try:
            with self.connect() as conn:
                conn.executescript(sql_path.read_text(encoding="utf-8"))
            return Result.ok({"db_path": str(self.db_path)})
        except Exception as exc:
            return Result.fail("MIGRATION_FAILED", str(exc), {"db_path": str(self.db_path)})

    def upsert(self, table: str, key: str, row: Dict[str, Any]) -> Result:
        now = datetime.utcnow().isoformat(timespec="seconds")
        row = dict(row)
        if "created_at" not in row:
            row["created_at"] = now
        if "updated_at" not in row and self._has_column(table, "updated_at"):
            row["updated_at"] = now
        payload = {k: self._encode(k, v) for k, v in row.items()}
        cols = list(payload)
        placeholders = ", ".join(["?"] * len(cols))
        updates = ", ".join([f"{c}=excluded.{c}" for c in cols if c != key])
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) ON CONFLICT({key}) DO UPDATE SET {updates}"
        try:
            with self.connect() as conn:
                conn.execute(sql, [payload[c] for c in cols])
            return Result.ok(row)
        except Exception as exc:
            return Result.fail("RDS_WRITE_FAILED", str(exc), {"table": table, "row": row})

    def insert(self, table: str, row: Dict[str, Any]) -> Result:
        row = dict(row)
        now = datetime.utcnow().isoformat(timespec="seconds")
        row.setdefault("created_at", now)
        if self._has_column(table, "updated_at"):
            row.setdefault("updated_at", now)
        payload = {k: self._encode(k, v) for k, v in row.items()}
        cols = list(payload)
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})"
        try:
            with self.connect() as conn:
                conn.execute(sql, [payload[c] for c in cols])
            return Result.ok(row)
        except Exception as exc:
            return Result.fail("RDS_WRITE_FAILED", str(exc), {"table": table, "row": row})

    def update(self, table: str, key: str, key_value: Any, values: Dict[str, Any]) -> Result:
        values = dict(values)
        if self._has_column(table, "updated_at"):
            values["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
        payload = {k: self._encode(k, v) for k, v in values.items()}
        sql = f"UPDATE {table} SET {', '.join([f'{c}=?' for c in payload])} WHERE {key}=?"
        try:
            with self.connect() as conn:
                conn.execute(sql, [*payload.values(), key_value])
            return Result.ok(values)
        except Exception as exc:
            return Result.fail("RDS_WRITE_FAILED", str(exc), {"table": table, "key": key, "key_value": key_value})

    def get(self, table: str, key: str, value: Any) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(f"SELECT * FROM {table} WHERE {key}=?", (value,)).fetchone()
        return self._row(row) if row else None

    def list_where(self, table: str, where: str = "1=1", params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {table} WHERE {where}", tuple(params)).fetchall()
        return [self._row(r) for r in rows]

    def _row(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        for key in list(data):
            if key in JSON_FIELDS and data[key] is not None:
                try:
                    data[key] = json.loads(data[key])
                except (TypeError, json.JSONDecodeError):
                    pass
        return data

    def _encode(self, key: str, value: Any) -> Any:
        if key in JSON_FIELDS and value is not None and not isinstance(value, str):
            return json.dumps(value, ensure_ascii=False)
        return value

    def _has_column(self, table: str, column: str) -> bool:
        with self.connect() as conn:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r["name"] == column for r in rows)


class MySQLRepository:
    """MySQL/RDS repository with the same small interface as SQLiteRepository."""

    dialect = "mysql"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        charset: str = "utf8mb4",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset

    @classmethod
    def from_url(cls, database_url: str) -> "MySQLRepository":
        parsed = urlparse(database_url)
        if parsed.scheme not in {"mysql", "mysql+pymysql"}:
            raise ValueError("database_url must start with mysql:// or mysql+pymysql://")
        query = parse_qs(parsed.query)
        return cls(
            host=parsed.hostname or "127.0.0.1",
            port=int(parsed.port or 3306),
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            database=(parsed.path or "").lstrip("/"),
            charset=(query.get("charset") or ["utf8mb4"])[0],
        )

    @contextmanager
    def connect(self):
        import pymysql

        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def migrate(self, sql_path: Path) -> Result:
        try:
            sql = sql_path.read_text(encoding="utf-8")
            with self.connect() as conn:
                for statement in _mysql_statements(sql):
                    with conn.cursor() as cur:
                        cur.execute(statement)
            return Result.ok({"database": self.database})
        except Exception as exc:
            return Result.fail("MIGRATION_FAILED", str(exc), {"database": self.database, "sql_path": str(sql_path)})

    def ensure_llm_router_tables(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS llm_cache (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              cache_key VARCHAR(128) NOT NULL UNIQUE,
              call_type VARCHAR(128),
              product_id VARCHAR(128),
              asset_id VARCHAR(128),
              segment_id VARCHAR(128),
              model_tier VARCHAR(64),
              model_name VARCHAR(256),
              prompt_version VARCHAR(64),
              input_hash VARCHAR(128),
              response_json JSON,
              created_at DATETIME,
              updated_at DATETIME
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS llm_call_logs (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              call_id VARCHAR(128) NOT NULL UNIQUE,
              call_type VARCHAR(128),
              route_policy VARCHAR(128),
              product_id VARCHAR(128),
              asset_id VARCHAR(128),
              segment_id VARCHAR(128),
              output_id VARCHAR(128),
              task_id VARCHAR(128),
              model_tier VARCHAR(64),
              model_name VARCHAR(256),
              provider VARCHAR(128),
              fallback_provider VARCHAR(128),
              prompt_version VARCHAR(64),
              input_hash VARCHAR(128),
              cache_hit TINYINT DEFAULT 0,
              result_status VARCHAR(64),
              error_code VARCHAR(128),
              error_message TEXT,
              retry_count INT DEFAULT 0,
              escalation_count INT DEFAULT 0,
              escalated_from VARCHAR(64),
              token_input INT DEFAULT 0,
              token_output INT DEFAULT 0,
              image_count INT DEFAULT 0,
              estimated_cost DECIMAL(12, 6) DEFAULT 0.0,
              latency_ms INT,
              created_at DATETIME
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS llm_calls (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              call_id VARCHAR(128) NOT NULL UNIQUE,
              task_id VARCHAR(128),
              product_id VARCHAR(128),
              asset_id VARCHAR(128),
              segment_id VARCHAR(128),
              output_id VARCHAR(128),
              call_type VARCHAR(128),
              model_tier VARCHAR(64),
              model_name VARCHAR(256),
              prompt_version VARCHAR(64),
              input_hash VARCHAR(128),
              cache_hit TINYINT DEFAULT 0,
              token_input INT,
              token_output INT,
              image_count INT,
              estimated_cost DECIMAL(12, 6),
              latency_ms INT,
              result_status VARCHAR(64),
              created_at DATETIME
            )
            """,
        ]
        with self.connect() as conn:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)

    def upsert(self, table: str, key: str, row: Dict[str, Any]) -> Result:
        now = datetime.utcnow().isoformat(timespec="seconds")
        row = dict(row)
        if "created_at" not in row:
            row["created_at"] = now
        if "updated_at" not in row and self._has_column(table, "updated_at"):
            row["updated_at"] = now
        payload = {k: self._encode(k, v) for k, v in row.items()}
        cols = list(payload)
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join([f"{c}=VALUES({c})" for c in cols if c != key])
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, [payload[c] for c in cols])
            return Result.ok(row)
        except Exception as exc:
            return Result.fail("RDS_WRITE_FAILED", str(exc), {"table": table, "row": row})

    def insert(self, table: str, row: Dict[str, Any]) -> Result:
        row = dict(row)
        now = datetime.utcnow().isoformat(timespec="seconds")
        row.setdefault("created_at", now)
        if self._has_column(table, "updated_at"):
            row.setdefault("updated_at", now)
        payload = {k: self._encode(k, v) for k, v in row.items()}
        cols = list(payload)
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, [payload[c] for c in cols])
            return Result.ok(row)
        except Exception as exc:
            return Result.fail("RDS_WRITE_FAILED", str(exc), {"table": table, "row": row})

    def update(self, table: str, key: str, key_value: Any, values: Dict[str, Any]) -> Result:
        values = dict(values)
        if self._has_column(table, "updated_at"):
            values["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
        payload = {k: self._encode(k, v) for k, v in values.items()}
        sql = f"UPDATE {table} SET {', '.join([f'{c}=%s' for c in payload])} WHERE {key}=%s"
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, [*payload.values(), key_value])
            return Result.ok(values)
        except Exception as exc:
            return Result.fail("RDS_WRITE_FAILED", str(exc), {"table": table, "key": key, "key_value": key_value})

    def get(self, table: str, key: str, value: Any) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table} WHERE {key}=%s", (value,))
                row = cur.fetchone()
        return self._row(row) if row else None

    def list_where(self, table: str, where: str = "1=1", params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
        where = where.replace("?", "%s")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table} WHERE {where}", tuple(params))
                rows = cur.fetchall()
        return [self._row(row) for row in rows]

    def _row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(row)
        for key in list(data):
            if key in JSON_FIELDS and data[key] is not None and isinstance(data[key], str):
                try:
                    data[key] = json.loads(data[key])
                except (TypeError, json.JSONDecodeError):
                    pass
        return data

    def _encode(self, key: str, value: Any) -> Any:
        if key in JSON_FIELDS and value is not None and not isinstance(value, str):
            return json.dumps(value, ensure_ascii=False)
        return value

    def _has_column(self, table: str, column: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s AND column_name=%s
                    LIMIT 1
                    """,
                    (self.database, table, column),
                )
                return cur.fetchone() is not None


def _mysql_statements(sql: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--") or stripped.upper().startswith("SOURCE "):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).rstrip(";"))
            current = []
    if current:
        statements.append("\n".join(current))
    return statements
