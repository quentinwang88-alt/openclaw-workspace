#!/usr/bin/env python3
"""轻量数据库封装，支持 SQLite 测试和 PostgreSQL 运行。"""

from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

from app.config import get_settings


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS creator_master (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    creator_key TEXT NOT NULL UNIQUE,
    creator_name TEXT NOT NULL,
    platform TEXT NOT NULL DEFAULT 'tiktok',
    country TEXT NOT NULL DEFAULT 'unknown',
    store TEXT NOT NULL DEFAULT '',
    first_seen_week TEXT,
    latest_seen_week TEXT,
    owner TEXT,
    status TEXT DEFAULT 'active',
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS creator_weekly_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_batch_id TEXT NOT NULL,
    stat_week TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    creator_name_raw TEXT NOT NULL,
    platform TEXT NOT NULL DEFAULT 'tiktok',
    country TEXT NOT NULL DEFAULT 'unknown',
    store TEXT NOT NULL DEFAULT '',
    gmv_raw TEXT,
    refund_amount_raw TEXT,
    order_count_raw TEXT,
    sold_item_count_raw TEXT,
    refunded_item_count_raw TEXT,
    avg_order_value_raw TEXT,
    avg_daily_sold_item_count_raw TEXT,
    video_count_raw TEXT,
    live_count_raw TEXT,
    estimated_commission_raw TEXT,
    shipped_sample_count_raw TEXT,
    row_hash TEXT,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS creator_weekly_clean (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stat_week TEXT NOT NULL,
    creator_id INTEGER NOT NULL REFERENCES creator_master(id),
    import_batch_id TEXT NOT NULL,
    store TEXT NOT NULL DEFAULT '',
    gmv NUMERIC NOT NULL DEFAULT 0,
    refund_amount NUMERIC NOT NULL DEFAULT 0,
    order_count INTEGER NOT NULL DEFAULT 0,
    sold_item_count INTEGER NOT NULL DEFAULT 0,
    refunded_item_count INTEGER NOT NULL DEFAULT 0,
    avg_order_value NUMERIC NOT NULL DEFAULT 0,
    avg_daily_sold_item_count NUMERIC NOT NULL DEFAULT 0,
    video_count INTEGER NOT NULL DEFAULT 0,
    live_count INTEGER NOT NULL DEFAULT 0,
    estimated_commission NUMERIC NOT NULL DEFAULT 0,
    shipped_sample_count INTEGER NOT NULL DEFAULT 0,
    content_action_count INTEGER NOT NULL DEFAULT 0,
    has_action INTEGER NOT NULL DEFAULT 0,
    has_result INTEGER NOT NULL DEFAULT 0,
    is_new_creator INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (stat_week, creator_id)
);

CREATE TABLE IF NOT EXISTS creator_weekly_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stat_week TEXT NOT NULL,
    creator_id INTEGER NOT NULL REFERENCES creator_master(id),
    store TEXT NOT NULL DEFAULT '',
    gmv NUMERIC NOT NULL DEFAULT 0,
    order_count INTEGER NOT NULL DEFAULT 0,
    content_action_count INTEGER NOT NULL DEFAULT 0,
    video_count INTEGER NOT NULL DEFAULT 0,
    live_count INTEGER NOT NULL DEFAULT 0,
    shipped_sample_count INTEGER NOT NULL DEFAULT 0,
    refund_rate NUMERIC,
    commission_rate NUMERIC,
    gmv_per_action NUMERIC,
    gmv_per_sample NUMERIC,
    items_per_order NUMERIC,
    gmv_wow NUMERIC,
    order_count_wow NUMERIC,
    action_count_wow NUMERIC,
    gmv_per_action_wow NUMERIC,
    refund_rate_wow NUMERIC,
    gmv_4w NUMERIC,
    order_count_4w INTEGER,
    action_count_4w INTEGER,
    avg_weekly_gmv_4w NUMERIC,
    avg_gmv_per_action_4w NUMERIC,
    avg_refund_rate_4w NUMERIC,
    gmv_lifetime NUMERIC,
    order_count_lifetime INTEGER,
    weeks_active_lifetime INTEGER,
    weeks_with_gmv_lifetime INTEGER,
    weeks_with_action_lifetime INTEGER,
    action_result_state TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (stat_week, creator_id)
);

CREATE TABLE IF NOT EXISTS creator_monitoring_result (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stat_week TEXT NOT NULL,
    creator_id INTEGER NOT NULL REFERENCES creator_master(id),
    store TEXT NOT NULL DEFAULT '',
    record_key TEXT NOT NULL,
    primary_tag TEXT NOT NULL,
    secondary_tags TEXT,
    risk_tags TEXT,
    priority_level TEXT,
    rule_version TEXT NOT NULL DEFAULT 'v1',
    decision_reason TEXT,
    next_action TEXT,
    owner TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (stat_week, creator_id)
);
"""


class Database:
    """统一数据库访问入口。"""

    def __init__(self, database_url: Optional[str] = None):
        self.settings = get_settings()
        self.database_url = database_url or self.settings.database_url
        self.driver = self._detect_driver(self.database_url)

    @staticmethod
    def _detect_driver(url: str) -> str:
        if url.startswith("postgresql://") or url.startswith("postgres://"):
            return "postgres"
        return "sqlite"

    def init_schema(self) -> None:
        if self.driver == "sqlite":
            db_path = self._sqlite_path()
            db_path.parent.mkdir(parents=True, exist_ok=True)
            with self.get_connection() as conn:
                conn.executescript(SQLITE_SCHEMA)
                self._ensure_sqlite_column(conn, "creator_master", "store", "TEXT NOT NULL DEFAULT ''")
                self._ensure_sqlite_column(conn, "creator_weekly_raw", "store", "TEXT NOT NULL DEFAULT ''")
                self._ensure_sqlite_column(conn, "creator_weekly_clean", "store", "TEXT NOT NULL DEFAULT ''")
                self._ensure_sqlite_column(conn, "creator_weekly_metrics", "store", "TEXT NOT NULL DEFAULT ''")
                self._ensure_sqlite_column(conn, "creator_monitoring_result", "store", "TEXT NOT NULL DEFAULT ''")
        else:
            init_sql = Path(__file__).resolve().parent.parent / "scripts" / "init_db.sql"
            with self.get_connection() as conn:
                conn.execute(init_sql.read_text(encoding="utf-8"))

    @staticmethod
    def _ensure_sqlite_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {row[1] for row in rows}
        if column_name not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

    def _sqlite_path(self) -> Path:
        prefix = "sqlite:///"
        if self.database_url.startswith(prefix):
            return Path(self.database_url[len(prefix):])
        return Path(self.database_url)

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        if self.driver == "sqlite":
            conn = sqlite3.connect(str(self._sqlite_path()))
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()
            return

        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("PostgreSQL 模式需要安装 psycopg") from exc

        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _adapt_query(self, query: str) -> str:
        if self.driver == "sqlite":
            return query
        return re.sub(r":([a-zA-Z_][a-zA-Z0-9_]*)", r"%(\1)s", query)

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        with self.get_connection() as conn:
            conn.execute(self._adapt_query(query), params or {})

    def executemany(self, query: str, params_list: Iterable[Dict[str, Any]]) -> None:
        with self.get_connection() as conn:
            conn.executemany(self._adapt_query(query), list(params_list))

    def fetchall(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute(self._adapt_query(query), params or {})
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def fetchone(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        rows = self.fetchall(query, params=params)
        return rows[0] if rows else None
