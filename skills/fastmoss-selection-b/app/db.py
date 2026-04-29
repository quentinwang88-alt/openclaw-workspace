#!/usr/bin/env python3
"""SQLite 封装。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

from app.config import get_settings


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fm_batch (
    batch_id TEXT PRIMARY KEY,
    batch_record_id TEXT,
    data_source TEXT,
    country TEXT,
    category TEXT,
    snapshot_time TEXT,
    source_service TEXT,
    raw_file_name TEXT,
    raw_record_count INTEGER,
    download_status TEXT,
    download_time TEXT,
    local_file_path TEXT,
    file_hash TEXT,
    import_status TEXT,
    import_time TEXT,
    rule_status TEXT,
    accio_status TEXT,
    accio_chat_id TEXT,
    accio_requested_at TEXT,
    accio_response_at TEXT,
    hermes_status TEXT,
    hermes_completed_at TEXT,
    overall_status TEXT,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    last_updated_at TEXT,
    raw_record_json TEXT
);

CREATE TABLE IF NOT EXISTS fm_product_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT,
    shop_name TEXT,
    product_image TEXT,
    product_url TEXT,
    listing_time TEXT,
    listing_days INTEGER,
    price_raw TEXT,
    price_low_local REAL,
    price_high_local REAL,
    price_mid_local REAL,
    fx_rate_to_rmb REAL,
    price_low_rmb REAL,
    price_high_rmb REAL,
    price_mid_rmb REAL,
    sales_7d REAL,
    revenue_7d REAL,
    avg_price_7d_rmb REAL,
    total_sales REAL,
    total_revenue REAL,
    avg_price_total_rmb REAL,
    creator_count REAL,
    creator_order_rate REAL,
    video_count REAL,
    live_count REAL,
    commission_rate REAL,
    video_competition_density REAL,
    creator_competition_density REAL,
    import_warnings TEXT,
    raw_row_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_id, product_id)
);

CREATE TABLE IF NOT EXISTS selection_result_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_id TEXT NOT NULL UNIQUE,
    batch_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    country TEXT,
    category TEXT,
    product_name TEXT,
    shop_name TEXT,
    product_image TEXT,
    product_url TEXT,
    listing_days INTEGER,
    price_raw TEXT,
    price_low_local REAL,
    price_high_local REAL,
    price_mid_local REAL,
    fx_rate_to_rmb REAL,
    price_low_rmb REAL,
    price_high_rmb REAL,
    price_mid_rmb REAL,
    sales_7d REAL,
    revenue_7d REAL,
    avg_price_7d_rmb REAL,
    total_sales REAL,
    total_revenue REAL,
    avg_price_total_rmb REAL,
    creator_count REAL,
    creator_order_rate REAL,
    video_count REAL,
    live_count REAL,
    commission_rate REAL,
    pool_type TEXT,
    video_competition_density REAL,
    creator_competition_density REAL,
    competition_maturity TEXT,
    rule_score REAL,
    rule_pass_reason TEXT,
    rule_status TEXT,
    accio_status TEXT,
    accio_source_url TEXT,
    procurement_price_rmb REAL,
    procurement_price_range TEXT,
    match_confidence REAL,
    abnormal_low_price INTEGER,
    accio_note TEXT,
    pricing_reference_rmb REAL,
    platform_fee_rate REAL,
    platform_fee_amount REAL,
    head_shipping_rmb REAL,
    head_shipping_rule TEXT,
    gross_margin_amount REAL,
    gross_margin_rate REAL,
    distribution_margin_amount REAL,
    distribution_margin_rate REAL,
    hermes_status TEXT,
    content_potential_score REAL,
    differentiation_score REAL,
    fit_judgment TEXT,
    strategy_suggestion TEXT,
    recommended_action TEXT,
    recommendation_reason TEXT,
    risk_warning TEXT,
    manual_final_status TEXT,
    owner TEXT,
    manual_note TEXT,
    followup_flag INTEGER DEFAULT 0,
    processed_at TEXT,
    record_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS followup_result_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    followup_id TEXT NOT NULL UNIQUE,
    source_work_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    country TEXT,
    category TEXT,
    product_name TEXT,
    manual_final_status TEXT,
    followup_started_at TEXT,
    strategy TEXT,
    actual_procurement_price REAL,
    actual_sale_price REAL,
    content_test_result TEXT,
    creator_test_result TEXT,
    ad_test_result TEXT,
    review_7d TEXT,
    review_14d TEXT,
    review_30d TEXT,
    final_conclusion TEXT,
    writeback_rule_flag INTEGER DEFAULT 0,
    writeback_hermes_flag INTEGER DEFAULT 0,
    review_note TEXT,
    record_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

MIGRATION_COLUMNS = {
    "fm_product_snapshot": {
        "price_low_rmb": "REAL",
        "price_high_rmb": "REAL",
    },
    "selection_result_archive": {
        "price_low_rmb": "REAL",
        "price_high_rmb": "REAL",
        "pricing_reference_rmb": "REAL",
        "platform_fee_rate": "REAL",
        "platform_fee_amount": "REAL",
        "head_shipping_rmb": "REAL",
        "head_shipping_rule": "TEXT",
    },
}


class Database(object):
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or get_settings().database_url

    def _sqlite_path(self) -> Path:
        prefix = "sqlite:///"
        if self.database_url.startswith(prefix):
            return Path(self.database_url[len(prefix):])
        return Path(self.database_url)

    @contextmanager
    def get_connection(self) -> Iterator[sqlite3.Connection]:
        db_path = self._sqlite_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(db_path))
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def init_schema(self) -> None:
        with self.get_connection() as conn:
            conn.executescript(SCHEMA_SQL)
            self._ensure_migration_columns(conn)

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        with self.get_connection() as conn:
            conn.execute(query, params or {})

    def executemany(self, query: str, params_list: Iterable[Dict[str, Any]]) -> None:
        rows = list(params_list)
        if not rows:
            return
        with self.get_connection() as conn:
            conn.executemany(query, rows)

    def fetchall(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute(query, params or {})
            return [dict(row) for row in cursor.fetchall()]

    def fetchone(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        rows = self.fetchall(query, params)
        return rows[0] if rows else None

    def _table_columns(self, table: str) -> List[str]:
        with self.get_connection() as conn:
            rows = conn.execute("PRAGMA table_info({table})".format(table=table)).fetchall()
            return [row[1] for row in rows]

    def _ensure_migration_columns(self, conn: sqlite3.Connection) -> None:
        for table, columns in MIGRATION_COLUMNS.items():
            existing = {row[1] for row in conn.execute("PRAGMA table_info({table})".format(table=table)).fetchall()}
            for column, column_type in columns.items():
                if column in existing:
                    continue
                conn.execute(
                    "ALTER TABLE {table} ADD COLUMN {column} {column_type}".format(
                        table=table,
                        column=column,
                        column_type=column_type,
                    )
                )

    def _upsert(self, table: str, rows: Iterable[Dict[str, Any]], conflict_columns: List[str]) -> None:
        row_list = [row for row in rows if row]
        if not row_list:
            return
        table_columns = set(self._table_columns(table))
        columns = []  # type: List[str]
        for row in row_list:
            for key in row.keys():
                if key not in columns and key in table_columns:
                    columns.append(key)
        placeholders = ", ".join(":{name}".format(name=name) for name in columns)
        updates = []
        for column in columns:
            if column in conflict_columns:
                continue
            if column == "updated_at":
                updates.append("{name}=CURRENT_TIMESTAMP".format(name=column))
            else:
                updates.append("{name}=excluded.{name}".format(name=column))
        if "updated_at" in table_columns and "updated_at" not in columns:
            updates.append("updated_at=CURRENT_TIMESTAMP")
        query = (
            "INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
            "ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        ).format(
            table=table,
            columns=", ".join(columns),
            placeholders=placeholders,
            conflict=", ".join(conflict_columns),
            updates=", ".join(updates),
        )
        normalized_rows = []
        for row in row_list:
            normalized_rows.append({key: row.get(key) for key in columns})
        self.executemany(query, normalized_rows)

    def upsert_batch(self, row: Dict[str, Any]) -> None:
        self._upsert("fm_batch", [row], ["batch_id"])

    def upsert_product_snapshots(self, rows: Iterable[Dict[str, Any]]) -> None:
        self._upsert("fm_product_snapshot", rows, ["batch_id", "product_id"])

    def upsert_selection_records(self, rows: Iterable[Dict[str, Any]]) -> None:
        merged_rows = []
        for row in rows:
            if not row:
                continue
            work_id = row.get("work_id")
            if work_id:
                existing = self.get_selection_record(str(work_id))
                if existing:
                    merged = dict(existing)
                    merged.update(row)
                    merged_rows.append(merged)
                    continue
            merged_rows.append(row)
        self._upsert("selection_result_archive", merged_rows, ["work_id"])

    def upsert_followup_records(self, rows: Iterable[Dict[str, Any]]) -> None:
        merged_rows = []
        for row in rows:
            if not row:
                continue
            followup_id = row.get("followup_id")
            if followup_id:
                existing = self.fetchone(
                    "SELECT * FROM followup_result_archive WHERE followup_id = :followup_id",
                    {"followup_id": followup_id},
                )
                if existing:
                    merged = dict(existing)
                    merged.update(row)
                    merged_rows.append(merged)
                    continue
            merged_rows.append(row)
        self._upsert("followup_result_archive", merged_rows, ["followup_id"])

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        return self.fetchone("SELECT * FROM fm_batch WHERE batch_id = :batch_id", {"batch_id": batch_id})

    def list_selection_records(self, batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if batch_id:
            return self.fetchall(
                "SELECT * FROM selection_result_archive WHERE batch_id = :batch_id ORDER BY rule_score DESC, work_id ASC",
                {"batch_id": batch_id},
            )
        return self.fetchall("SELECT * FROM selection_result_archive ORDER BY batch_id DESC, rule_score DESC, work_id ASC")

    def get_selection_record(self, work_id: str) -> Optional[Dict[str, Any]]:
        return self.fetchone("SELECT * FROM selection_result_archive WHERE work_id = :work_id", {"work_id": work_id})

    def list_followup_records(self) -> List[Dict[str, Any]]:
        return self.fetchall("SELECT * FROM followup_result_archive ORDER BY created_at DESC, followup_id ASC")
