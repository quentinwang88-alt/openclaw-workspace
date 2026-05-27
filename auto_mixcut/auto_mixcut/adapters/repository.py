from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from auto_mixcut.core.result import Result


JSON_FIELDS = {
    "product_anchor_json",
    "probe_json",
    "effective_roles_json",
    "secondary_roles_json",
    "raw_response",
    "response_json",
    "template_pool_json",
    "plan_json",
    "allowed_core_roles_json",
    "allowed_soft_roles_json",
    "reference_asset_ids",
    "reference_image_oss_ids",
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
