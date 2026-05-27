#!/usr/bin/env python3
"""Small SQLite/MySQL compatibility helpers for the migrated agent stores."""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence
from urllib.parse import parse_qs, unquote, urlparse


MYSQL_SCHEMES = {"mysql", "mysql+pymysql"}


class CompatRow:
    def __init__(self, columns: Sequence[str], values: Sequence[Any]):
        self._columns = list(columns)
        self._values = tuple(values)
        self._index = {name: idx for idx, name in enumerate(self._columns)}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._index[key]]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> List[str]:
        return list(self._columns)


class CompatCursor:
    def __init__(self, cursor, forced_rows: Optional[List[CompatRow]] = None):
        self._cursor = cursor
        self._forced_rows = forced_rows
        self.lastrowid = getattr(cursor, "lastrowid", None)

    def fetchone(self):
        if self._forced_rows is not None:
            if not self._forced_rows:
                return None
            return self._forced_rows.pop(0)
        row = self._cursor.fetchone()
        return self._wrap_row(row)

    def fetchall(self):
        if self._forced_rows is not None:
            rows = self._forced_rows
            self._forced_rows = []
            return rows
        return [self._wrap_row(row) for row in self._cursor.fetchall()]

    def __iter__(self):
        return iter(self.fetchall())

    def _wrap_row(self, row):
        if row is None:
            return None
        columns = [item[0] for item in (self._cursor.description or [])]
        return CompatRow(columns, row)


class MySQLCompatConnection:
    def __init__(self, url: str, table_prefix: str, table_names: Iterable[str], timeout: int = 30):
        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("PyMySQL is required for MySQL/RDS mode. Run: python3 -m pip install --user pymysql") from exc

        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        database = parsed.path.lstrip("/")
        charset = query.get("charset", ["utf8mb4"])[0]
        self._database = database
        self._prefix = table_prefix
        self._table_names = sorted(set(table_names), key=len, reverse=True)
        self.row_factory = None
        self._conn = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            database=database,
            charset=charset,
            autocommit=False,
            connect_timeout=timeout,
            read_timeout=max(timeout, 30),
            write_timeout=max(timeout, 30),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        self._conn.close()
        return False

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        pragma = self._parse_pragma_table_info(sql)
        if pragma:
            return self._pragma_table_info(pragma)
        rewritten = self._rewrite_sql(sql)
        if rewritten is None:
            return CompatCursor(_NoopCursor())
        cursor = self._conn.cursor()
        cursor.execute(rewritten, tuple(params or ()))
        return CompatCursor(cursor)

    def executemany(self, sql: str, params: Sequence[Sequence[Any]]):
        rewritten = self._rewrite_sql(sql)
        if rewritten is None:
            return CompatCursor(_NoopCursor())
        cursor = self._conn.cursor()
        cursor.executemany(rewritten, params)
        return CompatCursor(cursor)

    def executescript(self, script: str):
        for statement in script.split(";"):
            if statement.strip():
                self.execute(statement)
        return CompatCursor(_NoopCursor())

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def _rewrite_sql(self, sql: str) -> Optional[str]:
        text = sql.strip()
        upper = re.sub(r"\s+", " ", text).upper()
        if upper.startswith("CREATE TABLE IF NOT EXISTS ") or upper.startswith("CREATE INDEX IF NOT EXISTS "):
            return None
        text = self._replace_tables(text)
        text = self._rewrite_upsert(text)
        text = text.replace("?", "%s")
        text = text.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER PRIMARY KEY AUTO_INCREMENT")
        return text

    def _replace_tables(self, sql: str) -> str:
        text = sql
        for name in self._table_names:
            prefixed = self._prefix + name
            text = re.sub(r"(?<![A-Za-z0-9_`]){name}(?![A-Za-z0-9_`])".format(name=re.escape(name)), prefixed, text)
        return text

    def _rewrite_upsert(self, sql: str) -> str:
        match = re.search(r"\s+ON\s+CONFLICT\s*\([^)]+\)\s+DO\s+UPDATE\s+SET\s+", sql, flags=re.IGNORECASE)
        if not match:
            return sql
        insert_part = sql[: match.start()]
        update_part = sql[match.end() :]
        update_part = re.sub(r"\bexcluded\.([A-Za-z_][A-Za-z0-9_]*)", r"VALUES(\1)", update_part)
        return insert_part + "\nON DUPLICATE KEY UPDATE\n" + update_part

    def _parse_pragma_table_info(self, sql: str) -> str:
        match = re.match(r"\s*PRAGMA\s+table_info\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$", sql, flags=re.IGNORECASE)
        return match.group(1) if match else ""

    def _pragma_table_info(self, table_name: str) -> CompatCursor:
        prefixed = self._prefix + table_name if not table_name.startswith(self._prefix) else table_name
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT column_name, column_type, is_nullable, column_default, column_key
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (self._database, prefixed),
        )
        rows = []
        for idx, row in enumerate(cursor.fetchall()):
            name, column_type, nullable, default, key = row
            rows.append(CompatRow(["cid", "name", "type", "notnull", "dflt_value", "pk"], [idx, name, column_type, 1 if nullable == "NO" else 0, default, 1 if key == "PRI" else 0]))
        return CompatCursor(_NoopCursor(), rows)


class _NoopCursor:
    lastrowid = None
    description = []

    def fetchone(self):
        return None

    def fetchall(self):
        return []


def resolve_database_url(value: Any, env_name: str) -> str:
    env_value = os.environ.get(env_name) or os.environ.get("LIKEU_AI_DATABASE_URL") or ""
    raw = str(value or "")
    if raw.startswith("mysql://") or raw.startswith("mysql+pymysql://"):
        return raw
    return env_value.strip()


def is_mysql_url(url: str) -> bool:
    return urlparse(url).scheme in MYSQL_SCHEMES


def connect_sqlite_or_mysql(
    db_path: Any,
    *,
    database_url: str = "",
    env_name: str = "",
    table_prefix: str = "",
    table_names: Iterable[str] = (),
    timeout: int = 30,
):
    url = database_url or resolve_database_url(db_path, env_name)
    if is_mysql_url(url):
        return MySQLCompatConnection(url, table_prefix=table_prefix, table_names=table_names, timeout=timeout)
    connection = sqlite3.connect(str(db_path), timeout=timeout)
    return connection


def database_exists(db_path: Any, database_url: str = "", env_name: str = "") -> bool:
    url = database_url or (os.environ.get(env_name) if env_name else "") or os.environ.get("LIKEU_AI_DATABASE_URL") or ""
    if is_mysql_url(url):
        return True
    return Path(db_path).exists()
