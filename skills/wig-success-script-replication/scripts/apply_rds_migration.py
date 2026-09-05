#!/usr/bin/env python3
"""Explicit, hash-confirmed installer for all wig replication migrations."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import pymysql


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = WORKSPACE_ROOT / "packages" / "wig_success_replication"
MIGRATION_DIR = PACKAGE_ROOT / "migrations"
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()


def database_url() -> str:
    value = (
        os.environ.get("WIG_REPLICATION_DATABASE_URL")
        or os.environ.get("LIKEU_AI_DATABASE_URL")
        or ""
    ).strip()
    if not value:
        raise RuntimeError("WIG_REPLICATION_DATABASE_URL or LIKEU_AI_DATABASE_URL is required")
    return value


def connect(url: str):
    parsed = urlparse(url)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise RuntimeError("only mysql+pymysql URLs are supported")
    query = parse_qs(parsed.query)
    return pymysql.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=unquote(parsed.path.lstrip("/")),
        charset=(query.get("charset") or ["utf8mb4"])[0],
        connect_timeout=10,
        read_timeout=60,
        write_timeout=60,
    )


def adopted_migrations(cursor) -> set[str]:
    """Recognize already-deployed pre-ledger schemas without replaying data UPDATEs."""
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name LIKE 'wsr\\_%'")
    tables = {row[0] for row in cursor.fetchall()}
    adopted = set()
    # Take exact names from the shipped initial migration, not a fuzzy table count.
    initial = (MIGRATION_DIR / "001_create_wsr_tables_mysql.sql").read_text(encoding="utf-8")
    required = set(re.findall(r"CREATE TABLE IF NOT EXISTS\s+(wsr_\w+)", initial, flags=re.I))
    if required <= tables:
        adopted.add("001_create_wsr_tables_mysql.sql")
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='wsr_replication_prompt'")
    columns = {row[0] for row in cursor.fetchall()}
    if {"sequence_no", "replication_mode", "creative_route", "creative_signature_json", "planner_version"} <= columns:
        cursor.execute("SELECT COUNT(*) FROM wsr_replication_prompt WHERE sequence_no IS NULL OR replication_mode IS NULL OR creative_signature_json IS NULL OR planner_version IS NULL")
        if cursor.fetchone()[0] == 0:
            adopted.add("002_add_replication_modes_mysql.sql")
    return adopted


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply the idempotent WSR RDS migration")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-sha256", default="")
    parser.add_argument("--inspect", action="store_true", help="Read deployed schema without writing")
    args = parser.parse_args()
    migrations = sorted(MIGRATION_DIR.glob("*.sql"))
    if not migrations:
        raise RuntimeError(f"no migrations found in {MIGRATION_DIR}")
    sources = [(path, path.read_text(encoding="utf-8")) for path in migrations]
    digest_input = "".join(f"{path.name}\n{sql}\n" for path, sql in sources)
    digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()
    statements: list[tuple[Path, str]] = []
    for path, sql in sources:
        executable_sql = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        )
        statements.extend(
            (path, part.strip())
            for part in executable_sql.split(";")
            if part.strip()
        )
    print("migrations=" + ",".join(path.name for path in migrations))
    print(f"sha256={digest}")
    print(f"statement_count={len(statements)}")
    if not args.apply:
        print("mode=dry-run")
        if args.inspect:
            with connect(database_url()) as connection, connection.cursor() as cursor:
                print("existing_migrations=" + ",".join(sorted(adopted_migrations(cursor))))
        return 0
    if args.confirm_sha256 != digest:
        raise RuntimeError("--confirm-sha256 must exactly match the dry-run hash")
    skipped = 0
    with connect(database_url()) as connection, connection.cursor() as cursor:
        adopted = adopted_migrations(cursor)
        cursor.execute("CREATE TABLE IF NOT EXISTS wsr_schema_migration (name VARCHAR(191) PRIMARY KEY, sha256 CHAR(64) NOT NULL, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("SELECT name, sha256 FROM wsr_schema_migration")
        applied = dict(cursor.fetchall())
        for path, sql in sources:
            source_digest = hashlib.sha256(sql.encode()).hexdigest()
            if path.name in applied:
                if applied[path.name] != source_digest:
                    raise RuntimeError(f"applied migration changed: {path.name}")
                skipped += 1
                continue
            if path.name not in adopted:
                for _, statement in (item for item in statements if item[0] == path):
                    try:
                        cursor.execute(statement)
                    except pymysql.MySQLError as exc:
                        code = int(exc.args[0]) if exc.args else 0
                        duplicate_add = code in {1060, 1061}
                        already_dropped = code == 1091 and "DROP INDEX" in statement.upper()
                        if duplicate_add or already_dropped:
                            skipped += 1
                            continue
                        raise RuntimeError(f"migration failed in {path.name}: {exc}") from exc
            cursor.execute("INSERT INTO wsr_schema_migration (name,sha256) VALUES (%s,%s)", (path.name, source_digest))
            # DDL auto-commits in MySQL; record each completed migration so a
            # failed later migration never replays the legacy data backfill.
            connection.commit()
        connection.commit()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema=DATABASE() AND table_name LIKE 'wsr\\_%' ORDER BY table_name"
        )
        tables = [row[0] for row in cursor.fetchall()]
    print("mode=applied")
    print(f"already_present_statements={skipped}")
    print("tables=" + ",".join(tables))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
