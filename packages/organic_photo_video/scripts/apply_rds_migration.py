#!/usr/bin/env python3
"""Hash-confirmed RDS schema installer for the Organic Photo Video flow."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import pymysql


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = WORKSPACE_ROOT / "packages" / "organic_photo_video"
MIGRATION_DIR = PACKAGE_ROOT / "migrations"
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()


def database_url() -> str:
    value = (
        os.environ.get("ORGANIC_PHOTO_VIDEO_DATABASE_URL")
        or os.environ.get("LIKEU_AI_DATABASE_URL")
        or ""
    ).strip()
    if not value:
        raise RuntimeError(
            "ORGANIC_PHOTO_VIDEO_DATABASE_URL or LIKEU_AI_DATABASE_URL is required"
        )
    return value


def connect(url: str):
    parsed = urlparse(url)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise RuntimeError("only mysql or mysql+pymysql URLs are supported")
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


def migration_sources() -> list[tuple[Path, str]]:
    migrations = sorted(MIGRATION_DIR.glob("*.sql"))
    if not migrations:
        raise RuntimeError(f"no migrations found in {MIGRATION_DIR}")
    return [(path, path.read_text(encoding="utf-8")) for path in migrations]


def executable_statements(
    sources: list[tuple[Path, str]],
) -> list[tuple[Path, str]]:
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
    return statements


def source_digest(name: str, sql: str) -> str:
    return hashlib.sha256(f"{name}\n{sql}\n".encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply the idempotent OPV RDS migration")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-sha256", default="")
    args = parser.parse_args()

    sources = migration_sources()
    digest_input = "".join(f"{path.name}\n{sql}\n" for path, sql in sources)
    digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()
    statements = executable_statements(sources)

    print("migrations=" + ",".join(path.name for path, _ in sources))
    print(f"sha256={digest}")
    print(f"statement_count={len(statements)}")
    if not args.apply:
        print("mode=dry-run")
        return 0
    if args.confirm_sha256 != digest:
        raise RuntimeError("--confirm-sha256 must exactly match the dry-run hash")

    with connect(database_url()) as connection, connection.cursor() as cursor:
        # Ledger: ALTER TABLE migrations are not re-runnable, so applied files
        # are recorded by name and skipped on subsequent installs.
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS _opv_schema_migrations ("
            "migration_name VARCHAR(191) PRIMARY KEY, "
            "sha256 CHAR(64) NOT NULL, applied_at DATETIME(6) NOT NULL "
            "DEFAULT CURRENT_TIMESTAMP(6))"
        )
        cursor.execute("SELECT migration_name, sha256 FROM _opv_schema_migrations")
        applied = {row[0]: row[1] for row in cursor.fetchall()}
        connection.commit()
        file_statements: dict = {}
        file_order: list = []
        for path, statement in statements:
            if path.name not in file_statements:
                file_statements[path.name] = []
                file_order.append(path.name)
            file_statements[path.name].append(statement)
        for name in file_order:
            source_sql = next(sql for path, sql in sources if path.name == name)
            file_digest = source_digest(name, source_sql)
            if name in applied:
                stored = applied[name]
                if stored == file_digest:
                    print(f"skip (already applied, checksum verified): {name}")
                    continue
                # 001/002 were installed by the original bundle-hash ledger.
                # Preserve that history once, but require per-file checksums
                # for every migration created after the installer upgrade.
                if name in {
                    "001_create_opv_tables_mysql.sql",
                    "002_add_recipe_outfit_package.sql",
                }:
                    print(f"skip (legacy bundle checksum, unverified): {name}")
                    continue
                raise RuntimeError(
                    f"applied migration checksum mismatch: {name}"
                )
            try:
                for statement in file_statements[name]:
                    cursor.execute(statement)
            except pymysql.MySQLError as exc:
                raise RuntimeError(f"migration failed in {name}: {exc}") from exc
            cursor.execute(
                "INSERT INTO _opv_schema_migrations (migration_name, sha256) "
                "VALUES (%s, %s)",
                (name, file_digest),
            )
            print(f"applied: {name}")
        connection.commit()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema=DATABASE() AND table_name LIKE 'opv\\_%' "
            "ORDER BY table_name"
        )
        tables = [row[0] for row in cursor.fetchall()]

    print("mode=applied")
    print("tables=" + ",".join(tables))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
