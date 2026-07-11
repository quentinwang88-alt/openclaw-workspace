#!/usr/bin/env python3
"""Initialize Creator CRM RDS/SQLite schema."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from workspace_support import load_repo_env

load_repo_env()

from core.creator_repository import CreatorRepository, DEFAULT_TABLE_PREFIX


def main() -> int:
    parser = argparse.ArgumentParser(description="初始化 Creator CRM 长期达人池 RDS schema")
    parser.add_argument(
        "--database-url",
        default="",
        help="MySQL/RDS URL；默认读取 CREATOR_CRM_DATABASE_URL 或 LIKEU_AI_DATABASE_URL",
    )
    parser.add_argument(
        "--table-prefix",
        default=os.environ.get("CREATOR_CRM_TABLE_PREFIX", DEFAULT_TABLE_PREFIX),
        help=f"表名前缀，默认 {DEFAULT_TABLE_PREFIX}",
    )
    args = parser.parse_args()

    database_url = (
        args.database_url
        or os.environ.get("CREATOR_CRM_DATABASE_URL")
        or os.environ.get("LIKEU_AI_DATABASE_URL")
        or ""
    ).strip()
    if not database_url:
        print("❌ 缺少数据库 URL。请设置 CREATOR_CRM_DATABASE_URL 或传 --database-url")
        return 1

    repo = CreatorRepository(
        database_url,
        table_prefix=args.table_prefix,
        auto_create=True,
    )
    print("✅ Creator CRM 长期达人池 schema 初始化完成")
    print(f"   table_prefix: {repo.db.table_prefix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
