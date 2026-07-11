#!/usr/bin/env python3
"""Cleanup expired Creator CRM process assets from OSS."""

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
from core.oss_assets import CreatorAssetStorage


def main() -> int:
    parser = argparse.ArgumentParser(description="清理 Creator CRM 已过期 OSS 过程资产")
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
    parser.add_argument("--limit", type=int, default=200, help="单次最多清理资产数")
    parser.add_argument("--asset-type", default="grid_image", help="资产类型，默认 grid_image")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不删除 OSS，不改 RDS")
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

    repo = CreatorRepository(database_url, table_prefix=args.table_prefix, auto_create=True)
    storage = CreatorAssetStorage()
    assets = repo.list_expired_assets(limit=args.limit, asset_type=args.asset_type)

    print(f"🧹 待清理资产: {len(assets)}")
    deleted = 0
    failed = 0
    for asset in assets:
        asset_id = asset.get("asset_id")
        object_key = asset.get("object_key")
        provider = asset.get("storage_provider")
        print(f"  - {asset_id} {provider} {object_key}")
        if args.dry_run:
            continue
        try:
            storage.delete(object_key)
            repo.mark_asset_deleted(asset_id)
            deleted += 1
        except Exception as exc:
            failed += 1
            print(f"    ⚠️ 清理失败: {exc}")

    print(f"✅ 清理完成: deleted={deleted}, failed={failed}, dry_run={args.dry_run}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
