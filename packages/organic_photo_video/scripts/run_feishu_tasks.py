#!/usr/bin/env python3
"""Process state-driven OPV tasks from the compact Feishu workbench."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(WORKSPACE_ROOT), str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.feishu_workflow import FeishuTaskWorkflow  # noqa: E402
from services.main_schedule_bridge import MainScheduleBridge  # noqa: E402
from services.operation_product_pack_source import (  # noqa: E402
    DEFAULT_OPERATION_TABLE_ID,
    DEFAULT_OPERATION_WIKI_TOKEN,
    FeishuOperationProductPackSource,
)
from services.product_reference_resolver import ProductReferenceResolver  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default="TR10wxEXHiCYIhk8clActVdenpc")
    parser.add_argument("--table-id", default="tblj3x846gU3rshB")
    parser.add_argument(
        "--operation-wiki-token", default=DEFAULT_OPERATION_WIKI_TOKEN,
        help="短视频运营任务表 wiki token（只读商品图片来源）",
    )
    parser.add_argument(
        "--operation-table-id", default=DEFAULT_OPERATION_TABLE_ID,
        help="短视频运营任务表 table id（只读商品图片来源）",
    )
    parser.add_argument("--record-id", default="")
    parser.add_argument(
        "--resume-running",
        action="store_true",
        help="resume one explicitly selected record stuck in 生成中; requires --record-id",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.resume_running and not args.record_id:
        parser.error("--resume-running requires --record-id")
    client = FeishuBitableClient(resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)
    repository = RdsRepository.from_env()
    scheduler = None
    product_reference_resolver = None
    if not args.dry_run:
        scheduler = MainScheduleBridge(repository)
        product_reference_resolver = ProductReferenceResolver(
            repository,
            candidate_source=FeishuOperationProductPackSource(
                client_factory=lambda: FeishuBitableClient(
                    resolve_wiki_bitable_app_token(args.operation_wiki_token),
                    args.operation_table_id,
                )
            ),
        )
    report = FeishuTaskWorkflow(
        repository, client, publish_scheduler=scheduler,
        product_reference_resolver=product_reference_resolver,
    ).scan(
        dry_run=args.dry_run,
        record_id=args.record_id,
        resume_running=args.resume_running,
    )
    report["feishu_request_count"] = client.request_count
    report["feishu_retry_count"] = client.retry_count
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 2 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
