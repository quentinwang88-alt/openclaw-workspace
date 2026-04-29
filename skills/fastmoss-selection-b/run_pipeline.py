#!/usr/bin/env python3
"""FastMoss 选品方案 B 入口。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from app.config import get_settings  # noqa: E402
from app.db import Database  # noqa: E402
from app.pipeline import FastMossPipeline  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FastMoss 选品方案 B")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="初始化 SQLite")

    run_once = subparsers.add_parser("run-once", help="扫描批次、下载附件、导入并生成 shortlist")
    run_once.add_argument("--batch-id", help="只处理指定 batch_id")
    run_once.add_argument("--limit", type=int, help="限制处理批次数")
    run_once.add_argument("--skip-accio", action="store_true", help="只做第一阶段，不发 Accio 请求")

    collect_accio = subparsers.add_parser("collect-accio", help="回收 Accio 回复并回写工作台")
    collect_accio.add_argument("--batch-id", help="只处理指定 batch_id")
    collect_accio.add_argument("--limit", type=int, help="限制处理批次数")
    collect_accio.add_argument("--skip-hermes", action="store_true", help="只回收 Accio，不跑 Hermes")

    run_hermes = subparsers.add_parser("run-hermes", help="对已回收 Accio 的批次执行 Hermes")
    run_hermes.add_argument("--batch-id", help="只处理指定 batch_id")
    run_hermes.add_argument("--limit", type=int, help="限制处理批次数")

    subparsers.add_parser("sync-followup", help="把工作台中进入跟进的商品同步到复盘表")
    subparsers.add_parser("cleanup-archives", help="清理过期 runs 目录")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()

    if args.command == "init-db":
        database = Database(settings.database_url)
        database.init_schema()
        print(json.dumps({"status": "ok", "database_url": settings.database_url}, ensure_ascii=False))
        return

    pipeline = FastMossPipeline.from_settings(settings)
    if args.command == "run-once":
        result = pipeline.process_pending_batches(
            batch_id=args.batch_id,
            limit=args.limit,
            send_accio=not args.skip_accio,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "collect-accio":
        result = pipeline.collect_accio_results(
            batch_id=args.batch_id,
            limit=args.limit,
            run_hermes=not args.skip_hermes,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "run-hermes":
        result = pipeline.run_hermes_for_batches(batch_id=args.batch_id, limit=args.limit)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "sync-followup":
        result = pipeline.sync_followups()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "cleanup-archives":
        result = pipeline.cleanup_archives()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    parser.error("未知命令")


if __name__ == "__main__":
    main()
