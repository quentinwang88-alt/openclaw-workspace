#!/usr/bin/env python3
"""周任务入口。"""

from __future__ import annotations

import argparse
from typing import Dict, Optional

from app.config import get_settings
from app.db import Database
from app.services.clean_loader import build_clean_records, sync_creator_master
from app.services.feishu_sync import sync_current_action_table_to_feishu
from app.services.metrics_calculator import build_weekly_metrics
from app.services.raw_loader import load_raw_excel
from app.services.tag_engine import run_tag_engine
from app.services.threshold_calculator import calculate_market_thresholds


def run_weekly_creator_monitoring(
    stat_week: str,
    source_file_path: str,
    platform: str = "tiktok",
    country: str = "unknown",
    store: str = "",
    db: Optional[Database] = None,
) -> Dict[str, object]:
    database = db or Database()
    import_batch_id = load_raw_excel(stat_week, source_file_path, platform, country, store=store, db=database)
    sync_creator_master(import_batch_id, db=database)
    build_clean_records(stat_week, import_batch_id, store=store, db=database)
    build_weekly_metrics(stat_week, store=store, db=database)
    thresholds = calculate_market_thresholds(stat_week, store=store, db=database)
    run_tag_engine(stat_week, thresholds, store=store, db=database)
    sync_summary = sync_current_action_table_to_feishu(stat_week, store=store, db=database)
    return {
        "stat_week": stat_week,
        "store": store,
        "import_batch_id": import_batch_id,
        "thresholds": thresholds,
        "sync_summary": sync_summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="达人经营监控助手 V1")
    parser.add_argument("--stat-week", required=True, help="统计周，例如 2026-W13")
    parser.add_argument("--source-file-path", required=True, help="周报 Excel 路径")
    parser.add_argument("--platform", default=get_settings().platform_default)
    parser.add_argument("--country", default=get_settings().country_default)
    parser.add_argument("--store", default=get_settings().store_default, help="店铺名，例如 泰国服装1店")
    parser.add_argument("--init-db", action="store_true", help="初始化数据库表结构")
    args = parser.parse_args()

    database = Database()
    if args.init_db:
        database.init_schema()

    result = run_weekly_creator_monitoring(
        stat_week=args.stat_week,
        source_file_path=args.source_file_path,
        platform=args.platform,
        country=args.country,
        store=args.store,
        db=database,
    )
    print(result)


if __name__ == "__main__":
    main()
