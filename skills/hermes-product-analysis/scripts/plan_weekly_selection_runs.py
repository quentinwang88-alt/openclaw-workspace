#!/usr/bin/env python3
"""Plan weekly Selection Agent runs from standardized Feishu snapshots.

This script is intentionally non-mutating.  It answers: which market/category
batches should run now, which should wait for Market Agent briefs, and which
should be skipped because Selection Agent already consumed them.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from selection.weekly_incremental_trigger import plan_selection_runs  # noqa: E402
from src.agent_data_store import AgentDataStore  # noqa: E402
from src.feishu import build_bitable_client  # noqa: E402
from src.standardized_snapshot import load_standard_product_snapshots_from_feishu  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan weekly Selection Agent incremental runs.")
    parser.add_argument("--feishu-url", required=True, help="标准化商品快照表 URL")
    parser.add_argument("--db-path", default=str(ROOT / "artifacts" / "agent_runtime.sqlite3"))
    parser.add_argument("--crawl-batch-id", default="")
    parser.add_argument("--market-id", default="")
    parser.add_argument("--category-id", default="")
    parser.add_argument("--retry-attempt", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    client = build_bitable_client(feishu_url=args.feishu_url)
    loaded = load_standard_product_snapshots_from_feishu(
        client=client,
        crawl_batch_id=args.crawl_batch_id,
        market_id=args.market_id.upper() if args.market_id else "",
        category_id=args.category_id,
        limit=args.limit or None,
    )
    store = AgentDataStore(Path(args.db_path))
    plans = plan_selection_runs(store, loaded.snapshots, retry_attempt=args.retry_attempt)
    print(
        json.dumps(
            {
                "db_path": str(Path(args.db_path)),
                "source_row_count": loaded.source_row_count,
                "ready_snapshot_count": len(loaded.snapshots),
                "skipped_count": len(loaded.skipped),
                "planned_batches": len(plans),
                "trigger_count": sum(1 for item in plans if item.get("trigger_selection_agent_run")),
                "plans": plans,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
