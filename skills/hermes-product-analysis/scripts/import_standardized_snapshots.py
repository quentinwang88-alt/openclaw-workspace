#!/usr/bin/env python3
"""Import standardized Feishu product snapshots for Market/Selection agents."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agent_data_store import MARKET_AGENT, SELECTION_AGENT, AgentDataStore  # noqa: E402
from src.feishu import build_bitable_client, parse_feishu_bitable_url, resolve_wiki_bitable_app_token  # noqa: E402
from src.standardized_snapshot import load_standard_product_snapshots_from_feishu  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Import standardized product snapshots into agent DB tables.")
    parser.add_argument("--feishu-url", required=True, help="标准化商品快照表 URL")
    parser.add_argument("--db-path", default=str(ROOT / "artifacts" / "agent_runtime.sqlite3"))
    parser.add_argument("--agent", choices=["market", "selection", "both"], default="both")
    parser.add_argument("--crawl-batch-id", default="")
    parser.add_argument("--market-id", default="")
    parser.add_argument("--category-id", default="")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    table_info = parse_feishu_bitable_url(args.feishu_url)
    if not table_info:
        raise ValueError("无法解析飞书 URL: {url}".format(url=args.feishu_url))
    app_token = table_info.app_token
    if "/wiki/" in table_info.original_url:
        app_token = resolve_wiki_bitable_app_token(app_token)

    client = build_bitable_client(feishu_url=args.feishu_url)
    result = load_standard_product_snapshots_from_feishu(
        client=client,
        crawl_batch_id=args.crawl_batch_id,
        market_id=args.market_id.upper() if args.market_id else "",
        category_id=args.category_id,
        limit=args.limit or None,
    )

    store = AgentDataStore(Path(args.db_path))
    agents = []
    if args.agent in {"market", "both"}:
        agents.append(MARKET_AGENT)
    if args.agent in {"selection", "both"}:
        agents.append(SELECTION_AGENT)

    imports = [
        store.import_snapshots(
            agent_name=agent,
            snapshots=result.snapshots,
            feishu_table_id=table_info.table_id,
            skipped_count=len(result.skipped),
        )
        for agent in agents
    ]
    print(
        json.dumps(
            {
                "db_path": str(Path(args.db_path)),
                "app_token": app_token,
                "table_id": table_info.table_id,
                "source_row_count": result.source_row_count,
                "ready_snapshot_count": len(result.snapshots),
                "skipped_count": len(result.skipped),
                "skipped_reasons": _count_reasons(result.skipped),
                "imports": imports,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _count_reasons(skipped):
    counts = {}
    for item in skipped:
        reason = str(item.get("reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return counts


if __name__ == "__main__":
    raise SystemExit(main())
