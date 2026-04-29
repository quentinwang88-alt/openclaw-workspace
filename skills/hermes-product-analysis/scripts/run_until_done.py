#!/usr/bin/env python3
"""Continuously process pending Hermes product-analysis rows until the queue is empty."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.candidate_detail_store import CandidateDetailStore  # noqa: E402
from src.hermes_analyzer import HermesAnalyzer  # noqa: E402
from src.market_direction_matcher import MarketDirectionMatcher  # noqa: E402
from src.pipeline import CandidateAnalysisPipeline  # noqa: E402
from src.result_writer import ResultWriter  # noqa: E402
from src.rule_checker import RuleChecker  # noqa: E402
from src.scoring_engine import ScoringEngine  # noqa: E402
from src.table_adapter import TableAdapter  # noqa: E402
from src.title_parser import TitleParser  # noqa: E402


DEFAULT_MARKET_INSIGHT_ARTIFACTS_ROOT = ROOT / "artifacts" / "market_insight"
DEFAULT_CANDIDATE_ANALYSIS_DB_PATH = ROOT / "artifacts" / "candidate_analysis" / "candidate_analysis.db"


def count_pending_rows(adapter: TableAdapter, config, record_scope: str, only_risk_tag: str = "") -> int:
    client = adapter.get_client(config)
    records = adapter.read_pending_records(
        config,
        client,
        limit=None,
        record_scope=record_scope,
        only_risk_tag=only_risk_tag,
    )
    return len(records)


def load_single_config(adapter: TableAdapter, config_dir: Path, table_id: str):
    configs = adapter.load_table_configs(config_dir)
    for config in configs:
        if config.table_id == table_id:
            return config
    raise ValueError(f"未找到启用的 table_id: {table_id}")


def build_pipeline() -> CandidateAnalysisPipeline:
    return CandidateAnalysisPipeline(
        table_adapter=TableAdapter(),
        rule_checker=RuleChecker(),
        title_parser=TitleParser(),
        analyzer=HermesAnalyzer(skill_dir=ROOT),
        scoring_engine=ScoringEngine(),
        result_writer=ResultWriter(detail_store=CandidateDetailStore(DEFAULT_CANDIDATE_ANALYSIS_DB_PATH)),
        market_direction_matcher=MarketDirectionMatcher(DEFAULT_MARKET_INSIGHT_ARTIFACTS_ROOT),
    )


def run_until_done(
    config_dir: Path,
    table_id: str,
    batch_size: int,
    sleep_seconds: int,
    max_rounds: int | None,
    record_scope: str,
    only_risk_tag: str,
    max_workers: int,
) -> Dict[str, Any]:
    adapter = TableAdapter()
    config = load_single_config(adapter, config_dir, table_id)
    pipeline = build_pipeline()

    rounds = []
    total_processed = 0
    total_completed = 0
    total_failed = 0
    round_index = 0

    while True:
        pending_before = count_pending_rows(adapter, config, record_scope, only_risk_tag=only_risk_tag)
        if pending_before <= 0:
            summary = {
                "table_id": table_id,
                "rounds": rounds,
                "total_processed": total_processed,
                "total_completed": total_completed,
                "total_failed": total_failed,
                "pending_remaining": 0,
                "status": "done",
            }
            print(json.dumps({"event": "queue_empty", **summary}, ensure_ascii=False), flush=True)
            return summary

        if max_rounds is not None and round_index >= max_rounds:
            summary = {
                "table_id": table_id,
                "rounds": rounds,
                "total_processed": total_processed,
                "total_completed": total_completed,
                "total_failed": total_failed,
                "pending_remaining": pending_before,
                "status": "stopped_by_max_rounds",
            }
            print(json.dumps({"event": "max_rounds_reached", **summary}, ensure_ascii=False), flush=True)
            return summary

        round_index += 1
        print(
            json.dumps(
                {
                    "event": "round_start",
                    "round": round_index,
                    "table_id": table_id,
                    "pending_before": pending_before,
                    "record_scope": record_scope,
                    "only_risk_tag": only_risk_tag,
                    "batch_size": batch_size,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

        client = adapter.get_client(config)
        table_summary = pipeline.process_table(
            config,
            client,
            limit=batch_size,
            record_scope=record_scope,
            only_risk_tag=only_risk_tag,
            max_workers=max_workers,
        )
        pending_after = count_pending_rows(adapter, config, record_scope, only_risk_tag=only_risk_tag)

        total_processed += int(table_summary["processed"])
        total_completed += int(table_summary["completed"])
        total_failed += int(table_summary["failed"])

        round_summary = {
            "round": round_index,
            "processed": table_summary["processed"],
            "completed": table_summary["completed"],
            "failed": table_summary["failed"],
            "pending_before": pending_before,
            "pending_after": pending_after,
        }
        rounds.append(round_summary)
        print(json.dumps({"event": "round_complete", **round_summary}, ensure_ascii=False), flush=True)

        if pending_after <= 0:
            continue
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Continuously process pending Hermes product-analysis rows.")
    parser.add_argument("--config-dir", default=str(ROOT / "configs" / "table_configs"))
    parser.add_argument("--table-id", required=True)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--sleep-seconds", type=int, default=0)
    parser.add_argument("--max-rounds", type=int, default=None)
    parser.add_argument(
        "--record-scope",
        choices=["pending", "completed", "completed_missing_v2", "all"],
        default="pending",
        help="Choose whether to consume pending rows or re-run completed rows.",
    )
    parser.add_argument(
        "--only-risk-tag",
        default="",
        help="When set, only process rows whose current risk tag exactly matches this value.",
    )
    parser.add_argument("--max-workers", type=int, default=1)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    run_until_done(
        config_dir=Path(args.config_dir),
        table_id=args.table_id,
        batch_size=max(1, args.batch_size),
        sleep_seconds=max(0, args.sleep_seconds),
        max_rounds=args.max_rounds,
        record_scope=args.record_scope,
        only_risk_tag=args.only_risk_tag,
        max_workers=max(1, args.max_workers),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
