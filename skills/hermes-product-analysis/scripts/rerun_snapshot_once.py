#!/usr/bin/env python3
"""Re-run a fixed snapshot of Hermes product-analysis rows exactly once."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.candidate_detail_store import CandidateDetailStore  # noqa: E402
from src.hermes_analyzer import HermesAnalyzer  # noqa: E402
from src.pipeline import CandidateAnalysisPipeline  # noqa: E402
from src.result_writer import ResultWriter  # noqa: E402
from src.rule_checker import RuleChecker  # noqa: E402
from src.scoring_engine import ScoringEngine  # noqa: E402
from src.table_adapter import TableAdapter  # noqa: E402
from src.title_parser import TitleParser  # noqa: E402


DEFAULT_CANDIDATE_ANALYSIS_DB_PATH = ROOT / "artifacts" / "candidate_analysis" / "candidate_analysis.db"


class SnapshotSubsetClient(object):
    def __init__(self, records, upstream_client):
        self._records = list(records)
        self._upstream_client = upstream_client

    def list_records(self, page_size=100, limit=None):
        items = list(self._records)
        if limit is not None:
            return items[:limit]
        return items

    def update_record_fields(self, record_id, fields):
        self._upstream_client.update_record_fields(record_id, fields)
        for record in self._records:
            if record.record_id == record_id:
                record.fields.update(fields)
                break

    def get_tmp_download_url(self, file_token):
        return self._upstream_client.get_tmp_download_url(file_token)


def build_pipeline() -> CandidateAnalysisPipeline:
    return CandidateAnalysisPipeline(
        table_adapter=TableAdapter(),
        rule_checker=RuleChecker(),
        title_parser=TitleParser(),
        analyzer=HermesAnalyzer(skill_dir=ROOT),
        scoring_engine=ScoringEngine(),
        result_writer=ResultWriter(detail_store=CandidateDetailStore(DEFAULT_CANDIDATE_ANALYSIS_DB_PATH)),
    )


def load_single_config(adapter: TableAdapter, config_dir: Path, table_id: str):
    configs = adapter.load_table_configs(config_dir)
    for config in configs:
        if config.table_id == table_id:
            return config
    raise ValueError(f"未找到启用的 table_id: {table_id}")


def rerun_snapshot_once(
    config_dir: Path,
    table_id: str,
    record_scope: str,
    only_risk_tag: str,
    batch_size: int,
    max_workers: int,
) -> Dict[str, Any]:
    adapter = TableAdapter()
    config = load_single_config(adapter, config_dir, table_id)
    client = adapter.get_client(config)
    snapshot_records = adapter.read_pending_records(
        config,
        client,
        limit=None,
        record_scope=record_scope,
        only_risk_tag=only_risk_tag,
    )

    summary = {
        "table_id": table_id,
        "record_scope": record_scope,
        "only_risk_tag": only_risk_tag,
        "snapshot_size": len(snapshot_records),
        "processed": 0,
        "completed": 0,
        "failed": 0,
        "batches": [],
        "status": "done",
    }
    print(
        json.dumps(
            {
                "event": "snapshot_loaded",
                "table_id": table_id,
                "record_scope": record_scope,
                "only_risk_tag": only_risk_tag,
                "snapshot_size": len(snapshot_records),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    if not snapshot_records:
        return summary

    pipeline = build_pipeline()
    effective_batch_size = max(1, batch_size)
    for batch_index, offset in enumerate(range(0, len(snapshot_records), effective_batch_size), start=1):
        batch_records = snapshot_records[offset : offset + effective_batch_size]
        subset_client = SnapshotSubsetClient(batch_records, client)
        table_summary = pipeline.process_table(
            config,
            subset_client,
            limit=None,
            record_scope="all",
            max_workers=max_workers,
        )
        batch_summary = {
            "batch": batch_index,
            "size": len(batch_records),
            "processed": table_summary["processed"],
            "completed": table_summary["completed"],
            "failed": table_summary["failed"],
        }
        summary["batches"].append(batch_summary)
        summary["processed"] += int(table_summary["processed"])
        summary["completed"] += int(table_summary["completed"])
        summary["failed"] += int(table_summary["failed"])
        print(json.dumps({"event": "snapshot_batch_complete", **batch_summary}, ensure_ascii=False), flush=True)

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Re-run a fixed snapshot of Hermes product-analysis rows once.")
    parser.add_argument("--config-dir", default=str(ROOT / "configs" / "table_configs"))
    parser.add_argument("--table-id", required=True)
    parser.add_argument(
        "--record-scope",
        choices=["pending", "completed", "completed_missing_v2", "all"],
        default="completed",
    )
    parser.add_argument("--only-risk-tag", default="")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-workers", type=int, default=1)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = rerun_snapshot_once(
        config_dir=Path(args.config_dir),
        table_id=args.table_id,
        record_scope=args.record_scope,
        only_risk_tag=args.only_risk_tag,
        batch_size=max(1, args.batch_size),
        max_workers=max(1, args.max_workers),
    )
    print(json.dumps({"event": "snapshot_done", **summary}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
