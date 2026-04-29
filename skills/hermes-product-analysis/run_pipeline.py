#!/usr/bin/env python3
"""Hermes 选品分析 V2 入口。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from src.hermes_analyzer import HermesAnalyzer  # noqa: E402
from src.candidate_detail_store import CandidateDetailStore  # noqa: E402
from src.direction_diff_report import MarketInsightDiffReporter  # noqa: E402
from src.market_direction_matcher import MarketDirectionMatcher  # noqa: E402
from src.market_insight_aggregator import MarketInsightAggregator  # noqa: E402
from src.market_insight_analyzer import MarketInsightAnalyzer  # noqa: E402
from src.market_insight_feishu_sync import MarketInsightFeishuSyncer  # noqa: E402
from src.market_insight_pipeline import MarketInsightPipeline  # noqa: E402
from src.market_insight_report_generator import MarketInsightReportGenerator  # noqa: E402
from src.market_insight_report_publisher import MarketInsightReportPublisher  # noqa: E402
from src.market_insight_scoring import MarketInsightScoringEngine  # noqa: E402
from src.market_insight_table_adapter import MarketInsightTableAdapter  # noqa: E402
from src.market_insight_taxonomy import MarketInsightTaxonomyLoader  # noqa: E402
from src.market_insight_writer import MarketInsightWriter  # noqa: E402
from src.pipeline import CandidateAnalysisPipeline  # noqa: E402
from src.result_writer import ResultWriter  # noqa: E402
from src.rule_checker import RuleChecker  # noqa: E402
from src.scoring_engine import ScoringEngine  # noqa: E402
from src.table_adapter import TableAdapter  # noqa: E402
from src.title_parser import TitleParser  # noqa: E402


DEFAULT_CONFIG_DIR = SKILL_DIR / "configs" / "table_configs"
DEFAULT_MARKET_INSIGHT_CONFIG_DIR = SKILL_DIR / "configs" / "market_insight_table_configs"
DEFAULT_MARKET_INSIGHT_ARTIFACTS_DIR = SKILL_DIR / "artifacts" / "market_insight"
DEFAULT_CANDIDATE_ANALYSIS_DB_PATH = SKILL_DIR / "artifacts" / "candidate_analysis" / "candidate_analysis.db"
DEFAULT_REPORT_CONFIG_PATH = SKILL_DIR / "configs" / "report_config.yaml"


def command_validate_configs(args: argparse.Namespace) -> None:
    adapter = TableAdapter()
    configs = adapter.load_table_configs(Path(args.config_dir), validate_source=False)
    summary = {
        "config_dir": str(Path(args.config_dir)),
        "config_count": len(configs),
        "table_ids": [config.table_id for config in configs],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def command_run_once(args: argparse.Namespace) -> None:
    pipeline = CandidateAnalysisPipeline(
        table_adapter=TableAdapter(),
        rule_checker=RuleChecker(),
        title_parser=TitleParser(),
        analyzer=HermesAnalyzer(skill_dir=SKILL_DIR),
        scoring_engine=ScoringEngine(),
        result_writer=ResultWriter(detail_store=CandidateDetailStore(DEFAULT_CANDIDATE_ANALYSIS_DB_PATH)),
        market_direction_matcher=MarketDirectionMatcher(Path(args.market_insight_artifacts_dir)),
    )
    summary = pipeline.process_tables(
        config_dir=Path(args.config_dir),
        table_id=args.table_id,
        limit_per_table=args.limit_per_table,
        record_scope=args.record_scope,
        only_risk_tag=args.only_risk_tag,
        max_workers=args.max_workers,
        flush_every=args.flush_every,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def command_validate_market_insight_configs(args: argparse.Namespace) -> None:
    adapter = MarketInsightTableAdapter()
    configs = adapter.load_table_configs(Path(args.config_dir), validate_source=False)
    summary = {
        "config_dir": str(Path(args.config_dir)),
        "config_count": len(configs),
        "table_ids": [config.table_id for config in configs],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def command_run_market_insight(args: argparse.Namespace) -> None:
    progress_syncer = None
    if args.feishu_output_config:
        progress_syncer = MarketInsightFeishuSyncer(
            output_config_path=Path(args.feishu_output_config),
            artifacts_root=Path(args.artifacts_root),
            sync_every_completions=args.sync_every_completions,
        )
    pipeline = MarketInsightPipeline(
        table_adapter=MarketInsightTableAdapter(),
        taxonomy_loader=MarketInsightTaxonomyLoader(
            taxonomy_dir=SKILL_DIR / "configs" / "market_insight_taxonomies"
        ),
        analyzer=MarketInsightAnalyzer(skill_dir=SKILL_DIR),
        scoring_engine=MarketInsightScoringEngine(),
        aggregator=MarketInsightAggregator(),
        writer=MarketInsightWriter(artifacts_root=Path(args.artifacts_root)),
        report_generator=MarketInsightReportGenerator(
            skill_dir=SKILL_DIR,
            config_path=Path(args.report_config),
        ),
        report_publisher=MarketInsightReportPublisher(),
        progress_syncer=progress_syncer,
    )
    summary = pipeline.process_tables(
        config_dir=Path(args.config_dir),
        table_id=args.table_id,
        batch_date=args.batch_date,
        limit_per_table=args.limit_per_table,
        max_workers=args.max_workers,
        source_scope_override=args.source_scope,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def command_market_insight_diff_report(args: argparse.Namespace) -> None:
    reporter = MarketInsightDiffReporter(
        artifacts_root=Path(args.artifacts_root),
        skill_dir=SKILL_DIR,
        report_config_path=Path(args.report_config),
    )
    payload = reporter.build_recent_run_diff(
        country=args.country,
        category=args.category,
        recent_runs=args.recent_runs,
    )
    output_dir = Path(args.output_dir) if args.output_dir else Path(args.artifacts_root) / "diff_reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = "market_insight_diff_report__{country}__{category}".format(country=args.country, category=args.category)
    (output_dir / "{stem}.json".format(stem=stem)).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "{stem}.md".format(stem=stem)).write_text(str(payload.get("markdown") or ""), encoding="utf-8")
    print(json.dumps({"output_dir": str(output_dir), "payload_path": str(output_dir / (stem + ".json")), "markdown_path": str(output_dir / (stem + ".md"))}, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Hermes product analysis V2 pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate-configs", help="Validate table config files.")
    validate_parser.add_argument("--config-dir", default=str(DEFAULT_CONFIG_DIR))
    validate_parser.set_defaults(func=command_validate_configs)

    run_parser = subparsers.add_parser("run-once", help="Read selected records and process them once.")
    run_parser.add_argument("--config-dir", default=str(DEFAULT_CONFIG_DIR))
    run_parser.add_argument("--table-id", default="")
    run_parser.add_argument("--limit-per-table", type=int, default=None)
    run_parser.add_argument(
        "--record-scope",
        choices=["pending", "completed", "completed_missing_v2", "all"],
        default="pending",
        help="Choose whether to process pending rows, completed rows for V2 backfill, or all rows.",
    )
    run_parser.add_argument(
        "--only-risk-tag",
        default="",
        help="When set, only process records whose current risk tag exactly matches this value.",
    )
    run_parser.add_argument("--max-workers", type=int, default=1, help="Parallel analysis workers.")
    run_parser.add_argument(
        "--flush-every",
        type=int,
        default=1,
        help="Provisionally write back every N analyzed rows before final batch calibration rewrite. Use 0 to disable.",
    )
    run_parser.add_argument(
        "--market-insight-artifacts-dir",
        default=str(DEFAULT_MARKET_INSIGHT_ARTIFACTS_DIR),
        help="Latest market direction card artifacts root used by stage-2 matcher.",
    )
    run_parser.set_defaults(func=command_run_once)

    mi_validate_parser = subparsers.add_parser(
        "market-insight-validate-configs",
        help="Validate market insight table config files.",
    )
    mi_validate_parser.add_argument("--config-dir", default=str(DEFAULT_MARKET_INSIGHT_CONFIG_DIR))
    mi_validate_parser.set_defaults(func=command_validate_market_insight_configs)

    mi_run_parser = subparsers.add_parser(
        "market-insight-run",
        help="Run the Market Insight v1 pipeline for one or more ranking tables.",
    )
    mi_run_parser.add_argument("--config-dir", default=str(DEFAULT_MARKET_INSIGHT_CONFIG_DIR))
    mi_run_parser.add_argument("--table-id", default="")
    mi_run_parser.add_argument("--batch-date", default="")
    mi_run_parser.add_argument("--limit-per-table", type=int, default=None)
    mi_run_parser.add_argument("--max-workers", type=int, default=1)
    mi_run_parser.add_argument(
        "--source-scope",
        choices=["official", "experiment", "smoke_test", "backfill"],
        default="",
        help="Optional run scope tag. Stage-2 matcher only consumes official runs that pass minimum thresholds.",
    )
    mi_run_parser.add_argument(
        "--artifacts-root",
        default=str(DEFAULT_MARKET_INSIGHT_ARTIFACTS_DIR),
        help="Artifacts output root for market direction cards and reports.",
    )
    mi_run_parser.add_argument(
        "--feishu-output-config",
        default="",
        help="Optional Feishu output config path. When set, direction cards are synced during the run.",
    )
    mi_run_parser.add_argument(
        "--report-config",
        default=str(DEFAULT_REPORT_CONFIG_PATH),
        help="YAML config path for structured report thresholds and wording rules.",
    )
    mi_run_parser.add_argument(
        "--sync-every-completions",
        type=int,
        default=1,
        help="When Feishu sync is enabled, sync after every N completed product tags.",
    )
    mi_run_parser.set_defaults(func=command_run_market_insight)

    mi_diff_parser = subparsers.add_parser(
        "market-insight-diff-report",
        help="Dry-run recent market insight runs and output a diff report without publishing.",
    )
    mi_diff_parser.add_argument("--country", required=True)
    mi_diff_parser.add_argument("--category", required=True)
    mi_diff_parser.add_argument("--recent-runs", type=int, default=3)
    mi_diff_parser.add_argument(
        "--artifacts-root",
        default=str(DEFAULT_MARKET_INSIGHT_ARTIFACTS_DIR),
        help="Artifacts output root for market insight SQLite and diff outputs.",
    )
    mi_diff_parser.add_argument(
        "--report-config",
        default=str(DEFAULT_REPORT_CONFIG_PATH),
        help="YAML config path for report thresholds and wording rules.",
    )
    mi_diff_parser.add_argument("--output-dir", default="")
    mi_diff_parser.set_defaults(func=command_market_insight_diff_report)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
