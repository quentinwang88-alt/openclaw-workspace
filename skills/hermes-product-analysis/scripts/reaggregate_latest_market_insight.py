#!/usr/bin/env python3
"""Re-aggregate latest scored market-insight samples into fresh direction cards."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_aggregator import MarketInsightAggregator  # noqa: E402
from src.market_insight_db import MarketInsightDatabase  # noqa: E402
from src.market_insight_models import (  # noqa: E402
    MarketInsightConfig,
    MarketInsightProductTag,
    ProductRankingSnapshot,
    ScoredProductSnapshot,
    VOCLightSummary,
)
from src.market_insight_scoring import MarketInsightScoringEngine  # noqa: E402
from src.market_insight_report_publisher import MarketInsightReportPublisher  # noqa: E402
from src.market_insight_report_generator import MarketInsightReportGenerator  # noqa: E402
from src.market_insight_table_adapter import MarketInsightTableAdapter  # noqa: E402
from src.market_insight_writer import MarketInsightWriter  # noqa: E402


DEFAULT_ARTIFACTS_ROOT = ROOT / "artifacts" / "market_insight"


def _load_progress_payload(run_id: str) -> dict:
    progress_path = Path(run_id) / "market_insight_progress.json"
    if not progress_path.exists():
        return {}
    try:
        return json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _select_source_run(database: MarketInsightDatabase, country: str, category: str, table_id: str) -> dict:
    recent_runs = database.list_recent_completed_runs(country=country, category=category, limit=20)
    if not recent_runs:
        return {}
    for run in recent_runs:
        if (
            str(run.get("table_id") or "") == table_id
            and str(run.get("source_scope") or "official") == "official"
        ):
            return run
    for run in recent_runs:
        if str(run.get("table_id") or "") == table_id:
            return run
    return recent_runs[0]


def _build_scored_item(payload):
    snapshot_payload = dict(payload.get("snapshot") or {})
    tag_payload = dict(payload.get("tag") or {})
    return ScoredProductSnapshot(
        snapshot=ProductRankingSnapshot(
            batch_date=str(snapshot_payload.get("batch_date") or ""),
            batch_id=str(snapshot_payload.get("batch_id") or ""),
            country=str(snapshot_payload.get("country") or ""),
            category=str(snapshot_payload.get("category") or ""),
            product_id=str(snapshot_payload.get("product_id") or ""),
            product_name=str(snapshot_payload.get("product_name") or ""),
            shop_name=str(snapshot_payload.get("shop_name") or ""),
            price_min=snapshot_payload.get("price_min"),
            price_max=snapshot_payload.get("price_max"),
            price_mid=snapshot_payload.get("price_mid"),
            sales_7d=float(snapshot_payload.get("sales_7d") or 0.0),
            gmv_7d=float(snapshot_payload.get("gmv_7d") or 0.0),
            creator_count=float(snapshot_payload.get("creator_count") or 0.0),
            video_count=float(snapshot_payload.get("video_count") or 0.0),
            listing_days=snapshot_payload.get("listing_days"),
            product_images=list(snapshot_payload.get("product_images") or []),
            raw_product_images=snapshot_payload.get("raw_product_images"),
            image_url=str(snapshot_payload.get("image_url") or ""),
            product_url=str(snapshot_payload.get("product_url") or ""),
            rank_index=int(snapshot_payload.get("rank_index") or 0),
            listing_datetime=str(snapshot_payload.get("listing_datetime") or ""),
            product_age_days=snapshot_payload.get("product_age_days"),
            age_bucket=str(snapshot_payload.get("age_bucket") or ""),
            listing_date_parse_status=str(snapshot_payload.get("listing_date_parse_status") or ""),
            raw_category=str(snapshot_payload.get("raw_category") or ""),
            source_feishu_url=str(snapshot_payload.get("source_feishu_url") or ""),
            source_app_token=str(snapshot_payload.get("source_app_token") or ""),
            source_table_id=str(snapshot_payload.get("source_table_id") or ""),
            raw_fields=dict(snapshot_payload.get("raw_fields") or {}),
        ),
        tag=MarketInsightProductTag(
            is_valid_sample=bool(tag_payload.get("is_valid_sample")),
            style_cluster=str(tag_payload.get("style_cluster") or ""),
            style_tags_secondary=list(tag_payload.get("style_tags_secondary") or []),
            product_form=str(tag_payload.get("product_form") or ""),
            element_tags=list(tag_payload.get("element_tags") or []),
            value_points=list(tag_payload.get("value_points") or []),
            scene_tags=list(tag_payload.get("scene_tags") or []),
            reason_short=str(tag_payload.get("reason_short") or ""),
        ),
        heat_score=float(payload.get("heat_score") or 0.0),
        heat_level=str(payload.get("heat_level") or ""),
        crowd_score=float(payload.get("crowd_score") or 0.0),
        crowd_level=str(payload.get("crowd_level") or ""),
        priority_level=str(payload.get("priority_level") or ""),
        target_price_band=str(payload.get("target_price_band") or ""),
        direction_canonical_key=str(payload.get("direction_canonical_key") or ""),
        direction_family=str(payload.get("direction_family") or ""),
        direction_tier=str(payload.get("direction_tier") or ""),
        seasonal_trend=str(payload.get("seasonal_trend") or "unclear"),
        seasonal_trend_short=str(payload.get("seasonal_trend_short") or "unclear"),
        seasonal_trend_long=str(payload.get("seasonal_trend_long") or "unclear"),
        content_efficiency_signal=float(payload.get("content_efficiency_signal") or 0.0),
        content_efficiency_source=str(payload.get("content_efficiency_source") or "missing"),
        product_age_days=payload.get("product_age_days"),
        age_bucket=str(payload.get("age_bucket") or ""),
        listing_date_parse_status=str(payload.get("listing_date_parse_status") or ""),
        default_content_route_preference=str(payload.get("default_content_route_preference") or ""),
    )


def _rebuild_tag(payload):
    tag_payload = dict(payload.get("tag") or {})
    return MarketInsightProductTag(
        is_valid_sample=bool(tag_payload.get("is_valid_sample")),
        style_cluster=str(tag_payload.get("style_cluster") or ""),
        style_tags_secondary=list(tag_payload.get("style_tags_secondary") or []),
        product_form=str(tag_payload.get("product_form") or ""),
        length_form=str(tag_payload.get("length_form") or ""),
        element_tags=list(tag_payload.get("element_tags") or []),
        value_points=list(tag_payload.get("value_points") or []),
        scene_tags=list(tag_payload.get("scene_tags") or []),
        reason_short=str(tag_payload.get("reason_short") or ""),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-aggregate latest market insight run from stored scored samples.")
    parser.add_argument("--country", required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--table-id", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--artifacts-root", default=str(DEFAULT_ARTIFACTS_ROOT))
    parser.add_argument("--config-dir", default=str(ROOT / "configs" / "market_insight_table_configs"))
    parser.add_argument("--report-config", default=str(ROOT / "configs" / "report_config.yaml"))
    parser.add_argument("--publish-report", action="store_true")
    parser.add_argument("--disable-llm", action="store_true")
    parser.add_argument("--recompute-scoring", action="store_true")
    args = parser.parse_args()

    artifacts_root = Path(args.artifacts_root)
    database = MarketInsightDatabase(artifacts_root / "market_insight.db")
    adapter = MarketInsightTableAdapter()
    configs = adapter.load_table_configs(Path(args.config_dir), validate_source=False)
    config_by_id = {config.table_id: config for config in configs}
    loaded_config = config_by_id.get(
        args.table_id,
        MarketInsightConfig(
            table_id=args.table_id,
            table_name=args.table_name,
            enabled=True,
        ),
    )

    source_run = _select_source_run(database, country=args.country, category=args.category, table_id=args.table_id)
    if source_run:
        scored_payloads = database.load_scored_products_for_run(str(source_run.get("run_id") or ""))
    else:
        scored_payloads = database.load_latest_scored_products(country=args.country, category=args.category)
    if not scored_payloads:
        raise SystemExit("未找到可重聚合的 scored products")

    if args.recompute_scoring:
        snapshots = [_build_scored_item(item).snapshot for item in scored_payloads]
        tags = [_rebuild_tag(item) for item in scored_payloads]
        scored_items = MarketInsightScoringEngine().score_products(
            snapshots=snapshots,
            tags=tags,
            config=loaded_config,
        )
    else:
        scored_items = [_build_scored_item(item) for item in scored_payloads]
        snapshots = [item.snapshot for item in scored_items]

    aggregator = MarketInsightAggregator()
    cards = aggregator.build_direction_cards(scored_items)
    voc_summary = VOCLightSummary(voc_status="skipped")
    report_generator = MarketInsightReportGenerator(skill_dir=ROOT, config_path=Path(args.report_config))
    progress_payload = _load_progress_payload(str(source_run.get("run_id") or "")) if source_run else {}
    quality_gate = {
        "valid_sample_count": int(progress_payload.get("valid_sample_count") or 0),
        "invalid_sample_count": int(progress_payload.get("invalid_sample_count") or 0),
        "valid_sample_ratio": progress_payload.get("valid_sample_ratio"),
        "quality_gate_passed": bool(progress_payload.get("quality_gate_passed", True)),
        "quality_gate_reason": str(progress_payload.get("quality_gate_reason") or ""),
    }
    if not quality_gate["valid_sample_count"]:
        quality_gate = {}
    report_payload, report_markdown, report_meta = report_generator.generate_report(
        cards=cards,
        voc_summary=voc_summary,
        country=snapshots[0].country,
        category=snapshots[0].category,
        batch_date=snapshots[0].batch_date,
        use_llm=not args.disable_llm,
        total_product_count=int(progress_payload.get("total_product_count") or len(snapshots)),
        completed_product_count=int(progress_payload.get("completed_product_count") or len(snapshots)),
        source_scope=str(source_run.get("source_scope") or progress_payload.get("source_scope") or "official") if source_run else "official",
        quality_gate=quality_gate,
    )
    if args.publish_report:
        if bool(report_meta.get("report_publish_blocked")):
            report_delivery = {
                "status": "blocked",
                "message": "报告自洽性校验未通过，已阻止发布",
                "notification_status": "skipped",
                "notification_message": "",
                "feishu_doc_token": "",
                "feishu_doc_url": "",
                "notification_target": "",
            }
        else:
            report_delivery = MarketInsightReportPublisher().publish(
                report_markdown=report_markdown,
                report_payload=report_payload,
                country=snapshots[0].country,
                category=snapshots[0].category,
                batch_date=snapshots[0].batch_date,
                report_output=dict(getattr(loaded_config, "report_output", {}) or {}),
            )
    else:
        report_delivery = {"status": "skipped", "message": "重聚合脚本默认不推送飞书文档", "notification_status": "skipped"}

    writer = MarketInsightWriter(artifacts_root=artifacts_root)
    result = writer.write_product_run(
        config=loaded_config,
        input_mode="product_ranking",
        snapshots=snapshots,
        scored_items=scored_items,
        direction_cards=cards,
        report_payload=report_payload,
        report_markdown=report_markdown,
        voc_summary=voc_summary,
        report_delivery=report_delivery,
        llm_fallback_count=int(report_meta.get("fallback_count", 0) or 0),
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
