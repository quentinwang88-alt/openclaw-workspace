#!/usr/bin/env python3
"""Market Insight v1 pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from src.market_insight_input_mode_detector import PRODUCT_MODE, SHOP_MODE
from src.market_insight_models import MarketInsightRunResult, VOCLightSummary
from src.market_insight_quality_gate import evaluate_sample_quality


class MarketInsightPipeline(object):
    def __init__(
        self,
        table_adapter,
        taxonomy_loader,
        analyzer,
        scoring_engine,
        aggregator,
        writer,
        report_generator=None,
        report_publisher=None,
        progress_syncer=None,
    ):
        self.table_adapter = table_adapter
        self.taxonomy_loader = taxonomy_loader
        self.analyzer = analyzer
        self.scoring_engine = scoring_engine
        self.aggregator = aggregator
        self.writer = writer
        self.report_generator = report_generator
        self.report_publisher = report_publisher
        self.progress_syncer = progress_syncer

    def process_tables(
        self,
        config_dir: Path,
        table_id: str = "",
        batch_date: str = "",
        limit_per_table: Optional[int] = None,
        max_workers: int = 1,
        source_scope_override: str = "",
    ) -> Dict[str, Any]:
        configs = self.table_adapter.load_table_configs(config_dir)
        if table_id:
            configs = [config for config in configs if config.table_id == table_id]
        summary = {"config_dir": str(config_dir), "tables": []}
        for config in configs:
            client = self.table_adapter.get_client(config)
            summary["tables"].append(
                self.process_table(
                    config=config,
                    client=client,
                    batch_date=batch_date,
                    limit=limit_per_table,
                    max_workers=max_workers,
                    source_scope_override=source_scope_override,
                ).to_dict()
            )
        return summary

    def process_table(
        self,
        config,
        client,
        batch_date: str = "",
        limit: Optional[int] = None,
        max_workers: int = 1,
        source_scope_override: str = "",
    ) -> MarketInsightRunResult:
        input_mode = self.table_adapter.detect_input_mode(config, client)
        if input_mode == PRODUCT_MODE:
            snapshots = self.table_adapter.read_product_snapshots(config, client, batch_date_override=batch_date, limit=limit)
            if not snapshots:
                raise ValueError("未读取到有效商品榜单记录")
            source_scope = self._resolve_source_scope(
                config=config,
                limit=limit,
                source_scope_override=source_scope_override,
            )
            taxonomy = self.taxonomy_loader.load(snapshots[0].category)
            voc_summary = VOCLightSummary(voc_status="skipped")
            resume_payload = self.writer.resume_product_run(
                config=config,
                input_mode=input_mode,
                first_snapshot=snapshots[0],
                total_product_count=len(snapshots),
                voc_summary=voc_summary,
                source_scope=source_scope,
            )
            if resume_payload:
                run_state, existing_tags_by_row_key = resume_payload
            else:
                run_state = self.writer.start_product_run(
                    config=config,
                    input_mode=input_mode,
                    first_snapshot=snapshots[0],
                    total_product_count=len(snapshots),
                    voc_summary=voc_summary,
                    source_scope=source_scope,
                )
                existing_tags_by_row_key = {}
            tags_by_index = {}
            for index, snapshot in enumerate(snapshots):
                row_key = self.writer.snapshot_row_key(snapshot)
                if row_key in existing_tags_by_row_key:
                    tags_by_index[index] = existing_tags_by_row_key[row_key]
            latest_result = None
            remaining_pairs = [
                (index, snapshot)
                for index, snapshot in enumerate(snapshots)
                if index not in tags_by_index
            ]
            if not remaining_pairs:
                ordered_indices = sorted(tags_by_index)
                completed_snapshots = [snapshots[item_index] for item_index in ordered_indices]
                completed_tags = [tags_by_index[item_index] for item_index in ordered_indices]
                scored_items = self.scoring_engine.score_products(completed_snapshots, completed_tags, config)
                cards = self.aggregator.build_direction_cards(scored_items)
                run_status = "completed" if len(completed_snapshots) >= len(snapshots) else "running"
                report_quality = evaluate_sample_quality(scored_items, len(completed_snapshots), config)
                if self.report_generator is not None:
                    report_payload, report_markdown, report_meta = self.report_generator.generate_report(
                        cards=cards,
                        voc_summary=voc_summary,
                        country=snapshots[0].country,
                        category=snapshots[0].category,
                        batch_date=snapshots[0].batch_date,
                        use_llm=(run_status == "completed"),
                        total_product_count=len(snapshots),
                        completed_product_count=len(completed_snapshots),
                        source_scope=source_scope,
                        quality_gate=report_quality,
                    )
                else:
                    report_payload = self.aggregator.build_report_payload(scored_items, cards, voc_summary=voc_summary)
                    report_markdown = self.aggregator.render_report_markdown(report_payload, cards)
                    report_meta = {
                        "used_llm": False,
                        "requested_direction_count": 0,
                        "rendered_direction_count": 0,
                        "fallback_count": 0,
                    }
                report_delivery = {
                    "status": "skipped",
                    "message": "运行中，尚未创建飞书文档" if run_status != "completed" else "未配置报告推送",
                    "notification_status": "skipped",
                    "notification_message": "",
                    "feishu_doc_token": "",
                    "feishu_doc_url": "",
                    "notification_target": "",
                }
                report_payload.setdefault("quality_gate", report_quality)
                if run_status == "completed" and not bool(report_quality.get("quality_gate_passed")):
                    report_delivery = self._blocked_report_delivery(str(report_quality.get("quality_gate_reason") or "有效样本率未通过发布门槛"))
                elif run_status == "completed" and bool(report_meta.get("report_publish_blocked")):
                    report_delivery = {
                        "status": "blocked",
                        "message": "报告自洽性校验未通过，已阻止发布",
                        "notification_status": "skipped",
                        "notification_message": "",
                        "feishu_doc_token": "",
                        "feishu_doc_url": "",
                        "notification_target": "",
                    }
                elif run_status == "completed" and self.report_publisher is not None:
                    report_delivery = self.report_publisher.publish(
                        report_markdown=report_markdown,
                        report_payload=report_payload,
                        country=snapshots[0].country,
                        category=snapshots[0].category,
                        batch_date=snapshots[0].batch_date,
                        report_output=dict(getattr(config, "report_output", {}) or {}),
                    )
                latest_result = self.writer.update_product_run(
                    config=config,
                    run_state=run_state,
                    snapshots=completed_snapshots,
                    scored_items=scored_items,
                    direction_cards=cards,
                    report_payload=report_payload,
                    report_markdown=report_markdown,
                    report_delivery=report_delivery,
                    completed_product_count=len(completed_snapshots),
                    total_product_count=len(snapshots),
                    run_status=run_status,
                    llm_fallback_count=int(report_meta.get("fallback_count", 0) or 0),
                )
                if self.progress_syncer is not None:
                    self.progress_syncer.maybe_sync(latest_result)
            else:
                remaining_snapshots = [item[1] for item in remaining_pairs]
                for local_index, tag in self.analyzer.iter_tag_products(remaining_snapshots, taxonomy=taxonomy, max_workers=max_workers):
                    original_index = remaining_pairs[local_index][0]
                    tags_by_index[original_index] = tag
                    ordered_indices = sorted(tags_by_index)
                    completed_snapshots = [snapshots[item_index] for item_index in ordered_indices]
                    completed_tags = [tags_by_index[item_index] for item_index in ordered_indices]
                    scored_items = self.scoring_engine.score_products(completed_snapshots, completed_tags, config)
                    cards = self.aggregator.build_direction_cards(scored_items)
                    run_status = "completed" if len(completed_snapshots) >= len(snapshots) else "running"
                    report_quality = evaluate_sample_quality(scored_items, len(completed_snapshots), config)
                    if self.report_generator is not None:
                        report_payload, report_markdown, report_meta = self.report_generator.generate_report(
                            cards=cards,
                            voc_summary=voc_summary,
                            country=snapshots[0].country,
                            category=snapshots[0].category,
                            batch_date=snapshots[0].batch_date,
                            use_llm=(run_status == "completed"),
                            total_product_count=len(snapshots),
                            completed_product_count=len(completed_snapshots),
                            source_scope=source_scope,
                            quality_gate=report_quality,
                        )
                    else:
                        report_payload = self.aggregator.build_report_payload(scored_items, cards, voc_summary=voc_summary)
                        report_markdown = self.aggregator.render_report_markdown(report_payload, cards)
                        report_meta = {
                            "used_llm": False,
                            "requested_direction_count": 0,
                            "rendered_direction_count": 0,
                            "fallback_count": 0,
                        }
                    report_delivery = {
                        "status": "skipped",
                        "message": "运行中，尚未创建飞书文档" if run_status != "completed" else "未配置报告推送",
                        "notification_status": "skipped",
                        "notification_message": "",
                        "feishu_doc_token": "",
                        "feishu_doc_url": "",
                        "notification_target": "",
                    }
                    report_payload.setdefault("quality_gate", report_quality)
                    if run_status == "completed" and not bool(report_quality.get("quality_gate_passed")):
                        report_delivery = self._blocked_report_delivery(str(report_quality.get("quality_gate_reason") or "有效样本率未通过发布门槛"))
                    elif run_status == "completed" and bool(report_meta.get("report_publish_blocked")):
                        report_delivery = {
                            "status": "blocked",
                            "message": "报告自洽性校验未通过，已阻止发布",
                            "notification_status": "skipped",
                            "notification_message": "",
                            "feishu_doc_token": "",
                            "feishu_doc_url": "",
                            "notification_target": "",
                        }
                    elif run_status == "completed" and self.report_publisher is not None:
                        report_delivery = self.report_publisher.publish(
                            report_markdown=report_markdown,
                            report_payload=report_payload,
                            country=snapshots[0].country,
                            category=snapshots[0].category,
                            batch_date=snapshots[0].batch_date,
                            report_output=dict(getattr(config, "report_output", {}) or {}),
                        )
                    latest_result = self.writer.update_product_run(
                        config=config,
                        run_state=run_state,
                        snapshots=completed_snapshots,
                        scored_items=scored_items,
                        direction_cards=cards,
                        report_payload=report_payload,
                        report_markdown=report_markdown,
                        report_delivery=report_delivery,
                        completed_product_count=len(completed_snapshots),
                        total_product_count=len(snapshots),
                        run_status=run_status,
                        llm_fallback_count=int(report_meta.get("fallback_count", 0) or 0),
                    )
                    if self.progress_syncer is not None:
                        self.progress_syncer.maybe_sync(latest_result)
            if latest_result is None:
                raise ValueError("未产出任何商品打标结果")
            return latest_result

        if input_mode == SHOP_MODE:
            snapshots = self.table_adapter.read_shop_snapshots(config, client, batch_date_override=batch_date, limit=limit)
            if not snapshots:
                raise ValueError("未读取到有效店铺榜单记录")
            summary = self.aggregator.build_shop_landscape_summary(
                snapshots=snapshots,
                batch_date=snapshots[0].batch_date,
                country=snapshots[0].country,
                category=snapshots[0].category,
            )
            return self.writer.write_shop_run(config=config, input_mode=input_mode, summary=summary)

        raise ValueError("不支持的 input_mode: {mode}".format(mode=input_mode))

    def _resolve_source_scope(self, config, limit: Optional[int], source_scope_override: str = "") -> str:
        scope = str(source_scope_override or getattr(config, "source_scope", "") or "official").strip().lower()
        if scope not in {"official", "experiment", "smoke_test", "backfill"}:
            scope = "official"
        min_count = int(getattr(config, "min_consumable_product_count", 0) or 0)
        if scope == "official" and limit is not None and limit > 0 and min_count > 0 and limit < min_count:
            return "smoke_test"
        return scope

    def _blocked_report_delivery(self, message: str) -> Dict[str, str]:
        return {
            "status": "blocked",
            "message": message,
            "notification_status": "skipped",
            "notification_message": "",
            "feishu_doc_token": "",
            "feishu_doc_url": "",
            "notification_target": "",
        }
