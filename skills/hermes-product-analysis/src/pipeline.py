#!/usr/bin/env python3
"""V2 主流程编排。"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from src.enums import AnalysisStatus, HERMES_CONFIDENCE_VALUES, SUPPORTED_CATEGORIES
from src.models import PendingAnalysisItem, TableConfig


class CandidateAnalysisPipeline(object):
    def __init__(
        self,
        table_adapter,
        rule_checker,
        title_parser,
        analyzer,
        scoring_engine,
        result_writer,
        market_direction_matcher=None,
    ):
        self.table_adapter = table_adapter
        self.rule_checker = rule_checker
        self.title_parser = title_parser
        self.analyzer = analyzer
        self.scoring_engine = scoring_engine
        self.result_writer = result_writer
        self.market_direction_matcher = market_direction_matcher

    def process_tables(
        self,
        config_dir,
        table_id: str = "",
        limit_per_table: Optional[int] = None,
        record_scope: str = "pending",
        only_risk_tag: str = "",
        max_workers: int = 1,
        flush_every: int = 1,
    ) -> Dict[str, Any]:
        configs = self.table_adapter.load_table_configs(config_dir)
        if table_id:
            configs = [config for config in configs if config.table_id == table_id]
        summary = {
            "config_dir": str(config_dir),
            "tables": [],
            "processed": 0,
            "completed": 0,
            "failed": 0,
        }
        for config in configs:
            client = self.table_adapter.get_client(config)
            table_summary = self.process_table(
                config,
                client,
                limit=limit_per_table,
                record_scope=record_scope,
                only_risk_tag=only_risk_tag,
                max_workers=max_workers,
                flush_every=flush_every,
            )
            summary["tables"].append(table_summary)
            summary["processed"] += table_summary["processed"]
            summary["completed"] += table_summary["completed"]
            summary["failed"] += table_summary["failed"]
        return summary

    def process_table(
        self,
        table_config: TableConfig,
        client,
        limit: Optional[int] = None,
        record_scope: str = "pending",
        only_risk_tag: str = "",
        max_workers: int = 1,
        flush_every: int = 1,
    ) -> Dict[str, Any]:
        records = self.table_adapter.read_pending_records(
            table_config,
            client,
            limit=limit,
            record_scope=record_scope,
            only_risk_tag=only_risk_tag,
        )
        table_summary = {
            "table_id": table_config.table_id,
            "table_name": table_config.table_name,
            "record_scope": record_scope,
            "only_risk_tag": only_risk_tag,
            "max_workers": max(1, max_workers),
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "alerts": [],
            "records": [],
        }
        run_id = self._start_detail_run(
            table_config=table_config,
            record_scope=record_scope,
            only_risk_tag=only_risk_tag,
            max_workers=max_workers,
            flush_every=flush_every,
        )
        table_summary["run_id"] = run_id
        pending_items: List[PendingAnalysisItem] = []
        progressive_items: List[PendingAnalysisItem] = []
        prepared_jobs = []

        for record in records:
            table_summary["processed"] += 1
            try:
                task = self.table_adapter.map_record_to_candidate_task(record, table_config, client=client)
                title_parse_result = self.title_parser.parse_title(task.product_title)
                task.title_keyword_tags = title_parse_result.title_keyword_tags
                task.title_category_hint = title_parse_result.title_category_hint
                task.title_category_confidence = title_parse_result.title_category_confidence

                precheck = self.rule_checker.check(
                    task,
                    supported_manual_categories=table_config.supported_manual_categories or SUPPORTED_CATEGORIES,
                )
                if not precheck.should_continue:
                    self.result_writer.write_record_result(
                        client=client,
                        table_config=table_config,
                        record_id=record.record_id,
                        status=precheck.terminal_status or AnalysisStatus.FAILED.value,
                        task=task,
                        error_message=precheck.terminal_reason,
                        run_id=run_id,
                    )
                    table_summary["records"].append(
                        {"record_id": record.record_id, "status": precheck.terminal_status, "detail": precheck.terminal_reason}
                    )
                    continue

                manual_category = (task.manual_category or "").strip()
                if manual_category:
                    task.final_category = manual_category
                    task.category_confidence = "manual"
                prepared_jobs.append((record, task))
            except Exception as exc:
                table_summary["failed"] += 1
                failure_message = str(exc)
                try:
                    if "task" in locals() and getattr(task, "source_record_id", "") == record.record_id:
                        self.result_writer.write_record_result(
                            client=client,
                            table_config=table_config,
                            record_id=record.record_id,
                            status=AnalysisStatus.FAILED.value,
                            recognized_category=task.final_category,
                            category_confidence=task.category_confidence,
                            task=task,
                            error_message=failure_message,
                            run_id=run_id,
                        )
                    else:
                        self.result_writer.write_record_result(
                            client=client,
                            table_config=table_config,
                            record_id=record.record_id,
                            status=AnalysisStatus.FAILED.value,
                            error_message=failure_message,
                            run_id=run_id,
                        )
                except Exception as writeback_exc:
                    failure_message = "{error}; writeback_failed={writeback}".format(
                        error=failure_message,
                        writeback=writeback_exc,
                    )
                table_summary["records"].append(
                    {
                        "record_id": record.record_id,
                        "status": AnalysisStatus.FAILED.value,
                        "error": failure_message,
                    }
                )

        if max(1, max_workers) <= 1:
            for record, task in prepared_jobs:
                self._consume_analysis_result(
                    result=self._analyze_prepared_record(record, task, table_config, client),
                    table_summary=table_summary,
                    pending_items=pending_items,
                        progressive_items=progressive_items,
                        table_config=table_config,
                        client=client,
                        flush_every=flush_every,
                        run_id=run_id,
                    )
        else:
            with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
                futures = [
                    executor.submit(self._analyze_prepared_record, record, task, table_config, client)
                    for record, task in prepared_jobs
                ]
                for future in as_completed(futures):
                    self._consume_analysis_result(
                        result=future.result(),
                        table_summary=table_summary,
                        pending_items=pending_items,
                        progressive_items=progressive_items,
                        table_config=table_config,
                        client=client,
                        flush_every=flush_every,
                        run_id=run_id,
                    )

        self._flush_progressive_items(
            items=progressive_items,
            table_config=table_config,
            client=client,
            run_id=run_id,
        )
        calibrated_items = self._calibrate_items(pending_items)
        table_summary["alerts"] = self._build_batch_alerts(calibrated_items)
        for calibrated_item in calibrated_items:
            try:
                self.result_writer.write_record_result(
                    client=client,
                    table_config=table_config,
                    record_id=calibrated_item.task.source_record_id,
                    status=AnalysisStatus.COMPLETED.value,
                    recognized_category=calibrated_item.task.final_category,
                    category_confidence=calibrated_item.task.category_confidence,
                    task=calibrated_item.task,
                    feature_result=calibrated_item.feature_result,
                    scored_result=calibrated_item.scored_result,
                    run_id=run_id,
                )
                table_summary["completed"] += 1
                table_summary["records"].append(
                    {
                        "record_id": calibrated_item.task.source_record_id,
                        "status": AnalysisStatus.COMPLETED.value,
                        "recognized_category": calibrated_item.task.final_category,
                        "category_confidence": calibrated_item.task.category_confidence,
                        "batch_priority_score": calibrated_item.scored_result.batch_priority_score,
                        "suggested_action": calibrated_item.scored_result.suggested_action,
                    }
                )
            except Exception as exc:
                table_summary["failed"] += 1
                failure_message = str(exc)
                try:
                    self.result_writer.write_record_result(
                        client=client,
                        table_config=table_config,
                        record_id=calibrated_item.task.source_record_id,
                        status=AnalysisStatus.FAILED.value,
                        recognized_category=calibrated_item.task.final_category,
                        category_confidence=calibrated_item.task.category_confidence,
                        task=calibrated_item.task,
                        error_message=failure_message,
                        run_id=run_id,
                    )
                except Exception as writeback_exc:
                    failure_message = "{error}; writeback_failed={writeback}".format(
                        error=failure_message,
                        writeback=writeback_exc,
                    )
                table_summary["records"].append(
                    {
                        "record_id": calibrated_item.task.source_record_id,
                        "status": AnalysisStatus.FAILED.value,
                        "error": failure_message,
                    }
                )
        self._finish_detail_run(run_id=run_id, table_config=table_config, table_summary=table_summary)
        return table_summary

    def _analyze_prepared_record(self, record, task, table_config: TableConfig, client) -> Dict[str, Any]:
        working_task = task
        try:
            if not working_task.final_category:
                category_result = self.analyzer.identify_category(working_task)
                working_task.final_category = category_result.predicted_category
                working_task.category_confidence = category_result.confidence
                if (
                    working_task.final_category not in SUPPORTED_CATEGORIES
                    or working_task.category_confidence not in HERMES_CONFIDENCE_VALUES - {"low"}
                ):
                    return {
                        "type": "manual_confirm",
                        "record_id": record.record_id,
                        "task": working_task,
                    }

            try:
                feature_result = self.analyzer.analyze_features(working_task, working_task.final_category)
            except Exception as exc:
                if not self._looks_like_image_preparation_error(exc):
                    raise
                refreshed_task = self.table_adapter.map_record_to_candidate_task(record, table_config, client=client)
                title_parse_result = self.title_parser.parse_title(refreshed_task.product_title)
                refreshed_task.title_keyword_tags = title_parse_result.title_keyword_tags
                refreshed_task.title_category_hint = title_parse_result.title_category_hint
                refreshed_task.title_category_confidence = title_parse_result.title_category_confidence
                refreshed_task.final_category = working_task.final_category
                refreshed_task.category_confidence = working_task.category_confidence
                working_task = refreshed_task
                feature_result = self.analyzer.analyze_features(working_task, working_task.final_category)

            match_result = None
            if self.market_direction_matcher:
                try:
                    match_result = self.market_direction_matcher.match_candidate(
                        working_task,
                        working_task.final_category,
                    )
                except Exception:
                    match_result = None
            scored_result = self.scoring_engine.score_candidate(
                task=working_task,
                feature_result=feature_result,
                market_direction_result=match_result,
            )
            return {
                "type": "pending",
                "item": PendingAnalysisItem(
                    task=working_task,
                    feature_result=feature_result,
                    scored_result=scored_result,
                ),
            }
        except Exception as exc:
            return {
                "type": "failure",
                "record_id": record.record_id,
                "task": working_task,
                "error": str(exc),
            }

    def _consume_analysis_result(
        self,
        result: Dict[str, Any],
        table_summary: Dict[str, Any],
        pending_items: List[PendingAnalysisItem],
        progressive_items: List[PendingAnalysisItem],
        table_config: TableConfig,
        client,
        flush_every: int,
        run_id: str,
    ) -> None:
        result_type = result["type"]
        if result_type == "pending":
            item = result["item"]
            pending_items.append(item)
            progressive_items.append(item)
            if flush_every > 0 and len(progressive_items) >= flush_every:
                self._flush_progressive_items(
                    items=progressive_items,
                    table_config=table_config,
                    client=client,
                    run_id=run_id,
                )
            return

        record_id = result["record_id"]
        task = result.get("task")

        if result_type == "manual_confirm":
            self.result_writer.write_record_result(
                client=client,
                table_config=table_config,
                record_id=record_id,
                status=AnalysisStatus.NEED_MANUAL_CATEGORY.value,
                recognized_category=task.final_category,
                category_confidence=task.category_confidence,
                task=task,
                error_message="类目置信度不足，需人工复核",
                run_id=run_id,
            )
            table_summary["records"].append(
                {
                    "record_id": record_id,
                    "status": AnalysisStatus.NEED_MANUAL_CATEGORY.value,
                    "recognized_category": task.final_category,
                    "category_confidence": task.category_confidence,
                }
            )
            return

        table_summary["failed"] += 1
        failure_message = result["error"]
        try:
            self.result_writer.write_record_result(
                client=client,
                table_config=table_config,
                record_id=record_id,
                status=AnalysisStatus.FAILED.value,
                recognized_category=getattr(task, "final_category", ""),
                category_confidence=getattr(task, "category_confidence", ""),
                task=task,
                error_message=failure_message,
                run_id=run_id,
            )
        except Exception as writeback_exc:
            failure_message = "{error}; writeback_failed={writeback}".format(
                error=failure_message,
                writeback=writeback_exc,
            )
        table_summary["records"].append(
            {
                "record_id": record_id,
                "status": AnalysisStatus.FAILED.value,
                "error": failure_message,
            }
        )

    def _flush_progressive_items(
        self,
        items: List[PendingAnalysisItem],
        table_config: TableConfig,
        client,
        run_id: str,
    ) -> None:
        if not items:
            return
        for item in list(items):
            self.result_writer.write_record_result(
                client=client,
                table_config=table_config,
                record_id=item.task.source_record_id,
                status="待校准",
                recognized_category=item.task.final_category,
                category_confidence=item.task.category_confidence,
                task=item.task,
                feature_result=item.feature_result,
                scored_result=item.scored_result,
                run_id=run_id,
            )
        items[:] = []

    def _calibrate_items(self, items: List[PendingAnalysisItem]) -> List[PendingAnalysisItem]:
        grouped = defaultdict(list)
        for item in items:
            batch_key = item.task.batch_id or item.task.source_table_id
            grouped[(batch_key, item.task.final_category)].append(item)

        calibrated = []
        for _, group_items in grouped.items():
            calibrated.extend(self.scoring_engine.calibrate_group(group_items))
        return calibrated

    def _build_batch_alerts(self, items: List[PendingAnalysisItem]) -> List[Dict[str, Any]]:
        grouped = defaultdict(list)
        for item in items:
            batch_key = item.task.batch_id or item.task.source_table_id
            grouped[batch_key].append(item)

        alerts = []
        for batch_key, group_items in grouped.items():
            total = len(group_items)
            if total <= 0:
                continue
            uncovered = sum(
                1
                for item in group_items
                if getattr(item.scored_result, "market_match_status", "") == "uncovered"
            )
            ratio = float(uncovered) / float(total)
            if ratio > 0.30:
                alerts.append(
                    {
                        "type": "direction_uncovered_ratio_high",
                        "batch_key": batch_key,
                        "total_count": total,
                        "uncovered_count": uncovered,
                        "uncovered_ratio": round(ratio, 4),
                    }
                )
        return alerts

    def _start_detail_run(
        self,
        table_config: TableConfig,
        record_scope: str,
        only_risk_tag: str,
        max_workers: int,
        flush_every: int,
    ) -> str:
        detail_store = getattr(self.result_writer, "detail_store", None)
        if not detail_store or not hasattr(detail_store, "start_run"):
            return ""
        return detail_store.start_run(
            table_config=table_config,
            record_scope=record_scope,
            only_risk_tag=only_risk_tag,
            max_workers=max_workers,
            flush_every=flush_every,
        )

    def _finish_detail_run(self, run_id: str, table_config: TableConfig, table_summary: Dict[str, Any]) -> None:
        detail_store = getattr(self.result_writer, "detail_store", None)
        if not run_id or not detail_store or not hasattr(detail_store, "finish_run"):
            return
        detail_store.finish_run(
            run_id=run_id,
            table_config=table_config,
            summary=table_summary,
            alerts=table_summary.get("alerts") or [],
        )

    def _looks_like_image_preparation_error(self, exc: Exception) -> bool:
        message = str(exc)
        return "图片字段存在，但未能准备 Hermes 可读图片" in message or "缺少产品图片" in message
