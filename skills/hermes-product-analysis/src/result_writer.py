#!/usr/bin/env python3
"""结果写回。"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from src.enums import AnalysisStatus, DisplayAnalysisStatus
from src.models import CandidateTask, FeatureAnalysisResult, ScoredAnalysisResult, TableConfig


class ResultWriter(object):
    FEISHU_DECISION_KEYS = {
        "batch_priority_score",
        "suggested_action",
        "brief_reason",
        "market_match_score",
        "store_fit_score",
        "content_potential_score",
        "supply_check_status",
        "v2_total_score",
        "v2_suggested_action",
        "v2_matched_direction",
        "v2_task_pool",
        "v2_task_fit_level",
        "v2_task_fit_reason",
        "v2_task_type",
        "v2_lifecycle_status",
        "v2_direction_action",
        "v2_brief_source",
        "v2_differentiation_conclusion",
        "v2_brief_reason",
        "v2_risk_tags",
    }

    FEISHU_WORKFLOW_KEYS = {
        "analysis_status",
        "recognized_category",
        "category_confidence",
        "analysis_time",
        "analysis_error",
    }

    FEISHU_HIDDEN_DETAIL_KEYS = {
        "title_keyword_tags",
        "feature_scores_json",
        "risk_tag",
        "risk_note",
        "product_potential",
        "content_potential",
        "core_score_a",
        "route_a",
        "core_score_b",
        "route_b",
        "supply_summary",
        "market_match_status",
        "competition_reference_level",
        "competition_confidence",
        "decision_reason",
        "needs_manual_review",
        "manual_review_reason",
        "observation_tags",
        "recommended_content_formulas",
        "reserve_reason",
        "reserve_created_at",
        "reserve_expires_at",
        "reserve_status",
        "sample_check_status",
        "matched_market_direction_id",
        "matched_market_direction_name",
        "matched_market_direction_reason",
        "matched_market_direction_confidence",
        "matched_market_direction_family",
        "matched_market_direction_tier",
        "default_content_route_preference",
    }

    def __init__(self, detail_store=None):
        self.detail_store = detail_store

    def write_record_result(
        self,
        client,
        table_config: TableConfig,
        record_id: str,
        status: str,
        recognized_category: str = "",
        category_confidence: str = "",
        task: Optional[CandidateTask] = None,
        feature_result: Optional[FeatureAnalysisResult] = None,
        scored_result: Optional[ScoredAnalysisResult] = None,
        error_message: str = "",
        run_id: str = "",
    ) -> Dict[str, object]:
        fields = self.build_writeback_fields(
            table_config=table_config,
            status=status,
            recognized_category=recognized_category,
            category_confidence=category_confidence,
            task=task,
            feature_result=feature_result,
            scored_result=scored_result,
            error_message=error_message,
        )
        if self.detail_store:
            self.detail_store.persist_result(
                table_config=table_config,
                record_id=record_id,
                status=status,
                recognized_category=recognized_category,
                category_confidence=category_confidence,
                task=task,
                feature_result=feature_result,
                scored_result=scored_result,
                error_message=error_message,
                run_id=run_id,
                visible_status=self._feishu_status(status),
            )
        client.update_record_fields(record_id, fields)
        return fields

    def build_writeback_fields(
        self,
        table_config: TableConfig,
        status: str,
        recognized_category: str = "",
        category_confidence: str = "",
        task: Optional[CandidateTask] = None,
        feature_result: Optional[FeatureAnalysisResult] = None,
        scored_result: Optional[ScoredAnalysisResult] = None,
        error_message: str = "",
    ) -> Dict[str, object]:
        fields = {}
        writeback_map = table_config.writeback_map

        self._set_field(fields, writeback_map, "analysis_status", self._feishu_status(status))
        self._set_field(fields, writeback_map, "recognized_category", recognized_category or "")
        self._set_field(fields, writeback_map, "category_confidence", category_confidence or "")
        self._set_field(fields, writeback_map, "analysis_time", self._now_millis())

        if scored_result:
            self._set_field(fields, writeback_map, "batch_priority_score", scored_result.batch_priority_score)
            self._set_field(fields, writeback_map, "suggested_action", scored_result.suggested_action)
            self._set_field(fields, writeback_map, "brief_reason", scored_result.brief_reason)
            self._set_field(fields, writeback_map, "market_match_score", scored_result.market_match_score)
            self._set_field(fields, writeback_map, "store_fit_score", scored_result.store_fit_score)
            self._set_field(fields, writeback_map, "content_potential_score", scored_result.content_potential_score)
            self._set_field(fields, writeback_map, "supply_check_status", scored_result.supply_check_status)
            self._set_field(fields, writeback_map, "market_match_status", scored_result.market_match_status)
            self._set_field(fields, writeback_map, "needs_manual_review", scored_result.needs_manual_review)
            self._set_field(fields, writeback_map, "manual_review_reason", scored_result.manual_review_reason)
            self._set_field(fields, writeback_map, "observation_tags", ",".join(scored_result.observation_tags))
            v2 = scored_result.v2_shadow_result or {}
            self._set_field(fields, writeback_map, "v2_total_score", v2.get("total_score", ""))
            self._set_field(fields, writeback_map, "v2_suggested_action", v2.get("final_action_label", ""))
            self._set_field(
                fields,
                writeback_map,
                "v2_matched_direction",
                (v2.get("direction_match") or {}).get("matched_direction", ""),
            )
            market_task_fit = v2.get("market_task_fit") or {}
            unified_decision = v2.get("unified_decision") or {}
            brief_ref = v2.get("direction_execution_brief_ref") or {}
            self._set_field(fields, writeback_map, "v2_task_pool", v2.get("target_pool", ""))
            self._set_field(fields, writeback_map, "v2_task_fit_level", market_task_fit.get("fit_level", ""))
            self._set_field(fields, writeback_map, "v2_task_fit_reason", market_task_fit.get("task_fit_reason", ""))
            self._set_field(
                fields,
                writeback_map,
                "v2_task_type",
                market_task_fit.get("task_type", "") or unified_decision.get("task_type", ""),
            )
            self._set_field(fields, writeback_map, "v2_lifecycle_status", v2.get("lifecycle_status", ""))
            self._set_field(fields, writeback_map, "v2_direction_action", unified_decision.get("direction_action", ""))
            self._set_field(fields, writeback_map, "v2_brief_source", brief_ref.get("brief_source", ""))
            self._set_field(fields, writeback_map, "v2_differentiation_conclusion", v2.get("v2_differentiation_conclusion", ""))
            self._set_field(fields, writeback_map, "v2_brief_reason", v2.get("v2_brief_reason", ""))
            self._set_field(fields, writeback_map, "v2_risk_tags", ",".join(v2.get("risk_flags") or []))
            self._set_field(fields, writeback_map, "analysis_error", "")
        else:
            for key in [
                "batch_priority_score",
                "suggested_action",
                "brief_reason",
                "market_match_score",
                "store_fit_score",
                "content_potential_score",
                "supply_check_status",
                "market_match_status",
                "needs_manual_review",
                "manual_review_reason",
                "observation_tags",
                "v2_total_score",
                "v2_suggested_action",
                "v2_matched_direction",
                "v2_task_pool",
                "v2_task_fit_level",
                "v2_task_fit_reason",
                "v2_task_type",
                "v2_lifecycle_status",
                "v2_direction_action",
                "v2_brief_source",
                "v2_differentiation_conclusion",
                "v2_brief_reason",
                "v2_risk_tags",
            ]:
                self._set_field(fields, writeback_map, key, "")
            self._set_field(fields, writeback_map, "analysis_error", (error_message or "")[:500])
        for key in sorted(self.FEISHU_HIDDEN_DETAIL_KEYS):
            self._set_field(fields, writeback_map, key, self._empty_field_value(key))
        return fields

    def _set_field(self, fields: Dict[str, object], writeback_map: Dict[str, str], key: str, value: object) -> None:
        field_name = writeback_map.get(key)
        if field_name:
            fields[field_name] = value

    def _empty_field_value(self, key: str):
        if key in {"reserve_created_at", "reserve_expires_at"}:
            return None
        return ""

    def _now_millis(self) -> int:
        return int(datetime.now().timestamp() * 1000)

    def _feishu_status(self, raw_status: str) -> str:
        status = str(raw_status or "").strip()
        if status in {"初评中", "待校准"}:
            return DisplayAnalysisStatus.IN_PROGRESS.value
        if status == AnalysisStatus.COMPLETED.value:
            return DisplayAnalysisStatus.COMPLETED.value
        return DisplayAnalysisStatus.FAILED.value
