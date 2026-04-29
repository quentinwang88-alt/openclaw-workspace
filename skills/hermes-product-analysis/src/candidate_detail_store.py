#!/usr/bin/env python3
"""Persist full candidate-analysis details outside Feishu."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


class CandidateDetailStore(object):
    def __init__(self, db_path: Path, reports_dir: Optional[Path] = None):
        self.db_path = Path(db_path)
        self.reports_dir = Path(reports_dir) if reports_dir else self.db_path.parent / "reports"

    def start_run(
        self,
        table_config,
        record_scope: str,
        only_risk_tag: str = "",
        max_workers: int = 1,
        flush_every: int = 1,
    ) -> str:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        run_id = "{table}__{stamp}__{suffix}".format(
            table=str(getattr(table_config, "table_id", "") or "table"),
            stamp=datetime.now().strftime("%Y%m%d%H%M%S"),
            suffix=uuid4().hex[:8],
        )
        started_at_epoch = int(datetime.now().timestamp() * 1000)
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO candidate_analysis_runs (
                    run_id,
                    source_table_id,
                    source_table_name,
                    record_scope,
                    only_risk_tag,
                    max_workers,
                    flush_every,
                    run_status,
                    started_at_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    str(getattr(table_config, "table_id", "") or ""),
                    str(getattr(table_config, "table_name", "") or ""),
                    str(record_scope or ""),
                    str(only_risk_tag or ""),
                    int(max_workers or 1),
                    int(flush_every or 0),
                    "running",
                    started_at_epoch,
                ),
            )
            conn.commit()
        return run_id

    def finish_run(
        self,
        run_id: str,
        table_config,
        summary: Dict[str, Any],
        alerts: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, str]:
        alerts = list(alerts or [])
        reports = self.write_run_reports(run_id=run_id, table_config=table_config, recent_runs=5)
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            conn.execute(
                """
                UPDATE candidate_analysis_runs
                SET run_status = ?,
                    finished_at_epoch = ?,
                    processed_count = ?,
                    completed_count = ?,
                    failed_count = ?,
                    alert_count = ?,
                    alerts_json = ?,
                    summary_json = ?,
                    ab_diff_report_path = ?,
                    recent_diff_report_path = ?,
                    v2_shadow_report_path = ?
                WHERE run_id = ?
                """,
                (
                    "completed",
                    int(datetime.now().timestamp() * 1000),
                    int(summary.get("processed") or 0),
                    int(summary.get("completed") or 0),
                    int(summary.get("failed") or 0),
                    len(alerts),
                    json.dumps(alerts, ensure_ascii=False, sort_keys=True),
                    json.dumps(summary, ensure_ascii=False, sort_keys=True),
                    reports.get("ab_diff_markdown_path", ""),
                    reports.get("recent_diff_markdown_path", ""),
                    reports.get("v2_shadow_markdown_path", ""),
                    run_id,
                ),
            )
            conn.commit()
        return reports

    def persist_result(
        self,
        table_config,
        record_id: str,
        status: str,
        recognized_category: str = "",
        category_confidence: str = "",
        task=None,
        feature_result=None,
        scored_result=None,
        error_message: str = "",
        run_id: str = "",
        visible_status: str = "",
    ) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        payload_task = self._serialize(task)
        payload_feature = self._serialize(feature_result)
        payload_scored = self._serialize(scored_result)
        updated_at_epoch = int(datetime.now().timestamp() * 1000)

        source_table_id = str(getattr(table_config, "table_id", "") or "")
        source_table_name = str(getattr(table_config, "table_name", "") or "")
        source_type = str(getattr(task, "source_type", "") or "")
        batch_id = str(getattr(task, "batch_id", "") or "")
        target_market = str(getattr(task, "target_market", "") or "")
        product_title = str(getattr(task, "product_title", "") or "")
        manual_category = str(getattr(task, "manual_category", "") or "")
        suggested_action = str(getattr(scored_result, "suggested_action", "") or "")
        batch_priority_score = getattr(scored_result, "batch_priority_score", None)
        market_match_score = getattr(scored_result, "market_match_score", None)
        market_match_status = str(getattr(scored_result, "market_match_status", "") or "")
        store_fit_score = getattr(scored_result, "store_fit_score", None)
        content_potential_score = getattr(scored_result, "content_potential_score", None)
        core_score_a = getattr(scored_result, "core_score_a", None)
        route_a = str(getattr(scored_result, "route_a", "") or "")
        core_score_b = getattr(scored_result, "core_score_b", None)
        route_b = str(getattr(scored_result, "route_b", "") or "")
        supply_check_status = str(getattr(scored_result, "supply_check_status", "") or "")
        brief_reason = str(getattr(scored_result, "brief_reason", "") or "")
        needs_manual_review = bool(getattr(scored_result, "needs_manual_review", False))
        manual_review_reason = str(getattr(scored_result, "manual_review_reason", "") or "")
        observation_tags = list(getattr(scored_result, "observation_tags", []) or [])
        v2_result = dict(getattr(scored_result, "v2_shadow_result", {}) or {})
        v2_total_score = v2_result.get("total_score")
        v2_final_action = str(v2_result.get("final_action") or "")
        v2_matched_direction = str((v2_result.get("direction_match") or {}).get("matched_direction") or "")
        v2_risk_tags = list(v2_result.get("risk_flags") or [])

        row_payload = (
            source_table_id,
            source_table_name,
            str(record_id or ""),
            str(status or ""),
            str(visible_status or ""),
            str(recognized_category or ""),
            str(category_confidence or ""),
            source_type,
            batch_id,
            target_market,
            product_title,
            manual_category,
            suggested_action,
            self._nullable_float(batch_priority_score),
            self._nullable_float(market_match_score),
            market_match_status,
            self._nullable_float(store_fit_score),
            self._nullable_float(content_potential_score),
            self._nullable_float(core_score_a),
            route_a,
            self._nullable_float(core_score_b),
            route_b,
            supply_check_status,
            brief_reason,
            1 if needs_manual_review else 0,
            manual_review_reason,
            json.dumps(observation_tags, ensure_ascii=False, sort_keys=True),
            str(error_message or ""),
            json.dumps(payload_task, ensure_ascii=False, sort_keys=True),
            json.dumps(payload_feature, ensure_ascii=False, sort_keys=True),
            json.dumps(payload_scored, ensure_ascii=False, sort_keys=True),
            updated_at_epoch,
        )

        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO candidate_analysis_results (
                    source_table_id,
                    source_table_name,
                    source_record_id,
                    analysis_status,
                    visible_status,
                    recognized_category,
                    category_confidence,
                    source_type,
                    batch_id,
                    target_market,
                    product_title,
                    manual_category,
                    suggested_action,
                    batch_priority_score,
                    market_match_score,
                    market_match_status,
                    store_fit_score,
                    content_potential_score,
                    core_score_a,
                    route_a,
                    core_score_b,
                    route_b,
                    supply_check_status,
                    brief_reason,
                    needs_manual_review,
                    manual_review_reason,
                    observation_tags_json,
                    analysis_error,
                    task_json,
                    feature_result_json,
                    scored_result_json,
                    updated_at_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_table_id, source_record_id) DO UPDATE SET
                    source_table_name=excluded.source_table_name,
                    analysis_status=excluded.analysis_status,
                    visible_status=excluded.visible_status,
                    recognized_category=excluded.recognized_category,
                    category_confidence=excluded.category_confidence,
                    source_type=excluded.source_type,
                    batch_id=excluded.batch_id,
                    target_market=excluded.target_market,
                    product_title=excluded.product_title,
                    manual_category=excluded.manual_category,
                    suggested_action=excluded.suggested_action,
                    batch_priority_score=excluded.batch_priority_score,
                    market_match_score=excluded.market_match_score,
                    market_match_status=excluded.market_match_status,
                    store_fit_score=excluded.store_fit_score,
                    content_potential_score=excluded.content_potential_score,
                    core_score_a=excluded.core_score_a,
                    route_a=excluded.route_a,
                    core_score_b=excluded.core_score_b,
                    route_b=excluded.route_b,
                    supply_check_status=excluded.supply_check_status,
                    brief_reason=excluded.brief_reason,
                    needs_manual_review=excluded.needs_manual_review,
                    manual_review_reason=excluded.manual_review_reason,
                    observation_tags_json=excluded.observation_tags_json,
                    analysis_error=excluded.analysis_error,
                    task_json=excluded.task_json,
                    feature_result_json=excluded.feature_result_json,
                    scored_result_json=excluded.scored_result_json,
                    updated_at_epoch=excluded.updated_at_epoch
                """,
                row_payload,
            )
            conn.execute(
                """
                UPDATE candidate_analysis_results
                SET v2_total_score = ?,
                    v2_final_action = ?,
                    v2_matched_direction = ?,
                    v2_risk_tags_json = ?,
                    v2_result_json = ?
                WHERE source_table_id = ? AND source_record_id = ?
                """,
                (
                    self._nullable_float(v2_total_score),
                    v2_final_action,
                    v2_matched_direction,
                    json.dumps(v2_risk_tags, ensure_ascii=False, sort_keys=True),
                    json.dumps(v2_result, ensure_ascii=False, sort_keys=True),
                    source_table_id,
                    str(record_id or ""),
                ),
            )

            if run_id:
                conn.execute(
                    """
                    INSERT INTO candidate_analysis_run_results (
                        run_id,
                        source_table_id,
                        source_table_name,
                        source_record_id,
                        analysis_status,
                        visible_status,
                        recognized_category,
                        category_confidence,
                        source_type,
                        batch_id,
                        target_market,
                        product_title,
                        manual_category,
                        suggested_action,
                        batch_priority_score,
                        market_match_score,
                        market_match_status,
                        store_fit_score,
                        content_potential_score,
                        core_score_a,
                        route_a,
                        core_score_b,
                        route_b,
                        supply_check_status,
                        brief_reason,
                        needs_manual_review,
                        manual_review_reason,
                        observation_tags_json,
                        analysis_error,
                        task_json,
                        feature_result_json,
                        scored_result_json,
                        updated_at_epoch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, source_table_id, source_record_id) DO UPDATE SET
                        source_table_name=excluded.source_table_name,
                        analysis_status=excluded.analysis_status,
                        visible_status=excluded.visible_status,
                        recognized_category=excluded.recognized_category,
                        category_confidence=excluded.category_confidence,
                        source_type=excluded.source_type,
                        batch_id=excluded.batch_id,
                        target_market=excluded.target_market,
                        product_title=excluded.product_title,
                        manual_category=excluded.manual_category,
                        suggested_action=excluded.suggested_action,
                        batch_priority_score=excluded.batch_priority_score,
                        market_match_score=excluded.market_match_score,
                        market_match_status=excluded.market_match_status,
                        store_fit_score=excluded.store_fit_score,
                        content_potential_score=excluded.content_potential_score,
                        core_score_a=excluded.core_score_a,
                        route_a=excluded.route_a,
                        core_score_b=excluded.core_score_b,
                        route_b=excluded.route_b,
                        supply_check_status=excluded.supply_check_status,
                        brief_reason=excluded.brief_reason,
                        needs_manual_review=excluded.needs_manual_review,
                        manual_review_reason=excluded.manual_review_reason,
                        observation_tags_json=excluded.observation_tags_json,
                        analysis_error=excluded.analysis_error,
                        task_json=excluded.task_json,
                        feature_result_json=excluded.feature_result_json,
                        scored_result_json=excluded.scored_result_json,
                        updated_at_epoch=excluded.updated_at_epoch
                    """,
                    (run_id,) + row_payload,
                )
                conn.execute(
                    """
                    UPDATE candidate_analysis_run_results
                    SET v2_total_score = ?,
                        v2_final_action = ?,
                        v2_matched_direction = ?,
                        v2_risk_tags_json = ?,
                        v2_result_json = ?
                    WHERE run_id = ? AND source_table_id = ? AND source_record_id = ?
                    """,
                    (
                        self._nullable_float(v2_total_score),
                        v2_final_action,
                        v2_matched_direction,
                        json.dumps(v2_risk_tags, ensure_ascii=False, sort_keys=True),
                        json.dumps(v2_result, ensure_ascii=False, sort_keys=True),
                        run_id,
                        source_table_id,
                        str(record_id or ""),
                    ),
                )

            conn.commit()

    def write_run_reports(
        self,
        run_id: str,
        table_config,
        recent_runs: int = 5,
    ) -> Dict[str, str]:
        report_dir = self.reports_dir / str(getattr(table_config, "table_id", "") or "default")
        report_dir.mkdir(parents=True, exist_ok=True)

        ab_payload = self.build_ab_diff_payload(run_id=run_id)
        ab_stem = "ab_diff_candidates__{run_id}".format(run_id=run_id)
        ab_json_path = report_dir / "{stem}.json".format(stem=ab_stem)
        ab_md_path = report_dir / "{stem}.md".format(stem=ab_stem)
        ab_json_path.write_text(json.dumps(ab_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ab_md_path.write_text(str(ab_payload.get("markdown") or ""), encoding="utf-8")
        (report_dir / "ab_diff_candidates.json").write_text(json.dumps(ab_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        (report_dir / "ab_diff_candidates.md").write_text(str(ab_payload.get("markdown") or ""), encoding="utf-8")

        recent_payload = self.build_recent_run_diff(
            table_id=str(getattr(table_config, "table_id", "") or ""),
            current_run_id=run_id,
            recent_runs=recent_runs,
        )
        recent_stem = "recent_run_diff__{run_id}".format(run_id=run_id)
        recent_json_path = report_dir / "{stem}.json".format(stem=recent_stem)
        recent_md_path = report_dir / "{stem}.md".format(stem=recent_stem)
        recent_json_path.write_text(json.dumps(recent_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        recent_md_path.write_text(str(recent_payload.get("markdown") or ""), encoding="utf-8")
        (report_dir / "recent_run_diff.json").write_text(json.dumps(recent_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        (report_dir / "recent_run_diff.md").write_text(str(recent_payload.get("markdown") or ""), encoding="utf-8")

        v2_payload = self.build_v2_shadow_diagnostics(run_id=run_id)
        v2_stem = "v2_shadow_diagnostics__{run_id}".format(run_id=run_id)
        v2_json_path = report_dir / "{stem}.json".format(stem=v2_stem)
        v2_md_path = report_dir / "{stem}.md".format(stem=v2_stem)
        v2_json_path.write_text(json.dumps(v2_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        v2_md_path.write_text(str(v2_payload.get("markdown") or ""), encoding="utf-8")
        (report_dir / "v2_shadow_diagnostics.json").write_text(json.dumps(v2_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        (report_dir / "v2_shadow_diagnostics.md").write_text(str(v2_payload.get("markdown") or ""), encoding="utf-8")

        return {
            "ab_diff_json_path": str(ab_json_path),
            "ab_diff_markdown_path": str(ab_md_path),
            "recent_diff_json_path": str(recent_json_path),
            "recent_diff_markdown_path": str(recent_md_path),
            "v2_shadow_json_path": str(v2_json_path),
            "v2_shadow_markdown_path": str(v2_md_path),
        }

    def build_ab_diff_payload(self, run_id: str) -> Dict[str, Any]:
        rows = self._fetchall(
            """
            SELECT
                source_record_id,
                product_title,
                core_score_a,
                route_a,
                core_score_b,
                route_b,
                suggested_action,
                brief_reason,
                market_match_status,
                needs_manual_review,
                manual_review_reason
            FROM candidate_analysis_run_results
            WHERE run_id = ?
            ORDER BY COALESCE(core_score_a, 0) DESC, source_record_id ASC
            """,
            (run_id,),
        )
        items = []
        for row in rows:
            delta = round(float(row[4] or 0.0) - float(row[2] or 0.0), 2)
            route_a = str(row[3] or "")
            route_b = str(row[5] or "")
            items.append(
                {
                    "record_id": str(row[0] or ""),
                    "product_title": str(row[1] or ""),
                    "core_score_a": self._nullable_float(row[2]),
                    "route_a": route_a,
                    "core_score_b": self._nullable_float(row[4]),
                    "route_b": route_b,
                    "score_delta_b_minus_a": delta,
                    "suggested_action": str(row[6] or ""),
                    "brief_reason": str(row[7] or ""),
                    "market_match_status": str(row[8] or ""),
                    "needs_manual_review": bool(row[9]),
                    "manual_review_reason": str(row[10] or ""),
                }
            )

        route_diff_items = [item for item in items if item["route_a"] != item["route_b"]]
        score_gap_items = [item for item in items if abs(float(item["score_delta_b_minus_a"] or 0.0)) >= 5.0]
        promoted_by_b = [item for item in items if self._route_rank(item["route_b"]) > self._route_rank(item["route_a"]) or float(item["score_delta_b_minus_a"] or 0.0) >= 5.0]
        suppressed_by_b = [item for item in items if self._route_rank(item["route_b"]) < self._route_rank(item["route_a"]) or float(item["score_delta_b_minus_a"] or 0.0) <= -5.0]

        payload = {
            "run_id": run_id,
            "total_records": len(items),
            "route_diff_count": len(route_diff_items),
            "score_gap_count": len(score_gap_items),
            "route_diff_items": route_diff_items[:100],
            "score_gap_items": score_gap_items[:100],
            "promoted_by_b": promoted_by_b[:100],
            "suppressed_by_b": suppressed_by_b[:100],
        }
        payload["markdown"] = self._render_ab_diff_markdown(payload)
        return payload

    def build_v2_shadow_diagnostics(self, run_id: str) -> Dict[str, Any]:
        rows = self._fetchall(
            """
            SELECT
                source_record_id,
                product_title,
                core_score_a,
                route_a,
                v2_total_score,
                v2_final_action,
                v2_matched_direction,
                v2_risk_tags_json,
                v2_result_json,
                task_json
            FROM candidate_analysis_run_results
            WHERE run_id = ?
            ORDER BY COALESCE(v2_total_score, 0) DESC, source_record_id ASC
            """,
            (run_id,),
        )
        items = []
        for row in rows:
            v2_result = self._loads_json(row[8], {})
            risk_flags = self._loads_json(row[7], [])
            core_select = str(row[3] or "") == "priority_test"
            v2_select = str(row[5] or "") == "select"
            items.append(
                {
                    "record_id": str(row[0] or ""),
                    "product_title": str(row[1] or ""),
                    "core_score_a": self._nullable_float(row[2]),
                    "core_action": str(row[3] or ""),
                    "v2_total_score": self._nullable_float(row[4]),
                    "v2_action": str(row[5] or ""),
                    "v2_matched_direction": str(row[6] or ""),
                    "risk_flags": risk_flags if isinstance(risk_flags, list) else [],
                    "v2_result": v2_result if isinstance(v2_result, dict) else {},
                    "task": self._loads_json(row[9], {}),
                    "core_select": core_select,
                    "v2_select": v2_select,
                }
            )

        total = len(items)
        v2_select_count = sum(1 for item in items if item["v2_select"])
        core_select_count = sum(1 for item in items if item["core_select"])
        both_select = [item for item in items if item["v2_select"] and item["core_select"]]
        v2_only = [item for item in items if item["v2_select"] and not item["core_select"]]
        core_only = [item for item in items if item["core_select"] and not item["v2_select"]]
        both_not_select = [item for item in items if not item["v2_select"] and not item["core_select"]]
        disagreement_count = len(v2_only) + len(core_only)

        action_distribution: Dict[str, int] = {}
        for item in items:
            action = item["v2_action"] or "missing"
            action_distribution[action] = action_distribution.get(action, 0) + 1
        scores = [float(item["v2_total_score"] or 0.0) for item in items]
        direction_uncertain_count = sum(1 for item in items if item["v2_matched_direction"] in {"方向不确定", ""})
        other_direction_count = sum(1 for item in items if str(item["v2_matched_direction"]).lower() == "other")
        multi_direction_count = sum(1 for item in items if "multi_direction_candidate" in item["risk_flags"])
        missing_head_products_count = sum(1 for item in items if "missing_head_products" in item["risk_flags"])
        differentiation_counts: Dict[str, int] = {"high": 0, "medium": 0, "low": 0, "insufficient": 0}
        for item in items:
            confidence = str(((item.get("v2_result") or {}).get("differentiation") or {}).get("confidence") or "")
            if confidence in differentiation_counts:
                differentiation_counts[confidence] += 1

        diagnostics = {
            "total_products": total,
            "action_distribution": action_distribution,
            "score_distribution": {
                "p50": self._percentile(scores, 50),
                "p75": self._percentile(scores, 75),
                "p85": self._percentile(scores, 85),
                "p90": self._percentile(scores, 90),
            },
            "direction_coverage": {
                "other_direction_count": other_direction_count,
                "other_direction_ratio": self._ratio(other_direction_count, total),
                "direction_uncertain_count": direction_uncertain_count,
                "multi_direction_candidate_count": multi_direction_count,
                "multi_direction_candidate_ratio": self._ratio(multi_direction_count, total),
            },
            "constraint_stats": {
                "direction_constraint_downgrade_count": sum(1 for item in items if "direction_constraint_downgrade" in item["risk_flags"]),
                "cautious_test_quota_downgrade_count": sum(1 for item in items if "cautious_test_quota_downgrade" in item["risk_flags"]),
                "study_top_upgrade_count": sum(
                    1
                    for item in items
                    if ((item.get("v2_result") or {}).get("direction_action_constraint") or {}).get("study_top_upgraded")
                ),
            },
            "missing_data": {
                "price_band_missing_count": sum(1 for item in items if "price_band_missing" in item["risk_flags"]),
                "supply_match_missing_count": sum(1 for item in items if "supply_match_missing" in item["risk_flags"]),
                "missing_price_count": sum(1 for item in items if "price_missing" in item["risk_flags"]),
                "missing_image_count": sum(1 for item in items if not ((item.get("task") or {}).get("product_images") or [])),
                "missing_listing_date_count": sum(1 for item in items if not (item.get("task") or {}).get("listing_date")),
                "missing_head_products_count": missing_head_products_count,
            },
            "differentiation_stats": {
                "differentiation_high_count": differentiation_counts["high"],
                "differentiation_medium_count": differentiation_counts["medium"],
                "differentiation_low_count": differentiation_counts["low"],
                "differentiation_insufficient_count": differentiation_counts["insufficient"],
            },
            "shadow_compare": {
                "v2_vs_core_a_diff_count": disagreement_count,
                "v2_vs_core_a_diff_rate": self._ratio(disagreement_count, total),
            },
        }
        payload = {
            "run_id": run_id,
            "v2_vs_core_a_diagnostics": {
                "total_products": total,
                "v2_select_count": v2_select_count,
                "core_a_select_count": core_select_count,
                "both_select_count": len(both_select),
                "v2_only_select_count": len(v2_only),
                "core_a_only_select_count": len(core_only),
                "both_reject_count": len(both_not_select),
                "disagreement_count": disagreement_count,
                "disagreement_rate": self._ratio(disagreement_count, total),
            },
            "samples": {
                "both_select": self._v2_sample_items(both_select),
                "v2_only_select": self._v2_sample_items(v2_only),
                "core_a_only_select": self._v2_sample_items(core_only),
                "both_not_select": self._v2_sample_items(both_not_select),
            },
            "batch_diagnostics": diagnostics,
            "alerts": self._v2_alerts(diagnostics),
        }
        payload["markdown"] = self._render_v2_shadow_markdown(payload)
        return payload

    def build_recent_run_diff(self, table_id: str, current_run_id: str, recent_runs: int = 5) -> Dict[str, Any]:
        runs = self._fetchall(
            """
            SELECT run_id, started_at_epoch
            FROM candidate_analysis_runs
            WHERE source_table_id = ?
            ORDER BY started_at_epoch DESC
            LIMIT ?
            """,
            (table_id, int(max(recent_runs, 2))),
        )
        run_ids = [str(row[0] or "") for row in runs if str(row[0] or "")]
        comparisons = []
        current_rows = self._load_run_result_map(current_run_id)
        for previous_run_id in run_ids:
            if previous_run_id == current_run_id:
                continue
            previous_rows = self._load_run_result_map(previous_run_id)
            if not previous_rows:
                continue
            shared_ids = sorted(set(current_rows) & set(previous_rows))
            route_changes = []
            reject_changes = []
            review_changes = []
            for record_id in shared_ids:
                current = current_rows[record_id]
                previous = previous_rows[record_id]
                if current["route_a"] != previous["route_a"]:
                    route_changes.append(record_id)
                if (current["route_a"] == "reject") != (previous["route_a"] == "reject"):
                    reject_changes.append(record_id)
                if bool(current["needs_manual_review"]) != bool(previous["needs_manual_review"]):
                    review_changes.append(record_id)
            comparisons.append(
                {
                    "previous_run_id": previous_run_id,
                    "shared_record_count": len(shared_ids),
                    "route_change_count": len(route_changes),
                    "reject_change_count": len(reject_changes),
                    "review_change_count": len(review_changes),
                    "route_change_samples": route_changes[:20],
                    "reject_change_samples": reject_changes[:20],
                    "review_change_samples": review_changes[:20],
                }
            )
        payload = {
            "table_id": table_id,
            "current_run_id": current_run_id,
            "recent_runs_considered": run_ids,
            "comparisons": comparisons,
        }
        payload["markdown"] = self._render_recent_diff_markdown(payload)
        return payload

    def _load_run_result_map(self, run_id: str) -> Dict[str, Dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT source_record_id, route_a, needs_manual_review
            FROM candidate_analysis_run_results
            WHERE run_id = ?
            """,
            (run_id,),
        )
        return {
            str(row[0] or ""): {
                "route_a": str(row[1] or ""),
                "needs_manual_review": bool(row[2]),
            }
            for row in rows
            if str(row[0] or "")
        }

    def _fetchall(self, sql: str, params: tuple) -> List[tuple]:
        if not self.db_path.exists():
            return []
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            return conn.execute(sql, params).fetchall()

    def _render_ab_diff_markdown(self, payload: Dict[str, Any]) -> str:
        lines = [
            "# A/B 权重差异样本",
            "",
            "run_id: {run_id}".format(run_id=payload.get("run_id") or ""),
            "总记录数: {total}".format(total=payload.get("total_records") or 0),
            "A/B route 不一致: {count}".format(count=payload.get("route_diff_count") or 0),
            "A/B 分差 >= 5: {count}".format(count=payload.get("score_gap_count") or 0),
            "",
            "## Route 不一致样本",
        ]
        route_items = payload.get("route_diff_items") or []
        if not route_items:
            lines.append("无")
        else:
            for item in route_items[:20]:
                lines.append(
                    "- {record_id} | A {route_a} ({core_score_a}) -> B {route_b} ({core_score_b}) | {title}".format(
                        record_id=item.get("record_id") or "",
                        route_a=item.get("route_a") or "",
                        core_score_a=item.get("core_score_a"),
                        route_b=item.get("route_b") or "",
                        core_score_b=item.get("core_score_b"),
                        title=item.get("product_title") or "",
                    )
                )
        lines.extend(["", "## 被 B 推高的样本"])
        promoted = payload.get("promoted_by_b") or []
        if not promoted:
            lines.append("无")
        else:
            for item in promoted[:20]:
                lines.append(
                    "- {record_id} | Δ {delta:+.2f} | A {route_a} -> B {route_b} | {title}".format(
                        record_id=item.get("record_id") or "",
                        delta=float(item.get("score_delta_b_minus_a") or 0.0),
                        route_a=item.get("route_a") or "",
                        route_b=item.get("route_b") or "",
                        title=item.get("product_title") or "",
                    )
                )
        lines.extend(["", "## 被 B 压低的样本"])
        suppressed = payload.get("suppressed_by_b") or []
        if not suppressed:
            lines.append("无")
        else:
            for item in suppressed[:20]:
                lines.append(
                    "- {record_id} | Δ {delta:+.2f} | A {route_a} -> B {route_b} | {title}".format(
                        record_id=item.get("record_id") or "",
                        delta=float(item.get("score_delta_b_minus_a") or 0.0),
                        route_a=item.get("route_a") or "",
                        route_b=item.get("route_b") or "",
                        title=item.get("product_title") or "",
                    )
                )
        return "\n".join(lines).strip() + "\n"

    def _render_recent_diff_markdown(self, payload: Dict[str, Any]) -> str:
        lines = [
            "# 最近运行差异报告",
            "",
            "table_id: {table_id}".format(table_id=payload.get("table_id") or ""),
            "current_run_id: {run_id}".format(run_id=payload.get("current_run_id") or ""),
            "",
        ]
        comparisons = payload.get("comparisons") or []
        if not comparisons:
            lines.append("暂无可对比的历史 run。")
            return "\n".join(lines).strip() + "\n"
        for item in comparisons:
            lines.extend(
                [
                    "## 对比 {run_id}".format(run_id=item.get("previous_run_id") or ""),
                    "- shared_record_count: {count}".format(count=item.get("shared_record_count") or 0),
                    "- route_change_count: {count}".format(count=item.get("route_change_count") or 0),
                    "- reject_change_count: {count}".format(count=item.get("reject_change_count") or 0),
                    "- review_change_count: {count}".format(count=item.get("review_change_count") or 0),
                    "",
                ]
            )
        return "\n".join(lines).strip() + "\n"

    def _render_v2_shadow_markdown(self, payload: Dict[str, Any]) -> str:
        diagnostics = payload.get("v2_vs_core_a_diagnostics") or {}
        batch = payload.get("batch_diagnostics") or {}
        lines = [
            "# V2 Shadow 对账诊断",
            "",
            "run_id: {run_id}".format(run_id=payload.get("run_id") or ""),
            "总商品数: {count}".format(count=diagnostics.get("total_products") or 0),
            "V2 select: {count}".format(count=diagnostics.get("v2_select_count") or 0),
            "core_score_a priority: {count}".format(count=diagnostics.get("core_a_select_count") or 0),
            "分歧数: {count} ({rate:.1%})".format(
                count=diagnostics.get("disagreement_count") or 0,
                rate=float(diagnostics.get("disagreement_rate") or 0.0),
            ),
            "",
            "## 批次分布",
            "- action_distribution: {value}".format(value=batch.get("action_distribution") or {}),
            "- score_distribution: {value}".format(value=batch.get("score_distribution") or {}),
            "- direction_coverage: {value}".format(value=batch.get("direction_coverage") or {}),
            "- differentiation_stats: {value}".format(value=batch.get("differentiation_stats") or {}),
            "",
            "## Alerts",
        ]
        alerts = payload.get("alerts") or []
        if not alerts:
            lines.append("无")
        else:
            for alert in alerts:
                lines.append("- {type}: {message}".format(type=alert.get("type") or "", message=alert.get("message") or ""))
        samples = payload.get("samples") or {}
        for key, title in [
            ("both_select", "双方都推荐"),
            ("v2_only_select", "V2 推荐但 core_score_a 未推荐"),
            ("core_a_only_select", "core_score_a 推荐但 V2 未推荐"),
            ("both_not_select", "双方都不推荐"),
        ]:
            lines.extend(["", "## {title}".format(title=title)])
            items = samples.get(key) or []
            if not items:
                lines.append("无")
                continue
            for item in items[:20]:
                lines.append(
                    "- {record_id} | V2 {v2_action}({v2_score}) / A {core_action}({core_score}) | {title}".format(
                        record_id=item.get("record_id") or "",
                        v2_action=item.get("v2_action") or "",
                        v2_score=item.get("v2_total_score"),
                        core_action=item.get("core_action") or "",
                        core_score=item.get("core_score_a"),
                        title=item.get("product_title") or "",
                    )
                )
        return "\n".join(lines).strip() + "\n"

    def _v2_sample_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "record_id": item.get("record_id") or "",
                "product_title": item.get("product_title") or "",
                "core_score_a": item.get("core_score_a"),
                "core_action": item.get("core_action") or "",
                "v2_total_score": item.get("v2_total_score"),
                "v2_action": item.get("v2_action") or "",
                "v2_matched_direction": item.get("v2_matched_direction") or "",
                "risk_flags": item.get("risk_flags") or [],
            }
            for item in items[:100]
        ]

    def _v2_alerts(self, diagnostics: Dict[str, Any]) -> List[Dict[str, str]]:
        alerts = []
        direction = diagnostics.get("direction_coverage") or {}
        missing = diagnostics.get("missing_data") or {}
        shadow = diagnostics.get("shadow_compare") or {}
        action_distribution = diagnostics.get("action_distribution") or {}
        total = int(diagnostics.get("total_products") or 0)
        if float(direction.get("other_direction_ratio") or 0.0) > 0.30:
            alerts.append({"type": "direction_dictionary_coverage_low", "message": "other 方向比例超过 30%，方向字典覆盖不足。"})
        if float(direction.get("multi_direction_candidate_ratio") or 0.0) > 0.25:
            alerts.append({"type": "direction_dictionary_discrimination_low", "message": "多方向候选比例超过 25%，方向字典区分度不足。"})
        if total and int(missing.get("missing_head_products_count") or 0) / total > 0.30:
            alerts.append({"type": "head_product_pool_incomplete", "message": "缺少头部样本比例偏高，差异化评分置信度受限。"})
        select_count = int(action_distribution.get("select") or 0)
        if total and (select_count == 0 or select_count / total > 0.35):
            alerts.append({"type": "v2_select_distribution_needs_calibration", "message": "V2 select 数量过少或过多，阈值需要校准。"})
        if float(shadow.get("v2_vs_core_a_diff_rate") or 0.0) > 0.40:
            alerts.append({"type": "v2_core_a_diff_high", "message": "V2 与旧流程分歧率超过 40%，需要人工对账。"})
        return alerts

    def _percentile(self, values: List[float], percentile: int) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(float(item) for item in values)
        if len(ordered) == 1:
            return round(ordered[0], 2)
        rank = (len(ordered) - 1) * (float(percentile) / 100.0)
        lower = int(rank)
        upper = min(lower + 1, len(ordered) - 1)
        weight = rank - lower
        return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 2)

    def _ratio(self, count: int, total: int) -> float:
        return round(float(count) / float(total), 4) if total else 0.0

    def _loads_json(self, value: Any, fallback: Any) -> Any:
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(str(value or ""))
        except (TypeError, ValueError, json.JSONDecodeError):
            return fallback

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candidate_analysis_runs (
                run_id TEXT PRIMARY KEY,
                source_table_id TEXT NOT NULL DEFAULT '',
                source_table_name TEXT NOT NULL DEFAULT '',
                record_scope TEXT NOT NULL DEFAULT '',
                only_risk_tag TEXT NOT NULL DEFAULT '',
                max_workers INTEGER NOT NULL DEFAULT 1,
                flush_every INTEGER NOT NULL DEFAULT 0,
                run_status TEXT NOT NULL DEFAULT 'running',
                started_at_epoch INTEGER NOT NULL DEFAULT 0,
                finished_at_epoch INTEGER NOT NULL DEFAULT 0,
                processed_count INTEGER NOT NULL DEFAULT 0,
                completed_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                alert_count INTEGER NOT NULL DEFAULT 0,
                alerts_json TEXT NOT NULL DEFAULT '[]',
                summary_json TEXT NOT NULL DEFAULT '{}',
                ab_diff_report_path TEXT NOT NULL DEFAULT '',
                recent_diff_report_path TEXT NOT NULL DEFAULT '',
                v2_shadow_report_path TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candidate_analysis_results (
                source_table_id TEXT NOT NULL,
                source_table_name TEXT NOT NULL DEFAULT '',
                source_record_id TEXT NOT NULL,
                analysis_status TEXT NOT NULL DEFAULT '',
                visible_status TEXT NOT NULL DEFAULT '',
                recognized_category TEXT NOT NULL DEFAULT '',
                category_confidence TEXT NOT NULL DEFAULT '',
                source_type TEXT NOT NULL DEFAULT '',
                batch_id TEXT NOT NULL DEFAULT '',
                target_market TEXT NOT NULL DEFAULT '',
                product_title TEXT NOT NULL DEFAULT '',
                manual_category TEXT NOT NULL DEFAULT '',
                suggested_action TEXT NOT NULL DEFAULT '',
                batch_priority_score REAL,
                market_match_score REAL,
                market_match_status TEXT NOT NULL DEFAULT '',
                store_fit_score REAL,
                content_potential_score REAL,
                core_score_a REAL,
                route_a TEXT NOT NULL DEFAULT '',
                core_score_b REAL,
                route_b TEXT NOT NULL DEFAULT '',
                supply_check_status TEXT NOT NULL DEFAULT '',
                brief_reason TEXT NOT NULL DEFAULT '',
                needs_manual_review INTEGER NOT NULL DEFAULT 0,
                manual_review_reason TEXT NOT NULL DEFAULT '',
                observation_tags_json TEXT NOT NULL DEFAULT '[]',
                v2_total_score REAL,
                v2_final_action TEXT NOT NULL DEFAULT '',
                v2_matched_direction TEXT NOT NULL DEFAULT '',
                v2_risk_tags_json TEXT NOT NULL DEFAULT '[]',
                v2_result_json TEXT NOT NULL DEFAULT '{}',
                analysis_error TEXT NOT NULL DEFAULT '',
                task_json TEXT NOT NULL DEFAULT '{}',
                feature_result_json TEXT NOT NULL DEFAULT '{}',
                scored_result_json TEXT NOT NULL DEFAULT '{}',
                updated_at_epoch INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (source_table_id, source_record_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candidate_analysis_run_results (
                run_id TEXT NOT NULL,
                source_table_id TEXT NOT NULL DEFAULT '',
                source_table_name TEXT NOT NULL DEFAULT '',
                source_record_id TEXT NOT NULL,
                analysis_status TEXT NOT NULL DEFAULT '',
                visible_status TEXT NOT NULL DEFAULT '',
                recognized_category TEXT NOT NULL DEFAULT '',
                category_confidence TEXT NOT NULL DEFAULT '',
                source_type TEXT NOT NULL DEFAULT '',
                batch_id TEXT NOT NULL DEFAULT '',
                target_market TEXT NOT NULL DEFAULT '',
                product_title TEXT NOT NULL DEFAULT '',
                manual_category TEXT NOT NULL DEFAULT '',
                suggested_action TEXT NOT NULL DEFAULT '',
                batch_priority_score REAL,
                market_match_score REAL,
                market_match_status TEXT NOT NULL DEFAULT '',
                store_fit_score REAL,
                content_potential_score REAL,
                core_score_a REAL,
                route_a TEXT NOT NULL DEFAULT '',
                core_score_b REAL,
                route_b TEXT NOT NULL DEFAULT '',
                supply_check_status TEXT NOT NULL DEFAULT '',
                brief_reason TEXT NOT NULL DEFAULT '',
                needs_manual_review INTEGER NOT NULL DEFAULT 0,
                manual_review_reason TEXT NOT NULL DEFAULT '',
                observation_tags_json TEXT NOT NULL DEFAULT '[]',
                v2_total_score REAL,
                v2_final_action TEXT NOT NULL DEFAULT '',
                v2_matched_direction TEXT NOT NULL DEFAULT '',
                v2_risk_tags_json TEXT NOT NULL DEFAULT '[]',
                v2_result_json TEXT NOT NULL DEFAULT '{}',
                analysis_error TEXT NOT NULL DEFAULT '',
                task_json TEXT NOT NULL DEFAULT '{}',
                feature_result_json TEXT NOT NULL DEFAULT '{}',
                scored_result_json TEXT NOT NULL DEFAULT '{}',
                updated_at_epoch INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (run_id, source_table_id, source_record_id)
            )
            """
        )
        self._ensure_column(conn, "candidate_analysis_results", "source_table_name", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "visible_status", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "recognized_category", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "category_confidence", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "source_type", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "batch_id", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "target_market", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "product_title", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "manual_category", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "suggested_action", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "batch_priority_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_results", "market_match_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_results", "core_score_a", "REAL")
        self._ensure_column(conn, "candidate_analysis_results", "route_a", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "core_score_b", "REAL")
        self._ensure_column(conn, "candidate_analysis_results", "route_b", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "market_match_status", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "store_fit_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_results", "content_potential_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_results", "supply_check_status", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "brief_reason", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "needs_manual_review", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_results", "manual_review_reason", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "observation_tags_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "candidate_analysis_results", "v2_total_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_results", "v2_final_action", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "v2_matched_direction", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "v2_risk_tags_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "candidate_analysis_results", "v2_result_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_results", "analysis_error", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_results", "task_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_results", "feature_result_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_results", "scored_result_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_results", "updated_at_epoch", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_run_results", "source_table_name", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "visible_status", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "recognized_category", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "category_confidence", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "source_type", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "batch_id", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "target_market", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "product_title", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "manual_category", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "suggested_action", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "batch_priority_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_run_results", "market_match_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_run_results", "market_match_status", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "store_fit_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_run_results", "content_potential_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_run_results", "core_score_a", "REAL")
        self._ensure_column(conn, "candidate_analysis_run_results", "route_a", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "core_score_b", "REAL")
        self._ensure_column(conn, "candidate_analysis_run_results", "route_b", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "supply_check_status", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "brief_reason", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "needs_manual_review", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_run_results", "manual_review_reason", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "observation_tags_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "candidate_analysis_run_results", "v2_total_score", "REAL")
        self._ensure_column(conn, "candidate_analysis_run_results", "v2_final_action", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "v2_matched_direction", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "v2_risk_tags_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "candidate_analysis_run_results", "v2_result_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_run_results", "analysis_error", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_run_results", "task_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_run_results", "feature_result_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_run_results", "scored_result_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_run_results", "updated_at_epoch", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_runs", "source_table_name", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_runs", "record_scope", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_runs", "only_risk_tag", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_runs", "max_workers", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column(conn, "candidate_analysis_runs", "flush_every", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_runs", "run_status", "TEXT NOT NULL DEFAULT 'running'")
        self._ensure_column(conn, "candidate_analysis_runs", "finished_at_epoch", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_runs", "processed_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_runs", "completed_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_runs", "failed_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_runs", "alert_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "candidate_analysis_runs", "alerts_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "candidate_analysis_runs", "summary_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "candidate_analysis_runs", "ab_diff_report_path", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_runs", "recent_diff_report_path", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "candidate_analysis_runs", "v2_shadow_report_path", "TEXT NOT NULL DEFAULT ''")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS human_override_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL DEFAULT '',
                batch_id TEXT NOT NULL DEFAULT '',
                market_report_version TEXT NOT NULL DEFAULT '',
                direction_dictionary_version TEXT NOT NULL DEFAULT '',
                matched_direction TEXT NOT NULL DEFAULT '',
                v2_score REAL,
                v2_action TEXT NOT NULL DEFAULT '',
                core_score_a REAL,
                core_action TEXT NOT NULL DEFAULT '',
                human_action TEXT NOT NULL DEFAULT '',
                override_reason TEXT NOT NULL DEFAULT '',
                operator TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_candidate_analysis_results_updated ON candidate_analysis_results(updated_at_epoch)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_candidate_analysis_run_results_run ON candidate_analysis_run_results(run_id, updated_at_epoch)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_candidate_analysis_runs_table ON candidate_analysis_runs(source_table_id, started_at_epoch)"
        )

    def _ensure_column(self, conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
        columns = {str(row[1] or "") for row in conn.execute("PRAGMA table_info({table})".format(table=table_name)).fetchall()}
        if column_name not in columns:
            conn.execute(
                "ALTER TABLE {table} ADD COLUMN {column} {column_sql}".format(
                    table=table_name,
                    column=column_name,
                    column_sql=column_sql,
                )
            )

    def _serialize(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {}
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, dict):
            return dict(value)
        return {"value": value}

    def _nullable_float(self, value: Optional[float]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)

    def _route_rank(self, route: str) -> int:
        mapping = {
            "reject": 0,
            "pending_review": 1,
            "reserve": 2,
            "small_test": 3,
            "priority_test": 4,
        }
        return mapping.get(str(route or ""), 0)
