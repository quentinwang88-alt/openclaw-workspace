#!/usr/bin/env python3
"""SQLite store for decoupled Market Insight and Selection agents."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from selection.market_task_fit import build_fallback_direction_execution_brief
from src.standardized_snapshot import StandardizedProductSnapshot


MARKET_AGENT = "market_insight"
SELECTION_AGENT = "selection"

SAFE_FALLBACK_CONFIG = {
    "fallback_mapping": {
        "prioritize_low_cost_test": {"task_type": "low_cost_test", "target_pool": "test_product_pool"},
        "cautious_test": {"task_type": "low_cost_test", "target_pool": "test_product_pool"},
        "study_top_not_enter": {"task_type": "head_dissection", "target_pool": "head_reference_pool"},
        "strong_signal_verify": {"task_type": "signal_verify", "target_pool": "manual_review_pool"},
        "hidden_candidate": {"task_type": "hidden_candidate", "target_pool": "observe_pool"},
        "hidden_small_test": {"task_type": "low_cost_test", "target_pool": "manual_review_pool"},
        "observe": {"task_type": "observe", "target_pool": "observe_pool"},
        "avoid": {"task_type": "avoid", "target_pool": "eliminate"},
        "neutral": {"task_type": "observe", "target_pool": "observe_pool"},
    }
}


class AgentDataStore(object):
    """Persistence boundary between standardized snapshots and business agents."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def import_snapshots(
        self,
        agent_name: str,
        snapshots: Iterable[StandardizedProductSnapshot],
        feishu_table_id: str = "",
        skipped_count: int = 0,
    ) -> Dict[str, Any]:
        snapshot_list = list(snapshots)
        if not snapshot_list:
            return {
                "agent_name": agent_name,
                "source_row_count": int(skipped_count or 0),
                "imported_count": 0,
                "skipped_count": int(skipped_count or 0),
                "status": "empty",
            }
        crawl_batch_id = snapshot_list[0].crawl_batch_id
        market_id = snapshot_list[0].market_id
        category_id = snapshot_list[0].category_id
        now = int(time.time())
        table_name = _raw_table_for_agent(agent_name)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            for snapshot in snapshot_list:
                self._upsert_raw_snapshot(conn, table_name, snapshot, now)
            self._upsert_import_log(
                conn=conn,
                agent_name=agent_name,
                crawl_batch_id=crawl_batch_id,
                market_id=market_id,
                category_id=category_id,
                feishu_table_id=feishu_table_id,
                source_row_count=len(snapshot_list) + int(skipped_count or 0),
                imported_count=len(snapshot_list),
                skipped_count=int(skipped_count or 0),
                status="completed",
                error_message="",
                now=now,
            )
        return {
            "agent_name": agent_name,
            "crawl_batch_id": crawl_batch_id,
            "market_id": market_id,
            "category_id": category_id,
            "feishu_table_id": feishu_table_id,
            "source_row_count": len(snapshot_list) + int(skipped_count or 0),
            "imported_count": len(snapshot_list),
            "skipped_count": int(skipped_count or 0),
            "status": "completed",
        }

    def save_market_direction_report(
        self,
        report_id: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
        report_version: str,
        report_date: str,
        sample_count: int,
        valid_sample_count: int,
        direction_count: int,
        report_status: str,
        business_summary_markdown: str,
        full_report_markdown: str,
        structured_json: Dict[str, Any],
    ) -> None:
        now = int(time.time())
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO market_direction_report (
                    report_id, crawl_batch_id, market_id, category_id, report_version,
                    report_date, sample_count, valid_sample_count, direction_count,
                    report_status, business_summary_markdown, full_report_markdown,
                    structured_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(report_id) DO UPDATE SET
                    report_version=excluded.report_version,
                    report_date=excluded.report_date,
                    sample_count=excluded.sample_count,
                    valid_sample_count=excluded.valid_sample_count,
                    direction_count=excluded.direction_count,
                    report_status=excluded.report_status,
                    business_summary_markdown=excluded.business_summary_markdown,
                    full_report_markdown=excluded.full_report_markdown,
                    structured_json=excluded.structured_json,
                    updated_at=excluded.updated_at
                """,
                (
                    report_id,
                    crawl_batch_id,
                    market_id,
                    category_id,
                    report_version,
                    report_date,
                    int(sample_count or 0),
                    int(valid_sample_count or 0),
                    int(direction_count or 0),
                    report_status,
                    business_summary_markdown,
                    full_report_markdown,
                    json.dumps(structured_json or {}, ensure_ascii=False),
                    now,
                    now,
                ),
            )

    def save_direction_execution_briefs(
        self,
        report_id: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
        briefs: Iterable[Dict[str, Any]],
    ) -> int:
        now = int(time.time())
        count = 0
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            for brief in briefs:
                brief_id = _brief_id(report_id, brief)
                conn.execute(
                    """
                    INSERT INTO direction_execution_brief (
                        brief_id, report_id, crawl_batch_id, market_id, category_id,
                        direction_id, direction_name, direction_action, task_type,
                        target_pool, brief_source, brief_confidence,
                        product_selection_requirements_json, positive_signals_json,
                        negative_signals_json, sample_pool_requirements_json,
                        content_requirements_json, upgrade_condition_json,
                        stop_condition_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(brief_id) DO UPDATE SET
                        direction_action=excluded.direction_action,
                        task_type=excluded.task_type,
                        target_pool=excluded.target_pool,
                        brief_source=excluded.brief_source,
                        brief_confidence=excluded.brief_confidence,
                        product_selection_requirements_json=excluded.product_selection_requirements_json,
                        positive_signals_json=excluded.positive_signals_json,
                        negative_signals_json=excluded.negative_signals_json,
                        sample_pool_requirements_json=excluded.sample_pool_requirements_json,
                        content_requirements_json=excluded.content_requirements_json,
                        upgrade_condition_json=excluded.upgrade_condition_json,
                        stop_condition_json=excluded.stop_condition_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        brief_id,
                        report_id,
                        crawl_batch_id,
                        market_id,
                        category_id,
                        str(brief.get("direction_id") or ""),
                        str(brief.get("direction_name") or ""),
                        str(brief.get("direction_action") or ""),
                        str(brief.get("task_type") or ""),
                        str(brief.get("target_pool") or ""),
                        str(brief.get("brief_source") or "generated"),
                        str(brief.get("brief_confidence") or "medium"),
                        _json(brief.get("product_selection_requirements") or []),
                        _json(brief.get("positive_signals") or []),
                        _json(brief.get("negative_signals") or []),
                        _json(brief.get("sample_pool_requirements") or []),
                        _json(brief.get("content_requirements") or []),
                        _json(brief.get("upgrade_condition") or []),
                        _json(brief.get("stop_condition") or []),
                        now,
                        now,
                    ),
                )
                count += 1
        return count

    def save_market_direction_snapshots(
        self,
        report_id: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
        direction_rows: Iterable[Dict[str, Any]],
    ) -> int:
        now = int(time.time())
        count = 0
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            conn.execute("DELETE FROM market_direction_snapshot WHERE report_id = ?", (report_id,))
            for row in direction_rows:
                conn.execute(
                    """
                    INSERT INTO market_direction_snapshot (
                        report_id, crawl_batch_id, market_id, category_id, direction_id,
                        direction_name, direction_group, sample_count, direction_action,
                        task_type, target_pool, competition_type, competition_structure_json,
                        new_product_signal, old_product_dominance, business_priority, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report_id,
                        crawl_batch_id,
                        market_id,
                        category_id,
                        str(row.get("direction_id") or ""),
                        str(row.get("direction_name") or ""),
                        str(row.get("direction_group") or row.get("direction_family") or ""),
                        int(row.get("sample_count") or row.get("direction_item_count") or 0),
                        str(row.get("direction_action") or row.get("primary_action") or ""),
                        str(row.get("task_type") or ""),
                        str(row.get("target_pool") or ""),
                        str(row.get("competition_type") or ""),
                        _json(row.get("competition_structure") or {}),
                        str(row.get("new_product_signal") or row.get("raw_new_product_signal") or ""),
                        str(row.get("old_product_dominance") or ""),
                        str(row.get("business_priority") or row.get("priority") or ""),
                        now,
                    ),
                )
                count += 1
        return count

    def save_direction_sample_pool(
        self,
        report_id: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
        rows: Iterable[Dict[str, Any]],
    ) -> int:
        now = int(time.time())
        count = 0
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            conn.execute("DELETE FROM direction_sample_pool WHERE report_id = ?", (report_id,))
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO direction_sample_pool (
                        report_id, crawl_batch_id, market_id, category_id, direction_id,
                        product_id, product_snapshot_id, sample_type, sample_rank, title,
                        main_image_url, price_rmb, sales_7d, product_age_days, fastmoss_url,
                        sample_reason, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report_id,
                        crawl_batch_id,
                        market_id,
                        category_id,
                        str(row.get("direction_id") or ""),
                        str(row.get("product_id") or ""),
                        str(row.get("product_snapshot_id") or row.get("product_id") or ""),
                        str(row.get("sample_type") or ""),
                        int(row.get("sample_rank") or 0),
                        str(row.get("title") or row.get("product_title") or ""),
                        str(row.get("main_image_url") or row.get("image_url") or ""),
                        _float(row.get("price_rmb")),
                        _float(row.get("sales_7d")),
                        _int(row.get("product_age_days")),
                        str(row.get("fastmoss_url") or ""),
                        str(row.get("sample_reason") or row.get("reason_for_selection") or ""),
                        now,
                    ),
                )
                count += 1
        return count

    def load_latest_direction_execution_brief(
        self,
        market_id: str,
        category_id: str,
        direction_id: str,
        crawl_batch_id: str = "",
        direction_action: str = "observe",
        direction_name: str = "",
    ) -> Dict[str, Any]:
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            row = None
            if crawl_batch_id:
                row = conn.execute(
                    """
                    SELECT * FROM direction_execution_brief
                    WHERE market_id = ? AND category_id = ? AND direction_id = ? AND crawl_batch_id = ?
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (market_id, category_id, direction_id, crawl_batch_id),
                ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT * FROM direction_execution_brief
                    WHERE market_id = ? AND category_id = ? AND direction_id = ?
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (market_id, category_id, direction_id),
                ).fetchone()
        if row is None:
            return build_fallback_direction_execution_brief(
                direction_action=direction_action,
                direction_name=direction_name,
                direction_id=direction_id,
                config=SAFE_FALLBACK_CONFIG,
            )
        brief = self._brief_from_row(row)
        if crawl_batch_id and brief.get("crawl_batch_id") != crawl_batch_id:
            brief.setdefault("risk_flags", []).append("brief_from_previous_batch")
        return brief

    def save_product_selection_result(self, result: Dict[str, Any], crawl_batch_id: str, product_snapshot_id: str) -> None:
        now = int(time.time())
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            score_id = "{batch}__{snapshot}".format(batch=crawl_batch_id, snapshot=product_snapshot_id)
            conn.execute(
                """
                INSERT INTO product_selection_score (
                    score_id, crawl_batch_id, product_snapshot_id, product_id, market_id,
                    category_id, direction_id, direction_name, matched_direction_confidence,
                    market_report_id, direction_brief_id, direction_match_score,
                    market_task_fit_score, product_quality_score, content_potential_score,
                    differentiation_score, total_score, core_score_a, v2_score,
                    final_task_pool, final_action, risk_flags_json, score_detail_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(score_id) DO UPDATE SET
                    direction_id=excluded.direction_id,
                    direction_name=excluded.direction_name,
                    direction_match_score=excluded.direction_match_score,
                    market_task_fit_score=excluded.market_task_fit_score,
                    product_quality_score=excluded.product_quality_score,
                    content_potential_score=excluded.content_potential_score,
                    differentiation_score=excluded.differentiation_score,
                    total_score=excluded.total_score,
                    v2_score=excluded.v2_score,
                    final_task_pool=excluded.final_task_pool,
                    final_action=excluded.final_action,
                    risk_flags_json=excluded.risk_flags_json,
                    score_detail_json=excluded.score_detail_json,
                    updated_at=excluded.updated_at
                """,
                (
                    score_id,
                    crawl_batch_id,
                    product_snapshot_id,
                    str(result.get("product_id") or ""),
                    str(result.get("market_id") or ""),
                    str(result.get("category_id") or ""),
                    str(result.get("direction_id") or ""),
                    str(result.get("direction_name") or ""),
                    str((result.get("direction_match") or {}).get("match_level") or ""),
                    "",
                    "",
                    float((result.get("weighted_scores") or {}).get("direction_match") or 0.0),
                    float((result.get("weighted_scores") or {}).get("market_task_fit") or 0.0),
                    float((result.get("weighted_scores") or {}).get("product_quality") or 0.0),
                    float((result.get("weighted_scores") or {}).get("content_potential") or 0.0),
                    float((result.get("weighted_scores") or {}).get("differentiation") or 0.0),
                    float(result.get("total_score") or 0.0),
                    float(result.get("core_score_a") or 0.0),
                    float(result.get("total_score") or 0.0),
                    str(result.get("target_pool") or ""),
                    str(result.get("final_action") or ""),
                    _json(result.get("risk_flags") or []),
                    _json(result),
                    now,
                    now,
                ),
            )
            self._save_task_pool_result(conn, result, crawl_batch_id, product_snapshot_id, now)

    def save_human_override(self, payload: Dict[str, Any]) -> None:
        now = int(time.time())
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO human_override_log (
                    product_id, product_snapshot_id, crawl_batch_id, market_id, category_id,
                    direction_id, v2_score, v2_action, core_score_a, core_action,
                    human_action, override_reason, operator, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload.get("product_id") or ""),
                    str(payload.get("product_snapshot_id") or ""),
                    str(payload.get("crawl_batch_id") or ""),
                    str(payload.get("market_id") or ""),
                    str(payload.get("category_id") or ""),
                    str(payload.get("direction_id") or ""),
                    _float(payload.get("v2_score")) or 0.0,
                    str(payload.get("v2_action") or ""),
                    _float(payload.get("core_score_a")) or 0.0,
                    str(payload.get("core_action") or ""),
                    str(payload.get("human_action") or ""),
                    str(payload.get("override_reason") or ""),
                    str(payload.get("operator") or ""),
                    now,
                    now,
                ),
            )

    def record_agent_run(
        self,
        agent_name: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
        status: str,
        source_row_count: int = 0,
        imported_count: int = 0,
        processed_count: int = 0,
        skipped_count: int = 0,
        error_message: str = "",
        market_report_id: str = "",
        brief_version: str = "",
        batch_data_hash: str = "",
        rerun_count: int = 0,
        rerun_reason: str = "",
        run_id: str = "",
    ) -> str:
        """Persist weekly orchestration state for Market/Selection agents."""
        now = int(time.time())
        table_name = "market_agent_run_log" if agent_name == MARKET_AGENT else "selection_run_log"
        run_id = run_id or "{agent}__{batch}__{market}__{category}".format(
            agent=agent_name,
            batch=crawl_batch_id,
            market=market_id,
            category=category_id,
        )
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO {table} (
                    run_id, crawl_batch_id, market_id, category_id, source_row_count,
                    imported_count, processed_count, skipped_count, status, started_at,
                    finished_at, error_message, market_report_id, brief_version,
                    batch_data_hash, rerun_count, rerun_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    source_row_count=excluded.source_row_count,
                    imported_count=excluded.imported_count,
                    processed_count=excluded.processed_count,
                    skipped_count=excluded.skipped_count,
                    status=excluded.status,
                    finished_at=excluded.finished_at,
                    error_message=excluded.error_message,
                    market_report_id=excluded.market_report_id,
                    brief_version=excluded.brief_version,
                    batch_data_hash=excluded.batch_data_hash,
                    rerun_count=excluded.rerun_count,
                    rerun_reason=excluded.rerun_reason
                """.format(table=table_name),
                (
                    run_id,
                    crawl_batch_id,
                    market_id,
                    category_id,
                    int(source_row_count or 0),
                    int(imported_count or 0),
                    int(processed_count or 0),
                    int(skipped_count or 0),
                    status,
                    now,
                    now,
                    error_message,
                    market_report_id,
                    brief_version,
                    batch_data_hash,
                    int(rerun_count or 0),
                    rerun_reason,
                ),
            )
        return run_id

    def get_agent_run(
        self,
        agent_name: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
    ) -> Optional[Dict[str, Any]]:
        table_name = "market_agent_run_log" if agent_name == MARKET_AGENT else "selection_run_log"
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            row = conn.execute(
                """
                SELECT * FROM {table}
                WHERE crawl_batch_id = ? AND market_id = ? AND category_id = ?
                ORDER BY finished_at DESC, started_at DESC LIMIT 1
                """.format(table=table_name),
                (crawl_batch_id, market_id, category_id),
            ).fetchone()
            if row is None:
                return None
            columns = [item[1] for item in conn.execute("PRAGMA table_info({table})".format(table=table_name)).fetchall()]
        return dict(zip(columns, row))

    def acquire_run_lock(
        self,
        agent_name: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
        ttl_seconds: int = 7200,
    ) -> Dict[str, Any]:
        """Acquire a coarse run lock, expiring stale locks after ttl_seconds."""
        now = int(time.time())
        lock_key = "{agent}__{batch}__{market}__{category}".format(
            agent=agent_name,
            batch=crawl_batch_id,
            market=market_id,
            category=category_id,
        )
        expires_at = now + int(ttl_seconds or 7200)
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            current = conn.execute(
                "SELECT status, expires_at FROM agent_run_lock WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
            if current and current[0] == "running" and int(current[1] or 0) > now:
                return {"acquired": False, "lock_key": lock_key, "reason": "lock_already_running"}
            conn.execute(
                """
                INSERT INTO agent_run_lock (
                    lock_key, agent_name, crawl_batch_id, market_id, category_id,
                    status, acquired_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lock_key) DO UPDATE SET
                    status=excluded.status,
                    acquired_at=excluded.acquired_at,
                    expires_at=excluded.expires_at
                """,
                (lock_key, agent_name, crawl_batch_id, market_id, category_id, "running", now, expires_at),
            )
        return {"acquired": True, "lock_key": lock_key, "expires_at": expires_at}

    def release_run_lock(
        self,
        lock_key: str,
        status: str = "released",
    ) -> None:
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            conn.execute(
                "UPDATE agent_run_lock SET status = ?, expires_at = ? WHERE lock_key = ?",
                (status, int(time.time()), lock_key),
            )

    def latest_consumable_market_report(
        self,
        market_id: str,
        category_id: str,
        crawl_batch_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            row = None
            if crawl_batch_id:
                row = conn.execute(
                    """
                    SELECT report_id, crawl_batch_id, market_id, category_id, report_status,
                           report_version, report_date, updated_at
                    FROM market_direction_report
                    WHERE market_id = ? AND category_id = ? AND crawl_batch_id = ?
                      AND report_status IN ('consumable', 'ready', '可消费')
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (market_id, category_id, crawl_batch_id),
                ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT report_id, crawl_batch_id, market_id, category_id, report_status,
                           report_version, report_date, updated_at
                    FROM market_direction_report
                    WHERE market_id = ? AND category_id = ?
                      AND report_status IN ('consumable', 'ready', '可消费')
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (market_id, category_id),
                ).fetchone()
            if row is None:
                return None
        columns = [
            "report_id", "crawl_batch_id", "market_id", "category_id",
            "report_status", "report_version", "report_date", "updated_at",
        ]
        return dict(zip(columns, row))

    def count_ready_briefs(
        self,
        market_id: str,
        category_id: str,
        crawl_batch_id: str,
    ) -> int:
        with sqlite3.connect(str(self.db_path), timeout=30) as conn:
            self.ensure_schema(conn)
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM direction_execution_brief
                WHERE market_id = ? AND category_id = ? AND crawl_batch_id = ?
                  AND task_type != '' AND target_pool != ''
                """,
                (market_id, category_id, crawl_batch_id),
            ).fetchone()
        return int(row[0] or 0)

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_import_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT NOT NULL,
                crawl_batch_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                feishu_table_id TEXT NOT NULL DEFAULT '',
                source_row_count INTEGER NOT NULL DEFAULT 0,
                imported_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT '',
                error_message TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0,
                UNIQUE(agent_name, crawl_batch_id, market_id, category_id, feishu_table_id)
            )
            """
        )
        self._ensure_raw_table(conn, "market_raw_product_snapshot", include_content=True)
        self._ensure_raw_table(conn, "selection_raw_product_snapshot", include_content=False)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_direction_report (
                report_id TEXT PRIMARY KEY,
                crawl_batch_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                report_version TEXT NOT NULL DEFAULT '',
                report_date TEXT NOT NULL DEFAULT '',
                sample_count INTEGER NOT NULL DEFAULT 0,
                valid_sample_count INTEGER NOT NULL DEFAULT 0,
                direction_count INTEGER NOT NULL DEFAULT 0,
                report_status TEXT NOT NULL DEFAULT '',
                business_summary_markdown TEXT NOT NULL DEFAULT '',
                full_report_markdown TEXT NOT NULL DEFAULT '',
                structured_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_direction_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT NOT NULL,
                crawl_batch_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                direction_id TEXT NOT NULL,
                direction_name TEXT NOT NULL DEFAULT '',
                direction_group TEXT NOT NULL DEFAULT '',
                sample_count INTEGER NOT NULL DEFAULT 0,
                direction_action TEXT NOT NULL DEFAULT '',
                task_type TEXT NOT NULL DEFAULT '',
                target_pool TEXT NOT NULL DEFAULT '',
                competition_type TEXT NOT NULL DEFAULT '',
                competition_structure_json TEXT NOT NULL DEFAULT '{}',
                new_product_signal TEXT NOT NULL DEFAULT '',
                old_product_dominance TEXT NOT NULL DEFAULT '',
                business_priority TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS direction_execution_brief (
                brief_id TEXT PRIMARY KEY,
                report_id TEXT NOT NULL,
                crawl_batch_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                direction_id TEXT NOT NULL,
                direction_name TEXT NOT NULL DEFAULT '',
                direction_action TEXT NOT NULL DEFAULT '',
                task_type TEXT NOT NULL DEFAULT '',
                target_pool TEXT NOT NULL DEFAULT '',
                brief_source TEXT NOT NULL DEFAULT '',
                brief_confidence TEXT NOT NULL DEFAULT '',
                product_selection_requirements_json TEXT NOT NULL DEFAULT '[]',
                positive_signals_json TEXT NOT NULL DEFAULT '[]',
                negative_signals_json TEXT NOT NULL DEFAULT '[]',
                sample_pool_requirements_json TEXT NOT NULL DEFAULT '[]',
                content_requirements_json TEXT NOT NULL DEFAULT '[]',
                upgrade_condition_json TEXT NOT NULL DEFAULT '[]',
                stop_condition_json TEXT NOT NULL DEFAULT '[]',
                created_at INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS direction_sample_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT NOT NULL,
                crawl_batch_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                direction_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                product_snapshot_id TEXT NOT NULL,
                sample_type TEXT NOT NULL,
                sample_rank INTEGER NOT NULL DEFAULT 0,
                title TEXT NOT NULL DEFAULT '',
                main_image_url TEXT NOT NULL DEFAULT '',
                price_rmb REAL,
                sales_7d REAL,
                product_age_days INTEGER,
                fastmoss_url TEXT NOT NULL DEFAULT '',
                sample_reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_selection_score (
                score_id TEXT PRIMARY KEY,
                crawl_batch_id TEXT NOT NULL,
                product_snapshot_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                direction_id TEXT NOT NULL DEFAULT '',
                direction_name TEXT NOT NULL DEFAULT '',
                matched_direction_confidence TEXT NOT NULL DEFAULT '',
                market_report_id TEXT NOT NULL DEFAULT '',
                direction_brief_id TEXT NOT NULL DEFAULT '',
                direction_match_score REAL NOT NULL DEFAULT 0.0,
                market_task_fit_score REAL NOT NULL DEFAULT 0.0,
                product_quality_score REAL NOT NULL DEFAULT 0.0,
                content_potential_score REAL NOT NULL DEFAULT 0.0,
                differentiation_score REAL NOT NULL DEFAULT 0.0,
                total_score REAL NOT NULL DEFAULT 0.0,
                core_score_a REAL NOT NULL DEFAULT 0.0,
                v2_score REAL NOT NULL DEFAULT 0.0,
                final_task_pool TEXT NOT NULL DEFAULT '',
                final_action TEXT NOT NULL DEFAULT '',
                risk_flags_json TEXT NOT NULL DEFAULT '[]',
                score_detail_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_task_pool_result (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_batch_id TEXT NOT NULL,
                product_snapshot_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                direction_id TEXT NOT NULL DEFAULT '',
                direction_name TEXT NOT NULL DEFAULT '',
                task_type TEXT NOT NULL DEFAULT '',
                target_pool TEXT NOT NULL DEFAULT '',
                pool_family TEXT NOT NULL DEFAULT '',
                dissection_subtype TEXT NOT NULL DEFAULT '',
                task_fit_level TEXT NOT NULL DEFAULT '',
                task_fit_reason TEXT NOT NULL DEFAULT '',
                lifecycle_status TEXT NOT NULL DEFAULT 'pending',
                final_action TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0,
                UNIQUE(crawl_batch_id, product_snapshot_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS human_override_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL DEFAULT '',
                product_snapshot_id TEXT NOT NULL DEFAULT '',
                crawl_batch_id TEXT NOT NULL DEFAULT '',
                market_id TEXT NOT NULL DEFAULT '',
                category_id TEXT NOT NULL DEFAULT '',
                direction_id TEXT NOT NULL DEFAULT '',
                v2_score REAL NOT NULL DEFAULT 0.0,
                v2_action TEXT NOT NULL DEFAULT '',
                core_score_a REAL NOT NULL DEFAULT 0.0,
                core_action TEXT NOT NULL DEFAULT '',
                human_action TEXT NOT NULL DEFAULT '',
                override_reason TEXT NOT NULL DEFAULT '',
                operator TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._ensure_agent_run_log(conn, "market_agent_run_log")
        self._ensure_agent_run_log(conn, "selection_run_log")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_run_lock (
                lock_key TEXT PRIMARY KEY,
                agent_name TEXT NOT NULL,
                crawl_batch_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '',
                acquired_at INTEGER NOT NULL DEFAULT 0,
                expires_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )

    def _ensure_raw_table(self, conn: sqlite3.Connection, table_name: str, include_content: bool) -> None:
        content_columns = ", video_count REAL, creator_count REAL" if include_content else ""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_batch_id TEXT NOT NULL,
                product_snapshot_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                main_image_url TEXT NOT NULL DEFAULT '',
                price_rmb REAL,
                sales_7d REAL,
                sales_30d REAL{content_columns},
                listing_datetime TEXT NOT NULL DEFAULT '',
                product_age_days INTEGER,
                age_bucket TEXT NOT NULL DEFAULT '',
                fastmoss_url TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                data_quality_flags TEXT NOT NULL DEFAULT '[]',
                imported_at INTEGER NOT NULL DEFAULT 0,
                raw_snapshot_json TEXT NOT NULL DEFAULT '{{}}',
                UNIQUE(crawl_batch_id, product_snapshot_id)
            )
            """.format(table=table_name, content_columns=content_columns)
        )

    def _ensure_agent_run_log(self, conn: sqlite3.Connection, table_name: str) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS {table} (
                run_id TEXT PRIMARY KEY,
                crawl_batch_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                category_id TEXT NOT NULL,
                source_row_count INTEGER NOT NULL DEFAULT 0,
                imported_count INTEGER NOT NULL DEFAULT 0,
                processed_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT '',
                started_at INTEGER NOT NULL DEFAULT 0,
                finished_at INTEGER NOT NULL DEFAULT 0,
                error_message TEXT NOT NULL DEFAULT '',
                market_report_id TEXT NOT NULL DEFAULT '',
                brief_version TEXT NOT NULL DEFAULT '',
                batch_data_hash TEXT NOT NULL DEFAULT '',
                rerun_count INTEGER NOT NULL DEFAULT 0,
                rerun_reason TEXT NOT NULL DEFAULT ''
            )
            """.format(table=table_name)
        )

    def _upsert_raw_snapshot(self, conn: sqlite3.Connection, table_name: str, snapshot: StandardizedProductSnapshot, now: int) -> None:
        content_names = ", video_count, creator_count" if table_name == "market_raw_product_snapshot" else ""
        content_values = ", ?, ?" if table_name == "market_raw_product_snapshot" else ""
        payload = [
            snapshot.crawl_batch_id,
            snapshot.product_snapshot_id,
            snapshot.product_id,
            snapshot.market_id,
            snapshot.category_id,
            snapshot.title,
            snapshot.main_image_url,
            snapshot.price_rmb,
            snapshot.sales_7d,
            snapshot.sales_30d,
        ]
        if table_name == "market_raw_product_snapshot":
            payload.extend([snapshot.video_count, snapshot.creator_count])
        payload.extend([
            snapshot.listing_datetime,
            snapshot.product_age_days,
            snapshot.age_bucket,
            snapshot.fastmoss_url,
            snapshot.source,
            _json(snapshot.data_quality_flags),
            now,
            _json(snapshot.to_dict()),
        ])
        conn.execute(
            """
            INSERT INTO {table} (
                crawl_batch_id, product_snapshot_id, product_id, market_id, category_id,
                title, main_image_url, price_rmb, sales_7d, sales_30d{content_names},
                listing_datetime, product_age_days, age_bucket, fastmoss_url, source,
                data_quality_flags, imported_at, raw_snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?{content_values}, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(crawl_batch_id, product_snapshot_id) DO UPDATE SET
                product_id=excluded.product_id,
                market_id=excluded.market_id,
                category_id=excluded.category_id,
                title=excluded.title,
                main_image_url=excluded.main_image_url,
                price_rmb=excluded.price_rmb,
                sales_7d=excluded.sales_7d,
                sales_30d=excluded.sales_30d,
                listing_datetime=excluded.listing_datetime,
                product_age_days=excluded.product_age_days,
                age_bucket=excluded.age_bucket,
                fastmoss_url=excluded.fastmoss_url,
                source=excluded.source,
                data_quality_flags=excluded.data_quality_flags,
                imported_at=excluded.imported_at,
                raw_snapshot_json=excluded.raw_snapshot_json
            """.format(table=table_name, content_names=content_names, content_values=content_values),
            payload,
        )

    def _upsert_import_log(
        self,
        conn: sqlite3.Connection,
        agent_name: str,
        crawl_batch_id: str,
        market_id: str,
        category_id: str,
        feishu_table_id: str,
        source_row_count: int,
        imported_count: int,
        skipped_count: int,
        status: str,
        error_message: str,
        now: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO agent_import_log (
                agent_name, crawl_batch_id, market_id, category_id, feishu_table_id,
                source_row_count, imported_count, skipped_count, status, error_message,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(agent_name, crawl_batch_id, market_id, category_id, feishu_table_id) DO UPDATE SET
                source_row_count=excluded.source_row_count,
                imported_count=excluded.imported_count,
                skipped_count=excluded.skipped_count,
                status=excluded.status,
                error_message=excluded.error_message,
                updated_at=excluded.updated_at
            """,
            (
                agent_name,
                crawl_batch_id,
                market_id,
                category_id,
                feishu_table_id,
                int(source_row_count or 0),
                int(imported_count or 0),
                int(skipped_count or 0),
                status,
                error_message,
                now,
                now,
            ),
        )

    def _save_task_pool_result(self, conn: sqlite3.Connection, result: Dict[str, Any], crawl_batch_id: str, product_snapshot_id: str, now: int) -> None:
        market_task_fit = result.get("market_task_fit") or {}
        conn.execute(
            """
            INSERT INTO product_task_pool_result (
                crawl_batch_id, product_snapshot_id, product_id, market_id, category_id,
                direction_id, direction_name, task_type, target_pool, pool_family,
                dissection_subtype, task_fit_level, task_fit_reason, lifecycle_status,
                final_action, reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(crawl_batch_id, product_snapshot_id) DO UPDATE SET
                task_type=excluded.task_type,
                target_pool=excluded.target_pool,
                pool_family=excluded.pool_family,
                dissection_subtype=excluded.dissection_subtype,
                task_fit_level=excluded.task_fit_level,
                task_fit_reason=excluded.task_fit_reason,
                lifecycle_status=excluded.lifecycle_status,
                final_action=excluded.final_action,
                reason=excluded.reason,
                updated_at=excluded.updated_at
            """,
            (
                crawl_batch_id,
                product_snapshot_id,
                str(result.get("product_id") or ""),
                str(result.get("market_id") or ""),
                str(result.get("category_id") or ""),
                str(result.get("direction_id") or ""),
                str(result.get("direction_name") or ""),
                str(market_task_fit.get("task_type") or ""),
                str(result.get("target_pool") or ""),
                str(result.get("pool_family") or ""),
                str(result.get("dissection_subtype") or ""),
                str(market_task_fit.get("fit_level") or ""),
                str(market_task_fit.get("task_fit_reason") or ""),
                str(result.get("lifecycle_status") or "pending"),
                str(result.get("final_action") or ""),
                str(result.get("v2_brief_reason") or result.get("eliminate_reason") or ""),
                now,
                now,
            ),
        )

    def _brief_from_row(self, row: sqlite3.Row | tuple) -> Dict[str, Any]:
        columns = [
            "brief_id", "report_id", "crawl_batch_id", "market_id", "category_id",
            "direction_id", "direction_name", "direction_action", "task_type",
            "target_pool", "brief_source", "brief_confidence",
            "product_selection_requirements_json", "positive_signals_json",
            "negative_signals_json", "sample_pool_requirements_json",
            "content_requirements_json", "upgrade_condition_json",
            "stop_condition_json", "created_at", "updated_at",
        ]
        data = dict(zip(columns, row))
        return {
            "brief_id": data["brief_id"],
            "report_id": data["report_id"],
            "crawl_batch_id": data["crawl_batch_id"],
            "market_id": data["market_id"],
            "category_id": data["category_id"],
            "direction_id": data["direction_id"],
            "direction_name": data["direction_name"],
            "direction_action": data["direction_action"],
            "task_type": data["task_type"],
            "target_pool": data["target_pool"],
            "brief_source": data["brief_source"],
            "brief_confidence": data["brief_confidence"],
            "product_selection_requirements": _loads(data["product_selection_requirements_json"]),
            "positive_signals": _loads(data["positive_signals_json"]),
            "negative_signals": _loads(data["negative_signals_json"]),
            "sample_pool_requirements": _loads(data["sample_pool_requirements_json"]),
            "content_requirements": _loads(data["content_requirements_json"]),
            "upgrade_condition": _loads(data["upgrade_condition_json"]),
            "stop_condition": _loads(data["stop_condition_json"]),
            "risk_flags": [],
        }


def _raw_table_for_agent(agent_name: str) -> str:
    if agent_name == MARKET_AGENT:
        return "market_raw_product_snapshot"
    if agent_name == SELECTION_AGENT:
        return "selection_raw_product_snapshot"
    raise ValueError("未知 agent_name: {agent}".format(agent=agent_name))


def _brief_id(report_id: str, brief: Dict[str, Any]) -> str:
    direction_id = str(brief.get("direction_id") or brief.get("direction_name") or "").strip()
    return "{report}__{direction}".format(report=report_id, direction=direction_id)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _loads(value: Any) -> Any:
    try:
        return json.loads(str(value or "[]"))
    except ValueError:
        return []


def _float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    parsed = _float(value)
    return int(parsed) if parsed is not None else None
