"""V1 原创批次存储 — 两张表 + CRUD + PipelineStorage 接线"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.original_batch_models import (
    BatchRecord,
    PlanItem,
    BATCH_STATUSES,
    ITEM_STATUSES,
    generate_batch_id,
    generate_batch_item_id,
    build_allocation_signature,
    build_input_snapshot,
    build_data_snapshot_hash,
)




_BATCH_TABLE_NAMES = {
    "original_content_batch",
    "original_content_item",
}

POLICY_VERSION = "original-batch-allocation-v15-structure-pool-rotation"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)


# ── DDL ────────────────────────────────────────────────────────────────


_BATCH_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS original_content_batch (
    batch_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    workflow_type TEXT NOT NULL DEFAULT 'ORIGINAL_SCRIPT',
    product_code TEXT NOT NULL,
    source_record_id TEXT,
    target_country TEXT,
    target_language TEXT,
    top_category TEXT,
    product_type TEXT,
    requested_count INTEGER NOT NULL,
    planned_count INTEGER NOT NULL DEFAULT 0,
    ready_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    test_phase TEXT NOT NULL,
    duration_seconds REAL NOT NULL DEFAULT 15,
    execution_mode TEXT NOT NULL DEFAULT 'PLAN_ONLY',
    auto_video_enabled INTEGER NOT NULL DEFAULT 0,
    policy_version TEXT NOT NULL,
    random_seed INTEGER NOT NULL,
    data_snapshot_hash TEXT NOT NULL,
    input_snapshot_json TEXT NOT NULL,
    allocation_summary_json TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

_ITEM_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS original_content_item (
    batch_item_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    item_index INTEGER NOT NULL,
    item_role TEXT NOT NULL,
    product_code TEXT NOT NULL,
    selection_run_id TEXT NOT NULL,
    direction_assignment_id TEXT NOT NULL,
    compatibility_slot TEXT NOT NULL,
    cluster_id INTEGER,
    cluster_version TEXT,
    evidence_tier TEXT,
    macro_family_key TEXT,
    carrier_mode TEXT,
    structure_contract_json TEXT NOT NULL,
    execution_reference_json TEXT,
    content_bundle_id TEXT NOT NULL,
    content_bundle_json TEXT NOT NULL,
    content_angle_key TEXT NOT NULL,
    audience_tension_status TEXT NOT NULL,
    audience_tension_text TEXT,
    claim_keys_json TEXT NOT NULL,
    requested_hook_id TEXT,
    eligible_hook_ids_json TEXT,
    actual_hook_id TEXT,
    creative_contract_id TEXT,
    visual_signature TEXT,
    frozen_direction_package_json TEXT,
    stage_checkpoint_json TEXT,
    allocation_signature TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    item_snapshot_hash TEXT NOT NULL,
    consumer_run_id TEXT,
    script_id TEXT,
    content_id TEXT,
    video_prompt_id TEXT,
    structure_binding_id TEXT,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(batch_id, item_index),
    UNIQUE(batch_id, allocation_signature)
)
"""

_IDX_BATCH_PRODUCT = (
    "CREATE INDEX IF NOT EXISTS idx_batch_product ON original_content_batch(product_code, status)"
)
_IDX_ITEM_BATCH = (
    "CREATE INDEX IF NOT EXISTS idx_item_batch ON original_content_item(batch_id, item_index)"
)
_IDX_ITEM_STATUS = (
    "CREATE INDEX IF NOT EXISTS idx_item_status ON original_content_item(batch_id, status)"
)


# ── BatchStorage ───────────────────────────────────────────────────────


class BatchStorage:
    """Persist batch plans and items through the same SQLite/MySQL backends."""

    def __init__(self, db_path: Optional[Path] = None, database_url: str = ""):
        self.db_path = Path(db_path) if db_path else None
        self._database_url = database_url

    def _connect(self):
        import sqlite3
        from core.storage import default_db_path
        path = self.db_path or default_db_path()
        connection = sqlite3.connect(str(path), timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(_BATCH_TABLE_DDL)
            conn.execute(_ITEM_TABLE_DDL)
            columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(original_content_item)")
            }
            if "frozen_direction_package_json" not in columns:
                conn.execute(
                    "ALTER TABLE original_content_item "
                    "ADD COLUMN frozen_direction_package_json TEXT"
                )
            if "stage_checkpoint_json" not in columns:
                conn.execute(
                    "ALTER TABLE original_content_item "
                    "ADD COLUMN stage_checkpoint_json TEXT"
                )
            for statement in (_IDX_BATCH_PRODUCT, _IDX_ITEM_BATCH, _IDX_ITEM_STATUS):
                try:
                    conn.execute(statement)
                except Exception:
                    pass

    # ── Batch CRUD ─────────────────────────────────────────────────

    def create_batch(self, record: BatchRecord) -> str:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO original_content_batch (
                    batch_id, request_id, workflow_type, product_code,
                    source_record_id, target_country, target_language,
                    top_category, product_type, requested_count,
                    planned_count, ready_count, failed_count,
                    test_phase, duration_seconds, execution_mode,
                    auto_video_enabled, policy_version, random_seed,
                    data_snapshot_hash, input_snapshot_json,
                    allocation_summary_json, status, error_message,
                    created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    record.batch_id, record.request_id, "ORIGINAL_SCRIPT",
                    record.product_code,
                    record.source_record_id or None, record.target_country or None,
                    record.target_language or None, record.top_category or None,
                    record.product_type or None, record.requested_count,
                    record.planned_count, record.ready_count, record.failed_count,
                    record.test_phase, record.duration_seconds, record.execution_mode,
                    1 if record.auto_video_enabled else 0,
                    record.policy_version, record.random_seed,
                    record.data_snapshot_hash, record.input_snapshot_json,
                    record.allocation_summary_json or None, record.status,
                    record.error_message or None, _now(), _now(),
                ),
            )
        return record.batch_id

    def get_batch(self, batch_id: str) -> Optional[BatchRecord]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM original_content_batch WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
        if not row:
            return None
        return self._row_to_batch(row)

    def get_batch_by_request_id(self, request_id: str) -> Optional[BatchRecord]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM original_content_batch WHERE request_id=?",
                (request_id,),
            ).fetchone()
        if not row:
            return None
        return self._row_to_batch(row)

    def update_batch_status(
        self,
        batch_id: str,
        status: str,
        planned_count: Optional[int] = None,
        ready_count: Optional[int] = None,
        failed_count: Optional[int] = None,
        allocation_summary: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        sets: List[str] = ["status=?", "updated_at=?"]
        params: List[Any] = [status, _now()]
        if planned_count is not None:
            sets.append("planned_count=?")
            params.append(planned_count)
        if ready_count is not None:
            sets.append("ready_count=?")
            params.append(ready_count)
        if failed_count is not None:
            sets.append("failed_count=?")
            params.append(failed_count)
        if allocation_summary is not None:
            sets.append("allocation_summary_json=?")
            params.append(allocation_summary)
        if error_message is not None:
            sets.append("error_message=?")
            params.append(error_message)
        params.append(batch_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE original_content_batch SET {', '.join(sets)} WHERE batch_id=?",
                params,
            )

    def _row_to_batch(self, row: Any) -> BatchRecord:
        d = dict(row.items() if hasattr(row, "items") else zip(row.keys(), row))
        return BatchRecord(
            batch_id=d.get("batch_id", ""),
            request_id=d.get("request_id", ""),
            product_code=d.get("product_code", ""),
            requested_count=int(d.get("requested_count", 0)),
            test_phase=d.get("test_phase", "INITIAL"),
            execution_mode=d.get("execution_mode", "PLAN_ONLY"),
            policy_version=d.get("policy_version", ""),
            random_seed=int(d.get("random_seed", 0)),
            data_snapshot_hash=d.get("data_snapshot_hash", ""),
            input_snapshot_json=d.get("input_snapshot_json", ""),
            planned_count=int(d.get("planned_count", 0)),
            ready_count=int(d.get("ready_count", 0)),
            failed_count=int(d.get("failed_count", 0)),
            status=d.get("status", "REQUESTED"),
            error_message=d.get("error_message", ""),
            allocation_summary_json=d.get("allocation_summary_json", ""),
            duration_seconds=float(d.get("duration_seconds", 15)),
            auto_video_enabled=bool(int(d.get("auto_video_enabled", 0))),
            target_country=d.get("target_country", ""),
            target_language=d.get("target_language", ""),
            top_category=d.get("top_category", ""),
            product_type=d.get("product_type", ""),
            source_record_id=d.get("source_record_id", ""),
        )

    # ── Item CRUD ──────────────────────────────────────────────────

    def insert_item(self, item: PlanItem) -> str:
        cols = [
            "batch_item_id", "batch_id", "item_index", "item_role", "product_code",
            "selection_run_id", "direction_assignment_id", "compatibility_slot",
            "cluster_id", "cluster_version", "evidence_tier",
            "macro_family_key", "carrier_mode",
            "structure_contract_json", "execution_reference_json",
            "content_bundle_id", "content_bundle_json",
            "content_angle_key", "audience_tension_status", "audience_tension_text",
            "claim_keys_json", "requested_hook_id", "eligible_hook_ids_json",
            "actual_hook_id", "creative_contract_id", "visual_signature",
            "frozen_direction_package_json", "stage_checkpoint_json",
            "allocation_signature", "policy_version", "item_snapshot_hash",
            "consumer_run_id", "script_id", "content_id", "video_prompt_id",
            "structure_binding_id", "status", "attempt_count",
            "error_code", "error_message", "result_json",
            "created_at", "updated_at",
        ]
        vals = [
            item.batch_item_id, item.batch_id, item.item_index, item.item_role,
            item.product_code, item.selection_run_id, item.direction_assignment_id,
            item.compatibility_slot, item.cluster_id, item.cluster_version or None,
            item.evidence_tier or None, item.macro_family_key or None,
            item.carrier_mode or None, item.structure_contract_json,
            item.execution_reference_json or None, item.content_bundle_id,
            item.content_bundle_json, item.content_angle_key,
            item.audience_tension_status, item.audience_tension_text or None,
            item.claim_keys_json, item.requested_hook_id or None,
            item.eligible_hook_ids_json, item.actual_hook_id or None,
            item.creative_contract_id or None, item.visual_signature or None,
            item.frozen_direction_package_json or None,
            item.stage_checkpoint_json or None,
            item.allocation_signature, item.policy_version, item.item_snapshot_hash,
            item.consumer_run_id or None, item.script_id or None,
            item.content_id or None, item.video_prompt_id or None,
            item.structure_binding_id or None, item.status, item.attempt_count,
            item.error_code or None, item.error_message or None,
            item.result_json or None, _now(), _now(),
        ]
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT OR REPLACE INTO original_content_item ({','.join(cols)}) VALUES ({placeholders})"
        with self._connect() as conn:
            conn.execute(sql, vals)
        return item.batch_item_id

    def get_items(self, batch_id: str) -> List[PlanItem]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM original_content_item WHERE batch_id=? ORDER BY item_index",
                (batch_id,),
            ).fetchall()
        return [self._row_to_item(r) for r in rows]

    def get_item(self, batch_item_id: str) -> Optional[PlanItem]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM original_content_item WHERE batch_item_id=?",
                (batch_item_id,),
            ).fetchone()
        if not row:
            return None
        return self._row_to_item(row)

    def get_items_by_status(self, batch_id: str, status: str) -> List[PlanItem]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM original_content_item WHERE batch_id=? AND status=? ORDER BY item_index",
                (batch_id, status),
            ).fetchall()
        return [self._row_to_item(r) for r in rows]

    def get_recent_cluster_usage(
        self,
        product_code: str,
        *,
        limit_batches: int = 8,
    ) -> Dict[str, int]:
        """Return recent per-product structure usage for exploratory rotation.

        Both the versioned key and the cluster-only key are emitted so older
        rows without a cluster version remain useful.  This is planning input,
        not performance feedback.
        """
        with self._connect() as conn:
            batch_rows = conn.execute(
                "SELECT batch_id FROM original_content_batch "
                "WHERE product_code=? AND status NOT IN ('FAILED') "
                "ORDER BY created_at DESC LIMIT ?",
                (product_code, max(1, int(limit_batches))),
            ).fetchall()
            batch_ids = [str(row[0]) for row in batch_rows]
            if not batch_ids:
                return {}
            placeholders = ",".join(["?"] * len(batch_ids))
            rows = conn.execute(
                f"SELECT cluster_id, cluster_version, COUNT(*) AS use_count "
                f"FROM original_content_item WHERE batch_id IN ({placeholders}) "
                "AND cluster_id IS NOT NULL GROUP BY cluster_id, cluster_version",
                batch_ids,
            ).fetchall()
        usage: Dict[str, int] = {}
        for row in rows:
            cluster_id = str(row[0])
            version = str(row[1] or "")
            count = int(row[2] or 0)
            usage[cluster_id] = usage.get(cluster_id, 0) + count
            if version:
                usage[f"{cluster_id}:{version}"] = count
        return usage

    def update_item_status(
        self,
        batch_item_id: str,
        status: str,
        *,
        attempt_count: Optional[int] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        result_json: Optional[str] = None,
        actual_hook_id: Optional[str] = None,
        consumer_run_id: Optional[str] = None,
        script_id: Optional[str] = None,
        content_id: Optional[str] = None,
        video_prompt_id: Optional[str] = None,
        structure_binding_id: Optional[str] = None,
    ) -> None:
        sets: List[str] = ["status=?", "updated_at=?"]
        params: List[Any] = [status, _now()]
        field_map = {
            "attempt_count": attempt_count,
            "error_code": error_code,
            "error_message": error_message,
            "result_json": result_json,
            "actual_hook_id": actual_hook_id,
            "consumer_run_id": consumer_run_id,
            "script_id": script_id,
            "content_id": content_id,
            "video_prompt_id": video_prompt_id,
            "structure_binding_id": structure_binding_id,
        }
        for col, val in field_map.items():
            if val is not None:
                sets.append(f"{col}=?")
                params.append(val)
        params.append(batch_item_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE original_content_item SET {', '.join(sets)} WHERE batch_item_id=?",
                params,
            )

    def update_item_checkpoint(
        self,
        batch_item_id: str,
        checkpoint: Any,
    ) -> None:
        """Persist one item's stage checkpoint without changing item status."""
        payload = checkpoint if isinstance(checkpoint, str) else _json_dumps(checkpoint)
        with self._connect() as conn:
            conn.execute(
                "UPDATE original_content_item "
                "SET stage_checkpoint_json=?, updated_at=? WHERE batch_item_id=?",
                (payload, _now(), batch_item_id),
            )

    def _row_to_item(self, row: Any) -> PlanItem:
        d = dict(row.items() if hasattr(row, "items") else zip(row.keys(), row))
        return PlanItem(
            batch_item_id=d.get("batch_item_id", ""),
            batch_id=d.get("batch_id", ""),
            item_index=int(d.get("item_index", 0)),
            item_role=d.get("item_role", ""),
            product_code=d.get("product_code", ""),
            selection_run_id=d.get("selection_run_id", ""),
            direction_assignment_id=d.get("direction_assignment_id", ""),
            compatibility_slot=d.get("compatibility_slot", ""),
            structure_contract_json=d.get("structure_contract_json", ""),
            allocation_signature=d.get("allocation_signature", ""),
            policy_version=d.get("policy_version", ""),
            item_snapshot_hash=d.get("item_snapshot_hash", ""),
            content_bundle_id=d.get("content_bundle_id", ""),
            content_bundle_json=d.get("content_bundle_json", ""),
            content_angle_key=d.get("content_angle_key", ""),
            audience_tension_status=d.get("audience_tension_status", "UNAVAILABLE"),
            claim_keys_json=d.get("claim_keys_json", "[]"),
            requested_hook_id=d.get("requested_hook_id", ""),
            eligible_hook_ids_json=d.get("eligible_hook_ids_json", "[]"),
            cluster_id=d.get("cluster_id"),
            cluster_version=d.get("cluster_version", ""),
            evidence_tier=d.get("evidence_tier", ""),
            macro_family_key=d.get("macro_family_key", ""),
            carrier_mode=d.get("carrier_mode", ""),
            execution_reference_json=d.get("execution_reference_json", ""),
            audience_tension_text=d.get("audience_tension_text", ""),
            actual_hook_id=d.get("actual_hook_id", ""),
            creative_contract_id=d.get("creative_contract_id", ""),
            visual_signature=d.get("visual_signature", ""),
            frozen_direction_package_json=d.get("frozen_direction_package_json", ""),
            stage_checkpoint_json=d.get("stage_checkpoint_json") or "",
            consumer_run_id=d.get("consumer_run_id", ""),
            script_id=d.get("script_id", ""),
            content_id=d.get("content_id", ""),
            video_prompt_id=d.get("video_prompt_id", ""),
            structure_binding_id=d.get("structure_binding_id", ""),
            status=d.get("status", "PLANNED"),
            attempt_count=int(d.get("attempt_count", 0)),
            error_code=d.get("error_code", ""),
            error_message=d.get("error_message", ""),
            result_json=d.get("result_json", ""),
        )
