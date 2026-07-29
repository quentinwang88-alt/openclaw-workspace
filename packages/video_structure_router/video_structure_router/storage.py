"""Persistence for router-owned ``sr_*`` tables."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlparse

from .models import SelectionResult


class RouterStorage:
    def __init__(self, database_url: str = ""):
        self.database_url = (
            database_url
            or os.environ.get("STRUCTURE_ROUTER_DATABASE_URL")
            or os.environ.get("LIKEU_AI_DATABASE_URL")
            or ""
        ).strip()

    def _connect(self):
        if not self.database_url:
            raise RuntimeError("缺少 STRUCTURE_ROUTER_DATABASE_URL / LIKEU_AI_DATABASE_URL")
        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("RDS 结构路由需要 PyMySQL") from exc
        parsed = urlparse(self.database_url)
        return pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            database=parsed.path.lstrip("/"),
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=20,
            read_timeout=60,
            write_timeout=60,
        )

    @staticmethod
    def _dump(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    def ensure_schema(self) -> None:
        migration_path = Path(__file__).resolve().parents[1] / "migrations" / "001_create_sr_tables_mysql.sql"
        script = migration_path.read_text(encoding="utf-8")
        statements = [item.strip() for item in script.split(";") if item.strip()]
        with self._connect() as conn:
            with conn.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
            conn.commit()

    def persist_selection(self, result: SelectionResult) -> None:
        request = result.request
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO sr_selection_run (
                        selection_run_id, request_id, consumer_flow, product_code,
                        target_country, category, product_type, direction_count,
                        duration_seconds, policy_version, random_seed, selection_status,
                        degraded_reasons_json, request_json, input_snapshot_json,
                        data_snapshot_hash, selected_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        selection_status = VALUES(selection_status),
                        degraded_reasons_json = VALUES(degraded_reasons_json),
                        input_snapshot_json = VALUES(input_snapshot_json),
                        selected_count = VALUES(selected_count),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        result.selection_run_id,
                        request.request_id,
                        request.consumer_flow,
                        request.product_code or None,
                        request.target_country or None,
                        request.category or None,
                        request.product_type or None,
                        request.direction_count,
                        request.duration_seconds,
                        result.policy_version,
                        request.random_seed,
                        result.selection_status,
                        self._dump(result.degraded_reasons),
                        self._dump(request.to_dict()),
                        self._dump(result.input_snapshot),
                        result.data_snapshot_hash,
                        len(result.assignments),
                    ),
                )
                for assignment in result.assignments:
                    value = assignment.to_dict()
                    cursor.execute(
                        """
                        INSERT INTO sr_direction_assignment (
                            direction_assignment_id, selection_run_id, direction_index,
                            output_slot, direction_role, candidate_key, source_kind,
                            source_run_id, cluster_id, cluster_version, prototype_id,
                            evidence_tier, macro_family_key, visual_archetype_key,
                            structure_contract_json, selection_score
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            output_slot = VALUES(output_slot),
                            direction_role = VALUES(direction_role),
                            structure_contract_json = VALUES(structure_contract_json),
                            selection_score = VALUES(selection_score)
                        """,
                        (
                            value["direction_assignment_id"],
                            value["selection_run_id"],
                            value["direction_index"],
                            value["output_slot"],
                            value["direction_role"],
                            value["candidate_key"],
                            value["source_kind"],
                            value["source_run_id"] or None,
                            value["cluster_id"],
                            value["cluster_version"] or None,
                            value["prototype_id"] or None,
                            value["evidence_tier"],
                            value["macro_family_key"],
                            value["visual_archetype_key"],
                            self._dump(value["structure_contract"]),
                            value["selection_score"],
                        ),
                    )
            conn.commit()

    def bind_application(
        self,
        *,
        selection_run_id: str,
        direction_assignment_id: str,
        consumer_flow: str,
        consumer_run_id: str,
        record_id: str,
        product_code: str,
        application_stage: str,
        script_id: str = "",
        content_id: str = "",
        video_prompt_id: str = "",
        production_video_id: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        binding_material = {
            "direction_assignment_id": direction_assignment_id,
            "consumer_flow": consumer_flow,
            "consumer_run_id": consumer_run_id,
            "application_stage": application_stage,
        }
        binding_id = "SRB_" + hashlib.sha256(
            self._dump(binding_material).encode("utf-8")
        ).hexdigest()[:24]
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO sr_application_binding (
                        binding_id, selection_run_id, direction_assignment_id,
                        consumer_flow, consumer_run_id, record_id, product_code,
                        script_id, content_id, video_prompt_id, production_video_id,
                        application_stage, metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        script_id = VALUES(script_id),
                        content_id = VALUES(content_id),
                        video_prompt_id = VALUES(video_prompt_id),
                        production_video_id = VALUES(production_video_id),
                        metadata_json = VALUES(metadata_json),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        binding_id,
                        selection_run_id,
                        direction_assignment_id,
                        consumer_flow,
                        consumer_run_id or None,
                        record_id or None,
                        product_code or None,
                        script_id or None,
                        content_id or None,
                        video_prompt_id or None,
                        production_video_id or None,
                        application_stage,
                        self._dump(metadata or {}),
                    ),
                )
            conn.commit()
        return binding_id
