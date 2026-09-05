"""Persistence ports plus in-memory and MySQL/RDS implementations."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from .hashing import deterministic_id
from .model_run_logger import ModelRunRecord
from .models import BatchStatus, MotherContract, MotherReview, MotherStatus, ProductFactCard, ProductFactStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class MotherVersionRecord:
    mother_id: str
    version: int
    feishu_record_id: str
    name: str
    source_product_id: str
    source_script: str
    source_hash: str
    draft: MotherContract
    review: MotherReview
    contract: MotherContract
    status: MotherStatus = MotherStatus.PENDING_CONFIRMATION
    created_at: datetime = field(default_factory=utcnow)


@dataclass
class ProductFactRecord:
    product_id: str
    version: int
    source_hash: str
    fact: ProductFactCard
    human_summary: str
    status: ProductFactStatus = ProductFactStatus.PENDING_CONFIRMATION
    created_at: datetime = field(default_factory=utcnow)


@dataclass
class BatchRecord:
    batch_id: str
    idempotency_key: str
    mother_id: str
    mother_version: int
    product_ids: tuple[str, ...]
    special_requirements: str
    status: BatchStatus = BatchStatus.PENDING
    summary: str = ""
    created_at: datetime = field(default_factory=utcnow)
    publish_purpose: str = "带货"
    cart_enabled: bool | None = True
    handoff_context_by_product: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchProductRecord:
    batch_id: str
    product_id: str
    relationship: str
    product_fact_version: int
    status: str = "pending"
    error_detail: str = ""


@dataclass
class PromptRecord:
    prompt_id: str
    batch_id: str
    mother_id: str
    mother_version: int
    product_id: str
    variant_type: str
    variant_key: str
    mutation_key: str
    change_summary: str
    full_prompt: str
    prompt_hash: str
    sequence_no: int = 0
    replication_mode: str = ""
    creative_route: str = ""
    creative_signature: dict[str, Any] = field(default_factory=dict)
    planner_version: str = ""
    review_status: str = "pending_review"
    feishu_record_id: str = ""
    publish_purpose: str = "带货"
    cart_enabled: bool | None = True
    handoff_context: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScriptPoolBindingRecord:
    prompt_id: str
    target_record_id: str
    last_exported_hash: str
    script_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OutboxRecord:
    outbox_id: str
    aggregate_type: str
    aggregate_id: str
    operation: str
    payload: dict[str, Any]
    status: str = "pending"
    attempts: int = 0
    error_detail: str = ""


class Repository(Protocol):
    def latest_mother(self, mother_id: str) -> MotherVersionRecord | None: ...
    def get_mother_version(self, mother_id: str, version: int) -> MotherVersionRecord | None: ...
    def get_prompt(self, prompt_id: str) -> PromptRecord | None: ...
    def save_mother(self, record: MotherVersionRecord) -> None: ...
    def set_mother_status(self, mother_id: str, version: int, status: MotherStatus) -> None: ...
    def latest_product_fact(self, product_id: str) -> ProductFactRecord | None: ...
    def save_product_fact(self, record: ProductFactRecord) -> None: ...
    def set_product_status(self, product_id: str, version: int, status: ProductFactStatus) -> None: ...
    def get_or_create_batch(self, record: BatchRecord) -> tuple[BatchRecord, bool]: ...
    def get_batch_product(self, batch_id: str, product_id: str) -> BatchProductRecord | None: ...
    def save_batch_product(self, record: BatchProductRecord) -> None: ...
    def set_batch_status(self, batch_id: str, status: BatchStatus, summary: str = "") -> None: ...
    def prompt_exists(self, mother_id: str, mother_version: int, product_id: str, digest: str, publish_purpose: str = "带货") -> bool: ...
    def prompt_slot_exists(self, batch_id: str, product_id: str, variant_key: str, mutation_key: str) -> bool: ...
    def prompt_sequence_exists(self, mother_id: str, mother_version: int, product_id: str, sequence_no: int, publish_purpose: str = "带货") -> bool: ...
    def list_prompts(self, mother_id: str, mother_version: int, product_id: str, publish_purpose: str | None = "带货") -> list[PromptRecord]: ...
    def get_script_pool_binding(self, prompt_id: str) -> ScriptPoolBindingRecord | None: ...
    def save_script_pool_binding(self, record: ScriptPoolBindingRecord) -> None: ...
    def save_prompt(self, record: PromptRecord) -> bool: ...
    def save_model_run(self, record: ModelRunRecord) -> None: ...
    def enqueue_outbox(self, record: OutboxRecord) -> bool: ...
    def get_outbox(self, outbox_id: str) -> OutboxRecord | None: ...
    def pending_outbox(self, limit: int = 100) -> list[OutboxRecord]: ...
    def complete_outbox(self, outbox_id: str) -> None: ...
    def fail_outbox(self, outbox_id: str, error: str) -> None: ...


class InMemoryRepository:
    """Deterministic test/development repository with production uniqueness rules."""

    def __init__(self) -> None:
        self.mothers: dict[tuple[str, int], MotherVersionRecord] = {}
        self.products: dict[tuple[str, int], ProductFactRecord] = {}
        self.batches: dict[str, BatchRecord] = {}
        self.batch_by_key: dict[str, str] = {}
        self.batch_products: dict[tuple[str, str], BatchProductRecord] = {}
        self.prompts: dict[str, PromptRecord] = {}
        self.prompt_digests: set[tuple[str, int, str, str, str]] = set()
        self.script_pool_bindings: dict[str, ScriptPoolBindingRecord] = {}
        self.model_runs: list[ModelRunRecord] = []
        self.outbox: dict[str, OutboxRecord] = {}
        self._lock = threading.RLock()

    def latest_mother(self, mother_id: str) -> MotherVersionRecord | None:
        rows = [row for (key, _), row in self.mothers.items() if key == mother_id]
        return max(rows, key=lambda row: row.version) if rows else None

    def get_mother_version(self, mother_id: str, version: int) -> MotherVersionRecord | None:
        return self.mothers.get((mother_id, version))

    def get_prompt(self, prompt_id: str) -> PromptRecord | None:
        return self.prompts.get(prompt_id)

    def save_mother(self, record: MotherVersionRecord) -> None:
        key = (record.mother_id, record.version)
        if key in self.mothers:
            raise ValueError(f"mother version already exists: {key}")
        self.mothers[key] = record

    def set_mother_status(self, mother_id: str, version: int, status: MotherStatus) -> None:
        self.mothers[(mother_id, version)].status = status

    def latest_product_fact(self, product_id: str) -> ProductFactRecord | None:
        rows = [row for (key, _), row in self.products.items() if key == product_id]
        return max(rows, key=lambda row: row.version) if rows else None

    def save_product_fact(self, record: ProductFactRecord) -> None:
        key = (record.product_id, record.version)
        if key in self.products:
            raise ValueError(f"product fact version already exists: {key}")
        self.products[key] = record

    def set_product_status(self, product_id: str, version: int, status: ProductFactStatus) -> None:
        self.products[(product_id, version)].status = status
        self.products[(product_id, version)].fact.human_confirmed = status == ProductFactStatus.CONFIRMED

    def get_or_create_batch(self, record: BatchRecord) -> tuple[BatchRecord, bool]:
        with self._lock:
            existing_id = self.batch_by_key.get(record.idempotency_key)
            if existing_id:
                return self.batches[existing_id], False
            self.batches[record.batch_id] = record
            self.batch_by_key[record.idempotency_key] = record.batch_id
            return record, True

    def get_batch_product(self, batch_id: str, product_id: str) -> BatchProductRecord | None:
        with self._lock:
            return self.batch_products.get((batch_id, product_id))

    def save_batch_product(self, record: BatchProductRecord) -> None:
        with self._lock:
            self.batch_products[(record.batch_id, record.product_id)] = record

    def set_batch_status(self, batch_id: str, status: BatchStatus, summary: str = "") -> None:
        self.batches[batch_id].status = status
        self.batches[batch_id].summary = summary

    def prompt_exists(self, mother_id: str, mother_version: int, product_id: str, digest: str, publish_purpose: str = "带货") -> bool:
        with self._lock:
            return (mother_id, mother_version, product_id, publish_purpose, digest) in self.prompt_digests

    def prompt_slot_exists(self, batch_id: str, product_id: str, variant_key: str, mutation_key: str) -> bool:
        with self._lock:
            return any(
                row.batch_id == batch_id
                and row.product_id == product_id
                and row.variant_key == variant_key
                and row.mutation_key == mutation_key
                for row in self.prompts.values()
            )

    def prompt_sequence_exists(self, mother_id: str, mother_version: int, product_id: str, sequence_no: int, publish_purpose: str = "带货") -> bool:
        with self._lock:
            return any(
                row.mother_id == mother_id
                and row.mother_version == mother_version
                and row.product_id == product_id
                and row.sequence_no == sequence_no
                and row.publish_purpose == publish_purpose
                for row in self.prompts.values()
            )

    def list_prompts(self, mother_id: str, mother_version: int, product_id: str, publish_purpose: str | None = "带货") -> list[PromptRecord]:
        with self._lock:
            rows = [
                row for row in self.prompts.values()
                if row.mother_id == mother_id
                and row.mother_version == mother_version
                and row.product_id == product_id
                and (publish_purpose is None or row.publish_purpose == publish_purpose)
            ]
        return sorted(rows, key=lambda row: (row.sequence_no or 10_000, row.prompt_id))

    def save_prompt(self, record: PromptRecord) -> bool:
        with self._lock:
            digest_key = (record.mother_id, record.mother_version, record.product_id, record.publish_purpose, record.prompt_hash)
            sequence_exists = record.sequence_no > 0 and self.prompt_sequence_exists(
                record.mother_id, record.mother_version, record.product_id, record.sequence_no, record.publish_purpose
            )
            if record.prompt_id in self.prompts or digest_key in self.prompt_digests or sequence_exists:
                return False
            self.prompts[record.prompt_id] = record
            self.prompt_digests.add(digest_key)
            return True

    def get_script_pool_binding(self, prompt_id: str) -> ScriptPoolBindingRecord | None:
        return self.script_pool_bindings.get(prompt_id)

    def save_script_pool_binding(self, record: ScriptPoolBindingRecord) -> None:
        self.script_pool_bindings[record.prompt_id] = record

    def save_model_run(self, record: ModelRunRecord) -> None:
        with self._lock:
            self.model_runs.append(record)

    def enqueue_outbox(self, record: OutboxRecord) -> bool:
        with self._lock:
            if record.outbox_id in self.outbox or any(
                (row.aggregate_type, row.aggregate_id, row.operation)
                == (record.aggregate_type, record.aggregate_id, record.operation)
                for row in self.outbox.values()
            ):
                return False
            self.outbox[record.outbox_id] = record
            return True

    def pending_outbox(self, limit: int = 100) -> list[OutboxRecord]:
        return [row for row in self.outbox.values() if row.status in {"pending", "failed"}][:limit]

    def get_outbox(self, outbox_id: str) -> OutboxRecord | None:
        return self.outbox.get(outbox_id)

    def complete_outbox(self, outbox_id: str) -> None:
        self.outbox[outbox_id].status = "completed"
        self.outbox[outbox_id].error_detail = ""

    def fail_outbox(self, outbox_id: str, error: str) -> None:
        row = self.outbox[outbox_id]
        row.status = "failed"
        row.attempts += 1
        row.error_detail = error[:2000]


class MySQLRepository:
    """MySQL/RDS adapter. Schema is provisioned explicitly by migration 001."""

    def __init__(self, connection_factory: Callable[[], Any]):
        self.connection_factory = connection_factory

    def _fetchone(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        with self.connection_factory() as conn, conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def _execute(self, sql: str, params: tuple[Any, ...]) -> int:
        with self.connection_factory() as conn, conn.cursor() as cursor:
            count = cursor.execute(sql, params)
            conn.commit()
            return count

    @staticmethod
    def _mother(row: dict[str, Any] | None) -> MotherVersionRecord | None:
        if not row:
            return None
        return MotherVersionRecord(
            mother_id=row["mother_id"], version=int(row["version"]),
            feishu_record_id=row.get("feishu_record_id") or "", name=row["name"],
            source_product_id=row["source_product_id"], source_script=row["source_script"],
            source_hash=row["source_hash"], draft=MotherContract.model_validate(json.loads(row["draft_json"])),
            review=MotherReview.model_validate(json.loads(row["review_json"])),
            contract=MotherContract.model_validate(json.loads(row["contract_json"])),
            status=MotherStatus(row["status"]), created_at=row["created_at"],
        )

    def latest_mother(self, mother_id: str) -> MotherVersionRecord | None:
        return self._mother(self._fetchone(
            "SELECT * FROM wsr_mother_version WHERE mother_id=%s ORDER BY version DESC LIMIT 1", (mother_id,)
        ))

    def get_mother_version(self, mother_id: str, version: int) -> MotherVersionRecord | None:
        return self._mother(self._fetchone(
            "SELECT * FROM wsr_mother_version WHERE mother_id=%s AND version=%s LIMIT 1", (mother_id, version)
        ))

    def get_prompt(self, prompt_id: str) -> PromptRecord | None:
        row = self._fetchone("SELECT * FROM wsr_replication_prompt WHERE prompt_id=%s LIMIT 1", (prompt_id,))
        return self._prompt(row) if row else None

    def save_mother(self, record: MotherVersionRecord) -> None:
        self._execute(
            """INSERT INTO wsr_mother
            (mother_id,feishu_record_id,name,source_product_id,current_version,status)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE name=VALUES(name),source_product_id=VALUES(source_product_id),
              current_version=VALUES(current_version),status=VALUES(status)""",
            (record.mother_id, record.feishu_record_id, record.name, record.source_product_id,
             record.version, record.status.value),
        )
        self._execute(
            """INSERT INTO wsr_mother_version
            (mother_id,version,feishu_record_id,name,source_product_id,source_script,source_hash,
             draft_json,review_json,contract_json,status,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (record.mother_id, record.version, record.feishu_record_id, record.name,
             record.source_product_id, record.source_script, record.source_hash,
             record.draft.model_dump_json(), record.review.model_dump_json(), record.contract.model_dump_json(),
             record.status.value, record.created_at),
        )

    def set_mother_status(self, mother_id: str, version: int, status: MotherStatus) -> None:
        self._execute("UPDATE wsr_mother_version SET status=%s WHERE mother_id=%s AND version=%s", (status.value, mother_id, version))
        self._execute("UPDATE wsr_mother SET status=%s,current_version=%s WHERE mother_id=%s", (status.value, version, mother_id))

    @staticmethod
    def _product(row: dict[str, Any] | None) -> ProductFactRecord | None:
        if not row:
            return None
        return ProductFactRecord(
            product_id=row["product_id"], version=int(row["version"]), source_hash=row["source_hash"],
            fact=ProductFactCard.model_validate(json.loads(row["fact_json"])), human_summary=row["human_summary"],
            status=ProductFactStatus(row["status"]), created_at=row["created_at"],
        )

    def latest_product_fact(self, product_id: str) -> ProductFactRecord | None:
        return self._product(self._fetchone(
            "SELECT * FROM wsr_product_fact_version WHERE product_id=%s ORDER BY version DESC LIMIT 1", (product_id,)
        ))

    def save_product_fact(self, record: ProductFactRecord) -> None:
        self._execute(
            """INSERT INTO wsr_product_fact_version
            (product_id,version,source_hash,fact_json,human_summary,status,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (record.product_id, record.version, record.source_hash, record.fact.model_dump_json(),
             record.human_summary, record.status.value, record.created_at),
        )

    def set_product_status(self, product_id: str, version: int, status: ProductFactStatus) -> None:
        self._execute("UPDATE wsr_product_fact_version SET status=%s WHERE product_id=%s AND version=%s", (status.value, product_id, version))

    def get_or_create_batch(self, record: BatchRecord) -> tuple[BatchRecord, bool]:
        row = self._fetchone("SELECT * FROM wsr_replication_batch WHERE idempotency_key=%s", (record.idempotency_key,))
        if row:
            return BatchRecord(
                batch_id=row["batch_id"], idempotency_key=row["idempotency_key"], mother_id=row["mother_id"],
                mother_version=int(row["mother_version"]), product_ids=tuple(json.loads(row["product_ids_json"])),
                special_requirements=row.get("special_requirements") or "", status=BatchStatus(row["status"]),
                summary=row.get("summary") or "", created_at=row["created_at"],
                publish_purpose=row.get("publish_purpose") or "未分类",
                cart_enabled=None if row.get("cart_enabled") is None else bool(row["cart_enabled"]),
                handoff_context_by_product=json.loads(row.get("handoff_context_json") or "{}"),
            ), False
        self._execute(
            """INSERT INTO wsr_replication_batch
            (batch_id,idempotency_key,mother_id,mother_version,product_ids_json,special_requirements,status,created_at,
             publish_purpose,cart_enabled,handoff_context_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (record.batch_id, record.idempotency_key, record.mother_id, record.mother_version,
             json.dumps(record.product_ids, ensure_ascii=False), record.special_requirements,
             record.status.value, record.created_at, record.publish_purpose, record.cart_enabled,
             json.dumps(record.handoff_context_by_product, ensure_ascii=False)),
        )
        return record, True

    def get_batch_product(self, batch_id: str, product_id: str) -> BatchProductRecord | None:
        row = self._fetchone(
            "SELECT * FROM wsr_replication_batch_product WHERE batch_id=%s AND product_id=%s",
            (batch_id, product_id),
        )
        if not row:
            return None
        return BatchProductRecord(
            batch_id=row["batch_id"], product_id=row["product_id"],
            relationship=row["relationship"], product_fact_version=int(row["product_fact_version"]),
            status=row["status"], error_detail=row.get("error_detail") or "",
        )

    def save_batch_product(self, record: BatchProductRecord) -> None:
        self._execute(
            """INSERT INTO wsr_replication_batch_product
            (batch_id,product_id,relationship,product_fact_version,status,error_detail)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE status=VALUES(status),error_detail=VALUES(error_detail)""",
            (record.batch_id, record.product_id, record.relationship, record.product_fact_version,
             record.status, record.error_detail),
        )

    def set_batch_status(self, batch_id: str, status: BatchStatus, summary: str = "") -> None:
        self._execute("UPDATE wsr_replication_batch SET status=%s,summary=%s WHERE batch_id=%s", (status.value, summary, batch_id))

    def prompt_exists(self, mother_id: str, mother_version: int, product_id: str, digest: str, publish_purpose: str = "带货") -> bool:
        return self._fetchone(
            "SELECT prompt_id FROM wsr_replication_prompt WHERE mother_id=%s AND mother_version=%s AND product_id=%s AND publish_purpose=%s AND prompt_hash=%s LIMIT 1",
            (mother_id, mother_version, product_id, publish_purpose, digest),
        ) is not None

    def prompt_slot_exists(self, batch_id: str, product_id: str, variant_key: str, mutation_key: str) -> bool:
        return self._fetchone(
            """SELECT prompt_id FROM wsr_replication_prompt
            WHERE batch_id=%s AND product_id=%s AND variant_key=%s AND mutation_key=%s LIMIT 1""",
            (batch_id, product_id, variant_key, mutation_key),
        ) is not None

    def prompt_sequence_exists(self, mother_id: str, mother_version: int, product_id: str, sequence_no: int, publish_purpose: str = "带货") -> bool:
        return self._fetchone(
            """SELECT prompt_id FROM wsr_replication_prompt
            WHERE mother_id=%s AND mother_version=%s AND product_id=%s AND sequence_no=%s AND publish_purpose=%s LIMIT 1""",
            (mother_id, mother_version, product_id, sequence_no, publish_purpose),
        ) is not None

    @staticmethod
    def _prompt(row: dict[str, Any]) -> PromptRecord:
        signature = row.get("creative_signature_json") or {}
        if isinstance(signature, str):
            signature = json.loads(signature)
        return PromptRecord(
            prompt_id=row["prompt_id"], batch_id=row["batch_id"], mother_id=row["mother_id"],
            mother_version=int(row["mother_version"]), product_id=row["product_id"],
            variant_type=row["variant_type"], variant_key=row["variant_key"],
            mutation_key=row["mutation_key"], change_summary=row["change_summary"],
            full_prompt=row["full_prompt"], prompt_hash=row["prompt_hash"],
            sequence_no=int(row.get("sequence_no") or 0),
            replication_mode=row.get("replication_mode") or "",
            creative_route=row.get("creative_route") or "",
            creative_signature=signature if isinstance(signature, dict) else {},
            planner_version=row.get("planner_version") or "",
            review_status=row.get("review_status") or "pending_review",
            feishu_record_id=row.get("feishu_record_id") or "",
            publish_purpose=row.get("publish_purpose") or "未分类",
            cart_enabled=None if row.get("cart_enabled") is None else bool(row["cart_enabled"]),
            handoff_context=json.loads(row.get("handoff_context_json") or "{}"),
        )

    def list_prompts(self, mother_id: str, mother_version: int, product_id: str, publish_purpose: str | None = "带货") -> list[PromptRecord]:
        with self.connection_factory() as conn, conn.cursor() as cursor:
            purpose_clause = " AND publish_purpose=%s" if publish_purpose is not None else ""
            params = (mother_id, mother_version, product_id)
            if publish_purpose is not None:
                params += (publish_purpose,)
            cursor.execute(
                """SELECT * FROM wsr_replication_prompt
                WHERE mother_id=%s AND mother_version=%s AND product_id=%s"""
                + purpose_clause + " ORDER BY sequence_no,prompt_id",
                params,
            )
            rows = cursor.fetchall()
        return [self._prompt(row) for row in rows]

    def save_prompt(self, record: PromptRecord) -> bool:
        try:
            self._execute(
                """INSERT INTO wsr_replication_prompt
                (prompt_id,batch_id,mother_id,mother_version,product_id,variant_type,variant_key,mutation_key,
                 change_summary,full_prompt,prompt_hash,sequence_no,replication_mode,creative_route,
                 creative_signature_json,planner_version,review_status,feishu_record_id,
                 publish_purpose,cart_enabled,handoff_context_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (record.prompt_id, record.batch_id, record.mother_id, record.mother_version, record.product_id,
                 record.variant_type, record.variant_key, record.mutation_key, record.change_summary,
                 record.full_prompt, record.prompt_hash, record.sequence_no, record.replication_mode,
                 record.creative_route, json.dumps(record.creative_signature, ensure_ascii=False),
                 record.planner_version, record.review_status, record.feishu_record_id,
                 record.publish_purpose, record.cart_enabled, json.dumps(record.handoff_context, ensure_ascii=False)),
            )
            return True
        except Exception as exc:
            if "Duplicate" in str(exc) or "1062" in str(exc):
                return False
            raise

    def get_script_pool_binding(self, prompt_id: str) -> ScriptPoolBindingRecord | None:
        row = self._fetchone("SELECT * FROM wsr_script_pool_binding WHERE prompt_id=%s", (prompt_id,))
        if not row:
            return None
        return ScriptPoolBindingRecord(
            prompt_id=row["prompt_id"], target_record_id=row.get("target_record_id") or "",
            last_exported_hash=row["last_exported_hash"], script_id=row.get("script_id") or "",
            metadata=json.loads(row.get("metadata_json") or "{}"),
        )

    def save_script_pool_binding(self, record: ScriptPoolBindingRecord) -> None:
        self._execute(
            """INSERT INTO wsr_script_pool_binding
            (prompt_id,target_record_id,last_exported_hash,script_id,metadata_json)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE target_record_id=VALUES(target_record_id),
            last_exported_hash=VALUES(last_exported_hash),script_id=VALUES(script_id),
            metadata_json=VALUES(metadata_json)""",
            (record.prompt_id, record.target_record_id or None, record.last_exported_hash, record.script_id,
             json.dumps(record.metadata, ensure_ascii=False)),
        )

    def save_model_run(self, record: ModelRunRecord) -> None:
        self._execute(
            """INSERT INTO wsr_model_run
            (run_id,task_type,entity_id,model,reasoning_effort,schema_name,schema_version,
             provider_request_id,retry_count,duration_ms,status,error_summary,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (record.run_id, record.task_type, record.entity_id, record.model, record.reasoning_effort,
             record.schema_name, record.schema_version, record.provider_request_id, record.retry_count,
             record.duration_ms, record.status, record.error_summary, record.created_at),
        )

    def enqueue_outbox(self, record: OutboxRecord) -> bool:
        try:
            self._execute(
                """INSERT INTO wsr_feishu_outbox
                (outbox_id,aggregate_type,aggregate_id,operation,payload_json,status,attempts,error_detail)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (record.outbox_id, record.aggregate_type, record.aggregate_id, record.operation,
                 json.dumps(record.payload, ensure_ascii=False), record.status, record.attempts, record.error_detail),
            )
            return True
        except Exception as exc:
            if "Duplicate" in str(exc) or "1062" in str(exc):
                return False
            raise

    def get_outbox(self, outbox_id: str) -> OutboxRecord | None:
        row = self._fetchone("SELECT * FROM wsr_feishu_outbox WHERE outbox_id=%s LIMIT 1", (outbox_id,))
        if row is None:
            return None
        return OutboxRecord(outbox_id=row["outbox_id"], aggregate_type=row["aggregate_type"], aggregate_id=row["aggregate_id"],
            operation=row["operation"], payload=json.loads(row["payload_json"]), status=row["status"],
            attempts=int(row["attempts"]), error_detail=row.get("error_detail") or "")

    def pending_outbox(self, limit: int = 100) -> list[OutboxRecord]:
        with self.connection_factory() as conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM wsr_feishu_outbox WHERE status IN ('pending','failed') ORDER BY created_at LIMIT %s",
                (limit,),
            )
            rows = cursor.fetchall()
        return [OutboxRecord(
            outbox_id=row["outbox_id"], aggregate_type=row["aggregate_type"], aggregate_id=row["aggregate_id"],
            operation=row["operation"], payload=json.loads(row["payload_json"]), status=row["status"],
            attempts=int(row["attempts"]), error_detail=row.get("error_detail") or "",
        ) for row in rows]

    def complete_outbox(self, outbox_id: str) -> None:
        self._execute("UPDATE wsr_feishu_outbox SET status='completed',error_detail='' WHERE outbox_id=%s", (outbox_id,))

    def fail_outbox(self, outbox_id: str, error: str) -> None:
        self._execute(
            "UPDATE wsr_feishu_outbox SET status='failed',attempts=attempts+1,error_detail=%s WHERE outbox_id=%s",
            (error[:2000], outbox_id),
        )


def make_outbox_record(aggregate_type: str, aggregate_id: str, operation: str, payload: dict[str, Any]) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=deterministic_id("outbox", aggregate_type, aggregate_id, operation, payload),
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        operation=operation,
        payload=payload,
    )
