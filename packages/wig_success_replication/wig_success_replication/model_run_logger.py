"""Model run metadata logging without hidden reasoning or prompt contents."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from .hashing import deterministic_id


@dataclass(frozen=True)
class ModelRunRecord:
    run_id: str
    task_type: str
    entity_id: str
    model: str
    reasoning_effort: str
    schema_name: str
    schema_version: str
    provider_request_id: str
    retry_count: int
    duration_ms: int
    status: str
    error_summary: str
    created_at: datetime


class ModelRunSink(Protocol):
    def save_model_run(self, record: ModelRunRecord) -> None: ...


class ModelRunLogger:
    def __init__(self, sink: ModelRunSink):
        self.sink = sink

    def record(
        self,
        *,
        task_type: str,
        entity_id: str,
        schema_name: str,
        schema_version: str,
        provider_request_id: str,
        retry_count: int,
        duration_ms: int,
        status: str,
        error_summary: str = "",
        model: str = "gpt-5.6-sol",
        reasoning_effort: str = "high",
    ) -> ModelRunRecord:
        now = datetime.now(timezone.utc)
        record = ModelRunRecord(
            run_id=deterministic_id(
                "run", task_type, entity_id, schema_name, provider_request_id, now.isoformat()
            ),
            task_type=task_type,
            entity_id=entity_id,
            model=model,
            reasoning_effort=reasoning_effort,
            schema_name=schema_name,
            schema_version=schema_version,
            provider_request_id=provider_request_id,
            retry_count=retry_count,
            duration_ms=duration_ms,
            status=status,
            error_summary=error_summary[:1000],
            created_at=now,
        )
        self.sink.save_model_run(record)
        return record
