"""Retryable Feishu delivery; retries never invoke a model."""

from __future__ import annotations

from typing import Any, Protocol

from .repository import Repository


class FeishuWriter(Protocol):
    def apply(self, operation: str, payload: dict[str, Any]) -> None: ...


class FeishuOutboxService:
    def __init__(self, repository: Repository, writer: FeishuWriter):
        self.repository = repository
        self.writer = writer

    def retry(self, limit: int = 100) -> dict[str, int]:
        result = {"completed": 0, "failed": 0}
        for row in self.repository.pending_outbox(limit):
            try:
                self.writer.apply(row.operation, row.payload)
                self.repository.complete_outbox(row.outbox_id)
                result["completed"] += 1
            except Exception as exc:
                self.repository.fail_outbox(row.outbox_id, str(exc))
                result["failed"] += 1
        return result
