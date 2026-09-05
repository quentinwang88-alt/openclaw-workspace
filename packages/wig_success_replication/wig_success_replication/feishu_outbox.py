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

    def retry_one(self, outbox_id: str) -> dict[str, Any]:
        """Deliver only an explicitly exported revision, without draining others."""
        row = self.repository.get_outbox(outbox_id)
        if row is None:
            raise ValueError("outbox record not found")
        if row.status == "completed":
            return {"completed": 0, "failed": 0, "status": "already_delivered", "outbox_id": outbox_id}
        try:
            delivered = self.writer.apply(row.operation, row.payload)
            self.repository.complete_outbox(row.outbox_id)
            return {"completed": 1, "failed": 0, "status": "delivered", "outbox_id": outbox_id,
                    "delivery": delivered}
        except Exception as exc:
            self.repository.fail_outbox(row.outbox_id, str(exc))
            return {"completed": 0, "failed": 1, "status": "writeback_pending", "outbox_id": outbox_id}
