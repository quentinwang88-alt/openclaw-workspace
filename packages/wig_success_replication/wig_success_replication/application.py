"""Stable public adapter used by the thin OpenClaw skill entrypoints."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from .feishu_outbox import FeishuOutboxService


class PendingTaskRunner(Protocol):
    def run_pending(self, action: str, record_id: str | None = None, limit: int = 20) -> Any: ...


@dataclass
class WigReplicationApplication:
    task_runner: PendingTaskRunner | None = None
    outbox_service: FeishuOutboxService | None = None

    def run_pending(self, action: str, record_id: str | None = None, limit: int = 20) -> Any:
        if self.task_runner is None:
            raise RuntimeError("task runner is not configured; inject Feishu adapters in the skill composition root")
        return self.task_runner.run_pending(action=action, record_id=record_id, limit=limit)

    def retry_feishu_outbox(self, limit: int = 100) -> dict[str, int]:
        if self.outbox_service is None:
            raise RuntimeError("Feishu outbox writer is not configured")
        return self.outbox_service.retry(limit=limit)

    def check_runtime(self) -> dict[str, Any]:
        return {
            "ok": self.task_runner is not None and self.outbox_service is not None,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "database_configured": bool(os.environ.get("WIG_REPLICATION_DATABASE_URL") or os.environ.get("LIKEU_AI_DATABASE_URL")),
            "task_runner_configured": self.task_runner is not None,
            "outbox_configured": self.outbox_service is not None,
        }


_factory: Callable[[], WigReplicationApplication] | None = None


def configure_application(factory: Callable[[], WigReplicationApplication]) -> None:
    """Let the skill composition root inject Feishu/auth/database adapters."""
    global _factory
    _factory = factory


def build_application() -> WigReplicationApplication:
    """Return the configured app without performing migrations or external calls."""
    return _factory() if _factory is not None else WigReplicationApplication()
