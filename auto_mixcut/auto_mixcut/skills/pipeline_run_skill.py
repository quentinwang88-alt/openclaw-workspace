from __future__ import annotations

from datetime import datetime
from typing import Any

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class PipelineRunSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self._ensured = False

    def start_step(self, product_id: str, step_name: str, batch_id: str = "", detail: dict[str, Any] | None = None) -> str:
        self._ensure_table()
        step_run_id = new_id("PIPESTEP")
        self.ctx.repo.upsert(
            "pipeline_step_runs",
            "step_run_id",
            {
                "step_run_id": step_run_id,
                "product_id": product_id,
                "batch_id": batch_id,
                "step_name": step_name,
                "status": "running",
                "started_at": _now(),
                "detail_json": detail or {},
            },
        )
        return step_run_id

    def finish_step(self, step_run_id: str, result: Result, detail: dict[str, Any] | None = None) -> None:
        self._ensure_table()
        payload: dict[str, Any] = {
            "status": "success" if result.success else "failed",
            "finished_at": _now(),
            "detail_json": detail if detail is not None else (result.data if result.success else (result.error.detail if result.error else {})),
        }
        if result.error:
            payload.update({"error_code": result.error.code, "error_message": result.error.message})
        self.ctx.repo.update("pipeline_step_runs", "step_run_id", step_run_id, payload)

    def fail_step(self, step_run_id: str, code: str, message: str, detail: dict[str, Any] | None = None) -> None:
        self._ensure_table()
        self.ctx.repo.update(
            "pipeline_step_runs",
            "step_run_id",
            step_run_id,
            {
                "status": "failed",
                "finished_at": _now(),
                "error_code": code,
                "error_message": message,
                "detail_json": detail or {},
            },
        )

    def _ensure_table(self) -> None:
        if self._ensured:
            return
        if getattr(self.ctx.repo, "dialect", "sqlite") == "mysql":
            with self.ctx.repo.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS pipeline_step_runs (
                          id BIGINT PRIMARY KEY AUTO_INCREMENT,
                          step_run_id VARCHAR(128) NOT NULL UNIQUE,
                          product_id VARCHAR(128),
                          batch_id VARCHAR(128),
                          step_name VARCHAR(128),
                          status VARCHAR(64),
                          error_code VARCHAR(128),
                          error_message TEXT,
                          detail_json JSON,
                          started_at DATETIME,
                          finished_at DATETIME,
                          created_at DATETIME,
                          updated_at DATETIME,
                          KEY idx_pipeline_step_runs_product (product_id),
                          KEY idx_pipeline_step_runs_batch (batch_id),
                          KEY idx_pipeline_step_runs_step (step_name)
                        )
                        """
                    )
        else:
            with self.ctx.repo.connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_step_runs (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      step_run_id TEXT NOT NULL UNIQUE,
                      product_id TEXT,
                      batch_id TEXT,
                      step_name TEXT,
                      status TEXT,
                      error_code TEXT,
                      error_message TEXT,
                      detail_json TEXT,
                      started_at TEXT,
                      finished_at TEXT,
                      created_at TEXT,
                      updated_at TEXT
                    )
                    """
                )
        self._ensured = True


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
