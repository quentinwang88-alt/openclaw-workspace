from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys

from auto_mixcut.core.result import Result

from .context import SkillContext
from .feishu_review_skill import FeishuReviewSkill
from .final_video_qc_skill import FinalVideoQCSkill


ROOT = Path(__file__).resolve().parents[2]


class FinalVideoQCAsyncSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def dispatch_batch(self, batch_id: str) -> Result:
        batch_id = str(batch_id or "").strip()
        if not batch_id:
            return Result.fail("BATCH_ID_REQUIRED", "batch_id is required")
        batch = self.ctx.repo.get("mixcut_batches", "batch_id", batch_id)
        if not batch:
            return Result.fail("BATCH_NOT_FOUND", "batch not found", {"batch_id": batch_id})
        status = str(batch.get("final_qc_async_status") or "").strip().lower()
        if status in {"queued", "running", "done"}:
            return Result.ok({"batch_id": batch_id, "status": "skipped", "reason": f"final_qc_async_{status}"})

        pending = self.ctx.repo.list_where(
            "outputs",
            "batch_id=? AND final_qc_json IS NULL AND machine_quality_status IN ('pending','publish_ready','needs_review','passed','passed_with_warning')",
            (batch_id,),
        )
        if not pending:
            self.ctx.repo.update(
                "mixcut_batches",
                "batch_id",
                batch_id,
                {
                    "final_qc_async_status": "done",
                    "final_qc_async_error": "",
                    "final_qc_async_updated_at": _now(),
                },
            )
            return Result.ok({"batch_id": batch_id, "status": "skipped", "reason": "no_pending_outputs"})

        if getattr(self.ctx.ffmpeg, "mock", False):
            return Result.ok({"batch_id": batch_id, "status": "skipped", "reason": "mock_ffmpeg_context", "pending_count": len(pending)})

        log_dir = ROOT / "logs" / "final_video_qc"
        log_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        log_path = log_dir / f"final_video_qc_{batch_id}_{stamp}.log"
        cmd = [sys.executable, str(ROOT / "scripts" / "run_async_final_video_qc.py"), "--batch-id", batch_id]

        self.ctx.repo.update(
            "mixcut_batches",
            "batch_id",
            batch_id,
            {
                "final_qc_async_status": "queued",
                "final_qc_async_error": "",
                "final_qc_async_log_path": str(log_path),
                "final_qc_async_updated_at": _now(),
            },
        )

        env = os.environ.copy()
        log_fh = log_path.open("a", encoding="utf-8")
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                env=env,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
        finally:
            log_fh.close()
        return Result.ok(
            {
                "batch_id": batch_id,
                "status": "queued",
                "pid": proc.pid,
                "log_path": str(log_path),
                "pending_count": len(pending),
            }
        )

    def run_batch(self, batch_id: str) -> Result:
        batch_id = str(batch_id or "").strip()
        batch = self.ctx.repo.get("mixcut_batches", "batch_id", batch_id)
        if not batch:
            return Result.fail("BATCH_NOT_FOUND", "batch not found", {"batch_id": batch_id})
        product_id = str(batch.get("product_id") or "").strip()
        self.ctx.repo.update(
            "mixcut_batches",
            "batch_id",
            batch_id,
            {
                "final_qc_async_status": "running",
                "final_qc_async_error": "",
                "final_qc_async_updated_at": _now(),
            },
        )
        qc = FinalVideoQCSkill(self.ctx).check_batch(batch_id)
        if not qc.success:
            self.ctx.repo.update(
                "mixcut_batches",
                "batch_id",
                batch_id,
                {
                    "final_qc_async_status": "failed",
                    "final_qc_async_error": qc.error.message if qc.error else "final video qc failed",
                    "final_qc_async_updated_at": _now(),
                },
            )
            return qc

        output_sync = FeishuReviewSkill(self.ctx).sync_output_qc(batch_id)
        task_sync = FeishuReviewSkill(self.ctx).sync_task(product_id) if product_id else Result.ok({"status": "skipped", "reason": "missing_product_id"})
        self.ctx.repo.update(
            "mixcut_batches",
            "batch_id",
            batch_id,
            {
                "final_qc_async_status": "done",
                "final_qc_async_error": "",
                "final_qc_async_updated_at": _now(),
            },
        )
        return Result.ok({"batch_id": batch_id, "final_qc": qc.to_dict(), "sync_feishu": output_sync.to_dict(), "task_sync": task_sync.to_dict()})


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
