from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext


class FrameSampleSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def sample_product(self, product_id: str, force: bool = False) -> Result:
        segments = self.ctx.repo.list_where("segments", "product_id=? AND segment_status IN ('created','qc_passed','qc_failed')", (product_id,))
        max_workers = _frame_concurrency()
        if max_workers <= 1 or len(segments) <= 1:
            results = [self.sample_segment(s["segment_id"], force=force).to_dict() for s in segments]
        else:
            results_by_id = {}
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(self.sample_segment, s["segment_id"], force): s["segment_id"] for s in segments}
                for future in as_completed(futures):
                    segment_id = futures[future]
                    try:
                        results_by_id[segment_id] = future.result().to_dict()
                    except Exception as exc:
                        results_by_id[segment_id] = Result.fail("FRAME_SAMPLE_EXCEPTION", str(exc), {"segment_id": segment_id, "exception_type": type(exc).__name__}).to_dict()
            results = [results_by_id[s["segment_id"]] for s in segments]
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "FRAMES_SAMPLED"})
        return Result.ok({"count": len(results), "results": results})

    def sample_segment(self, segment_id: str, force: bool = False) -> Result:
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        if not segment:
            return Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": segment_id})
        count = 9 if segment["source_type"] == "ai_generated" else 4
        existing = self.ctx.repo.list_where("segment_frames", "segment_id=? ORDER BY frame_index", (segment_id,))
        if len(existing) >= count and not force:
            return Result.ok({"segment_id": segment_id, "frames": [row["frame_id"] for row in existing], "skipped": True, "reason": "frames_exist"})
        if existing and force and hasattr(self.ctx.repo, "delete_where"):
            deleted = self.ctx.repo.delete_where("segment_frames", "segment_id=?", (segment_id,))
            if not deleted.success:
                return deleted
        product = self.ctx.repo.get("products", "product_id", segment["product_id"]) or {}
        frames = []
        for idx in range(1, count + 1):
            frame_id = new_id("FRAME")
            local = self.ctx.settings.temp_root / "frames" / segment["product_id"] / segment_id / f"frame_{idx:03d}.jpg"
            local.parent.mkdir(parents=True, exist_ok=True)
            timestamp = int((segment["duration_ms"] or 3000) * idx / (count + 1))
            if self.ctx.ffmpeg.mock:
                local.write_bytes(b"\xff\xd8\xff\xe0mock-jpeg\xff\xd9")
            else:
                source = _segment_path(self.ctx, segment)
                if not source:
                    return Result.fail("FRAME_SAMPLE_FAILED", "segment source object not found", {"segment_id": segment_id})
                sampled = self.ctx.ffmpeg.run(
                    ["-y", "-ss", f"{timestamp / 1000:.3f}", "-i", str(source), "-frames:v", "1", "-q:v", "2", str(local)],
                    "FRAME_SAMPLE_FAILED",
                )
                if not sampled.success:
                    return sampled
            object_key = f"auto_mixcut/frames/{product.get('market','NA')}/{product.get('category','uncategorized')}/{segment['product_id']}/{segment_id}/frame_{idx:03d}.jpg"
            upload = self.ctx.oss.upload(local, object_key)
            if not upload.success:
                return upload
            oss_row = dict(upload.data, object_type="frame", mime_type="image/jpeg")
            self.ctx.repo.upsert("oss_objects", "object_id", oss_row)
            row = {"frame_id": frame_id, "segment_id": segment_id, "frame_index": idx, "timestamp_ms": timestamp, "oss_object_id": oss_row["object_id"]}
            self.ctx.repo.upsert("segment_frames", "frame_id", row)
            frames.append(frame_id)
        return Result.ok({"segment_id": segment_id, "frames": frames})


def _segment_path(ctx: SkillContext, segment: dict) -> Path | None:
    return require_oss_object_path(ctx, segment.get("segment_oss_object_id"), "frames_source")


def _frame_concurrency() -> int:
    try:
        return max(1, min(8, int(os.environ.get("AUTO_MIXCUT_FRAME_CONCURRENCY", "3") or "3")))
    except ValueError:
        return 3
