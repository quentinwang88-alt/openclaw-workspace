from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.storage_paths import require_oss_object_path
from auto_mixcut.core.result import Result

from .context import SkillContext
from .llm_router_skill import LLMRouterSkill


class FinalVideoQCSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_batch(self, batch_id: str) -> Result:
        outputs = self.ctx.repo.list_where(
            "outputs",
            "batch_id=? AND machine_quality_status IN ('publish_ready','needs_review','passed','passed_with_warning')",
            (batch_id,),
        )
        results = []
        for output in outputs:
            res = self.check_output(output["output_id"])
            if not res.success:
                return res
            results.append(res.data)
        return Result.ok({"batch_id": batch_id, "results": results})

    def check_output(self, output_id: str) -> Result:
        output = self.ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            return Result.fail("OUTPUT_NOT_FOUND", "output not found", {"output_id": output_id})
        video_path = self._output_path(output)
        if not video_path:
            return Result.fail("OUTPUT_FILE_NOT_FOUND", "output video file missing", {"output_id": output_id})
        frames = self._sample_frames(output, video_path)
        if not frames.success:
            return frames
        payload = {
            "output_id": output_id,
            "product_id": output.get("product_id"),
            "template_id": output.get("template_id"),
            "duration_ms": output.get("duration_ms"),
            "bgm_plan": output.get("bgm_plan_json") or {},
            "image_paths": frames.data["image_paths"],
            "image_count": len(frames.data["image_paths"]),
            "prompt_version": "final_video_qc_v1",
        }
        call = LLMRouterSkill(self.ctx).call("final_video_qc", payload, product_id=output.get("product_id") or "")
        if not call.success:
            return call
        qc = call.data["response"]
        status = qc.get("final_qc_status") or "needs_review"
        updates = {"final_qc_json": qc}
        if status == "fail":
            updates["machine_quality_status"] = "draft_only"
        elif status == "needs_review":
            updates["machine_quality_status"] = "needs_review"
        elif status == "pass":
            updates["machine_quality_status"] = "publish_ready"
        self.ctx.repo.update("outputs", "output_id", output_id, updates)
        return Result.ok({"output_id": output_id, "final_qc_status": status, "final_qc": qc})

    def _output_path(self, output: dict) -> Path | None:
        object_id = output.get("bgm_output_oss_object_id") or output.get("output_oss_object_id")
        return require_oss_object_path(self.ctx, object_id, "final_qc_outputs")

    def _sample_frames(self, output: dict, video_path: Path) -> Result:
        target_dir = self.ctx.settings.temp_root / "final_qc_frames" / str(output.get("product_id") or "unknown") / output["output_id"]
        target_dir.mkdir(parents=True, exist_ok=True)
        duration_sec = self._duration_sec(video_path, output)
        timestamps = [0.5, 1.5, duration_sec * 0.35, duration_sec * 0.60, max(0.5, duration_sec - 1.0)]
        image_paths = []
        for index, ts in enumerate(timestamps, start=1):
            frame = target_dir / f"frame_{index:02d}.jpg"
            if self.ctx.ffmpeg.mock:
                frame.write_bytes(b"\xff\xd8\xff\xe0" + b"mock-final-qc-frame" * 128 + b"\xff\xd9")
            else:
                res = self.ctx.ffmpeg.run(
                    ["-y", "-ss", f"{ts:.2f}", "-i", str(video_path), "-frames:v", "1", "-q:v", "2", str(frame)],
                    "FINAL_QC_FRAME_FAILED",
                )
                if not res.success:
                    continue
            if frame.exists() and frame.stat().st_size > 1024:
                image_paths.append(str(frame))
        if not image_paths:
            return Result.fail("FINAL_QC_NO_FRAMES", "no frames sampled for final video QC", {"output_id": output["output_id"]})
        return Result.ok({"image_paths": image_paths})

    def _duration_sec(self, video_path: Path, output: dict) -> float:
        probed = self.ctx.ffmpeg.probe(video_path)
        if probed.success:
            return max(1.0, float(probed.data.get("duration_ms") or 0) / 1000)
        return max(1.0, float(output.get("duration_ms") or 15000) / 1000)
