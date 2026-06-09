from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.storage_paths import require_oss_object_path
from auto_mixcut.core.result import Result

from .context import SkillContext
from .bgm_usage_skill import BgmUsageSkill
from .feishu_review_skill import sync_product_task_best_effort
from .llm_router_skill import LLMRouterSkill
from .usage_counter_skill import is_good_rendered_output


class FinalVideoQCSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_batch(self, batch_id: str) -> Result:
        outputs = self.ctx.repo.list_where(
            "outputs",
            "batch_id=? AND final_qc_json IS NULL AND machine_quality_status IN ('pending','publish_ready','needs_review','passed','passed_with_warning')",
            (batch_id,),
        )
        results = []
        for output in outputs:
            res = self.check_output(output["output_id"])
            if not res.success:
                return res
            results.append(res.data)
        task_refresh = _refresh_task_actual_count(self.ctx, batch_id)
        return Result.ok({"batch_id": batch_id, "results": results, "task_refresh": task_refresh})

    def check_output(self, output_id: str) -> Result:
        output = self.ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            return Result.fail("OUTPUT_NOT_FOUND", "output not found", {"output_id": output_id})
        if output.get("final_qc_json"):
            qc = _normalize_final_qc_response(output.get("final_qc_json"))
            return Result.ok({"output_id": output_id, "final_qc_status": qc.get("final_qc_status", "needs_review"), "final_qc": qc, "skipped": True, "reason": "final_qc_exists"})
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
            "prompt_version": "final_video_qc_v2_json",
            "prompt_text": _final_qc_prompt(repair=False),
        }
        router = LLMRouterSkill(self.ctx)
        call = router.call("final_video_qc", payload, product_id=output.get("product_id") or "", output_id=output_id)
        if not call.success:
            return call
        qc = _normalize_final_qc_response(call.data["response"])
        if not _is_structured_final_qc(qc):
            repair_payload = {**payload, "prompt_version": "final_video_qc_v2_json_repair", "prompt_text": _final_qc_prompt(repair=True), "previous_response": call.data["response"]}
            repair_call = router.call("final_video_qc", repair_payload, product_id=output.get("product_id") or "", output_id=output_id)
            if repair_call.success:
                repaired_qc = _normalize_final_qc_response(repair_call.data["response"])
                if _is_structured_final_qc(repaired_qc):
                    qc = repaired_qc
        status = qc.get("final_qc_status") or "needs_review"
        updates = {"final_qc_json": qc}
        if status == "fail":
            updates["machine_quality_status"] = "draft_only"
        elif status == "needs_review":
            updates["machine_quality_status"] = "needs_review"
        elif status == "pass":
            updates["machine_quality_status"] = "publish_ready"
        self.ctx.repo.update("outputs", "output_id", output_id, updates)
        BgmUsageSkill(self.ctx).record_output_feedback(output_id, _bgm_feedback_status(status), "final_video_qc")
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


def _normalize_final_qc_response(response) -> dict:
    if isinstance(response, dict):
        data = dict(response)
    else:
        data = {"text": str(response or "")}
    text = str(data.get("text") or data.get("reason") or "")
    status = str(data.get("final_qc_status") or data.get("status") or "").strip().lower()
    if status in {"pass", "passed", "publish_ready"}:
        normalized_status = "pass"
    elif status in {"fail", "failed", "reject", "rejected", "draft_only"}:
        normalized_status = "fail"
    elif status in {"needs_review", "review", "uncertain"}:
        normalized_status = "needs_review"
    else:
        normalized_status = _infer_status_from_text(text)
    return {
        "final_qc_status": normalized_status,
        "pass_reasons": _as_list(data.get("pass_reasons")),
        "fail_reasons": _as_list(data.get("fail_reasons")),
        "review_reasons": _as_list(data.get("review_reasons")) or ([] if normalized_status != "needs_review" else ["model_response_unstructured"]),
        "subtitle_issue": _as_bool(data.get("subtitle_issue")),
        "product_match_issue": _as_bool(data.get("product_match_issue")),
        "audio_issue": _as_bool(data.get("audio_issue")),
        "raw_text": text,
        "raw_response": data,
    }


def _is_structured_final_qc(qc: dict) -> bool:
    raw = qc.get("raw_response") or {}
    if not isinstance(raw, dict):
        return False
    if set(raw.keys()) <= {"text"}:
        return False
    return any(qc.get(key) not in (None, "", []) for key in ["pass_reasons", "fail_reasons", "review_reasons"]) or any(
        key in raw for key in ["final_qc_status", "status", "subtitle_issue", "product_match_issue", "audio_issue"]
    )


def _final_qc_prompt(repair: bool = False) -> str:
    prefix = "上一次回答不是JSON,请重新严格按JSON返回。" if repair else ""
    return (
        f"{prefix}你是短视频混剪成片质检器。只判断画面、商品一致性、字幕/水印、音频和剪辑可用性,不要写商品介绍文案。"
        "必须只返回一个JSON对象,不要Markdown,不要解释。JSON字段固定如下:"
        '{"final_qc_status":"pass|needs_review|fail",'
        '"pass_reasons":["通过原因"],'
        '"fail_reasons":["失败原因"],'
        '"review_reasons":["需人工复核原因"],'
        '"subtitle_issue":false,'
        '"product_match_issue":false,'
        '"audio_issue":false}'
        "判定规则:明显错品/严重水印/大面积遮挡/音频缺失为fail;轻微不确定为needs_review;画面商品清晰且无明显问题为pass。"
    )


def _infer_status_from_text(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["不通过", "不可用", "废弃", "fail", "failed", "reject", "rejected", "严重"]):
        return "fail"
    if any(token in lowered for token in ["通过", "可发布", "可使用", "pass", "passed", "publish"]):
        return "pass"
    return "needs_review"


def _as_list(value) -> list:
    if value in (None, "", []):
        return []
    if isinstance(value, list):
        return value
    return [str(value)]


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "是", "有"}


def _bgm_feedback_status(final_qc_status: str) -> str:
    if final_qc_status == "pass":
        return "final_qc_pass"
    if final_qc_status == "fail":
        return "final_qc_fail"
    return "final_qc_needs_review"


def _refresh_task_actual_count(ctx: SkillContext, batch_id: str) -> dict:
    batch = ctx.repo.get("mixcut_batches", "batch_id", batch_id) or {}
    product_id = str(batch.get("product_id") or "").strip()
    if not product_id:
        return {"product_id": product_id, "actual_variant_count": 0, "updated": False}
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    actual = sum(1 for output in outputs if is_good_rendered_output(output))
    tasks = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    if not tasks:
        return {"product_id": product_id, "actual_variant_count": actual, "updated": False, "reason": "task_not_found"}
    ctx.repo.update("content_tasks", "task_id", tasks[0]["task_id"], {"actual_variant_count": actual})
    task_sync = sync_product_task_best_effort(ctx, product_id)
    return {"product_id": product_id, "actual_variant_count": actual, "updated": True, "task_sync": task_sync}
