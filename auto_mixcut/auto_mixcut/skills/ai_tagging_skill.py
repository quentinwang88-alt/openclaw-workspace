from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext
from .hard_subtitle_policy import classify_text_overlay
from .llm_router_skill import LLMRouterSkill


class AITaggingSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self.router = LLMRouterSkill(ctx)

    def submit_batch(self, product_id: str, prompt_version: str = "v1.0", source_types: list[str] | None = None) -> Result:
        segments = _segments_for_source_types(self.ctx, product_id, source_types)
        segments = _limit_segments(segments)
        latest_tags = _latest_tags_by_segment(self.ctx, [str(segment.get("segment_id") or "") for segment in segments])
        segments = [segment for segment in segments if str(segment.get("segment_id") or "") not in latest_tags]
        batch_id = new_id("AIBATCH")
        self.ctx.repo.upsert(
            "ai_batches",
            "ai_batch_id",
            {
                "ai_batch_id": batch_id,
                "product_id": product_id,
                "batch_type": "segment_tagging",
                "status": "submitted",
                "total_segments": len(segments),
                "model_tier": "medium_vision",
                "prompt_version": prompt_version,
            },
        )
        return Result.ok({"ai_batch_id": batch_id, "total_segments": len(segments)})

    def poll_results(self, product_id: str, prompt_version: str = "v1.0", force: bool = False, source_types: list[str] | None = None) -> Result:
        segments = _segments_for_source_types(self.ctx, product_id, source_types)
        segments = _limit_segments(segments)
        completed = skipped = failed = 0
        segment_ids = [str(segment.get("segment_id") or "") for segment in segments if segment.get("segment_id")]
        latest_tags = _latest_tags_by_segment(self.ctx, segment_ids)
        if not force:
            skipped = sum(1 for segment in segments if str(segment.get("segment_id") or "") in latest_tags)
            segments = [segment for segment in segments if str(segment.get("segment_id") or "") not in latest_tags]
        frame_counts = _frame_counts_by_segment(self.ctx, [str(segment.get("segment_id") or "") for segment in segments if segment.get("segment_id")])
        max_workers = _tag_concurrency()
        indexed = list(enumerate(segments))
        total = len(indexed)
        started_at = time.monotonic()
        progress_every = _progress_every()
        timeout_sec = _total_timeout_sec()
        if max_workers <= 1 or len(indexed) <= 1:
            results = []
            for idx, segment in indexed:
                if timeout_sec and time.monotonic() - started_at > timeout_sec:
                    results.append({"status": "failed", "segment_id": segment["segment_id"], "error_code": "TAG_TOTAL_TIMEOUT"})
                    continue
                segment_id = str(segment.get("segment_id") or "")
                results.append(self._poll_segment(product_id, segment, idx, prompt_version, force, latest_tags.get(segment_id, {}), frame_counts.get(segment_id)).data)
                _emit_progress(product_id, len(results), total, started_at, progress_every)
        else:
            results_by_segment = {}
            pool = ThreadPoolExecutor(max_workers=max_workers)
            futures = {
                pool.submit(
                    self._poll_segment,
                    product_id,
                    segment,
                    idx,
                    prompt_version,
                    force,
                    latest_tags.get(str(segment.get("segment_id") or ""), {}),
                    frame_counts.get(str(segment.get("segment_id") or "")),
                ): segment["segment_id"]
                for idx, segment in indexed
            }
            try:
                seen = 0
                for future in as_completed(futures, timeout=timeout_sec or None):
                    segment_id = futures[future]
                    try:
                        res = future.result()
                        results_by_segment[segment_id] = res.data if res.success else {"status": "failed", "segment_id": segment_id, "error": res.to_dict()}
                    except Exception as exc:
                        results_by_segment[segment_id] = {"status": "failed", "segment_id": segment_id, "error": str(exc), "exception_type": type(exc).__name__}
                    seen += 1
                    _emit_progress(product_id, seen, total, started_at, progress_every)
            except FuturesTimeoutError:
                for future, segment_id in futures.items():
                    if segment_id not in results_by_segment:
                        future.cancel()
                        results_by_segment[segment_id] = {"status": "failed", "segment_id": segment_id, "error_code": "TAG_TOTAL_TIMEOUT", "timeout_seconds": timeout_sec}
                _emit_progress(product_id, total, total, started_at, progress_every)
            finally:
                pool.shutdown(wait=False, cancel_futures=True)
            results = [results_by_segment.get(segment["segment_id"], {"status": "failed", "segment_id": segment["segment_id"], "error": "missing worker result"}) for _, segment in indexed]
        for item in results:
            if item.get("status") == "completed":
                completed += 1
            elif item.get("status") == "skipped":
                skipped += 1
            else:
                failed += 1
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "AI_TAGGED"})
        quality = _material_quality_summary(self.ctx, [segment for _, segment in indexed])
        if _quality_alert_enabled() and quality.get("blocked"):
            self.ctx.repo.update(
                "content_tasks",
                "product_id",
                product_id,
                {
                    "pipeline_status": "BLOCKED",
                    "next_action": "NEED_BETTER_MATERIAL",
                    "last_error": "MATERIAL_QUALITY_TOO_LOW",
                    "current_bottleneck": "素材质量过低",
                    "capacity_note": quality.get("note"),
                },
            )
            return Result.fail(
                "MATERIAL_QUALITY_TOO_LOW",
                str(quality.get("note") or "tagged material quality too low"),
                {"completed_segments": completed, "skipped_segments": skipped, "failed_segments": failed, "concurrency": max_workers, "quality": quality},
            )
        return Result.ok({"completed_segments": completed, "skipped_segments": skipped, "failed_segments": failed, "concurrency": max_workers, "quality": quality})

    def retry_failed(self, product_id: str) -> Result:
        return self.poll_results(product_id)

    def _poll_segment(self, product_id: str, segment: dict, idx: int, prompt_version: str, force: bool, latest_tag: dict | None = None, frame_count: int | None = None) -> Result:
        if not force and (latest_tag if latest_tag is not None else _latest_tag(self.ctx, segment["segment_id"])):
            return Result.ok({"status": "skipped", "segment_id": segment["segment_id"], "reason": "tag_exists"})
        call = self.router.call(
            "segment_tagging_default",
            {"segment_id": segment["segment_id"], "index": idx, "prompt_version": prompt_version, "image_count": frame_count if frame_count is not None else _frame_count(self.ctx, segment["segment_id"])},
            product_id=product_id,
            segment_id=segment["segment_id"],
            asset_id=segment["asset_id"],
        )
        if not call.success:
            return Result.ok({"status": "failed", "segment_id": segment["segment_id"], "error": call.to_dict()})
        tag = call.data["response"]
        overlay = classify_text_overlay(tag)
        tag["text_overlay_risk"] = overlay["risk"]
        tag["text_language"] = tag.get("text_language") or overlay["language"]
        tag["text_overlay_reason"] = tag.get("text_overlay_reason") or overlay["reason"]
        review = _needs_review(segment, tag)
        tag["needs_human_review"] = bool(tag.get("needs_human_review") or review)
        self.ctx.repo.insert(
            "ai_tag_runs",
            {
                "tag_run_id": new_id("TAGRUN"),
                "segment_id": segment["segment_id"],
                "model_tier": call.data["route"]["model_tier"],
                "model_name": call.data["route"]["model_name"],
                "prompt_version": prompt_version,
                "run_type": "segment_tagging",
                "temperature": 0.0,
                "raw_response": tag,
                "parsed_success": 1,
            },
        )
        self.ctx.repo.insert(
            "segment_tags",
            {
                "segment_id": segment["segment_id"],
                "tag_source": "ai",
                "primary_shot_role": tag["primary_shot_role"],
                "secondary_roles_json": tag["secondary_roles"],
                "product_visibility": tag["product_visibility"],
                "hook_strength": tag["hook_strength"],
                "mixcut_usability": tag["mixcut_usability"],
                "risk_level": tag["risk_level"],
                "text_overlay_risk": tag["text_overlay_risk"],
                "text_language": tag["text_language"],
                "text_overlay_reason": tag["text_overlay_reason"],
                "confidence": tag["confidence"],
                "needs_human_review": int(tag["needs_human_review"]),
                "reason": tag["reason"],
            },
        )
        return Result.ok({"status": "completed", "segment_id": segment["segment_id"]})


def _frame_count(ctx: SkillContext, segment_id: str) -> int:
    return len(ctx.repo.list_where("segment_frames", "segment_id=?", (segment_id,)))


def _frame_counts_by_segment(ctx: SkillContext, segment_ids: list[str]) -> dict[str, int]:
    segment_ids = [str(item) for item in segment_ids if str(item or "").strip()]
    if not segment_ids:
        return {}
    counts: dict[str, int] = {}
    for chunk in _chunks(segment_ids, 200):
        placeholders = ",".join("?" for _ in chunk)
        rows = ctx.repo.list_where("segment_frames", f"segment_id IN ({placeholders})", tuple(chunk))
        for row in rows:
            segment_id = str(row.get("segment_id") or "")
            if segment_id:
                counts[segment_id] = counts.get(segment_id, 0) + 1
    return counts


def _latest_tag(ctx: SkillContext, segment_id: str) -> dict:
    rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment_id,))
    return rows[0] if rows else {}


def _latest_tags_by_segment(ctx: SkillContext, segment_ids: list[str]) -> dict[str, dict]:
    segment_ids = [str(item) for item in segment_ids if str(item or "").strip()]
    if not segment_ids:
        return {}
    latest: dict[str, dict] = {}
    for chunk in _chunks(segment_ids, 200):
        placeholders = ",".join("?" for _ in chunk)
        rows = ctx.repo.list_where("segment_tags", f"segment_id IN ({placeholders}) ORDER BY segment_id, id DESC", tuple(chunk))
        for row in rows:
            segment_id = str(row.get("segment_id") or "")
            if segment_id and segment_id not in latest:
                latest[segment_id] = row
    return latest


def _segments_for_source_types(ctx: SkillContext, product_id: str, source_types: list[str] | None = None) -> list[dict]:
    if not source_types:
        return ctx.repo.list_where("segments", "product_id=?", (product_id,))
    placeholders = ",".join("?" for _ in source_types)
    return ctx.repo.list_where(
        "segments",
        f"product_id=? AND source_type IN ({placeholders})",
        (product_id, *source_types),
    )


def _needs_review(segment, tag) -> bool:
    return (
        tag.get("confidence") == "low"
        or tag.get("risk_level") in {"medium", "high"}
        or segment.get("product_match_status") == "uncertain"
        or tag.get("mixcut_usability") in {"needs_processing", "no"}
    )


def _limit_segments(segments):
    limit = int(os.environ.get("AUTO_MIXCUT_TAG_LIMIT", "0") or "0")
    if limit > 0:
        return segments[:limit]
    return segments


def _tag_concurrency() -> int:
    try:
        return max(1, min(4, int(os.environ.get("AUTO_MIXCUT_TAG_CONCURRENCY", "2") or "2")))
    except ValueError:
        return 2


def _progress_every() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_TAG_PROGRESS_EVERY", "10") or "10"))
    except ValueError:
        return 10


def _total_timeout_sec() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_TAG_TOTAL_TIMEOUT_SEC", "0") or "0"))
    except ValueError:
        return 0


def _emit_progress(product_id: str, done: int, total: int, started_at: float, every: int) -> None:
    if every <= 0 or total <= 0:
        return
    if done < total and done % every != 0:
        return
    payload = {"event": "tag_poll_progress", "product_id": product_id, "completed": done, "total": total, "elapsed_sec": round(time.monotonic() - started_at, 1)}
    print(__import__("json").dumps(payload, ensure_ascii=False), file=sys.stderr, flush=True)


def _quality_alert_enabled() -> bool:
    return os.environ.get("AUTO_MIXCUT_TAG_QUALITY_ALERT", "1").strip().lower() not in {"0", "false", "no", "off"}


def _material_quality_summary(ctx: SkillContext, segments: list[dict]) -> dict:
    segment_ids = [str(segment.get("segment_id") or "") for segment in segments if segment.get("segment_id")]
    if not segment_ids:
        return {"tagged": 0, "blocked": False}
    tags = list(_latest_tags_by_segment(ctx, segment_ids).values())
    total = len(tags)
    unusable = sum(1 for tag in tags if tag.get("primary_shot_role") == "unusable" or tag.get("mixcut_usability") == "no")
    core = sum(1 for tag in tags if {tag.get("primary_shot_role"), *(tag.get("secondary_roles_json") or [])}.intersection({"hero", "detail", "result"}))
    unusable_ratio = unusable / total if total else 0
    threshold = _quality_unusable_threshold()
    min_total = _quality_min_total()
    blocked = total >= min_total and unusable_ratio > threshold
    note = f"素材质量过低: 已打标={total}, unusable={unusable}({unusable_ratio:.0%}), 核心角色命中={core}"
    return {"tagged": total, "unusable": unusable, "unusable_ratio": round(unusable_ratio, 4), "core_role_hits": core, "threshold": threshold, "blocked": blocked, "note": note if blocked else ""}


def _quality_unusable_threshold() -> float:
    try:
        return max(0.0, min(1.0, float(os.environ.get("AUTO_MIXCUT_UNUSABLE_ALERT_RATIO", "0.8") or "0.8")))
    except ValueError:
        return 0.8


def _quality_min_total() -> int:
    try:
        return max(1, int(os.environ.get("AUTO_MIXCUT_QUALITY_ALERT_MIN_TAGGED", "20") or "20"))
    except ValueError:
        return 20


def _chunks(items: list[str], size: int):
    for idx in range(0, len(items), max(1, size)):
        yield items[idx : idx + size]
