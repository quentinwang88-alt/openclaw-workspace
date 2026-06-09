#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.agent.orchestrator import AutoMixcutOrchestratorAgent  # noqa: E402
from auto_mixcut.cli import _top_up  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.core.result import Result  # noqa: E402
from auto_mixcut.skills.capacity_counter_skill import CapacityCounterSkill  # noqa: E402
from auto_mixcut.skills.ai_anchor_check_skill import AIAnchorCheckSkill  # noqa: E402
from auto_mixcut.skills.ai_generated_consistency_skill import AIGeneratedConsistencySkill  # noqa: E402
from auto_mixcut.skills.ai_generation_qc_skill import AIGenerationQCSkill  # noqa: E402
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill  # noqa: E402
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill  # noqa: E402
from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill  # noqa: E402
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill  # noqa: E402
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402
from auto_mixcut.skills.segment_fingerprint_skill import SegmentFingerprintSkill  # noqa: E402
from auto_mixcut.skills.segment_skill import SegmentSkill  # noqa: E402
from auto_mixcut.skills.usage_counter_skill import is_good_rendered_output  # noqa: E402
from auto_mixcut.skills.watermark_detect_skill import WatermarkDetectSkill  # noqa: E402
from auto_mixcut.skills.watermark_process_skill import WatermarkProcessSkill  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one guarded auto_mixcut pass for a product.")
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--target", type=int)
    parser.add_argument("--name", default="")
    parser.add_argument("--market", default="")
    parser.add_argument("--category", default="")
    parser.add_argument("--max-rounds", type=int, default=2)
    parser.add_argument("--skip-upload-sync", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps(init.to_dict(), ensure_ascii=False, indent=2, default=str))
        return 1

    res = run_guard_pass(
        ctx,
        product_id=args.product_id,
        target=args.target,
        name=args.name,
        market=args.market,
        category=args.category,
        max_rounds=args.max_rounds,
        process_uploads=not args.skip_upload_sync,
    )
    print(json.dumps(res.to_dict(), ensure_ascii=False, indent=2, default=str))
    return 0 if res.success else 1


def run_guard_pass(ctx, product_id: str, target: int | None = None, name: str = "", market: str = "", category: str = "", max_rounds: int = 2, process_uploads: bool = True) -> Result:
    product_id = str(product_id or "").strip()
    if not product_id:
        return Result.fail("PRODUCT_ID_REQUIRED", "product_id is required")

    task = _latest_task(ctx, product_id)
    product = ctx.repo.get("products", "product_id", product_id)
    if not task:
        if not (name and market and category and target):
            detail = _status_detail(ctx, product_id, target)
            _safe_guard_update(ctx, product_id, "BLOCKED", "NEED_CREATE_TASK_FIELDS", "缺少商品名/市场/类目/目标数量，无法从零创建任务", detail)
            return Result.fail("TASK_NOT_FOUND", "task not found; provide --name --market --category --target to create it", detail)
        created = RDSRepositorySkill(ctx).create_product_task(product_id, name, market, category, int(target))
        if not created.success:
            _safe_guard_update(ctx, product_id, "ERROR", "CREATE_TASK_FAILED", created.error.message if created.error else "create task failed", created.to_dict())
            return created
        task = _latest_task(ctx, product_id)
        product = ctx.repo.get("products", "product_id", product_id)

    if target:
        ctx.repo.update("content_tasks", "task_id", task["task_id"], {"requested_variant_count": int(target)})
    else:
        target = int(task.get("requested_variant_count") or task.get("allowed_variant_count") or 0)

    initial_detail = _status_detail(ctx, product_id, target)
    if int(initial_detail.get("remaining_count") or 0) <= 0:
        _safe_guard_update(ctx, product_id, "DONE", "NONE", "", initial_detail)
        return Result.ok({"product_id": product_id, "pipeline_status": "DONE", "next_action": "NONE", "detail": initial_detail})

    _safe_guard_update(ctx, product_id, "RUNNING", "GUARD_PASS_STARTED", "", initial_detail)

    anchor = _ensure_anchor_confirmed(ctx, product_id)
    if not anchor.success:
        detail = {**_status_detail(ctx, product_id, target), "anchor": anchor.to_dict()}
        status, action = _classify_failure(anchor)
        _safe_guard_update(ctx, product_id, status, action, anchor.error.message if anchor.error else "", detail)
        return anchor

    upload_sync = None
    if process_uploads:
        upload_sync = _process_uploads(product_id)

    assets = ctx.repo.list_where("assets", "product_id=?", (product_id,))
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    if not assets:
        detail = {**_status_detail(ctx, product_id, target), "upload_sync": upload_sync}
        _safe_guard_update(ctx, product_id, "BLOCKED", "NEED_MATERIAL_UPLOAD", "没有可处理素材，请先上传素材或等待AI回流", detail)
        return Result.ok({"product_id": product_id, "pipeline_status": "BLOCKED", "next_action": "NEED_MATERIAL_UPLOAD", "detail": detail})

    stale_segments = _stale_segment_summary(ctx, segments)
    stale_ai_segments = _stale_segment_summary(ctx, [s for s in segments if s.get("source_type") == "ai_generated"])
    stale_repair_source_types = _stale_repair_source_types(ctx, segments)
    if not segments:
        batch = AutoMixcutOrchestratorAgent(ctx).run_product(product_id, requested_count=target, auto_confirm_anchor=True)
        if not batch.success:
            status, action = _classify_failure(batch)
            detail = {**_status_detail(ctx, product_id, target), "upload_sync": upload_sync, "stale_segments": stale_segments, "stale_ai_segments": stale_ai_segments, "stale_repair_source_types": stale_repair_source_types, "batch": batch.to_dict()}
            _safe_guard_update(ctx, product_id, status, action, batch.error.message if batch.error else "", detail)
            return batch
    elif stale_repair_source_types:
        repaired = _run_incremental_postprocess(ctx, product_id, source_types=stale_repair_source_types)
        if not repaired.success:
            status, action = _classify_failure(repaired)
            detail = {
                **_status_detail(ctx, product_id, target),
                "upload_sync": upload_sync,
                "stale_segments": stale_segments,
                "stale_ai_segments": stale_ai_segments,
                "stale_repair_source_types": stale_repair_source_types,
                "incremental_postprocess": repaired.to_dict(),
            }
            _safe_guard_update(ctx, product_id, status, action, repaired.error.message if repaired.error else "", detail)
            return repaired
        refreshed_segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
        refreshed_stale = _stale_segment_summary(ctx, refreshed_segments)
        if refreshed_stale["stale_count"]:
            detail = {
                **_status_detail(ctx, product_id, target),
                "upload_sync": upload_sync,
                "stale_segments": refreshed_stale,
                "stale_ai_segments": _stale_segment_summary(ctx, [s for s in refreshed_segments if s.get("source_type") == "ai_generated"]),
                "stale_repair_source_types": _stale_repair_source_types(ctx, refreshed_segments),
                "incremental_postprocess": repaired.to_dict(),
            }
            _safe_guard_update(ctx, product_id, "READY_TO_CONTINUE", "RUN_GUARD_AGAIN", "stale segments repaired in bounded batch; run guard again", detail)
            return Result.ok({"product_id": product_id, "pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "detail": detail})

    top_up = _top_up(ctx, product_id, target, max_rounds=max_rounds)
    final = _status_after_top_up(ctx, product_id, target, top_up)
    detail = {
        **_status_detail(ctx, product_id, target),
        "upload_sync": upload_sync,
        "stale_segments": stale_segments,
        "stale_ai_segments": stale_ai_segments,
        "stale_repair_source_types": stale_repair_source_types,
        "top_up": top_up.to_dict(),
        "final": final,
    }
    _safe_guard_update(ctx, product_id, final["pipeline_status"], final["next_action"], final.get("last_error") or "", detail, final.get("last_batch_id") or "")
    return Result.ok({"product_id": product_id, **final, "detail": detail})


def _process_uploads(product_id: str) -> dict[str, Any]:
    try:
        from scripts.process_asset_uploads import AutoMixcutFeishuClient, build_context, process_record, text

        ctx = build_context()
        client = AutoMixcutFeishuClient("商品素材上传表")
        results = []
        for record in client.list_records(limit=None):
            if text((record.fields or {}).get("商品ID")) != product_id:
                continue
            results.append(process_record(ctx, client, record, dry_run=False))
        return {"status": "ok", "results": results}
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


def _ensure_anchor_confirmed(ctx, product_id: str) -> Result:
    product = ctx.repo.get("products", "product_id", product_id)
    if not product:
        return Result.fail("PRODUCT_NOT_FOUND", "product not found", {"product_id": product_id})
    if product.get("anchor_status") == "confirmed":
        return Result.ok({"product_id": product_id, "anchor_status": "confirmed", "skipped": True})
    skill = ProductAnchorSkill(ctx)
    drafted = skill.draft_anchor(product_id)
    if not drafted.success:
        return drafted
    confirmed = skill.confirm_anchor(product_id, "guard_auto")
    if not confirmed.success:
        return confirmed
    return Result.ok({"product_id": product_id, "anchor_status": "confirmed", "draft": drafted.data, "confirm": confirmed.data})


def _stale_segment_summary(ctx, segments: list[dict[str, Any]]) -> dict[str, Any]:
    stale = []
    for segment in segments:
        reasons = _stale_segment_reasons(ctx, segment)
        if reasons:
            stale.append({"segment_id": segment.get("segment_id"), "source_type": segment.get("source_type"), "reasons": reasons})
    return {
        "segment_count": len(segments),
        "stale_count": len(stale),
        "sample": stale[:20],
    }


def _stale_repair_source_types(ctx, segments: list[dict[str, Any]]) -> list[str]:
    source_types = set()
    for segment in segments:
        if not _stale_segment_reasons(ctx, segment):
            continue
        source_type = str(segment.get("source_type") or "").strip()
        if source_type:
            source_types.add(source_type)
    return sorted(source_types)


def _stale_retag_segment_ids(ctx, product_id: str, source_types: list[str]) -> list[str]:
    segment_ids: list[str] = []
    for segment in _segments_for_source_types(ctx, product_id, source_types):
        reasons = set(_stale_segment_reasons(ctx, segment))
        if reasons.intersection({"frames_missing", "tag_missing"}):
            segment_ids.append(str(segment.get("segment_id") or ""))
    segment_ids = [item for item in segment_ids if item]
    limit = _guard_retag_limit()
    if limit > 0:
        return segment_ids[:limit]
    return segment_ids


def _guard_retag_limit() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_GUARD_RETAG_LIMIT", "20") or "20"))
    except ValueError:
        return 20


def _guard_frame_limit() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_GUARD_FRAME_LIMIT", "20") or "20"))
    except ValueError:
        return 20


def _guard_frame_timeout() -> int:
    try:
        return max(10, int(os.environ.get("AUTO_MIXCUT_GUARD_FRAME_TIMEOUT", "60") or "60"))
    except ValueError:
        return 60


def _guard_tag_timeout() -> int:
    try:
        return max(30, int(os.environ.get("AUTO_MIXCUT_GUARD_TAG_TIMEOUT", "180") or "180"))
    except ValueError:
        return 180


def _stale_segment_reasons(ctx, segment: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    segment_id = str(segment.get("segment_id") or "")
    if not segment_id:
        return ["missing_segment_id"]
    if not _has_frames(ctx, segment_id):
        reasons.append("frames_missing")
    if not segment.get("visual_phash"):
        reasons.append("visual_phash_missing")
    if not _has_tag(ctx, segment_id):
        reasons.append("tag_missing")
    if segment.get("source_type") == "ai_generated":
        if str(segment.get("segment_status") or "") in {"", "created"}:
            reasons.append("ai_qc_missing")
        if not segment.get("frame_consistency_status"):
            reasons.append("ai_consistency_missing")
        if str(segment.get("segment_status") or "") == "qc_passed" and not segment.get("anchor_match_level"):
            reasons.append("ai_anchor_check_missing")
    if not segment.get("effective_roles_updated_at"):
        reasons.append("effective_roles_missing")
    return reasons


def _has_frames(ctx, segment_id: str) -> bool:
    rows = ctx.repo.list_where("segment_frames", "segment_id=? LIMIT 1", (segment_id,))
    return bool(rows)


def _has_tag(ctx, segment_id: str) -> bool:
    rows = ctx.repo.list_where("segment_tags", "segment_id=? LIMIT 1", (segment_id,))
    return bool(rows)


def _status_after_top_up(ctx, product_id: str, target: int, top_up: Result) -> dict[str, Any]:
    detail = _status_detail(ctx, product_id, target)
    if not top_up.success:
        status, action = _classify_failure(top_up)
        return {**detail, "pipeline_status": status, "next_action": action, "last_error": top_up.error.message if top_up.error else "top-up failed"}
    data = top_up.data or {}
    stop = str(data.get("stop_reason") or "")
    target_remaining = int((data.get("final") or {}).get("target_remaining_variant_count") or detail.get("target_remaining_variant_count") or 0)
    batch_ids = data.get("batch_ids") or []
    if target_remaining <= 0 or stop in {"target_already_filled", "target_filled"}:
        return {**detail, "pipeline_status": "DONE", "next_action": "NONE", "last_error": "", "last_batch_id": batch_ids[-1] if batch_ids else ""}
    if _top_up_created_ai_supplement(data):
        return {**detail, "pipeline_status": "WAITING_AI_RETURN", "next_action": "WAIT_AI_SEGMENT_RETURN", "last_error": "", "last_batch_id": batch_ids[-1] if batch_ids else ""}
    if stop in {"render_plan_empty", "no_material_pool_capacity", "no_material_pool_capacity_after_round"}:
        return {**detail, "pipeline_status": "BLOCKED", "next_action": "NEED_MORE_MATERIAL_OR_AI_SUPPLEMENT", "last_error": stop, "last_batch_id": batch_ids[-1] if batch_ids else ""}
    if batch_ids:
        return {**detail, "pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "last_error": "", "last_batch_id": batch_ids[-1]}
    return {**detail, "pipeline_status": "BLOCKED", "next_action": "CHECK_PIPELINE_LOG", "last_error": stop or "unknown_stop", "last_batch_id": ""}


def _top_up_created_ai_supplement(data: dict[str, Any]) -> bool:
    for round_item in data.get("rounds") or []:
        supplement = ((round_item.get("steps") or {}).get("ai_supplement_workbench") or {})
        if not supplement:
            continue
        if not supplement.get("success", True):
            continue
        payload = supplement.get("data") or {}
        if payload.get("skipped"):
            continue
        workbench = payload.get("workbench") or {}
        if workbench.get("created") or payload.get("final_task_sync"):
            return True
    return False


def _run_incremental_postprocess(ctx, product_id: str, source_types: list[str] | None = None) -> Result:
    """Repair stale material metadata without creating a full render batch."""
    steps = []
    source_types = [str(item) for item in (source_types or []) if str(item or "").strip()]
    if not source_types:
        source_types = sorted({str(s.get("source_type") or "") for s in ctx.repo.list_where("segments", "product_id=?", (product_id,)) if str(s.get("source_type") or "").strip()})
    retag_segment_ids = _stale_retag_segment_ids(ctx, product_id, source_types)
    includes_ai_generated = "ai_generated" in set(source_types)
    for name, fn in [
        ("probe", lambda: MediaProbeSkill(ctx).probe_product(product_id, source_types=source_types)),
        ("watermark", lambda: WatermarkDetectSkill(ctx).check_product(product_id, source_types=source_types)),
        ("watermark_process", lambda: WatermarkProcessSkill(ctx).process_product(product_id, source_types=source_types)),
        ("segment", lambda: SegmentSkill(ctx).segment_product(product_id, source_types=source_types)),
        ("frames", lambda: _sample_missing_frames(ctx, product_id, source_types=source_types)),
        ("fingerprint", lambda: _fingerprint_missing(ctx, product_id, source_types=source_types)),
        ("tag_submit", lambda: _submit_missing_tags(ctx, product_id, source_types=source_types)),
        ("tag_poll", lambda: _poll_missing_tags(ctx, product_id, source_types=source_types, force_segment_ids=retag_segment_ids)),
        ("ai_generation_qc", lambda: AIGenerationQCSkill(ctx).check_product(product_id) if includes_ai_generated else Result.ok({"skipped": True, "reason": "no_ai_generated_source"})),
        ("consistency", lambda: AIGeneratedConsistencySkill(ctx).check_product(product_id) if includes_ai_generated else Result.ok({"skipped": True, "reason": "no_ai_generated_source"})),
        ("ai_anchor_check", lambda: AIAnchorCheckSkill(ctx).check_product(product_id) if includes_ai_generated else Result.ok({"skipped": True, "reason": "no_ai_generated_source"})),
        ("effective_roles", lambda: EffectiveRoleSkill(ctx).compute_product(product_id, source_types=source_types)),
    ]:
        res = fn()
        steps.append({"step": name, **res.to_dict()})
        if not res.success:
            return Result.fail(
                res.error.code if res.error else "INCREMENTAL_POSTPROCESS_FAILED",
                res.error.message if res.error else f"incremental postprocess failed at {name}",
                {"product_id": product_id, "stage": name, "steps": steps},
            )
    return Result.ok({"product_id": product_id, "source_types": source_types, "force_retag_segment_ids": retag_segment_ids, "steps": steps})


def _segments_for_source_types(ctx, product_id: str, source_types: list[str]) -> list[dict[str, Any]]:
    if not source_types:
        return ctx.repo.list_where("segments", "product_id=?", (product_id,))
    placeholders = ",".join("?" for _ in source_types)
    return ctx.repo.list_where(
        "segments",
        f"product_id=? AND source_type IN ({placeholders})",
        (product_id, *source_types),
    )


def _sample_missing_frames(ctx, product_id: str, source_types: list[str]) -> Result:
    results = []
    skill = FrameSampleSkill(ctx)
    sampled_or_attempted = 0
    limit = _guard_frame_limit()
    timeout_seconds = _guard_frame_timeout()
    for segment in _segments_for_source_types(ctx, product_id, source_types):
        segment_id = str(segment.get("segment_id") or "")
        expected = 9 if segment.get("source_type") == "ai_generated" else 4
        if len(ctx.repo.list_where("segment_frames", "segment_id=? LIMIT ?", (segment_id, expected))) >= expected:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "frames_exist"})
            continue
        if limit > 0 and sampled_or_attempted >= limit:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "outside_frame_repair_batch"})
            continue
        sampled_or_attempted += 1
        try:
            with _guard_timeout(timeout_seconds):
                res = skill.sample_segment(segment_id)
        except _GuardTimeout:
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FRAME_SAMPLE_TIMEOUT", "timeout_seconds": timeout_seconds})
            continue
        except Exception as exc:
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FRAME_SAMPLE_EXCEPTION", "error": str(exc)})
            continue
        results.append(res.to_dict())
        if not res.success:
            results[-1]["status"] = "warning"
            continue
    warnings = [item for item in results if item.get("status") == "warning"]
    return Result.ok({"count": len(results), "attempted_count": sampled_or_attempted, "warning_count": len(warnings), "results": results, "source_types": source_types})


def _fingerprint_missing(ctx, product_id: str, source_types: list[str]) -> Result:
    results = []
    skill = SegmentFingerprintSkill(ctx)
    timeout_seconds = max(5, int(os.environ.get("AUTO_MIXCUT_GUARD_FINGERPRINT_TIMEOUT", "45") or "45"))
    for segment in _segments_for_source_types(ctx, product_id, source_types):
        segment_id = str(segment.get("segment_id") or "")
        if segment.get("visual_phash"):
            results.append({"segment_id": segment_id, "skipped": True, "reason": "fingerprint_exists"})
            continue
        try:
            with _guard_timeout(timeout_seconds):
                res = skill.fingerprint_segment(segment_id)
        except _GuardTimeout:
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FINGERPRINT_TIMEOUT", "timeout_seconds": timeout_seconds})
            continue
        except Exception as exc:
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FINGERPRINT_EXCEPTION", "error": str(exc)})
            continue
        results.append(res.to_dict())
        if not res.success:
            results[-1]["status"] = "warning"
            continue
    warnings = [item for item in results if item.get("status") == "warning"]
    return Result.ok({"count": len(results), "warning_count": len(warnings), "results": results, "source_types": source_types})


def _submit_missing_tags(ctx, product_id: str, source_types: list[str] | None = None) -> Result:
    segments = _segments_for_source_types(ctx, product_id, source_types or [])
    missing = []
    for segment in segments:
        if not _has_tag(ctx, str(segment.get("segment_id") or "")):
            missing.append(segment.get("segment_id"))
    if not missing:
        return Result.ok({"skipped": True, "reason": "tags_exist", "missing_segments": []})
    res = AITaggingSkill(ctx).submit_batch(product_id, source_types=source_types)
    if not res.success:
        return res
    return Result.ok({**(res.data or {}), "missing_segments": missing})


def _poll_missing_tags(ctx, product_id: str, source_types: list[str], force_segment_ids: list[str] | None = None) -> Result:
    tagger = AITaggingSkill(ctx)
    segments = _segments_for_source_types(ctx, product_id, source_types)
    force_set = set(force_segment_ids or [])
    timeout_seconds = _guard_tag_timeout()
    completed = skipped = failed = 0
    results = []
    for idx, segment in enumerate(segments):
        segment_id = str(segment.get("segment_id") or "")
        if force_set and segment_id not in force_set and _latest_tag(ctx, segment_id):
            item = {"status": "skipped", "segment_id": segment_id, "reason": "outside_retag_batch"}
            results.append(item)
            skipped += 1
            continue
        try:
            with _guard_timeout(timeout_seconds):
                res = tagger._poll_segment(product_id, segment, idx, "v1.0", segment_id in force_set)
        except _GuardTimeout:
            res = Result.ok({"status": "failed", "segment_id": segment_id, "error_code": "TAG_POLL_TIMEOUT", "timeout_seconds": timeout_seconds})
        except Exception as exc:
            res = Result.ok({"status": "failed", "segment_id": segment_id, "error_code": "TAG_POLL_EXCEPTION", "error": str(exc)})
        item = res.data if res.success else {"status": "failed", "segment_id": segment.get("segment_id"), "error": res.to_dict()}
        results.append(item)
        if item.get("status") == "completed":
            completed += 1
        elif item.get("status") == "skipped":
            skipped += 1
        else:
            failed += 1
    ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "AI_TAGGED"})
    return Result.ok({"completed_segments": completed, "skipped_segments": skipped, "failed_segments": failed, "force_retag_segments": len(force_set), "results": results, "source_types": source_types})


class _GuardTimeout(Exception):
    pass


@contextmanager
def _guard_timeout(seconds: int):
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    previous_handler = signal.getsignal(signal.SIGALRM)

    def _raise_timeout(signum, frame):
        raise _GuardTimeout(f"timed out after {seconds}s")

    signal.signal(signal.SIGALRM, _raise_timeout)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def _status_detail(ctx, product_id: str, target: int | None) -> dict[str, Any]:
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    task = _latest_task(ctx, product_id) or {}
    capacity = CapacityCounterSkill(ctx).refresh_product(product_id) if task else Result.ok({})
    cap = capacity.data if capacity.success else {}
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    stale = _stale_segment_summary(ctx, segments)
    first_candidates = sum(1 for seg in segments if "hero" in (seg.get("effective_roles_json") or []))
    effective = sum(1 for output in outputs if is_good_rendered_output(output))
    target_value = int(target or task.get("requested_variant_count") or task.get("allowed_variant_count") or 0)
    return {
        "target_count": target_value,
        "effective_count": effective,
        "remaining_count": max(0, target_value - effective),
        "target_remaining_variant_count": cap.get("target_remaining_variant_count"),
        "material_pool_extra_capacity": cap.get("material_pool_extra_capacity"),
        "first_slot_remaining_capacity": cap.get("first_slot_remaining_capacity"),
        "current_bottleneck": cap.get("current_bottleneck"),
        "capacity_note": cap.get("capacity_note"),
        "first_slot_candidates": first_candidates,
        "stale_segment_count": stale["stale_count"],
        "task_status": task.get("task_status"),
        "material_status": task.get("material_status"),
        "ai_supplement_status": task.get("ai_supplement_status"),
    }


def _classify_failure(result: Result) -> tuple[str, str]:
    code = result.error.code if result.error else ""
    if code in {"ANCHOR_PENDING"}:
        return "BLOCKED", "WAIT_ANCHOR_CONFIRMATION"
    if code in {"TASK_NOT_FOUND", "PRODUCT_NOT_FOUND"}:
        return "BLOCKED", "NEED_CREATE_TASK_FIELDS"
    if code in {"MATERIAL_NOT_READY"}:
        return "BLOCKED", "NEED_MORE_MATERIAL_OR_AI_SUPPLEMENT"
    if code in {"MATERIAL_QUALITY_TOO_LOW"}:
        return "BLOCKED", "NEED_BETTER_MATERIAL"
    if code in {"RENDER_PLAN_TIMEOUT"}:
        return "BLOCKED", "CHECK_PIPELINE_LOG"
    if code in {"ASSET_PROBE_FAILED", "OSS_DOWNLOAD_FAILED"}:
        return "ERROR", "CHECK_OSS_DOWNLOAD_OR_ASSET_SOURCE"
    return "ERROR", "CHECK_ERROR"


def _safe_guard_update(ctx, product_id: str, status: str, next_action: str, last_error: str, detail: dict[str, Any], last_batch_id: str = "") -> None:
    task = _latest_task(ctx, product_id)
    if not task:
        return
    ctx.repo.update(
        "content_tasks",
        "task_id",
        task["task_id"],
        {
            "pipeline_status": status,
            "next_action": next_action,
            "last_error": last_error,
            "last_batch_id": last_batch_id,
            "guard_detail_json": detail,
        },
    )


def _latest_task(ctx, product_id: str) -> dict | None:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return rows[0] if rows else None


if __name__ == "__main__":
    raise SystemExit(main())
