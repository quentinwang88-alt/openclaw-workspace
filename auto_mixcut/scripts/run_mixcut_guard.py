#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.agent.orchestrator import AutoMixcutOrchestratorAgent  # noqa: E402
from auto_mixcut.cli import _top_up  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.core.ids import new_id  # noqa: E402
from auto_mixcut.core.result import Result  # noqa: E402
from auto_mixcut.skills.capacity_counter_skill import CapacityCounterSkill  # noqa: E402
from auto_mixcut.skills.ai_anchor_check_skill import AIAnchorCheckSkill  # noqa: E402
from auto_mixcut.skills.ai_generated_consistency_skill import AIGeneratedConsistencySkill  # noqa: E402
from auto_mixcut.skills.ai_generation_qc_skill import _basic_qc  # noqa: E402
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

    _guard_log("guard_start", product_id=args.product_id, target=args.target)
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

    _guard_log("load_task", product_id=product_id)
    task = _latest_task(ctx, product_id)
    product = ctx.repo.get("products", "product_id", product_id)
    if not task:
        bootstrap = _task_bootstrap_payload(ctx, product_id, target=target, name=name, market=market, category=category)
        if not bootstrap.success:
            detail = {**_status_detail(ctx, product_id, target), "task_bootstrap": bootstrap.to_dict()}
            _safe_guard_update(ctx, product_id, "BLOCKED", "NEED_CREATE_TASK_FIELDS", "缺少商品名/市场/类目/目标数量，无法从零创建任务", detail)
            return Result.fail("TASK_NOT_FOUND", "task not found; provide --name --market --category --target or create a row in 商品内容任务表", detail)
        payload = bootstrap.data or {}
        created = RDSRepositorySkill(ctx).create_product_task(
            product_id,
            str(payload.get("product_name") or product_id),
            str(payload.get("market") or ""),
            str(payload.get("category") or ""),
            int(payload.get("requested_variant_count") or 0),
            shop_id=str(payload.get("shop_id") or ""),
            priority=str(payload.get("priority") or "normal"),
        )
        if not created.success:
            _safe_guard_update(ctx, product_id, "ERROR", "CREATE_TASK_FAILED", created.error.message if created.error else "create task failed", created.to_dict())
            return created
        if payload.get("source_record_id"):
            task_rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
            if task_rows:
                ctx.repo.update("content_tasks", "task_id", task_rows[0]["task_id"], {"created_by": "feishu_product_task"})
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

    _guard_log("ensure_anchor", product_id=product_id)
    anchor = _ensure_anchor_confirmed(ctx, product_id)
    if not anchor.success:
        detail = {**_status_detail(ctx, product_id, target), "anchor": anchor.to_dict()}
        status, action = _classify_failure(anchor)
        _safe_guard_update(ctx, product_id, status, action, anchor.error.message if anchor.error else "", detail)
        return anchor

    upload_sync = None
    if process_uploads:
        _guard_log("process_uploads", product_id=product_id)
        upload_sync = _process_uploads(product_id)
    ai_return_sync = _process_prompt_package_returns(product_id) if process_uploads else {"status": "skipped", "reason": "upload_sync_disabled"}

    assets = ctx.repo.list_where("assets", "product_id=?", (product_id,))
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    _guard_log("material_loaded", product_id=product_id, assets=len(assets), segments=len(segments))
    if not assets:
        detail = {**_status_detail(ctx, product_id, target), "upload_sync": upload_sync, "ai_return_sync": ai_return_sync}
        _safe_guard_update(ctx, product_id, "BLOCKED", "NEED_MATERIAL_UPLOAD", "没有可处理素材，请先上传素材或等待AI回流", detail)
        return Result.ok({"product_id": product_id, "pipeline_status": "BLOCKED", "next_action": "NEED_MATERIAL_UPLOAD", "detail": detail})

    _guard_log("stale_analysis_start", product_id=product_id, segments=len(segments))
    stale_index = _build_stale_index(ctx, segments)
    stale_segments = _stale_segment_summary(ctx, segments, stale_index)
    stale_ai_segments = _stale_segment_summary(ctx, [s for s in segments if s.get("source_type") == "ai_generated"], stale_index)
    stale_repair_source_types = _stale_repair_source_types(ctx, segments, stale_index)
    _guard_log("stale_analysis_done", product_id=product_id, stale_count=stale_segments["stale_count"], source_types=stale_repair_source_types)
    if not segments:
        _guard_log("repair_material_only_start", product_id=product_id)
        repaired = _run_incremental_postprocess(ctx, product_id, material_only=True)
        detail = {
            **_status_detail(ctx, product_id, target),
            "upload_sync": upload_sync,
            "ai_return_sync": ai_return_sync,
            "stale_segments": stale_segments,
            "stale_ai_segments": stale_ai_segments,
            "stale_repair_source_types": stale_repair_source_types,
            "material_work": _material_work_summary(ctx, product_id),
            "incremental_postprocess": repaired.to_dict(),
        }
        if not repaired.success:
            status, action = _classify_failure(repaired)
            _safe_guard_update(ctx, product_id, status, action, repaired.error.message if repaired.error else "", detail)
            return repaired
        refreshed_segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
        refreshed_index = _build_stale_index(ctx, refreshed_segments)
        refreshed_stale = _stale_segment_summary(ctx, refreshed_segments, refreshed_index)
        if not refreshed_segments or refreshed_stale["stale_count"] or _has_more_material_work(detail["material_work"]):
            _safe_guard_update(ctx, product_id, "READY_TO_CONTINUE", "RUN_GUARD_AGAIN", "", detail)
            return Result.ok({"product_id": product_id, "pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "detail": detail})
    elif stale_repair_source_types:
        _guard_log("repair_stale_start", product_id=product_id, source_types=stale_repair_source_types, stale_count=stale_segments["stale_count"])
        repaired = _run_incremental_postprocess(ctx, product_id, source_types=stale_repair_source_types, include_material_steps=False)
        if not repaired.success:
            status, action = _classify_failure(repaired)
            detail = {
                **_status_detail(ctx, product_id, target),
                "upload_sync": upload_sync,
                "ai_return_sync": ai_return_sync,
                "stale_segments": stale_segments,
                "stale_ai_segments": stale_ai_segments,
                "stale_repair_source_types": stale_repair_source_types,
                "incremental_postprocess": repaired.to_dict(),
            }
            _safe_guard_update(ctx, product_id, status, action, repaired.error.message if repaired.error else "", detail)
            return repaired
        refreshed_segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
        refreshed_index = _build_stale_index(ctx, refreshed_segments)
        refreshed_stale = _stale_segment_summary(ctx, refreshed_segments, refreshed_index)
        if refreshed_stale["stale_count"]:
            ready_detail = _status_detail(ctx, product_id, target)
            detail = {
                **ready_detail,
                "upload_sync": upload_sync,
                "stale_segments": refreshed_stale,
                "stale_ai_segments": _stale_segment_summary(ctx, [s for s in refreshed_segments if s.get("source_type") == "ai_generated"], refreshed_index),
                "stale_repair_source_types": _stale_repair_source_types(ctx, refreshed_segments, refreshed_index),
                "incremental_postprocess": repaired.to_dict(),
            }
            refreshed_stale_ai = _stale_segment_summary(ctx, [s for s in refreshed_segments if s.get("source_type") == "ai_generated"], refreshed_index)
            if refreshed_stale_ai["stale_count"]:
                _guard_log(
                    "stale_ai_blocks_top_up",
                    product_id=product_id,
                    stale_ai_count=refreshed_stale_ai["stale_count"],
                    material_pool_extra_capacity=ready_detail.get("material_pool_extra_capacity"),
                    remaining_count=ready_detail.get("remaining_count"),
                )
                _safe_guard_update(ctx, product_id, "READY_TO_CONTINUE", "RUN_GUARD_AGAIN", "", detail)
                return Result.ok({"product_id": product_id, "pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "detail": detail})
            if not _guard_allows_top_up_with_stale() or int(ready_detail.get("material_pool_extra_capacity") or 0) <= 0 or int(ready_detail.get("remaining_count") or 0) <= 0:
                _safe_guard_update(ctx, product_id, "READY_TO_CONTINUE", "RUN_GUARD_AGAIN", "", detail)
                return Result.ok({"product_id": product_id, "pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "detail": detail})
            _guard_log(
                "stale_repair_deferred_top_up_allowed",
                product_id=product_id,
                stale_count=refreshed_stale["stale_count"],
                material_pool_extra_capacity=ready_detail.get("material_pool_extra_capacity"),
                remaining_count=ready_detail.get("remaining_count"),
            )

    _guard_log("top_up_start", product_id=product_id, target=target)
    top_up = _top_up(ctx, product_id, target, max_rounds=max_rounds)
    ai_submit = _maybe_prepare_ai_submit(product_id, top_up.data or {}) if top_up.success else {"status": "skipped", "reason": "top_up_failed"}
    final = _status_after_top_up(ctx, product_id, target, top_up)
    if ai_submit.get("status") in {"manual_required", "failed"} and final.get("pipeline_status") == "WAITING_AI_RETURN":
        final["next_action"] = "RUN_AI_SEGMENT_WORKER"
        final["last_error"] = ai_submit.get("error") or ai_submit.get("reason") or ""
    detail = {
        **_status_detail(ctx, product_id, target),
        "upload_sync": upload_sync,
        "ai_return_sync": ai_return_sync,
        "ai_submit": ai_submit,
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


def _task_bootstrap_payload(ctx, product_id: str, target: int | None, name: str, market: str, category: str) -> Result:
    product = ctx.repo.get("products", "product_id", product_id) or {}
    payload = {
        "product_id": product_id,
        "product_name": name or product.get("product_name") or "",
        "market": market or product.get("market") or "",
        "category": category or product.get("category") or "",
        "requested_variant_count": int(target or 0),
        "shop_id": product.get("shop_id") or "",
        "priority": product.get("priority") or "normal",
        "source": "args_or_rds",
    }
    if _has_required_task_payload(payload):
        return Result.ok(payload)

    feishu = _fetch_product_task_from_feishu(product_id)
    if feishu.success:
        row = feishu.data or {}
        payload.update({
            "product_name": payload["product_name"] or row.get("product_name") or product_id,
            "market": payload["market"] or row.get("market") or "",
            "category": payload["category"] or row.get("category") or "",
            "requested_variant_count": int(target or row.get("requested_variant_count") or 0),
            "shop_id": payload["shop_id"] or row.get("shop_id") or "",
            "priority": payload["priority"] or row.get("priority") or "normal",
            "source": "feishu_product_task",
            "source_record_id": row.get("record_id") or "",
        })
    if _has_required_task_payload(payload):
        return Result.ok(payload)
    return Result.fail(
        "TASK_BOOTSTRAP_FIELDS_MISSING",
        "product task fields missing in RDS/args/商品内容任务表",
        {"product_id": product_id, "payload": payload, "feishu": feishu.to_dict()},
    )


def _has_required_task_payload(payload: dict[str, Any]) -> bool:
    try:
        requested = int(payload.get("requested_variant_count") or 0)
    except (TypeError, ValueError):
        requested = 0
    return bool(payload.get("product_name") and payload.get("market") and payload.get("category") and requested > 0)


def _fetch_product_task_from_feishu(product_id: str) -> Result:
    if os.environ.get("AUTO_MIXCUT_GUARD_BOOTSTRAP_FROM_FEISHU", "1").strip().lower() in {"0", "false", "no", "off"}:
        return Result.fail("FEISHU_BOOTSTRAP_DISABLED", "feishu bootstrap disabled", {"product_id": product_id})
    try:
        from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient

        client = AutoMixcutFeishuClient("商品内容任务表")
        for record in client.list_records():
            fields = getattr(record, "fields", {}) or {}
            if _cell_text(fields.get("商品ID")) != product_id:
                continue
            target = _cell_number(fields.get("目标生成数量")) or _cell_number(fields.get("系统允许生成数量"))
            return Result.ok({
                "record_id": getattr(record, "record_id", ""),
                "product_id": product_id,
                "product_name": _cell_text(fields.get("商品名称")) or product_id,
                "market": _cell_text(fields.get("市场")),
                "category": _cell_text(fields.get("类目")) or _cell_text(fields.get("归一类目")),
                "shop_id": _cell_text(fields.get("店铺ID")) or _cell_text(fields.get("店铺")),
                "priority": _cell_text(fields.get("优先级")) or "normal",
                "requested_variant_count": int(target or 0),
            })
        return Result.fail("FEISHU_PRODUCT_TASK_NOT_FOUND", "product not found in 商品内容任务表", {"product_id": product_id})
    except Exception as exc:
        return Result.fail("FEISHU_PRODUCT_TASK_LOOKUP_FAILED", str(exc), {"product_id": product_id})


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            text = _cell_text(value.get(key))
            if text:
                return text
        if "link" in value:
            return _cell_text(value.get("link"))
        return ""
    if isinstance(value, list):
        return "\n".join(item for item in (_cell_text(item) for item in value) if item).strip()
    return str(value).strip()


def _cell_number(value: Any) -> int:
    text = _cell_text(value).replace(",", "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _process_prompt_package_returns(product_id: str) -> dict[str, Any]:
    if os.environ.get("AUTO_MIXCUT_GUARD_PROCESS_AI_RETURNS", "1").strip().lower() in {"0", "false", "no", "off"}:
        return {"status": "skipped", "reason": "disabled"}
    timeout = _guard_ai_return_timeout()
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "process_prompt_package_returns.py"),
        "--product-id",
        product_id,
    ]
    try:
        limit = int(os.environ.get("AUTO_MIXCUT_GUARD_AI_RETURN_IMPORT_LIMIT", "0") or "0")
    except ValueError:
        limit = 0
    if limit > 0:
        cmd.extend(["--limit", str(limit)])
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "timeout_seconds": timeout}
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}
    payload = _parse_result_json(proc.stdout)
    if proc.returncode != 0:
        return {"status": "failed", "returncode": proc.returncode, "stdout": (proc.stdout or "")[-2000:], "stderr": (proc.stderr or "")[-2000:], "result": payload}
    imported = int(((payload.get("import") or {}).get("count") or 0)) if isinstance(payload, dict) else 0
    return {"status": "ok", "imported_count": imported, "result": payload}


def _guard_ai_return_timeout() -> int:
    try:
        return max(30, int(os.environ.get("AUTO_MIXCUT_GUARD_AI_RETURN_TIMEOUT", "240") or "240"))
    except ValueError:
        return 240


def _maybe_prepare_ai_submit(product_id: str, top_up_data: dict[str, Any]) -> dict[str, Any]:
    if not _top_up_created_ai_supplement(top_up_data):
        return {"status": "skipped", "reason": "no_ai_supplement_created"}
    if os.environ.get("AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES", "1").strip().lower() in {"0", "false", "no", "off"}:
        command = _ai_segment_worker_command(product_id)
        return {
            "status": "manual_required",
            "reason": "auto submit disabled",
            "command": " ".join(command),
            "product_id": product_id,
        }
    command = _ai_segment_worker_command(product_id)
    timeout = _guard_ai_submit_timeout()
    try:
        proc = subprocess.run(
            command,
            cwd=str(ROOT.parent / "skills" / "jimeng-video-generator"),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "timeout_seconds": timeout,
            "command": " ".join(command),
            "product_id": product_id,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "command": " ".join(command),
            "product_id": product_id,
        }
    if proc.returncode != 0:
        return {
            "status": "failed",
            "returncode": proc.returncode,
            "stdout": (proc.stdout or "")[-2000:],
            "stderr": (proc.stderr or "")[-2000:],
            "command": " ".join(command),
            "product_id": product_id,
        }
    return {
        "status": "ok",
        "stdout": (proc.stdout or "")[-2000:],
        "command": " ".join(command),
        "product_id": product_id,
    }


def _ai_segment_worker_command(product_id: str) -> list[str]:
    try:
        limit = max(1, int(os.environ.get("AUTO_MIXCUT_GUARD_AI_SUBMIT_LIMIT", "5") or "5"))
    except ValueError:
        limit = 5
    return [
        "node",
        str(ROOT.parent / "skills" / "jimeng-video-generator" / "segment-package-worker.js"),
        "--submit-only",
        "--one-shot",
        f"--product-id={product_id}",
        f"--limit={limit}",
    ]


def _guard_ai_submit_timeout() -> int:
    try:
        return max(30, int(os.environ.get("AUTO_MIXCUT_GUARD_AI_SUBMIT_TIMEOUT", "300") or "300"))
    except ValueError:
        return 300


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


def _stale_segment_summary(ctx, segments: list[dict[str, Any]], stale_index: dict[str, Any] | None = None) -> dict[str, Any]:
    stale_index = stale_index or _build_stale_index(ctx, segments)
    stale = []
    for segment in segments:
        reasons = _stale_segment_reasons(ctx, segment, stale_index)
        if reasons:
            stale.append({"segment_id": segment.get("segment_id"), "source_type": segment.get("source_type"), "reasons": reasons})
    return {
        "segment_count": len(segments),
        "stale_count": len(stale),
        "sample": stale[:20],
    }


def _stale_repair_source_types(ctx, segments: list[dict[str, Any]], stale_index: dict[str, Any] | None = None) -> list[str]:
    stale_index = stale_index or _build_stale_index(ctx, segments)
    source_types = set()
    for segment in segments:
        if not _stale_segment_reasons(ctx, segment, stale_index):
            continue
        source_type = str(segment.get("source_type") or "").strip()
        if source_type:
            source_types.add(source_type)
    return sorted(source_types)


def _stale_retag_segment_ids(ctx, product_id: str, source_types: list[str]) -> list[str]:
    segment_ids: list[str] = []
    segments = _segments_for_source_types(ctx, product_id, source_types)
    stale_index = _build_stale_index(ctx, segments)
    retag_segments = []
    for segment in segments:
        reasons = set(_stale_segment_reasons(ctx, segment, stale_index))
        segment_id = str(segment.get("segment_id") or "")
        if "tag_missing" in reasons and _has_frames(ctx, segment_id, stale_index):
            retag_segments.append(segment)
    retag_segments.sort(key=_retag_segment_priority)
    segment_ids = [str(item.get("segment_id") or "") for item in retag_segments if item.get("segment_id")]
    limit = _guard_retag_limit()
    if limit > 0:
        return segment_ids[:limit]
    return segment_ids


def _guard_retag_limit() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_GUARD_RETAG_LIMIT", "40") or "40"))
    except ValueError:
        return 40


def _retag_segment_priority(segment: dict[str, Any]) -> tuple[int, int, str]:
    source_type = str(segment.get("source_type") or "")
    status = str(segment.get("segment_status") or "")
    source_rank = 0 if source_type == "ai_generated" else 1
    status_rank = 0 if status in {"created", "qc_passed"} else 1
    created = str(segment.get("created_at") or segment.get("segment_id") or "")
    return (source_rank, status_rank, created)


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


def _guard_requires_phash() -> bool:
    return os.environ.get("AUTO_MIXCUT_GUARD_REQUIRE_PHASH", "0").strip().lower() in {"1", "true", "yes", "on"}


def _guard_runs_consistency() -> bool:
    return os.environ.get("AUTO_MIXCUT_GUARD_RUN_CONSISTENCY", "1").strip().lower() in {"1", "true", "yes", "on"}


def _guard_runs_ai_anchor_check() -> bool:
    return os.environ.get("AUTO_MIXCUT_GUARD_RUN_AI_ANCHOR_CHECK", "1").strip().lower() in {"1", "true", "yes", "on"}


def _guard_allows_top_up_with_stale() -> bool:
    return os.environ.get("AUTO_MIXCUT_GUARD_TOP_UP_WITH_STALE", "1").strip().lower() not in {"0", "false", "no", "off"}


def _guard_tag_timeout() -> int:
    try:
        return max(30, int(os.environ.get("AUTO_MIXCUT_GUARD_TAG_TIMEOUT", "180") or "180"))
    except ValueError:
        return 180


def _stale_segment_reasons(ctx, segment: dict[str, Any], stale_index: dict[str, Any] | None = None) -> list[str]:
    reasons: list[str] = []
    segment_id = str(segment.get("segment_id") or "")
    if not segment_id:
        return ["missing_segment_id"]
    if _is_guard_failed_segment(segment):
        return []
    if not _has_frames(ctx, segment_id, stale_index):
        reasons.append("frames_missing")
    if _guard_requires_phash() and not segment.get("visual_phash"):
        reasons.append("visual_phash_missing")
    if not _has_tag(ctx, segment_id, stale_index):
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


def _build_stale_index(ctx, segments: list[dict[str, Any]]) -> dict[str, Any]:
    segment_ids = [str(segment.get("segment_id") or "") for segment in segments if segment.get("segment_id")]
    frame_ids: set[str] = set()
    tag_ids: set[str] = set()
    frame_counts: dict[str, int] = {}
    for chunk in _chunks(segment_ids, 200):
        placeholders = ",".join("?" for _ in chunk)
        for row in ctx.repo.list_where("segment_frames", f"segment_id IN ({placeholders})", tuple(chunk)):
            if row.get("segment_id"):
                sid = str(row["segment_id"])
                frame_ids.add(sid)
                frame_counts[sid] = frame_counts.get(sid, 0) + 1
        for row in ctx.repo.list_where("segment_tags", f"segment_id IN ({placeholders})", tuple(chunk)):
            if row.get("segment_id"):
                tag_ids.add(str(row["segment_id"]))
    return {"frame_ids": frame_ids, "tag_ids": tag_ids, "frame_counts": frame_counts}


def _chunks(items: list[str], size: int):
    for idx in range(0, len(items), max(1, size)):
        yield items[idx : idx + size]


def _has_frames(ctx, segment_id: str, stale_index: dict[str, Any] | None = None) -> bool:
    if stale_index is not None:
        return segment_id in stale_index.get("frame_ids", set())
    rows = ctx.repo.list_where("segment_frames", "segment_id=? LIMIT 1", (segment_id,))
    return bool(rows)


def _frame_count(segment_id: str, stale_index: dict[str, Any]) -> int:
    return int((stale_index.get("frame_counts") or {}).get(segment_id) or 0)


def _has_tag(ctx, segment_id: str, stale_index: dict[str, Any] | None = None) -> bool:
    if stale_index is not None:
        return segment_id in stale_index.get("tag_ids", set())
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
    if batch_ids:
        return {**detail, "pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "last_error": "", "last_batch_id": batch_ids[-1]}
    if _top_up_created_ai_supplement(data):
        return {**detail, "pipeline_status": "WAITING_AI_RETURN", "next_action": "WAIT_AI_SEGMENT_RETURN", "last_error": "", "last_batch_id": batch_ids[-1] if batch_ids else ""}
    if stop == "waiting_ai_postprocess":
        return {**detail, "pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "last_error": "", "last_batch_id": batch_ids[-1] if batch_ids else ""}
    if stop in {"render_plan_empty", "no_material_pool_capacity", "no_material_pool_capacity_after_round"}:
        return {**detail, "pipeline_status": "BLOCKED", "next_action": "NEED_MORE_MATERIAL_OR_AI_SUPPLEMENT", "last_error": stop, "last_batch_id": batch_ids[-1] if batch_ids else ""}
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


def _run_incremental_postprocess(ctx, product_id: str, source_types: list[str] | None = None, include_material_steps: bool = True, material_only: bool = False) -> Result:
    """Repair stale material metadata without creating a full render batch."""
    steps = []
    source_types = [str(item) for item in (source_types or []) if str(item or "").strip()]
    if not source_types:
        source_types = sorted({str(s.get("source_type") or "") for s in ctx.repo.list_where("segments", "product_id=?", (product_id,)) if str(s.get("source_type") or "").strip()})
    if not source_types:
        source_types = sorted({str(a.get("source_type") or "") for a in ctx.repo.list_where("assets", "product_id=?", (product_id,)) if str(a.get("source_type") or "").strip()})
    retag_segment_ids = _stale_retag_segment_ids(ctx, product_id, source_types)
    includes_ai_generated = "ai_generated" in set(source_types)
    material_steps = [
        ("probe", lambda: MediaProbeSkill(ctx).probe_product(product_id, source_types=source_types)),
        ("watermark", lambda: WatermarkDetectSkill(ctx).check_product(product_id, source_types=source_types)),
        ("watermark_process", lambda: WatermarkProcessSkill(ctx).process_product(product_id, source_types=source_types)),
        ("segment", lambda: SegmentSkill(ctx).segment_product(product_id, source_types=source_types)),
    ]
    postprocess_steps = [
        ("frames", lambda: _sample_missing_frames(ctx, product_id, source_types=source_types)),
        ("fingerprint", lambda: _fingerprint_missing(ctx, product_id, source_types=source_types) if _guard_requires_phash() else Result.ok({"skipped": True, "reason": "phash_not_required_for_guard"})),
        ("tag_submit", lambda: _submit_missing_tags(ctx, product_id, source_types=source_types, missing_segment_ids=retag_segment_ids)),
        ("tag_poll", lambda: _poll_missing_tags(ctx, product_id, source_types=source_types, force_segment_ids=retag_segment_ids)),
        ("ai_generation_qc", lambda: _ai_generation_qc_missing(ctx, product_id) if includes_ai_generated else Result.ok({"skipped": True, "reason": "no_ai_generated_source"})),
        ("consistency", lambda: _ai_consistency_missing(ctx, product_id) if includes_ai_generated and _guard_runs_consistency() else Result.ok({"skipped": True, "reason": "consistency_not_required_for_guard"})),
        ("ai_anchor_check", lambda: _ai_anchor_check_missing(ctx, product_id) if includes_ai_generated and _guard_runs_ai_anchor_check() else Result.ok({"skipped": True, "reason": "ai_anchor_check_not_required_for_guard"})),
        ("effective_roles", lambda: _compute_missing_effective_roles(ctx, product_id, source_types=source_types, force_segment_ids=retag_segment_ids)),
    ]
    planned_steps = []
    if include_material_steps:
        planned_steps.extend(material_steps)
    if not material_only:
        planned_steps.extend(postprocess_steps)
    for name, fn in planned_steps:
        _guard_log("step_start", product_id=product_id, step=name, source_types=source_types)
        res = fn()
        steps.append({"step": name, **res.to_dict()})
        _guard_log("step_done", product_id=product_id, step=name, success=res.success)
        if not res.success:
            return Result.fail(
                res.error.code if res.error else "INCREMENTAL_POSTPROCESS_FAILED",
                res.error.message if res.error else f"incremental postprocess failed at {name}",
                {"product_id": product_id, "stage": name, "steps": steps},
            )
    return Result.ok({"product_id": product_id, "source_types": source_types, "force_retag_segment_ids": retag_segment_ids, "steps": steps})


def _material_work_summary(ctx, product_id: str) -> dict[str, Any]:
    assets = ctx.repo.list_where("assets", "product_id=?", (product_id,))
    probe_pending = sum(1 for asset in assets if str(asset.get("probe_status") or "pending") != "done")
    watermark_pending = sum(
        1
        for asset in assets
        if str(asset.get("probe_status") or "") == "done"
        and str(asset.get("has_watermark") or "pending") in {"", "pending", "unknown"}
    )
    no_watermark_assets = [
        asset
        for asset in assets
        if str(asset.get("probe_status") or "") == "done" and str(asset.get("has_watermark") or "") == "no"
    ]
    unsegmented_assets = sum(
        1
        for asset in no_watermark_assets
        if not ctx.repo.list_where("segments", "asset_id=? LIMIT 1", (asset["asset_id"],))
    )
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    stale = _stale_segment_summary(ctx, segments, _build_stale_index(ctx, segments))
    return {
        "asset_count": len(assets),
        "probe_pending_count": probe_pending,
        "watermark_pending_count": watermark_pending,
        "no_watermark_asset_count": len(no_watermark_assets),
        "unsegmented_asset_count": unsegmented_assets,
        "segment_count": len(segments),
        "stale_segment_count": stale["stale_count"],
    }


def _has_more_material_work(summary: dict[str, Any]) -> bool:
    return any(int(summary.get(key) or 0) > 0 for key in ("probe_pending_count", "watermark_pending_count", "unsegmented_asset_count", "stale_segment_count"))


def _segments_for_source_types(ctx, product_id: str, source_types: list[str]) -> list[dict[str, Any]]:
    if not source_types:
        return ctx.repo.list_where("segments", "product_id=?", (product_id,))
    placeholders = ",".join("?" for _ in source_types)
    return ctx.repo.list_where(
        "segments",
        f"product_id=? AND source_type IN ({placeholders})",
        (product_id, *source_types),
    )


def _segments_by_ids(ctx, segment_ids: list[str]) -> list[dict[str, Any]]:
    segment_ids = [str(item) for item in segment_ids if str(item or "").strip()]
    if not segment_ids:
        return []
    rows: list[dict[str, Any]] = []
    for chunk in _chunks(segment_ids, 200):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(ctx.repo.list_where("segments", f"segment_id IN ({placeholders})", tuple(chunk)))
    order = {segment_id: idx for idx, segment_id in enumerate(segment_ids)}
    return sorted(rows, key=lambda row: order.get(str(row.get("segment_id") or ""), 10**9))


def _sample_missing_frames(ctx, product_id: str, source_types: list[str]) -> Result:
    results = []
    skill = FrameSampleSkill(ctx)
    sampled_or_attempted = 0
    limit = _guard_frame_limit()
    timeout_seconds = _guard_frame_timeout()
    segments = _segments_for_source_types(ctx, product_id, source_types)
    stale_index = _build_stale_index(ctx, segments)
    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        if _is_guard_failed_segment(segment) and str(segment.get("segment_status") or "") != "effective_role_failed":
            results.append({"segment_id": segment_id, "skipped": True, "reason": str(segment.get("segment_status") or "guard_failed")})
            continue
        expected = 9 if segment.get("source_type") == "ai_generated" else 4
        if _frame_count(segment_id, stale_index) >= expected:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "frames_exist"})
            continue
        if limit > 0 and sampled_or_attempted >= limit:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "outside_frame_repair_batch"})
            continue
        sampled_or_attempted += 1
        _guard_log("segment_step_start", product_id=product_id, step="frame_sample", segment_id=segment_id, attempt=sampled_or_attempted, limit=limit)
        try:
            if _guard_segment_subprocess_enabled():
                res = _run_segment_guard_step("frame_sample", segment_id, timeout_seconds)
            else:
                with _guard_timeout(timeout_seconds):
                    res = skill.sample_segment(segment_id)
        except _GuardTimeout:
            _mark_segment_guard_failed(ctx, segment_id, "frame_sample_timeout", f"frame sample timed out after {timeout_seconds}s")
            _guard_log("segment_step_timeout", product_id=product_id, step="frame_sample", segment_id=segment_id, timeout_seconds=timeout_seconds)
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FRAME_SAMPLE_TIMEOUT", "timeout_seconds": timeout_seconds})
            continue
        except Exception as exc:
            _mark_segment_guard_failed(ctx, segment_id, "frame_sample_failed", f"frame sample exception: {exc}")
            _guard_log("segment_step_exception", product_id=product_id, step="frame_sample", segment_id=segment_id, error=str(exc)[:300])
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FRAME_SAMPLE_EXCEPTION", "error": str(exc)})
            continue
        results.append(res.to_dict())
        if not res.success:
            message = res.error.message if res.error else "frame sample failed"
            _mark_segment_guard_failed(ctx, segment_id, "frame_sample_failed", message)
            _guard_log("segment_step_failed", product_id=product_id, step="frame_sample", segment_id=segment_id, error=message[:300])
            results[-1]["status"] = "warning"
            continue
        _guard_log("segment_step_done", product_id=product_id, step="frame_sample", segment_id=segment_id)
    warnings = [item for item in results if item.get("status") == "warning"]
    return Result.ok({"count": len(results), "attempted_count": sampled_or_attempted, "warning_count": len(warnings), "results": results, "source_types": source_types})


def _fingerprint_missing(ctx, product_id: str, source_types: list[str]) -> Result:
    results = []
    skill = SegmentFingerprintSkill(ctx)
    timeout_seconds = max(5, int(os.environ.get("AUTO_MIXCUT_GUARD_FINGERPRINT_TIMEOUT", "45") or "45"))
    limit = _guard_fingerprint_limit()
    attempted = 0
    segments = _segments_for_source_types(ctx, product_id, source_types)
    stale_index = _build_stale_index(ctx, segments)
    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        if _is_guard_failed_segment(segment):
            results.append({"segment_id": segment_id, "skipped": True, "reason": str(segment.get("segment_status") or "guard_failed")})
            continue
        if segment.get("visual_phash"):
            results.append({"segment_id": segment_id, "skipped": True, "reason": "fingerprint_exists"})
            continue
        if not _has_frames(ctx, segment_id, stale_index):
            results.append({"segment_id": segment_id, "skipped": True, "reason": "frames_missing"})
            continue
        if limit > 0 and attempted >= limit:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "outside_fingerprint_repair_batch"})
            continue
        attempted += 1
        _guard_log("segment_step_start", product_id=product_id, step="fingerprint", segment_id=segment_id, attempt=attempted, limit=limit)
        try:
            if _guard_segment_subprocess_enabled():
                res = _run_segment_guard_step("fingerprint", segment_id, timeout_seconds)
            else:
                with _guard_timeout(timeout_seconds):
                    res = skill.fingerprint_segment(segment_id)
        except _GuardTimeout:
            _mark_segment_guard_failed(ctx, segment_id, "fingerprint_failed", f"fingerprint timed out after {timeout_seconds}s")
            _guard_log("segment_step_timeout", product_id=product_id, step="fingerprint", segment_id=segment_id, timeout_seconds=timeout_seconds)
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FINGERPRINT_TIMEOUT", "timeout_seconds": timeout_seconds})
            continue
        except Exception as exc:
            _mark_segment_guard_failed(ctx, segment_id, "fingerprint_failed", f"fingerprint exception: {exc}")
            _guard_log("segment_step_exception", product_id=product_id, step="fingerprint", segment_id=segment_id, error=str(exc)[:300])
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "FINGERPRINT_EXCEPTION", "error": str(exc)})
            continue
        results.append(res.to_dict())
        if not res.success:
            message = res.error.message if res.error else "fingerprint failed"
            _mark_segment_guard_failed(ctx, segment_id, "fingerprint_failed", message)
            _guard_log("segment_step_failed", product_id=product_id, step="fingerprint", segment_id=segment_id, error=message[:300])
            results[-1]["status"] = "warning"
            continue
        _guard_log("segment_step_done", product_id=product_id, step="fingerprint", segment_id=segment_id)
    warnings = [item for item in results if item.get("status") == "warning"]
    return Result.ok({"count": len(results), "attempted_count": attempted, "warning_count": len(warnings), "results": results, "source_types": source_types})


def _guard_fingerprint_limit() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_GUARD_FINGERPRINT_LIMIT", "20") or "20"))
    except ValueError:
        return 20


def _submit_missing_tags(ctx, product_id: str, source_types: list[str] | None = None, missing_segment_ids: list[str] | None = None) -> Result:
    limit = _guard_retag_limit()
    missing = [str(item) for item in (missing_segment_ids or []) if str(item or "").strip()]
    if limit > 0:
        missing = missing[:limit]
    if not missing:
        return Result.ok({"skipped": True, "reason": "tags_exist", "missing_segments": []})
    batch_id = new_id("AIBATCH")
    write = ctx.repo.upsert(
        "ai_batches",
        "ai_batch_id",
        {
            "ai_batch_id": batch_id,
            "product_id": product_id,
            "batch_type": "segment_tagging",
            "status": "submitted",
            "total_segments": len(missing),
            "model_tier": "medium_vision",
            "prompt_version": "v1.0",
        },
    )
    if not write.success:
        return write
    return Result.ok({"ai_batch_id": batch_id, "total_segments": len(missing), "missing_segments": missing, "source_types": source_types or []})


def _poll_missing_tags(ctx, product_id: str, source_types: list[str], force_segment_ids: list[str] | None = None) -> Result:
    import time

    _guard_log("tag_poll_prepare_start", product_id=product_id, source_types=source_types)
    tagger = None
    force_set = set(force_segment_ids or [])
    if not force_set:
        return Result.ok({"skipped": True, "reason": "no_retag_batch", "source_types": source_types})
    segments = _segments_by_ids(ctx, list(force_set))
    _guard_log("tag_poll_segments_loaded", product_id=product_id, segment_count=len(segments))
    stale_index = _build_stale_index(ctx, segments)
    _guard_log("tag_poll_index_built", product_id=product_id, frame_segments=len(stale_index.get("frame_ids", set())), tag_segments=len(stale_index.get("tag_ids", set())))
    per_segment_timeout = _guard_tag_timeout()
    total_timeout = _guard_tag_total_timeout()
    started_at = time.monotonic()
    completed = skipped = failed = 0
    results = []
    runnable: list[tuple[int, dict[str, Any], int]] = []
    for idx, segment in enumerate(segments):
        segment_id = str(segment.get("segment_id") or "")
        if _is_guard_failed_segment(segment):
            item = {"status": "skipped", "segment_id": segment_id, "reason": str(segment.get("segment_status") or "guard_failed")}
            results.append(item)
            skipped += 1
            continue
        if not _has_frames(ctx, segment_id, stale_index):
            item = {"status": "skipped", "segment_id": segment_id, "reason": "frames_missing"}
            results.append(item)
            skipped += 1
            continue
        if force_set and segment_id not in force_set:
            item = {"status": "skipped", "segment_id": segment_id, "reason": "outside_retag_batch"}
            results.append(item)
            skipped += 1
            continue
        if total_timeout > 0 and time.monotonic() - started_at >= total_timeout:
            res = Result.ok({"status": "skipped", "segment_id": segment_id, "reason": "outside_tag_time_budget", "timeout_seconds": total_timeout})
            item = res.data
            results.append(item)
            skipped += 1
            continue
        remaining_timeout = per_segment_timeout
        if total_timeout > 0:
            remaining_timeout = max(1, min(per_segment_timeout, int(total_timeout - (time.monotonic() - started_at))))
        if _guard_segment_subprocess_enabled() and _guard_tag_concurrency() > 1:
            runnable.append((idx, segment, remaining_timeout))
            continue
        _guard_log("segment_step_start", product_id=product_id, step="tag_poll", segment_id=segment_id, timeout_seconds=remaining_timeout)
        try:
            if _guard_segment_subprocess_enabled():
                res = _run_segment_guard_step("tag_poll", segment_id, remaining_timeout, product_id=product_id, index=idx, force=False)
            else:
                tagger = tagger or AITaggingSkill(ctx)
                with _guard_timeout(remaining_timeout):
                    res = tagger._poll_segment(product_id, segment, idx, "v1.0", False)
        except _GuardTimeout:
            _mark_segment_guard_failed(ctx, segment_id, "tag_failed", f"tag poll timed out after {remaining_timeout}s")
            _guard_log("segment_step_timeout", product_id=product_id, step="tag_poll", segment_id=segment_id, timeout_seconds=remaining_timeout)
            res = Result.ok({"status": "failed", "segment_id": segment_id, "error_code": "TAG_POLL_TIMEOUT", "timeout_seconds": remaining_timeout})
        except Exception as exc:
            _guard_log("segment_step_exception", product_id=product_id, step="tag_poll", segment_id=segment_id, error=str(exc)[:300])
            res = Result.ok({"status": "failed", "segment_id": segment_id, "error_code": "TAG_POLL_EXCEPTION", "error": str(exc)})
        item = res.data if res.success else {"status": "failed", "segment_id": segment.get("segment_id"), "error": res.to_dict()}
        results.append(item)
        if item.get("status") == "completed":
            completed += 1
            _guard_log("segment_step_done", product_id=product_id, step="tag_poll", segment_id=segment_id, status="completed")
        elif item.get("status") == "skipped":
            skipped += 1
        else:
            failed += 1
            _guard_log("segment_step_failed", product_id=product_id, step="tag_poll", segment_id=segment_id, status=item.get("status"), error_code=item.get("error_code"))
    if runnable:
        parallel = _poll_tag_segments_parallel(ctx, product_id, runnable, max_workers=_guard_tag_concurrency())
        for item in parallel:
            results.append(item)
            if item.get("status") == "completed":
                completed += 1
            elif item.get("status") == "skipped":
                skipped += 1
            else:
                failed += 1
    _update_latest_task(ctx, product_id, {"task_status": "AI_TAGGED"})
    return Result.ok({
        "completed_segments": completed,
        "skipped_segments": skipped,
        "failed_segments": failed,
        "force_retag_segments": len(force_set),
        "elapsed_seconds": round(time.monotonic() - started_at, 1),
        "total_timeout_seconds": total_timeout,
        "results": results,
        "source_types": source_types,
    })


def _poll_tag_segments_parallel(ctx, product_id: str, runnable: list[tuple[int, dict[str, Any], int]], max_workers: int) -> list[dict[str, Any]]:
    results_by_index: dict[int, dict[str, Any]] = {}
    workers = max(1, min(max_workers, len(runnable)))
    _guard_log("tag_poll_parallel_start", product_id=product_id, segment_count=len(runnable), concurrency=workers)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for idx, segment, timeout_seconds in runnable:
            segment_id = str(segment.get("segment_id") or "")
            _guard_log("segment_step_queued", product_id=product_id, step="tag_poll", segment_id=segment_id, timeout_seconds=timeout_seconds, parallel=True)
            futures[executor.submit(_poll_tag_segment_subprocess, product_id, segment_id, idx, timeout_seconds, False)] = (idx, segment_id, timeout_seconds)
        for future in as_completed(futures):
            idx, segment_id, timeout_seconds = futures[future]
            try:
                item = future.result()
            except _GuardTimeout:
                _mark_segment_guard_failed(ctx, segment_id, "tag_failed", f"tag poll timed out after {timeout_seconds}s")
                item = {"status": "failed", "segment_id": segment_id, "error_code": "TAG_POLL_TIMEOUT", "timeout_seconds": timeout_seconds}
                _guard_log("segment_step_timeout", product_id=product_id, step="tag_poll", segment_id=segment_id, timeout_seconds=timeout_seconds, parallel=True)
            except Exception as exc:
                item = {"status": "failed", "segment_id": segment_id, "error_code": "TAG_POLL_EXCEPTION", "error": str(exc)}
                _guard_log("segment_step_exception", product_id=product_id, step="tag_poll", segment_id=segment_id, error=str(exc)[:300], parallel=True)
            results_by_index[idx] = item
            if item.get("status") == "completed":
                _guard_log("segment_step_done", product_id=product_id, step="tag_poll", segment_id=segment_id, status="completed", parallel=True)
            elif item.get("status") == "skipped":
                _guard_log("segment_step_skipped", product_id=product_id, step="tag_poll", segment_id=segment_id, reason=item.get("reason"), parallel=True)
            else:
                _guard_log("segment_step_failed", product_id=product_id, step="tag_poll", segment_id=segment_id, status=item.get("status"), error_code=item.get("error_code"), parallel=True)
    return [results_by_index[idx] for idx, _, _ in runnable if idx in results_by_index]


def _poll_tag_segment_subprocess(product_id: str, segment_id: str, index: int, timeout_seconds: int, force: bool) -> dict[str, Any]:
    _guard_log("segment_step_start", product_id=product_id, step="tag_poll", segment_id=segment_id, timeout_seconds=timeout_seconds, parallel=True)
    res = _run_segment_guard_step("tag_poll", segment_id, timeout_seconds, product_id=product_id, index=index, force=force)
    return res.data if res.success else {"status": "failed", "segment_id": segment_id, "error": res.to_dict()}


def _guard_tag_concurrency() -> int:
    try:
        return max(1, min(4, int(os.environ.get("AUTO_MIXCUT_GUARD_TAG_CONCURRENCY", "4") or "4")))
    except ValueError:
        return 4


def _compute_missing_effective_roles(ctx, product_id: str, source_types: list[str], force_segment_ids: list[str] | None = None) -> Result:
    segments = _segments_for_source_types(ctx, product_id, source_types)
    stale_index = _build_stale_index(ctx, segments)
    skill = EffectiveRoleSkill(ctx)
    results = []
    attempted = 0
    limit = _guard_effective_role_limit()
    force_set = set(force_segment_ids or [])
    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        if _is_guard_failed_segment(segment):
            results.append({"segment_id": segment_id, "skipped": True, "reason": str(segment.get("segment_status") or "guard_failed")})
            continue
        if segment.get("effective_roles_updated_at") and segment_id not in force_set:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "effective_roles_exist"})
            continue
        if not _has_tag(ctx, segment_id, stale_index):
            results.append({"segment_id": segment_id, "skipped": True, "reason": "tag_missing"})
            continue
        if limit > 0 and attempted >= limit:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "outside_effective_role_batch"})
            continue
        attempted += 1
        _guard_log("segment_step_start", product_id=product_id, step="effective_roles", segment_id=segment_id, attempt=attempted, limit=limit)
        try:
            with _guard_timeout(_guard_effective_role_timeout()):
                res = skill.compute_segment(segment_id)
        except _GuardTimeout:
            _mark_segment_guard_failed(ctx, segment_id, "effective_role_failed", f"effective roles timed out after {_guard_effective_role_timeout()}s")
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "EFFECTIVE_ROLE_TIMEOUT", "timeout_seconds": _guard_effective_role_timeout()})
            _guard_log("segment_step_timeout", product_id=product_id, step="effective_roles", segment_id=segment_id, timeout_seconds=_guard_effective_role_timeout())
            continue
        except Exception as exc:
            results.append({"segment_id": segment_id, "status": "warning", "error_code": "EFFECTIVE_ROLE_EXCEPTION", "error": str(exc)})
            _guard_log("segment_step_exception", product_id=product_id, step="effective_roles", segment_id=segment_id, error=str(exc)[:300])
            continue
        results.append(res.to_dict())
        if not res.success:
            results[-1]["status"] = "warning"
            _guard_log("segment_step_failed", product_id=product_id, step="effective_roles", segment_id=segment_id, error=(res.error.message if res.error else "")[:300])
            continue
        _guard_log("segment_step_done", product_id=product_id, step="effective_roles", segment_id=segment_id)
    _update_latest_task(ctx, product_id, {"task_status": "EFFECTIVE_ROLES_COMPUTED"})
    warnings = [item for item in results if item.get("status") == "warning"]
    return Result.ok({"count": len(results), "attempted_count": attempted, "warning_count": len(warnings), "results": results, "source_types": source_types})


def _ai_generation_qc_missing(ctx, product_id: str) -> Result:
    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (product_id,))
    limit = _guard_ai_stage_limit()
    attempted = passed = failed = 0
    results = []
    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        status = str(segment.get("segment_status") or "")
        if _is_guard_failed_segment(segment):
            results.append({"segment_id": segment_id, "skipped": True, "reason": status or "guard_failed"})
            continue
        if status in {"qc_passed", "qc_failed", "frame_sample_failed", "frame_sample_timeout", "fingerprint_failed", "tag_failed"}:
            results.append({"segment_id": segment_id, "skipped": True, "reason": status or "qc_exists"})
            continue
        if limit > 0 and attempted >= limit:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "outside_ai_qc_batch"})
            continue
        attempted += 1
        ok, issues = _basic_qc(segment)
        next_status = "qc_passed" if ok else "qc_failed"
        ctx.repo.update("segments", "segment_id", segment_id, {"segment_status": next_status})
        if ok:
            passed += 1
        else:
            failed += 1
        results.append({"segment_id": segment_id, "segment_status": next_status, "issues": issues})
    _update_latest_task(ctx, product_id, {"task_status": "AI_GENERATION_QC_COMPLETED"})
    return Result.ok({"product_id": product_id, "checked": attempted, "passed": passed, "failed": failed, "results": results})


def _ai_consistency_missing(ctx, product_id: str) -> Result:
    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (product_id,))
    limit = _guard_ai_stage_limit()
    timeout = _guard_ai_stage_timeout()
    attempted = 0
    results = []
    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        if _is_guard_failed_segment(segment):
            results.append({"segment_id": segment_id, "skipped": True, "reason": str(segment.get("segment_status") or "guard_failed")})
            continue
        if segment.get("frame_consistency_status"):
            results.append({"segment_id": segment_id, "skipped": True, "reason": "consistency_exists"})
            continue
        if limit > 0 and attempted >= limit:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "outside_consistency_batch"})
            continue
        attempted += 1
        _guard_log("segment_step_start", product_id=product_id, step="consistency", segment_id=segment_id, attempt=attempted, limit=limit)
        try:
            if _guard_segment_subprocess_enabled():
                res = _run_segment_guard_step("consistency", segment_id, timeout)
            else:
                with _guard_timeout(timeout):
                    res = AIGeneratedConsistencySkill(ctx).check_segment(segment_id)
        except _GuardTimeout:
            _mark_segment_guard_failed(ctx, segment_id, "ai_stage_failed", f"consistency timed out after {timeout}s")
            res = Result.ok({"status": "failed", "segment_id": segment_id, "error_code": "CONSISTENCY_TIMEOUT", "timeout_seconds": timeout})
            _guard_log("segment_step_timeout", product_id=product_id, step="consistency", segment_id=segment_id, timeout_seconds=timeout)
        results.append(res.to_dict())
    _update_latest_task(ctx, product_id, {"task_status": "CONSISTENCY_CHECKED"})
    return Result.ok({"checked_segments": attempted, "results": results})


def _ai_anchor_check_missing(ctx, product_id: str) -> Result:
    segments = ctx.repo.list_where(
        "segments",
        "product_id=? AND source_type='ai_generated' AND (segment_status='qc_passed' OR (segment_status='ai_stage_failed' AND effective_roles_reason LIKE ?))",
        (product_id, "ai anchor check timed out%"),
    )
    limit = _guard_ai_stage_limit()
    timeout = _guard_ai_stage_timeout()
    attempted = 0
    results = []
    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        if segment.get("anchor_match_level"):
            results.append({"segment_id": segment_id, "skipped": True, "reason": "anchor_check_exists"})
            continue
        if limit > 0 and attempted >= limit:
            results.append({"segment_id": segment_id, "skipped": True, "reason": "outside_anchor_check_batch"})
            continue
        attempted += 1
        _guard_log("segment_step_start", product_id=product_id, step="ai_anchor_check", segment_id=segment_id, attempt=attempted, limit=limit)
        try:
            with _guard_timeout(timeout):
                res = AIAnchorCheckSkill(ctx).check_segment(segment_id)
        except _GuardTimeout:
            _mark_segment_guard_failed(ctx, segment_id, "ai_stage_failed", f"ai anchor check timed out after {timeout}s")
            res = Result.ok({"status": "failed", "segment_id": segment_id, "error_code": "AI_ANCHOR_CHECK_TIMEOUT", "timeout_seconds": timeout})
            _guard_log("segment_step_timeout", product_id=product_id, step="ai_anchor_check", segment_id=segment_id, timeout_seconds=timeout)
        results.append(res.to_dict())
        if res.success and str(segment.get("segment_status") or "") == "ai_stage_failed":
            data = res.data or {}
            if str(data.get("anchor_match_level") or "") != "fail":
                ctx.repo.update("segments", "segment_id", segment_id, {"segment_status": "qc_passed"})
    _update_latest_task(ctx, product_id, {"task_status": "AI_ANCHOR_CHECKED"})
    return Result.ok({"product_id": product_id, "checked": attempted, "results": results})


def _guard_ai_stage_limit() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_GUARD_AI_STAGE_LIMIT", "10") or "10"))
    except ValueError:
        return 10


def _guard_ai_stage_timeout() -> int:
    try:
        return max(10, int(os.environ.get("AUTO_MIXCUT_GUARD_AI_STAGE_TIMEOUT", "45") or "45"))
    except ValueError:
        return 45


def _guard_effective_role_limit() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_LIMIT", "20") or "20"))
    except ValueError:
        return 20


def _guard_effective_role_timeout() -> int:
    try:
        return max(5, int(os.environ.get("AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_TIMEOUT", "20") or "20"))
    except ValueError:
        return 20


def _guard_tag_total_timeout() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_TAG_TOTAL_TIMEOUT_SEC", "600") or "600"))
    except ValueError:
        return 600


def _is_guard_failed_segment(segment: dict[str, Any]) -> bool:
    return str(segment.get("segment_status") or "") in {
        "frame_sample_failed",
        "frame_sample_timeout",
        "fingerprint_failed",
        "tag_failed",
        "ai_stage_failed",
        "effective_role_failed",
    }


def _mark_segment_guard_failed(ctx, segment_id: str, status: str, reason: str) -> None:
    ctx.repo.update(
        "segments",
        "segment_id",
        segment_id,
        {
            "segment_status": status,
            "effective_roles_json": [],
            "effective_roles_updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            "effective_roles_reason": reason[:500],
        },
    )


def _guard_segment_subprocess_enabled() -> bool:
    return os.environ.get("AUTO_MIXCUT_GUARD_SEGMENT_SUBPROCESS", "1").strip().lower() not in {"0", "false", "no", "off"}


def _run_segment_guard_step(step: str, segment_id: str, timeout_seconds: int, product_id: str = "", index: int = 0, force: bool = False) -> Result:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_segment_guard_step.py"),
        "--step",
        step,
        "--segment-id",
        segment_id,
    ]
    if product_id:
        cmd.extend(["--product-id", product_id])
    if index:
        cmd.extend(["--index", str(index)])
    if force:
        cmd.append("--force")
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    deadline = time.monotonic() + max(1, timeout_seconds)
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.2)
    if proc.poll() is None:
        _kill_process_group(proc)
        raise _GuardTimeout(f"{step} timed out after {timeout_seconds}s")
    try:
        stdout, stderr = proc.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        _kill_process_group(proc)
        raise _GuardTimeout(f"{step} output collection timed out after subprocess exit")

    payload = _parse_result_json(stdout)
    if not payload:
        message = (stderr or stdout or f"{step} subprocess returned {proc.returncode}").strip()
        return Result.fail(f"{step.upper()}_SUBPROCESS_FAILED", message[:1000], {"segment_id": segment_id, "returncode": proc.returncode})
    if payload.get("success"):
        return Result.ok(payload.get("data") or {})
    error = payload.get("error") or {}
    return Result.fail(
        str(error.get("code") or f"{step.upper()}_FAILED"),
        str(error.get("message") or f"{step} failed"),
        {"segment_id": segment_id, "subprocess": payload},
    )


def _kill_process_group(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    try:
        proc.communicate(timeout=2)
    except Exception:
        pass


def _parse_result_json(stdout: str) -> dict[str, Any]:
    text = (stdout or "").strip()
    if text:
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass
    for line in reversed((stdout or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            return data
    return {}


def _guard_log(event: str, **payload: Any) -> None:
    payload = {"event": event, **payload}
    print(json.dumps(payload, ensure_ascii=False, default=str), file=sys.stderr, flush=True)


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
    stale = _stale_segment_summary(ctx, segments, _build_stale_index(ctx, segments))
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
    base_values = {
        "pipeline_status": status,
        "next_action": next_action,
        "last_error": last_error,
        "last_batch_id": last_batch_id,
    }
    first = ctx.repo.update(
        "content_tasks",
        "task_id",
        task["task_id"],
        {**base_values, "guard_detail_json": _compact_guard_detail(detail)},
    )
    if first.success:
        return
    ctx.repo.update("content_tasks", "task_id", task["task_id"], base_values)


def _update_latest_task(ctx, product_id: str, values: dict[str, Any]) -> Result:
    task = _latest_task(ctx, product_id)
    if not task:
        return Result.fail("TASK_NOT_FOUND", "task not found", {"product_id": product_id})
    return ctx.repo.update("content_tasks", "task_id", task["task_id"], values)


def _compact_guard_detail(value: Any, depth: int = 0) -> Any:
    if depth > 5:
        return str(value)[:500]
    if isinstance(value, dict):
        return {str(k): _compact_guard_detail(v, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        kept = [_compact_guard_detail(item, depth + 1) for item in value[:20]]
        if len(value) > 20:
            kept.append({"truncated_count": len(value) - 20})
        return kept
    if isinstance(value, str) and len(value) > 2000:
        return value[:2000] + "...[truncated]"
    return value


def _latest_task(ctx, product_id: str) -> dict | None:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return rows[0] if rows else None


if __name__ == "__main__":
    raise SystemExit(main())
