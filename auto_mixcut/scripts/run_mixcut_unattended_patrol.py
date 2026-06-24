#!/usr/bin/env python3
"""Lightweight self-healing patrol for unattended auto_mixcut runs.

This script does not render new outputs. It reconciles state so the scanner can
choose the right next worker on the next pass.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.ai_supplement_gateway_skill import AISupplementGatewaySkill  # noqa: E402
from auto_mixcut.skills.capacity_counter_skill import CapacityCounterSkill  # noqa: E402
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill  # noqa: E402
from auto_mixcut.skills.feishu_review_skill import sync_product_task_best_effort  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402
from scripts.run_mixcut_guard import _ai_effective_roles_need_recompute, _build_stale_index  # noqa: E402


RUNNABLE_STATUSES = {"", "RUNNING", "READY_TO_CONTINUE", "WAITING_AI_RETURN", "BLOCKED", "ERROR"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Unattended patrol for auto_mixcut state reconciliation.")
    parser.add_argument("--product-id", default="")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--skip-feishu-status-sync", action="store_true")
    parser.add_argument("--skip-feishu-task-sync", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps({"success": False, "stage": "init_db", "error": init.to_dict()}, ensure_ascii=False, indent=2, default=str))
        return 1

    product_ids = _candidate_product_ids(ctx, args.product_id, args.limit)
    results = [patrol_product(ctx, product_id, args) for product_id in product_ids]
    payload = {"success": all(item.get("success", True) for item in results), "count": len(results), "results": results}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if payload["success"] else 1


def patrol_product(ctx: Any, product_id: str, args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {"success": True, "product_id": product_id, "steps": []}
    if not args.skip_feishu_status_sync:
        sync = _sync_prompt_package_returns(product_id, dry_run=args.dry_run)
        result["steps"].append({"step": "sync_prompt_package_returns", **sync})

    repaired = _repair_empty_ai_roles(ctx, product_id, dry_run=args.dry_run)
    result["steps"].append({"step": "repair_empty_ai_roles", **repaired})

    if args.dry_run:
        capacity = _capacity_snapshot(ctx, product_id)
        result["steps"].append({"step": "refresh_capacity", "success": True, "dry_run": True, "data": capacity})
    else:
        capacity = CapacityCounterSkill(ctx).refresh_product(product_id)
        result["steps"].append({"step": "refresh_capacity", **capacity.to_dict()})

    normalized = _normalize_waiting_state(ctx, product_id, dry_run=args.dry_run)
    result["steps"].append({"step": "normalize_waiting_state", **normalized})

    package_state = AISupplementGatewaySkill(ctx).package_state(product_id)
    result["package_state"] = package_state
    task = _latest_task(ctx, product_id) or {}
    result["task_state"] = {
        key: task.get(key)
        for key in (
            "task_id",
            "requested_variant_count",
            "actual_variant_count",
            "target_remaining_variant_count",
            "material_pool_extra_capacity",
            "pipeline_status",
            "next_action",
            "last_error",
        )
    }

    if not args.skip_feishu_task_sync and not args.dry_run:
        result["task_sync"] = sync_product_task_best_effort(ctx, product_id)
    return result


def _candidate_product_ids(ctx: Any, product_id: str, limit: int) -> list[str]:
    if product_id:
        return [product_id]
    rows = ctx.repo.list_where(
        "content_tasks",
        "pipeline_status IN ('RUNNING','READY_TO_CONTINUE','WAITING_AI_RETURN','BLOCKED','ERROR') OR target_remaining_variant_count > 0 ORDER BY updated_at DESC LIMIT ?",
        (max(1, limit),),
    )
    seen: set[str] = set()
    product_ids: list[str] = []
    for row in rows:
        pid = str(row.get("product_id") or "").strip()
        if pid and pid not in seen:
            seen.add(pid)
            product_ids.append(pid)
    return product_ids


def _sync_prompt_package_returns(product_id: str, dry_run: bool) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "process_prompt_package_returns.py"),
        "--product-id",
        product_id,
        "--no-postprocess",
    ]
    if dry_run:
        cmd.append("--dry-run")
    return _run(cmd, timeout=_env_int("AUTO_MIXCUT_PATROL_SYNC_TIMEOUT", 300))


def _repair_empty_ai_roles(ctx: Any, product_id: str, dry_run: bool) -> dict[str, Any]:
    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (product_id,))
    stale_index = _build_stale_index(ctx, segments)
    candidates = [
        str(segment.get("segment_id") or "")
        for segment in segments
        if _ai_effective_roles_need_recompute(ctx, segment, stale_index)
    ]
    if dry_run:
        return {"success": True, "dry_run": True, "candidate_count": len(candidates), "segment_ids": candidates[:20]}

    skill = EffectiveRoleSkill(ctx)
    repaired = []
    failed = []
    for segment_id in candidates:
        res = skill.compute_segment(segment_id)
        if res.success:
            repaired.append(res.data)
        else:
            failed.append(res.to_dict())
    return {
        "success": not failed,
        "candidate_count": len(candidates),
        "repaired_count": len(repaired),
        "failed_count": len(failed),
        "repaired": repaired[:20],
        "failed": failed[:20],
    }


def _normalize_waiting_state(ctx: Any, product_id: str, dry_run: bool) -> dict[str, Any]:
    task = _latest_task(ctx, product_id)
    if not task:
        return {"success": True, "skipped": True, "reason": "task_not_found"}
    try:
        remaining = int(task.get("target_remaining_variant_count") or 0)
        extra_capacity = int(task.get("material_pool_extra_capacity") or 0)
    except (TypeError, ValueError):
        return {"success": True, "skipped": True, "reason": "capacity_unavailable"}
    if str(task.get("pipeline_status") or "") != "WAITING_AI_RETURN" or remaining <= 0 or extra_capacity < remaining:
        return {"success": True, "skipped": True, "reason": "no_state_change", "remaining": remaining, "material_pool_extra_capacity": extra_capacity}
    patch = {"pipeline_status": "READY_TO_CONTINUE", "next_action": "RUN_GUARD_AGAIN", "last_error": ""}
    if dry_run:
        return {"success": True, "dry_run": True, "patch": patch, "remaining": remaining, "material_pool_extra_capacity": extra_capacity}
    write = ctx.repo.update("content_tasks", "task_id", task["task_id"], patch)
    return {"success": write.success, "patch": patch, "remaining": remaining, "material_pool_extra_capacity": extra_capacity, "write": write.to_dict()}


def _latest_task(ctx: Any, product_id: str) -> dict[str, Any] | None:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))
    return rows[0] if rows else None


def _capacity_snapshot(ctx: Any, product_id: str) -> dict[str, Any]:
    task = _latest_task(ctx, product_id) or {}
    return {
        "product_id": product_id,
        "actual_variant_count": task.get("actual_variant_count"),
        "target_remaining_variant_count": task.get("target_remaining_variant_count"),
        "material_pool_extra_capacity": task.get("material_pool_extra_capacity"),
        "first_slot_remaining_capacity": task.get("first_slot_remaining_capacity"),
        "current_bottleneck": task.get("current_bottleneck"),
        "capacity_note": task.get("capacity_note"),
    }


def _run(cmd: list[str], timeout: int) -> dict[str, Any]:
    env = os.environ.copy()
    env.setdefault("AUTO_MIXCUT_DB_PROVIDER", "mysql")
    env.setdefault("AUTO_MIXCUT_OSS_PROVIDER", "aliyun")
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), env=env, text=True, capture_output=True, timeout=timeout)
        return {"success": proc.returncode == 0, "status": "ok" if proc.returncode == 0 else "failed", "returncode": proc.returncode, "stdout": (proc.stdout or "")[-4000:], "stderr": (proc.stderr or "")[-2000:]}
    except subprocess.TimeoutExpired as exc:
        return {"success": False, "status": "timeout", "timeout_seconds": timeout, "stdout": (exc.stdout or "")[-2000:], "stderr": (exc.stderr or "")[-1000:]}


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)) or default)
    except ValueError:
        return default


if __name__ == "__main__":
    raise SystemExit(main())
