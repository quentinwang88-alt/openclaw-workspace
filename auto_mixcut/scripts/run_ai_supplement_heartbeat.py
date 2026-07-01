#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.ai_supplement_scheduler_skill import (  # noqa: E402
    approve_pending_products,
    approve_product,
    pending_ai_supplement_products,
    request_daytime_batch_approval,
    should_submit_now,
)
from auto_mixcut.skills.ai_supplement_cycle_skill import AISupplementCycleSkill  # noqa: E402
from auto_mixcut.skills.ai_supplement_gateway_skill import AISupplementGatewaySkill, normalize_package, submit_budget_from_state  # noqa: E402
from auto_mixcut.skills.product_run_lock_skill import ProductRunLockSkill, default_product_run_owner  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="AI supplement heartbeat for auto_mixcut.")
    parser.add_argument("--mode", choices=["daytime", "nightly", "scan"], default="scan")
    parser.add_argument("--product-id", default="", help="Only process one product.")
    parser.add_argument("--approve-product", default="", help="Approve one product and optionally run immediately.")
    parser.add_argument("--approve-all", action="store_true", help="Approve all pending daytime AI supplement requests.")
    parser.add_argument("--run-now", action="store_true", help="After approval, submit immediately.")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-result-uploader", action="store_true")
    parser.add_argument("--skip-guard", action="store_true")
    parser.add_argument("--max-nightly-passes", type=int, default=_timeout("AUTO_MIXCUT_AI_HEARTBEAT_NIGHTLY_MAX_PASSES", 5))
    args = parser.parse_args()

    ctx = build_context()
    RDSRepositorySkill(ctx).init_db()
    results: list[dict[str, Any]] = []

    if args.approve_all:
        if args.dry_run:
            approved = {"success": True, "dry_run": True, "would_approve": "all_pending"}
        else:
            approved = approve_pending_products(ctx).to_dict()
        item: dict[str, Any] = {"approve_all": approved}
        if args.run_now and approved.get("success"):
            pending_after_approval = pending_ai_supplement_products(ctx)
            if args.product_id:
                pending_after_approval = [row for row in pending_after_approval if row["product_id"] == args.product_id]
            item["submit"] = [submit_product(ctx, row["product_id"], dry_run=args.dry_run) for row in pending_after_approval[: max(1, args.limit)]]
        results.append(item)
        print(json.dumps({"success": all(_ok(item) for item in results), "results": results}, ensure_ascii=False, indent=2, default=str))
        return 0

    if args.approve_product:
        if args.dry_run:
            approved = {"success": True, "dry_run": True, "product_id": args.approve_product, "would_set_status": "approved"}
        else:
            approved = approve_product(ctx, args.approve_product).to_dict()
        item = {"product_id": args.approve_product, "approve": approved}
        if args.run_now and approved.get("success"):
            item["submit"] = submit_product(ctx, args.approve_product, dry_run=args.dry_run)
        results.append(item)
        print(json.dumps({"success": all(_ok(item) for item in results), "results": results}, ensure_ascii=False, indent=2, default=str))
        return 0

    pending = pending_ai_supplement_products(ctx)
    if args.product_id:
        pending = [item for item in pending if item["product_id"] == args.product_id]
    pending = pending[: max(1, args.limit)]

    if args.mode == "scan":
        print(json.dumps({"success": True, "pending_count": len(pending), "products": pending}, ensure_ascii=False, indent=2, default=str))
        return 0

    if args.mode == "daytime":
        approval_items = [
            item
            for item in pending
            if not should_submit_now(ctx, item["product_id"], "daytime")
            and int(_budget_from_pending(item).get("submit_limit") or 0) > 0
        ]
        if args.dry_run:
            results.append(
                {
                    "status": "would_request_batch_approval",
                    "product_count": len(approval_items),
                    "products": [
                        {
                            "product_id": item["product_id"],
                            "remaining_count": item.get("remaining_count"),
                            "ready_to_submit_count": item.get("ready_to_submit_count"),
                            "inflight_count": item.get("inflight_count"),
                            "budget": _budget_from_pending(item),
                        }
                        for item in approval_items
                    ],
                }
            )
        else:
            results.append(request_daytime_batch_approval(ctx, approval_items))
        print(json.dumps({"success": all(_ok(item) for item in results), "mode": args.mode, "processed_count": len(results), "results": results}, ensure_ascii=False, indent=2, default=str))
        return 0

    for item in pending:
        results.append(process_nightly_product(ctx, item["product_id"], args))

    print(json.dumps({"success": all(_ok(item) for item in results), "mode": args.mode, "processed_count": len(results), "results": results}, ensure_ascii=False, indent=2, default=str))
    return 0


def process_nightly_product(ctx: Any, product_id: str, args: argparse.Namespace) -> dict[str, Any]:
    if args.dry_run:
        cycle = AISupplementCycleSkill(ctx).run_once(
            product_id,
            submit=True,
            recover=not args.skip_result_uploader,
            import_returns=True,
            run_guard=not args.skip_guard,
            dry_run=True,
        )
        return {
            "product_id": product_id,
            "status": "nightly_planned",
            "max_passes": max(1, int(args.max_nightly_passes or 1)),
            "cycle": cycle.to_dict(),
        }

    lock_owner = os.environ.get("AUTO_MIXCUT_RUN_LOCK_OWNER") or default_product_run_owner("ai_heartbeat")
    lock_data = {"acquired": True, "release_on_exit": False}
    if not _product_run_lock_disabled():
        lock = ProductRunLockSkill(ctx).acquire(product_id, owner=lock_owner, ttl_minutes=_timeout("AUTO_MIXCUT_LOCK_TTL_MINUTES", 60))
        if not lock.success:
            return {"product_id": product_id, "status": "failed", "reason": "lock_failed", "lock": lock.to_dict()}
        lock_data = lock.data or {}
        if not lock_data.get("acquired"):
            return {"product_id": product_id, "status": "skipped", "reason": "locked", "lock": lock_data}

    passes: list[dict[str, Any]] = []
    max_passes = max(1, int(args.max_nightly_passes or 1))
    final_state: dict[str, Any] = {}
    stop_reason = "max_passes_reached"
    try:
        cycle_skill = AISupplementCycleSkill(ctx)
        for pass_index in range(1, max_passes + 1):
            cycle = cycle_skill.run_once(
                product_id,
                submit=should_submit_now(ctx, product_id, "nightly"),
                recover=not args.skip_result_uploader,
                import_returns=True,
                run_guard=not args.skip_guard,
                dry_run=False,
                lock_owner=lock_owner,
                submit_fn=submit_product,
                recover_fn=recover_product_results,
                import_returns_fn=import_product_returns,
                guard_fn=run_guard_once,
            )
            cycle_data = cycle.data or {}
            pass_result: dict[str, Any] = {"pass": pass_index, "cycle": cycle.to_dict()}
            pass_result.update(cycle_data.get("steps") or {})
            final_state = cycle_data.get("state_after") or _task_state(ctx, product_id)
            pass_result["state_after"] = final_state
            passes.append(pass_result)
            if not cycle.success:
                stop_reason = "cycle_failed"
                break
            if not cycle_data.get("continue_recommended"):
                stop_reason = str(cycle_data.get("reason") or cycle_data.get("cycle_status") or "stable_no_more_immediate_work")
                break
        return {
            "product_id": product_id,
            "status": "nightly_completed" if stop_reason != "max_passes_reached" else "nightly_max_passes_reached",
            "stop_reason": stop_reason,
            "pass_count": len(passes),
            "final_state": final_state,
            "passes": passes,
        }
    finally:
        if lock_data.get("release_on_exit"):
            ProductRunLockSkill(ctx).release(product_id, owner=lock_owner)


def submit_product(ctx: Any, product_id: str, dry_run: bool = False) -> dict[str, Any]:
    pending = [item for item in pending_ai_supplement_products(ctx) if item["product_id"] == product_id]
    if pending:
        budget = _budget_from_pending(pending[0])
    else:
        budget = AISupplementGatewaySkill(ctx).submit_budget(product_id, remaining_count=1, configured_limit=1)
    if int(budget.get("submit_limit") or 0) <= 0:
        result = {
            "success": True,
            "status": "skipped",
            "reason": budget.get("submit_block_reason") or "ai_submit_no_ready_prompt_package",
            "product_id": product_id,
            **budget,
        }
        if not dry_run:
            result["status_sync_after_submit"] = import_product_returns(product_id, dry_run=False)
            _mark_waiting_return_after_submit(ctx, product_id, result)
        return result
    cmd = _submit_command(product_id, budget)
    result = _run(cmd, cwd=WORKSPACE / "skills" / "jimeng-video-generator", dry_run=dry_run, timeout=_timeout("AUTO_MIXCUT_AI_HEARTBEAT_SUBMIT_TIMEOUT", 900), env_extra={"IMINI_ALLOW_REAL_SUBMIT": "1"})
    if not dry_run:
        result["status_sync_after_submit"] = import_product_returns(product_id, dry_run=False)
        _mark_waiting_return_after_submit(ctx, product_id, result)
    return result


def _mark_waiting_return_after_submit(ctx: Any, product_id: str, submit_result: dict[str, Any]) -> None:
    state = AISupplementGatewaySkill(ctx).package_state(product_id)
    if int(state.get("inflight_count") or 0) <= 0:
        return
    if int(state.get("ready_to_submit_count") or 0) > 0:
        return
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    task = rows[0] if rows else None
    if not task:
        return
    ctx.repo.update(
        "content_tasks",
        "task_id",
        task["task_id"],
        {
            "task_status": "AI_SUPPLEMENT_CREATED",
            "pipeline_status": "WAITING_AI_RETURN",
            "next_action": "WAIT_AI_SEGMENT_RETURN",
            "last_error": "",
            "ai_supplement_status": "submitted",
            "ai_supplement_package_count": int(state.get("inflight_count") or task.get("ai_supplement_package_count") or 0),
        },
    )


def recover_product_results(ctx: Any, product_id: str, dry_run: bool = False) -> dict[str, Any]:
    packages = ctx.repo.list_where("segment_prompt_packages", "product_id=?", (product_id,))
    actionable = [
        row for row in packages
        if normalize_package(row) in {"inflight", "recoverable_failed"}
    ]
    task_names = [str(row.get("segment_prompt_id") or "") for row in _sort_recoverable_packages(actionable) if row.get("segment_prompt_id")]
    if not task_names:
        return {"status": "skipped", "reason": "no_inflight_or_recoverable_prompt_packages", "package_count": len(packages)}
    cmd = [
        "node",
        str(WORKSPACE / "skills" / "jimeng-video-generator" / "result-uploader.js"),
        "--config",
        str(WORKSPACE / "skills" / "jimeng-video-generator" / "segment-package.json"),
        "--channel",
        "imini",
        "--ignore-generating-count",
        "--force-asset-read",
        "--asset-scan-batches=1",
        "--max-asset-candidates=20",
        "--disable-asset-cursor",
        "--task-name",
        ",".join(task_names[:20]),
    ]
    return _run(cmd, cwd=WORKSPACE / "skills" / "jimeng-video-generator", dry_run=dry_run, timeout=_timeout("AUTO_MIXCUT_AI_HEARTBEAT_RECOVER_TIMEOUT", 900))


def _sort_recoverable_packages(packages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(row: dict[str, Any]) -> tuple[int, str]:
        normalized = normalize_package(row)
        priority = 0 if normalized == "inflight" else 1
        timestamp = str(row.get("updated_at") or row.get("created_at") or "")
        return (priority, timestamp)

    return sorted(packages, key=key)


def import_product_returns(product_id: str, dry_run: bool = False) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "process_prompt_package_returns.py"),
        "--product-id",
        product_id,
        "--no-postprocess",
    ]
    if dry_run:
        cmd.append("--dry-run")
    return _run(cmd, cwd=ROOT, dry_run=False, timeout=_timeout("AUTO_MIXCUT_AI_HEARTBEAT_IMPORT_TIMEOUT", 300))


def run_guard_once(product_id: str, dry_run: bool = False, lock_owner: str = "") -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_mixcut_guard.py"),
        "--product-id",
        product_id,
        "--max-rounds",
        "1",
        "--skip-upload-sync",
    ]
    env_extra = {"AUTO_MIXCUT_RUN_LOCK_OWNER": lock_owner} if lock_owner else None
    return _run(cmd, cwd=ROOT, dry_run=dry_run, timeout=_timeout("AUTO_MIXCUT_AI_HEARTBEAT_GUARD_TIMEOUT", 1200), env_extra=env_extra)


def _task_state(ctx: Any, product_id: str) -> dict[str, Any]:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    task = rows[0] if rows else {}
    pending = [item for item in pending_ai_supplement_products(ctx) if item["product_id"] == product_id]
    package_state = pending[0] if pending else AISupplementGatewaySkill(ctx).package_state(product_id)
    return {
        "task_id": task.get("task_id"),
        "target_count": task.get("requested_variant_count"),
        "actual_count": task.get("actual_variant_count"),
        "remaining_count": task.get("target_remaining_variant_count"),
        "task_status": task.get("task_status"),
        "pipeline_status": task.get("pipeline_status"),
        "next_action": task.get("next_action"),
        "last_error": task.get("last_error"),
        "ai_supplement_status": task.get("ai_supplement_status"),
        "material_pool_extra_capacity": task.get("material_pool_extra_capacity"),
        "first_slot_remaining_capacity": task.get("first_slot_remaining_capacity"),
        "ready_to_submit_count": package_state.get("ready_to_submit_count", 0),
        "inflight_count": package_state.get("inflight_count", 0),
        "imported_package_count": package_state.get("imported_package_count", 0),
    }


def _submit_command(product_id: str, budget: dict[str, Any]) -> list[str]:
    limit = max(1, int(budget.get("submit_limit") or budget.get("ready_to_submit_count") or budget.get("remaining_count") or 1))
    needed = max(1, int(budget.get("target_remaining") or budget.get("remaining_count") or limit))
    channel = str(os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_SUBMIT_CHANNEL") or "Imini").strip()
    command = [
        "node",
        str(WORKSPACE / "skills" / "jimeng-video-generator" / "segment-package-worker.js"),
        "--submit-only",
        f"--product-id={product_id}",
        f"--limit={limit}",
        f"--max-submit-needed={needed}",
    ]
    if channel:
        command.append(f"--channel={channel}")
    submit_slot_role = str(budget.get("submit_slot_role") if "submit_slot_role" in budget else budget.get("priority_role") or "").strip()
    if submit_slot_role:
        command.append(f"--slot-role={submit_slot_role}")
    return command


def _budget_from_pending(item: dict[str, Any]) -> dict[str, Any]:
    remaining = max(1, int(item.get("remaining_count") or 1))
    return submit_budget_from_state(remaining, item, priority_role=str(item.get("priority_role") or ""))


def _run(cmd: list[str], cwd: Path, dry_run: bool, timeout: int, env_extra: dict[str, str] | None = None) -> dict[str, Any]:
    if dry_run:
        return {"dry_run": True, "command": " ".join(cmd), "cwd": str(cwd)}
    env = os.environ.copy()
    env.update(env_extra or {})
    env.setdefault("AUTO_MIXCUT_DB_PROVIDER", "mysql")
    env.setdefault("AUTO_MIXCUT_OSS_PROVIDER", "aliyun")
    env.setdefault("IMINI_ALLOW_REAL_SUBMIT", "1")
    _ensure_tool_path(env)
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    try:
        proc = subprocess.run(cmd, cwd=str(cwd), env=env, text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        return {"status": "timeout", "timeout_seconds": timeout, "command": " ".join(cmd), "stdout": (exc.stdout or "")[-2000:], "stderr": (exc.stderr or "")[-2000:]}
    return {
        "status": "ok" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "command": " ".join(cmd),
        "stdout": (proc.stdout or "")[-4000:],
        "stderr": (proc.stderr or "")[-2000:],
    }


def _timeout(name: str, default: int) -> int:
    try:
        return max(30, int(os.environ.get(name, str(default)) or default))
    except ValueError:
        return default


def _product_run_lock_disabled() -> bool:
    return str(os.environ.get("AUTO_MIXCUT_DISABLE_PRODUCT_RUN_LOCK") or "0").strip().lower() in {"1", "true", "yes", "on"}


def _ensure_tool_path(env: dict[str, str]) -> None:
    extra = [
        str(Path.home() / ".local" / "bin"),
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ]
    current = env.get("PATH") or ""
    parts = [item for item in current.split(os.pathsep) if item]
    for item in reversed(extra):
        if item not in parts:
            parts.insert(0, item)
    env["PATH"] = os.pathsep.join(parts)


def _ok(item: dict[str, Any]) -> bool:
    if item.get("status") in {"failed", "timeout"}:
        return False
    for value in item.values():
        if isinstance(value, dict) and value.get("status") in {"failed", "timeout"}:
            return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
