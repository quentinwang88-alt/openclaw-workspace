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
        return {
            "product_id": product_id,
            "status": "nightly_planned",
            "max_passes": max(1, int(args.max_nightly_passes or 1)),
            "submit": submit_product(ctx, product_id, dry_run=True),
            "recover": {"dry_run": bool(not args.skip_result_uploader)},
            "import": import_product_returns(product_id, dry_run=True),
            "guard": run_guard_once(product_id, dry_run=True) if not args.skip_guard else {"status": "skipped", "reason": "skip_guard"},
        }

    passes: list[dict[str, Any]] = []
    max_passes = max(1, int(args.max_nightly_passes or 1))
    final_state: dict[str, Any] = {}
    stop_reason = "max_passes_reached"
    for pass_index in range(1, max_passes + 1):
        pass_result: dict[str, Any] = {"pass": pass_index}
        if should_submit_now(ctx, product_id, "nightly"):
            pass_result["submit"] = submit_product(ctx, product_id, dry_run=False)
        if not args.skip_result_uploader:
            pass_result["recover"] = recover_product_results(ctx, product_id, dry_run=False)
        pass_result["import"] = import_product_returns(product_id, dry_run=False)
        if not args.skip_guard:
            pass_result["guard"] = run_guard_once(product_id, dry_run=False)
        final_state = _task_state(ctx, product_id)
        pass_result["state_after"] = final_state
        passes.append(pass_result)
        if not _should_continue_nightly(ctx, product_id, final_state, pass_result):
            stop_reason = _stable_reason(ctx, product_id, final_state)
            break
    return {
        "product_id": product_id,
        "status": "nightly_completed" if stop_reason != "max_passes_reached" else "nightly_max_passes_reached",
        "stop_reason": stop_reason,
        "pass_count": len(passes),
        "final_state": final_state,
        "passes": passes,
    }


def submit_product(ctx: Any, product_id: str, dry_run: bool = False) -> dict[str, Any]:
    pending = [item for item in pending_ai_supplement_products(ctx) if item["product_id"] == product_id]
    budget = _budget_from_pending(pending[0] if pending else {"remaining_count": 1, "ready_to_submit_count": 1})
    if int(budget.get("submit_limit") or 0) <= 0:
        return {
            "success": True,
            "status": "skipped",
            "reason": "ai_submit_inflight_capacity_filled",
            "product_id": product_id,
            **budget,
        }
    cmd = _submit_command(product_id, budget)
    return _run(cmd, cwd=WORKSPACE / "skills" / "jimeng-video-generator", dry_run=dry_run, timeout=_timeout("AUTO_MIXCUT_AI_HEARTBEAT_SUBMIT_TIMEOUT", 900), env_extra={"IMINI_ALLOW_REAL_SUBMIT": "1"})


def recover_product_results(ctx: Any, product_id: str, dry_run: bool = False) -> dict[str, Any]:
    packages = ctx.repo.list_where("segment_prompt_packages", "product_id=?", (product_id,))
    task_names = [str(row.get("segment_prompt_id") or "") for row in packages if row.get("segment_prompt_id")]
    if not task_names:
        return {"status": "skipped", "reason": "no_prompt_packages"}
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


def run_guard_once(product_id: str, dry_run: bool = False) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_mixcut_guard.py"),
        "--product-id",
        product_id,
        "--max-rounds",
        "1",
        "--skip-upload-sync",
    ]
    return _run(cmd, cwd=ROOT, dry_run=dry_run, timeout=_timeout("AUTO_MIXCUT_AI_HEARTBEAT_GUARD_TIMEOUT", 1200))


def _task_state(ctx: Any, product_id: str) -> dict[str, Any]:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    task = rows[0] if rows else {}
    pending = [item for item in pending_ai_supplement_products(ctx) if item["product_id"] == product_id]
    package_state = pending[0] if pending else {}
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
        "ready_to_submit_count": package_state.get("ready_to_submit_count", 0),
        "inflight_count": package_state.get("inflight_count", 0),
        "imported_package_count": package_state.get("imported_package_count", 0),
    }


def _should_continue_nightly(ctx: Any, product_id: str, state: dict[str, Any], pass_result: dict[str, Any]) -> bool:
    if str(state.get("pipeline_status") or "") in {"DONE", "BLOCKED"}:
        return False
    if int(state.get("remaining_count") or 0) <= 0:
        return False
    next_action = str(state.get("next_action") or "")
    pipeline_status = str(state.get("pipeline_status") or "")
    ready = int(state.get("ready_to_submit_count") or 0)
    inflight = int(state.get("inflight_count") or 0)
    if next_action in {"RUN_GUARD_AGAIN", "RUN_AI_SEGMENT_WORKER", "WAIT_AI_SUPPLEMENT_APPROVAL"}:
        return True
    if pipeline_status == "READY_TO_CONTINUE":
        return True
    if pipeline_status == "WAITING_AI_RETURN":
        return ready > 0 and inflight <= 0
    imported = _imported_count(pass_result.get("import") or {})
    if imported > 0:
        return True
    return False


def _stable_reason(ctx: Any, product_id: str, state: dict[str, Any]) -> str:
    pipeline_status = str(state.get("pipeline_status") or "")
    next_action = str(state.get("next_action") or "")
    if pipeline_status == "DONE" or int(state.get("remaining_count") or 0) <= 0:
        return "done"
    if pipeline_status == "BLOCKED":
        return "blocked"
    if pipeline_status == "WAITING_AI_RETURN":
        if int(state.get("inflight_count") or 0) > 0:
            return "waiting_ai_return"
        if next_action == "WAIT_AI_SUPPLEMENT_APPROVAL":
            return "waiting_ai_supplement_approval"
    return "stable_no_more_immediate_work"


def _imported_count(result: dict[str, Any]) -> int:
    payload = _json_from_stdout(str(result.get("stdout") or ""))
    if not isinstance(payload, dict):
        return 0
    direct = payload.get("imported_count")
    if direct is not None:
        try:
            return int(direct or 0)
        except (TypeError, ValueError):
            return 0
    nested = payload.get("import") or {}
    try:
        return int(nested.get("count") or nested.get("imported_count") or 0)
    except (TypeError, ValueError, AttributeError):
        return 0


def _json_from_stdout(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return None
    for index, char in enumerate(stripped):
        if char not in "[{":
            continue
        try:
            return json.loads(stripped[index:])
        except json.JSONDecodeError:
            continue
    return None


def _submit_command(product_id: str, budget: dict[str, Any]) -> list[str]:
    limit = max(1, int(budget.get("submit_limit") or budget.get("ready_to_submit_count") or budget.get("remaining_count") or 1))
    needed = max(1, int(budget.get("target_remaining") or budget.get("remaining_count") or limit))
    return [
        "node",
        str(WORKSPACE / "skills" / "jimeng-video-generator" / "segment-package-worker.js"),
        "--submit-only",
        f"--product-id={product_id}",
        f"--limit={limit}",
        f"--max-submit-needed={needed}",
    ]


def _budget_from_pending(item: dict[str, Any]) -> dict[str, int]:
    remaining = max(1, int(item.get("remaining_count") or 1))
    ready = int(item.get("ready_to_submit_count") or 0)
    inflight = int(item.get("inflight_count") or 0)
    submit_limit = min(ready, remaining) if ready > 0 else max(0, remaining - inflight)
    return {
        "remaining_count": remaining,
        "target_remaining": remaining,
        "ready_to_submit_count": ready,
        "ai_submit_inflight_count": inflight,
        "submit_limit": max(0, submit_limit),
    }


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
