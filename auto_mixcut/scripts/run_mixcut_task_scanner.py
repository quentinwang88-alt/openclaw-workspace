#!/usr/bin/env python3
"""Scan Feishu product tasks and run auto_mixcut guarded passes.

This is intentionally a light scheduler:
- Feishu is the human trigger/status board.
- RDS is the source of truth for pipeline state.
- ADS factory products keep running short guard passes until they reach a
  terminal state, while Feishu is synced once at the end.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient, datetime_cell  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.feishu_review_skill import sync_product_task_best_effort  # noqa: E402
from auto_mixcut.skills.mixcut_state_machine_skill import decide_factory_state, decide_mixcut_state  # noqa: E402
from auto_mixcut.skills.product_run_lock_skill import ProductRunLockSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402


ACTIVE_FEISHU_STATES = {"待开始", "生产中", "可混剪", "运行中", "等待AI补素材", "等待AI回流"}
HARD_SKIP_FEISHU_STATES = {"不处理", "暂停"}
DONE_FEISHU_STATES = {"已完成", "完成"}
SKIP_FEISHU_STATES = {*HARD_SKIP_FEISHU_STATES, *DONE_FEISHU_STATES}
PRIORITY_RANK = {"urgent": 0, "high": 1, "normal": 2, "low": 3, "": 4}
DEFAULT_MIXCUT_USE_CASE = "投流混剪"
MIXCUT_FACTORY_TIERS = {20, 40, 60, 80}
MIXCUT_TIER_FIELDS = ("投流混剪档位", "混剪档位", "目标混剪档位", "混剪工厂档位")
TASK_STATE_FIELDS = ("任务状态", "混剪任务状态", "混剪状态")


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan 商品内容任务表 and run auto_mixcut tasks.")
    parser.add_argument("--product-id", default="", help="Only run one product.")
    parser.add_argument("--max-products", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_MAX_PRODUCTS", 2))
    parser.add_argument("--max-workers", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_MAX_WORKERS", 2))
    parser.add_argument("--guard-timeout", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_GUARD_TIMEOUT", 1800))
    parser.add_argument("--patrol-timeout", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_PATROL_TIMEOUT", 600))
    parser.add_argument("--skip-patrol", action="store_true", help="Skip unattended preflight patrol before guard/AI heartbeat.")
    parser.add_argument("--lock-ttl-minutes", type=int, default=_env_int("AUTO_MIXCUT_LOCK_TTL_MINUTES", 60))
    parser.add_argument("--retry-backoff-minutes", type=int, default=_env_int("AUTO_MIXCUT_RETRY_BACKOFF_MINUTES", 30))
    parser.add_argument("--max-ads-passes", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_MAX_ADS_PASSES", 12))
    parser.add_argument("--max-no-progress-passes", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_MAX_NO_PROGRESS_PASSES", 2))
    parser.add_argument("--no-ads-auto-continue", action="store_true", help="Run only one ADS pass even when RDS asks for RUN_GUARD_AGAIN.")
    parser.add_argument("--loop", action="store_true", help="Keep scanning forever.")
    parser.add_argument("--interval-seconds", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_INTERVAL_SECONDS", 7200))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.loop:
        while True:
            run = run_once(args)
            print(json.dumps(run, ensure_ascii=False, indent=2, default=str))
            time.sleep(max(60, int(args.interval_seconds or 7200)))

    run = run_once(args)
    print(json.dumps(run, ensure_ascii=False, indent=2, default=str))
    return 0 if run.get("success") else 1


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        return {"success": False, "stage": "init_db", "error": init.to_dict()}

    candidates = scan_candidates(ctx, product_id=args.product_id)
    selected = candidates[: max(1, int(args.max_products or 1))]
    if args.dry_run:
        return {"success": True, "dry_run": True, "candidate_count": len(candidates), "selected": selected}

    results: list[dict[str, Any]] = []
    workers = max(1, min(int(args.max_workers or 1), len(selected) or 1))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(run_candidate, item, args) for item in selected]
        for future in as_completed(futures):
            results.append(future.result())

    return {
        "success": all(item.get("success", True) for item in results),
        "candidate_count": len(candidates),
        "processed_count": len(results),
        "results": sorted(results, key=lambda item: str(item.get("product_id") or "")),
    }


def scan_candidates(ctx: Any, product_id: str = "") -> list[dict[str, Any]]:
    client = AutoMixcutFeishuClient("商品内容任务表")
    records = client.list_records(limit=500)
    now = datetime.utcnow()
    items: list[dict[str, Any]] = []

    for record in records:
        fields = record.fields or {}
        pid = _text(fields.get("商品ID"))
        if not pid or (product_id and pid != product_id):
            continue
        shop_id = _text(fields.get("店铺ID")) or _text(fields.get("店铺"))

        task = _latest_task(ctx, pid)
        product = ctx.repo.get("products", "product_id", pid) or {}
        quantity_goal = _mixcut_quantity_goal(fields, task)
        target = int(quantity_goal.get("target") or 0)
        target_increased = _target_exceeds_completed_task(task, target)
        state = _task_state(fields)
        if not _feishu_state_allows_scan(state, target_increased):
            continue
        if _next_retry_in_future(task, now):
            continue
        if not state and not _rds_needs_scanner(task) and not target_increased:
            continue

        item = {
            "record_id": getattr(record, "record_id", ""),
            "product_id": pid,
            "product_name": _text(fields.get("商品名称")) or str(product.get("product_name") or ""),
            "market": _text(fields.get("市场")) or str(product.get("market") or ""),
                "category": _text(fields.get("归一类目")) or _text(fields.get("类目")) or str(product.get("category") or ""),
            "shop_id": shop_id or str(product.get("shop_id") or ""),
            "target": target,
            "goal_mode": quantity_goal.get("goal_mode", "absolute_target"),
            "factory_tier": quantity_goal.get("factory_tier", 0),
            "mixcut_use_case": _mixcut_use_case(fields),
            "priority": _text(fields.get("优先级")) or str(product.get("priority") or "normal"),
            "feishu_state": state,
            "pipeline_status": str((task or {}).get("pipeline_status") or ""),
            "next_action": str((task or {}).get("next_action") or ""),
            "task_id": str((task or {}).get("task_id") or ""),
            "created_at": str((task or {}).get("created_at") or ""),
        }
        items.append(item)

    return _dedupe_candidates(sorted(items, key=_candidate_sort_key))


def run_candidate(item: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    ctx = build_context()
    product_id = str(item.get("product_id") or "")
    shop_id = str(item.get("shop_id") or "")
    task = _latest_task(ctx, product_id)
    target = _int(item.get("target"))
    if _task_done(task) and not _target_exceeds_completed_task(task, target):
        finished_at = datetime.utcnow()
        _update_feishu_record(item.get("record_id"), _feishu_status_fields(task, finished_at))
        return {
            "success": True,
            "product_id": product_id,
            "status": "skipped",
            "reason": "already_done",
        }
    field_issue = _candidate_field_issue(item)
    if field_issue:
        finished_at = datetime.utcnow()
        _update_feishu_record(
            item.get("record_id"),
            {
                "混剪任务状态": "阻断需人工处理",
                "任务状态": "异常",
                "工厂状态": "异常",
                "混剪最后运行时间": datetime_cell(finished_at),
                "混剪阻断原因": field_issue,
                "当前问题": field_issue,
                "处理建议": "补齐商品名称、市场、归一类目和投流混剪档位后重试",
                "异常等级": "阻塞",
                "最近更新时间": datetime_cell(finished_at),
            },
        )
        return {
            "success": True,
            "product_id": product_id,
            "status": "skipped",
            "reason": "missing_required_fields",
            "message": field_issue,
        }
    owner = _owner()
    lock = acquire_lock(ctx, product_id, shop_id, owner, ttl_minutes=int(args.lock_ttl_minutes or 60))
    if not lock.get("acquired"):
        return {"success": True, "product_id": product_id, "status": "skipped", "reason": "locked", "lock": lock}

    started_at = datetime.utcnow()
    try:
        # Avoid blocking production on a best-effort UI status write.
        patrol = {"status": "skipped", "reason": "skip_patrol"}
        if not args.skip_patrol:
            patrol = _run_with_lock_owner(_patrol_command(product_id), timeout=int(args.patrol_timeout or 600), owner=owner)
            if patrol.get("status") != "ok":
                finished_at = datetime.utcnow()
                _update_feishu_record(
                    item.get("record_id"),
                    {
                        "混剪任务状态": "阻断需人工处理",
                        "任务状态": "异常",
                        "工厂状态": "异常",
                        "混剪最后运行时间": datetime_cell(finished_at),
                        "混剪阻断原因": f"patrol_{patrol.get('status')}",
                        "当前问题": f"patrol_{patrol.get('status')}",
                        "处理建议": "查看巡检日志后重试",
                        "异常等级": "阻塞",
                        "最近更新时间": datetime_cell(finished_at),
                    },
                )
                return {
                    "success": False,
                    "product_id": product_id,
                    "mode": "patrol",
                    "command": " ".join(_patrol_command(product_id)),
                    "process": patrol,
                }

        ctx = build_context()
        task_after_patrol = _latest_task(ctx, product_id)
        if _task_done(task_after_patrol) and not _target_exceeds_completed_task(task_after_patrol, target):
            finished_at = datetime.utcnow()
            _update_feishu_record(item.get("record_id"), _feishu_status_fields(task_after_patrol, finished_at))
            return {
                "success": True,
                "product_id": product_id,
                "mode": "patrol",
                "status": "skipped",
                "reason": "done_after_patrol",
                "patrol": patrol,
            }
        if task_after_patrol:
            item = {
                **item,
                "pipeline_status": str(task_after_patrol.get("pipeline_status") or ""),
                "next_action": str(task_after_patrol.get("next_action") or ""),
            }
        if _should_run_ai_return_heartbeat(ctx, item):
            command = _ai_return_command(product_id)
            mode = "ai_return_heartbeat"
        elif _is_ads_mixcut(item):
            return _run_ads_candidate_until_settled(item, args, patrol, owner)
        else:
            command = _guard_command(item)
            mode = "guard"

        _mark_task_started(ctx, product_id, owner, started_at)
        proc = _run_with_lock_owner(command, timeout=int(args.guard_timeout or 1800), owner=owner)
        finished_at = datetime.utcnow()
        ctx = build_context()
        _mark_task_finished(ctx, product_id, proc, finished_at, retry_backoff_minutes=int(args.retry_backoff_minutes or 30))
        if mode == "ads_mixcut":
            sync = {"status": "skipped", "reason": "ads_full_run_handles_task_state"}
        else:
            sync = sync_product_task_best_effort(ctx, product_id)
        task = _latest_task(ctx, product_id)
        _update_feishu_record(item.get("record_id"), _feishu_status_fields(task, finished_at))
        return {
            "success": proc["status"] == "ok",
            "product_id": product_id,
            "mode": mode,
            "command": " ".join(command),
            "patrol": patrol,
            "process": proc,
            "sync": sync,
        }
    finally:
        release_lock(build_context(), product_id, shop_id, owner)


def _run_ads_candidate_until_settled(item: dict[str, Any], args: argparse.Namespace, patrol: dict[str, Any], owner: str) -> dict[str, Any]:
    product_id = str(item.get("product_id") or "")
    shop_id = str(item.get("shop_id") or "")
    target = int(item.get("target") or 0)
    max_passes = 1 if getattr(args, "no_ads_auto_continue", False) else max(1, int(getattr(args, "max_ads_passes", 1) or 1))
    max_no_progress = max(1, int(getattr(args, "max_no_progress_passes", 2) or 2))
    pass_results: list[dict[str, Any]] = []
    terminal_reason = "not_started"
    last_actual = _task_actual_count(_latest_task(build_context(), product_id))
    no_progress_streak = 0

    for pass_no in range(1, max_passes + 1):
        ctx = build_context()
        before_task = _latest_task(ctx, product_id)
        if _ads_target_met(before_task, target):
            terminal_reason = "target_met_before_pass"
            break

        command = _ads_command(item, args)
        started_at = datetime.utcnow()
        _mark_task_started(ctx, product_id, owner, started_at)
        proc = _run_with_lock_owner(command, timeout=int(args.guard_timeout or 1800), owner=owner)
        finished_at = datetime.utcnow()

        ctx = build_context()
        _mark_task_finished(ctx, product_id, proc, finished_at, retry_backoff_minutes=int(args.retry_backoff_minutes or 30))
        after_task = _latest_task(ctx, product_id)
        pass_result = {
            "pass_no": pass_no,
            "command": " ".join(command),
            "process": proc,
            "before": _task_progress_summary(before_task, target),
            "after": _task_progress_summary(after_task, target),
            "factory_target_count": target,
            "pass_target_count": _extract_pass_target(proc) or _task_actual_count(after_task),
        }
        pass_results.append(pass_result)

        embedded_failure = _proc_embedded_failure(proc)
        if proc.get("status") != "ok":
            terminal_reason = str(proc.get("status") or "process_failed")
            break
        if embedded_failure:
            terminal_reason = "embedded_failure"
            break
        if _ads_target_met(after_task, target):
            terminal_reason = "target_met"
            break
        if not _ads_should_continue(after_task, target):
            terminal_reason = _ads_stop_reason(after_task, target)
            break

        actual = _task_actual_count(after_task)
        if actual <= last_actual:
            no_progress_streak += 1
        else:
            no_progress_streak = 0
        last_actual = max(last_actual, actual)
        if no_progress_streak >= max_no_progress:
            terminal_reason = "no_progress"
            break
    else:
        terminal_reason = "max_ads_passes_reached"

    finished_at = datetime.utcnow()
    ctx = build_context()
    final_task = _latest_task(ctx, product_id)
    sync = _sync_product_task_final(ctx, product_id, item.get("record_id"), final_task, finished_at)
    failure_reason = next((str(item.get("process", {}).get("status")) for item in pass_results if item.get("process", {}).get("status") != "ok"), "")
    success = not failure_reason and terminal_reason not in {"embedded_failure"}
    return {
        "success": success,
        "product_id": product_id,
        "mode": "ads_mixcut",
        "status": "completed" if _ads_target_met(final_task, target) else "in_progress",
        "terminal_reason": terminal_reason,
        "factory_target_count": target,
        "final_effective_count": _task_actual_count(final_task),
        "remaining_to_factory_target": max(0, target - _task_actual_count(final_task)) if target > 0 else _task_remaining_count(final_task),
        "pass_count": len(pass_results),
        "max_ads_passes": max_passes,
        "patrol": patrol,
        "passes": pass_results,
        "final_task": _task_progress_summary(final_task, target),
        "sync": sync,
    }


def _candidate_field_issue(item: dict[str, Any]) -> str:
    missing: list[str] = []
    if not str(item.get("product_name") or "").strip():
        missing.append("商品名称")
    if not str(item.get("market") or "").strip():
        missing.append("市场")
    if not str(item.get("category") or "").strip():
        missing.append("归一类目/类目")
    try:
        target = int(item.get("target") or 0)
    except (TypeError, ValueError):
        target = 0
    if target <= 0:
        missing.append("目标混剪档位/目标混剪数量/目标生成数量")
    if not missing:
        return ""
    return "缺少必填字段：" + "、".join(missing) + "，无法创建/续跑混剪任务"


def acquire_lock(ctx: Any, product_id: str, shop_id: str, owner: str, ttl_minutes: int) -> dict[str, Any]:
    res = ProductRunLockSkill(ctx).acquire(product_id, owner=owner, shop_id=shop_id, ttl_minutes=ttl_minutes)
    if res.success:
        return res.data or {"acquired": False, "error": "empty_lock_result"}
    return {"acquired": False, "owner": owner, "error": res.error.message if res.error else "lock_failed"}


def release_lock(ctx: Any, product_id: str, shop_id: str, owner: str) -> None:
    try:
        ProductRunLockSkill(ctx).release(product_id, owner=owner)
    except Exception:
        pass


def _guard_command(item: dict[str, Any]) -> list[str]:
    # max-rounds=3：让新商品在一个 scanner 周期内多跑几轮（probe→segment→tag→render），
    # 避免 --max-rounds 1 时新商品要等好几个 2 小时周期才出成片。
    cmd = [sys.executable, str(ROOT / "scripts" / "run_mixcut_guard.py"), "--product-id", str(item["product_id"]), "--max-rounds", "3"]
    if item.get("target"):
        cmd.extend(["--target", str(int(item["target"]))])
    if item.get("product_name"):
        cmd.extend(["--name", str(item["product_name"])])
    if item.get("market"):
        cmd.extend(["--market", str(item["market"])])
    if item.get("category"):
        cmd.extend(["--category", str(item["category"])])
    return cmd


def _ads_command(item: dict[str, Any], args: argparse.Namespace) -> list[str]:
    timeout_minutes = max(10, int(int(args.guard_timeout or 1800) / 60) - 5)
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_ads_mixcut_unattended.py"),
        "--product-id",
        str(item["product_id"]),
        "--full-run",
        "--write",
        "--wait-timeout-minutes",
        "5",
        "--return-scan-timeout-minutes",
        "10",
        "--render-timeout-minutes",
        str(timeout_minutes),
    ]
    if str(item.get("goal_mode") or "") == "factory_tier" and int(item.get("factory_tier") or 0) in MIXCUT_FACTORY_TIERS:
        cmd.extend(["--goal-mode", "factory_tier", "--factory-tier", str(int(item.get("factory_tier") or 0))])
    else:
        cmd.extend(["--target-count", str(int(item.get("target") or 0))])
    return cmd


def _ai_return_command(product_id: str) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "scripts" / "run_ai_supplement_heartbeat.py"),
        "--mode",
        "nightly",
        "--product-id",
        product_id,
        "--max-nightly-passes",
        "1",
    ]


def _patrol_command(product_id: str) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "scripts" / "run_mixcut_unattended_patrol.py"),
        "--product-id",
        product_id,
        "--skip-feishu-task-sync",
    ]


def _run(cmd: list[str], timeout: int, env_extra: dict[str, str] | None = None) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(env_extra or {})
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    env.setdefault("AUTO_MIXCUT_DB_PROVIDER", "mysql")
    env.setdefault("AUTO_MIXCUT_OSS_PROVIDER", "aliyun")
    if Path(cmd[1]).name == "run_ads_mixcut_unattended.py":
        env["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
        env["AUTO_MIXCUT_SKIP_INLINE_FEISHU_SYNC"] = "1"
    _ensure_tool_path(env)
    try:
        proc = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True, timeout=timeout)
        return {
            "status": "ok" if proc.returncode == 0 else "failed",
            "returncode": proc.returncode,
            "stdout": (proc.stdout or "")[-6000:],
            "stderr": (proc.stderr or "")[-3000:],
        }
    except subprocess.TimeoutExpired as exc:
        return {"status": "timeout", "timeout_seconds": timeout, "stdout": (exc.stdout or "")[-3000:], "stderr": (exc.stderr or "")[-3000:]}


def _run_with_lock_owner(cmd: list[str], timeout: int, owner: str) -> dict[str, Any]:
    try:
        return _run(cmd, timeout=timeout, env_extra={"AUTO_MIXCUT_RUN_LOCK_OWNER": owner})
    except TypeError as exc:
        if "env_extra" not in str(exc):
            raise
        return _run(cmd, timeout=timeout)


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


def _mark_task_started(ctx: Any, product_id: str, owner: str, started_at: datetime) -> None:
    task = _latest_task(ctx, product_id)
    if not task:
        return
    attempts = int(task.get("run_attempt_count") or 0) + 1
    ctx.repo.update(
        "content_tasks",
        "task_id",
        task["task_id"],
        {
            "pipeline_status": "RUNNING",
            "next_action": "GUARD_PASS_STARTED",
            "last_error": "",
            "run_attempt_count": attempts,
            "last_guard_started_at": _db_ts(ctx, started_at),
            "scanner_owner": owner,
        },
    )


def _mark_task_finished(ctx: Any, product_id: str, proc: dict[str, Any], finished_at: datetime, retry_backoff_minutes: int) -> None:
    task = _latest_task(ctx, product_id)
    if not task:
        return
    attempts = max(1, int(task.get("run_attempt_count") or 0))
    patch: dict[str, Any] = {"last_guard_finished_at": _db_ts(ctx, finished_at), "run_attempt_count": attempts}
    embedded_failure = _proc_embedded_failure(proc)
    if proc.get("status") in {"failed", "timeout"} or embedded_failure:
        status = "timeout" if proc.get("status") == "timeout" else "failed"
        reason = _proc_failure_reason(proc, embedded_failure)
        patch.update(
            {
                "pipeline_status": "ERROR" if status == "failed" else "BLOCKED",
                "next_action": "CHECK_PIPELINE_LOG",
                "last_error": reason,
                "next_retry_at": _db_ts(ctx, finished_at + timedelta(minutes=max(5, retry_backoff_minutes))),
                "guard_detail_json": proc,
            }
        )
    ctx.repo.update("content_tasks", "task_id", task["task_id"], patch)


def _proc_embedded_failure(proc: dict[str, Any]) -> dict[str, Any] | None:
    payload = _proc_stdout_json(proc)
    if not isinstance(payload, dict):
        return None
    return _find_failed_status(payload)


def _proc_stdout_json(proc: dict[str, Any]) -> dict[str, Any] | None:
    stdout = str(proc.get("stdout") or "").strip()
    if not stdout:
        return None
    try:
        value = json.loads(stdout)
    except Exception:
        return None
    return value if isinstance(value, dict) else None


def _find_failed_status(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        status = str(value.get("status") or "").strip().lower()
        if status in {"failed", "blocked", "timeout"}:
            return value
        for item in value.values():
            found = _find_failed_status(item)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_failed_status(item)
            if found:
                return found
    return None


def _proc_failure_reason(proc: dict[str, Any], embedded_failure: dict[str, Any] | None = None) -> str:
    if proc.get("status") == "timeout":
        return "scanner_timeout"
    if embedded_failure:
        error = embedded_failure.get("error") or embedded_failure.get("reason") or embedded_failure.get("status") or "embedded_failure"
        return str(error)[:200]
    stderr = str(proc.get("stderr") or "").strip()
    if stderr:
        return stderr.splitlines()[-1][:200]
    return f"scanner_{proc.get('status') or 'failed'}"


def _feishu_status_fields(task: dict[str, Any] | None, finished_at: datetime) -> dict[str, Any]:
    task = task or {}
    state = _display_state(task)
    simplified = _simplified_task_state(state)
    return {
        "混剪任务状态": state,
        "任务状态": simplified,
        "工厂状态": _factory_state(state),
        "混剪最后运行时间": datetime_cell(finished_at),
        "混剪阻断原因": task.get("last_error") or task.get("blocked_reason") or task.get("failure_reason") or "",
        "当前问题": _scanner_problem(task, state),
        "处理建议": _scanner_advice(task, state),
        "异常等级": "阻塞" if state == "阻断需人工处理" else "无",
        "最近更新时间": datetime_cell(finished_at),
        "下次重试时间": datetime_cell(task.get("next_retry_at")),
    }


def _display_state(task: dict[str, Any]) -> str:
    return decide_mixcut_state(task).display_state


def _simplified_task_state(display_state: str) -> str:
    if display_state == "已完成":
        return "完成"
    if display_state == "阻断需人工处理":
        return "异常"
    if display_state in {"运行中", "等待AI补素材", "等待AI回流"}:
        return "生产中"
    return "生产中" if display_state else ""


def _factory_state(display_state: str) -> str:
    if display_state == "已完成":
        return "完成"
    if display_state == "阻断需人工处理":
        return "异常"
    if display_state == "等待AI回流":
        return "等待AI回流"
    if display_state == "等待AI补素材":
        return "等待素材"
    if display_state == "运行中":
        return "生产中"
    return ""


def _scanner_problem(task: dict[str, Any], display_state: str) -> str:
    if display_state == "阻断需人工处理":
        return str(task.get("last_error") or task.get("blocked_reason") or task.get("failure_reason") or "流程阻断需人工处理")
    if display_state == "等待AI回流":
        return "等待AI片段生成回流"
    if display_state == "等待AI补素材":
        return "等待AI补素材任务处理"
    return ""


def _scanner_advice(task: dict[str, Any], display_state: str) -> str:
    if display_state == "阻断需人工处理":
        return "查看当前问题和RDS运行日志后重试"
    if display_state == "等待AI回流":
        return "等待worker回流或运行AI回流巡检"
    if display_state == "等待AI补素材":
        return "等待补素材worker处理"
    return ""


def _update_feishu_record(record_id: Any, fields: dict[str, Any]) -> None:
    record_id = str(record_id or "")
    if not record_id:
        return
    try:
        client = AutoMixcutFeishuClient("商品内容任务表")
        existing = {field.field_name for field in client.client.list_fields()}
        payload = {key: value for key, value in fields.items() if key in existing and value not in (None, {})}
        if payload:
            client.update_record(record_id, payload)
    except Exception:
        return


def _latest_task(ctx: Any, product_id: str) -> dict[str, Any] | None:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))
    return rows[0] if rows else None


def _task_done(task: dict[str, Any] | None) -> bool:
    return decide_mixcut_state(task).is_done


def _task_actual_count(task: dict[str, Any] | None) -> int:
    return _int((task or {}).get("actual_variant_count"))


def _task_remaining_count(task: dict[str, Any] | None) -> int:
    return _int((task or {}).get("target_remaining_variant_count"))


def _task_progress_summary(task: dict[str, Any] | None, factory_target: int = 0) -> dict[str, Any]:
    task = task or {}
    actual = _task_actual_count(task)
    requested = _int(task.get("requested_variant_count")) or int(factory_target or 0)
    remaining = _task_remaining_count(task)
    if remaining <= 0 and requested > actual:
        remaining = max(0, requested - actual)
    return {
        "task_id": task.get("task_id"),
        "requested_variant_count": requested,
        "factory_target_count": int(factory_target or requested or 0),
        "actual_variant_count": actual,
        "target_remaining_variant_count": remaining,
        "pipeline_status": task.get("pipeline_status"),
        "next_action": task.get("next_action"),
        "task_status": task.get("task_status"),
        "last_error": task.get("last_error") or task.get("blocked_reason") or task.get("failure_reason") or "",
        "last_batch_id": task.get("last_batch_id") or "",
    }


def _ads_target_met(task: dict[str, Any] | None, target: int) -> bool:
    if not task:
        return False
    actual = _task_actual_count(task)
    remaining = _task_remaining_count(task)
    if int(target or 0) > 0:
        return actual >= int(target)
    return str(task.get("pipeline_status") or "") == "DONE" and remaining <= 0


def _ads_should_continue(task: dict[str, Any] | None, target: int) -> bool:
    if not task or _ads_target_met(task, target):
        return False
    decision = decide_factory_state(task, facts={"target_count": target})
    return decision.should_continue_ads_loop


def _ads_stop_reason(task: dict[str, Any] | None, target: int) -> str:
    if _ads_target_met(task, target):
        return "target_met"
    if not task:
        return "task_missing"
    decision = decide_factory_state(task, facts={"target_count": target})
    return decision.stable_reason or "settled"


def _extract_pass_target(proc: dict[str, Any]) -> int:
    payload = _proc_stdout_json(proc)
    if not isinstance(payload, dict):
        return 0
    render = payload.get("render") if isinstance(payload.get("render"), dict) else {}
    return _int(render.get("guard_target_count") or render.get("pass_target_count"))


def _sync_product_task_final(ctx: Any, product_id: str, record_id: Any, task: dict[str, Any] | None, finished_at: datetime) -> dict[str, Any]:
    if os.environ.get("AUTO_MIXCUT_SCANNER_SKIP_FINAL_FEISHU_SYNC", "").strip().lower() in {"1", "true", "yes", "on"}:
        return {"status": "skipped", "reason": "final_feishu_sync_disabled"}
    previous = os.environ.pop("AUTO_MIXCUT_SKIP_INLINE_FEISHU_SYNC", None)
    try:
        product_sync = sync_product_task_best_effort(ctx, product_id)
    finally:
        if previous is not None:
            os.environ["AUTO_MIXCUT_SKIP_INLINE_FEISHU_SYNC"] = previous
    board_fields = _feishu_status_fields(task, finished_at)
    _update_feishu_record(record_id, board_fields)
    return {
        "status": "completed",
        "product_task_sync": product_sync,
        "board_status_sync": {"status": "attempted", "record_id": str(record_id or "")},
    }


def _target_exceeds_completed_task(task: dict[str, Any] | None, target: int) -> bool:
    if not task or int(target or 0) <= 0:
        return False
    current_target = _int(task.get("requested_variant_count")) or _int(task.get("allowed_variant_count"))
    actual_count = _int(task.get("actual_variant_count"))
    return int(target) > max(current_target, actual_count)


def _rds_needs_scanner(task: dict[str, Any] | None) -> bool:
    if not task:
        return False
    return decide_factory_state(task).should_scan_from_rds


def _should_run_ai_return_heartbeat(ctx: Any, item: dict[str, Any]) -> bool:
    task = _latest_task(ctx, str(item.get("product_id") or ""))
    state = str(item.get("feishu_state") or "")
    fallback = {
        "pipeline_status": item.get("pipeline_status"),
        "next_action": item.get("next_action"),
        "task_status": item.get("task_status"),
    }
    return decide_factory_state(task or fallback, feishu_state=state, facts={"target_count": item.get("target")}).scanner_mode == "ai_return_heartbeat"


def _next_retry_in_future(task: dict[str, Any] | None, now: datetime) -> bool:
    if not task or not task.get("next_retry_at"):
        return False
    try:
        retry_at = datetime.fromisoformat(str(task["next_retry_at"]).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return False
    return retry_at > now


def _candidate_sort_key(item: dict[str, Any]) -> tuple[int, int, str, str]:
    state = str(item.get("feishu_state") or "")
    rds_waiting = decide_factory_state(item, feishu_state=state, facts={"target_count": item.get("target")}).scanner_mode == "ai_return_heartbeat"
    state_rank = 0 if state == "可混剪" else 1 if state in ACTIVE_FEISHU_STATES or rds_waiting else 2 if not state else 3
    priority_rank = PRIORITY_RANK.get(str(item.get("priority") or "").lower(), 4)
    target_rank = -int(item.get("target") or 0)
    return (priority_rank, state_rank, target_rank, str(item.get("created_at") or ""), str(item.get("product_id") or ""))


def _dedupe_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for item in items:
        key = str(item.get("product_id") or "")
        if key not in selected:
            selected[key] = item
    return list(selected.values())


def _mixcut_use_case(fields: dict[str, Any]) -> str:
    value = _text(fields.get("混剪用途"))
    return value or DEFAULT_MIXCUT_USE_CASE


def _task_state(fields: dict[str, Any]) -> str:
    return _first_field_text(fields, TASK_STATE_FIELDS)


def _feishu_state_allows_scan(state: str, target_increased: bool = False) -> bool:
    state = str(state or "").strip()
    if state in HARD_SKIP_FEISHU_STATES:
        return False
    if state in DONE_FEISHU_STATES:
        return bool(target_increased)
    if state and state not in ACTIVE_FEISHU_STATES:
        return bool(target_increased)
    return True


def _mixcut_quantity_goal(fields: dict[str, Any], task: dict[str, Any] | None = None) -> dict[str, Any]:
    tier = _parse_factory_tier(_first_field_text(fields, MIXCUT_TIER_FIELDS))
    if tier:
        return {"goal_mode": "factory_tier", "target": tier, "factory_tier": tier}
    target = _int(fields.get("目标混剪数量")) or _int(fields.get("目标生成数量")) or int((task or {}).get("requested_variant_count") or 0)
    return {"goal_mode": "absolute_target", "target": target, "factory_tier": 0}


def _first_field_text(fields: dict[str, Any], names: tuple[str, ...]) -> str:
    for name in names:
        value = _text(fields.get(name))
        if value:
            return value
    return ""


def _parse_factory_tier(value: Any) -> int:
    text = _text(value)
    if not text:
        return 0
    direct = _int(text)
    if direct in MIXCUT_FACTORY_TIERS:
        return direct
    digits = "".join(ch if ch.isdigit() else " " for ch in text).split()
    for item in digits:
        tier = _int(item)
        if tier in MIXCUT_FACTORY_TIERS:
            return tier
    return 0


def _is_ads_mixcut(item: dict[str, Any]) -> bool:
    value = str(item.get("mixcut_use_case") or DEFAULT_MIXCUT_USE_CASE).strip().lower()
    return value not in {"普通混剪", "normal", "standard", "ordinary", "regular"}


def _owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _db_ts(ctx: Any, value: datetime) -> str:
    if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value.isoformat(timespec="seconds")


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "\n".join(_text(item) for item in value if _text(item)).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url"):
            if _text(value.get(key)):
                return _text(value.get(key))
    return str(value).strip()


def _int(value: Any) -> int:
    text = _text(value)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except ValueError:
        return default


if __name__ == "__main__":
    raise SystemExit(main())
