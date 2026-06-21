#!/usr/bin/env python3
"""Scan Feishu product tasks and run auto_mixcut guarded passes.

This is intentionally a light scheduler:
- Feishu is the human trigger/status board.
- RDS is the source of truth for pipeline state.
- Each product gets one short guard/heartbeat pass per scanner run.
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
from auto_mixcut.skills.mixcut_state_machine_skill import decide_mixcut_state  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402


ACTIVE_FEISHU_STATES = {"可混剪", "运行中", "等待AI补素材", "等待AI回流"}
SKIP_FEISHU_STATES = {"", "不处理", "暂停", "已完成"}
PRIORITY_RANK = {"urgent": 0, "high": 1, "normal": 2, "low": 3, "": 4}


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan 商品内容任务表 and run auto_mixcut tasks.")
    parser.add_argument("--product-id", default="", help="Only run one product.")
    parser.add_argument("--max-products", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_MAX_PRODUCTS", 2))
    parser.add_argument("--max-workers", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_MAX_WORKERS", 2))
    parser.add_argument("--guard-timeout", type=int, default=_env_int("AUTO_MIXCUT_SCANNER_GUARD_TIMEOUT", 1800))
    parser.add_argument("--lock-ttl-minutes", type=int, default=_env_int("AUTO_MIXCUT_LOCK_TTL_MINUTES", 60))
    parser.add_argument("--retry-backoff-minutes", type=int, default=_env_int("AUTO_MIXCUT_RETRY_BACKOFF_MINUTES", 30))
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
    seen: set[tuple[str, str]] = set()

    for record in records:
        fields = record.fields or {}
        pid = _text(fields.get("商品ID"))
        if not pid or (product_id and pid != product_id):
            continue
        state = _text(fields.get("混剪任务状态"))
        if state in SKIP_FEISHU_STATES:
            continue
        if state and state not in ACTIVE_FEISHU_STATES:
            continue
        shop_id = _text(fields.get("店铺ID")) or _text(fields.get("店铺"))
        key = (pid, shop_id or "DEFAULT")
        if key in seen:
            continue
        seen.add(key)

        task = _latest_task(ctx, pid)
        product = ctx.repo.get("products", "product_id", pid) or {}
        if _next_retry_in_future(task, now):
            continue
        if not state and not _rds_needs_scanner(task):
            continue

        target = _int(fields.get("目标混剪数量")) or _int(fields.get("目标生成数量")) or int((task or {}).get("requested_variant_count") or 0)
        item = {
            "record_id": getattr(record, "record_id", ""),
            "product_id": pid,
            "product_name": _text(fields.get("商品名称")) or str(product.get("product_name") or ""),
            "market": _text(fields.get("市场")) or str(product.get("market") or ""),
            "category": _text(fields.get("归一类目")) or _text(fields.get("类目")) or str(product.get("category") or ""),
            "shop_id": shop_id or str(product.get("shop_id") or ""),
            "target": target,
            "priority": _text(fields.get("优先级")) or str(product.get("priority") or "normal"),
            "feishu_state": state,
            "pipeline_status": str((task or {}).get("pipeline_status") or ""),
            "next_action": str((task or {}).get("next_action") or ""),
            "task_id": str((task or {}).get("task_id") or ""),
            "created_at": str((task or {}).get("created_at") or ""),
        }
        items.append(item)

    return sorted(items, key=_candidate_sort_key)


def run_candidate(item: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    ctx = build_context()
    RDSRepositorySkill(ctx).init_db()
    product_id = str(item.get("product_id") or "")
    shop_id = str(item.get("shop_id") or "")
    task = _latest_task(ctx, product_id)
    if _task_done(task):
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
                "混剪最后运行时间": datetime_cell(finished_at),
                "混剪阻断原因": field_issue,
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
        _update_feishu_record(
            item.get("record_id"),
            {
                "混剪任务状态": "运行中",
                "混剪最后运行时间": datetime_cell(started_at),
                "混剪阻断原因": "",
            },
        )
        _mark_task_started(ctx, product_id, owner, started_at)
        if _should_run_ai_return_heartbeat(ctx, item):
            command = _ai_return_command(product_id)
            mode = "ai_return_heartbeat"
        else:
            command = _guard_command(item)
            mode = "guard"

        proc = _run(command, timeout=int(args.guard_timeout or 1800))
        finished_at = datetime.utcnow()
        ctx = build_context()
        _mark_task_finished(ctx, product_id, proc, finished_at, retry_backoff_minutes=int(args.retry_backoff_minutes or 30))
        sync = sync_product_task_best_effort(ctx, product_id)
        task = _latest_task(ctx, product_id)
        _update_feishu_record(item.get("record_id"), _feishu_status_fields(task, finished_at))
        return {
            "success": proc["status"] == "ok",
            "product_id": product_id,
            "mode": mode,
            "command": " ".join(command),
            "process": proc,
            "sync": sync,
        }
    finally:
        release_lock(build_context(), product_id, shop_id, owner)


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
        missing.append("目标混剪数量/目标生成数量")
    if not missing:
        return ""
    return "缺少必填字段：" + "、".join(missing) + "，无法创建/续跑混剪任务"


def acquire_lock(ctx: Any, product_id: str, shop_id: str, owner: str, ttl_minutes: int) -> dict[str, Any]:
    lock_key = _lock_key(product_id, shop_id)
    now = _db_now(ctx)
    expires = _db_ts(ctx, datetime.utcnow() + timedelta(minutes=max(5, ttl_minutes)))
    dialect = getattr(ctx.repo, "dialect", "sqlite")
    try:
        with ctx.repo.connect() as conn:
            if dialect == "mysql":
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM mixcut_task_locks WHERE lock_key=%s AND (expires_at<=%s OR status!='running')", (lock_key, now))
                    cur.execute(
                        """
                        INSERT INTO mixcut_task_locks
                          (lock_key, product_id, shop_id, owner, status, locked_at, expires_at, heartbeat_at, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, 'running', %s, %s, %s, %s, %s)
                        """,
                        (lock_key, product_id, shop_id, owner, now, expires, now, now, now),
                    )
            else:
                conn.execute("DELETE FROM mixcut_task_locks WHERE lock_key=? AND (expires_at<=? OR status!='running')", (lock_key, now))
                conn.execute(
                    """
                    INSERT INTO mixcut_task_locks
                      (lock_key, product_id, shop_id, owner, status, locked_at, expires_at, heartbeat_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?, ?)
                    """,
                    (lock_key, product_id, shop_id, owner, now, expires, now, now, now),
                )
        return {"acquired": True, "lock_key": lock_key, "owner": owner, "expires_at": expires}
    except Exception as exc:
        current = ctx.repo.get("mixcut_task_locks", "lock_key", lock_key) or {}
        return {"acquired": False, "lock_key": lock_key, "owner": owner, "held_by": current.get("owner"), "expires_at": current.get("expires_at"), "error": str(exc)}


def release_lock(ctx: Any, product_id: str, shop_id: str, owner: str) -> None:
    lock_key = _lock_key(product_id, shop_id)
    now = _db_now(ctx)
    try:
        dialect = getattr(ctx.repo, "dialect", "sqlite")
        with ctx.repo.connect() as conn:
            if dialect == "mysql":
                with conn.cursor() as cur:
                    cur.execute("UPDATE mixcut_task_locks SET status='released', updated_at=%s WHERE lock_key=%s AND owner=%s", (now, lock_key, owner))
            else:
                conn.execute("UPDATE mixcut_task_locks SET status='released', updated_at=? WHERE lock_key=? AND owner=?", (now, lock_key, owner))
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


def _run(cmd: list[str], timeout: int) -> dict[str, Any]:
    env = os.environ.copy()
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    env.setdefault("AUTO_MIXCUT_DB_PROVIDER", "mysql")
    env.setdefault("AUTO_MIXCUT_OSS_PROVIDER", "aliyun")
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
    if proc.get("status") in {"failed", "timeout"}:
        patch.update(
            {
                "pipeline_status": "ERROR" if proc.get("status") == "failed" else "BLOCKED",
                "next_action": "CHECK_PIPELINE_LOG",
                "last_error": f"scanner_{proc.get('status')}",
                "next_retry_at": _db_ts(ctx, finished_at + timedelta(minutes=max(5, retry_backoff_minutes))),
                "guard_detail_json": proc,
            }
        )
    ctx.repo.update("content_tasks", "task_id", task["task_id"], patch)


def _feishu_status_fields(task: dict[str, Any] | None, finished_at: datetime) -> dict[str, Any]:
    task = task or {}
    state = _display_state(task)
    return {
        "混剪任务状态": state,
        "混剪最后运行时间": datetime_cell(finished_at),
        "混剪阻断原因": task.get("last_error") or task.get("blocked_reason") or task.get("failure_reason") or "",
        "下次重试时间": datetime_cell(task.get("next_retry_at")),
    }


def _display_state(task: dict[str, Any]) -> str:
    return decide_mixcut_state(task).display_state


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


def _rds_needs_scanner(task: dict[str, Any] | None) -> bool:
    return decide_mixcut_state(task).should_scan_from_rds


def _should_run_ai_return_heartbeat(ctx: Any, item: dict[str, Any]) -> bool:
    task = _latest_task(ctx, str(item.get("product_id") or ""))
    state = str(item.get("feishu_state") or "")
    fallback = {
        "pipeline_status": item.get("pipeline_status"),
        "next_action": item.get("next_action"),
        "task_status": item.get("task_status"),
    }
    return decide_mixcut_state(task or fallback, feishu_state=state).scanner_mode == "ai_return_heartbeat"


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
    state_rank = 0 if state == "可混剪" else 1
    priority_rank = PRIORITY_RANK.get(str(item.get("priority") or "").lower(), 4)
    return (priority_rank, state_rank, str(item.get("created_at") or ""), str(item.get("product_id") or ""))


def _lock_key(product_id: str, shop_id: str) -> str:
    return f"{product_id}:{shop_id or 'DEFAULT'}"


def _owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _db_now(ctx: Any) -> str:
    return _db_ts(ctx, datetime.utcnow())


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
