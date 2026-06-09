#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


TERMINAL_STATUSES = {"DONE", "BLOCKED", "ERROR", "WAITING_AI_RETURN"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run auto_mixcut guard in bounded subprocess passes.")
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--target", type=int)
    parser.add_argument("--name", default="")
    parser.add_argument("--market", default="")
    parser.add_argument("--category", default="")
    parser.add_argument("--max-passes", type=int, default=12)
    parser.add_argument("--max-rounds", type=int, default=1)
    parser.add_argument("--round-timeout", type=int, default=480)
    parser.add_argument("--max-consecutive-timeouts", type=int, default=2)
    parser.add_argument("--skip-upload-sync", action="store_true")
    args = parser.parse_args()

    summary = run_guard_loop(
        product_id=args.product_id,
        target=args.target,
        name=args.name,
        market=args.market,
        category=args.category,
        max_passes=args.max_passes,
        max_rounds=args.max_rounds,
        round_timeout=args.round_timeout,
        max_consecutive_timeouts=args.max_consecutive_timeouts,
        skip_upload_sync=args.skip_upload_sync,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0 if summary.get("success") else 1


def run_guard_loop(
    *,
    product_id: str,
    target: int | None,
    name: str = "",
    market: str = "",
    category: str = "",
    max_passes: int = 12,
    max_rounds: int = 1,
    round_timeout: int = 480,
    max_consecutive_timeouts: int = 2,
    skip_upload_sync: bool = False,
) -> dict[str, Any]:
    product_id = str(product_id or "").strip()
    passes: list[dict[str, Any]] = []
    consecutive_timeouts = 0
    started = time.time()

    for pass_no in range(1, max(1, max_passes) + 1):
        cmd = _guard_command(product_id, target, name, market, category, max_rounds, skip_upload_sync)
        pass_started = time.time()
        effective_timeout = max(30, _dynamic_round_timeout(product_id, round_timeout))
        child_env = os.environ.copy()
        child_env.setdefault("AUTO_MIXCUT_SKIP_FINAL_VIDEO_QC", "1")
        child_env.setdefault("AUTO_MIXCUT_TOP_UP_MAX_PER_ROUND", "5")
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(ROOT),
                env=child_env,
                capture_output=True,
                text=True,
                timeout=effective_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            consecutive_timeouts += 1
            pass_item = {
                "pass_no": pass_no,
                "status": "timeout",
                "timeout_seconds": effective_timeout,
                "elapsed_seconds": round(time.time() - pass_started, 1),
                "stdout_tail": _tail(exc.stdout),
                "stderr_tail": _tail(exc.stderr),
            }
            passes.append(pass_item)
            if consecutive_timeouts >= max(1, max_consecutive_timeouts):
                return _summary(False, product_id, "BLOCKED", "CHECK_PIPELINE_LOG", started, passes, "guard subprocess timeout")
            continue

        consecutive_timeouts = 0
        parsed = _parse_guard_stdout(proc.stdout)
        pass_item = {
            "pass_no": pass_no,
            "status": "completed",
            "returncode": proc.returncode,
            "elapsed_seconds": round(time.time() - pass_started, 1),
            "stdout_tail": _tail(proc.stdout),
            "stderr_tail": _tail(proc.stderr),
            "guard_result": _compact_guard_result(parsed),
        }
        passes.append(pass_item)

        if proc.returncode != 0:
            status, action = _status_action_from_result(parsed, default_status="ERROR", default_action="CHECK_PIPELINE_LOG")
            return _summary(False, product_id, status, action, started, passes, "guard pass failed")

        status, action = _status_action_from_result(parsed)
        if status in TERMINAL_STATUSES:
            return _summary(status == "DONE" or status == "WAITING_AI_RETURN", product_id, status, action, started, passes)
        if action != "RUN_GUARD_AGAIN":
            return _summary(True, product_id, status, action, started, passes)

    return _summary(True, product_id, "READY_TO_CONTINUE", "RUN_GUARD_AGAIN", started, passes, f"max passes reached: {max_passes}")


def _guard_command(product_id: str, target: int | None, name: str, market: str, category: str, max_rounds: int, skip_upload_sync: bool) -> list[str]:
    cmd = [sys.executable, "scripts/run_mixcut_guard.py", "--product-id", product_id, "--max-rounds", str(max_rounds)]
    if target is not None:
        cmd.extend(["--target", str(target)])
    if name:
        cmd.extend(["--name", name])
    if market:
        cmd.extend(["--market", market])
    if category:
        cmd.extend(["--category", category])
    if skip_upload_sync:
        cmd.append("--skip-upload-sync")
    return cmd


def _parse_guard_stdout(stdout: str | bytes | None) -> dict[str, Any]:
    text = stdout.decode() if isinstance(stdout, bytes) else str(stdout or "")
    text = text.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return {}
    return {}


def _status_action_from_result(parsed: dict[str, Any], default_status: str = "READY_TO_CONTINUE", default_action: str = "RUN_GUARD_AGAIN") -> tuple[str, str]:
    data = parsed.get("data") if isinstance(parsed.get("data"), dict) else {}
    status = str(data.get("pipeline_status") or parsed.get("pipeline_status") or default_status)
    action = str(data.get("next_action") or parsed.get("next_action") or default_action)
    return status, action


def _compact_guard_result(parsed: dict[str, Any]) -> dict[str, Any]:
    if not parsed:
        return {}
    data = parsed.get("data") if isinstance(parsed.get("data"), dict) else {}
    error = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
    detail = data.get("detail") if isinstance(data.get("detail"), dict) else {}
    return {
        "success": parsed.get("success"),
        "pipeline_status": data.get("pipeline_status"),
        "next_action": data.get("next_action"),
        "remaining_count": detail.get("remaining_count"),
        "stale_segment_count": detail.get("stale_segment_count"),
        "current_bottleneck": detail.get("current_bottleneck"),
        "error_code": error.get("code"),
        "error_message": error.get("message"),
    }


def _tail(value: str | bytes | None, limit: int = 4000) -> str:
    text = value.decode(errors="replace") if isinstance(value, bytes) else str(value or "")
    return text[-limit:]


def _summary(success: bool, product_id: str, final_status: str, next_action: str, started: float, passes: list[dict[str, Any]], message: str = "") -> dict[str, Any]:
    return {
        "success": success,
        "product_id": product_id,
        "final_status": final_status,
        "next_action": next_action,
        "message": message,
        "elapsed_seconds": round(time.time() - started, 1),
        "pass_count": len(passes),
        "passes": passes,
    }


def _dynamic_round_timeout(product_id: str, base_timeout: int) -> int:
    enabled = str(os.environ.get("AUTO_MIXCUT_GUARD_DYNAMIC_TIMEOUT", "")).strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return base_timeout
    minute_per_output = int(os.environ.get("AUTO_MIXCUT_GUARD_TIMEOUT_PER_OUTPUT", "60") or "60")
    buffer = int(os.environ.get("AUTO_MIXCUT_GUARD_TIMEOUT_BUFFER", "120") or "120")
    try:
        from auto_mixcut.core.bootstrap import build_context
        ctx = build_context()
        task = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
        if task:
            allowed = int(task[0].get("allowed_variant_count") or 0)
            if allowed > 0:
                return max(base_timeout, allowed * minute_per_output + buffer)
    except Exception:
        pass
    return base_timeout


if __name__ == "__main__":
    raise SystemExit(main())
