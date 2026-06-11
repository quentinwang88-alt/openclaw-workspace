#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import selectors
import signal
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
    parser.add_argument("--detach", action="store_true", help="Start the guard loop in the background and return immediately.")
    parser.add_argument("--log-dir", default=str(ROOT / "logs"), help="Directory for detached guard logs.")
    args = parser.parse_args()

    if args.detach:
        summary = _detach_guard_loop(args)
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        return 0 if summary.get("success") else 1

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


def _detach_guard_loop(args: argparse.Namespace) -> dict[str, Any]:
    log_dir = Path(args.log_dir).expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d%H%M%S")
    safe_product = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(args.product_id))
    log_path = log_dir / f"guard_loop_{safe_product}_{stamp}.log"

    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--product-id",
        str(args.product_id),
        "--max-passes",
        str(args.max_passes),
        "--max-rounds",
        str(args.max_rounds),
        "--round-timeout",
        str(args.round_timeout),
        "--max-consecutive-timeouts",
        str(args.max_consecutive_timeouts),
        "--log-dir",
        str(log_dir),
    ]
    if args.target is not None:
        cmd.extend(["--target", str(args.target)])
    if args.name:
        cmd.extend(["--name", args.name])
    if args.market:
        cmd.extend(["--market", args.market])
    if args.category:
        cmd.extend(["--category", args.category])
    if args.skip_upload_sync:
        cmd.append("--skip-upload-sync")

    env = os.environ.copy()
    log_fh = log_path.open("a", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
    finally:
        log_fh.close()

    pid_path = log_dir / f"guard_loop_{safe_product}.pid"
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    return {
        "success": True,
        "detached": True,
        "product_id": str(args.product_id),
        "pid": proc.pid,
        "pid_path": str(pid_path),
        "log_path": str(log_path),
        "message": "guard loop started in background; monitor the log/RDS status instead of waiting in bash",
    }


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
        child_env.setdefault("AUTO_MIXCUT_WATERMARK_CHECK_LIMIT", "8")
        child_env.setdefault("AUTO_MIXCUT_SEGMENT_ASSET_LIMIT", "8")
        child_env.setdefault("AUTO_MIXCUT_GUARD_TOP_UP_WITH_STALE", "1")
        child_env.setdefault("AUTO_MIXCUT_GUARD_SEGMENT_SUBPROCESS", "1")
        child_env.setdefault("AUTO_MIXCUT_GUARD_FRAME_LIMIT", "20")
        child_env.setdefault("AUTO_MIXCUT_GUARD_FRAME_TIMEOUT", "60")
        child_env.setdefault("AUTO_MIXCUT_GUARD_FINGERPRINT_LIMIT", "20")
        child_env.setdefault("AUTO_MIXCUT_GUARD_FINGERPRINT_TIMEOUT", "45")
        child_env.setdefault("AUTO_MIXCUT_GUARD_RETAG_LIMIT", "20")
        child_env.setdefault("AUTO_MIXCUT_GUARD_TAG_CONCURRENCY", "2")
        child_env.setdefault("AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_LIMIT", "20")
        child_env.setdefault("AUTO_MIXCUT_GUARD_EFFECTIVE_ROLE_TIMEOUT", "20")
        child_env.setdefault("AUTO_MIXCUT_GUARD_AI_STAGE_LIMIT", "10")
        child_env.setdefault("AUTO_MIXCUT_GUARD_AI_STAGE_TIMEOUT", "45")
        child_env.setdefault("AUTO_MIXCUT_TAG_TOTAL_TIMEOUT_SEC", "240")
        child_env.setdefault("AUTO_MIXCUT_TAG_PROGRESS_EVERY", "5")
        child_env.setdefault("AUTO_MIXCUT_FFMPEG_TIMEOUT_SEC", "45")
        proc_result = _run_guard_subprocess_streaming(cmd, child_env, effective_timeout)
        if proc_result["timed_out"]:
            consecutive_timeouts += 1
            pass_item = {
                "pass_no": pass_no,
                "status": "timeout",
                "timeout_seconds": effective_timeout,
                "elapsed_seconds": round(time.time() - pass_started, 1),
                "stdout_tail": _tail(proc_result.get("stdout")),
                "stderr_tail": "",
            }
            passes.append(pass_item)
            if consecutive_timeouts >= max(1, max_consecutive_timeouts):
                return _summary(False, product_id, "BLOCKED", "CHECK_PIPELINE_LOG", started, passes, "guard subprocess timeout")
            continue

        consecutive_timeouts = 0
        parsed = _parse_guard_stdout(proc_result.get("stdout"))
        pass_item = {
            "pass_no": pass_no,
            "status": "completed",
            "returncode": proc_result["returncode"],
            "elapsed_seconds": round(time.time() - pass_started, 1),
            "stdout_tail": _tail(proc_result.get("stdout")),
            "stderr_tail": "",
            "guard_result": _compact_guard_result(parsed),
        }
        passes.append(pass_item)

        if proc_result["returncode"] != 0:
            status, action = _status_action_from_result(parsed, default_status="ERROR", default_action="CHECK_PIPELINE_LOG")
            return _summary(False, product_id, status, action, started, passes, "guard pass failed")

        status, action = _status_action_from_result(parsed)
        if status in TERMINAL_STATUSES:
            return _summary(status == "DONE" or status == "WAITING_AI_RETURN", product_id, status, action, started, passes)
        if action != "RUN_GUARD_AGAIN":
            return _summary(True, product_id, status, action, started, passes)

    return _summary(True, product_id, "READY_TO_CONTINUE", "RUN_GUARD_AGAIN", started, passes, f"max passes reached: {max_passes}")


def _run_guard_subprocess_streaming(cmd: list[str], env: dict[str, str], timeout_seconds: int) -> dict[str, Any]:
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    stdout_parts: list[str] = []
    selector = selectors.DefaultSelector()
    if proc.stdout is not None:
        selector.register(proc.stdout, selectors.EVENT_READ)
    started = time.time()
    timed_out = False
    try:
        while True:
            if time.time() - started > timeout_seconds:
                timed_out = True
                _terminate_process_group(proc)
                break
            if proc.poll() is not None:
                break
            events = selector.select(timeout=1.0)
            for key, _ in events:
                line = key.fileobj.readline()
                if not line:
                    continue
                stdout_parts.append(line)
                print(line, end="", flush=True)
        if proc.stdout is not None:
            for line in proc.stdout:
                stdout_parts.append(line)
                print(line, end="", flush=True)
    finally:
        selector.close()
    return {
        "returncode": proc.wait(timeout=5) if proc.poll() is not None else None,
        "timed_out": timed_out,
        "stdout": "".join(stdout_parts),
    }


def _terminate_process_group(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


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
            actual = int(task[0].get("actual_variant_count") or 0)
            remaining = int(task[0].get("target_remaining_variant_count") or 0)
            planned = max(0, min(max(0, allowed - actual), remaining or allowed))
            if planned > 0:
                return max(base_timeout, planned * minute_per_output + buffer)
    except Exception:
        pass
    return base_timeout


if __name__ == "__main__":
    raise SystemExit(main())
