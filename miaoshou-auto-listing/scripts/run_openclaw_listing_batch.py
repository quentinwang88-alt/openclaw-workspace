#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from miaoshou_auto_listing.config import load_config  # noqa: E402
from miaoshou_auto_listing.services.feishu_task_table import FeishuTaskTable  # noqa: E402


CommandRunner = Callable[[List[str]], subprocess.CompletedProcess[str]]
SAFE_PREFLIGHT_SKIP_CODES = {
    "SIZE_CHART_REQUIRED",
    "SIZE_CHART_DETECTION_FAILED",
    "DESCRIPTION_COUNT_UNAVAILABLE",
    "DESCRIPTION_LIMIT_EXCEEDED",
}
PENDING_VERIFICATION_STATUS = "SUBMITTED_PENDING_VERIFICATION"


def parse_execution_result(stdout: str) -> Dict[str, Any]:
    """Parse the single JSON result emitted by the listing CLI."""
    payload = stdout.strip()
    if not payload:
        raise ValueError("上架执行器没有返回结果")
    try:
        result = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("上架执行器返回了无法解析的结果") from exc
    if not isinstance(result, dict) or not result.get("task_id"):
        raise ValueError("上架执行器结果缺少 task_id")
    return result


def execute_pending_batch(
    table: FeishuTaskTable,
    *,
    config_dir: Path,
    max_items: int,
    command_runner: CommandRunner,
    verify_only: bool = False,
) -> Dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    items: List[Dict[str, Any]] = []
    verification_items: List[Dict[str, Any]] = []
    stopped_reason = "queue_empty"
    initial_verify_timeout_ms = int(
        table.settings.get("initial_verify_timeout_ms", 90_000)
    ) if hasattr(table, "settings") else 90_000
    reconcile_verify_timeout_ms = int(
        table.settings.get("reconcile_verify_timeout_ms", 30_000)
    ) if hasattr(table, "settings") else 30_000

    for index in range(1, (0 if verify_only else max_items) + 1):
        queue = table.inspect()
        if queue["actionable"] == 0:
            break

        print(
            f"[妙手批次] 开始第 {index} 条，当前待执行 {queue['actionable']} 条",
            flush=True,
        )
        completed = command_runner(
            [
                sys.executable,
                "-u",
                "-m",
                "miaoshou_auto_listing.cli",
                "--config-dir",
                str(config_dir),
                "--feishu-once",
                "--verify-timeout-ms",
                str(initial_verify_timeout_ms),
            ]
        )
        try:
            result = parse_execution_result(completed.stdout)
        except ValueError as exc:
            claim_rejected = (
                completed.returncode == 0
                and "No pending Feishu task" in completed.stdout
            )
            result = {
                "task_id": "unknown",
                "success": False,
                "error_code": (
                    "QUEUE_VALIDATION_REJECTED"
                    if claim_rejected
                    else "RUNNER_OUTPUT_INVALID"
                ),
                "error_message": (
                    "待执行记录在领取阶段未通过字段校验，请查看飞书执行结果"
                    if claim_rejected
                    else str(exc)
                ),
            }
        result["process_exit_code"] = completed.returncode
        if completed.stderr.strip():
            result["process_stderr"] = completed.stderr.strip()[-2000:]
        items.append(result)

        if result.get("published_status") == PENDING_VERIFICATION_STATUS:
            print(
                "[妙手批次] 本条已提交并转为待核验；不会重发，继续下一条",
                flush=True,
            )
            continue

        if not result.get("success") and result.get("error_code") in SAFE_PREFLIGHT_SKIP_CODES:
            print(
                "[妙手批次] 本条为发布前资料异常，已记录并继续下一条："
                f"{result.get('error_code')}",
                flush=True,
            )
            continue

        if completed.returncode != 0 or not result.get("success"):
            stopped_reason = "first_error"
            print(
                "[妙手批次] 本条为系统性异常，已停止批次；不会自动重试或继续发布",
                flush=True,
            )
            break

        print(
            "[妙手批次] 发布成功："
            f"{result.get('task_id')} -> {result.get('platform_product_id')}",
            flush=True,
        )
    else:
        stopped_reason = (
            "verification_only_complete" if verify_only else "max_items_reached"
        )

    if stopped_reason != "first_error":
        pending_ids = (
            table.verification_pending_record_ids(limit=max_items)
            if hasattr(table, "verification_pending_record_ids")
            else []
        )
        for record_id in pending_ids:
            print(f"[妙手核验] 只核验 {record_id}", flush=True)
            completed = command_runner(
                [
                    sys.executable,
                    "-u",
                    "-m",
                    "miaoshou_auto_listing.cli",
                    "--config-dir",
                    str(config_dir),
                    "--feishu-verify-record",
                    record_id,
                    "--verify-timeout-ms",
                    str(reconcile_verify_timeout_ms),
                ]
            )
            try:
                verified = parse_execution_result(completed.stdout)
            except ValueError as exc:
                verified = {
                    "task_id": record_id,
                    "success": False,
                    "error_code": "VERIFY_RUNNER_OUTPUT_INVALID",
                    "error_message": str(exc),
                }
            verified["process_exit_code"] = completed.returncode
            verification_items.append(verified)
            if verified.get("success"):
                print(
                    "[妙手核验] 已确认发布："
                    f"{record_id} -> {verified.get('platform_product_id')}",
                    flush=True,
                )
            elif verified.get("published_status") == PENDING_VERIFICATION_STATUS:
                print(f"[妙手核验] {record_id} 仍待核验", flush=True)
            else:
                print(
                    f"[妙手核验] {record_id} 已得到明确失败/异常结果",
                    flush=True,
                )

    remaining = table.inspect()
    finished_at = datetime.now(timezone.utc)
    latest_outcomes: Dict[str, Dict[str, Any]] = {}
    for outcome in [*items, *verification_items]:
        task_id = str(outcome.get("task_id") or "")
        if task_id:
            latest_outcomes[task_id] = outcome
    confirmed_success = sum(
        bool(outcome.get("success")) for outcome in latest_outcomes.values()
    )
    submitted_pending = sum(
        outcome.get("published_status") == PENDING_VERIFICATION_STATUS
        for outcome in latest_outcomes.values()
    )
    needs_input = sum(
        not bool(outcome.get("success"))
        and outcome.get("published_status") != PENDING_VERIFICATION_STATUS
        and outcome.get("error_code") in SAFE_PREFLIGHT_SKIP_CODES
        for outcome in latest_outcomes.values()
    )
    true_failed = sum(
        not bool(outcome.get("success"))
        and outcome.get("published_status") != PENDING_VERIFICATION_STATUS
        and outcome.get("error_code") not in SAFE_PREFLIGHT_SKIP_CODES
        for outcome in latest_outcomes.values()
    )
    return {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "processed": len(items),
        "succeeded": confirmed_success,
        "confirmed_success": confirmed_success,
        "submitted_pending": submitted_pending,
        "needs_input": needs_input,
        "true_failed": true_failed,
        "failed": needs_input + true_failed,
        "verification_pending_submissions": sum(
            item.get("published_status") == PENDING_VERIFICATION_STATUS
            for item in items
        ),
        "skipped": sum(
            item.get("error_code") in SAFE_PREFLIGHT_SKIP_CODES for item in items
        ),
        "blocking_failed": true_failed,
        "stopped_reason": stopped_reason,
        "remaining_actionable": remaining["actionable"],
        "remaining_pending_verification": remaining.get(
            "pending_verification", 0
        ),
        "items": items,
        "verification_items": verification_items,
    }


def _run_command(command: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_summary(summary: Dict[str, Any]) -> Path:
    output_dir = PROJECT_ROOT / "runtime" / "batches"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    destination = output_dir / f"openclaw_listing_{timestamp}.json"
    destination.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Safely execute a bounded batch from the Feishu listing queue"
    )
    parser.add_argument("--max-items", type=int, default=20)
    parser.add_argument("--config-dir", type=Path, default=PROJECT_ROOT / "config")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only inspect the queue; do not start Chrome or claim tasks",
    )
    parser.add_argument(
        "--verify-pending-only",
        action="store_true",
        help="Only reconcile submitted rows in 待核验; never publish",
    )
    args = parser.parse_args()
    if args.max_items < 1 or args.max_items > 50:
        raise SystemExit("--max-items must be between 1 and 50")

    config_dir = args.config_dir.resolve()
    table = FeishuTaskTable(load_config(config_dir))
    if args.check_only:
        print(json.dumps(table.inspect(), ensure_ascii=False, indent=2))
        return 0

    lock_path = PROJECT_ROOT / "runtime" / "miaoshou_listing.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(
                json.dumps(
                    {
                        "success": False,
                        "error": "另一个妙手上架批次正在运行，本次未执行",
                    },
                    ensure_ascii=False,
                )
            )
            return 2

        initial = table.inspect()
        if initial["actionable"] == 0 and initial.get("pending_verification", 0) == 0:
            summary = {
                "processed": 0,
                "succeeded": 0,
                "confirmed_success": 0,
                "submitted_pending": 0,
                "needs_input": 0,
                "true_failed": 0,
                "failed": 0,
                "skipped": 0,
                "blocking_failed": 0,
                "stopped_reason": "queue_empty",
                "remaining_actionable": 0,
                "remaining_pending_verification": 0,
                "items": [],
                "verification_items": [],
            }
        else:
            browser = _run_command([str(PROJECT_ROOT / "scripts/start_miaoshou_chrome.sh")])
            if browser.returncode != 0:
                summary = {
                    "processed": 0,
                    "succeeded": 0,
                    "confirmed_success": 0,
                    "submitted_pending": 0,
                    "needs_input": 0,
                    "true_failed": 1,
                    "failed": 1,
                    "skipped": 0,
                    "blocking_failed": 1,
                    "stopped_reason": "browser_start_failed",
                    "remaining_actionable": initial["actionable"],
                    "items": [
                        {
                            "task_id": "browser",
                            "success": False,
                            "error_code": "BROWSER_START_FAILED",
                            "error_message": (browser.stderr or browser.stdout).strip(),
                        }
                    ],
                }
            else:
                browser_check = _run_command(
                    [
                        sys.executable,
                        "-u",
                        "-m",
                        "miaoshou_auto_listing.cli",
                        "--config-dir",
                        str(config_dir),
                        "--browser-check",
                    ]
                )
                if browser_check.returncode != 0:
                    message = (browser_check.stderr or browser_check.stdout).strip()
                    error_code = (
                        "LOGIN_EXPIRED"
                        if "LOGIN_EXPIRED" in message or "login" in message.lower()
                        else "BROWSER_CHECK_FAILED"
                    )
                    summary = {
                        "processed": 0,
                        "succeeded": 0,
                        "confirmed_success": 0,
                        "submitted_pending": 0,
                        "needs_input": 0,
                        "true_failed": 1,
                        "failed": 1,
                        "skipped": 0,
                        "blocking_failed": 1,
                        "stopped_reason": "browser_check_failed",
                        "remaining_actionable": initial["actionable"],
                        "remaining_pending_verification": initial.get(
                            "pending_verification", 0
                        ),
                        "items": [
                            {
                                "task_id": "browser",
                                "success": False,
                                "error_code": error_code,
                                "error_message": message,
                            }
                        ],
                        "verification_items": [],
                    }
                else:
                    summary = execute_pending_batch(
                        table,
                        config_dir=config_dir,
                        max_items=args.max_items,
                        command_runner=_run_command,
                        verify_only=args.verify_pending_only,
                    )

        summary_path = _write_summary(summary)
        summary["summary_path"] = str(summary_path)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if summary.get("blocking_failed", summary["failed"]) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
