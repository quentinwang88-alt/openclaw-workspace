from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Optional, Sequence

from .browser.evidence import EvidenceRecorder
from .browser.session import BrowserSession
from .browser.selectors import SelectorRegistry
from .config import load_config, validate_task_config
from .models import ErrorCode, ExecutionResult, ProductTask, Step
from .services.feishu_task_table import FeishuTaskTable
from .state import (
    AcquisitionReceiptStore,
    CheckpointStore,
    JsonlStateSink,
    ImageTranslationReceiptStore,
    SUBMISSION_PENDING,
    SubmissionReceiptStore,
)
from .workflows import PublishProductWorkflow


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _clear_current_cancellation() -> None:
    """Let a cancelled asyncio task finish the terminal Feishu writeback."""
    current = asyncio.current_task()
    if current is None or not hasattr(current, "uncancel"):
        return
    while current.cancelling():
        current.uncancel()


async def _complete_feishu_safely(
    task_table: FeishuTaskTable, claimed, result: ExecutionResult
) -> None:
    completion = asyncio.create_task(
        asyncio.to_thread(task_table.complete, claimed, result)
    )
    try:
        await asyncio.shield(completion)
    except asyncio.CancelledError:
        # A second Ctrl-C must not strand a claimed row in “执行中”. The table
        # update is small and bounded by the configured HTTP timeout.
        _clear_current_cancellation()
        await completion


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the deterministic Miaoshou product editor workflow"
    )
    parser.add_argument("--task", type=Path, help="Task JSON file")
    parser.add_argument(
        "--feishu-once",
        action="store_true",
        help="Claim and execute the first pending row in the configured Feishu table",
    )
    parser.add_argument(
        "--feishu-record",
        metavar="RECORD_ID",
        help="Execute exactly one Feishu row in the normal single-pass workflow",
    )
    parser.add_argument(
        "--feishu-verify-record",
        metavar="RECORD_ID",
        help="Verify one submitted Feishu row without publishing",
    )
    parser.add_argument(
        "--feishu-check",
        action="store_true",
        help="Read-only Feishu connectivity and pending-task summary",
    )
    parser.add_argument(
        "--feishu-retry-record",
        metavar="RECORD_ID",
        help="Reset one failed Feishu record to pending after validation",
    )
    parser.add_argument("--config-dir", type=Path, default=PROJECT_ROOT / "config")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Allow the final save-and-publish action; omitted disables publishing",
    )
    parser.add_argument(
        "--resume-from", choices=[step.value for step in Step], help="Override current_step"
    )
    parser.add_argument(
        "--prepare-profile",
        action="store_true",
        help="Open the dedicated profile for manual login, then wait for Enter",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Use the dedicated profile without attaching to a visible Chrome window",
    )
    parser.add_argument(
        "--browser-check",
        action="store_true",
        help="Validate the dedicated browser and Miaoshou login without writes",
    )
    parser.add_argument(
        "--verify-timeout-ms",
        type=int,
        help="Override the bounded publish-result verification timeout",
    )
    return parser


async def run(args: argparse.Namespace) -> int:
    config_dir = args.config_dir.resolve()
    config = load_config(config_dir)
    if args.verify_timeout_ms is not None:
        if args.verify_timeout_ms < 1000 or args.verify_timeout_ms > 600000:
            raise SystemExit("--verify-timeout-ms must be between 1000 and 600000")
        config.browser.publish_verify_timeout_ms = args.verify_timeout_ms
    if args.headless:
        config.browser.headless = True
        config.browser.cdp_url = ""
    selectors = SelectorRegistry(config_dir / "selectors")
    session = BrowserSession(config.browser, selectors)
    if args.browser_check:
        if any(
            (
                args.task is not None,
                args.feishu_once,
                args.feishu_record,
                args.feishu_verify_record,
                args.feishu_check,
                args.feishu_retry_record,
                args.execute,
            )
        ):
            raise SystemExit("--browser-check cannot be combined with task or Feishu flags")
        async with session.page() as page:
            await session.assert_logged_in(page)
            print(json.dumps({"success": True, "url": page.url}, ensure_ascii=False))
        return 0
    if args.feishu_check:
        if (
            args.task is not None
            or args.feishu_once
            or args.feishu_record
            or args.feishu_verify_record
            or args.execute
            or args.feishu_retry_record
        ):
            raise SystemExit("--feishu-check cannot be combined with task or execute flags")
        summary = await asyncio.to_thread(FeishuTaskTable(config).inspect)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.feishu_retry_record:
        if (
            args.task is not None
            or args.feishu_once
            or args.feishu_record
            or args.feishu_verify_record
            or args.execute
        ):
            raise SystemExit("--feishu-retry-record cannot be combined with task or execute flags")
        await asyncio.to_thread(
            FeishuTaskTable(config).retry_error, args.feishu_retry_record
        )
        print(f"Reset {args.feishu_retry_record} to pending.")
        return 0
    if args.prepare_profile:
        async with session.page() as page:
            await page.goto(config.browser.base_url, wait_until="domcontentloaded")
            print("Complete Miaoshou login in the opened browser, then press Enter here.")
            await asyncio.to_thread(input)
        return 0
    task_sources = sum(
        bool(value)
        for value in (
            args.task,
            args.feishu_once,
            args.feishu_record,
            args.feishu_verify_record,
        )
    )
    if task_sources > 1:
        raise SystemExit(
            "--task and Feishu task-selection flags are mutually exclusive"
        )
    task_table = None
    claimed = None
    linear_feishu = False
    if args.feishu_verify_record:
        if args.execute:
            raise SystemExit("--feishu-verify-record never accepts --execute")
        args.execute = True
        linear_feishu = True
        task_table = FeishuTaskTable(config)
        claimed = await asyncio.to_thread(
            task_table.claim_for_verification, args.feishu_verify_record
        )
        task = claimed.task
        task.current_step = Step.VERIFY
    elif args.feishu_record:
        if args.execute:
            raise SystemExit(
                "--feishu-record is already a complete publish action"
            )
        args.execute = True
        linear_feishu = True
        task_table = FeishuTaskTable(config)
        claimed = await asyncio.to_thread(
            task_table.claim_for_execute, args.feishu_record
        )
        task = claimed.task
    elif args.feishu_once:
        args.execute = True
        linear_feishu = True
        task_table = FeishuTaskTable(config)
        claimed = await asyncio.to_thread(task_table.claim_next)
        if claimed is None:
            print("No pending Feishu task with a non-empty source URL.")
            return 0
        task = claimed.task
    elif args.task is not None:
        task = ProductTask.model_validate_json(args.task.read_text(encoding="utf-8"))
    else:
        raise SystemExit(
            "--task, --feishu-record, --feishu-verify-record or --feishu-once "
            "is required unless "
            "--prepare-profile is used"
        )
    if args.resume_from:
        if linear_feishu:
            raise SystemExit("Feishu linear tasks do not support --resume-from")
        task.current_step = Step(args.resume_from)
    interrupted = False
    submission_store = SubmissionReceiptStore(
        PROJECT_ROOT / "runtime" / "submissions"
    )
    try:
        validate_task_config(task, config)
        runtime = PROJECT_ROOT / "runtime"
        workflow = PublishProductWorkflow(
            config=config,
            selectors=selectors,
            evidence=EvidenceRecorder(runtime / "artifacts"),
            state_sink=JsonlStateSink(runtime / "state" / f"{task.task_id}.jsonl"),
            checkpoint_store=CheckpointStore(runtime / "checkpoints"),
            submission_store=submission_store,
            acquisition_store=AcquisitionReceiptStore(runtime / "acquisitions"),
            image_translation_store=ImageTranslationReceiptStore(
                runtime / "image_translations"
            ),
        )
        async with session.page() as page:
            result = await workflow.execute(
                page,
                task,
                allow_publish=args.execute,
                linear=linear_feishu,
            )
    except asyncio.CancelledError:
        if task_table is None or claimed is None:
            raise
        interrupted = True
        _clear_current_cancellation()
        receipt = submission_store.load(task)
        submitted = receipt is not None and receipt.status == SUBMISSION_PENDING
        result = ExecutionResult(
            task_id=task.task_id,
            success=False,
            current_step=Step.VERIFY if submitted else (task.current_step or Step.LOCATE_PRODUCT),
            error_code=ErrorCode.UNKNOWN_ERROR,
            error_message=(
                "执行被人工中断；发布请求已提交，禁止自动重发，需继续验证"
                if submitted
                else "执行被人工中断，任务已安全结束"
            ),
            published_status=(
                "SUBMITTED_PENDING_VERIFICATION" if submitted else ""
            ),
        )
    except Exception as exc:
        if task_table is None or claimed is None:
            raise
        result = ExecutionResult(
            task_id=task.task_id,
            success=False,
            current_step=task.current_step,
            error_code=ErrorCode.UNKNOWN_ERROR,
            error_message=f"执行器启动失败: {exc}",
        )
    if task_table is not None and claimed is not None:
        await _complete_feishu_safely(task_table, claimed, result)
    print(result.model_dump_json(indent=2))
    if interrupted:
        return 130
    return 0 if result.success else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        # Cancellation inside a claimed Feishu run is handled by run(). This
        # fallback covers interruption before any record was claimed.
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
