#!/usr/bin/env python3
"""Safe OpenClaw adapter for selected original-production scripts.

Only the existing one-row-one-script synchronizer performs Feishu writes.
This adapter accepts typed selectors and fixed actions so an OpenClaw language
command cannot turn into an arbitrary shell invocation.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable


SKILL_ROOT = Path(__file__).resolve().parents[1]
RUNNER = SKILL_ROOT / "run_pipeline.py"
LONGFORM_RUNNER = (
    Path("/Users/likeu3/.openclaw/workspace/skills/original-script-generator")
    / "scripts" / "run_feishu_longform_production_tasks.py"
)
RECORD_ID_RE = re.compile(r"^rec[A-Za-z0-9]+$")
PRODUCT_CODE_RE = re.compile(r"^[A-Za-z0-9]{1,30}$")


def _validate_record_id(value: str) -> str:
    if not RECORD_ID_RE.fullmatch(value or ""):
        raise ValueError("record_id 必须是 rec 开头的飞书记录 ID")
    return value


def _validate_product_code(value: str) -> str:
    if not PRODUCT_CODE_RE.fullmatch(value or ""):
        raise ValueError("product_code 必须是 1 至 30 位字母或数字产品编码")
    return value


def build_sync_command(
    *,
    action: str,
    record_id: str | None = None,
    product_code: str | None = None,
    limit: int | None = None,
) -> list[str]:
    if action not in {"check", "sync"}:
        raise ValueError(f"未知 action: {action}")
    if record_id and product_code:
        raise ValueError("record_id 与 product_code 只能指定一个")
    if record_id:
        _validate_record_id(record_id)
    if product_code:
        _validate_product_code(product_code)
    if limit is not None and not 1 <= limit <= 20:
        raise ValueError("一次最多同步 20 条已勾选的生产脚本")

    command = [
        sys.executable,
        str(RUNNER),
        "--mode",
        "scheduled",
        "--source-kind",
        "original-batch",
    ]
    if action == "check":
        command.append("--dry-run")
    if record_id:
        command.extend(["--record-id", record_id])
    if product_code:
        command.extend(["--product-code", product_code])
    if limit is not None:
        command.extend(["--limit", str(limit)])
    return command


def build_longform_command(
    *,
    action: str,
    record_id: str | None = None,
    product_code: str | None = None,
    limit: int | None = None,
) -> list[str]:
    """Build the unified long-form branch for original and remake rows.

    Both sources share one operator checkbox and one paid-media path; only the
    source compiler differs inside the producer.
    """

    if action not in {"check", "sync"}:
        raise ValueError(f"未知 action: {action}")
    command = [sys.executable, str(LONGFORM_RUNNER)]
    if action == "check":
        command.append("--dry-run")
    else:
        # The user's single `进入生产` decision authorizes the uninterrupted
        # H3 + Edge TTS path; there is no per-segment confirmation.
        command.extend(["--allow-real-submit", "--allow-external-tts"])
    if record_id:
        command.extend(["--record-id", _validate_record_id(record_id)])
    if product_code:
        command.extend(["--product-code", _validate_product_code(product_code)])
    if limit is not None:
        if not 1 <= limit <= 20:
            raise ValueError("一次最多处理 20 条已勾选的生产脚本")
        command.extend(["--limit", str(limit)])
    return command


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OpenClaw 原创生产脚本同步适配器（只同步勾选进入生产的脚本）"
    )
    parser.add_argument("action", choices=["check", "sync"])
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--record-id")
    selector.add_argument("--product-code")
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        command = build_sync_command(
            action=args.action,
            record_id=args.record_id,
            product_code=args.product_code,
            limit=args.limit,
        )
    except ValueError as exc:
        print(f"参数错误: {exc}", file=sys.stderr)
        return 2

    try:
        longform_command = build_longform_command(
            action=args.action,
            record_id=args.record_id,
            product_code=args.product_code,
            limit=args.limit,
        )
    except ValueError as exc:
        print(f"参数错误: {exc}", file=sys.stderr)
        return 2

    print(f"OpenClaw 原创生产脚本分流动作: {args.action}")
    print("1/2 统一长视频生产（原创 + 复刻）")
    longform = subprocess.run(longform_command, cwd=str(LONGFORM_RUNNER.parent.parent), check=False)
    print("2/2 15秒短视频同步")
    shortform = subprocess.run(command, cwd=str(SKILL_ROOT), check=False)
    return longform.returncode or shortform.returncode


if __name__ == "__main__":
    raise SystemExit(main())
