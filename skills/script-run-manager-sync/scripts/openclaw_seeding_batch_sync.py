#!/usr/bin/env python3
"""Typed adapter for synchronizing reviewed organic-seeding scripts."""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable


SKILL_ROOT = Path(__file__).resolve().parents[1]
RUNNER = SKILL_ROOT / "run_pipeline.py"
RECORD_ID_RE = re.compile(r"^rec[A-Za-z0-9]+$")


def build_command(
    *, action: str, source_url: str, record_id: str = "", product_code: str = "", limit: int = 20
) -> list[str]:
    if action not in {"check", "sync"}:
        raise ValueError("action 必须是 check 或 sync")
    if not source_url.startswith("https://"):
        raise ValueError("必须提供种草生产脚本表的 https 飞书链接")
    if record_id and not RECORD_ID_RE.fullmatch(record_id):
        raise ValueError("record_id 必须是 rec 开头的飞书记录 ID")
    if not 1 <= int(limit) <= 20:
        raise ValueError("一次最多同步 20 条")
    command = [
        sys.executable, str(RUNNER), "--mode", "scheduled",
        "--source-kind", "seeding-batch", "--source-feishu-url", source_url,
        "--limit", str(limit), "--lock-file", "/tmp/seeding_run_manager_sync.pid",
    ]
    if action == "check":
        command.append("--dry-run")
    if record_id:
        command.extend(["--record-id", record_id])
    if product_code:
        command.extend(["--product-code", product_code])
    return command


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="种草生产脚本 -> 运行管理表")
    parser.add_argument("action", choices=["check", "sync"])
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--record-id", default="")
    parser.add_argument("--product-code", default="")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(argv)
    try:
        command = build_command(
            action=args.action, source_url=args.source_url, record_id=args.record_id,
            product_code=args.product_code, limit=args.limit,
        )
    except ValueError as exc:
        print(f"参数错误: {exc}", file=sys.stderr)
        return 2
    return subprocess.run(command, cwd=str(SKILL_ROOT), check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
