#!/usr/bin/env python3
"""Whitelist-only OpenClaw adapter for natural-language task routing."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
RUNNER = SCRIPT_DIR / "run_pending_tasks.py"
OUTBOX = SCRIPT_DIR / "retry_feishu_outbox.py"
SCHEMA = SCRIPT_DIR / "ensure_feishu_schema.py"
CHECK_RUNTIME = SCRIPT_DIR / "check_runtime.py"
REVISION = SCRIPT_DIR / "run_script_revision.py"
RECORD_ID_RE = re.compile(r"^rec[A-Za-z0-9]+$")
ACTIONS = (
    "check",
    "one-click",
    "process-mother",
    "confirm-mother",
    "generate",
    "retry-outbox",
    "check-runtime",
    "schema-check",
    "revise", "compare", "export-revision",
)


def _record_id(value: str) -> str:
    if not RECORD_ID_RE.fullmatch(value or ""):
        raise argparse.ArgumentTypeError("record id must start with rec and be alphanumeric")
    return value


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe OpenClaw wig replication adapter")
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("--record-id", type=_record_id)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--prompt-id")
    parser.add_argument("--instruction-file", type=Path)
    parser.add_argument("--changes-file", type=Path)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--output-dir", type=Path)
    execution = parser.add_mutually_exclusive_group()
    execution.add_argument("--apply", action="store_true")
    execution.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def build_command(args: argparse.Namespace) -> list[str]:
    revision_options = ("prompt_id", "instruction_file", "changes_file", "snapshot", "artifact", "output_dir", "apply", "dry_run")
    if args.action in {"revise", "compare", "export-revision"}:
        if args.record_id or args.limit is not None:
            raise ValueError("revision actions do not accept task record IDs or cumulative limits")
        if args.action == "export-revision":
            if not getattr(args, "artifact", None) or any(getattr(args, name, None) for name in ("prompt_id", "instruction_file", "changes_file", "snapshot")):
                raise ValueError("export-revision requires only --artifact")
        elif not getattr(args, "instruction_file", None) or not (getattr(args, "prompt_id", None) or getattr(args, "snapshot", None)) or getattr(args, "artifact", None):
            raise ValueError("revision requires --instruction-file and --prompt-id/--snapshot")
        if getattr(args, "prompt_id", None) and not re.fullmatch(r"(?:wsr_)?(?:prompt|revision)_[A-Za-z0-9_-]{1,80}", args.prompt_id):
            raise ValueError("invalid prompt ID")
        command = [sys.executable, str(REVISION), args.action]
        for name in revision_options[:-2]:
            value = getattr(args, name, None)
            if value is not None:
                command.extend(["--" + name.replace("_", "-"), str(value)])
        command.append("--apply" if getattr(args, "apply", False) else "--dry-run")
        return command
    if any(getattr(args, name, None) for name in revision_options):
        raise ValueError("revision-specific options cannot be used for cumulative task actions")
    if args.action in {"one-click", "process-mother", "confirm-mother", "generate"}:
        if not args.record_id:
            raise ValueError(f"{args.action} requires --record-id")
        if args.limit is not None:
            raise ValueError(f"{args.action} does not accept --limit")
        return [
            sys.executable,
            str(RUNNER),
            "--action",
            args.action,
            "--record-id",
            args.record_id,
            "--limit",
            "1",
        ]
    if args.record_id:
        raise ValueError(f"{args.action} does not accept --record-id")
    if args.action == "check":
        limit = args.limit if args.limit is not None else 5
        if not 1 <= limit <= 20:
            raise ValueError("check limit must be between 1 and 20")
        return [sys.executable, str(RUNNER), "--dry-run", "--limit", str(limit)]
    if args.action == "retry-outbox":
        limit = args.limit if args.limit is not None else 50
        if not 1 <= limit <= 200:
            raise ValueError("outbox limit must be between 1 and 200")
        return [sys.executable, str(OUTBOX), "--limit", str(limit)]
    if args.limit is not None:
        raise ValueError(f"{args.action} does not accept --limit")
    if args.action == "check-runtime":
        return [sys.executable, str(CHECK_RUNTIME)]
    if args.action == "schema-check":
        return [sys.executable, str(SCHEMA), "--dry-run"]
    raise ValueError(f"unsupported action: {args.action}")


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        command = build_command(args)
    except ValueError as exc:
        print(f"error: {exc}")
        return 2
    completed = subprocess.run(command, cwd=str(SCRIPT_DIR.parent), check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
