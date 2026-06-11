#!/usr/bin/env python3
"""Small launchd wrapper that keeps the executable identity on Python.app."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--lock-dir", required=True)
    parser.add_argument("--tmp-prefix", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("command is required after --")
    return args


def main() -> int:
    args = parse_args()
    stamp = now_text()
    lock_dir = Path(args.lock_dir)
    workdir = Path(args.workdir)

    try:
        lock_dir.mkdir()
    except FileExistsError:
        print(f"\n[{stamp}] {args.name} skipped: previous run still active")
        return 0

    fd, tmp_path = tempfile.mkstemp(prefix=f"{args.tmp_prefix}.", suffix=".log")
    os.close(fd)
    tmp_file = Path(tmp_path)

    try:
        command = list(args.command)
        if command[0].endswith(".py"):
            command = [sys.executable, *command]

        with tmp_file.open("w", encoding="utf-8") as handle:
            result = subprocess.run(
                command,
                cwd=str(workdir),
                stdout=handle,
                stderr=subprocess.STDOUT,
                check=False,
            )

        output_stream = sys.stdout if result.returncode == 0 else sys.stderr
        state = "start" if result.returncode == 0 else f"failed (exit={result.returncode})"
        print(f"\n[{stamp}] {args.name} {state}", file=output_stream)
        with tmp_file.open("r", encoding="utf-8", errors="replace") as handle:
            shutil.copyfileobj(handle, output_stream)
        if result.returncode == 0:
            print(f"[{now_text()}] {args.name} end", file=output_stream)
        return result.returncode
    finally:
        try:
            tmp_file.unlink()
        except FileNotFoundError:
            pass
        try:
            lock_dir.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
