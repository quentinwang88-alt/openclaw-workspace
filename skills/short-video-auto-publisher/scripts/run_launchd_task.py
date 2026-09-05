#!/usr/bin/env python3
"""Small launchd wrapper that keeps the executable identity on Python.app."""

from __future__ import annotations

import argparse
import fcntl
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def runtime_environment() -> dict:
    """launchd has no interactive shell PATH; expose installed media tools."""
    env = dict(os.environ)
    paths = [str(Path.home() / ".local/bin"), "/opt/homebrew/bin", "/usr/local/bin"]
    paths.extend(env.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin").split(os.pathsep))
    env["PATH"] = os.pathsep.join(dict.fromkeys(paths))
    return env


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


def acquire_process_lock(path: Path):
    """Acquire a crash-safe, non-blocking lock and return its open handle."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def main() -> int:
    args = parse_args()
    stamp = now_text()
    # Keep the existing CLI argument for launchd compatibility, but use a
    # sidecar file lock.  The file may remain after SIGKILL; the kernel lock
    # cannot, so a crash never blocks all future scheduled runs.
    lock_file = Path(f"{args.lock_dir}.flock")
    workdir = Path(args.workdir)

    lock_handle = acquire_process_lock(lock_file)
    if lock_handle is None:
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
                env=runtime_environment(),
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
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
