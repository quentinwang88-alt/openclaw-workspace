#!/usr/bin/env python3
"""Install or inspect the per-minute V1.2 worker LaunchAgent."""

from __future__ import annotations

import argparse
import hashlib
import os
import plistlib
from pathlib import Path


LABEL = "com.likeu3.wig-success-script-replication"
SKILL_ROOT = Path(__file__).resolve().parents[1]
TARGET = Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"
LOG_DIR = SKILL_ROOT / "var"


def payload() -> dict:
    return {
        "Label": LABEL,
        "ProgramArguments": [
            "/bin/zsh",
            "-lc",
            "exec /usr/bin/python3 "
            + str(SKILL_ROOT / "scripts" / "run_worker_once.py")
            + " --limit 2",
        ],
        "WorkingDirectory": str(SKILL_ROOT),
        "RunAtLoad": False,
        "StartInterval": 60,
        "ProcessType": "Background",
        "EnvironmentVariables": {
            "PATH": "/Users/likeu3/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "WIG_REPLICATION_TASK_CONCURRENCY": "1",
        },
        "StandardOutPath": str(LOG_DIR / "worker.log"),
        "StandardErrorPath": str(LOG_DIR / "worker.error.log"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Install the wig replication LaunchAgent")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-sha256", default="")
    args = parser.parse_args()
    data = plistlib.dumps(payload(), fmt=plistlib.FMT_XML, sort_keys=True)
    digest = hashlib.sha256(data).hexdigest()
    print(f"target={TARGET}")
    print(f"sha256={digest}")
    print("start_interval_seconds=60")
    print("worker_limit=2")
    if not args.apply:
        print("mode=dry-run")
        return 0
    if args.confirm_sha256 != digest:
        raise RuntimeError("--confirm-sha256 must match the dry-run hash")
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    temporary = TARGET.with_suffix(".plist.tmp")
    temporary.write_bytes(data)
    os.replace(temporary, TARGET)
    print("mode=applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
