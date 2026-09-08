#!/usr/bin/env python3
"""Kernel-owned scanner slot lock: bounded parallel scanners, process-safe.

A pool of N slot locks (default 2, OPV_SCANNER_SLOT_COUNT) bounds how many
scanner processes run concurrently; per-record mutual exclusion is enforced
inside the workflow scan (record-level flock), so two scanners never process
the same row. Slot and record locks are kernel-owned: process death releases
them automatically.
"""
import fcntl
import json
import os
from pathlib import Path
import sys
import time


def claim_slot(slot_dir="/tmp", count=None):
    """Claim one free slot fd; None when all slots are busy."""
    if count is None:
        try:
            count = max(1, int(os.environ.get("OPV_SCANNER_SLOT_COUNT", "2")))
        except ValueError:
            count = 2
    for index in range(count):
        fd = os.open(f"{slot_dir}/opv_scanner_slot_{index}.flock",
                     os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            continue
        # Pass the held descriptor through exec. There is no wrapper that can
        # die while leaving an orphan child running without the slot held.
        os.set_inheritable(fd, True)
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps({
            "slot": index, "pid": os.getpid(), "started_at": time.time(),
        }).encode())
        return fd
    return None


def run(argv, *, slot_dir="/tmp", slot_count=None) -> int:
    """Testable entry: claim a slot, exec the scanner with argv.

    Returns 0 without spawning when every slot is busy (an existing scanner
    will pick pending records up).
    """
    package = Path(__file__).resolve().parents[1]
    os.chdir(package)
    fd = claim_slot(slot_dir=slot_dir, count=slot_count)
    if fd is None:
        return 0
    os.execv(
        sys.executable,
        [sys.executable, "-u", str(package / "scripts/run_feishu_tasks.py"), *list(argv)],
    )
    return 0  # unreachable; exec replaces the process


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
