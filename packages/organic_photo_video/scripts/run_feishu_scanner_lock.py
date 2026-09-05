#!/usr/bin/env python3
"""Kernel-owned scanner lock: process death cannot leave a permanent lock."""
import fcntl
import json
import os
from pathlib import Path
import sys
import time


def run(command, *, lock_path="/tmp/opv_feishu_scanner.flock"):
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return 0
    # Pass the held descriptor through exec. There is no wrapper that can die
    # while leaving an orphan child running without the lock.
    os.set_inheritable(fd, True)
    os.ftruncate(fd, 0)
    os.write(fd, json.dumps({"pid": os.getpid(), "started_at": time.time()}).encode())
    os.execv(command[0], command)


if __name__ == "__main__":
    package = Path(__file__).resolve().parents[1]
    os.chdir(package)
    raise SystemExit(run([sys.executable, "-u", str(package / "scripts/run_feishu_tasks.py"), *sys.argv[1:]]))
