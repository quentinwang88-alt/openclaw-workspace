"""Scanner slot lock + per-record flock tests (kernel locks, no real scan)."""

import fcntl
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sys

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = PACKAGE_ROOT / "scripts"
for value in (str(PACKAGE_ROOT), str(SCRIPTS)):
    if value not in sys.path:
        sys.path.insert(0, value)

from run_feishu_scanner_lock import claim_slot, run  # noqa: E402
from services.feishu_workflow import _RecordFlock  # noqa: E402


def _hold(path, count=1):
    """Open `count` non-blocking exclusive flocks; return held fds."""
    fds = []
    for _ in range(count):
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fds.append(fd)
        except BlockingIOError:
            os.close(fd)
            break
    return fds


class SlotLockTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = self.tmp.name

    def test_all_slots_claimable_and_fifth_exits(self):
        held = []
        try:
            for _ in range(4):
                fd = claim_slot(slot_dir=self.dir, count=4)
                self.assertIsNotNone(fd)
                held.append(fd)
            self.assertIsNone(claim_slot(slot_dir=self.dir, count=4), "第 5 个请求直接退出")
        finally:
            for fd in held:
                os.close(fd)

    def test_released_slot_is_reclaimable(self):
        fd = claim_slot(slot_dir=self.dir, count=2)
        self.assertIsNotNone(fd)
        os.close(fd)
        again = claim_slot(slot_dir=self.dir, count=2)
        self.assertIsNotNone(again, "锁释放后可以重新领取")
        os.close(again)

    def test_default_slot_count_is_two(self):
        with patch.dict("os.environ", {"OPV_SCANNER_SLOT_COUNT": ""}):
            held = []
            try:
                for _ in range(2):
                    fd = claim_slot(slot_dir=self.dir)
                    self.assertIsNotNone(fd)
                    held.append(fd)
                self.assertIsNone(claim_slot(slot_dir=self.dir))
            finally:
                for fd in held:
                    os.close(fd)

    def test_run_busy_returns_zero_without_spawn(self):
        held = [claim_slot(slot_dir=self.dir, count=2) for _ in range(2)]
        try:
            with patch("os.execv") as exec_mock:
                code = run(["--dry-run"], slot_dir=self.dir, slot_count=2)
            self.assertEqual(code, 0)
            exec_mock.assert_not_called()
        finally:
            for fd in held:
                os.close(fd)


class RecordFlockTest(unittest.TestCase):
    def test_second_worker_skips_locked_record(self):
        with tempfile.TemporaryDirectory() as folder:
            with _RecordFlock("rec-1", directory=folder) as first:
                self.assertTrue(first)
                with _RecordFlock("rec-1", directory=folder) as second:
                    self.assertFalse(second, "同一记录只允许一个 scanner 处理")
                with _RecordFlock("rec-2", directory=folder) as other:
                    self.assertTrue(other, "不同记录互不阻塞")

    def test_release_allows_reentry(self):
        with tempfile.TemporaryDirectory() as folder:
            with _RecordFlock("rec-1", directory=folder) as first:
                self.assertTrue(first)
            with _RecordFlock("rec-1", directory=folder) as second:
                self.assertTrue(second, "释放后可重新领取")


if __name__ == "__main__":
    unittest.main()
