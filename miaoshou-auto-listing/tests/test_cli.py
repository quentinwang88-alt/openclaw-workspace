import asyncio
import time
import unittest
from types import SimpleNamespace

from miaoshou_auto_listing.cli import _complete_feishu_safely
from miaoshou_auto_listing.models import ExecutionResult


class CliSafetyTest(unittest.IsolatedAsyncioTestCase):
    async def test_cancellation_waits_for_terminal_feishu_writeback(self) -> None:
        class SlowTable:
            completed = False

            def complete(self, claimed, result):
                time.sleep(0.03)
                self.completed = True

        table = SlowTable()
        result = ExecutionResult(task_id="T1", success=False)
        operation = asyncio.create_task(
            _complete_feishu_safely(table, SimpleNamespace(), result)
        )
        await asyncio.sleep(0)
        operation.cancel()
        await operation
        self.assertTrue(table.completed)


if __name__ == "__main__":
    unittest.main()
