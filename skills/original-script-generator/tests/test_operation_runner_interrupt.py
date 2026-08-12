import unittest

from scripts.run_feishu_operation_tasks import RunInterrupted, _interruption_error


class OperationRunnerInterruptTest(unittest.TestCase):
    def test_interruption_is_actionable_and_bounded(self):
        message = _interruption_error(RunInterrupted("收到 SIGTERM"))

        self.assertEqual(message, "运行被中断，可重试：收到 SIGTERM")
        self.assertLessEqual(len(message), 1800)


if __name__ == "__main__":
    unittest.main()
