#!/usr/bin/env python3
"""模型调用熔断器回归测试。"""

from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.model_circuit_breaker import ModelCircuitBreaker  # noqa: E402


class ModelCircuitBreakerTest(unittest.TestCase):
    def _breaker(self, path: Path) -> ModelCircuitBreaker:
        return ModelCircuitBreaker(
            state_path=path,
            enabled=True,
            rate_limit_threshold=2,
            rate_limit_window_seconds=60,
            rate_limit_cooldown_seconds=300,
            model_consecutive_threshold=2,
            model_window_threshold=3,
            model_window_seconds=60,
            model_cooldown_seconds=120,
            auth_cooldown_seconds=600,
        )

    def test_rate_limit_opens_after_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            breaker = self._breaker(Path(tmp_dir) / "circuit.json")

            opened, _ = breaker.record_failure("rate_limit", "429 TooManyRequests", "script_s1")
            self.assertFalse(opened)
            opened, reason = breaker.record_failure("rate_limit", "RequestBurstTooFast", "script_s2")

            self.assertTrue(opened)
            self.assertIn("模型限流", reason)
            can_start, guard_reason = breaker.can_start()
            self.assertFalse(can_start)
            self.assertIn("模型限流", guard_reason)

    def test_consecutive_model_timeout_opens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            breaker = self._breaker(Path(tmp_dir) / "circuit.json")

            breaker.record_failure("timeout", "script_s1 阶段超过 420 秒", "script_s1")
            opened, reason = breaker.record_failure("model_error", "LLM 调用最终失败", "script_s2")

            self.assertTrue(opened)
            self.assertIn("模型连续超时或异常", reason)
            can_start, _ = breaker.can_start()
            self.assertFalse(can_start)

    def test_auth_failure_opens_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            breaker = self._breaker(Path(tmp_dir) / "circuit.json")

            opened, reason = breaker.record_failure("model_error", "401 unauthorized", "anchor_card")

            self.assertTrue(opened)
            self.assertIn("认证失败", reason)

    def test_success_closes_half_open_circuit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "circuit.json"
            breaker = self._breaker(state_path)

            breaker.record_failure("model_error", "timeout", "script_s1")
            breaker.record_failure("model_error", "timeout", "script_s2")
            can_start, _ = breaker.can_start()
            self.assertFalse(can_start)

            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["open_until"] = 0
            state_path.write_text(json.dumps(state), encoding="utf-8")
            can_start, _ = breaker.can_start()
            self.assertTrue(can_start)

            breaker.record_success()
            can_start, reason = breaker.can_start()

            self.assertTrue(can_start)
            self.assertEqual(reason, "")

    def test_success_does_not_close_active_open_circuit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            breaker = self._breaker(Path(tmp_dir) / "circuit.json")

            breaker.record_failure("rate_limit", "429", "script_s1")
            breaker.record_failure("rate_limit", "429", "script_s2")
            can_start, _ = breaker.can_start()
            self.assertFalse(can_start)

            breaker.record_success()
            can_start, reason = breaker.can_start()

            self.assertFalse(can_start)
            self.assertIn("模型限流", reason)


if __name__ == "__main__":
    unittest.main()
