#!/usr/bin/env python3
"""Lightweight runtime circuit breaker for model/API failures."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


class CircuitBreakerOpen(RuntimeError):
    """Raised when the current run should stop instead of burning more tasks."""


@dataclass
class FailureKind:
    category: str
    immediate_stop: bool = False
    should_cooldown: bool = False


class ModelCircuitBreaker:
    def __init__(
        self,
        *,
        model_failure_threshold: int = 3,
        record_failure_threshold: int = 2,
        rate_limit_cooldown_seconds: int = 120,
        enabled: bool = True,
    ) -> None:
        self.model_failure_threshold = max(int(model_failure_threshold or 0), 1)
        self.record_failure_threshold = max(int(record_failure_threshold or 0), 1)
        self.rate_limit_cooldown_seconds = max(int(rate_limit_cooldown_seconds or 0), 0)
        self.enabled = enabled
        self.consecutive_model_failures = 0
        self.consecutive_record_failures = 0
        self.open_reason = ""

    def before_model_call(self) -> None:
        if self.enabled and self.open_reason:
            raise CircuitBreakerOpen(self.open_reason)

    def record_model_success(self) -> None:
        if not self.enabled:
            return
        self.consecutive_model_failures = 0

    def record_model_failure(self, exc: Exception) -> None:
        if not self.enabled:
            return
        kind = classify_failure(exc)
        message = str(exc)
        if kind.immediate_stop:
            self.open_reason = f"模型调用触发立即熔断：{kind.category} | {message}"
            raise CircuitBreakerOpen(self.open_reason) from exc

        if kind.should_cooldown and self.rate_limit_cooldown_seconds > 0:
            print(f"  ⏸ 模型限流/服务繁忙，冷却 {self.rate_limit_cooldown_seconds}s 后再继续")
            time.sleep(self.rate_limit_cooldown_seconds)

        self.consecutive_model_failures += 1
        if self.consecutive_model_failures >= self.model_failure_threshold:
            self.open_reason = (
                f"连续模型调用失败 {self.consecutive_model_failures} 次，停止本轮任务。"
                f"最后错误：{kind.category} | {message}"
            )
            raise CircuitBreakerOpen(self.open_reason) from exc

    def record_successful_record(self) -> None:
        if not self.enabled:
            return
        self.consecutive_record_failures = 0

    def record_failed_record(self, exc: Optional[Exception] = None) -> None:
        if not self.enabled:
            return
        self.consecutive_record_failures += 1
        if self.consecutive_record_failures >= self.record_failure_threshold:
            suffix = f" 最后错误：{exc}" if exc else ""
            self.open_reason = f"连续记录失败 {self.consecutive_record_failures} 条，停止本轮任务。{suffix}"
            raise CircuitBreakerOpen(self.open_reason) from exc


def classify_failure(exc: Exception) -> FailureKind:
    text = str(exc).lower()
    auth_markers = (
        "unauthorized",
        "forbidden",
        "invalid api key",
        "invalid_access_token",
        "access token",
        "api key not found",
        "codex api key not found",
        "authentication",
        "permission_denied",
        "401",
        "403",
    )
    rate_limit_markers = (
        "rate limit",
        "ratelimit",
        "too many requests",
        "429",
        "quota",
        "temporarily unavailable",
        "service unavailable",
        "overloaded",
        "503",
    )
    timeout_markers = (
        "timeout",
        "timed out",
        "read operation timed out",
        "connection",
        "incomplete chunked read",
        "peer closed connection",
        "502",
        "504",
        "500",
    )
    if any(marker in text for marker in auth_markers):
        return FailureKind("鉴权/权限错误", immediate_stop=True)
    if any(marker in text for marker in rate_limit_markers):
        return FailureKind("限流/服务繁忙", should_cooldown=True)
    if any(marker in text for marker in timeout_markers):
        return FailureKind("网络/超时/服务错误")
    return FailureKind("模型调用错误")
