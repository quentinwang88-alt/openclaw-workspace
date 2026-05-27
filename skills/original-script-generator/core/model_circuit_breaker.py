#!/usr/bin/env python3
"""原创脚本生成模型调用熔断器。"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


DEFAULT_STATE_PATH = Path.home() / ".openclaw" / "shared" / "data" / "original_script_model_circuit.json"


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = True) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


class ModelCircuitBreaker:
    """跨定时任务的轻量模型熔断状态。

    目标是保护队列，不让模型认证失败、持续限流或连续超时继续拉起新任务。
    熔断打开时调用方应停止启动后续任务，但不要把待开始任务标失败。
    """

    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    MODEL_ERROR = "model_error"
    AUTH_ERROR = "auth_error"

    def __init__(
        self,
        state_path: Path | str | None = None,
        enabled: bool | None = None,
        rate_limit_threshold: int | None = None,
        rate_limit_window_seconds: int | None = None,
        rate_limit_cooldown_seconds: int | None = None,
        model_consecutive_threshold: int | None = None,
        model_window_threshold: int | None = None,
        model_window_seconds: int | None = None,
        model_cooldown_seconds: int | None = None,
        auth_cooldown_seconds: int | None = None,
    ) -> None:
        configured_path = os.environ.get("ORIGINAL_SCRIPT_MODEL_CIRCUIT_STATE_PATH")
        self.state_path = Path(state_path or configured_path or DEFAULT_STATE_PATH).expanduser()
        self.enabled = _env_bool("ORIGINAL_SCRIPT_MODEL_CIRCUIT_ENABLED", True) if enabled is None else enabled
        self.rate_limit_threshold = rate_limit_threshold or _env_int("ORIGINAL_SCRIPT_RATE_LIMIT_CIRCUIT_THRESHOLD", 3)
        self.rate_limit_window_seconds = rate_limit_window_seconds or _env_int(
            "ORIGINAL_SCRIPT_RATE_LIMIT_CIRCUIT_WINDOW_SECONDS",
            600,
        )
        self.rate_limit_cooldown_seconds = rate_limit_cooldown_seconds or _env_int(
            "ORIGINAL_SCRIPT_RATE_LIMIT_CIRCUIT_COOLDOWN_SECONDS",
            900,
        )
        self.model_consecutive_threshold = model_consecutive_threshold or _env_int(
            "ORIGINAL_SCRIPT_MODEL_ERROR_CONSECUTIVE_THRESHOLD",
            3,
        )
        self.model_window_threshold = model_window_threshold or _env_int("ORIGINAL_SCRIPT_MODEL_ERROR_WINDOW_THRESHOLD", 5)
        self.model_window_seconds = model_window_seconds or _env_int("ORIGINAL_SCRIPT_MODEL_ERROR_WINDOW_SECONDS", 1800)
        self.model_cooldown_seconds = model_cooldown_seconds or _env_int(
            "ORIGINAL_SCRIPT_MODEL_ERROR_CIRCUIT_COOLDOWN_SECONDS",
            600,
        )
        self.auth_cooldown_seconds = auth_cooldown_seconds or _env_int(
            "ORIGINAL_SCRIPT_AUTH_ERROR_CIRCUIT_COOLDOWN_SECONDS",
            3600,
        )
        self._lock = threading.Lock()

    def can_start(self) -> Tuple[bool, str]:
        if not self.enabled:
            return True, ""
        with self._lock:
            state = self._load_state()
            now = time.time()
            if state.get("status") != "open":
                return True, ""
            open_until = float(state.get("open_until") or 0)
            if open_until <= now:
                state["status"] = "half_open"
                state["reason"] = "熔断冷却结束，允许下一次模型调用探测"
                state["updated_at"] = now
                self._save_state(state)
                return True, ""
            reason = str(state.get("reason") or "模型熔断中")
            remaining = int(max(1, open_until - now))
            return False, f"{reason}；约 {remaining} 秒后重试"

    def record_success(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            state = self._load_state()
            now = time.time()
            if state.get("status") == "open" and float(state.get("open_until") or 0) > now:
                state["last_success_at"] = now
                state["updated_at"] = now
                self._save_state(state)
                return
            state["status"] = "closed"
            state["reason"] = ""
            state["open_until"] = 0
            state["consecutive_model_errors"] = 0
            state["updated_at"] = now
            state["last_success_at"] = state["updated_at"]
            self._save_state(state)

    def record_failure(self, kind: str, detail: str = "", stage_name: str = "") -> Tuple[bool, str]:
        if not self.enabled:
            return False, ""
        kind = self.normalize_failure_kind(kind, detail)
        with self._lock:
            state = self._load_state()
            now = time.time()
            events = self._recent_events(state.get("events", []), now, max(self.model_window_seconds, self.rate_limit_window_seconds))
            events.append(
                {
                    "kind": kind,
                    "stage_name": stage_name,
                    "detail": self._truncate_detail(detail),
                    "ts": now,
                }
            )
            state["events"] = events
            state["last_failure_at"] = now
            state["last_failure_kind"] = kind
            state["last_failure_stage"] = stage_name

            if kind == self.AUTH_ERROR:
                return self._open(
                    state,
                    now,
                    self.auth_cooldown_seconds,
                    "LLM 认证失败，已暂停原创脚本队列；请重新登录后再恢复巡检",
                )

            if kind == self.RATE_LIMIT:
                state["consecutive_model_errors"] = 0
                recent_rate_limits = self._count_recent(events, now, self.rate_limit_window_seconds, {self.RATE_LIMIT})
                if recent_rate_limits >= self.rate_limit_threshold:
                    return self._open(
                        state,
                        now,
                        self.rate_limit_cooldown_seconds,
                        f"模型限流在 {self.rate_limit_window_seconds // 60} 分钟内触发 {recent_rate_limits} 次，已暂停拉起新任务",
                    )
                state["updated_at"] = now
                self._save_state(state)
                return False, ""

            if kind in {self.TIMEOUT, self.MODEL_ERROR}:
                state["consecutive_model_errors"] = int(state.get("consecutive_model_errors") or 0) + 1
                recent_model_errors = self._count_recent(
                    events,
                    now,
                    self.model_window_seconds,
                    {self.TIMEOUT, self.MODEL_ERROR},
                )
                if (
                    state["consecutive_model_errors"] >= self.model_consecutive_threshold
                    or recent_model_errors >= self.model_window_threshold
                ):
                    return self._open(
                        state,
                        now,
                        self.model_cooldown_seconds,
                        "模型连续超时或异常，已暂停拉起新任务，等待冷却后再探测",
                    )

            state["updated_at"] = now
            self._save_state(state)
            return False, ""

    @classmethod
    def normalize_failure_kind(cls, kind: str, detail: str = "") -> str:
        normalized = str(kind or "").strip().lower()
        detail_text = str(detail or "")
        lowered = detail_text.lower()
        if normalized == cls.AUTH_ERROR or any(
            token in lowered
            for token in (
                "llm 认证失败",
                "token_expired",
                "authentication token is expired",
                "provided authentication token is expired",
                "invalid_api_key",
                "unauthorized",
                "401",
            )
        ):
            return cls.AUTH_ERROR
        if normalized == cls.RATE_LIMIT or any(
            token in detail_text for token in ("RequestBurstTooFast", "TooManyRequests", "429", "限流")
        ):
            return cls.RATE_LIMIT
        if normalized == cls.TIMEOUT or "阶段超过" in detail_text or "timeout" in lowered or "timed out" in lowered:
            return cls.TIMEOUT
        return cls.MODEL_ERROR

    def _open(self, state: Dict[str, Any], now: float, cooldown_seconds: int, reason: str) -> Tuple[bool, str]:
        state["status"] = "open"
        state["reason"] = reason
        state["opened_at"] = now
        state["open_until"] = now + max(1, cooldown_seconds)
        state["updated_at"] = now
        self._save_state(state)
        return True, reason

    def _load_state(self) -> Dict[str, Any]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("status", "closed")
                payload.setdefault("events", [])
                payload.setdefault("consecutive_model_errors", 0)
                return payload
        except FileNotFoundError:
            pass
        except Exception:
            pass
        return {
            "status": "closed",
            "reason": "",
            "open_until": 0,
            "events": [],
            "consecutive_model_errors": 0,
        }

    def _save_state(self, state: Dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.state_path)

    @staticmethod
    def _recent_events(events: Any, now: float, window_seconds: int) -> List[Dict[str, Any]]:
        if not isinstance(events, list):
            return []
        cutoff = now - max(1, window_seconds)
        recent: List[Dict[str, Any]] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            try:
                ts = float(event.get("ts") or 0)
            except (TypeError, ValueError):
                continue
            if ts >= cutoff:
                recent.append(event)
        return recent[-100:]

    @staticmethod
    def _count_recent(events: List[Dict[str, Any]], now: float, window_seconds: int, kinds: set[str]) -> int:
        cutoff = now - max(1, window_seconds)
        count = 0
        for event in events:
            try:
                ts = float(event.get("ts") or 0)
            except (TypeError, ValueError):
                continue
            if ts >= cutoff and str(event.get("kind") or "") in kinds:
                count += 1
        return count

    @staticmethod
    def _truncate_detail(detail: str) -> str:
        text = str(detail or "").strip()
        return text[:500]
