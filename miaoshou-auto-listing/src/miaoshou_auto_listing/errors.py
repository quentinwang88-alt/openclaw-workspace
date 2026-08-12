from __future__ import annotations

from typing import Optional

from .models import ErrorCode, Step


class ExecutorError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        step: Optional[Step] = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.step = step
        self.retryable = retryable


class SelectorNotFound(ExecutorError):
    def __init__(self, selector_key: str, *, step: Optional[Step] = None) -> None:
        super().__init__(
            ErrorCode.PAGE_STRUCTURE_CHANGED,
            f"Selector registry key could not resolve a visible element: {selector_key}",
            step=step,
            retryable=False,
        )
        self.selector_key = selector_key
