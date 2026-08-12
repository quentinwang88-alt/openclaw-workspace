from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from time import monotonic
from typing import Any, Iterable, Tuple

from ..errors import ExecutorError, SelectorNotFound
from ..models import ErrorCode
from .selectors import SelectorRegistry


async def wait_for_key(
    page: Any,
    selectors: SelectorRegistry,
    key: str,
    *,
    timeout_ms: int,
    root: Any = None,
    **params: str,
) -> Any:
    target = root or page
    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        try:
            return await selectors.resolve(target, key, **params)
        except SelectorNotFound:
            await page.wait_for_timeout(200)
    raise ExecutorError(
        ErrorCode.ELEMENT_TIMEOUT,
        f"Timed out waiting for selector signal: {key}",
        retryable=True,
    )


async def wait_for_any_key(
    page: Any,
    selectors: SelectorRegistry,
    keys: Iterable[str],
    *,
    timeout_ms: int,
) -> Tuple[str, Any]:
    keys = tuple(keys)
    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        for key in keys:
            try:
                return key, await selectors.resolve(page, key)
            except SelectorNotFound:
                pass
        await page.wait_for_timeout(200)
    raise ExecutorError(
        ErrorCode.ELEMENT_TIMEOUT,
        f"Timed out waiting for one of: {', '.join(keys)}",
        retryable=True,
    )


async def wait_for_loading_to_finish(
    page: Any, selectors: SelectorRegistry, *, timeout_ms: int
) -> None:
    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        if not await selectors.exists(page, "loading_mask"):
            return
        await page.wait_for_timeout(200)
    raise ExecutorError(
        ErrorCode.ELEMENT_TIMEOUT,
        "Page loading mask did not disappear",
        retryable=True,
    )


async def element_value(locator: Any) -> str:
    try:
        value = await locator.input_value()
        if value:
            return value.strip()
    except Exception:
        pass
    return (await locator.text_content() or "").strip()


async def fill_and_verify(locator: Any, value: str) -> None:
    await locator.fill(value)
    actual = await locator.input_value()
    if actual.strip() != value:
        raise ValueError(f"Input verification failed: expected {value!r}, got {actual!r}")


async def wait_for_text_in_key(
    page: Any,
    selectors: SelectorRegistry,
    key: str,
    expected: str,
    *,
    timeout_ms: int,
) -> None:
    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        try:
            locators = await selectors.resolve_all(page, key)
            values = []
            for index in range(await locators.count()):
                values.append(await element_value(locators.nth(index)))
            if expected in " ".join(values):
                return
        except SelectorNotFound:
            pass
        await page.wait_for_timeout(200)
    raise ValueError(f"Expected {expected!r} to appear in selector {key!r}")


def parse_decimal(text: str) -> Decimal:
    normalized = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Cannot parse numeric value from {text!r}") from exc
