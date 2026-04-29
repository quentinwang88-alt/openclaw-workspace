#!/usr/bin/env python3
"""Shared helpers for normalizing task prices to RMB/CNY."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional


TARGET_PRICE_RMB_FIELDS = [
    "target_price_rmb",
    "target_price_cny",
    "售价_rmb",
    "售价_cny",
    "目标售价_rmb",
    "目标售价_cny",
    "目标售价人民币",
]

PRICE_TO_CNY_RATE_FIELDS = [
    "price_to_cny_rate",
    "target_price_to_cny_rate",
    "exchange_rate_to_cny",
    "exchange_rate_rmb",
    "汇率到人民币",
    "人民币汇率",
]

CURRENCY_FIELDS = [
    "target_price_currency",
    "price_currency",
    "currency",
    "售价币种",
    "目标售价币种",
    "币种",
]

MARKET_RATE_TO_CNY = {
    "CN": 1.0,
    "CNY": 1.0,
    "RMB": 1.0,
    "中国": 1.0,
    "人民币": 1.0,
    "VN": 0.000259,
    "VND": 0.000259,
    "越南": 0.000259,
}


def normalize_task_target_price_to_cny(task) -> Optional[float]:
    extra_fields = getattr(task, "extra_fields", {}) or {}

    explicit_rmb = _to_number(_find_extra_value(extra_fields, TARGET_PRICE_RMB_FIELDS))
    if explicit_rmb is not None:
        return explicit_rmb if explicit_rmb >= 0 else None

    raw_target_price = _to_number(getattr(task, "target_price", None))
    if raw_target_price is None or raw_target_price < 0:
        return None

    rate = resolve_price_to_cny_rate(
        target_market=getattr(task, "target_market", ""),
        extra_fields=extra_fields,
    )
    if rate is None:
        return round(raw_target_price, 4)
    return round(raw_target_price * rate, 4)


def resolve_price_to_cny_rate(target_market: str, extra_fields: Dict[str, Any]) -> Optional[float]:
    explicit_rate = _to_number(_find_extra_value(extra_fields, PRICE_TO_CNY_RATE_FIELDS))
    if explicit_rate is not None and explicit_rate > 0:
        return explicit_rate

    explicit_currency = _safe_text(_find_extra_value(extra_fields, CURRENCY_FIELDS))
    if explicit_currency:
        normalized_currency = _normalize_market_or_currency(explicit_currency)
        if normalized_currency in MARKET_RATE_TO_CNY:
            return MARKET_RATE_TO_CNY[normalized_currency]

    normalized_market = _normalize_market_or_currency(target_market)
    if normalized_market in MARKET_RATE_TO_CNY:
        return MARKET_RATE_TO_CNY[normalized_market]
    return None


def _find_extra_value(mapping: Dict[str, Any], aliases: Iterable[str]) -> Any:
    alias_set = {_normalize_key(alias) for alias in aliases}
    for key, value in mapping.items():
        if _normalize_key(key) in alias_set:
            return value
    return None


def _normalize_market_or_currency(value: Any) -> str:
    text = _safe_text(value).upper()
    if text in {"越南", "VN", "VND"}:
        return "VN"
    if text in {"中国", "CN", "CNY", "RMB", "人民币"}:
        return "CN"
    return text


def _normalize_key(value: Any) -> str:
    return _safe_text(value).replace("_", "").replace("-", "").replace(" ", "").lower()


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _to_number(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = _safe_text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
