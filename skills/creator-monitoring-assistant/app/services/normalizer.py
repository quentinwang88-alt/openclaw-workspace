#!/usr/bin/env python3
"""清洗与标准化。"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation


def normalize_creator_name(name: str) -> str:
    if name is None:
        return ""
    value = str(name).replace("\u200b", " ").replace("\ufeff", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value.lower()


def parse_money(value: str) -> Decimal:
    if value is None:
        return Decimal("0")
    text = str(value).strip()
    if not text:
        return Decimal("0")
    text = text.replace(",", "")
    text = re.sub(r"[^\d.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def parse_int(value: str) -> int:
    if value is None:
        return 0
    text = str(value).strip()
    if not text:
        return 0
    text = text.replace(",", "")
    text = re.sub(r"[^\d.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0

