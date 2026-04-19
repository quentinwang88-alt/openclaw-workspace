#!/usr/bin/env python3
"""统计周处理工具。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, List, Tuple


def parse_stat_week(stat_week: str) -> Tuple[int, int]:
    """
    将统计周解析成可排序元组。

    支持：
    - 2026-W13
    - 2026W13
    - 2026-13
    """
    raw = stat_week.strip().upper().replace(" ", "")
    raw = raw.replace("W", "-").replace("_", "-")
    parts = [part for part in raw.split("-") if part]
    if len(parts) != 2:
        raise ValueError(f"无法解析统计周: {stat_week}")
    return (int(parts[0]), int(parts[1]))


def sort_stat_weeks(stat_weeks: Iterable[str]) -> List[str]:
    return sorted(stat_weeks, key=parse_stat_week)


def current_timestamp_text() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")

