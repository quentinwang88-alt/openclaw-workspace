"""Branch-neutral speech duration and writing-system helpers."""
from __future__ import annotations

import re
from typing import Any


_THAI_RE = re.compile(r"[\u0E00-\u0E7F]")
_CJK_RE = re.compile(r"[\u3400-\u9FFF]")
_WORD_RE = re.compile(r"[A-Za-zÀ-ỹ]+(?:['’-][A-Za-zÀ-ỹ]+)?")
_PAUSE_RE = re.compile(r"[。！？!?；;,.，…]")


def _text(value: Any) -> str:
    return str(value or "").strip()


def estimate_spoken_duration_seconds(text: Any, target_language: str) -> float:
    """Return a conservative, deterministic center estimate for short-form VO."""

    value = _text(text)
    if not value:
        return 0.0
    language = _text(target_language).lower()
    pauses = len(_PAUSE_RE.findall(value)) * 0.12
    if "泰" in language or language in {"thai", "th", "th-th"}:
        spoken_units = len(_THAI_RE.findall(value))
        seconds = spoken_units / 13.5
    elif "中" in language or language in {"chinese", "zh", "zh-cn"}:
        spoken_units = len(_CJK_RE.findall(value))
        seconds = spoken_units / 4.2
    else:
        words = len(_WORD_RE.findall(value))
        seconds = words / 2.65 if words else len(value) / 14.0
    return round(max(0.5, seconds + pauses), 1)


def contains_thai_script(value: Any) -> bool:
    return bool(_THAI_RE.search(_text(value)))


def contains_cjk(value: Any) -> bool:
    return bool(_CJK_RE.search(_text(value)))
