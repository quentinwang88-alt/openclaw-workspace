#!/usr/bin/env python3
"""Lightweight Thai phrase normalizer for likeU title postprocessing."""
from __future__ import annotations

import re
from typing import Dict


REPLACEMENTS: Dict[str, str] = {
    "ทรงครอปหลวม": "ทรงครอปแบบหลวม",
    "ทรงสั้นทรงหลวม": "ทรงสั้นแบบหลวม",
    "ขนฟูเนื้อนุ่ม": "ขนฟูนุ่ม",
    "ดีไซน์เปิดหน้า": "แบบเปิดหน้า",
}

_REPLACEMENT_KEYS = sorted(REPLACEMENTS.keys(), key=len, reverse=True)
_WHITESPACE = re.compile(r"\s+")


def normalize_title(raw_title: str) -> str:
    title = _WHITESPACE.sub(" ", (raw_title or "").strip())
    for key in _REPLACEMENT_KEYS:
        if key in title:
            title = title.replace(key, REPLACEMENTS[key])
    return _WHITESPACE.sub(" ", title).strip()
