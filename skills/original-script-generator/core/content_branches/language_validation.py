"""Branch-neutral lightweight writing-system guard for spoken surfaces."""
from __future__ import annotations

import re


THAI_RE = re.compile(r"[\u0E00-\u0E7F]")
CJK_RE = re.compile(r"[\u3400-\u9FFF]")
LATIN_RE = re.compile(r"[A-Za-zÀ-ỹ]")


def target_language_surface_error(text: str, target_language: str) -> str:
    value = str(text or "").strip()
    language = str(target_language or "").strip().lower()
    if not value or not language:
        return ""
    if language in {"泰语", "thai", "th", "th-th"}:
        if not THAI_RE.search(value):
            return "TARGET_LANGUAGE_THAI_SCRIPT_MISSING"
        if CJK_RE.search(value):
            return "TARGET_LANGUAGE_THAI_CONTAINS_CJK"
        return ""
    if language in {"中文", "汉语", "chinese", "zh", "zh-cn"}:
        return "" if CJK_RE.search(value) else "TARGET_LANGUAGE_CJK_SCRIPT_MISSING"
    if language in {
        "越南语", "vietnamese", "vi", "vi-vn",
        "马来语", "malay", "ms", "ms-my",
        "西班牙语", "墨西哥西班牙语", "spanish", "es", "es-mx",
        "英语", "english", "en", "en-us",
    }:
        if CJK_RE.search(value) or THAI_RE.search(value):
            return "TARGET_LANGUAGE_WRITING_SYSTEM_MISMATCH"
        if not LATIN_RE.search(value):
            return "TARGET_LANGUAGE_LATIN_SCRIPT_MISSING"
    return ""
