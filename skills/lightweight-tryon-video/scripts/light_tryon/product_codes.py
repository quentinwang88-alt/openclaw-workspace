"""Product identity normalization shared by config sync and read adapters."""
from __future__ import annotations

from typing import Any


# Clipboard object markers and zero-width formatting are not part of SKU IDs.
# Keep punctuation, visible whitespace and arbitrary letters intact: cleaning
# must not turn an invalid value into a different, apparently authorized SKU.
_CLIPBOARD_FORMATTING = str.maketrans("", "", "\ufffc\ufeff\u200b\u200c\u200d\u2060")


def normalize_product_code(value: Any) -> str:
    return str(value or "").translate(_CLIPBOARD_FORMATTING).strip()


def normalize_product_codes(values: list[Any]) -> list[str]:
    return list(dict.fromkeys(
        cleaned for value in values if (cleaned := normalize_product_code(value))
    ))
