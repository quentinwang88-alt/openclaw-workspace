"""Locale purity checks for user-visible OPV copy.

Instructions/prompts may be Chinese, but titles, captions, hashtags and image
overlays must not leak another market's script into the final asset.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_THAI_RE = re.compile(r"[\u0e00-\u0e7f]")


def visible_text_issues(
    text: Any, locale: str, field: str, *, require_locale_script: bool = True
) -> List[str]:
    value = str(text or "").strip()
    if not value:
        return []
    issues: List[str] = []
    if _CJK_RE.search(value):
        issues.append(f"{field} contains CJK characters")
    if require_locale_script and locale.lower().startswith("th-") and not _THAI_RE.search(value):
        issues.append(f"{field} has no Thai characters for locale {locale}")
    if not locale.lower().startswith("th-") and _THAI_RE.search(value):
        issues.append(f"{field} contains Thai characters for locale {locale}")
    return issues


def copy_locale_issues(copy_block: Dict[str, Any], locale: str) -> List[str]:
    issues: List[str] = []
    for field in ("title", "caption", "cover_text"):
        issues.extend(visible_text_issues(copy_block.get(field), locale, field))
    for index, hashtag in enumerate(copy_block.get("hashtags") or []):
        issues.extend(
            visible_text_issues(
                hashtag,
                locale,
                f"hashtags[{index}]",
                require_locale_script=False,
            )
        )
    for index, text in enumerate(copy_block.get("slide_texts") or []):
        issues.extend(
            # A slide may intentionally be only "A · OOTD" or another
            # short Latin label.  Title/caption already establish the locale;
            # here we only prevent a foreign script from leaking on-image.
            visible_text_issues(
                text, locale, f"slide_texts[{index}]",
                require_locale_script=False,
            )
        )
    return issues
