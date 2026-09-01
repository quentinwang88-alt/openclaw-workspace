"""Rule-based Thai title/copy composer for OPV (Stage B/F copy step).

Works from a versioned title scheme (config/copy/TITLE_SCHEME_*.json):

- Hook formulas are templates with slots ({product_short}/{scenario}/
  {benefit}/{vibe}) filled from the slot bank, filtered by the task's theme.
- Output is deterministic and review-friendly: N ranked proposals plus the
  chosen top one, always flagged ``proposed_needs_confirmation`` — the human
  operator confirms or edits before submit (publish gate keeps this honest).
- Hashtags mix theme seeds + big-traffic tails, deduped, capped by the
  scheme's count range.

LLM polish can later rewrite WITHIN these rules; the scheme stays the contract.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from domain import contracts
from domain.contracts import ensure_valid

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEME_PATH = PACKAGE_ROOT / "config" / "copy" / "TITLE_SCHEME_TH_V1.json"

PROPOSED_STATUS = "proposed_needs_confirmation"

# THEME_<MARKET>_<KEY>[_Vn] -> scheme content key
THEME_CONTENT_KEY = {
    "TRAVEL_DEPARTURE": "travel_departure",
    "CAFE_DATE": "cafe",
    "OFFICE_COMMUTE": "office",
    "WEEKEND_MARKET": "market",
    "RAINY_LAYERING": "rainy",
    "CAMPUS_CASUAL": "campus",
    "PETITE_PROPORTION": "petite",
    "COLOR_POP": "colorpop",
}


class CopySchemeError(ValueError):
    pass


def load_scheme(path: Optional[Path] = None) -> Dict[str, Any]:
    scheme_path = Path(path) if path else DEFAULT_SCHEME_PATH
    with scheme_path.open("r", encoding="utf-8") as handle:
        scheme = json.load(handle)
    try:
        ensure_valid(validate_scheme(scheme), f"title scheme {scheme_path.name}")
    except contracts.ContractViolationError as exc:
        raise CopySchemeError(str(exc)) from exc
    return scheme


def validate_scheme(scheme: Any) -> List[str]:
    if not isinstance(scheme, dict):
        return ["scheme must be an object"]
    errors: List[str] = []
    if scheme.get("schema_version") != "opv-title-scheme-v1":
        errors.append("schema_version must be opv-title-scheme-v1")
    if not scheme.get("scheme_id"):
        errors.append("scheme_id required")
    rules = scheme.get("title_rules") or {}
    if not isinstance(rules.get("target_chars"), list) or len(rules["target_chars"]) != 2:
        errors.append("title_rules.target_chars must be [min, max]")
    if int(rules.get("max_chars") or 0) <= 0:
        errors.append("title_rules.max_chars must be positive")
    formulas = scheme.get("hook_formulas") or []
    if not formulas:
        errors.append("hook_formulas must not be empty")
    for formula in formulas:
        if not formula.get("id") or "{benefit}" not in str(formula.get("template") or "") and "{product_short}" not in str(formula.get("template") or ""):
            errors.append(f"formula {formula.get('id')!r} needs id and at least one slot")
    hashtags = scheme.get("hashtag_strategy") or {}
    if not isinstance(hashtags.get("count"), list) or len(hashtags["count"]) != 2:
        errors.append("hashtag_strategy.count must be [min, max]")
    return errors


def theme_content_key(plan: Dict[str, Any]) -> str:
    theme_id = str((plan.get("theme") or {}).get("id") or "")
    match = re.search(r"THEME_[A-Z]{2}_([A-Z_]+?)(?:_V\d+)?$", theme_id)
    if not match:
        return "default"
    return THEME_CONTENT_KEY.get(
        match.group(1).rstrip("_"), "default"
    )


def _slot(scheme: Dict[str, Any], key: str, content_key: str, category: str) -> str:
    bank = (scheme.get("slot_bank") or {}).get(key) or {}
    if key == "product_short":
        by_category = bank.get("by_category") or {}
        return str(by_category.get(category) or bank.get("default") or "ตัวนี้")
    value = bank.get(content_key) or bank.get("default") or ""
    return str(value or "")


def _benefit(scheme: Dict[str, Any], content_key: str) -> str:
    bank = (scheme.get("slot_bank") or {}).get("benefit") or {}
    options = bank.get(content_key) or bank.get("default") or []
    return str(options[0]) if options else "ใส่สบาย ชิคสุดๆ"


def compose_proposals(
    plan: Dict[str, Any],
    scheme: Dict[str, Any],
    *,
    product_category: str = "",
    count: int = 3,
) -> List[Dict[str, Any]]:
    content_key = theme_content_key(plan)
    slots = {
        "product_short": _slot(scheme, "product_short", content_key, product_category),
        "scenario": _slot(scheme, "scenario", content_key, product_category),
        "benefit": _benefit(scheme, content_key),
        "vibe": _slot(scheme, "vibe_emoji", content_key, product_category),
    }
    rules = scheme.get("title_rules") or {}
    max_chars = int(rules.get("max_chars") or 80)
    max_emoji = int(rules.get("max_emoji") or 0)
    banned = [w.lower() for w in (rules.get("banned_words") or [])]

    applicable: List[Dict[str, Any]] = []
    for formula in scheme.get("hook_formulas") or []:
        themes = list(formula.get("themes") or [])
        if themes and content_key not in themes:
            continue
        applicable.append(formula)
    applicable.sort(key=lambda f: int(f.get("weight") or 0), reverse=True)

    emoji_pattern = re.compile(
        "[\U0001F300-\U0001FAFF\u2600-\u27BF]", flags=re.UNICODE
    )
    proposals: List[Dict[str, Any]] = []
    seen: set = set()
    for formula in applicable:
        if len(proposals) >= max(count, 0):
            break
        title = str(formula.get("template") or "").format(**slots)
        title = re.sub(r"\s{2,}", " ", title).strip()
        if not title or title.lower() in seen:
            continue
        if len(title) > max_chars:
            continue
        if len(emoji_pattern.findall(title)) > max_emoji:
            continue
        if any(word in title.lower() for word in banned):
            continue
        seen.add(title.lower())
        proposals.append({"formula_id": formula["id"], "title": title})
    if not proposals:
        raise CopySchemeError(
            "no title proposal satisfied the scheme rules; widen the formula bank"
        )
    return proposals


def build_hashtags(plan: Dict[str, Any], scheme: Dict[str, Any]) -> List[str]:
    strategy = scheme.get("hashtag_strategy") or {}
    count_min, count_max = (int(v) for v in (strategy.get("count") or [3, 6]))
    content_key = theme_content_key(plan)
    ordered: List[str] = []
    for tag in (
        list(strategy.get("seeds_th") or [])
        + list((strategy.get("by_theme") or {}).get(content_key) or [])
        + list(strategy.get("tail") or [])
    ):
        tag = str(tag).strip()
        if tag and tag not in ordered:
            ordered.append(tag)
    return ordered[:count_max] if count_max else ordered[:6]


def build_caption(title: str, one_liner: str, hashtags: List[str]) -> str:
    lines = [title]
    if one_liner:
        lines.append(one_liner)
    if hashtags:
        lines.append(" ".join(hashtags))
    return "\n".join(line for line in lines if line)


def propose_copy(
    plan: Dict[str, Any],
    scheme: Dict[str, Any],
    *,
    product_category: str = "",
    count: int = 3,
) -> Dict[str, Any]:
    """Full copy proposal: title + caption + hashtags, human-confirm required."""
    proposals = compose_proposals(
        plan, scheme, product_category=product_category, count=count
    )
    chosen = proposals[0]["title"]
    content_key = theme_content_key(plan)
    one_liner_bank = (scheme.get("slot_bank") or {}).get("one_liner") or {}
    one_liner = str(
        one_liner_bank.get(content_key) or one_liner_bank.get("default")
        or (plan.get("theme") or {}).get("topic") or ""
    )
    rules = scheme.get("caption_rules") or {}
    one_liner = one_liner[: int(rules.get("one_liner_max_chars") or 80)]
    hashtags = build_hashtags(plan, scheme)
    cover_bank = (scheme.get("slot_bank") or {}).get("cover_text") or {}
    cover_text = str(
        cover_bank.get(content_key) or cover_bank.get("default")
        or _slot(scheme, "vibe_emoji", content_key, product_category)
    )
    return {
        "title": chosen,
        "caption": build_caption(chosen, one_liner, hashtags),
        "hashtags": hashtags,
        "cover_text": cover_text,
        "proposals": proposals,
        "status": PROPOSED_STATUS,
    }
