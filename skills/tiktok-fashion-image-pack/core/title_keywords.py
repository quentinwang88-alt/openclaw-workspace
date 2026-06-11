#!/usr/bin/env python3
"""Title keyword library loader for likeU category title generation."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


SKILL_DIR = Path(__file__).resolve().parents[1]
DEFAULT_KEYWORDS_PATH = SKILL_DIR / "references" / "title_keywords_th_womens_outerwear.json"
HAIR_ACCESSORY_KEYWORDS_PATH = SKILL_DIR / "references" / "title_keywords_hair_accessories.json"


SUBTYPE_SERIES_MAP: Dict[str, str] = {
    "leather_jacket": "leather_jacket",
    "suede_jacket": "suede_jacket",
    "faux_fur_jacket": "faux_fur_jacket",
    "puffer_jacket": "puffer_jacket",
    "down_jacket": "down_jacket",
    "bomber_jacket": "bomber_jacket",
    "baseball_jacket": "bomber_jacket",
    "utility_jacket": "utility_jacket",
    "knit_top": "knit_top",
    "cardigan": "cardigan",
    "unknown_womens_top": "unknown_womens_top",
}

HAIR_ACCESSORY_SUBTYPE_SERIES_MAP: Dict[str, str] = {
    "hair_clip": "hair_clip",
    "claw_clip": "claw_clip",
    "hair_bow": "hair_bow",
    "headband": "headband",
    "scrunchie": "scrunchie",
    "hair_tie": "hair_tie",
    "hair_pin": "hair_pin",
    "unknown_hair_accessory": "unknown_hair_accessory",
}


def normalize_country(country: str = "") -> str:
    value = (country or "TH").strip().upper()
    if value in ("THAILAND", "ไทย"):
        return "TH"
    if value in ("VIETNAM", "VIET NAM", "เวียดนาม"):
        return "VN"
    return value or "TH"


def normalize_category(category: str = "") -> str:
    value = (category or "女装上装/外套").strip()
    if value in ("hair_accessory", "hair_accessories", "发饰", "頭飾"):
        return "发饰"
    if value in ("womens_tops", "women_outerwear", "女装上装", "女装外套", "女装上装/外套"):
        return "女装上装/外套"
    return value


def load_keywords(path: Optional[Path] = None) -> Dict[str, Any]:
    path = path or DEFAULT_KEYWORDS_PATH
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_keywords_for(category: str = "女装上装/外套", country: str = "TH") -> Dict[str, Any]:
    normalized_category = normalize_category(category)
    normalized_country = normalize_country(country)
    if normalized_category == "发饰":
        data = load_keywords(HAIR_ACCESSORY_KEYWORDS_PATH)
        return data.get(normalized_country, data.get("TH", {}))
    return load_keywords(DEFAULT_KEYWORDS_PATH)


def resolve_keyword_group(
    subtype: str,
    keywords: Optional[Dict[str, Any]] = None,
    *,
    category: str = "女装上装/外套",
    country: str = "TH",
) -> Dict[str, Any]:
    if keywords is None:
        keywords = load_keywords_for(category=category, country=country)
    normalized_category = normalize_category(category)
    if normalized_category == "发饰":
        lookup_key = HAIR_ACCESSORY_SUBTYPE_SERIES_MAP.get(subtype, "unknown_hair_accessory")
        return keywords.get(lookup_key, keywords.get("unknown_hair_accessory", {}))
    lookup_key = SUBTYPE_SERIES_MAP.get(subtype, "unknown_womens_top")
    return keywords.get(lookup_key, keywords.get("unknown_womens_top", {}))


def get_series_info(
    subtype: str,
    keywords: Optional[Dict[str, Any]] = None,
    *,
    category: str = "女装上装/外套",
    country: str = "TH",
) -> Dict[str, str]:
    group = resolve_keyword_group(subtype, keywords, category=category, country=country)
    return {
        "series_name_cn": str(group.get("series_name_cn") or ""),
        "series_code_prefix": str(group.get("series_code_prefix") or ""),
    }


def get_available_terms(
    subtype: str,
    keywords: Optional[Dict[str, Any]] = None,
    *,
    category: str = "女装上装/外套",
    country: str = "TH",
) -> Dict[str, List[str]]:
    group = resolve_keyword_group(subtype, keywords, category=category, country=country)
    return {
        "core_terms": group.get("core_terms") or [],
        "material_terms": group.get("material_terms") or [],
        "fit_terms": group.get("fit_terms") or [],
        "structure_terms": group.get("structure_terms") or [],
        "style_terms": group.get("style_terms") or [],
    }


def build_keywords_prompt_text(
    subtype: str,
    keywords: Optional[Dict[str, Any]] = None,
    *,
    category: str = "女装上装/外套",
    country: str = "TH",
) -> str:
    group = resolve_keyword_group(subtype, keywords, category=category, country=country)
    lines: List[str] = []
    sections: List[tuple] = [
        ("核心品类词", group.get("core_terms") or []),
        ("材质/外观词", group.get("material_terms") or []),
        ("版型/长度词", group.get("fit_terms") or []),
        ("结构卖点词", group.get("structure_terms") or []),
        ("风格/场景词", group.get("style_terms") or []),
    ]
    for label, terms in sections:
        if terms:
            lines.append(f"- {label}: {' / '.join(terms)}")
    return "\n".join(lines) if lines else "（无预置关键词，从商品事实中自行提取）"
