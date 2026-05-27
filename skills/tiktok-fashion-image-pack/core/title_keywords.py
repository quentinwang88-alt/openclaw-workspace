#!/usr/bin/env python3
"""Title keyword library loader for likeU womens outerwear."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


SKILL_DIR = Path(__file__).resolve().parents[1]
DEFAULT_KEYWORDS_PATH = SKILL_DIR / "references" / "title_keywords_th_womens_outerwear.json"


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


def load_keywords(path: Optional[Path] = None) -> Dict[str, Any]:
    path = path or DEFAULT_KEYWORDS_PATH
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_keyword_group(subtype: str, keywords: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if keywords is None:
        keywords = load_keywords()
    lookup_key = SUBTYPE_SERIES_MAP.get(subtype, "unknown_womens_top")
    return keywords.get(lookup_key, keywords.get("unknown_womens_top", {}))


def get_series_info(subtype: str, keywords: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    group = resolve_keyword_group(subtype, keywords)
    return {
        "series_name_cn": str(group.get("series_name_cn") or ""),
        "series_code_prefix": str(group.get("series_code_prefix") or ""),
    }


def get_available_terms(subtype: str, keywords: Optional[Dict[str, Any]] = None) -> Dict[str, List[str]]:
    group = resolve_keyword_group(subtype, keywords)
    return {
        "core_terms": group.get("core_terms") or [],
        "material_terms": group.get("material_terms") or [],
        "fit_terms": group.get("fit_terms") or [],
        "structure_terms": group.get("structure_terms") or [],
        "style_terms": group.get("style_terms") or [],
    }


def build_keywords_prompt_text(subtype: str, keywords: Optional[Dict[str, Any]] = None) -> str:
    group = resolve_keyword_group(subtype, keywords)
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
