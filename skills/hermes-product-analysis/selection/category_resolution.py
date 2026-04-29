#!/usr/bin/env python3
"""Resolve market_id/category_id without cross-category fallback."""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import groupby
from typing import Any, Dict, Iterable, List, Tuple


SUPPORTED_MARKETS = {"VN", "TH", "MY"}
CATEGORY_ALIASES = {
    "发饰": "hair_accessory",
    "hair_accessory": "hair_accessory",
    "hair accessory": "hair_accessory",
    "耳环": "earrings",
    "耳饰": "earrings",
    "earrings": "earrings",
    "earring": "earrings",
    "女装上装": "womens_tops",
    "轻上装": "womens_tops",
    "womens_tops": "womens_tops",
    "women tops": "womens_tops",
    "light_tops": "womens_tops",
}
CATEGORY_KEYWORDS = {
    "earrings": ["耳环", "耳饰", "耳钉", "耳坠", "耳夹", "earring", "earrings", "khuyên tai", "ต่างหู", "subang"],
    "hair_accessory": ["发饰", "发夹", "抓夹", "发箍", "发圈", "hair clip", "claw clip", "kẹp tóc", "กิ๊บติดผม"],
    "womens_tops": ["上衣", "开衫", "衬衫", "罩衫", "防晒衫", "cardigan", "shirt", "blouse"],
}


@dataclass
class ResolutionResult:
    market_id: str = ""
    market_resolution_method: str = ""
    market_confidence: float = 0.0
    category_id: str = ""
    category_name: str = ""
    category_resolution_method: str = ""
    category_confidence: float = 0.0
    category_resolution_version: str = "category_resolution.v1"
    risk_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "market_resolution_method": self.market_resolution_method,
            "market_confidence": self.market_confidence,
            "category_id": self.category_id,
            "category_name": self.category_name,
            "category_resolution_method": self.category_resolution_method,
            "category_confidence": self.category_confidence,
            "category_resolution_version": self.category_resolution_version,
            "risk_flags": list(self.risk_flags),
        }


def resolve_market_id(record: Dict[str, Any]) -> Tuple[str, str, float, List[str]]:
    for key, method in (
        ("market_id", "market_id_field"),
        ("市场ID", "market_id_field"),
        ("country", "fastmoss_country_field"),
        ("国家/地区", "fastmoss_country_field"),
        ("站点", "tiktok_shop_site_field"),
        ("market", "manual_market_field"),
        ("市场", "manual_market_field"),
    ):
        value = _normalize_market(record.get(key))
        if value:
            return value, method, 1.0, []
    return "", "missing", 0.0, ["market_id_missing"]


def resolve_category_id(record: Dict[str, Any]) -> Tuple[str, str, str, float, List[str]]:
    for key, method in (
        ("category_id", "category_id_field"),
        ("类目ID", "category_id_field"),
        ("platform_category_path", "platform_mapping"),
        ("平台类目路径", "platform_mapping"),
        ("manual_category", "manual_field"),
        ("人工类目", "manual_field"),
        ("类目", "manual_field"),
        ("category", "manual_field"),
        ("商品分类", "manual_field"),
    ):
        category = _normalize_category(record.get(key))
        if category:
            return category, _category_name(category), method, 1.0, []

    text = " ".join(str(record.get(key) or "") for key in ("product_title", "商品标题", "product_name", "商品名称", "title"))
    keyword_matches = []
    lowered = text.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(keyword.lower() in lowered for keyword in keywords):
            keyword_matches.append(category)
    if len(keyword_matches) == 1:
        return keyword_matches[0], _category_name(keyword_matches[0]), "keyword_rule", 0.82, []
    if len(keyword_matches) > 1:
        return keyword_matches[0], _category_name(keyword_matches[0]), "keyword_rule", 0.65, ["manual_category_review_required", "multi_category_keyword_hit"]
    return "", "", "missing", 0.0, ["manual_category_review_required", "category_id_missing"]


def resolve_market_and_category(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    resolved = []
    for record in records:
        item = dict(record)
        market_id, market_method, market_confidence, market_flags = resolve_market_id(item)
        category_id, category_name, category_method, category_confidence, category_flags = resolve_category_id(item)
        flags = list(market_flags) + list(category_flags)
        if category_confidence and category_confidence < 0.8 and "manual_category_review_required" not in flags:
            flags.append("manual_category_review_required")
        item.update(
            {
                "market_id": market_id,
                "market_resolution_method": market_method,
                "market_confidence": market_confidence,
                "category_id": category_id,
                "category_name": category_name,
                "category_resolution_method": category_method,
                "category_confidence": category_confidence,
                "category_resolution_version": "category_resolution.v1",
                "risk_flags": sorted(set(list(item.get("risk_flags") or []) + flags)),
            }
        )
        resolved.append(item)
    return resolved


def group_by_market_and_category(records: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    resolved = resolve_market_and_category(records)
    valid = [item for item in resolved if item.get("market_id") and item.get("category_id")]
    valid.sort(key=lambda item: (str(item.get("market_id")), str(item.get("category_id"))))
    return {
        key: list(items)
        for key, items in groupby(valid, key=lambda item: (str(item.get("market_id")), str(item.get("category_id"))))
    }


def process_batch_by_market_and_category(records: Iterable[Dict[str, Any]], processor) -> Dict[Tuple[str, str], Any]:
    grouped = group_by_market_and_category(records)
    return {key: processor(market_id=key[0], category_id=key[1], records=items) for key, items in grouped.items()}


def _normalize_market(value: Any) -> str:
    raw = str(value or "").strip().upper()
    aliases = {"越南": "VN", "泰国": "TH", "马来西亚": "MY", "VIETNAM": "VN", "THAILAND": "TH", "MALAYSIA": "MY"}
    normalized = aliases.get(raw, raw)
    return normalized if normalized in SUPPORTED_MARKETS else ""


def _normalize_category(value: Any) -> str:
    raw = str(value or "").strip()
    return CATEGORY_ALIASES.get(raw, CATEGORY_ALIASES.get(raw.lower(), ""))


def _category_name(category_id: str) -> str:
    return {"hair_accessory": "发饰", "earrings": "耳环", "womens_tops": "女装上装"}.get(category_id, category_id)
