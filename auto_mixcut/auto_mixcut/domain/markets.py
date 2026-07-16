from __future__ import annotations


_MARKET_ALIASES = {
    "VN": "VN",
    "VIETNAM": "VN",
    "越南": "VN",
    "TH": "TH",
    "THAILAND": "TH",
    "泰国": "TH",
    "MY": "MY",
    "MALAYSIA": "MY",
    "马来西亚": "MY",
    "PH": "PH",
    "PHILIPPINES": "PH",
    "菲律宾": "PH",
    "SG": "SG",
    "SINGAPORE": "SG",
    "新加坡": "SG",
    "ID": "ID",
    "INDONESIA": "ID",
    "印度尼西亚": "ID",
    "印尼": "ID",
    "TW": "TW",
    "TAIWAN": "TW",
    "中国台湾": "TW",
    "台湾": "TW",
    "SEA": "SEA",
    "SOUTHEAST ASIA": "SEA",
    "东南亚通用": "SEA",
}


def canonical_market(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return _MARKET_ALIASES.get(text.upper(), _MARKET_ALIASES.get(text, text.upper()))


def market_matches(track_country: object, target_market: object) -> bool:
    track = canonical_market(track_country)
    target = canonical_market(target_market)
    return bool(track and target and track == target)
