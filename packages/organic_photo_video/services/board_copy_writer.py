"""Deterministic localized copy for outfit-breakdown board covers."""

from __future__ import annotations

from typing import Dict, List, Mapping


TH_HOOK_COPY: Dict[str, Dict[str, str]] = {
    "multi_look": {
        "title": "ไอเท็มเดียว แต่งได้หลายลุค",
        "footer": "ชอบลุคไหนมากที่สุด?",
    },
    "outfit_formula": {
        "title": "สูตรแต่งตัวลุคนี้",
        "footer": "เซฟลุคนี้ไว้แต่งตาม",
    },
    "save_this_look": {
        "title": "เซฟลุคนี้ไว้แต่งตาม",
        "footer": "เรียบง่าย แต่แต่งตามได้จริง",
    },
    "what_i_wore": {
        "title": "วันนี้ใส่อะไรบ้าง",
        "footer": "ชอบชิ้นไหนมากที่สุด?",
    },
    "three_piece_formula": {
        "title": "3 ชิ้น ก็ได้ลุคนี้",
        "footer": "ลุคนี้แต่งตามง่ายมาก",
    },
}

TH_ROLE_LABELS = {
    "target_product": "ไอเท็มหลัก",
    "top_inner": "เสื้อด้านใน",
    "bottom": "ท่อนล่าง",
    "onepiece": "ชุดชิ้นเดียว",
    "footwear": "รองเท้า",
    "bag": "กระเป๋า",
    "accessories": "เครื่องประดับ",
}


class BoardCopyError(ValueError):
    pass


def board_copy(hook_strategy: str, locale: str = "th-TH") -> Dict[str, str]:
    if locale != "th-TH":
        raise BoardCopyError(f"outfit board copy is not approved for {locale!r}")
    key = str(hook_strategy or "outfit_formula")
    return dict(TH_HOOK_COPY.get(key) or TH_HOOK_COPY["outfit_formula"])


def item_label(
    role: str,
    item: Mapping[str, object] | None = None,
    *,
    locale: str = "th-TH",
) -> str:
    if locale != "th-TH":
        raise BoardCopyError(f"outfit board item labels are not approved for {locale!r}")
    payload = dict(item or {})
    localized = payload.get("title_i18n") or payload.get("label_i18n") or {}
    if isinstance(localized, Mapping):
        value = str(localized.get(locale) or "").strip()
        if value:
            return value
    value = str(payload.get("label") or payload.get("title") or "").strip()
    # Production boards must never leak Chinese template descriptions.  A
    # localized role label is safer than pretending an untranslated title is
    # approved market copy.
    if value and not any("\u4e00" <= char <= "\u9fff" for char in value):
        return value
    return TH_ROLE_LABELS.get(str(role), "ไอเท็มแต่งตัว")


def numbered_item_lines(items: List[Mapping[str, object]], locale: str) -> List[str]:
    return [
        item_label(str(item.get("role") or ""), item, locale=locale)
        for item in items
    ]
