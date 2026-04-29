#!/usr/bin/env python3
"""Sync store positioning cards from Feishu into the Market Insight SQLite DB."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List


SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from src.feishu import build_bitable_client, parse_feishu_bitable_url  # noqa: E402
from src.market_insight_db import MarketInsightDatabase  # noqa: E402
from src.models import StorePositioningCard  # noqa: E402


DEFAULT_DB_PATH = SKILL_DIR / "artifacts" / "market_insight" / "market_insight.db"

FIELD_ALIASES = {
    "store_id": ["店铺ID", "store_id", "shop_id", "店铺"],
    "card_name": ["店铺定位卡名称", "店铺名称", "店铺名", "card_name", "店铺ID"],
    "country": ["国家", "country"],
    "category": ["类目", "category", "产品类目"],
    "style_whitelist": ["风格白名单", "style_whitelist"],
    "style_blacklist": ["风格黑名单", "style_blacklist"],
    "target_price_bands": ["目标价格带", "target_price_bands", "price_bands"],
    "core_scenes": ["核心场景", "core_scenes", "scene_tags"],
    "content_tones": ["内容调性", "content_tones", "content_tone"],
    "core_value_points": ["核心价值点", "core_value_points", "value_points"],
    "target_audience": ["目标人群", "target_audience"],
    "selection_principles": ["选品原则", "selection_principles"],
    "notes": ["备注", "notes"],
}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip()


def _find_value(fields: Dict[str, Any], aliases: Iterable[str]) -> Any:
    alias_set = {_normalize_key(alias) for alias in aliases}
    for key, value in fields.items():
        if _normalize_key(key) in alias_set:
            return value
    return None


def _normalize_key(value: Any) -> str:
    return _safe_text(value).replace("_", "").replace("-", "").replace(" ", "").lower()


def _to_string_list(value: Any) -> List[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        results = []
        for item in value:
            if isinstance(item, dict):
                for key in ("name", "text", "value", "label"):
                    candidate = _safe_text(item.get(key))
                    if candidate:
                        results.append(candidate)
                        break
                continue
            text = _safe_text(item)
            if text:
                results.append(text)
        return results
    if isinstance(value, dict):
        return [text for text in (_safe_text(item) for item in value.values()) if text]
    text = _safe_text(value)
    if not text:
        return []
    normalized = text.replace("，", ",").replace("、", ",").replace("\n", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def _normalize_country(value: Any) -> str:
    text = _safe_text(value).upper()
    if text in {"越南", "VN"}:
        return "VN"
    if text in {"泰国", "TH"}:
        return "TH"
    return text


def _normalize_category(value: Any) -> str:
    text = _safe_text(value)
    mapping = {
        "发饰": "hair_accessory",
        "轻上装": "light_tops",
        "hair_accessory": "hair_accessory",
        "light_tops": "light_tops",
    }
    return mapping.get(text, text.lower())


def _build_card(record_id: str, fields: Dict[str, Any]) -> StorePositioningCard:
    return StorePositioningCard(
        source_record_id=record_id,
        store_id=_safe_text(_find_value(fields, FIELD_ALIASES["store_id"])),
        card_name=_safe_text(_find_value(fields, FIELD_ALIASES["card_name"])),
        country=_normalize_country(_find_value(fields, FIELD_ALIASES["country"])),
        category=_normalize_category(_find_value(fields, FIELD_ALIASES["category"])),
        style_whitelist=_to_string_list(_find_value(fields, FIELD_ALIASES["style_whitelist"])),
        style_blacklist=_to_string_list(_find_value(fields, FIELD_ALIASES["style_blacklist"])),
        target_price_bands=_to_string_list(_find_value(fields, FIELD_ALIASES["target_price_bands"])),
        core_scenes=_to_string_list(_find_value(fields, FIELD_ALIASES["core_scenes"])),
        content_tones=_to_string_list(_find_value(fields, FIELD_ALIASES["content_tones"])),
        core_value_points=_to_string_list(_find_value(fields, FIELD_ALIASES["core_value_points"])),
        target_audience=_to_string_list(_find_value(fields, FIELD_ALIASES["target_audience"])),
        selection_principles=_to_string_list(_find_value(fields, FIELD_ALIASES["selection_principles"])),
        notes=_safe_text(_find_value(fields, FIELD_ALIASES["notes"])),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync store positioning cards from Feishu into SQLite.")
    parser.add_argument("--feishu-url", required=True)
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    args = parser.parse_args()

    info = parse_feishu_bitable_url(args.feishu_url)
    if not info:
        raise SystemExit("无法解析飞书表链接")
    client = build_bitable_client(feishu_url=args.feishu_url)
    cards: List[StorePositioningCard] = []
    skipped = 0
    for record in client.list_records(page_size=100, limit=None):
        card = _build_card(record.record_id, dict(record.fields))
        if not card.store_id and not card.card_name:
            skipped += 1
            continue
        cards.append(card)

    database = MarketInsightDatabase(Path(args.db_path))
    stored = database.upsert_store_positioning_cards(
        source_table_id=info.table_id,
        cards=cards,
        updated_at_epoch=int(time.time()),
    )
    print(
        json.dumps(
            {
                "source_table_id": info.table_id,
                "db_path": str(Path(args.db_path)),
                "processed": len(cards),
                "skipped": skipped,
                "stored": stored,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
