#!/usr/bin/env python3
"""Dry-run first migration helper for market/category isolation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


MAPPING = {
    "少女礼物感型": "VN__hair_accessory__sweet_gift",
    "盘发效率型": "VN__hair_accessory__hair_up_efficiency",
    "头盔友好整理型": "VN__hair_accessory__helmet_friendly",
    "甜感装饰型": "VN__hair_accessory__sweet_decorative",
    "大体量气质型": "VN__hair_accessory__volume_elegance",
    "韩系轻通勤型": "VN__hair_accessory__korean_light_commute",
    "基础通勤型": "VN__hair_accessory__basic_commute",
    "发箍修饰型": "VN__hair_accessory__headband_shape",
    "发圈套组型": "VN__hair_accessory__hair_tie_set",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--output", default="artifacts/category_isolation_migration_plan.json")
    args = parser.parse_args()
    payload = {
        "dry_run": bool(args.dry_run),
        "market_id": "VN",
        "category_id": "hair_accessory",
        "direction_id_migration": MAPPING,
        "note": "This helper currently emits the migration plan; DB/Feishu writes must be enabled explicitly in a later guarded migration.",
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
