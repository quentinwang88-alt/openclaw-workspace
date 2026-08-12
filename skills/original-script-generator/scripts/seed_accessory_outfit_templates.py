#!/usr/bin/env python3
"""Seed scarf/headscarf outfit templates into the shared lightweight database."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

SKILL_ROOT = Path(__file__).resolve().parents[1]
OPENCLAW_SKILLS = SKILL_ROOT.parent
LIGHTWEIGHT_ROOT = OPENCLAW_SKILLS / "lightweight-tryon-video"
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))
if str(LIGHTWEIGHT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(LIGHTWEIGHT_ROOT / "scripts"))

from core.complete_script_v3 import _ACCESSORY_OUTFIT_PROFILES  # noqa: E402
from light_tryon.database import LightTryonDB  # noqa: E402
from light_tryon.utils import json_dumps, now_iso  # noqa: E402


DEFAULT_DB_PATH = LIGHTWEIGHT_ROOT / "var" / "light_tryon.sqlite3"
SYNC_VERSION = "ORIGINAL_ACCESSORY_OUTFIT_V1"

TYPE_LABELS = {
    "silk_scarf": "丝巾",
    "headscarf": "头巾",
    "bracelet": "手链",
    "bangle": "手镯",
    "slim_bangle": "细手圈",
    "claw_clip": "抓夹",
    "hair_clip": "发夹",
}
ROLE_BY_TYPE = {
    "silk_scarf": "SUPPORTING_OUTFIT_NECK",
    "headscarf": "SUPPORTING_OUTFIT_HEAD",
    "bracelet": "SUPPORTING_OUTFIT_WRIST",
    "bangle": "SUPPORTING_OUTFIT_WRIST",
    "slim_bangle": "SUPPORTING_OUTFIT_WRIST",
    "claw_clip": "SUPPORTING_OUTFIT_HAIR",
    "hair_clip": "SUPPORTING_OUTFIT_HAIR",
}
BOTTOM_TYPE_BY_KEY = {
    "WIDELEG": "wide_leg_pants",
    "SHORTS": "white_shorts",
    "JUMPSUIT": "wide_leg_pants",
    "DENIM": "straight_jeans",
    "COMMUTE": "straight_jeans",
    "Y2K": "wide_leg_pants",
    "STREET": "white_shorts",
    "RESORT": "wide_leg_pants",
    "CITY": "wide_leg_pants",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _bottom_type(silhouette_key: str) -> str:
    for token, value in BOTTOM_TYPE_BY_KEY.items():
        if token in silhouette_key:
            return value
    return "wide_leg_pants"


def _template_id(product_type: str, profile: Dict[str, Any]) -> str:
    return "ORIG_" + product_type.upper() + "_" + _text(profile.get("silhouette_key"))


def _row(product_type: str, profile: Dict[str, Any], index: int) -> Dict[str, Any]:
    silhouette_key = _text(profile.get("silhouette_key"))
    role = ROLE_BY_TYPE[product_type]
    modes = [str(item).upper() for item in profile.get("supported_demonstration_modes") or []]
    zones = [str(item).upper() for item in profile.get("visibility_zones") or []]
    scene_families = [str(item).upper() for item in profile.get("scene_families") or []]
    recipe = profile.get("outfit_recipe") if isinstance(profile.get("outfit_recipe"), dict) else {}
    return {
        "styling_id": _template_id(product_type, profile),
        "styling_name": f"原创共享-{TYPE_LABELS[product_type]}-{silhouette_key}",
        "status": "enabled",
        "applicable_product_codes": ["*"],
        "applicable_product_type": [product_type],
        "target_role": role,
        "supported_demonstration_modes": modes,
        "scene_families": scene_families,
        "style_intensity": _text(profile.get("style_intensity") or "DAILY_STYLED"),
        "climate_profile": _text(profile.get("climate_profile") or "TH_WARM"),
        "silhouette_key": silhouette_key,
        "product_fit": ["不限"],
        "bottom_type": _bottom_type(silhouette_key),
        "bottom_color": [],
        "bottom_fit": [],
        "inner_type": _text(recipe.get("top")),
        "inner_color": "",
        "inner_requirements": "",
        "outfit_recipe": recipe,
        "base_outfit_direction": _text(profile.get("base_outfit_direction")),
        "hair_direction": _text(profile.get("hair_direction")),
        "neckline_direction": _text(profile.get("neckline_direction")),
        "outer_layer_direction": _text(profile.get("outer_layer_direction")),
        "palette_relation": _text(profile.get("palette_relation")),
        "visibility_zones": zones,
        "visibility_requirement": _text(profile.get("visibility_requirement")),
        "finish_direction": _text(profile.get("finish_direction")),
        "accessory_level": "minimal",
        "footwear_visibility": "optional",
        "vibe_tag": [_text(profile.get("style_family")) or "配饰穿搭"],
        "prompt_core": _text(profile.get("base_outfit_direction")),
        "notes": "由原创脚本配饰穿搭池同步生成；可在飞书调整结构化字段，标题和正文不进入原创匹配。",
        "config_version": SYNC_VERSION,
        "priority": 120 - index,
        "sync_status": "synced",
        "last_synced_at": now_iso(),
        "sync_error": "",
        "source_hash": "",
        "source_payload": {
            "source": "original-script-generator",
            "source_profile_key": silhouette_key,
            "product_type": product_type,
            "target_role": role,
            "config_version": SYNC_VERSION,
        },
    }


def _rows(product_types: Iterable[str]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for product_type in product_types:
        profiles = _ACCESSORY_OUTFIT_PROFILES.get(product_type) or ()
        for index, profile in enumerate(profiles, start=1):
            output.append(_row(product_type, dict(profile), index))
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument(
        "--product-type",
        action="append",
        choices=list(TYPE_LABELS),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    product_types = args.product_type or ["silk_scarf", "headscarf"]
    rows = _rows(product_types)
    if args.dry_run:
        print(json_dumps({"dry_run": True, "count": len(rows), "ids": [row["styling_id"] for row in rows]}))
        return 0

    db = LightTryonDB(args.db)
    db.init_schema()
    for row in rows:
        db.upsert_template("styling", row)
    print(json_dumps({"status": "seeded", "db": str(Path(args.db).expanduser()), "count": len(rows), "ids": [row["styling_id"] for row in rows]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
