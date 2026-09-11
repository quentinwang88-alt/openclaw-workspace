"""Register the user-selected MX wig persona (candidate A) as a persona asset.

Idempotent, additive-only: inserts or refreshes exactly ONE row
(MX_WIG_CAST_A_001) in the existing light-tryon persona library and never
touches any other row. The persona binds only OPV_MX_PHOTO_001; its primary
face reference is the user-casted original image, verified by SHA256 before
any write. The reference is NOT copied over and the original file is never
modified.

Run:  /usr/bin/python3 scripts/register_mx_wig_persona.py [--db PATH]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
DEFAULT_DB = (
    WORKSPACE_ROOT / "skills" / "lightweight-tryon-video" / "var" / "light_tryon.sqlite3"
)

PERSONA_ID = "MX_WIG_CAST_A_001"
PERSONA_NAME = "MX 假发图文人物 A｜日常亲和"
PRIMARY_REFERENCE = Path(
    "/Users/likeu3/output/imagegen/mx-wig-casting-v1/A-日常亲和.png"
)
EXPECTED_SHA256 = (
    "5d162ac84a9a6f419b675a392ca968c2b4654e577a24bb3662b34f2ad560bff0"
)
# 人物主参考只负责脸部身份、自然肤色与年龄感；发型/发色/表情/姿态由每页
# 计划决定（MX_WEEKEND_HAIR_CHOICE 首批 choice_axis=style）。这是 MX 专属
# 契约，不修改泰语人物包“主脸负责发型”的默认语义。
PROMPT_CORE = (
    "虚构成年女性（约27岁），柔和椭圆脸、自然暖肤色、温暖棕色眼睛、自然精致妆；"
    "亲近、放松、可信的日常美感。脸部身份、肤色与年龄感以主参考图为唯一权威；"
    "发型、发色、表情与姿态按每页计划执行，不锁定。仅用于墨西哥假发图文。"
)
ACCOUNT_IDS = ["OPV_MX_PHOTO_001"]
MARKETS = ["MX"]


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--apply", action="store_true",
                        help="without this flag the script only verifies and previews")
    args = parser.parse_args()

    if not PRIMARY_REFERENCE.is_file():
        print(f"FAIL: persona primary reference missing: {PRIMARY_REFERENCE}")
        return 2
    actual = sha256_of(PRIMARY_REFERENCE)
    if actual != EXPECTED_SHA256:
        print("FAIL: persona primary reference sha256 mismatch — "
              f"expected {EXPECTED_SHA256}, got {actual}; refusing to register")
        return 2

    reference_entry = {
        "local_path": str(PRIMARY_REFERENCE),
        "sha256": actual,
        "role": "FACE_FRONT_NEUTRAL",
        "approved": True,
        "is_primary": True,
        "name": "A-日常亲和（用户选定选角原图）",
        "note": "选角原图由内置 imagegen 生成（prompts.json candidate A）；"
                "主参考只负责脸部身份，不锁定发型。",
    }
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "persona_id": PERSONA_ID,
        "persona_name": PERSONA_NAME,
        "status": "testing",
        "gender": "female",
        "age_group": "25-30",
        "hair_style": "variable_by_page_plan",
        "hair_color": "variable_by_page_plan",
        "skin_tone": "medium warm beige (locked to reference)",
        "face_visibility": "face_visible",
        "makeup_style": "natural_polished",
        "vibe": "approachable_everyday",
        "prompt_core": PROMPT_CORE,
        "prompt_negative": "",
        "notes": "MX wig native-photo persona; registered by "
                 "packages/organic_photo_video/scripts/register_mx_wig_persona.py",
        "account_ids": json.dumps(ACCOUNT_IDS),
        "markets": json.dumps(MARKETS),
        "reference_images": json.dumps([reference_entry], ensure_ascii=False),
        "consistency_version": "PERSONA_MX_WIG_V1",
        "config_version": "V1",
        "source_hash": actual,
        "source_payload": json.dumps({
            "casting_dir": str(PRIMARY_REFERENCE.parent),
            "casting_prompts": "prompts.json (candidate A)",
            "casting_design": "人物设计方案.md",
            "registered_by": "register_mx_wig_persona.py",
        }, ensure_ascii=False),
        "updated_at": now,
    }

    connection = sqlite3.connect(args.db)
    connection.row_factory = sqlite3.Row
    try:
        existing = connection.execute(
            "SELECT persona_id, source_hash, status FROM persona_templates "
            "WHERE persona_id=?", (PERSONA_ID,)
        ).fetchone()
        if existing is not None and existing["source_hash"] != actual:
            print(f"FAIL: {PERSONA_ID} already registered with a different "
                  "reference hash; refusing to overwrite — register a new id instead")
            return 2
        if existing is not None:
            print(f"OK(already): {PERSONA_ID} registered, hash matches, "
                  f"status={existing['status']}")
            return 0
        if not args.apply:
            print("DRY-RUN: verified reference hash; would insert "
                  f"{PERSONA_ID} into {args.db} (re-run with --apply)")
            return 0
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(persona_templates)")
        }
        values = dict(payload)
        values.setdefault(
            "created_at", now
        )
        missing = set(values) - columns
        if missing:
            print(f"FAIL: persona_templates lacks columns {sorted(missing)}")
            return 2
        fields = ", ".join(values)
        placeholders = ", ".join("?" for _ in values)
        connection.execute(
            f"INSERT INTO persona_templates ({fields}) VALUES ({placeholders})",
            tuple(values.values()),
        )
        connection.commit()
        print(f"OK: inserted {PERSONA_ID} (reference sha256 {actual[:16]}…, "
              f"status testing, accounts {ACCOUNT_IDS})")
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
