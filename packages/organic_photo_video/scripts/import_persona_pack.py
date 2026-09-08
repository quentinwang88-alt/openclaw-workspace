#!/usr/bin/env python3
"""Import an approved persona-pack candidate as a production persona asset.

One-time ops after the user picks a candidate (TH persona realism handoff 6.1):
validates the pack, copies images to the canonical persona directory, and
(upserts) the ``persona_templates`` row with role-typed references. Account
switch is separate: pass --switch-account to rebind OPV_TH_TEST_001.

Dry-run by default; pass --apply to write.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
DB_PATH = WORKSPACE_ROOT / "skills" / "lightweight-tryon-video" / "var" / "light_tryon.sqlite3"
PERSONA_ROOT = WORKSPACE_ROOT / "shared" / "data" / "persona_templates"
ACCOUNT_CONFIG = PACKAGE_ROOT / "config" / "accounts" / "OPV_TH_TEST_001.json"

sys.path.insert(0, str(PACKAGE_ROOT))

ROLE_ORDER = [
    "01_face_front_neutral", "02_face_three_quarter",
    "03_body_full_neutral", "04_body_full_motion",
]
ROLE_META = {
    "01_face_front_neutral": "FACE_FRONT_NEUTRAL",
    "02_face_three_quarter": "FACE_THREE_QUARTER",
    "03_body_full_neutral": "BODY_FULL_NEUTRAL",
    "04_body_full_motion": "BODY_FULL_MOTION",
}
PERSONA_ID = "TH_APPAREL_REAL_01_001"
PROMPT_CORE = (
    "泰国年轻女性，自然日常感，真实皮肤纹理，头颈自然水平，"
    "表情放松不刻意，像同行朋友手机抓拍的穿搭内容人物"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, choices=["A", "B"])
    parser.add_argument("--candidates-root", default=str(
        PERSONA_ROOT / "_candidates_20260907"))
    parser.add_argument("--apply", action="store_true", help="write DB + account config")
    parser.add_argument("--switch-account", action="store_true",
                        help="also rebind OPV_TH_TEST_001 persona_ref_id")
    args = parser.parse_args()

    from services.persona_pack import build_persona_pack, evaluate_persona_pack

    source_dir = Path(args.candidates_root) / f"TH_APPAREL_REAL_01_{args.candidate}"
    if not source_dir.is_dir():
        raise SystemExit(f"候选目录不存在：{source_dir}")
    files = []
    for role in ROLE_ORDER:
        path = source_dir / f"{role}.png"
        if not path.is_file():
            raise SystemExit(f"缺少角色图：{path}")
        files.append((role, path))

    snapshot = {"persona_id": PERSONA_ID, "reference_items": [
        {"local_path": str(path), "role": ROLE_META[role], "approved": True}
        for role, path in files
    ]}
    evaluation = evaluate_persona_pack(build_persona_pack(snapshot))
    if not evaluation["ready"]:
        raise SystemExit(f"候选包未通过 readiness 校验：{evaluation['issues']}")

    target_dir = PERSONA_ROOT / PERSONA_ID
    reference_items = []
    for role, path in files:
        target = target_dir / path.name
        reference_items.append({
            "local_path": str(target), "name": path.name,
            "sha256": sha256(path), "size": path.stat().st_size,
            "type": "image/png", "role": ROLE_META[role], "approved": True,
        })

    now = datetime.now(timezone.utc).isoformat()
    plan = {
        "persona_id": PERSONA_ID,
        "persona_name": f"泰国真实感人物 01｜候选{args.candidate}",
        "status": "testing",
        "markets": json.dumps(["TH"]),
        "prompt_core": PROMPT_CORE,
        "reference_images": json.dumps(reference_items, ensure_ascii=False),
        "hair_style": "candidate-specific",
        "notes": f"persona realism program pack; source={source_dir}; qa=vision_qa.json",
        "sync_status": "local_only",
        "last_synced_at": now,
        "source_payload": json.dumps({
            "authority": "persona_realism_program_20260907",
            "candidate": args.candidate,
            "qa_evidence": str(source_dir / "vision_qa.json"),
        }, ensure_ascii=False),
        "updated_at": now,
    }
    print("待写入 persona_templates：")
    print(json.dumps({**plan, "reference_images": reference_items},
                     ensure_ascii=False, indent=1)[:1200])
    if args.switch_account:
        print("\n待切换 config/accounts/OPV_TH_TEST_001.json：")
        print(f"  persona_ref_id -> {PERSONA_ID}")
        print("  allowed_persona_refs 前插", PERSONA_ID)
    if not args.apply:
        print("\n(dry-run) 确认无误后加 --apply 执行；账号切换另加 --switch-account。")
        return 0

    target_dir.mkdir(parents=True, exist_ok=True)
    for role, path in files:
        shutil.copy2(path, target_dir / path.name)
    connection = sqlite3.connect(DB_PATH)
    try:
        existing = connection.execute(
            "SELECT persona_id FROM persona_templates WHERE persona_id=?", (PERSONA_ID,)
        ).fetchone()
        if existing:
            sets = ", ".join(f"{key}=?" for key in plan)
            connection.execute(
                f"UPDATE persona_templates SET {sets} WHERE persona_id=?",
                [*plan.values(), PERSONA_ID],
            )
        else:
            connection.execute(
                "INSERT INTO persona_templates (persona_id, created_at, " +
                ", ".join(plan) + ") VALUES (" + ", ".join("?" for _ in range(len(plan) + 2)) + ")",
                [PERSONA_ID, now, *plan.values()],
            )
        connection.commit()
    finally:
        connection.close()
    print(f"persona_templates 已写入 {PERSONA_ID}（status=testing）")

    if args.switch_account:
        config = json.loads(ACCOUNT_CONFIG.read_text(encoding="utf-8"))
        config["persona_ref_id"] = PERSONA_ID
        allowed = config.get("operating_rules", {}).get("allowed_persona_refs") or []
        config["operating_rules"]["allowed_persona_refs"] = [
            PERSONA_ID] + [ref for ref in allowed if ref != PERSONA_ID]
        config.setdefault("persona_snapshot", {})["note"] = (
            f"2026-09-07 人物真实感改造切换；来源候选 {args.candidate}；"
            "旧人物保留在 allowed_persona_refs 供历史追溯"
        )
        config["persona_snapshot"]["reference_image_count"] = 4
        ACCOUNT_CONFIG.write_text(
            json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"账号 {ACCOUNT_CONFIG.name} 已切换到 {PERSONA_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
