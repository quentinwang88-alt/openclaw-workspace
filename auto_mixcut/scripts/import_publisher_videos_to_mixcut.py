#!/usr/bin/env python3
"""Import downloaded original/remake videos into the Auto Mixcut material pool."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.oss_storage_skill import OSSStorageSkill  # noqa: E402


DEFAULT_PUBLISH_DB = Path("/Users/likeu3/.openclaw/shared/data/short_video_auto_publish.sqlite3")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--publish-db", default=str(DEFAULT_PUBLISH_DB))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    rows = _publisher_rows(Path(args.publish_db), args.product_id)
    if args.limit:
        rows = rows[: args.limit]

    imported: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in rows:
        script_id = str(row.get("script_id") or "").strip()
        path = Path(str(row.get("local_file_path") or ""))
        source_identity = f"publisher:{script_id}"
        if not script_id:
            skipped.append({"reason": "missing_script_id", "row": row})
            continue
        if not path.exists():
            skipped.append({"script_id": script_id, "reason": "local_file_missing", "path": str(path)})
            continue
        existing = ctx.repo.list_where("assets", "product_id=? AND source_identity=?", (args.product_id, source_identity))
        if existing:
            skipped.append({"script_id": script_id, "reason": "already_imported", "asset_id": existing[0].get("asset_id")})
            continue
        if args.dry_run:
            imported.append({"script_id": script_id, "dry_run": True, "path": str(path)})
            continue

        source_type, trust = _source_policy(row)
        res = OSSStorageSkill(ctx).upload_asset(
            args.product_id,
            str(path),
            source_type=source_type,
            source_trust_level=trust,
            product_binding_type="exact_sku",
        )
        if not res.success:
            skipped.append({"script_id": script_id, "reason": "upload_failed", "error": res.to_dict()})
            continue
        asset_id = res.data["asset_id"]
        ctx.repo.update(
            "assets",
            "asset_id",
            asset_id,
            {
                "source_identity": source_identity,
                "scene_tag": str(row.get("script_source") or row.get("publish_purpose") or "publisher_video"),
                "local_file_status": "imported_from_short_video_publisher",
            },
        )
        imported.append({
            "script_id": script_id,
            "asset_id": asset_id,
            "source_type": source_type,
            "source_trust_level": trust,
            "path": str(path),
        })

    print({"product_id": args.product_id, "found": len(rows), "imported": imported, "skipped": skipped})
    return 0


def _publisher_rows(db_path: Path, product_id: str) -> list[dict[str, Any]]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT
              va.canonical_script_key,
              va.script_id,
              sm.product_id,
              sm.task_no,
              sm.product_type,
              sm.script_source,
              sm.publish_purpose,
              va.local_file_path,
              va.download_status,
              va.run_video_status,
              va.publish_status,
              va.video_source_type,
              va.video_source_value
            FROM video_assets va
            LEFT JOIN script_metadata sm
              ON sm.canonical_script_key = va.canonical_script_key
            WHERE va.download_status = '下载成功'
              AND (sm.product_id = ? OR sm.task_no = ?)
            ORDER BY
              CASE WHEN COALESCE(sm.script_source, '') = '短视频复刻' THEN 1 ELSE 0 END,
              va.updated_at DESC
            """,
            (product_id, product_id),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def _source_policy(row: dict[str, Any]) -> tuple[str, str]:
    source = str(row.get("script_source") or "")
    purpose = str(row.get("publish_purpose") or "")
    if "复刻" in source or "复刻" in purpose:
        return "authorized_creator", "medium"
    return "authorized_creator", "high"


if __name__ == "__main__":
    raise SystemExit(main())
