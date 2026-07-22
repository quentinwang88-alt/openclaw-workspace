#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _version_dir(path: Path, root: Path) -> Path | None:
    path = path.expanduser().resolve()
    root = root.resolve()
    if not _inside(path, root):
        return None
    relative = path.relative_to(root)
    # Layout: <root>/<job_id>/<source_hash>/<file>
    return root.joinpath(*relative.parts[:2]) if len(relative.parts) >= 3 else None


def main() -> int:
    parser = argparse.ArgumentParser(description="清理轻视频复核流程的历史媒体版本，只保留数据库当前引用版本")
    parser.add_argument("--db", default=str(SKILL_DIR / "var" / "light_tryon.sqlite3"))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--manifest", default="")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    roots = [SKILL_DIR / "var" / "review_videos", SKILL_DIR / "var" / "review_bgm"]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT job_id, raw_video_path, output_video_path, output_cover_path FROM video_jobs"
    ).fetchall()
    conn.close()

    keep: set[Path] = set()
    for row in rows:
        for key in ("raw_video_path", "output_video_path", "output_cover_path"):
            raw = str(row[key] or "").strip()
            if not raw:
                continue
            path = Path(raw)
            for root in roots:
                version = _version_dir(path, root)
                if version:
                    keep.add(version.resolve())

    candidates: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        for job_dir in (item for item in root.iterdir() if item.is_dir()):
            versions = [item for item in job_dir.iterdir() if item.is_dir()]
            referenced = {item.resolve() for item in versions if item.resolve() in keep}
            if not referenced and versions:
                # Defensive fallback for an old job whose current path was never recorded.
                referenced.add(max(versions, key=lambda item: item.stat().st_mtime).resolve())
            candidates.extend(item for item in versions if item.resolve() not in referenced)

    entries = []
    total = 0
    for path in sorted(candidates):
        size = sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
        total += size
        entries.append({"path": str(path), "bytes": size})

    manifest_path = Path(args.manifest).expanduser().resolve() if args.manifest else (
        SKILL_DIR / "var" / f"review_media_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    payload = {
        "executed": bool(args.execute),
        "database": str(db_path),
        "kept_version_dirs": len(keep),
        "deleted_version_dirs": len(entries) if args.execute else 0,
        "candidate_version_dirs": len(entries),
        "reclaimable_bytes": total,
        "entries": entries,
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.execute:
        for entry in entries:
            path = Path(entry["path"])
            if not any(_inside(path, root) for root in roots):
                raise RuntimeError(f"拒绝删除媒体目录之外的路径: {path}")
            shutil.rmtree(path)

    print(json.dumps({**payload, "manifest": str(manifest_path), "entries": f"{len(entries)} items"}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
