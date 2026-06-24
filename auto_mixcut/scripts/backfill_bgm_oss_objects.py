#!/usr/bin/env python3
"""Backfill missing oss_objects rows for BGM tracks and optionally cache local paths."""

from __future__ import annotations

import argparse
import json
import mimetypes
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.storage_paths import resolve_oss_object_path
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write oss_objects rows; default is dry-run")
    parser.add_argument("--cache", action="store_true", help="download/resolve BGM files and update bgm_tracks.local_file_path")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--report-path", default="")
    args = parser.parse_args()

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps(init.to_dict(), ensure_ascii=False, indent=2, default=str))
        return 1

    tracks = ctx.repo.list_where("bgm_tracks", "COALESCE(oss_object_id,'')!='' ORDER BY id")
    if args.limit:
        tracks = tracks[: args.limit]
    rows = []
    for track in tracks:
        rows.append(_inspect_track(ctx, track, apply=args.apply, cache=args.cache))

    summary = {
        "dry_run": not args.apply,
        "cache": args.cache,
        "tracks_seen": len(tracks),
        "oss_rows_missing": sum(1 for row in rows if row["oss_row_state"] == "missing"),
        "oss_rows_created": sum(1 for row in rows if row.get("oss_row_created")),
        "cache_paths_updated": sum(1 for row in rows if row.get("cache_path_updated")),
        "unresolved": [row for row in rows if row["resolved_object_key"] == ""],
        "rows": rows,
    }
    text = json.dumps(summary, ensure_ascii=False, indent=2, default=str)
    print(text)
    if args.report_path:
        path = Path(args.report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    return 0 if not summary["unresolved"] else 2


def _inspect_track(ctx, track: Dict[str, Any], apply: bool, cache: bool) -> Dict[str, Any]:
    bgm_id = str(track.get("bgm_id") or "")
    oss_object_id = str(track.get("oss_object_id") or "")
    existing = ctx.repo.get("oss_objects", "object_id", oss_object_id)
    row = {
        "bgm_id": bgm_id,
        "track_name": track.get("track_name") or "",
        "oss_object_id": oss_object_id,
        "oss_row_state": "present" if existing else "missing",
        "resolved_object_key": "",
        "oss_row_created": False,
        "cache_path_updated": False,
        "local_file_path": track.get("local_file_path") or "",
    }
    object_row = existing
    if not object_row:
        resolved = _resolve_candidate_object(ctx, track)
        if not resolved:
            resolved = _repairable_local_object(track)
            if not resolved:
                return row
            row["repair_source_path"] = str(resolved["source_path"])
            if apply:
                uploaded = ctx.oss.upload(resolved["source_path"], resolved["object_key"])
                if not uploaded.success:
                    row["repair_error"] = uploaded.to_dict()
                    return row
                resolved["upload_data"] = uploaded.data
            else:
                resolved["upload_data"] = {}
        object_row = _object_row(ctx, track, resolved)
        row["resolved_object_key"] = object_row["object_key"]
        if apply:
            result = ctx.repo.upsert("oss_objects", "object_id", object_row)
            row["oss_row_created"] = bool(result.success)
    else:
        row["resolved_object_key"] = object_row.get("object_key") or ""

    if cache and apply and object_row:
        resolved_path = resolve_oss_object_path(ctx, object_row["object_id"], "render_bgm")
        if resolved_path.success:
            path = str(resolved_path.data["path"])
            result = ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, {"local_file_path": path})
            row["cache_path_updated"] = bool(result.success)
            row["local_file_path"] = path
        else:
            row["cache_error"] = resolved_path.to_dict()
    return row


def _resolve_candidate_object(ctx, track: Dict[str, Any]) -> Dict[str, Any] | None:
    for object_key in _candidate_object_keys(track):
        try:
            head = ctx.oss._bucket.head_object(object_key)
        except Exception:
            continue
        return {"object_key": object_key, "head": head}
    return None


def _repairable_local_object(track: Dict[str, Any]) -> Dict[str, Any] | None:
    file_name = str(track.get("file_name") or "").strip()
    if not file_name:
        return None
    root = Path(__file__).resolve().parents[1]
    candidates = [
        root / "assets" / "bgm" / file_name,
        root / "assets" / "bgm" / "cc0_opengameart" / file_name,
    ]
    for source_path in candidates:
        if source_path.exists():
            return {
                "object_key": f"auto_mixcut/bgm/{file_name}",
                "source_path": source_path,
            }
    return None


def _candidate_object_keys(track: Dict[str, Any]) -> List[str]:
    bgm_id = str(track.get("bgm_id") or "").strip()
    file_name = str(track.get("file_name") or "").strip()
    exts = _candidate_exts(track)
    keys: List[str] = []
    for ext in exts:
        if bgm_id:
            keys.append(f"auto_mixcut/bgm_library/raw/{bgm_id}.{ext}")
    if file_name:
        keys.append(f"auto_mixcut/bgm/{file_name}")
        keys.append(f"auto_mixcut/bgm_library/{file_name}")
    return _unique(keys)


def _candidate_exts(track: Dict[str, Any]) -> List[str]:
    exts = []
    for value in [track.get("audio_format"), Path(str(track.get("file_name") or "")).suffix.lstrip("."), Path(str(track.get("local_file_path") or "")).suffix.lstrip(".")]:
        value = str(value or "").strip().lower()
        if value:
            exts.append(value)
    exts.extend(["mp3", "m4a", "wav", "aac"])
    return _unique(exts)


def _object_row(ctx, track: Dict[str, Any], resolved: Dict[str, Any]) -> Dict[str, Any]:
    object_key = resolved["object_key"]
    file_name = Path(object_key).name
    ext = Path(file_name).suffix.lstrip(".")
    if "head" in resolved:
        head = resolved["head"]
        headers = dict(getattr(head, "headers", {}) or {})
        file_size = int(getattr(head, "content_length", 0) or 0)
        file_hash = _header_get(headers, "x-oss-meta-sha256")
    elif "upload_data" in resolved:
        upload_data = resolved["upload_data"] or {}
        file_size = int(upload_data.get("file_size") or resolved["source_path"].stat().st_size)
        file_hash = str(upload_data.get("file_hash") or "")
    else:
        file_size = int(resolved["source_path"].stat().st_size)
        file_hash = ""
    return {
        "object_id": track["oss_object_id"],
        "bucket": getattr(ctx.oss, "bucket_name", ctx.settings.bucket),
        "object_key": object_key,
        "object_type": "bgm_library" if "/bgm_library/" in object_key else "bgm",
        "file_name": file_name,
        "file_ext": ext,
        "mime_type": mimetypes.guess_type(file_name)[0] or "audio/mpeg",
        "file_size": file_size,
        "file_hash": file_hash,
        "storage_status": "uploaded",
    }


def _header_get(headers: Dict[str, Any], name: str) -> str:
    target = name.lower()
    for key, value in headers.items():
        if str(key).lower() == target:
            return str(value or "")
    return ""


def _unique(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
