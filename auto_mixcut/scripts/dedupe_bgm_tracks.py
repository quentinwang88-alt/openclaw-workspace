#!/usr/bin/env python3
"""Soft-dedupe BGM tracks by canonicalizing references and archiving duplicates."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.bgm_usage_skill import refresh_bgm_track_usage
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


ARCHIVE_REASON_PREFIX = "duplicate_archived canonical_bgm_id="
BLOCKED_TRACK_STATUSES = {"paused", "rejected", "expired", "inactive", "disabled", "duplicate_archived"}
LICENSE_RANK = {"verified": 4, "available": 4, "pending": 2, "": 1}
TAG_RANK = {"tagged": 3, "fallback": 2, "tagging": 1, "untagged": 0, "": 0}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write changes; default is dry-run")
    parser.add_argument("--include-archived", action="store_true", help="also consider already archived duplicate tracks")
    parser.add_argument("--report-path", default="", help="optional JSON report output path")
    args = parser.parse_args()

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps(init.to_dict(), ensure_ascii=False, indent=2))
        return 1

    tracks = ctx.repo.list_where("bgm_tracks", "1=1")
    plan = build_dedupe_plan(tracks, include_archived=args.include_archived)
    summary = summarize_plan(ctx, plan)
    summary["dry_run"] = not args.apply

    if args.apply:
        apply_summary = apply_dedupe_plan(ctx, plan)
        summary.update(apply_summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


def build_dedupe_plan(tracks: Iterable[Dict[str, Any]], include_archived: bool = False) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for track in tracks:
        if not include_archived and is_archived_duplicate(track):
            continue
        key = track_identity(track)
        if not key:
            continue
        groups.setdefault(key, []).append(track)

    plan = []
    for key, group in sorted(groups.items()):
        if len(group) < 2:
            continue
        ranked = sorted(group, key=lambda item: (_canonical_score(item), str(item.get("bgm_id") or "")), reverse=True)
        canonical = ranked[0]
        duplicates = [item for item in ranked[1:] if item.get("bgm_id") != canonical.get("bgm_id")]
        if duplicates:
            plan.append(
                {
                    "identity": key,
                    "track_name": canonical.get("track_name") or "",
                    "artist_name": canonical.get("artist_name") or "",
                    "canonical_bgm_id": canonical.get("bgm_id"),
                    "duplicate_bgm_ids": [item.get("bgm_id") for item in duplicates],
                    "canonical_score": round(_canonical_score(canonical), 3),
                    "duplicate_scores": {str(item.get("bgm_id")): round(_canonical_score(item), 3) for item in duplicates},
                }
            )
    return plan


def summarize_plan(ctx, plan: List[Dict[str, Any]]) -> Dict[str, Any]:
    mapping = _mapping_from_plan(plan)
    outputs_to_update = 0
    for output in ctx.repo.list_where("outputs", "1=1"):
        bgm_id = _bgm_id_from_plan(output.get("bgm_plan_json"))
        if bgm_id in mapping:
            outputs_to_update += 1

    usage_events_to_update = 0
    for event in ctx.repo.list_where("bgm_usage_events", "1=1"):
        if str(event.get("bgm_id") or "") in mapping:
            usage_events_to_update += 1

    return {
        "duplicate_groups": len(plan),
        "duplicate_tracks": sum(len(item["duplicate_bgm_ids"]) for item in plan),
        "outputs_to_update": outputs_to_update,
        "usage_events_to_update": usage_events_to_update,
        "groups": plan,
    }


def apply_dedupe_plan(ctx, plan: List[Dict[str, Any]]) -> Dict[str, Any]:
    mapping = _mapping_from_plan(plan)
    now = datetime.utcnow().isoformat(timespec="seconds")
    outputs_updated = 0
    usage_events_updated = 0
    tracks_archived = 0
    canonical_touched = set()

    for output in ctx.repo.list_where("outputs", "1=1"):
        plan_json = output.get("bgm_plan_json")
        bgm_plan = _coerce_bgm_plan(plan_json)
        old_bgm_id = str(bgm_plan.get("bgm_id") or "")
        canonical_bgm_id = mapping.get(old_bgm_id)
        if not canonical_bgm_id:
            continue
        bgm_plan.setdefault("legacy_bgm_id", old_bgm_id)
        bgm_plan["bgm_id"] = canonical_bgm_id
        bgm_plan["bgm_canonicalized_at"] = now
        result = ctx.repo.update("outputs", "output_id", output["output_id"], {"bgm_plan_json": bgm_plan})
        if result.success:
            outputs_updated += 1
            canonical_touched.add(canonical_bgm_id)

    for event in ctx.repo.list_where("bgm_usage_events", "1=1"):
        old_bgm_id = str(event.get("bgm_id") or "")
        canonical_bgm_id = mapping.get(old_bgm_id)
        if not canonical_bgm_id:
            continue
        result = ctx.repo.update("bgm_usage_events", "event_id", event["event_id"], {"bgm_id": canonical_bgm_id})
        if result.success:
            usage_events_updated += 1
            canonical_touched.add(canonical_bgm_id)

    for item in plan:
        canonical_bgm_id = str(item.get("canonical_bgm_id") or "")
        if canonical_bgm_id:
            canonical_touched.add(canonical_bgm_id)
        for duplicate_bgm_id in item["duplicate_bgm_ids"]:
            track = ctx.repo.get("bgm_tracks", "bgm_id", duplicate_bgm_id)
            if not track:
                continue
            reason = _archive_reason(track, canonical_bgm_id)
            result = ctx.repo.update(
                "bgm_tracks",
                "bgm_id",
                duplicate_bgm_id,
                {
                    "status": "paused",
                    "usage_count": 0,
                    "rejected_usage_count": 0,
                    "bgm_tag_reason": reason,
                },
            )
            if result.success:
                tracks_archived += 1

    for bgm_id in sorted(set(mapping) | canonical_touched):
        refresh_bgm_track_usage(ctx, bgm_id)

    return {
        "outputs_updated": outputs_updated,
        "usage_events_updated": usage_events_updated,
        "tracks_archived": tracks_archived,
    }


def track_identity(track: Dict[str, Any]) -> str:
    name = _normalize_identity_part(track.get("track_name"))
    if not name:
        return ""
    artist = _normalize_identity_part(track.get("artist_name"))
    return f"{name}|{artist}"


def is_archived_duplicate(track: Dict[str, Any]) -> bool:
    status = str(track.get("status") or "").strip().lower()
    reason = str(track.get("bgm_tag_reason") or "")
    return status == "paused" and ARCHIVE_REASON_PREFIX in reason


def _canonical_score(track: Dict[str, Any]) -> float:
    score = 0.0
    status = str(track.get("status") or "active").strip().lower()
    if status not in BLOCKED_TRACK_STATUSES:
        score += 25
    else:
        score -= 20

    license_status = str(track.get("license_status") or "").strip().lower()
    score += LICENSE_RANK.get(license_status, 0) * 8

    tag_status = str(track.get("bgm_tag_status") or "").strip().lower()
    score += TAG_RANK.get(tag_status, 0) * 4

    bgm_id = str(track.get("bgm_id") or "")
    if bgm_id and not bgm_id.startswith("BGM_"):
        score += 18
    elif re.match(r"^BGM_\d{14}_[A-Z0-9]+$", bgm_id):
        score += 7

    if track.get("oss_object_id"):
        score += 12
    local_path = str(track.get("local_file_path") or "")
    if local_path:
        score += 2
        if Path(local_path).exists():
            score += 6
    if track.get("source_url"):
        score += 3
    if track.get("license_note"):
        score += 3

    score += min(int(track.get("usage_count") or 0), 20) * 0.4
    rejected = int(track.get("rejected_usage_count") or 0)
    score -= min(rejected, 20) * 0.5
    return score


def _archive_reason(track: Dict[str, Any], canonical_bgm_id: str) -> str:
    existing = str(track.get("bgm_tag_reason") or "").strip()
    marker = f"{ARCHIVE_REASON_PREFIX}{canonical_bgm_id}"
    if marker in existing:
        return existing
    if existing:
        return f"{existing} | {marker}"
    return marker


def _mapping_from_plan(plan: List[Dict[str, Any]]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for item in plan:
        canonical = str(item.get("canonical_bgm_id") or "")
        for duplicate in item.get("duplicate_bgm_ids") or []:
            duplicate_id = str(duplicate or "")
            if duplicate_id and canonical:
                mapping[duplicate_id] = canonical
    return mapping


def _bgm_id_from_plan(value: Any) -> str:
    return str(_coerce_bgm_plan(value).get("bgm_id") or "")


def _coerce_bgm_plan(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _normalize_identity_part(value: Any) -> str:
    text = str(value or "").strip().lower()
    return re.sub(r"\s+", " ", text)


if __name__ == "__main__":
    raise SystemExit(main())
