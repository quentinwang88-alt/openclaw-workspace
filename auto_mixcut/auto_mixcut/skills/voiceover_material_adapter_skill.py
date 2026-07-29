from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any

from auto_mixcut.core.result import Result

from .context import SkillContext
from .material_pool_query import list_material_segments


_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_LIGHT_TRYON_SCRIPTS = _WORKSPACE_ROOT / "skills" / "lightweight-tryon-video" / "scripts"
if str(_LIGHT_TRYON_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_LIGHT_TRYON_SCRIPTS))

from light_tryon.voiceover_visual_match_core import (  # noqa: E402
    MATCH_CORE_VERSION,
    apply_key_match_policy,
    candidate_supported_roles,
    normalized_list,
    select_voice_candidate,
    voiceover_intervals,
)


class VoiceoverMaterialAdapterSkill:
    """Adapts the shared material library to the proven light-video matcher."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def candidates(self, product_id: str) -> Result:
        segments = [
            row
            for row in list_material_segments(self.ctx, product_id)
            if str(row.get("segment_status") or "") in {"created", "qc_passed"}
        ]
        if not segments:
            return Result.ok({"product_id": product_id, "candidates": []})
        segment_ids = [str(row["segment_id"]) for row in segments]
        asset_ids = sorted({str(row.get("asset_id") or "") for row in segments if row.get("asset_id")})
        tags = _latest_tags(self.ctx, segment_ids)
        assets = _rows_by_key(self.ctx, "assets", "asset_id", asset_ids)
        output = []
        for segment in segments:
            tag = tags.get(str(segment["segment_id"])) or {}
            asset = assets.get(str(segment.get("asset_id") or "")) or {}
            if not _eligible(segment, asset, tag):
                continue
            duration_ms = int(segment.get("duration_ms") or 0)
            if duration_ms < 500:
                continue
            roles = normalized_list(segment.get("effective_roles_json"))
            primary = str(tag.get("primary_shot_role") or (roles[0] if roles else "scene"))
            secondary = normalized_list(tag.get("secondary_roles_json")) or [role for role in roles if role != primary]
            output.append(
                {
                    "segment_key": str(segment["segment_id"]),
                    "segment_id": str(segment["segment_id"]),
                    "asset_id": str(segment.get("asset_id") or ""),
                    "duplicate_group_id": str(segment.get("visual_phash") or segment.get("asset_id") or ""),
                    "start_ms": 0,
                    "end_ms": duration_ms,
                    "primary_shot_role": primary,
                    "secondary_roles": secondary,
                    "hook_visual_type": str(tag.get("hook_visual_type") or "none"),
                    "product_visibility": str(tag.get("product_visibility") or "medium"),
                    "confidence": tag.get("confidence"),
                    "reason": str(tag.get("reason") or segment.get("effective_roles_reason") or ""),
                    "source_system": asset.get("source_flow") or asset.get("source_type"),
                    "source_record_id": asset.get("source_record_id"),
                }
            )
        return Result.ok({"product_id": product_id, "candidates": output, "match_core_version": MATCH_CORE_VERSION})

    def match(
        self,
        product_id: str,
        tts_timeline: list[dict[str, Any]],
        target_duration_ms: int,
        *,
        beat_plan: list[dict[str, Any]] | dict[str, Any] | None = None,
        primary_selling_point: str = "",
    ) -> Result:
        candidate_result = self.candidates(product_id)
        if not candidate_result.success:
            return candidate_result
        candidates = candidate_result.data["candidates"]
        if not candidates:
            return Result.fail("VOICEOVER_MATERIAL_MISSING", "no eligible material candidates", {"product_id": product_id})
        lines = apply_key_match_policy(
            _normalize_timeline(tts_timeline),
            beat_plan=beat_plan,
            primary_selling_point=primary_selling_point,
        )
        if not lines:
            return Result.fail("VOICEOVER_TIMELINE_MISSING", "tts timeline has no spoken lines", {"product_id": product_id})
        intervals = voiceover_intervals(lines, target_duration_ms, candidates)
        usage = _product_usage(self.ctx, product_id)
        selected = []
        gaps = []
        warnings = []
        used_segments: set[str] = set()
        used_assets: set[str] = set()
        previous_segment_key = ""
        for sequence_no, interval in enumerate(intervals, start=1):
            required_roles = normalized_list(interval.get("required_shot_roles"))
            critical_evidence = bool(interval.get("critical_evidence"))
            candidate, evidence_match = select_voice_candidate(
                candidates,
                desired_role=str(interval.get("desired_role") or "result"),
                duration_ms=int(interval["duration_ms"]),
                required_roles=required_roles,
                speech_text=str(interval.get("chinese_translation") or "") + str(interval.get("speech_text") or ""),
                used=used_segments,
                used_assets=used_assets,
                previous_segment_key=previous_segment_key,
                usage=usage,
                sequence_no=sequence_no,
                current_sequence=[str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in selected],
            )
            if not candidate:
                fallback_candidates = _non_adjacent_candidates(candidates, selected) or candidates
                candidate, evidence_match = select_voice_candidate(
                    fallback_candidates,
                    desired_role=str(interval.get("desired_role") or "result"),
                    duration_ms=int(interval["duration_ms"]),
                    required_roles=required_roles if critical_evidence else [],
                    speech_text=str(interval.get("chinese_translation") or "") + str(interval.get("speech_text") or ""),
                    used=used_segments,
                    used_assets=set(),
                    previous_segment_key=previous_segment_key,
                    usage=usage,
                    sequence_no=sequence_no,
                    current_sequence=[str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in selected],
                )
                if candidate:
                    warnings.append(
                        {
                            "beat_id": interval.get("beat_id"),
                            "reason": "material_reuse_fallback",
                            "asset_id": candidate.get("asset_id"),
                        }
                    )
            if not candidate or (critical_evidence and required_roles and not evidence_match):
                gaps.append(
                    {
                        "beat_id": interval.get("beat_id"),
                        "speech_text": interval.get("speech_text"),
                        "required_shot_roles": required_roles,
                        "start_ms": interval.get("start_ms"),
                        "end_ms": interval.get("end_ms"),
                        "match_priority": interval.get("match_priority"),
                        "critical_evidence": True,
                        "reason": "no_duration_candidate" if not candidate else "no_evidence_match",
                    }
                )
                continue
            original_roles = normalized_list(interval.get("original_required_shot_roles"))
            if not critical_evidence and original_roles and not set(original_roles).intersection(candidate_supported_roles(candidate)):
                warnings.append(
                    {
                        "beat_id": interval.get("beat_id"),
                        "reason": "soft_evidence_unmatched",
                        "original_required_shot_roles": original_roles,
                        "asset_id": candidate.get("asset_id"),
                    }
                )
            used_segments.add(candidate["segment_key"])
            used_assets.add(str(candidate.get("duplicate_group_id") or candidate.get("asset_id") or ""))
            used_assets.add(str(candidate.get("asset_id") or ""))
            previous_segment_key = candidate["segment_key"]
            selected.append(
                {
                    **candidate,
                    "sequence_no": sequence_no,
                    "role": interval.get("desired_role"),
                    "beat_id": interval.get("beat_id"),
                    "speech_text": interval.get("speech_text"),
                    "timeline_start_ms": int(interval.get("start_ms") or 0),
                    "timeline_end_ms": int(interval.get("end_ms") or 0),
                    "duration_ms": int(interval.get("duration_ms") or 0),
                    "required_shot_roles": required_roles,
                    "original_required_shot_roles": original_roles,
                    "match_priority": interval.get("match_priority"),
                    "critical_evidence": critical_evidence,
                    "evidence_match": evidence_match,
                }
            )
        return Result.ok(
            {
                "product_id": product_id,
                "target_duration_ms": target_duration_ms,
                "clips": selected,
                "evidence_gaps": gaps,
                "match_warnings": warnings,
                "key_beat_id": next((str(row.get("block_id") or "") for row in lines if row.get("match_priority") == "key"), ""),
                "match_plan_version": MATCH_CORE_VERSION,
                "candidate_count": len(candidates),
            }
        )


def _non_adjacent_candidates(candidates: list[dict[str, Any]], selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not selected:
        return candidates
    previous = selected[-1]
    previous_asset = str(previous.get("asset_id") or "")
    previous_group = str(previous.get("duplicate_group_id") or previous_asset)
    return [
        row
        for row in candidates
        if str(row.get("asset_id") or "") != previous_asset
        and str(row.get("duplicate_group_id") or row.get("asset_id") or "") != previous_group
    ]


def _eligible(segment: dict, asset: dict, tag: dict) -> bool:
    if str(asset.get("asset_status") or "").lower() in {"failed", "rejected", "quarantined", "deleted", "blocked"}:
        return False
    if str(asset.get("has_watermark") or "pending") in {"yes", "true", "1"}:
        return False
    if str(tag.get("mixcut_usability") or "yes") == "no":
        return False
    if str(tag.get("risk_level") or "low") == "high":
        return False
    if str(segment.get("product_match_status") or "") in {"mismatch", "failed", "rejected"}:
        return False
    return bool(tag or segment.get("effective_roles_json"))


def _latest_tags(ctx: SkillContext, segment_ids: list[str]) -> dict[str, dict]:
    rows = _rows_in(ctx, "segment_tags", "segment_id", segment_ids, suffix="ORDER BY id")
    latest = {}
    for row in rows:
        latest[str(row.get("segment_id") or "")] = row
    return latest


def _rows_by_key(ctx: SkillContext, table: str, key: str, values: list[str]) -> dict[str, dict]:
    return {str(row.get(key) or ""): row for row in _rows_in(ctx, table, key, values)}


def _rows_in(ctx: SkillContext, table: str, key: str, values: list[str], suffix: str = "") -> list[dict]:
    output = []
    for start in range(0, len(values), 300):
        chunk = values[start : start + 300]
        placeholders = ",".join("?" for _ in chunk)
        output.extend(ctx.repo.list_where(table, f"{key} IN ({placeholders}) {suffix}".strip(), tuple(chunk)))
    return output


def _normalize_timeline(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for index, row in enumerate(rows):
        speech = str(row.get("speech_text") or row.get("text") or "").strip()
        if not speech:
            continue
        start_ms = int(row.get("start_ms") or round(float(row.get("start_seconds") or 0) * 1000))
        end_ms = int(row.get("end_ms") or round(float(row.get("end_seconds") or 0) * 1000))
        output.append({**row, "speech_text": speech, "start_ms": start_ms, "end_ms": end_ms, "block_id": row.get("block_id") or row.get("beat_id") or f"B{index + 1}"})
    return sorted(output, key=lambda row: int(row["start_ms"]))


def _product_usage(ctx: SkillContext, product_id: str) -> dict[str, Any]:
    segment_use: Counter[str] = Counter()
    asset_use: Counter[str] = Counter()
    first_asset_use: Counter[str] = Counter()
    sequences = []
    outputs = ctx.repo.list_where("outputs", "product_id=? AND render_status='rendered'", (product_id,))
    for output in outputs:
        slots = ctx.repo.list_where("output_segments", "output_id=? ORDER BY slot_index", (output["output_id"],))
        if not slots:
            continue
        group_by_segment = {}
        segment_ids = [str(slot.get("segment_id") or "") for slot in slots if slot.get("segment_id")]
        for row in _rows_in(ctx, "segments", "segment_id", segment_ids):
            group_by_segment[str(row.get("segment_id") or "")] = str(row.get("visual_phash") or row.get("asset_id") or "")
        sequence = tuple(
            group_by_segment.get(str(slot.get("segment_id") or "")) or str(slot.get("asset_id") or "")
            for slot in slots
        )
        sequences.append(sequence)
        for slot, group_id in zip(slots, sequence):
            segment_use[str(slot.get("segment_id") or "")] += 1
            asset_use[group_id] += 1
        first_asset_use[sequence[0]] += 1
    return {"segment_use": segment_use, "asset_use": asset_use, "first_asset_use": first_asset_use, "sequences": sequences}
