#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
LIGHT_TRYON_SCRIPTS = WORKSPACE / "skills" / "lightweight-tryon-video" / "scripts"
for path in (ROOT, LIGHT_TRYON_SCRIPTS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.ids import new_id
from auto_mixcut.skills.output_material_usage_skill import OutputMaterialUsageSkill
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_skill import RenderSkill
from auto_mixcut.skills.voiceover_material_adapter_skill import VoiceoverMaterialAdapterSkill
from auto_mixcut.skills.voiceover_mixcut_orchestrator_skill import VoiceoverMixcutOrchestratorSkill
from light_tryon.database import LightTryonDB
from light_tryon.voiceover_visual_match_core import apply_key_match_policy


PILOT_SPECS = (
    {"variant_no": 3, "mode": "short", "max_beats": 2, "target_ms": 9063},
    {"variant_no": 4, "mode": "short", "max_beats": 2, "target_ms": 8000},
    {"variant_no": 1, "mode": "long", "max_beats": 0, "target_ms": 0},
    {"variant_no": 2, "mode": "long", "max_beats": 0, "target_ms": 0},
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Render four non-publishing voiceover gray-pilot outputs.")
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--light-product-id", required=True)
    parser.add_argument("--light-db", default=str(WORKSPACE / "skills" / "lightweight-tryon-video" / "var" / "light_tryon.sqlite3"))
    parser.add_argument("--pilot-key", default="VOICE_GRAY_20260722_V1")
    parser.add_argument("--variant-no", type=int, default=0, help="run one pilot variant; 0 runs all four")
    parser.add_argument(
        "--full-timeline",
        action="store_true",
        help="use the selected variant's complete TTS timeline instead of the short-pilot truncation",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report-path", default="")
    parser.add_argument("--skip-init-db", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    if not args.skip_init_db:
        initialized = RDSRepositorySkill(ctx).init_db()
        if not initialized.success:
            return _print_result(initialized.to_dict(), 1)
    light_db = LightTryonDB(args.light_db)
    variants = {int(row.get("variant_no") or 0): row for row in light_db.list_narrative_variants(args.light_product_id)}
    reports = []
    specs = [spec for spec in PILOT_SPECS if not args.variant_no or spec["variant_no"] == args.variant_no]
    if args.full_timeline:
        if not args.variant_no:
            parser.error("--full-timeline requires --variant-no")
        specs = [{"variant_no": args.variant_no, "mode": "full", "max_beats": 0, "target_ms": 0}]
    for spec in specs:
        variant = variants.get(spec["variant_no"])
        if not variant:
            reports.append({"success": False, "variant_no": spec["variant_no"], "error": "variant_missing"})
            continue
        report = run_one(ctx, light_db, args, variant, spec)
        reports.append(report)
        print(json.dumps({"progress": len(reports), "report": report}, ensure_ascii=False, default=str), flush=True)
    payload = {"success": all(row.get("success") for row in reports), "dry_run": args.dry_run, "product_id": args.product_id, "reports": reports}
    if args.report_path:
        report_path = Path(args.report_path).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return _print_result(payload, 0 if payload["success"] else 1)


def run_one(ctx, light_db: LightTryonDB, args, variant: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    variant_no = int(variant["variant_no"])
    suffix = f"V{variant_no}_{spec['mode'].upper()}"
    task_id = f"TASK_{args.pilot_key}_{suffix}"
    batch_id = f"BATCH_{args.pilot_key}_{suffix}"
    existing = ctx.repo.list_where("outputs", "batch_id=? ORDER BY id DESC", (batch_id,))
    if existing:
        output = existing[0]
        usage = ctx.repo.list_where("output_material_usage", "output_id=?", (output["output_id"],))
        return {"success": True, "status": "already_rendered", "variant_no": variant_no, "mode": spec["mode"], "batch_id": batch_id, "output_id": output["output_id"], "quality_status": output.get("machine_quality_status"), "material_count": len(usage)}

    full_timeline = list(variant.get("tts_timeline") or [])
    response = variant.get("voiceover_response") or {}
    full_beats = response.get("beats") or variant.get("beat_plan") or []
    strategy = light_db.get_content_strategy(str(variant.get("strategy_group_id") or "")) or {}
    policy_timeline = apply_key_match_policy(
        full_timeline,
        beat_plan=full_beats,
        primary_selling_point=str(strategy.get("primary_selling_point") or ""),
    )
    key_beat_id = next((str(row.get("block_id") or "") for row in policy_timeline if row.get("match_priority") == "key"), "")
    timeline = full_timeline
    if spec["max_beats"]:
        timeline = timeline[: int(spec["max_beats"])]
    if not timeline:
        return {"success": False, "variant_no": variant_no, "error": "tts_timeline_missing"}
    speech_end_ms = int(timeline[-1].get("end_ms") or 0)
    target_ms = int(spec["target_ms"] or speech_end_ms)
    beat_ids = {str(row.get("block_id") or row.get("beat_id") or "") for row in timeline}
    beats = [row for row in full_beats if str(row.get("beat_id") or row.get("block_id") or "") in beat_ids]
    if spec["max_beats"] and key_beat_id and key_beat_id not in beat_ids:
        return {
            "success": False,
            "variant_no": variant_no,
            "mode": spec["mode"],
            "error": "short_key_beat_not_in_audio_window",
            "key_beat_id": key_beat_id,
            "selected_beat_ids": sorted(beat_ids),
        }
    matched = VoiceoverMaterialAdapterSkill(ctx).match(
        args.product_id,
        timeline,
        target_ms,
        beat_plan=beats,
        primary_selling_point=str(strategy.get("primary_selling_point") or ""),
    )
    matched_clips = (matched.data or {}).get("clips") or [] if matched.success else []
    base = {
        "variant_no": variant_no,
        "variant_id": variant["variant_id"],
        "mode": spec["mode"],
        "target_duration_ms": target_ms,
        "speech_end_ms": speech_end_ms,
        "hook_id": strategy.get("hook_id"),
        "primary_selling_point": strategy.get("primary_selling_point"),
        "key_beat_id": (matched.data or {}).get("key_beat_id") or key_beat_id,
        "clip_count": len(matched_clips),
        "first_asset_id": matched_clips[0].get("asset_id") if matched_clips else "",
        "selected_asset_ids": [str(row.get("asset_id") or "") for row in matched_clips],
        "evidence_gaps": (matched.data or {}).get("evidence_gaps") or [],
        "match_warnings": (matched.data or {}).get("match_warnings") or [],
    }
    if not matched.success:
        return {**base, "success": False, "error": matched.error.to_dict() if matched.error else {}}
    if base["evidence_gaps"]:
        return {**base, "success": False, "error": "evidence_gaps"}
    if args.dry_run:
        return {**base, "success": True, "status": "dry_run_matched"}
    print(json.dumps({"stage": "matched", **base}, ensure_ascii=False, default=str), flush=True)
    pending_plans = ctx.repo.list_where("render_plans", "batch_id=? AND render_status='planned' ORDER BY id", (batch_id,))
    if pending_plans:
        plan_id = pending_plans[0]["render_plan_id"]
        print(json.dumps({"stage": "resume_render_plan", "variant_no": variant_no, "render_plan_id": plan_id}, ensure_ascii=False), flush=True)
        return _render_and_check(ctx, plan_id, base, task_id, batch_id)

    source_voice = _voice_track_path(variant["variant_id"])
    if not source_voice.is_file():
        return {**base, "success": False, "error": f"voice_track_missing:{source_voice}"}
    trimmed_voice = ctx.settings.temp_root / "voiceover_gray_pilot" / args.pilot_key / f"{suffix}.m4a"
    trimmed_voice.parent.mkdir(parents=True, exist_ok=True)
    audio = ctx.ffmpeg.run(["-y", "-i", str(source_voice), "-t", f"{target_ms / 1000:.3f}", "-af", "apad", "-c:a", "aac", "-ar", "44100", "-ac", "2", str(trimmed_voice)], "VOICEOVER_TRIM_FAILED")
    if not audio.success:
        return {**base, "success": False, "error": audio.error.to_dict() if audio.error else {}}
    upload = ctx.oss.upload(trimmed_voice, f"auto_mixcut/voiceover_gray_pilot/{args.product_id}/{args.pilot_key}/{suffix}.m4a")
    if not upload.success:
        return {**base, "success": False, "error": upload.error.to_dict() if upload.error else {}}
    ctx.repo.upsert("oss_objects", "object_id", dict(upload.data, object_type="voiceover", mime_type="audio/mp4", lifecycle_policy="pilot_non_publish"))
    print(json.dumps({"stage": "voiceover_uploaded", "variant_no": variant_no, "object_id": upload.data["object_id"]}, ensure_ascii=False), flush=True)
    ctx.repo.upsert("content_tasks", "task_id", {"task_id": task_id, "product_id": args.product_id, "task_type": "voiceover_mixcut_pilot", "content_mode": "voiceover", "target_language": "th", "target_duration_ms": target_ms, "requested_variant_count": 1, "task_status": "PILOT_PREPARING", "created_by": "voiceover_gray_pilot"})
    ctx.repo.upsert("mixcut_batches", "batch_id", {"batch_id": batch_id, "product_id": args.product_id, "task_id": task_id, "requested_count": 1, "allowed_count": 1, "batch_status": "planning", "material_tier": "voiceover_gray_pilot", "experiment_batch": args.pilot_key})
    prepared = VoiceoverMixcutOrchestratorSkill(ctx).prepare_render_plan(
        task_id=task_id,
        batch_id=batch_id,
        variant_no=variant_no,
        voiceover_variant_id=variant["variant_id"],
        voiceover_oss_object_id=upload.data["object_id"],
        tts_timeline=timeline,
        beat_plan=beats,
        hook_id=str(strategy.get("hook_id") or ""),
        primary_selling_point=str(strategy.get("primary_selling_point") or ""),
    )
    if not prepared.success:
        return {**base, "success": False, "task_id": task_id, "batch_id": batch_id, "error": prepared.error.to_dict() if prepared.error else {}}
    plan_id = prepared.data["render_plan"]["render_plan_id"]
    print(json.dumps({"stage": "render_plan_ready", "variant_no": variant_no, "render_plan_id": plan_id}, ensure_ascii=False), flush=True)
    return _render_and_check(ctx, plan_id, base, task_id, batch_id)


def _render_and_check(ctx, plan_id: str, base: dict[str, Any], task_id: str, batch_id: str) -> dict[str, Any]:
    rendered = RenderSkill(ctx).render_plan(plan_id)
    if not rendered.success:
        return {**base, "success": False, "task_id": task_id, "batch_id": batch_id, "error": rendered.error.to_dict() if rendered.error else {}}
    output_id = rendered.data["output_id"]
    checked = QualityGateSkill(ctx).check_output(output_id)
    output = ctx.repo.get("outputs", "output_id", output_id) or {}
    usage = OutputMaterialUsageSkill(ctx).refresh_output(output_id)
    return {
        **base,
        "success": bool(checked.success and checked.data.get("machine_quality_status") == "passed"),
        "status": "rendered_non_publish",
        "task_id": task_id,
        "batch_id": batch_id,
        "output_id": output_id,
        "output_object_id": output.get("output_oss_object_id"),
        "quality": checked.data if checked.success else checked.to_dict(),
        "material_count": (usage.data or {}).get("material_count") if usage.success else None,
        "publish_task_id": output.get("publish_task_id"),
        "publish_result": output.get("publish_result"),
    }


def _voice_track_path(variant_id: str) -> Path:
    return WORKSPACE / "skills" / "lightweight-tryon-video" / "var" / "narrative_tts" / variant_id / f"{variant_id}_voice_track.m4a"


def _print_result(payload: dict[str, Any], code: int) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
