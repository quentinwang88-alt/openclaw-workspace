#!/usr/bin/env python3
"""Run a real local BGM render smoke test against the configured repository."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.ids import new_id
from auto_mixcut.skills.bgm_usage_skill import refresh_bgm_track_usage
from auto_mixcut.skills.render_skill import RenderSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


def main() -> int:
    os.environ.setdefault("AUTO_MIXCUT_BGM_VOLUME_MODE", "bed")
    if os.environ.get("AUTO_MIXCUT_BGM_SMOKE_FEISHU_SYNC") not in {"1", "true", "yes"}:
        os.environ["AUTO_MIXCUT_FEISHU_ENABLED"] = "0"
    ctx = build_context()
    result = {"started_at": datetime.utcnow().isoformat(timespec="seconds"), "steps": []}

    def step(name: str, res):
        result["steps"].append({"name": name, "result": res.to_dict() if hasattr(res, "to_dict") else res})
        if hasattr(res, "success") and not res.success:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            raise SystemExit(1)
        return res

    step("init_db", RDSRepositorySkill(ctx).init_db())
    if ctx.ffmpeg.mock:
        result["error"] = "AUTO_MIXCUT_MOCK_FFMPEG is enabled; real BGM render smoke requires ffmpeg"
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 1

    smoke_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    product_id = f"BGM_SMOKE_{smoke_suffix}"
    task_id = f"TASK_{product_id}"
    batch_id = f"BATCH_{product_id}"
    plan_id = f"PLAN_{product_id}_001"
    source_dir = ctx.settings.temp_root / "bgm_smoke" / product_id / "sources"
    source_dir.mkdir(parents=True, exist_ok=True)

    _upsert_seed_rows(ctx, product_id, task_id, batch_id)
    slots = []
    for index in range(2):
        source_path = source_dir / f"segment_{index + 1:02d}.mp4"
        _make_source_clip(source_path, index)
        object_key = f"auto_mixcut/smoke/{product_id}/segments/source_{index + 1:02d}.mp4"
        upload = step(f"upload_segment_{index + 1:02d}", ctx.oss.upload(source_path, object_key))
        object_row = dict(upload.data, object_type="segment", mime_type="video/mp4")
        step(f"save_segment_object_{index + 1:02d}", ctx.repo.upsert("oss_objects", "object_id", object_row))
        asset_id = f"ASSET_{product_id}_{index + 1:02d}"
        segment_id = f"SEG_{product_id}_{index + 1:02d}"
        _upsert_asset_and_segment(ctx, product_id, asset_id, segment_id, object_row["object_id"], index)
        slots.append(
            {
                "slot": index + 1,
                "segment_id": segment_id,
                "asset_id": asset_id,
                "role": "demo",
                "start_ms_in_output": index * 3000,
                "end_ms_in_output": (index + 1) * 3000,
            }
        )

    plan_json = {
        "template": {
            "template_id": "BGM_SMOKE_TEMPLATE",
            "name": "BGM smoke template",
            "bgm_profile": {
                "moods": ["daily_clean"],
                "energy": "high",
                "preferred_vocal_types": ["instrumental", "light_vocal"],
            },
        },
        "segments": slots,
        "subtitles": {"enabled": False, "items": []},
    }
    step(
        "save_render_plan",
        ctx.repo.upsert(
            "render_plans",
            "render_plan_id",
            {
                "render_plan_id": plan_id,
                "batch_id": batch_id,
                "product_id": product_id,
                "variant_no": 1,
                "template_id": "BGM_SMOKE_TEMPLATE",
                "planned_duration_ms": 6000,
                "plan_json": plan_json,
                "quality_gate_status": "passed",
                "render_status": "planned",
            },
        ),
    )

    rendered = step("render_batch", RenderSkill(ctx).render_batch(batch_id))
    output_id = (rendered.data.get("output_ids") or [""])[0]
    output = ctx.repo.get("outputs", "output_id", output_id) or {}
    bgm_plan = output.get("bgm_plan_json") or {}
    bgm_id = str(bgm_plan.get("bgm_id") or "")
    counters = refresh_bgm_track_usage(ctx, bgm_id) if bgm_id else {}
    events = ctx.repo.list_where("bgm_usage_events", "output_id=?", (output_id,))
    output_object = ctx.repo.get("oss_objects", "object_id", output.get("output_oss_object_id")) or {}
    output_path = ctx.settings.oss_root / str(output_object.get("object_key") or "")
    probe = _probe_output(output_path)

    result["summary"] = {
        "product_id": product_id,
        "batch_id": batch_id,
        "output_id": output_id,
        "output_path": str(output_path),
        "output_exists": output_path.exists(),
        "output_size": output_path.stat().st_size if output_path.exists() else 0,
        "bgm_plan": bgm_plan,
        "bgm_usage_events": events,
        "usage_counters": counters,
        "probe": probe,
        "db_path": str(ctx.settings.db_path),
        "oss_root": str(ctx.settings.oss_root),
        "temp_root": str(ctx.settings.temp_root),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if _is_successful_summary(result["summary"]) else 1


def _upsert_seed_rows(ctx, product_id: str, task_id: str, batch_id: str) -> None:
    ctx.repo.upsert(
        "products",
        "product_id",
        {
            "product_id": product_id,
            "product_name": "BGM Render Smoke Product",
            "market": "VN",
            "category": "generic_fashion",
            "shop_id": "smoke_shop",
            "priority": "normal",
            "product_status": "smoke",
        },
    )
    ctx.repo.upsert(
        "content_tasks",
        "task_id",
        {
            "task_id": task_id,
            "product_id": product_id,
            "task_type": "smoke",
            "requested_variant_count": 1,
            "allowed_variant_count": 1,
            "actual_variant_count": 0,
            "material_tier": "smoke",
            "material_status": "ready",
            "task_status": "PLANNED",
            "created_by": "bgm_render_smoke",
        },
    )
    ctx.repo.upsert(
        "mixcut_batches",
        "batch_id",
        {
            "batch_id": batch_id,
            "product_id": product_id,
            "task_id": task_id,
            "requested_count": 1,
            "allowed_count": 1,
            "rendered_count": 0,
            "batch_status": "planned",
            "material_tier": "smoke",
            "template_pool_json": {"templates": ["BGM_SMOKE_TEMPLATE"]},
            "experiment_batch": batch_id,
        },
    )


def _upsert_asset_and_segment(ctx, product_id: str, asset_id: str, segment_id: str, object_id: str, index: int) -> None:
    now_ms = index * 3000
    ctx.repo.upsert(
        "assets",
        "asset_id",
        {
            "asset_id": asset_id,
            "product_id": product_id,
            "source_type": "smoke_generated",
            "source_trust_level": "high",
            "product_binding_type": "exact_sku",
            "media_type": "video",
            "original_oss_object_id": object_id,
            "normalized_oss_object_id": object_id,
            "file_status": "uploaded",
            "probe_status": "ok",
            "duration_ms": 3000,
            "width": 1080,
            "height": 1920,
            "fps": 30,
            "codec": "h264",
            "orientation": "portrait",
            "has_audio": 0,
            "asset_status": "active",
        },
    )
    ctx.repo.upsert(
        "segments",
        "segment_id",
        {
            "segment_id": segment_id,
            "asset_id": asset_id,
            "product_id": product_id,
            "segment_oss_object_id": object_id,
            "start_ms": 0,
            "end_ms": 3000,
            "duration_ms": 3000,
            "width": 1080,
            "height": 1920,
            "fps": 30,
            "segment_status": "active",
            "source_type": "smoke_generated",
            "source_trust_level": "high",
            "product_binding_type": "exact_sku",
            "product_match_status": "matched",
            "product_match_confidence": "high",
            "effective_roles_json": ["demo"],
            "used_in_outputs_count": 0,
            "used_in_rejected_outputs_count": 0,
        },
    )


def _make_source_clip(path: Path, index: int) -> None:
    color = "0x3b82f6" if index == 0 else "0x10b981"
    text = f"BGM SMOKE {index + 1}"
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=c={color}:s=1080x1920:d=3:r=30",
        "-vf",
        f"drawtext=text='{text}':x=(w-text_w)/2:y=(h-text_h)/2:fontsize=72:fontcolor=white",
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-pix_fmt",
        "yuv420p",
        str(path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-2000:])


def _probe_output(path: Path) -> dict:
    if not path.exists():
        return {}
    proc = subprocess.run(
        ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_format", str(path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        return {"error": proc.stderr[-1000:]}
    raw = json.loads(proc.stdout)
    streams = raw.get("streams") or []
    video = next((item for item in streams if item.get("codec_type") == "video"), {})
    audio = next((item for item in streams if item.get("codec_type") == "audio"), {})
    return {
        "duration": (raw.get("format") or {}).get("duration"),
        "width": video.get("width"),
        "height": video.get("height"),
        "video_codec": video.get("codec_name"),
        "has_audio": bool(audio),
        "audio_codec": audio.get("codec_name"),
        "audio_sample_rate": audio.get("sample_rate"),
        "audio_channels": audio.get("channels"),
    }


def _is_successful_summary(summary: dict) -> bool:
    bgm_plan = summary.get("bgm_plan") or {}
    probe = summary.get("probe") or {}
    return all(
        [
            summary.get("output_exists"),
            int(summary.get("output_size") or 0) > 0,
            bool(bgm_plan.get("bgm_id")),
            bool(summary.get("bgm_usage_events")),
            int((summary.get("usage_counters") or {}).get("rendered_usage_count") or 0) >= 1,
            probe.get("has_audio") is True,
            probe.get("width") == 1080,
            probe.get("height") == 1920,
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
