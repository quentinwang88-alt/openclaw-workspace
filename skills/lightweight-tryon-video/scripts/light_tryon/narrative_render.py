from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from .database import LightTryonDB


def render_narrative_voiceover_mix(
    db: LightTryonDB,
    variant_id: str,
    output_path: str | Path,
    *,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
) -> dict[str, Any]:
    variant = db.get_narrative_variant(variant_id)
    if not variant:
        raise KeyError(f"找不到增强型内容变体: {variant_id}")
    assembly = variant.get("assembly_plan") or {}
    roughcut = Path(str(assembly.get("roughcut_path") or "")).expanduser()
    voice_track = Path(str((assembly.get("tts") or {}).get("voice_track_path") or "")).expanduser()
    if not roughcut.is_file():
        raise ValueError("增强型视频缺少已渲染的视觉粗剪")
    if not voice_track.is_file():
        raise ValueError("增强型视频缺少真实时长回校后的口播音轨")
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error",
        "-i", str(roughcut), "-i", str(voice_track),
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "copy", "-c:a", "aac", "-b:a", "160k",
        "-af", "loudnorm=I=-16:TP=-1.5:LRA=8",
        "-movflags", "+faststart", "-shortest", str(output),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=600, check=False)
    if completed.returncode != 0 or not output.is_file() or output.stat().st_size <= 0:
        raise RuntimeError(f"增强型口播混音失败: {completed.stderr[-2000:]}")
    probe = _probe(output, ffprobe_bin)
    target = float(variant.get("target_duration_seconds") or 22)
    actual = float((probe.get("format") or {}).get("duration") or 0)
    streams = probe.get("streams") or []
    if abs(actual - target) > 0.25:
        raise RuntimeError(f"增强型成片时长异常: {actual:.3f}s，目标 {target:.3f}s")
    if not any(row.get("codec_type") == "audio" for row in streams):
        raise RuntimeError("增强型成片缺少口播音轨")
    mix = {
        "status": "success",
        "output_path": str(output),
        "file_size": output.stat().st_size,
        "duration_seconds": round(actual, 3),
        "audio_policy": "voiceover_only_no_random_model_bgm",
        "loudness_target": "-16 LUFS",
    }
    db.update_narrative_variant(
        variant_id,
        workflow_state="final_qc",
        assembly_plan={
            **assembly,
            "final_mix": mix,
            "final_qc": {
                "status": "pending",
                "reason": "new_render_requires_fresh_qc",
            },
        },
        last_error="",
    )
    # Re-evaluate the whole product after every fresh render.  This turns the
    # batch diversity policy into a real delivery gate instead of an advisory
    # report that can be overwritten by the mix step's pending status.
    from .diversity import evaluate_product_diversity

    diversity = evaluate_product_diversity(db, str(variant.get("product_id") or ""), persist=True)
    return {"variant_id": variant_id, **mix, "delivery_gate": diversity.get("delivery_gate") or {}}


def _probe(path: Path, ffprobe_bin: str) -> dict[str, Any]:
    completed = subprocess.run(
        [
            ffprobe_bin, "-v", "error", "-show_entries",
            "format=duration,size:stream=codec_type,codec_name,width,height",
            "-of", "json", str(path),
        ],
        capture_output=True, text=True, timeout=60, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"增强型成片探测失败: {completed.stderr[-1000:]}")
    return json.loads(completed.stdout or "{}")
