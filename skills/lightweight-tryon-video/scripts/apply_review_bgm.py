#!/usr/bin/env python3
"""Apply the existing auto_mixcut BGM recommendation/mix policy to review videos."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


SKILL_DIR = Path(__file__).resolve().parents[1]
WORKSPACE = SKILL_DIR.parents[1]
AUTO_MIXCUT_ROOT = WORKSPACE / "auto_mixcut"
os.environ.setdefault("AUTO_MIXCUT_ROOT", str(AUTO_MIXCUT_ROOT))
sys.path.insert(0, str(SKILL_DIR / "scripts"))
sys.path.insert(0, str(AUTO_MIXCUT_ROOT))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.bgm_library_skill import BgmLibrarySkill
from auto_mixcut.skills.render_skill import (
    _apply_audio_mix_suggestions,
    _bgm_audio_filter,
    _bgm_manifest,
    _bgm_track_path,
    _normalize_bgm_mix,
)
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from light_tryon.database import LightTryonDB
from light_tryon.feishu_client import load_feishu_config, resolve_endpoints
from light_tryon.feishu_sync import build_clients
from light_tryon.utils import stable_hash
from light_tryon.workers import validate_video_decode


POLICY_VERSION = "light-tryon-auto-mixcut-bgm-v1.0.0"


def _default_media_bin(name: str) -> str:
    env_value = str(os.environ.get(f"{name.upper()}_BIN") or "").strip()
    if env_value:
        return env_value
    discovered = shutil.which(name)
    if discovered:
        return discovered
    local_bin = Path.home() / ".local" / "bin" / name
    return str(local_bin) if local_bin.is_file() else name


def _probe_duration_ms(path: Path, ffprobe_bin: str) -> int:
    proc = subprocess.run(
        [ffprobe_bin, "-v", "error", "-show_entries", "format=duration", "-of", "json", str(path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or "ffprobe failed")[-2000:])
    duration = float((json.loads(proc.stdout).get("format") or {}).get("duration") or 0)
    if duration <= 0:
        raise RuntimeError(f"无法读取视频时长: {path}")
    return int(round(duration * 1000))


def _mix_video(
    source: Path,
    target: Path,
    track: dict[str, Any],
    duration_ms: int,
    *,
    ffmpeg_bin: str,
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    source_size = source.stat().st_size
    start_sec = max(float(track.get("recommended_start_sec") or 0), 0)
    audio_filter = _bgm_audio_filter(track, duration_ms)
    duration_sec = max(duration_ms, 500) / 1000
    temporary = target.with_name(f".{target.stem}.{os.getpid()}.tmp{target.suffix}")
    temporary.unlink(missing_ok=True)
    command = [
        ffmpeg_bin,
        "-y",
        "-i",
        str(source),
        "-stream_loop",
        "-1",
        "-ss",
        f"{start_sec:.3f}",
        "-i",
        str(track["path"]),
        "-filter_complex",
        f"[1:a]{audio_filter}[bgm]",
        "-map",
        "0:v:0",
        "-map",
        "[bgm]",
        "-t",
        f"{duration_sec:.3f}",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-ar",
        "44100",
        "-ac",
        "2",
        "-movflags",
        "+faststart",
        str(temporary),
    ]
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if proc.returncode != 0 or not temporary.is_file() or temporary.stat().st_size <= 0:
            raise RuntimeError((proc.stderr or "BGM mix failed")[-3000:])
        if temporary.stat().st_size < max(200_000, int(source_size * 0.10)):
            raise RuntimeError(
                f"BGM成片体积异常: source={source_size} output={temporary.stat().st_size}"
            )
        validate_video_decode(
            temporary,
            ffmpeg_bin=ffmpeg_bin,
            expected_duration=duration_sec,
        )
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(SKILL_DIR / "var" / "light_tryon.sqlite3"))
    parser.add_argument("--job-id", action="append")
    parser.add_argument("--market", default="TH")
    parser.add_argument("--category", default="womens_outerwear")
    parser.add_argument("--mood", default="daily_clean")
    parser.add_argument("--energy", default="medium")
    parser.add_argument("--template-id", default="AI_PRODUCT_FIRST_20S")
    parser.add_argument("--ffmpeg", default=_default_media_bin("ffmpeg"))
    parser.add_argument("--ffprobe", default=_default_media_bin("ffprobe"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    db = LightTryonDB(args.db)
    db.init_schema()
    config = load_feishu_config(SKILL_DIR / "config" / "feishu_tables.json")
    review_client = build_clients(resolve_endpoints(config))["review"]
    records = {
        str(record.fields.get("视频任务ID") or "").strip(): record
        for record in review_client.list_records(page_size=500)
        if record.fields.get("初始成片")
    }
    selected = {str(item).strip() for item in (args.job_id or []) if str(item).strip()}
    jobs = [db.get_job(job_id) for job_id in records]
    jobs = [job for job in jobs if job and (not selected or job["job_id"] in selected)]
    jobs.sort(key=lambda row: (int(row.get("variant_no") or 0), row["job_id"]))

    ctx = build_context()
    initialized = RDSRepositorySkill(ctx).init_db()
    if not initialized.success:
        raise RuntimeError(initialized.message or "BGM数据库初始化失败")
    recommendation = BgmLibrarySkill(ctx).get_recommendation(
        category=args.category,
        mood=args.mood,
        template_id=args.template_id,
        energy=args.energy,
        preferred_vocal_types=["instrumental"],
        market=args.market,
    )
    if not recommendation.success:
        raise RuntimeError(recommendation.message or "BGM推荐失败")
    tracks = recommendation.data.get("recommendations") or []
    if len(tracks) < len(jobs):
        raise RuntimeError(f"可用去重BGM不足: 任务{len(jobs)}条，曲目{len(tracks)}首")

    manifest_root = db.path.parent / "review_bgm"
    result = {"candidates": len(jobs), "processed": 0, "skipped": 0, "failed": 0, "items": []}
    for index, job in enumerate(jobs):
        job_id = job["job_id"]
        record = records[job_id]
        recommendation_row = dict(tracks[index])
        full_track = ctx.repo.get("bgm_tracks", "bgm_id", recommendation_row.get("bgm_id")) or {}
        track = _apply_audio_mix_suggestions({**recommendation_row, **full_track})
        source = Path(str(job.get("output_video_path") or "")).expanduser().resolve()
        job_manifest = manifest_root / f"{job_id}.json"
        previous: dict[str, Any] = {}
        if job_manifest.is_file():
            try:
                previous = json.loads(job_manifest.read_text(encoding="utf-8"))
            except Exception:
                previous = {}
        previous_output = Path(str(previous.get("output_video_path") or "")).expanduser()
        if source == previous_output and previous.get("source_video_path"):
            source = Path(str(previous["source_video_path"])).expanduser().resolve()
        try:
            if not source.is_file():
                raise FileNotFoundError(f"找不到品牌后处理视频: {source}")
            duration_ms = _probe_duration_ms(source, args.ffprobe)
            validate_video_decode(
                source,
                ffmpeg_bin=args.ffmpeg,
                ffprobe_bin=args.ffprobe,
                expected_duration=duration_ms / 1000,
            )
            audio_path = _bgm_track_path(ctx, track)
            if not audio_path or not audio_path.is_file():
                raise FileNotFoundError(f"BGM音频不可用: {track.get('bgm_id')}")
            track = _normalize_bgm_mix(ctx, {**track, "path": str(audio_path)}, audio_path, duration_ms)
            track["path"] = str(audio_path)
            source_stat = source.stat()
            mix_hash = stable_hash(
                POLICY_VERSION,
                str(source),
                source_stat.st_size,
                source_stat.st_mtime_ns,
                _bgm_manifest(track),
                length=24,
            )
            if not args.force and previous.get("mix_hash") == mix_hash and previous_output.is_file():
                result["skipped"] += 1
                result["items"].append({"job_id": job_id, "status": "skipped", "bgm_id": track.get("bgm_id")})
                continue
            target = manifest_root / job_id / mix_hash / f"{job_id}_bgm_final.mp4"
            _mix_video(source, target, track, duration_ms, ffmpeg_bin=args.ffmpeg)
            uploaded = review_client.upload_attachment(
                target.read_bytes(), target.name, "video/mp4", target.stat().st_size,
                parent_type="bitable_file",
            )
            review_client.update_record_fields(
                record.record_id,
                {"最终视频": [uploaded], "数据同步状态": "已同步", "同步错误信息": ""},
            )
            db.set_postprocessed_assets(job_id, str(target), str(job.get("output_cover_path") or ""))
            manifest = {
                "policy_version": POLICY_VERSION,
                "job_id": job_id,
                "source_video_path": str(source),
                "output_video_path": str(target),
                "mix_hash": mix_hash,
                "bgm": _bgm_manifest(track),
                "final_attachment": uploaded,
            }
            job_manifest.parent.mkdir(parents=True, exist_ok=True)
            job_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            result["processed"] += 1
            result["items"].append({
                "job_id": job_id,
                "status": "success",
                "bgm_id": track.get("bgm_id"),
                "track_name": track.get("track_name"),
                "output_video_path": str(target),
                "final_file_token": uploaded.get("file_token") or "",
            })
        except Exception as exc:
            result["failed"] += 1
            result["items"].append({"job_id": job_id, "status": "failed", "error": str(exc)})
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not result["failed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
