#!/usr/bin/env python3
"""Local debug tool: generate, download and concatenate one remake video.

This is NOT the production entry point.  Formal remake production goes through
the shared operator entry point only:

    python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py sync --limit 20

which compiles the frozen remake script into a Plan C job and reuses the same
keyframes/H3/resume/TTS/merge/validation/Feishu write-back as original
long-form.  This script deliberately carries no product-specific prompt
corrections, no fixed BGM length and no fixed subtitle window; every such value
must be passed explicitly.  It never migrates tasks from the legacy
``remake_video_execution.sqlite3`` database.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from core.longform.h3_gateway import H3Gateway
from remake_video_execution.contracts import SourceSnapshot
from remake_video_execution.executor import submit_frozen_segments
from remake_video_execution.parser import parse_source
from remake_video_execution.planner import plan_segments
from remake_video_execution.providers.plan_c import PlanCMediaAdapter
from remake_video_execution.repository import RemakeExecutionRepository
from remake_video_execution.references import resolve_production_references, frozen_product_paths


def _status(response: dict[str, Any]) -> str:
    return str((((response.get("response") or {}).get("task") or {}).get("status") or "")).lower()


def _video_seconds(path: Path) -> float:
    """Read the real duration instead of assuming any fixed length."""

    ffprobe = shutil.which("ffprobe") or "/Users/likeu3/.local/bin/ffprobe"
    completed = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("读取视频时长失败: " + completed.stderr[-800:])
    return float(completed.stdout.strip())


def _merge(segment_paths: list[Path], output: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("缺少 ffmpeg")
    filters = []
    for index in range(len(segment_paths)):
        filters.append(
            f"[{index}:v]scale=720:1280:force_original_aspect_ratio=decrease,"
            f"pad=720:1280:(ow-iw)/2:(oh-ih)/2,fps=30,format=yuv420p,setpts=PTS-STARTPTS[v{index}]"
        )
    inputs = "".join(f"[v{index}]" for index in range(len(segment_paths)))
    filters.append(f"{inputs}concat=n={len(segment_paths)}:v=1:a=0[outv]")
    command = [ffmpeg, "-y"]
    for path in segment_paths:
        command.extend(["-i", str(path)])
    command.extend([
        "-filter_complex", ";".join(filters), "-map", "[outv]", "-an",
        "-c:v", "libx264", "-preset", "medium", "-crf", "18",
        "-movflags", "+faststart", str(output),
    ])
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("视频拼接失败: " + completed.stderr[-1600:])


def _add_continuous_bgm(silent_video: Path, bgm: Path, output: Path) -> None:
    """Replace all provider segment audio with one continuous licensed track."""

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("缺少 ffmpeg")
    seconds = _video_seconds(silent_video)
    completed = subprocess.run([
        ffmpeg, "-y", "-i", str(silent_video), "-stream_loop", "-1", "-i", str(bgm),
        "-filter_complex",
        f"[1:a]atrim=0:{seconds:.3f},asetpts=PTS-STARTPTS,loudnorm=I=-16:TP=-1.5:LRA=11,"
        f"volume=0.55,afade=t=in:st=0:d=0.18,afade=t=out:st={max(0.0, seconds - 0.7):.3f}:d=0.7[a]",
        "-map", "0:v:0", "-map", "[a]", "-t", f"{seconds:.3f}", "-c:v", "copy", "-c:a", "aac",
        "-ar", "44100", "-ac", "2", "-b:a", "160k", "-movflags", "+faststart", str(output),
    ], capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("连续 BGM 合成失败: " + completed.stderr[-1600:])


def _burn_subtitle(source: Path, output: Path, text: str, start_seconds: float, end_seconds: float) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("缺少 ffmpeg")
    subtitle = output.with_suffix(".srt")
    def stamp(value: float) -> str:
        milliseconds = round(value * 1000)
        hours, milliseconds = divmod(milliseconds, 3_600_000)
        minutes, milliseconds = divmod(milliseconds, 60_000)
        seconds, milliseconds = divmod(milliseconds, 1000)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"
    subtitle.write_text(
        f"1\n{stamp(start_seconds)} --> {stamp(end_seconds)}\n{text}\n", encoding="utf-8"
    )
    style = "FontName=Thonburi,FontSize=18,Alignment=2,MarginV=72,Outline=2,Shadow=0"
    completed = subprocess.run([
        ffmpeg, "-y", "-i", str(source), "-vf",
        f"subtitles={subtitle}:fontsdir=/System/Library/Fonts/Supplemental:force_style='{style}'",
        "-c:v", "libx264", "-preset", "medium", "-crf", "18", "-c:a", "copy",
        "-movflags", "+faststart", str(output),
    ], capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("字幕渲染失败: " + completed.stderr[-1600:])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="复刻长视频本地调试：真实生成、下载并拼接（非正式生产入口）"
    )
    parser.add_argument("--plan", required=True)
    parser.add_argument("--start-frame", required=True)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--market", default="")
    parser.add_argument("--sku-id", default="DEFAULT")
    parser.add_argument("--reference-image-pack-id", default="")
    parser.add_argument("--reference-group-id", default="")
    parser.add_argument("--reference-source", choices=("auto", "operation", "amc"), default="auto")
    parser.add_argument("--bgm", default="", help="可选本地BGM；默认不加，交给发布端选音乐")
    parser.add_argument("--subtitle-text", default="", help="后期字幕文本；需同时给出时间窗")
    parser.add_argument("--subtitle-start", type=float, default=None)
    parser.add_argument("--subtitle-end", type=float, default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--allow-real-submit", action="store_true")
    parser.add_argument("--poll-interval", type=int, default=20)
    parser.add_argument("--max-wait", type=int, default=2400)
    args = parser.parse_args()
    if not args.allow_real_submit:
        raise SystemExit("真实 H3 提交必须显式使用 --allow-real-submit")
    if args.subtitle_text and (args.subtitle_start is None or args.subtitle_end is None):
        raise SystemExit("字幕必须显式提供 --subtitle-start 与 --subtitle-end，禁止硬编码时间窗")

    plan_path = Path(args.plan).resolve()
    start_frame = Path(args.start_frame).resolve()
    bgm = Path(args.bgm).resolve() if args.bgm else None
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if not plan_path.is_file() or not start_frame.is_file():
        raise SystemExit("计划或首帧不存在")
    if bgm is not None and not bgm.is_file():
        raise SystemExit("指定的 BGM 不存在")

    frozen = json.loads(plan_path.read_text(encoding="utf-8"))
    source = SourceSnapshot(**frozen["source"])
    snapshot_path = output_dir / "frozen_reference_source.json"
    if snapshot_path.is_file():
        prior = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if prior["input_source_revision_hash"] != source.source_revision_hash:
            raise ValueError("FROZEN_SOURCE_CHANGED_USE_NEW_OUTPUT_DIR")
        source = SourceSnapshot(**prior["source"])
    input_revision = frozen["source"]["source_revision_hash"]
    source = resolve_production_references(
        source, output_dir / "product_references", product_id=args.product_id,
        market=args.market, sku_id=args.sku_id, pack_id=args.reference_image_pack_id,
        group_id=args.reference_group_id, reference_source=args.reference_source,
    )
    product_images = frozen_product_paths(source)
    snapshot_path.write_text(json.dumps({"input_source_revision_hash": input_revision,
                                       "source": source.to_dict()}, ensure_ascii=False, indent=2), encoding="utf-8")
    # The frozen prompt is executed verbatim.  No product-specific correction is
    # applied here: the product reference image is the only appearance authority.
    execution = parse_source(source)
    segment_plan = plan_segments(execution)
    segments = [item.to_dict() for item in segment_plan.segments]
    job_id = f"real_{source.script_id}_{source.source_revision_hash[:12]}"
    repository = RemakeExecutionRepository(output_dir / "state.sqlite3")
    repository.ensure_schema()
    repository.save_plan(job_id, source.to_dict(), {
        **segment_plan.to_dict(), "issues": [], "real_test_override": True,
    })
    gateway = H3Gateway(state_root=output_dir / "h3_state")
    preflight = gateway.preflight(require_api_key=True)
    if not preflight["ready"]:
        raise RuntimeError("；".join(preflight["problems"]))
    adapter = PlanCMediaAdapter(gateway)
    print(json.dumps({"stage": "PREFLIGHT_READY", "job_id": job_id,
                      "segments": len(segments)}, ensure_ascii=False), flush=True)
    submissions = submit_frozen_segments(
        repository=repository, adapter=adapter, job_id=job_id, segments=segments,
        start_frames={item["segment_id"]: str(start_frame) for item in segments},
        reference_images={
            item["segment_id"]: [*product_images, str(start_frame)]
            for item in segments if item["incoming_boundary"] == "CUT"
        },
        request_dir=output_dir / "requests", allow_submit=True,
    )
    print(json.dumps({"stage": "SUBMITTED", "items": submissions}, ensure_ascii=False), flush=True)

    with repository.connect() as connection:
        task_rows = [dict(row) for row in connection.execute(
            "SELECT segment_id,platform_task_id FROM remake_segment WHERE job_id=? ORDER BY segment_id",
            (job_id,),
        ).fetchall()]
    deadline = time.monotonic() + max(60, args.max_wait)
    pending = {row["segment_id"]: row["platform_task_id"] for row in task_rows}
    last_status: dict[str, str] = {}
    while pending:
        for segment_id, task_id in list(pending.items()):
            response = gateway.query(task_id)
            state = _status(response)
            if last_status.get(segment_id) != state:
                print(json.dumps({"stage": "H3_STATUS", "segment_id": segment_id,
                                  "task_id": task_id, "status": state}, ensure_ascii=False), flush=True)
                last_status[segment_id] = state
            if state in {"succeeded", "success", "completed"}:
                target = output_dir / "segments" / f"{segment_id}.mp4"
                target.parent.mkdir(parents=True, exist_ok=True)
                gateway.download(task_id, target)
                with repository.connect() as connection:
                    connection.execute(
                        "UPDATE remake_segment SET status='READY',output_video_path=?,updated_at=? "
                        "WHERE job_id=? AND segment_id=?",
                        (str(target), int(time.time()), job_id, segment_id),
                    )
                pending.pop(segment_id)
                print(json.dumps({"stage": "DOWNLOADED", "segment_id": segment_id,
                                  "path": str(target)}, ensure_ascii=False), flush=True)
            elif state in {"failed", "error", "cancelled", "canceled"}:
                raise RuntimeError(f"{segment_id} H3 任务失败: {state}")
        if pending:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"等待视频生成超时；未完成: {sorted(pending)}")
            time.sleep(max(5, args.poll_interval))

    segment_paths = [output_dir / "segments" / f"{item['segment_id']}.mp4" for item in segments]
    silent = output_dir / "merged_silent.mp4"
    final = output_dir / "final_video.mp4"
    _merge(segment_paths, silent)
    audio_mode = "continuous_local_bgm_no_voiceover" if bgm else "no_voiceover_no_bgm"
    if bgm is not None:
        mixed = output_dir / "final_video_continuous_bgm.mp4"
        _add_continuous_bgm(silent, bgm, mixed)
    else:
        mixed = silent
    if args.subtitle_text:
        _burn_subtitle(mixed, final, args.subtitle_text, args.subtitle_start, args.subtitle_end)
    else:
        shutil.copy2(mixed, final)
    probe = subprocess.run([
        shutil.which("ffprobe") or "ffprobe", "-v", "error", "-show_entries",
        "format=duration,size", "-show_entries", "stream=index,codec_type,codec_name,width,height",
        "-of", "json", str(final),
    ], capture_output=True, text=True, check=True)
    report = {
        "job_id": job_id, "script_id": source.script_id, "status": "FINAL_READY",
        "final_video_path": str(final), "silent_video_path": str(silent),
        "start_frame_path": str(start_frame), "segments": task_rows,
        "product_authority_images": product_images,
        "reference_image_pack_id": source.reference_image_pack_id,
        "reference_image_version": source.reference_image_version,
        "reference_selection": source.reference_selection,
        "probe": json.loads(probe.stdout),
        "source_blockers_bypassed_for_test": [issue.code for issue in execution.issues if issue.severity == "BLOCK"],
        "production_entry_point": "openclaw_original_batch_sync.py sync (this run is a local debug tool)",
        "audio": {"mode": audio_mode, "path": str(bgm) if bgm else "", "tts_called": False},
        "subtitle": {
            "text": args.subtitle_text, "start_seconds": args.subtitle_start,
            "end_seconds": args.subtitle_end, "render_mode": "POST_PROCESS" if args.subtitle_text else "NONE",
        },
    }
    (output_dir / "execution_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
