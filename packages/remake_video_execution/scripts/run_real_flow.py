#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
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


def _correct_product_conflicts(prompt: str) -> str:
    replacements = {
        "深棕色": "真实商品图中的暖棕色",
        "小翻领/小领口结构": "宽尖角翻领结构",
        "简洁小翻领/小领口结构": "宽尖角翻领结构",
        "小翻领": "宽尖角翻领",
        "三颗纽扣": "四枚金色圆形按扣",
        "前三颗前中纽扣": "四枚前中金色圆形按扣",
        "前三颗纽扣": "四枚金色圆形按扣",
        "第一、第二、第三颗前中纽扣": "第一至第四枚前中金色圆形按扣",
        "第三、第二、第一颗纽扣": "第四至第一枚金色圆形按扣",
    }
    for old, new in replacements.items():
        prompt = prompt.replace(old, new)
    prompt = re.sub(r"(?m)^\s*(?:屏幕文字|字幕|评论气泡)\s*[：:].*$", "", prompt)
    product_lock = (
        "\n\n【真实商品图权威纠偏】\n"
        "真实商品参考图优先于原提示词中的冲突描述。外套固定为暖棕色哑光仿麂皮PU短款箱型外套，"
        "宽尖角翻领，单排四枚金色圆形按扣，袖口各有同款金色圆扣；禁止改成三扣、黑扣、窄小领、"
        "拉链、亮面皮革或长款。观众可见文字统一由后期字幕生成，视频模型不得自行画字。"
    )
    return prompt + product_lock


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
    completed = subprocess.run([
        ffmpeg, "-y", "-i", str(silent_video), "-stream_loop", "-1", "-i", str(bgm),
        "-filter_complex",
        "[1:a]atrim=0:41,asetpts=PTS-STARTPTS,loudnorm=I=-16:TP=-1.5:LRA=11,"
        "volume=0.55,afade=t=in:st=0:d=0.18,afade=t=out:st=40.3:d=0.7[a]",
        "-map", "0:v:0", "-map", "[a]", "-t", "41", "-c:v", "copy", "-c:a", "aac",
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
    parser = argparse.ArgumentParser(description="真实生成、下载并拼接一条复刻长视频")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--start-frame", required=True)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--market", default="")
    parser.add_argument("--sku-id", default="DEFAULT")
    parser.add_argument("--reference-image-pack-id", default="")
    parser.add_argument("--reference-group-id", default="")
    parser.add_argument("--reference-source", choices=("auto", "operation", "amc"), default="auto")
    parser.add_argument("--bgm", required=True)
    parser.add_argument("--subtitle-text", default="")
    parser.add_argument("--subtitle-start", type=float, default=39.0)
    parser.add_argument("--subtitle-end", type=float, default=41.0)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--allow-real-submit", action="store_true")
    parser.add_argument("--poll-interval", type=int, default=20)
    parser.add_argument("--max-wait", type=int, default=2400)
    args = parser.parse_args()
    if not args.allow_real_submit:
        raise SystemExit("真实 H3 提交必须显式使用 --allow-real-submit")

    plan_path = Path(args.plan).resolve()
    start_frame = Path(args.start_frame).resolve()
    bgm = Path(args.bgm).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if not plan_path.is_file() or not start_frame.is_file() or not bgm.is_file():
        raise SystemExit("计划、首帧或 BGM 不存在")

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
    corrected_prompt = _correct_product_conflicts(source.raw_prompt)
    source = SourceSnapshot(**{
        **source.to_dict(),
        "raw_prompt": corrected_prompt,
        "source_revision_hash": hashlib.sha256(
            (source.source_revision_hash + corrected_prompt).encode()
        ).hexdigest(),
    })
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
    mixed = output_dir / "final_video_continuous_bgm.mp4"
    final = output_dir / "final_video.mp4"
    _merge(segment_paths, silent)
    _add_continuous_bgm(silent, bgm, mixed)
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
        "audio": {"mode": "continuous_local_bgm_no_voiceover", "path": str(bgm)},
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
