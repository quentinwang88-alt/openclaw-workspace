from __future__ import annotations

import json
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from .database import LightTryonDB
from .prompting import build_jimeng_record
from .utils import json_dumps, now_iso


class WorkerError(RuntimeError):
    pass


def run_json_command(command: str, payload: dict[str, Any], timeout: int = 900) -> dict[str, Any]:
    args = shlex.split(command)
    if not args:
        raise ValueError("外部 worker 命令不能为空")
    completed = subprocess.run(
        args,
        input=json_dumps(payload),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "未知错误").strip()
        raise WorkerError(f"worker 退出码 {completed.returncode}: {detail[:1600]}")
    output = completed.stdout.strip()
    if not output:
        raise WorkerError("worker 未输出 JSON")
    try:
        result = json.loads(output)
    except json.JSONDecodeError as exc:
        raise WorkerError(f"worker 输出不是合法 JSON: {output[:1000]}") from exc
    if not isinstance(result, dict):
        raise WorkerError("worker JSON 顶层必须是对象")
    return result


def run_generation_worker(
    db: LightTryonDB,
    command: str,
    *,
    limit: int = 1,
    timeout: int = 900,
    provider: str = "command",
    max_attempts: int = 2,
) -> dict[str, Any]:
    jobs = db.claim_pending_jobs(limit, provider)
    summary: dict[str, Any] = {"claimed": len(jobs), "success": 0, "retrying": 0, "failed": 0, "items": []}
    for job in jobs:
        context = db.get_job_context(job["job_id"])
        payload = {
            "protocol_version": "1.0",
            "job": context["job"],
            "product": context["product"],
            "prompt_payload": context["job"]["prompt_payload"],
            "jimeng_record": build_jimeng_record(context, context["job"]["prompt_payload"]),
        }
        try:
            response = run_json_command(command, payload, timeout=timeout)
            output_path = Path(str(response.get("output_video_path") or "")).expanduser()
            if not output_path.is_file():
                raise WorkerError(f"worker 返回的视频文件不存在: {output_path}")
            response["output_video_path"] = str(output_path.resolve())
            db.complete_generation(job["job_id"], response)
            summary["success"] += 1
            summary["items"].append({"job_id": job["job_id"], "status": "success", "output": response["output_video_path"]})
        except Exception as exc:  # 单条失败不阻塞其他任务
            retryable = int(job.get("retry_count") or 0) < int(max_attempts)
            db.fail_generation(job["job_id"], str(exc), retryable=retryable)
            target = "retrying" if retryable else "failed"
            summary[target] += 1
            summary["items"].append({"job_id": job["job_id"], "status": target, "error": str(exc)})
    return summary


def export_job_records(
    db: LightTryonDB,
    output_path: str | Path,
    *,
    product_id: str | None = None,
    status: str | None = "pending",
) -> dict[str, Any]:
    jobs = db.list_jobs(product_id=product_id, generation_status=status)
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    for job in jobs:
        context = db.get_job_context(job["job_id"])
        prompt_payload = context["job"].get("prompt_payload") or {}
        if not prompt_payload or context["job"].get("prompt_version") == "unbuilt":
            continue
        records.append(build_jimeng_record(context, prompt_payload))
    path.write_text("\n".join(json_dumps(item) for item in records) + ("\n" if records else ""), encoding="utf-8")
    return {"output_path": str(path), "records": len(records), "format": "jimeng-jsonl"}


def probe_video(path: str | Path, ffprobe_bin: str = "ffprobe") -> dict[str, Any]:
    video_path = Path(path).expanduser().resolve()
    if not video_path.is_file():
        raise FileNotFoundError(f"视频不存在: {video_path}")
    command = [
        ffprobe_bin,
        "-v",
        "error",
        "-show_entries",
        "format=duration:stream=index,codec_type,codec_name,width,height,avg_frame_rate,duration",
        "-of",
        "json",
        str(video_path),
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False, timeout=60)
    if completed.returncode != 0:
        raise WorkerError((completed.stderr or "ffprobe 失败").strip())
    data = json.loads(completed.stdout)
    video_stream = next((stream for stream in data.get("streams", []) if stream.get("codec_type") == "video"), None)
    if not video_stream:
        raise WorkerError("文件中没有视频流")
    duration_raw = data.get("format", {}).get("duration") or video_stream.get("duration") or 0
    try:
        duration = float(duration_raw)
    except (TypeError, ValueError):
        duration = 0.0
    return {
        "path": str(video_path),
        "duration_seconds": duration,
        "width": int(video_stream.get("width") or 0),
        "height": int(video_stream.get("height") or 0),
        "codec": video_stream.get("codec_name") or "",
        "avg_frame_rate": video_stream.get("avg_frame_rate") or "",
        "has_video_stream": True,
    }


def structural_qc(probe: dict[str, Any], *, min_duration: float = 8.0, max_duration: float = 10.0) -> dict[str, Any]:
    duration = float(probe.get("duration_seconds") or 0)
    width = int(probe.get("width") or 0)
    height = int(probe.get("height") or 0)
    failures: list[str] = []
    warnings: list[str] = []
    # 编码容器常有 1-2 帧尾差，允许 0.12 秒技术容差，但报告仍记录真实值。
    if duration < min_duration - 0.12 or duration > max_duration + 0.12:
        failures.append("failed_length")
    elif duration < min_duration or duration > max_duration:
        warnings.append("duration_encoding_tolerance")
    if width <= 0 or height <= 0:
        failures.append("failed_video_stream")
    else:
        ratio = width / height
        target = 9 / 16
        if height <= width or abs(ratio - target) > 0.025:
            failures.append("failed_aspect_ratio")
        if width < 360 or height < 640:
            warnings.append("low_resolution")
    return {
        "passed": not failures,
        "failures": failures,
        "warnings": warnings,
        "duration_seconds": round(duration, 3),
        "width": width,
        "height": height,
        "observed_ratio": round(width / height, 5) if height else None,
        "expected_ratio": "9:16",
    }


def _score(value: Any, maximum: int) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return round(min(float(maximum), max(0.0, number)), 2)


def evaluate_qc(
    structural: dict[str, Any],
    vision: dict[str, Any] | None = None,
    expectations: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    result: dict[str, Any] = {
        "schema_version": "1.0.0",
        "checked_at": now_iso(),
        "structural": structural,
        "vision": vision or {},
        "expectations": expectations or {},
    }
    if not structural.get("passed"):
        result["decision"] = "failed"
        result["failure_codes"] = structural.get("failures") or ["failed_video_structure"]
        result["total_score"] = 0
        return "failed", result
    if not vision:
        result["decision"] = "manual_review"
        result["failure_codes"] = []
        result["manual_review_reasons"] = ["vision_qc_not_run"]
        result["total_score"] = None
        return "manual_review", result

    scores = {
        "scene_consistency": _score(vision.get("scene_consistency"), 30),
        "person_naturalness": _score(vision.get("person_naturalness"), 20),
        "clothing_clarity": _score(vision.get("clothing_clarity"), 20),
        "action_completeness": _score(vision.get("action_completeness"), 15),
        "realism": _score(vision.get("realism"), 15),
    }
    total = round(sum(scores.values()), 2)
    severe: list[str] = []
    required_booleans = {
        "phone_covers_face": "failed_face_visibility",
        "clothing_identifiable": "failed_clothing",
        "no_severe_body_anomaly": "failed_body",
        "scene_matches_template": "failed_scene",
        "action_complete": "failed_motion",
        "camera_motion_matches": "failed_camera_motion",
        "brightness_adequate": "failed_brightness",
        "no_overexposure": "failed_overexposure",
        "product_color_preserved": "failed_product_color",
    }
    if (expectations or {}).get("upper_garment_fully_visible"):
        required_booleans["upper_garment_fully_visible"] = "failed_upper_garment_crop"
    missing_required: list[str] = []
    for field, code in required_booleans.items():
        if field not in vision:
            missing_required.append(field)
        elif vision.get(field) is False:
            severe.append(code)
    if "visible_anchor_count" not in vision:
        missing_required.append("visible_anchor_count")
    else:
        anchor_count = int(vision.get("visible_anchor_count") or 0)
        if anchor_count < 2:
            severe.append("failed_scene_anchors")
    result["scores"] = scores
    result["total_score"] = total
    result["failure_codes"] = sorted(set(severe))
    result["missing_required_fields"] = missing_required
    result["evidence"] = vision.get("evidence") or []
    if severe or total < 60:
        status = "failed"
    elif missing_required or total < 80:
        status = "manual_review"
    else:
        status = "passed"
    result["decision"] = status
    return status, result


def qc_job(
    db: LightTryonDB,
    job_id: str,
    *,
    vision_result: dict[str, Any] | None = None,
    vision_command: str | None = None,
    ffprobe_bin: str = "ffprobe",
    timeout: int = 600,
) -> dict[str, Any]:
    context = db.get_job_context(job_id)
    job = context["job"]
    if job.get("generation_status") != "success":
        raise ValueError(f"任务尚未生成成功，不能 QC: {job_id}")
    probe = probe_video(job["output_video_path"], ffprobe_bin=ffprobe_bin)
    structural = structural_qc(probe)
    vision = vision_result
    if vision_command and structural.get("passed"):
        vision_payload = {
            "protocol_version": "1.0",
            "job": job,
            "video_path": job["output_video_path"],
            "scene_template": context["scene"],
            "action_template": context["action"],
            "product": context["product"],
            "qc_expectations": (job.get("prompt_payload") or {}).get("qc_expectations") or {},
        }
        vision = run_json_command(vision_command, vision_payload, timeout=timeout)
    expectations = (job.get("prompt_payload") or {}).get("qc_expectations") or {}
    status, result = evaluate_qc(structural, vision, expectations)
    db.apply_qc(job_id, status, result)
    return {"job_id": job_id, "qc_status": status, "qc_result": result}


def qc_pending_jobs(
    db: LightTryonDB,
    *,
    limit: int = 20,
    vision_command: str | None = None,
    ffprobe_bin: str = "ffprobe",
    timeout: int = 600,
) -> dict[str, Any]:
    jobs = db.list_jobs(generation_status="success", qc_status="pending", limit=limit)
    summary: dict[str, Any] = {"processed": 0, "passed": 0, "failed": 0, "manual_review": 0, "errors": []}
    for job in jobs:
        try:
            item = qc_job(
                db,
                job["job_id"],
                vision_command=vision_command,
                ffprobe_bin=ffprobe_bin,
                timeout=timeout,
            )
            summary["processed"] += 1
            summary[item["qc_status"]] += 1
        except Exception as exc:
            summary["errors"].append({"job_id": job["job_id"], "error": str(exc)})
    return summary


def _srt_timestamp(seconds: float) -> str:
    milliseconds = max(0, int(round(seconds * 1000)))
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    secs, millis = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def render_subtitles(
    input_video: str | Path,
    output_video: str | Path,
    subtitle_plan: dict[str, Any],
    *,
    ffmpeg_bin: str = "ffmpeg",
    font_name: str = "Arial Unicode MS",
) -> dict[str, Any]:
    source = Path(input_video).expanduser().resolve()
    target = Path(output_video).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"视频不存在: {source}")
    cues = subtitle_plan.get("cues") or []
    if not cues:
        raise ValueError("字幕计划没有 cue")
    target.parent.mkdir(parents=True, exist_ok=True)
    srt_lines: list[str] = []
    for index, cue in enumerate(cues, start=1):
        text = str(cue.get("text") or "").replace("\r", " ").replace("\n", " ").strip()
        if not text:
            continue
        srt_lines.extend(
            [
                str(index),
                f"{_srt_timestamp(float(cue['start']))} --> {_srt_timestamp(float(cue['end']))}",
                text,
                "",
            ]
        )
    if not srt_lines:
        raise ValueError("字幕计划没有有效文字")
    with tempfile.TemporaryDirectory(prefix="ltv_subtitle_") as tmp:
        srt_path = Path(tmp) / "captions.srt"
        srt_path.write_text("\n".join(srt_lines), encoding="utf-8")
        escaped = str(srt_path).replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")
        style = (
            f"FontName={font_name},FontSize=19,PrimaryColour=&H00FFFFFF,"
            "OutlineColour=&H80000000,BorderStyle=3,Outline=1,Shadow=0,"
            "Alignment=2,MarginV=110"
        )
        filter_value = f"subtitles='{escaped}':force_style='{style}'"
        command = [
            ffmpeg_bin,
            "-y",
            "-i",
            str(source),
            "-vf",
            filter_value,
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            "18",
            "-c:a",
            "copy",
            "-movflags",
            "+faststart",
            str(target),
        ]
        completed = subprocess.run(command, text=True, capture_output=True, check=False, timeout=300)
        if completed.returncode != 0:
            raise WorkerError((completed.stderr or "ffmpeg 字幕烧录失败")[-3000:])
    if not target.is_file() or target.stat().st_size <= 0:
        raise WorkerError("字幕烧录未生成有效文件")
    return {"output_video_path": str(target), "cue_count": len(cues), "font_name": font_name}


BRAND_COLOR_MAP = {
    "cream_white": "#F7F1E8",
    "white": "#FFFFFF",
    "light_gold": "#E8D18B",
    "warm_gray": "#C9C2B8",
}


def _brand_font_path(style_preset: str, *, secondary: bool = False) -> str:
    if secondary or style_preset == "minimal_sans":
        candidates = (
            "/System/Library/Fonts/Avenir Next.ttc",
            "/System/Library/Fonts/Avenir.ttc",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
        )
    else:
        candidates = (
            "/System/Library/Fonts/Supplemental/Didot.ttc",
            "/System/Library/Fonts/Supplemental/Times New Roman.ttf",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
        )
    return next((item for item in candidates if Path(item).is_file()), candidates[-1])


def _render_brand_overlay_png(width: int, height: int, brand_plan: dict[str, Any], output_path: Path) -> dict[str, Any]:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise WorkerError("品牌叠加需要 Pillow") from exc

    if width <= 0 or height <= 0:
        raise ValueError("品牌叠加需要有效的视频尺寸")
    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    style = str(brand_plan.get("style_preset") or "cream_serif")
    color_value = str(brand_plan.get("primary_color") or "cream_white")
    color = BRAND_COLOR_MAP.get(color_value, color_value if color_value.startswith("#") else "#F7F1E8")
    display_name = str(brand_plan.get("display_name") or "").strip()
    series_title = str(brand_plan.get("series_title") or "").strip()
    logo_path = next(
        (Path(item).expanduser().resolve() for item in brand_plan.get("logo_images") or [] if Path(str(item)).expanduser().is_file()),
        None,
    )
    max_width = max(1, int(width * float(brand_plan.get("max_width_ratio") or 0.40)))
    primary_height = max(32, int(height * 0.060))
    secondary_height = max(18, int(height * 0.020))
    gap = max(8, int(height * 0.008))
    primary_image = None
    if logo_path:
        primary_image = Image.open(logo_path).convert("RGBA")
        max_logo_height = max(40, int(height * 0.090))
        scale = min(max_width / primary_image.width, max_logo_height / primary_image.height, 1.0)
        primary_image = primary_image.resize(
            (max(1, int(primary_image.width * scale)), max(1, int(primary_image.height * scale))),
            Image.Resampling.LANCZOS,
        )
        primary_size = primary_image.size
    elif display_name:
        font_size = primary_height
        primary_font = ImageFont.truetype(_brand_font_path(style), font_size)
        measure = ImageDraw.Draw(canvas)
        bbox = measure.textbbox((0, 0), display_name, font=primary_font, stroke_width=max(1, width // 540))
        while bbox[2] - bbox[0] > max_width and font_size > 24:
            font_size -= 2
            primary_font = ImageFont.truetype(_brand_font_path(style), font_size)
            bbox = measure.textbbox((0, 0), display_name, font=primary_font, stroke_width=max(1, width // 540))
        primary_size = (bbox[2] - bbox[0], bbox[3] - bbox[1])
    else:
        raise ValueError("品牌计划缺少店铺Logo和品牌展示名称")

    secondary_font = ImageFont.truetype(_brand_font_path(style, secondary=True), secondary_height)
    measure = ImageDraw.Draw(canvas)
    secondary_bbox = measure.textbbox((0, 0), series_title, font=secondary_font) if series_title else (0, 0, 0, 0)
    secondary_size = (secondary_bbox[2] - secondary_bbox[0], secondary_bbox[3] - secondary_bbox[1])
    block_width = max(primary_size[0], secondary_size[0])
    block_height = primary_size[1] + (gap + secondary_size[1] if series_title else 0)
    center_y = int(height * float(brand_plan.get("center_y_ratio") or 0.50))
    origin_x = max(int(width * 0.05), min((width - block_width) // 2, int(width * 0.95) - block_width))
    origin_y = max(int(height * 0.10), min(center_y - block_height // 2, int(height * 0.90) - block_height))
    shadow_offset = max(1, width // 360)
    stroke_width = max(1, width // 540)

    if primary_image is not None:
        x = (width - primary_image.width) // 2
        canvas.alpha_composite(primary_image, (x, origin_y))
    else:
        x = (width - primary_size[0]) // 2
        draw = ImageDraw.Draw(canvas)
        draw.text(
            (x + shadow_offset, origin_y + shadow_offset), display_name, font=primary_font,
            fill=(0, 0, 0, 105), stroke_width=stroke_width, stroke_fill=(0, 0, 0, 80),
        )
        draw.text(
            (x, origin_y), display_name, font=primary_font, fill=color,
            stroke_width=stroke_width, stroke_fill=(0, 0, 0, 95),
        )
    if series_title:
        secondary_y = origin_y + primary_size[1] + gap
        secondary_x = (width - secondary_size[0]) // 2
        draw = ImageDraw.Draw(canvas)
        draw.text((secondary_x + shadow_offset, secondary_y + shadow_offset), series_title, font=secondary_font, fill=(0, 0, 0, 110))
        draw.text((secondary_x, secondary_y), series_title, font=secondary_font, fill=color)
    canvas.save(output_path)
    return {
        "overlay_path": str(output_path), "width": width, "height": height,
        "block_box": [origin_x, origin_y, origin_x + block_width, origin_y + block_height],
        "used_logo": bool(primary_image), "display_name": display_name, "series_title": series_title,
    }


def render_brand_overlay(
    input_video: str | Path,
    output_video: str | Path,
    brand_plan: dict[str, Any],
    *,
    cover_output: str | Path | None = None,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
) -> dict[str, Any]:
    if not brand_plan.get("enabled"):
        raise ValueError("品牌叠加计划未启用")
    source = Path(input_video).expanduser().resolve()
    target = Path(output_video).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"视频不存在: {source}")
    if source == target:
        raise ValueError("品牌叠加输出不能覆盖输入原片")
    target.parent.mkdir(parents=True, exist_ok=True)
    probe = probe_video(source, ffprobe_bin=ffprobe_bin)
    display_seconds = max(0.2, float(brand_plan.get("display_seconds") or 0.8))
    raw_fade_in = brand_plan.get("fade_in_seconds")
    raw_fade_out = brand_plan.get("fade_out_seconds")
    fade_in = min(display_seconds / 2, max(0.0, float(0.12 if raw_fade_in is None else raw_fade_in)))
    fade_out = min(display_seconds / 2, max(0.0, float(0.18 if raw_fade_out is None else raw_fade_out)))
    fade_out_start = max(fade_in, display_seconds - fade_out)
    with tempfile.TemporaryDirectory(prefix="ltv_brand_") as tmp:
        overlay_path = Path(tmp) / "brand-overlay.png"
        overlay_meta = _render_brand_overlay_png(probe["width"], probe["height"], brand_plan, overlay_path)
        brand_filters = ["format=rgba"]
        if fade_in > 0:
            brand_filters.append(f"fade=t=in:st=0:d={fade_in:.3f}:alpha=1")
        if fade_out > 0:
            brand_filters.append(f"fade=t=out:st={fade_out_start:.3f}:d={fade_out:.3f}:alpha=1")
        filter_value = (
            f"[1:v]{','.join(brand_filters)}[brand];"
            f"[0:v][brand]overlay=0:0:enable='between(t,0,{display_seconds:.3f})':shortest=1[v]"
        )
        command = [
            ffmpeg_bin, "-y", "-i", str(source), "-loop", "1", "-i", str(overlay_path),
            "-filter_complex", filter_value, "-map", "[v]", "-map", "0:a?",
            "-c:v", "libx264", "-preset", "medium", "-crf", "18", "-pix_fmt", "yuv420p",
            "-c:a", "copy", "-movflags", "+faststart", str(target),
        ]
        completed = subprocess.run(command, text=True, capture_output=True, check=False, timeout=300)
        if completed.returncode != 0:
            raise WorkerError((completed.stderr or "品牌叠加失败")[-3000:])
    if not target.is_file() or target.stat().st_size <= 0:
        raise WorkerError("品牌叠加未生成有效文件")
    cover_path = Path(cover_output).expanduser().resolve() if cover_output else target.with_name(target.stem + "_cover.jpg")
    cover_path.parent.mkdir(parents=True, exist_ok=True)
    cover_command = [
        ffmpeg_bin, "-y", "-ss", f"{min(0.35, display_seconds / 2):.3f}", "-i", str(target),
        "-frames:v", "1", "-q:v", "2", str(cover_path),
    ]
    completed = subprocess.run(cover_command, text=True, capture_output=True, check=False, timeout=120)
    if completed.returncode != 0 or not cover_path.is_file():
        raise WorkerError((completed.stderr or "品牌封面提取失败")[-3000:])
    return {
        "output_video_path": str(target), "output_cover_path": str(cover_path),
        "display_seconds": display_seconds, "overlay": overlay_meta,
    }


def render_postprocessed_video(
    input_video: str | Path,
    output_video: str | Path,
    subtitle_plan: dict[str, Any],
    brand_plan: dict[str, Any],
    *,
    cover_output: str | Path | None = None,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
    subtitle_font_name: str = "Arial Unicode MS",
) -> dict[str, Any]:
    source = Path(input_video).expanduser().resolve()
    target = Path(output_video).expanduser().resolve()
    captions_enabled = bool(subtitle_plan.get("cues"))
    branding_enabled = bool(brand_plan.get("enabled"))
    if not captions_enabled and not branding_enabled:
        raise ValueError("字幕和品牌叠加均未启用")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ltv_postprocess_") as tmp:
        captioned = Path(tmp) / "captioned.mp4"
        branded_source = source
        subtitle_result: dict[str, Any] = {}
        if captions_enabled:
            subtitle_target = captioned if branding_enabled else target
            subtitle_result = render_subtitles(
                source, subtitle_target, subtitle_plan, ffmpeg_bin=ffmpeg_bin, font_name=subtitle_font_name,
            )
            branded_source = subtitle_target
        if branding_enabled:
            brand_result = render_brand_overlay(
                branded_source, target, brand_plan, cover_output=cover_output,
                ffmpeg_bin=ffmpeg_bin, ffprobe_bin=ffprobe_bin,
            )
        else:
            brand_result = {"output_video_path": str(target), "output_cover_path": ""}
        if not target.is_file() and branded_source.is_file():
            shutil.copy2(branded_source, target)
    return {
        **brand_result,
        "subtitle_rendered": captions_enabled,
        "brand_rendered": branding_enabled,
        "subtitle": subtitle_result,
    }
