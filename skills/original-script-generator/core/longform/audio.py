from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping


DEFAULT_VOICEOVER_ROOT = Path.home() / "voiceover_copy_engine"
EDGE_TRIM_VERSION = "edge-boundary-silence-v1"
LAYOUT_VERSION = "bounded-semantic-continuation-v1"
def _binary(name: str) -> str:
    value = shutil.which(name)
    if not value:
        local = Path.home() / ".local" / "bin" / name
        if local.is_file():
            return str(local)
        raise RuntimeError(f"缺少 {name}")
    return value


def audio_duration_seconds(path: str | Path) -> float:
    completed = subprocess.run(
        [_binary("ffprobe"), "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("读取音频时长失败: " + completed.stderr[-800:])
    return float(completed.stdout.strip())


def choose_narration_rate(measured_seconds: float, video_seconds: float) -> int:
    """Only speed up genuine overflow; never slow short copy into an unnatural read."""

    usable = max(1.0, video_seconds - 1.5)
    if measured_seconds > usable:
        return 8
    return 0


def _synthesize_edge(text: str, output_path: Path, *, voice_id: str, rate_percent: int,
                     voiceover_root: Path) -> None:
    if not voiceover_root.is_dir():
        raise RuntimeError(f"中央口播 TTS 目录不存在: {voiceover_root}")
    if str(voiceover_root) not in sys.path:
        sys.path.insert(0, str(voiceover_root))
    from voiceover_copy_engine.adapters.tts import TTSRequest, provider_from_name

    output_path.parent.mkdir(parents=True, exist_ok=True)
    asyncio.run(provider_from_name("edge").synthesize(TTSRequest(
        text=text, locale="th-TH", voice_id=voice_id,
        output_path=output_path, rate_percent=rate_percent,
    )))


def voiceover_text_hash(value: str) -> str:
    return hashlib.sha256(str(value or "").strip().encode("utf-8")).hexdigest()


def audio_asset_hash(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def measure_speech_window(path: str | Path, duration: float | None = None) -> Dict[str, Any]:
    """Trim only confidently quiet file edges, retaining 60ms of safety.

    Interior pauses remain untouched. Detection failure is observable but
    falls back to the complete asset, including old cached assets.
    """
    duration = audio_duration_seconds(path) if duration is None else duration
    report = {"trim_version": EDGE_TRIM_VERSION, "raw_tts_seconds": duration,
              "trim_start_seconds": 0.0, "trim_end_seconds": duration,
              "effective_tts_seconds": duration, "trim_status": "UNAVAILABLE"}
    try:
        result = subprocess.run([
            _binary("ffmpeg"), "-hide_banner", "-i", str(path), "-af",
            "silencedetect=noise=-50dB:d=0.12", "-f", "null", "-",
        ], capture_output=True, text=True, check=False)
        if result.returncode:
            return report
        events = re.findall(r"silence_(start|end):\s*([0-9.]+)", result.stderr or "")
        start, end = 0.0, duration
        if events and events[0][0] == "start" and float(events[0][1]) <= 0.03:
            if len(events) > 1 and events[1][0] == "end":
                start = max(0.0, float(events[1][1]) - 0.06)
        if len(events) >= 2 and events[-1][0] == "end" and float(events[-1][1]) >= duration - 0.10:
            if events[-2][0] == "start":
                end = min(duration, float(events[-2][1]) + 0.06)
        if end - start < 0.15:  # never turn a quiet/ambiguous file into nothing
            return {**report, "trim_status": "AMBIGUOUS_KEEP_FULL"}
        return {**report, "trim_start_seconds": round(start, 4),
                "trim_end_seconds": round(end, 4),
                "effective_tts_seconds": round(end - start, 4), "trim_status": "MEASURED"}
    except (OSError, RuntimeError, ValueError):
        return report


def plan_semantic_audio_starts(durations: list[float], spoken: list[float],
                               opening_delay: float = 0.35) -> list[float]:
    """Bounded placement, not global gap packing or sentence-to-shot locking."""
    total = sum(durations)
    starts, cursor = [], 0.0
    for index, (duration, length) in enumerate(zip(durations, spoken)):
        nominal = cursor + min(opening_delay, max(0.0, duration - length - 0.08))
        lower = max(0.0, cursor - 0.75)
        upper = min(cursor + 0.75, total - length - 0.075)
        if index:
            previous_end = starts[-1] + spoken[index - 1]
            lower = max(lower, previous_end + 0.12)
            candidate = min(nominal, max(lower, previous_end + 0.22))
            # Do not improve an internal gap by creating a worse terminal gap.
            if index == len(spoken) - 1:
                old_gap = max(nominal - previous_end, total - nominal - length)
                candidate = max(candidate, total - length - old_gap)
        else:
            candidate = nominal
        if lower > upper + 0.001:
            raise RuntimeError("口播在有限跨段范围内无法完整容纳；不截断语句")
        starts.append(round(max(lower, min(upper, candidate)), 4))
        cursor += duration
    return starts


def synthesize_segment_preflight(
    sections: Iterable[Mapping[str, Any]],
    segment_plan: Iterable[Mapping[str, Any]],
    output_dir: str | Path,
    *,
    voice_id: str = "th-TH-PremwadeeNeural",
    voiceover_root: str | Path = DEFAULT_VOICEOVER_ROOT,
) -> Dict[str, Any]:
    """Measure the real Edge voice before any paid video submission.

    The output audio is a frozen production asset.  Finalization reuses it
    when the section text hash and voice still match, instead of synthesizing
    the same copy a second time.
    """

    section_rows = [dict(item) for item in sections if isinstance(item, Mapping)]
    segments = [dict(item) for item in segment_plan if isinstance(item, Mapping)]
    if len(section_rows) != len(segments) or len(segments) not in {2, 3}:
        raise ValueError("Edge TTS 预检要求口播语义段与2至3个视频片段一一对应")
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    reports = []
    for index, (section, segment) in enumerate(zip(section_rows, segments), 1):
        segment_id = str(segment.get("segment_id") or chr(64 + index)).upper()
        section_text = str(section.get("target_text") or "").strip()
        if not section_text:
            raise ValueError(f"片段{segment_id}缺少口播文本")
        text_hash = voiceover_text_hash(section_text)
        voice_key = hashlib.sha256(voice_id.encode()).hexdigest()[:8]
        target = root / f"voiceover_{segment_id}_{text_hash[:12]}_{voice_key}_rate_0.mp3"
        if not target.is_file():
            _synthesize_edge(
                section_text, target, voice_id=voice_id, rate_percent=0,
                voiceover_root=Path(voiceover_root).expanduser().resolve(),
            )
        measured = audio_duration_seconds(target)
        window = measure_speech_window(target, measured)
        measured = float(window["effective_tts_seconds"])
        duration = float(segment.get("duration_seconds") or 0)
        ratio = measured / duration if duration else 0.0
        if 0.90 <= ratio <= 0.96:
            status = "TARGET_FIT"
        elif 0.86 <= ratio <= 1.0:
            status = "ACCEPTABLE"
        elif ratio < 0.86:
            status = "TOO_SHORT"
        else:
            status = "TOO_LONG"
        reports.append({
            **window,
            "audio_sha256": audio_asset_hash(target),
            "segment_id": segment_id,
            "text_sha256": text_hash,
            "target_text": section_text,
            "planned_segment_seconds": duration,
            "actual_tts_seconds": round(measured, 3),
            "coverage_ratio": round(ratio, 4),
            "target_coverage_ratio": [0.90, 0.96],
            "acceptable_coverage_ratio": [0.86, 1.0],
            "status": status,
            "rate_percent": 0,
            "audio_path": str(target),
        })
    return {
        "schema_version": "longform-edge-tts-preflight-v2-effective-speech",
        "provider": "edge",
        "voice_id": voice_id,
        "sections": reports,
        "actual_tts_seconds_total": round(sum(item["actual_tts_seconds"] for item in reports), 3),
        "all_target_fit": all(item["status"] == "TARGET_FIT" for item in reports),
        "all_acceptable": all(item["status"] in {"TARGET_FIT", "ACCEPTABLE"} for item in reports),
    }


def _resolve_bgm_path(explicit: str | Path | None = None) -> Path | None:
    configured = str(explicit or os.environ.get("LONGFORM_BGM_PATH") or "").strip()
    if not configured:
        return None
    candidate = Path(configured).expanduser().resolve()
    return candidate if candidate.is_file() else None


def _preflight_audio_for_section(
    voiceover: Mapping[str, Any], section: Mapping[str, Any], segment_id: str,
    *, voice_id: str,
) -> Dict[str, Any] | None:
    preflight = voiceover.get("tts_preflight") or {}
    if str(preflight.get("voice_id") or "") != voice_id:
        return None
    expected_hash = voiceover_text_hash(str(section.get("target_text") or ""))
    for item in preflight.get("sections") or []:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("segment_id") or "").upper() != segment_id.upper():
            continue
        path = Path(str(item.get("audio_path") or ""))
        if (str(item.get("text_sha256") or "") == expected_hash and path.is_file()
                and (not item.get("audio_sha256") or item["audio_sha256"] == audio_asset_hash(path))):
            return {**dict(item), "audio_path": str(path)}
    return None


def finalize_with_voiceover(
    video_path: str | Path,
    voiceover: Dict[str, Any],
    output_path: str | Path,
    *,
    allow_external_tts: bool,
    voice_id: str = "th-TH-PremwadeeNeural",
    opening_delay_ms: int = 350,
    voiceover_root: str | Path = DEFAULT_VOICEOVER_ROOT,
    segment_plan: Iterable[Mapping[str, Any]] | None = None,
    bgm_path: str | Path | None = None,
) -> Dict[str, Any]:
    if not allow_external_tts:
        raise ValueError("最终混音需要显式 --allow-external-tts")
    text = str(voiceover.get("target_text") or "").strip()
    if not text:
        raise ValueError("统一口播缺少 target_text")
    video = Path(video_path).resolve()
    if not video.is_file():
        raise ValueError("待混音长视频不存在")
    output = Path(output_path).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = output.parent / "finalization.json"
    planned_segments = [dict(item) for item in (segment_plan or [])]
    sections = [dict(item) for item in voiceover.get("semantic_sections") or [] if isinstance(item, Mapping)]
    segmented = (
        len(planned_segments) in {2, 3}
        and len(sections) == len(planned_segments)
        and all(str(item.get("target_text") or "").strip() for item in sections)
    )
    resolved_bgm = _resolve_bgm_path(bgm_path)
    hash_material = {
        "layout_version": LAYOUT_VERSION,
        "voice_id": voice_id,
        "opening_delay_ms": opening_delay_ms,
        "preflight_audio": voiceover.get("tts_preflight") or {},
        "text": text,
        "layout": "SEGMENTED" if segmented else "LEGACY_SINGLE",
        "sections": [item.get("target_text") for item in sections] if segmented else [],
        "durations": [item.get("duration_seconds") for item in planned_segments] if segmented else [],
        "bgm_path": str(resolved_bgm) if resolved_bgm else "PLATFORM_BGM",
    }
    text_hash = hashlib.sha256(
        json.dumps(hash_material, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    if manifest_path.is_file() and output.is_file():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("text_sha256") == text_hash and existing.get("final_video_path") == str(output):
            return {**existing, "action": "IDEMPOTENT_REUSE"}

    video_seconds = _video_duration_seconds(video)
    if segmented:
        return _finalize_segmented_voiceover(
            video, sections, planned_segments, output,
            voice_id=voice_id, opening_delay_ms=opening_delay_ms,
            voiceover_root=Path(voiceover_root).expanduser().resolve(),
            text_hash=text_hash, video_seconds=video_seconds,
            voiceover=voiceover, bgm_path=resolved_bgm,
        )
    original_audio = output.parent / "voiceover_th_rate_0.mp3"
    _synthesize_edge(text, original_audio, voice_id=voice_id, rate_percent=0,
                     voiceover_root=Path(voiceover_root).expanduser().resolve())
    original_seconds = audio_duration_seconds(original_audio)
    selected_rate = choose_narration_rate(original_seconds, video_seconds)
    selected_audio = original_audio
    if selected_rate:
        selected_audio = output.parent / f"voiceover_th_rate_{selected_rate:+d}.mp3"
        _synthesize_edge(text, selected_audio, voice_id=voice_id, rate_percent=selected_rate,
                         voiceover_root=Path(voiceover_root).expanduser().resolve())
    selected_seconds = audio_duration_seconds(selected_audio)
    if selected_seconds + opening_delay_ms / 1000.0 > video_seconds - 0.15:
        raise RuntimeError(
            f"TTS 时长仍超出视频: {selected_seconds:.2f}s > {video_seconds:.2f}s"
        )

    temp = output.with_suffix(".tmp.mp4")
    bgm = resolved_bgm
    command = [_binary("ffmpeg"), "-y", "-i", str(video), "-i", str(selected_audio)]
    voice_filter = (
        f"[1:a]loudnorm=I=-16:TP=-1.5:LRA=11,adelay={opening_delay_ms}:all=1,"
        f"apad,atrim=duration={video_seconds:.3f}[voice]"
    )
    filters = [voice_filter]
    if bgm:
        command.extend(["-stream_loop", "-1", "-i", str(bgm)])
        filters.extend([
            f"[2:a]volume=0.09,afade=t=in:st=0:d=0.5,"
            f"afade=t=out:st={max(0.0, video_seconds - 0.8):.3f}:d=0.8,"
            f"atrim=duration={video_seconds:.3f}[bed]",
            "[voice][bed]amix=inputs=2:duration=longest:dropout_transition=2,"
            "alimiter=limit=0.95[a]",
        ])
    else:
        filters.append("[voice]anull[a]")
    command.extend([
        "-filter_complex", ";".join(filters),
        "-map", "0:v:0", "-map", "[a]", "-c:v", "copy", "-c:a", "aac",
        "-b:a", "192k", "-ar", "44100", "-movflags", "+faststart", "-t", f"{video_seconds:.3f}", "-shortest", str(temp),
    ])
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("最终长视频混音失败: " + completed.stderr[-1200:])
    temp.replace(output)
    result = {
        "schema_version": "longform-finalization-v1",
        "action": "FINALIZED",
        "text_sha256": text_hash,
        "provider": "edge",
        "voice_id": voice_id,
        "initial_rate_percent": 0,
        "selected_rate_percent": selected_rate,
        "initial_tts_seconds": round(original_seconds, 3),
        "selected_tts_seconds": round(selected_seconds, 3),
        "video_seconds": round(video_seconds, 3),
        "opening_delay_ms": opening_delay_ms,
        "bgm_policy": "LIGHT_BED_APPLIED" if bgm else "PLATFORM_BGM_EXPECTED",
        "bgm_path": str(bgm) if bgm else "",
        "audio_path": str(selected_audio),
        "final_video_path": str(output),
    }
    manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def _finalize_segmented_voiceover(
    video: Path,
    sections: list[Dict[str, Any]],
    segments: list[Dict[str, Any]],
    output: Path,
    *,
    voice_id: str,
    opening_delay_ms: int,
    voiceover_root: Path,
    text_hash: str,
    video_seconds: float,
    voiceover: Mapping[str, Any],
    bgm_path: Path | None,
) -> Dict[str, Any]:
    """Place one semantic continuation near each H3 segment start.

    This distributes natural breathing space across the work. It deliberately
    does not stretch short speech to fill a segment and does not bind lines to
    individual shots.
    """

    audio_paths: list[Path] = []
    section_reports: list[Dict[str, Any]] = []
    for index, (section, segment) in enumerate(zip(sections, segments), 1):
        segment_id = str(segment.get("segment_id") or chr(64 + index))
        duration = float(segment.get("duration_seconds") or 0)
        section_text = str(section.get("target_text") or "").strip()
        preflight_report = _preflight_audio_for_section(
            voiceover, section, segment_id, voice_id=voice_id,
        )
        original = Path(str(preflight_report.get("audio_path"))) if preflight_report else (
            output.parent / f"voiceover_{segment_id}_{voiceover_text_hash(section_text)[:12]}_rate_0.mp3"
        )
        reused_preflight = preflight_report is not None
        if not reused_preflight:
            _synthesize_edge(
                section_text, original, voice_id=voice_id, rate_percent=0,
                voiceover_root=voiceover_root,
            )
        initial_seconds = audio_duration_seconds(original)
        # Preflight audio is an accepted frozen production asset. Re-running
        # the rate chooser here created a second, contradictory speed choice.
        selected_rate = int(preflight_report.get("rate_percent") or 0) if preflight_report else (
            choose_narration_rate(initial_seconds, duration)
        )
        selected = original
        if selected_rate and not reused_preflight:
            selected = output.parent / f"voiceover_{segment_id}_rate_{selected_rate:+d}.mp3"
            _synthesize_edge(
                section_text, selected, voice_id=voice_id, rate_percent=selected_rate,
                voiceover_root=voiceover_root,
            )
        selected_seconds = audio_duration_seconds(selected)
        window = (
            {key: preflight_report[key] for key in (
                "trim_version", "raw_tts_seconds", "trim_start_seconds", "trim_end_seconds",
                "effective_tts_seconds", "trim_status",
            )}
            if preflight_report and preflight_report.get("trim_version") == EDGE_TRIM_VERSION
            and all(key in preflight_report for key in (
                "raw_tts_seconds", "trim_start_seconds", "trim_end_seconds", "effective_tts_seconds", "trim_status",
            )) else measure_speech_window(selected, selected_seconds)
        )
        selected_seconds = float(window["effective_tts_seconds"])
        effective_delay_ms = min(
            opening_delay_ms,
            max(0, int((duration - 0.08 - selected_seconds) * 1000)),
        )
        if selected_seconds > duration + 0.65:
            raise RuntimeError(
                f"片段{segment_id}口播仍超时: {selected_seconds:.2f}s > {duration:.2f}s"
            )
        audio_paths.append(selected)
        section_reports.append({
            **window,
            "segment_id": segment_id,
            "planned_segment_seconds": duration,
            "initial_tts_seconds": round(initial_seconds, 3),
            "selected_tts_seconds": round(selected_seconds, 3),
            "selected_rate_percent": selected_rate,
            "opening_delay_ms": effective_delay_ms,
            "audio_path": str(selected),
            "preflight_audio_reused": reused_preflight,
            "preflight_text_sha256": (
                str(preflight_report.get("text_sha256") or "") if preflight_report else ""
            ),
        })

    starts = plan_semantic_audio_starts(
        [float(row.get("duration_seconds") or 0) for row in segments],
        [row["selected_tts_seconds"] for row in section_reports], opening_delay_ms / 1000.0,
    )
    cursor = 0.0
    for report, start in zip(section_reports, starts):
        report["timeline_start_seconds"] = start
        report["segment_boundary_offset_seconds"] = round(start - cursor, 4)
        cursor += report["planned_segment_seconds"]
    command = [_binary("ffmpeg"), "-y", "-i", str(video)]
    for path in audio_paths:
        command.extend(["-i", str(path)])
    bgm_input_index = None
    if bgm_path:
        bgm_input_index = len(audio_paths) + 1
        command.extend(["-stream_loop", "-1", "-i", str(bgm_path)])
    filters = []
    labels = []
    for index, segment in enumerate(segments):
        report = section_reports[index]
        section_delay_ms = round(starts[index] * 1000)
        label = f"section_{index}"
        filters.append(
            f"[{index + 1}:a]atrim=start={report['trim_start_seconds']}:end={report['trim_end_seconds']},"
            f"asetpts=PTS-STARTPTS,loudnorm=I=-16:TP=-1.5:LRA=11,"
            f"adelay={section_delay_ms}:all=1[{label}]"
        )
        labels.append(f"[{label}]")
    filters.append(
        "".join(labels)
        + f"amix=inputs={len(labels)}:duration=longest:normalize=0:dropout_transition=0,"
        f"apad,atrim=duration={video_seconds:.3f}[voice]"
    )
    if bgm_input_index is not None:
        filters.extend([
            f"[{bgm_input_index}:a]volume=0.09,afade=t=in:st=0:d=0.5,"
            f"afade=t=out:st={max(0.0, video_seconds - 0.8):.3f}:d=0.8,"
            f"atrim=duration={video_seconds:.3f}[bed]",
            "[voice][bed]amix=inputs=2:duration=longest:dropout_transition=2,"
            "alimiter=limit=0.95[a]",
        ])
    else:
        filters.append("[voice]anull[a]")
    temp = output.with_suffix(".tmp.mp4")
    command.extend([
        "-filter_complex", ";".join(filters),
        "-map", "0:v:0", "-map", "[a]", "-c:v", "copy", "-c:a", "aac",
        "-b:a", "192k", "-ar", "44100", "-movflags", "+faststart", "-t", f"{video_seconds:.3f}", "-shortest", str(temp),
    ])
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("长视频分段口播混音失败: " + completed.stderr[-1200:])
    temp.replace(output)
    result = {
        "schema_version": "longform-finalization-v3-bounded-continuation",
        "action": "FINALIZED",
        "text_sha256": text_hash,
        "provider": "edge",
        "voice_id": voice_id,
        "tts_layout": LAYOUT_VERSION,
        "video_seconds": round(video_seconds, 3),
        "sections": section_reports,
        "selected_tts_seconds_total": round(
            sum(item["selected_tts_seconds"] for item in section_reports), 3
        ),
        "bgm_policy": "LIGHT_BED_APPLIED" if bgm_path else "PLATFORM_BGM_EXPECTED",
        "bgm_path": str(bgm_path) if bgm_path else "",
        "final_video_path": str(output),
    }
    (output.parent / "finalization.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def _video_duration_seconds(path: Path) -> float:
    completed = subprocess.run(
        [_binary("ffprobe"), "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("读取视频时长失败: " + completed.stderr[-800:])
    return float(completed.stdout.strip())
