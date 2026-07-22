from __future__ import annotations

import asyncio
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

from .database import LightTryonDB
from .utils import normalized_list, stable_hash
from .voiceover_engine_bridge import DEFAULT_VOICEOVER_ENGINE_ROOT


TTS_BRIDGE_VERSION = "voiceover-tts-bridge-v2-continuous"


def synthesize_variant_tts(
    db: LightTryonDB,
    variant_id: str,
    *,
    output_dir: str | Path,
    provider_name: str = "edge",
    voice_id: str = "th-TH-PremwadeeNeural",
    rate_percent: int = -18,
    voiceover_root: str | Path | None = None,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
    provider: Any | None = None,
) -> dict[str, Any]:
    variant = db.get_narrative_variant(variant_id)
    if not variant:
        raise KeyError(f"找不到增强型内容变体: {variant_id}")
    response = variant.get("voiceover_response") or {}
    beats = response.get("beats") or variant.get("beat_plan") or []
    if not beats:
        raise ValueError("必须先完成口播文案，才能生成 TTS")
    target_ms = int(variant.get("target_duration_seconds") or 22) * 1000
    root = Path(voiceover_root or DEFAULT_VOICEOVER_ENGINE_ROOT).expanduser().resolve()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    if provider is None:
        from voiceover_copy_engine.adapters.tts import provider_from_name

        provider = provider_from_name(provider_name)
    from voiceover_copy_engine.adapters.tts import TTSRequest

    output = Path(output_dir).expanduser().resolve() / variant_id
    segment_dir = output / "segments"
    segment_dir.mkdir(parents=True, exist_ok=True)
    if target_ms >= 18_000:
        return _synthesize_continuous_narration(
            db,
            variant,
            beats,
            output=output,
            segment_dir=segment_dir,
            provider=provider,
            provider_name=provider_name,
            voice_id=voice_id,
            rate_percent=rate_percent,
            target_ms=target_ms,
            tts_request_type=TTSRequest,
            ffmpeg_bin=ffmpeg_bin,
            ffprobe_bin=ffprobe_bin,
        )

    blocks = _semantic_blocks(beats)
    selected: list[dict[str, Any]] = []
    for block in blocks:
        digest = stable_hash(variant_id, block["block_id"], voice_id, rate_percent, block["text"], length=14)
        media_path = segment_dir / f"{block['block_id']}_{digest}.mp3"
        trimmed_path = segment_dir / f"{block['block_id']}_{digest}_trimmed.wav"
        if not media_path.is_file() or media_path.stat().st_size == 0:
            asyncio.run(provider.synthesize(TTSRequest(
                text=block["text"],
                locale="th-TH",
                voice_id=voice_id,
                output_path=media_path,
                rate_percent=int(rate_percent),
            )))
        _trim_audio(media_path, trimmed_path, ffmpeg_bin=ffmpeg_bin)
        duration_ms = _audio_duration_ms(trimmed_path, ffprobe_bin=ffprobe_bin)
        selected.append({
            **block,
            "media_path": str(media_path),
            "trimmed_audio_path": str(trimmed_path),
            "duration_ms": duration_ms,
        })
    opening_ms = 300 if target_ms <= 10_000 else 600
    ending_ms = 700 if target_ms <= 10_000 else 1000
    pause_ms = 240
    max_interblock_gap_ms = 900 if target_ms >= 18_000 else None
    dense_required_ms = opening_ms + ending_ms + sum(row["duration_ms"] for row in selected) + pause_ms * max(0, len(selected) - 1)
    if dense_required_ms > target_ms:
        db.update_narrative_variant(
            variant_id,
            workflow_state="tts_duration_revision_required",
            last_error=f"TTS_DURATION_EXCEEDED:{dense_required_ms}>{target_ms}",
        )
        raise ValueError(f"真实 TTS 时长超出视频：需要 {dense_required_ms}ms，视频只有 {target_ms}ms；必须回到现有口播流程精简")
    cursor = opening_ms
    placements: list[dict[str, Any]] = []
    for index, item in enumerate(selected):
        preferred_start = int(item.get("suggested_start_ms") or 0)
        start_ms = max(cursor, preferred_start)
        if index > 0 and max_interblock_gap_ms is not None:
            start_ms = min(start_ms, placements[-1]["end_ms"] + max_interblock_gap_ms)
        end_ms = start_ms + int(item["duration_ms"])
        placements.append({
            "block_id": item["block_id"],
            "beat_ids": item["beat_ids"],
            "speech_text": item["text"],
            "start_ms": start_ms,
            "end_ms": end_ms,
            "duration_ms": item["duration_ms"],
            "audio_path": item["trimmed_audio_path"],
        })
        cursor = end_ms + (pause_ms if index < len(selected) - 1 else 0)
    minimum_tail_ms = 300 if target_ms <= 10_000 else 400
    scheduled_required_ms = placements[-1]["end_ms"] + minimum_tail_ms
    if scheduled_required_ms > target_ms:
        db.update_narrative_variant(
            variant_id,
            workflow_state="tts_duration_revision_required",
            last_error=f"TTS_SCHEDULE_EXCEEDED:{scheduled_required_ms}>{target_ms}",
        )
        raise ValueError(
            f"真实 TTS 按画面对齐后超出视频：需要 {scheduled_required_ms}ms，视频只有 {target_ms}ms；必须回到现有口播流程调整"
        )
    voice_track = output / f"{variant_id}_voice_track.m4a"
    _compose_voice_track(
        placements,
        voice_track,
        target_ms=target_ms,
        ffmpeg_bin=ffmpeg_bin,
    )
    result = {
        "tts_bridge_version": TTS_BRIDGE_VERSION,
        "provider": getattr(provider, "provider_name", provider_name),
        "voice_id": voice_id,
        "rate_percent": int(rate_percent),
        "target_duration_ms": target_ms,
        "required_duration_ms": dense_required_ms,
        "scheduled_required_duration_ms": scheduled_required_ms,
        "opening_silence_ms": opening_ms,
        "ending_silence_ms": target_ms - placements[-1]["end_ms"],
        "pause_ms": pause_ms,
        "max_interblock_gap_ms": max_interblock_gap_ms,
        "voice_track_path": str(voice_track),
        "voice_track_duration_ms": _audio_duration_ms(voice_track, ffprobe_bin=ffprobe_bin),
        "placements": placements,
    }
    assembly = {**(variant.get("assembly_plan") or {}), "tts": result}
    db.update_narrative_variant(
        variant_id,
        workflow_state="matching_assets",
        tts_timeline=placements,
        assembly_plan=assembly,
        last_error="",
    )
    return {"variant_id": variant_id, "status": "ready_for_asset_match", **result}


def _synthesize_continuous_narration(
    db: LightTryonDB,
    variant: dict[str, Any],
    beats: list[dict[str, Any]],
    *,
    output: Path,
    segment_dir: Path,
    provider: Any,
    provider_name: str,
    voice_id: str,
    rate_percent: int,
    target_ms: int,
    tts_request_type: Any,
    ffmpeg_bin: str,
    ffprobe_bin: str,
) -> dict[str, Any]:
    variant_id = str(variant.get("variant_id") or "")
    narration_text, spans = _narration_text_and_spans(beats)
    digest = stable_hash(
        variant_id, TTS_BRIDGE_VERSION, "continuous", voice_id, rate_percent, narration_text, length=14
    )
    media_path = segment_dir / f"FULL_{digest}.mp3"
    boundary_path = segment_dir / f"FULL_{digest}_boundaries.json"
    boundaries: list[dict[str, Any]] = []
    if not media_path.is_file() or media_path.stat().st_size == 0:
        synth_result = asyncio.run(provider.synthesize(tts_request_type(
            text=narration_text,
            locale="th-TH",
            voice_id=voice_id,
            output_path=media_path,
            rate_percent=int(rate_percent),
        )))
        boundaries = [dict(row) for row in (getattr(synth_result, "word_boundaries", ()) or ())]
        boundary_path.write_text(
            json.dumps(boundaries, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    elif boundary_path.is_file():
        try:
            loaded = json.loads(boundary_path.read_text(encoding="utf-8"))
            boundaries = [dict(row) for row in loaded if isinstance(row, dict)] if isinstance(loaded, list) else []
        except (OSError, json.JSONDecodeError):
            boundaries = []
    duration_ms = _audio_duration_ms(media_path, ffprobe_bin=ffprobe_bin)
    opening_ms = 350
    ending_ms = 900
    required_ms = opening_ms + duration_ms + ending_ms
    if required_ms > target_ms:
        db.update_narrative_variant(
            variant_id,
            workflow_state="tts_duration_revision_required",
            last_error=f"CONTINUOUS_TTS_DURATION_EXCEEDED:{required_ms}>{target_ms}",
        )
        raise ValueError(
            f"完整口播真实时长超出视频：需要 {required_ms}ms，视频只有 {target_ms}ms；必须回到口播流程精简"
        )
    line_timeline, alignment_method = _align_beats_to_narration(
        beats,
        narration_text,
        spans,
        boundaries,
        opening_ms=opening_ms,
        audio_duration_ms=duration_ms,
        audio_path=str(media_path),
    )
    voice_track = output / f"{variant_id}_voice_track.m4a"
    _compose_voice_track(
        [{"audio_path": str(media_path), "start_ms": opening_ms}],
        voice_track,
        target_ms=target_ms,
        ffmpeg_bin=ffmpeg_bin,
    )
    spoken_start_ms = min(int(row["start_ms"]) for row in line_timeline)
    spoken_end_ms = max(int(row["end_ms"]) for row in line_timeline)
    result = {
        "tts_bridge_version": TTS_BRIDGE_VERSION,
        "narration_mode": "single_continuous_request",
        "provider": getattr(provider, "provider_name", provider_name),
        "voice_id": voice_id,
        "rate_percent": int(rate_percent),
        "target_duration_ms": target_ms,
        "required_duration_ms": required_ms,
        "scheduled_required_duration_ms": opening_ms + duration_ms,
        "opening_silence_ms": spoken_start_ms,
        "ending_silence_ms": target_ms - spoken_end_ms,
        "pause_ms": 0,
        "max_interblock_gap_ms": 0,
        "alignment_method": alignment_method,
        "boundary_count": len(boundaries),
        "narration_audio_path": str(media_path),
        "narration_audio_duration_ms": duration_ms,
        "voice_track_path": str(voice_track),
        "voice_track_duration_ms": _audio_duration_ms(voice_track, ffprobe_bin=ffprobe_bin),
        "placements": line_timeline,
    }
    assembly = {**(variant.get("assembly_plan") or {}), "tts": result}
    db.update_narrative_variant(
        variant_id,
        workflow_state="matching_assets",
        tts_timeline=line_timeline,
        assembly_plan=assembly,
        last_error="",
    )
    return {"variant_id": variant_id, "status": "ready_for_asset_match", **result}


def _narration_text_and_spans(
    beats: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    chunks: list[str] = []
    spans: list[dict[str, Any]] = []
    cursor = 0
    for index, beat in enumerate(beats, start=1):
        text = str(beat.get("speech_text") or "").strip()
        if not text:
            continue
        if chunks:
            chunks.append(" ")
            cursor += 1
        start = cursor
        chunks.append(text)
        cursor += len(text)
        spans.append({
            "beat_id": str(beat.get("beat_id") or f"B{index}"),
            "text": text,
            "start_char": start,
            "end_char": cursor,
        })
    narration = "".join(chunks)
    if not narration:
        raise ValueError("Beat 中没有可生成连续口播的文本")
    return narration, spans


def _align_beats_to_narration(
    beats: list[dict[str, Any]],
    narration_text: str,
    spans: list[dict[str, Any]],
    boundaries: list[dict[str, Any]],
    *,
    opening_ms: int,
    audio_duration_ms: int,
    audio_path: str,
) -> tuple[list[dict[str, Any]], str]:
    located: list[dict[str, Any]] = []
    search_cursor = 0
    for row in boundaries:
        word = str(row.get("text") or "")
        if not word:
            continue
        position = narration_text.find(word, search_cursor)
        if position < 0:
            position = narration_text.find(word)
        if position < 0:
            continue
        located.append({**row, "char_start": position, "char_end": position + len(word)})
        search_cursor = position + len(word)
    line_timeline: list[dict[str, Any]] = []
    if located:
        for index, span in enumerate(spans):
            hits = [
                row for row in located
                if int(row["char_start"]) < int(span["end_char"])
                and int(row["char_end"]) > int(span["start_char"])
            ]
            if not hits:
                break
            start_ms = opening_ms + round(float(hits[0].get("offset_ms") or 0))
            end_ms = opening_ms + round(
                float(hits[-1].get("offset_ms") or 0)
                + float(hits[-1].get("duration_ms") or 0)
            )
            beat = next(
                row for row in beats
                if str(row.get("beat_id") or "") == str(span["beat_id"])
            )
            line_timeline.append(_aligned_line(beat, start_ms, end_ms, audio_path))
        if len(line_timeline) == len(spans):
            return line_timeline, "provider_word_boundaries"
    weights = [max(1, len("".join(str(span["text"]).split()))) for span in spans]
    total = sum(weights)
    cursor = 0
    line_timeline = []
    for span, weight in zip(spans, weights):
        start_ms = opening_ms + round(audio_duration_ms * cursor / total)
        cursor += weight
        end_ms = opening_ms + round(audio_duration_ms * cursor / total)
        beat = next(
            row for row in beats
            if str(row.get("beat_id") or "") == str(span["beat_id"])
        )
        line_timeline.append(_aligned_line(beat, start_ms, end_ms, audio_path))
    return line_timeline, "duration_weighted_fallback"


def _aligned_line(
    beat: dict[str, Any], start_ms: int, end_ms: int, audio_path: str
) -> dict[str, Any]:
    return {
        "block_id": str(beat.get("beat_id") or ""),
        "beat_ids": [str(beat.get("beat_id") or "")],
        "speech_text": str(beat.get("speech_text") or ""),
        "chinese_translation": str(beat.get("chinese_translation") or ""),
        "role": str(beat.get("role") or "proof"),
        "required_evidence": normalized_list(beat.get("required_evidence")),
        "required_shot_roles": normalized_list(beat.get("required_shot_roles")),
        "start_ms": int(start_ms),
        "end_ms": max(int(start_ms) + 1, int(end_ms)),
        "duration_ms": max(1, int(end_ms) - int(start_ms)),
        "audio_path": audio_path,
    }


def _semantic_blocks(beats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for row in beats if str(row.get("speech_text") or "").strip()]
    if not rows:
        raise ValueError("Beat 中没有可生成语音的文本")
    block_count = min(3, max(1, math.ceil(len(rows) / 2)))
    chunk_size = math.ceil(len(rows) / block_count)
    blocks: list[dict[str, Any]] = []
    for index in range(0, len(rows), chunk_size):
        chunk = rows[index:index + chunk_size]
        blocks.append({
            "block_id": f"TB{len(blocks) + 1}",
            "beat_ids": [str(row.get("beat_id") or f"B{index + offset + 1}") for offset, row in enumerate(chunk)],
            "text": " ".join(str(row.get("speech_text") or "").strip() for row in chunk),
            "suggested_start_ms": min(
                (int(row.get("suggested_start_ms") or 0) for row in chunk),
                default=0,
            ),
            "suggested_end_ms": max(
                (int(row.get("suggested_end_ms") or 0) for row in chunk),
                default=0,
            ),
        })
    return blocks


def _trim_audio(source: Path, target: Path, *, ffmpeg_bin: str) -> None:
    command = [
        ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error", "-i", str(source),
        "-af",
        "silenceremove=start_periods=1:start_duration=0.03:start_threshold=-45dB,"
        "areverse,silenceremove=start_periods=1:start_duration=0.08:start_threshold=-45dB,areverse,"
        "afade=t=in:st=0:d=0.012",
        "-ar", "24000", "-ac", "1", "-c:a", "pcm_s16le", str(target),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=180, check=False)
    if completed.returncode != 0 or not target.is_file() or target.stat().st_size <= 44:
        raise RuntimeError(f"TTS 静音裁切失败: {completed.stderr[-1500:]}")


def _audio_duration_ms(path: Path, *, ffprobe_bin: str) -> int:
    completed = subprocess.run(
        [ffprobe_bin, "-v", "error", "-show_entries", "format=duration", "-of", "json", str(path)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"读取 TTS 时长失败: {completed.stderr[-1000:]}")
    payload = json.loads(completed.stdout or "{}")
    duration = float((payload.get("format") or {}).get("duration") or 0)
    if duration <= 0:
        raise ValueError("TTS 音频时长无效")
    return round(duration * 1000)


def _compose_voice_track(
    placements: list[dict[str, Any]],
    output: Path,
    *,
    target_ms: int,
    ffmpeg_bin: str,
) -> None:
    command: list[str] = [ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error"]
    for item in placements:
        command.extend(["-i", str(item["audio_path"])])
    filters = []
    labels = []
    for index, item in enumerate(placements):
        label = f"a{index}"
        filters.append(f"[{index}:a]adelay={int(item['start_ms'])}:all=1[{label}]")
        labels.append(f"[{label}]")
    filters.append(
        "".join(labels)
        + f"amix=inputs={len(labels)}:duration=longest:normalize=0,apad,atrim=0:{target_ms / 1000:.3f}[outa]"
    )
    command.extend([
        "-filter_complex", ";".join(filters),
        "-map", "[outa]",
        "-c:a", "aac", "-b:a", "160k", str(output),
    ])
    completed = subprocess.run(command, capture_output=True, text=True, timeout=300, check=False)
    if completed.returncode != 0 or not output.is_file() or output.stat().st_size <= 0:
        raise RuntimeError(f"合成口播音轨失败: {completed.stderr[-1500:]}")
