from __future__ import annotations

import json
import math
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext


class BgmAudioAnalysisSkill:
    """Audio-only BGM analysis.

    This layer intentionally ignores track names, artists, source URLs, and file names.
    Labels are inferred from decoded audio signal features only.
    """

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def analyze_track(self, bgm_id: str, apply_tags: bool = True) -> Result:
        track = self.ctx.repo.get("bgm_tracks", "bgm_id", bgm_id)
        if not track:
            return Result.fail("BGM_NOT_FOUND", "BGM track not found", {"bgm_id": bgm_id})
        local = self._ensure_local_audio(track)
        if not local.success:
            return local
        audio_path = Path(local.data["audio_file_path"])
        decoded = _decode_audio(audio_path)
        if not decoded.success:
            return decoded
        analysis = analyze_audio_array(decoded.data["samples"], decoded.data["sample_rate"])
        updates = {
            "audio_analysis_json": analysis,
            "audio_analyzed_at": datetime.utcnow().isoformat(timespec="seconds"),
            "audio_tag_source": "audio_only_signal_analysis",
            "audio_tag_confidence": analysis["tag_confidence"],
            "duration_ms": analysis.get("duration_ms") or track.get("duration_ms"),
        }
        if apply_tags:
            tags = analysis["audio_suggested_tags"]
            updates.update(
                {
                    "mood_tags_json": json.dumps(tags["mood_tags"], ensure_ascii=False),
                    "energy_level": tags["energy_level"],
                    "vocal_type": tags["vocal_type"],
                    "tag_confidence": analysis["tag_confidence"],
                    "recommended_start_sec": analysis["mix_suggestions"]["recommended_start_sec"],
                    "default_volume": analysis["mix_suggestions"]["default_volume"],
                    "fade_in_ms": analysis["mix_suggestions"]["fade_in_ms"],
                    "fade_out_ms": analysis["mix_suggestions"]["fade_out_ms"],
                    "suitable_for_intro": 1 if analysis["mix_suggestions"]["suitable_for_intro"] else 0,
                    "loop_friendly": 1 if analysis["mix_suggestions"]["loop_friendly"] else 0,
                    "voiceover_friendly": 1 if analysis["mix_suggestions"]["voiceover_friendly"] else 0,
                }
            )
        write = self.ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, updates)
        if not write.success:
            return write
        return Result.ok({"bgm_id": bgm_id, "audio_file_path": str(audio_path), "analysis": analysis, "applied": apply_tags})

    def analyze_all(self, limit: int | None = None, only_missing: bool = False, apply_tags: bool = True) -> Result:
        where = "COALESCE(local_file_path,'')!='' OR COALESCE(oss_object_id,'')!=''"
        tracks = self.ctx.repo.list_where("bgm_tracks", where)
        if only_missing:
            tracks = [t for t in tracks if not t.get("audio_analysis_json")]
        if limit:
            tracks = tracks[:limit]
        results = []
        for track in tracks:
            res = self.analyze_track(track["bgm_id"], apply_tags=apply_tags)
            results.append(res.to_dict())
        return Result.ok({"count": len(results), "results": results})

    def _ensure_local_audio(self, track: dict[str, Any]) -> Result:
        raw_path = str(track.get("local_file_path") or "").strip()
        path = Path(raw_path) if raw_path else None
        if path and path.exists():
            return Result.ok({"audio_file_path": str(path), "cached": False})
        oss_path = require_oss_object_path(self.ctx, track.get("oss_object_id"), "bgm_audio_analysis")
        if oss_path and oss_path.exists():
            self.ctx.repo.update("bgm_tracks", "bgm_id", track["bgm_id"], {"local_file_path": str(oss_path)})
            return Result.ok({"audio_file_path": str(oss_path), "cached": True})
        return Result.fail("BGM_AUDIO_NOT_LOCAL", "BGM has no local audio path or OSS object", {"bgm_id": track.get("bgm_id")})


def analyze_audio_array(samples: np.ndarray, sample_rate: int) -> dict[str, Any]:
    samples = np.asarray(samples, dtype=np.float32)
    if samples.size == 0:
        raise ValueError("empty audio")
    duration_sec = float(samples.size / sample_rate)
    rms = _rms(samples)
    peak = float(np.max(np.abs(samples)))
    frame_rms = _frame_rms(samples, sample_rate)
    onset = _onset_envelope(samples, sample_rate)
    bpm, beat_confidence = _estimate_bpm(onset, sample_rate)
    centroid = _spectral_centroid(samples, sample_rate)
    brightness = min(1.0, centroid / 5000.0)
    vocal_score = _vocal_presence_score(samples, sample_rate)
    energy_score = _energy_score(rms, frame_rms, onset)
    energy_level = "high" if energy_score >= 0.68 else "medium" if energy_score >= 0.36 else "low"
    mood_tags = _mood_tags(energy_level, bpm, brightness, vocal_score)
    vocal_type = "vocal" if vocal_score >= 0.64 else "light_vocal" if vocal_score >= 0.46 else "instrumental"
    if beat_confidence < 0.08 and vocal_score < 0.38:
        vocal_type = "unknown"
    confidence = _tag_confidence(duration_sec, beat_confidence, peak)
    start_sec = _recommended_start_sec(frame_rms, sample_rate)
    default_volume = 0.18 if energy_level == "high" else 0.22 if energy_level == "medium" else 0.26
    loop_friendly = bool(beat_confidence >= 0.18 and energy_level in {"medium", "high"})
    voiceover_friendly = bool(vocal_type in {"instrumental", "unknown"} and energy_level != "high")
    return {
        "source": "audio_only_signal_analysis",
        "duration_ms": int(duration_sec * 1000),
        "sample_rate": sample_rate,
        "features": {
            "rms": round(rms, 5),
            "peak": round(peak, 5),
            "energy_score": round(energy_score, 4),
            "estimated_bpm": bpm,
            "beat_confidence": round(beat_confidence, 4),
            "spectral_centroid_hz": round(float(centroid), 2),
            "brightness_score": round(float(brightness), 4),
            "vocal_presence_score": round(float(vocal_score), 4),
        },
        "audio_suggested_tags": {
            "mood_tags": mood_tags,
            "energy_level": energy_level,
            "vocal_type": vocal_type,
        },
        "mix_suggestions": {
            "recommended_start_sec": start_sec,
            "default_volume": default_volume,
            "fade_in_ms": 500,
            "fade_out_ms": 800,
            "suitable_for_intro": energy_level in {"medium", "high"},
            "loop_friendly": loop_friendly,
            "voiceover_friendly": voiceover_friendly,
        },
        "tag_confidence": confidence,
        "reason": "基于音频波形、频谱、节拍包络和能量分布生成；未使用曲名、artist、链接或文件名。",
    }


def _decode_audio(path: Path, sample_rate: int = 22050, max_seconds: int = 120) -> Result:
    cmd = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        str(path),
        "-t",
        str(max_seconds),
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        "-f",
        "f32le",
        "-",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        return Result.fail("BGM_AUDIO_DECODE_FAILED", proc.stderr.decode("utf-8", errors="ignore"), {"path": str(path)})
    samples = np.frombuffer(proc.stdout, dtype=np.float32)
    if samples.size == 0:
        return Result.fail("BGM_AUDIO_EMPTY", "decoded audio is empty", {"path": str(path)})
    samples = np.nan_to_num(samples, nan=0.0, posinf=0.0, neginf=0.0)
    samples = np.clip(samples, -1.0, 1.0)
    return Result.ok({"samples": samples, "sample_rate": sample_rate})


def _rms(samples: np.ndarray) -> float:
    return float(np.sqrt(np.mean(np.square(samples)) + 1e-12))


def _frame_rms(samples: np.ndarray, sample_rate: int, frame_ms: int = 500) -> np.ndarray:
    frame = max(1, int(sample_rate * frame_ms / 1000))
    count = max(1, math.ceil(samples.size / frame))
    values = []
    for idx in range(count):
        chunk = samples[idx * frame : (idx + 1) * frame]
        if chunk.size:
            values.append(_rms(chunk))
    return np.asarray(values, dtype=np.float32)


def _onset_envelope(samples: np.ndarray, sample_rate: int) -> np.ndarray:
    frame = 1024
    hop = 512
    if samples.size < frame * 4:
        return np.zeros(1, dtype=np.float32)
    window = np.hanning(frame).astype(np.float32)
    prev = None
    flux = []
    for start in range(0, samples.size - frame, hop):
        spectrum = np.abs(np.fft.rfft(samples[start : start + frame] * window))
        if prev is not None:
            flux.append(float(np.maximum(spectrum - prev, 0).sum()))
        prev = spectrum
    env = np.asarray(flux, dtype=np.float32)
    if env.size == 0:
        return np.zeros(1, dtype=np.float32)
    env -= float(env.min())
    peak = float(env.max())
    return env / peak if peak > 0 else env


def _estimate_bpm(onset: np.ndarray, sample_rate: int, hop: int = 512) -> tuple[int | None, float]:
    if onset.size < 8 or float(onset.max()) <= 0:
        return None, 0.0
    onset = onset - float(onset.mean())
    corr = np.correlate(onset, onset, mode="full")[onset.size - 1 :]
    frame_rate = sample_rate / hop
    candidates = []
    for bpm in range(70, 181):
        lag = int(round((60.0 / bpm) * frame_rate))
        if 1 <= lag < corr.size:
            candidates.append((float(corr[lag]), bpm))
    if not candidates:
        return None, 0.0
    best_value, best_bpm = max(candidates, key=lambda item: item[0])
    confidence = max(0.0, best_value / (float(corr[0]) + 1e-9))
    return int(best_bpm), float(min(1.0, confidence))


def _spectral_centroid(samples: np.ndarray, sample_rate: int) -> float:
    n = min(samples.size, sample_rate * 45)
    chunk = samples[:n]
    frame = 2048
    hop = 2048
    if chunk.size < frame:
        return 0.0
    freqs = np.fft.rfftfreq(frame, d=1.0 / sample_rate)
    values = []
    window = np.hanning(frame).astype(np.float32)
    for start in range(0, chunk.size - frame, hop):
        mag = np.abs(np.fft.rfft(chunk[start : start + frame] * window)) + 1e-9
        values.append(float((freqs * mag).sum() / mag.sum()))
    return float(np.median(values)) if values else 0.0


def _vocal_presence_score(samples: np.ndarray, sample_rate: int) -> float:
    n = min(samples.size, sample_rate * 60)
    chunk = samples[:n]
    frame = 2048
    hop = 2048
    if chunk.size < frame:
        return 0.0
    freqs = np.fft.rfftfreq(frame, d=1.0 / sample_rate)
    vocal_band = (freqs >= 300) & (freqs <= 3400)
    low_band = (freqs >= 60) & (freqs < 300)
    high_band = (freqs > 3400) & (freqs <= 9000)
    ratios = []
    window = np.hanning(frame).astype(np.float32)
    for start in range(0, chunk.size - frame, hop):
        power = np.square(np.abs(np.fft.rfft(chunk[start : start + frame] * window))) + 1e-12
        total = float(power[vocal_band | low_band | high_band].sum())
        if total > 0:
            ratios.append(float(power[vocal_band].sum() / total))
    if not ratios:
        return 0.0
    return float(min(1.0, max(0.0, (np.median(ratios) - 0.35) / 0.45)))


def _energy_score(rms: float, frame_rms: np.ndarray, onset: np.ndarray) -> float:
    loudness = min(1.0, rms / 0.18)
    movement = float(np.percentile(frame_rms, 90) - np.percentile(frame_rms, 20)) if frame_rms.size else 0.0
    onset_strength = float(np.percentile(onset, 90)) if onset.size else 0.0
    return float(min(1.0, loudness * 0.45 + min(1.0, movement / 0.12) * 0.25 + onset_strength * 0.30))


def _mood_tags(energy_level: str, bpm: int | None, brightness: float, vocal_score: float) -> list[str]:
    tags: list[str] = []
    if energy_level == "high" or (bpm and bpm >= 130):
        tags.append("energetic")
    if brightness >= 0.45:
        tags.append("fresh_summer")
    if energy_level == "low":
        tags.extend(["calm_lifestyle", "minimal_clean"])
    if vocal_score >= 0.55:
        tags.append("fashion_chic")
    if not tags:
        tags.append("daily_clean")
    if "daily_clean" not in tags and energy_level == "medium":
        tags.append("daily_clean")
    return list(dict.fromkeys(tags))[:3]


def _recommended_start_sec(frame_rms: np.ndarray, sample_rate: int) -> int:
    if frame_rms.size == 0:
        return 0
    threshold = max(float(np.percentile(frame_rms, 60)), 0.02)
    for idx, value in enumerate(frame_rms[:60]):
        if float(value) >= threshold:
            return int(idx * 0.5)
    return 0


def _tag_confidence(duration_sec: float, beat_confidence: float, peak: float) -> str:
    if duration_sec < 8 or peak < 0.01:
        return "low"
    if beat_confidence >= 0.18:
        return "high"
    if beat_confidence >= 0.08:
        return "medium"
    return "low"
