"""Local, cached audio-signal analysis for NeoBund BGM candidates.

This module deliberately uses no LLM and no title/artist semantics.  It
downloads the signed preview once per ``music_id`` (when needed), decodes it
with ffmpeg, and derives a small, auditable rhythm feature set from the signal.
The resulting cache is shared by the organic-photo-video and short-video
publisher paths.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable, List

import numpy as np
import requests


ANALYSIS_VERSION = "neobund-bgm-audio-v2"
DEFAULT_CACHE_ROOT = Path("/Users/likeu3/.openclaw/shared/data/neobund_bgm_audio")
MAX_AUDIO_BYTES = 24 * 1024 * 1024
MAX_ANALYZED_CANDIDATES = 8
MAX_ANALYSIS_SECONDS = 45


class BgmAudioError(RuntimeError):
    pass


def enrich_candidates(
    candidates: Iterable[Any], *, cache_root: Path = DEFAULT_CACHE_ROOT,
    limit: int = MAX_ANALYZED_CANDIDATES,
) -> List[Any]:
    """Attach local signal analysis to the first candidates in parallel.

    A failed CDN preview only marks that candidate unavailable for strong-rhythm
    selection.  It never causes a different song to be silently substituted.
    """
    all_candidates = list(candidates)
    selected = all_candidates[: max(0, int(limit))]
    if not selected:
        return []
    with ThreadPoolExecutor(max_workers=min(4, len(selected))) as pool:
        list(pool.map(lambda candidate: enrich_candidate(candidate, cache_root=cache_root), selected))
    return all_candidates


def enrich_candidate(candidate: Any, *, cache_root: Path = DEFAULT_CACHE_ROOT) -> Any:
    raw = getattr(candidate, "raw", None)
    if not isinstance(raw, dict):
        return candidate
    existing = raw.get("audio_analysis")
    if isinstance(existing, dict) and existing.get("analysis_version") == ANALYSIS_VERSION:
        return candidate
    music_id = str(getattr(candidate, "music_id", "") or "").strip()
    url = str(raw.get("play_url") or "").strip()
    if not music_id or not url:
        raw["audio_analysis"] = _unavailable("missing music_id or play_url")
        return candidate
    try:
        cache_root.mkdir(parents=True, exist_ok=True)
        stem = _safe_stem(music_id)
        audio_path = cache_root / f"{stem}.audio"
        meta_path = cache_root / f"{stem}.json"
        cached = _load_cached(meta_path, audio_path)
        if cached is not None:
            raw["audio_analysis"] = cached
            return candidate
        _download_audio(url, audio_path)
        analysis = analyze_audio_file(audio_path)
        analysis["music_id"] = music_id
        analysis["audio_sha256"] = _sha256(audio_path)
        meta_path.write_text(json.dumps(analysis, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        raw["audio_analysis"] = analysis
    except Exception as exc:  # candidate-level failure; selection decides fallback
        raw["audio_analysis"] = _unavailable(str(exc)[:240])
    return candidate


def analyze_audio_file(path: Path) -> dict[str, Any]:
    samples, sample_rate = _decode_mono(path)
    if samples.size < sample_rate:
        raise BgmAudioError("audio preview is shorter than one second")
    return analyze_audio_array(samples, sample_rate)


def video_duration_ms(path: Path) -> int:
    """Read the actual video duration without decoding or changing the file."""
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True, check=False, timeout=15,
    )
    if result.returncode != 0:
        raise BgmAudioError("ffprobe duration probe failed")
    try:
        duration = float(result.stdout.strip())
    except ValueError as exc:
        raise BgmAudioError("ffprobe returned an invalid duration") from exc
    if duration <= 0:
        raise BgmAudioError("video duration must be positive")
    return int(round(duration * 1000))


def analyze_audio_array(samples: np.ndarray, sample_rate: int) -> dict[str, Any]:
    samples = np.asarray(samples, dtype=np.float32)
    if samples.size == 0:
        raise BgmAudioError("empty audio")
    frame = 1024
    hop = 512
    if samples.size < frame * 4:
        raise BgmAudioError("audio preview is too short for beat analysis")
    window = np.hanning(frame).astype(np.float32)
    spectra = []
    energy = []
    for start in range(0, samples.size - frame, hop):
        chunk = samples[start : start + frame] * window
        spectra.append(np.abs(np.fft.rfft(chunk)))
        energy.append(float(np.sqrt(np.mean(np.square(chunk)) + 1e-12)))
    values = np.asarray(spectra, dtype=np.float32)
    onset = np.maximum(values[1:] - values[:-1], 0).sum(axis=1)
    onset = _normalize(onset)
    bpm, beat_confidence = _estimate_bpm(onset, sample_rate, hop)
    beat_times_ms = _detect_beat_times(onset, sample_rate, hop, bpm)
    energy_score = _energy_score(np.asarray(energy, dtype=np.float32))
    onset_clarity = float(np.percentile(onset, 90) - np.percentile(onset, 50)) if onset.size else 0.0
    onset_clarity = max(0.0, min(1.0, onset_clarity))
    rhythm_strength = round(min(1.0, 0.50 * beat_confidence + 0.30 * energy_score + 0.20 * onset_clarity), 4)
    return {
        "analysis_version": ANALYSIS_VERSION,
        "status": "ready",
        "duration_ms": int(samples.size * 1000 / sample_rate),
        "features": {
            "estimated_bpm": bpm,
            "beat_confidence": round(beat_confidence, 4),
            "energy_score": round(energy_score, 4),
            "onset_clarity": round(onset_clarity, 4),
            "rhythm_strength": rhythm_strength,
        },
        # These are candidates, not a claim that the platform starts playback
        # at any particular offset.
        "beat_times_ms": beat_times_ms[:128],
        "strong_rhythm": bool(rhythm_strength >= 0.45 and len(beat_times_ms) >= 3),
    }


def _download_audio(url: str, destination: Path) -> None:
    response = requests.get(url, timeout=20, stream=True)
    response.raise_for_status()
    total = 0
    with destination.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=128 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_AUDIO_BYTES:
                raise BgmAudioError("audio preview exceeds cache size limit")
            handle.write(chunk)
    if total == 0:
        raise BgmAudioError("audio preview download was empty")


def _decode_mono(path: Path) -> tuple[np.ndarray, int]:
    sample_rate = 22050
    result = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(path), "-t", str(MAX_ANALYSIS_SECONDS), "-f", "f32le", "-ac", "1", "-ar", str(sample_rate), "pipe:1"],
        capture_output=True, check=False, timeout=45,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="ignore").strip()
        raise BgmAudioError(f"ffmpeg decode failed: {detail[:160]}")
    samples = np.frombuffer(result.stdout, dtype=np.float32)
    if samples.size == 0:
        raise BgmAudioError("ffmpeg decoded no audio samples")
    return np.nan_to_num(samples, nan=0.0, posinf=0.0, neginf=0.0), sample_rate


def _estimate_bpm(onset: np.ndarray, sample_rate: int, hop: int) -> tuple[int | None, float]:
    if onset.size < 8 or float(onset.max()) <= 0:
        return None, 0.0
    centered = onset - float(onset.mean())
    corr = np.correlate(centered, centered, mode="full")[centered.size - 1 :]
    frame_rate = sample_rate / hop
    options = []
    for bpm in range(70, 181):
        lag = int(round(60.0 * frame_rate / bpm))
        if 1 <= lag < corr.size:
            options.append((float(corr[lag]), bpm))
    if not options or float(corr[0]) <= 0:
        return None, 0.0
    value, bpm = max(options, key=lambda item: item[0])
    return int(bpm), max(0.0, min(1.0, value / float(corr[0])))


def _detect_beat_times(onset: np.ndarray, sample_rate: int, hop: int, bpm: int | None) -> list[int]:
    if onset.size < 3:
        return []
    threshold = max(0.22, float(np.percentile(onset, 70)))
    candidates = [
        index for index in range(1, onset.size - 1)
        if onset[index] >= threshold and onset[index] >= onset[index - 1] and onset[index] > onset[index + 1]
    ]
    min_gap = max(1, int((60.0 * sample_rate / (hop * max(bpm or 120, 70))) * 0.45))
    accepted: list[int] = []
    for index in candidates:
        if not accepted or index - accepted[-1] >= min_gap:
            accepted.append(index)
        elif onset[index] > onset[accepted[-1]]:
            accepted[-1] = index
    return [int((index + 1) * hop * 1000 / sample_rate) for index in accepted]


def _energy_score(values: np.ndarray) -> float:
    if values.size == 0:
        return 0.0
    mean = float(values.mean())
    spread = float(values.std())
    # Smooth saturation avoids treating every normalized streaming preview as
    # maximum energy merely because its master gain is high.
    mean_component = mean / (mean + 0.08)
    spread_component = spread / (spread + 0.05)
    return max(0.0, min(1.0, mean_component * 0.65 + spread_component * 0.35))


def _normalize(values: np.ndarray) -> np.ndarray:
    if values.size == 0:
        return values
    high = float(values.max())
    low = float(values.min())
    return (values - low) / (high - low) if high > low else np.zeros_like(values)


def _load_cached(meta_path: Path, audio_path: Path) -> dict[str, Any] | None:
    if not meta_path.exists() or not audio_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return payload if payload.get("analysis_version") == ANALYSIS_VERSION else None


def _safe_stem(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:120] or "music"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _unavailable(reason: str) -> dict[str, Any]:
    return {"analysis_version": ANALYSIS_VERSION, "status": "unavailable", "reason": reason}
