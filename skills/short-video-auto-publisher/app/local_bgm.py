#!/usr/bin/env python3
"""Licensed local-BGM selection and rendering for CreatOK content videos.

CreatOK Content Posting video does not currently expose a platform music-id
contract.  This module therefore reuses the shared OPV ranking policy, but its
source is restricted to files explicitly marked for commercial use in the
auto_mixcut licence manifest.  The selected track is embedded before the
CreatOK compatibility transcode.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any, Dict, Iterable, List, Mapping, Tuple


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
AUTO_MIXCUT_BGM_ROOT = WORKSPACE_ROOT / "auto_mixcut" / "assets" / "bgm"
DEFAULT_OUTPUT_ROOT = Path(
    "/Users/likeu3/.openclaw/shared/data/short_video_auto_publish_bgm"
)
DEFAULT_ANALYSIS_ROOT = Path(
    "/Users/likeu3/.openclaw/shared/data/local_bgm_audio_analysis"
)
POLICY_VERSION = "creatok-local-bgm-v1"


class LocalBgmError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or ""))[:100] or "bgm"


def _display_metadata(path: Path) -> Tuple[str, str]:
    parts = path.stem.split("__")
    title = parts[0].replace("_", " ").strip().title()
    author = parts[1].replace("_", " ").strip().title() if len(parts) > 1 else ""
    return title, author


def _mood_tags(path: Path) -> Tuple[str, ...]:
    text = path.stem.lower()
    tags: set[str] = set()
    mapping = {
        "city": ("bright", "dance"),
        "cyberpunk": ("dance", "high_energy"),
        "bpm125": ("dance", "high_energy"),
        "plains": ("bright", "dance"),
        "happier": ("bright",),
        "rainbow": ("bright", "soft"),
        "magic": ("soft", "calm"),
        "moonlight": ("soft", "calm"),
    }
    for token, values in mapping.items():
        if token in text:
            tags.update(values)
    return tuple(sorted(tags or {"bright"}))


def _probe_duration_ms(path: Path, ffprobe: str) -> int:
    result = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "json", str(path)],
        capture_output=True, text=True, timeout=30, check=False,
    )
    if result.returncode != 0:
        raise LocalBgmError(f"无法读取 BGM 时长：{path.name}")
    try:
        duration = float((json.loads(result.stdout or "{}").get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LocalBgmError(f"BGM 时长格式无效：{path.name}") from exc
    if duration <= 0:
        raise LocalBgmError(f"BGM 时长无效：{path.name}")
    return int(round(duration * 1000))


def load_licensed_tracks(
    *, bgm_root: Path = AUTO_MIXCUT_BGM_ROOT, ffprobe: str = "ffprobe",
) -> List[Dict[str, Any]]:
    """Return only local files explicitly approved for commercial embedding."""
    ffprobe_bin = shutil.which(ffprobe) or str(Path.home() / ".local" / "bin" / "ffprobe")
    manifest = bgm_root / "LICENSES.csv"
    if not manifest.is_file():
        raise LocalBgmError(f"找不到 BGM 授权清单：{manifest}")
    tracks: List[Dict[str, Any]] = []
    with manifest.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            commercial = str(row.get("commercial_use") or "").strip().lower()
            license_type = str(row.get("license_type") or "").strip()
            relative = str(row.get("file_name") or "").strip()
            if commercial not in {"yes", "true", "1", "是"} or not relative:
                continue
            path = (bgm_root / relative).resolve()
            if not path.is_file() or path.suffix.lower() not in {".mp3", ".wav", ".m4a", ".aac"}:
                continue
            title, author = _display_metadata(path)
            digest = _sha256(path)
            tracks.append({
                "music_id": f"LOCAL_{digest[:16].upper()}",
                "music_title": title,
                "music_author": author,
                "path": str(path),
                "sha256": digest,
                "duration_ms": _probe_duration_ms(path, ffprobe_bin),
                "mood_tags": _mood_tags(path),
                "license_type": license_type,
                "license_source": str(row.get("source") or "").strip(),
                "license_url": str(row.get("track_url") or "").strip(),
            })
    if not tracks:
        raise LocalBgmError("授权清单中没有可商用且本地文件可用的 BGM")
    return tracks


def _analysis_for(path: Path, music_id: str, cache_root: Path) -> Dict[str, Any]:
    from services import bgm_audio

    cache_root.mkdir(parents=True, exist_ok=True)
    digest = _sha256(path)
    cache_path = cache_root / f"{_safe_id(music_id)}_{digest[:16]}.json"
    if cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if cached.get("audio_sha256") == digest and cached.get("status") == "ready":
                return cached
        except (OSError, ValueError, json.JSONDecodeError):
            pass
    analysis = dict(bgm_audio.analyze_audio_file(path))
    analysis["audio_sha256"] = digest
    temporary = cache_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(analysis, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temporary, cache_path)
    return analysis


def select_track(
    *, tracks: Iterable[Mapping[str, Any]], mood_hints: Iterable[str],
    rhythm_preference: str, video_duration_ms: int,
    use_counts: Mapping[str, int], analysis_root: Path = DEFAULT_ANALYSIS_ROOT,
) -> Tuple[Dict[str, Any], float, List[Dict[str, Any]]]:
    """Apply the same duration, dedupe, mood and signal ranking as NeoBund."""
    from services import neobund_music

    candidates = []
    by_id: Dict[str, Dict[str, Any]] = {}
    for rank, item in enumerate(tracks, start=1):
        track = dict(item)
        path = Path(str(track.get("path") or ""))
        if not path.is_file():
            continue
        analysis = _analysis_for(path, str(track["music_id"]), analysis_root)
        candidate = neobund_music.BgmCandidate(
            music_id=str(track["music_id"]),
            title=str(track["music_title"]),
            rank=rank,
            mood_tags=tuple(track.get("mood_tags") or ()),
            duration_ms=int(track.get("duration_ms") or 0),
            # A local licensed library has no defensible platform-hot score.
            hot_score=0.5,
            raw={"author": track.get("music_author", ""), "audio_analysis": analysis},
        )
        candidates.append(candidate)
        by_id[candidate.music_id] = track
    eligible = neobund_music.eligible_candidates(
        candidates, video_duration_ms=video_duration_ms, use_counts=use_counts,
    )
    ranked = neobund_music.select_top(
        eligible,
        mood_hints=mood_hints,
        video_duration_ms=video_duration_ms,
        use_counts=use_counts,
        rhythm_preference=rhythm_preference,
        require_audio_analysis=rhythm_preference == "strong",
        require_usable_duration=True,
    )
    if not ranked:
        detail = "且节奏分析通过" if rhythm_preference == "strong" else ""
        raise LocalBgmError(f"没有时长足够、未超出去重上限{detail}的授权 BGM")
    selected, score = ranked[0]
    track = dict(by_id[selected.music_id])
    track["audio_analysis"] = selected.raw.get("audio_analysis") or {}
    top = [
        {"music_id": candidate.music_id, "title": candidate.title, "score": candidate_score}
        for candidate, candidate_score in ranked
    ]
    return track, score, top


def _source_has_audio(path: Path, ffprobe: str) -> bool:
    result = subprocess.run(
        [ffprobe, "-v", "error", "-select_streams", "a", "-show_entries", "stream=index", "-of", "csv=p=0", str(path)],
        capture_output=True, text=True, timeout=30, check=False,
    )
    return result.returncode == 0 and bool(result.stdout.strip())


def render_mix(
    *, source_path: str, track: Mapping[str, Any], video_duration_ms: int,
    voice_present: bool, output_root: Path = DEFAULT_OUTPUT_ROOT,
    ffmpeg: str = "ffmpeg", ffprobe: str = "ffprobe",
) -> Dict[str, Any]:
    source = Path(source_path).expanduser().resolve()
    music = Path(str(track.get("path") or "")).expanduser().resolve()
    if not source.is_file() or not music.is_file():
        raise LocalBgmError("待混音视频或 BGM 文件不存在")
    ffmpeg_bin = shutil.which(ffmpeg) or "/Users/likeu3/.local/bin/ffmpeg"
    ffprobe_bin = shutil.which(ffprobe) or str(Path(ffmpeg_bin).with_name("ffprobe"))
    source_sha = _sha256(source)
    # This is applied after -16 LUFS normalization.  A silent slideshow needs
    # a clearly audible bed; voice content remains deliberately conservative.
    mix_volume = 0.16 if voice_present else 0.70
    analysis = track.get("audio_analysis") if isinstance(track.get("audio_analysis"), dict) else {}
    beats = [int(value) for value in (analysis.get("beat_times_ms") or []) if int(value) >= 0]
    max_start_ms = max(int(track.get("duration_ms") or 0) - video_duration_ms, 0)
    preferred_ms = min(12_000, max_start_ms)
    start_ms = min(beats, key=lambda value: abs(value - preferred_ms)) if beats else preferred_ms
    start_ms = min(max(start_ms, 0), max_start_ms)
    fingerprint = hashlib.sha256(json.dumps({
        "policy": POLICY_VERSION,
        "source_sha256": source_sha,
        "music_sha256": track.get("sha256"),
        "start_ms": start_ms,
        "volume": mix_volume,
        "voice": voice_present,
    }, sort_keys=True).encode("utf-8")).hexdigest()[:24]
    target_dir = output_root / _safe_id(source.stem) / fingerprint
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{source.stem}_bgm.mp4"
    if target.is_file() and target.stat().st_size > 0:
        verified = subprocess.run(
            [ffmpeg_bin, "-v", "error", "-i", str(target), "-f", "null", "-"],
            capture_output=True, text=True, timeout=120, check=False,
        )
        if verified.returncode == 0 and _source_has_audio(target, ffprobe_bin):
            return {
                "path": str(target), "sha256": _sha256(target), "source_sha256": source_sha,
                "start_ms": start_ms, "mix_volume": mix_volume,
                "source_had_audio": _source_has_audio(source, ffprobe_bin),
            }
    has_audio = _source_has_audio(source, ffprobe_bin)
    duration_sec = max(video_duration_ms, 500) / 1000.0
    fade_out = max(duration_sec - 0.7, 0)
    bgm_filter = (
        f"atrim=0:{duration_sec:.3f},asetpts=PTS-STARTPTS,"
        f"loudnorm=I=-16:TP=-1.5:LRA=11,volume={mix_volume:.3f},"
        f"afade=t=in:st=0:d=0.18,afade=t=out:st={fade_out:.3f}:d=0.7"
    )
    if voice_present and has_audio:
        filter_complex = (
            f"[0:a]aresample=44100,volume=1.0[voice];"
            f"[1:a]{bgm_filter}[bgm];"
            "[voice][bgm]amix=inputs=2:duration=first:normalize=0[outa]"
        )
    else:
        filter_complex = f"[1:a]{bgm_filter}[outa]"
    temporary = target.with_name(f".{target.stem}.{os.getpid()}.tmp.mp4")
    command = [
        ffmpeg_bin, "-y", "-i", str(source), "-stream_loop", "-1",
        "-ss", f"{start_ms / 1000:.3f}", "-i", str(music),
        "-filter_complex", filter_complex, "-map", "0:v:0", "-map", "[outa]",
        "-t", f"{duration_sec:.3f}", "-c:v", "copy", "-c:a", "aac",
        "-ar", "44100", "-ac", "2", "-movflags", "+faststart", str(temporary),
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=300, check=False)
        if result.returncode != 0 or not temporary.is_file() or temporary.stat().st_size <= 0:
            raise LocalBgmError("BGM 混音失败：" + (result.stderr or "")[-600:])
        verify = subprocess.run(
            [ffmpeg_bin, "-v", "error", "-i", str(temporary), "-f", "null", "-"],
            capture_output=True, text=True, timeout=120, check=False,
        )
        if verify.returncode != 0 or not _source_has_audio(temporary, ffprobe_bin):
            raise LocalBgmError("BGM 成片音轨或完整解码校验失败")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "path": str(target), "sha256": _sha256(target), "source_sha256": source_sha,
        "start_ms": start_ms, "mix_volume": mix_volume, "source_had_audio": has_audio,
    }
