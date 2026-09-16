"""Reproduce frozen on-screen copy on the merged long-form master.

A remake source may name on-screen copy (``屏幕文字：…`` / ``字幕：…``).  That copy
is deliberately stripped from the video-model prompt and carried in the plan's
``postprocess_contract.subtitles`` instead, so the video model never draws text
and the approved wording survives byte-for-byte.  This module is the consumer of
that contract: it burns the cues on the frozen source timeline.

An original generated job carries no subtitle contract, so every function here
is a no-op for it and original masters are bit-identical to before.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Iterable, Mapping

from .audio import _binary


SUBTITLE_MODE_PRESERVE = "PRESERVE_SOURCE_TIMELINE"
SUBTITLE_SCHEMA_VERSION = "longform-subtitle-burn-v1"

# A remake target language may be Thai, Chinese or English.  The copy is never
# re-translated, so the font must be able to render the frozen script as-is.
FONT_CANDIDATES: dict[str, tuple[str, ...]] = {
    "th": (
        "/System/Library/Fonts/Supplemental/Thonburi.ttc",
        "/System/Library/Fonts/ThonburiUI.ttc",
        "/System/Library/Fonts/Supplemental/Ayuthaya.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    ),
    "zh": (
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/PingFang.ttc",
    ),
    "default": (
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ),
}
THAI_LANGUAGE_TOKENS = {"泰语", "泰文", "thai", "th", "th-th"}
CHINESE_LANGUAGE_TOKENS = {"中文", "汉语", "简体中文", "chinese", "zh", "zh-cn"}

# The frozen master is 9:16 and Plan C renders at 1080x1920.
ASS_PLAY_RES = (1080, 1920)


def _language_bucket(target_language: str) -> str:
    value = str(target_language or "").strip().lower()
    if value in THAI_LANGUAGE_TOKENS or value.startswith("th-"):
        return "th"
    if value in CHINESE_LANGUAGE_TOKENS or value.startswith("zh"):
        return "zh"
    return "default"


def resolve_subtitle_font(
    target_language: str = "", *, override: str | Path | None = None,
) -> Path:
    """Return a font file that can render the frozen copy, or fail loudly.

    Rendering Thai copy with a font that lacks Thai glyphs silently produces
    boxes, which is exactly the kind of text loss the frozen-source contract
    forbids.  A missing font must therefore stop the job instead of degrading.
    """

    candidates: list[str] = []
    if override:
        # An explicitly configured font that does not exist is a configuration
        # error; silently falling back would mask a broken deployment.
        configured = Path(str(override)).expanduser()
        if not configured.is_file():
            raise RuntimeError(f"配置的字幕字体不存在: {configured}")
        return configured
    bucket = _language_bucket(target_language)
    candidates.extend(FONT_CANDIDATES[bucket])
    candidates.extend(FONT_CANDIDATES["default"])
    for candidate in candidates:
        path = Path(candidate).expanduser()
        if path.is_file():
            return path
    raise RuntimeError(
        "找不到可渲染冻结字幕的字体；已尝试: " + "、".join(candidates)
    )


def _font_family(path: Path) -> str:
    """Approximate the family name libass will register for this file."""

    stem = path.stem
    return {
        "Arial Unicode": "Arial Unicode MS",
        "Hiragino Sans GB": "Hiragino Sans GB",
    }.get(stem, stem)


def subtitle_cues(plan: Mapping[str, Any]) -> list[dict]:
    """Read the subtitle contract from a long-form plan.

    Only a frozen remake timeline is ever burned in; anything else (including a
    malformed contract) returns no cues rather than guessing.
    """

    contract = dict((plan or {}).get("postprocess_contract") or {})
    subtitles = dict(contract.get("subtitles") or {})
    if str(subtitles.get("mode") or "").upper() != SUBTITLE_MODE_PRESERVE:
        return []
    cues: list[dict] = []
    for index, item in enumerate(subtitles.get("items") or [], 1):
        if not isinstance(item, Mapping):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        try:
            start_ms = max(0, int(item.get("start_ms") or 0))
            end_ms = int(item.get("end_ms") or 0)
        except (TypeError, ValueError):
            continue
        if end_ms <= start_ms:
            continue
        cues.append({
            "cue_id": str(item.get("cue_id") or f"CUE_{index:02d}"),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "text": text,
            "source_shot_id": str(item.get("source_shot_id") or ""),
        })
    cues.sort(key=lambda cue: (cue["start_ms"], cue["end_ms"]))
    return cues


def _ass_time(milliseconds: int) -> str:
    centiseconds = int(round(max(0, int(milliseconds)) / 10))
    seconds, cs = divmod(centiseconds, 100)
    minutes, sec = divmod(seconds, 60)
    hours, minute = divmod(minutes, 60)
    return f"{hours}:{minute:02d}:{sec:02d}.{cs:02d}"


def _ass_text(text: str) -> str:
    value = str(text or "").replace("\\", "\\\\")
    value = value.replace("{", "\\{").replace("}", "\\}")
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    return value.replace("\n", "\\N")


def build_ass_document(
    cues: Iterable[Mapping[str, Any]], *, font_name: str,
    play_res: tuple[int, int] = ASS_PLAY_RES,
) -> str:
    """Render the frozen cues as an ASS document.

    An ASS file is used instead of ``drawtext`` because frozen copy may contain
    Thai, Chinese, colons, commas and quotes; escaping all of those through a
    filter chain silently mangles text.
    """

    width, height = play_res
    font_size = max(28, int(round(width * 0.052)))
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {width}",
        f"PlayResY: {height}",
        "WrapStyle: 2",
        "ScaledBorderAndShadow: yes",
        "YCbCr Matrix: TV.709",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, "
        "ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, "
        "MarginL, MarginR, MarginV, Encoding",
        f"Style: FrozenCopy,{font_name},{font_size},&H00FFFFFF,&H00FFFFFF,"
        f"&H00202020,&H80000000,0,0,0,0,100,100,0,0,1,3,1,2,"
        f"{int(width * 0.06)},{int(width * 0.06)},{int(height * 0.11)},1",
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]
    for cue in cues:
        lines.append(
            "Dialogue: 0,"
            f"{_ass_time(int(cue['start_ms']))},{_ass_time(int(cue['end_ms']))},"
            f"FrozenCopy,,0,0,0,,{_ass_text(str(cue['text']))}"
        )
    return "\n".join(lines) + "\n"


def _burn_text_hash(
    video_seconds: float, cues: list[dict], font: Path, path: Path,
) -> str:
    return hashlib.sha256(json.dumps({
        "schema_version": SUBTITLE_SCHEMA_VERSION,
        "video_seconds": round(float(video_seconds), 3),
        "font_path": str(font),
        "ass_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "cues": [
            {"text": cue["text"], "start_ms": cue["start_ms"], "end_ms": cue["end_ms"]}
            for cue in cues
        ],
    }, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def burn_subtitles(
    video_path: str | Path,
    cues: list[Mapping[str, Any]],
    output_path: str | Path,
    *,
    target_language: str = "",
    font_path: str | Path | None = None,
) -> dict:
    """Burn the frozen cues into a copy of the merged master (no audio touch)."""

    if not cues:
        raise ValueError("字幕后处理需要至少一条冻结字幕")
    video = Path(video_path).expanduser().resolve()
    if not video.is_file():
        raise ValueError("待加字幕的长视频不存在")
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    font = Path(font_path).expanduser() if font_path else resolve_subtitle_font(
        target_language,
    )
    if not font.is_file():
        raise RuntimeError(f"字幕字体不存在: {font}")

    ordered = sorted(
        ({"start_ms": int(c["start_ms"]), "end_ms": int(c["end_ms"]),
          "text": str(c["text"])} for c in cues),
        key=lambda cue: (cue["start_ms"], cue["end_ms"]),
    )
    ass_path = output.with_suffix(".ass")
    ass_path.write_text(
        build_ass_document(ordered, font_name=_font_family(font)), encoding="utf-8",
    )
    manifest_path = output.parent / "subtitles.json"
    video_seconds = _video_seconds(video)
    text_hash = _burn_text_hash(video_seconds, ordered, font, ass_path)

    if manifest_path.is_file() and output.is_file():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}
        if (
            existing.get("text_sha256") == text_hash
            and existing.get("subtitled_video_path") == str(output)
        ):
            try:
                validation = _validate_captioned(output, video_seconds)
            except RuntimeError:
                pass
            else:
                return {**existing, "action": "IDEMPOTENT_REUSE",
                        "media_validation": validation}

    escaped = str(ass_path).replace("\\", "\\\\").replace("'", "\\'")
    fonts_dir = str(font.parent).replace("\\", "\\\\").replace("'", "\\'")
    temp = output.with_suffix(".tmp.mp4")
    command = [
        _binary("ffmpeg"), "-y", "-i", str(video),
        "-vf", f"ass=filename='{escaped}':fontsdir='{fonts_dir}'",
        "-an", "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(temp),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0 or not temp.is_file():
        temp.unlink(missing_ok=True)
        raise RuntimeError("冻结字幕烧录失败: " + completed.stderr[-1200:])
    try:
        validation = _validate_captioned(temp, video_seconds)
    except Exception:
        temp.unlink(missing_ok=True)
        raise
    temp.replace(output)
    result = {
        "schema_version": SUBTITLE_SCHEMA_VERSION,
        "action": "SUBTITLES_BURNED",
        "text_sha256": text_hash,
        "font_path": str(font),
        "font_family": _font_family(font),
        "ass_path": str(ass_path),
        "cue_count": len(ordered),
        "target_language": str(target_language or ""),
        "video_seconds": round(video_seconds, 3),
        "subtitled_video_path": str(output),
        "media_validation": validation,
    }
    manifest_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return result


def _video_seconds(path: Path) -> float:
    completed = subprocess.run(
        [_binary("ffprobe"), "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("读取待加字幕视频时长失败: " + completed.stderr[-800:])
    return float(completed.stdout.strip())


def _validate_captioned(path: Path, expected_seconds: float) -> dict:
    """The caption pass is an intermediate: video stream required, audio not yet.

    Audio is attached afterwards by ``finalize_with_voiceover`` /
    ``finalize_silent``, so the caption artifact must not be judged by the
    final-master validator.
    """

    completed = subprocess.run(
        [_binary("ffprobe"), "-v", "error", "-show_entries",
         "format=duration:stream=codec_type,width,height", "-of", "json", str(path)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("字幕成片检查失败: " + completed.stderr[-800:])
    try:
        probe = json.loads(completed.stdout or "{}")
        streams = list(probe.get("streams") or [])
        duration = float(dict(probe.get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"字幕成片检查结果不可解析: {exc}") from exc
    stream_types = {str(item.get("codec_type") or "") for item in streams}
    problems: list[str] = []
    if "video" not in stream_types:
        problems.append("缺少视频流")
    if duration <= 1.0:
        problems.append(f"时长异常({duration:.3f}s)")
    if not path.is_file() or path.stat().st_size < 1_024:
        # The intermediate is intentionally video-only and may compress very
        # small (a flat background), so only a corrupt file is rejected here.
        # The full deliverable size rule runs later on the finalized master.
        problems.append("文件异常过小")
    tolerance = max(0.75, float(expected_seconds) * 0.03)
    if abs(duration - float(expected_seconds)) > tolerance:
        problems.append(
            f"时长与原片不一致({duration:.3f}s vs {float(expected_seconds):.3f}s)"
        )
    if problems:
        raise RuntimeError("字幕成片不可用: " + "；".join(problems))
    return {"status": "PASS", "duration_seconds": round(duration, 3),
            "stream_types": sorted(stream_types)}
