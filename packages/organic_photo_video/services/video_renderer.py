"""FFmpeg still-image slideshow renderer (Stage E).

Renders the approved 5-shot group into the 12.5s vertical master defined by
the render preset: 1080x1920, 30fps, H.264/yuv420p, light motion per slot
(slow_push / light_pan / detail_zoom / upper_body_focus / static_hold), short dissolves between
shots, no audio stream (dual-audio impossible; silent-AAC vs silent master is
pending the NeoBund upload test, see MODEL_HANDOFF section 15).

Timeline math: with transition t between shots, each clip is extended by
(t_prev + t_next) / 2 so the final duration stays equal to the plan total
(e.g. 12500ms) instead of shrinking by the dissolve overlap.

Text overlays are optional and rendered from an explicit approved overlay
profile. No local BGM is ever burned (audio_policy platform_hot_bgm).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

OUTPUT_WIDTH = 1080
OUTPUT_HEIGHT = 1920
FPS = 30
DISSOLVE_MS = 300
SUPER_SCALE = 2  # zoompan works on a 2x upscaled canvas to reduce jitter
OVERLAY_HEADER_HEIGHT = 240

MOTION_EXPRESSIONS = {
    "slow_push": ("1+0.065*on/{n}", "(iw-iw/zoom)/2", "(ih-ih/zoom)/2"),
    "detail_zoom": ("1+0.08*on/{n}", "(iw-iw/zoom)/2", "(ih-ih/zoom)/2"),
    # Fallback for a detail slot without a true detail reference. It only
    # reframes the existing generated image and therefore cannot invent
    # product structures. The upward bias keeps the garment/waist in frame.
    "upper_body_focus": (
        "1.28+0.07*on/{n}",
        "(iw-iw/zoom)/2",
        "(ih-ih/zoom)*0.30",
    ),
    "light_pan": ("1.055", "(iw-iw/zoom)*(on/{n})", "(ih-ih/zoom)/2"),
    "static_hold": ("1.0", "(iw-iw/zoom)/2", "(ih-ih/zoom)/2"),
}


class RenderError(RuntimeError):
    pass


@dataclass
class TimelineSlot:
    slot_index: int
    shot_id: str
    shot_version: int
    image_path: str
    motion_preset: str
    transition_out: str
    planned_ms: int
    effective_ms: int
    fit_mode: str = "cover"
    start_ms: int = 0
    overlay_text: str = ""
    overlay_spec: Optional[Dict[str, Any]] = None
    # Preview-only marker.  It is deliberately part of the pixels rather than
    # merely sidecar metadata, so a review copy cannot be mistaken for a
    # publishable master if it is forwarded outside the workbench.
    review_watermark_text: str = ""
    render_background: str = "#FAFAF7"


@dataclass
class RenderResult:
    ok: bool
    output_path: Optional[str] = None
    duration_ms: Optional[int] = None
    sha256: Optional[str] = None
    qc: Optional[Dict[str, Any]] = None
    error: str = ""


def build_timeline(
    plan_shots: Sequence[Dict[str, Any]],
    shots_by_slot: Dict[int, Any],
    image_paths: Dict[int, str],
) -> List[TimelineSlot]:
    """Extend each clip by half of its adjacent dissolves (see module docstring)."""
    transitions: Dict[int, int] = {}
    for shot in plan_shots:
        slot = int(shot["slot_index"])
        transitions[slot] = (
            DISSOLVE_MS if shot.get("transition_out") == "short_dissolve" else 0
        )
    slots: List[TimelineSlot] = []
    for shot in plan_shots:
        slot = int(shot["slot_index"])
        planned = int(shot["duration_ms"])
        prev_t = transitions.get(slot - 1, 0)
        next_t = transitions.get(slot, 0)
        effective = planned + (prev_t + next_t) // 2
        shot_row = shots_by_slot[slot]
        slots.append(
            TimelineSlot(
                slot_index=slot,
                shot_id=shot_row.shot_id,
                shot_version=shot_row.shot_version,
                image_path=image_paths[slot],
                motion_preset=shot.get("motion_preset", "slow_push"),
                transition_out=shot.get("transition_out", "short_dissolve"),
                planned_ms=planned,
                effective_ms=effective,
                fit_mode=str(shot.get("fit_mode") or "cover"),
                overlay_text=str(shot.get("overlay_text") or ""),
                overlay_spec=dict(shot.get("overlay_spec") or {}),
                render_background=str(shot.get("render_background") or "#FAFAF7"),
            )
        )
    start = 0
    for index, slot in enumerate(slots):
        slot.start_ms = start
        start += slot.effective_ms
        if index < len(slots) - 1:
            start -= transitions[slot.slot_index]
    return slots


def build_filter_graph(slots: Sequence[TimelineSlot]) -> str:
    parts: List[str] = []
    reserve_header = any(slot.overlay_text and (slot.overlay_spec or {}).get("layout_policy") == "reserved_header_v2"
                         for slot in slots)
    for index, slot in enumerate(slots):
        n = max(int(round(slot.effective_ms * FPS / 1000)), 1)
        z_expr, x_expr, y_expr = MOTION_EXPRESSIONS.get(
            slot.motion_preset, MOTION_EXPRESSIONS["slow_push"]
        )
        z = z_expr.format(n=n)
        x = x_expr.format(n=n)
        y = y_expr.format(n=n)
        if slot.fit_mode == "contain":
            background = slot.render_background
            if not re.fullmatch(r"#[0-9a-fA-F]{6}", background):
                raise RenderError("render background must be a six-digit hex color")
            sizing = (
                f"scale={OUTPUT_WIDTH * SUPER_SCALE}:{OUTPUT_HEIGHT * SUPER_SCALE}"
                ":force_original_aspect_ratio=decrease,"
                f"pad={OUTPUT_WIDTH * SUPER_SCALE}:{OUTPUT_HEIGHT * SUPER_SCALE}"
                f":(ow-iw)/2:(oh-ih)/2:color=0x{background[1:]}"
            )
        else:
            sizing = (
                f"scale={OUTPUT_WIDTH * SUPER_SCALE}:{OUTPUT_HEIGHT * SUPER_SCALE}"
                ":force_original_aspect_ratio=increase,"
                f"crop={OUTPUT_WIDTH * SUPER_SCALE}:{OUTPUT_HEIGHT * SUPER_SCALE}"
            )
        # New plans reserve real pixels for copy, rather than merely asking
        # generation to leave headroom. Apply AFTER motion so zoom cannot
        # move the subject into the title. Legacy frozen plans stay unchanged.
        header_filter = ""
        if reserve_header:
            background = slot.render_background
            if not re.fullmatch(r"#[0-9a-fA-F]{6}", background):
                raise RenderError("render background must be a six-digit hex color")
            header_filter = (
                f",scale={OUTPUT_WIDTH}:{OUTPUT_HEIGHT - OVERLAY_HEADER_HEIGHT}"
                ":force_original_aspect_ratio=decrease:force_divisible_by=2,"
                f"pad={OUTPUT_WIDTH}:{OUTPUT_HEIGHT}:(ow-iw)/2:{OVERLAY_HEADER_HEIGHT}"
                f":color=0x{background[1:]}"
            )
        parts.append(
            f"[{index}:v]{sizing},"
            f"zoompan=z='{z}':x='{x}':y='{y}':d=1:s={OUTPUT_WIDTH}x{OUTPUT_HEIGHT}"
            f":fps={FPS}{header_filter},format=yuv420p,settb=AVTB[v{index}]"
        )
    if len(slots) == 1:
        parts.append("[v0]copy[vout]")
        return ";".join(parts)
    current = "v0"
    offset_ms = 0
    for index in range(len(slots) - 1):
        slot = slots[index]
        offset_ms += slot.effective_ms
        if slot.transition_out == "short_dissolve":
            offset_ms -= DISSOLVE_MS
            label = f"x{index}"
            parts.append(
                f"[{current}][v{index + 1}]xfade=transition=fade"
                f":duration={DISSOLVE_MS / 1000:.3f}"
                f":offset={offset_ms / 1000:.3f}[{label}]"
            )
            current = label
        else:
            label = f"x{index}"
            parts.append(
                f"[{current}][v{index + 1}]concat=n=2:v=1:a=0[{label}]"
            )
            current = label
    parts.append(f"[{current}]format=yuv420p[vout]")
    return ";".join(parts)


def _ass_time(milliseconds: int) -> str:
    centiseconds = max(0, int(round(milliseconds / 10)))
    hours, remainder = divmod(centiseconds, 360000)
    minutes, remainder = divmod(remainder, 6000)
    seconds, fraction = divmod(remainder, 100)
    return f"{hours}:{minutes:02d}:{seconds:02d}.{fraction:02d}"


def _ass_escape(text: str) -> str:
    return (
        str(text)
        .replace("\\", "\\\\")
        .replace("{", "\\{")
        .replace("}", "\\}")
        .replace("\n", "\\N")
    )


def build_ass_document(slots: Sequence[TimelineSlot]) -> str:
    """Build a UTF-8 ASS document using fixed TikTok-safe styles."""
    header = """[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 2
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Hook,Thonburi,66,&H00FFFFFF,&H00FFFFFF,&H90000000,&H70000000,-1,0,0,0,100,100,0,0,1,4,1,7,72,180,150,1
Style: Tag,Thonburi,48,&H00FFFFFF,&H00FFFFFF,&H90000000,&H70000000,-1,0,0,0,100,100,0,0,1,3,1,7,72,180,170,1
Style: End,Thonburi,54,&H00FFFFFF,&H00FFFFFF,&H90000000,&H70000000,-1,0,0,0,100,100,0,0,1,4,1,2,90,190,360,1
Style: SafeHook,Thonburi,48,&H00202020,&H00202020,&H00FFFFFF,&H00000000,-1,0,0,0,100,100,0,0,1,1,0,7,72,180,150,1
Style: SafeTag,Thonburi,44,&H00202020,&H00202020,&H00FFFFFF,&H00000000,-1,0,0,0,100,100,0,0,1,1,0,7,72,180,150,1
Style: Review,Thonburi,38,&H00FFFFFF,&H00FFFFFF,&H90000000,&H80000000,-1,0,0,0,100,100,0,0,1,3,1,9,72,72,72,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    style_by_template = {
        "HOOK_TITLE_V1": "Hook",
        "STATE_TAG_V1": "Tag",
        "END_QUESTION_V1": "End",
    }
    events = []
    for slot in slots:
        if not slot.overlay_text:
            continue
        template = str((slot.overlay_spec or {}).get("template_id") or "")
        style = style_by_template.get(template)
        if style is None:
            raise RenderError(f"unknown overlay template: {template!r}")
        if (slot.overlay_spec or {}).get("layout_policy") == "reserved_header_v2":
            style = "SafeHook" if template == "HOOK_TITLE_V1" else "SafeTag"
        start = slot.start_ms
        end = slot.start_ms + slot.planned_ms
        events.append(
            "Dialogue: 0,"
            f"{_ass_time(start)},{_ass_time(end)},{style},,0,0,0,,"
            f"{_ass_escape(slot.overlay_text)}"
        )
    for slot in slots:
        if not slot.review_watermark_text:
            continue
        start = slot.start_ms
        end = slot.start_ms + slot.planned_ms
        events.append(
            "Dialogue: 5,"
            f"{_ass_time(start)},{_ass_time(end)},Review,,0,0,0,,"
            f"{_ass_escape(slot.review_watermark_text)}"
        )
    return header + "\n".join(events) + "\n"


class FFmpegStillRenderer:
    def __init__(
        self,
        ffmpeg_bin: Optional[str] = None,
        ffprobe_bin: Optional[str] = None,
        runner: Optional[Callable[[Sequence[str]], Tuple[int, str, str]]] = None,
    ):
        self._ffmpeg = self._resolve_binary(
            ffmpeg_bin, env_name="OPV_FFMPEG_BIN", executable="ffmpeg"
        )
        self._ffprobe = self._resolve_binary(
            ffprobe_bin, env_name="OPV_FFPROBE_BIN", executable="ffprobe"
        )
        self._runner = runner or self._subprocess_runner

    @staticmethod
    def _resolve_binary(
        requested: Optional[str], *, env_name: str, executable: str
    ) -> str:
        """Resolve media binaries without relying on launchd's sparse PATH."""
        explicit = str(requested or os.environ.get(env_name) or "").strip()
        if explicit:
            return explicit
        discovered = shutil.which(executable)
        if discovered:
            return discovered
        local_candidate = Path.home() / ".local" / "bin" / executable
        if local_candidate.is_file():
            return str(local_candidate)
        return executable

    @staticmethod
    def _subprocess_runner(argv: Sequence[str]) -> Tuple[int, str, str]:
        completed = subprocess.run(
            list(argv), capture_output=True, text=True, timeout=600
        )
        return completed.returncode, completed.stdout, completed.stderr

    # ------------------------------------------------------------------

    def build_command(
        self,
        slots: Sequence[TimelineSlot],
        output_path: Path,
        subtitle_path: Optional[Path] = None,
    ) -> List[str]:
        argv: List[str] = [self._ffmpeg, "-y"]
        for slot in slots:
            argv += [
                "-loop", "1",
                "-framerate", str(FPS),
                "-t", f"{slot.effective_ms / 1000:.3f}",
                "-i", slot.image_path,
            ]
        graph = build_filter_graph(slots)
        output_label = "vout"
        if subtitle_path is not None:
            escaped = str(subtitle_path).replace("\\", "\\\\").replace("'", "\\'")
            graph += (
                f";[vout]ass=filename='{escaped}':"
                "fontsdir='/System/Library/Fonts/Supplemental'[vtext]"
            )
            output_label = "vtext"
        argv += [
            "-filter_complex", graph,
            "-map", f"[{output_label}]",
            "-r", str(FPS),
            "-c:v", "libx264",
            "-preset", "medium",
            "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-an",
            str(output_path),
        ]
        return argv

    def render(
        self, slots: Sequence[TimelineSlot], output_path: Path
    ) -> Tuple[bool, str]:
        subtitle_path = None
        if any(slot.overlay_text or slot.review_watermark_text for slot in slots):
            subtitle_path = output_path.with_suffix(".ass")
            subtitle_path.write_text(build_ass_document(slots), encoding="utf-8")
        argv = self.build_command(slots, output_path, subtitle_path)
        try:
            returncode, _stdout, stderr = self._runner(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return False, f"ffmpeg launch failed: {exc}"
        if returncode != 0:
            tail = "\n".join(stderr.splitlines()[-8:])
            return False, f"ffmpeg exit {returncode}: {tail}"
        if not output_path.exists():
            return False, "ffmpeg reported success but output file is missing"
        return True, ""

    # ------------------------------------------------------------------

    def probe(self, path: Path) -> Dict[str, Any]:
        argv = [
            self._ffprobe, "-v", "error", "-print_format", "json",
            "-show_format", "-show_streams", str(path),
        ]
        returncode, stdout, stderr = self._runner(argv)
        if returncode != 0:
            raise RenderError(f"ffprobe failed: {stderr.strip()[:400]}")
        return json.loads(stdout)

    def black_frames(self, path: Path) -> List[str]:
        argv = [
            self._ffmpeg, "-xerror", "-i", str(path),
            "-vf", "blackdetect=d=0.1:pix_th=0.10",
            "-an", "-f", "null", "-",
        ]
        rc, _stdout, stderr = self._runner(argv)
        if rc != 0:
            raise RenderError("video decode/black-frame scan failed")
        return [
            line for line in stderr.splitlines() if "black_start" in line
        ]

    def qc_video(self, path: Path, expected_ms: int) -> Dict[str, Any]:
        info = self.probe(path)
        video_stream = next(
            (s for s in info.get("streams", []) if s.get("codec_type") == "video"),
            None,
        )
        audio_streams = [
            s for s in info.get("streams", []) if s.get("codec_type") == "audio"
        ]
        duration_s = float(info.get("format", {}).get("duration", 0) or 0)
        duration_ms = int(round(duration_s * 1000))
        decodable = True
        try:
            black = self.black_frames(path)
        except (RenderError, OSError, subprocess.TimeoutExpired):
            black, decodable = [], False
        checks = {
            "resolution_1080x1920": (
                video_stream is not None
                and int(video_stream.get("width", 0) or 0) == OUTPUT_WIDTH
                and int(video_stream.get("height", 0) or 0) == OUTPUT_HEIGHT
            ),
            "fps_30": video_stream is not None
            and abs(eval_fps(video_stream) - FPS) < 0.5,
            "video_decodable": decodable,
            "codec_h264": video_stream is not None
            and video_stream.get("codec_name") == "h264",
            "pix_fmt_yuv420p": video_stream is not None
            and video_stream.get("pix_fmt") == "yuv420p",
            # Six-second multi-look is an explicit profile; legacy renders
            # retain the historical 10–15s guard.
            "duration_10000_15000ms": (
                abs(duration_ms - expected_ms) <= 400 if expected_ms < 10000
                else 10000 <= duration_ms <= 15000
            ),
            "duration_close_to_plan": abs(duration_ms - expected_ms) <= 400,
            "no_audio_stream": len(audio_streams) == 0,
            "no_black_frames": len(black) == 0,
        }
        return {
            "checks": checks,
            "passed": all(checks.values()),
            "duration_ms": duration_ms,
            "width": int(video_stream.get("width", 0) or 0) if video_stream else 0,
            "height": int(video_stream.get("height", 0) or 0) if video_stream else 0,
            "codec": video_stream.get("codec_name") if video_stream else None,
            "audio_streams": len(audio_streams),
            "black_frames": black,
            "sha256": sha256_file(path),
        }

    @staticmethod
    def file_sha256(path: Path) -> str:
        return sha256_file(path)


def eval_fps(stream: Dict[str, Any]) -> float:
    rate = str(stream.get("avg_frame_rate") or stream.get("r_frame_rate") or "0/1")
    num, _, den = rate.partition("/")
    try:
        numerator = float(num)
        denominator = float(den) if den else 1.0
        return numerator / denominator if denominator else 0.0
    except ValueError:
        return 0.0


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
