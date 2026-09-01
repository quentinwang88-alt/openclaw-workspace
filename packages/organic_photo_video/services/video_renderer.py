"""FFmpeg still-image slideshow renderer (Stage E).

Renders the approved 5-shot group into the 12.5s vertical master defined by
the render preset: 1080x1920, 30fps, H.264/yuv420p, light motion per slot
(slow_push / light_pan / detail_zoom / static_hold), short dissolves between
shots, no audio stream (dual-audio impossible; silent-AAC vs silent master is
pending the NeoBund upload test, see MODEL_HANDOFF section 15).

Timeline math: with transition t between shots, each clip is extended by
(t_prev + t_next) / 2 so the final duration stays equal to the plan total
(e.g. 12500ms) instead of shrinking by the dissolve overlap.

Text overlays are intentionally NOT burned in V1: copy is
``draft_needs_human_review`` and TikTok UI safe-area rendering lands after
copy approval. No local BGM is ever burned (audio_policy platform_hot_bgm).
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

OUTPUT_WIDTH = 1080
OUTPUT_HEIGHT = 1920
FPS = 30
DISSOLVE_MS = 300
SUPER_SCALE = 2  # zoompan works on a 2x upscaled canvas to reduce jitter

MOTION_EXPRESSIONS = {
    "slow_push": ("1+0.04*on/{n}", "(iw-iw/zoom)/2", "(ih-ih/zoom)/2"),
    "detail_zoom": ("1+0.04*on/{n}", "(iw-iw/zoom)/2", "(ih-ih/zoom)/2"),
    "light_pan": ("1.03", "(iw-iw/zoom)*(on/{n})", "(ih-ih/zoom)/2"),
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
    start_ms: int = 0


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
    for index, slot in enumerate(slots):
        n = max(int(round(slot.effective_ms * FPS / 1000)), 1)
        z_expr, x_expr, y_expr = MOTION_EXPRESSIONS.get(
            slot.motion_preset, MOTION_EXPRESSIONS["slow_push"]
        )
        z = z_expr.format(n=n)
        x = x_expr.format(n=n)
        y = y_expr.format(n=n)
        parts.append(
            f"[{index}:v]scale={OUTPUT_WIDTH * SUPER_SCALE}:{OUTPUT_HEIGHT * SUPER_SCALE}"
            f":force_original_aspect_ratio=increase,"
            f"crop={OUTPUT_WIDTH * SUPER_SCALE}:{OUTPUT_HEIGHT * SUPER_SCALE},"
            f"zoompan=z='{z}':x='{x}':y='{y}':d=1:s={OUTPUT_WIDTH}x{OUTPUT_HEIGHT}"
            f":fps={FPS},format=yuv420p,settb=AVTB[v{index}]"
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


class FFmpegStillRenderer:
    def __init__(
        self,
        ffmpeg_bin: str = "ffmpeg",
        ffprobe_bin: str = "ffprobe",
        runner: Optional[Callable[[Sequence[str]], Tuple[int, str, str]]] = None,
    ):
        self._ffmpeg = ffmpeg_bin
        self._ffprobe = ffprobe_bin
        self._runner = runner or self._subprocess_runner

    @staticmethod
    def _subprocess_runner(argv: Sequence[str]) -> Tuple[int, str, str]:
        completed = subprocess.run(
            list(argv), capture_output=True, text=True, timeout=600
        )
        return completed.returncode, completed.stdout, completed.stderr

    # ------------------------------------------------------------------

    def build_command(
        self, slots: Sequence[TimelineSlot], output_path: Path
    ) -> List[str]:
        argv: List[str] = [self._ffmpeg, "-y"]
        for slot in slots:
            argv += [
                "-loop", "1",
                "-framerate", str(FPS),
                "-t", f"{slot.effective_ms / 1000:.3f}",
                "-i", slot.image_path,
            ]
        argv += [
            "-filter_complex", build_filter_graph(slots),
            "-map", "[vout]",
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
        argv = self.build_command(slots, output_path)
        returncode, _stdout, stderr = self._runner(argv)
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
            self._ffmpeg, "-i", str(path),
            "-vf", "blackdetect=d=0.1:pix_th=0.10",
            "-an", "-f", "null", "-",
        ]
        _rc, _stdout, stderr = self._runner(argv)
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
        black = self.black_frames(path)
        checks = {
            "resolution_1080x1920": (
                video_stream is not None
                and int(video_stream.get("width", 0) or 0) == OUTPUT_WIDTH
                and int(video_stream.get("height", 0) or 0) == OUTPUT_HEIGHT
            ),
            "fps_30": video_stream is not None
            and abs(eval_fps(video_stream)) - FPS < 0.5,
            "codec_h264": video_stream is not None
            and video_stream.get("codec_name") == "h264",
            "pix_fmt_yuv420p": video_stream is not None
            and video_stream.get("pix_fmt") == "yuv420p",
            "duration_10000_15000ms": 10000 <= duration_ms <= 15000,
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
