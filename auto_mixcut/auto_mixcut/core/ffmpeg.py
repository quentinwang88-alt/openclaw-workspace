from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .result import Result


class FFmpeg:
    def __init__(self, mock: bool = False):
        self.mock = mock

    def require_tools(self) -> Result:
        if self.mock:
            return Result.ok({"mock": True})
        missing = [tool for tool in ("ffmpeg", "ffprobe") if shutil.which(tool) is None]
        if missing:
            return Result.fail("FFMPEG_NOT_FOUND", "ffmpeg/ffprobe is required", {"missing": missing})
        return Result.ok()

    def probe(self, path: Path) -> Result:
        if self.mock:
            return Result.ok(
                {
                    "duration_ms": 3000,
                    "width": 1080,
                    "height": 1920,
                    "fps": 30.0,
                    "codec": "h264",
                    "has_audio": True,
                    "orientation": "vertical",
                    "raw": {"mock": True, "path": str(path)},
                }
            )
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            str(path),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            return Result.fail("PROBE_FAILED", "ffprobe failed", {"stderr": proc.stderr, "path": str(path)})
        raw = json.loads(proc.stdout)
        video = next((s for s in raw.get("streams", []) if s.get("codec_type") == "video"), None)
        audio = next((s for s in raw.get("streams", []) if s.get("codec_type") == "audio"), None)
        if not video:
            return Result.fail("PROBE_FAILED", "no video stream found", {"path": str(path), "raw": raw})
        duration = float(video.get("duration") or raw.get("format", {}).get("duration") or 0)
        fps = _parse_fps(video.get("avg_frame_rate") or video.get("r_frame_rate") or "0/1")
        width = int(video.get("width") or 0)
        height = int(video.get("height") or 0)
        return Result.ok(
            {
                "duration_ms": int(duration * 1000),
                "width": width,
                "height": height,
                "fps": fps,
                "codec": video.get("codec_name"),
                "has_audio": audio is not None,
                "orientation": "vertical" if height >= width else "horizontal",
                "raw": raw,
            }
        )

    def run(self, args: Iterable[str], error_code: str) -> Result:
        if self.mock:
            return Result.ok({"mock": True, "args": list(args)})
        cmd = ["ffmpeg", *args]
        timeout = _ffmpeg_timeout_sec()
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout or None)
        except subprocess.TimeoutExpired as exc:
            return Result.fail(
                error_code,
                f"ffmpeg timed out after {timeout}s",
                {"stderr": (exc.stderr or ""), "stdout": (exc.stdout or ""), "args": cmd, "timeout_seconds": timeout},
            )
        if proc.returncode != 0:
            return Result.fail(error_code, "ffmpeg failed", {"stderr": proc.stderr, "args": list(args)})
        return Result.ok({"stdout": proc.stdout})


def _parse_fps(value: str) -> float:
    try:
        if "/" in value:
            num, den = value.split("/", 1)
            return float(num) / float(den or 1)
        return float(value)
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


def _ffmpeg_timeout_sec() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_FFMPEG_TIMEOUT_SEC", "0") or "0"))
    except ValueError:
        return 0
