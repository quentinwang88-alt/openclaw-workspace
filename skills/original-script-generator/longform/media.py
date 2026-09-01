from __future__ import annotations

import json
import math
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable


def _require_binary(name: str) -> str:
    value = shutil.which(name)
    if not value:
        raise RuntimeError(f"缺少 {name}")
    return value


def probe_video(path: str | Path) -> Dict[str, Any]:
    ffprobe = _require_binary("ffprobe")
    completed = subprocess.run(
        [ffprobe, "-v", "error", "-print_format", "json", "-show_streams", "-show_format", str(path)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("ffprobe 失败: " + completed.stderr[-800:])
    return json.loads(completed.stdout)


def extract_bridge_candidates(video_path: str | Path, output_dir: str | Path,
                              window_seconds: float = 0.8, count: int = 4) -> list[str]:
    ffmpeg = _require_binary("ffmpeg")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    pattern = output / "bridge_%02d.jpg"
    completed = subprocess.run(
        [ffmpeg, "-y", "-sseof", f"-{window_seconds:g}", "-i", str(video_path),
         "-vf", f"fps={count / window_seconds:g}", "-frames:v", str(count), "-q:v", "2", str(pattern)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("桥接帧提取失败: " + completed.stderr[-800:])
    return [str(path) for path in sorted(output.glob("bridge_*.jpg"))]


def select_bridge_candidate(candidate_paths: Iterable[str | Path]) -> Dict[str, Any]:
    """Choose a clear, low-motion middle frame without semantic scoring.

    This deliberately does not judge pose, beauty or product meaning. Product
    readability is a generation-time soft preference, not a blocking selector
    rule. Pillow is optional; if unavailable, the second frame is a stable
    deterministic fallback.
    """

    paths = [Path(item) for item in candidate_paths if Path(item).is_file()]
    if not paths:
        raise ValueError("没有可用桥接候选帧")
    if len(paths) == 1:
        return {
            "selected": str(paths[0]), "method": "ONLY_CANDIDATE", "scores": [],
            "product_readability": "NOT_SEMANTICALLY_EVALUATED",
            "failure_policy": "CONTINUE_WITH_BEST_AVAILABLE_FRAME",
        }
    try:
        from PIL import Image, ImageChops, ImageFilter, ImageStat

        images = [Image.open(path).convert("L").resize((180, 320)) for path in paths]
        metrics = []
        for index, image in enumerate(images):
            edge_variance = float(ImageStat.Stat(image.filter(ImageFilter.FIND_EDGES)).var[0])
            brightness = float(ImageStat.Stat(image).mean[0])
            neighbor_diffs = []
            for neighbor in (index - 1, index + 1):
                if 0 <= neighbor < len(images):
                    neighbor_diffs.append(float(ImageStat.Stat(ImageChops.difference(image, images[neighbor])).mean[0]))
            motion = sum(neighbor_diffs) / max(1, len(neighbor_diffs))
            exposure_penalty = max(0.0, 32.0 - brightness) + max(0.0, brightness - 225.0)
            boundary_penalty = 0.15 if index in {0, len(images) - 1} else 0.0
            score = math.log1p(edge_variance) - motion * 0.035 - exposure_penalty * 0.02 - boundary_penalty
            metrics.append({
                "path": str(paths[index]), "score": round(score, 6),
                "edge_variance": round(edge_variance, 3),
                "neighbor_difference": round(motion, 3),
                "brightness": round(brightness, 3),
            })
        selected = max(metrics, key=lambda item: item["score"])
        return {
            "selected": selected["path"], "method": "CLARITY_LOW_MOTION_V1", "scores": metrics,
            "product_readability": "NOT_SEMANTICALLY_EVALUATED",
            "failure_policy": "CONTINUE_WITH_BEST_AVAILABLE_FRAME",
        }
    except (ImportError, OSError):
        fallback = paths[min(1, len(paths) - 1)]
        return {
            "selected": str(fallback), "method": "DETERMINISTIC_SECOND_FRAME", "scores": [],
            "product_readability": "NOT_SEMANTICALLY_EVALUATED",
            "failure_policy": "CONTINUE_WITH_BEST_AVAILABLE_FRAME",
        }


def merge_segments(segment_paths: Iterable[str | Path], output_path: str | Path,
                   width: int = 1080, height: int = 1920, fps: int = 30) -> str:
    """Normalize, strip source audio and hard-cut concat two or three segments."""
    ffmpeg = _require_binary("ffmpeg")
    paths = [Path(item) for item in segment_paths]
    if not 2 <= len(paths) <= 3 or any(not item.exists() for item in paths):
        raise ValueError("合并必须提供2至3个已存在的片段")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    filters = []
    for index in range(len(paths)):
        filters.append(
            f"[{index}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,fps={fps},format=yuv420p,setpts=PTS-STARTPTS[v{index}]"
        )
    inputs = "".join(f"[v{index}]" for index in range(len(paths)))
    filters.append(f"{inputs}concat=n={len(paths)}:v=1:a=0[outv]")
    command = [ffmpeg, "-y"]
    for path in paths:
        command.extend(["-i", str(path)])
    command.extend([
        "-filter_complex", ";".join(filters), "-map", "[outv]", "-an",
        "-c:v", "libx264", "-preset", "medium", "-crf", "18", "-movflags", "+faststart", str(output),
    ])
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("长视频合并失败: " + completed.stderr[-1200:])
    return str(output)
