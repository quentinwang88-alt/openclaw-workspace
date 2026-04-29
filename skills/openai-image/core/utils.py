#!/usr/bin/env python3
"""Utility helpers for the openai-image skill."""

from __future__ import annotations

import base64
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping


def log_info(message: str) -> None:
    print(f"[openai-image] {message}", file=sys.stderr)


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def sanitize_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    sanitized = sanitized.strip("._")
    return sanitized or "image_task"


def build_output_image_path(output_dir: Path, task_id: str, index: int, output_format: str) -> Path:
    extension = (output_format or "png").lower()
    filename = f"{sanitize_filename(task_id)}_{index}.{extension}"
    return ensure_directory(output_dir) / filename


def save_base64_image(image_b64: str, destination: Path) -> Path:
    image_bytes = base64.b64decode(image_b64)
    ensure_directory(destination.parent)
    destination.write_bytes(image_bytes)
    return destination


def write_json_file(destination: Path, payload: Mapping[str, Any]) -> Path:
    ensure_directory(destination.parent)
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return destination
