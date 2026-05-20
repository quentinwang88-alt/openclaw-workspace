#!/usr/bin/env python3
"""Adapter around the existing openai-image skill."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

SKILL_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = SKILL_DIR.parents[1]
OPENAI_IMAGE_SKILL = WORKSPACE_DIR / "skills" / "openai-image"
if str(OPENAI_IMAGE_SKILL) not in sys.path:
    sys.path.insert(0, str(OPENAI_IMAGE_SKILL))
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))

from app.config import get_settings  # type: ignore  # noqa: E402
from core.image_service import ImageService  # type: ignore  # noqa: E402
from core.schemas import ImageTaskRequest  # type: ignore  # noqa: E402


def generate_main_image(
    *,
    task_id: str,
    prompt: str,
    input_image_path: str,
    input_image_paths: Optional[List[str]] = None,
    output_dir: Path,
    quality: str = "medium",
) -> List[str]:
    ordered_images: List[str] = []
    for candidate in [input_image_path, *(input_image_paths or [])]:
        if candidate and candidate not in ordered_images:
            ordered_images.append(candidate)
    service = ImageService(settings=get_settings())
    request = ImageTaskRequest(
        task_id=task_id,
        task_type="likeu_main_image",
        target_field="首图结果",
        mode="edit",
        prompt=prompt,
        input_image_path=ordered_images[0],
        input_image_paths=ordered_images,
        size="1024x1024",
        quality=quality,
        output_format="png",
        output_dir=str(output_dir),
        n=1,
        metadata={"skill": "tiktok-fashion-image-pack"},
    )
    result = service.process_task(request)
    if result.status != "success":
        raise RuntimeError(result.error_message or "image generation failed")
    return result.output_image_paths
