#!/usr/bin/env python3
"""CLI entrypoint for the openai-image skill."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

SKILL_DIR = Path(__file__).resolve().parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from workspace_support import load_repo_env

load_repo_env()

from app.config import get_settings
from core.image_service import ImageService
from core.schemas import ImageTaskRequest, ImageTaskResult


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OpenAI image generation or editing from an input JSON file.")
    parser.add_argument("--input", required=True, help="Path to the input JSON file")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    service = ImageService(settings=settings)

    try:
        input_path = Path(args.input).expanduser().resolve()
        payload = _load_payload(input_path)
        request = ImageTaskRequest.from_dict(payload)
        result = service.process_task(request)
    except Exception as exc:
        result = ImageTaskResult(
            task_id="",
            task_type="",
            target_field="",
            mode="",
            status="failed",
            prompt="",
            output_image_paths=[],
            model=settings.image_model,
            size=settings.default_size,
            quality=settings.default_quality,
            output_format=settings.default_output_format,
            error_message=str(exc),
            metadata={},
        )
        service.persist_result(result)

    print(result.to_json())
    return 0 if result.status == "success" else 1


def _load_payload(input_path: Path) -> Dict[str, Any]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_path}")
    with input_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


if __name__ == "__main__":
    raise SystemExit(main())
