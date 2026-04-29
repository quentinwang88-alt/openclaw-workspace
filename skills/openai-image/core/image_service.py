#!/usr/bin/env python3
"""Business service for OpenAI image generation and editing."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import List, Optional

from app.config import Settings, get_settings
from core.openai_image_client import OpenAIImageClient
from core.schemas import ImageTaskRequest, ImageTaskResult
from core.utils import build_output_image_path, ensure_directory, log_info, save_base64_image, write_json_file


SUPPORTED_MODES = {"generate", "edit"}
SUPPORTED_SIZES = {"1024x1024", "1024x1536", "1536x1024", "auto"}
SUPPORTED_QUALITIES = {"low", "medium", "high", "auto"}
SUPPORTED_OUTPUT_FORMATS = {"png", "jpeg", "webp"}
SUPPORTED_API_MODES = {"codex", "openai"}
SKILL_DIR = Path(__file__).resolve().parents[1]
RESULT_OUTPUT_DIR = SKILL_DIR / "outputs"


class ImageService:
    """Coordinates validation, API calls, and local output persistence."""

    def __init__(self, settings: Optional[Settings] = None, client: Optional[OpenAIImageClient] = None):
        self.settings = settings or get_settings()
        self.client = client or OpenAIImageClient(self.settings)

    def process_task(self, request: ImageTaskRequest) -> ImageTaskResult:
        resolved_request = self._resolve_request(request)

        try:
            self._validate_request(resolved_request)
            output_dir = ensure_directory(Path(resolved_request.output_dir).expanduser())

            if resolved_request.mode == "generate":
                response = self.client.generate_image(
                    prompt=resolved_request.prompt,
                    size=resolved_request.size,
                    quality=resolved_request.quality,
                    output_format=resolved_request.output_format,
                    n=resolved_request.n,
                )
            else:
                response = self.client.edit_image(
                    prompt=resolved_request.prompt,
                    input_image_path=resolved_request.input_image_path,
                    mask_image_path=resolved_request.mask_image_path,
                    size=resolved_request.size,
                    quality=resolved_request.quality,
                    output_format=resolved_request.output_format,
                    n=resolved_request.n,
                )

            output_image_paths = self._save_output_images(
                task_id=resolved_request.task_id,
                output_dir=output_dir,
                output_format=resolved_request.output_format,
                response_data=response.get("data", []),
            )

            result = self._build_result(
                request=resolved_request,
                status="success",
                output_image_paths=output_image_paths,
            )
            self.persist_result(result)
            for path in output_image_paths:
                log_info(f"Saved image to {path}")
            return result
        except Exception as exc:
            result = self._build_result(
                request=resolved_request,
                status="failed",
                output_image_paths=[],
                error_message=str(exc),
            )
            self.persist_result(result)
            return result

    def persist_result(self, result: ImageTaskResult) -> Path:
        output_path = ensure_directory(RESULT_OUTPUT_DIR) / f"{self._result_basename(result.task_id)}_result.json"
        return write_json_file(output_path, result.to_dict())

    def _resolve_request(self, request: ImageTaskRequest) -> ImageTaskRequest:
        output_dir = request.output_dir or str(self.settings.image_output_dir)
        input_image_path = self._resolve_optional_path(request.input_image_path)
        mask_image_path = self._resolve_optional_path(request.mask_image_path)
        resolved_output_dir = str(Path(output_dir).expanduser().resolve())

        return replace(
            request,
            mode=(request.mode or "").strip().lower(),
            size=(request.size or self.settings.default_size).strip(),
            quality=(request.quality or self.settings.default_quality).strip().lower(),
            output_format=(request.output_format or self.settings.default_output_format).strip().lower(),
            output_dir=resolved_output_dir,
            input_image_path=input_image_path,
            mask_image_path=mask_image_path,
            n=max(int(request.n or 1), 1),
        )

    @staticmethod
    def _resolve_optional_path(raw_path: str) -> str:
        if not raw_path:
            return ""
        return str(Path(raw_path).expanduser().resolve())

    def _validate_request(self, request: ImageTaskRequest) -> None:
        # Validate API key based on api_mode
        if self.settings.api_mode == "codex":
            if not self.settings.codex_api_key:
                raise ValueError(
                    "Codex API key not found. Please login to OpenClaw/Hermes or set "
                    "ORIGINAL_SCRIPT_PRIMARY_LLM_API_KEY env var."
                )
        else:
            if not self.settings.openai_api_key:
                raise ValueError("OPENAI_API_KEY is missing")

        api_mode = (self.settings.api_mode or "").strip().lower()
        if api_mode not in SUPPORTED_API_MODES:
            raise ValueError(f"api_mode must be one of: {', '.join(sorted(SUPPORTED_API_MODES))}")
        if not request.task_id:
            raise ValueError("task_id is required")
        if not request.prompt:
            raise ValueError("prompt is required")
        if request.mode not in SUPPORTED_MODES:
            raise ValueError("mode must be one of: generate, edit")
        if request.size not in SUPPORTED_SIZES:
            raise ValueError(f"size must be one of: {', '.join(sorted(SUPPORTED_SIZES))}")
        if request.quality not in SUPPORTED_QUALITIES:
            raise ValueError(f"quality must be one of: {', '.join(sorted(SUPPORTED_QUALITIES))}")
        if request.output_format not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(f"output_format must be one of: {', '.join(sorted(SUPPORTED_OUTPUT_FORMATS))}")

        if request.mode == "edit":
            if not request.input_image_path:
                raise ValueError("input_image_path is required for edit mode")
            if not Path(request.input_image_path).exists():
                raise FileNotFoundError(f"Input image not found: {request.input_image_path}")
            if request.mask_image_path and not Path(request.mask_image_path).exists():
                raise FileNotFoundError(f"Mask image not found: {request.mask_image_path}")

    def _save_output_images(
        self,
        *,
        task_id: str,
        output_dir: Path,
        output_format: str,
        response_data: List[dict],
    ) -> List[str]:
        if not response_data:
            raise RuntimeError("OpenAI API returned no image data")

        saved_paths: List[str] = []
        for index, item in enumerate(response_data, start=1):
            # Prefer b64_json; fall back to downloading URL
            image_b64 = str(item.get("b64_json") or "").strip()
            image_url = str(item.get("url") or "").strip()

            if image_b64:
                destination = build_output_image_path(output_dir, task_id, index, output_format)
                save_base64_image(image_b64, destination)
                saved_paths.append(str(destination))
            elif image_url:
                # Download from URL
                import requests

                destination = build_output_image_path(output_dir, task_id, index, output_format)
                resp = requests.get(image_url, timeout=120)
                resp.raise_for_status()
                destination.write_bytes(resp.content)
                saved_paths.append(str(destination))
            else:
                raise RuntimeError("OpenAI API response did not include b64_json or url image data")

        return saved_paths

    def _build_result(
        self,
        *,
        request: ImageTaskRequest,
        status: str,
        output_image_paths: List[str],
        error_message: str = "",
    ) -> ImageTaskResult:
        return ImageTaskResult(
            task_id=request.task_id,
            task_type=request.task_type,
            target_field=request.target_field,
            mode=request.mode,
            status=status,
            prompt=request.prompt,
            output_image_paths=output_image_paths,
            model=self.settings.effective_model,
            size=request.size,
            quality=request.quality,
            output_format=request.output_format,
            error_message=error_message,
            metadata=dict(request.metadata),
        )

    @staticmethod
    def _result_basename(task_id: str) -> str:
        stripped = (task_id or "").strip()
        return stripped if stripped else "unknown_task"
