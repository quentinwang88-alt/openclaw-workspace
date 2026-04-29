#!/usr/bin/env python3
"""Dataclasses for the openai-image skill."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping


def _normalize_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_metadata(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    return {"value": value}


def _normalize_positive_int(value: Any, default: int = 1) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    return normalized if normalized > 0 else default


@dataclass(frozen=True)
class ImageTaskRequest:
    task_id: str = ""
    task_type: str = ""
    target_field: str = ""
    mode: str = ""
    prompt: str = ""
    input_image_path: str = ""
    mask_image_path: str = ""
    size: str = ""
    quality: str = ""
    output_format: str = ""
    output_dir: str = ""
    n: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ImageTaskRequest":
        if not isinstance(payload, Mapping):
            raise ValueError("Input payload must be a JSON object")
        return cls(
            task_id=_normalize_string(payload.get("task_id")),
            task_type=_normalize_string(payload.get("task_type")),
            target_field=_normalize_string(payload.get("target_field")),
            mode=_normalize_string(payload.get("mode")).lower(),
            prompt=_normalize_string(payload.get("prompt")),
            input_image_path=_normalize_string(payload.get("input_image_path")),
            mask_image_path=_normalize_string(payload.get("mask_image_path")),
            size=_normalize_string(payload.get("size")),
            quality=_normalize_string(payload.get("quality")).lower(),
            output_format=_normalize_string(payload.get("output_format")).lower(),
            output_dir=_normalize_string(payload.get("output_dir")),
            n=_normalize_positive_int(payload.get("n"), default=1),
            metadata=_normalize_metadata(payload.get("metadata")),
        )


@dataclass(frozen=True)
class ImageTaskResult:
    task_id: str = ""
    task_type: str = ""
    target_field: str = ""
    mode: str = ""
    status: str = "failed"
    prompt: str = ""
    output_image_paths: List[str] = field(default_factory=list)
    model: str = ""
    size: str = ""
    quality: str = ""
    output_format: str = ""
    error_message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
