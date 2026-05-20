#!/usr/bin/env python3
"""Multimodal JSON client backed by the current OpenClaw/Codex auth."""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

SKILL_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = SKILL_DIR.parents[1]
OPENAI_IMAGE_SKILL = WORKSPACE_DIR / "skills" / "openai-image"
CORE_DIR = Path(__file__).resolve().parent
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))
if str(OPENAI_IMAGE_SKILL) not in sys.path:
    sys.path.append(str(OPENAI_IMAGE_SKILL))

from app.config import resolve_codex_access_token, resolve_codex_base_url  # type: ignore  # noqa: E402
from openai import OpenAI  # noqa: E402

from json_utils import parse_json_object  # noqa: E402


class VisionJSONClient:
    """Call an OpenAI Responses-compatible model with local images and parse JSON."""

    def __init__(
        self,
        *,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: int = 180,
    ):
        self.model = model or os.environ.get("LIKEU_VISION_MODEL", "gpt-5.5")
        self.base_url = (base_url or resolve_codex_base_url()).rstrip("/")
        self.api_key = api_key or resolve_codex_access_token()
        self.timeout = timeout
        self._client: Optional[OpenAI] = None

    def call_json(self, prompt: str, image_paths: List[str], max_output_tokens: int = 3500) -> Any:
        text = self.call_text(prompt, image_paths, max_output_tokens=max_output_tokens)
        return parse_json_object(text)

    def call_text(self, prompt: str, image_paths: List[str], max_output_tokens: int = 3500) -> str:
        if not self.api_key:
            raise RuntimeError("缺少 OpenClaw/Codex access token，请先登录")
        client = self._get_client()
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for path in image_paths:
            content.append({"type": "input_image", "image_url": image_path_to_data_url(path)})
        text_chunks: List[str] = []
        fallback_text = ""
        with client.responses.stream(
            model=self.model,
            reasoning={"effort": os.environ.get("LIKEU_VISION_REASONING_EFFORT", "medium")},
            instructions=(
                "You are a precise ecommerce product-image analyst. "
                "When asked for JSON, return only valid JSON and no markdown."
            ),
            store=False,
            input=[{"role": "user", "content": content}],
        ) as stream:
            for event in stream:
                event_type = str(getattr(event, "type", "") or "")
                if event_type == "response.output_text.delta":
                    delta = str(getattr(event, "delta", "") or "")
                    if delta:
                        text_chunks.append(delta)
                elif event_type == "response.output_text.done":
                    done_text = str(getattr(event, "text", "") or "")
                    if done_text:
                        fallback_text = done_text
            response = stream.get_final_response()
        dumped = response.model_dump(mode="json")
        text = "".join(text_chunks).strip() or fallback_text.strip()
        if not text:
            text = getattr(response, "output_text", "") or extract_responses_text(dumped)
        if not text.strip():
            raise RuntimeError("Vision model returned no text")
        return text.strip()

    def _get_client(self) -> OpenAI:
        if self._client is None:
            self._client = OpenAI(api_key=self.api_key, base_url=self.base_url, timeout=self.timeout, max_retries=0)
        return self._client


def image_path_to_data_url(image_path: str) -> str:
    path = Path(image_path)
    image_bytes = path.read_bytes()
    image_base64 = base64.b64encode(image_bytes).decode("utf-8")
    image_format = path.suffix.lower().lstrip(".")
    if image_format == "jpg":
        image_format = "jpeg"
    if not image_format:
        image_format = "jpeg"
    return f"data:image/{image_format};base64,{image_base64}"


def extract_responses_text(result: Dict[str, Any]) -> str:
    output = result.get("output")
    if not isinstance(output, list):
        return ""
    parts: List[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "output_text":
                text = str(block.get("text") or "").strip()
                if text:
                    parts.append(text)
    return "\n".join(parts).strip()
