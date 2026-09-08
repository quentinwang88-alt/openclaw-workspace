"""Codex Responses API vision client (planning/QA route, e.g. gpt-5.6-sol).

Contract-compatible with ``_DoubaoVisionClient``: ``chat_with_multiple_images``
returns a ``{"choices": [{"message": {"content": ...}}]}`` envelope so
``parse_json_response`` behaves identically across providers.

Codex hard constraints (verified 2026-09-07): ``stream=true``, ``store=false``,
no ``max_output_tokens``, no ``response_format``; JSON discipline is enforced
via instructions and the shared tolerant JSON parsing.
"""
from __future__ import annotations

import base64
import json
import mimetypes
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import httpx

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
OPENAI_IMAGE_SKILL_DIR = WORKSPACE_ROOT / "skills" / "openai-image"

DEFAULT_REASONING_EFFORT = "medium"


class CodexVisionError(RuntimeError):
    pass


def resolve_codex_runtime() -> tuple[str, str]:
    """(access_token, base_url) from the openai-image skill's resolution chain."""
    import sys

    if str(OPENAI_IMAGE_SKILL_DIR) not in sys.path:
        sys.path.insert(0, str(OPENAI_IMAGE_SKILL_DIR))
    try:
        from app.config import resolve_codex_access_token, resolve_codex_base_url
    except ModuleNotFoundError:
        # Another package claimed the "app" root names; purge and re-import.
        for root in ("app",):
            for name in [m for m in list(sys.modules)
                         if m == root or m.startswith(root + ".")]:
                del sys.modules[name]
        from app.config import resolve_codex_access_token, resolve_codex_base_url
    return resolve_codex_access_token(), resolve_codex_base_url()


class CodexVisionClient:
    def __init__(self, *, model: str, reasoning_effort: str = DEFAULT_REASONING_EFFORT,
                 timeout: int = 180, access_token: str = "", base_url: str = "",
                 max_attempts: int = 2):
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout = timeout
        self.max_attempts = max(1, int(max_attempts))
        if not access_token or not base_url:
            resolved_token, resolved_base = resolve_codex_runtime()
            self.access_token = access_token or resolved_token
            self.base_url = (base_url or resolved_base).rstrip("/")
        else:
            self.access_token = access_token
            self.base_url = base_url.rstrip("/")
        if not self.access_token:
            raise CodexVisionError("codex access token 未配置")

    # ------------------------------------------------------------------
    # Public API (Doubao-compatible envelope)
    # ------------------------------------------------------------------
    def chat_with_multiple_images(self, paths: Sequence[str], prompt: str,
                                  max_tokens: Optional[int] = None,
                                  instructions: str = "") -> Dict[str, Any]:
        """Return ``{"choices": [{"message": {"content": text}}]}``."""
        if not paths and not prompt:
            raise CodexVisionError("codex 视觉调用缺少输入")
        content: List[Dict[str, Any]] = [
            {"type": "input_image", "image_url": self._data_url(path)}
            for path in paths
        ]
        content.append({"type": "input_text", "text": prompt})
        payload: Dict[str, Any] = {
            "model": self.model,
            "instructions": instructions or "你是严谨的视觉分析与质检引擎，只输出符合要求的 JSON 对象，不要输出 Markdown 或解释文字。",
            "input": [{"role": "user", "content": content}],
            "reasoning": {"effort": self.reasoning_effort},
            "store": False,
            "stream": True,
        }
        text = self._stream_with_retry(payload)
        return {"choices": [{"message": {"content": text}}]}

    @staticmethod
    def parse_json_response(response: Dict[str, Any]) -> Dict[str, Any]:
        from services.photo_reference_vision import parse_vision_envelope
        try:
            return parse_vision_envelope(response)
        except Exception as exc:
            raise CodexVisionError(str(exc)) from exc

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _data_url(path: str) -> str:
        source = Path(path)
        mime = mimetypes.guess_type(source.name)[0] or "image/jpeg"
        encoded = base64.b64encode(source.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{encoded}"

    def _stream_with_retry(self, payload: Dict[str, Any]) -> str:
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt < self.max_attempts:
            attempt += 1
            try:
                return self._stream_once(payload)
            except CodexVisionError as exc:
                last_error = exc
                if not self._retriable(exc):
                    raise
                if attempt < self.max_attempts:
                    time.sleep(2)
        raise last_error or CodexVisionError("codex 视觉调用失败")

    @staticmethod
    def _retriable(exc: CodexVisionError) -> bool:
        return str(exc).startswith(("codex HTTP 429", "codex HTTP 5", "codex 网络调用失败"))

    def _stream_once(self, payload: Dict[str, Any]) -> str:
        text_parts: List[str] = []
        try:
            with httpx.stream(
                "POST", f"{self.base_url}/responses",
                headers={"Authorization": f"Bearer {self.access_token}",
                         "Content-Type": "application/json"},
                json=payload, timeout=self.timeout,
            ) as response:
                if response.status_code != 200:
                    body = response.read().decode("utf-8", errors="replace")[:300]
                    raise CodexVisionError(f"codex HTTP {response.status_code}: {body}")
                for line in response.iter_lines():
                    if not line.startswith("data: "):
                        continue
                    chunk = line[6:]
                    if chunk == "[DONE]":
                        break
                    try:
                        event = json.loads(chunk)
                    except json.JSONDecodeError:
                        continue
                    event_type = event.get("type") or ""
                    if event_type == "response.output_text.delta":
                        text_parts.append(event.get("delta") or "")
                    elif event_type in ("response.failed", "response.incomplete"):
                        raise CodexVisionError(
                            f"codex HTTP 5xx: response {event_type}"
                        )
        except httpx.HTTPError as exc:
            raise CodexVisionError(f"codex 网络调用失败：{exc}") from exc
        text = "".join(text_parts).strip()
        if not text:
            raise CodexVisionError("codex HTTP 5xx: empty stream output")
        return text
