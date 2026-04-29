#!/usr/bin/env python3
"""OpenAI Image API wrapper for the openai-image skill.

Supports two API modes:
  - "codex" (default): Uses OpenAI Responses API via chatgpt.com/backend-api/codex
    with SOCKS5 proxy support via httpx.
  - "openai" (legacy): Uses the standard OpenAI Images API via api.openai.com.
"""

from __future__ import annotations

import base64
import io
import json
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from app.config import Settings


class OpenAIImageClient:
    """Thin wrapper around the OpenAI Images API with codex+SOCKS5 support."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._client: Optional[Any] = None
        self._httpx_client: Optional[Any] = None

    # ------------------------------------------------------------------
    # Legacy OpenAI Python SDK path
    # ------------------------------------------------------------------

    def _get_client(self) -> Any:
        if self._client is None:
            from openai import OpenAI

            if not self.settings.openai_api_key:
                raise ValueError("OPENAI_API_KEY is missing")
            self._client = OpenAI(
                api_key=self.settings.openai_api_key,
                base_url=self.settings.openai_base_url,
                timeout=self.settings.timeout_seconds,
                max_retries=0,
            )
        return self._client

    def _should_retry(self, exc: Exception) -> bool:
        from openai import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError

        if isinstance(exc, (APITimeoutError, APIConnectionError, RateLimitError)):
            return True
        if isinstance(exc, APIStatusError):
            status_code = getattr(exc, "status_code", None)
            if status_code in {408, 409, 429}:
                return True
            if isinstance(status_code, int) and status_code >= 500:
                return True
            return False
        return True

    def with_retry(self, action_name: str, operation: Callable[[], Any]) -> Any:
        attempts = max(int(self.settings.max_retries), 0) + 1
        last_error: Optional[Exception] = None

        for attempt_index in range(attempts):
            try:
                return operation()
            except Exception as exc:
                last_error = exc
                if attempt_index >= attempts - 1 or not self._should_retry(exc):
                    raise
                wait_seconds = 2 ** attempt_index
                print(
                    f"[openai-image] {action_name} failed ({exc}); retrying in {wait_seconds}s "
                    f"({attempt_index + 1}/{attempts - 1})..."
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"{action_name} failed: {last_error}")

    # ------------------------------------------------------------------
    # Legacy: standard OpenAI Images API
    # ------------------------------------------------------------------

    def generate_image(
        self,
        *,
        prompt: str,
        size: str,
        quality: str,
        output_format: str,
        n: int = 1,
    ) -> Dict[str, Any]:
        if self.settings.api_mode == "codex":
            return self._generate_via_codex(prompt=prompt, size=size, quality=quality, output_format=output_format, n=n)

        client = self._get_client()
        response = self.with_retry(
            "generate_image",
            lambda: client.images.generate(
                model=self.settings.image_model,
                prompt=prompt,
                size=size,
                quality=quality,
                output_format=output_format,
                n=max(int(n), 1),
                response_format="b64_json",
                timeout=self.settings.timeout_seconds,
            ),
        )
        return self._normalize_response(response)

    def edit_image(
        self,
        *,
        prompt: str,
        input_image_path: str,
        mask_image_path: str,
        size: str,
        quality: str,
        output_format: str,
        n: int = 1,
    ) -> Dict[str, Any]:
        if self.settings.api_mode == "codex":
            return self._edit_via_codex(
                prompt=prompt,
                input_image_path=input_image_path,
                mask_image_path=mask_image_path,
                size=size,
                quality=quality,
                output_format=output_format,
                n=n,
            )

        client = self._get_client()

        def _operation() -> Any:
            with ExitStack() as stack:
                image_file = stack.enter_context(open(input_image_path, "rb"))
                kwargs: Dict[str, Any] = {
                    "model": self.settings.image_model,
                    "image": image_file,
                    "prompt": prompt,
                    "size": size,
                    "quality": quality,
                    "output_format": output_format,
                    "n": max(int(n), 1),
                    "response_format": "b64_json",
                    "timeout": self.settings.timeout_seconds,
                }
                if mask_image_path:
                    kwargs["mask"] = stack.enter_context(open(mask_image_path, "rb"))
                return client.images.edit(**kwargs)

        response = self.with_retry("edit_image", _operation)
        return self._normalize_response(response)

    # ------------------------------------------------------------------
    # New: Codex Responses API path with SOCKS5 proxy
    # ------------------------------------------------------------------

    def _get_httpx_client(self) -> Any:
        """Create or return a httpx client with optional SOCKS5 proxy."""
        if self._httpx_client is not None:
            return self._httpx_client

        import httpx

        proxy_url = self.settings.socks5_proxy
        if proxy_url:
            try:
                import httpx_socks  # noqa: F401 - verify availability

                from httpx_socks import SyncProxyTransport

                # httpx_socks supports socks5:// URLs directly
                transport = SyncProxyTransport.from_url(proxy_url)
                self._httpx_client = httpx.Client(
                    transport=transport,
                    timeout=self.settings.timeout_seconds,
                )
                print(f"[openai-image] Using SOCKS5 proxy: {proxy_url}")
                return self._httpx_client
            except ImportError:
                print(
                    "[openai-image] WARNING: httpx-socks not installed. "
                    "SOCKS5 proxy not available. Install with: pip install httpx-socks"
                )
                # Fall through to no-proxy httpx client

        # Check for HTTP_PROXY / HTTPS_PROXY as fallback
        http_proxy = _env_proxy()
        if http_proxy:
            self._httpx_client = httpx.Client(
                proxy=http_proxy,
                timeout=self.settings.timeout_seconds,
            )
            print(f"[openai-image] Using HTTP proxy: {http_proxy}")
            return self._httpx_client

        self._httpx_client = httpx.Client(timeout=self.settings.timeout_seconds)
        return self._httpx_client

    def _generate_via_codex(
        self,
        *,
        prompt: str,
        size: str,
        quality: str,
        output_format: str,
        n: int = 1,
    ) -> Dict[str, Any]:
        """Generate image via the Codex Responses API (SSE streaming)."""
        if not self.settings.codex_api_key:
            raise ValueError(
                "Codex API key not found. Please login to OpenClaw/Hermes or set "
                "ORIGINAL_SCRIPT_PRIMARY_LLM_API_KEY env var."
            )

        client = self._get_httpx_client()
        url = f"{self.settings.codex_base_url}/responses"

        headers = {
            "Authorization": f"Bearer {self.settings.codex_api_key}",
            "Content-Type": "application/json",
        }

        input_content: List[Dict[str, Any]] = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt,
                    }
                ],
            }
        ]

        tool_config: Dict[str, Any] = {
            "type": "image_generation",
            "quality": quality,
            "size": size,
            "output_format": output_format,
        }

        body: Dict[str, Any] = {
            "model": self.settings.codex_model,
            "instructions": self.settings.codex_instructions,
            "input": input_content,
            "tools": [tool_config],
            "stream": True,
            "store": False,
        }

        def _do_request() -> Dict[str, Any]:
            return self._send_codex_streaming_request(client, url, headers, body)

        result = self.with_retry("generate_via_codex", _do_request)
        return self._parse_codex_image_response(result)

    def _edit_via_codex(
        self,
        *,
        prompt: str,
        input_image_path: str,
        mask_image_path: str,
        size: str,
        quality: str,
        output_format: str,
        n: int = 1,
    ) -> Dict[str, Any]:
        """Edit image via the Codex Responses API (SSE streaming)."""
        if not self.settings.codex_api_key:
            raise ValueError(
                "Codex API key not found. Please login to OpenClaw/Hermes or set "
                "ORIGINAL_SCRIPT_PRIMARY_LLM_API_KEY env var."
            )

        client = self._get_httpx_client()
        url = f"{self.settings.codex_base_url}/responses"

        headers = {
            "Authorization": f"Bearer {self.settings.codex_api_key}",
            "Content-Type": "application/json",
        }

        # Build content with input image
        image_data_url = _image_path_to_data_url(input_image_path)
        content_items: List[Dict[str, Any]] = [
            {"type": "input_text", "text": prompt},
            {"type": "input_image", "image_url": image_data_url},
        ]

        # If mask provided, add it as another input image
        if mask_image_path:
            mask_data_url = _image_path_to_data_url(mask_image_path)
            content_items.append({"type": "input_image", "image_url": mask_data_url})

        input_content = [
            {
                "role": "user",
                "content": content_items,
            }
        ]

        tool_config: Dict[str, Any] = {
            "type": "image_generation",
            "quality": quality,
            "size": size,
            "output_format": output_format,
        }

        body: Dict[str, Any] = {
            "model": self.settings.codex_model,
            "instructions": self.settings.codex_instructions,
            "input": input_content,
            "tools": [tool_config],
            "stream": True,
            "store": False,
        }

        def _do_request() -> Dict[str, Any]:
            return self._send_codex_streaming_request(client, url, headers, body)

        result = self.with_retry("edit_via_codex", _do_request)
        return self._parse_codex_image_response(result)

    def _send_codex_streaming_request(
        self,
        client: Any,
        url: str,
        headers: Dict[str, str],
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Send a streaming request to the Codex Responses API and collect the full response.

        The Codex API requires ``stream: True`` and returns Server-Sent Events.
        We accumulate the response from the ``response.completed`` event which
        contains the full output including image data.
        """
        with client.stream("POST", url, json=body, headers=headers) as resp:
            if resp.status_code != 200:
                # Read the error body for a useful message
                error_body = resp.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Codex API returned {resp.status_code}: {error_body[:500]}"
                )
            completed_event: Optional[Dict[str, Any]] = None
            for line in resp.iter_lines():
                if not line or not line.startswith("data: "):
                    continue
                data_str = line[6:]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    event = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                if event.get("type") == "response.completed":
                    completed_event = event

        if completed_event is None:
            raise RuntimeError(
                "Codex streaming response ended without a 'response.completed' event"
            )
        # Return the response object which has the same shape as the non-streaming response
        return completed_event.get("response", completed_event)

    @staticmethod
    def _parse_codex_image_response(result: Dict[str, Any]) -> Dict[str, Any]:
        """Parse the Codex Responses API output to match our normalized format."""
        normalized: List[Dict[str, Any]] = []
        output = result.get("output") or []

        for item in output:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "")
            if item_type != "image_generation_call":
                continue
            b64_json = str(item.get("result") or "").strip()
            if b64_json:
                normalized.append({
                    "b64_json": b64_json,
                    "url": None,
                    "revised_prompt": None,
                })

        # Fallback: check content blocks in output
        if not normalized:
            for item in output:
                if not isinstance(item, dict):
                    continue
                content = item.get("content") or []
                if not isinstance(content, list):
                    continue
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") == "image":
                        b64_json = str(block.get("image_url") or block.get("data") or "").strip()
                        if b64_json and not b64_json.startswith("http"):
                            normalized.append({
                                "b64_json": b64_json,
                                "url": None,
                                "revised_prompt": None,
                            })
                        elif b64_json.startswith("http"):
                            normalized.append({
                                "b64_json": None,
                                "url": b64_json,
                                "revised_prompt": None,
                            })

        return {
            "created": result.get("created"),
            "data": normalized,
        }

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_response(response: Any) -> Dict[str, Any]:
        normalized: List[Dict[str, Any]] = []
        for item in getattr(response, "data", []) or []:
            normalized.append(
                {
                    "b64_json": getattr(item, "b64_json", None),
                    "url": getattr(item, "url", None),
                    "revised_prompt": getattr(item, "revised_prompt", None),
                }
            )
        return {
            "created": getattr(response, "created", None),
            "data": normalized,
        }


def _env_proxy() -> str:
    """Return the first available HTTP(S) proxy from environment variables."""
    import os

    for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        value = str(os.environ.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _image_path_to_data_url(image_path: str) -> str:
    """Convert a local image file to a data: URL for the API."""
    image_bytes = Path(image_path).read_bytes()
    image_base64 = base64.b64encode(image_bytes).decode("utf-8")
    image_format = Path(image_path).suffix.lower().lstrip(".")
    if image_format == "jpg":
        image_format = "jpeg"
    mime_type = f"image/{image_format}"
    return f"data:{mime_type};base64,{image_base64}"
