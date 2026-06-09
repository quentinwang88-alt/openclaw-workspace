from __future__ import annotations

import base64
import json
import os
import re
from pathlib import Path
from typing import Any

import httpx
from openai import OpenAI


DEFAULT_BASE_URL = "https://chatgpt.com/backend-api/codex"
DEFAULT_MODEL = "gpt-5.5"


class VisionJSONClient:
    def __init__(self, model: str | None = None, base_url: str | None = None, api_key: str | None = None, timeout: int = 180):
        self.model = model or os.environ.get("AUTO_MIXCUT_VISION_MODEL", DEFAULT_MODEL)
        self.base_url = (base_url or _resolve_codex_base_url()).rstrip("/")
        self.api_key = api_key or _resolve_codex_access_token()
        self.timeout = timeout
        self._client: OpenAI | None = None

    def call_json(self, prompt: str, image_paths: list[str], max_output_tokens: int = 1800) -> Any:
        text = self.call_text(prompt, image_paths, max_output_tokens=max_output_tokens)
        return parse_json(text)

    def call_text(self, prompt: str, image_paths: list[str], max_output_tokens: int = 1800) -> str:
        if not self.api_key:
            raise RuntimeError("缺少 OpenClaw/Codex access token，请先登录 OpenClaw/Codex")
        content: list[dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for index, path in enumerate(image_paths, 1):
            content.append({"type": "input_text", "text": f"Frame {index}:"})
            content.append({"type": "input_image", "image_url": image_path_to_data_url(path)})
        return self._call_content(content, max_output_tokens=max_output_tokens)

    def call_audio(self, prompt: str, audio_path: str, max_output_tokens: int = 1500) -> str:
        if not self.api_key:
            raise RuntimeError("缺少 OpenClaw/Codex access token，请先登录 OpenClaw/Codex")
        content: list[dict[str, Any]] = [
            {"type": "input_text", "text": prompt},
            {"type": "input_audio", "input_audio": audio_path_to_input_audio(audio_path)},
        ]
        return self._call_content(content, max_output_tokens=max_output_tokens)

    def _call_content(self, content: list[dict[str, Any]], max_output_tokens: int = 1800) -> str:
        text_chunks: list[str] = []
        fallback_text = ""
        try:
            with self._get_client().responses.stream(
                model=self.model,
                reasoning={"effort": os.environ.get("AUTO_MIXCUT_VISION_REASONING_EFFORT", "medium")},
                instructions="You are a precise ecommerce short-video segment analyst. Return only valid JSON when requested.",
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
                        text = fallback_text.strip() or "".join(text_chunks).strip()
                        if text:
                            return text
        except TypeError as exc:
            if "NoneType" not in str(exc):
                raise
            text = fallback_text.strip() or "".join(text_chunks).strip()
            if text:
                return text
            raise
        text = fallback_text.strip() or "".join(text_chunks).strip()
        if not text:
            raise RuntimeError("Vision model returned no text")
        return text

    def _get_client(self) -> OpenAI:
        if self._client is None:
            self._client = OpenAI(api_key=self.api_key, base_url=self.base_url, timeout=self.timeout, max_retries=0, http_client=_http_client())
        return self._client


def image_path_to_data_url(image_path: str) -> str:
    path = Path(image_path)
    image_format = path.suffix.lower().lstrip(".") or "jpeg"
    if image_format == "jpg":
        image_format = "jpeg"
    return f"data:image/{image_format};base64,{base64.b64encode(path.read_bytes()).decode('utf-8')}"


def audio_path_to_input_audio(audio_path: str) -> dict[str, str]:
    path = Path(audio_path)
    audio_format = path.suffix.lower().lstrip(".") or "mp3"
    if audio_format == "m4a":
        audio_format = "mp4"
    return {"data": base64.b64encode(path.read_bytes()).decode("utf-8"), "format": audio_format}


def parse_json(text: str) -> Any:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("empty JSON text")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return json.loads(fenced.group(1).strip())
    start_candidates = [idx for idx in (raw.find("{"), raw.find("[")) if idx >= 0]
    if not start_candidates:
        raise ValueError("no JSON object found")
    start = min(start_candidates)
    end = max(raw.rfind("}"), raw.rfind("]"))
    if end <= start:
        raise ValueError("truncated JSON text")
    return json.loads(raw[start : end + 1])


def _http_client() -> httpx.Client | None:
    proxy = os.environ.get("ALL_PROXY") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "socks5://127.0.0.1:10808"
    try:
        return httpx.Client(proxy=proxy, timeout=httpx.Timeout(180.0, connect=20.0))
    except Exception:
        return None


def _resolve_codex_access_token() -> str:
    for path in [
        Path(os.environ.get("OPENCLAW_AGENT_AUTH_PROFILE_PATH", str(Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"))),
        Path(os.environ.get("CODEX_AUTH_PATH", str(Path.home() / ".codex" / "auth.json"))),
        Path(os.environ.get("HERMES_AUTH_PATH", str(Path.home() / ".hermes" / "auth.json"))),
    ]:
        token = _extract_token(path)
        if token:
            return token
    return ""


def _extract_token(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if path.name == "auth-profiles.json":
        profile = ((payload.get("profiles") or {}).get("openai-codex:default") or {})
        return str(profile.get("access") or "").strip()
    tokens = payload.get("tokens")
    if isinstance(tokens, dict) and tokens.get("access_token"):
        return str(tokens.get("access_token") or "").strip()
    providers = payload.get("providers")
    if isinstance(providers, dict):
        provider = providers.get("openai-codex")
        if isinstance(provider, dict):
            provider_tokens = provider.get("tokens")
            if isinstance(provider_tokens, dict) and provider_tokens.get("access_token"):
                return str(provider_tokens.get("access_token") or "").strip()
    pool = (payload.get("credential_pool") or {}).get("openai-codex") if isinstance(payload.get("credential_pool"), dict) else []
    if isinstance(pool, list):
        for item in pool:
            if isinstance(item, dict) and item.get("access_token"):
                return str(item.get("access_token") or "").strip()
    return ""


def _resolve_codex_base_url() -> str:
    env_value = str(os.environ.get("OPENAI_CODEX_BASE_URL", "") or "").strip()
    if env_value:
        return env_value.rstrip("/")
    config_path = Path(os.environ.get("OPENCLAW_CONFIG_PATH", str(Path.home() / ".openclaw" / "openclaw.json")))
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        codex = (((payload.get("models") or {}).get("providers") or {}).get("openai-codex") or {})
        base_url = str(codex.get("baseUrl") or "").strip()
        if base_url:
            return base_url.rstrip("/")
    except Exception:
        pass
    return DEFAULT_BASE_URL
