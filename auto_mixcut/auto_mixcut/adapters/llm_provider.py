from __future__ import annotations

import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from auto_mixcut.core.result import Result


@dataclass
class LLMResponse:
    text: str
    parsed: Optional[Dict[str, Any]] = None
    raw_response: Optional[Dict[str, Any]] = None
    usage: Dict[str, int] = field(default_factory=dict)
    model: str = ""
    provider_name: str = ""
    latency_ms: int = 0
    retry_count: int = 0


class LLMProvider(ABC):
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def call_text(self, prompt: str, image_paths: List[str], model: str, max_output_tokens: int, timeout_ms: int) -> LLMResponse:
        ...

    def call_json(self, prompt: str, image_paths: List[str], model: str, max_output_tokens: int = 1800, timeout_ms: int = 120000) -> LLMResponse:
        resp = self.call_text(prompt, image_paths, model, max_output_tokens, timeout_ms)
        parsed = _parse_json_safe(resp.text)
        return LLMResponse(text=resp.text, parsed=parsed, raw_response=resp.raw_response, usage=resp.usage, model=resp.model, provider_name=resp.provider_name, latency_ms=resp.latency_ms, retry_count=resp.retry_count)

    def call_audio(self, prompt: str, audio_path: str, model: str, max_output_tokens: int = 1500, timeout_ms: int = 120000) -> LLMResponse:
        raise NotImplementedError(f"{self.name()} does not support audio input")

    @staticmethod
    def create(provider_config: Dict[str, Any]) -> "LLMProvider":
        provider_type = provider_config.get("type", "mock")
        if provider_type == "openai_codex":
            return OpenAICodexProvider(provider_config)
        if provider_type == "openai_compatible":
            return OpenAICompatibleProvider(provider_config)
        if provider_type == "mock":
            return MockLLMProvider(provider_config)
        raise ValueError(f"unknown provider type: {provider_type}")


class MockLLMProvider(LLMProvider):
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._config = config or {}

    def name(self) -> str:
        return self._config.get("display_name", "mock")

    def call_text(self, prompt: str, image_paths: List[str], model: str, max_output_tokens: int, timeout_ms: int) -> LLMResponse:
        return LLMResponse(text='{"ok":true}', parsed={"ok": True}, model=model, provider_name=self.name(), latency_ms=1)


class OpenAICodexProvider(LLMProvider):
    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._client = None

    def name(self) -> str:
        return self._config.get("display_name", "openai_codex")

    def _get_client(self):
        if self._client is None:
            from auto_mixcut.adapters.vision_json import VisionJSONClient
            self._client = VisionJSONClient(
                base_url=os.environ.get(self._config.get("base_url_env", "")) or None,
                api_key=os.environ.get(self._config.get("api_key_env", "")) or None,
                timeout=int(self._config.get("default_timeout", 180)),
            )
        return self._client

    def call_text(self, prompt: str, image_paths: List[str], model: str, max_output_tokens: int, timeout_ms: int) -> LLMResponse:
        import hashlib
        input_hash = hashlib.sha256(json.dumps({"prompt": prompt, "image_count": len(image_paths)}, sort_keys=True).encode()).hexdigest()[:16]
        started = time.time()
        try:
            client = self._get_client()
            original_model = client.model
            client.model = model
            client.timeout = max(timeout_ms // 1000, 30)
            text = client.call_text(prompt, image_paths, max_output_tokens=max_output_tokens)
            client.model = original_model
            latency = int((time.time() - started) * 1000)
            return LLMResponse(text=text, model=model, provider_name=self.name(), latency_ms=latency)
        except Exception:
            latency = int((time.time() - started) * 1000)
            raise


class OpenAICompatibleProvider(LLMProvider):
    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._client = None

    def name(self) -> str:
        return self._config.get("display_name", "openai_compatible")

    def _get_client(self):
        if self._client is None:
            try:
                from openai import OpenAI
            except ImportError:
                raise RuntimeError("openai package required for OpenAICompatibleProvider")
            base_url = self._config.get("base_url", "")
            api_key = os.environ.get(self._config.get("api_key_env", ""), "")
            self._client = OpenAI(base_url=base_url, api_key=api_key, timeout=self._config.get("default_timeout", 180))
        return self._client

    def call_text(self, prompt: str, image_paths: List[str], model: str, max_output_tokens: int, timeout_ms: int) -> LLMResponse:
        started = time.time()
        client = self._get_client()
        messages: List[Dict[str, Any]] = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        for path in image_paths[:10]:
            import base64
            p = Path(path)
            suffix = p.suffix.lower().lstrip(".") or "jpeg"
            fmt = "jpeg" if suffix == "jpg" else suffix
            data_url = f"data:image/{fmt};base64,{base64.b64encode(p.read_bytes()).decode()}"
            messages[0]["content"].append({"type": "image_url", "image_url": {"url": data_url}})
        response = client.chat.completions.create(model=model, messages=messages, max_tokens=max_output_tokens)
        latency = int((time.time() - started) * 1000)
        text = response.choices[0].message.content or ""
        usage = {"input": response.usage.prompt_tokens if response.usage else 0, "output": response.usage.completion_tokens if response.usage else 0}
        return LLMResponse(text=text, model=model, provider_name=self.name(), latency_ms=latency, usage=usage)

    def call_audio(self, prompt: str, audio_path: str, model: str, max_output_tokens: int = 1500, timeout_ms: int = 120000) -> LLMResponse:
        import base64
        started = time.time()
        client = self._get_client()
        p = Path(audio_path)
        suffix = p.suffix.lower().lstrip(".") or "mp3"
        audio_b64 = base64.b64encode(p.read_bytes()).decode()
        messages: List[Dict[str, Any]] = [{"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "input_audio", "input_audio": {"data": audio_b64, "format": suffix}},
        ]}]
        response = client.chat.completions.create(model=model, messages=messages, max_tokens=max_output_tokens, timeout=timeout_ms / 1000.0)
        latency = int((time.time() - started) * 1000)
        text = response.choices[0].message.content or ""
        usage = {"input": response.usage.prompt_tokens if response.usage else 0, "output": response.usage.completion_tokens if response.usage else 0}
        return LLMResponse(text=text, model=model, provider_name=self.name(), latency_ms=latency, usage=usage)


def _parse_json_safe(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    import re
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        try:
            return json.loads(fenced.group(1).strip())
        except json.JSONDecodeError:
            pass
    start_candidates = [idx for idx in (raw.find("{"), raw.find("[")) if idx >= 0]
    if not start_candidates:
        return None
    start = min(start_candidates)
    end = max(raw.rfind("}"), raw.rfind("]"))
    if end <= start:
        return None
    try:
        return json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        return None
