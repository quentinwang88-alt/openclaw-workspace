"""voc-insight LLM 表达润色客户端 — 双模式：Codex Responses + OpenAI Chat Completions。

设计抄自 skills/creator-profile-card/app/services/llm_client.py，去掉 vision，纯文本润色。
env 三级回退：VOC_INSIGHT_LLM_* → LLM_API_* → Codex token。
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import openai

logger = logging.getLogger(__name__)

# env 三级回退：VOC_INSIGHT_LLM_* → LLM_API_* → 默认 Doubao coding 端点
LLM_API_URL = (
    os.environ.get("VOC_INSIGHT_LLM_API_URL")
    or os.environ.get("LLM_API_URL")
    or "https://ark.cn-beijing.volces.com/api/coding/v3"
)
LLM_MODEL = (
    os.environ.get("VOC_INSIGHT_LLM_MODEL")
    or os.environ.get("LLM_MODEL")
    or "Doubao-Seed-2.0-pro"
)
LLM_API_KEY = os.environ.get("VOC_INSIGHT_LLM_API_KEY") or os.environ.get("LLM_API_KEY") or ""
LLM_REASONING_EFFORT = os.environ.get("VOC_INSIGHT_LLM_REASONING_EFFORT", "")
LLM_TIMEOUT = int(os.environ.get("VOC_INSIGHT_LLM_TIMEOUT", "120"))
LLM_TEMPERATURE = float(os.environ.get("VOC_INSIGHT_LLM_TEMPERATURE", "0.4"))


def _resolve_codex_token() -> str:
    """从本地 auth 文件解析 Codex access token（gpt-5.5 线）。"""
    paths = [
        Path.home() / ".codex" / "auth.json",
        Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json",
        Path.home() / ".hermes" / "auth.json",
    ]
    for path in paths:
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        profiles = payload.get("profiles", {})
        if isinstance(profiles, dict):
            tok = profiles.get("openai-codex:default", {}).get("access", "")
            if tok:
                return str(tok)
            for k, v in profiles.items():
                if "codex" in k and isinstance(v, dict) and v.get("access"):
                    return str(v["access"])
        tokens = payload.get("tokens", {})
        if isinstance(tokens, dict):
            tok = tokens.get("access_token", "")
            if tok:
                return str(tok)
        providers = payload.get("providers", {})
        if isinstance(providers, dict):
            tok = providers.get("openai-codex", {}).get("tokens", {}).get("access_token", "")
            if tok:
                return str(tok)
        credential_pool = payload.get("credential_pool", {})
        if isinstance(credential_pool, dict):
            pool = credential_pool.get("openai-codex", [])
            if isinstance(pool, list) and pool:
                tok = pool[0].get("access_token", "")
                if tok:
                    return str(tok)
    return ""


def _is_codex_backend(url: str) -> bool:
    return "codex" in url.lower() or "chatgpt.com" in url.lower()


def _needs_proxy_bypass(url: str) -> bool:
    return "volces.com" in url.lower()


def _build_http_client(api_url: str) -> Any:
    if _needs_proxy_bypass(api_url):
        import httpx
        return httpx.Client(transport=httpx.HTTPTransport(retries=0), timeout=LLM_TIMEOUT)
    return None


class PolishLLMClient:
    """voc-insight 润色 LLM 客户端，双模式自动切换。

    - Doubao/Volcano Ark（默认，便宜）：走 chat.completions，env LLM_API_URL/KEY/MODEL
    - Codex/gpt-5.5（切高级）：走 responses API，api_key 从 Codex 登录态取
    """

    def __init__(self, api_url: Optional[str] = None, api_key: Optional[str] = None,
                 model: Optional[str] = None):
        self._api_url = api_url or LLM_API_URL
        self._model = model or LLM_MODEL
        self._is_codex = _is_codex_backend(self._api_url)

        if api_key:
            self._api_key = api_key
        elif LLM_API_KEY:
            self._api_key = LLM_API_KEY
        elif self._is_codex:
            self._api_key = _resolve_codex_token()
            if not self._api_key:
                raise RuntimeError("Codex 模式但未找到 access token")
        else:
            raise RuntimeError("非 Codex 模式需要设置 VOC_INSIGHT_LLM_API_KEY 或 LLM_API_KEY")

        self._client = openai.OpenAI(
            api_key=self._api_key,
            base_url=self._api_url,
            timeout=LLM_TIMEOUT,
            max_retries=0,
            http_client=_build_http_client(self._api_url),
        )

    @property
    def using_responses_api(self) -> bool:
        return self._is_codex

    @property
    def model(self) -> str:
        return self._model

    @property
    def api_url(self) -> str:
        return self._api_url

    def call_json(self, prompt: str, system_prompt: Optional[str] = None,
                  max_tokens: int = 2048, max_retries: int = 3) -> Dict[str, Any]:
        """调用 LLM 返回 JSON。纯文本，无图片。"""
        for attempt in range(max_retries):
            try:
                if self._is_codex:
                    text = self._call_codex(prompt, system_prompt, max_tokens)
                else:
                    text = self._call_chat(prompt, system_prompt, max_tokens)
                return self._parse_json(text)
            except Exception as e:
                logger.warning("LLM call attempt %d/%d failed: %s", attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise RuntimeError("LLM call failed after {} attempts: {}".format(max_retries, e)) from e
        return {}

    def _call_codex(self, prompt: str, system_prompt: Optional[str], max_tokens: int) -> str:
        kwargs: Dict[str, Any] = {
            "model": self._model,
            "store": False,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "max_output_tokens": max_tokens,
        }
        if system_prompt:
            kwargs["instructions"] = system_prompt
        if LLM_REASONING_EFFORT:
            kwargs["reasoning"] = {"effort": LLM_REASONING_EFFORT}
        response = self._client.responses.create(**kwargs)
        return response.output_text or ""

    def _call_chat(self, prompt: str, system_prompt: Optional[str], max_tokens: int) -> str:
        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        response = self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=LLM_TEMPERATURE,
        )
        return response.choices[0].message.content or ""

    @staticmethod
    def _parse_json(text: str) -> Dict[str, Any]:
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1).strip())
            except json.JSONDecodeError:
                pass
        brace_start = text.find("{")
        brace_end = text.rfind("}")
        if brace_start >= 0 and brace_end > brace_start:
            try:
                return json.loads(text[brace_start:brace_end + 1])
            except json.JSONDecodeError:
                pass
        raise ValueError("Failed to parse JSON from LLM output. Raw text: {}".format(text[:500]))


def get_llm_client(api_url: Optional[str] = None, api_key: Optional[str] = None,
                   model: Optional[str] = None) -> PolishLLMClient:
    return PolishLLMClient(api_url=api_url, api_key=api_key, model=model)
