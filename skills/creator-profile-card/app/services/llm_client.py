"""
LLM 调用客户端 — 双模式：Codex Responses API + OpenAI Chat Completions API。
"""
import base64
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import openai

from ..config import LLM_API_URL, LLM_API_KEY, LLM_MODEL, LLM_REASONING_EFFORT, LLM_TIMEOUT

logger = logging.getLogger(__name__)


def _resolve_codex_token() -> str:
    """从本地 auth 文件解析 Codex access token。"""
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
        # auth-profiles format
        profiles = payload.get("profiles", {})
        if isinstance(profiles, dict):
            tok = profiles.get("openai-codex:default", {}).get("access", "")
            if tok:
                return str(tok)
            for k, v in profiles.items():
                if "codex" in k and isinstance(v, dict) and v.get("access"):
                    return str(v["access"])
        # codex CLI format
        tokens = payload.get("tokens", {})
        if isinstance(tokens, dict):
            tok = tokens.get("access_token", "")
            if tok:
                return str(tok)
        # hermes providers format
        providers = payload.get("providers", {})
        if isinstance(providers, dict):
            tok = providers.get("openai-codex", {}).get("tokens", {}).get("access_token", "")
            if tok:
                return str(tok)
        # hermes credential_pool format
        credential_pool = payload.get("credential_pool", {})
        if isinstance(credential_pool, dict):
            pool = credential_pool.get("openai-codex", [])
            if isinstance(pool, list) and pool:
                tok = pool[0].get("access_token", "")
                if tok:
                    return str(tok)
    return ""


def _image_to_data_url(image_path: Path) -> str:
    """将图片文件转为 base64 data URL。"""
    data = image_path.read_bytes()
    ext = image_path.suffix.lower()
    mime = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "webp": "webp"}.get(ext.lstrip("."), "jpeg")
    b64 = base64.b64encode(data).decode()
    return f"data:image/{mime};base64,{b64}"


def _is_codex_backend(url: str) -> bool:
    """判断是否为 Codex/Responses API 后端。"""
    return "codex" in url.lower() or "chatgpt.com" in url.lower()


def _needs_proxy_bypass(url: str) -> bool:
    """某些 API 从本机通过代理无法访问，需要直连。"""
    return "volces.com" in url.lower()


def _build_http_client(api_url: str) -> Any:
    """构建 httpx 客户端，volces API 强制直连绕过系统代理。"""
    if _needs_proxy_bypass(api_url):
        import httpx
        return httpx.Client(transport=httpx.HTTPTransport(retries=0), timeout=LLM_TIMEOUT)
    return None  # 使用 openai 默认行为（尊重系统代理）


class ProfileLLMClient:
    """达人画像卡 LLM 客户端，支持双模式自动切换。"""

    def __init__(self, api_url: Optional[str] = None, api_key: Optional[str] = None,
                 model: Optional[str] = None):
        self._api_url = api_url or LLM_API_URL
        self._model = model or LLM_MODEL
        self._is_codex = _is_codex_backend(self._api_url)

        # 确定 api_key
        if api_key:
            self._api_key = api_key
        elif LLM_API_KEY:
            self._api_key = LLM_API_KEY
        elif self._is_codex:
            self._api_key = _resolve_codex_token()
            if not self._api_key:
                raise RuntimeError("Codex 模式但未找到 access token")
        else:
            raise RuntimeError("非 Codex 模式需要设置 CREATOR_PROFILE_LLM_API_KEY 或 LLM_API_KEY")

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

    def call_json(
        self,
        prompt: str,
        image_paths: Optional[List[str]] = None,
        system_prompt: Optional[str] = None,
        max_tokens: int = 4096,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """调用 LLM 并返回解析后的 JSON。"""
        image_paths = image_paths or []

        for attempt in range(max_retries):
            try:
                if self._is_codex:
                    text = self._call_codex(prompt, image_paths, system_prompt, max_tokens)
                else:
                    text = self._call_chat(prompt, image_paths, system_prompt, max_tokens)
                return self._parse_json(text)
            except Exception as e:
                logger.warning("LLM call attempt %d/%d failed: %s", attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise RuntimeError(f"LLM call failed after {max_retries} attempts") from e
        return {}

    def _call_codex(self, prompt: str, image_paths: List[str],
                    system_prompt: Optional[str], max_tokens: int) -> str:
        """Codex Responses API 模式。"""
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for img_path in image_paths:
            p = Path(img_path)
            if p.exists():
                content.append({
                    "type": "input_image",
                    "image_url": _image_to_data_url(p),
                })

        kwargs = {
            "model": self._model,
            "store": False,
            "input": [{"role": "user", "content": content}],
            "max_output_tokens": max_tokens,
        }
        if system_prompt:
            kwargs["instructions"] = system_prompt
        if LLM_REASONING_EFFORT:
            kwargs["reasoning"] = {"effort": LLM_REASONING_EFFORT}

        response = self._client.responses.create(**kwargs)
        return response.output_text or ""

    def _call_chat(self, prompt: str, image_paths: List[str],
                   system_prompt: Optional[str], max_tokens: int) -> str:
        """OpenAI Chat Completions 模式。"""
        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        # 构建用户消息（支持多模态）
        user_content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
        for img_path in image_paths:
            p = Path(img_path)
            if p.exists():
                user_content.append({
                    "type": "image_url",
                    "image_url": {"url": _image_to_data_url(p)},
                })
        messages.append({"role": "user", "content": user_content})

        response = self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=0.3,
        )
        return response.choices[0].message.content or ""

    @staticmethod
    def _parse_json(text: str) -> Dict[str, Any]:
        """从 LLM 输出中提取 JSON。"""
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        m = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
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
        raise ValueError(f"Failed to parse JSON from LLM output. Raw text: {text[:500]}")


def get_llm_client(
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> ProfileLLMClient:
    """工厂函数：创建 LLM 客户端（默认使用 .env 配置）。"""
    return ProfileLLMClient(api_url=api_url, api_key=api_key, model=model)
