#!/usr/bin/env python3
"""
原创脚本生成 LLM 客户端。

当前主线路默认对齐 OpenClaw / Hermes 主 agent：
- primary: 只走 openai-codex / gpt-5.4
- backup: 只走 Yunwu GPT-5.4
- gemini: 只走 Gemini 3.1 Pro（仅手动调试保留）
- auto: 默认优先 primary，失败时自动切到 backup
"""

import base64
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests
from openai import OpenAI

from core.json_parser import JSONParseError, parse_json_text


PRIMARY_ROUTE = "primary"
BACKUP_ROUTE = "backup"
GEMINI_ROUTE = "gemini"
AUTO_ROUTE = "auto"
SUPPORTED_ROUTES = {PRIMARY_ROUTE, BACKUP_ROUTE, GEMINI_ROUTE, AUTO_ROUTE}


def normalize_route_order(route_order: Optional[Any]) -> Optional[List[str]]:
    if route_order is None:
        return None
    if isinstance(route_order, str):
        raw_items = [item.strip().lower() for item in route_order.split(",")]
    elif isinstance(route_order, list):
        raw_items = [str(item or "").strip().lower() for item in route_order]
    else:
        raise ValueError(f"不支持的 llm route order: {route_order}")

    normalized: List[str] = []
    for item in raw_items:
        if not item:
            continue
        if item not in SUPPORTED_ROUTES or item == AUTO_ROUTE:
            raise ValueError(f"llm route order 中存在不支持的线路: {item}")
        if item not in normalized:
            normalized.append(item)
    return normalized or None

BACKUP_LLM_API_URL = os.environ.get("ORIGINAL_SCRIPT_BACKUP_LLM_API_URL", "https://yunwu.ai/v1")
BACKUP_LLM_MODEL = os.environ.get("ORIGINAL_SCRIPT_BACKUP_LLM_MODEL", "gpt-5.4")
BACKUP_LLM_API_KEY = os.environ.get(
    "ORIGINAL_SCRIPT_BACKUP_LLM_API_KEY",
    "sk-amhH6Lm8iWryav8lWyhw9WG4D90RF2PlkXhXwlcgNPXBcECh",
)
GEMINI_LLM_API_URL = os.environ.get(
    "ORIGINAL_SCRIPT_GEMINI_API_URL",
    "https://yunwu.ai/v1",
)
GEMINI_LLM_MODEL = os.environ.get("ORIGINAL_SCRIPT_GEMINI_LLM_MODEL", "gemini-3.1-pro-preview")
GEMINI_LLM_API_KEY = os.environ.get(
    "ORIGINAL_SCRIPT_GEMINI_LLM_API_KEY",
    "sk-HA5F4HQ686iQdiEHDC2n0mdfRWU6k82fuOYlomIu6Sj1DQKK",
)
PRIMARY_LLM_DEFAULT_API_URL = "https://chatgpt.com/backend-api/codex"
PRIMARY_LLM_DEFAULT_MODEL = "gpt-5.4"
PRIMARY_LLM_REASONING_EFFORT = os.environ.get("ORIGINAL_SCRIPT_PRIMARY_REASONING_EFFORT", "high")
OPENCLAW_CONFIG_PATH = Path(
    os.environ.get("OPENCLAW_CONFIG_PATH", str(Path.home() / ".openclaw" / "openclaw.json"))
)
HERMES_CONFIG_PATH = Path(
    os.environ.get("HERMES_CONFIG_PATH", str(Path.home() / ".hermes" / "config.yaml"))
)
HERMES_AUTH_PATH = Path(
    os.environ.get("HERMES_AUTH_PATH", str(Path.home() / ".hermes" / "auth.json"))
)


def _safe_read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _extract_openclaw_primary_model() -> str:
    payload = _safe_read_json(OPENCLAW_CONFIG_PATH)
    agents = payload.get("agents") if isinstance(payload, dict) else {}
    selected = ""
    if isinstance(agents, dict):
        agent_list = agents.get("list")
        if isinstance(agent_list, list):
            for item in agent_list:
                if isinstance(item, dict) and item.get("default") is True:
                    selected = str((item.get("model") or {}).get("primary") or "").strip()
                    break
        if not selected:
            selected = str((agents.get("defaults") or {}).get("model", {}).get("primary") or "").strip()
    if selected.startswith("openai-codex/"):
        model = selected.split("/", 1)[1].strip()
        if model:
            return model
    return ""


def _extract_hermes_model_config() -> Dict[str, str]:
    text = _safe_read_text(HERMES_CONFIG_PATH)
    if not text:
        return {}

    in_model_block = False
    config: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not in_model_block:
            if stripped == "model:":
                in_model_block = True
            continue
        if not raw_line.startswith("  "):
            break
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        config[key.strip()] = value.strip().strip('"').strip("'")
    return config


def _extract_codex_access_token() -> str:
    payload = _safe_read_json(HERMES_AUTH_PATH)
    providers = payload.get("providers") if isinstance(payload, dict) else {}
    provider = providers.get("openai-codex") if isinstance(providers, dict) else {}
    if isinstance(provider, dict):
        tokens = provider.get("tokens")
        if isinstance(tokens, dict):
            access_token = str(tokens.get("access_token") or "").strip()
            if access_token:
                return access_token
    credential_pool = payload.get("credential_pool") if isinstance(payload, dict) else {}
    pool = credential_pool.get("openai-codex") if isinstance(credential_pool, dict) else []
    if isinstance(pool, list):
        for item in pool:
            if not isinstance(item, dict):
                continue
            access_token = str(item.get("access_token") or "").strip()
            if access_token:
                return access_token
    return ""


class OriginalScriptLLMClient:
    """原创脚本生成 LLM 客户端，支持主备线路切换。"""

    def __init__(
        self,
        route: str = AUTO_ROUTE,
        primary_api_url: Optional[str] = None,
        primary_api_key: Optional[str] = None,
        primary_model: Optional[str] = None,
        backup_api_url: str = BACKUP_LLM_API_URL,
        backup_api_key: str = BACKUP_LLM_API_KEY,
        backup_model: str = BACKUP_LLM_MODEL,
        gemini_api_url: str = GEMINI_LLM_API_URL,
        gemini_api_key: str = GEMINI_LLM_API_KEY,
        gemini_model: str = GEMINI_LLM_MODEL,
        timeout: int = 120,
        max_retries: int = 2,
        route_order: Optional[List[str]] = None,
    ):
        normalized_route = str(route or AUTO_ROUTE).strip().lower()
        if normalized_route not in SUPPORTED_ROUTES:
            raise ValueError(f"不支持的 llm route: {route}")

        self.route = normalized_route
        self._primary_client: Optional[OpenAI] = None
        self.timeout = timeout
        self.max_retries = max_retries

        self.primary_api_url = self._resolve_primary_api_url(primary_api_url)
        self.primary_api_key = self._resolve_primary_api_key(primary_api_key)
        self.primary_model = self._resolve_primary_model(primary_model)
        self.backup_api_url = self._normalize_backup_api_url(backup_api_url)
        self.backup_api_key = backup_api_key
        self.backup_model = backup_model
        self.gemini_api_url = self._normalize_gemini_api_url(gemini_api_url)
        self.gemini_api_key = gemini_api_key
        self.gemini_model = gemini_model
        self._working_route: Optional[str] = None
        self.route_order = normalize_route_order(route_order)

    def _get_primary_client(self) -> OpenAI:
        if self._primary_client is None:
            if not self.primary_api_key:
                raise Exception("主线路未找到 openai-codex access token，请先登录当前 OpenClaw / Hermes 账号")
            self._primary_client = OpenAI(
                api_key=self.primary_api_key,
                base_url=self.primary_api_url,
                timeout=self.timeout,
                max_retries=0,
            )
        return self._primary_client

    def call_json(
        self,
        prompt: str,
        image_paths: Optional[List[str]] = None,
        max_tokens: int = 3000,
        max_attempts: int = 3,
        rate_limit_max_attempts: int = 4,
        validator: Optional[Callable[[Any], None]] = None,
    ) -> Any:
        last_error: Optional[Exception] = None
        last_text: str = ""
        image_paths = image_paths or []
        rate_limit_attempts = 0

        for attempt in range(max_attempts):
            prompt_to_send = prompt
            if attempt > 0:
                prompt_to_send = (
                    f"{prompt}\n\n"
                    "补充要求：你上一次返回的内容不是合法 JSON。"
                    "这一次只能输出一个合法 JSON 对象或数组，不要输出 markdown，不要输出解释，不要输出多余文本。"
                    "不要在 JSON 前后加入任何说明、前言、总结或额外换行文本。"
                )
            try:
                response = self._call_raw(prompt_to_send, image_paths=image_paths, max_tokens=max_tokens)
                text = self._extract_text(response)
                last_text = text
                parsed = parse_json_text(text)
                if validator:
                    validator(parsed)
                return parsed
            except JSONParseError as exc:
                last_error = exc
                if attempt < max_attempts - 1:
                    print(f"    ⚠️ JSON 解析失败，准备进行修复重试 ({attempt + 1}/{max_attempts - 1})...")
                    time.sleep(1)
                    continue
                if last_text.strip():
                    repaired = self._repair_json_output(
                        prompt=prompt,
                        raw_text=last_text,
                        max_tokens=max_tokens,
                    )
                    parsed = parse_json_text(repaired)
                    if validator:
                        validator(parsed)
                    return parsed
                raise
            except Exception as exc:
                last_error = exc
                if self._is_rate_limit_error(exc):
                    rate_limit_attempts += 1
                    if rate_limit_attempts <= rate_limit_max_attempts:
                        wait_time = self._rate_limit_backoff_seconds(rate_limit_attempts)
                        print(
                            f"    ⚠️ 命中模型限流，等待 {wait_time} 秒后重试 "
                            f"({rate_limit_attempts}/{rate_limit_max_attempts})..."
                        )
                        time.sleep(wait_time)
                        continue
                if attempt < max_attempts - 1:
                    print(f"    ⚠️ LLM 阶段失败，准备重试 ({attempt + 1}/{max_attempts - 1})...")
                    time.sleep(1)
                    continue
                raise Exception(f"LLM 调用最终失败: {last_error}")

        raise Exception(f"LLM 调用最终失败: {last_error}")

    def _repair_json_output(self, prompt: str, raw_text: str, max_tokens: int) -> str:
        repair_prompt = (
            "你是 JSON 修复器。"
            "请把下面这段模型输出整理成唯一合法 JSON。"
            "不要补充解释，不要输出 markdown，不要输出多余文本。"
            "若原文前面混入说明文字，请删除说明文字，只保留 JSON。"
            "若原文 JSON 末尾截断，请在不改变已有业务含义的前提下补齐为结构完整的 JSON。"
            "必须严格保持原任务需要的字段结构，不要改写字段名。\n\n"
            "原始任务提示摘要：\n"
            f"{prompt[:2000]}\n\n"
            "待修复原始输出：\n"
            f"{raw_text[:12000]}"
        )
        print("    ⚠️ 触发一次 JSON 修复调用...")
        response = self._call_raw(repair_prompt, image_paths=[], max_tokens=max(max_tokens, 5200))
        return self._extract_text(response)

    def _call_raw(
        self,
        prompt: str,
        image_paths: List[str],
        max_tokens: int,
    ) -> Dict[str, Any]:
        route_candidates = self._route_candidates()
        last_error: Optional[Exception] = None

        for index, route in enumerate(route_candidates):
            is_last = index == len(route_candidates) - 1
            try:
                print(f"    🛣️ 使用 LLM 线路: {route}")
                result = self._call_raw_via_route(route, prompt=prompt, image_paths=image_paths, max_tokens=max_tokens)
                if self.route == AUTO_ROUTE:
                    self._working_route = route
                return result
            except Exception as exc:
                last_error = exc
                if is_last or not self._is_failover_eligible_error(exc):
                    raise
                print(f"    ⚠️ LLM 线路 {route} 不可用，自动切换到下一条线路...")

        raise Exception(f"LLM 调用最终失败: {last_error}")

    def _route_candidates(self) -> List[str]:
        if self.route_order:
            return list(self.route_order)
        if self.route == GEMINI_ROUTE:
            return [GEMINI_ROUTE, PRIMARY_ROUTE, BACKUP_ROUTE]
        if self.route == BACKUP_ROUTE:
            return [BACKUP_ROUTE, PRIMARY_ROUTE]
        if self.route == PRIMARY_ROUTE:
            return [PRIMARY_ROUTE, BACKUP_ROUTE]
        if self._working_route == PRIMARY_ROUTE:
            return [PRIMARY_ROUTE, BACKUP_ROUTE]
        if self._working_route == BACKUP_ROUTE:
            return [BACKUP_ROUTE, PRIMARY_ROUTE]
        if self._working_route == GEMINI_ROUTE:
            return [GEMINI_ROUTE, PRIMARY_ROUTE, BACKUP_ROUTE]
        return [PRIMARY_ROUTE, BACKUP_ROUTE]

    def _call_raw_via_route(
        self,
        route: str,
        prompt: str,
        image_paths: List[str],
        max_tokens: int,
    ) -> Dict[str, Any]:
        if route == PRIMARY_ROUTE:
            return self._call_primary(prompt=prompt, image_paths=image_paths, max_tokens=max_tokens)
        if route == BACKUP_ROUTE:
            return self._call_backup(prompt=prompt, image_paths=image_paths, max_tokens=max_tokens)
        if route == GEMINI_ROUTE:
            return self._call_gemini(prompt=prompt, image_paths=image_paths, max_tokens=max_tokens)
        raise ValueError(f"未知的 llm route: {route}")

    def _call_primary(
        self,
        prompt: str,
        image_paths: List[str],
        max_tokens: int,
    ) -> Dict[str, Any]:
        client = self._get_primary_client()
        input_content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for image_path in image_paths:
            input_content.append(
                {
                    "type": "input_image",
                    "image_url": self._image_path_to_data_url(image_path),
                }
            )
        text_chunks: List[str] = []
        fallback_text = ""
        with client.responses.stream(
            model=self.primary_model,
            reasoning={"effort": PRIMARY_LLM_REASONING_EFFORT},
            instructions=(
                "You are a multimodal content generation worker for original short-video scripting. "
                "Follow the user prompt exactly. "
                "If the prompt asks for JSON, output only valid JSON with no extra prose."
            ),
            store=False,
            input=[{"role": "user", "content": input_content}],
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
            text = getattr(response, "output_text", "") or self._extract_openai_responses_text(dumped)
        return {
            "choices": [
                {
                    "message": {
                        "content": text,
                    }
                }
            ],
            "_raw_response": dumped,
        }

    def _call_primary_text(self, prompt: str, max_tokens: int) -> Dict[str, Any]:
        return self._call_primary(prompt=prompt, image_paths=[], max_tokens=max_tokens)

    def _call_backup(
        self,
        prompt: str,
        image_paths: List[str],
        max_tokens: int,
    ) -> Dict[str, Any]:
        content: List[Dict[str, Any]] = []
        for image_path in image_paths:
            image_data_url = self._image_path_to_data_url(image_path)
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": image_data_url},
                }
            )
        content.append({"type": "text", "text": prompt})

        payload = {
            "model": self.backup_model,
            "messages": [{"role": "user", "content": content}],
            "max_tokens": max_tokens,
            "temperature": 0.3,
        }
        return self._post_backup_chat(payload)

    def _post_backup_chat(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.backup_api_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.backup_api_key}",
            "Content-Type": "application/json",
        }

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout,
                )
                if response.status_code != 200:
                    raise Exception(f"备用线路调用失败: {response.status_code} - {response.text}")
                try:
                    return response.json()
                except ValueError as exc:
                    raise Exception(
                        "备用线路返回的不是 JSON: "
                        f"content-type={response.headers.get('content-type')} "
                        f"body={response.text[:400]}"
                    ) from exc
            except requests.exceptions.Timeout as exc:
                last_error = exc
            except requests.exceptions.RequestException as exc:
                last_error = exc
            except Exception as exc:
                last_error = exc

            if attempt < self.max_retries:
                wait_time = 2 ** attempt
                print(f"    ⚠️ 备用线路调用异常，{wait_time} 秒后重试 ({attempt + 1}/{self.max_retries})...")
                time.sleep(wait_time)

        raise Exception(f"备用线路调用最终失败: {last_error}")

    def _call_gemini(
        self,
        prompt: str,
        image_paths: List[str],
        max_tokens: int,
    ) -> Dict[str, Any]:
        parts: List[Dict[str, Any]] = [{"text": prompt}]
        for image_path in image_paths:
            parts.append(self._image_path_to_gemini_inline_data(image_path))

        payload = {
            "contents": [{"parts": parts}],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": max_tokens,
            },
        }
        return self._post_gemini_content(payload)

    def _post_gemini_content(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.gemini_api_key:
            raise Exception("Gemini 线路未配置 API Key")

        url = f"{self.gemini_api_url}/models/{self.gemini_model}:generateContent"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.gemini_api_key}",
        }

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout,
                )
                if response.status_code != 200:
                    raise Exception(f"Gemini 线路调用失败: {response.status_code} - {response.text}")
                try:
                    result = response.json()
                except ValueError as exc:
                    raise Exception(
                        "Gemini 线路返回的不是 JSON: "
                        f"content-type={response.headers.get('content-type')} "
                        f"body={response.text[:400]}"
                    ) from exc
                return self._normalize_gemini_response(result)
            except requests.exceptions.Timeout as exc:
                last_error = exc
            except requests.exceptions.RequestException as exc:
                last_error = exc
            except Exception as exc:
                last_error = exc

            if attempt < self.max_retries:
                wait_time = 2 ** attempt
                print(f"    ⚠️ Gemini 线路调用异常，{wait_time} 秒后重试 ({attempt + 1}/{self.max_retries})...")
                time.sleep(wait_time)

        raise Exception(f"Gemini 线路调用最终失败: {last_error}")

    @staticmethod
    def _image_path_to_data_url(image_path: str) -> str:
        image_bytes = Path(image_path).read_bytes()
        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        image_format = Path(image_path).suffix.lower().lstrip(".")
        if image_format == "jpg":
            image_format = "jpeg"
        mime_type = f"image/{image_format}"
        return f"data:{mime_type};base64,{image_base64}"

    @staticmethod
    def _image_path_to_gemini_inline_data(image_path: str) -> Dict[str, Any]:
        image_bytes = Path(image_path).read_bytes()
        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        image_format = Path(image_path).suffix.lower().lstrip(".")
        if image_format == "jpg":
            image_format = "jpeg"
        mime_type = f"image/{image_format}"
        return {
            "inline_data": {
                "mime_type": mime_type,
                "data": image_base64,
            }
        }

    @staticmethod
    def _normalize_backup_api_url(api_url: str) -> str:
        normalized = str(api_url or BACKUP_LLM_API_URL).strip().rstrip("/")
        if normalized.endswith("/V1"):
            return normalized[:-3] + "/v1"
        return normalized

    @staticmethod
    def _normalize_gemini_api_url(api_url: str) -> str:
        normalized = str(api_url or GEMINI_LLM_API_URL).strip().rstrip("/")
        if normalized.endswith("/V1"):
            return normalized[:-3] + "/v1"
        if normalized.endswith("/v1"):
            return normalized
        if normalized.endswith("/v1beta"):
            return normalized
        if "generativelanguage.googleapis.com" in normalized and not normalized.endswith("/v1beta"):
            return normalized + "/v1beta"
        return normalized + "/v1"

    @staticmethod
    def _normalize_gemini_response(result: Dict[str, Any]) -> Dict[str, Any]:
        try:
            candidates = result.get("candidates") or []
            if not candidates:
                raise Exception("Gemini 响应中缺少 candidates")
            content = candidates[0].get("content") or {}
            parts = content.get("parts") or []
            text_parts: List[str] = []
            for item in parts:
                if isinstance(item, dict) and item.get("text"):
                    text_parts.append(str(item["text"]))
            text = "\n".join(part.strip() for part in text_parts if part and part.strip()).strip()
            if not text:
                raise Exception(f"Gemini 响应中未提取到文本: {json.dumps(result, ensure_ascii=False)[:1200]}")
            return {
                "choices": [
                    {
                        "message": {
                            "content": text,
                        }
                    }
                ]
            }
        except Exception as exc:
            raise Exception(
                f"解析 Gemini 响应失败: {exc}; 原始响应: {json.dumps(result, ensure_ascii=False)[:1200]}"
            ) from exc

    @staticmethod
    def _extract_openai_responses_text(result: Dict[str, Any]) -> str:
        output = result.get("output")
        if not isinstance(output, list):
            return ""
        text_parts: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "output_text":
                    text = str(block.get("text") or "").strip()
                    if text:
                        text_parts.append(text)
        return "\n".join(text_parts).strip()

    @staticmethod
    def _resolve_primary_api_url(override: Optional[str]) -> str:
        if override and str(override).strip():
            return str(override).strip().rstrip("/")
        hermes = _extract_hermes_model_config()
        base_url = str(hermes.get("base_url") or "").strip()
        if base_url:
            return base_url.rstrip("/")
        return PRIMARY_LLM_DEFAULT_API_URL

    @staticmethod
    def _resolve_primary_model(override: Optional[str]) -> str:
        if override and str(override).strip():
            return str(override).strip()
        openclaw_model = _extract_openclaw_primary_model()
        if openclaw_model:
            return openclaw_model
        hermes = _extract_hermes_model_config()
        hermes_model = str(hermes.get("default") or "").strip()
        if hermes_model:
            return hermes_model
        return PRIMARY_LLM_DEFAULT_MODEL

    @staticmethod
    def _resolve_primary_api_key(override: Optional[str]) -> str:
        if override and str(override).strip():
            return str(override).strip()
        env_value = str(os.environ.get("ORIGINAL_SCRIPT_PRIMARY_LLM_API_KEY", "") or "").strip()
        if env_value:
            return env_value
        return _extract_codex_access_token()

    @staticmethod
    def _is_failover_eligible_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "timeout" in message
            or "超时" in message
            or "429" in message
            or "502" in message
            or "503" in message
            or "504" in message
            or "temporarily unavailable" in message
            or "connection" in message
            or "dns" in message
            or "no available channel" in message
            or "not found" in message
            or "rate limit" in message
            or "限流" in message
            or "resource exhausted" in message
            or "insufficient_quota" in message
            or "quota" in message
            or "401" in message
            or "403" in message
            or "unauthorized" in message
            or "forbidden" in message
            or "deadline exceeded" in message
            or "unavailable" in message
            or "proxy" in message
            or "ssl" in message
            or "eof" in message
            or "remote end closed connection" in message
        )

    @staticmethod
    def _is_rate_limit_error(exc: Exception) -> bool:
        message = str(exc)
        return (
            "RequestBurstTooFast" in message
            or "TooManyRequests" in message
            or "429" in message
            or "限流" in message
        )

    @staticmethod
    def _rate_limit_backoff_seconds(attempt: int) -> int:
        schedule = [20, 40, 80, 120]
        return schedule[min(max(attempt, 1), len(schedule)) - 1]

    @staticmethod
    def _extract_text(result: Dict[str, Any]) -> str:
        try:
            content = result["choices"][0]["message"]["content"]
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, list):
                text_parts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        text_parts.append(item.get("text", ""))
                return "\n".join(part for part in text_parts if part).strip()
            return json.dumps(content, ensure_ascii=False)
        except Exception as exc:
            raise Exception(
                f"解析模型响应失败: {exc}; 原始响应: {json.dumps(result, ensure_ascii=False)[:1200]}"
            )
