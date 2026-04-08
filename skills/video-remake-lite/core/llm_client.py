#!/usr/bin/env python3
"""
视频复刻 LLM 客户端。

模型、接口地址、超时与重试策略与 creator_crm 的视频评分调用保持一致。
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workspace_support import load_repo_env

load_repo_env()

LLM_API_URL = os.environ.get("LLM_API_URL", "https://ark.cn-beijing.volces.com/api/coding/v3")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "Doubao-Seed-2.0-pro")


class VideoRemakeLLMClient:
    """复用 creator_crm 评分链路默认配置的 LLM 客户端。"""

    def __init__(
        self,
        api_url: str = LLM_API_URL,
        api_key: str = LLM_API_KEY,
        model: str = LLM_MODEL,
        timeout: int = 120,
        max_retries: int = 2,
    ):
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries

    def chat_with_video(self, video_url: str, prompt: str, max_tokens: int = 2500) -> str:
        """将视频 URL 直接传给模型进行分析。"""
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "video_url",
                            "video_url": {
                                "url": video_url
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "max_tokens": max_tokens
        }
        return self._post_chat(payload)

    def chat_text(self, prompt: str, max_tokens: int = 2500) -> str:
        """使用与视频分析相同的模型和接口，执行纯文本阶段。"""
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "max_tokens": max_tokens
        }
        return self._post_chat(payload)

    def _post_chat(self, payload: Dict[str, Any]) -> str:
        url = f"{self.api_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout,
                )
                if response.status_code != 200:
                    raise Exception(f"LLM 调用失败: {response.status_code} - {response.text}")

                result = response.json()
                return self._extract_text(result)
            except requests.exceptions.Timeout as exc:
                last_error = exc
            except requests.exceptions.RequestException as exc:
                last_error = exc
            except Exception as exc:
                last_error = exc

            if attempt < self.max_retries:
                wait_time = 2 ** attempt
                print(f"    ⚠️ LLM 调用异常，{wait_time} 秒后重试 ({attempt + 1}/{self.max_retries})...")
                time.sleep(wait_time)

        raise Exception(f"LLM 调用最终失败: {last_error}")

    @staticmethod
    def _extract_text(result: Dict[str, Any]) -> str:
        try:
            if "choices" in result and result["choices"]:
                content = result["choices"][0]["message"]["content"]
                if isinstance(content, str):
                    return content.strip()
                if isinstance(content, list):
                    text_parts = []
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            text_parts.append(item.get("text", ""))
                    return "\n".join(part for part in text_parts if part).strip()
            raise KeyError("choices[0].message.content")
        except Exception as exc:
            raise Exception(
                f"解析模型响应失败: {exc}\n原始响应: {json.dumps(result, ensure_ascii=False)[:1500]}"
            )
