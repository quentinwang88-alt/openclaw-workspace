#!/usr/bin/env python3
"""LLM helpers for translating and tagging product candidates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Sequence

import requests


class LLMError(Exception):
    """Raised when the model call or response parsing fails."""


@dataclass
class CandidateLLMResult:
    chinese_name: str
    subcategory: str
    reason: str


class CandidateLLMClient:
    """OpenAI-compatible client for name translation and subcategory tagging."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        subcategories: Sequence[str],
        timeout: int = 120,
    ):
        if not api_key:
            raise LLMError("缺少 LLM API Key")

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.subcategories = list(subcategories)
        self.timeout = timeout

    def translate_and_tag(
        self,
        product_name: str,
        product_category: str = "",
        country: str = "",
        image_url: str = "",
    ) -> CandidateLLMResult:
        prompt = self._build_prompt(
            product_name=product_name,
            product_category=product_category,
            country=country,
            image_url=image_url,
        )
        response_text = self._chat(prompt)
        return self._parse_response(response_text)

    def _build_prompt(
        self,
        product_name: str,
        product_category: str,
        country: str,
        image_url: str,
    ) -> str:
        subcategory_text = " / ".join(self.subcategories)
        return f"""你是跨境商品信息清洗助手，要把原始商品标题整理成结构化中文信息。

任务：
1. 将“商品名称”翻译成自然、简洁、可直接写入表格的中文商品名
2. 只能从以下子类目中选择 1 个：
{subcategory_text}

子类目判定标准：
- 发夹：夹子、抓夹、鲨鱼夹、香蕉夹、边夹、鸭嘴夹、蝴蝶夹、barrette、hair clip、claw clip
- 发簪：簪子、发钗、插针式盘发工具、hair stick、hair pin
- 发带：柔性布艺发带、绑带、头巾式发带、ribbon、hair band
- 发箍：硬质或半硬质头箍、bando、headband、băng đô
- 其它：无法稳定归入以上 4 类时使用

命名规则：
- 保留真实商品核心，不要保留夸张营销词
- 不要编造材质、数量、尺寸、功效
- 如果原名本身是发饰，请直接翻成对应商品名
- 中文名称尽量控制在 6 到 20 个字

请只返回 JSON，不要加解释，不要使用 Markdown 代码块。
JSON 格式：
{{
  "chinese_name": "中文商品名",
  "subcategory": "发夹",
  "reason": "一句很短的判断理由"
}}

输入：
- 国家/地区：{country or "未知"}
- 商品分类：{product_category or "未知"}
- 商品图片：{image_url or "未提供"}
- 商品名称：{product_name}
"""

    def _chat(self, prompt: str) -> str:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "temperature": 0.2,
            "messages": [
                {
                    "role": "system",
                    "content": "你只输出严格 JSON，不输出额外文字。",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            "max_tokens": 400,
        }

        response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
        if response.status_code != 200:
            raise LLMError(f"LLM 调用失败: HTTP {response.status_code} - {response.text[:400]}")

        try:
            result = response.json()
        except ValueError as exc:
            raise LLMError(f"LLM 返回了非 JSON 响应: {exc}") from exc

        choices = result.get("choices") or []
        if not choices:
            raise LLMError(f"LLM 响应缺少 choices: {json.dumps(result, ensure_ascii=False)[:500]}")
        content = choices[0].get("message", {}).get("content")
        if not isinstance(content, str) or not content.strip():
            raise LLMError(f"LLM 响应缺少 message.content: {json.dumps(result, ensure_ascii=False)[:500]}")
        return content.strip()

    def _parse_response(self, content: str) -> CandidateLLMResult:
        payload = self._extract_json(content)

        chinese_name = str(payload.get("chinese_name", "")).strip()
        subcategory = str(payload.get("subcategory", "")).strip()
        reason = str(payload.get("reason", "")).strip()

        if not chinese_name:
            raise LLMError(f"LLM 未返回 chinese_name: {content}")
        if subcategory not in self.subcategories:
            raise LLMError(f"LLM 返回了非法子类目 '{subcategory}': {content}")

        return CandidateLLMResult(
            chinese_name=chinese_name,
            subcategory=subcategory,
            reason=reason,
        )

    def _extract_json(self, content: str) -> Dict[str, Any]:
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end >= start:
            text = text[start : end + 1]

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise LLMError(f"无法从模型输出中解析 JSON: {content}") from exc

        if not isinstance(payload, dict):
            raise LLMError(f"模型输出不是对象 JSON: {content}")
        return payload
