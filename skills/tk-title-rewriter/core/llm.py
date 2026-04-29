#!/usr/bin/env python3
"""LLM helpers for TK title rewriting."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from core.mapping import normalize_name


class LLMError(Exception):
    """Raised when the model call or response parsing fails."""


@dataclass(frozen=True)
class RewriteInput:
    record_id: str
    category: str
    original_title: str


@dataclass(frozen=True)
class ParsedRewrite:
    original_title: str
    tk_title: str
    character_count: Optional[int] = None
    extracted_attributes: str = ""
    chinese_summary: str = ""
    warning: str = ""


@dataclass(frozen=True)
class RewriteFailure:
    record_id: str
    original_title: str
    reason: str


class TKTitleRewriterLLMClient:
    """OpenAI-compatible chat client for batch title rewriting."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        timeout: int = 120,
        max_retries: int = 2,
    ):
        if not api_key:
            raise LLMError("缺少 LLM API Key，请设置 TK_TITLE_REWRITER_LLM_API_KEY")

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries

    def rewrite_batch(
        self,
        system_prompt: str,
        items: Sequence[RewriteInput],
        include_cn_summary: bool = False,
    ) -> Tuple[str, List[ParsedRewrite]]:
        if not items:
            return "", []

        raw_text = self._chat(
            system_prompt=system_prompt,
            user_prompt=self._build_user_prompt(items, include_cn_summary=include_cn_summary),
            max_tokens=min(4000, max(1000, 320 * len(items))),
        )
        return raw_text, parse_rewrite_output(raw_text)

    def _build_user_prompt(self, items: Sequence[RewriteInput], include_cn_summary: bool) -> str:
        lines = [
            "请改写以下标题。",
            "要求：必须严格按输入顺序逐条输出，不要合并条目，不要遗漏条目，不要补充解释。",
            "原标题必须保持与输入一致，便于程序回填。",
        ]
        if include_cn_summary:
            lines.append("额外要求：每条在 TK标题 后追加一行 中文摘要，用简洁中文概括目标语言标题含义。")
        lines.append("")
        for index, item in enumerate(items, start=1):
            lines.append(f"{index}. {item.original_title}")
        return "\n".join(lines)

    def _chat(self, system_prompt: str, user_prompt: str, max_tokens: int) -> str:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
        }

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
                if response.status_code != 200:
                    raise LLMError(f"LLM 调用失败: HTTP {response.status_code} - {response.text[:400]}")
                result = response.json()
                return _extract_message_text(result)
            except (requests.exceptions.RequestException, ValueError, LLMError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
        raise LLMError(f"LLM 调用最终失败: {last_error}")


def parse_rewrite_output(content: str) -> List[ParsedRewrite]:
    text = _strip_code_fence(content).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return []

    sections = [
        section.strip()
        for section in re.split(r"(?:^|\n)\s*---+\s*(?:\n|$)", text)
        if section and section.strip()
    ]
    parsed: List[ParsedRewrite] = []
    for section in sections:
        original_match = re.search(r"(?im)^原标题\s*[：:]\s*(.+?)\s*$", section)
        tk_match = re.search(r"(?im)^TK标题\s*[：:]\s*(.+?)\s*$", section)
        if not original_match or not tk_match:
            continue

        char_match = re.search(r"(?im)字符数\s*[：:]\s*(\d+)", section)
        attrs_match = re.search(r"(?im)提取属性\s*[：:]\s*(.+?)\s*$", section)
        cn_match = re.search(r"(?im)^中文摘要\s*[：:]\s*(.+?)\s*$", section)
        warning_match = re.search(r"(?im)^警告\s*[：:]\s*(.+?)\s*$", section)

        attrs_text = attrs_match.group(1).strip() if attrs_match else ""
        warning = warning_match.group(1).strip() if warning_match else ""
        if not warning and "警告=" in attrs_text:
            warning = attrs_text.split("警告=", 1)[1].strip()

        parsed.append(
            ParsedRewrite(
                original_title=original_match.group(1).strip(),
                tk_title=tk_match.group(1).strip(),
                character_count=int(char_match.group(1)) if char_match else None,
                extracted_attributes=attrs_text,
                chinese_summary=cn_match.group(1).strip() if cn_match else "",
                warning=warning,
            )
        )
    return parsed


def align_rewrites(
    requested_items: Sequence[RewriteInput],
    parsed_items: Sequence[ParsedRewrite],
) -> Tuple[List[Tuple[RewriteInput, ParsedRewrite]], List[RewriteFailure]]:
    if not requested_items:
        return [], []

    if len(requested_items) == len(parsed_items):
        successes = [
            (requested_item, parsed_item)
            for requested_item, parsed_item in zip(requested_items, parsed_items)
            if parsed_item.tk_title
        ]
        failures = [
            RewriteFailure(
                record_id=requested_item.record_id,
                original_title=requested_item.original_title,
                reason="模型输出缺少 TK标题",
            )
            for requested_item, parsed_item in zip(requested_items, parsed_items)
            if not parsed_item.tk_title
        ]
        return successes, failures

    buckets: Dict[str, List[ParsedRewrite]] = {}
    for parsed_item in parsed_items:
        buckets.setdefault(normalize_name(parsed_item.original_title), []).append(parsed_item)

    successes: List[Tuple[RewriteInput, ParsedRewrite]] = []
    failures: List[RewriteFailure] = []
    for requested_item in requested_items:
        key = normalize_name(requested_item.original_title)
        bucket = buckets.get(key) or []
        if bucket:
            parsed_item = bucket.pop(0)
            if parsed_item.tk_title:
                successes.append((requested_item, parsed_item))
            else:
                failures.append(
                    RewriteFailure(
                        record_id=requested_item.record_id,
                        original_title=requested_item.original_title,
                        reason="模型输出缺少 TK标题",
                    )
                )
            continue

        failures.append(
            RewriteFailure(
                record_id=requested_item.record_id,
                original_title=requested_item.original_title,
                reason="模型输出无法对齐到该原标题",
            )
        )

    return successes, failures


def _extract_message_text(result: Dict[str, Any]) -> str:
    choices = result.get("choices") or []
    if not choices:
        raise LLMError(f"LLM 响应缺少 choices: {json.dumps(result, ensure_ascii=False)[:500]}")

    content = choices[0].get("message", {}).get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()
    if isinstance(content, list):
        text_parts: List[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = str(item.get("text") or "").strip()
                if text:
                    text_parts.append(text)
        if text_parts:
            return "\n".join(text_parts).strip()
    raise LLMError(f"LLM 响应缺少可读文本: {json.dumps(result, ensure_ascii=False)[:500]}")


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped

    lines = stripped.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()
