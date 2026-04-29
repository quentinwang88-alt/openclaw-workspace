#!/usr/bin/env python3
"""Prompt template loading and category matching."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from core.mapping import normalize_name


class PromptTemplateError(Exception):
    """Raised when a prompt template file is invalid."""


@dataclass(frozen=True)
class PromptTemplate:
    category_name: str
    aliases: Tuple[str, ...]
    target_market: str
    target_language: str
    system_prompt: str
    path: Path


def load_prompt_templates(prompts_dir: Path) -> List[PromptTemplate]:
    templates: List[PromptTemplate] = []
    for path in sorted(prompts_dir.glob("*.md")):
        templates.append(_parse_prompt_template(path))
    return templates


def match_prompt_template(
    category_name: str,
    templates: Sequence[PromptTemplate],
) -> Tuple[Optional[PromptTemplate], Optional[str]]:
    normalized_category = normalize_name(category_name)
    if not normalized_category:
        return None, None

    for template in templates:
        if normalize_name(template.category_name) == normalized_category:
            return template, "exact"

    for template in templates:
        for alias in template.aliases:
            if normalize_name(alias) == normalized_category:
                return template, "alias"

    return None, None


def build_template_lookup(templates: Sequence[PromptTemplate]) -> Dict[str, PromptTemplate]:
    lookup: Dict[str, PromptTemplate] = {}
    for template in templates:
        lookup[normalize_name(template.category_name)] = template
        for alias in template.aliases:
            lookup.setdefault(normalize_name(alias), template)
    return lookup


def _parse_prompt_template(path: Path) -> PromptTemplate:
    text = path.read_text(encoding="utf-8")
    aliases = _read_header_list(text, "aliases")
    target_market = _read_header_value(text, "target_market")
    target_language = _read_header_value(text, "target_language")
    system_prompt = _extract_first_code_block(text)
    if not system_prompt:
        raise PromptTemplateError(f"{path} 缺少 System Prompt 代码块")

    return PromptTemplate(
        category_name=path.stem,
        aliases=tuple(alias for alias in aliases if alias),
        target_market=target_market,
        target_language=target_language,
        system_prompt=system_prompt,
        path=path,
    )


def _read_header_value(text: str, key: str) -> str:
    pattern = re.compile(rf"(?im)^{re.escape(key)}\s*:\s*(.+?)\s*$")
    match = pattern.search(text)
    if not match:
        return ""
    return match.group(1).strip()

def _read_header_list(text: str, key: str) -> List[str]:
    raw = _read_header_value(text, key)
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _extract_first_code_block(text: str) -> str:
    match = re.search(r"```(?:[\w+-]+)?\n(.*?)\n```", text, re.DOTALL)
    if not match:
        return ""
    return match.group(1).strip()
