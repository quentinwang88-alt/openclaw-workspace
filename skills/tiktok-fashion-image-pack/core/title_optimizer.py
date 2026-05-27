#!/usr/bin/env python3
"""Title optimizer for likeU TikTok Shop womens outerwear."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from title_keywords import (
    build_keywords_prompt_text,
    get_available_terms,
    get_series_info,
    load_keywords,
)
from title_postprocess import normalize_title
from title_qa import qa_title
from vision_client import VisionJSONClient


SKILL_DIR = Path(__file__).resolve().parents[1]
DEFAULT_TEMPLATE_PATH = SKILL_DIR / "prompts" / "女装外套_泰国.md"


TITLE_PARSE_PATTERN = re.compile(
    r"TK标题[：:]\s*(.+?)(?:\n|$)",
    re.MULTILINE,
)
CHARS_PATTERN = re.compile(r"字符数[：:]\s*(\d+)")
ATTRS_PATTERN = re.compile(r"提取属性[：:]\s*(.+?)(?:\n|$)", re.MULTILINE)
SUMMARY_PATTERN = re.compile(r"中文摘要[：:]\s*(.+?)(?:\n|$)", re.MULTILINE)


def build_title_prompt(
    *,
    product_truth: Dict[str, Any],
    subtype: str,
    original_title: str = "",
    human_requirement: str = "",
    template_path: Optional[Path] = None,
) -> str:
    template_path = template_path or DEFAULT_TEMPLATE_PATH
    template = template_path.read_text(encoding="utf-8")

    keywords_text = build_keywords_prompt_text(subtype)
    truth_json = json.dumps(product_truth, ensure_ascii=False, indent=2)
    req = human_requirement or "（无特殊要求，按默认规则生成标题）"

    return (
        template.replace("{{keywords_text}}", keywords_text)
        .replace("{{product_truth_json}}", truth_json)
        .replace("{{original_title}}", original_title or "（无原标题）")
        .replace("{{human_requirement}}", req)
    )


def parse_title_output(raw_text: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "tk_title": "",
        "char_count": 0,
        "extracted_attrs": "",
        "cn_summary": "",
        "raw": raw_text,
    }

    title_match = TITLE_PARSE_PATTERN.search(raw_text)
    if title_match:
        result["tk_title"] = title_match.group(1).strip()
    else:
        result["tk_title"] = raw_text.strip().split("\n")[0].strip()

    chars_match = CHARS_PATTERN.search(raw_text)
    if chars_match:
        try:
            result["char_count"] = int(chars_match.group(1))
        except ValueError:
            result["char_count"] = len(result["tk_title"])

    attrs_match = ATTRS_PATTERN.search(raw_text)
    if attrs_match:
        result["extracted_attrs"] = attrs_match.group(1).strip()

    summary_match = SUMMARY_PATTERN.search(raw_text)
    if summary_match:
        result["cn_summary"] = summary_match.group(1).strip()

    if not result["char_count"]:
        result["char_count"] = len(result["tk_title"])

    return result


def generate_title(
    *,
    product_truth: Dict[str, Any],
    subtype: str,
    original_title: str = "",
    human_requirement: str = "",
    vision_client: Optional[VisionJSONClient] = None,
) -> Dict[str, Any]:
    prompt = build_title_prompt(
        product_truth=product_truth,
        subtype=subtype,
        original_title=original_title,
        human_requirement=human_requirement,
    )
    client = vision_client or VisionJSONClient()
    raw_text = client.call_text(prompt, image_paths=[], max_output_tokens=1000)
    parsed = parse_title_output(raw_text)
    parsed["prompt"] = prompt

    raw_title = parsed["tk_title"]
    parsed["normalized_title"] = normalize_title(raw_title)
    parsed["postprocess_applied"] = raw_title != parsed["normalized_title"]

    series = get_series_info(subtype)
    terms = get_available_terms(subtype)

    parsed["series_name_cn"] = series["series_name_cn"]
    parsed["series_code_prefix"] = series["series_code_prefix"]
    parsed["keywords_used"] = _extract_used_keywords(parsed["normalized_title"], terms)

    qa_result = qa_title(
        tk_title=parsed["normalized_title"],
        product_truth=product_truth,
        original_title=original_title,
    )
    parsed["qa_result"] = qa_result["result"]
    parsed["qa_issues"] = qa_result["issues"]
    parsed["qa_summary"] = qa_result["summary"]
    parsed["compliance_risk"] = qa_result.get("compliance_risk", False)

    if parsed["postprocess_applied"]:
        parsed["qa_issues"].append("已自动修正自然表达")
        if parsed["qa_result"] == "通过":
            parsed["qa_result"] = "轻微问题可用"

    return parsed


def _extract_used_keywords(title: str, terms: Dict[str, Any]) -> str:
    found: list = []
    for category in ("core_terms", "material_terms", "fit_terms", "structure_terms", "style_terms"):
        for term in terms.get(category, []):
            if term in title:
                found.append(term)
    return ", ".join(found) if found else ""


def generate_series_code(
    subtype: str,
    product_truth: Dict[str, Any],
    record_index: int = 0,
) -> str:
    series = get_series_info(subtype)
    prefix = series["series_code_prefix"] or "U"
    return f"{prefix}{200 + record_index}"
