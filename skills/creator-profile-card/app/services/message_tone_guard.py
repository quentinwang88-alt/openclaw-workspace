"""Tone guard for creator outreach messages."""

from __future__ import annotations

import re
from typing import Any, Dict, List

from .message_rulebook import HARD_RULES, SOFTEN_REPLACEMENTS


def strip_batch_greeting(message: str) -> str:
    text = (message or "").strip()
    if not text:
        return ""
    patterns = [
        r"^(?:哈喽|你好|您好|嗨|Hi|Hello|Hey)\s*@?[\w.\-\u4e00-\u9fff]+[～~,!！,，\s]+",
        r"^(?:สวัสดีค่ะ|สวัสดีครับ|สวัสดี)\s*@?[\w.\-]+[～~,!！,，\s]+",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text, count=1, flags=re.IGNORECASE).strip()
    return text


def soften_message(message: str) -> str:
    text = strip_batch_greeting(message)
    for old, new in SOFTEN_REPLACEMENTS.items():
        text = text.replace(old, new)
    return text.strip()


def _find_terms(text: str, terms: List[str]) -> List[str]:
    combined = text or ""
    return [term for term in terms if term and term in combined]


def check_message_tone(
    purpose: str,
    message_cn: str = "",
    message_local: str = "",
    relationship_stage: str = "",
) -> Dict[str, Any]:
    rules = HARD_RULES.get(purpose, {})
    zh_hits = _find_terms(message_cn, rules.get("zh", []))
    local_hits = _find_terms(message_local, rules.get("th", []) + rules.get("local", []))
    issues: List[str] = []
    if zh_hits:
        issues.append(f"中文命中硬禁词: {', '.join(zh_hits)}")
    if local_hits:
        issues.append(f"本地语言命中硬禁词: {', '.join(local_hits)}")

    if purpose == "batch_content_opportunity":
        if not (message_cn or message_local):
            issues.append("批量话术为空")
        if "?" in message_cn and "适合你" in message_cn:
            issues.append("批量话术疑似假个性化问句")
        if len(message_cn) > 160:
            issues.append("批量中文话术过长")

    if purpose == "single_product_invitation" and relationship_stage in {"陌生", "冷"}:
        if any(term in message_cn for term in ["安排寄样", "我这边安排", "马上安排"]):
            issues.append("冷关系单点话术 CTA 过强")

    if purpose == "relationship_maintenance":
        if any(term in message_cn for term in ["商品", "新品", "款式"]):
            issues.append("关系维护疑似提及商品")

    if purpose == "follow_up":
        if not any(term in message_cn for term in ["不着急", "不合适也没关系", "方便的时候", "有空"]):
            issues.append("跟进话术缺少低压/拒绝空间")

    return {
        "purpose": purpose,
        "relationship_stage": relationship_stage,
        "hard_term_hits": zh_hits + local_hits,
        "issues": issues,
        "passed": not issues,
    }


def apply_tone_guard(
    purpose: str,
    result: Dict[str, Any],
    relationship_stage: str = "",
    soften: bool = True,
) -> Dict[str, Any]:
    output = dict(result or {})
    if soften:
        output["message_cn_for_operator"] = soften_message(output.get("message_cn_for_operator", ""))
        output["message_local"] = soften_message(output.get("message_local", ""))
    guard = check_message_tone(
        purpose,
        output.get("message_cn_for_operator", ""),
        output.get("message_local", ""),
        relationship_stage=relationship_stage,
    )
    output["tone_guard"] = guard
    output.setdefault("risk_check", {})
    output["risk_check"]["tone_guard_passed"] = guard["passed"]
    output["risk_check"]["tone_guard_issues"] = guard["issues"]
    return output
