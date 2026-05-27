#!/usr/bin/env python3
"""Code-based title QA for likeU womens outerwear TikTok titles.

Does NOT call an LLM — pure rule checks against product truth.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


THAI_CHAR_RANGE = re.compile(r"[\u0E00-\u0E7F]")
CHINESE_CHAR_RANGE = re.compile(r"[\u4E00-\u9FFF]")
PROMO_PATTERNS = re.compile(
    r"(จัดส่งฟรี|ส่งฟรี|ลดราคา|โปรโมชั่น|โปร\s?ฯ|ส่วนลด|โค้ด|Flash\s*Sale|"
    r"รีวิว|ของแท้\s*100%|ประกัน|รับรอง|การันตี|ลด\s*\d+|ลดสูงสุด|"
    r"เพียง\s*\d+\s*บาท|แถมฟรี|ซื้อ\s*\d+\s*แถม|โปร\s*แรง|"
    r"ฟรี|ลดราคาแรง)",
    re.IGNORECASE,
)
ABSOLUTE_PATTERNS = re.compile(
    r"(ที่สุด|ดีที่สุด|ยอดเยี่ยมที่สุด|ฮิตที่สุด|ขายดีที่สุด|ขายดีสุด|"
    r"100%|100\s*%|รับประกัน\s*\d+\s*ปี|ไม่มีวัน|ตลอดไป|"
    r"ของแท้)",
    re.IGNORECASE,
)
BRAND_PATTERNS = re.compile(r"\b(likeU|like\s*U|likeyou|H&M|Zara|Uniqlo|Shein)\b", re.IGNORECASE)
YEAR_PATTERNS = re.compile(r"\b(202[0-9]|19\d{2})\b")

# Compliance risk patterns — material claims that conflict with product truth
DOWN_CLAIM = re.compile(r"ขนเป็ด|ขนห่าน|ขนนก|down\s*feather", re.IGNORECASE)
REAL_LEATHER_CLAIM = re.compile(r"หนังแท้|หนังวัว|หนังแกะ|หนังลูกวัว|genuine\s*leather|real\s*leather", re.IGNORECASE)
REAL_FUR_CLAIM = re.compile(r"ขนแท้|เฟอร์แท้|fur\s*แท้|พรีเมียมแท้", re.IGNORECASE)


def qa_title(
    *,
    tk_title: str,
    product_truth: Optional[Dict[str, Any]] = None,
    original_title: str = "",
) -> Dict[str, Any]:
    issues: List[str] = []
    compliance_risk = False
    title = (tk_title or "").strip()

    _check_empty(title, issues)
    _check_chinese(title, issues)
    _check_length(title, issues)
    _check_promo(title, issues)
    _check_absolute(title, issues)
    _check_brand(title, issues)
    _check_year(title, issues)
    _check_core_term(title, issues)
    _check_style_overload(title, issues)
    _check_thai_naturalness(title, issues)
    _check_info_block_count(title, issues)

    if product_truth:
        cr = _check_compliance(title, product_truth, issues)
        if cr:
            compliance_risk = True

    result, score = _decide_result(issues, compliance_risk)
    return {
        "result": result,
        "score": score,
        "issues": issues,
        "summary": "; ".join(issues) if issues else "通过",
        "compliance_risk": compliance_risk,
    }


def _check_compliance(
    title: str, product_truth: Dict[str, Any], issues: List[str]
) -> bool:
    """Check material claims against product truth. Returns True if hard risk found."""
    subtype = str(product_truth.get("subtype") or "").lower()
    material = str(product_truth.get("material") or "").lower()
    risk = False

    is_pu_leather = ("leather" in subtype and "pu" in material) or "pu leather" in material
    if is_pu_leather and REAL_LEATHER_CLAIM.search(title):
        issues.append("不合规: PU 皮商品标题不得写 หนังแท้ / 真皮承诺")
        risk = True

    is_faux_fur = subtype in ("faux_fur_jacket",)
    if is_faux_fur and REAL_FUR_CLAIM.search(title):
        issues.append("不合规: 皮草/毛毛商品标题不得写 ขนแท้ / เฟอร์แท้ / 真实皮草承诺")
        risk = True

    is_down = "down" in subtype or "ลง" in material or "ขนเป็ด" in material
    if not is_down and DOWN_CLAIM.search(title):
        issues.append("不合规: 非羽绒商品标题不得写 ขนเป็ด / 羽绒承诺")
        risk = True

    return risk


def _check_empty(title: str, issues: List[str]) -> None:
    if not title:
        issues.append("标题为空")


def _check_chinese(title: str, issues: List[str]) -> None:
    chinese_chars = CHINESE_CHAR_RANGE.findall(title)
    if chinese_chars:
        issues.append(f"标题包含中文字符: {''.join(chinese_chars[:10])}")


def _check_length(title: str, issues: List[str]) -> None:
    length = len(title)
    if length < 25:
        issues.append(f"标题过短 ({length} 字符，建议 >=25)")
    if length > 200:
        issues.append(f"标题过长 ({length} 字符，应 <=200)")


def _check_promo(title: str, issues: List[str]) -> None:
    matches = PROMO_PATTERNS.findall(title)
    if matches:
        issues.append(f"标题包含促销词: {', '.join(set(matches))}")


def _check_absolute(title: str, issues: List[str]) -> None:
    matches = ABSOLUTE_PATTERNS.findall(title)
    if matches:
        issues.append(f"标题包含绝对化词: {', '.join(set(matches))}")


def _check_brand(title: str, issues: List[str]) -> None:
    if BRAND_PATTERNS.search(title):
        issues.append("标题包含品牌/店铺名称")


def _check_year(title: str, issues: List[str]) -> None:
    if YEAR_PATTERNS.search(title):
        issues.append("标题包含年份数字")


def _check_core_term(title: str, issues: List[str]) -> None:
    if not THAI_CHAR_RANGE.search(title):
        issues.append("标题缺少泰语内容")
        return
    core_patterns = [
        "เสื้อ", "แจ็คเก็ต", "แจ็กเก็ต", "โค้ท", "คลุม", "สเวตเตอร์",
        "คาร์ดิแกน", "กันหนาว", "บุนวม", "ขนเฟอร์",
    ]
    has_core = any(p in title for p in core_patterns)
    if not has_core:
        issues.append("标题缺少核心品类词（如 เสื้อแจ็คเก็ต / เสื้อคลุม / เสื้อกันหนาว）")


def _check_style_overload(title: str, issues: List[str]) -> None:
    style_candidates = [
        "สไตล์", "ลุค", "มินิมอล", "เกาหลี", "ญี่ปุ่น", "วินเทจ",
        "แฟชั่น", "เทรนด์", "ฮิต", "ใส่ง่าย", "แมตช์ง่าย", "คลาสสิก",
        "หรูหรา", "น่ารัก", "เท่", "ชิค", "แคชชวล",
    ]
    found = [w for w in style_candidates if w in title]
    if len(found) > 2:
        issues.append(f"标题风格词过多 ({len(found)} 个: {', '.join(found)})，建议 <=2 个")


def _check_thai_naturalness(title: str, issues: List[str]) -> None:
    unnatural_patterns = [
        (r"ทรงครอปหลวม(?!\s*แบบ)", "ทรงครอปหลวม 应改为 ทรงครอปแบบหลวม"),
        (r"ทรงสั้นทรงหลวม", "ทรงสั้นทรงหลวม 应改为 ทรงสั้นแบบหลวม"),
        (r"ดีไซน์เปิดหน้า", "ดีไซน์เปิดหน้า 不自然，应改为 แบบเปิดหน้า 或 เปิดหน้า"),
        (r"ดีไซน์", "避免机械使用 ดีไซน์ 翻译\"设计\""),
        (r"ขนฟูเนื้อนุ่ม", "ขนฟูเนื้อนุ่ม 应改为 ขนฟูนุ่ม 或 เนื้อขนฟูนุ่ม"),
        (r"มีกระเป๋าฝาปิดใหญ่", "มีกระเป๋าฝาปิดใหญ่ 非必要不加 ใหญ่，应改为 กระเป๋าฝาปิด"),
    ]
    for pattern, message in unnatural_patterns:
        if re.search(pattern, title):
            issues.append(f"泰语自然度: {message}")


def _check_info_block_count(title: str, issues: List[str]) -> None:
    parts = title.replace("\u200b", " ")
    blocks = [b for b in parts.split(" ") if b.strip()]
    if len(blocks) > 8:
        issues.append(f"标题信息块过多 ({len(blocks)} 个)，建议控制在 5-6 个")


def _decide_result(issues: List[str], compliance_risk: bool = False) -> tuple:
    if compliance_risk:
        return "不通过", 0.2
    if not issues:
        return "通过", 1.0
    critical = any(
        keyword in issue
        for keyword in ["为空", "中文", "过长", "brand", "核心品类词", "ขาด"]
        for issue in issues
    )
    if critical:
        return "不通过", 0.3
    minor = any(
        keyword in issue
        for keyword in ["过短", "年份", "促销词", "风格词过多", "绝对化词", "泰语自然度", "信息块过多"]
        for issue in issues
    )
    if minor:
        return "轻微问题可用", 0.7
    return "需人工复核", 0.5
