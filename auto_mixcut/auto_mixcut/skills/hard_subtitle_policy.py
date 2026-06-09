from __future__ import annotations

from typing import Any


REPAIRABLE_BOTTOM_CAPTION = "bottom_caption_repairable"
UNUSABLE_TEXT_RISKS = {"foreign_language_caption", "large_obstructive_text", "platform_ui_or_watermark"}


def classify_text_overlay(tag: dict[str, Any] | None) -> dict[str, str]:
    tag = tag or {}
    risk = _normalize_risk(tag.get("text_overlay_risk"))
    reason = str(tag.get("text_overlay_reason") or tag.get("reason") or "")
    language = str(tag.get("text_language") or "")
    if risk != "none":
        inferred = _infer_from_text(reason)
        if risk == "foreign_language_caption" and inferred == REPAIRABLE_BOTTOM_CAPTION:
            return {"risk": REPAIRABLE_BOTTOM_CAPTION, "language": language or _infer_language(reason), "reason": reason}
        return {"risk": risk, "language": language, "reason": reason}

    inferred = _infer_from_text(reason)
    return {"risk": inferred, "language": language or _infer_language(reason), "reason": reason}


def is_repairable_bottom_caption(tag: dict[str, Any] | None) -> bool:
    return classify_text_overlay(tag)["risk"] == REPAIRABLE_BOTTOM_CAPTION


def is_unusable_hard_subtitle(tag: dict[str, Any] | None) -> bool:
    return classify_text_overlay(tag)["risk"] in UNUSABLE_TEXT_RISKS


def has_text_overlay_risk(tag: dict[str, Any] | None) -> bool:
    risk = classify_text_overlay(tag)["risk"]
    return risk != "none" and risk != "safe_product_label"


def _normalize_risk(value: Any) -> str:
    normalized = str(value or "").strip()
    aliases = {
        "bottom_caption": REPAIRABLE_BOTTOM_CAPTION,
        "bottom_subtitle": REPAIRABLE_BOTTOM_CAPTION,
        "repairable_bottom_caption": REPAIRABLE_BOTTOM_CAPTION,
        "foreign_caption": "foreign_language_caption",
        "subtitle": "foreign_language_caption",
        "caption": "foreign_language_caption",
        "large_text": "large_obstructive_text",
        "watermark": "platform_ui_or_watermark",
    }
    normalized = aliases.get(normalized, normalized)
    allowed = {"none", "safe_product_label", REPAIRABLE_BOTTOM_CAPTION, *UNUSABLE_TEXT_RISKS}
    return normalized if normalized in allowed else "none"


def _infer_from_text(text: str) -> str:
    if not text:
        return "none"
    lower = text.lower()
    if any(token in lower for token in ["sale", "discount", "off", "voucher"]) or any(token in text for token in ["促销", "折扣", "营销标识"]):
        return "platform_ui_or_watermark"
    subtitle_tokens = ["字幕", "外文", "越南语", "泰文", "当地语言", "底部文字", "subtitle", "caption", "chi tiet"]
    if not any(token in lower or token in text for token in subtitle_tokens):
        return "none"
    if any(token in text for token in ["水印", "平台", "账号", "logo", "Logo", "品牌包"]):
        return "platform_ui_or_watermark"
    has_obstruction = any(token in text for token in ["遮挡商品", "遮挡主体", "遮挡脸", "遮挡手部", "严重遮挡"])
    negated_obstruction = any(token in text for token in ["不遮挡", "未遮挡", "没有遮挡", "无遮挡"])
    if any(token in text for token in ["大面积", "多行", "滚动", "贯穿", "中部", "中间"]) or (has_obstruction and not negated_obstruction):
        return "large_obstructive_text"
    if any(token in text for token in ["底部", "底端", "下方", "安全区", "底部文字"]):
        return REPAIRABLE_BOTTOM_CAPTION
    return "foreign_language_caption"


def _infer_language(text: str) -> str:
    lower = text.lower()
    if "越南" in text or "chi tiet" in lower:
        return "vietnamese"
    if "泰" in text:
        return "thai"
    if "外文" in text or "当地语言" in text:
        return "foreign_or_local"
    return ""
