"""Small operator-facing theme contract for native-photo content.

The first production version is intentionally finite.  Operators choose a
plain-language theme; code freezes its localized copy and visual direction so
the image overlays and TikTok metadata cannot drift apart.

2026-09-08: six travel topic themes (旅行·…) link the travel outfit preset to
a place + topic so planning, title and per-page copy revolve around one
selection.  Their identity lives in ``travel_theme_type`` while ``theme_key``
stays ``COOL_WEATHER_TRAVEL`` for every existing validation path.
"""
from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


THEME_OPTIONS = ("自动", "秋季穿搭", "凉爽旅行", "日常通勤", "咖啡约会",
                 "旅行·打卡穿搭", "旅行·环境协调", "旅行·拍照穿搭",
                 "旅行·温度穿搭", "旅行·四选一", "旅行·配色参考")

_TRAVEL_THEME_CONFIG = (
    Path(__file__).resolve().parents[1] / "config" / "travel_theme_templates.json"
)

_PROFILES = {
    "秋季穿搭": {
        "theme_key": "AUTUMN_OUTFIT",
        "label_zh": "秋季暖色穿搭",
        "visual_brief": "暖棕、焦糖、奶油和深牛仔色；轻层搭；适合凉爽天气的城市日常穿搭",
        "title": "4 ลุคโทนอุ่นสำหรับอากาศเย็น",
        "cover": "4 ลุคโทนอุ่น\nA B C หรือ D?",
        "caption": "รวม 4 ไอเดียแต่งตัวโทนอุ่นสำหรับอากาศเย็น คุณชอบลุค A B C หรือ D มากที่สุด?",
        "hashtags": ["#ไอเดียแต่งตัว", "#แต่งตัวไปเที่ยว", "#OOTD"],
        "cta": "คุณชอบลุคไหน?",
    },
    "凉爽旅行": {
        "theme_key": "COOL_WEATHER_TRAVEL",
        "label_zh": "凉爽天气旅行穿搭",
        "visual_brief": "适合去凉爽地区旅行的轻层搭，方便走路、拍照和室内外切换",
        "title": "4 ลุคสำหรับทริปอากาศเย็น",
        "cover": "ทริปอากาศเย็น\nA B C หรือ D?",
        "caption": "ถ้าไปเที่ยวเมืองอากาศเย็น คุณอยากใส่ลุค A B C หรือ D มากที่สุด?",
        "hashtags": ["#แต่งตัวไปเที่ยว", "#ไอเดียแต่งตัว", "#OOTD"],
        "cta": "ไปเที่ยวจะเลือกลุคไหน?",
    },
    "日常通勤": {
        "theme_key": "DAILY_COMMUTE",
        "label_zh": "日常通勤穿搭",
        "visual_brief": "干净利落、舒适可走动的日常通勤穿搭，中性色和低饱和色为主",
        "title": "4 ลุคไปทำงานแบบแต่งตามง่าย",
        "cover": "ลุคไปทำงาน\nA B C หรือ D?",
        "caption": "4 ไอเดียแต่งตัวไปทำงานแบบเรียบง่าย คุณชอบลุคไหนมากที่สุด?",
        "hashtags": ["#แฟชั่นทำงาน", "#ไอเดียแต่งตัว", "#OOTD"],
        "cta": "พรุ่งนี้จะเลือกลุคไหน?",
    },
    "咖啡约会": {
        "theme_key": "CAFE_DATE",
        "label_zh": "咖啡约会穿搭",
        "visual_brief": "轻松、适合拍照的咖啡约会穿搭，柔和暖色和自然层搭",
        "title": "4 ลุคไปคาเฟ่ เลือกลุคไหนดี",
        "cover": "ไปคาเฟ่\nA B C หรือ D?",
        "caption": "ถ้าไปคาเฟ่วันนี้ คุณอยากใส่ลุค A B C หรือ D?",
        "hashtags": ["#ลุคคาเฟ่", "#ไอเดียแต่งตัว", "#OOTD"],
        "cta": "ไปคาเฟ่จะเลือกลุคไหน?",
    },
}

_ALIASES = {
    "秋天的穿搭": "秋季穿搭", "秋天穿搭": "秋季穿搭", "秋季": "秋季穿搭",
    "autumn": "秋季穿搭", "autumn outfit": "秋季穿搭",
    "冷天气旅行": "凉爽旅行", "旅行穿搭": "凉爽旅行",
    "通勤": "日常通勤", "咖啡": "咖啡约会",
}


@lru_cache(maxsize=1)
def travel_theme_templates() -> dict[str, dict[str, Any]]:
    """Six travel topic themes from config/travel_theme_templates.json."""
    if not _TRAVEL_THEME_CONFIG.is_file():
        return {}
    payload = json.loads(_TRAVEL_THEME_CONFIG.read_text(encoding="utf-8"))
    return {key: dict(value) for key, value in (payload.get("themes") or {}).items()}


def _travel_theme_profile(theme_type: str, label: str,
                          template: Mapping[str, Any]) -> dict[str, Any]:
    thai = dict(template.get("thai_fallback") or {})
    return {
        "theme_key": "COOL_WEATHER_TRAVEL",  # 兼容既有 supported_theme_keys 校验
        "label_zh": label,
        "travel_theme_type": theme_type,
        "travel_theme_version": int(template.get("version") or 1),
        "visual_brief": str(template.get("planning_focus") or ""),
        "title": str(thai.get("title") or ""),
        "cover": str(thai.get("cover") or ""),
        "caption": str(thai.get("caption") or ""),
        "hashtags": list(thai.get("hashtags") or []),
        "cta": str(thai.get("cta") or ""),
        "topic_patterns": list(template.get("topic_patterns") or []),
        "body_copy_focus": str(template.get("body_copy_focus") or ""),
        "cta_patterns": list(template.get("cta_patterns") or []),
    }


def resolve_travel_theme_type(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    for theme_type, template in travel_theme_templates().items():
        if text in {theme_type, str(template.get("label_zh") or "")}:
            return theme_type
    return None


def resolve_photo_theme(value: Any) -> Optional[dict[str, Any]]:
    text = str(value or "").strip()
    if not text or text == "自动":
        return None
    key = _ALIASES.get(text, _ALIASES.get(text.lower(), text))
    profile = _PROFILES.get(key)
    if profile is None and travel_theme_templates():
        for theme_type, template in travel_theme_templates().items():
            label = str(template.get("label_zh") or "")
            if text in {theme_type, label}:
                return {"input": text, **_travel_theme_profile(theme_type, label, template)}
    if profile is None:
        raise ValueError(
            f"暂不支持图文主题“{text}”；请选择：" + "、".join(THEME_OPTIONS)
        )
    return {"input": text, **deepcopy(profile)}


def style_look_specs(
    theme: Mapping[str, Any], variation: Optional[Mapping[str, Any]] = None,
) -> list[dict[str, Any]]:
    """Return the exact frozen Looks when the content planner supplied them."""
    planned = list((variation or {}).get("looks") or [])
    if planned:
        if [item.get("role") for item in planned] != [
            "look_a", "look_b", "look_c", "look_d"
        ]:
            raise ValueError("冻结内容计划必须按 A/B/C/D 提供四套穿搭")
        return deepcopy(planned)

    # Compatibility for already-frozen historical tasks that predate the
    # content planner. New reference-driven runs always use `planned` above.
    key = str(theme.get("theme_key") or "")
    if key in {"AUTUMN_OUTFIT", "COOL_WEATHER_TRAVEL"}:
        looks = [
            {"role": "look_a", "display_label": "เบลเซอร์กับยีนส์", "outerwear": "深巧克力色宽松西装",
             "top_inner": "灰褐色轻薄高领针织", "bottom": "中蓝色高腰阔腿牛仔裤", "shoes": "低调暖色运动鞋"},
            {"role": "look_b", "display_label": "แจ็กเก็ตสั้นกับกางเกงดำ", "outerwear": "低饱和姜黄色短款工装夹克",
             "top_inner": "奶油色轻薄针织", "bottom": "炭黑色高腰阔腿长裤", "shoes": "米白色简洁运动鞋"},
            {"role": "look_c", "display_label": "แจ็กเก็ตสั้นกับกระโปรง", "outerwear": "低饱和姜黄色短款工装夹克",
             "top_inner": "奶油色轻薄针织", "bottom": "深灰色A字中长裙和深色连裤袜", "shoes": "深棕色短靴"},
            {"role": "look_d", "display_label": "แจ็กเก็ตกับกระโปรงยีนส์", "outerwear": "焦糖棕色短款休闲夹克",
             "top_inner": "象牙白圆领针织", "bottom": "深靛蓝直筒牛仔中长裙", "shoes": "深棕色乐福鞋和棕色短袜"},
        ]
        modifier = str((variation or {}).get("style_modifier") or "").strip()
        if modifier:
            for look in looks:
                look["style_modifier"] = modifier
        return looks
    # The first MVP shares a restrained four-look structure for other themes;
    # theme-specific copy and visual brief still distinguish the content.
    return [
        {"role": "look_a", "display_label": "เบลเซอร์กับยีนส์", "outerwear": "棕色宽松西装", "top_inner": "奶油色内搭", "bottom": "高腰阔腿牛仔裤", "shoes": "简洁运动鞋"},
        {"role": "look_b", "display_label": "แจ็กเก็ตสั้นกับกางเกงดำ", "outerwear": "米色短款夹克", "top_inner": "白色针织", "bottom": "黑色高腰长裤", "shoes": "米白运动鞋"},
        {"role": "look_c", "display_label": "แจ็กเก็ตสั้นกับกระโปรง", "outerwear": "米色短款夹克", "top_inner": "白色针织", "bottom": "深灰中长裙", "shoes": "棕色短靴"},
        {"role": "look_d", "display_label": "แจ็กเก็ตกับกระโปรงยีนส์", "outerwear": "焦糖色休闲夹克", "top_inner": "象牙白内搭", "bottom": "深色牛仔中长裙", "shoes": "深棕乐福鞋"},
    ]


def _wrap_cover(text: str) -> str:
    """Keep the full hook while giving the renderer a useful two-line break."""
    text = str(text or "").strip()
    if "\n" in text or len(text) <= 34:
        return text
    spaces = [index for index, char in enumerate(text) if char == " "]
    if not spaces:
        return text
    midpoint = len(text) / 2
    split_at = min(spaces, key=lambda index: abs(index - midpoint))
    return text[:split_at].rstrip() + "\n" + text[split_at + 1:].lstrip()


def _short_look_label(text: str) -> str:
    """Use the planned look name as image copy; details remain in the caption."""
    text = str(text or "").strip()
    for separator in (" — ", " – ", " - ", "：", ":"):
        if separator in text:
            head = text.split(separator, 1)[0].strip()
            if head:
                return head
    return text


def _display_slide_texts(values: Sequence[Any], *, cta: str) -> list[str]:
    """Convert model prose to complete, compact overlay copy without ellipses."""
    slides = [str(value or "").strip() for value in values]
    if len(slides) != 5 or not all(slides):
        return slides
    slides[0] = _wrap_cover(slides[0])
    for index in range(1, 4):
        slides[index] = _short_look_label(slides[index])

    final = slides[4]
    planned_cta = str(cta or "").strip()
    cta_markers = ("คุณเลือก", "คุณชอบ", "เลือกลุคไหน", "เลือก A", "A, B, C", "A B C")
    marker_positions = [final.find(marker) for marker in cta_markers if final.find(marker) > 0]
    if marker_positions:
        position = min(marker_positions)
        final_label = final[:position].strip(" ·,-—–")
    else:
        final_label = final.splitlines()[0].strip()
    final_label = _short_look_label(final_label)
    # The model may repeat all four look names in its final sentence.  Keep the
    # frozen topic CTA instead: the last page should remain readable at phone size.
    slides[4] = f"{final_label}\n{planned_cta}" if planned_cta else final_label
    return slides


def build_theme_copy(
    theme: Mapping[str, Any], assets: Sequence[Mapping[str, Any]],
    variation: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    by_role = {str(item.get("role") or ""): item for item in assets}
    labels = []
    for letter, role in zip("ABCD", ("look_a", "look_b", "look_c", "look_d")):
        raw_label = by_role.get(role, {}).get("display_label") or {}
        label = (raw_label.get("th-TH") if isinstance(raw_label, Mapping)
                 else str(raw_label).strip())
        labels.append(f"{letter} · {label or ('ลุค ' + letter)}")
    variation = dict(variation or {})
    planned_copy = dict(variation.get("copy") or {})
    cta = str(planned_copy.get("cta") or theme["cta"])
    labels[-1] += "\n" + cta
    # 主题联动（2026-09-08）：规划产出的逐页 slide_texts 是围绕同一选题的
    # 完整文案，优先使用，不得被标签重组覆盖；缺失时退回旧组装路径。
    topic_slides = _display_slide_texts(
        planned_copy.get("slide_texts") or [], cta=cta,
    )
    if len(topic_slides) == 5 and all(topic_slides):
        return {
            "copy_policy_version": 2,
            "place_localized": str(planned_copy.get("place_localized") or ""),
            "title": str(planned_copy.get("title") or theme["title"]),
            "caption": str(planned_copy.get("caption") or theme["caption"]),
            "hashtags": [str(v) for v in planned_copy.get("hashtags") or theme["hashtags"]],
            "slide_texts": topic_slides,
            "language_review_status": str(
                planned_copy.get("language_review_status") or "DRAFT_TRAVEL_TOPIC"),
        }
    return {
        "place_localized": str(planned_copy.get("place_localized") or ""),
        "title": str(planned_copy.get("title") or variation.get("thai_hook") or theme["title"]),
        "caption": str(planned_copy.get("caption") or variation.get("thai_caption") or theme["caption"]),
        "hashtags": list(theme["hashtags"]),
        "slide_texts": [str(planned_copy.get("cover") or theme["cover"]), *labels],
        "language_review_status": "production_theme_profile",
    }
