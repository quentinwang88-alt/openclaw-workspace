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

from domain.photo_contracts import LABEL_PLACEHOLDER, normalize_publish_copy

from services.photo_flow_registry import (
    PhotoFlowRegistryError, flow_contract_from, role_marker,
    validate_ordered_roles,
)
from services.photo_locale import travel_copy_template

#: Publish language the inline Thai copy was authored for.  It stays the default
#: so every existing (TH) caller keeps its byte-identical output.
DEFAULT_PUBLISH_LOCALE = "th-TH"
LEGACY_LOOK_LABEL_PREFIX = "ลุค "


THEME_OPTIONS = ("自动", "秋季穿搭", "凉爽旅行", "日常通勤", "咖啡约会", "冷热切换",
                 "温度穿搭", "旅行·打卡穿搭", "旅行·环境协调", "旅行·拍照穿搭",
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
    "冷热切换": {
        "theme_key": "THERMAL_TRANSITION",
        "label_zh": "日常冷热切换穿搭",
        "hook_strategy": "thermal_contrast",
        "visual_brief": "同一人物同一天从室外炎热进入 BTS/商场再进入办公室强空调，逐层增加可穿脱的上身层",
        "title": "ร้อนข้างนอก แอร์แรงข้างใน",
        "cover": "ร้อนข้างนอก แอร์แรงข้างใน\nแต่งตัวยังไงดี?",
        "caption": "ไอเดียแต่งตัวสำหรับวันที่ต้องเจอทั้งความร้อนข้างนอกและแอร์แรงในออฟฟิศ ลองใช้เป็นแนวทางแล้วปรับตามความรู้สึกของคุณ",
        "hashtags": ["#แอร์แรง", "#แต่งตัวไปออฟฟิศ", "#ไอเดียแต่งตัว"],
        "cta": "คุณแพ้ร้อนหรือแพ้แอร์มากกว่ากัน?",
    },
    "温度穿搭": {
        "theme_key": "TEMPERATURE_DRESSING",
        "label_zh": "温度分层穿搭",
        "visual_brief": "同一人物、同一机位，从基础层到中间层再到外层逐层增加服装",
        "title": "อากาศแบบนี้ควรใส่กี่ชั้น?",
        "cover": "อากาศแบบนี้\nควรใส่กี่ชั้น?",
        "caption": "ไอเดียแต่งตัวแบบเพิ่มทีละชั้น ปรับตามอุณหภูมิ กิจกรรม และความรู้สึกหนาวของคุณ",
        "hashtags": ["#แต่งตัวตามอากาศ", "#ไอเดียแต่งตัว", "#OOTD"],
        "cta": "เซฟไว้แล้วปรับตามความรู้สึกของคุณ",
    },
}

_ALIASES = {
    "秋天的穿搭": "秋季穿搭", "秋天穿搭": "秋季穿搭", "秋季": "秋季穿搭",
    "autumn": "秋季穿搭", "autumn outfit": "秋季穿搭",
    "冷天气旅行": "凉爽旅行", "旅行穿搭": "凉爽旅行",
    "通勤": "日常通勤", "咖啡": "咖啡约会",
    "温度": "温度穿搭", "分层穿搭": "温度穿搭", "temperature dressing": "温度穿搭",
    "冷热切换": "冷热切换", "冷热": "冷热切换", "空调穿搭": "冷热切换",
    "thermal transition": "冷热切换", "thermal_transition": "冷热切换",
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
        # Only the TEMPERATURE theme ships this block; every other travel theme
        # must stay without it so the planning prompt stays unchanged for them.
        "thermal_sensitivity_planning": dict(
            template.get("thermal_sensitivity_planning") or {}
        ),
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
        try:
            flow, roles = flow_contract_from(variation)
            validate_ordered_roles(
                planned, planning_flow=flow, required_roles=roles,
            )
        except PhotoFlowRegistryError as exc:
            raise ValueError("冻结内容计划" + str(exc)) from exc
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


def _fill_label_placeholders(
    values: Sequence[Any], labels_by_letter: Mapping[str, str],
) -> list[str]:
    """Substitute deferred ``{{label_x}}`` tokens with the frozen asset labels.

    The travel copy packs deliberately keep ``A · {{label_a}}`` … in their
    ``slide_texts``: the label can only be bound once the exact source photo is
    chosen, so ``PhotoRequestFactory`` resolves them at freeze time.  Theme copy
    *replaces* that resolved copy afterwards, so it must bind the same labels
    here — otherwise a literal ``{{label_a}}`` reaches the frozen request and
    ``validate_frozen_request`` rejects the row (2026-09-14: TH 旅行线 5 行全被
    堵死，且失败发生在付费素材生成之后).
    """
    filled: list[str] = []
    for value in values:
        text = str(value or "")
        replaced = LABEL_PLACEHOLDER.sub(
            lambda match: labels_by_letter.get(match.group(1), match.group(0)),
            text,
        )
        filled.append(replaced)
    unresolved = [
        text for text in filled if "{{" in text or "}}" in text
    ]
    if unresolved:
        # Fail loudly instead of freezing a literal template token: the row
        # would otherwise be rejected later by the publish contract anyway,
        # but with a message that hides which label was missing.
        raise ValueError(
            "主题文案存在无法解析的占位符（缺少对应 Look 标签）："
            + "；".join(unresolved[:2])
        )
    return filled


def is_thai_locale(locale: Any) -> bool:
    """Whether this publish language may use the inline Thai labels and copy."""
    return str(locale or "").strip().lower().startswith("th")


def bound_look_label(
    raw_label: Any, *, locale: str, locale_pack: Optional[Mapping[str, Any]],
    marker: str,
) -> str:
    """Resolve one Look label in the task's own language.

    Priority: the frozen asset's ``display_label`` for this locale → the bound
    Locale Pack's generic Look label → (Thai only) the legacy ``ลุค X`` literal.

    A non-Thai task must never silently inherit Thai.  On 2026-09-14 a VN run
    whose assets only carried ``vi-VN`` labels resolved every ``{{label_x}}``
    to ``A · ลุค A``.  Failing here instead surfaces "your language pack is
    missing a Look label" while the failure is still free, rather than after
    four images have been billed.
    """
    if isinstance(raw_label, Mapping):
        # Mirror ``photo_copy.resolve_photo_copy``: an asset may be tagged with
        # the full tag or only its primary subtag.
        value = str(
            raw_label.get(locale) or raw_label.get(str(locale).split("-")[0]) or ""
        ).strip()
        if value:
            return value
    elif raw_label:
        return str(raw_label).strip()
    if isinstance(locale_pack, Mapping):
        generic = dict((locale_pack.get("labels") or {})).get("generic") or {}
        pattern = str(dict(generic).get("look_label") or "").strip()
        if pattern:
            return pattern.replace("{letter}", marker)
    if is_thai_locale(locale):
        return LEGACY_LOOK_LABEL_PREFIX + marker
    raise ValueError(
        "缺少 " + (str(locale or "").strip() or "未指定语言")
        + " 的 Look 标签，且语言包未提供通用 Look 标签；非泰语任务不得回退泰文"
    )


def theme_copy_fallback(
    theme: Mapping[str, Any], *, locale: str,
    locale_pack: Optional[Mapping[str, Any]], variation_index: int = 1,
) -> dict[str, Any]:
    """Theme-supplied copy fields in the task's own language.

    Every built-in theme authors its inline ``title``/``cover``/``caption``/
    ``cta``/``hashtags`` in Thai, so a non-Thai run must take the bound Locale
    Pack instead.  The theme keeps supplying the *semantics* (``label_zh``,
    ``visual_brief``, ``travel_theme_type``); it never supplies the published
    language to a market it was not written for.
    """
    if is_thai_locale(locale) or not isinstance(locale_pack, Mapping):
        return {
            "title": str(theme.get("title") or ""),
            "cover": str(theme.get("cover") or ""),
            "caption": str(theme.get("caption") or ""),
            "cta": str(theme.get("cta") or ""),
            "hashtags": [str(value) for value in theme.get("hashtags") or []],
        }
    template = dict(travel_copy_template(locale_pack, variation_index) or {})
    return {
        "title": str(template.get("title") or ""),
        "cover": str(template.get("cover") or ""),
        "caption": str(template.get("caption") or ""),
        "cta": str(template.get("cta") or ""),
        "hashtags": [str(value) for value in template.get("hashtags") or []],
    }


def build_theme_copy(
    theme: Mapping[str, Any], assets: Sequence[Mapping[str, Any]],
    variation: Optional[Mapping[str, Any]] = None, *,
    locale: str = DEFAULT_PUBLISH_LOCALE,
    locale_pack: Optional[Mapping[str, Any]] = None,
    variation_index: int = 1,
) -> dict[str, Any]:
    by_role = {str(item.get("role") or ""): item for item in assets}
    try:
        _flow, frozen_roles = flow_contract_from(variation)
    except PhotoFlowRegistryError as exc:
        raise ValueError(str(exc)) from exc
    # 2026-09-14：主题内联文案（title/cover/caption/cta/hashtags）都是泰语，
    # 非 TH 任务必须改取绑定语言包，否则 VN 帖会带着泰语标题、CTA 和 hashtags
    # 发布。TH 走同一函数但返回主题原值，输出逐字不变。
    fallback = theme_copy_fallback(
        theme, locale=locale, locale_pack=locale_pack,
        variation_index=variation_index,
    )
    labels = []
    labels_by_letter: dict[str, str] = {}
    for index, role in enumerate(frozen_roles):
        marker = role_marker(role, index)
        label = bound_look_label(
            by_role.get(role, {}).get("display_label"),
            locale=locale, locale_pack=locale_pack, marker=marker,
        )
        labels.append(f"{marker} · {label}")
        if len(marker) == 1:
            labels_by_letter[marker.lower()] = label
    variation = dict(variation or {})
    planned_copy = dict(variation.get("copy") or {})
    cta = str(planned_copy.get("cta") or fallback["cta"])
    labels[-1] += "\n" + cta
    # 主题联动（2026-09-08）：规划产出的逐页 slide_texts 是围绕同一选题的
    # 完整文案，优先使用，不得被标签重组覆盖；缺失时退回旧组装路径。
    # 2026-09-14：这条分支还必须把文案包里推迟到冻结点才替换的 {{label_x}}
    # 解析掉，否则主题覆盖会把字面占位符冻进发布文案。
    topic_slides = _fill_label_placeholders(
        _display_slide_texts(planned_copy.get("slide_texts") or [], cta=cta),
        labels_by_letter,
    )
    # 发布契约兜底（2026-09-13）：theme copy 是最终被冻结进 request 的文案，而
    # 它的 title 可能来自模型自由撰写，也可能来自**上一轮已冻结**的 variation
    # copy（断点续跑不会重跑规划）。封版校验发生在付费素材生成之后，所以超限
    # 必须在这里就夹掉——否则续跑会拿着同一个超限 title 再失败一次。
    if len(topic_slides) == 5 and all(topic_slides):
        return normalize_publish_copy({
            "copy_policy_version": 2,
            "place_localized": str(planned_copy.get("place_localized") or ""),
            "title": str(planned_copy.get("title") or fallback["title"]),
            "caption": str(planned_copy.get("caption") or fallback["caption"]),
            "hashtags": [
                str(value)
                for value in (planned_copy.get("hashtags") or fallback["hashtags"])
            ],
            "slide_texts": topic_slides,
            "language_review_status": str(
                planned_copy.get("language_review_status") or "DRAFT_TRAVEL_TOPIC"),
            **({"language_review": dict(planned_copy["language_review"])}
               if isinstance(planned_copy.get("language_review"), Mapping) else {}),
        })
    # 旧组装路径（规划未给出完整 5 页）：``thai_hook``/``thai_caption`` 是 v1 计划
    # 遗留的内联泰语字段，非 TH 任务直接跳过，改由语言包兜底。
    legacy_hook = str(variation.get("thai_hook") or "") if is_thai_locale(locale) else ""
    legacy_caption = (
        str(variation.get("thai_caption") or "") if is_thai_locale(locale) else ""
    )
    return normalize_publish_copy({
        "place_localized": str(planned_copy.get("place_localized") or ""),
        "title": str(planned_copy.get("title") or legacy_hook or fallback["title"]),
        "caption": str(
            planned_copy.get("caption") or legacy_caption or fallback["caption"]),
        "hashtags": list(fallback["hashtags"]),
        "slide_texts": [str(planned_copy.get("cover") or fallback["cover"]), *labels],
        "language_review_status": "production_theme_profile",
    })
