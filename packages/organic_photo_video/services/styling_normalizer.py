"""Deterministic, field-scoped import of legacy styling data; no model calls.

Legacy prompt_core also describes a different target garment.  Only allowlisted
styling nouns/attributes under recognised labels are imported, never that prose.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Mapping


def normalize_product_id(value: Any) -> str:
    """Remove clipboard formatting artifacts, not arbitrary SKU characters."""
    return "".join(c for c in str(value or "") if c != "\ufffc" and unicodedata.category(c) != "Cf").strip()


def _text(value: Any) -> str:
    if isinstance(value, Mapping):
        return " ".join(_text(v) for k, v in sorted(value.items()))
    if isinstance(value, (list, tuple)):
        return " ".join(_text(v) for v in value)
    return str(value or "").strip()


def _list(value):
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value or "[]")
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def _column_text(value):
    parsed = _list(value)
    return "、".join(map(str, parsed)) if parsed else _text(value)


_COLORS = ("黑白", "深棕色", "浅棕色", "米白色", "奶白色", "纯白色", "深蓝色", "浅蓝色", "复古水洗蓝", "牛仔浅蓝", "白色", "黑色", "棕色", "灰色", "蓝色", "米色", "卡其色")
_SHOES = ("玛丽珍单鞋", "玛丽珍", "厚底运动鞋", "平底运动鞋", "老爹鞋", "运动鞋", "雪地靴", "棉鞋", "小白鞋", "乐福鞋", "短靴", "长靴", "高跟鞋", "平底鞋", "凉鞋", "sneakers", "loafers", "boots")
_SOCKS = ("堆堆袜", "连裤袜", "短袜", "长袜", "腿套", "袜套", "socks")
_BAGS = ("斜挎小方包", "小方包", "斜挎包", "单肩包", "手提包", "托特包", "双肩包", "小包", "bag")
_ACCESSORIES = ("流苏围巾", "围巾", "耳钉", "耳环", "项链", "腰带", "发箍", "棒球帽")
_MATERIALS = ("麂皮", "毛绒", "厚底", "平底", "漆皮", "流苏", "针织", "皮质")


def _sections(prompt: str) -> dict[str, str]:
    # A known label must begin a line/semicolon-separated section.  In
    # particular, "外套与围巾" is NOT a permitted accessory section.
    pattern = r"(?:^|[\n；;])\s*(鞋履|鞋袜配饰|鞋袜|袜子|包袋|包包|配饰|下装)\s*[:：]\s*([^\n；;]*)"
    return {m.group(1): m.group(2) for m in re.finditer(pattern, prompt or "")}


def _safe_item(section: str, nouns: tuple[str, ...]) -> str:
    matches = [(m.start(), -len(noun), m, noun) for noun in nouns
               for m in re.finditer(re.escape(noun), section, re.IGNORECASE)]
    if not matches:
        return ""
    _, _, match, noun = min(matches, key=lambda x: (x[0], x[1]))
    # Do not carry words from another clause (such as coat colour or branding).
    prefix = re.split(r"[，,。；;、]", section[:match.start()])[-1][-24:]
    colors = [(prefix.rfind(color), -len(color), color) for color in _COLORS if color in prefix]
    # Prefer the longest colour when colour words overlap (深棕色 vs 棕色).
    color = next((c for c in _COLORS if c in prefix), "") if colors else ""
    attributes = [token for token in _MATERIALS if token in prefix and token not in noun]
    return " ".join([x for x in (color, *attributes, noun) if x])


def normalize_styling_recipe(row: Mapping[str, Any], explicit_recipe=None):
    """Return (recipe, field_sources), preserving explicit recipes first."""
    raw = dict(row)
    explicit = dict(explicit_recipe or {})
    recipe, sources = {}, {}

    def put(key, value, source, columns):
        if value:
            recipe[key] = value
            sources[key] = {"source": source, "columns": columns}

    inner = " ".join(_text(raw.get(k)) for k in ("inner_color", "inner_type") if raw.get(k))
    colors = _list(raw.get("bottom_color"))
    bottom_color = "/".join(map(str, colors)) or _text(raw.get("bottom_color"))
    bottom_fit = _column_text(raw.get("bottom_fit"))
    bottom_type = _text(raw.get("bottom_type"))
    bottom = " ".join(x for x in (bottom_color, bottom_fit, bottom_type) if x)
    put("top_inner", inner, "structured_columns", ["inner_color", "inner_type"])
    put("bottom", bottom, "structured_columns", ["bottom_color", "bottom_fit", "bottom_type"])
    for key, value in (("bottom_type", bottom_type), ("bottom_color", bottom_color), ("bottom_fit", bottom_fit)):
        put(key, value, "structured_columns", [key])
    put("accessories", _text(raw.get("accessory_level")), "structured_columns", ["accessory_level"])
    style_text = _text(raw.get("base_outfit_direction") or raw.get("vibe_tag") or raw.get("styling_name"))
    style_words = ("清爽显高", "清爽", "显高", "极简", "休闲", "复古", "学院", "甜美", "街头", "慵懒", "通勤", "优雅", "运动", "旅行", "minimal", "casual", "vintage", "preppy")
    style = "、".join(x for x in style_words if x in style_text and not any(x != other and x in other and other in style_text for other in style_words))
    put("overall_style", style, "structured_columns_style_allowlist", ["base_outfit_direction", "vibe_tag", "styling_name"])
    put("target_outer", "目标商品本身", "product_authority", [])
    sections = _sections(_text(raw.get("prompt_core")))
    shoe_section = next((sections[k] for k in ("鞋履", "鞋袜配饰", "鞋袜") if k in sections), "")
    for key, section, nouns in (
        ("footwear", shoe_section, _SHOES),
        ("socks", sections.get("袜子") or shoe_section, _SOCKS),
        ("bag", sections.get("包袋") or sections.get("包包") or sections.get("配饰", ""), _BAGS),
    ):
        put(key, _safe_item(section, nouns), "prompt_core_labeled_allowlist", ["prompt_core"])
    if not recipe.get("bag"):
        put("bag", _safe_item(_text(raw.get("accessory_level")), _BAGS), "structured_columns_allowlist", ["accessory_level"])
    if not recipe.get("accessories"):
        put("accessories", _safe_item(sections.get("配饰", ""), _ACCESSORIES), "prompt_core_labeled_allowlist", ["prompt_core"])
    # Bounded shape vocabulary supplements, rather than contradicts, columns.
    details = [x for x in ("弧形剪裁", "立体剪裁", "裤脚褶皱") if x in sections.get("下装", "")]
    if details:
        put("bottom_details", "、".join(details), "prompt_core_labeled_allowlist", ["prompt_core"])
        if recipe.get("bottom"):
            recipe["bottom"] += " " + recipe["bottom_details"]
            sources["bottom"]["supplement"] = "bottom_details"
    # Do not merge conflicting normalized descriptors into explicit recipes.
    if explicit:
        for field in ("bottom_type", "bottom_color", "bottom_fit", "bottom_details"):
            if explicit.get("bottom") and field not in explicit:
                recipe.pop(field, None)
                sources.pop(field, None)
        for key, value in explicit.items():
            recipe[key] = value
            sources[key] = {"source": "outfit_recipe", "columns": ["outfit_recipe." + key]}
    return recipe, sources


def _canonical(value: Any) -> str:
    return re.sub(r"[\s，,。()（）/、_-]+", "", unicodedata.normalize("NFKC", _text(value)).lower())


def outfit_visual_features(look_or_recipe: Mapping[str, Any]) -> dict[str, str]:
    recipe = look_or_recipe.get("recipe") if "recipe" in look_or_recipe else look_or_recipe
    recipe = dict(recipe or {})
    if not recipe.get("footwear"):
        recipe["footwear"] = recipe.get("shoes")
    if isinstance(recipe.get("bottom"), Mapping):
        bottom_fields = recipe["bottom"]
        parts = [_text(bottom_fields.get("type"))]
        for key in ("color", "fit"):
            value = _text(bottom_fields.get(key))
            if value and "按选中 Look" not in value and value not in parts[0]:
                parts.insert(0, value)
        recipe["bottom"] = " ".join(x for x in parts if x)
        recipe.setdefault("bottom_fit", bottom_fields.get("fit", ""))
        if "按选中 Look" not in _text(bottom_fields.get("color")):
            recipe.setdefault("bottom_color", bottom_fields.get("color", ""))
    recipe["onepiece"] = recipe.get("onepiece") or recipe.get("dress")
    result = {k: _canonical(recipe.get(k)) for k in ("top_inner", "onepiece", "bottom", "bottom_fit", "footwear", "socks", "bag", "accessories")}
    bottom = _text(recipe.get("onepiece") or recipe.get("bottom")).lower()
    if any(x in bottom for x in ("连衣裙", "dress", "无独立下装")):
        kind = "DRESS"
    elif any(x in bottom for x in ("短裙", "miniskirt", "mini skirt")):
        kind = "SHORT_SKIRT"
    elif any(x in bottom for x in ("裙", "skirt")):
        kind = "SKIRT"
    elif any(x in bottom for x in ("短裤", "shorts")):
        kind = "SHORTS"
    elif any(x in bottom for x in ("裤", "pants", "jeans", "denim")):
        kind = "PANTS"
    else:
        kind = "OTHER"
    color_text = _text(recipe.get("onepiece") or recipe.get("bottom_color") or recipe.get("bottom")).lower()
    colors = (("CHECK", ("格纹", "格子", "格", "check")), ("WHITE", ("白", "white", "cream")),
              ("BLUE", ("蓝", "blue")), ("BLACK", ("黑", "black")), ("GRAY", ("灰", "gray", "grey")), ("BROWN", ("棕", "brown")))
    color = next((name for name, tokens in colors if any(x in color_text for x in tokens)), "NEUTRAL")
    fit_text = _text(recipe.get("bottom_fit") or recipe.get("bottom")).lower()
    fit = next((name for name, tokens in (("WIDE", ("阔腿", "wide")), ("STRAIGHT", ("直筒", "straight")), ("PLEATED", ("百褶", "pleated"))) if any(x in fit_text for x in tokens)), "REGULAR")
    result["silhouette"] = "_".join((color, fit, kind))
    # Legacy rows without executable bottoms retain a stable fallback.
    if kind == "OTHER":
        result["silhouette"] = str(look_or_recipe.get("silhouette_key") or result["bottom"] or "UNSPECIFIED").upper()
    return result


def outfit_fingerprint(look_or_recipe: Mapping[str, Any], *, include_accessories: bool = True) -> str:
    features = outfit_visual_features(look_or_recipe)
    if not include_accessories:
        features = {k: v for k, v in features.items() if k not in {"accessories", "bag", "socks"}}
    return hashlib.sha256(json.dumps(features, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
