#!/usr/bin/env python3
"""Product-truth extraction for likeU fashion categories."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from vision_client import VisionJSONClient


SUPPORTED_SUBTYPES = [
    "leather_jacket",
    "suede_jacket",
    "utility_jacket",
    "puffer_jacket",
    "faux_fur_jacket",
    "knit_top",
    "cardigan",
    "unknown_womens_top",
]

HAIR_ACCESSORY_SUBTYPES = [
    "hair_clip",
    "claw_clip",
    "hair_bow",
    "headband",
    "scrunchie",
    "hair_tie",
    "hair_pin",
    "unknown_hair_accessory",
]

WIG_SUBTYPES = [
    "lace_front_wig",
    "full_cap_wig",
    "u_part_wig",
    "headband_wig",
    "ponytail_piece",
    "clip_in_extension",
    "hair_topper",
    "unknown_wig",
]

WIG_PRODUCT_FORMS = {
    "full_wig",
    "ponytail_piece",
    "clip_in_extension",
    "hair_topper",
    "unknown",
}


def is_hair_accessory_category(category: str) -> bool:
    return str(category or "").strip().lower() in {
        "发饰",
        "hair_accessory",
        "hair_accessories",
        "hair accessory",
        "hair accessories",
    }


def is_wig_category(category: str) -> bool:
    return str(category or "").strip().lower() in {
        "假发",
        "假髮",
        "wig",
        "wigs",
        "peluca",
        "pelucas",
    }


def build_product_truth_prompt(country: str, category: str, notes: str = "") -> str:
    if is_wig_category(category):
        return build_wig_truth_prompt(country=country, category=category, notes=notes)
    if is_hair_accessory_category(category):
        return build_hair_accessory_truth_prompt(country=country, category=category, notes=notes)
    return f"""
你是 TikTok Shop 女装商品图生成前的商品事实识别器。
请基于输入图片，识别这件商品的真实结构，用于后续 AI 主图生成。不要发挥，不要把商品改成更高级款。

目标国家: {country or "TH"}
人工类目: {category or "女装上装/外套"}
人工备注/覆盖要求: {notes or "无"}

只输出合法 JSON 对象，不要 markdown。字段必须完整：
{{
  "category": "womens_tops",
  "subtype": "leather_jacket|suede_jacket|utility_jacket|puffer_jacket|faux_fur_jacket|knit_top|cardigan|unknown_womens_top",
  "product_type_name_en": "2-4 words uppercase, e.g. SUEDE JACKET",
  "product_type_name_zh": "中文短类目名",
  "source_image_type": "on_body_model|product_only|hanger|flat_lay|white_bg|mixed|unknown",
  "has_on_body_model": true,
  "main_color": "",
  "is_probably_multicolor": false,
  "sellable_colors_observed": [],
  "material": "",
  "silhouette": "",
  "length": "",
  "collar": "",
  "closure": "",
  "pockets": "",
  "sleeves": "",
  "hem": "",
  "core_selling_points": [],
  "recommended_scenes": [],
  "target_customer": "",
  "must_preserve": [],
  "must_not_add": [],
  "non_sold_accessory_policy": "avoid non-sold accessories in the main image",
  "main_image_template": "womens_tops_2x2_truth_collage",
  "detail_image_sequence": [],
  "confidence": 0.0,
  "review_reasons": []
}}

识别规则：
- subtype 必须从枚举中选择。
- 图片顺序有业务含义：第 1 张图片默认是主推款/主推色，main_color 必须优先写第 1 张主推款颜色；第 2 张及之后只用于判断是否多色和补充售卖颜色。
- 如果多张图片是同款不同色，is_probably_multicolor=true，sellable_colors_observed 按图片顺序写观察到的颜色；不要把多色误判为不同商品系列。
- source_image_type 用来判断原图呈现方式：真人上身写 on_body_model；只有衣服挂拍/平铺/白底/纯背景图分别写 hanger/flat_lay/white_bg/product_only；既有真人又有商品图写 mixed。
- has_on_body_model 只要任意输入图里有真人穿着该商品就写 true；如果全部是挂拍、平铺、白底、纯背景商品图就写 false。
- material 要区分 PU leather / suede-nubuck / cotton twill / puffer / faux fur / knit。
- closure 必须写清楚是纽扣、按扣、拉链还是开衫。
- pockets 必须写口袋数量、位置和大致形态。
- must_preserve 写 5-10 个生成时绝不能改的结构点。
- must_not_add 写图片里没有但 AI 容易乱加的元素，如 hood, zipper, fur collar, quilting, belt, embroidery, extra pockets。
- product_type_name_en 会用于主图小字，与 likeU 组合展示；只写商品类型，不写促销。
- 如果图片太糊、遮挡严重或看不清结构，confidence 低于 0.65，并在 review_reasons 中说明。
""".strip()


def build_wig_truth_prompt(country: str, category: str, notes: str = "") -> str:
    return f"""
你是 TikTok Shop 墨西哥站假发商品图生成前的商品事实识别器。
请基于输入图片识别假发的真实外观与结构，用于平台主图、W1-W6 详情/场景图和西语标题生成。不要美化成更高等级材质，不要把“看起来自然”误判为真人发。

目标国家: {country or "MX"}
人工类目: {category or "假发"}
人工备注/覆盖要求: {notes or "无"}

只输出合法 JSON 对象，不要 markdown。字段必须完整：
{{
  "category": "wig",
  "subtype": "lace_front_wig|full_cap_wig|u_part_wig|headband_wig|ponytail_piece|clip_in_extension|hair_topper|unknown_wig",
  "product_form": "full_wig|ponytail_piece|clip_in_extension|hair_topper|unknown",
  "product_type_name_en": "WIG or LACE FRONT WIG",
  "product_type_name_zh": "中文短类目名",
  "source_image_type": "on_body_model|product_only|white_bg|mixed|unknown",
  "has_on_body_model": true,
  "has_back_reference": false,
  "has_hairline_reference": false,
  "has_cap_construction_reference": false,
  "has_attachment_reference": false,
  "main_color": "",
  "root_color": "",
  "color_gradient": "",
  "is_probably_multicolor": false,
  "sellable_colors_observed": [],
  "fiber_type": "human_hair|synthetic|heat_resistant_synthetic|blend|unknown",
  "construction_type": "lace_front|full_cap|u_part|headband|unknown",
  "lace_area": "",
  "hairline_type": "",
  "parting_type": "",
  "length": "",
  "density": "",
  "texture": "straight|wavy|curly|kinky|unknown",
  "curl_pattern": "",
  "bangs": "",
  "layers": "",
  "cap_size": "",
  "cap_features": "",
  "heat_resistance": "",
  "pack_count": "",
  "core_selling_points": [],
  "recommended_scenes": [],
  "target_customer": "",
  "must_preserve": [],
  "must_not_add": [],
  "non_sold_accessory_policy": "show only what the customer receives",
  "main_image_template": "wig_model_front_back_split",
  "detail_image_sequence": [],
  "confidence": 0.0,
  "review_reasons": []
}}

识别规则：
- 图片 1 是主推款/主推色；后续图片只用于补充背面、帽网、发际线、颜色和真实结构。
- 先判断 product_form：整顶覆盖头部为 full_wig；扎在自有马尾上的为 ponytail_piece；多片带夹接发为 clip_in_extension；只覆盖头顶稀疏区域为 hair_topper。不得把后三者识别成整顶假发。
- subtype 必须与 product_form 一致；无法判断产品形态时 product_form=unknown、subtype=unknown_wig，并在 review_reasons 说明，后续不得套用整顶假发帽网或背面模板。
- fiber_type 只有明确证据时才能写 human_hair、heat_resistant_synthetic 或 blend；否则写 synthetic 或 unknown。
- lace_area、density、cap_size、heat_resistance 及最高温度只能根据可见标签、规格或人工备注填写，禁止从外观猜测。
- length 优先保留明确厘米/英寸；卷发长度不确定时写 unknown，不得目测编造。
- 必须准确描述发根色、渐变位置、挑染分布、卷度、刘海、层次和发尾形态。
- has_hairline_reference 只有输入图清楚展示发际线/lace/分缝才为 true。
- has_cap_construction_reference 只有输入图清楚展示帽网内部、梳齿、卡扣或调节带才为 true。
- has_back_reference 只有输入图真实展示完整背面或完整产品背部才为 true；单张正面图不能视为背面证据。
- has_attachment_reference 只有输入图清楚展示马尾梳齿/抽绳、接发夹或发顶片卡扣/底网时才为 true。
- 没有结构参考时，在 review_reasons 说明缺失，后续不得生成虚构的帽网或 lace 证明图。
- must_preserve 至少包含主色、发根色/渐变、长度、卷度、发际线/分缝、帽网结构中可确认的项目。
- must_not_add 包含未观察到的 lace、刘海、挑染、额外发量、梳齿、卡扣、品牌标识、赠品和套装数量。
- 图片模糊或关键结构缺失时 confidence 低于 0.65。
""".strip()


def build_hair_accessory_truth_prompt(country: str, category: str, notes: str = "") -> str:
    return f"""
你是 TikTok Shop 发饰商品图生成前的商品事实识别器。
请基于输入图片，识别这件发饰的真实结构，用于后续 AI 主图/场景图/标题生成。不要发挥，不要把商品改成更高级款。

目标国家: {country or "TH"}
人工类目: {category or "发饰"}
人工备注/覆盖要求: {notes or "无"}

只输出合法 JSON 对象，不要 markdown。字段必须完整：
{{
  "category": "hair_accessory",
  "subtype": "hair_clip|claw_clip|hair_bow|headband|scrunchie|hair_tie|hair_pin|unknown_hair_accessory",
  "product_type_name_en": "2-4 words uppercase, e.g. CLAW CLIP",
  "product_type_name_zh": "中文短类目名",
  "source_image_type": "on_body_model|product_only|flat_lay|white_bg|mixed|unknown",
  "has_on_body_model": true,
  "main_color": "",
  "is_probably_multicolor": false,
  "sellable_colors_observed": [],
  "material": "",
  "size_scale": "",
  "wearing_position": "",
  "grip_structure": "",
  "decorative_elements": "",
  "pack_count": "",
  "core_selling_points": [],
  "recommended_scenes": [],
  "target_customer": "",
  "must_preserve": [],
  "must_not_add": [],
  "non_sold_accessory_policy": "avoid non-sold accessories that compete with the hair accessory",
  "main_image_template": "hair_accessory_worn_closeup_split",
  "detail_image_sequence": [],
  "confidence": 0.0,
  "review_reasons": []
}}

识别规则：
- subtype 必须从枚举中选择。
- 图片顺序有业务含义：第 1 张图片默认是主推款/主推色，main_color 必须优先写第 1 张主推款颜色；第 2 张及之后只用于判断是否多色和补充售卖颜色。
- 如果多张图片是同款不同色，is_probably_multicolor=true，sellable_colors_observed 按图片顺序写观察到的颜色。
- source_image_type：真人佩戴写 on_body_model；纯商品图写 product_only/flat_lay/white_bg；既有佩戴又有商品图写 mixed。
- has_on_body_model 只要任意输入图里有真人佩戴该发饰就写 true。
- material 要区分 satin / velvet / acrylic / plastic / metal / pearl-like / rhinestone-like / lace / fabric。
- size_scale 写清小/中/大/oversized/mini，避免生成时比例夸张。
- grip_structure 写清夹子、鲨鱼夹齿、弹力发圈、发箍框、边夹、蝴蝶结夹等固定方式。
- decorative_elements 写清蝴蝶结、珍珠、水钻、花朵、纯色、图案等；不确定真假时不要写“真”。
- pack_count 只有在图片或备注明确时才写数量；不确定就写 unknown，不要编造套装数量。
- must_preserve 写 5-10 个生成时绝不能改的点，包括颜色、尺寸比例、固定结构、装饰元素、材质表面。
- must_not_add 写图片里没有但 AI 容易乱加的元素，如 extra pearls, rhinestones, bow, flowers, logo, extra pieces, earrings, necklace。
- product_type_name_en 会用于主图小字，与 likeU 组合展示；只写商品类型，不写促销。
- 如果图片太糊、遮挡严重或看不清发饰结构，confidence 低于 0.65，并在 review_reasons 中说明。
""".strip()


def analyze_product_truth(
    *,
    image_paths: List[str],
    country: str,
    category: str,
    notes: str = "",
    client: Optional[VisionJSONClient] = None,
) -> Dict[str, Any]:
    if not image_paths:
        raise ValueError("image_paths is required")
    vision = client or VisionJSONClient()
    result = vision.call_json(
        build_product_truth_prompt(country=country, category=category, notes=notes),
        image_paths=image_paths[:6],
        max_output_tokens=3200,
    )
    if not isinstance(result, dict):
        raise ValueError("product truth response must be a JSON object")
    truth = normalize_product_truth(result, category_hint=category)
    return repair_multicolor_truth_from_sources(truth, image_paths)


def repair_multicolor_truth_from_sources(truth: Dict[str, Any], image_paths: List[str]) -> Dict[str, Any]:
    """Use a conservative local color fallback when vision misses obvious multi-color refs."""
    if len(image_paths) < 2:
        return truth
    existing_colors = [str(item).strip() for item in truth.get("sellable_colors_observed") or [] if str(item).strip()]
    if truth.get("is_probably_multicolor") and len(existing_colors) > 1:
        return truth

    inferred = infer_reference_color_names(image_paths[:4])
    unique = unique_preserve_order(inferred)
    if len(unique) < 2:
        return truth

    repaired = dict(truth)
    repaired["is_probably_multicolor"] = True
    repaired["sellable_colors_observed"] = unique
    repaired["main_color"] = unique[0]
    reasons = normalize_string_list(repaired.get("review_reasons"))
    reasons.append("local color fallback detected multiple source-image colors")
    repaired["review_reasons"] = unique_preserve_order(reasons)
    return repaired


def infer_reference_color_names(image_paths: List[str]) -> List[str]:
    colors: List[str] = []
    for path in image_paths:
        color = infer_reference_color_name(path)
        if color:
            colors.append(color)
    return colors


def infer_reference_color_name(image_path: str) -> str:
    try:
        from PIL import Image
    except Exception:
        return ""

    try:
        image = Image.open(image_path).convert("RGB")
    except Exception:
        return ""

    width, height = image.size
    left = int(width * 0.18)
    right = int(width * 0.82)
    top = int(height * 0.18)
    bottom = int(height * 0.62)
    crop = image.crop((left, top, right, bottom)).resize((80, 80))

    counts = {
        "black": 0,
        "ivory white": 0,
        "brown": 0,
        "gray": 0,
        "pink": 0,
        "khaki": 0,
        "other": 0,
    }
    for red, green, blue in crop.getdata():
        name = classify_rgb_color(red, green, blue)
        counts[name] = counts.get(name, 0) + 1

    # Prefer clothing-relevant dark/white/brown colors over weak background noise.
    priority = ["black", "ivory white", "brown", "khaki", "gray", "pink", "other"]
    ranked = sorted(counts.items(), key=lambda item: (item[1], -priority.index(item[0])), reverse=True)
    best, count = ranked[0]
    if count < 500 or best == "other":
        return ""
    return best


def classify_rgb_color(red: int, green: int, blue: int) -> str:
    max_c = max(red, green, blue)
    min_c = min(red, green, blue)
    delta = max_c - min_c
    avg = (red + green + blue) / 3

    if avg < 72:
        return "black"
    if avg > 185 and delta < 55:
        return "ivory white"
    if delta < 28:
        return "gray" if avg < 170 else "ivory white"
    if red > green > blue and red >= 85 and green >= 55 and blue <= 130:
        if avg < 170:
            return "brown"
        return "khaki"
    if red >= 140 and blue >= 110 and green < red * 0.9:
        return "pink"
    if red >= 120 and green >= 105 and blue <= 120:
        return "khaki"
    return "other"


def unique_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    unique: List[str] = []
    for item in items:
        normalized = str(item).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)
    return unique


def normalize_product_truth(raw: Dict[str, Any], category_hint: str = "") -> Dict[str, Any]:
    truth = dict(raw)
    raw_category = str(truth.get("category") or category_hint or "").strip()
    if is_wig_category(raw_category) or str(raw_category).strip().lower() == "wig":
        return normalize_wig_truth(truth)
    if is_hair_accessory_category(raw_category) or str(raw_category).strip().lower() == "hair_accessory":
        return normalize_hair_accessory_truth(truth)
    subtype = str(truth.get("subtype") or "").strip()
    if subtype not in SUPPORTED_SUBTYPES:
        subtype = infer_subtype_from_text(" ".join(str(v) for v in truth.values()))
    truth["category"] = "womens_tops"
    truth["subtype"] = subtype
    truth["product_type_name_en"] = normalize_type_name_en(
        truth.get("product_type_name_en"),
        subtype,
    )
    truth["product_type_name_zh"] = str(truth.get("product_type_name_zh") or subtype).strip()
    truth["source_image_type"] = normalize_source_image_type(truth.get("source_image_type"))
    truth["has_on_body_model"] = normalize_bool(truth.get("has_on_body_model"), default=True)
    truth["confidence"] = normalize_confidence(truth.get("confidence"))
    for key in ("sellable_colors_observed", "core_selling_points", "recommended_scenes", "must_preserve", "must_not_add", "detail_image_sequence", "review_reasons"):
        truth[key] = normalize_string_list(truth.get(key))
    if not truth["must_not_add"]:
        truth["must_not_add"] = default_must_not_add(subtype)
    if not truth["detail_image_sequence"]:
        truth["detail_image_sequence"] = [
            "cleaned real on-body proof",
            "material and construction details",
            "fit and length explanation",
            "scenario styling image",
        ]
    return truth


def normalize_hair_accessory_truth(raw: Dict[str, Any]) -> Dict[str, Any]:
    truth = dict(raw)
    subtype = str(truth.get("subtype") or "").strip()
    if subtype not in HAIR_ACCESSORY_SUBTYPES:
        subtype = infer_hair_accessory_subtype_from_text(" ".join(str(v) for v in truth.values()))
    truth["category"] = "hair_accessory"
    truth["subtype"] = subtype
    truth["product_type_name_en"] = normalize_type_name_en(
        truth.get("product_type_name_en"),
        subtype,
    )
    truth["product_type_name_zh"] = str(truth.get("product_type_name_zh") or subtype).strip()
    truth["source_image_type"] = normalize_source_image_type(truth.get("source_image_type"))
    truth["has_on_body_model"] = normalize_bool(truth.get("has_on_body_model"), default=False)
    truth["confidence"] = normalize_confidence(truth.get("confidence"))
    for key in ("sellable_colors_observed", "core_selling_points", "recommended_scenes", "must_preserve", "must_not_add", "detail_image_sequence", "review_reasons"):
        truth[key] = normalize_string_list(truth.get(key))
    for key in ("material", "size_scale", "wearing_position", "grip_structure", "decorative_elements", "pack_count"):
        truth[key] = str(truth.get(key) or "unknown").strip() or "unknown"
    if not truth["must_preserve"]:
        truth["must_preserve"] = [
            "main color",
            "actual size scale",
            "material surface",
            "grip or fastening structure",
            "decorative elements",
            "observed pack count only",
        ]
    if not truth["must_not_add"]:
        truth["must_not_add"] = default_hair_accessory_must_not_add(subtype)
    if not truth["detail_image_sequence"]:
        truth["detail_image_sequence"] = [
            "worn close-up proof",
            "product-only detail and scale",
            "grip structure close-up",
            "color or set options if observed",
        ]
    return truth


def normalize_wig_truth(raw: Dict[str, Any]) -> Dict[str, Any]:
    truth = dict(raw)
    subtype = str(truth.get("subtype") or "").strip()
    if subtype not in WIG_SUBTYPES:
        subtype = infer_wig_subtype_from_text(" ".join(str(v) for v in truth.values()))
    truth["category"] = "wig"
    truth["subtype"] = subtype
    truth["product_form"] = normalize_wig_product_form(truth.get("product_form"), subtype)
    truth["product_type_name_en"] = normalize_type_name_en(truth.get("product_type_name_en"), subtype)
    truth["product_type_name_zh"] = str(truth.get("product_type_name_zh") or "假发").strip()
    truth["source_image_type"] = normalize_source_image_type(truth.get("source_image_type"))
    truth["has_on_body_model"] = normalize_bool(truth.get("has_on_body_model"), default=False)
    truth["has_back_reference"] = normalize_bool(truth.get("has_back_reference"), default=False)
    truth["has_hairline_reference"] = normalize_bool(truth.get("has_hairline_reference"), default=False)
    truth["has_cap_construction_reference"] = normalize_bool(
        truth.get("has_cap_construction_reference"), default=False
    )
    truth["has_attachment_reference"] = normalize_bool(
        truth.get("has_attachment_reference"), default=False
    )
    truth["confidence"] = normalize_confidence(truth.get("confidence"))
    for key in (
        "sellable_colors_observed",
        "core_selling_points",
        "recommended_scenes",
        "must_preserve",
        "must_not_add",
        "detail_image_sequence",
        "review_reasons",
    ):
        truth[key] = normalize_string_list(truth.get(key))
    string_defaults = {
        "main_color": "unknown",
        "root_color": "unknown",
        "color_gradient": "none observed",
        "fiber_type": "unknown",
        "construction_type": "unknown",
        "lace_area": "unknown",
        "hairline_type": "unknown",
        "parting_type": "unknown",
        "length": "unknown",
        "density": "unknown",
        "texture": "unknown",
        "curl_pattern": "unknown",
        "bangs": "unknown",
        "layers": "unknown",
        "cap_size": "unknown",
        "cap_features": "unknown",
        "heat_resistance": "unknown",
        "pack_count": "unknown",
    }
    for key, default in string_defaults.items():
        truth[key] = str(truth.get(key) or default).strip() or default
    # Shared infrastructure still reads material; keep it aligned with the stricter wig field.
    truth["material"] = truth["fiber_type"]
    if not truth["must_preserve"]:
        truth["must_preserve"] = [
            "main and root color",
            "gradient/highlight placement",
            "wig length and layers",
            "texture and curl pattern",
            "bangs and parting",
            "observed hairline and cap construction only",
        ]
    if not truth["must_not_add"]:
        truth["must_not_add"] = default_wig_must_not_add(subtype)
    if not truth["detail_image_sequence"]:
        truth["detail_image_sequence"] = default_wig_detail_sequence(truth["product_form"])
    if truth["product_form"] == "full_wig" and not truth["has_back_reference"]:
        append_unique(truth["review_reasons"], "missing real back reference; generated back is visual inference")
    if truth["product_form"] == "full_wig" and not truth["has_hairline_reference"] and "missing clear hairline/lace reference" not in truth["review_reasons"]:
        truth["review_reasons"].append("missing clear hairline/lace reference")
    if truth["product_form"] == "full_wig" and not truth["has_cap_construction_reference"] and "missing cap construction reference" not in truth["review_reasons"]:
        truth["review_reasons"].append("missing cap construction reference")
    if truth["product_form"] in {"ponytail_piece", "clip_in_extension", "hair_topper"} and not truth["has_attachment_reference"]:
        append_unique(truth["review_reasons"], "missing attachment/base reference; do not invent fastening structure")
    if truth["product_form"] == "unknown":
        append_unique(truth["review_reasons"], "product form unresolved; do not render as a full wig")
    return truth


def heuristic_product_truth(image_paths: List[str], category: str = "女装上装/外套") -> Dict[str, Any]:
    """Fallback used for dry-runs and tests; does not inspect pixels."""
    text = " ".join(Path(p).name for p in image_paths).lower() + " " + category.lower()
    if is_wig_category(category):
        subtype = infer_wig_subtype_from_text(text)
        return normalize_product_truth(
            {
                "category": "wig",
                "subtype": subtype,
                "product_form": infer_wig_product_form_from_subtype(subtype),
                "product_type_name_en": normalize_type_name_en("", subtype),
                "product_type_name_zh": "假发",
                "source_image_type": "product_only",
                "has_on_body_model": False,
                "has_back_reference": False,
                "has_hairline_reference": False,
                "has_cap_construction_reference": False,
                "has_attachment_reference": False,
                "main_color": "unknown",
                "root_color": "unknown",
                "color_gradient": "unknown",
                "is_probably_multicolor": False,
                "sellable_colors_observed": [],
                "fiber_type": "unknown",
                "construction_type": subtype.replace("_wig", ""),
                "texture": infer_wig_texture_from_text(text),
                "core_selling_points": ["truth-first wig display", "natural everyday styling"],
                "recommended_scenes": ["front worn effect", "back length proof", "fiber close-up"],
                "target_customer": "Mexico women wig shoppers",
                "must_not_add": default_wig_must_not_add(subtype),
                "confidence": 0.35,
                "review_reasons": ["heuristic fallback; no vision model used"],
            },
            category_hint=category,
        )
    if is_hair_accessory_category(category):
        subtype = infer_hair_accessory_subtype_from_text(text)
        return normalize_product_truth(
            {
                "category": "hair_accessory",
                "subtype": subtype,
                "product_type_name_en": normalize_type_name_en("", subtype),
                "product_type_name_zh": subtype,
                "source_image_type": "product_only",
                "has_on_body_model": False,
                "main_color": "unknown",
                "is_probably_multicolor": False,
                "sellable_colors_observed": [],
                "material": "unknown",
                "size_scale": "unknown",
                "wearing_position": "hair styling area",
                "grip_structure": "unknown",
                "decorative_elements": "unknown",
                "pack_count": "unknown",
                "core_selling_points": ["clean hair accessory display", "easy everyday styling"],
                "recommended_scenes": ["hair close-up", "vanity table", "daily styling"],
                "target_customer": "young women shoppers",
                "must_preserve": ["main color", "size scale", "grip structure", "decorative elements", "material"],
                "must_not_add": default_hair_accessory_must_not_add(subtype),
                "confidence": 0.35,
                "review_reasons": ["heuristic fallback; no vision model used"],
            },
            category_hint=category,
        )
    subtype = infer_subtype_from_text(text)
    return normalize_product_truth(
        {
            "category": "womens_tops",
            "subtype": subtype,
            "product_type_name_en": normalize_type_name_en("", subtype),
            "product_type_name_zh": subtype,
            "main_color": "unknown",
            "is_probably_multicolor": False,
            "sellable_colors_observed": [],
            "material": subtype.replace("_", " "),
            "silhouette": "cropped or regular womens top/jacket",
            "length": "unknown",
            "collar": "unknown",
            "closure": "unknown",
            "pockets": "unknown",
            "sleeves": "unknown",
            "hem": "unknown",
            "core_selling_points": ["high-trust product display", "clean Thai/Korean styling"],
            "recommended_scenes": ["clean city/cafe background"],
            "target_customer": "Thai young women",
            "must_preserve": ["main color", "material", "collar", "closure", "pocket layout", "length"],
            "must_not_add": default_must_not_add(subtype),
            "confidence": 0.35,
            "review_reasons": ["heuristic fallback; no vision model used"],
        }
    )


def infer_subtype_from_text(text: str) -> str:
    lowered = text.lower()
    if re.search(r"suede|nubuck|麂皮|绒面", lowered):
        return "suede_jacket"
    if re.search(r"leather|pu|皮衣|皮革", lowered):
        return "leather_jacket"
    if re.search(r"utility|工装|twill|khaki", lowered):
        return "utility_jacket"
    if re.search(r"puffer|羽绒|棉服|down", lowered):
        return "puffer_jacket"
    if re.search(r"fur|皮草|毛绒|fluffy", lowered):
        return "faux_fur_jacket"
    if re.search(r"cardigan|开衫", lowered):
        return "cardigan"
    if re.search(r"knit|针织|毛衣", lowered):
        return "knit_top"
    return "unknown_womens_top"


def infer_hair_accessory_subtype_from_text(text: str) -> str:
    lowered = text.lower()
    if re.search(r"claw|càng cua|鲨鱼夹|กิ๊บหนีบ", lowered):
        return "claw_clip"
    if re.search(r"bow|蝴蝶结|โบว์|nơ", lowered):
        return "hair_bow"
    if re.search(r"headband|发箍|ที่คาดผม|băng đô|cài tóc", lowered):
        return "headband"
    if re.search(r"scrunchie|scrunchies|大肠|ยางมัดผม|dây buộc tóc", lowered):
        return "scrunchie"
    if re.search(r"hair.?tie|发圈|皮筋|ยางรัดผม|thun cột tóc", lowered):
        return "hair_tie"
    if re.search(r"pin|barrette|边夹|发卡|กิ๊บเป๊าะแป๊ะ|kẹp mái", lowered):
        return "hair_pin"
    if re.search(r"clip|发夹|กิ๊บ|kẹp tóc", lowered):
        return "hair_clip"
    return "unknown_hair_accessory"


def infer_wig_subtype_from_text(text: str) -> str:
    lowered = text.lower()
    if re.search(r"ponytail|drawstring|coleta[ _-]?postiza|cola[ _-]?de[ _-]?caballo|马尾", lowered):
        return "ponytail_piece"
    if re.search(r"clip[ _-]?in|extensiones?[ _-]?con[ _-]?clip|weft|发片|接发片", lowered):
        return "clip_in_extension"
    if re.search(r"topper|hair topper|top piece|发顶片|补发片|头顶补发", lowered):
        return "hair_topper"
    if re.search(r"u[ -]?part|u型|u形", lowered):
        return "u_part_wig"
    if re.search(r"headband|band wig|发带假发", lowered):
        return "headband_wig"
    if re.search(r"lace|front lace|lace front|蕾丝|头套前网|peluca de encaje", lowered):
        return "lace_front_wig"
    if re.search(r"full[ -]?cap|machine made|机制头套|全头套|peluca completa", lowered):
        return "full_cap_wig"
    return "unknown_wig"


def infer_wig_texture_from_text(text: str) -> str:
    lowered = text.lower()
    if re.search(r"curly|rizada|rizad[oa]|卷发|kinky", lowered):
        return "curly"
    if re.search(r"wavy|wave|ondulada|波浪", lowered):
        return "wavy"
    if re.search(r"straight|lacia|liso|直发", lowered):
        return "straight"
    return "unknown"


def normalize_type_name_en(value: Any, subtype: str) -> str:
    text = str(value or "").strip().upper()
    if text and len(text) <= 32:
        return re.sub(r"[^A-Z0-9 /-]", "", text).strip() or fallback_type_name(subtype)
    return fallback_type_name(subtype)


def normalize_source_image_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    allowed = {"on_body_model", "product_only", "hanger", "flat_lay", "white_bg", "mixed", "unknown"}
    if text in allowed:
        return text
    if any(token in text for token in ("hanger", "挂拍", "衣架")):
        return "hanger"
    if any(token in text for token in ("flat", "lay", "平铺")):
        return "flat_lay"
    if any(token in text for token in ("white", "白底")):
        return "white_bg"
    if any(token in text for token in ("model", "try", "wear", "真人", "上身", "模特")):
        return "on_body_model"
    if any(token in text for token in ("product", "商品", "纯背景")):
        return "product_only"
    return "unknown"


def normalize_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "有", "是"}:
        return True
    if text in {"false", "0", "no", "n", "无", "否"}:
        return False
    return default


def fallback_type_name(subtype: str) -> str:
    return {
        "leather_jacket": "LEATHER JACKET",
        "suede_jacket": "SUEDE JACKET",
        "utility_jacket": "UTILITY JACKET",
        "puffer_jacket": "PUFFER JACKET",
        "faux_fur_jacket": "FLUFFY JACKET",
        "knit_top": "KNIT TOP",
        "cardigan": "CARDIGAN",
        "hair_clip": "HAIR CLIP",
        "claw_clip": "CLAW CLIP",
        "hair_bow": "HAIR BOW",
        "headband": "HEADBAND",
        "scrunchie": "SCRUNCHIE",
        "hair_tie": "HAIR TIE",
        "hair_pin": "HAIR PIN",
        "unknown_hair_accessory": "HAIR ACCESSORY",
        "lace_front_wig": "LACE FRONT WIG",
        "full_cap_wig": "FULL CAP WIG",
        "u_part_wig": "U-PART WIG",
        "headband_wig": "HEADBAND WIG",
        "ponytail_piece": "PONYTAIL EXTENSION",
        "clip_in_extension": "CLIP-IN EXTENSION",
        "hair_topper": "HAIR TOPPER",
        "unknown_wig": "WIG",
    }.get(subtype, "FASHION JACKET")


def default_must_not_add(subtype: str) -> List[str]:
    base = ["extra pockets", "embroidery", "belt", "unrelated accessories", "large promotional text"]
    subtype_specific = {
        "leather_jacket": ["hood", "fur", "quilting", "zipper if not present", "suede texture"],
        "suede_jacket": ["hood", "fur", "quilting", "leather shine", "zipper if not present"],
        "utility_jacket": ["hood", "fur", "quilting", "leather shine", "ribbed bomber cuffs"],
        "puffer_jacket": ["leather shine", "unobserved fur collar", "unobserved hood"],
        "faux_fur_jacket": ["leather panels", "puffer quilting", "unobserved hood"],
        "knit_top": ["jacket pockets", "leather texture", "heavy outerwear structure"],
        "cardigan": ["leather texture", "hood", "puffer quilting"],
    }
    return subtype_specific.get(subtype, []) + base


def default_hair_accessory_must_not_add(subtype: str) -> List[str]:
    base = [
        "extra pieces",
        "extra colors",
        "brand logo",
        "cartoon IP",
        "earrings",
        "necklace",
        "makeup product",
        "large promotional text",
    ]
    subtype_specific = {
        "claw_clip": ["extra pearls", "rhinestones if not present", "bow if not present", "changed claw teeth"],
        "hair_bow": ["pearls if not present", "rhinestones if not present", "changed bow size", "extra lace"],
        "headband": ["extra bow", "extra pearls", "changed band thickness"],
        "scrunchie": ["metal clip", "extra bow", "changed fabric volume"],
        "hair_tie": ["large scrunchie fabric if not present", "extra charms"],
        "hair_pin": ["extra pearls", "extra rhinestones", "changed pin shape"],
    }
    return subtype_specific.get(subtype, []) + base


def default_wig_must_not_add(subtype: str) -> List[str]:
    base = [
        "unobserved color or highlights",
        "changed root color or gradient placement",
        "changed length, density, layers, bangs, texture, or curl pattern",
        "unobserved combs, clips, straps, lace area, or cap construction",
        "extra volume or extra hair pieces",
        "false human-hair or heat-resistant claim",
        "brand logo, border, watermark, price, or promotional text",
        "unobserved gifts or pack quantity",
    ]
    subtype_specific = {
        "lace_front_wig": ["changed lace width/depth", "invented baby hair", "different parting"],
        "full_cap_wig": ["invented lace front", "invented free parting"],
        "u_part_wig": ["changed U opening size", "invented lace hairline"],
        "headband_wig": ["changed headband width or color", "invented lace front"],
        "ponytail_piece": ["invented full wig cap", "invented drawstring or comb", "changed ponytail base"],
        "clip_in_extension": ["invented full wig cap", "changed piece count", "invented clips or weft widths"],
        "hair_topper": ["invented full wig cap", "changed base area", "invented clips or lace"],
    }
    return subtype_specific.get(subtype, []) + base


def infer_wig_product_form_from_subtype(subtype: str) -> str:
    if subtype in {"lace_front_wig", "full_cap_wig", "u_part_wig", "headband_wig"}:
        return "full_wig"
    if subtype in {"ponytail_piece", "clip_in_extension", "hair_topper"}:
        return subtype
    return "unknown"


def normalize_wig_product_form(value: Any, subtype: str) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "wig": "full_wig",
        "full cap": "full_wig",
        "full_cap": "full_wig",
        "ponytail": "ponytail_piece",
        "clip-in": "clip_in_extension",
        "clip_in": "clip_in_extension",
        "topper": "hair_topper",
    }
    text = aliases.get(text, text)
    if text in WIG_PRODUCT_FORMS:
        return text
    return infer_wig_product_form_from_subtype(subtype)


def default_wig_detail_sequence(product_form: str) -> List[str]:
    if product_form == "ponytail_piece":
        return [
            "W1 worn ponytail effect",
            "W2 complete detached product proof",
            "W3 base and hair-fiber detail",
            "W4 comb/drawstring proof when sourced",
            "W5 color/length/ends detail",
            "W6 daily lifestyle effect",
        ]
    if product_form == "clip_in_extension":
        return [
            "W1 worn extension effect",
            "W2 exact detached pieces proof",
            "W3 weft and fiber detail",
            "W4 clip structure proof when sourced",
            "W5 color/length/ends detail",
            "W6 daily lifestyle effect",
        ]
    if product_form == "hair_topper":
        return [
            "W1 top/front worn coverage effect",
            "W2 complete topper product proof",
            "W3 parting/crown detail when sourced",
            "W4 base and clip proof when sourced",
            "W5 color/length/ends detail",
            "W6 daily lifestyle effect",
        ]
    return [
        "W1 natural front or three-quarter worn effect",
        "W2 full back length and texture proof",
        "W3 hairline/parting proof when sourced",
        "W4 cap construction proof when sourced",
        "W5 fiber/color/length detail",
        "W6 daily lifestyle or observed color options",
    ]


def append_unique(items: List[str], value: str) -> None:
    if value not in items:
        items.append(value)


def normalize_string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [item.strip() for item in re.split(r"[,，;/；\n]", value) if item.strip()]
    return []


def normalize_confidence(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, parsed))
