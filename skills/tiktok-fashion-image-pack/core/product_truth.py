#!/usr/bin/env python3
"""Product-truth extraction for womens tops and jackets."""

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


def build_product_truth_prompt(country: str, category: str, notes: str = "") -> str:
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
        image_paths=image_paths[:4],
        max_output_tokens=3200,
    )
    if not isinstance(result, dict):
        raise ValueError("product truth response must be a JSON object")
    return normalize_product_truth(result)


def normalize_product_truth(raw: Dict[str, Any]) -> Dict[str, Any]:
    truth = dict(raw)
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


def heuristic_product_truth(image_paths: List[str], category: str = "女装上装/外套") -> Dict[str, Any]:
    """Fallback used for dry-runs and tests; does not inspect pixels."""
    text = " ".join(Path(p).name for p in image_paths).lower() + " " + category.lower()
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
