#!/usr/bin/env python3
"""Prompt builders for likeU fashion image packs."""

from __future__ import annotations

from typing import Any, Dict, List


def build_main_image_prompt(
    *,
    product_truth: Dict[str, Any],
    brand_name: str = "likeU",
    label_strategy: str = "likeU + 产品类型",
    label_override: str = "",
    country: str = "TH",
) -> str:
    if is_hair_accessory(product_truth):
        return build_hair_accessory_main_image_prompt(
            product_truth=product_truth,
            brand_name=brand_name,
            label_strategy=label_strategy,
            label_override=label_override,
            country=country,
        )
    label = build_label(
        brand_name=brand_name,
        product_type=str(product_truth.get("product_type_name_en") or "FASHION JACKET"),
        strategy=label_strategy,
        override=label_override,
    )
    preserve = join_list(product_truth.get("must_preserve"))
    must_not_add = join_list(product_truth.get("must_not_add"))
    scenes = join_list(product_truth.get("recommended_scenes"))
    colors = [str(item).strip() for item in product_truth.get("sellable_colors_observed") or [] if str(item).strip()]
    layout = select_main_image_layout(product_truth)
    product_truth["main_image_template"] = layout["template"]
    multicolor_guidance = build_multicolor_guidance(colors) if layout["template"] == "womens_tops_multicolor_triptych" else ""
    details = [
        f"subtype: {product_truth.get('subtype')}",
        f"source image type: {product_truth.get('source_image_type')}",
        f"has on-body model reference: {product_truth.get('has_on_body_model')}",
        f"main color: {product_truth.get('main_color')}",
        f"material: {product_truth.get('material')}",
        f"silhouette: {product_truth.get('silhouette')}",
        f"length: {product_truth.get('length')}",
        f"collar: {product_truth.get('collar')}",
        f"closure: {product_truth.get('closure')}",
        f"pockets: {product_truth.get('pockets')}",
        f"sleeves: {product_truth.get('sleeves')}",
        f"hem: {product_truth.get('hem')}",
    ]
    return f"""
Create a 1:1 square TikTok Shop main product image for likeU, using the attached supplier images as strict product-truth references.

Target market: {country or "TH"}.
Goal: high-trust Thai/Korean fashion product main image with CHUUCHOP-inspired collage logic, optimized for product accuracy and conversion. It should feel branded and clean, not a luxury ad and not a cheap marketplace poster.

STRICT PRODUCT FIDELITY:
- Reference image order matters: IMAGE 1 is the default promoted hero color/style and must dominate the main image. Images 2+ are auxiliary color references only.
- Preserve the product exactly according to these facts: {'; '.join(details)}.
- Must preserve: {preserve}.
- Do NOT add or change: {must_not_add}.
- Do not invent colors, logos, extra accessories, new trims, new pockets, new fasteners, or a different garment length.
- No handbags, sunglasses, shoes, cups, gloves, scarves, or other non-sold accessories may appear in product-only panels.
- If the source garment is matte, keep it matte; if it is leather/PU, keep the correct soft sheen; if it is suede/nubuck, do not turn it into glossy leather.
{multicolor_guidance}

LAYOUT:
{layout["instructions"]}

TEXT LABEL:
- No price, no sale badge, no Chinese/Thai promotional text.
- Keep only one small elegant English label: "{label}".
- Product type text should stay in English for brand tone; do not translate it into Thai.

STYLE:
Warm neutral grading, cream/soft gray/wood/city palette, subtle shadows, realistic fabric texture, polished ecommerce look. Avoid over-stylized AI fashion editorial, stock-photo look, distorted hands/faces, confusing bundled-item visuals.
Model realism: when a model appears, use a realistic everyday Thai/Asian try-on look, natural body proportions, light makeup, candid posture, and product-first composition. Avoid perfect influencer face, beauty-retouched skin, luxury editorial posing, overly symmetrical facial features, and glamorous studio model look.

Suggested scenes: {scenes}.
""".strip()


def select_main_image_layout(product_truth: Dict[str, Any]) -> Dict[str, str]:
    if is_hair_accessory(product_truth):
        return select_hair_accessory_layout(product_truth)
    colors = [str(item).strip() for item in product_truth.get("sellable_colors_observed") or [] if str(item).strip()]
    is_multicolor = bool(product_truth.get("is_probably_multicolor")) or len(colors) > 1
    if is_product_only_reference(product_truth):
        return {
            "template": "womens_tops_product_only_to_tryon_truth_split",
            "instructions": build_product_only_to_tryon_layout(colors=colors, is_multicolor=is_multicolor),
        }
    if is_multicolor:
        return {
            "template": "womens_tops_multicolor_triptych",
            "instructions": build_multicolor_triptych_layout(colors),
        }
    if has_structure_feature(product_truth):
        return {
            "template": "womens_tops_structure_feature_split",
            "instructions": build_structure_feature_layout(),
        }
    if has_material_mood(product_truth):
        return {
            "template": "womens_tops_material_mood_split",
            "instructions": build_material_mood_layout(),
        }
    return {
        "template": "womens_tops_single_hero_detail_sidebar",
        "instructions": build_default_single_layout(),
    }


def build_hair_accessory_main_image_prompt(
    *,
    product_truth: Dict[str, Any],
    brand_name: str,
    label_strategy: str,
    label_override: str,
    country: str,
) -> str:
    label = build_label(
        brand_name=brand_name,
        product_type=str(product_truth.get("product_type_name_en") or "HAIR ACCESSORY"),
        strategy=label_strategy,
        override=label_override,
    )
    preserve = join_list(product_truth.get("must_preserve"))
    must_not_add = join_list(product_truth.get("must_not_add"))
    scenes = join_list(product_truth.get("recommended_scenes"))
    colors = [str(item).strip() for item in product_truth.get("sellable_colors_observed") or [] if str(item).strip()]
    layout = select_hair_accessory_layout(product_truth)
    product_truth["main_image_template"] = layout["template"]
    multicolor_guidance = build_hair_accessory_multicolor_guidance(colors) if is_multicolor_product(product_truth) else ""
    details = [
        f"subtype: {product_truth.get('subtype')}",
        f"source image type: {product_truth.get('source_image_type')}",
        f"has worn/model reference: {product_truth.get('has_on_body_model')}",
        f"main color: {product_truth.get('main_color')}",
        f"material: {product_truth.get('material')}",
        f"size/scale: {product_truth.get('size_scale')}",
        f"wearing position: {product_truth.get('wearing_position')}",
        f"grip/fastening structure: {product_truth.get('grip_structure')}",
        f"decorative elements: {product_truth.get('decorative_elements')}",
        f"pack count: {product_truth.get('pack_count')}",
    ]
    return f"""
Create a 1:1 square TikTok Shop main product image for likeU hair accessories, using the attached supplier images as strict product-truth references.

Target market: {country or "TH"}.
Goal: high-trust Thai/Vietnamese/Korean-style ecommerce main image, optimized for product accuracy, click appeal, and low-AI-feeling realism. It should feel clean and branded, not a beauty portrait, not a cheap marketplace poster.

STRICT PRODUCT FIDELITY:
- Reference image order matters: IMAGE 1 is the default promoted hero color/style and must dominate the main image. Images 2+ are auxiliary color references only.
- Preserve the product exactly according to these facts: {'; '.join(details)}.
- Must preserve: {preserve}.
- Do NOT add or change: {must_not_add}.
- Do not invent extra pieces, colors, pearls, rhinestones, bows, flowers, logos, cartoon IP, jewelry, or pack quantity.
- Keep the accessory scale believable on hair/head/hand. Do not make it oversized unless the source is clearly oversized.
- If pack count is unknown, do not show a set/combo; show only the observed product.
{multicolor_guidance}

LAYOUT:
{layout["instructions"]}

TEXT LABEL:
- No price, no sale badge, no Chinese/Thai/Vietnamese promotional text.
- Keep only one small elegant English label: "{label}".
- Product type text should stay in English for brand tone; do not translate it.

STYLE:
Warm neutral daylight, cream/soft gray/wood/vanity/cafe palette, subtle shadows, realistic material texture, polished ecommerce look.
Human realism: if hair/head/hand appears, use natural everyday Asian shopper styling, partial face or back/side hair crop, ordinary skin/hair texture, and product-first composition. Avoid perfect influencer face, glossy beauty retouching, symmetrical AI face, fantasy hair, luxury editorial posing, and unrelated jewelry focus.

Suggested scenes: {scenes}.
""".strip()


def select_hair_accessory_layout(product_truth: Dict[str, Any]) -> Dict[str, str]:
    colors = [str(item).strip() for item in product_truth.get("sellable_colors_observed") or [] if str(item).strip()]
    if is_multicolor_product(product_truth):
        return {
            "template": "hair_accessory_multicolor_options",
            "instructions": build_hair_accessory_multicolor_layout(colors),
        }
    if has_confirmed_pack_count(product_truth):
        return {
            "template": "hair_accessory_set_flatlay",
            "instructions": build_hair_accessory_set_flatlay_layout(),
        }
    if is_product_only_reference(product_truth):
        return {
            "template": "hair_accessory_product_detail_split",
            "instructions": build_hair_accessory_product_detail_layout(),
        }
    return {
        "template": "hair_accessory_worn_closeup_split",
        "instructions": build_hair_accessory_worn_closeup_layout(),
    }


def build_hair_accessory_worn_closeup_layout() -> str:
    return (
        "Use a premium worn-closeup + product proof layout.\n"
        "LEFT HERO ZONE, about 60-65% width and full height: close-up worn effect from IMAGE 1, using side hair, back bun, "
        "partial-face, or cropped-head composition. The accessory must be the first read and remain faithful in color, size, "
        "material, grip structure, and decorative elements.\n"
        "RIGHT TOP ZONE, about 35-40% width and 50-55% height: clean product-only proof on a neutral surface or simple hair/hand "
        "scale reference. Do not imply extra pieces are included.\n"
        "RIGHT BOTTOM ZONE, about 35-40% width and 45-50% height: 2-3 close detail crops such as claw teeth, clip spring, bow fabric, "
        "pearl/rhinestone decoration, elastic texture, or headband edge. No unrelated jewelry or cosmetics."
    )


def build_hair_accessory_product_detail_layout() -> str:
    return (
        "Use a product-only-to-usage truth split layout for references without a real wearing model.\n"
        "LEFT HERO ZONE, about 58-63% width and full height: create a believable close usage context, such as back/side hair crop, "
        "hand holding near hair, or vanity-table hand scale. Avoid full AI beauty face. Keep the accessory's exact shape, color, "
        "material, size, grip structure, and decoration.\n"
        "RIGHT TOP ZONE, about 37-42% width and 50-55% height: refined product-only proof from the source, with clean background, "
        "natural shadow, and sharp edges.\n"
        "RIGHT BOTTOM ZONE, about 37-42% width and 45-50% height: product-only detail/scale panel showing fastening/grip and material. "
        "Do not add extra pieces, pearls, bows, labels, jewelry, makeup, or set quantity."
    )


def build_hair_accessory_multicolor_layout(colors: List[str]) -> str:
    color_text = ", ".join(colors) if colors else "observed colors"
    return (
        "Use a premium multi-color hair accessory three-zone split layout.\n"
        "LEFT HERO ZONE, about 60-65% width and full height: IMAGE 1 promoted color worn close-up or hand/hair usage context. "
        "The IMAGE 1 color/style must dominate.\n"
        "RIGHT TOP ZONE, about 35-40% width and 50-55% height: second controlled usage/detail panel in the same promoted IMAGE 1 color only.\n"
        "RIGHT BOTTOM ZONE, about 35-40% width and 45-50% height: compact product-only color options for observed colors "
        f"({color_text}). Keep the same accessory shape and material across colors. Other colors must stay smaller than the IMAGE 1 hero. "
        "No extra unobserved colors, no extra set quantity, no unrelated jewelry/cosmetics."
    )


def build_hair_accessory_set_flatlay_layout() -> str:
    return (
        "Use a clean set/pack proof layout only for confirmed multi-piece products.\n"
        "LEFT HERO ZONE, about 58-63% width and full height: show the main promoted piece in a close worn or hand-scale context.\n"
        "RIGHT TOP ZONE: product-only flat-lay showing the exact confirmed pack contents, with no extra pieces.\n"
        "RIGHT BOTTOM ZONE: close detail crops of material, grip/fastening structure, and decoration. Keep the pack count accurate."
    )


def build_hair_accessory_multicolor_guidance(colors: List[str]) -> str:
    color_text = ", ".join(colors) if colors else "the observed colors"
    return f"""

MULTI-COLOR HAIR ACCESSORY RULES:
- This product has multiple sellable colors observed: {color_text}.
- The first reference image is the promoted hero color and must dominate the hero/usage panels.
- Show other colors only as compact product-only options unless a later scene slot explicitly asks for alternate color wearing.
- Do not invent new colors or make different colors look like different accessory shapes.
""".rstrip()


def build_multicolor_triptych_layout(colors: List[str]) -> str:
    color_text = ", ".join(colors) if colors else "observed colors"
    return (
        "Use a premium three-zone split layout, not a 2x2 grid.\n"
        "LEFT HERO ZONE, about 60-65% width and full height: cleaned real-reference hero panel from IMAGE 1. "
        "IMAGE 1 is the promoted default color/style. Crop tighter around the model and garment, remove clutter, "
        "improve lighting/color/sharpness, and keep the actual garment structure and outfit truth.\n"
        "RIGHT TOP ZONE, about 35-40% width and 55-60% height: controlled AI styling panel in the same promoted "
        "IMAGE 1 color only. Asian/Thai young woman, natural makeup, clean hair, simple fitted inner top and neutral "
        "bottoms only. Garment large, unobstructed, and clearly the same product.\n"
        "RIGHT BOTTOM ZONE, about 35-40% width and 40-45% height: clean product-only color options panel. Show compact "
        f"mini flat-lay/hanger thumbnails or neat swatches for the observed colors ({color_text}). The IMAGE 1 color "
        "should remain the largest option; other colors stay smaller. No model faces, handbags, sunglasses, shoes, "
        "cups, gloves, scarves, or other non-sold accessories. A tiny neutral English label such as \"3 colors\" is allowed."
    )


def build_product_only_to_tryon_layout(*, colors: List[str], is_multicolor: bool) -> str:
    color_sentence = ""
    if is_multicolor:
        color_text = ", ".join(colors) if colors else "observed colors"
        color_sentence = (
            f" If multiple colors are observed ({color_text}), keep IMAGE 1 as the promoted try-on color and include "
            "the other colors only as compact product-only mini thumbnails or small swatches in the lower-right detail area."
        )
    return (
        "Use a product-only-to-try-on truth split layout, designed for references without a real model.\n"
        "LEFT HERO ZONE, about 60-65% width and full height: create a faceless cropped try-on image from neck/chin "
        "to mid-thigh. The person should feel like an everyday Thai/Asian shopper or shop-owner try-on, not a "
        "perfect influencer or luxury model. Crop out the full face, use side angle, phone-covering-face, back/side "
        "pose, or low-head pose. Garment must dominate the frame and follow IMAGE 1 exactly for color, length, collar, "
        "closure, pockets, sleeves, hem, fabric thickness, and silhouette.\n"
        "RIGHT TOP ZONE, about 35-40% width and 50-55% height: refined product-only proof from the original reference. "
        "Use clean hanger, flat-lay, invisible mannequin, or pure-background presentation; improve light, shadow, "
        "edge clarity, and texture without changing structure.\n"
        "RIGHT BOTTOM ZONE, about 35-40% width and 45-50% height: product-only material and construction detail panel. "
        "Show 2-4 detail crops such as collar, closure, pocket, cuff/sleeve, hem, drawcord, quilting, knit, suede, "
        f"fur, or leather surface. No face, no handbags, no extra styling accessories.{color_sentence}\n"
        "ANTI-AI MODEL RULES: do not show a full perfect face, beauty portrait, glossy retouched skin, exaggerated "
        "model pose, luxury editorial lighting, or body proportions that make the product look unrealistic."
    )


def build_default_single_layout() -> str:
    return (
        "Use a premium single-color hero + detail sidebar layout, not a 2x2 grid.\n"
        "LEFT HERO ZONE, about 60-65% width and full height: model on-body hero from IMAGE 1. Crop tighter around the "
        "garment, clean the background, improve light and sharpness, and keep the actual product structure.\n"
        "RIGHT TOP ZONE, about 35-40% width and 55-60% height: controlled AI styling panel in the same color and same "
        "garment, front-facing or slight 3/4 angle, clean city/cafe/street background, hands not covering key details.\n"
        "RIGHT BOTTOM ZONE, about 35-40% width and 40-45% height: product-only detail panel with 2-4 clean crops of "
        "material, collar, cuff/sleeve, hem, closure, or pocket edge. No unrelated accessories."
    )


def build_structure_feature_layout() -> str:
    return (
        "Use a premium structure-feature split layout.\n"
        "LEFT HERO ZONE, about 55-60% width and full height: model on-body image from IMAGE 1, showing fit and length clearly.\n"
        "RIGHT TOP ZONE, about 40-45% width and 50-55% height: product-only structure view, such as hanger, flat-lay, or "
        "invisible mannequin front view. Make pocket layout, collar, closure, hem, and overall shape easy to inspect.\n"
        "RIGHT BOTTOM ZONE, about 40-45% width and 45-50% height: detail crops for structural selling points such as "
        "pockets, buttons/snaps, zipper/closure, collar, cuff, waistband, drawstring, or hardware. No unrelated accessories."
    )


def build_material_mood_layout() -> str:
    return (
        "Use a premium material-mood split layout.\n"
        "TOP LEFT and TOP RIGHT, together about 60% of total height: two on-body panels in the same color and same garment. "
        "One should feel like cleaned real-reference proof from IMAGE 1; the other may be a controlled city/cafe/street "
        "styling panel. Keep the garment large and unobstructed.\n"
        "BOTTOM ZONE, about 40% of total height: large product-only material detail area. Show realistic texture and 2-3 "
        "small construction details such as collar, cuff/sleeve, hem, closure, or fabric surface. Do not add unrelated accessories."
    )


def build_multicolor_guidance(colors: List[str]) -> str:
    color_text = ", ".join(colors) if colors else "the observed colors"
    return f"""

MULTI-COLOR PRODUCT RULES:
- This product has multiple sellable colors observed: {color_text}.
- The first reference image is the promoted hero color. Keep panels 1 and 2 in this hero color.
- Show the other colors only as compact product-only color options, not as additional worn outfits.
- Do not recolor the hero garment into an unobserved shade. Do not invent extra colorways.
- Color options should look like the same garment shape/material in different colors, not different products.
""".rstrip()


def build_panel_4_instruction(*, is_multicolor: bool, colors: List[str]) -> str:
    if not is_multicolor:
        return (
            "Show the garment alone, laid flat, on a clean hanger, or invisible mannequin, "
            "front view, open slightly if useful. No handbags, sunglasses, shoes, cups, gloves, "
            "scarves, or other non-sold accessories."
        )
    color_text = ", ".join(colors) if colors else "observed colors"
    return (
        "Create a clean product-only color options panel. Use IMAGE 1 as the large promoted "
        "hero color, then show compact smaller swatches or mini flat-lay/hanger thumbnails for "
        f"the other observed colors ({color_text}). Keep the same garment structure across all "
        "color options. Do not add model faces, handbags, sunglasses, shoes, cups, gloves, scarves, "
        "or other non-sold accessories. A tiny neutral label such as \"3 colors\" is allowed, "
        "but avoid promotional text."
    )


def has_structure_feature(product_truth: Dict[str, Any]) -> bool:
    text = " ".join(
        str(product_truth.get(key) or "").lower()
        for key in ("subtype", "collar", "closure", "pockets", "sleeves", "hem", "core_selling_points", "must_preserve")
    )
    for phrase in (
        "no visible buttons", "no buttons", "without buttons",
        "no visible button", "no visible zipper", "no zipper", "without zipper",
        "no visible pockets", "no clearly visible external pockets", "no pockets",
    ):
        text = text.replace(phrase, "")
    structure_terms = (
        "cargo", "flap pocket", "patch pocket", "chest pocket", "large pocket", "pocket layout",
        "button", "snap", "zipper", "zip", "belt", "drawstring", "toggle", "buckle", "stud",
        "baseball", "bomber", "ribbed", "hardware", "epaulet",
    )
    return any(term in text for term in structure_terms)


def is_product_only_reference(product_truth: Dict[str, Any]) -> bool:
    source_type = str(product_truth.get("source_image_type") or "unknown").strip().lower()
    has_model = product_truth.get("has_on_body_model")
    product_only_types = {"product_only", "hanger", "flat_lay", "white_bg"}
    if source_type in product_only_types:
        return True
    if has_model is False and source_type != "mixed":
        return True
    return False


def has_material_mood(product_truth: Dict[str, Any]) -> bool:
    text = " ".join(
        str(product_truth.get(key) or "").lower()
        for key in ("subtype", "material", "core_selling_points", "must_preserve")
    )
    material_terms = ("faux fur", "fur", "suede", "nubuck", "knit", "pu leather", "leather", "plush", "fleece")
    return any(term in text for term in material_terms)


def is_hair_accessory(product_truth: Dict[str, Any]) -> bool:
    return str(product_truth.get("category") or "").strip().lower() in {"hair_accessory", "hair_accessories", "发饰"}


def is_multicolor_product(product_truth: Dict[str, Any]) -> bool:
    colors = [str(item).strip() for item in product_truth.get("sellable_colors_observed") or [] if str(item).strip()]
    return bool(product_truth.get("is_probably_multicolor")) or len(colors) > 1


def has_confirmed_pack_count(product_truth: Dict[str, Any]) -> bool:
    text = str(product_truth.get("pack_count") or "").strip().lower()
    if not text or text in {"unknown", "ไม่ทราบ", "không rõ", "n/a", "none"}:
        return False
    return any(char.isdigit() for char in text) or any(token in text for token in ("pair", "set", "pack", "คู่", "เซต", "bộ", "cặp"))


def build_label(*, brand_name: str, product_type: str, strategy: str, override: str = "") -> str:
    brand = (brand_name or "likeU").strip() or "likeU"
    product = (override or product_type or "FASHION JACKET").strip().upper()
    normalized_strategy = (strategy or "likeU + 产品类型").strip()
    if normalized_strategy == "仅likeU":
        return brand
    if normalized_strategy == "仅产品类型":
        return product
    if normalized_strategy == "不加字":
        return ""
    return f"{brand} · {product}"


def join_list(value: Any) -> str:
    if isinstance(value, list):
        items: List[str] = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(items) if items else "none"
    text = str(value or "").strip()
    return text or "none"
