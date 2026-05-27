#!/usr/bin/env python3
"""Scene image prompt builders for likeU womenswear image packs."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


DEFAULT_SCENE_SLOTS = ["S1", "S2", "S3", "S4"]
MULTICOLOR_EXTRA_SCENE_SLOTS = ["S5", "S6"]
ALL_SCENE_SLOTS = [*DEFAULT_SCENE_SLOTS, *MULTICOLOR_EXTRA_SCENE_SLOTS]


def build_scene_image_prompts(
    *,
    product_truth: Dict[str, Any],
    brand_name: str = "likeU",
    country: str = "TH",
    scene_slots: Optional[List[str]] = None,
    scene_preference: str = "",
    has_scene_reference: bool = False,
) -> List[Dict[str, Any]]:
    slots = normalize_scene_slots(scene_slots)
    if is_multicolor_product(product_truth) and slots == DEFAULT_SCENE_SLOTS:
        slots = [*DEFAULT_SCENE_SLOTS, *MULTICOLOR_EXTRA_SCENE_SLOTS]
    prompts: List[Dict[str, Any]] = []
    for slot in slots:
        spec = build_scene_spec(
            slot=slot,
            product_truth=product_truth,
            brand_name=brand_name,
            country=country,
            scene_preference=scene_preference,
            has_scene_reference=has_scene_reference,
        )
        prompts.append(spec)
    return prompts


def build_scene_spec(
    *,
    slot: str,
    product_truth: Dict[str, Any],
    brand_name: str,
    country: str,
    scene_preference: str = "",
    has_scene_reference: bool = False,
) -> Dict[str, Any]:
    normalized_slot = slot.upper().strip()
    facts = build_product_fact_lines(product_truth)
    product_only = is_product_only_reference(product_truth)
    multicolor = is_multicolor_product(product_truth)
    colors = [str(item).strip() for item in product_truth.get("sellable_colors_observed") or [] if str(item).strip()]
    target_color = choose_slot_target_color(normalized_slot, colors)
    scene = slot_scene_strategy(normalized_slot, product_truth, product_only=product_only, multicolor=multicolor)
    preserve = join_list(product_truth.get("must_preserve"))
    must_not_add = join_list(product_truth.get("must_not_add"))
    action = choose_detail_action(product_truth)
    preference_line = f"\nUser scene preference: {scene_preference.strip()}." if scene_preference.strip() else ""
    color_rule = build_color_rule(colors, multicolor)
    model_rule = build_model_rule(product_only=product_only)
    scene_reference_rule = build_scene_reference_rule(has_scene_reference)
    prompt = f"""
Create one 1:1 square TikTok Shop lifestyle scene image for the likeU product image pack.

Image slot: {normalized_slot} - {scene["role"]}.
Target market: {country or "TH"}.
Use the attached supplier images as strict product-truth references. IMAGE 1 is the default promoted color/style and remains the priority reference.
{scene_reference_rule}

STRICT PRODUCT FIDELITY:
- Preserve these product facts exactly: {'; '.join(facts)}.
- Must preserve: {preserve}.
- Do NOT add or change: {must_not_add}.
- Keep collar, closure, pocket count/position, sleeve/cuff, hem, length, thickness, silhouette, and fabric surface faithful to the reference.
- Do not invent hats, scarves, handbags, sunglasses, gloves, cups, shoes as sellable bundled items. Simple non-sold background props may appear only when they are clearly not part of the offer.
- If the reference has no zipper, do not add a zipper. If the reference has no visible buttons, do not add buttons. If pockets are not visible, do not invent large pockets.
- Target worn color for this image: {target_color}.
{color_rule}

SCENE DIRECTION:
{scene["instruction"]}
- This slot must look clearly different from the other scene slots in pose, crop, camera distance, or setting. Do not create near-duplicate cafe-front smiling portraits across S1/S2/S3.
{detail_action_line(normalized_slot, action)}
{model_rule}
{preference_line}

STYLE:
- Thai/Korean everyday fashion ecommerce, clean and high-trust, not luxury editorial and not cheap marketplace poster.
- Warm neutral daylight, realistic texture, natural body proportions, ordinary shopper/store-owner try-on energy.
- Product must occupy 55-75% of the frame and remain easy to inspect.
- Keep only a tiny optional English micro-label if needed: "{brand_name}"; no price, sale badge, long text, Chinese text, or Thai promotional copy.

NEGATIVE PROMPT:
perfect influencer face, plastic AI skin, luxury runway pose, distorted hands, extra fingers, changed garment structure, wrong color, wrong fabric, invented trims, unreadable product, cluttered collage, promotional poster text.
""".strip()
    return {
        "image_id": normalized_slot,
        "image_role": scene["role"],
        "scene_strategy": scene["strategy"],
        "target_color": target_color,
        "detail_action": action if normalized_slot == "S4" else "",
        "reference_policy": "IMAGE 1 is promoted hero reference; other images only verify colors/details.",
        "prompt": prompt,
    }


def normalize_scene_slots(scene_slots: Optional[List[str]]) -> List[str]:
    if not scene_slots:
        return list(DEFAULT_SCENE_SLOTS)
    valid = []
    for slot in scene_slots:
        normalized = str(slot or "").strip().upper()
        for candidate in ALL_SCENE_SLOTS:
            if normalized == candidate or normalized.startswith(candidate + " ") or normalized.startswith(candidate + "-"):
                if candidate not in valid:
                    valid.append(candidate)
    return valid or list(DEFAULT_SCENE_SLOTS)


def parse_scene_slots(raw_value: Any) -> List[str]:
    if isinstance(raw_value, list):
        parts = []
        for item in raw_value:
            if isinstance(item, dict):
                text_item = str(item.get("text") or item.get("name") or item.get("value") or "").strip()
            else:
                text_item = str(item or "").strip()
            if text_item:
                parts.append(text_item)
        return normalize_scene_slots(parts)
    text = str(raw_value or "").strip()
    if not text:
        return list(DEFAULT_SCENE_SLOTS)
    parts = text.replace("，", ",").replace("/", ",").replace("、", ",").replace(" ", ",").split(",")
    return normalize_scene_slots(parts)


def slot_scene_strategy(
    slot: str,
    product_truth: Dict[str, Any],
    *,
    product_only: bool,
    multicolor: bool,
) -> Dict[str, str]:
    scenes = recommended_scene_text(product_truth)
    if slot == "S1":
        if product_only:
            instruction = (
                "Create a believable faceless or weak-face try-on scene: crop from chin/neck to mid-thigh, side angle, "
                "phone-covering-face pose, back/side pose, or low-head candid pose. The goal is click appeal while avoiding "
                "an obviously AI-perfect model. Use a simple cafe entrance, apartment hallway, clean street, or shop mirror setting."
            )
        else:
            instruction = (
                "Create a cleaned hero lifestyle scene based on the real on-body reference. Keep the wearer natural and candid, "
                "improve background/light, and make the garment the clear first read. Use the strongest click-oriented crop, "
                "but avoid a full front-facing beauty smile; prefer a partial face, looking slightly away/down, mirror phone, "
                "or candid side angle."
            )
        role = "hero lifestyle try-on"
        strategy = "realistic_tryon"
    elif slot == "S2":
        instruction = (
            f"Create a daily outing scene that matches the product's plausible use cases ({scenes}). Use a clean city street, "
            "mall walkway, cafe exterior, campus path, or casual travel moment. This must not repeat the S1 pose or camera distance. "
            "The outfit should stay simple and not imply unavailable accessories are included. Use a walking, looking-down, side-glance, "
            "or small-face candid crop rather than a direct camera-facing portrait."
        )
        role = "daily use atmosphere"
        strategy = "daily_scene"
    elif slot == "S3":
        if multicolor:
            instruction = (
                "Create a fit-and-color proof scene. Use the IMAGE 1 color on the wearer as the main subject, then show other "
                "observed colors only as small product-only swatches or mini hanger/flat-lay references in one clean corner. "
                "Do not put multiple models in different colors."
            )
        else:
            instruction = (
                "Create a fit proof scene showing length, shoulder shape, sleeve volume, and hem clearly. Use a front or slight "
                "3/4 standing pose with hands away from key details. Keep the background calm and product-first, with a more analytical "
                "fit-inspection feel than S1/S2."
            )
        role = "fit and color proof"
        strategy = "fit_proof"
    elif slot == "S4":
        instruction = (
            "Create a close lifestyle detail scene. The image can be waist-up or cropped closer, but must still show enough of "
            "the garment to recognize the product. Focus on one real structural/material action from the reference."
        )
        role = "material and construction detail"
        strategy = "detail_action"
    elif slot == "S5":
        instruction = (
            "Create an alternate color on-body try-on scene for a multi-color product. Use the assigned target worn color only, "
            "based on observed reference colors. Make this visibly different from S1-S3 through pose and setting: mirror selfie, "
            "shop/studio fitting corner, or clean indoor daylight. Prefer weak-face or phone-covering-face try-on. Keep the garment "
            "structure identical across colors."
        )
        role = "alternate color try-on 1"
        strategy = "multicolor_tryon"
    elif slot == "S6":
        instruction = (
            "Create a second alternate color on-body try-on scene for a multi-color product. Use the assigned target worn color only, "
            "based on observed reference colors. Make this a different angle from S5, such as slight side view, walking pose, or "
            "waist-up detail with clear hem and sleeve shape. Prefer cropped/side/looking-down face treatment. Do not create a new "
            "unobserved color."
        )
        role = "alternate color try-on 2"
        strategy = "multicolor_tryon"
    else:
        instruction = "Create a clean realistic lifestyle try-on scene with strict product fidelity."
        role = "lifestyle scene"
        strategy = "general_scene"
    return {"role": role, "strategy": strategy, "instruction": instruction}


def choose_slot_target_color(slot: str, colors: List[str]) -> str:
    hero_color = colors[0] if colors else "IMAGE 1 hero color"
    if slot == "S5":
        return colors[1] if len(colors) >= 2 else hero_color
    if slot == "S6":
        if len(colors) >= 3:
            return colors[2]
        if len(colors) >= 2:
            return f"{colors[1]} in a different angle from S5"
        return hero_color
    if slot == "S3" and len(colors) > 1:
        return f"{hero_color}; show other colors only as compact product-only references"
    return hero_color


def choose_detail_action(product_truth: Dict[str, Any]) -> str:
    text = " ".join(
        str(product_truth.get(key) or "").lower()
        for key in ("closure", "pockets", "collar", "sleeves", "hem", "material", "core_selling_points", "must_preserve")
    )
    negative_zip = any(phrase in text for phrase in ("no zipper", "without zipper", "no visible zipper"))
    negative_button = any(phrase in text for phrase in ("no button", "without button", "no visible button"))
    if any(term in text for term in ("drawstring", "drawcord", "toggle")):
        return "hand lightly adjusting the real drawstring/drawcord at the hem or collar"
    if "zip" in text and not negative_zip:
        return "hand lightly holding the real zipper pull without inventing new hardware"
    if any(term in text for term in ("snap", "button")) and not negative_button:
        return "hand lightly touching the real snap/button closure"
    if "pocket" in text and "no visible pocket" not in text and "no pocket" not in text:
        return "hand near the real pocket edge, showing pocket placement and stitching"
    if any(term in text for term in ("fur", "fleece", "suede", "nubuck", "leather", "knit", "puffer", "quilt")):
        return "close crop of the real material surface and edge stitching"
    if "collar" in text:
        return "close crop of the collar shape and neckline"
    if any(term in text for term in ("cuff", "sleeve")):
        return "close crop of sleeve volume and cuff shape"
    return "close crop of the real fabric texture, collar, and hem edge"


def detail_action_line(slot: str, action: str) -> str:
    if slot != "S4":
        return ""
    return f"\nS4 DETAIL ACTION: {action}. Use only this real product action; do not invent unrelated fasteners or accessories."


def build_product_fact_lines(product_truth: Dict[str, Any]) -> List[str]:
    keys = [
        ("subtype", "subtype"),
        ("source_image_type", "source image type"),
        ("main_color", "main color"),
        ("material", "material"),
        ("silhouette", "silhouette"),
        ("length", "length"),
        ("collar", "collar"),
        ("closure", "closure"),
        ("pockets", "pockets"),
        ("sleeves", "sleeves"),
        ("hem", "hem"),
    ]
    return [f"{label}: {product_truth.get(key)}" for key, label in keys]


def build_color_rule(colors: List[str], multicolor: bool) -> str:
    if not multicolor:
        return "- Use only the IMAGE 1 color as the sold product color in this image."
    color_text = ", ".join(colors) if colors else "observed reference colors"
    return (
        f"- Observed sellable colors: {color_text}.\n"
        "- IMAGE 1 is the promoted hero color. It must be the worn color unless the slot explicitly shows product-only color references.\n"
        "- Other colors can appear only as compact product-only swatches/mini references, not as extra worn outfits."
    )


def build_model_rule(*, product_only: bool) -> str:
    if product_only:
        return (
            "\nMODEL REALISM FOR PRODUCT-ONLY REFERENCES:\n"
            "- Use faceless/weak-face composition by default: cropped face, side/back angle, phone-covering-face, or lowered head.\n"
            "- Avoid a full perfect AI face, glossy retouched skin, exaggerated pose, or unrealistic body proportions.\n"
            "- The model is a natural support for scale and fit, not a beauty portrait."
        )
    return (
        "\nMODEL REALISM:\n"
        "- If a model appears, keep an everyday Thai/Asian try-on look with natural proportions, light makeup, and candid posture.\n"
        "- Avoid perfect influencer face, direct camera-facing beauty smile, luxury editorial posing, and beauty-retouched skin.\n"
        "- Prefer weak-face realism: partial face, looking down, side glance, hair partly covering face, mirror phone, or cropped face. "
        "The face should not become the selling point."
    )


def build_scene_reference_rule(has_scene_reference: bool) -> str:
    if not has_scene_reference:
        return ""
    return (
        "\nAdditional attached images after the product references are scene/style references only. "
        "Use them for background mood, camera angle, lighting, and composition. Do not copy any garment, color, "
        "accessory, logo, model styling, or product detail from scene references."
    )


def is_multicolor_product(product_truth: Dict[str, Any]) -> bool:
    colors = [str(item).strip() for item in product_truth.get("sellable_colors_observed") or [] if str(item).strip()]
    return bool(product_truth.get("is_probably_multicolor")) or len(colors) > 1


def is_product_only_reference(product_truth: Dict[str, Any]) -> bool:
    source_type = str(product_truth.get("source_image_type") or "unknown").strip().lower()
    has_model = product_truth.get("has_on_body_model")
    product_only_types = {"product_only", "hanger", "flat_lay", "white_bg"}
    if source_type in product_only_types:
        return True
    if has_model is False and source_type != "mixed":
        return True
    return False


def recommended_scene_text(product_truth: Dict[str, Any]) -> str:
    value = product_truth.get("recommended_scenes")
    if isinstance(value, list):
        text = ", ".join(str(item).strip() for item in value if str(item).strip())
        return text or "daily commuting, casual shopping, cafe, mild winter travel"
    return str(value or "daily commuting, casual shopping, cafe, mild winter travel")


def join_list(value: Any) -> str:
    if isinstance(value, list):
        items: List[str] = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(items) if items else "none"
    text = str(value or "").strip()
    return text or "none"
