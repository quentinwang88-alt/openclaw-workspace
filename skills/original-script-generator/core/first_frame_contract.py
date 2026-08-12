"""Compile a user-triggered, cacheable first-frame image contract.

The contract is deliberately downstream of the complete script.  It freezes
the already selected product/persona/outfit/scene/opening state and never
re-selects creative inputs or changes the script/voiceover.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping, Sequence


CONTRACT_VERSION = "original-first-frame-contract-v2"
PROMPT_VERSION = "original-first-frame-prompt-v2-compact-scene"
DEFAULT_IMAGE_MODEL = "gpt-image-2"
DEFAULT_ASPECT_RATIO = "9:16"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, (list, tuple)) else []


def _stable_hash(value: Any, length: int = 32) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _asset_id(value: Any) -> str:
    if isinstance(value, Mapping):
        return _text(
            value.get("file_token")
            or value.get("asset_id")
            or value.get("cached_path")
            or value.get("path")
            or value.get("url")
        )
    return _text(value)


def _first_shot(script: Mapping[str, Any], brief: Mapping[str, Any]) -> Dict[str, Any]:
    for source in (brief.get("storyboard"), script.get("storyboard")):
        shots = _list(source)
        if shots and isinstance(shots[0], Mapping):
            return dict(shots[0])
    return {}


def _opening_snapshot(script: Mapping[str, Any], brief: Mapping[str, Any]) -> Dict[str, Any]:
    shot = _first_shot(script, brief)
    allocated = _dict(script.get("allocated_direction"))
    opening_job = _dict(allocated.get("opening_visual_job"))
    return {
        "opening_job": _text(opening_job.get("job")),
        "opening_guidance": _text(opening_job.get("guidance_zh")),
        "visual_content": _text(shot.get("visual_content")),
        "character_action": _text(shot.get("character_action")),
        "natural_emotion": _text(shot.get("natural_emotion")),
        "camera": _text(shot.get("camera")),
        "product_anchors_visible": [
            _text(item) for item in _list(shot.get("product_anchors_visible")) if _text(item)
        ],
    }


def build_first_frame_contract(
    *,
    script_id: str,
    product_code: str,
    product_images: Sequence[Mapping[str, Any]],
    script: Mapping[str, Any],
    aspect_ratio: str = DEFAULT_ASPECT_RATIO,
    model: str = DEFAULT_IMAGE_MODEL,
) -> Dict[str, Any]:
    """Freeze one first-frame request from the completed production script."""

    brief = _dict(script.get("video_generation_brief"))
    production = _dict(brief.get("production_design")) or _dict(
        script.get("production_design")
    )
    persona = _dict(brief.get("persona_selection_contract")) or _dict(
        production.get("persona_selection_contract")
    )
    from core.outfit_selection import upgrade_outfit_structure_contract
    from core.visual_execution_contract import (
        build_opening_scene_projection,
        finalize_visual_execution_contract,
    )

    outfit = upgrade_outfit_structure_contract(
        _dict(brief.get("outfit_selection_contract"))
    )
    outfit_projection = _dict(brief.get("outfit_prompt_projection"))
    scene = _dict(production.get("scene"))
    visual_execution = finalize_visual_execution_contract(
        _dict(brief.get("visual_execution_contract")),
        production_scene=scene,
        presentation_mode=_text(production.get("presentation_mode")),
    )
    visual_saliency = _dict(visual_execution.get("visual_saliency"))
    opening_scene_projection = _dict(
        visual_execution.get("opening_scene_projection")
    )
    if not opening_scene_projection:
        opening_scene_projection = build_opening_scene_projection(
            scene,
            presentation_mode=_text(production.get("presentation_mode")),
        )
    identity = _dict(brief.get("product_identity_lock"))
    product_truth = _dict(brief.get("product_truth"))
    product_refs = [dict(item) for item in product_images if isinstance(item, Mapping)]
    persona_refs = [
        dict(item) if isinstance(item, Mapping) else item
        for item in _list(persona.get("reference_images"))
        if _asset_id(item)
    ]
    presentation = _text(production.get("presentation_mode")).upper()
    persona_available = _text(persona.get("availability")).upper() == "AVAILABLE"
    if presentation in {"PERSON_ON_CAMERA", "WEARER_ACTIVE", "MIXED"} and not persona_available:
        availability = "PERSONA_REFERENCE_UNAVAILABLE"
    elif not product_refs:
        availability = "PRODUCT_REFERENCE_UNAVAILABLE"
    else:
        availability = "AVAILABLE"

    contract: Dict[str, Any] = {
        "schema_version": CONTRACT_VERSION,
        "prompt_version": PROMPT_VERSION,
        "availability": availability,
        "script_id": _text(script_id),
        "product_code": _text(product_code),
        "image_model": _text(model) or DEFAULT_IMAGE_MODEL,
        "aspect_ratio": _text(aspect_ratio) or DEFAULT_ASPECT_RATIO,
        "reference_order": ["PRODUCT_IDENTITY", "PERSONA_IDENTITY"],
        "product_reference_assets": product_refs,
        "product_reference_asset_ids": [_asset_id(item) for item in product_refs],
        "persona_reference_assets": persona_refs,
        "persona_reference_asset_ids": [_asset_id(item) for item in persona_refs],
        "product_identity_lock": identity,
        "product_truth": product_truth,
        "persona_contract": persona,
        "outfit_contract": outfit,
        "outfit_prompt_projection": outfit_projection,
        "scene_contract": scene,
        "visual_execution_contract": visual_execution,
        "visual_saliency": visual_saliency,
        "opening_scene_projection": opening_scene_projection,
        "opening_contract": _opening_snapshot(script, brief),
        "presentation_mode": presentation,
        "capture_mode": _text(brief.get("capture_mode") or production.get("capture_mode")),
        "authority_order": [
            "PRODUCT_REFERENCES_CONTROL_PRODUCT_IDENTITY",
            "PERSONA_REFERENCES_CONTROL_PERSON_IDENTITY",
            "FROZEN_OUTFIT_CONTROLS_STYLING",
            "FROZEN_SCENE_CONTROLS_ENVIRONMENT",
            "FROZEN_OPENING_CONTROLS_START_STATE_AND_FRAMING",
        ],
    }
    fingerprint_payload = {
        "prompt_version": contract["prompt_version"],
        "image_model": contract["image_model"],
        "aspect_ratio": contract["aspect_ratio"],
        "product_code": contract["product_code"],
        "product_reference_asset_ids": contract["product_reference_asset_ids"],
        "product_identity_lock": identity,
        "persona_id": _text(persona.get("persona_id")),
        "persona_version": _text(persona.get("template_version")),
        "persona_body_proportion": _text(
            _dict(persona.get("identity_lock")).get("body_proportion_text")
        ),
        "persona_structured_snapshot_hash": _stable_hash(
            persona.get("structured_template_snapshot") or {}, 20
        ),
        "persona_reference_asset_ids": contract["persona_reference_asset_ids"],
        "outfit_template_id": _text(outfit.get("template_id")),
        "outfit_template_version": _text(outfit.get("template_version")),
        "outfit_recipe": _dict(outfit.get("outfit_recipe")),
        "outfit_structure": _text(outfit.get("outfit_structure")),
        "outfit_structured_snapshot_hash": _stable_hash(
            outfit.get("structured_template_snapshot") or {}, 20
        ),
        "scene_contract": scene,
        "opening_scene_projection": opening_scene_projection,
        "visual_saliency": visual_saliency,
        "opening_contract": contract["opening_contract"],
    }
    contract["asset_fingerprint"] = _stable_hash(fingerprint_payload, 40)
    contract["contract_id"] = "FFC_" + _stable_hash(fingerprint_payload, 20).upper()
    return contract


def _lines(title: str, values: Sequence[Any]) -> str:
    cleaned = [_text(item) for item in values if _text(item)]
    return f"{title}：" + ("；".join(cleaned) if cleaned else "无额外已批准信息")


def render_first_frame_prompt(contract: Mapping[str, Any]) -> str:
    """Render the exact image-edit prompt; reference roles stay explicit."""

    identity = _dict(contract.get("product_identity_lock"))
    truth = _dict(contract.get("product_truth"))
    persona = _dict(contract.get("persona_contract"))
    persona_projection = _dict(persona.get("script_projection"))
    outfit = _dict(contract.get("outfit_contract"))
    outfit_projection = _dict(contract.get("outfit_prompt_projection"))
    scene = _dict(contract.get("scene_contract"))
    visual_saliency = _dict(contract.get("visual_saliency"))
    opening_scene = _dict(contract.get("opening_scene_projection"))
    opening = _dict(contract.get("opening_contract"))
    must_preserve = _list(identity.get("must_preserve")) or _list(truth.get("identity_anchors"))
    must_not = _list(identity.get("must_not_change"))
    quantity = _dict(truth.get("display_quantity_contract"))
    prompt_negative = _text(persona.get("prompt_negative"))
    target_role = _text(outfit.get("target_role"))
    body_proportion = _text(
        _dict(persona.get("identity_lock")).get("body_proportion_text")
    )
    recipe = _dict(outfit.get("outfit_recipe"))
    recipe_labels = {
        "one_piece": "连体单品",
        "top": "上装",
        "bottom": "下装",
        "footwear": "鞋履",
        "bag": "包袋",
        "other_accessories": "辅助配饰",
    }
    recipe_text = "；".join(
        f"{label}：{_text(recipe.get(key))}"
        for key, label in recipe_labels.items()
        if _text(recipe.get(key))
    )
    frozen_outfit_text = (
        recipe_text
        if _text(outfit.get("outfit_structure")).upper() == "ONE_PIECE"
        else _text(outfit_projection.get("frozen_outfit")) or recipe_text
    )
    exposure = _dict(visual_saliency.get("exposure"))
    separation = _dict(visual_saliency.get("separation"))
    opening_focus = _dict(visual_saliency.get("opening_focus"))
    dense_opening = _text(opening_scene.get("source_density")).upper() == "HIGH"
    opening_visual = _text(opening.get("visual_content"))
    opening_action = _text(opening.get("character_action"))
    dense_prop_tokens = ("书", "货架", "陈列", "收据", "招牌", "文字牌")
    if dense_opening and any(token in opening_visual for token in dense_prop_tokens):
        opening_visual = (
            "保持冻结人物、商品与穿搭的开场状态；目标商品完整可辨，"
            "地点仅以侧边或远处的一个局部锚点识别"
        )
    if dense_opening and any(token in opening_action for token in dense_prop_tokens):
        opening_action = "处于冻结动作的自然开始状态；场景道具动作留给后续片段"

    category_extension = ""
    canonical_type = _text(
        truth.get("canonical_product_type")
        or persona.get("product_type")
        or outfit.get("product_type")
    ).lower()
    if canonical_type in {"outerwear", "top", "dress"}:
        category_extension = "服装：目标服装必须完整可辨，衣长、领型、门襟、袖口与参考图一致，内搭和下装不得遮挡关键结构。"
    elif canonical_type in {"silk_scarf", "scarf", "winter_scarf"}:
        category_extension = "丝巾/围巾：保持图案、边框、垂端和已冻结佩戴方式，只呈现一个连续佩戴状态。"
    elif canonical_type == "headscarf":
        category_extension = "头巾：保持头部佩戴完成状态、图案方向与露发关系，不得在首帧重复系结。"
    elif canonical_type == "earring":
        category_extension = "耳饰：使用半脸耳侧近景，耳饰是清楚主体，数量和左右佩戴关系全片一致。"
    elif canonical_type in {"hair_accessory", "hairclip"}:
        category_extension = "发饰：使用头肩或侧后方三分之四近景，发饰与已完成发型清楚可见。"
    elif canonical_type in {"bracelet", "bangle"}:
        category_extension = "腕饰：使用手腕前臂近景，展示数量必须与冻结数量合同一致。"

    return "\n".join(
        [
            f"生成一张竖屏 {_text(contract.get('aspect_ratio')) or DEFAULT_ASPECT_RATIO} 短视频统一首帧，作为后续视频生成的视觉参考。",
            "",
            "【参考图角色与权威顺序】",
            "1. 前面的商品参考图只决定目标商品的颜色、图案、材质观感、形状、数量和结构；忽略商品图中的模特、脸、妆发、姿态、滤镜与背景。",
            "2. 后面的人物参考图只决定同一人物的脸部身份、肤色、自然皮肤纹理、身材比例和妆发；不得把人物图中的衣服、商品或背景带入结果。",
            "3. 下方冻结穿搭决定人物穿什么；冻结场景决定在哪里；冻结开场决定首帧状态、动作瞬间和景别。",
            "4. 商品一致性优先于人物美感、场景氛围和构图效果。",
            "",
            "【商品身份锁】",
            _lines("必须保持", must_preserve),
            _lines("负向结构约束", must_not),
            f"展示数量合同：{json.dumps(quantity, ensure_ascii=False, sort_keys=True) if quantity else '只按参考图实际数量，不主动复制商品'}",
            "",
            "【人物身份锁】",
            f"人物模板：{_text(persona.get('persona_name')) or _text(persona.get('persona_id')) or '不适用'}",
            f"人物身份与外貌：{_text(persona_projection.get('identity'))}；{_text(persona_projection.get('appearance'))}",
            f"身材比例：{body_proportion or '服从人物模板参考，不从商品参考图复制或重新设计'}",
            f"妆发：{_text(persona_projection.get('hair_makeup'))}",
            f"人物补充核心：{_text(persona.get('prompt_core'))}",
            "",
            "【冻结穿搭】",
            f"商品角色：{target_role or '按冻结脚本中的商品角色'}",
            f"完整穿搭：{frozen_outfit_text or json.dumps(recipe, ensure_ascii=False)}",
            f"穿搭结构：{_text(outfit.get('outfit_structure')) or '按冻结配方'}；连体单品不得拆成上装和下装。",
            f"发型/领口/外层：{_text(outfit_projection.get('hair_neckline_outer'))}",
            f"配色/可见性/完成效果：{_text(outfit_projection.get('palette_visibility_finish'))}",
            "",
            "【首帧场景投影｜后续视频仍使用完整冻结场景】",
            f"地点：{_text(opening_scene.get('location_identity')) or _text(scene.get('location'))}",
            f"时刻与光线：{_text(scene.get('moment'))}；{_text(scene.get('lighting'))}",
            f"背景锚点：{_text(opening_scene.get('opening_background_anchor')) or '只保留一个主要背景层次'}",
            f"主体正后方：{_text(opening_scene.get('subject_backdrop_guidance')) or _text(scene.get('background_depth'))}",
            f"密集元素位置：{_text(opening_scene.get('dense_elements_placement')) or '不额外增加文字和陈列'}",
            f"生活痕迹：{_text(opening_scene.get('lived_in_trace_guidance')) or '最多保留一处轻量生活痕迹'}",
            "首帧不需要复刻完整场景清单；地点可识别即可，商品和人物是绝对主体。",
            "",
            "【商品显著性与曝光】",
            f"曝光：{_text(exposure.get('guidance')) or '人物脸部和商品处于画面主要亮部，明亮清透但保留真实纹理。'}",
            f"商品与穿搭：{_text(separation.get('outfit_guidance')) or '商品与相邻穿搭保持清楚轮廓边界。'}",
            f"商品与背景：{_text(separation.get('background_guidance')) or '商品与背景保持清楚明暗或冷暖分离。'}",
            f"首镜焦点：{_text(opening_focus.get('guidance')) or '商品先成为第一视觉焦点。'}",
            "",
            "【冻结开场状态】",
            f"首帧画面：{opening_visual}",
            f"动作瞬间：{opening_action}",
            f"自然状态：{_text(opening.get('natural_emotion'))}",
            f"景别与手机：{_text(opening.get('camera'))}",
            f"首帧商品可见锚点：{'；'.join(_list(opening.get('product_anchors_visible')))}",
            "",
            "【画面风格】",
            "普通创作者自己用手机记录的原生短视频首帧；自然白平衡、清楚明亮但不过曝；保留真实皮肤纹理、轻微手机自动曝光和普通室内/自然光质感。",
            "画面首先像真实生活记录，其次才是好看。不要棚拍、广告大片、电影灯光、奢侈品精修、过度景深、磨皮塑料脸、HDR 过强、统一灰滤镜、夸张网红姿势。",
            category_extension,
            "",
            "【通用负向要求】",
            "不要文字、字幕、水印、Logo 杜撰；不要多余人物、助手手臂、三只手、多余肢体、畸形手指、扭曲脸部；不要改变商品款式或创造参考图不存在的结构。",
            f"人物模板负向：{prompt_negative}" if prompt_negative else "人物模板负向：避免塑料皮肤、畸形脸与夸张商业模特姿态。",
            "只输出一张可直接作为视频首帧参考的完整画面。",
        ]
    ).strip()
