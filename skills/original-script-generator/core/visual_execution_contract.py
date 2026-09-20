"""Deterministic visual-finish contract for simplified original scripts.

The contract keeps five independent concerns from collapsing into one prompt:
product identity, native capture texture, styling completion, scene/event
context, and visual saliency.  It is advisory apart from the pre-existing product identity lock.
No model call, retry, or hard validation is introduced here.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Mapping


VISUAL_EXECUTION_CONTRACT_VERSION = "visual-execution-contract-v4-wearable-saliency"
VISUAL_FINISH_PROFILE = "NATIVE_STYLED"
_SCARF_GREY_TYPES = {"scarf", "winter_scarf", "silk_scarf", "headscarf"}
_APPAREL_TYPES = {"outerwear", "top", "dress"}
_SUPPORTED_TYPES = _SCARF_GREY_TYPES | _APPAREL_TYPES
_DARK_COLOR_TOKENS = (
    "黑", "深蓝", "藏青", "海军蓝", "深棕", "咖啡色", "炭灰", "深灰", "墨绿", "酒红",
)
_LIGHT_COLOR_TOKENS = (
    "白", "米白", "象牙白", "奶白", "浅蓝", "浅绿", "薄荷绿", "浅粉", "粉色", "奶油色", "浅灰",
)
_COLOR_FAMILIES = {
    "BLACK": ("黑",),
    "WHITE": ("白", "米白", "象牙白", "奶白", "奶油色"),
    "BLUE": ("蓝", "藏青", "海军蓝"),
    "GREEN": ("绿", "抹茶", "薄荷"),
    "RED": ("红", "酒红"),
    "ORANGE": ("橙",),
    "YELLOW": ("黄",),
    "PINK": ("粉",),
    "PURPLE": ("紫",),
    "BROWN": ("棕", "咖啡"),
    "GREY": ("灰", "炭灰"),
}
_PATTERN_TOKENS = ("条纹", "波点", "星星", "印花", "图案", "格纹", "花纹", "撞色")
_HIGH_DENSITY_SCENE_TOKENS = (
    "书店", "书架", "货架", "陈列墙", "陈列台", "展览", "密集招牌", "大量文字", "商品陈列",
)
_MEDIUM_DENSITY_SCENE_TOKENS = (
    "咖啡", "餐厅", "酒店", "商场", "办公室", "会议室", "连廊", "车站",
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def visual_execution_v2_enabled() -> bool:
    value = _text(os.environ.get("ORIGINAL_SCRIPT_VISUAL_EXECUTION_V2_ENABLED", "1"))
    return value.lower() in {"1", "true", "yes", "on"}


def visual_saliency_v1_enabled() -> bool:
    value = _text(os.environ.get("ORIGINAL_SCRIPT_VISUAL_SALIENCY_V1_ENABLED", "1"))
    return value.lower() in {"1", "true", "yes", "on"}


def _dedupe(values: Any, *, limit: int = 4) -> list[str]:
    result: list[str] = []
    for value in values or []:
        text = _text(value)
        if text and text not in result:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def _palette_evidence(product_truth: Mapping[str, Any] | None) -> list[str]:
    truth = dict(product_truth or {})
    candidates = _dedupe(
        [
            *(truth.get("identity_anchors") or []),
            *(truth.get("visible_detail_anchors") or []),
        ],
        limit=10,
    )
    tokens = tuple(
        dict.fromkeys(
            (*_DARK_COLOR_TOKENS, *_LIGHT_COLOR_TOKENS, *_PATTERN_TOKENS,
             *(token for family in _COLOR_FAMILIES.values() for token in family))
        )
    )
    return [value for value in candidates if any(token in value for token in tokens)][:4]


def _palette_class(evidence: list[str]) -> str:
    text = "；".join(evidence)
    families = {
        family
        for family, tokens in _COLOR_FAMILIES.items()
        if any(token in text for token in tokens)
    }
    if any(token in text for token in _PATTERN_TOKENS) or len(families) >= 2:
        return "PATTERNED_OR_MULTICOLOR"
    if "BROWN" in families:
        return "BROWN_WARM"
    has_dark = any(token in text for token in _DARK_COLOR_TOKENS)
    has_light = any(token in text for token in _LIGHT_COLOR_TOKENS)
    if has_dark and not has_light:
        return "DARK"
    if has_light and not has_dark:
        return "LIGHT"
    return "UNKNOWN"


def _separation_contract(product_truth: Mapping[str, Any] | None) -> Dict[str, Any]:
    evidence = _palette_evidence(product_truth)
    palette_class = _palette_class(evidence)
    if palette_class == "DARK":
        return {
            "product_palette_evidence": evidence,
            "palette_class": palette_class,
            "contrast_axis": "LIGHTNESS_PRIMARY",
            "outfit_guidance": "商品相邻的内搭优先使用清楚的浅中性色或明显更浅的纯色，保留商品轮廓边界。",
            "background_guidance": "商品附近的背景与商品保持明暗分离；沿用已选场景，不把主体放进同样偏暗的表面。",
        }
    if palette_class == "LIGHT":
        return {
            "product_palette_evidence": evidence,
            "palette_class": palette_class,
            "contrast_axis": "LIGHTNESS_PRIMARY",
            "outfit_guidance": "商品相邻的内搭使用中等或较深的纯色建立边界，避免商品、内搭和背景同时接近浅色。",
            "background_guidance": "沿用已选场景，在可用表面中选择与商品有清楚明暗差的位置。",
        }
    if palette_class == "BROWN_WARM":
        return {
            "product_palette_evidence": evidence,
            "palette_class": palette_class,
            "contrast_axis": "LIGHTNESS_AND_TEMPERATURE",
            "outfit_guidance": "商品相邻内搭优先选择清楚的浅中性色或偏冷纯色，避免同色棕、驼、暖木色把商品轮廓吃掉。",
            "background_guidance": "沿用已选场景，但主体背后优先选择明度更高或偏中性的安静表面；暖木、棕色书架和密集陈列只放侧边或远处。",
        }
    if palette_class == "PATTERNED_OR_MULTICOLOR":
        return {
            "product_palette_evidence": evidence,
            "palette_class": palette_class,
            "contrast_axis": "CLARITY_AND_LIGHTNESS",
            "outfit_guidance": "相邻穿搭保持纯色和低图案密度，并与商品形成清楚明暗边界，让商品成为主要色彩图案焦点。",
            "background_guidance": "背景保留真实材质但减少近似图案干扰，并与商品至少保持一项明暗或冷暖分离。",
        }
    return {
        "product_palette_evidence": [],
        "palette_class": "UNKNOWN",
        "contrast_axis": "EDGE_SEPARATION",
        "outfit_guidance": "不猜商品颜色；让商品与相邻穿搭至少具有清楚的轮廓和明暗边界。",
        "background_guidance": "不改写已选场景，只保证人物或商品与背景不是同一片模糊明度。",
    }


def _opening_focus_contract(
    *,
    canonical_type: str,
    presentation_mode: str,
    outfit_contract: Mapping[str, Any],
    opening_visual_job: Mapping[str, Any] | None,
    action_design: Mapping[str, Any] | None,
) -> Dict[str, str]:
    presentation = _text(presentation_mode).upper()
    demonstration = _text(outfit_contract.get("demonstration_mode")).upper()
    opening = dict(opening_visual_job or {})
    action = dict(action_design or {})
    action_mode = _text(action.get("primary_action_mode")).upper()
    if presentation == "HANDS_ONLY":
        return {
            "source_job": _text(opening.get("job")) or "PRODUCT_FIRST",
            "focus_subject": "PRODUCT_AND_HANDS",
            "framing": "PRODUCT_DOMINANT_CLOSE_OR_MID",
            "natural_change": "服从本条已经冻结的手部商品动作，不额外增加助手、第二组手或新的商品互动。",
            "guidance": "首镜先让商品与同一人物的双手关系清楚，再保留少量真实环境。",
        }
    if presentation == "STATIC_PRODUCT":
        return {
            "source_job": _text(opening.get("job")) or "PRODUCT_FIRST",
            "focus_subject": "PRODUCT_FIRST",
            "framing": "PRODUCT_DOMINANT_CLOSE_OR_MID",
            "natural_change": "手机只完成一次轻微构图靠近或角度稳定，随后保持真实观察；商品不额外移动。",
            "guidance": "首镜先让商品成为画面最明确主体，再保留少量真实环境。",
        }
    if action_mode == "SIMPLE_WEAR_PROCESS":
        natural_change = "服从本条已经冻结的简单佩戴动作，首镜不再叠加额外状态变化。"
    elif canonical_type == "headscarf" or demonstration == "HEAD_WORN":
        natural_change = "从已经完成的佩戴结果开始，人物可轻微转动头肩或进入主要亮部，不重新包裹或系结。"
    else:
        natural_change = "从已经完成的佩戴结果开始，人物可轻微转动上半身或进入主要亮部，不重新系结。"
    if canonical_type == "headscarf" or demonstration == "HEAD_WORN":
        return {
            "source_job": _text(opening.get("job")) or "SHOW_RESULT",
            "focus_subject": "HEAD_WORN_RESULT",
            "framing": "HEAD_SHOULDER_TO_UPPER_BODY",
            "natural_change": natural_change,
            "guidance": "首镜先看清头巾、脸部和肩部关系，让头巾成为第一视觉焦点，再交代完整穿搭。",
        }
    if canonical_type == "silk_scarf" or demonstration in {"NECK_WORN", "HAIR_TIE"}:
        return {
            "source_job": _text(opening.get("job")) or "SHOW_RESULT",
            "focus_subject": "SCARF_WORN_RESULT",
            "framing": "FACE_NECK_UPPER_BODY",
            "natural_change": natural_change,
            "guidance": "首镜先让脸部、丝巾、领口和肩部关系清楚，让丝巾成为第一色彩焦点，再扩展到半身穿搭。",
        }
    if canonical_type in _APPAREL_TYPES:
        detail_opening = any(
            token in (_text(opening.get("job")) + _text(opening.get("guidance_zh"))).upper()
            for token in ("DETAIL", "细节", "领", "扣", "拉链", "面料")
        )
        return {
            "source_job": _text(opening.get("job")) or "SHOW_WORN_RESULT",
            "focus_subject": "TARGET_GARMENT_WORN_RESULT",
            "framing": "UPPER_BODY_DETAIL" if detail_opening else "MID_THIGH_OR_FULL_BODY",
            "natural_change": "从已经穿好的结果开始，只保留一次自然站姿、轻微转身或向前一步，不增加摆拍动作清单。",
            "guidance": "首镜先让目标服装完整可辨并占据主体区域，脸部和穿搭只服务真实分享；场景先退后，不能抢商品。",
        }
    return {
        "source_job": _text(opening.get("job")) or "SHOW_RESULT",
        "focus_subject": "NECK_SHOULDER_RESULT",
        "framing": "NECK_SHOULDER_UPPER_BODY",
        "natural_change": natural_change,
        "guidance": "首镜优先看清围巾与颈肩、上半身的关系，再交代完整穿搭。",
    }


def build_subject_visibility_guidance(
    *,
    presentation_mode: str,
    scene_card: Mapping[str, Any],
    product_truth: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """Shared exposure/separation only; no action or framing decisions."""
    if not visual_saliency_v1_enabled():
        return {}
    lighting = _text(scene_card.get("lighting"))
    exposure_guidance = (
        "人物脸部与商品处于画面主要亮部，画面明亮清透、白平衡自然，"
        "不欠曝、不蒙灰，同时保留真实皮肤、发丝和衣物纹理。"
    )
    if "窗" in lighting or "自然光" in lighting:
        exposure_guidance += "人物或商品面向、侧向现场主要自然光，不让明亮窗口在主体身后形成灰脸或剪影。"
    return {
        "exposure": {
            "profile": "BRIGHT_NATIVE",
            "subject_priority": {
                "STATIC_PRODUCT": "PRODUCT",
                "HANDS_ONLY": "PRODUCT_AND_HANDS",
            }.get(_text(presentation_mode).upper(), "FACE_AND_PRODUCT"),
            "guidance": exposure_guidance,
        },
        "separation": _separation_contract(product_truth),
    }


def _visual_saliency_contract(
    *,
    canonical_type: str,
    presentation_mode: str,
    outfit_contract: Mapping[str, Any],
    scene_card: Mapping[str, Any],
    product_truth: Mapping[str, Any] | None,
    opening_visual_job: Mapping[str, Any] | None,
    action_design: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    visibility = build_subject_visibility_guidance(
        presentation_mode=presentation_mode, scene_card=scene_card,
        product_truth=product_truth,
    )
    if not visibility:
        return {}
    return {
        "authority": "SOFT_FINAL_COMPOSITION",
        **visibility,
        "opening_focus": _opening_focus_contract(
            canonical_type=canonical_type,
            presentation_mode=presentation_mode,
            outfit_contract=outfit_contract,
            opening_visual_job=opening_visual_job,
            action_design=action_design,
        ),
        "policy": {
            "mode": "SOFT_ONLY",
            "may_block_generation": False,
            "may_trigger_retry": False,
            "may_override_product_identity_or_wear_state": False,
        },
    }


def _scene_projection_source(scene_reference: Mapping[str, Any] | None) -> Dict[str, str]:
    scene = dict(scene_reference or {})
    card = dict(scene.get("execution_card") or {}) if isinstance(
        scene.get("execution_card"), Mapping
    ) else {}
    space = dict(card.get("space") or {}) if isinstance(card.get("space"), Mapping) else {}
    anchors = card.get("background_anchors") or scene.get("realism_anchors") or []
    if isinstance(anchors, str):
        anchors = [anchors]
    return {
        "location": _text(scene.get("location") or space.get("location") or scene.get("prototype_name")),
        "background": _text(scene.get("background") or "；".join(_dedupe(anchors, limit=2))),
        "background_depth": _text(scene.get("background_depth") or space.get("background_depth")),
        "lived_in_trace": _text(scene.get("lived_in_trace") or card.get("lived_in_trace")),
        "lighting": _text(scene.get("lighting") or card.get("lighting")),
    }


def build_opening_scene_projection(
    scene_reference: Mapping[str, Any] | None,
    *,
    presentation_mode: str,
) -> Dict[str, Any]:
    """Project a full production scene into a quieter first-frame backdrop.

    This is deliberately not a scene selector.  It preserves the chosen place,
    removes dense competing elements only from the opening composition, and
    never blocks generation or changes later clips.
    """

    source = _scene_projection_source(scene_reference)
    evidence = "；".join(source.values())
    if any(token in evidence for token in _HIGH_DENSITY_SCENE_TOKENS):
        density = "HIGH"
    elif any(token in evidence for token in _MEDIUM_DENSITY_SCENE_TOKENS):
        density = "MEDIUM"
    else:
        density = "LOW"
    if density == "HIGH":
        anchor_rule = "只保留一个能说明地点的局部环境锚点"
        backdrop = "在同一场景内选择较安静、低纹理、少文字的墙面、玻璃或通道区域作为主体正后方"
        dense_rule = "书架、货架、文字和陈列只放画面侧边或远处，不铺满人物或商品背后"
        lived_in = "首帧不额外强调生活道具；生活痕迹留给后续片段"
    elif density == "MEDIUM":
        anchor_rule = "最多保留一个地点锚点，不与商品争夺注意力"
        backdrop = "在已选地点内使用轮廓清楚、纹理适中的局部背景承托主体"
        dense_rule = "桌椅、招牌和人流只作侧边或远景信息，不穿过商品轮廓"
        lived_in = "首帧最多保留一处轻量生活痕迹"
    else:
        anchor_rule = "保留原地点识别，但首帧只用一个主要背景层次"
        backdrop = "主体正后方保持清楚、低干扰并与商品有明暗或冷暖分离"
        dense_rule = "不额外添加新的装饰、文字或陈列"
        lived_in = "可保留一处已有生活痕迹，但不得成为第二主体"
    return {
        "schema_version": "opening-scene-projection-v1",
        "authority": "FIRST_FRAME_AND_FIRST_CAPTURE_UNIT_ONLY",
        "location_identity": source["location"],
        "source_density": density,
        "opening_background_anchor": anchor_rule,
        "subject_backdrop_guidance": backdrop,
        "dense_elements_placement": dense_rule,
        "lived_in_trace_guidance": lived_in,
        "lighting": source["lighting"],
        "presentation_mode": _text(presentation_mode).upper(),
        "does_not_change_full_scene": True,
        "may_block_generation": False,
    }


def finalize_visual_execution_contract(
    contract: Mapping[str, Any] | None,
    *,
    production_scene: Mapping[str, Any] | None,
    presentation_mode: str,
) -> Dict[str, Any]:
    result = dict(contract or {})
    if not result:
        return {}
    result["opening_scene_projection"] = build_opening_scene_projection(
        production_scene,
        presentation_mode=presentation_mode,
    )
    return result


ACCESSORY_MIXED_VISUAL_CONTRACT_VERSION = "accessory-mixed-visual-contract-v1"

#: The one profile that owns a *frozen* per-shot contract of its own.
#:
#: Every other accessory family describes its lighting and framing from the
#: shared `accessory_mixed_templates.json`, which is the live configuration.  A
#: necklace task freezes its own `environment_recipe` and its own resolved
#: per-shot `allowed_framing` / `forbidden_framing` into the mixed contract, so
#: reading the shared config for it asks a question about *another* document.
NECKLACE_V1_FEATURE_PROFILE = "NECKLACE_MIXED_V1"

#: Non-operative observations: worth telling a reader, never a reason to call a
#: contract incomplete.  Kept apart from ``gaps`` so "the label is missing" and
#: "the framing is missing" cannot be confused by whoever reads this later.
FROZEN_PROJECTION_STATUS_COMPLETE = "COMPLETE"
FROZEN_PROJECTION_STATUS_INCOMPLETE = "INCOMPLETE"


def _necklace_frozen_visual_contract(
    *,
    frozen: Mapping[str, Any],
    zone: str,
    presentation_mode: str,
    capture_mode: str,
    requested_recipe_id: str,
    worn_allowed: Any,
    worn_forbidden: Any,
    consistency: Any,
    authenticity: Any,
) -> Dict[str, Any]:
    """Lighting and framing read off the frozen necklace contract, gaps included.

    F2 (review 2026-09-20): this branch used to call ``get_environment_recipe``
    and ``load_mixed_template_definition`` -- both of which answer from the
    *current* configuration.  A necklace recipe id does not exist in the shared
    accessory recipe table (it lives in the necklace config), so the lookup
    raised, the projection fell back to ``{}``, and the model was handed
    ``recipe_id: NMX_WARM_NEUTRAL_WINDOW_V1`` with **no** environment, goal or
    label text at all -- while the blueprint two fields away quoted the frozen
    recipe verbatim.  One input, two different surfaces and light directions.
    The framing had the same shape of failure: ``category_rules`` has no ``NECK``
    zone, so ``allowed_framing`` / ``forbidden_framing`` came out empty even
    though the frozen contract carries a resolved framing list per shot.

    So this branch reads nothing but the frozen contract.  Where the frozen data
    is genuinely absent it says so -- ``frozen_projection.gaps`` names each
    missing item and the status flips to ``INCOMPLETE``.  It never backfills a
    historical task from today's config, because that would describe a film that
    was frozen against a different document.
    """

    recipe = frozen.get("environment_recipe")
    recipe = recipe if isinstance(recipe, Mapping) else {}
    block = frozen.get("necklace_contract")
    block = block if isinstance(block, Mapping) else {}

    frozen_recipe_id = _text(frozen.get("environment_recipe_id"))
    recipe_version = recipe.get("recipe_version")
    if recipe_version in (None, ""):
        recipe_version = frozen.get("environment_recipe_version")

    label = _text(recipe.get("label"))
    environment = _text(recipe.get("environment"))
    goal = _text(recipe.get("goal"))

    allowed = list(worn_allowed(frozen) or [])
    forbidden = list(worn_forbidden(frozen) or [])
    units = [unit for unit in (frozen.get("capture_units") or []) if isinstance(unit, Mapping)]
    worn_units = [unit for unit in units if _is_worn_unit(unit, zone)]

    gaps: list = []
    if not frozen_recipe_id:
        gaps.append(
            {
                "field": "environment_recipe_id",
                "reason": "冻结合同没有记录光影配方 id，无法确认这一版用的是哪套光影",
            }
        )
    if not environment or not goal:
        gaps.append(
            {
                "field": "environment_recipe",
                "reason": (
                    "冻结合同的 environment_recipe 缺少 environment/goal，"
                    "光影与商品原色约束无法还原；不从当前配置补写"
                ),
            }
        )
    if not units:
        gaps.append(
            {
                "field": "capture_units",
                "reason": "冻结合同没有逐镜执行单元，无法还原任何一镜的取景边界",
            }
        )
    elif not worn_units:
        gaps.append(
            {
                "field": "capture_units[body_zone=%s]" % zone,
                "reason": "冻结合同里没有佩戴镜，颈部取景边界无从汇总",
            }
        )
    elif not allowed or not forbidden:
        gaps.append(
            {
                "field": "capture_units[].allowed_framing/forbidden_framing",
                "reason": "佩戴镜的取景边界为空，等于没有约束",
            }
        )
    if not _text(block.get("profile_config_hash")):
        gaps.append(
            {
                "field": "necklace_contract.profile_config_hash",
                "reason": "冻结合同没有 profile_config_hash，无法确认与哪一版项链配置一致",
            }
        )

    notes: list = []
    if not _text(frozen.get("zone_label")):
        # Only a display label; the operative framing text names 颈部 itself.
        notes.append(
            "冻结合同未记录 zone_label（仅展示用标签），未从当前配置补写"
        )
    if _text(requested_recipe_id) and _text(requested_recipe_id) != frozen_recipe_id:
        notes.append(
            "调用方传入的 environment_recipe_id（%s）与冻结合同（%s）不一致，"
            "已采用冻结合同" % (_text(requested_recipe_id), frozen_recipe_id or "空")
        )

    return {
        "schema_version": ACCESSORY_MIXED_VISUAL_CONTRACT_VERSION,
        "feature_scope": "ACCESSORY_MIXED_TEMPLATE",
        "visual_finish_profile": VISUAL_FINISH_PROFILE,
        "presentation_mode": _text(presentation_mode),
        "capture_mode": _text(capture_mode),
        "authorities": {
            "product_integrity": "HARD_EXISTING_IDENTITY_LOCK",
            "lighting_recipe": "FROZEN_PER_VIDEO",
            # The per-shot boundaries are the authority here, not a zone rule
            # read from the live config: only the worn shots carry the neck
            # framing, and the hand-held / static shots keep their own.
            "framing_zone": "FROZEN_PER_SHOT",
            "capture_texture": "SOFT",
            "evidence": "REFERENCE_LIMITED",
        },
        "lighting_recipe": {
            "recipe_id": frozen_recipe_id,
            "recipe_version": recipe_version,
            "label": label,
            "environment": environment,
            "goal": goal,
            "keep_constant": list((consistency or {}).get("keep_constant") or []),
            "allowed_variation": list((consistency or {}).get("allowed_variation") or []),
            "forbidden": list((consistency or {}).get("forbidden") or []),
        },
        "framing_zone": {
            "zone": zone,
            "zone_label": _text(frozen.get("zone_label")),
            "allowed_framing": allowed,
            "forbidden_framing": forbidden,
            "instruction": (
                "同一条视频使用同一套环境配方：光向、白平衡、肤色与主要背景材质保持一致；"
                "允许正常角度变化引起的合理高光变化。"
            ),
            # Which shots the two arrays above describe.  Stated so nobody reads
            # the neck requirement as applying to the hand-held or static shots.
            "applies_to": "佩戴镜（module=WORN_DETAIL/WORN_RELATION）",
            "unit_framing": [
                {
                    "unit_id": _text(unit.get("unit_id")),
                    "module": _text(unit.get("module")),
                    "module_label": _text(unit.get("module_label")),
                    "carrier_mode": _text(unit.get("carrier_mode")),
                    "body_zone": _text(unit.get("body_zone")),
                    "face_policy": _text(unit.get("face_policy")),
                    "allowed_framing": list(unit.get("allowed_framing") or []),
                    "forbidden_framing": list(unit.get("forbidden_framing") or []),
                }
                for unit in units
            ],
        },
        "authenticity": {
            "reference_limited": _text((authenticity or {}).get("reference_limited")),
            "no_fabricated_material": _text(
                (authenticity or {}).get("no_fabricated_material")
            ),
            "relative_only": _text((authenticity or {}).get("relative_only")),
            "pairing_authority": _text((authenticity or {}).get("pairing_authority")),
        },
        "diagnostics_policy": {
            "mode": "SOFT_ONLY",
            "may_block_generation": False,
            "may_trigger_retry": False,
        },
        # What this projection was built from, and what it could not find.  A
        # reader must be able to tell "the frozen contract said nothing here"
        # from "the frozen contract said this".
        "frozen_projection": {
            "feature_profile": NECKLACE_V1_FEATURE_PROFILE,
            "template_id": _text(frozen.get("template_id")),
            "template_version": frozen.get("template_version"),
            "feature_version": frozen.get("feature_version"),
            "profile_config_hash": _text(block.get("profile_config_hash")),
            "environment_recipe_id": frozen_recipe_id,
            "environment_recipe_version": recipe_version,
            "worn_unit_ids": [_text(unit.get("unit_id")) for unit in worn_units],
            "status": (
                FROZEN_PROJECTION_STATUS_INCOMPLETE
                if gaps
                else FROZEN_PROJECTION_STATUS_COMPLETE
            ),
            "gaps": gaps,
            "notes": notes,
        },
    }


def _is_worn_unit(unit: Mapping[str, Any], zone: str) -> bool:
    """Whether one frozen capture unit puts the product on the wearer's body.

    The hand-held and static units carry an empty ``body_zone``, so the zone
    itself is the signal; ``carrier_mode`` is the fallback for a contract that
    omitted it.
    """

    body_zone = _text(unit.get("body_zone"))
    if body_zone:
        return body_zone == _text(zone)
    return _text(unit.get("carrier_mode")).startswith("WEARER")


def build_accessory_mixed_visual_contract(
    *,
    canonical_product_type: str,
    presentation_mode: str,
    capture_mode: str,
    environment_recipe_id: str = "",
    frozen_contract: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Accessory-specific visual contract for the authored mixed template mode.

    This is deliberately a separate branch rather than an extension of
    ``_SUPPORTED_TYPES``: widening the support set alone would let accessories
    inherit the scarf grey/default rules.  Returns ``{}`` whenever the mode owns
    nothing here and the product is outside the four supported accessory
    families, so the existing scarf / apparel contract stays byte-for-byte
    unchanged.

    Review #7: the environment recipe must come from the *frozen* mixed contract.
    The blueprint already quotes the frozen recipe, so a visual contract that
    quietly picked the default instead handed one model input two different
    surfaces and light directions.  ``frozen_contract`` also keeps a task frozen:
    once a contract exists, the ``..._MIXED_TEMPLATE_V1_ENABLED`` switch must not
    be able to flip a re-run back to the legacy contract.

    F2: for ``NECKLACE_MIXED_V1`` the *whole* projection comes off the frozen
    contract -- see :func:`_necklace_frozen_visual_contract`.  Everything else
    still reads the shared configuration, unchanged.
    """

    frozen = frozen_contract if isinstance(frozen_contract, Mapping) else {}
    try:
        from core.accessory_mixed_templates import (
            accessory_mixed_template_enabled,
            get_environment_recipe,
            load_mixed_template_definition,
            resolve_mixed_zone,
            worn_body_forbidden_framing,
            worn_body_framing,
        )
    except Exception:  # noqa: BLE001 - never break the legacy contract path
        return {}
    if not frozen and not accessory_mixed_template_enabled():
        return {}
    # The frozen contract names its own zone, so a frozen task does not depend on
    # type resolution being able to re-derive it.
    zone = _text(frozen.get("category_zone"))
    if not zone:
        resolved, _canonical = resolve_mixed_zone(canonical_product_type, "")
        zone = resolved or ""
    if not zone:
        return {}

    definition = load_mixed_template_definition()
    consistency = definition.get("consistency_rules") or {}
    authenticity = definition.get("authenticity_and_evidence_rules") or {}

    if _text(frozen.get("feature_profile")) == NECKLACE_V1_FEATURE_PROFILE:
        return _necklace_frozen_visual_contract(
            frozen=frozen,
            zone=zone,
            presentation_mode=presentation_mode,
            capture_mode=capture_mode,
            requested_recipe_id=environment_recipe_id,
            worn_allowed=worn_body_framing,
            worn_forbidden=worn_body_forbidden_framing,
            consistency=consistency,
            authenticity=authenticity,
        )

    rules = (definition.get("category_rules") or {}).get(zone)
    rules = rules if isinstance(rules, Mapping) else {}
    recipe_id = (
        _text(environment_recipe_id)
        or _text(frozen.get("environment_recipe_id"))
        or _text(definition.get("default_environment_recipe"))
    )
    try:
        recipe = get_environment_recipe(recipe_id)
    except ValueError:
        recipe = {}
    return {
        "schema_version": ACCESSORY_MIXED_VISUAL_CONTRACT_VERSION,
        "feature_scope": "ACCESSORY_MIXED_TEMPLATE",
        "visual_finish_profile": VISUAL_FINISH_PROFILE,
        "presentation_mode": _text(presentation_mode),
        "capture_mode": _text(capture_mode),
        "authorities": {
            "product_integrity": "HARD_EXISTING_IDENTITY_LOCK",
            "lighting_recipe": "FROZEN_PER_VIDEO",
            "framing_zone": "CATEGORY_EXTENSION",
            "capture_texture": "SOFT",
            "evidence": "REFERENCE_LIMITED",
        },
        "lighting_recipe": {
            "recipe_id": _text(recipe_id),
            "recipe_version": recipe.get("recipe_version"),
            "label": _text(recipe.get("label")),
            "environment": _text(recipe.get("environment")),
            "goal": _text(recipe.get("goal")),
            "keep_constant": list(consistency.get("keep_constant") or []),
            "allowed_variation": list(consistency.get("allowed_variation") or []),
            "forbidden": list(consistency.get("forbidden") or []),
        },
        "framing_zone": {
            "zone": zone,
            "zone_label": _text(rules.get("zone_label")),
            "allowed_framing": list(rules.get("allowed_framing") or []),
            "forbidden_framing": list(rules.get("forbidden_framing") or []),
            "instruction": (
                "同一条视频使用同一套环境配方：光向、白平衡、肤色与主要背景材质保持一致；"
                "允许正常角度变化引起的合理高光变化。"
            ),
        },
        "authenticity": {
            "reference_limited": _text(authenticity.get("reference_limited")),
            "no_fabricated_material": _text(authenticity.get("no_fabricated_material")),
            "relative_only": _text(authenticity.get("relative_only")),
            "pairing_authority": _text(authenticity.get("pairing_authority")),
        },
        "diagnostics_policy": {
            "mode": "SOFT_ONLY",
            "may_block_generation": False,
            "may_trigger_retry": False,
        },
    }


def build_visual_execution_contract(
    *,
    canonical_product_type: str,
    presentation_mode: str,
    capture_mode: str,
    outfit_contract: Mapping[str, Any] | None,
    scene_reference: Mapping[str, Any] | None,
    product_truth: Mapping[str, Any] | None = None,
    opening_visual_job: Mapping[str, Any] | None = None,
    action_design: Mapping[str, Any] | None = None,
    suggested_opening_action: str = "",
    suggested_event_flow: str = "",
    accessory_environment_recipe_id: str = "",
    accessory_frozen_contract: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return one thin wearable visual contract, or empty for unsupported flows."""

    canonical_type = _text(canonical_product_type).lower()
    accessory_contract = build_accessory_mixed_visual_contract(
        canonical_product_type=canonical_product_type,
        presentation_mode=presentation_mode,
        capture_mode=capture_mode,
        environment_recipe_id=accessory_environment_recipe_id,
        frozen_contract=accessory_frozen_contract,
    )
    if accessory_contract:
        return accessory_contract
    if not visual_execution_v2_enabled() or canonical_type not in _SUPPORTED_TYPES:
        return {}

    outfit = dict(outfit_contract or {})
    scene = dict(scene_reference or {})
    card = (
        dict(scene.get("execution_card") or {})
        if isinstance(scene.get("execution_card"), Mapping)
        else {}
    )
    scene_recipe = (
        dict(card.get("visual_scene_recipe") or {})
        if isinstance(card.get("visual_scene_recipe"), Mapping)
        else {}
    )
    contract = {
        "schema_version": VISUAL_EXECUTION_CONTRACT_VERSION,
        "feature_scope": (
            "SCARF_ACCESSORY_GREY"
            if canonical_type in _SCARF_GREY_TYPES
            else "WEARABLE_VISUAL_SALIENCY"
        ),
        "visual_finish_profile": VISUAL_FINISH_PROFILE,
        "presentation_mode": _text(presentation_mode),
        "capture_mode": _text(capture_mode),
        "authorities": {
            "product_integrity": "HARD_EXISTING_IDENTITY_LOCK",
            "capture_texture": "SOFT",
            "styling_context": "SOFT",
            "scene_context": "SOFT",
            "event_progression": "SOFT",
            "visual_saliency": "SOFT_FINAL_COMPOSITION",
        },
        "capture_texture": {
            "direction": (
                "像有审美的真实创作者用自己的手机随手分享；保留自然皮肤、衣物纹理、"
                "现场光线和轻微构图不完美，不转成广告大片。"
            ),
            "native_does_not_mean_plain": True,
        },
        "styling_context": {
            "silhouette_key": _text(outfit.get("silhouette_key")),
            "style_family": _text(outfit.get("style_family")),
            "style_intensity": _text(outfit.get("style_intensity")),
            "target_role": _text(outfit.get("target_role")),
            "climate_profile": _text(outfit.get("climate_profile")),
            "demonstration_mode": _text(outfit.get("demonstration_mode")),
            "outfit_recipe": dict(outfit.get("outfit_recipe") or {}),
            "visibility_zones": list(outfit.get("visibility_zones") or []),
            "base_outfit_direction": (
                "" if any(_text(value) for value in (outfit.get("outfit_recipe") or {}).values())
                else _text(outfit.get("base_outfit_direction"))
            ),
            "hair_direction": _text(outfit.get("hair_direction")),
            "palette_relation": _text(outfit.get("palette_relation")),
            "finish_direction": _text(outfit.get("finish_direction")),
            "supporting_elements": _text(outfit.get("supporting_elements")),
            "grooming_direction": _text(outfit.get("grooming_direction")),
            "instruction": (
                "低冲突不等于朴素或没搭配：人物应像真实账号中已经完成当天造型的人；"
                "允许一至两个不遮挡商品的辅助元素，但不要求机械凑齐。"
            ),
        },
        "scene_context": {
            "scene_family_key": _text(scene.get("scene_family_key")),
            "prototype_name": _text(scene.get("prototype_name")),
            "source_quality": _text(card.get("source_quality")),
            "situation_tags": list(card.get("situation_tags") or [])[:2],
            "aesthetic_anchors": list(card.get("aesthetic_anchors") or [])[:2],
            "visual_scene_recipe": {
                key: _text(scene_recipe.get(key))
                for key in (
                    "space_relationship",
                    "material_palette",
                    "lighting_texture",
                    "lived_in_detail",
                )
            },
            "realism_anchor": _text(card.get("lived_in_trace")),
            "lighting": _text(card.get("lighting")),
            "coherence_key": _text(card.get("coherence_key")),
            "instruction": (
                "场景从同一场景来源建立真实与审美完成度；保留生活痕迹，也允许空间本身"
                "有明确材质、色调或氛围，不另造影棚布景。"
            ),
        },
        "event_progression": {
            "suggested_opening_action": _text(suggested_opening_action),
            "suggested_event_flow": _text(suggested_event_flow),
            "instruction": (
                "全片保持一个连续生活时刻，最多一个自然状态变化；它用于避免全程定住，"
                "不是动作清单，也不要求每镜都变化。"
            ),
            "hard_required": False,
        },
        "diagnostics_policy": {
            "mode": "SOFT_ONLY",
            "may_block_generation": False,
            "may_trigger_retry": False,
        },
    }
    saliency = _visual_saliency_contract(
        canonical_type=canonical_type,
        presentation_mode=presentation_mode,
        outfit_contract=outfit,
        scene_card=card,
        product_truth=product_truth,
        opening_visual_job=opening_visual_job,
        action_design=action_design,
    )
    if saliency:
        contract["visual_saliency"] = saliency
    contract["opening_scene_projection"] = build_opening_scene_projection(
        scene,
        presentation_mode=presentation_mode,
    )
    return contract


def build_visual_execution_diagnostics(
    *, contract: Mapping[str, Any] | None, production_design: Mapping[str, Any] | None
) -> Dict[str, Any]:
    """Structural observability only; never judges taste or blocks output."""

    visual = dict(contract or {})
    if not visual:
        return {}
    production = dict(production_design or {})
    life_event = (
        dict(production.get("life_event") or {})
        if isinstance(production.get("life_event"), Mapping)
        else {}
    )
    styling = visual.get("styling_context") or {}
    scene = visual.get("scene_context") or {}
    saliency = visual.get("visual_saliency") or {}
    warnings = []
    if not _text(life_event.get("continuous_event")):
        warnings.append("EVENT_PROGRESSION_NOT_EXPLICIT")
    if not _text(styling.get("base_outfit_direction")):
        warnings.append("STYLING_CONTEXT_UNAVAILABLE")
    recipe = (
        scene.get("visual_scene_recipe")
        if isinstance(scene.get("visual_scene_recipe"), Mapping)
        else {}
    )
    if not (
        _text(scene.get("realism_anchor"))
        or scene.get("aesthetic_anchors")
        or any(_text(recipe.get(key)) for key in recipe)
    ):
        warnings.append("SCENE_CONTEXT_UNAVAILABLE")
    separation = (
        saliency.get("separation")
        if isinstance(saliency, Mapping) and isinstance(saliency.get("separation"), Mapping)
        else {}
    )
    if saliency and not separation.get("product_palette_evidence"):
        warnings.append("PRODUCT_PALETTE_EVIDENCE_UNAVAILABLE")
    return {
        "schema_version": "visual-execution-diagnostics-v1",
        "mode": "SOFT_ONLY",
        "warnings": warnings,
        "event_progression_status": (
            "EXPLICIT" if _text(life_event.get("continuous_event")) else "FALLBACK_OR_ABSENT"
        ),
        "styling_context_status": "AVAILABLE" if styling else "UNAVAILABLE",
        "scene_context_status": "AVAILABLE" if scene else "UNAVAILABLE",
        "visual_saliency_status": "AVAILABLE" if saliency else "UNAVAILABLE",
    }
