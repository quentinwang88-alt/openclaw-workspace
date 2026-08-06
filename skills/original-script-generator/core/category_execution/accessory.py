"""Accessory execution adapter for hair clips, earrings and scarves.

The adapter deliberately reuses the existing type registry and AI shot-risk
registry.  It does not select claims, structures, scenes, hooks or dialogue.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List

from core.product_type_resolution import normalize_product_type

from .base import CategoryExecutionAdapter


_RISK_REGISTRY_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "ai_shot_risk_registry.json"
)

_HAIR_TYPES = {
    "hair_accessory_generic",
    "claw_clip",
    "hair_clip",
    "headband",
    "scrunchie",
    "hair_tie",
    "ribbon",
    "hair_pin",
}
_SCARF_TYPES = {"scarf", "winter_scarf", "silk_scarf", "headscarf"}
_SUPPORTED_TYPES = {"earring", *_SCARF_TYPES, *_HAIR_TYPES}

_PAIR_TERMS = ("一对", "成对", "一副", "双耳", "两只")
_SINGLE_TERMS = ("单只", "单个", "单耳", "单边")
_PAIR_OUTPUT_TERMS = ("一对", "成对", "一副", "双耳", "两只", "这对")
_SINGLE_OUTPUT_TERMS = ("单只", "单个", "单耳", "单边", "这一只")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _dedupe(values: Iterable[Any], limit: int = 8) -> List[str]:
    result: List[str] = []
    for value in values:
        text = _text(value)
        if text and text not in result:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def _anchor_texts(anchor_card: Dict[str, Any]) -> List[str]:
    """Read only hard identity anchors as relationship authority.

    ``display_anchors`` may contain a model-authored presentation suggestion
    such as "手持展示成对耳饰".  That is not proof of the sold unit, so it must
    never authorize PAIR/SINGLE semantics.
    """

    values: List[str] = []
    for item in anchor_card.get("hard_anchors") or []:
        if isinstance(item, dict):
            values.append(
                item.get("anchor")
                or item.get("anchor_text")
                or item.get("name")
                or item.get("value")
            )
        else:
            values.append(item)
    return _dedupe(values, limit=30)


def _earring_pairing_authority(anchor_card: Dict[str, Any]) -> Dict[str, Any]:
    anchors = _anchor_texts(anchor_card)
    joined = "；".join(anchors)
    has_pair = any(term in joined for term in _PAIR_TERMS)
    has_single = any(term in joined for term in _SINGLE_TERMS)
    if has_pair and not has_single:
        mode = "PAIR"
    elif has_single and not has_pair:
        mode = "SINGLE"
    else:
        mode = "UNAVAILABLE"
    return {
        "pairing_mode": mode,
        "authority_source": "APPROVED_ANCHORS" if mode != "UNAVAILABLE" else "UNAVAILABLE",
        "must_not_assume": (
            [] if mode != "UNAVAILABLE" else ["耳饰为单只还是成对"]
        ),
    }


@lru_cache(maxsize=1)
def _risk_registry() -> Dict[str, Any]:
    try:
        data = json.loads(_RISK_REGISTRY_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _profile_definition(canonical_type: str) -> Dict[str, Any]:
    if canonical_type == "earring":
        return {
            "display_family": "EAR_ACCESSORY",
            "wearing_zone": "EAR",
            "required_result_view": "ALREADY_WORN_EAR_VISIBLE",
            "supporting_style_context": "FACE_AND_OUTFIT_RELATION",
            "risk_registry_key": "ear_accessory",
            "identity_priority": [
                "pair_or_single",
                "attachment_type",
                "component_order",
                "relative_length",
                "material_color",
            ],
        }
    if canonical_type in _HAIR_TYPES:
        return {
            "display_family": "HAIR_ACCESSORY",
            "wearing_zone": "HAIR",
            "required_result_view": "ALREADY_STYLED_HAIR_RESULT",
            "supporting_style_context": "DAILY_HAIRSTYLE",
            "risk_registry_key": "hair_accessory",
            "identity_priority": [
                "accessory_subtype",
                "overall_shape",
                "ornament_layout",
                "relative_size",
                "placement_zone",
            ],
        }
    scarf_profiles = {
        "winter_scarf": {
            "display_family": "WORN_ACCESSORY",
            "wearing_zone": "NECK_SHOULDER",
            "required_result_view": "ALREADY_WORN_UPPER_BODY_RESULT",
            "supporting_style_context": "OUTERWEAR_OR_KNIT_LAYERING",
            "process_policy": "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "base_top_direction": "简洁针织上衣、基础高领或日常内搭",
                "outer_layer_direction": "简洁外套或针织层；不根据类型名补充保暖功效",
                "neckline_direction": "肩颈区域清楚，不被衣领、头发或包带完全遮住",
                "hair_direction": "自然披发、耳后别发或简单束发，避免遮住围巾主体",
                "palette_relation": "与目标商品自然协调，不要求同色，也不只靠换色制造差异",
                "visibility_requirement": "至少一段清楚看到围巾与脖颈、肩部及上半身穿搭关系",
            },
            "scene_preferences": [
                "HOME_ROUTINE", "OFFICE_WORKBREAK", "STREET_OUTING", "CAR_TRANSIT",
            ],
            "risk_registry_key": "winter_scarf",
            "identity_priority": [
                "main_color", "pattern", "fringe", "overall_shape", "drape_relation"
            ],
        },
        "silk_scarf": {
            "display_family": "LIGHT_NECK_ACCESSORY",
            "wearing_zone": "NECK_UPPER_BODY",
            "required_result_view": "ALREADY_STYLED_NECK_RESULT",
            "supporting_style_context": "LIGHT_OUTFIT_ACCENT",
            "process_policy": "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "base_top_direction": "纯色基础上衣、简洁衬衫或轻针织上装",
                "outer_layer_direction": "不强制外套；基础穿搭避免复杂图案抢过丝巾",
                "neckline_direction": "领口保持清楚，丝巾与颈部、领口关系完整可见",
                "hair_direction": "耳后别发、自然短发或简单束发，避免大面积遮住丝巾",
                "palette_relation": "基础服装与商品主色或图案自然协调，不猜测真实材质",
                "visibility_requirement": "至少一段清楚看到丝巾与领口、脸部附近及上半身关系",
            },
            "scene_preferences": [
                "CAFE_DINING", "OFFICE_WORKBREAK", "HOME_ROUTINE", "STREET_OUTING",
            ],
            "risk_registry_key": "silk_scarf",
            "identity_priority": [
                "main_color", "pattern", "overall_shape", "neckline_relation"
            ],
        },
        "headscarf": {
            "display_family": "HEAD_SCARF_ACCESSORY",
            "wearing_zone": "HEAD_HAIR",
            "required_result_view": "ALREADY_STYLED_HEAD_RESULT",
            "supporting_style_context": "DAILY_HEAD_STYLE",
            "process_policy": "RESULT_FIRST_NO_WRAPPING_TUTORIAL",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "base_top_direction": "简洁日常上装，避免复杂图案与头巾竞争",
                "outer_layer_direction": "按真实日常场景轻搭配，不推断宗教或文化用途",
                "neckline_direction": "上半身轮廓清楚，服务头巾与整体穿搭关系",
                "hair_direction": "明确已经完成的日常发型或覆盖状态，不拍完整包裹过程",
                "palette_relation": "与目标商品自然协调，不根据场景补充身份含义",
                "visibility_requirement": "至少一段清楚看到头巾位置、轮廓、头发状态与穿搭关系",
            },
            "scene_preferences": [
                "HOME_ROUTINE", "VANITY_TRYON", "CAFE_DINING", "STREET_OUTING",
            ],
            "risk_registry_key": "headscarf",
            "identity_priority": [
                "main_color", "pattern", "overall_shape", "placement_zone"
            ],
        },
        "scarf": {
            "display_family": "WORN_ACCESSORY",
            "wearing_zone": "NECK_SHOULDER",
            "required_result_view": "ALREADY_WORN_UPPER_BODY_RESULT",
            "supporting_style_context": "OUTFIT_LAYERING",
            "process_policy": "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "base_top_direction": "简洁基础上衣",
                "outer_layer_direction": "按商品图和运营卖点保守搭配，不默认冬季外套",
                "neckline_direction": "肩颈关系清楚",
                "hair_direction": "避免遮住商品主体",
                "palette_relation": "与目标商品自然协调",
                "visibility_requirement": "至少一段清楚看到商品与肩颈、上半身穿搭关系",
            },
            "scene_preferences": ["HOME_ROUTINE", "OFFICE_WORKBREAK", "STREET_OUTING"],
            "risk_registry_key": "scarf",
            "identity_priority": [
                "main_color", "pattern", "fringe", "overall_shape", "drape_relation"
            ],
        },
    }
    return scarf_profiles[canonical_type]


_RESULT_LABELS = {
    "ALREADY_WORN_EAR_VISIBLE": "从已经佩戴好的状态开始，至少有一段清楚看到耳饰与耳部、脸部或整套穿搭的关系",
    "ALREADY_STYLED_HAIR_RESULT": "真人方向从已经固定好的发型状态开始，至少有一段清楚看到发饰位置、相对大小和发型结果",
    "ALREADY_WORN_UPPER_BODY_RESULT": "从已经围好或披好的状态开始，至少有一段清楚看到围巾与脖颈、肩部及上半身穿搭的关系",
    "ALREADY_STYLED_NECK_RESULT": "从已经搭配好的状态开始，至少有一段清楚看到丝巾与颈部、领口及上半身穿搭的关系",
    "ALREADY_STYLED_HEAD_RESULT": "从已经完成的头部造型开始，至少有一段清楚看到头巾位置、轮廓及与头发和穿搭的关系",
    "PRODUCT_DETAIL_ONLY": "当前承载只展示商品外观与结构，不声称必须依靠真人佩戴才能证明的效果",
}

_RELATION_LABELS = {
    "EAR": "商品已经正确佩戴在耳部，耳侧保持清楚可见",
    "HAIR": "商品已经固定在头发或发型的正确位置",
    "NECK_SHOULDER": "商品已经自然位于脖颈和肩部区域",
    "NECK_UPPER_BODY": "商品已经自然搭配在颈部和上半身领口区域",
    "HEAD_HAIR": "商品已经位于头部或头发的日常造型位置",
}

_IDENTITY_LABELS = {
    "pair_or_single": "单只或成对关系",
    "attachment_type": "佩戴连接结构",
    "component_order": "部件数量与排列顺序",
    "relative_length": "相对长度和比例",
    "material_color": "材质观感和颜色",
    "accessory_subtype": "发饰具体类型",
    "overall_shape": "整体形状",
    "ornament_layout": "装饰排列",
    "relative_size": "与头发的相对大小",
    "placement_zone": "佩戴位置",
    "main_color": "主色",
    "pattern": "图案",
    "fringe": "流苏或边缘结构",
    "drape_relation": "肩颈垂落关系",
    "neckline_relation": "与领口的搭配关系",
}


def _pairing_guidance(authority: Dict[str, Any]) -> str:
    mode = _text(authority.get("pairing_mode")).upper()
    if mode == "PAIR":
        return "授权锚点明确为成对耳饰；保持成对关系，不改写成单只商品"
    if mode == "SINGLE":
        return "授权锚点明确为单只耳饰；保持单只关系，不改写成一对商品"
    return "授权锚点未说明单只或成对；只写‘耳饰/耳侧’，不得补写一对、双耳、两只、单只或单耳"


def _identity_labels(values: Iterable[Any], limit: int = 4) -> List[str]:
    return [
        _IDENTITY_LABELS.get(value, value)
        for value in _dedupe(values, limit=limit)
    ]


class AccessoryExecutionAdapter(CategoryExecutionAdapter):
    domain = "ACCESSORY"

    def supports(self, *, product_type: str, top_category: str) -> bool:
        resolved = normalize_product_type(product_type, top_category)
        return (
            resolved.recognized_by_registry
            and resolved.canonical_type in _SUPPORTED_TYPES
        )

    def compile_profile(
        self,
        *,
        product_type: str,
        top_category: str,
        anchor_card: Dict[str, Any],
    ) -> Dict[str, Any]:
        resolved = normalize_product_type(product_type, top_category)
        if not (
            resolved.recognized_by_registry
            and resolved.canonical_type in _SUPPORTED_TYPES
        ):
            return {}
        definition = _profile_definition(resolved.canonical_type)
        risk = _risk_registry().get(definition["risk_registry_key"])
        risk = risk if isinstance(risk, dict) else {}
        interaction_boundary = list(risk.get("forbidden") or [])
        if resolved.canonical_type in _SCARF_TYPES:
            # The existing type registry already owns the scarf-specific
            # boundaries; reuse them instead of introducing a second rule set.
            interaction_boundary = [
                *resolved.forbidden_terms,
                *interaction_boundary,
            ]
        profile = {
            "display_family": definition["display_family"],
            "product_subtype": resolved.canonical_type,
            "wearing_zone": definition["wearing_zone"],
            "required_result_view": definition["required_result_view"],
            "supporting_style_context": definition["supporting_style_context"],
            "interaction_boundary": _dedupe(interaction_boundary, limit=4),
            "identity_priority": list(definition["identity_priority"]),
        }
        if resolved.canonical_type in _SCARF_TYPES:
            profile.update({
                "process_policy": definition["process_policy"],
                "preferred_carriers": list(definition["preferred_carriers"]),
                "compatible_proof_subjects": list(
                    definition["compatible_proof_subjects"]
                ),
                "outfit_context": dict(definition["outfit_context"]),
                "scene_preferences": list(definition["scene_preferences"]),
            })
        if resolved.canonical_type == "earring":
            profile["identity_authority"] = _earring_pairing_authority(anchor_card)
        return {
            "domain": self.domain,
            "schema_version": (
                "accessory-execution-profile-v3-scarf"
                if resolved.canonical_type in _SCARF_TYPES
                else "accessory-execution-profile-v2"
            ),
            "product_type_source": {
                "canonical_type": resolved.canonical_type,
                "canonical_family": resolved.canonical_family,
                "canonical_slot": resolved.canonical_slot,
                "display_type": resolved.display_type,
                "recognized_by_registry": True,
            },
            "profile": profile,
            "authority": {
                "carrier": "STRUCTURE_CONTRACT",
                "content": "CENTRAL_SELLING_ARGUMENT",
                "appearance": "REFERENCE_IMAGE_AND_APPROVED_ANCHORS",
                "physical_execution": "CATEGORY_EXTENSION",
            },
        }

    def resolve_carrier_execution(
        self,
        extension: Dict[str, Any],
        *,
        presentation_mode: str,
    ) -> Dict[str, Any]:
        profile = extension.get("profile") if isinstance(extension.get("profile"), dict) else {}
        mode = _text(presentation_mode).upper()
        wearer = mode in {"PERSON_ON_CAMERA", "MIXED"}
        required_view = (
            _text(profile.get("required_result_view"))
            if wearer
            else "PRODUCT_DETAIL_ONLY"
        )
        return {
            "presentation_mode": mode,
            "wearing_zone": _text(profile.get("wearing_zone")),
            "required_view": required_view,
            "required_view_guidance_zh": _RESULT_LABELS.get(required_view, ""),
            "product_relation_zh": _RELATION_LABELS.get(
                _text(profile.get("wearing_zone")), ""
            ),
            "interaction_boundary": list(profile.get("interaction_boundary") or []),
            "identity_priority": list(profile.get("identity_priority") or []),
            "identity_authority": dict(profile.get("identity_authority") or {}),
            "process_policy": _text(profile.get("process_policy")),
            "preferred_carriers": list(profile.get("preferred_carriers") or []),
            "compatible_proof_subjects": list(
                profile.get("compatible_proof_subjects") or []
            ),
            "outfit_context": dict(profile.get("outfit_context") or {}),
            "scene_preferences": list(profile.get("scene_preferences") or []),
            "claim_boundary": (
                "允许展示并表达已经佩戴后的关系；不要求完整佩戴过程"
                if wearer
                else "只展示商品外观与结构；不把静物或手持画面写成佩戴结果证明"
            ),
        }

    def build_blueprint_guidance(
        self,
        extension: Dict[str, Any],
        *,
        carrier_execution: Dict[str, Any],
    ) -> str:
        profile = extension.get("profile") if isinstance(extension.get("profile"), dict) else {}
        lines = [
            carrier_execution.get("product_relation_zh"),
            carrier_execution.get("required_view_guidance_zh"),
            carrier_execution.get("claim_boundary"),
        ]
        boundaries = _dedupe(carrier_execution.get("interaction_boundary") or [], limit=3)
        if boundaries:
            lines.append("避免：" + "；".join(boundaries))
        priorities = _identity_labels(profile.get("identity_priority") or [], limit=4)
        if priorities:
            lines.append("商品身份优先保持：" + "、".join(priorities))
        outfit = profile.get("outfit_context") if isinstance(profile.get("outfit_context"), dict) else {}
        if outfit:
            lines.append(
                "穿搭可见关系："
                + _text(outfit.get("visibility_requirement"))
            )
        identity_authority = (
            profile.get("identity_authority")
            if isinstance(profile.get("identity_authority"), dict)
            else {}
        )
        if identity_authority:
            lines.append(_pairing_guidance(identity_authority))
        return "\n".join(_dedupe(lines, limit=7))

    def build_video_brief(
        self,
        extension: Dict[str, Any],
        *,
        carrier_execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        profile = extension.get("profile") if isinstance(extension.get("profile"), dict) else {}
        return {
            "schema_version": (
                "accessory-video-handoff-v3-scarf"
                if _text(profile.get("product_subtype")) in _SCARF_TYPES
                else "accessory-video-handoff-v2"
            ),
            "product_relation": _text(carrier_execution.get("product_relation_zh")),
            "required_visible_result": _text(
                carrier_execution.get("required_view_guidance_zh")
            ),
            "interaction_limit": _dedupe(
                carrier_execution.get("interaction_boundary") or [], limit=3
            ),
            "identity_focus": _identity_labels(
                profile.get("identity_priority") or [], limit=4
            ),
            "claim_boundary": _text(carrier_execution.get("claim_boundary")),
            "identity_authority": dict(profile.get("identity_authority") or {}),
            "identity_authority_guidance": (
                _pairing_guidance(profile.get("identity_authority") or {})
                if profile.get("identity_authority")
                else ""
            ),
            "outfit_context": dict(profile.get("outfit_context") or {}),
            "process_policy": _text(profile.get("process_policy")),
        }

    def validate_identity(
        self,
        extension: Dict[str, Any],
        *,
        script: Dict[str, Any],
    ) -> List[str]:
        profile = (
            extension.get("profile")
            if isinstance(extension.get("profile"), dict)
            else {}
        )
        if _text(profile.get("product_subtype")) != "earring":
            return []
        authority = (
            profile.get("identity_authority")
            if isinstance(profile.get("identity_authority"), dict)
            else {}
        )
        mode = _text(authority.get("pairing_mode")).upper() or "UNAVAILABLE"

        # Check authored content only.  Do not scan the execution brief because
        # its own guard sentence intentionally contains the forbidden terms.
        concept = script.get("script_concept") or {}
        raw_production = script.get("production_design") or {}
        production = {
            key: value
            for key, value in raw_production.items()
            if key != "accessory_execution"
        }
        usage = script.get("product_usage") or {}
        voice = script.get("continuous_voiceover") or {}
        content_scope = {
            "script_concept": concept,
            "production_design": production,
            "product_usage": usage,
            "storyboard": script.get("storyboard") or [],
            "voiceover_translation": voice.get("chinese_translation"),
            "voiceover_realization_zh": voice.get("selling_argument_realization_zh"),
        }
        content = json.dumps(content_scope, ensure_ascii=False)
        pair_hits = [term for term in _PAIR_OUTPUT_TERMS if term in content]
        single_hits = [term for term in _SINGLE_OUTPUT_TERMS if term in content]
        if mode == "UNAVAILABLE" and (pair_hits or single_hits):
            return [
                "PRODUCT_IDENTITY_CONFLICT：授权锚点未说明耳饰单双关系，"
                "脚本不得推断=" + "、".join(_dedupe([*pair_hits, *single_hits]))
            ]
        if mode == "PAIR" and single_hits:
            return [
                "PRODUCT_IDENTITY_CONFLICT：授权为成对耳饰，脚本却写成单只="
                + "、".join(_dedupe(single_hits))
            ]
        if mode == "SINGLE" and pair_hits:
            return [
                "PRODUCT_IDENTITY_CONFLICT：授权为单只耳饰，脚本却写成成对="
                + "、".join(_dedupe(pair_hits))
            ]
        return []
