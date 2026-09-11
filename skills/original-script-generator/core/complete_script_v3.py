"""Governed creative diversity and complete-script contracts for stage-0 V3.1.

Observed execution facts remain untouched.  This module allocates explicitly
labelled production design choices before an LLM writes the creative blueprint,
so the model cannot silently fall back to its most common try-on template.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from core.scene_reference_adapter import (
    scene_family_for_motif,
    scene_reference_contract_for_family,
)
from core.outfit_template_provider import (
    load_structured_outfit_templates,
    without_outfit_display_metadata,
)
from core.outfit_selection import (
    OUTFIT_SELECTION_CONTRACT_VERSION,
    demonstration_mode_from_direction,
    normalize_outfit_candidate,
    select_outfit_candidate,
    target_role_for,
)
from core.product_type_resolution import normalize_product_type
from core.semantic_spine import (
    classify_scene_relation,
    scene_relation_score,
    semantic_spine_enabled,
)


CREATIVE_DIVERSITY_POLICY_VERSION = "creative-diversity-v14-semantic-scene-bridge"
OUTFIT_SCENE_AFFINITY_POLICY_VERSION = "outfit-scene-affinity-v2-exact-soft-boost"
OUTFIT_SCENE_MATCH_BONUS = 24
EXACT_PRODUCT_OUTFIT_SCENE_MATCH_BONUS = 30
SELLING_SCENE_SEMANTIC_MATCH_BONUS = 40
ACCESSORY_OUTFIT_SELECTION_CONTRACT_VERSION = OUTFIT_SELECTION_CONTRACT_VERSION
COMPLETE_BLUEPRINT_SCHEMA_VERSION = "complete-script-blueprint-v4-carrier"
COMPLETE_SCRIPT_POLICY_VERSION = "complete-script-qc-v22-event-driven-light"

RECENT_FAILURE_QUARANTINE_PATTERNS = [
    "家中卧室镜前＋低头看腰线",
    "半步后退＋转身＋整理衣摆",
    "低头确认衣摆后轻笑或点头",
    "咖啡场景＋通用穿搭展示",
    "静物人台＋轻推近＋整体收尾",
]

AI_CONTROL_TERMS = (
    "轻判断",
    "轻满意",
    "轻安心",
    "情绪推进",
    "情绪弧",
    "决策信号",
    "当前主proof",
    "用户记住",
    "卖点成立",
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, value: Any, length: int = 24) -> str:
    value = without_outfit_display_metadata(value)
    material = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(material.encode("utf-8")).hexdigest()[:length].upper()


def _direction_id(direction: Dict[str, Any]) -> str:
    return _text(direction.get("direction_assignment_id") or direction.get("output_slot"))


def _reference_action_text(direction: Dict[str, Any]) -> str:
    reference = direction.get("execution_reference") if isinstance(direction.get("execution_reference"), dict) else {}
    pieces = [*reference.get("behavior_chain", [])]
    pieces.extend(
        _text(item.get("observable_action"))
        for item in reference.get("shot_execution_spine", [])
        if isinstance(item, dict)
    )
    return " ".join(_text(item).lower() for item in pieces if _text(item))


def authoritative_carrier(direction: Dict[str, Any]) -> str:
    """Resolve the one carrier authority used by every creative consumer.

    The execution plan is the structure router's executable contract.  Older
    references are only a fallback, so a static structure cannot be rewritten
    as a wearer story merely because its historical reference involved a person.
    """

    plan = direction.get("structure_execution_plan")
    plan = plan if isinstance(plan, dict) else {}
    carrier = _text(plan.get("content_carrier")).upper()
    if carrier:
        return carrier
    carriers = {
        _text(shot.get("carrier_mode")).upper()
        for shot in plan.get("shot_plan", [])
        if isinstance(shot, dict) and _text(shot.get("carrier_mode"))
    }
    if len(carriers) == 1:
        return next(iter(carriers))
    contract = direction.get("structure_contract")
    contract = contract if isinstance(contract, dict) else {}
    carrier = _text(contract.get("content_carrier")).upper()
    if carrier:
        return carrier
    reference = direction.get("execution_reference")
    reference = reference if isinstance(reference, dict) else {}
    return _text(reference.get("content_carrier")).upper() or "UNAVAILABLE"


def _carrier_contract(carrier: str) -> Dict[str, str]:
    normalized = _text(carrier).upper()
    if normalized == "STATIC_PRODUCT":
        return {
            "required_carrier": normalized,
            "required_presentation_mode": "STATIC_PRODUCT",
            "on_camera_policy": "NO_PERSON_NO_HANDS",
        }
    if normalized == "HAND_ONLY":
        return {
            "required_carrier": normalized,
            "required_presentation_mode": "HANDS_ONLY",
            "on_camera_policy": "HANDS_AND_PRODUCT_ONLY",
        }
    if normalized == "WEARER_ACTIVE":
        return {
            "required_carrier": normalized,
            "required_presentation_mode": "PERSON_ON_CAMERA",
            "on_camera_policy": "WEARER_REQUIRED",
        }
    return {
        "required_carrier": normalized or "UNAVAILABLE",
        "required_presentation_mode": "MIXED" if normalized == "MIXED" else "UNAVAILABLE",
        "on_camera_policy": "STRUCTURE_PLAN_GOVERNS",
    }


def creative_product_profile(product_type: str, category: str = "") -> str:
    """Return a soft production profile; structure carrier remains authoritative."""

    value = f"{_text(product_type)} {_text(category)}".lower()
    worn_accessory_tokens = (
        "围巾", "丝巾", "头巾", "披肩", "帽", "耳环", "耳饰", "耳线", "项链", "项圈",
        "手链", "手镯", "手环", "手串", "发饰", "发夹", "抓夹", "发圈",
        "包", "墨镜", "太阳镜", "眼镜", "scarf", "hat", "earring", "necklace", "bag",
        "bracelet", "bangle", "hair clip", "hair accessory",
    )
    hand_static_tokens = (
        "戒指", "ring",
    )
    apparel_tokens = (
        "女装", "服装", "外套", "上装", "夹克", "衬衫", "毛衣", "卫衣", "裙", "裤",
        "apparel", "jacket", "coat", "top", "dress", "skirt", "trousers", "pants",
    )
    if any(token in value for token in worn_accessory_tokens):
        return "WORN_ACCESSORY"
    if any(token in value for token in hand_static_tokens):
        return "HAND_STATIC_ACCESSORY"
    if any(token in value for token in apparel_tokens):
        return "WORN_APPAREL"
    if any(token in value for token in ("配饰", "饰品", "首饰", "accessor")):
        return "HAND_STATIC_ACCESSORY"
    return "GENERAL_PRODUCT"


def _selling_argument_scene_preferences(direction: Dict[str, Any]) -> List[str]:
    """Translate the frozen selling argument into soft scene-affinity tags.

    This is deliberately a small deterministic vocabulary.  It changes only
    candidate ranking and never authorizes a product fact or blocks a scene.
    """

    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict)
        else {}
    )
    value = (
        bundle.get("value_proposition")
        if isinstance(bundle.get("value_proposition"), dict)
        else {}
    )
    material = " ".join(
        _text(item).lower()
        for item in (
            argument.get("core_value"),
            argument.get("operator_expression"),
            argument.get("target_need"),
            argument.get("proof_thesis"),
            value.get("text"),
            bundle.get("content_mainline"),
            bundle.get("audience_tension_text"),
        )
        if _text(item)
    )
    tag_keywords = {
        "PHOTO_FRIENDLY": (
            "拍照", "出片", "打卡", "探店", "上镜", "咖啡", "photo", "camera",
        ),
        "PREMIUM_AMBIENCE": (
            "显贵", "有钱感", "老钱", "复古", "高级", "质感", "富婆", "精致",
            "气质", "old money", "premium", "vintage", "luxury",
        ),
        "COMMUTE": (
            "通勤", "办公室", "上班", "工作", "会议", "空调房", "冷气房",
            "office", "commute", "work",
        ),
        "BODY_RESULT_CLEAR": (
            "显瘦", "遮肉", "赘肉", "身形", "身材", "比例", "腰线", "腰部",
            "腿部", "版型", "slim", "body", "waist", "proportion",
        ),
        "MULTI_OCCASION": (
            "百搭", "多场景", "多种场合", "什么场合", "旅行", "使用率",
            "versatile", "occasion", "travel",
        ),
        "DAYTIME_USE": (
            "防晒", "遮阳", "阳光直射", "直射阳光", "烈日", "太阳", "sun",
            "shade",
        ),
    }
    return [
        tag
        for tag, keywords in tag_keywords.items()
        if any(keyword in material for keyword in keywords)
    ]


def _scene_affinity_tags(scene_motif: Any) -> List[str]:
    """Describe existing scene candidates for soft matching only."""

    scene = _text(scene_motif).lower()
    tag_keywords = {
        "PHOTO_FRIENDLY": (
            "咖啡", "精品", "酒店", "商场", "展览", "书店", "落地窗",
        ),
        "PREMIUM_AMBIENCE": (
            "咖啡", "精品", "酒店", "书店", "展览", "暖色书架",
        ),
        "COMMUTE": (
            "办公室", "写字楼", "电梯", "会议", "通勤",
        ),
        "BODY_RESULT_CLEAR": (
            "单色", "白墙", "浅色墙", "金属墙", "走廊", "自然光墙面",
            "落地窗", "窗边", "靠窗", "酒店", "商场",
        ),
        "MULTI_OCCASION": (
            "玄关", "商场", "等候", "出口", "连廊", "车道",
        ),
        "DAYTIME_USE": (
            "户外", "街", "街边", "入口", "门廊", "遮檐", "室外",
            "外侧", "步道",
        ),
    }
    return [
        tag
        for tag, keywords in tag_keywords.items()
        if any(keyword in scene for keyword in keywords)
    ]


def _category_scene_preferences(direction: Dict[str, Any]) -> List[str]:
    extension = (
        direction.get("category_execution_extension")
        if isinstance(direction.get("category_execution_extension"), dict)
        else {}
    )
    profile = (
        extension.get("profile")
        if isinstance(extension.get("profile"), dict)
        else {}
    )
    return list(dict.fromkeys(
        _text(item).upper()
        for item in profile.get("scene_preferences") or []
        if _text(item)
    ))


def _category_scene_affinity_tags(candidate: Dict[str, Any]) -> List[str]:
    """Describe an existing candidate in the adapter's compact scene vocabulary."""

    moment = _text(candidate.get("moment_family_id")).upper()
    scene = _text(candidate.get("scene_motif"))
    tags: List[str] = []
    if moment in {"HOME_ROUTINE", "READY_TO_LEAVE"} or any(
        token in scene for token in ("公寓", "客厅", "玄关", "梳妆")
    ):
        tags.append("HOME_ROUTINE")
    if moment in {"OFFICE_BREAK", "COMMUTE_TRANSITION"} or any(
        token in scene for token in ("办公室", "写字楼", "电梯")
    ):
        tags.append("OFFICE_WORKBREAK")
    if "咖啡" in scene:
        tags.append("CAFE_DINING")
    if "梳妆" in scene or "镜" in scene:
        tags.append("VANITY_TRYON")
    if moment in {"LEISURE_OUTING", "WAITING_IN_TRANSIT"} or any(
        token in scene for token in ("街", "入口", "户外", "等候")
    ):
        tags.append("STREET_OUTING")
    if moment == "WAITING_IN_TRANSIT" or any(
        token in scene for token in ("车", "搭车")
    ):
        tags.append("CAR_TRANSIT")
    return list(dict.fromkeys(tags))


def _scene_request_affinity_tags(scene_request: Mapping[str, Any]) -> List[str]:
    """Small bridge from a compiled scene request to existing candidate tags."""

    tags: List[str] = []
    scene_intent = _text(scene_request.get("scene_intent")).upper()
    light_need = _text(scene_request.get("time_light_need")).upper()
    if scene_intent == "DAYTIME_USE" or light_need == "DAYLIGHT":
        tags.append("DAYTIME_USE")
    if scene_intent == "PHOTO_FRIENDLY":
        tags.append("PHOTO_FRIENDLY")
    if scene_intent == "BODY_RESULT":
        tags.append("BODY_RESULT_CLEAR")
    if scene_intent == "SCENE_USAGE":
        tags.append("MULTI_OCCASION")
    return tags


def _scene_request_proof_environment_score(
    scene_request: Mapping[str, Any], candidate: Mapping[str, Any]
) -> int:
    """Prefer a scene that can naturally carry the authorised use occasion.

    This is deliberately a positive planning signal, not a validator.  It
    solves cases such as a SUN_SHADE angle drifting into a generic apartment
    simply because that apartment combination had been used less recently.
    """

    intent = _text(scene_request.get("scene_intent")).upper()
    theme = _text(scene_request.get("argument_theme")).upper()
    scene = _text(candidate.get("scene_motif"))
    if intent != "DAYTIME_USE" and theme != "SUN_SHADE":
        return 0
    outdoor_tokens = (
        "户外", "室外", "街边", "临街", "步道", "楼下", "门廊", "遮檐", "外侧",
    )
    if any(token in scene for token in outdoor_tokens):
        return 48
    return 16 if "DAYTIME_USE" in _scene_affinity_tags(scene) else 0


def _scene_request_contract(
    *, product_type: str, category: str, direction: Dict[str, Any],
    carrier_contract: Dict[str, str], country: str,
) -> Dict[str, Any]:
    """Compile one small semantic request; no seller-copy keyword rules here."""

    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict)
        else {}
    )
    argument_theme = _text(argument.get("argument_theme")).upper()
    proof_subject = _text(argument.get("proof_subject")).upper()
    scene_intent_by_theme = {
        "SUN_SHADE": "DAYTIME_USE",
        "HAIR_RESCUE": "GET_READY",
        "COLOR_MOOD": "PHOTO_FRIENDLY",
        "SURFACE_GLOSS": "DETAIL_DISCOVERY",
        "MULTI_USE": "MULTI_OCCASION",
    }
    scene_intent = scene_intent_by_theme.get(argument_theme, "")
    if not scene_intent and proof_subject == "ON_BODY_RESULT":
        scene_intent = "BODY_RESULT"
    if not scene_intent and proof_subject == "SCENE_USAGE":
        scene_intent = "SCENE_USAGE"
    scene_intent = scene_intent or "GENERAL_USE"

    carrier = _text(carrier_contract.get("required_carrier")).upper()
    capture_mode = {
        "WEARER_ACTIVE": "CREATOR_SELF_SHOT",
        "HAND_ONLY": "HANDS_PRODUCT_SHARE",
        "STATIC_PRODUCT": "STATIC_PRODUCT_RECORD",
    }.get(carrier, "CREATOR_SELF_SHOT" if carrier == "MIXED" else "UNAVAILABLE")
    canonical_type = normalize_product_type(product_type, category).canonical_type
    return {
        "schema_version": "scene-request-v1",
        "canonical_product_type": canonical_type,
        "presentation_mode": _text(
            carrier_contract.get("required_presentation_mode")
        ),
        "scene_intent": scene_intent,
        "time_light_need": "DAYLIGHT" if argument_theme == "SUN_SHADE" else "FLEXIBLE",
        "capture_mode": capture_mode,
        "country": _text(country),
        "argument_theme": argument_theme,
    }


def _creative_combinations(
    direction: Dict[str, Any], product_type: str, category: str = ""
) -> List[Dict[str, Any]]:
    action_text = _reference_action_text(direction)
    carrier = authoritative_carrier(direction)
    product_profile = creative_product_profile(product_type, category)
    direction_carrier = authoritative_carrier(direction)
    upper_apparel = any(
        token in _text(product_type).lower()
        for token in ("衣", "外套", "上装", "夹克", "jacket", "coat", "top")
    )
    if product_profile == "WORN_APPAREL" and upper_apparel and carrier in {"WEARER_ACTIVE", "MIXED", "UNAVAILABLE"}:
        combinations = [
            {
                "moment_family_id": "READY_TO_LEAVE",
                "persona_role": "下班前收拾随身物品的通勤者",
                "viewer_relationship": "像朋友分享自己刚完成的一套通勤穿搭",
                "scene_motif": "办公室衣帽区靠窗墙面",
                "opening_action": "完成外搭穿着动作后伸手拿起放在一旁的随身物品",
                "action_grammar": "完成穿着→拿起随身物品→离开原位置",
                "visual_tone": "旁观式通勤记录",
            },
            {
                "moment_family_id": "READY_TO_LEAVE",
                "persona_role": "准备出门的日常通勤者",
                "viewer_relationship": "像朋友分享出门前刚搭好的基础穿搭",
                "scene_motif": "公寓玄关自然光墙面",
                "opening_action": "完成外搭穿着动作后拿起玄关台面上的钥匙",
                "action_grammar": "完成穿着→拿起钥匙→向门口移动",
                "visual_tone": "固定机位出门记录",
            },
            {
                "moment_family_id": "READY_TO_LEAVE",
                "persona_role": "收好衣物准备离开的办公室使用者",
                "viewer_relationship": "像同事分享一件能补足基础穿搭层次的外搭",
                "scene_motif": "服装收纳架旁的浅色墙面",
                "opening_action": "外搭已经穿到身上，人物把空衣架放回收纳架",
                "action_grammar": "放回衣架→站直整理随身物品→走出画面",
                "visual_tone": "低干预生活记录",
            },
            {
                "moment_family_id": "READY_TO_LEAVE",
                "persona_role": "从客厅准备出门的居家通勤者",
                "viewer_relationship": "像朋友分享一套自己正准备穿出门的搭配",
                "scene_motif": "客厅窗边的单色背景区域",
                "opening_action": "完成外搭穿着动作后从沙发扶手拿起随身包",
                "action_grammar": "完成穿着→拿起随身包→经过窗边准备离开",
                "visual_tone": "自然窗光旁观记录",
            },
            {
                "moment_family_id": "TRAVEL_PREP",
                "persona_role": "出发前整理旅行行李的城市穿搭者",
                "viewer_relationship": "像朋友分享这次旅行为什么只带这一件外搭",
                "scene_motif": "卧室行李箱旁的自然光空地",
                "opening_action": "外搭已经穿好，人物把一件折好的内搭放进行李箱后回到手机前",
                "action_grammar": "放入一件内搭→回到手机前展示整体→补录外搭细节",
                "visual_tone": "出发前真实行李整理记录",
            },
            {
                "moment_family_id": "TRAVEL_TRANSIT",
                "persona_role": "在机场等待登机的旅行穿搭者",
                "viewer_relationship": "像朋友分享这一趟旅行随身穿着的一件外搭",
                "scene_motif": "机场候机区靠窗的普通座位边缘",
                "opening_action": "外搭已经穿好，人物从座位旁拿起随身小包后面对手机",
                "action_grammar": "拿起随身包→手机前展示整体→近距离补录外搭细节",
                "visual_tone": "候机时的普通手机分享",
            },
            {
                "moment_family_id": "TRAVEL_STAY",
                "persona_role": "到达目的地后准备从酒店出门的旅行者",
                "viewer_relationship": "像朋友分享旅行当天已经搭好的外出穿搭",
                "scene_motif": "酒店房间行李架旁的自然光墙面",
                "opening_action": "外搭已经穿好，人物从行李架旁拿起随身包回到手机前",
                "action_grammar": "拿起随身包→手机前展示整体→在身补录外搭细节",
                "visual_tone": "酒店出门前的真实手机记录",
            },
            {
                "moment_family_id": "COMMUTE_TRANSITION",
                "persona_role": "午休准备下楼的办公室使用者",
                "viewer_relationship": "像同事分享午休出门时正在穿的一套外搭",
                "scene_motif": "写字楼电梯厅的浅色金属墙面",
                "opening_action": "外搭已经穿好，人物看一眼楼层指示后走向开启的电梯",
                "action_grammar": "等待电梯→电梯门开启→自然走入",
                "visual_tone": "固定机位午间行动记录",
            },
            {
                "moment_family_id": "LEISURE_OUTING",
                "persona_role": "傍晚从书店离开的城市日常穿搭者",
                "viewer_relationship": "像朋友分享逛完书店后这一身的真实状态",
                "scene_motif": "书店出口旁的暖色书架过道",
                "opening_action": "外搭已经穿好，人物把看完的书放回陈列台后转向出口",
                "action_grammar": "放回书→沿书架过道前行→走向出口",
                "visual_tone": "暖光下的低干预生活记录",
            },
            {
                "moment_family_id": "LEISURE_OUTING",
                "persona_role": "在咖啡厅短暂停留的城市日常穿搭者",
                "viewer_relationship": "像朋友分享这一身放进有质感空间后的整体效果",
                "scene_motif": "咖啡厅靠窗的质感座位区域",
                "opening_action": "外搭已经穿好，人物自然处在窗边座位附近，完整造型先进入画面",
                "action_grammar": "完整造型建立→局部或侧面观察→回到整体",
                "visual_tone": "窗边自然光下的整体穿搭观察",
            },
            {
                "moment_family_id": "LEISURE_OUTING",
                "persona_role": "在精品酒店休息区等候会面的城市穿搭者",
                "viewer_relationship": "像朋友分享一套经典、克制又适合城市场合的穿搭",
                "scene_motif": "精品酒店大堂的暖色休息区",
                "opening_action": "外搭已经穿好，人物自然处在休息区，整体轮廓保持清楚",
                "action_grammar": "完整造型建立→侧面或细节观察→整体收束",
                "visual_tone": "暖色空间里的低干预穿搭观察",
            },
            {
                "moment_family_id": "LEISURE_OUTING",
                "persona_role": "周末准备逛街的城市日常穿搭者",
                "viewer_relationship": "像朋友分享周末走动时整套穿搭的自然比例",
                "scene_motif": "商场连廊靠窗的自然光休息区",
                "opening_action": "外搭已经穿好，人物从窗边长椅自然起身并沿连廊前行",
                "action_grammar": "从长椅起身→经过窗边→沿连廊继续前行",
                "visual_tone": "自然光下的旁观式行动记录",
            },
            {
                "moment_family_id": "QUICK_ERRAND",
                "persona_role": "下楼取件的公寓住户",
                "viewer_relationship": "像邻居分享临时下楼时随手穿的一套外搭",
                "scene_motif": "公寓大堂快递柜旁的干净墙面",
                "opening_action": "外搭已经穿好，人物关上快递柜门后转身走向大堂出口",
                "action_grammar": "关上柜门→转身经过大厅→走向出口",
                "visual_tone": "公寓公共空间的日常记录",
            },
            {
                "moment_family_id": "WAITING_IN_TRANSIT",
                "persona_role": "准备搭车去见朋友的城市日常穿搭者",
                "viewer_relationship": "像朋友分享等车时这一身在自然走动中的样子",
                "scene_motif": "公寓楼下有顶车道的自然光等候区",
                "opening_action": "外搭已经穿好，人物从立柱旁走到等候线并看向来车方向",
                "action_grammar": "走到等候区→自然停留→向来车方向继续前行",
                "visual_tone": "有环境纵深的城市生活记录",
            },
            {
                "moment_family_id": "LEISURE_OUTING",
                "persona_role": "周末去看展的城市日常穿搭者",
                "viewer_relationship": "像朋友分享进入展览空间前这一身的整体效果",
                "scene_motif": "小型展览空间入口的白墙走廊",
                "opening_action": "外搭已经穿好，人物从导览牌旁自然转入主展厅方向",
                "action_grammar": "经过导览牌→沿白墙前行→进入展厅",
                "visual_tone": "白墙空间里的克制跟随记录",
            },
        ]
        if not any(token in action_text for token in ("button", "扣", "fasten", "前襟", "门襟")):
            for item in combinations:
                item["opening_action"] = item["opening_action"].replace(
                    "完成外搭穿着动作后", "外搭已经穿好，人物"
                ).replace("外搭已经穿到身上，人物", "外搭已经穿好，人物")
        return combinations
    if product_profile == "WORN_APPAREL" and carrier in {"WEARER_ACTIVE", "MIXED", "UNAVAILABLE"}:
        return [
            {
                "persona_role": "出门前完成日常穿搭的人",
                "viewer_relationship": "像朋友分享自己刚穿好的一套日常搭配",
                "scene_motif": "公寓玄关自然光区域",
                "opening_action": "商品已经穿好，人物拿起出门要带的随身物品",
                "action_grammar": "穿着结果已形成→拿起随身物品→自然离开",
                "visual_tone": "固定机位生活记录",
            },
            {
                "persona_role": "准备开始当天行程的日常使用者",
                "viewer_relationship": "像朋友分享穿好以后在行动中的真实状态",
                "scene_motif": "客厅通往门口的自然光动线",
                "opening_action": "人物已完成穿着，从原位置起身并走向门口",
                "action_grammar": "起身→经过自然光区域→继续当天行程",
                "visual_tone": "旁观式行动记录",
            },
        ]
    if product_profile == "WORN_ACCESSORY" and carrier in {"WEARER_ACTIVE", "MIXED", "UNAVAILABLE"}:
        canonical_type = normalize_product_type(product_type, category).canonical_type
        subtype_combinations = {
            "winter_scarf": [
                {
                    "moment_family_id": "READY_TO_LEAVE",
                    "persona_role": "出门前已经围好围巾的日常通勤者",
                    "viewer_relationship": "像朋友分享围巾放进当天上半身穿搭后的真实样子",
                    "scene_motif": "公寓玄关靠近门口的自然光区域",
                    "opening_action": "围巾已经围好，人物拿起门边的随身包准备离开",
                    "action_grammar": "佩戴结果已形成→拿起随身包→向门口移动",
                    "visual_tone": "固定手机的出门前记录",
                },
                {
                    "moment_family_id": "COMMUTE_TRANSITION",
                    "persona_role": "午间准备下楼的办公室使用者",
                    "viewer_relationship": "像同事分享围巾和当天外层搭配放在一起的效果",
                    "scene_motif": "写字楼电梯厅靠墙的普通等候位置",
                    "opening_action": "围巾已经佩戴到位，人物站在电梯外自然等待",
                    "action_grammar": "佩戴结果建立→短暂停留→电梯门开后自然前行",
                    "visual_tone": "普通手机通勤记录",
                },
                {
                    "moment_family_id": "WAITING_IN_TRANSIT",
                    "persona_role": "准备搭车外出的城市日常穿搭者",
                    "viewer_relationship": "像朋友分享等车时围巾与上半身搭配的自然状态",
                    "scene_motif": "公寓楼下有顶入口的自然光等候区",
                    "opening_action": "围巾已经围好，人物从入口内侧走到等候位置",
                    "action_grammar": "走到等候区→上半身结果保持可见→自然停留",
                    "visual_tone": "低干预城市生活记录",
                },
                {
                    "moment_family_id": "HOME_ROUTINE",
                    "persona_role": "在窗边确认当天穿搭的日常使用者",
                    "viewer_relationship": "像朋友近距离看一眼围巾与基础上装的搭配关系",
                    "scene_motif": "客厅窗边靠墙的普通自然光位置",
                    "opening_action": "围巾已经围好，人物站在窗边让完整肩颈关系进入画面",
                    "action_grammar": "上半身结果建立→一次局部补拍→回到整体",
                    "visual_tone": "单手机视角的自然分享",
                },
            ],
            "silk_scarf": [
                {
                    "moment_family_id": "READY_TO_LEAVE",
                    "persona_role": "已经搭好丝巾准备出门的日常使用者",
                    "viewer_relationship": "像朋友分享丝巾放到基础上衣领口后的真实效果",
                    "scene_motif": "公寓玄关侧面的自然光墙面",
                    "opening_action": "丝巾已经搭配完成，人物拿起玄关处的钥匙",
                    "action_grammar": "颈部结果建立→拿起钥匙→自然准备离开",
                    "visual_tone": "固定手机的日常出门记录",
                },
                {
                    "moment_family_id": "OFFICE_BREAK",
                    "persona_role": "午间离开工位的办公室使用者",
                    "viewer_relationship": "像同事分享丝巾与简洁通勤上装的领口关系",
                    "scene_motif": "办公室公共休息区靠窗的自然光位置",
                    "opening_action": "丝巾已经搭配完成，人物拿起放在身边的手机准备离开",
                    "action_grammar": "半身结果建立→一次领口细节补拍→自然离开原位置",
                    "visual_tone": "普通手机的午间记录",
                },
                {
                    "moment_family_id": "LEISURE_OUTING",
                    "persona_role": "在咖啡厅短暂停留的城市日常使用者",
                    "viewer_relationship": "像朋友分享自然光下丝巾图案和上半身搭配的样子",
                    "scene_motif": "咖啡厅靠窗的普通座位边缘",
                    "opening_action": "丝巾已经搭配完成，人物自然坐在窗边让半身结果先进入画面",
                    "action_grammar": "半身结果建立→图案或边缘补拍→回到自然坐姿",
                    "visual_tone": "窗边手机随手记录",
                },
                {
                    "moment_family_id": "LEISURE_OUTING",
                    "persona_role": "准备进入展览空间的城市日常使用者",
                    "viewer_relationship": "像朋友分享走动时丝巾与简洁穿搭保持成立的状态",
                    "scene_motif": "小型展览入口的普通白墙走廊",
                    "opening_action": "丝巾已经搭配完成，人物从入口导览牌旁自然经过",
                    "action_grammar": "半身结果建立→沿白墙短距离前行→整体收束",
                    "visual_tone": "克制的跟随式手机记录",
                },
            ],
            "headscarf": [
                {
                    "moment_family_id": "DAYTIME_OUTING",
                    "persona_role": "白天准备步行去附近地点的城市日常使用者",
                    "viewer_relationship": "像朋友分享白天外出时头巾与整套穿搭的真实状态",
                    "scene_motif": "公寓楼下临街步道的白天自然光区域",
                    "opening_action": "头巾已经佩戴完成，人物从楼下入口自然走到临街步道",
                    "action_grammar": "头部和半身结果建立→短距离自然步行→停在普通等候位置",
                    "visual_tone": "手机随手记录的白天城市生活",
                },
                {
                    "moment_family_id": "READY_TO_LEAVE",
                    "persona_role": "已经完成日常头巾造型的外出使用者",
                    "viewer_relationship": "像朋友分享头巾和当天穿搭放在一起的真实样子",
                    "scene_motif": "公寓玄关靠门的自然光区域",
                    "opening_action": "头巾已经佩戴完成，人物拿起门边的随身包",
                    "action_grammar": "头部结果建立→拿起随身包→自然向门口移动",
                    "visual_tone": "固定手机的出门前记录",
                },
                {
                    "moment_family_id": "HOME_ROUTINE",
                    "persona_role": "在窗边分享当天头部造型的日常使用者",
                    "viewer_relationship": "像朋友看一眼头巾位置、轮廓和上半身穿搭关系",
                    "scene_motif": "客厅窗边靠墙的普通自然光位置",
                    "opening_action": "头巾已经佩戴完成，人物自然站在窗边面对手机",
                    "action_grammar": "正面结果建立→一次轻微侧面观察→回到整体",
                    "visual_tone": "单手机视角的自然分享",
                },
                {
                    "moment_family_id": "LEISURE_OUTING",
                    "persona_role": "周末准备与朋友见面的城市日常使用者",
                    "viewer_relationship": "像朋友分享普通外出状态下头巾与穿搭的关系",
                    "scene_motif": "咖啡厅入口内侧的自然光过渡区",
                    "opening_action": "头巾已经佩戴完成，人物从入口内侧自然走向座位方向",
                    "action_grammar": "头部和半身结果建立→短距离前行→自然收束",
                    "visual_tone": "低干预手机生活记录",
                },
                {
                    "moment_family_id": "COMMUTE_TRANSITION",
                    "persona_role": "准备离开办公室的日常头巾使用者",
                    "viewer_relationship": "像同事分享下班时头巾与简洁上装的实际状态",
                    "scene_motif": "办公室出口附近的普通浅色走廊",
                    "opening_action": "头巾已经佩戴完成，人物拿起随身物品准备离开",
                    "action_grammar": "结果建立→拿起随身物品→沿走廊自然离开",
                    "visual_tone": "自然通勤手机记录",
                },
            ],
        }
        if canonical_type in subtype_combinations:
            return subtype_combinations[canonical_type]
        return [
            {
                "persona_role": "出门前已经佩戴好配饰的日常使用者",
                "viewer_relationship": "像朋友分享配饰放进整套穿搭后的真实样子",
                "scene_motif": "公寓玄关旁的自然光区域",
                "opening_action": "配饰已经佩戴到位，人物拿起出门要带的随身物品",
                "action_grammar": "佩戴结果已形成→拿起随身物品→自然走向门口",
                "visual_tone": "低干预出门记录",
            },
            {
                "persona_role": "准备离开室内的日常配饰使用者",
                "viewer_relationship": "像朋友分享动作中才看清的佩戴关系",
                "scene_motif": "客厅窗边通往门口的自然动线",
                "opening_action": "配饰已经佩戴到位，人物从窗边起身准备离开",
                "action_grammar": "起身→经过自然光→佩戴关系自然显现→离开",
                "visual_tone": "旁观式生活记录",
            },
            {
                "persona_role": "下班前收好物品的通勤配饰使用者",
                "viewer_relationship": "像同事分享配饰与当天穿搭放在一起的效果",
                "scene_motif": "办公室出口附近的自然光墙面",
                "opening_action": "配饰已经佩戴到位，人物收起桌边的随身物品",
                "action_grammar": "收起物品→转向出口→穿戴关系保持可见",
                "visual_tone": "自然通勤记录",
            },
        ]
    if carrier == "STATIC_PRODUCT":
        if product_profile in {"WORN_ACCESSORY", "HAND_STATIC_ACCESSORY"}:
            return [
                {
                    "persona_role": "人物不出镜的配饰实物观察",
                    "viewer_relationship": "像朋友替你把配饰的外观和比例看清",
                    "scene_motif": "窗边浅色收纳托盘与干净桌面",
                    "opening_action": "配饰已放在托盘上，镜头从最明显的可见结构开始观察",
                    "action_grammar": "局部进入→同一承载上的角度变化→回到整体比例",
                    "visual_tone": "固定机位静物观察",
                },
                {
                    "persona_role": "人物不出镜的配饰到货记录",
                    "viewer_relationship": "像朋友分享刚放到桌面后最先注意到的细节",
                    "scene_motif": "玄关矮柜上的自然光台面",
                    "opening_action": "配饰已从包装中取出并保持完整可见",
                    "action_grammar": "整体落定→局部观察→换一个自然角度→整体收束",
                    "visual_tone": "克制的手机实物记录",
                },
            ]
        return [
            {
                "persona_role": "人物不出镜的质感静物商品观察",
                "viewer_relationship": "像朋友分享商品放进有质感陈列环境后的完整外观",
                "scene_motif": "精品服装工作室的暖色木质挂架区域",
                "opening_action": "商品已完整悬挂在木质挂架上，第一画面先保留整体轮廓",
                "action_grammar": "商品整体建立→局部或侧面观察→回到完整轮廓",
                "visual_tone": "暖色环境里的克制静物观察",
            },
            {
                "persona_role": "人物不出镜的质感静物商品观察",
                "viewer_relationship": "替用户看清商品在简洁质感空间里的颜色与轮廓",
                "scene_motif": "精品试衣空间的深木衣架与暖色墙面",
                "opening_action": "商品已挂在深木衣架上，画面先展示完整正面",
                "action_grammar": "完整正面建立→景别变化观察→整体收束",
                "visual_tone": "无人物的低干预商品记录",
            },
            {
                "persona_role": "人物不出镜的静物商品观察",
                "viewer_relationship": "像朋友替你把商品在自然光下看清",
                "scene_motif": "客厅窗边的浅色衣架与单色墙面",
                "opening_action": "商品已挂在衣架上，镜头从局部轮廓开始靠近",
                "action_grammar": "局部进入→同一承载上的角度变化→整体自然收束",
                "visual_tone": "固定机位静物观察",
            },
            {
                "persona_role": "人物不出镜的静物商品观察",
                "viewer_relationship": "替用户核对商品在日常空间里的真实轮廓",
                "scene_motif": "服装收纳架旁的自然光浅色墙面",
                "opening_action": "商品已在收纳架上展开，镜头从可见结构缓慢移到整体",
                "action_grammar": "结构露出→同一承载上的景别变化→保留整体尾帧",
                "visual_tone": "克制的静物手机记录",
            },
            {
                "persona_role": "人物不出镜的静物商品观察",
                "viewer_relationship": "像朋友分享自己刚挂好后看到的外观细节",
                "scene_motif": "玄关挂衣区的干净单色背景",
                "opening_action": "商品已挂在衣钩上，镜头从前部可见结构开始记录",
                "action_grammar": "正面局部→自然光下的连续观察→回到完整轮廓",
                "visual_tone": "旁观式静物记录",
            },
        ]
    if carrier == "HAND_ONLY":
        return [
            {
                "persona_role": "仅手部出镜的商品演示者",
                "viewer_relationship": "替用户在近距离把商品细节看清",
                "scene_motif": "浅色工作台自然光区域",
                "opening_action": "手将商品带入画面并停在关键结构附近",
                "action_grammar": "手部带入→局部观察→角度变化→放回或停住",
                "visual_tone": "固定机位真实演示",
            },
            {
                "persona_role": "仅手部出镜的商品记录者",
                "viewer_relationship": "像朋友展示刚拿到手时可核对的部分",
                "scene_motif": "日常收纳区的干净桌面",
                "opening_action": "手从画面边缘拿起商品并露出正面",
                "action_grammar": "拿起→局部翻看→角度核对→自然停住",
                "visual_tone": "低剪辑手部记录",
            },
        ]
    return [
        {
            "persona_role": "商品细节演示者",
            "viewer_relationship": "替用户展示一个可核对细节",
            "scene_motif": "浅色工作台自然光区域",
            "opening_action": "手将商品带入画面并停住",
            "action_grammar": "进入画面→局部证明→角度变化→静态收束",
            "visual_tone": "固定机位真实演示",
        },
        {
            "persona_role": "使用过程记录者",
            "viewer_relationship": "像朋友展示实际操作过程",
            "scene_motif": "日常收纳区的干净桌面",
            "opening_action": "从原位置拿起商品进入操作",
            "action_grammar": "拿起→操作→局部检查→放回或停住",
            "visual_tone": "低剪辑生活记录",
        },
        {
            "persona_role": "细节验收者",
            "viewer_relationship": "替用户把外观细节逐项看清",
            "scene_motif": "开放式置物架旁的自然光台面",
            "opening_action": "商品已局部展开，手指从关键结构移开",
            "action_grammar": "结构露出→手势撤离→角度核对→保留静止尾帧",
            "visual_tone": "近距离实物核对",
        },
        {
            "persona_role": "到货记录者",
            "viewer_relationship": "像朋友分享刚拆开后最先注意到的部分",
            "scene_motif": "玄关矮柜的干净台面",
            "opening_action": "从包装边缘取出商品并直接露出正面",
            "action_grammar": "取出→正面落定→局部翻看→回到整体",
            "visual_tone": "克制的手机到货记录",
        },
    ]


def _usage_signature(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _text(row.get("persona_role")),
        _text(row.get("scene_motif")),
        _text(row.get("opening_action")),
    )


_APPAREL_SURFACE_PROFILES = (
    {
        "silhouette_key": "FITTED_WIDE_TROUSER",
        "style_family": "RELAXED_COMMUTE",
        "hair_direction": "自然披发、耳后别发或短发，不默认低马尾",
        "base_outfit_direction": "合身或简洁内搭配有垂感的宽松长裤，普通平底鞋；重点形成上窄下松的轮廓",
    },
    {
        "silhouette_key": "DENIM_EVERYDAY",
        "style_family": "EVERYDAY_DENIM",
        "hair_direction": "松散盘发、抓夹或自然短发",
        "base_outfit_direction": "基础内搭配直筒或微喇牛仔下装，普通运动鞋或平底鞋；保留清楚的日常牛仔轮廓",
    },
    {
        "silhouette_key": "SKIRT_LAYERING",
        "style_family": "FEMININE_CASUAL",
        "hair_direction": "自然披发或简单半扎发",
        "base_outfit_direction": "简洁内搭配不过度精致的中长裙和平底鞋，目标商品作为外层形成裙装叠穿",
    },
    {
        "silhouette_key": "DRESS_LAYERING",
        "style_family": "ONE_PIECE_LAYERING",
        "hair_direction": "耳后别发、自然卷发或齐肩发",
        "base_outfit_direction": "日常连衣裙配目标商品作为外层，鞋履保持简单；不要改写成上衣加长裤",
    },
    {
        "silhouette_key": "TONAL_COLUMN",
        "style_family": "TONAL_MINIMAL",
        "hair_direction": "随手抓起的发型或自然披发",
        "base_outfit_direction": "上下装使用相近色阶形成纵向整体轮廓，单品保持日常简洁，不堆叠广告造型式配饰",
    },
)

_ACCESSORY_OUTFIT_PROFILES = {
    "scarf": (
        {
            "silhouette_key": "SCARF_PLAIN_NECKLINE",
            "style_family": "DAILY_NECK_ACCENT",
            "hair_direction": "自然披发放到肩后、耳后别发或简单束发，避免遮住商品主体",
            "base_outfit_direction": "轮廓清楚的纯色上衣配高腰长裤或利落半裙，肩颈与整体比例完整，不用空白基础款敷衍",
            "outer_layer_direction": "不强制外套；如有外层，以不遮挡商品为先",
            "neckline_direction": "领口与商品之间保留清楚边界，不展示复杂系法",
            "palette_relation": "基础服装使用能衬托商品现有颜色或图案的克制配色",
            "visibility_requirement": "至少一段清楚看到商品主体、佩戴位置与上半身搭配关系",
            "finish_direction": "真实日常造型已有清楚轮廓和配色层次，同时保留自然穿着质感",
            "supporting_elements": "可自然保留日常包、腕表或眼镜中一项，不遮挡商品",
            "grooming_direction": "妆发自然有气色，不做精修广告妆",
        },
        {
            "silhouette_key": "SCARF_SIMPLE_SHIRT",
            "style_family": "LIGHT_DAILY_LAYERING",
            "hair_direction": "自然短发、耳后别发或低存在感束发",
            "base_outfit_direction": "有版型的衬衫或轻薄上装配垂感长裤或中长半裙，不堆叠抢眼配饰",
            "outer_layer_direction": "可无外层或使用简洁开衫，不遮挡商品主体",
            "neckline_direction": "肩颈、领口与商品搭配状态完整可见",
            "palette_relation": "使用基础中性色或邻近色，避免凭空改写商品颜色",
            "visibility_requirement": "商品形状、边缘和半身穿搭关系稳定可见",
            "finish_direction": "轻通勤或城市休闲完成度，精致来自衣服轮廓和配色，不来自布光",
            "supporting_elements": "允许结构简洁的小包或腕表一项作为真实出门线索",
            "grooming_direction": "整理完成但保留碎发和皮肤纹理的自然妆发",
        },
        {
            "silhouette_key": "SCARF_TONAL_BASE",
            "style_family": "TONAL_DAILY_ACCENT",
            "hair_direction": "简单束发或自然披发放到肩后",
            "base_outfit_direction": "上下装保持低图案密度，以明暗层次或清楚腰线形成完整日常轮廓，让商品成为上半身重点",
            "outer_layer_direction": "外层保持简洁，不根据商品名称补充季节或材质效果",
            "neckline_direction": "商品与领口、肩部的层次清楚但不过度造型",
            "palette_relation": "以商品现有主色或图案为中心做自然协调，不要求完全同色",
            "visibility_requirement": "商品整体形状和上半身搭配关系至少有一段完整呈现",
            "finish_direction": "同色或邻近色有层次而不寡淡，保持真实衣料与自然褶皱",
            "supporting_elements": "可保留一个低冲突日常配饰，不机械凑齐",
            "grooming_direction": "自然有气色、像真实账号已经准备好出门的妆发",
        },
    ),
    "winter_scarf": (
        {
            "silhouette_key": "SCARF_KNIT_BASE",
            "style_family": "WINTER_DAILY_LAYERING",
            "hair_direction": "自然披发、耳后别发或简单束发，不遮住围巾主体",
            "base_outfit_direction": "有肌理或清楚版型的针织上衣配垂感长裤或中长半裙，保持肩颈和半身比例清楚",
            "outer_layer_direction": "基础外套或针织层，不根据类型名称补充保暖功效",
            "neckline_direction": "肩颈区域无遮挡，围巾与上装关系完整可见",
            "palette_relation": "与围巾主色或图案自然协调，不要求完全同色",
            "visibility_requirement": "至少一段清楚看到围巾与脖颈、肩部及上半身关系",
            "finish_direction": "秋冬日常造型完整但不过度层叠，人物像真实通勤或外出状态",
            "supporting_elements": "可使用一只日常包或手表补足生活状态，不添加抢眼颈部配饰",
            "grooming_direction": "自然妆发有气色，头发不遮挡围巾主体",
        },
        {
            "silhouette_key": "SCARF_SIMPLE_COAT",
            "style_family": "COMMUTE_OUTER_LAYER",
            "hair_direction": "自然短发、耳后别发或低存在感束发",
            "base_outfit_direction": "纯色合身上衣配高腰直筒下装，外层有清楚肩线和长度层次",
            "outer_layer_direction": "普通通勤外层，避免夸张大翻领遮住围巾",
            "neckline_direction": "围巾边缘和肩部垂落关系保持清楚",
            "palette_relation": "使用邻近色或基础中性色衬托商品，不增加复杂配饰",
            "visibility_requirement": "围巾主体、边缘与外层搭配同时可见",
            "finish_direction": "通勤外层造型有完整比例和配色关系，但保持真实穿着纹理",
            "supporting_elements": "允许日常通勤包或腕表一项，不堆叠围巾附近的装饰",
            "grooming_direction": "清爽自然妆发，不使用广告式精修",
        },
        {
            "silhouette_key": "SCARF_TURTLENECK_LAYER",
            "style_family": "TONAL_WINTER_BASE",
            "hair_direction": "简单束发或自然披发放到肩后",
            "base_outfit_direction": "有细微肌理的高领或圆领针织配垂感日常下装，用明暗层次完成造型，不做棚拍式层叠",
            "outer_layer_direction": "可无外套或使用简洁外层，以商品可见为先",
            "neckline_direction": "围巾与领口保持层次但不互相遮挡",
            "palette_relation": "基础服装色彩克制，让围巾图案或主色成为上半身重点",
            "visibility_requirement": "围巾与领口、肩部和整体上半身关系清楚",
            "finish_direction": "柔和秋冬造型不等于全身朴素，保留清楚轮廓、色阶与自然质感",
            "supporting_elements": "可加入一个低冲突的包或腕表作为真实外出线索",
            "grooming_direction": "妆发整理完成但自然，头发不遮挡商品",
        },
    ),
    "silk_scarf": (
        {
            "silhouette_key": "SILK_SCARF_CAMISOLE_WIDELEG",
            "style_family": "TH_WARM_CITY_FEMININE",
            "style_intensity": "FASHION_FORWARD",
            "climate_profile": "TH_WARM",
            "hair_direction": "自然披发放到肩后、耳后别发或松散束发，避免遮住丝巾主体",
            "base_outfit_direction": "合身纯色吊带或方领无袖上衣配高腰垂感阔腿裤，腰线和肩颈清楚，像真实创作者在暖天气已经搭好的出门造型",
            "outer_layer_direction": "不增加外套，保留轻盈的暖天气轮廓",
            "neckline_direction": "开放领口与丝巾之间保留清楚边界",
            "palette_relation": "基础服装用商品现有主色的邻近色、中性色或自然对比色，不把全身压成制服式同色",
            "visibility_requirement": "丝巾图案、边缘、领口和上半身搭配关系稳定可见",
            "finish_direction": "轻松但有造型的城市日常感，精致来自轮廓、配色与真实妆发",
            "supporting_elements": "可自然保留一只小号肩包或手表，不增加项链",
            "grooming_direction": "自然有气色的妆面与真实发丝，发型保持松弛日常，不做广告精修",
            "supported_demonstration_modes": ["NECK_WORN", "HEAD_WORN", "HAIR_TIE", "BAG_ACCENT"],
            "scene_families": ["CAFE_DINING", "STREET_OUTING", "HOME_ROUTINE"],
            "visibility_zones": ["NECK", "SHOULDER", "UPPER_BODY", "WAISTLINE"],
            "outfit_recipe": {
                "top": "合身纯色吊带或方领无袖上衣",
                "bottom": "高腰垂感阔腿裤",
                "footwear": "简洁凉鞋或日常平底鞋",
                "bag": "小号肩包",
                "other_accessories": "不增加项链，其他配饰保持低存在感",
            },
        },
        {
            "silhouette_key": "SILK_SCARF_CAMISOLE_SHORTS",
            "style_family": "TH_WARM_CASUAL_CHIC",
            "style_intensity": "FASHION_FORWARD",
            "climate_profile": "TH_WARM",
            "hair_direction": "齐肩发、自然披发或简单半扎，脸侧与颈部保持清楚",
            "base_outfit_direction": "合身吊带或简洁无袖上衣配高腰利落短裤，保留真实暖天气出门穿搭的轻盈比例",
            "outer_layer_direction": "不强制外层；如现场需要，只允许不遮挡丝巾的轻薄敞开层",
            "neckline_direction": "肩颈和领口完整露出，不增加项链",
            "palette_relation": "用一组低图案密度但有明暗层次的颜色衬托丝巾",
            "visibility_requirement": "丝巾、领口、腰线与完整半身穿搭至少有一段同时成立",
            "finish_direction": "像去咖啡厅、逛街或见朋友前已经搭好的轻盈暖天气造型",
            "supporting_elements": "小号腋下包、腕表或日常耳钉中至多一项",
            "grooming_direction": "自然底妆、清楚眉眼和有气色唇色，保留真实皮肤纹理",
            "supported_demonstration_modes": ["NECK_WORN", "HEAD_WORN", "HAIR_TIE", "BAG_ACCENT"],
            "scene_families": ["CAFE_DINING", "STREET_OUTING", "VANITY_TRYON"],
            "visibility_zones": ["NECK", "SHOULDER", "UPPER_BODY", "WAISTLINE"],
            "outfit_recipe": {
                "top": "合身吊带或简洁无袖上衣",
                "bottom": "高腰利落短裤",
                "footwear": "凉鞋、平底鞋或普通运动鞋",
                "bag": "小号腋下包",
                "other_accessories": "无项链，至多一项低存在感配饰",
            },
        },
        {
            "silhouette_key": "SILK_SCARF_SLEEVELESS_JUMPSUIT",
            "style_family": "RESORT_CITY_CHIC",
            "style_intensity": "FASHION_FORWARD",
            "climate_profile": "TH_WARM",
            "hair_direction": "自然短发、蓬松披发或松散束发，头发不压住商品",
            "base_outfit_direction": "纯色吊带式或无袖阔腿连体裤形成干净纵向轮廓，丝巾作为上半身唯一图案重点",
            "outer_layer_direction": "无外层，保持连体裤完整轮廓",
            "neckline_direction": "领口清楚，不增加颈部装饰",
            "palette_relation": "连体裤使用与商品自然协调的纯色，不要求同色",
            "visibility_requirement": "丝巾与脸部、领口和整套纵向轮廓关系清楚",
            "finish_direction": "度假感与城市日常之间的真实穿搭，不做礼服化或商业大片化处理",
            "supporting_elements": "允许一只日常小包和简单鞋履，不堆叠首饰",
            "grooming_direction": "轻松但整理完成的头发与自然妆面",
            "supported_demonstration_modes": ["NECK_WORN", "HEAD_WORN", "HAIR_TIE", "BAG_ACCENT"],
            "scene_families": ["CAFE_DINING", "STREET_OUTING", "HOME_ROUTINE"],
            "visibility_zones": ["NECK", "UPPER_BODY", "FULL_SILHOUETTE"],
            "outfit_recipe": {
                "top": "纯色吊带式或无袖连体上身",
                "bottom": "同一件阔腿连体裤下身",
                "footwear": "简洁凉鞋或平底鞋",
                "bag": "日常小包",
                "other_accessories": "不叠加颈部首饰",
            },
        },
        {
            "silhouette_key": "SILK_SCARF_SQUARE_NECK_DENIM",
            "style_family": "EVERYDAY_DENIM_FEMININE",
            "style_intensity": "DAILY_STYLED",
            "climate_profile": "TH_WARM",
            "hair_direction": "自然披发放到肩后或简单半扎，避免遮住丝巾",
            "base_outfit_direction": "合身方领无袖上衣配直筒牛仔裤或牛仔短裤，保持清楚腰线与轻松日常比例",
            "outer_layer_direction": "不强制外层",
            "neckline_direction": "方领与丝巾之间有清楚留白",
            "palette_relation": "牛仔色与纯色上衣衬托商品现有图案，不增加第二种抢眼图案",
            "visibility_requirement": "商品主体、开放领口和半身牛仔轮廓清楚可见",
            "finish_direction": "真实日常账号可直接穿出门的轻松造型",
            "supporting_elements": "可搭普通帆布包或小肩包，不增加项链",
            "grooming_direction": "自然有气色的日常妆发",
            "supported_demonstration_modes": ["NECK_WORN", "HEAD_WORN", "HAIR_TIE", "BAG_ACCENT"],
            "scene_families": ["HOME_ROUTINE", "STREET_OUTING", "CAFE_DINING"],
            "visibility_zones": ["NECK", "SHOULDER", "UPPER_BODY", "WAISTLINE"],
            "outfit_recipe": {
                "top": "合身方领无袖上衣",
                "bottom": "直筒牛仔裤或牛仔短裤",
                "footwear": "普通运动鞋或平底鞋",
                "bag": "帆布包或小肩包",
                "other_accessories": "无项链",
            },
        },
        {
            "silhouette_key": "SILK_SCARF_LIGHT_COMMUTE",
            "style_family": "LIGHT_COMMUTE_ACCENT",
            "style_intensity": "DAILY",
            "climate_profile": "TH_WARM",
            "hair_direction": "耳后别发、自然短发或松散束发，保持日常发丝质感",
            "base_outfit_direction": "轻薄短袖衬衫或利落无袖上衣配高腰直筒裤，保持轻通勤但不使用成套制服式半裙",
            "outer_layer_direction": "不强制外套",
            "neckline_direction": "丝巾与领口关系完整可见",
            "palette_relation": "用商品主色的邻近色或自然中性色建立层次",
            "visibility_requirement": "丝巾图案、领口和半身关系稳定可见",
            "finish_direction": "保留一套轻通勤对照，以短袖、长裤和自然妆发保持轻松城市感",
            "supporting_elements": "允许结构简洁的通勤包或腕表一项",
            "grooming_direction": "自然底妆、清楚眉眼与真实发丝",
            "supported_demonstration_modes": ["NECK_WORN", "HEAD_WORN", "HAIR_TIE", "BAG_ACCENT"],
            "scene_families": ["OFFICE_WORKBREAK", "CAFE_DINING"],
            "visibility_zones": ["NECK", "SHOULDER", "UPPER_BODY"],
            "outfit_recipe": {
                "top": "轻薄短袖衬衫或利落无袖上衣",
                "bottom": "高腰直筒裤",
                "footwear": "普通通勤平底鞋",
                "bag": "结构简洁的通勤包",
                "other_accessories": "腕表或无其他配饰",
            },
        },
    ),
    "headscarf": (
        {
            "silhouette_key": "HEADSCARF_Y2K_BOLD",
            "style_family": "Y2K_BOLD_FEMININE",
            "style_intensity": "FASHION_FORWARD",
            "climate_profile": "TH_WARM",
            "hair_direction": "明确已经完成的日常发型或覆盖状态，不展示包裹过程",
            "base_outfit_direction": "合身吊带或短款上衣配中腰宽松工装裤或阔腿牛仔裤，用清楚腰线和上窄下松轮廓形成轻辣妹、Y2K日常感",
            "outer_layer_direction": "不强制外层，不增加帽子或厚重外套",
            "neckline_direction": "头肩和上半身轮廓清楚，服务头巾成为造型重点",
            "palette_relation": "服装使用纯色、牛仔色或低图案密度配色衬托头巾，不根据场景推断身份",
            "visibility_requirement": "头巾位置、轮廓、头发状态、上半身和腰线关系清楚可见",
            "finish_direction": "真实社交账号已经搭好的轻辣妹/Y2K出门造型，保留手机原生质感，不做棚拍",
            "supporting_elements": "可用小号腋下包或细框眼镜一项，不叠加帽子和夸张耳饰",
            "grooming_direction": "清楚眉眼与有气色唇色，妆面自然但不寡淡，保留真实皮肤纹理",
            "supported_demonstration_modes": ["HEAD_WORN"],
            "scene_families": ["VANITY_TRYON", "CAFE_DINING", "STREET_OUTING"],
            "visibility_zones": ["HEAD", "HAIR", "UPPER_BODY", "WAISTLINE"],
            "outfit_recipe": {
                "top": "合身吊带或短款上衣",
                "bottom": "中腰宽松工装裤或阔腿牛仔裤",
                "footwear": "日常厚底运动鞋",
                "bag": "小号腋下包",
                "other_accessories": "不增加帽子和夸张耳饰",
            },
        },
        {
            "silhouette_key": "HEADSCARF_STREET_FEMININE",
            "style_family": "STREET_FEMININE",
            "style_intensity": "FASHION_FORWARD",
            "climate_profile": "TH_WARM",
            "hair_direction": "头巾已经佩戴完成，发际或覆盖边界按当前设计保持稳定",
            "base_outfit_direction": "合身无袖短上衣配牛仔短裤或简洁工装短裙，保持真实街头日常比例，不做舞台造型",
            "outer_layer_direction": "无外层或只保留不遮挡头肩关系的轻薄敞开层",
            "neckline_direction": "头肩轮廓完整，让头巾与脸部关系清楚",
            "palette_relation": "用牛仔色和一件纯色上衣衬托头巾现有图案",
            "visibility_requirement": "正面或轻侧面能看清头巾轮廓、头发状态和整套街头穿搭",
            "finish_direction": "像逛街或见朋友时随手拍的真实街头女性造型",
            "supporting_elements": "可保留小肩包或腕表一项，避免帽子与夸张耳饰",
            "grooming_direction": "自然底妆、清楚眉眼与有气色唇色，保留发丝和皮肤纹理",
            "supported_demonstration_modes": ["HEAD_WORN"],
            "scene_families": ["STREET_OUTING", "CAFE_DINING", "VANITY_TRYON"],
            "visibility_zones": ["HEAD", "HAIR", "UPPER_BODY", "WAISTLINE"],
            "outfit_recipe": {
                "top": "合身无袖短上衣",
                "bottom": "牛仔短裤或简洁工装短裙",
                "footwear": "普通运动鞋或日常短靴",
                "bag": "小号肩包",
                "other_accessories": "无帽子、无夸张耳饰",
            },
        },
        {
            "silhouette_key": "HEADSCARF_RESORT_CHIC",
            "style_family": "RESORT_CHIC",
            "style_intensity": "FASHION_FORWARD",
            "climate_profile": "TH_WARM",
            "hair_direction": "已完成头部造型，从稳定结果开始拍摄",
            "base_outfit_direction": "挂脖或合身吊带上衣配垂感阔腿裤或轻盈长裙，形成真实暖天气度假与城市休闲之间的完整轮廓",
            "outer_layer_direction": "不增加遮挡头肩关系的外层",
            "neckline_direction": "上半身线条简洁，头巾成为头部重点",
            "palette_relation": "用纯色服装衬托头巾现有主色或图案，不要求完全同色",
            "visibility_requirement": "头巾、头发状态、肩颈与整套纵向轮廓稳定可见",
            "finish_direction": "具有时尚感但仍像真实旅行或周末出门穿搭，不做杂志大片",
            "supporting_elements": "可使用藤编小包或日常肩包一项，不堆叠头部配饰",
            "grooming_direction": "有气色的自然妆面，头部造型整理完成但保留真实发丝",
            "supported_demonstration_modes": ["HEAD_WORN"],
            "scene_families": ["STREET_OUTING", "CAFE_DINING", "HOME_ROUTINE"],
            "visibility_zones": ["HEAD", "HAIR", "UPPER_BODY", "FULL_SILHOUETTE"],
            "outfit_recipe": {
                "top": "挂脖或合身吊带上衣",
                "bottom": "垂感阔腿裤或轻盈长裙",
                "footwear": "简洁凉鞋",
                "bag": "小号度假感手袋或日常肩包",
                "other_accessories": "不增加帽子和头部配饰",
            },
        },
        {
            "silhouette_key": "HEADSCARF_CITY_MINIMAL",
            "style_family": "CITY_MINIMAL_HEAD_STYLE",
            "style_intensity": "DAILY_STYLED",
            "climate_profile": "TH_WARM",
            "hair_direction": "头巾已经佩戴完成，头发边界保持自然稳定",
            "base_outfit_direction": "合身纯色无袖或短袖上衣配垂感宽松长裤，用清楚腰线与轻松比例完成城市日常造型",
            "outer_layer_direction": "不强制外层，避免帽子和抢眼肩部装饰",
            "neckline_direction": "头肩比例与上半身线条清楚",
            "palette_relation": "用基础色和明暗层次衬托商品图案，不把全身压成灰暗素装",
            "visibility_requirement": "头巾整体形状、覆盖位置与半身穿搭关系稳定可见",
            "finish_direction": "相对克制的城市日常方向，但妆发、腰线和配色已经完整",
            "supporting_elements": "可加入小号肩包或腕表一项",
            "grooming_direction": "自然底妆、清楚眉眼与适度唇色",
            "supported_demonstration_modes": ["HEAD_WORN"],
            "scene_families": ["CAFE_DINING", "OFFICE_WORKBREAK", "HOME_ROUTINE"],
            "visibility_zones": ["HEAD", "HAIR", "UPPER_BODY", "WAISTLINE"],
            "outfit_recipe": {
                "top": "合身纯色无袖或短袖上衣",
                "bottom": "垂感宽松长裤",
                "footwear": "普通平底鞋或运动鞋",
                "bag": "小号肩包",
                "other_accessories": "腕表或无其他配饰",
            },
        },
    ),
}

# Wrist and hair accessories reuse the same outfit-selection authority as
# apparel/scarves.  These are soft internal fallbacks: structured Feishu
# templates with the same roles still take priority when operators add them.
_ACCESSORY_OUTFIT_PROFILES.update({
    canonical: (
        {
            "silhouette_key": "WRIST_SLEEVE_BALANCE",
            "style_family": "DAILY_WRIST_ACCENT",
            "style_intensity": "DAILY_STYLED",
            "climate_profile": "TH_WARM",
            "hair_direction": "自然披发、耳后别发或简单束发，保持真实日常妆发",
            "base_outfit_direction": "有清楚轮廓的纯色无袖、短袖或袖口不过腕的上衣，配高腰长裤或利落半裙；腕部自然露出，不用素色空壳造型敷衍",
            "outer_layer_direction": "如有外层，袖口不得遮住目标腕饰",
            "neckline_direction": "领口保持日常简洁，不与腕部商品争抢视觉重点",
            "palette_relation": "服装使用能衬托商品现有金属色、颜色或结构的中性色、邻近色或自然对比色，避免商品与袖口融成一片",
            "visibility_requirement": "腕饰、手腕、前臂和至少一段上半身穿搭关系清楚可见",
            "finish_direction": "像真实创作者已经搭好后顺手分享的城市日常造型，精致来自轮廓和配色，不来自商业布光",
            "supporting_elements": "同一手腕不叠戴手表或竞争性腕饰；可保留日常包或普通手机",
            "grooming_direction": "自然有气色、保留真实皮肤和发丝，不使用首饰广告式精修",
            "target_role": "SUPPORTING_OUTFIT_WRIST",
            "supported_target_roles": ["SUPPORTING_OUTFIT_WRIST"],
            "supported_demonstration_modes": ["WRIST_WORN"],
            "scene_families": ["HOME_ROUTINE", "CAFE_DINING", "OFFICE_WORKBREAK", "STREET_OUTING"],
            "visibility_zones": ["WRIST", "FOREARM", "UPPER_BODY"],
            "outfit_recipe": {
                "top": "纯色无袖、短袖或袖口不过腕的上衣",
                "bottom": "高腰直筒长裤或利落半裙",
                "footwear": "普通平底鞋、凉鞋或运动鞋",
                "bag": "日常小包",
                "other_accessories": "目标手腕不叠戴手表或其他腕饰",
            },
        },
        {
            "silhouette_key": "WRIST_CITY_CASUAL",
            "style_family": "CITY_WRIST_DETAIL",
            "style_intensity": "DAILY",
            "climate_profile": "TH_WARM",
            "hair_direction": "自然短发、松散束发或披发，不做广告式造型",
            "base_outfit_direction": "简洁背心或合身短袖配牛仔裤、阔腿裤或日常短裤，保留真实暖天气比例和清楚腕部",
            "outer_layer_direction": "不强制外层；如有薄衬衫，袖口自然卷到前臂且不遮挡商品",
            "neckline_direction": "不堆叠抢眼首饰",
            "palette_relation": "基础服装不与商品完全同色，保持腕饰能从袖口和背景中自然分离",
            "visibility_requirement": "至少一段日常动作中仍能看清目标腕饰整体轮廓与佩戴比例",
            "finish_direction": "周末、咖啡或出门前的普通个人账号状态，穿搭完整但不摆拍",
            "supporting_elements": "可用杯子、包带、书页中一项承接自然腕部动作，不强制出现",
            "grooming_direction": "自然底妆和真实肤质，不强化滤镜",
            "target_role": "SUPPORTING_OUTFIT_WRIST",
            "supported_target_roles": ["SUPPORTING_OUTFIT_WRIST"],
            "supported_demonstration_modes": ["WRIST_WORN"],
            "scene_families": ["CAFE_DINING", "HOME_ROUTINE", "STREET_OUTING"],
            "visibility_zones": ["WRIST", "FOREARM", "UPPER_BODY"],
            "outfit_recipe": {
                "top": "简洁背心、合身短袖或卷袖轻衬衫",
                "bottom": "牛仔裤、阔腿裤或日常短裤",
                "footwear": "普通凉鞋、平底鞋或运动鞋",
                "bag": "日常小包",
                "other_accessories": "目标腕部无竞争性首饰",
            },
        },
    )
    for canonical in ("bracelet", "bangle", "slim_bangle")
})

_ACCESSORY_OUTFIT_PROFILES.update({
    canonical: (
        {
            "silhouette_key": "HAIR_RESULT_CLEAN_SHOULDER",
            "style_family": "DAILY_HAIR_RESULT",
            "style_intensity": "DAILY_STYLED",
            "climate_profile": "TH_WARM",
            "hair_direction": "商品已经固定在与类型匹配的日常发型中，从完成结果开始；发束边界清楚，不反复夹发",
            "base_outfit_direction": "有清楚肩颈轮廓的纯色吊带、方领无袖或简洁短袖上衣，配高腰日常下装，让侧后方发型与上半身穿搭同时成立",
            "outer_layer_direction": "不增加遮挡肩颈与后脑关系的帽子、头巾或厚重外层",
            "neckline_direction": "领口简洁，肩颈线条清楚，不堆叠夸张耳饰",
            "palette_relation": "上衣和背景与商品现有颜色保持自然明暗或冷暖分离，不把发饰融进头发和墙面",
            "visibility_requirement": "发饰位置、相对大小、发束固定关系、肩颈和上半身穿搭至少有一段同时清楚",
            "finish_direction": "像真实创作者出门前已经整理好的发型与穿搭，保留碎发和皮肤纹理，不做美发广告精修",
            "supporting_elements": "可保留日常小包或普通耳钉一项，不增加竞争性头部配饰",
            "grooming_direction": "自然有气色，保留真实发丝、发际线和皮肤纹理，避免重滤镜改变五官",
            "target_role": "SUPPORTING_OUTFIT_HAIR",
            "supported_target_roles": ["SUPPORTING_OUTFIT_HAIR"],
            "supported_demonstration_modes": ["HAIR_WORN"],
            "scene_families": ["VANITY_TRYON", "HOME_ROUTINE", "CAFE_DINING", "STREET_OUTING"],
            "visibility_zones": ["HEAD", "HAIR", "SHOULDER", "UPPER_BODY"],
            "outfit_recipe": {
                "top": "纯色吊带、方领无袖或简洁短袖上衣",
                "bottom": "高腰牛仔裤、阔腿裤或日常短裤",
                "footwear": "普通凉鞋、平底鞋或运动鞋",
                "bag": "日常小包",
                "other_accessories": "无帽子、头巾和竞争性发饰",
            },
        },
        {
            "silhouette_key": "HAIR_CITY_FEMININE",
            "style_family": "CITY_HAIR_ACCENT",
            "style_intensity": "FASHION_FORWARD",
            "climate_profile": "TH_WARM",
            "hair_direction": "从已完成的半扎、低束或盘发结果开始，具体发型服从商品类型和参考图，不展示复杂造型过程",
            "base_outfit_direction": "合身短款上衣或利落无袖上衣配宽松长裤、牛仔短裤或简洁半裙，形成真实城市女性的完整轮廓",
            "outer_layer_direction": "可无外层或使用不遮挡头肩关系的轻薄敞开层",
            "neckline_direction": "头肩与领口关系完整，避免夸张耳饰抢走商品重点",
            "palette_relation": "用一组有明暗层次但不过度同色的服装衬托商品，头发、发饰和背景三者必须可分辨",
            "visibility_requirement": "侧后方或镜面视角能看清商品与发型结果，同时保留至少半身穿搭语境",
            "finish_direction": "时尚感来自真实妆发、轮廓和配色，仍像个人账号手机记录，不变成沙龙广告大片",
            "supporting_elements": "允许小号肩包或普通耳钉一项，不叠加帽子和头巾",
            "grooming_direction": "眉眼和唇色有气色但不过度精修，真实发丝与发际线必须保留",
            "target_role": "SUPPORTING_OUTFIT_HAIR",
            "supported_target_roles": ["SUPPORTING_OUTFIT_HAIR"],
            "supported_demonstration_modes": ["HAIR_WORN"],
            "scene_families": ["VANITY_TRYON", "CAFE_DINING", "STREET_OUTING"],
            "visibility_zones": ["HEAD", "HAIR", "SHOULDER", "UPPER_BODY", "WAISTLINE"],
            "outfit_recipe": {
                "top": "合身短款上衣或利落无袖上衣",
                "bottom": "宽松长裤、牛仔短裤或简洁半裙",
                "footwear": "日常运动鞋、平底鞋或短靴",
                "bag": "小号肩包",
                "other_accessories": "无帽子、头巾和竞争性发饰",
            },
        },
    )
    for canonical in (
        "hair_accessory_generic", "claw_clip", "hair_clip", "headband",
        "scrunchie", "hair_tie", "ribbon", "hair_pin",
    )
})

_ACCESSORY_OUTFIT_PROFILES["ring"] = (
    {
        "silhouette_key": "RING_CLEAN_HAND_CONTEXT",
        "style_family": "DAILY_RING_ACCENT",
        "style_intensity": "DAILY_STYLED",
        "climate_profile": "TH_WARM",
        "hair_direction": "自然日常妆发；手部是主要商品证明，上半身只提供真实人物关系",
        "base_outfit_direction": "简洁纯色无袖、短袖或袖口不过腕的日常上衣，手部与前臂无遮挡",
        "outer_layer_direction": "不使用遮挡手部和前臂的宽大袖口",
        "neckline_direction": "领口简洁，不叠加抢主体的项链或胸前装饰",
        "palette_relation": "上衣和背景与戒指现有金属色保持自然明暗分离，不做珠宝广告式黑棚",
        "visibility_requirement": "戒指、手指、手部和必要时的简洁上半身关系清楚；指甲干净自然",
        "finish_direction": "像个人账号在普通生活场景分享当天手部小配饰，不使用精修珠宝大片造型",
        "supporting_elements": "不叠戴竞争性戒指、手链或腕表；生活道具最多自然出现一项",
        "grooming_direction": "手部和指甲干净自然，保留真实肤质，不使用过度磨皮与高反商业布光",
        "target_role": "SUPPORTING_OUTFIT_HAND",
        "supported_target_roles": ["SUPPORTING_OUTFIT_HAND"],
        "supported_demonstration_modes": ["FINGER_WORN"],
        "scene_families": ["HOME_ROUTINE", "CAFE_DINING", "OFFICE_WORKBREAK"],
        "visibility_zones": ["FINGER", "HAND", "FOREARM", "UPPER_BODY"],
        "outfit_recipe": {
            "top": "简洁纯色无袖、短袖或袖口不过腕的日常上衣",
            "bottom": "不要求入镜；需要上半身关系时保持普通日常下装",
            "footwear": "不要求入镜",
            "bag": "普通日常小包或无包",
            "other_accessories": "无竞争性戒指、手链和腕表",
        },
    },
)


def _usage_metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    metadata = row.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    raw = row.get("metadata_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _usage_outfit_silhouette_key(row: Dict[str, Any]) -> str:
    direct = row.get("outfit_selection_contract")
    if isinstance(direct, dict):
        key = _text(direct.get("silhouette_key"))
        if key:
            return key
    surface = row.get("surface_profile")
    if isinstance(surface, dict):
        key = _text(surface.get("silhouette_key") or surface.get("surface_profile_key"))
        if key:
            return key
    metadata = _usage_metadata(row)
    nested = metadata.get("outfit_selection_contract")
    if isinstance(nested, dict):
        key = _text(nested.get("silhouette_key"))
        if key:
            return key
    legacy = metadata.get("surface_profile")
    if isinstance(legacy, dict):
        return _text(legacy.get("silhouette_key") or legacy.get("surface_profile_key"))
    return ""


def _usage_outfit_selection_key(row: Dict[str, Any]) -> str:
    direct = row.get("outfit_selection_contract")
    if not isinstance(direct, dict):
        metadata = _usage_metadata(row)
        direct = metadata.get("outfit_selection_contract")
    if isinstance(direct, dict):
        source = _text(direct.get("source_type")) or "INTERNAL_PROFILE"
        template_id = _text(direct.get("template_id"))
        silhouette = _text(direct.get("silhouette_key"))
        if template_id:
            return f"{source}:{template_id}"
        if silhouette:
            return f"{source}:{silhouette}"
    silhouette = _usage_outfit_silhouette_key(row)
    return f"INTERNAL_PROFILE:{silhouette}" if silhouette else ""


def _perceptual_repeat_penalty_enabled() -> bool:
    value = _text(
        os.environ.get("ORIGINAL_SCRIPT_PERCEPTUAL_REPEAT_PENALTY_V1", "1")
    ).lower()
    return value not in {"0", "false", "off", "no"}


def _perceptual_action_family(value: Any) -> str:
    """Collapse wording variants into one viewer-perceived action family."""

    text = _text(value).lower()
    families = (
        ("HAND_PRODUCT", ("手部", "拿起", "取出", "展开", "翻看", "放回", "托住")),
        ("TRANSIT_WAIT", ("电梯", "等候", "来车", "站台")),
        ("TRANSIT_WALK", ("沿", "走向", "前行", "经过", "离开", "出口", "门口")),
        ("DETAIL_RESULT", ("局部", "细节", "角度", "侧面", "近看", "观察", "核对")),
        ("LIFE_OBJECT", ("拿包", "随身包", "钥匙", "翻页", "拿杯", "笔记本")),
        ("STATIC_RESULT", ("静态", "静止", "落定", "整体收束", "完整轮廓")),
    )
    for family, markers in families:
        if any(marker in text for marker in markers):
            return family
    return "OTHER_ACTION"


def _perceptual_signature(
    *,
    product_code: str,
    item: Mapping[str, Any],
    outfit_contract: Mapping[str, Any],
    direction_carrier: str,
    structure_family: str,
) -> str:
    """Return a compact, deterministic cross-batch perception key.

    Product colour, persona name, hook wording and free-form scene prose are
    intentionally excluded.  The key catches the same viewer experience even
    when surface text changes, while remaining a soft ranking signal only.
    """

    del product_code  # product scope is applied by the history filter.
    return "|".join((
        _text(item.get("scene_family_key")) or "GENERIC_INDOOR",
        _perceptual_action_family(item.get("action_grammar")),
        _text(structure_family) or "UNAVAILABLE_STRUCTURE",
        _text(outfit_contract.get("silhouette_key")) or "PRODUCT_LED",
        _text(direction_carrier) or "UNAVAILABLE_CARRIER",
    ))


def _usage_perceptual_signature(row: Mapping[str, Any]) -> str:
    direct = _text(row.get("perceptual_signature"))
    if direct:
        return direct
    return _text(_usage_metadata(dict(row)).get("perceptual_signature"))


def _select_outfit_contract(
    *,
    seed: int,
    recent_usage: Sequence[Dict[str, Any]],
    product_code: str,
    product_type: str,
    top_category: str,
    product_profile: str,
    direction_carrier: str,
    country: str,
    scene_family: str,
    direction: Dict[str, Any],
) -> Tuple[Dict[str, Any], int, int]:
    is_wearer_apparel = (
        product_profile == "WORN_APPAREL"
        and direction_carrier in {"WEARER_ACTIVE", "MIXED", "UNAVAILABLE"}
    )
    canonical_type = normalize_product_type(
        product_type, top_category
    ).canonical_type
    demonstration_mode = demonstration_mode_from_direction(
        direction, canonical_type
    )
    target_role = target_role_for(
        product_profile=product_profile,
        canonical_type=canonical_type,
        demonstration_mode=demonstration_mode,
    )
    is_wearer_accessory = (
        canonical_type in _ACCESSORY_OUTFIT_PROFILES
        and direction_carrier in {"WEARER_ACTIVE", "MIXED", "UNAVAILABLE"}
    )
    if not (is_wearer_apparel or is_wearer_accessory):
        return ({
            "contract_version": OUTFIT_SELECTION_CONTRACT_VERSION,
            "source_type": "INTERNAL_PROFILE",
            "template_id": None,
            "template_version": None,
            "silhouette_key": "PRODUCT_LED",
            "style_family": "PRODUCT_LED",
            "style_intensity": "DAILY",
            "target_role": target_role,
            "product_type": canonical_type,
            "demonstration_mode": demonstration_mode,
            "visibility_zones": [],
            "outfit_recipe": {},
            "hair_direction": "不适用",
            "base_outfit_direction": "不适用",
            "palette_relation": "不适用",
            "selection_policy": "PRODUCT_PRESENTATION_ONLY",
            "hard_required": False,
        }, 0, 0)

    candidates: List[Dict[str, Any]] = []
    if is_wearer_apparel or is_wearer_accessory:
        for template in load_structured_outfit_templates(
            product_code=product_code,
            product_type=product_type,
        ):
            candidates.append({
                **template,
                "contract_version": OUTFIT_SELECTION_CONTRACT_VERSION,
                "palette_relation": _text(template.get("palette_relation")),
                "selection_policy": "EXPLICIT_PRODUCT_CODE_THEN_LEAST_USED",
                "hard_required": False,
                "source_preference": int(template.get("match_rank") or 0),
            })
    if is_wearer_apparel:
        internal_profiles = _APPAREL_SURFACE_PROFILES
    else:
        internal_profiles = _ACCESSORY_OUTFIT_PROFILES[canonical_type]
    for profile in internal_profiles:
        candidates.append({
            "contract_version": OUTFIT_SELECTION_CONTRACT_VERSION,
            "source_type": "INTERNAL_PROFILE",
            "template_id": None,
            "template_version": None,
            "match_scope": "INTERNAL_FALLBACK",
            "silhouette_key": _text(profile.get("silhouette_key")),
            "style_family": _text(profile.get("style_family")),
            "hair_direction": _text(profile.get("hair_direction")),
            "base_outfit_direction": _text(profile.get("base_outfit_direction")),
            "outer_layer_direction": _text(profile.get("outer_layer_direction")),
            "neckline_direction": _text(profile.get("neckline_direction")),
            "palette_relation": (
                _text(profile.get("palette_relation"))
                or "与目标商品自然协调，不要求完全同色，也不只靠换颜色制造差异"
            ),
            "visibility_requirement": _text(
                profile.get("visibility_requirement")
            ),
            "finish_direction": _text(profile.get("finish_direction")),
            "supporting_elements": _text(profile.get("supporting_elements")),
            "grooming_direction": _text(profile.get("grooming_direction")),
            "target_role": _text(profile.get("target_role") or target_role),
            "supported_target_roles": list(
                profile.get("supported_target_roles") or [target_role]
            ),
            "style_intensity": _text(
                profile.get("style_intensity") or "DAILY"
            ),
            "climate_profile": _text(profile.get("climate_profile")),
            "supported_demonstration_modes": list(
                profile.get("supported_demonstration_modes") or []
            ),
            "scene_families": list(profile.get("scene_families") or []),
            "visibility_zones": list(profile.get("visibility_zones") or []),
            "outfit_recipe": dict(profile.get("outfit_recipe") or {}),
            "selection_policy": "LEAST_RECENTLY_USED_SOFT",
            "hard_required": False,
            "source_preference": 2,
            "style_preference_rank": int(
                profile.get("style_preference_rank") or 0
            ),
        })
    normalized_candidates = [
        normalize_outfit_candidate(
            candidate,
            target_role=target_role,
            canonical_type=canonical_type,
            country=country,
            scene_family=scene_family,
            demonstration_mode=demonstration_mode,
        )
        for candidate in candidates
    ]
    return select_outfit_candidate(
        candidates=normalized_candidates,
        recent_usage=recent_usage,
        seed=seed,
        target_role=target_role,
        demonstration_mode=demonstration_mode,
        scene_family=scene_family,
        product_colors=(direction.get("outfit_variant_context") or {}).get("product_colors"),
    )


def _outfit_scene_affinity_contract(
    outfit_contract: Mapping[str, Any], scene_family: Any
) -> Dict[str, Any]:
    """Describe one outfit/scene pairing without creating a hard gate."""

    selected_family = _text(scene_family).upper() or "GENERIC_INDOOR"
    preferred = list(dict.fromkeys(
        _text(item).upper()
        for item in outfit_contract.get("scene_families") or []
        if _text(item)
    ))
    if not preferred:
        status = "NO_PREFERENCE"
        bonus = 0
        fallback_reason = ""
    elif selected_family in preferred:
        status = "MATCHED"
        bonus = (
            EXACT_PRODUCT_OUTFIT_SCENE_MATCH_BONUS
            if _text(outfit_contract.get("source_tier")) == "EXACT_PRODUCT_TEMPLATE"
            or _text(outfit_contract.get("match_scope")) == "EXACT_PRODUCT_CODE"
            else OUTFIT_SCENE_MATCH_BONUS
        )
        fallback_reason = ""
    else:
        status = "FALLBACK"
        bonus = 0
        fallback_reason = "NO_COMPATIBLE_PREFERRED_SCENE_SELECTED"
    return {
        "policy_version": OUTFIT_SCENE_AFFINITY_POLICY_VERSION,
        "template_id": _text(outfit_contract.get("template_id")),
        "template_display_name": _text(
            outfit_contract.get("template_display_name")
        ),
        "template_version": _text(outfit_contract.get("template_version")),
        "outfit_source_type": _text(outfit_contract.get("source_type")),
        "preferred_scene_families": preferred,
        "selected_scene_family": selected_family,
        "match_status": status,
        "ranking_bonus": bonus,
        "fallback_reason": fallback_reason,
        "authority": "SOFT_PREFERENCE",
        "hard_required": False,
    }


def build_creative_diversity_contract(
    *,
    product_code: str,
    country: str,
    category: str,
    product_type: str,
    direction: Dict[str, Any],
    recent_usage: Sequence[Dict[str, Any]],
    scene_reference_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Choose a positive creative combination before the model writes prose."""

    product_profile = creative_product_profile(product_type, category)
    direction_carrier = authoritative_carrier(direction)
    structure = direction.get("structure_execution_plan") if isinstance(direction.get("structure_execution_plan"), dict) else {}
    structure_contract = (
        direction.get("structure_contract")
        if isinstance(direction.get("structure_contract"), dict)
        else {}
    )
    structure_family = _text(
        structure.get("macro_family_key")
        or (structure_contract.get("direction_identity") or {}).get(
            "macro_family_key"
        )
        or (structure_contract.get("hard_constraints") or {}).get(
            "macro_family_key"
        )
    )
    candidates = [
        {
            **candidate,
            # This labels only the controlled internal candidate pool.  It
            # does not infer a scene from seller copy or add a new constraint.
            "scene_family_key": scene_family_for_motif(candidate.get("scene_motif")),
        }
        for candidate in _creative_combinations(direction, product_type, category)
    ]
    exact_counts = Counter(_usage_signature(row) for row in recent_usage)
    scene_counts = Counter(_text(row.get("scene_motif")) for row in recent_usage)
    scene_family_counts = Counter(
        _text(row.get("scene_family_key"))
        or scene_family_for_motif(row.get("scene_motif"))
        for row in recent_usage
    )
    opening_counts = Counter(_text(row.get("opening_action")) for row in recent_usage)
    persona_counts = Counter(_text(row.get("persona_role")) for row in recent_usage)
    same_product_perceptual_counts = Counter(
        _usage_perceptual_signature(row)
        for row in recent_usage
        if _text(row.get("product_code")) == _text(product_code)
        and _usage_perceptual_signature(row)
    )
    selling_scene_preferences = _selling_argument_scene_preferences(direction)
    category_scene_preferences = _category_scene_preferences(direction)
    carrier_contract = _carrier_contract(direction_carrier)
    scene_request = _scene_request_contract(
        product_type=product_type,
        category=category,
        direction=direction,
        carrier_contract=carrier_contract,
        country=country,
    )
    scene_preferences = list(dict.fromkeys([
        *selling_scene_preferences,
        *category_scene_preferences,
        *_scene_request_affinity_tags(scene_request),
    ]))
    content_bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    semantic_spine = (
        content_bundle.get("semantic_spine_contract")
        if isinstance(content_bundle.get("semantic_spine_contract"), dict)
        else {}
    )
    seed_material = f"{product_code}|{_direction_id(direction)}|{direction.get('cluster_id')}"
    seed = int(hashlib.sha256(seed_material.encode("utf-8")).hexdigest()[:12], 16)
    # Batch request idempotency is handled by the frozen batch itself.  A new
    # request for the same product/direction must be allowed to explore the
    # next least-used combination rather than silently replaying an old one.
    scored: List[Tuple[float, int, Dict[str, Any]]] = []
    for index, item in enumerate(candidates):
        signature = _usage_signature(item)
        reuse_penalty = 100 * exact_counts[signature]
        axis_penalty = (
            12 * scene_counts[item["scene_motif"]]
            + 8 * opening_counts[item["opening_action"]]
            + 5 * persona_counts[item["persona_role"]]
        )
        family_key = _text(item.get("scene_family_key")) or "GENERIC_INDOOR"
        # Batch diversity is a soft planning preference, independent of scene
        # evidence. Evidence can add a small positive bonus, but absence of a
        # matrix never disables ordinary family rotation.
        family_repeat_penalty = 25 * scene_family_counts[family_key]
        candidate_tags = list(dict.fromkeys([
            *_scene_affinity_tags(item.get("scene_motif")),
            *_category_scene_affinity_tags(item),
        ]))
        affinity_matches = [
            tag for tag in scene_preferences if tag in candidate_tags
        ]
        selling_affinity_matches = [
            tag for tag in selling_scene_preferences if tag in candidate_tags
        ]
        # Soft preference only.  One exact recent combination still costs 100,
        # so semantic fit cannot collapse a batch back into one repeated scene.
        # A scene-explicit selling argument must still outrank the 30-point
        # exact-outfit preference, without becoming a validator or hard gate.
        generic_affinity_bonus = min(30, 18 * len(affinity_matches))
        selling_scene_semantic_bonus = (
            SELLING_SCENE_SEMANTIC_MATCH_BONUS
            if selling_affinity_matches
            else 0
        )
        affinity_bonus = max(
            generic_affinity_bonus,
            selling_scene_semantic_bonus,
        )
        proof_environment_score = _scene_request_proof_environment_score(
            scene_request, item
        )
        outfit_contract, outfit_recent_count, outfit_batch_count = (
            _select_outfit_contract(
                seed=seed,
                recent_usage=recent_usage,
                product_code=product_code,
                product_type=product_type,
                top_category=category,
                product_profile=product_profile,
                direction_carrier=direction_carrier,
                country=country,
                scene_family=family_key,
                direction=direction,
            )
        )
        market_context = (
            semantic_spine.get("product_market_context")
            if isinstance(semantic_spine.get("product_market_context"), dict)
            else {}
        )
        market_primary = (
            market_context.get("primary_usage_world")
            if isinstance(market_context.get("primary_usage_world"), dict)
            else {}
        )
        if _text(market_primary.get("kind")) == "TRAVEL_TO_COOLER_DESTINATION":
            # Country climate describes where the audience lives; it must not
            # override the destination climate explicitly maintained by the
            # operator.  This is a soft styling context, not a material or
            # warmth claim about the product.
            outfit_contract["climate_profile"] = "TRAVEL_COOL_DESTINATION"
            outfit_contract["market_context_affinity_contract"] = {
                "status": "MATCHED",
                "context_id": _text(market_primary.get("context_id")),
                "context_text": _text(market_primary.get("text")),
                "authority": "SOFT_STYLING_CONTEXT",
                "product_performance_authority": False,
            }
        outfit_scene_affinity = _outfit_scene_affinity_contract(
            outfit_contract, family_key
        )
        outfit_scene_bonus = int(
            outfit_scene_affinity.get("ranking_bonus") or 0
        )
        semantic_scene_relation = (
            classify_scene_relation(semantic_spine, item)
            if semantic_spine_enabled() and semantic_spine
            else {
                "relation": "UNAVAILABLE",
                "reason": "SEMANTIC_SPINE_DISABLED_OR_UNAVAILABLE",
            }
        )
        semantic_scene_score = scene_relation_score(semantic_scene_relation)
        perceptual_signature = _perceptual_signature(
            product_code=product_code,
            item=item,
            outfit_contract=outfit_contract,
            direction_carrier=direction_carrier,
            structure_family=structure_family,
        )
        perceptual_repeat_count = same_product_perceptual_counts[
            perceptual_signature
        ]
        perceptual_repeat_penalty = (
            55 * perceptual_repeat_count
            if _perceptual_repeat_penalty_enabled()
            else 0
        )
        scene_reference = scene_reference_contract_for_family(
            scene_reference_context,
            family_key,
            scene_motif=_text(item.get("scene_motif")),
            presentation=_text(carrier_contract.get("required_presentation_mode")),
            country=country,
            category=category,
            product_type=product_type,
            scene_request=scene_request,
        )
        matrix_bonus = int(scene_reference.get("matrix_bonus") or 0)
        tie_break = (seed + index * 7919) % 1000
        enriched_item = {
            **item,
            "scene_affinity_tags": candidate_tags,
            "scene_affinity_matches": affinity_matches,
            "scene_affinity_score": affinity_bonus,
            "selling_scene_affinity_matches": selling_affinity_matches,
            "selling_scene_semantic_bonus": selling_scene_semantic_bonus,
            "scene_proof_environment_score": proof_environment_score,
            "scene_reference_contract": scene_reference,
            "scene_reference_bonus": matrix_bonus,
            "_joint_outfit_contract": outfit_contract,
            "_joint_outfit_recent_count": outfit_recent_count,
            "_joint_outfit_batch_count": outfit_batch_count,
            "_joint_outfit_scene_affinity": outfit_scene_affinity,
            "perceptual_signature": perceptual_signature,
            "perceptual_repeat_count": perceptual_repeat_count,
            "perceptual_repeat_status": (
                "SOFT_REPEAT_FALLBACK" if perceptual_repeat_count else "NEW"
            ),
            "perceptual_repeat_penalty": perceptual_repeat_penalty,
            "semantic_scene_relation": semantic_scene_relation,
            "semantic_scene_score": semantic_scene_score,
        }
        scored.append((
            reuse_penalty + axis_penalty + family_repeat_penalty
            + perceptual_repeat_penalty
            + semantic_scene_score
            - affinity_bonus - proof_environment_score - matrix_bonus
            - outfit_scene_bonus,
            tie_break,
            enriched_item,
        ))
    _, _, selected = min(scored, key=lambda row: (row[0], row[1]))
    outfit_contract = dict(selected.pop("_joint_outfit_contract", {}) or {})
    outfit_recent_count = int(selected.pop("_joint_outfit_recent_count", 0) or 0)
    outfit_batch_count = int(selected.pop("_joint_outfit_batch_count", 0) or 0)
    outfit_scene_affinity = dict(
        selected.pop("_joint_outfit_scene_affinity", {}) or {}
    )
    from core.persona_selection import (
        build_outfit_persona_affinity_contract,
        select_persona_contract,
    )

    canonical_type = normalize_product_type(product_type, category).canonical_type
    demonstration_mode = demonstration_mode_from_direction(direction, canonical_type)
    persona_contract, persona_recent_count, persona_batch_count = select_persona_contract(
        product_type=product_type,
        top_category=category,
        country=country,
        presentation_mode=_text(carrier_contract.get("required_presentation_mode")),
        capture_mode=_text(scene_request.get("capture_mode")),
        demonstration_mode=demonstration_mode,
        seed=seed,
        recent_usage=recent_usage,
        preferred_persona_ids=list(
            outfit_contract.get("preferred_persona_ids") or []
        ),
        prefer_reference_pack=bool(direction.get("prefer_persona_reference_pack")),
    )
    outfit_persona_affinity = build_outfit_persona_affinity_contract(
        outfit_contract, persona_contract
    )
    selected.update({
        "surface_profile_key": outfit_contract.get("silhouette_key"),
        "hair_direction": outfit_contract.get("hair_direction"),
        "base_outfit_direction": outfit_contract.get("base_outfit_direction"),
        "outfit_selection_contract": outfit_contract,
        "outfit_scene_affinity_contract": outfit_scene_affinity,
        "persona_selection_contract": persona_contract,
        "outfit_persona_affinity_contract": outfit_persona_affinity,
    })
    # Candidate scoring only needs the compact matrix signal.  Once the
    # creative combination and carrier are frozen, compile a scene execution
    # card for that exact motif.  It is advisory and never changes selection,
    # product facts, or failure behaviour.
    selected["scene_reference_contract"] = scene_reference_contract_for_family(
        scene_reference_context,
        _text(selected.get("scene_family_key")) or "GENERIC_INDOOR",
        scene_motif=_text(selected.get("scene_motif")),
        presentation=_text(carrier_contract.get("required_presentation_mode")),
        country=country,
        category=category,
        product_type=product_type,
        scene_request=scene_request,
    )
    selected["scene_request_contract"] = scene_request
    snapshot = {
        "recent_usage_count": len(recent_usage),
        "recent_exact_signature_count": exact_counts[_usage_signature(selected)],
        "scene_recent_count": scene_counts[selected["scene_motif"]],
        "scene_family_recent_count": scene_family_counts[
            _text(selected.get("scene_family_key")) or "GENERIC_INDOOR"
        ],
        "opening_recent_count": opening_counts[selected["opening_action"]],
        "scene_affinity_preferences": scene_preferences,
        "selling_scene_affinity_preferences": selling_scene_preferences,
        "category_scene_affinity_preferences": category_scene_preferences,
        "scene_affinity_matches": list(selected.get("scene_affinity_matches") or []),
        "scene_affinity_score": int(selected.get("scene_affinity_score") or 0),
        "selling_scene_affinity_matches": list(
            selected.get("selling_scene_affinity_matches") or []
        ),
        "selling_scene_semantic_bonus": int(
            selected.get("selling_scene_semantic_bonus") or 0
        ),
        "scene_proof_environment_score": int(
            selected.get("scene_proof_environment_score") or 0
        ),
        "scene_reference_bonus": int(selected.get("scene_reference_bonus") or 0),
        "scene_request_contract": scene_request,
        "semantic_scene_relation": dict(
            selected.get("semantic_scene_relation") or {}
        ),
        "outfit_silhouette_recent_count": outfit_recent_count,
        "outfit_silhouette_batch_count": outfit_batch_count,
        "outfit_scene_match_status": _text(
            outfit_scene_affinity.get("match_status")
        ),
        "outfit_scene_affinity_score": int(
            outfit_scene_affinity.get("ranking_bonus") or 0
        ),
        "outfit_preferred_scene_families": list(
            outfit_scene_affinity.get("preferred_scene_families") or []
        ),
        "persona_template_recent_count": persona_recent_count,
        "persona_template_batch_count": persona_batch_count,
        "outfit_persona_match_status": _text(
            outfit_persona_affinity.get("match_status")
        ),
        "reused_same_product_direction": False,
        "perceptual_signature": _text(selected.get("perceptual_signature")),
        "perceptual_repeat_count": int(
            selected.get("perceptual_repeat_count") or 0
        ),
        "perceptual_repeat_status": _text(
            selected.get("perceptual_repeat_status")
        ) or "NEW",
        "perceptual_repeat_penalty": int(
            selected.get("perceptual_repeat_penalty") or 0
        ),
    }
    material = {
        "product_code": product_code,
        "direction_id": _direction_id(direction),
        "selected": selected,
        "carrier_contract": carrier_contract,
        "creative_product_profile": product_profile,
        "product_type": product_type,
        "policy": CREATIVE_DIVERSITY_POLICY_VERSION,
    }
    contract_id = _stable_id("CDV_", material)
    outfit_signature = ":".join([
        _text(outfit_contract.get("source_type")) or "INTERNAL_PROFILE",
        _text(outfit_contract.get("template_id")) or _text(outfit_contract.get("silhouette_key")),
    ])
    visual_signature = "|".join(
        [
            selected["persona_role"], selected["scene_motif"], selected["opening_action"],
            selected["action_grammar"], outfit_signature,
            _text(persona_contract.get("persona_id")) or "PERSONA_UNAVAILABLE",
        ]
    )
    return {
        "contract_id": contract_id,
        "policy_version": CREATIVE_DIVERSITY_POLICY_VERSION,
        "authority": "CREATIVE_DESIGN",
        **selected,
        **carrier_contract,
        "creative_product_profile": product_profile,
        "product_type": product_type,
        "required_difference_axes": ["scene_motif", "opening_action", "action_grammar"],
        "scene_affinity_policy": "SOFT_PREFERENCE_ONLY",
        "scene_affinity_preferences": scene_preferences,
        "selling_scene_affinity_preferences": selling_scene_preferences,
        "category_scene_affinity_preferences": category_scene_preferences,
        "scene_family_key": _text(selected.get("scene_family_key")) or "GENERIC_INDOOR",
        "surface_profile": {
            "surface_profile_key": _text(selected.get("surface_profile_key")),
            "silhouette_key": _text(outfit_contract.get("silhouette_key")),
            "style_family": _text(outfit_contract.get("style_family")),
            "hair_direction": _text(selected.get("hair_direction")),
            "base_outfit_direction": _text(selected.get("base_outfit_direction")),
            "hard_required": False,
        },
        "outfit_selection_contract": outfit_contract,
        "outfit_scene_affinity_contract": outfit_scene_affinity,
        "persona_selection_contract": persona_contract,
        "outfit_persona_affinity_contract": outfit_persona_affinity,
        "scene_reference_contract": selected.get("scene_reference_contract") or {},
        # These are failed *combinations*, not permanent bans on bedrooms,
        # mirrors, turns or any single creative axis. A future allocator may
        # reuse one axis after the exact combination has left the recent window.
        "forbidden_recent_patterns": list(RECENT_FAILURE_QUARANTINE_PATTERNS),
        "history_snapshot": snapshot,
        "structure_family": _text(structure.get("macro_family_key")),
        "visual_signature": visual_signature,
        "perceptual_signature": _text(selected.get("perceptual_signature")),
        "perceptual_repeat_status": _text(
            selected.get("perceptual_repeat_status")
        ) or "NEW",
        "perceptual_repeat_count": int(
            selected.get("perceptual_repeat_count") or 0
        ),
        "product_code": product_code,
        "country": country,
        "category": category,
    }


def creative_usage_row(
    *,
    contract: Dict[str, Any],
    product_code: str,
    direction: Dict[str, Any],
    source_run_id: int,
) -> Dict[str, Any]:
    usage_id = _stable_id(
        "CPU_",
        {
            "contract_id": contract.get("contract_id"),
            "source_run_id": source_run_id,
            "time_ns": time.time_ns(),
        },
    )
    hooks = direction.get("content_bundle_brief", {}).get("eligible_hook_ids", [])
    return {
        "usage_id": usage_id,
        "product_code": product_code,
        "country": contract.get("country", ""),
        "category": contract.get("category", ""),
        "direction_id": _direction_id(direction),
        "structure_family": contract.get("structure_family", ""),
        "persona_role": contract.get("persona_role", ""),
        "viewer_relationship": contract.get("viewer_relationship", ""),
        "scene_motif": contract.get("scene_motif", ""),
        "opening_action": contract.get("opening_action", ""),
        "action_grammar": contract.get("action_grammar", ""),
        "visual_signature": contract.get("visual_signature", ""),
        "hook_id": hooks[0] if hooks else "",
        "policy_version": contract.get("policy_version", CREATIVE_DIVERSITY_POLICY_VERSION),
        "status": "RESERVED",
        "source_run_id": source_run_id,
        "metadata": {
            "contract_id": contract.get("contract_id"),
            "scene_family_key": contract.get("scene_family_key"),
            "surface_profile": contract.get("surface_profile") or {},
            "outfit_selection_contract": contract.get("outfit_selection_contract") or {},
            "outfit_scene_affinity_contract": contract.get("outfit_scene_affinity_contract") or {},
            "persona_selection_contract": contract.get("persona_selection_contract") or {},
            "outfit_persona_affinity_contract": contract.get("outfit_persona_affinity_contract") or {},
            "perceptual_signature": contract.get("perceptual_signature", ""),
            "perceptual_repeat_status": contract.get(
                "perceptual_repeat_status", "NEW"
            ),
        },
    }


FIELD_CONSUMERS = {
    "presentation_mode": ["VIDEO_PROMPT", "STORYBOARD", "HUMAN_REVIEW"],
    "creative_thesis": ["VOICEOVER", "STORY_COHERENCE", "HUMAN_REVIEW"],
    "creator_motivation": ["VOICEOVER", "STORY_COHERENCE", "HUMAN_REVIEW"],
    "viewer_relationship": ["VOICEOVER", "HUMAN_REVIEW"],
    "retention_hook.opening_event": ["VIDEO_PROMPT", "STORYBOARD", "HUMAN_REVIEW"],
    "retention_hook.delayed_answer": ["VOICEOVER", "STORY_COHERENCE", "HUMAN_REVIEW"],
    "retention_hook.payoff_time": ["VIDEO_PROMPT", "STORYBOARD", "HUMAN_REVIEW"],
    "persona.identity": ["VIDEO_PROMPT", "STORYBOARD", "HUMAN_REVIEW"],
    "persona.age_presence": ["VIDEO_PROMPT", "STORYBOARD"],
    "persona.appearance": ["VIDEO_PROMPT", "STORYBOARD"],
    "persona.hair_makeup": ["VIDEO_PROMPT", "STORYBOARD"],
    "persona.styling": ["VIDEO_PROMPT", "STORYBOARD"],
    "persona.speaking_personality": ["VOICEOVER", "HUMAN_REVIEW"],
    "persona.performance_intensity": ["VIDEO_PROMPT", "STORYBOARD"],
    "scene.location": ["VIDEO_PROMPT", "STORYBOARD", "HUMAN_REVIEW"],
    "scene.moment": ["VOICEOVER", "STORY_COHERENCE", "HUMAN_REVIEW"],
    "scene.lighting": ["VIDEO_PROMPT", "STORYBOARD"],
    "scene.background": ["VIDEO_PROMPT", "STORYBOARD"],
    "scene.camera_setup": ["VIDEO_PROMPT", "STORYBOARD"],
    "scene.why_this_scene": ["STORY_COHERENCE", "HUMAN_REVIEW"],
    "performance_flow.entry_state": ["VIDEO_PROMPT", "STORYBOARD"],
    "performance_flow.behavior_motivation": ["VOICEOVER", "STORY_COHERENCE"],
    "performance_flow.reaction_points": ["VIDEO_PROMPT", "STORYBOARD"],
    "performance_flow.ending_state": ["VIDEO_PROMPT", "STORYBOARD"],
    "event_design.event_motif": ["VIDEO_PROMPT", "STORY_COHERENCE", "HUMAN_REVIEW"],
    "event_design.start_state": ["VIDEO_PROMPT", "STORYBOARD"],
    "event_design.natural_event": ["VIDEO_PROMPT", "STORYBOARD", "VOICEOVER", "HUMAN_REVIEW"],
    "event_design.core_result_moment": ["VIDEO_PROMPT", "STORYBOARD", "VOICEOVER"],
    "event_design.end_state": ["VIDEO_PROMPT", "STORYBOARD"],
    "macro_visual_passages": ["VIDEO_PROMPT", "STORYBOARD", "HUMAN_REVIEW"],
    "visual_language.image_texture": ["VIDEO_PROMPT", "STORYBOARD"],
    "visual_language.camera_behavior": ["VIDEO_PROMPT", "STORYBOARD"],
    "visual_language.framing_bias": ["VIDEO_PROMPT", "STORYBOARD"],
    "visual_language.editing_rhythm": ["VIDEO_PROMPT", "STORYBOARD"],
    "visual_language.anti_template_rules": ["STORYBOARD", "HUMAN_REVIEW"],
    "voice_identity": ["VOICEOVER", "HUMAN_REVIEW"],
    "audio_direction": ["STORYBOARD", "VOICEOVER", "HUMAN_REVIEW"],
}


def attach_field_consumers(blueprint: Dict[str, Any]) -> Dict[str, Any]:
    return {**blueprint, "field_consumers": dict(FIELD_CONSUMERS)}


def validate_complete_blueprint(
    blueprint: Dict[str, Any],
    contract: Dict[str, Any],
) -> Dict[str, Any]:
    issues: List[str] = []
    if _text(blueprint.get("schema_version")) != COMPLETE_BLUEPRINT_SCHEMA_VERSION:
        issues.append("完整脚本蓝图schema_version错误")
    if _text(blueprint.get("diversity_contract_id")) != _text(contract.get("contract_id")):
        issues.append("蓝图没有准确继承creative_diversity_contract")
    if _text(blueprint.get("viewer_relationship")) != _text(contract.get("viewer_relationship")):
        issues.append("蓝图擅自改写viewer_relationship")
    required_presentation = _text(contract.get("required_presentation_mode"))
    if required_presentation in {"PERSON_ON_CAMERA", "HANDS_ONLY", "STATIC_PRODUCT", "MIXED"} and _text(blueprint.get("presentation_mode")) != required_presentation:
        issues.append("蓝图没有准确继承required_presentation_mode")
    persona = blueprint.get("persona") if isinstance(blueprint.get("persona"), dict) else {}
    scene = blueprint.get("scene") if isinstance(blueprint.get("scene"), dict) else {}
    performance = blueprint.get("performance_flow") if isinstance(blueprint.get("performance_flow"), dict) else {}
    retention = blueprint.get("retention_hook") if isinstance(blueprint.get("retention_hook"), dict) else {}
    visual = blueprint.get("visual_language") if isinstance(blueprint.get("visual_language"), dict) else {}
    voice = blueprint.get("voice_identity") if isinstance(blueprint.get("voice_identity"), dict) else {}
    audio = blueprint.get("audio_direction") if isinstance(blueprint.get("audio_direction"), dict) else {}
    event = blueprint.get("event_design") if isinstance(blueprint.get("event_design"), dict) else {}
    passages = blueprint.get("macro_visual_passages") if isinstance(blueprint.get("macro_visual_passages"), list) else []
    for field in ("creative_thesis", "creator_motivation", "viewer_relationship"):
        if not _text(blueprint.get(field)):
            issues.append(f"蓝图缺少{field}")
    for section_name, section, required in (
        ("persona", persona, ("identity", "age_presence", "appearance", "hair_makeup", "styling", "speaking_personality", "performance_intensity")),
        ("retention_hook", retention, ("opening_event", "delayed_answer", "payoff_time")),
        ("scene", scene, ("location", "moment", "lighting", "background", "camera_setup", "why_this_scene")),
        ("performance_flow", performance, ("entry_state", "behavior_motivation", "ending_state")),
        ("event_design", event, ("event_motif", "start_state", "natural_event", "core_result_moment", "end_state")),
        ("visual_language", visual, ("image_texture", "camera_behavior", "framing_bias", "editing_rhythm", "anti_template_rules")),
        ("voice_identity", voice, ("tone", "relationship_mode", "particle_density", "sales_pressure", "forbidden_tone")),
        ("audio_direction", audio, ("bgm_style", "environment_sound", "voiceover_priority")),
    ):
        if not section:
            issues.append(f"蓝图缺少{section_name}")
            continue
        for field in required:
            value = section.get(field)
            if value in (None, "", []):
                issues.append(f"蓝图缺少{section_name}.{field}")
    if len(passages) != 3:
        issues.append(f"蓝图macro_visual_passages应为3段，实际{len(passages)}")
    for index, passage in enumerate(passages, 1):
        if not isinstance(passage, dict):
            issues.append(f"宏观画面段{index}不是对象")
            continue
        for field in (
            "passage_no",
            "narrative_role",
            "visible_process",
            "observable_action",
            "camera_observation",
            "product_visibility",
        ):
            if passage.get(field) in (None, "", []):
                issues.append(f"宏观画面段{index}缺少{field}")
        if not isinstance(passage.get("supported_claim_keys"), list):
            issues.append(f"宏观画面段{index}.supported_claim_keys必须是数组")
    if _text(scene.get("location")) != _text(contract.get("scene_motif")):
        issues.append("蓝图擅自改写scene_motif")
    contract_opening = _text(contract.get("opening_action"))
    if contract_opening and contract_opening not in " ".join(
        [
            _text(blueprint.get("creator_motivation")),
            _text(performance.get("behavior_motivation")),
            _text(event.get("natural_event")),
            json.dumps(performance.get("reaction_points", []), ensure_ascii=False),
        ]
    ):
        issues.append("蓝图没有执行分配的opening_action")
    visible_text = json.dumps(blueprint, ensure_ascii=False)
    for term in AI_CONTROL_TERMS:
        if term in visible_text:
            issues.append(f"蓝图包含抽象AI控制词：{term}")
    consumers = blueprint.get("field_consumers")
    if consumers != FIELD_CONSUMERS:
        issues.append("蓝图field_consumers不是代码权威映射")
    carrier = _text(contract.get("required_carrier")).upper()
    # The field itself is authoritative; this small lexical check catches the
    # only harmful leak: a static/hand direction being narrated as a wearer
    # life event. It deliberately does not prescribe every visual detail.
    activity_text = " ".join(
        [
            _text(event.get("natural_event")),
            _text(event.get("event_motif")),
            _text(performance.get("behavior_motivation")),
            *[
                f"{_text(item.get('visible_process'))} {_text(item.get('observable_action'))}"
                for item in passages if isinstance(item, dict)
            ],
        ]
    )
    if carrier == "STATIC_PRODUCT":
        # A static blueprint must often say "人物不出镜" or "不含手部" to
        # make its render boundary explicit. Strip those negative boundary
        # statements before checking for an actual person/hand action.
        static_activity_text = re.sub(
            r"(?:人物|模特|她|他|手部|双手|人手)(?:均)?不(?:出镜|出现|入镜|进入画面)|"
            r"不(?:出现|含|见)(?:人物|模特|她|他|手部|双手|人手)(?:或(?:人物|模特|她|他|手部|双手|人手))*|"
            r"(?:无|没有)(?:人物|模特|她|他|手部|双手|人手)(?:或(?:人物|模特|她|他|手部|双手|人手))*",
            "",
            activity_text,
        )
        if re.search(r"人物|模特|她|他|穿着|拿起|背着|走向|开门|双手|手部|人手", static_activity_text):
            issues.append("STATIC_PRODUCT蓝图混入人物或手部生活动作")
    if carrier == "HAND_ONLY" and re.search(r"人物|她|他|全身|半身|穿着|走向|开门|转身", activity_text):
        issues.append("HAND_ONLY蓝图混入人物整体生活动作")
    return {
        "policy_version": COMPLETE_SCRIPT_POLICY_VERSION,
        "valid": not issues,
        "issues": issues,
        "judge": "RULE",
    }


def video_prompt_projection(blueprint: Dict[str, Any]) -> Dict[str, Any]:
    """Return only blueprint fields explicitly authorised for video rendering."""

    consumers = blueprint.get("field_consumers") if isinstance(blueprint.get("field_consumers"), dict) else {}
    result: Dict[str, Any] = {}
    for path, targets in consumers.items():
        if "VIDEO_PROMPT" not in targets:
            continue
        current: Any = blueprint
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current in (None, "", []):
            continue
        target = result
        parts = path.split(".")
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = current
    return result


def retention_review(script: Dict[str, Any]) -> Dict[str, Any]:
    """Return two planning-only signals without pretending to judge a render."""

    storyboard = [
        item for item in script.get("storyboard", []) if isinstance(item, dict)
    ]
    blueprint = (
        script.get("creative_blueprint")
        if isinstance(script.get("creative_blueprint"), dict)
        else {}
    )
    retention = (
        blueprint.get("retention_hook")
        if isinstance(blueprint.get("retention_hook"), dict)
        else {}
    )
    event = (
        blueprint.get("event_design")
        if isinstance(blueprint.get("event_design"), dict)
        else {}
    )
    passages = [
        item for item in blueprint.get("macro_visual_passages", []) if isinstance(item, dict)
    ]
    first_action = _text(storyboard[0].get("observable_action")) if storyboard else ""
    static_openings = ("静置", "静止展示", "站着展示", "商品居中", "仅展示", "无动作")
    opening_not_static = bool(
        _text(retention.get("opening_event"))
        and first_action
        and not any(term in first_action for term in static_openings)
    )
    action_text = " ".join(
        _text(shot.get("observable_action")) for shot in storyboard
    )
    checklist_patterns = (
        r"逐颗|逐一|依次.*(?:扣|口袋|袖)",
        r"分别.*(?:指|摸|触|停).*(?:左|右|两侧)",
        r"手指.*(?:扣|口袋|袖)",
        r"从上.*到下.*扣",
        r"指向.*(?:扣|口袋|袖)",
    )
    checklist_hits = [
        pattern for pattern in checklist_patterns if re.search(pattern, action_text)
    ]
    no_checklist_action = not checklist_hits

    signals = {
        "opening_not_static": opening_not_static,
        "no_checklist_action": no_checklist_action,
        "single_event_mainline": bool(
            _text(event.get("natural_event")) and len(passages) == 3
        ),
    }
    return {
        "policy_version": "retention-review-v22-event-planning-only",
        "scope": "TEXT_PLAN_ONLY_NOT_RENDER_JUDGMENT",
        **signals,
        "passed_count": sum(bool(value) for value in signals.values()),
        "checklist_patterns_hit": checklist_hits,
        "is_blocking": False,
    }


def validate_complete_script(script: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[str] = []
    storyboard = script.get("storyboard") if isinstance(script.get("storyboard"), list) else []
    blueprint = script.get("creative_blueprint") if isinstance(script.get("creative_blueprint"), dict) else {}
    contract = script.get("creative_diversity_contract") if isinstance(script.get("creative_diversity_contract"), dict) else {}
    production_design = (
        script.get("production_design")
        if isinstance(script.get("production_design"), dict)
        else {}
    )
    recording_context = (
        blueprint.get("recording_context")
        if isinstance(blueprint.get("recording_context"), dict)
        else {}
    )
    direct_share = _text(recording_context.get("recording_mode")).upper() == "CREATOR_DIRECT_SHARE"
    if not blueprint:
        issues.append("完整脚本缺少creative_blueprint")
    if not contract:
        issues.append("完整脚本缺少creative_diversity_contract")
    if not storyboard:
        issues.append("完整脚本缺少storyboard")
    fact_keys = set()
    spoken_count = 0
    for shot in storyboard:
        if not isinstance(shot, dict):
            issues.append("storyboard包含非对象项")
            continue
        required_fields = ("shot_content", "framing", "audio_actual") if direct_share else (
            "shot_content", "observable_action", "framing", "audio_actual"
        )
        for field in required_fields:
            if not _text(shot.get(field)):
                issues.append(f"镜头{shot.get('shot_no')}缺少{field}")
        fact_keys.update(_text(item) for item in shot.get("supported_claim_keys", []) if _text(item))
        if _text(shot.get("audio_actual")) in {
            "VOICEOVER",
            "VOICEOVER_CONTINUATION",
            "VOICEOVER_WITH_NATURAL_SOUND",
        } or (
            not _text(shot.get("audio_actual"))
            and _text(shot.get("voiceover_text_target_language"))
        ):
            spoken_count += 1
        hard = _text(shot.get("audio_hard_constraint"))
        actual = _text(shot.get("audio_actual"))
        if hard == "MUST_BE_SILENT" and actual != "SILENT":
            issues.append(f"镜头{shot.get('shot_no')}违反硬静默约束")
    if not 2 <= len(fact_keys) <= 3:
        issues.append(f"完整脚本应覆盖2至3个卖点，实际{len(fact_keys)}")
    all_text = json.dumps(storyboard, ensure_ascii=False)
    for term in AI_CONTROL_TERMS:
        if term in all_text:
            issues.append(f"最终脚本包含AI控制词：{term}")
    retention = retention_review(script)
    warnings = (
        []
        if production_design
        else ["旧脚本缺少显式production_design；新V21组装会自动补齐"]
    )
    retention_warning_labels = {
        "opening_not_static": "开头仍像静态站桩，建议从人物原本就在进行的自然动作中途开始",
        "no_checklist_action": "中段动作出现逐项指向或核对商品部位的计划痕迹",
        "single_event_mainline": "脚本没有形成一件连续生活事件",
    }
    warnings.extend(
        message
        for key, message in retention_warning_labels.items()
        if not retention[key]
    )
    return {
        "policy_version": COMPLETE_SCRIPT_POLICY_VERSION,
        "valid": not issues,
        "issues": issues,
        "warnings": warnings,
        "retention_review": retention,
        "judges": {
            "rule": "PASS" if not issues else "FAIL",
            "independent_model": "PENDING",
            "thai_native_human": "PENDING",
            "content_human": "PENDING",
            "render_recon": "PENDING",
        },
        "release_status": "MACHINE_SCREENED_NOT_HUMAN_APPROVED" if not issues else "RULE_REJECTED",
    }


def assign_audio_actual(shots: Iterable[Dict[str, Any]], spoken_shots: Iterable[int]) -> List[Dict[str, Any]]:
    spoken = {int(item) for item in spoken_shots}
    result: List[Dict[str, Any]] = []
    for item in shots:
        shot = dict(item)
        number = int(shot.get("shot_no") or len(result) + 1)
        hard = _text(shot.get("audio_hard_constraint")) or "NONE"
        if hard == "MUST_BE_SILENT":
            actual = "SILENT"
        elif hard == "MUST_KEEP_NATURAL_SOUND":
            actual = "VOICEOVER_WITH_NATURAL_SOUND" if number in spoken else "NATURAL_SOUND"
        elif number in spoken:
            actual = "VOICEOVER"
        else:
            preference = _text(shot.get("audio_preference"))
            actual = "AMBIENT" if preference == "AMBIENT_PREFERRED" else "SILENT"
        shot["audio_actual"] = actual
        result.append(shot)
    return result
