"""Governed creative diversity and complete-script contracts for stage-0 V3.1.

Observed execution facts remain untouched.  This module allocates explicitly
labelled production design choices before an LLM writes the creative blueprint,
so the model cannot silently fall back to its most common try-on template.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.scene_reference_adapter import (
    scene_family_for_motif,
    scene_reference_contract_for_family,
)
from core.outfit_template_provider import load_structured_outfit_templates
from core.product_type_resolution import normalize_product_type


CREATIVE_DIVERSITY_POLICY_VERSION = "creative-diversity-v7-shared-outfit-template"
OUTFIT_SELECTION_CONTRACT_VERSION = "outfit-selection-v2-shared-provider"
ACCESSORY_OUTFIT_SELECTION_CONTRACT_VERSION = "outfit-selection-v3-worn-accessory"
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
        "包", "墨镜", "太阳镜", "眼镜", "scarf", "hat", "earring", "necklace", "bag",
    )
    hand_static_tokens = (
        "戒指", "手链", "手镯", "手环", "手串", "发饰", "发夹", "抓夹", "发圈",
        "ring", "bracelet", "hair clip", "hair accessory",
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
            "base_outfit_direction": "纯色基础上衣配普通日常下装，肩颈和上半身关系保持清楚",
            "outer_layer_direction": "不强制外套；如有外层，以不遮挡商品为先",
            "neckline_direction": "领口与商品之间保留清楚边界，不展示复杂系法",
            "palette_relation": "基础服装使用能衬托商品现有颜色或图案的克制配色",
            "visibility_requirement": "至少一段清楚看到商品主体、佩戴位置与上半身搭配关系",
        },
        {
            "silhouette_key": "SCARF_SIMPLE_SHIRT",
            "style_family": "LIGHT_DAILY_LAYERING",
            "hair_direction": "自然短发、耳后别发或低存在感束发",
            "base_outfit_direction": "简洁衬衫或轻薄上装配日常长裤或半裙，不堆叠抢眼配饰",
            "outer_layer_direction": "可无外层或使用简洁开衫，不遮挡商品主体",
            "neckline_direction": "肩颈、领口与商品搭配状态完整可见",
            "palette_relation": "使用基础中性色或邻近色，避免凭空改写商品颜色",
            "visibility_requirement": "商品形状、边缘和半身穿搭关系稳定可见",
        },
        {
            "silhouette_key": "SCARF_TONAL_BASE",
            "style_family": "TONAL_DAILY_ACCENT",
            "hair_direction": "简单束发或自然披发放到肩后",
            "base_outfit_direction": "上下装保持低图案密度和普通日常轮廓，让商品成为上半身重点",
            "outer_layer_direction": "外层保持简洁，不根据商品名称补充季节或材质效果",
            "neckline_direction": "商品与领口、肩部的层次清楚但不过度造型",
            "palette_relation": "以商品现有主色或图案为中心做自然协调，不要求完全同色",
            "visibility_requirement": "商品整体形状和上半身搭配关系至少有一段完整呈现",
        },
    ),
    "winter_scarf": (
        {
            "silhouette_key": "SCARF_KNIT_BASE",
            "style_family": "WINTER_DAILY_LAYERING",
            "hair_direction": "自然披发、耳后别发或简单束发，不遮住围巾主体",
            "base_outfit_direction": "简洁针织上衣配日常长裤或半裙，保持肩颈和上半身轮廓清楚",
            "outer_layer_direction": "基础外套或针织层，不根据类型名称补充保暖功效",
            "neckline_direction": "肩颈区域无遮挡，围巾与上装关系完整可见",
            "palette_relation": "与围巾主色或图案自然协调，不要求完全同色",
            "visibility_requirement": "至少一段清楚看到围巾与脖颈、肩部及上半身关系",
        },
        {
            "silhouette_key": "SCARF_SIMPLE_COAT",
            "style_family": "COMMUTE_OUTER_LAYER",
            "hair_direction": "自然短发、耳后别发或低存在感束发",
            "base_outfit_direction": "纯色基础上衣配简洁下装，外层轮廓克制",
            "outer_layer_direction": "普通通勤外层，避免夸张大翻领遮住围巾",
            "neckline_direction": "围巾边缘和肩部垂落关系保持清楚",
            "palette_relation": "使用邻近色或基础中性色衬托商品，不增加复杂配饰",
            "visibility_requirement": "围巾主体、边缘与外层搭配同时可见",
        },
        {
            "silhouette_key": "SCARF_TURTLENECK_LAYER",
            "style_family": "TONAL_WINTER_BASE",
            "hair_direction": "简单束发或自然披发放到肩后",
            "base_outfit_direction": "基础高领或圆领针织配日常下装，不做棚拍式层叠",
            "outer_layer_direction": "可无外套或使用简洁外层，以商品可见为先",
            "neckline_direction": "围巾与领口保持层次但不互相遮挡",
            "palette_relation": "基础服装色彩克制，让围巾图案或主色成为上半身重点",
            "visibility_requirement": "围巾与领口、肩部和整体上半身关系清楚",
        },
    ),
    "silk_scarf": (
        {
            "silhouette_key": "SILK_SCARF_PLAIN_SHIRT",
            "style_family": "LIGHT_COMMUTE_ACCENT",
            "hair_direction": "耳后别发、自然短发或简单束发，避免遮住丝巾",
            "base_outfit_direction": "纯色简洁衬衫配日常下装，领口和上半身保持清楚",
            "outer_layer_direction": "不强制外套，避免复杂领型与丝巾竞争",
            "neckline_direction": "丝巾与衬衫领口关系完整可见",
            "palette_relation": "基础上衣使用能衬托商品图案的克制配色，不猜测材质",
            "visibility_requirement": "至少一段清楚看到丝巾图案、边缘、领口与半身关系",
        },
        {
            "silhouette_key": "SILK_SCARF_CREW_NECK",
            "style_family": "EVERYDAY_NECK_ACCENT",
            "hair_direction": "自然短发、耳后别发或低存在感束发",
            "base_outfit_direction": "纯色圆领基础上衣配日常长裤或半裙，不堆叠首饰",
            "outer_layer_direction": "无外层或只保留极简开衫，不遮挡颈部",
            "neckline_direction": "圆领与丝巾之间保留清楚边界",
            "palette_relation": "基础服装保持低图案密度，让商品主色或图案成为视觉重点",
            "visibility_requirement": "丝巾主体和领口在正面半身镜中保持清楚",
        },
        {
            "silhouette_key": "SILK_SCARF_LIGHT_KNIT",
            "style_family": "SOFT_DAILY_ACCENT",
            "hair_direction": "简单束发或披发放到肩后",
            "base_outfit_direction": "轻薄纯色针织上装配简洁下装，整体保持日常",
            "outer_layer_direction": "不使用抢眼外套或复杂项链",
            "neckline_direction": "颈部与丝巾搭配状态完整，不拍复杂系法",
            "palette_relation": "用邻近色或基础中性色衬托现有商品颜色",
            "visibility_requirement": "图案、包边和颈部搭配关系至少有一段稳定可见",
        },
    ),
    "headscarf": (
        {
            "silhouette_key": "HEADSCARF_PLAIN_TOP",
            "style_family": "DAILY_HEAD_STYLE",
            "hair_direction": "明确已经完成的日常发型或覆盖状态，不展示包裹过程",
            "base_outfit_direction": "纯色简洁上装配日常下装，避免复杂图案与头巾竞争",
            "outer_layer_direction": "按日常场景自然搭配，不增加宗教或文化身份暗示",
            "neckline_direction": "上半身轮廓保持清楚，服务头巾与穿搭整体关系",
            "palette_relation": "与头巾主色或图案自然协调，不根据场景推断身份",
            "visibility_requirement": "头巾位置、轮廓、头发状态和半身穿搭同时可见",
        },
        {
            "silhouette_key": "HEADSCARF_SHIRT_BASE",
            "style_family": "CITY_DAILY_HEAD_STYLE",
            "hair_direction": "头巾已经佩戴完成，发际或覆盖边界按当前设计保持稳定",
            "base_outfit_direction": "简洁衬衫或轻上装配普通日常下装",
            "outer_layer_direction": "不强制外层，避免帽子、夸张耳饰等竞争元素",
            "neckline_direction": "领口和肩部保持简洁，让头部造型成为重点",
            "palette_relation": "基础上装保持纯色或低图案密度",
            "visibility_requirement": "正面或轻侧面能看清头巾轮廓及与上半身关系",
        },
        {
            "silhouette_key": "HEADSCARF_TONAL_BASE",
            "style_family": "TONAL_HEAD_ACCENT",
            "hair_direction": "已完成头部造型，从稳定结果开始拍摄",
            "base_outfit_direction": "上下装使用克制相近色阶，造型保持普通日常",
            "outer_layer_direction": "可使用简洁轻外层，但不得遮挡头肩关系",
            "neckline_direction": "头肩比例与上半身线条清楚",
            "palette_relation": "用基础色衬托商品现有图案，不擅自改变颜色关系",
            "visibility_requirement": "头巾整体形状、覆盖位置与穿搭关系稳定可见",
        },
    ),
}


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


def _select_outfit_contract(
    *,
    seed: int,
    recent_usage: Sequence[Dict[str, Any]],
    product_code: str,
    product_type: str,
    top_category: str,
    product_profile: str,
    direction_carrier: str,
) -> Tuple[Dict[str, Any], int, int]:
    is_wearer_apparel = (
        product_profile == "WORN_APPAREL"
        and direction_carrier in {"WEARER_ACTIVE", "MIXED", "UNAVAILABLE"}
    )
    canonical_type = normalize_product_type(
        product_type, top_category
    ).canonical_type
    is_wearer_accessory = (
        product_profile == "WORN_ACCESSORY"
        and canonical_type in _ACCESSORY_OUTFIT_PROFILES
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
            "hair_direction": "不适用",
            "base_outfit_direction": "不适用",
            "palette_relation": "不适用",
            "selection_policy": "PRODUCT_PRESENTATION_ONLY",
            "hard_required": False,
        }, 0, 0)

    historical_counts: Counter = Counter()
    batch_counts: Counter = Counter()
    for usage_row in recent_usage:
        key = _usage_outfit_selection_key(usage_row)
        if not key or key.endswith(":PRODUCT_LED"):
            continue
        target = batch_counts if usage_row.get("_batch_reserved") else historical_counts
        target[key] += 1

    candidates: List[Dict[str, Any]] = []
    if is_wearer_apparel:
        for template in load_structured_outfit_templates(
            product_code=product_code,
            product_type=product_type,
        ):
            candidates.append({
                **template,
                "contract_version": OUTFIT_SELECTION_CONTRACT_VERSION,
                "palette_relation": "由模板结构化颜色字段决定；不得读取模板标题或正文补充",
                "selection_policy": "EXPLICIT_PRODUCT_CODE_THEN_LEAST_USED",
                "hard_required": False,
                "source_preference": int(template.get("match_rank") or 0),
            })
        internal_profiles = _APPAREL_SURFACE_PROFILES
        contract_version = OUTFIT_SELECTION_CONTRACT_VERSION
    else:
        internal_profiles = _ACCESSORY_OUTFIT_PROFILES[canonical_type]
        contract_version = ACCESSORY_OUTFIT_SELECTION_CONTRACT_VERSION
    for profile in internal_profiles:
        candidates.append({
            "contract_version": contract_version,
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
            "selection_policy": "LEAST_RECENTLY_USED_SOFT",
            "hard_required": False,
            "source_preference": 2,
        })

    ranked: List[Tuple[int, int, int, int, int, Dict[str, Any]]] = []
    for index, candidate in enumerate(candidates):
        source = _text(candidate.get("source_type")) or "INTERNAL_PROFILE"
        template_id = _text(candidate.get("template_id"))
        silhouette = _text(candidate.get("silhouette_key"))
        key = f"{source}:{template_id or silhouette}"
        tie_break = (seed + index * 6151) % 1000
        ranked.append((
            batch_counts[key],
            int(candidate.get("source_preference") or 0),
            historical_counts[key],
            -int(candidate.get("priority") or 0),
            tie_break,
            candidate,
        ))
    batch_count, _, historical_count, _, _, selected = min(
        ranked, key=lambda row: (row[0], row[1], row[2], row[3], row[4])
    )
    contract = dict(selected)
    contract.pop("source_preference", None)
    contract.pop("match_rank", None)
    return (contract, historical_count, batch_count)


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
    selling_scene_preferences = _selling_argument_scene_preferences(direction)
    category_scene_preferences = _category_scene_preferences(direction)
    scene_preferences = list(dict.fromkeys([
        *selling_scene_preferences,
        *category_scene_preferences,
    ]))
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
        # Soft preference only.  One exact recent combination still costs 100,
        # so semantic fit cannot collapse a batch back into one repeated scene.
        affinity_bonus = min(30, 18 * len(affinity_matches))
        scene_reference = scene_reference_contract_for_family(
            scene_reference_context,
            family_key,
        )
        matrix_bonus = int(scene_reference.get("matrix_bonus") or 0)
        tie_break = (seed + index * 7919) % 1000
        enriched_item = {
            **item,
            "scene_affinity_tags": candidate_tags,
            "scene_affinity_matches": affinity_matches,
            "scene_affinity_score": affinity_bonus,
            "scene_reference_contract": scene_reference,
            "scene_reference_bonus": matrix_bonus,
        }
        scored.append((
            reuse_penalty + axis_penalty + family_repeat_penalty - affinity_bonus - matrix_bonus,
            tie_break,
            enriched_item,
        ))
    _, _, selected = min(scored, key=lambda row: (row[0], row[1]))
    structure = direction.get("structure_execution_plan") if isinstance(direction.get("structure_execution_plan"), dict) else {}
    carrier_contract = _carrier_contract(authoritative_carrier(direction))
    outfit_contract, outfit_recent_count, outfit_batch_count = _select_outfit_contract(
        seed=seed,
        recent_usage=recent_usage,
        product_code=product_code,
        product_type=product_type,
        top_category=category,
        product_profile=product_profile,
        direction_carrier=direction_carrier,
    )
    selected.update({
        "surface_profile_key": outfit_contract.get("silhouette_key"),
        "hair_direction": outfit_contract.get("hair_direction"),
        "base_outfit_direction": outfit_contract.get("base_outfit_direction"),
        "outfit_selection_contract": outfit_contract,
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
    )
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
        "scene_reference_bonus": int(selected.get("scene_reference_bonus") or 0),
        "outfit_silhouette_recent_count": outfit_recent_count,
        "outfit_silhouette_batch_count": outfit_batch_count,
        "reused_same_product_direction": False,
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
        "scene_reference_contract": selected.get("scene_reference_contract") or {},
        # These are failed *combinations*, not permanent bans on bedrooms,
        # mirrors, turns or any single creative axis. A future allocator may
        # reuse one axis after the exact combination has left the recent window.
        "forbidden_recent_patterns": list(RECENT_FAILURE_QUARANTINE_PATTERNS),
        "history_snapshot": snapshot,
        "structure_family": _text(structure.get("macro_family_key")),
        "visual_signature": visual_signature,
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
        for field in ("shot_content", "observable_action", "framing", "audio_actual"):
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
