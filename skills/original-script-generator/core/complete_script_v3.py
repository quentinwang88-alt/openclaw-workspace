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
from typing import Any, Dict, Iterable, List, Sequence, Tuple


CREATIVE_DIVERSITY_POLICY_VERSION = "creative-diversity-v3-upper-apparel-life-events"
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
        "围巾", "丝巾", "披肩", "帽", "耳环", "耳饰", "耳线", "项链", "项圈",
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


def _creative_combinations(
    direction: Dict[str, Any], product_type: str, category: str = ""
) -> List[Dict[str, str]]:
    action_text = _reference_action_text(direction)
    carrier = authoritative_carrier(direction)
    product_profile = creative_product_profile(product_type, category)
    upper_apparel = any(
        token in _text(product_type).lower()
        for token in ("衣", "外套", "上装", "夹克", "jacket", "coat", "top")
    )
    if product_profile == "WORN_APPAREL" and upper_apparel and carrier in {"WEARER_ACTIVE", "MIXED", "UNAVAILABLE"}:
        combinations = [
            {
                "persona_role": "下班前收拾随身物品的通勤者",
                "viewer_relationship": "像朋友分享自己刚完成的一套通勤穿搭",
                "scene_motif": "办公室衣帽区靠窗墙面",
                "opening_action": "完成外搭穿着动作后伸手拿起放在一旁的随身物品",
                "action_grammar": "完成穿着→拿起随身物品→离开原位置",
                "visual_tone": "旁观式通勤记录",
            },
            {
                "persona_role": "准备出门的日常通勤者",
                "viewer_relationship": "像朋友分享出门前刚搭好的基础穿搭",
                "scene_motif": "公寓玄关自然光墙面",
                "opening_action": "完成外搭穿着动作后拿起玄关台面上的钥匙",
                "action_grammar": "完成穿着→拿起钥匙→向门口移动",
                "visual_tone": "固定机位出门记录",
            },
            {
                "persona_role": "收好衣物准备离开的办公室使用者",
                "viewer_relationship": "像同事分享一件能补足基础穿搭层次的外搭",
                "scene_motif": "服装收纳架旁的浅色墙面",
                "opening_action": "外搭已经穿到身上，人物把空衣架放回收纳架",
                "action_grammar": "放回衣架→站直整理随身物品→走出画面",
                "visual_tone": "低干预生活记录",
            },
            {
                "persona_role": "从客厅准备出门的居家通勤者",
                "viewer_relationship": "像朋友分享一套自己正准备穿出门的搭配",
                "scene_motif": "客厅窗边的单色背景区域",
                "opening_action": "完成外搭穿着动作后从沙发扶手拿起随身包",
                "action_grammar": "完成穿着→拿起随身包→经过窗边准备离开",
                "visual_tone": "自然窗光旁观记录",
            },
            {
                "persona_role": "午休准备下楼的办公室使用者",
                "viewer_relationship": "像同事分享午休出门时正在穿的一套外搭",
                "scene_motif": "写字楼电梯厅的浅色金属墙面",
                "opening_action": "外搭已经穿好，人物看一眼楼层指示后走向开启的电梯",
                "action_grammar": "等待电梯→电梯门开启→自然走入",
                "visual_tone": "固定机位午间行动记录",
            },
            {
                "persona_role": "傍晚从书店离开的城市日常穿搭者",
                "viewer_relationship": "像朋友分享逛完书店后这一身的真实状态",
                "scene_motif": "书店出口旁的暖色书架过道",
                "opening_action": "外搭已经穿好，人物把看完的书放回陈列台后转向出口",
                "action_grammar": "放回书→沿书架过道前行→走向出口",
                "visual_tone": "暖光下的低干预生活记录",
            },
            {
                "persona_role": "周末准备逛街的城市日常穿搭者",
                "viewer_relationship": "像朋友分享周末走动时整套穿搭的自然比例",
                "scene_motif": "商场连廊靠窗的自然光休息区",
                "opening_action": "外搭已经穿好，人物从窗边长椅自然起身并沿连廊前行",
                "action_grammar": "从长椅起身→经过窗边→沿连廊继续前行",
                "visual_tone": "自然光下的旁观式行动记录",
            },
            {
                "persona_role": "下楼取件的公寓住户",
                "viewer_relationship": "像邻居分享临时下楼时随手穿的一套外搭",
                "scene_motif": "公寓大堂快递柜旁的干净墙面",
                "opening_action": "外搭已经穿好，人物关上快递柜门后转身走向大堂出口",
                "action_grammar": "关上柜门→转身经过大厅→走向出口",
                "visual_tone": "公寓公共空间的日常记录",
            },
            {
                "persona_role": "准备搭车去见朋友的城市日常穿搭者",
                "viewer_relationship": "像朋友分享等车时这一身在自然走动中的样子",
                "scene_motif": "公寓楼下有顶车道的自然光等候区",
                "opening_action": "外搭已经穿好，人物从立柱旁走到等候线并看向来车方向",
                "action_grammar": "走到等候区→自然停留→向来车方向继续前行",
                "visual_tone": "有环境纵深的城市生活记录",
            },
            {
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


def build_creative_diversity_contract(
    *,
    product_code: str,
    country: str,
    category: str,
    product_type: str,
    direction: Dict[str, Any],
    recent_usage: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Choose a positive creative combination before the model writes prose."""

    product_profile = creative_product_profile(product_type, category)
    candidates = _creative_combinations(direction, product_type, category)
    exact_counts = Counter(_usage_signature(row) for row in recent_usage)
    scene_counts = Counter(_text(row.get("scene_motif")) for row in recent_usage)
    opening_counts = Counter(_text(row.get("opening_action")) for row in recent_usage)
    persona_counts = Counter(_text(row.get("persona_role")) for row in recent_usage)
    seed_material = f"{product_code}|{_direction_id(direction)}|{direction.get('cluster_id')}"
    seed = int(hashlib.sha256(seed_material.encode("utf-8")).hexdigest()[:12], 16)
    # Batch request idempotency is handled by the frozen batch itself.  A new
    # request for the same product/direction must be allowed to explore the
    # next least-used combination rather than silently replaying an old one.
    scored: List[Tuple[float, int, Dict[str, str]]] = []
    for index, item in enumerate(candidates):
        signature = _usage_signature(item)
        reuse_penalty = 100 * exact_counts[signature]
        axis_penalty = (
            12 * scene_counts[item["scene_motif"]]
            + 8 * opening_counts[item["opening_action"]]
            + 5 * persona_counts[item["persona_role"]]
        )
        tie_break = (seed + index * 7919) % 1000
        scored.append((reuse_penalty + axis_penalty, tie_break, item))
    _, _, selected = min(scored, key=lambda row: (row[0], row[1]))
    structure = direction.get("structure_execution_plan") if isinstance(direction.get("structure_execution_plan"), dict) else {}
    carrier_contract = _carrier_contract(authoritative_carrier(direction))
    snapshot = {
        "recent_usage_count": len(recent_usage),
        "recent_exact_signature_count": exact_counts[_usage_signature(selected)],
        "scene_recent_count": scene_counts[selected["scene_motif"]],
        "opening_recent_count": opening_counts[selected["opening_action"]],
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
    visual_signature = "|".join(
        [selected["persona_role"], selected["scene_motif"], selected["opening_action"], selected["action_grammar"]]
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
        "metadata": {"contract_id": contract.get("contract_id")},
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
