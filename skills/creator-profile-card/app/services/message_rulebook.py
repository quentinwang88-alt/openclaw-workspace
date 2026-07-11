"""Shared rulebook for creator outreach messages.

This is the single source of truth for message structure, hard-block terms,
CTA strength, and fixed local-language fragments used by prompts and guards.
"""

from __future__ import annotations

from typing import Dict, Iterable, List


PURPOSE_STRUCTURES: Dict[str, List[str]] = {
    "batch_content_opportunity": [
        "内容机会钩子",
        "低成本拍摄场景",
        "产品方向",
        "轻 CTA",
    ],
    "single_product_invitation": [
        "真实观察",
        "商品/机会匹配",
        "具体拍法",
        "低压下一步",
    ],
    "relationship_maintenance": [
        "关系承接",
        "公开内容观察",
        "后续匹配意愿",
        "不索取回复",
    ],
    "follow_up": [
        "承接上次",
        "不催促",
        "给拒绝空间",
        "轻提醒",
    ],
}


HARD_RULES: Dict[str, Dict[str, List[str]]] = {
    "batch_content_opportunity": {
        "zh": [
            "达人名",
            "@",
            "适合你账号",
            "我看了你的视频",
            "我翻了你的视频",
            "给你安排",
            "我这边安排",
            "马上寄样",
            "安排寄样",
            "爆款",
            "高转化",
            "一定出单",
            "流量表现好",
            "曝光增量",
            "曝光",
            "容易涨粉",
            "名额有限",
        ],
        "th": [
            "@",
            "เหมาะกับบัญชีของคุณ",
            "ดูวิดีโอของคุณ",
            "จัดส่งตัวอย่างทันที",
            "ส่งตัวอย่างให้ทันที",
            "ยอดขายปังแน่นอน",
            "ขายดีแน่นอน",
            "จำนวนจำกัด",
        ],
    },
    "single_product_invitation": {
        "zh": [
            "我翻了你的视频",
            "一定出单",
            "爆款",
            "高转化",
            "名额有限",
        ],
        "th": [
            "ยอดขายปังแน่นอน",
            "ขายดีแน่นอน",
            "จำนวนจำกัด",
        ],
    },
    "relationship_maintenance": {
        "zh": [
            "寄样",
            "佣金",
            "商品链接",
            "带货",
            "合作",
            "下单",
            "安排",
        ],
        "th": [
            "ส่งตัวอย่าง",
            "คอมมิชชั่น",
            "ลิงก์สินค้า",
            "ร่วมงาน",
            "จัดให้",
        ],
    },
    "follow_up": {
        "zh": [
            "看到你没回",
            "再确认一下",
            "催一下",
            "名额有限",
            "现在可以安排",
            "马上安排",
            "最后一次",
        ],
        "th": [
            "เห็นว่ายังไม่ตอบ",
            "รีบตอบ",
            "จำนวนจำกัด",
            "จัดให้ตอนนี้",
            "ครั้งสุดท้าย",
        ],
    },
}


SOFTEN_REPLACEMENTS: Dict[str, str] = {
    "很适合": "比较适合",
    "非常适合": "可以考虑",
    "适合你账号": "适合短视频展示",
    "我这边安排": "我再发具体信息",
    "给你安排": "发你具体信息",
    "马上安排": "再发具体信息",
    "马上寄样": "再发具体信息",
    "安排寄样": "再发具体信息",
    "เหมาะกับแผนไหมค่อยคืนข้อความ": "ถ้าคิดว่าแนวนี้โอเค ค่อยทักกลับมาได้",
    "ค่อยคืนข้อความ": "ค่อยทักกลับมาได้",
}


CTA_BY_STAGE: Dict[str, Dict[str, str]] = {
    "batch": {
        "陌生": "如果近期有相关内容方向，可以看看；觉得方向合适再回我，我再发具体信息。",
        "冷": "如果近期有相关内容方向，可以看看；觉得方向合适再回我，我再发具体信息。",
        "温": "如果这批方向你觉得可以，可以先回我，我发具体款式和合作信息给你看。",
    },
    "single": {
        "陌生": "如果你觉得方向可以，我可以先发具体款式和合作信息给你看看。",
        "冷": "如果你觉得方向可以，我可以先发具体款式和合作信息给你看看。",
        "温": "你觉得可以的话，我这边帮你确认样品/合作信息。",
        "热": "我可以先把款式和样品安排发你确认。",
    },
}


TH_BATCH_HOOKS = {
    "轻上装": "รอบนี้เป็นสินค้าแนวเสื้อคลุม/เสื้อเบาๆ",
    "女装": "รอบนี้เป็นสินค้าแฟชั่นผู้หญิงที่ทำคอนเทนต์ได้ง่าย",
    "裤装": "รอบนี้เป็นสินค้าแนวกางเกงที่ถ่ายคอนเทนต์ได้ง่าย",
    "裙装": "รอบนี้เป็นสินค้าแนวเดรส/กระโปรงที่ถ่ายลุคได้ง่าย",
    "发饰": "รอบนี้เป็นสินค้าเครื่องประดับผมที่ทำคอนเทนต์สั้นได้ง่าย",
    "配饰": "รอบนี้เป็นสินค้าเครื่องประดับที่ถ่ายใกล้ๆ แล้วเห็นรายละเอียดชัด",
    "默认": "รอบนี้เป็นสินค้าแนวแฟชั่นที่ทำคอนเทนต์ได้ง่าย",
}

TH_BATCH_CTA = "ถ้าคิดว่าแนวนี้โอเค ค่อยทักกลับมาได้ เดี๋ยวส่งรายละเอียดให้ดูเพิ่มนะคะ"


def hard_terms_for(purpose: str) -> List[str]:
    rules = HARD_RULES.get(purpose, {})
    terms: List[str] = []
    for values in rules.values():
        terms.extend(values)
    return terms


def format_hard_rules_for_prompt(purpose: str) -> str:
    terms = hard_terms_for(purpose)
    if not terms:
        return "无额外硬禁词。"
    return "\n".join(f"- 禁止出现：{term}" for term in terms)


def format_structure_for_prompt(purpose: str) -> str:
    parts = PURPOSE_STRUCTURES.get(purpose, [])
    return " → ".join(parts)


def is_thai_language(target_language: str) -> bool:
    text = (target_language or "").lower()
    return "泰" in text or "thai" in text or "th" == text


def thai_batch_hook(product_category: str) -> str:
    return TH_BATCH_HOOKS.get(product_category or "", TH_BATCH_HOOKS["默认"])


def join_compact(values: Iterable[str], limit: int = 2) -> str:
    cleaned = [str(v).strip() for v in values if str(v or "").strip()]
    return "、".join(cleaned[:limit])
