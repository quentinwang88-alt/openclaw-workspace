#!/usr/bin/env python3
"""产品类型归一、冲突仲裁与 prompt 约束构建。"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Sequence


FAMILY_LABELS = {
    "apparel": "服饰",
    "jewelry": "首饰",
    "hair_accessory": "发饰",
    "accessory": "配饰",
    "unknown": "未知",
}

SLOT_LABELS = {
    "body": "身体整体穿着",
    "upper_body": "上半身穿着",
    "lower_body": "下半身穿着",
    "full_body": "全身穿着",
    "wrist": "手腕佩戴",
    "neck": "颈部佩戴",
    "ear": "耳部佩戴",
    "finger": "手指佩戴",
    "hair": "头发 / 发型位置使用",
    "unknown": "未知部位",
}

BUSINESS_CATEGORY_ALIASES = {
    "女装": "apparel",
    "轻上装": "apparel",
    "上装": "apparel",
    "服装": "apparel",
    "服饰": "apparel",
    "首饰": "jewelry",
    "珠宝": "jewelry",
    "饰品": "jewelry",
    "发饰": "hair_accessory",
    "头饰": "hair_accessory",
    "配饰": "accessory",
}


@dataclass(frozen=True)
class TypeDefinition:
    canonical_type: str
    family: str
    slot: str
    aliases: Sequence[str]
    display_name: str
    description: str
    required_terms: Sequence[str]
    forbidden_terms: Sequence[str]


TYPE_DEFINITIONS: Sequence[TypeDefinition] = (
    TypeDefinition(
        canonical_type="womenwear",
        family="apparel",
        slot="body",
        aliases=("女装",),
        display_name="女装",
        description="服饰大类，默认按穿着类商品处理，文案需围绕版型、面料、上身效果展开。",
        required_terms=("穿着", "版型", "面料"),
        forbidden_terms=("项圈", "手镯", "发夹", "耳环"),
    ),
    TypeDefinition(
        canonical_type="light_top",
        family="apparel",
        slot="upper_body",
        aliases=("轻上装", "轻薄上装", "上装"),
        display_name="轻上装",
        description="上半身穿着的轻薄上装，重点描述上身版型、面料、领口和穿着效果。",
        required_terms=("上身", "穿着", "版型", "面料"),
        forbidden_terms=("项圈", "手镯", "发夹", "戴在手上"),
    ),
    TypeDefinition(
        canonical_type="top",
        family="apparel",
        slot="upper_body",
        aliases=("上衣", "t恤", "衬衫", "背心", "针织上衣", "毛衣"),
        display_name="上衣",
        description="上半身穿着单品，重点描述版型、长度、领口、袖型和上身效果。",
        required_terms=("上身", "穿着", "版型"),
        forbidden_terms=("手镯", "项圈", "发饰"),
    ),
    TypeDefinition(
        canonical_type="outerwear",
        family="apparel",
        slot="upper_body",
        aliases=("外套", "开衫", "夹克"),
        display_name="外套",
        description="上半身外搭单品，重点描述廓形、门襟、长度和搭配方式。",
        required_terms=("外搭", "上身", "版型"),
        forbidden_terms=("手镯", "项圈", "发夹"),
    ),
    TypeDefinition(
        canonical_type="dress",
        family="apparel",
        slot="full_body",
        aliases=("连衣裙", "裙装"),
        display_name="连衣裙",
        description="全身穿着类商品，重点描述裙型、长度、领口、腰线和整体穿着效果。",
        required_terms=("上身", "裙型", "穿着"),
        forbidden_terms=("手镯", "项圈", "发夹"),
    ),
    TypeDefinition(
        canonical_type="bottom",
        family="apparel",
        slot="lower_body",
        aliases=("下装", "裤子", "半裙", "短裙"),
        display_name="下装",
        description="下半身穿着类商品，重点描述腰线、裤型或裙型、长度和修饰效果。",
        required_terms=("穿着", "版型", "长度"),
        forbidden_terms=("手镯", "项圈", "发夹"),
    ),
    TypeDefinition(
        canonical_type="necklace",
        family="jewelry",
        slot="neck",
        aliases=("项链",),
        display_name="项链",
        description="颈部佩戴的链式首饰，可描述链条、吊坠、锁骨位置与颈部佩戴效果。",
        required_terms=("颈部", "佩戴", "项链"),
        forbidden_terms=("手腕", "手镯", "发夹"),
    ),
    TypeDefinition(
        canonical_type="choker",
        family="jewelry",
        slot="neck",
        aliases=("项圈", "颈圈", "choker"),
        display_name="项圈",
        description="颈部贴近佩戴的短款颈饰，可描述贴颈、锁骨上方、开口或短链结构。",
        required_terms=("颈部", "贴颈", "佩戴"),
        forbidden_terms=("手腕", "手镯", "发夹"),
    ),
    TypeDefinition(
        canonical_type="bracelet",
        family="jewelry",
        slot="wrist",
        aliases=("手链",),
        display_name="手链",
        description="手腕佩戴的链式腕饰，可描述链条、腕部活动感和手上佩戴效果。",
        required_terms=("手腕", "佩戴", "腕部"),
        forbidden_terms=("颈部", "项圈", "锁骨", "贴颈"),
    ),
    TypeDefinition(
        canonical_type="bangle",
        family="jewelry",
        slot="wrist",
        aliases=("手镯", "手环"),
        display_name="手镯",
        description="手腕佩戴的环状腕饰，可描述单圈、开口、硬挺轮廓和腕部佩戴效果。",
        required_terms=("手腕", "腕部", "佩戴"),
        forbidden_terms=("项圈", "颈圈", "choker", "贴颈", "锁骨", "脖子"),
    ),
    TypeDefinition(
        canonical_type="slim_bangle",
        family="jewelry",
        slot="wrist",
        aliases=("细手圈", "细手环", "细手镯", "细手圈手镯", "开口细手镯"),
        display_name="细手圈",
        description="手腕佩戴的细款单圈腕饰，可描述开口结构、金属细环、腕部佩戴效果，但禁止写成项圈或颈饰。",
        required_terms=("手腕", "腕部", "佩戴在手上"),
        forbidden_terms=("项圈", "颈圈", "choker", "贴颈", "锁骨", "脖子留白"),
    ),
    TypeDefinition(
        canonical_type="ring",
        family="jewelry",
        slot="finger",
        aliases=("戒指",),
        display_name="戒指",
        description="手指佩戴的环状首饰，可描述指间佩戴、戒面和手部细节。",
        required_terms=("手指", "佩戴", "戒指"),
        forbidden_terms=("手腕", "项圈", "发夹"),
    ),
    TypeDefinition(
        canonical_type="earring",
        family="jewelry",
        slot="ear",
        aliases=("耳饰", "耳环", "耳钉", "耳坠", "耳夹"),
        display_name="耳饰",
        description="耳部佩戴首饰，可描述耳垂、耳边线条、耳饰结构和上耳效果。",
        required_terms=("耳部", "佩戴", "耳饰"),
        forbidden_terms=("手腕", "项圈", "发夹"),
    ),
    TypeDefinition(
        canonical_type="claw_clip",
        family="hair_accessory",
        slot="hair",
        aliases=("抓夹", "发抓", "鲨鱼夹"),
        display_name="抓夹",
        description="用于头发固定的发饰，可描述夹持方式、发型位置和发量适配。",
        required_terms=("头发", "固定", "发型"),
        forbidden_terms=("耳饰", "项链", "手镯"),
    ),
    TypeDefinition(
        canonical_type="hair_clip",
        family="hair_accessory",
        slot="hair",
        aliases=("发夹", "边夹", "顶夹"),
        display_name="发夹",
        description="用于头发局部固定的发饰，可描述侧边夹取、顶部固定和发型点缀。",
        required_terms=("头发", "固定", "发型"),
        forbidden_terms=("耳饰", "项链", "手镯"),
    ),
    TypeDefinition(
        canonical_type="headband",
        family="hair_accessory",
        slot="hair",
        aliases=("发箍", "头箍"),
        display_name="发箍",
        description="佩戴在头发或头顶的发饰，可描述头顶佩戴、压发和整体造型效果。",
        required_terms=("头发", "头顶", "佩戴"),
        forbidden_terms=("耳饰", "项链", "手镯"),
    ),
    TypeDefinition(
        canonical_type="scrunchie",
        family="hair_accessory",
        slot="hair",
        aliases=("发圈", "大肠发圈"),
        display_name="发圈",
        description="用于扎发的发饰，可描述绑发、发尾位置和头发造型效果。",
        required_terms=("头发", "扎发", "发型"),
        forbidden_terms=("耳饰", "项链", "手镯"),
    ),
    TypeDefinition(
        canonical_type="hair_tie",
        family="hair_accessory",
        slot="hair",
        aliases=("皮筋", "扎发绳"),
        display_name="扎发绳",
        description="用于束发的发饰，可描述绑发位置、弹性和发型固定方式。",
        required_terms=("头发", "扎发", "固定"),
        forbidden_terms=("耳饰", "项链", "手镯"),
    ),
    TypeDefinition(
        canonical_type="ribbon",
        family="hair_accessory",
        slot="hair",
        aliases=("发带", "蝴蝶结发饰", "丝带发饰"),
        display_name="发带",
        description="用于发型点缀的发饰，可描述蝴蝶结、丝带、绑发或别在头发上的效果。",
        required_terms=("头发", "发型", "点缀"),
        forbidden_terms=("耳饰", "项链", "手镯"),
    ),
    TypeDefinition(
        canonical_type="hair_pin",
        family="hair_accessory",
        slot="hair",
        aliases=("发簪", "发针"),
        display_name="发簪",
        description="用于盘发或固定发型的发饰，可描述插入头发、盘发位置和装饰效果。",
        required_terms=("头发", "固定", "盘发"),
        forbidden_terms=("耳饰", "项链", "手镯"),
    ),
)


TYPE_BY_CANONICAL = {item.canonical_type: item for item in TYPE_DEFINITIONS}
ALIAS_TO_TYPE = {
    alias.casefold(): item.canonical_type
    for item in TYPE_DEFINITIONS
    for alias in item.aliases
}


@dataclass(frozen=True)
class ProductTypeContext:
    raw_product_type: str
    business_category: str
    business_family: str
    canonical_family: str
    canonical_slot: str
    canonical_type: str
    display_type: str
    prompt_label: str
    required_terms: List[str]
    forbidden_terms: List[str]
    source: str = "table"

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ResolvedProductContext(ProductTypeContext):
    vision_family: Optional[str] = None
    vision_slot: Optional[str] = None
    vision_type: Optional[str] = None
    vision_confidence: Optional[float] = None
    conflict_level: str = "none"
    resolution_policy: str = "prefer_table"
    review_required: bool = False
    conflict_reason: Optional[str] = None


@dataclass(frozen=True)
class VisionContext:
    family: str
    slot: str
    canonical_type: str
    confidence: Optional[float] = None


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return (
        value.strip()
        .replace("（", "")
        .replace("）", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
        .casefold()
    )


def _resolve_business_family(business_category: Optional[str]) -> str:
    normalized = _normalize_text(business_category)
    if not normalized:
        return "unknown"
    for alias, family in BUSINESS_CATEGORY_ALIASES.items():
        if _normalize_text(alias) == normalized:
            return family
    return "unknown"


def _fallback_canonical_type(business_family: str, raw_product_type: Optional[str]) -> str:
    normalized_raw = _normalize_text(raw_product_type)
    if normalized_raw == _normalize_text("轻上装"):
        return "light_top"
    if normalized_raw == _normalize_text("女装"):
        return "womenwear"
    if business_family == "apparel":
        return "womenwear"
    if business_family in {"jewelry", "accessory"}:
        return "bangle"
    if business_family == "hair_accessory":
        return "hair_clip"
    return "womenwear"


def _match_canonical_type(raw_product_type: Optional[str], business_family: str) -> Optional[str]:
    normalized = _normalize_text(raw_product_type)
    if not normalized:
        return None

    matched = []
    for alias, canonical_type in ALIAS_TO_TYPE.items():
        if alias == normalized:
            matched.append(canonical_type)

    if not matched:
        return None

    if business_family in {"unknown", "accessory"}:
        return matched[0]

    for canonical_type in matched:
        if TYPE_BY_CANONICAL[canonical_type].family == business_family:
            return canonical_type

    return matched[0]


def normalize_product_type(raw_product_type: Optional[str], business_category: Optional[str] = None) -> ProductTypeContext:
    business_family = _resolve_business_family(business_category)
    canonical_type = _match_canonical_type(raw_product_type, business_family)
    if canonical_type is None:
        canonical_type = _fallback_canonical_type(business_family, raw_product_type)

    definition = TYPE_BY_CANONICAL[canonical_type]
    display_type = raw_product_type.strip() if raw_product_type and raw_product_type.strip() else definition.display_name
    prompt_label = f"{display_type}，属于{SLOT_LABELS.get(definition.slot, definition.slot)}的{definition.description}"

    return ProductTypeContext(
        raw_product_type=raw_product_type or "",
        business_category=business_category or "",
        business_family=business_family,
        canonical_family=definition.family,
        canonical_slot=definition.slot,
        canonical_type=definition.canonical_type,
        display_type=display_type,
        prompt_label=prompt_label,
        required_terms=list(definition.required_terms),
        forbidden_terms=list(definition.forbidden_terms),
    )


def normalize_vision_type(
    vision_type: Optional[str] = None,
    vision_family: Optional[str] = None,
    vision_slot: Optional[str] = None,
    vision_confidence: Optional[float] = None,
) -> Optional[VisionContext]:
    if not any([vision_type, vision_family, vision_slot]):
        return None

    if vision_type:
        normalized = normalize_product_type(vision_type, vision_family)
        return VisionContext(
            family=normalized.canonical_family,
            slot=vision_slot or normalized.canonical_slot,
            canonical_type=normalized.canonical_type,
            confidence=vision_confidence,
        )

    return VisionContext(
        family=vision_family or "unknown",
        slot=vision_slot or "unknown",
        canonical_type="unknown",
        confidence=vision_confidence,
    )


def resolve_product_context(
    raw_product_type: Optional[str],
    business_category: Optional[str] = None,
    vision_type: Optional[str] = None,
    vision_family: Optional[str] = None,
    vision_slot: Optional[str] = None,
    vision_confidence: Optional[float] = None,
) -> ResolvedProductContext:
    table_context = normalize_product_type(raw_product_type, business_category)
    vision_context = normalize_vision_type(
        vision_type=vision_type,
        vision_family=vision_family,
        vision_slot=vision_slot,
        vision_confidence=vision_confidence,
    )

    conflict_level = "none"
    conflict_reason = None
    review_required = False
    resolution_policy = "prefer_table"

    if vision_context:
        if table_context.canonical_family == vision_context.family and table_context.canonical_slot == vision_context.slot:
            if table_context.canonical_type != vision_context.canonical_type:
                conflict_level = "low"
                conflict_reason = "视觉类型与表格类型细分类不同，但佩戴/使用部位一致"
        else:
            conflict_level = "high"
            conflict_reason = "视觉识别与表格类型的族类或佩戴/使用部位冲突"
            review_required = True

    return ResolvedProductContext(
        **table_context.to_dict(),
        vision_family=vision_context.family if vision_context else None,
        vision_slot=vision_context.slot if vision_context else None,
        vision_type=vision_context.canonical_type if vision_context else None,
        vision_confidence=vision_context.confidence if vision_context else None,
        conflict_level=conflict_level,
        resolution_policy=resolution_policy,
        review_required=review_required,
        conflict_reason=conflict_reason,
    )


def build_prompt_contract(context: ProductTypeContext) -> str:
    family_label = FAMILY_LABELS.get(context.canonical_family, context.canonical_family)
    slot_label = SLOT_LABELS.get(context.canonical_slot, context.canonical_slot)
    required_terms = "、".join(context.required_terms) if context.required_terms else "无"
    forbidden_terms = "、".join(context.forbidden_terms) if context.forbidden_terms else "无"

    return (
        f"- 业务大类：{context.business_category or family_label}\n"
        f"- 标准族类：{family_label}\n"
        f"- 最终产品类型：{context.display_type}\n"
        f"- 标准佩戴/使用部位：{slot_label}\n"
        f"- 类型解释：{context.prompt_label}\n"
        f"- 必须围绕这些词展开：{required_terms}\n"
        f"- 禁止出现这些错类词：{forbidden_terms}\n"
        "- 如果图片视觉上与最终产品类型冲突，必须以最终产品类型和标准佩戴/使用部位为准，不得擅自改写到其他身体部位。\n"
        "- 如果图片存在白底、无尺度参照、单张易误判等情况，也必须优先遵循最终产品类型。"
    )
