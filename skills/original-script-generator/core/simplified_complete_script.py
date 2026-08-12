"""Simplified complete-script path for original batch production.

This module intentionally keeps the path thin:

1. freeze product truth + one creative direction;
2. ask one model for the complete visual script;
3. reuse the central voiceover engine;
4. mount the result without another semantic rewrite.

The legacy reality-reference path remains available and unchanged.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Dict, Iterable, List, Tuple


SCRIPT_MODE_LEGACY = "legacy_v2"
SCRIPT_MODE_SIMPLIFIED = "simplified_v1"
CREATIVE_SEED_SCHEMA_VERSION = "simplified-creative-seed-v17-wearable-visual"
VISUAL_SCRIPT_SCHEMA_VERSION = "simplified-complete-visual-script-v8-explicit-display-quantity"
VALIDATION_POLICY_VERSION = "simplified-minimum-gates-v5-action-soft-signal"
VIDEO_BRIEF_SCHEMA_VERSION = "production-video-brief-v8-opening-projection"
VIDEO_RENDER_PROFILE = "UGC_NATIVE_V2_MULTICLIP"

CAPTURE_MODE_CREATOR_SELF_SHOT = "CREATOR_SELF_SHOT"
CAPTURE_MODE_HANDS_PRODUCT_SHARE = "HANDS_PRODUCT_SHARE"
CAPTURE_MODE_STATIC_PRODUCT_RECORD = "STATIC_PRODUCT_RECORD"

CAPTURE_RHYTHM_PROFILE_ENV = "ORIGINAL_SCRIPT_CAPTURE_RHYTHM_PROFILE"
CAPTURE_RHYTHM_MULTICLIP = "NATIVE_MULTI_CLIP_V1"
CAPTURE_RHYTHM_LEGACY = "LEGACY_ONE_TAKE"
CAPTURE_RHYTHM_SCHEMA_VERSION = "capture-rhythm-contract-v2-validated-boundaries"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, material: Any) -> str:
    from core.outfit_template_provider import without_outfit_display_metadata

    material = without_outfit_display_metadata(material)
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20].upper()


def _dedupe_text(values: Iterable[Any], limit: int = 8) -> List[str]:
    result: List[str] = []
    for value in values:
        text = _text(value)
        if text and text.upper() != "UNAVAILABLE" and text not in result:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def build_capture_rhythm_contract(
    *,
    capture_mode: str,
    macro_structure: Iterable[Any],
) -> Dict[str, Any]:
    """Compile one small public capture contract without another model call.

    Structure beats keep narrative authority.  This contract only says how
    many separately recorded phone clips should carry those beats.  It keeps
    person/product/scene continuity while no longer equating authenticity with
    one fixed camera position.
    """

    configured = _text(os.environ.get(CAPTURE_RHYTHM_PROFILE_ENV)).upper()
    if configured in {"LEGACY", "LEGACY_ONE_TAKE", "ONE_TAKE"}:
        profile = CAPTURE_RHYTHM_LEGACY
    else:
        profile = CAPTURE_RHYTHM_MULTICLIP
    mode = _text(capture_mode).upper() or CAPTURE_MODE_CREATOR_SELF_SHOT
    beats = [_text(value).upper() for value in macro_structure if _text(value)]
    if profile == CAPTURE_RHYTHM_LEGACY:
        target_units = 1
        grammar = "CONTINUOUS_SINGLE_PHONE_VIEW"
    else:
        target_units = 3 if mode == CAPTURE_MODE_CREATOR_SELF_SHOT else 2
        if any(value in {"USE", "USE_PROCESS", "SCENE_USE"} for value in beats):
            grammar = "CONTEXT_TO_USE_TO_RESULT"
        elif any("DETAIL" in value for value in beats):
            grammar = "DETAIL_TO_RELATION_TO_CONTEXT"
        else:
            grammar = "OPENING_TO_PROOF_TO_CONTEXT"
    return {
        "schema_version": CAPTURE_RHYTHM_SCHEMA_VERSION,
        "profile": profile,
        "capture_unit_count": target_units,
        "capture_grammar": grammar,
        "edit_style": (
            "CONTINUOUS_RECORDING"
            if profile == CAPTURE_RHYTHM_LEGACY
            else "NATIVE_HARD_CUT"
        ),
        "continuity_lock": [
            "SAME_CREATOR",
            "SAME_PRODUCT",
            "SAME_OUTFIT",
            "SAME_LOCATION",
            "SAME_TIME",
            "SAME_DEVICE",
        ],
        "commercial_camera_forbidden": True,
        "hard_required": False,
        "fallback_policy": "DETERMINISTIC_GROUP_NO_RETRY",
    }


def compile_capture_units(
    storyboard: Iterable[Any],
    contract: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Attach authoritative capture-unit ids and return compact unit metadata.

    The model may suggest ids, but deterministic grouping prevents prompt drift
    from sending a script back into a repair loop.  Four-to-six structural
    passages become two or three actual phone recordings.
    """

    shots = [dict(value) for value in storyboard if isinstance(value, dict)]
    if not shots:
        return [], []
    profile = _text(contract.get("profile")).upper()
    requested = int(contract.get("capture_unit_count") or 1)
    unit_count = 1 if profile == CAPTURE_RHYTHM_LEGACY else max(2, requested)
    unit_count = min(unit_count, len(shots))

    grouping_source = "DETERMINISTIC_FALLBACK"
    suggested_sizes: List[int] = []
    if unit_count > 1:
        # The visual author sees the actual action and camera prose, so its
        # proposed boundaries are semantically better than blind equal-sized
        # partitions.  Preserve them only when they form exactly the frozen
        # number of contiguous, correctly numbered units.  Any malformed or
        # interleaved suggestion still falls back to deterministic grouping.
        expected_unit = 1
        current_size = 0
        suggestion_valid = True
        for shot_index, shot in enumerate(shots):
            unit_id = _text(shot.get("capture_unit_id")).upper()
            matched = re.fullmatch(r"CU_(\d{2})", unit_id)
            if not matched:
                suggestion_valid = False
                break
            number = int(matched.group(1))
            if shot_index == 0:
                if number != 1:
                    suggestion_valid = False
                    break
                current_size = 1
                continue
            if number == expected_unit:
                current_size += 1
                if shot.get("starts_new_take") is True:
                    suggestion_valid = False
                    break
                continue
            if number == expected_unit + 1:
                if shot.get("starts_new_take") is False:
                    suggestion_valid = False
                    break
                suggested_sizes.append(current_size)
                expected_unit = number
                current_size = 1
                continue
            suggestion_valid = False
            break
        if suggestion_valid and current_size:
            suggested_sizes.append(current_size)
        if (
            suggestion_valid
            and len(suggested_sizes) == unit_count
            and sum(suggested_sizes) == len(shots)
            and all(size > 0 for size in suggested_sizes)
        ):
            grouping_source = "MODEL_BOUNDARIES_VALIDATED"

    if grouping_source == "MODEL_BOUNDARIES_VALIDATED":
        group_sizes = suggested_sizes
    elif unit_count == 1:
        group_sizes = [len(shots)]
    elif unit_count == 2:
        first_size = max(1, len(shots) // 2)
        group_sizes = [first_size, len(shots) - first_size]
    else:
        # Keep the opening short, then balance proof/use and context/ending.
        remaining = len(shots) - 1
        middle_size = (remaining + 1) // 2
        group_sizes = [1, middle_size, remaining - middle_size]
        group_sizes = [value for value in group_sizes if value > 0]

    roles_by_count = {
        1: ["CONTINUOUS_SHARE"],
        2: ["OPENING", "PROOF_AND_CONTEXT"],
        3: ["OPENING", "CORE_PROOF_OR_USE", "CONTEXT_END"],
    }
    framing_by_count = {
        1: ["沿用同一普通手机关系"],
        2: [
            "独立录制抓人结果景或商品主体景",
            "同一地点重新放置手机，录制证明、使用关系或生活收束，景别与上一段有可感知差异",
        ],
        3: [
            "独立录制抓人结果景或商品主体景",
            "同一地点重新放置手机，录制商品证明或使用关系，景别与上一段有可感知差异",
            "同一地点补录人物、商品与生活场景关系，完成自然收束",
        ],
    }
    roles = roles_by_count[len(group_sizes)]
    framing = framing_by_count[len(group_sizes)]
    units: List[Dict[str, Any]] = []
    annotated: List[Dict[str, Any]] = []
    cursor = 0
    for unit_index, size in enumerate(group_sizes, 1):
        unit_id = f"CU_{unit_index:02d}"
        group = shots[cursor:cursor + size]
        shot_numbers = []
        narrative_roles = []
        for local_index, shot in enumerate(group):
            item = dict(shot)
            item["capture_unit_id"] = unit_id
            item["starts_new_take"] = local_index == 0
            item["edit_before"] = (
                "START"
                if unit_index == 1 and local_index == 0
                else "DIRECT_CUT"
                if local_index == 0
                else "CONTINUE"
            )
            item["capture_unit_role"] = roles[unit_index - 1]
            annotated.append(item)
            shot_numbers.append(int(item.get("shot_no") or cursor + local_index + 1))
            role = _text(item.get("narrative_role")).upper()
            if role and role not in narrative_roles:
                narrative_roles.append(role)
        units.append(
            {
                "capture_unit_id": unit_id,
                "order": unit_index,
                "shot_numbers": shot_numbers,
                "narrative_roles": narrative_roles,
                "unit_role": roles[unit_index - 1],
                "framing_guidance": framing[unit_index - 1],
                "edit_before": "START" if unit_index == 1 else "DIRECT_CUT",
                "grouping_source": grouping_source,
            }
        )
        cursor += size
    return annotated, units


def _anchor_segments(value: Any) -> List[str]:
    """Split an approved compound anchor without authorizing loose substrings."""

    text = _text(value)
    if not text:
        return []
    return _dedupe_text(re.split(r"[，,；;。]", text), limit=12)


def _anchor_is_authorized(candidate: Any, approved_anchors: Iterable[str]) -> bool:
    """Accept an exact anchor or exact clauses from one approved compound anchor."""

    candidate_text = _text(candidate)
    if not candidate_text:
        return False
    candidate_segments = set(_anchor_segments(candidate_text))
    for approved in approved_anchors:
        approved_text = _text(approved)
        if candidate_text == approved_text:
            return True
        approved_segments = set(_anchor_segments(approved_text))
        if candidate_segments and candidate_segments.issubset(approved_segments):
            return True
    return False


def _anchor_texts(anchor_card: Dict[str, Any], key: str) -> List[str]:
    values: List[str] = []
    for item in anchor_card.get(key) or []:
        if isinstance(item, dict):
            values.append(
                item.get("anchor")
                or item.get("anchor_text")
                or item.get("name")
                or item.get("value")
            )
        else:
            values.append(item)
    return _dedupe_text(values)


_BUTTON_COUNT_TOKEN = r"(?:\d+|[一二三四五六七八九十两]+)"
_SCARF_CANONICAL_TYPES = {"scarf", "winter_scarf", "silk_scarf", "headscarf"}
_WRIST_ACCESSORY_TYPES = {"bracelet", "bangle", "slim_bangle"}
_HAIR_ACCESSORY_TYPES = {
    "hair_accessory_generic", "claw_clip", "hair_clip", "headband",
    "scrunchie", "hair_tie", "ribbon", "hair_pin",
}
_OTHER_ACCESSORY_TYPES = {"earring", "ring", "necklace", "choker"}
_COLOR_TERMS = (
    "黑", "白", "灰", "米", "棕", "咖", "红", "粉", "橙", "黄", "绿", "蓝",
    "紫", "金", "银", "卡其", "驼色", "藏青", "酒红", "抹茶", "奶油",
)


def _visible_button_count(evidence_text: str) -> str:
    """Return only a count explicitly tied to a visible/front button anchor."""

    patterns = (
        rf"(?:左侧|右侧)[^，；。]{{0,16}}?可见[^，；。]{{0,8}}?({_BUTTON_COUNT_TOKEN})\s*颗?[^，；。]{{0,10}}?(?:扣子|纽扣|按扣|圆扣|纹理扣|扣)",
        rf"(?:正面|前襟)[^，；。]{{0,14}}?({_BUTTON_COUNT_TOKEN})\s*颗?[^，；。]{{0,10}}?(?:扣子|纽扣|按扣|圆扣|纹理扣|扣)",
    )
    for pattern in patterns:
        match = re.search(pattern, evidence_text)
        if match:
            return _text(match.group(1))
    return ""


def _visible_closure_contract(evidence_text: str) -> Dict[str, Any]:
    """Compile only visually explicit single-row closure semantics.

    A hidden counterpart is a closure mechanism, not a second visible button
    row.  Keeping that distinction structured prevents later video-prompt
    prose from accidentally literalising hidden snaps as double-breasted
    buttons.  Unknown layouts stay unavailable rather than being inferred.
    """

    if not evidence_text or "双排" in evidence_text:
        return {"status": "UNAVAILABLE"}
    hidden_counterpart = "暗扣" in evidence_text and any(
        token in evidence_text for token in ("可见", "隐藏", "不外露")
    )
    single_front_signal = any(
        token in evidence_text for token in ("单排", "单列", "竖向前襟", "竖直前襟")
    ) or hidden_counterpart
    if not single_front_signal:
        return {"status": "UNAVAILABLE"}
    count = _visible_button_count(evidence_text)
    visible_description = (
        f"前襟只允许一列{count}颗可见扣子"
        if count
        else "前襟只允许一列可见扣子"
    )
    hidden_description = (
        "另一侧暗扣属于隐藏闭合件，不得显示为第二列可见纽扣"
        if hidden_counterpart
        else ""
    )
    return {
        "status": "AVAILABLE",
        "layout": "SINGLE_VISIBLE_VERTICAL_ROW",
        "visible_button_count": count,
        "hidden_counterpart": hidden_counterpart,
        "visible_description": visible_description,
        "hidden_description": hidden_description,
        "authority": "APPROVED_ANCHOR",
    }


def _matching_anchor_segments(
    anchors: Iterable[str],
    tokens: Iterable[str],
    *,
    limit: int = 4,
) -> List[str]:
    return _dedupe_text(
        (
            segment
            for anchor in anchors
            for segment in _anchor_segments(anchor)
            if any(token in segment for token in tokens)
        ),
        limit=limit,
    )


def _scarf_identity_contract(
    product_truth: Dict[str, Any],
    *,
    anchors: Iterable[str],
) -> Dict[str, Any]:
    """Group only explicitly approved scarf evidence into a compact card."""

    anchor_list = list(anchors)
    shape_anchors = _matching_anchor_segments(
        anchor_list,
        ("方形", "方巾", "正方", "长条", "长方", "矩形", "三角"),
    )
    color_anchors = _matching_anchor_segments(anchor_list, _COLOR_TERMS)
    pattern_anchors = _matching_anchor_segments(
        anchor_list,
        ("图案", "印花", "花纹", "格纹", "条纹", "波点", "纯色", "撞色", "渐变"),
    )
    edge_anchors = _matching_anchor_segments(
        anchor_list,
        ("流苏", "包边", "边框", "卷边", "毛边", "锁边", "滚边"),
    )
    logo_text_anchors = _matching_anchor_segments(
        anchor_list,
        ("logo", "LOGO", "Logo", "文字", "字母", "品牌标识"),
    )
    canonical_type = _text(product_truth.get("canonical_product_type"))
    wearing_zone = {
        "winter_scarf": "NECK_SHOULDER",
        "silk_scarf": "NECK_UPPER_BODY",
        "headscarf": "HEAD_HAIR",
        "scarf": "NECK_SHOULDER",
    }.get(canonical_type, "UNAVAILABLE")
    return {
        "status": "AVAILABLE",
        "canonical_product_type": canonical_type,
        "shape_anchors": shape_anchors or ["UNAVAILABLE"],
        "color_anchors": color_anchors or ["UNAVAILABLE"],
        "pattern_anchors": pattern_anchors or ["UNAVAILABLE"],
        "edge_anchors": edge_anchors or ["UNAVAILABLE"],
        "logo_text_anchors": logo_text_anchors or ["UNAVAILABLE"],
        "placement_authority": wearing_zone,
        "material_authority": "UNAVAILABLE_UNLESS_APPROVED_ANCHOR",
        "authority": "APPROVED_ANCHORS_AND_REFERENCE_IMAGE",
    }


def build_product_identity_lock(product_truth: Dict[str, Any]) -> Dict[str, Any]:
    """Compile a compact, deterministic product-identity hand-off.

    The lock deliberately uses only already-approved identity/detail anchors.
    It does not call a model and does not invent missing garment properties.
    Reference-image authority covers attributes that are visually obvious but
    not safely expressible as new structured facts.
    """

    identity_anchors = _dedupe_text(product_truth.get("identity_anchors") or [], limit=8)
    visible_details = _dedupe_text(product_truth.get("visible_detail_anchors") or [], limit=6)
    product_identity = _text(product_truth.get("product_identity"))
    must_preserve = _dedupe_text(
        [product_identity, *identity_anchors],
        limit=8,
    )
    evidence_text = "；".join([*must_preserve, *visible_details])
    canonical_type = _text(product_truth.get("canonical_product_type"))
    raw_quantity_contract = (
        product_truth.get("display_quantity_contract")
        if isinstance(product_truth.get("display_quantity_contract"), dict)
        else {}
    )
    required_quantity_text = _text(
        raw_quantity_contract.get("required_display_count")
    )
    quantity_authorized = (
        canonical_type in _WRIST_ACCESSORY_TYPES
        and _text(raw_quantity_contract.get("status")) == "AUTHORIZED"
        and _text(raw_quantity_contract.get("mode")) == "SAME_SKU_STACK"
        and required_quantity_text in {"2", "3"}
    )
    if canonical_type in _SCARF_CANONICAL_TYPES:
        scarf_contract = _scarf_identity_contract(
            product_truth,
            anchors=[*must_preserve, *visible_details],
        )
        must_not_change = [
            "禁止把商品替换成相似款或根据常见款式重新设计",
            "禁止改变参考图和已批准锚点中的颜色、图案布局、边框、流苏或整体形状",
            "禁止把方形商品改成长条形，或把长条形商品改成方形",
            "禁止新增参考图和已批准锚点中没有的Logo、文字或装饰",
        ]
        if canonical_type == "silk_scarf":
            must_not_change.append(
                "产品类型‘丝巾’不授权真丝、桑蚕丝、冰凉或亲肤材质表现"
            )
        if canonical_type == "headscarf":
            must_not_change.append(
                "禁止根据商品外观新增宗教、民族或文化身份"
            )
        return {
            "reference_image_is_authority": True,
            "priority": "HIGHEST",
            "must_preserve": must_preserve,
            "critical_visible_details": visible_details,
            "must_not_change": _dedupe_text(must_not_change, limit=7),
            "visible_closure_contract": {"status": "NOT_APPLICABLE"},
            "scarf_identity_contract": scarf_contract,
            "compiler_version": "product-identity-lock-v3-scarf",
        }
    if canonical_type in {
        *_WRIST_ACCESSORY_TYPES,
        *_HAIR_ACCESSORY_TYPES,
        *_OTHER_ACCESSORY_TYPES,
    }:
        must_not_change = [
            "禁止把商品替换成相似款或根据常见配饰重新设计",
            "禁止改变参考图和已批准锚点中的配饰类型、整体形状、颜色、相对尺寸或装饰布局",
            "禁止新增参考图和已批准锚点中没有的宝石、链条、吊坠、文字或Logo",
        ]
        if canonical_type in _WRIST_ACCESSORY_TYPES:
            if quantity_authorized:
                required_count = int(required_quantity_text)
                must_not_change.extend([
                    "禁止把已确认的手链、手镯或细手圈改成另一类腕饰；禁止改变环体或链体的相对宽窄、开口",
                    f"本条已由运营卖点明确授权同款叠戴，目标手腕必须始终佩戴{required_count}只同款商品；不得中途增加、减少、摘下或改到另一只手",
                    "除这组已授权同款叠戴外，不得生成其他腕饰、手表或背景中的重复商品",
                ])
            else:
                must_not_change.extend([
                    "禁止把已确认的手链、手镯或细手圈改成另一类腕饰；禁止改变环体或链体的相对宽窄、开口和佩戴数量",
                    "商品只能出现在手腕位置；不得改成项圈、颈饰、戒指或佩戴在另一只手上的重复商品",
                    "未经运营数量授权，禁止新增第二只同款商品或自行设计叠戴",
                ])
        elif canonical_type in _HAIR_ACCESSORY_TYPES:
            must_not_change.extend([
                "禁止把已确认的抓夹、发夹、发圈、发带或发针改成另一种发饰；齿梳、夹体、装饰布局和相对大小必须服从参考图",
                "禁止在头发另一侧、手中或背景中生成第二个同款商品；不得新增参考图没有的蝴蝶结、花朵、珍珠或宝石",
            ])
        elif canonical_type == "earring":
            must_not_change.append(
                "耳饰的单双关系、连接结构、部件顺序和相对长度只服从已批准锚点与参考图"
            )
        return {
            "reference_image_is_authority": True,
            "priority": "HIGHEST",
            "must_preserve": must_preserve,
            "critical_visible_details": visible_details,
            "must_not_change": _dedupe_text(must_not_change, limit=7),
            "visible_closure_contract": {"status": "NOT_APPLICABLE"},
            "display_quantity_contract": (
                dict(raw_quantity_contract) if quantity_authorized else {
                    "status": "SINGLE_PRODUCT_DEFAULT",
                    "required_display_count": 1,
                    "authority": "DEFAULT_IDENTITY_SAFETY",
                }
            ),
            "compiler_version": "product-identity-lock-v4-explicit-quantity",
        }
    closure_contract = _visible_closure_contract(evidence_text)
    if closure_contract.get("status") == "AVAILABLE":
        must_preserve = _dedupe_text(
            [
                *must_preserve,
                closure_contract.get("visible_description"),
                closure_contract.get("hidden_description"),
            ],
            limit=10,
        )
    must_not_change = [
        "禁止把商品替换成相似款或根据常见款式重新设计",
        "禁止改变参考图中的颜色、版型、衣长、领型、前襟和袖口结构",
    ]

    has_button = any(token in evidence_text for token in ("扣子", "纽扣", "按扣", "圆扣", "纹理扣"))
    explicit_button_count = bool(
        re.search(
            r"(?:\d+|[一二三四五六七八九十两]+)[^，；。]{0,12}(?:扣子|纽扣|按扣|圆扣|纹理扣|扣)",
            evidence_text,
        )
    )
    if has_button:
        must_not_change.append("扣子数量、位置、排列方式和间距必须与参考图一致")
        if explicit_button_count:
            must_not_change.append("禁止增加、减少或改写任何已经明确数量的扣子")
        if closure_contract.get("status") == "AVAILABLE":
            must_not_change.append("禁止将参考图中的前襟扣子改成双排扣")
            must_not_change.append("禁止左右对称生成两列可见纽扣")
            if closure_contract.get("hidden_counterpart"):
                must_not_change.append("禁止把隐藏暗扣画成外露纽扣")

    if "拉链" in evidence_text:
        must_not_change.append("禁止把参考图中的拉链替换成纽扣、暗扣或其他闭合结构")

    return {
        "reference_image_is_authority": True,
        "priority": "HIGHEST",
        "must_preserve": must_preserve,
        "critical_visible_details": visible_details,
        "must_not_change": _dedupe_text(must_not_change, limit=9),
        "visible_closure_contract": closure_contract,
        "compiler_version": "product-identity-lock-v2",
    }


def _macro_structure(contract: Dict[str, Any]) -> List[str]:
    hard = contract.get("hard_constraints") if isinstance(contract.get("hard_constraints"), dict) else {}
    sequence = hard.get("beat_sequence") or contract.get("beat_sequence") or []
    result = [
        _text(item) for item in sequence[:8]
        if _text(item) and _text(item).upper() != "UNAVAILABLE"
    ] if isinstance(sequence, list) else []
    if result:
        return result
    identity = contract.get("direction_identity") if isinstance(contract.get("direction_identity"), dict) else {}
    family = _text(identity.get("macro_family_key") or hard.get("macro_family_key"))
    return [part.strip() for part in family.split(">") if part.strip()] or ["HOOK", "PROOF"]


def _is_apparel(product_type: str, top_category: str, anchor_card: Dict[str, Any]) -> bool:
    material = " ".join(
        [
            _text(product_type),
            _text(top_category),
            _text(anchor_card.get("product_type")),
            _text(anchor_card.get("top_category")),
            json.dumps(anchor_card.get("category_execution_contract") or {}, ensure_ascii=False),
        ]
    ).lower()
    return any(
        token in material
        for token in (
            "女装", "男装", "服装", "外套", "上衣", "裙", "裤", "衬衫",
            "夹克", "针织", "apparel", "jacket", "shirt", "dress", "coat",
        )
    )


def _opening_visual_job(
    product_truth: Dict[str, Any],
    *,
    presentation: str,
    requested_hook_id: str,
) -> Dict[str, Any]:
    """Compile one soft first-three-second viewing job from the frozen value."""

    argument = (
        product_truth.get("selling_argument")
        if isinstance(product_truth.get("selling_argument"), dict)
        else {}
    )
    mode = _text(presentation).upper()
    hook_id = _text(requested_hook_id).upper()
    claim_type = _text(argument.get("claim_type")).lower()
    if mode == "STATIC_PRODUCT":
        job = "PRODUCT_FIRST"
    elif mode == "HANDS_ONLY":
        job = "SHOW_DETAIL"
    elif claim_type == "visual_result":
        job = "SHOW_RESULT"
    elif hook_id == "DETAIL_SURPRISE":
        job = "SHOW_DETAIL"
    else:
        job = "SHOW_RESULT"

    guidance = {
        "SHOW_RESULT": "前三秒优先让穿着后的整体结果、轮廓或比例清楚可见",
        "SHOW_DETAIL": "前三秒优先让一个已确认且与本条价值相关的可见细节清楚可见",
        "SHOW_USE_SCENE": "前三秒优先让本条卖点对应的使用场景与完整商品关系成立",
        "PRODUCT_FIRST": "前三秒可以先看清静置商品本身，再按结构进入后续画面",
    }[job]
    return {
        "job": job,
        "first_window_seconds": [0, 3],
        "guidance_zh": guidance,
        "authority": "SOFT_CREATIVE_GUIDANCE",
        "must_not_force": ["前后对比", "身体缺点特写", "情绪反转表演", "额外剧情动作"],
    }


def _preferred_presentation(
    *,
    source_carrier: str,
    bundle: Dict[str, Any],
    product_type: str,
    top_category: str,
    anchor_card: Dict[str, Any],
) -> Tuple[str, str]:
    carrier = _text(source_carrier).upper()
    if carrier in {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"}:
        return "PERSON_ON_CAMERA", "沿用计划阶段冻结的真人承载"
    if carrier in {"HAND_ONLY", "HANDS_ONLY"}:
        return "HANDS_ONLY", "沿用计划阶段冻结的手部承载"
    if carrier == "STATIC_PRODUCT":
        return "STATIC_PRODUCT", "沿用计划阶段冻结的静物承载"
    return "PERSON_ON_CAMERA", "结构承载未知，服饰原创沿用既有真人兜底"


def _capture_mode_for_presentation(presentation: str) -> str:
    """Resolve who owns the camera without changing the routed carrier."""

    return {
        "PERSON_ON_CAMERA": CAPTURE_MODE_CREATOR_SELF_SHOT,
        "HANDS_ONLY": CAPTURE_MODE_HANDS_PRODUCT_SHARE,
        "STATIC_PRODUCT": CAPTURE_MODE_STATIC_PRODUCT_RECORD,
    }.get(_text(presentation).upper(), CAPTURE_MODE_CREATOR_SELF_SHOT)


def _visual_selling_argument_view(raw_argument: Dict[str, Any]) -> Dict[str, Any]:
    """Build the only selling-argument view exposed to the visual model.

    Operator wording remains in the frozen content bundle for the central
    voiceover.  The visual generator receives IDs, governance semantics and a
    normalized creative value only, so source rhetoric cannot turn into a
    character biography or scene premise.
    """

    operator_wording_present = bool(_text(raw_argument.get("operator_expression")))
    creative_value = _text(raw_argument.get("creative_core_value"))
    if not creative_value and not operator_wording_present:
        # Formal legacy strategies were already written as creative briefs and
        # do not carry a raw operator sentence.
        creative_value = _text(raw_argument.get("core_value"))
    return {
        "argument_id": _text(raw_argument.get("argument_id")),
        "source_argument_id": _text(raw_argument.get("source_argument_id")),
        "source_claim_ids": list(raw_argument.get("source_claim_ids") or []),
        "status": _text(raw_argument.get("status")),
        "mapping_status": _text(raw_argument.get("mapping_status")),
        "claim_type": _text(raw_argument.get("claim_type")),
        "claim_theme": _text(raw_argument.get("claim_theme")),
        "allowed_strength": _text(raw_argument.get("allowed_strength")),
        "visual_dependency": _text(raw_argument.get("visual_dependency")) or "FLEXIBLE",
        "compatible_carriers": list(raw_argument.get("compatible_carriers") or []),
        "proof_subject": _text(raw_argument.get("proof_subject")) or "GENERAL_EXPRESSION",
        "concept_ids": list(raw_argument.get("concept_ids") or []),
        "argument_theme": _text(raw_argument.get("argument_theme")),
        "primary_demonstration_mode": _text(
            raw_argument.get("primary_demonstration_mode")
        ),
        "supported_demonstration_modes": list(
            raw_argument.get("supported_demonstration_modes") or []
        ),
        "evidence_mode": _text(raw_argument.get("evidence_mode")),
        "preferred_action_mode": _text(raw_argument.get("preferred_action_mode")),
        "display_quantity_contract": dict(
            raw_argument.get("display_quantity_contract") or {}
        ),
        "demonstration_policy": _text(raw_argument.get("demonstration_policy")),
        "creative_core_value": creative_value,
        # Compatibility alias for the prompt.  It is normalized creative
        # semantics, never the reviewed operator sentence.
        "core_value": creative_value,
        "core_proof_claim_keys": list(raw_argument.get("core_proof_claim_keys") or []),
        "optional_visual_claim_keys": list(raw_argument.get("optional_visual_claim_keys") or []),
        "expression_boundary": "RAW_OPERATOR_WORDING_RESERVED_FOR_CENTRAL_VOICEOVER",
    }


_WEARER_TERMS = (
    "上身", "试穿", "穿搭", "搭配", "腰线", "身材", "版型", "比例",
    "穿起来", "佩戴效果", "wearer", "try-on", "styling",
)


def _claim_needs_wearer(value: Any) -> bool:
    text = _text(value).lower()
    return any(term in text for term in _WEARER_TERMS)


def _reference_is_compatible(presentation: str, reference_carrier: str) -> bool:
    carrier = _text(reference_carrier).upper()
    allowed = {
        "PERSON_ON_CAMERA": {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"},
        "STATIC_PRODUCT": {"STATIC_PRODUCT"},
        "HANDS_ONLY": {"HAND_ONLY", "HANDS_ONLY", "MIXED"},
    }
    return carrier in allowed.get(presentation, set())


def _split_action_grammar(value: Any) -> List[str]:
    return _dedupe_text(
        re.split(r"\s*(?:→|->|—>|＞|>)\s*", _text(value)),
        limit=5,
    )


def _compile_action_design(
    *,
    creative_contract: Dict[str, Any],
    structure_contract: Dict[str, Any],
    presentation: str,
    carrier_execution: Dict[str, Any],
    proof_subject: str,
    preferred_action_mode: str = "",
) -> Dict[str, Any]:
    """Freeze one executable action spine without adding another model stage.

    Category adapters own safe product interactions.  The creative contract's
    existing action grammar remains a supporting real-life movement instead
    of being expanded into a second checklist.  The choice is deterministic,
    so retries and snapshot replays keep the same action.
    """

    grammar = _split_action_grammar(creative_contract.get("action_grammar"))
    opening = _text(creative_contract.get("opening_action"))
    capabilities = [
        dict(item)
        for item in carrier_execution.get("interaction_capabilities") or []
        if isinstance(item, dict) and _text(item.get("interaction_id"))
    ]
    macro = _macro_structure(structure_contract)
    proof = _text(proof_subject).upper()
    selected: Dict[str, Any] = {}
    if capabilities:
        preferred_mode = _text(
            preferred_action_mode
            or carrier_execution.get("preferred_action_mode")
        ).upper()
        if not preferred_mode and "USE_PROCESS" in macro:
            preferred_mode = "SIMPLE_WEAR_PROCESS"
        elif not preferred_mode and (proof == "PRODUCT_DETAIL" or any(
            term in "；".join(grammar)
            for term in ("细节", "局部", "图案", "边缘", "拿近", "近看")
        )):
            preferred_mode = "DETAIL_SHOW"
        preferred = [
            item
            for item in capabilities
            if _text(item.get("primary_action_mode")).upper() == preferred_mode
        ]
        pool = preferred or capabilities
        material = {
            "opening": opening,
            "grammar": grammar,
            "macro": macro,
            "proof": proof,
            "presentation": presentation,
            "capabilities": [item.get("interaction_id") for item in pool],
        }
        digest = hashlib.sha256(
            json.dumps(material, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).digest()
        selected = dict(pool[digest[0] % len(pool)])

    if selected:
        supporting = "→".join(grammar)
        selected.update(
            {
                "schema_version": "action-design-v1",
                "supporting_scene_action": supporting,
                "source": "CATEGORY_CAPABILITY",
                "selection_policy": "action-variety-v1",
                "hard_required": False,
            }
        )
    else:
        # Apparel and legacy categories continue to use the already allocated
        # creative grammar.  This adds structure, not a new action invention.
        start = grammar[0] if grammar else opening
        core = grammar[1] if len(grammar) > 1 else (opening or start)
        end = grammar[-1] if len(grammar) > 1 else "保持商品清楚可见，自然结束这一小段分享"
        selected = {
            "schema_version": "action-design-v1",
            "interaction_id": "CREATIVE_ACTION_GRAMMAR",
            "primary_action_mode": (
                "HANDHELD_PRODUCT"
                if presentation in {"HANDS_ONLY", "STATIC_PRODUCT"}
                else "LIFESTYLE_USE"
            ),
            "start_state": start or "商品和当前人物或展示面关系已经建立",
            "core_action": core or "保持同一生活时刻，自然展示商品",
            "end_state": end,
            "supporting_scene_action": "",
            "risk_tier": "LOW",
            "action_keywords": _dedupe_text([*grammar, opening], limit=4),
            "source": "CREATIVE_ACTION_GRAMMAR",
            "selection_policy": "action-variety-v1",
            "hard_required": False,
        }
    selected["action_signature"] = _stable_id(
        "ACT_",
        {
            "interaction_id": selected.get("interaction_id"),
            "start": selected.get("start_state"),
            "core": selected.get("core_action"),
            "end": selected.get("end_state"),
            "supporting": selected.get("supporting_scene_action"),
        },
    )
    return selected


def build_simplified_creative_seed(
    *,
    anchor_card: Dict[str, Any],
    structure_contract: Dict[str, Any],
    content_bundle: Dict[str, Any],
    creative_contract: Dict[str, Any],
    execution_reference: Dict[str, Any],
    requested_hook_id: str,
    content_angle_key: str,
    relationship_device: str = "",
    product_type: str = "",
    top_category: str = "",
    category_execution_extension: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Freeze only the information needed by the simplified generator."""

    hard = structure_contract.get("hard_constraints") if isinstance(structure_contract.get("hard_constraints"), dict) else {}
    source_carrier = _text(hard.get("content_carrier") or execution_reference.get("content_carrier"))
    presentation, presentation_reason = _preferred_presentation(
        source_carrier=source_carrier,
        bundle=content_bundle,
        product_type=product_type,
        top_category=top_category,
        anchor_card=anchor_card,
    )
    capture_mode = _capture_mode_for_presentation(presentation)
    if category_execution_extension is None:
        from core.category_execution import compile_category_execution_extension

        category_execution_extension = compile_category_execution_extension(
            product_type=product_type,
            top_category=top_category,
            anchor_card=anchor_card,
        )
    else:
        category_execution_extension = dict(category_execution_extension or {})
    carrier_specific_execution: Dict[str, Any] = {}
    if category_execution_extension:
        from core.category_execution import resolve_category_carrier_execution

        carrier_specific_execution = resolve_category_carrier_execution(
            category_execution_extension,
            presentation_mode=presentation,
        )
    claim_atoms = [
        {
            "claim_key": _text(item.get("claim_key")),
            "fact_text": _text(item.get("fact_text")),
            "role": _text(item.get("role")) or "visual_proof",
            "semantic_group": _text(item.get("semantic_group")),
        }
        for item in content_bundle.get("claim_atoms") or []
        if isinstance(item, dict) and _text(item.get("claim_key")) and _text(item.get("fact_text"))
    ]
    if presentation == "STATIC_PRODUCT":
        static_claims = [
            item for item in claim_atoms if not _claim_needs_wearer(item.get("fact_text"))
        ]
        if static_claims:
            claim_atoms = static_claims
    identity_anchors = _anchor_texts(anchor_card, "hard_anchors")
    visible_anchors = _dedupe_text(
        [
            *_anchor_texts(anchor_card, "display_anchors"),
            *[item.get("fact_text") for item in claim_atoms],
        ],
        limit=10,
    )
    forbidden = _dedupe_text(
        [
            *(anchor_card.get("distortion_alerts") or []),
            *(content_bundle.get("forbidden_inferences") or []),
            *(execution_reference.get("do_not_invent") or []),
        ],
        limit=12,
    )
    ref_carrier = _text(execution_reference.get("content_carrier") or source_carrier)
    explicit_reference_status = _text(execution_reference.get("reference_status"))
    has_observed_reference = (
        explicit_reference_status != "STRUCTURE_ONLY"
        and (
            explicit_reference_status == "VIDEO_REFERENCED"
            or bool(_text(execution_reference.get("execution_card_id")))
            or bool(execution_reference.get("action_spine"))
            or bool(execution_reference.get("camera_grammar"))
            # Legacy frozen packages predate reference_status.  Preserve their
            # prior carrier-only behaviour.
            or not explicit_reference_status
        )
    )
    compatible = has_observed_reference and _reference_is_compatible(
        presentation, ref_carrier
    )
    optional_reference = {
        "status": (
            "AVAILABLE"
            if compatible
            else "UNAVAILABLE_STRUCTURE_ONLY"
            if _text(execution_reference.get("reference_status")) == "STRUCTURE_ONLY"
            else "SKIPPED_INCOMPATIBLE"
        ),
        "content_carrier": ref_carrier,
        "action_spine": execution_reference.get("action_spine") or execution_reference.get("action_sequence") or [],
        "camera_grammar": execution_reference.get("camera_grammar") or execution_reference.get("camera_sequence") or [],
        "visual_hook_type": _text(execution_reference.get("visual_hook_type")),
        "usage_boundary": "只借鉴镜头节奏与承载关系，不复制原视频动作，不为卖点编触发事件",
    }
    if not compatible:
        optional_reference["action_spine"] = []
        optional_reference["camera_grammar"] = []
        optional_reference["visual_hook_type"] = ""

    raw_value = content_bundle.get("value_proposition") or {}
    raw_argument = (
        content_bundle.get("selling_argument")
        if isinstance(content_bundle.get("selling_argument"), dict)
        else {}
    )
    compatible_carriers = {
        _text(value).upper() for value in raw_argument.get("compatible_carriers") or []
    }
    safe_argument = _visual_selling_argument_view(raw_argument)
    argument_is_available = _text(raw_argument.get("status")) == "AVAILABLE"
    presentation_carriers = {
        "PERSON_ON_CAMERA": {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"},
        "STATIC_PRODUCT": {"STATIC_PRODUCT", "MIXED"},
        "HANDS_ONLY": {"HAND_ONLY", "HANDS_ONLY", "MIXED"},
    }.get(presentation, {presentation})
    argument_compatible = not compatible_carriers or bool(
        presentation_carriers & compatible_carriers
    )
    # This should already be MATCHED because the allocator filtered explicit
    # mismatches.  Keeping the status in the frozen seed makes old snapshots
    # diagnosable without letting the visual model change carrier authority.
    if argument_is_available:
        safe_argument["carrier_match_status"] = (
            "MATCHED" if argument_compatible else "UNMATCHED"
        )
    mainline = (
        _text(safe_argument.get("creative_core_value"))
        if argument_is_available
        else _text(raw_value.get("text")) if isinstance(raw_value, dict) else ""
    )
    safe_value = {
        "status": "AVAILABLE" if argument_is_available else _text(raw_value.get("status")),
        "text": mainline,
        "authority": "NORMALIZED_CREATIVE_SEMANTICS" if mainline else "UNAVAILABLE_TO_VISUAL_MODEL",
        "allowed_strength": _text(safe_argument.get("allowed_strength")),
    }
    if not mainline:
        # A visual fact may guide what is shown, but it must not silently
        # replace an operator-maintained selling point as the content thesis.
        mainline = ""
    raw_scene_reference = (
        creative_contract.get("scene_reference_contract")
        if isinstance(creative_contract.get("scene_reference_contract"), dict)
        else {}
    )
    raw_execution_card = (
        raw_scene_reference.get("scene_execution_card")
        if isinstance(raw_scene_reference.get("scene_execution_card"), dict)
        else {}
    )
    raw_space = (
        raw_execution_card.get("space")
        if isinstance(raw_execution_card.get("space"), dict)
        else {}
    )
    raw_scene_recipe = (
        raw_execution_card.get("visual_scene_recipe")
        if isinstance(raw_execution_card.get("visual_scene_recipe"), dict)
        else {}
    )
    raw_scene_request = (
        raw_execution_card.get("scene_request")
        if isinstance(raw_execution_card.get("scene_request"), dict)
        else raw_scene_reference.get("scene_request")
        if isinstance(raw_scene_reference.get("scene_request"), dict)
        else {}
    )
    # The matrix is a scene-realism hint, not a second content source.  Keep
    # only the small execution card needed to make a real phone-recordable
    # micro-space. Source ids, counts, raw source prompts, and product facts
    # never reach the visual model.
    scene_reference = {
        "status": _text(raw_scene_reference.get("status")) or "UNAVAILABLE",
        "selection_mode": _text(raw_scene_reference.get("selection_mode")) or "NO_EFFECT",
        "scene_family_key": _text(raw_scene_reference.get("scene_family_key")),
        "prototype_name": _text(raw_scene_reference.get("prototype_name")),
        "realism_anchors": _dedupe_text(
            raw_scene_reference.get("approved_realism_anchors") or [], limit=2
        ),
        "instruction": (
            "仅作场景真实感提示；不新增人物动作、剧情、卖点或商品事实。"
            if _text(raw_execution_card.get("status")) == "AVAILABLE"
            else "不可用时忽略，不改变原有创意方向。"
        ),
        "scene_request": {
            key: _text(raw_scene_request.get(key))
            for key in (
                "canonical_product_type",
                "presentation_mode",
                "scene_intent",
                "time_light_need",
                "capture_mode",
                "country",
            )
        },
        "execution_card": {
            "status": _text(raw_execution_card.get("status")) or "UNAVAILABLE",
            "source_quality": _text(raw_execution_card.get("source_quality")),
            "prototype_name": _text(raw_execution_card.get("prototype_name")),
            "space": {
                key: _text(raw_space.get(key))
                for key in ("location", "subspace", "phone_placement", "subject_position", "background_depth")
            },
            "background_anchors": _dedupe_text(raw_execution_card.get("background_anchors") or [], limit=2),
            # Operational labels such as WORK_BREAK stay available for
            # provenance/routing but are not presented as visual aesthetics.
            "situation_tags": _dedupe_text(raw_execution_card.get("situation_tags") or [], limit=2),
            "aesthetic_anchors": _dedupe_text(raw_execution_card.get("aesthetic_anchors") or [], limit=2),
            "visual_scene_recipe": {
                key: _text(raw_scene_recipe.get(key))
                for key in (
                    "space_relationship",
                    "material_palette",
                    "lighting_texture",
                    "lived_in_detail",
                )
            },
            "lived_in_trace": _text(raw_execution_card.get("lived_in_trace")),
            "lighting": _text(raw_execution_card.get("lighting")),
            "avoid_overdesign": _text(raw_execution_card.get("avoid_overdesign")),
            "coherence_key": _text(raw_execution_card.get("coherence_key")),
            "instruction": "只补足同一场景来源中的空间、真实感与审美锚点，不改变商品事实、卖点主线、人物动作或结构。",
        },
    }
    seed = {
        "schema_version": CREATIVE_SEED_SCHEMA_VERSION,
        "product_truth": {
            "product_identity": _text(
                anchor_card.get("product_positioning_one_liner")
                or anchor_card.get("product_name")
                or product_type
            ),
            "identity_anchors": identity_anchors,
            "visible_detail_anchors": visible_anchors,
            "approved_claims": claim_atoms,
            "value_proposition": safe_value,
            "selling_argument": safe_argument,
            "display_quantity_contract": dict(
                safe_argument.get("display_quantity_contract") or {}
            ),
            "core_proof_claim_keys": list(safe_argument.get("core_proof_claim_keys") or []),
            "optional_visual_claim_keys": list(safe_argument.get("optional_visual_claim_keys") or []),
            "content_mode": (
                "SELLING_ARGUMENT"
                if _text(safe_argument.get("status")) == "AVAILABLE"
                else "FACTUAL_OBSERVATION"
            ),
            "content_mainline": mainline,
            "forbidden_inferences": forbidden,
        },
        "creative_direction": {
            "content_angle_key": _text(content_angle_key),
            "requested_hook_id": _text(requested_hook_id),
            "macro_structure": _macro_structure(structure_contract),
            "preferred_presentation": presentation,
            "presentation_reason": presentation_reason,
            "capture_mode": capture_mode,
            "source_structure_carrier": source_carrier,
            "continuity_hint": _text(hard.get("continuity_mode")),
        },
        # This is a frozen surface preference for the central voiceover
        # engine.  It changes only the speaker-to-viewer relationship, never
        # product truth, visual design, validation, or retry behaviour.
        "voiceover_surface_contract": {
            "relationship_device": _text(relationship_device) or "HOOK_DECIDES",
            "speaker_position": (
                "CREATOR_TO_CAMERA"
                if capture_mode == CAPTURE_MODE_CREATOR_SELF_SHOT
                else "VOICEOVER_OVER_PRODUCT"
            ),
            "policy_version": "audience-relation-rotation-v2-capture-aware",
            "hard_required": False,
        },
        "diversity_context": {
            "preferred_persona_role": _text(creative_contract.get("persona_role")),
            "preferred_scene_motif": _text(creative_contract.get("scene_motif")),
            "preferred_surface_profile": dict(creative_contract.get("surface_profile") or {}),
            "outfit_selection_contract": dict(
                creative_contract.get("outfit_selection_contract") or {}
            ),
            "outfit_scene_affinity_contract": dict(
                creative_contract.get("outfit_scene_affinity_contract") or {}
            ),
            "persona_selection_contract": dict(
                creative_contract.get("persona_selection_contract") or {}
            ),
            "outfit_persona_affinity_contract": dict(
                creative_contract.get("outfit_persona_affinity_contract") or {}
            ),
            "avoid_patterns": _dedupe_text(
                [
                    *(creative_contract.get("anti_template_rules") or []),
                    *(creative_contract.get("forbidden_recent_patterns") or []),
                ],
                limit=10,
            ),
            "instruction": "这些只用于促成人物与场景差异，不要求设计剧情或卖点触发动作",
            "scene_reference": scene_reference,
        },
        "optional_visual_inspiration": optional_reference,
    }
    action_design = _compile_action_design(
        creative_contract=creative_contract,
        structure_contract=structure_contract,
        presentation=presentation,
        carrier_execution=carrier_specific_execution,
        proof_subject=_text(safe_argument.get("proof_subject")),
        preferred_action_mode=_text(safe_argument.get("preferred_action_mode")),
    )
    seed["action_design"] = action_design
    seed["creative_direction"]["primary_action_mode"] = _text(
        action_design.get("primary_action_mode")
    )
    seed["creative_direction"]["action_signature"] = _text(
        action_design.get("action_signature")
    )
    seed["creative_direction"]["opening_visual_job"] = _opening_visual_job(
        seed["product_truth"],
        presentation=presentation,
        requested_hook_id=requested_hook_id,
    )
    capture_rhythm_contract = build_capture_rhythm_contract(
        capture_mode=capture_mode,
        macro_structure=seed["creative_direction"].get("macro_structure") or [],
    )
    seed["capture_rhythm_contract"] = capture_rhythm_contract
    seed["creative_direction"]["capture_rhythm_profile"] = _text(
        capture_rhythm_contract.get("profile")
    )
    from core.product_type_resolution import normalize_product_type

    canonical_product_type = normalize_product_type(
        product_type,
        top_category,
    ).canonical_type
    # Category adapters remain optional business logic.  The thin visual
    # contract below is shared by wearable categories and never adds a model
    # call, retry, or hard gate.
    if category_execution_extension:
        profile = (
            category_execution_extension.get("profile")
            if isinstance(category_execution_extension.get("profile"), dict)
            else {}
        )
        canonical_product_type = (
            _text(profile.get("product_subtype")) or canonical_product_type
        )
        carrier_specific_execution = dict(carrier_specific_execution)
        carrier_specific_execution["selected_action_design"] = dict(action_design)
        seed["category_execution_extension"] = category_execution_extension
        seed["carrier_specific_execution"] = carrier_specific_execution
    if canonical_product_type:
        seed["product_truth"]["canonical_product_type"] = canonical_product_type
    from core.visual_execution_contract import build_visual_execution_contract

    visual_execution_contract = build_visual_execution_contract(
        canonical_product_type=canonical_product_type,
        presentation_mode=presentation,
        capture_mode=capture_mode,
        outfit_contract=seed["diversity_context"].get("outfit_selection_contract"),
        scene_reference=scene_reference,
        product_truth=seed["product_truth"],
        opening_visual_job=seed["creative_direction"].get("opening_visual_job"),
        action_design=action_design,
        suggested_opening_action=_text(creative_contract.get("opening_action")),
        suggested_event_flow=_text(creative_contract.get("action_grammar")),
    )
    if visual_execution_contract:
        seed["visual_execution_contract"] = visual_execution_contract
    seed["creative_seed_id"] = _stable_id("SCS_", seed)
    return seed


def build_simplified_script_prompt(
    seed: Dict[str, Any],
    *,
    target_country: str,
    target_language: str,
    duration_seconds: float,
) -> str:
    capture_mode = _text(
        seed.get("creative_direction", {}).get("capture_mode")
    ) or CAPTURE_MODE_CREATOR_SELF_SHOT
    schema = {
        "schema_version": VISUAL_SCRIPT_SCHEMA_VERSION,
        "script_concept": {
            "one_sentence_idea": "一句话创意",
            "viewer_need": "观众需求或观看理由",
            "hook_intent": "开头如何制造具体关注",
            "macro_structure": ["HOOK", "PROOF"],
        },
        "production_design": {
            "presentation_mode": "PERSON_ON_CAMERA|STATIC_PRODUCT|HANDS_ONLY",
            "capture_mode": (
                "CREATOR_SELF_SHOT|HANDS_PRODUCT_SHARE|STATIC_PRODUCT_RECORD"
            ),
            "character": {
                "persona_id": "必须继承冻结人物模板ID；不可用时留空",
                "identity": "人物身份；无人物则写不适用",
                "appearance": "年龄感、气质与可见外形",
                "hair_makeup": "发型妆容",
                "speaking_personality": "说话人格",
            },
            "outfit": {
                "base_outfit": "除目标商品外的完整基础穿搭",
                "product_role": "目标商品在造型中的位置",
                "accessories": "必要配饰，没有则写无",
            },
            "scene": {
                "location": "具体地点",
                "moment": "具体生活时刻",
                "lighting": "光线",
                "background": "背景陈设",
                "phone_placement": "第一段或主要片段的基础手机位置；后续拍摄单元允许在同一小片区域重新放置",
                "subject_position": "人物或商品在这一小段空间中的位置",
                "background_depth": "前景与背景保留的普通生活层次",
                "lived_in_trace": "一个自然出现的使用痕迹或随身物品；没有则写无",
            },
            "emotion": {
                "starting_state": "开场自然状态",
                "natural_change": "动作带来的轻微变化",
                "ending_state": "结尾状态",
            },
            "life_event": {
                "continuous_event": "这一条视频发生的同一个生活时刻",
                "starting_context": "开场时人物或商品正在什么状态",
                "action_progression": "按冻结action_design完成开始→核心动作→完成状态；允许1至2次自然过渡，不要求每镜变化",
                "ending_context": "同一时刻如何自然结束",
            },
            "action_execution": {
                "primary_action_mode": "必须等于冻结action_design.primary_action_mode",
                "start_state": "必须等于冻结action_design.start_state",
                "core_action": "必须等于冻结action_design.core_action",
                "end_state": "必须等于冻结action_design.end_state",
                "supporting_scene_action": "有则自然衔接；不得扩写成另一套动作清单",
            },
        },
        "product_usage": {
            "identity_anchors_preserved": ["必须原样来自授权锚点"],
            "selling_points_used": ["仅填写本条画面实际采用的approved_claims.claim_key；允许只选一部分"],
        },
        "storyboard": [
            {
                "shot_no": 1,
                "time_range": "0.0-2.0s",
                "visual_content": "包含人物/商品/场景关系的完整可见画面",
                "character_action": "具体可执行动作",
                "natural_emotion": "可见但不过度表演的情绪",
                "camera": "景别、机位、运动",
                "capture_unit_id": "按capture_rhythm_contract分配，如CU_01；模型建议会由代码确定性归并",
                "starts_new_take": True,
                "product_anchors_visible": ["来自授权锚点"],
                "supported_claim_keys": ["当前镜头实际支持的claim_key"],
                "narrative_role": "HOOK|PROOF|USE|TRANSITION|ENDING",
            }
        ],
        "voiceover_context": {
            "viewer_relationship": "与观众的关系",
            "speaking_intent": "为什么此刻开口",
            "desired_tone": "自然口语语气",
        },
    }
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    carrier_execution = (
        seed.get("carrier_specific_execution")
        if isinstance(seed.get("carrier_specific_execution"), dict)
        else {}
    )
    category_guidance = ""
    if category_extension:
        from core.category_execution import build_category_blueprint_guidance

        category_guidance = build_category_blueprint_guidance(
            category_extension,
            carrier_execution=carrier_execution,
        )
        schema["production_design"]["accessory_execution"] = {
            "wearing_zone": "必须等于冻结配饰执行档案",
            "required_view": "必须等于当前承载下的冻结结果视角",
            "interaction_boundary": ["只继承输入，不新增规则"],
            "identity_priority": ["只继承输入，不推断未知商品属性"],
        }
    category_guidance_block = (
        f"\n\n类目执行补充：\n{category_guidance}\n"
        if category_guidance
        else "\n"
    )
    capture_contract = (
        seed.get("capture_rhythm_contract")
        if isinstance(seed.get("capture_rhythm_contract"), dict)
        else build_capture_rhythm_contract(
            capture_mode=capture_mode,
            macro_structure=seed.get("creative_direction", {}).get("macro_structure") or [],
        )
    )
    if _text(capture_contract.get("profile")) == CAPTURE_RHYTHM_LEGACY:
        capture_guidance = (
            "本条使用旧版连续录制关系：保持一个主要手机视角和一个连续时刻。"
        )
    else:
        capture_guidance = (
            f"本条使用{_text(capture_contract.get('profile'))}：同一创作者或商品、同一地点、"
            f"同一时刻和同一部手机，分别录制{int(capture_contract.get('capture_unit_count') or 2)}段简短素材，"
            "片段间使用普通直接剪切或自然跳剪。不同片段可以在同一小片区域重新放置手机、改变人物与手机距离，"
            "或补录商品细节；人物、商品、穿搭、光线和生活状态必须连续。景别变化来自下一段真实录制，"
            "不得只靠数字裁切、连续变焦或人物在一个长镜头里反复走近走远来假装切镜。"
            "这仍是创作者自己完成的手机分享，不得扩写成摄影团队、第三人跟拍、正反打、稳定器运镜、跨房间调度、商业景深或广告定格。"
        )
    model_visible_seed = _model_visible_creative_seed(seed)
    return f"""你是使用手机创作内容的短视频分享者与完整脚本作者。请为{target_country}市场生成一条约{duration_seconds:g}秒的原创商品短视频视觉脚本。

这不是分层规划题。请一次写出能够直接拍摄/生成的完整内容：人物、外形、穿搭、场景、自然状态和4至6个结构时间段必须同时成立。storyboard 继续表达结构内容段，但必须按 capture_rhythm_contract 归并成2至3个真实拍摄单元；同一 capture_unit_id 内连续录制，不同 capture_unit_id 之间是直接剪切后的另一段手机素材。

拍摄关系：
{capture_guidance}{category_guidance_block}
核心原则：
1. 商品事实只能来自 product_truth；不知道的内容不补写，绝不虚构功效、材质、颜色或使用结果。
2. product_truth.content_mode=SELLING_ARGUMENT 时，只有非空的 content_mainline / selling_argument.creative_core_value 可以作为视觉创作语义；它是全片购买理由，但不要求人物动作或场景制造这个理由。若两者为空，表示原始运营卖点措辞仅供中央口播使用：不得从卖点推断人物出身、职业、地域、经济身份或特殊场景，只按 creative_direction、商品锚点和普通生活状态完成画面。approved_claims 只用作画面证据，禁止把第一个扣子、口袋或袖型细节改写成全片主题。content_mode=FACTUAL_OBSERVATION 时围绕可见事实做观察，不伪造用户痛点或产品收益。
3. creative_direction.macro_structure 只控制观看顺序，不规定统一镜头模板；capture_rhythm_contract 只控制真实拍摄单元数量与剪辑关系，不改变Beat、承载、动作主线或商品事实。requested_hook_id 只描述口播意图，本步骤不写{target_language}口播。
4. presentation_mode 必须等于 preferred_presentation，capture_mode 必须等于 creative_direction.capture_mode。PERSON_ON_CAMERA 必须写完整人物、穿搭、场景和自然状态；CREATOR_SELF_SHOT 中人物是正在对自己的手机镜头说话的创作者，不是被摄影团队拍摄的沉默模特。STATIC_PRODUCT 不虚构出镜人物或商品情绪；HANDS_ONLY 只允许手部进入画面。
5. 默认采用观察式画面，但 action_design 是本条已经选定的动作主线，围绕它完成“开始状态→一个核心商品互动或生活动作→完成状态”。动作不必承担卖点因果，也不要求每镜变化；同一动作可以跨两个内容段自然完成。supporting_scene_action 只作前后衔接，不得与核心动作叠成清单。不要为了证明卖点制造遮挡后揭示、偶然吹开、通知弹出、道具机关或“恰好发现”等剧情。
6. 商品锚点与 claim_key 必须逐字从输入中选择。approved_claims 是可选事实池，不是拍摄清单：只选择当前结构自然需要的少量事实，未选事实无需安排镜头。被写入 selling_points_used 或 supported_claim_keys 的事实必须来自池内；同一事实只需全片有一处自然可见，不要求逐项触摸、指向或分配独立动作。本步骤不得决定中央口播最终选择哪些事实，也不得按口播逐句设计镜头。
7. 人物和场景要具体但克制，情绪是自然的小变化，不写广告演员式惊讶。CREATOR_SELF_SHOT 的场景只是分享发生的普通背景，不得扩写成走廊、电梯、室内外连续调度。diversity_context.scene_reference.scene_request 只说明当前商品展示所需的场景语义；其中 time_light_need=DAYLIGHT 时保持同一地点的白天自然光，不能改成夜间氛围。execution_card 若为 AVAILABLE，优先把它的 space 翻译为场景字段：写清手机放在哪里、人物与手机的自然相对位置、背景的前后层次，并自然保留至多两项 background_anchors 和一个 lived_in_trace。位置只用“靠近、旁边、前后、同一小片区域”等相对关系；输入没有实测值时，不写米、厘米、精确距离或精确机位高度。场景卡不是拍摄任务清单，道具不得变成必须触摸或使用的动作；照样只使用现场已有自然光或普通室内光。execution_card 不可用时按原有创意方向完成。
8. diversity_context.outfit_selection_contract 是本条生成前已经选定的穿搭合同。outfit_recipe 是本条唯一配方，其中非空的连体单品，或非空的上装与下装，以及鞋包和辅助配饰应被完整写入 production_design.outfit.base_outfit，不得重新选择、拆分或替换其中任一单品；one_piece 非空时必须按一件连体服装执行，top / bottom 应为空，不得把连衣裙或连体裤改写成上下装。target_role 只说明目标商品在整套造型中的角色，visibility_zones 用于保持商品可见。source_type=LIGHTWEIGHT_TEMPLATE 时，只执行合同里已经标准化的结构化字段和 base_outfit_direction；不得猜测或索取模板标题、正文、prompt_core、notes。source_type=INTERNAL_PROFILE 时，结合 silhouette_key、style_family、style_intensity、outfit_recipe 和 base_outfit_direction形成可感知的完整轮廓；outer_layer_direction / neckline_direction / hair_direction / palette_relation / visibility_requirement 只作柔性设计参考，不增加独立动作或质检门槛。允许因真实场景做轻微自然调整，但不要仅换颜色后重新回到近期相同的“基础上衣＋长裤”组合。preferred_surface_profile 只作旧字段兼容。全片保持一个连续、普通的生活时刻，但允许分成多个手机拍摄单元；不要默认写成“靠近镜头→退后展示→整理衣服→微笑收尾”的固定动作链。
8.1 diversity_context.persona_selection_contract 若 availability=AVAILABLE，人物身份只允许来自这份冻结合同：persona_id、script_projection、identity_lock 和参考资产共同拥有权威，production_design.character 必须逐项继承 script_projection。商品参考图只决定商品，不得继承其中模特的脸、年龄、体型、发型、滤镜、姿态或背景。模型只可按当前生活时刻调整自然表情、视线和小动作，不得重新设计人物。若合同为 UNAVAILABLE/NOT_APPLICABLE，沿用普通创作者设计且不伪装成已使用人物库。
9. creative_direction.opening_visual_job 只是前三秒的软观看任务：优先让核心结果、相关细节、使用场景或静物商品中的一种尽快成立。不得为了执行它强制设计前后对比、身体缺点特写、苦恼到惊喜的情绪反转或额外剧情动作；卖点无法被画面直接证明时，保持商品与场景清楚可见即可，由中央口播承担表达。
10. 类目执行补充中的 interaction_boundary 是全片一次生效的物理边界。storyboard 只写这一镜实际发生的正向动作，不要在每个镜头反复写“不得、禁止、不缠绕、不重新系”等负向规则。若冻结了 selected_action_design，按它执行一个核心商品互动；不得再把 optional_simple_interactions 当作候选清单逐项加入。若冻结了 primary_demonstration_mode，本条只表现这一种用法；supported_demonstration_modes 只是授权范围，不是镜头清单。
10.1 product_truth.display_quantity_contract 只有 status=AUTHORIZED 时才允许出现多只同款商品。此时 required_display_count 是全片唯一数量权威：这组同款不是“竞争性配饰”，人物穿搭与每个拍摄单元都必须保持该数量，不得从单戴切成叠戴、边拍边增加或中途摘下。字段为空时继续默认只出现一只目标商品，禁止模型自行复制。
11. 若输入含 visual_execution_contract，必须把“手机原生质感”和“人物/穿搭/场景的审美完成度”当成两个独立维度：NATIVE_STYLED 是真实创作者已经完成当天造型后的手机分享，不是精修广告，也不是素上衣、无妆发、空背景的代名词。优先执行 styling_context 的轮廓、配色、妆发与少量辅助元素；scene_context.situation_tags 只说明生活情境，不得当作材质、色调或审美措辞。场景优先消费 visual_scene_recipe 中非空的空间关系、材质色调、光线质感和生活痕迹；aesthetic_anchors 只作补充，而且所有视觉元素必须来自同一 coherence_key，不混拼成布景。配方字段为空就自然忽略，不得自行补造。visual_saliency 只负责最终画面关系：在不改变商品身份、佩戴状态、冻结穿搭轮廓和既有场景的前提下，把主体放在清楚亮部，按 separation 建立商品与内搭/背景的边界，并让 storyboard 首镜服从 opening_focus 的焦点与景别；natural_change 最多作为一次低表演感状态变化，不能覆盖 action_design。life_event 只写同一地点和时刻中的连续状态，按action_design允许1至2次自然过渡；它不是逐镜动作配额。这些均为软创意指导，不得因此拒绝产出或增加商品事实。
12. 只返回一个JSON对象，不要Markdown，不要解释。字段齐全，结构如下：
{json.dumps(schema, ensure_ascii=False, indent=2)}

冻结输入：
{json.dumps(model_visible_seed, ensure_ascii=False, indent=2)}
"""


def _model_visible_creative_seed(seed: Dict[str, Any]) -> Dict[str, Any]:
    """Hide outfit audit alternatives after one concrete recipe is frozen.

    The full seed stays persisted for lineage.  The blueprint model only needs
    the selected recipe; showing both ``吊带`` and the audited source
    ``吊带或短袖`` re-opened a decision that code had already made.
    """

    visible = json.loads(json.dumps(seed, ensure_ascii=False))
    diversity = (
        visible.get("diversity_context")
        if isinstance(visible.get("diversity_context"), dict)
        else {}
    )
    contract = (
        diversity.get("outfit_selection_contract")
        if isinstance(diversity.get("outfit_selection_contract"), dict)
        else {}
    )
    # The outfit/scene score is routing lineage, not creative material.  Keep
    # it in the persisted seed and final brief, but do not ask the model to
    # interpret an internal ranking decision it cannot improve.
    diversity.pop("outfit_scene_affinity_contract", None)
    recipe = contract.get("outfit_recipe") if isinstance(contract.get("outfit_recipe"), dict) else {}
    if any(_text(value) for value in recipe.values()):
        projection = _outfit_prompt_projection(contract)
        for key in (
            "source_outfit_recipe",
            "source_accessory_items",
            "inner_type",
            "inner_color",
            "bottom_type",
            "bottom_color",
            "bottom_fit",
            "accessory_level",
            "footwear_visibility",
            "applicable_product_codes",
            "applicable_product_type",
            "ignored_unstructured_fields",
            "structured_fields_used",
            "normalization_warnings",
            "template_display_name",
        ):
            contract.pop(key, None)
        # Keep one compact direction for prose continuity, but make it the
        # deterministic projection of the recipe instead of the raw options.
        contract["base_outfit_direction"] = _text(
            projection.get("frozen_outfit")
        )
        legacy = diversity.get("preferred_surface_profile")
        if isinstance(legacy, dict):
            legacy["base_outfit_direction"] = contract["base_outfit_direction"]
    return visible


def normalize_simplified_visual_script(
    raw: Dict[str, Any],
    seed: Dict[str, Any],
    *,
    generation_provenance: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    script = dict(raw)
    script["schema_version"] = VISUAL_SCRIPT_SCHEMA_VERSION
    script["creative_seed_id"] = _text(seed.get("creative_seed_id"))
    script["allocated_direction"] = dict(seed.get("creative_direction") or {})
    production = (
        dict(script.get("production_design"))
        if isinstance(script.get("production_design"), dict)
        else {}
    )
    # Capture ownership is frozen before generation.  Missing or drifted model
    # output is normalized rather than sent through another repair loop.
    production["capture_mode"] = _text(
        seed.get("creative_direction", {}).get("capture_mode")
    ) or _capture_mode_for_presentation(production.get("presentation_mode"))
    persona_contract = (
        dict((seed.get("diversity_context") or {}).get("persona_selection_contract") or {})
        if isinstance(seed.get("diversity_context"), dict)
        else {}
    )
    outfit_persona_affinity = (
        dict(
            (seed.get("diversity_context") or {}).get(
                "outfit_persona_affinity_contract"
            ) or {}
        )
        if isinstance(seed.get("diversity_context"), dict)
        else {}
    )
    if _text(persona_contract.get("availability")) == "AVAILABLE":
        character = (
            dict(production.get("character") or {})
            if isinstance(production.get("character"), dict)
            else {}
        )
        projection = (
            persona_contract.get("script_projection")
            if isinstance(persona_contract.get("script_projection"), dict)
            else {}
        )
        character["persona_id"] = _text(persona_contract.get("persona_id"))
        for key in ("identity", "appearance", "hair_makeup", "speaking_personality"):
            if _text(projection.get(key)):
                character[key] = _text(projection.get(key))
        production["character"] = character
        production["persona_selection_contract"] = persona_contract
        script["persona_selection_contract"] = persona_contract
    if outfit_persona_affinity:
        production["outfit_persona_affinity_contract"] = (
            outfit_persona_affinity
        )
        script["outfit_persona_affinity_contract"] = (
            outfit_persona_affinity
        )
    frozen_outfit_contract = (
        dict((seed.get("diversity_context") or {}).get("outfit_selection_contract") or {})
        if isinstance(seed.get("diversity_context"), dict)
        else {}
    )
    if frozen_outfit_contract and _text(production.get("presentation_mode")).upper() != "STATIC_PRODUCT":
        from core.outfit_selection import upgrade_outfit_structure_contract

        frozen_outfit_contract = upgrade_outfit_structure_contract(
            frozen_outfit_contract
        )
        frozen_outfit_projection = _outfit_prompt_projection(
            frozen_outfit_contract
        )
        frozen_outfit_text = _text(
            frozen_outfit_projection.get("frozen_outfit")
        )
        if frozen_outfit_text:
            production_outfit = (
                dict(production.get("outfit") or {})
                if isinstance(production.get("outfit"), dict)
                else {}
            )
            production_outfit["base_outfit"] = frozen_outfit_text
            accessory_items = list(
                frozen_outfit_contract.get("accessory_items") or []
            )
            if accessory_items:
                production_outfit["accessories"] = "；".join(
                    _text(item) for item in accessory_items if _text(item)
                )
            production["outfit"] = production_outfit
    action_design = (
        dict(seed.get("action_design") or {})
        if isinstance(seed.get("action_design"), dict)
        else {}
    )
    if action_design:
        production["action_execution"] = dict(action_design)
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        from core.category_execution import resolve_category_carrier_execution

        carrier_execution = (
            dict(seed.get("carrier_specific_execution") or {})
            if isinstance(seed.get("carrier_specific_execution"), dict)
            else {}
        )
        if not carrier_execution:
            carrier_execution = resolve_category_carrier_execution(
                category_extension,
                presentation_mode=_text(production.get("presentation_mode"))
                or _text(seed.get("creative_direction", {}).get("preferred_presentation")),
            )
        # The adapter owns physical execution.  Model output cannot redefine
        # the wearing zone or interaction boundary.
        production["accessory_execution"] = carrier_execution
        script["category_execution_extension"] = dict(category_extension)
    scene = dict(production.get("scene") or {}) if isinstance(production.get("scene"), dict) else {}
    # Scene references contain no measured geometry.  Strip model-invented
    # precision deterministically instead of adding another validation/retry.
    for field in ("phone_placement", "subject_position", "background_depth"):
        value = _text(scene.get(field))
        value = re.sub(
            r"(?<![A-Za-z0-9])\d+(?:\.\d+)?\s*(?:米|厘米|公分|cm|m)(?![A-Za-z])",
            "自然距离",
            value,
            flags=re.IGNORECASE,
        )
        scene[field] = value
    if scene:
        production["scene"] = scene
    visual_contract = (
        dict(seed.get("visual_execution_contract") or {})
        if isinstance(seed.get("visual_execution_contract"), dict)
        else {}
    )
    if visual_contract:
        life_event = (
            dict(production.get("life_event") or {})
            if isinstance(production.get("life_event"), dict)
            else {}
        )
        if not _text(life_event.get("continuous_event")):
            concept = script.get("script_concept") if isinstance(script.get("script_concept"), dict) else {}
            emotion = production.get("emotion") if isinstance(production.get("emotion"), dict) else {}
            event_contract = (
                visual_contract.get("event_progression")
                if isinstance(visual_contract.get("event_progression"), dict)
                else {}
            )
            life_event = {
                "continuous_event": _text(
                    event_contract.get("suggested_event_flow")
                    or event_contract.get("suggested_opening_action")
                    or concept.get("one_sentence_idea")
                ),
                "starting_context": _text(scene.get("moment")),
                "action_progression": (
                    "→".join(
                        _dedupe_text(
                            [
                                action_design.get("start_state"),
                                action_design.get("core_action"),
                                action_design.get("end_state"),
                            ],
                            limit=3,
                        )
                    )
                    if action_design
                    else "保持同一真实分享状态"
                ),
                "ending_context": _text(emotion.get("ending_state")),
                "source": "DETERMINISTIC_CONTEXT_FALLBACK",
            }
        production["life_event"] = life_event
        script["visual_execution_contract"] = visual_contract
    script["production_design"] = production
    if visual_contract:
        from core.visual_execution_contract import build_visual_execution_diagnostics

        script["visual_execution_diagnostics"] = build_visual_execution_diagnostics(
            contract=visual_contract,
            production_design=production,
        )
    capture_rhythm_contract = (
        dict(seed.get("capture_rhythm_contract") or {})
        if isinstance(seed.get("capture_rhythm_contract"), dict)
        else build_capture_rhythm_contract(
            capture_mode=_text(production.get("capture_mode")),
            macro_structure=(seed.get("creative_direction") or {}).get("macro_structure") or [],
        )
    )
    compiled_storyboard, capture_units = compile_capture_units(
        script.get("storyboard") or [],
        capture_rhythm_contract,
    )
    script["storyboard"] = compiled_storyboard
    script["capture_rhythm_contract"] = capture_rhythm_contract
    script["capture_units"] = capture_units
    script["generation_provenance"] = dict(generation_provenance)
    script["simplified_script_id"] = _stable_id("SSV_", script)
    return script


def _required_texts(mapping: Dict[str, Any], keys: Iterable[str]) -> bool:
    return all(_text(mapping.get(key)) for key in keys)


def validate_simplified_visual_script(
    script: Dict[str, Any], seed: Dict[str, Any]
) -> Dict[str, Any]:
    """Only three hard dimensions: usability, truth, and carrier."""

    issues: List[str] = []
    warnings: List[str] = []
    concept = script.get("script_concept") if isinstance(script.get("script_concept"), dict) else {}
    production = script.get("production_design") if isinstance(script.get("production_design"), dict) else {}
    usage = script.get("product_usage") if isinstance(script.get("product_usage"), dict) else {}
    shots = [item for item in script.get("storyboard") or [] if isinstance(item, dict)]
    voice = script.get("voiceover_context") if isinstance(script.get("voiceover_context"), dict) else {}

    # 1. Output usability.
    if not _required_texts(concept, ("one_sentence_idea", "viewer_need", "hook_intent")):
        issues.append("输出不可用：缺少完整脚本概念")
    if not 4 <= len(shots) <= 6:
        issues.append("输出不可用：分镜必须为4至6个对象")
    for index, shot in enumerate(shots, 1):
        if not _required_texts(
            shot,
            ("time_range", "visual_content", "character_action", "natural_emotion", "camera", "narrative_role"),
        ):
            issues.append(f"输出不可用：第{index}镜字段不完整")
    if not _required_texts(voice, ("viewer_relationship", "speaking_intent", "desired_tone")):
        issues.append("输出不可用：缺少中央口播所需的人物语境")

    truth = seed.get("product_truth") if isinstance(seed.get("product_truth"), dict) else {}
    approved_claims = {
        _text(item.get("claim_key")): _text(item.get("fact_text"))
        for item in truth.get("approved_claims") or []
        if isinstance(item, dict) and _text(item.get("claim_key"))
    }
    approved_anchors = set(
        _dedupe_text(
            [*(truth.get("identity_anchors") or []), *(truth.get("visible_detail_anchors") or [])],
            limit=30,
        )
    )
    used_claims = _dedupe_text(usage.get("selling_points_used") or [], limit=12)
    preserved_identity = _dedupe_text(
        usage.get("identity_anchors_preserved") or [], limit=12
    )
    unknown_identity = [
        anchor for anchor in preserved_identity
        if not _anchor_is_authorized(anchor, approved_anchors)
    ]
    if unknown_identity:
        issues.append("商品事实冲突：脚本声明未授权身份锚点=" + ",".join(unknown_identity))
    required_identity = _dedupe_text(truth.get("identity_anchors") or [], limit=12)
    if required_identity and not any(anchor in preserved_identity for anchor in required_identity):
        issues.append("输出不可用：没有保留至少一个商品身份锚点")
    unknown_claims = [key for key in used_claims if key not in approved_claims]
    if unknown_claims:
        issues.append("商品事实冲突：出现未授权claim_key=" + ",".join(sorted(set(unknown_claims))))
    for shot in shots:
        unknown = [
            _text(item)
            for item in shot.get("product_anchors_visible") or []
            if _text(item) and not _anchor_is_authorized(item, approved_anchors)
        ]
        if unknown:
            issues.append("商品事实冲突：镜头使用未授权锚点=" + ",".join(unknown))
        unknown_shot_claims = [
            _text(item)
            for item in shot.get("supported_claim_keys") or []
            if _text(item) and _text(item) not in approved_claims
        ]
        if unknown_shot_claims:
            issues.append("商品事实冲突：镜头声明未授权claim_key=" + ",".join(unknown_shot_claims))
    evidence_keys = {
        _text(item)
        for shot in shots
        for item in (shot.get("supported_claim_keys") or [])
        if _text(item)
    }
    missing_used_evidence = [key for key in used_claims if key not in evidence_keys]
    if missing_used_evidence:
        warnings.append(
            "脚本声明使用但未单独标注画面证据的事实="
            + ",".join(missing_used_evidence)
        )
    visible_in_story = {
        _text(item)
        for shot in shots
        for item in (shot.get("product_anchors_visible") or [])
        if _text(item)
    }
    if approved_anchors and not visible_in_story.intersection(approved_anchors):
        issues.append("输出不可用：分镜没有明确呈现任何授权商品锚点")

    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        from core.category_execution import validate_category_execution_identity

        issues.extend(
            validate_category_execution_identity(
                category_extension,
                script=script,
            )
        )

    authorized_text = json.dumps(truth, ensure_ascii=False)
    # Effect terms are product-truth checks, so only inspect fields whose job is
    # to describe the target product or its result.  Character actions, scene
    # prose and emotion can legitimately contain words such as “舒适位置”
    # without claiming that the product itself is comfortable.
    effect_scope = {
        "script_concept": {
            key: concept.get(key)
            for key in ("one_sentence_idea", "viewer_need", "hook_intent")
        },
        "product_role": (
            production.get("outfit", {}).get("product_role")
            if isinstance(production.get("outfit"), dict)
            else ""
        ),
        "product_usage": usage,
        "storyboard": [
            {
                "visual_content": shot.get("visual_content"),
                "product_anchors_visible": shot.get("product_anchors_visible"),
            }
            for shot in shots
        ],
    }
    effect_text = json.dumps(effect_scope, ensure_ascii=False)
    # “舒适坐姿/位置/状态” describes the creator, not a product benefit.
    # Remove only these unambiguous human-state phrases; product comfort
    # statements such as “穿着舒适” remain governed by authorised truth.
    effect_text = re.sub(r"舒适(?:的)?(?:坐姿|位置|状态|姿势|地坐下)", "", effect_text)
    for term in ("显腿长", "腿更长", "显瘦", "显高", "塑形", "保暖", "舒适", "不挑人", "百搭"):
        if term in effect_text and term not in authorized_text:
            issues.append(f"商品事实冲突：出现未授权效果词={term}")

    # 2. Carrier conflict and completeness within that carrier.
    preferred = _text(seed.get("creative_direction", {}).get("preferred_presentation")).upper()
    actual = _text(production.get("presentation_mode")).upper()
    if actual != preferred:
        issues.append(f"承载冲突：要求{preferred or '未知'}，实际{actual or '空'}")
    expected_capture = _text(
        seed.get("creative_direction", {}).get("capture_mode")
    ).upper()
    actual_capture = _text(production.get("capture_mode")).upper()
    if expected_capture and actual_capture and actual_capture != expected_capture:
        issues.append(
            f"拍摄关系冲突：要求{expected_capture}，实际{actual_capture or '空'}"
        )
    elif expected_capture and not actual_capture:
        warnings.append("模型未返回拍摄关系，归一化阶段将使用冻结capture_mode")
    character = production.get("character") if isinstance(production.get("character"), dict) else {}
    outfit = production.get("outfit") if isinstance(production.get("outfit"), dict) else {}
    scene = production.get("scene") if isinstance(production.get("scene"), dict) else {}
    emotion = production.get("emotion") if isinstance(production.get("emotion"), dict) else {}
    if not _required_texts(scene, ("location", "moment", "lighting", "background")):
        issues.append("输出不可用：场景设定不完整")
    if preferred == "PERSON_ON_CAMERA":
        if not _required_texts(character, ("identity", "appearance", "hair_makeup", "speaking_personality")):
            issues.append("输出不可用：真人方向的人物设定不完整")
        if not _required_texts(outfit, ("base_outfit", "product_role", "accessories")):
            issues.append("输出不可用：真人方向的穿搭设定不完整")
        if not _required_texts(emotion, ("starting_state", "natural_change", "ending_state")):
            issues.append("输出不可用：真人方向的情绪变化不完整")
    elif preferred == "STATIC_PRODUCT":
        # Product anchors can legitimately contain phrases such as “上身效果”.
        # They describe the target product, not necessarily an on-camera person.
        # Judge the actual production fields and executable actions instead of
        # keyword-scanning the whole JSON document.
        action_text = " ".join(
            _text(shot.get("character_action")) + " " + _text(shot.get("visual_content"))
            for shot in shots
        )
        # Do not use single-character pronouns here: “他” also appears inside
        # ordinary static wording such as “其他填充物”.  Match executable
        # performer actions instead, and ignore explicit negative phrases.
        normalized_action_text = action_text
        for negative in (
            "无人物出镜", "人物不出镜", "无人物动作", "无模特",
            "没有人物试穿", "无人试穿", "无真人试穿", "不含人物试穿",
            "不安排人物试穿", "没有人物穿着", "无人穿着", "无真人穿着",
            "没有模特动作", "无模特动作", "不含模特动作",
        ):
            normalized_action_text = normalized_action_text.replace(negative, "")
        performer_terms = (
            "穿上", "试穿", "转身展示", "人物出镜", "模特出镜",
            "她穿", "她走", "她站", "她转", "他穿", "他走", "他站", "他转",
            "人物走", "人物站",
        )
        has_on_camera_action = any(term in normalized_action_text for term in performer_terms)
        character_text = " ".join(_text(value) for value in character.values())
        has_character_design = bool(character_text) and not any(
            marker in character_text for marker in ("不适用", "无人物", "不出镜")
        )
        if has_on_camera_action or has_character_design:
            issues.append("承载冲突：静物方向包含真人上身或模特动作")

    signatures: List[str] = []
    for shot in shots:
        signature = _text(shot.get("visual_content")) + "|" + _text(shot.get("character_action"))
        if signature in signatures:
            warnings.append("分镜存在重复画面动作，但不阻断")
        signatures.append(signature)
    action_design = (
        seed.get("action_design")
        if isinstance(seed.get("action_design"), dict)
        else {}
    )
    action_keywords = _dedupe_text(
        action_design.get("action_keywords") or [], limit=8
    )
    authored_actions = "；".join(
        _text(shot.get("character_action")) + "；" + _text(shot.get("visual_content"))
        for shot in shots
    )
    if action_keywords and not any(
        keyword in authored_actions for keyword in action_keywords if len(keyword) >= 2
    ):
        warnings.append(
            "动作主线未明显落到分镜，可人工复核但不阻断="
            + _text(action_design.get("interaction_id"))
        )
    return {
        "valid": not issues,
        "issues": list(dict.fromkeys(issues)),
        "warnings": list(dict.fromkeys(warnings)),
        "policy_version": VALIDATION_POLICY_VERSION,
    }


def build_simplified_voiceover_inputs(
    script: Dict[str, Any], seed: Dict[str, Any], frozen: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    production = script.get("production_design") or {}
    character = production.get("character") or {}
    scene = production.get("scene") or {}
    emotion = production.get("emotion") or {}
    life_event = production.get("life_event") or {}
    visual_execution = (
        seed.get("visual_execution_contract")
        if isinstance(seed.get("visual_execution_contract"), dict)
        else {}
    )
    # Preserve the established scarf voiceover grounding behavior.  Apparel
    # saliency is composition-only and must not make the central voiceover
    # inherit a visual life event.
    visual_execution_enabled = (
        _text(visual_execution.get("feature_scope")) == "SCARF_ACCESSORY_GREY"
    )
    voice = script.get("voiceover_context") or {}
    bundle = dict(frozen.get("content_bundle_brief") or {})
    evidence_keys = {
        _text(key)
        for item in script.get("storyboard") or []
        if isinstance(item, dict)
        for key in item.get("supported_claim_keys") or []
        if _text(key)
    }
    bundle["claim_atoms"] = [
        item for item in bundle.get("claim_atoms") or []
        if isinstance(item, dict) and _text(item.get("claim_key")) in evidence_keys
    ]
    product_truth = seed.get("product_truth") if isinstance(seed.get("product_truth"), dict) else {}
    # The visual seed intentionally contains a sanitized argument view.  The
    # central voiceover must keep consuming the full frozen content bundle,
    # including the reviewed operator expression and original content
    # mainline.  Only the actually visible claim atoms are narrowed here.
    shots: List[Dict[str, Any]] = []
    shot_plan: List[Dict[str, Any]] = []
    presentation = _text(production.get("presentation_mode")).upper()
    carrier = {
        "PERSON_ON_CAMERA": "WEARER_ACTIVE",
        "STATIC_PRODUCT": "STATIC_PRODUCT",
        "HANDS_ONLY": "HAND_ONLY",
    }.get(presentation, presentation)
    for index, item in enumerate(script.get("storyboard") or [], 1):
        if not isinstance(item, dict):
            continue
        supported = [
            _text(key) for key in item.get("supported_claim_keys") or []
            if _text(key) in evidence_keys
        ]
        shots.append({
            "shot_no": int(item.get("shot_no") or index),
            "duration": _text(item.get("time_range")),
            "shot_content": _text(item.get("visual_content")),
            "observable_action": _text(item.get("character_action")),
            "natural_emotion": _text(item.get("natural_emotion")),
            "framing": _text(item.get("camera")),
            "product_visibility": list(item.get("product_anchors_visible") or []),
            "supported_claim_keys": supported,
            "carrier_mode": carrier,
            "structure_beat": _text(item.get("narrative_role")),
            "audio_hard_constraint": "NONE",
            "audio_preference": "VOICEOVER_ALLOWED",
        })
        shot_plan.append({
            "shot_no": int(item.get("shot_no") or index),
            "structure_beat": _text(item.get("narrative_role")),
            "carrier_mode": carrier,
            "continuity_group": "EVENT_1",
        })
    creative_blueprint = {
        "creative_thesis": _text(script.get("script_concept", {}).get("one_sentence_idea")),
        "creator_motivation": _text(voice.get("speaking_intent")),
        "voiceover_grounding_mode": "CONTENT_FIRST_WHOLE_VIDEO",
        "viewer_relationship": _text(voice.get("viewer_relationship")),
        "persona": {
            "identity": _text(character.get("identity")),
            "appearance": _text(character.get("appearance")),
            "hair_makeup": _text(character.get("hair_makeup")),
            "speaking_personality": _text(character.get("speaking_personality")),
        },
        "scene": {
            "location": _text(scene.get("location")),
            "moment": _text(scene.get("moment")),
            "lighting": _text(scene.get("lighting")),
            "background": _text(scene.get("background")),
        },
        "event_design": {
            "natural_event": (
                _text(life_event.get("continuous_event"))
                if visual_execution_enabled
                else ""
            ),
            "core_result_moment": _text(product_truth.get("content_mainline")),
            "starting_state": _text(
                life_event.get("starting_context") or emotion.get("starting_state")
            ),
            "ending_state": _text(
                life_event.get("ending_context") or emotion.get("ending_state")
            ),
        },
        "retention_hook": {
            "opening_event": "",
            "delayed_answer": "",
        },
        "voice_identity": {
            "tone": _text(voice.get("desired_tone")),
            "relationship_mode": _text(voice.get("viewer_relationship")),
            "particle_density": "NATURAL_1_TO_3",
            "sales_pressure": "LOW",
            "forbidden_tone": ["主播叫卖", "参数清单", "广告腔"],
        },
        "capture_context": {
            "capture_mode": _text(production.get("capture_mode")),
            "speaker_position": _text(
                seed.get("voiceover_surface_contract", {}).get("speaker_position")
            ),
            "relationship_to_lens": (
                "创作者本人面对自己的手机镜头向观众分享"
                if _text(production.get("capture_mode"))
                == CAPTURE_MODE_CREATOR_SELF_SHOT
                else "口播覆盖商品展示画面"
            ),
        },
    }
    reference = dict(frozen.get("execution_reference") or {})
    if _text(seed.get("optional_visual_inspiration", {}).get("status")) != "AVAILABLE":
        reference = {}
    direction = {
        "structure_contract": frozen.get("structure_contract") or {},
        "structure_execution_plan": {
            "macro_family_key": ">".join(seed.get("creative_direction", {}).get("macro_structure") or []),
            "shot_plan": shot_plan,
        },
        "execution_reference": reference,
        "content_bundle_brief": bundle,
        "p2_lite": frozen.get("p2_lite") or {},
        "creative_diversity_contract": frozen.get("creative_diversity_contract") or {},
        "creative_blueprint": creative_blueprint,
    }
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        # The central engine remains the sole dialogue author.  It receives the
        # frozen category authority as context, without a new prompt stage or
        # a second set of copywriting rules.
        direction["category_execution_extension"] = dict(category_extension)
        direction["carrier_specific_execution"] = dict(
            seed.get("carrier_specific_execution") or {}
        )
    visual_plan = {
        "schema_version": "simplified-voiceover-visual-plan-v1",
        "shots": shots,
        "production_design": production,
    }
    return direction, visual_plan


def assemble_simplified_complete_script(
    visual_script: Dict[str, Any],
    seed: Dict[str, Any],
    voiceover_plan: Dict[str, Any],
) -> Dict[str, Any]:
    result = dict(visual_script)
    production_design = (
        result.get("production_design")
        if isinstance(result.get("production_design"), dict)
        else {}
    )
    capture_rhythm_contract = (
        dict(seed.get("capture_rhythm_contract") or {})
        if isinstance(seed.get("capture_rhythm_contract"), dict)
        else build_capture_rhythm_contract(
            capture_mode=_text(production_design.get("capture_mode")),
            macro_structure=(seed.get("creative_direction") or {}).get("macro_structure") or [],
        )
    )
    compiled_storyboard, capture_units = compile_capture_units(
        result.get("storyboard") or [],
        capture_rhythm_contract,
    )
    result["storyboard"] = compiled_storyboard
    result["capture_rhythm_contract"] = capture_rhythm_contract
    result["capture_units"] = capture_units
    product_truth = seed.get("product_truth") if isinstance(seed.get("product_truth"), dict) else {}
    lines = [item for item in voiceover_plan.get("lines") or [] if isinstance(item, dict)]
    target = " ".join(_text(item.get("voiceover_text_target_language")) for item in lines).strip()
    translation = " ".join(_text(item.get("voiceover_text_zh")) for item in lines).strip()
    result["continuous_voiceover"] = {
        "hook_id": _text(voiceover_plan.get("hook_id")),
        "target_language": _text(voiceover_plan.get("target_language")),
        "target_language_key": _text(voiceover_plan.get("target_language_key")),
        "language_validation": _text(voiceover_plan.get("language_validation")),
        "target_text": target,
        "chinese_translation": translation,
        "selected_claim_ids": list(voiceover_plan.get("selected_claim_ids") or []),
        "selected_selling_argument_id": _text(
            voiceover_plan.get("selected_selling_argument_id")
        ),
        "selling_argument_realization": _text(
            voiceover_plan.get("selling_argument_realization")
        ),
        "selling_argument_realization_zh": _text(
            voiceover_plan.get("selling_argument_realization_zh")
        ),
        "generation_mode": "CENTRAL_VOICEOVER_COMPLETE_UTTERANCE",
    }
    brief_product_truth = {
        "product_identity": _text(product_truth.get("product_identity")),
        "identity_anchors": product_truth.get("identity_anchors") or [],
        "visible_detail_anchors": product_truth.get("visible_detail_anchors") or [],
    }
    if _text(product_truth.get("canonical_product_type")):
        brief_product_truth["canonical_product_type"] = _text(
            product_truth.get("canonical_product_type")
        )
    if isinstance(product_truth.get("display_quantity_contract"), dict):
        brief_product_truth["display_quantity_contract"] = dict(
            product_truth.get("display_quantity_contract") or {}
        )
    outfit_contract = dict(
        (seed.get("diversity_context") or {}).get("outfit_selection_contract")
        or {}
    )
    outfit_prompt_projection = _outfit_prompt_projection(outfit_contract)
    video_generation_brief = {
        "schema_version": VIDEO_BRIEF_SCHEMA_VERSION,
        "render_profile": VIDEO_RENDER_PROFILE,
        "capture_mode": _text(
            (result.get("production_design") or {}).get("capture_mode")
        ),
        "production_design": result.get("production_design") or {},
        "storyboard": result.get("storyboard") or [],
        "capture_rhythm_contract": capture_rhythm_contract,
        "capture_units": capture_units,
        "product_truth": brief_product_truth,
        "product_identity_lock": build_product_identity_lock(brief_product_truth),
        "action_design": dict(seed.get("action_design") or {}),
        "outfit_selection_contract": outfit_contract,
        "outfit_prompt_projection": outfit_prompt_projection,
        "persona_selection_contract": dict(
            (seed.get("diversity_context") or {}).get("persona_selection_contract")
            or {}
        ),
        "outfit_persona_affinity_contract": dict(
            (seed.get("diversity_context") or {}).get(
                "outfit_persona_affinity_contract"
            ) or {}
        ),
        "outfit_scene_affinity_contract": dict(
            (seed.get("diversity_context") or {}).get(
                "outfit_scene_affinity_contract"
            ) or {}
        ),
        "voiceover": result["continuous_voiceover"],
        "instruction": (
            "保持同一人物、商品、穿搭、地点、时刻和生活事件；按拍摄节奏合同分别录制2至3段普通手机素材并直接剪切。"
            "商品一致性优先于场景美感和镜头效果"
            if _text(capture_rhythm_contract.get("profile")) == CAPTURE_RHYTHM_MULTICLIP
            else "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果"
        ),
    }
    visual_contract = (
        dict(seed.get("visual_execution_contract") or {})
        if isinstance(seed.get("visual_execution_contract"), dict)
        else {}
    )
    if visual_contract:
        from core.visual_execution_contract import finalize_visual_execution_contract

        visual_contract = finalize_visual_execution_contract(
            visual_contract,
            production_scene=(result.get("production_design") or {}).get("scene") or {},
            presentation_mode=_text(
                (result.get("production_design") or {}).get("presentation_mode")
            ),
        )
        result["visual_execution_contract"] = visual_contract
        video_generation_brief["visual_execution_contract"] = visual_contract
        video_generation_brief["instruction"] = (
            "保持同一人物、商品、穿搭、地点、时刻和生活事件；商品严格按参考图保持一致，"
            "按拍摄节奏合同分别录制普通手机素材并直接剪切，同时保留人物造型与真实场景的完整表达"
        )
        video_generation_brief["visual_execution_diagnostics"] = dict(
            result.get("visual_execution_diagnostics") or {}
        )
    if _text(video_generation_brief["persona_selection_contract"].get("availability")) == "AVAILABLE":
        video_generation_brief["instruction"] = (
            "保持冻结人物模板、商品、穿搭、地点、时刻和连续事件；按拍摄节奏合同分别录制普通手机素材并直接剪切；人物身份只取人物模板参考，"
            "商品外观只取商品参考，商品图中的模特、滤镜、姿态和背景均无人物权威"
        )
    category_extension = (
        seed.get("category_execution_extension")
        if isinstance(seed.get("category_execution_extension"), dict)
        else {}
    )
    if category_extension:
        from core.category_execution import (
            build_category_video_brief,
            resolve_category_carrier_execution,
        )

        carrier_execution = (
            (result.get("production_design") or {}).get("accessory_execution")
            if isinstance(
                (result.get("production_design") or {}).get("accessory_execution"),
                dict,
            )
            else {}
        )
        if not carrier_execution:
            carrier_execution = resolve_category_carrier_execution(
                category_extension,
                presentation_mode=_text(
                    (result.get("production_design") or {}).get("presentation_mode")
                ),
            )
        category_video_brief = build_category_video_brief(
            category_extension,
            carrier_execution=carrier_execution,
        )
        video_generation_brief["category_execution_extension"] = dict(
            category_extension
        )
        video_generation_brief["accessory_execution_brief"] = category_video_brief
    result["video_generation_brief"] = video_generation_brief
    result["assembly_provenance"] = {
        "script_mode": SCRIPT_MODE_SIMPLIFIED,
        "downstream_visual_rewritten": False,
        "voiceover_engine": "central-complete-voiceover",
        "selling_argument_id": _text(
            (product_truth.get("selling_argument") or {}).get("argument_id")
            if isinstance(product_truth.get("selling_argument"), dict)
            else ""
        ),
        "selling_argument_source_claim_ids": list(
            (product_truth.get("selling_argument") or {}).get("source_claim_ids") or []
            if isinstance(product_truth.get("selling_argument"), dict)
            else []
        ),
    }
    result["complete_script_id"] = _stable_id("SCSCRIPT_", result)
    return result


def _outfit_prompt_projection(contract: Dict[str, Any]) -> Dict[str, str]:
    """Compile only prompt-visible styling details from the frozen contract.

    The projection is deliberately compact and soft.  It prevents the final
    prompt from losing an already selected top/bottom/hair/palette decision,
    without creating another validation layer or repeating internal metadata.
    """

    if not isinstance(contract, dict) or not contract:
        return {}
    recipe = contract.get("outfit_recipe")
    recipe = recipe if isinstance(recipe, dict) else {}
    labels = {
        "one_piece": "连体单品",
        "top": "上装",
        "bottom": "下装",
        "footwear": "鞋履",
        "bag": "包袋",
        "other_accessories": "辅助配饰",
    }
    def prompt_text(value: Any) -> str:
        text = _text(value)
        if text.upper() in {"UNAVAILABLE", "NOT_APPLICABLE"} or text in {
            "不适用", "不由穿搭模板决定",
        }:
            return ""
        if text.count("（") > text.count("）"):
            text = text.replace("（", "，")
        return text

    recipe_parts = [
        f"{labels[key]}：{prompt_text(recipe.get(key))}"
        for key in labels
        if prompt_text(recipe.get(key))
    ]
    if prompt_text(contract.get("inner_requirements")):
        recipe_parts.append(
            f"内搭要求：{prompt_text(contract.get('inner_requirements'))}"
        )
    if not recipe_parts and _text(contract.get("base_outfit_direction")):
        recipe_parts = [_text(contract.get("base_outfit_direction"))]
    grooming_parts = [
        prompt_text(contract.get("hair_direction")),
        prompt_text(contract.get("neckline_direction")),
        prompt_text(contract.get("outer_layer_direction")),
    ]
    style_family = _text(contract.get("style_family"))
    style_direction = (
        f"整体风格：{style_family}"
        if style_family
        and style_family != "STRUCTURED_TEMPLATE"
        and not re.fullmatch(r"[A-Z0-9_]+", style_family)
        else ""
    )
    finish_parts = [
        style_direction,
        prompt_text(contract.get("palette_relation")),
        prompt_text(contract.get("visibility_requirement")),
        prompt_text(contract.get("finish_direction")),
    ]
    return {
        "frozen_outfit": "；".join(item for item in recipe_parts if item),
        "hair_neckline_outer": "；".join(item for item in grooming_parts if item),
        "palette_visibility_finish": "；".join(item for item in finish_parts if item),
        "authority": "FROZEN_SOFT_STYLING",
    }


def validate_simplified_complete_script(script: Dict[str, Any]) -> Dict[str, Any]:
    voice = script.get("continuous_voiceover") if isinstance(script.get("continuous_voiceover"), dict) else {}
    issues = []
    if not _text(voice.get("target_text")) or not _text(voice.get("chinese_translation")):
        issues.append("中央口播没有成功装配到完整脚本")
    video_brief = script.get("video_generation_brief")
    if not isinstance(video_brief, dict):
        issues.append("完整脚本缺少视频生成简报")
    elif _text(video_brief.get("schema_version")) in {
        VIDEO_BRIEF_SCHEMA_VERSION,
        "production-video-brief-v2-ugc-native",
    }:
        identity_lock = (
            video_brief.get("product_identity_lock")
            if isinstance(video_brief.get("product_identity_lock"), dict)
            else {}
        )
        if not identity_lock.get("reference_image_is_authority"):
            issues.append("视频生成简报没有声明参考图商品权威")
        if not _dedupe_text(identity_lock.get("must_preserve") or []):
            issues.append("视频生成简报缺少商品身份锁")
        if not _dedupe_text(identity_lock.get("must_not_change") or []):
            issues.append("视频生成简报缺少商品负向约束")
        category_extension = (
            video_brief.get("category_execution_extension")
            if isinstance(video_brief.get("category_execution_extension"), dict)
            else {}
        )
        if category_extension:
            from core.category_execution import validate_category_execution_identity

            issues.extend(
                validate_category_execution_identity(
                    category_extension,
                    script=script,
                )
            )
    return {
        "valid": not issues,
        "issues": issues,
        "policy_version": "simplified-complete-assembly-v2-product-lock",
    }
