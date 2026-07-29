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
from typing import Any, Dict, Iterable, List, Tuple


SCRIPT_MODE_LEGACY = "legacy_v2"
SCRIPT_MODE_SIMPLIFIED = "simplified_v1"
CREATIVE_SEED_SCHEMA_VERSION = "simplified-creative-seed-v6-audience-relation"
VISUAL_SCRIPT_SCHEMA_VERSION = "simplified-complete-visual-script-v3-shared-evidence-pool"
VALIDATION_POLICY_VERSION = "simplified-minimum-gates-v3-whole-video-evidence"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, material: Any) -> str:
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


def _preferred_presentation(
    *,
    source_carrier: str,
    bundle: Dict[str, Any],
    product_type: str,
    top_category: str,
    anchor_card: Dict[str, Any],
) -> Tuple[str, str]:
    carrier = _text(source_carrier).upper()
    facts = [
        _text(item.get("fact_text"))
        for item in bundle.get("claim_atoms") or []
        if isinstance(item, dict) and _text(item.get("fact_text"))
    ]
    if carrier == "STATIC_PRODUCT" and any(
        not _claim_needs_wearer(fact) for fact in facts
    ):
        return "STATIC_PRODUCT", "静物结构已有可独立拍清的商品细节事实"
    bundle_text = json.dumps(bundle, ensure_ascii=False).lower()
    if _is_apparel(product_type, top_category, anchor_card) and any(
        term in bundle_text for term in _WEARER_TERMS
    ):
        return "PERSON_ON_CAMERA", "当前内容主张必须通过真人上身或穿搭状态证明"
    if carrier in {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"}:
        return "PERSON_ON_CAMERA", "结构来源适合真人承载"
    if carrier in {"HAND_ONLY", "HANDS_ONLY"}:
        return "HANDS_ONLY", "结构来源适合手部演示"
    if carrier == "STATIC_PRODUCT":
        return "STATIC_PRODUCT", "当前内容只需商品静物与细节证明"
    return "PERSON_ON_CAMERA", "没有可靠承载信息，原创服饰默认使用真人自然分享"


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
    safe_value = dict(raw_value) if isinstance(raw_value, dict) else {}
    safe_argument = dict(raw_argument)
    argument_is_available = _text(raw_argument.get("status")) == "AVAILABLE"
    argument_compatible = not compatible_carriers or (
        "STATIC_PRODUCT" in compatible_carriers
        if presentation == "STATIC_PRODUCT"
        else bool({"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"} & compatible_carriers)
    )
    # Carrier compatibility is a creative preference, not a veto over an
    # operator-maintained selling point.  A static direction may still speak
    # an authorised wearing/use value; the visible facts simply remain
    # optional supporting detail rather than fabricated causal proof.
    if argument_is_available:
        safe_argument["carrier_match_status"] = (
            "MATCHED" if argument_compatible else "UNMATCHED"
        )
    # An AVAILABLE operator-maintained selling argument is the content
    # authority.  Some historical bundles have that argument populated while
    # value_proposition is empty or stale; never fall through to a button or
    # pocket fact in that case.
    mainline = (
        _text(safe_argument.get("core_value"))
        if argument_is_available
        else _text(safe_value.get("text"))
    )
    if argument_is_available and not _text(safe_value.get("text")):
        safe_value = {
            "status": "AVAILABLE",
            "text": mainline,
            "authority": "SELLING_ARGUMENT",
            "allowed_strength": _text(safe_argument.get("allowed_strength")),
        }
    if not mainline:
        mainline = (
            _text(claim_atoms[0].get("fact_text"))
            if claim_atoms
            else _text(content_bundle.get("content_mainline"))
        )
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
            "core_proof_claim_keys": list(safe_argument.get("core_proof_claim_keys") or []),
            "optional_visual_claim_keys": list(safe_argument.get("optional_visual_claim_keys") or []),
            "content_mode": (
                "SELLING_ARGUMENT"
                if _text(safe_argument.get("status")) == "AVAILABLE" and mainline
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
            "source_structure_carrier": source_carrier,
            "continuity_hint": _text(hard.get("continuity_mode")),
        },
        # This is a frozen surface preference for the central voiceover
        # engine.  It changes only the speaker-to-viewer relationship, never
        # product truth, visual design, validation, or retry behaviour.
        "voiceover_surface_contract": {
            "relationship_device": _text(relationship_device) or "HOOK_DECIDES",
            "policy_version": "audience-relation-rotation-v1",
            "hard_required": False,
        },
        "diversity_context": {
            "preferred_persona_role": _text(creative_contract.get("persona_role")),
            "preferred_scene_motif": _text(creative_contract.get("scene_motif")),
            "avoid_patterns": _dedupe_text(
                [
                    *(creative_contract.get("anti_template_rules") or []),
                    *(creative_contract.get("forbidden_recent_patterns") or []),
                ],
                limit=10,
            ),
            "instruction": "这些只用于促成人物与场景差异，不要求设计剧情或卖点触发动作",
        },
        "optional_visual_inspiration": optional_reference,
    }
    seed["creative_seed_id"] = _stable_id("SCS_", seed)
    return seed


def build_simplified_script_prompt(
    seed: Dict[str, Any],
    *,
    target_country: str,
    target_language: str,
    duration_seconds: float,
) -> str:
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
            "character": {
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
            },
            "emotion": {
                "starting_state": "开场自然状态",
                "natural_change": "动作带来的轻微变化",
                "ending_state": "结尾状态",
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
    return f"""你是短视频导演与完整脚本作者。请为{target_country}市场生成一条约{duration_seconds:g}秒的原创商品短视频视觉脚本。

这不是分层规划题。请一次写出能够直接拍摄/生成的完整成片设计：人物、外形、穿搭、场景、自然状态和4至6个分镜必须同时成立。

核心原则：
1. 商品事实只能来自 product_truth；不知道的内容不补写，绝不虚构功效、材质、颜色或使用结果。
2. product_truth.content_mode=SELLING_ARGUMENT 时，content_mainline / selling_argument.core_value 是全片的购买理由，但不要求人物动作或场景制造这个理由；只需让核心证明在普通状态中清楚可见。approved_claims 只用作画面证据，禁止把第一个扣子、口袋或袖型细节改写成全片主题。content_mode=FACTUAL_OBSERVATION 时围绕可见事实做观察，不伪造用户痛点或产品收益。
3. creative_direction.macro_structure 只控制观看顺序，不规定统一镜头模板；requested_hook_id 只描述口播意图，本步骤不写{target_language}口播。
4. presentation_mode 必须等于 preferred_presentation。PERSON_ON_CAMERA 必须写完整人物、穿搭、场景和自然状态；STATIC_PRODUCT 不虚构出镜人物或商品情绪；HANDS_ONLY 只允许手部进入画面。
5. 默认采用观察式画面：人物可以已经穿好后自然站立、坐着、走动或完成一个无特殊意义的简单动作；静物可以保持静置或由手自然展开。不要为了证明卖点制造遮挡后揭示、偶然吹开、通知弹出、道具机关或“恰好发现”等剧情。动作不必承担卖点因果，商品本身可见即可。
6. 商品锚点与 claim_key 必须逐字从输入中选择。approved_claims 是可选事实池，不是拍摄清单：只选择当前结构自然需要的少量事实，未选事实无需安排镜头。被写入 selling_points_used 或 supported_claim_keys 的事实必须来自池内；同一事实只需全片有一处自然可见，不要求逐项触摸、指向或分配独立动作。本步骤不得决定中央口播最终选择哪些事实，也不得按口播逐句设计镜头。
7. 人物和场景要具体但克制，情绪是自然的小变化，不写广告演员式惊讶。可借鉴 AVAILABLE 的视觉参考；SKIPPED_INCOMPATIBLE 视为不存在。
8. 只返回一个JSON对象，不要Markdown，不要解释。字段齐全，结构如下：
{json.dumps(schema, ensure_ascii=False, indent=2)}

冻结输入：
{json.dumps(seed, ensure_ascii=False, indent=2)}
"""


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
        anchor for anchor in preserved_identity if anchor not in approved_anchors
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
            if _text(item) and _text(item) not in approved_anchors
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

    authorized_text = json.dumps(truth, ensure_ascii=False)
    full_text = json.dumps(script, ensure_ascii=False)
    for term in ("显腿长", "腿更长", "显瘦", "显高", "塑形", "保暖", "舒适", "不挑人", "百搭"):
        if term in full_text and term not in authorized_text:
            issues.append(f"商品事实冲突：出现未授权效果词={term}")

    # 2. Carrier conflict and completeness within that carrier.
    preferred = _text(seed.get("creative_direction", {}).get("preferred_presentation")).upper()
    actual = _text(production.get("presentation_mode")).upper()
    if actual != preferred:
        issues.append(f"承载冲突：要求{preferred or '未知'}，实际{actual or '空'}")
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
    # The frozen seed is the final authority after carrier compatibility is
    # resolved.  Keep visible claims as evidence; never let their first item
    # overwrite the selling argument as the speech mainline.
    if isinstance(product_truth.get("value_proposition"), dict):
        bundle["value_proposition"] = dict(product_truth.get("value_proposition"))
    if isinstance(product_truth.get("selling_argument"), dict):
        bundle["selling_argument"] = dict(product_truth.get("selling_argument"))
    bundle["content_mode"] = _text(product_truth.get("content_mode")) or _text(bundle.get("content_mode"))
    bundle["content_mainline"] = _text(product_truth.get("content_mainline")) or _text(bundle.get("content_mainline"))
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
            "natural_event": "",
            "core_result_moment": _text(product_truth.get("content_mainline")),
            "starting_state": _text(emotion.get("starting_state")),
            "ending_state": _text(emotion.get("ending_state")),
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
    product_truth = seed.get("product_truth") if isinstance(seed.get("product_truth"), dict) else {}
    lines = [item for item in voiceover_plan.get("lines") or [] if isinstance(item, dict)]
    target = " ".join(_text(item.get("voiceover_text_target_language")) for item in lines).strip()
    translation = " ".join(_text(item.get("voiceover_text_zh")) for item in lines).strip()
    result["continuous_voiceover"] = {
        "hook_id": _text(voiceover_plan.get("hook_id")),
        "target_text": target,
        "chinese_translation": translation,
        "selected_claim_ids": list(voiceover_plan.get("selected_claim_ids") or []),
        "selected_selling_argument_id": _text(
            voiceover_plan.get("selected_selling_argument_id")
        ),
        "selling_argument_realization": _text(
            voiceover_plan.get("selling_argument_realization")
        ),
        "generation_mode": "CENTRAL_VOICEOVER_COMPLETE_UTTERANCE",
    }
    result["video_generation_brief"] = {
        "production_design": result.get("production_design") or {},
        "storyboard": result.get("storyboard") or [],
        "product_truth": {
            "identity_anchors": seed.get("product_truth", {}).get("identity_anchors") or [],
            "visible_detail_anchors": seed.get("product_truth", {}).get("visible_detail_anchors") or [],
        },
        "voiceover": result["continuous_voiceover"],
        "instruction": "保持同一人物、穿搭、场景和连续事件；按分镜执行，不重新设计语义",
    }
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


def validate_simplified_complete_script(script: Dict[str, Any]) -> Dict[str, Any]:
    voice = script.get("continuous_voiceover") if isinstance(script.get("continuous_voiceover"), dict) else {}
    issues = []
    if not _text(voice.get("target_text")) or not _text(voice.get("chinese_translation")):
        issues.append("中央口播没有成功装配到完整脚本")
    if not isinstance(script.get("video_generation_brief"), dict):
        issues.append("完整脚本缺少视频生成简报")
    return {
        "valid": not issues,
        "issues": issues,
        "policy_version": "simplified-complete-assembly-v1",
    }
