"""Thin complete-utterance bridge for batch original scripts.

The central voiceover engine still owns hook vocabulary and factual authority.
This bridge deliberately skips the old line/beat planner: one model call writes
one complete utterance and code only mounts it across the whole visual plan.
"""
from __future__ import annotations

import json
import hashlib
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from core.complete_script_v3 import creative_product_profile
from core.reality_reference import validate_voiceover_plan
from core.reality_voiceover_bridge import (
    VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH,
    build_voiceover_expression_contract,
    load_active_voiceover_hooks,
    resolve_voiceover_hook_policy,
    select_voiceover_claim_atoms,
)


SCHEMA_VERSION = "creative-full-script-voiceover-v5-multilingual"
HOOK_EXECUTION_POLICY_VERSION = "central-voiceover-v36-target-language-safe"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _target_language_key(value: str) -> str:
    normalized = _text(value).lower().replace("_", "-")
    if any(token in normalized for token in ("泰语", "thai", "th-th")) or normalized == "th":
        return "th"
    if any(token in normalized for token in ("越南语", "vietnamese", "vi-vn")) or normalized == "vi":
        return "vi"
    if any(token in normalized for token in ("马来语", "马来西亚语", "malay", "ms-my")) or normalized == "ms":
        return "ms"
    if any(token in normalized for token in ("中文", "汉语", "chinese", "zh-cn")) or normalized == "zh":
        return "zh"
    return "unknown"


def _target_language_error(text: str, target_language: str) -> str:
    """Reject a mislabeled target-language field before it reaches Feishu.

    This is deliberately a script-family check, not a fluency score.  It catches
    the production regression where the Thai-only model wrapper returned Thai
    while a batch was frozen as Malay or Vietnamese.
    """

    value = _text(text)
    if not value:
        return "目标语言口播为空"
    key = _target_language_key(target_language)
    thai_count = len(re.findall(r"[\u0E00-\u0E7F]", value))
    cjk_count = len(re.findall(r"[\u3400-\u9FFF]", value))
    latin_count = len(re.findall(r"[A-Za-zÀ-ỹĐđ]", value))
    compact_count = len(re.sub(r"\s+", "", value))
    if key == "th":
        if cjk_count or thai_count < max(4, int(compact_count * 0.35)):
            return "任务目标语言为泰语，但口播正文不是泰语"
    elif key == "vi":
        vietnamese_marks = len(re.findall(
            r"[ăâđêôơưĂÂĐÊÔƠƯáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệ"
            r"íìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ"
            r"ÁÀẢÃẠẤẦẨẪẬẮẰẲẴẶÉÈẺẼẸẾỀỂỄỆÍÌỈĨỊÓÒỎÕỌ"
            r"ỐỒỔỖỘỚỜỞỠỢÚÙỦŨỤỨỪỬỮỰÝỲỶỸỴ]",
            value,
        ))
        if thai_count or cjk_count or latin_count < 8 or vietnamese_marks < 2:
            return "任务目标语言为越南语，但口播正文不是越南语"
    elif key == "ms":
        malay_markers = re.findall(
            r"\b(?:yang|untuk|ini|itu|kalau|dengan|nampak|saya|aku|kita|korang|"
            r"bila|memang|boleh|tak|dekat|pada|dari|tengok|baru)\b",
            value.lower(),
        )
        if thai_count or cjk_count or latin_count < 8 or len(malay_markers) < 2:
            return "任务目标语言为马来语，但口播正文不是马来语"
    elif key == "zh":
        if cjk_count < 2:
            return "任务目标语言为中文，但口播正文不是中文"
    return ""


def _estimated_spoken_seconds(text: str, target_language: str) -> float:
    if _target_language_key(target_language) in {"vi", "ms"}:
        words = re.findall(r"[A-Za-zÀ-ỹĐđ]+", _text(text))
        return round(len(words) / 2.7, 2)
    return round(len(re.sub(r"\s+", "", _text(text))) / 13.0, 2)


def hook_knowledge_snapshot_hash() -> str:
    """Return a compact dependency fingerprint for the governed hook snapshot."""

    try:
        return hashlib.sha256(
            Path(VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH).read_bytes()
        ).hexdigest()[:20].upper()
    except Exception:
        return "UNAVAILABLE"


def _invoke_model(model_command: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    command = shlex.split(model_command)
    if not command:
        raise ValueError("批次完整口播需要 voiceover_model_command")
    completed = subprocess.run(
        command,
        input=json.dumps(
            {"contract_name": "creative_full_single_v1", "payload": payload},
            ensure_ascii=False,
        ),
        text=True,
        capture_output=True,
        timeout=360,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "")[:1200]
        raise RuntimeError(
            f"中央完整口播模型失败(code={completed.returncode}): {detail}"
        )
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"中央完整口播返回非JSON: {completed.stdout[:1200]}"
        ) from exc
    if result.get("error"):
        raise RuntimeError(_text(result.get("error")))
    return result


def _expression_with_selected_claims(
    direction: Dict[str, Any], visual_plan: Dict[str, Any]
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    expression = build_voiceover_expression_contract(direction, visual_plan)
    atoms = [
        item
        for item in expression.get("claim_atoms") or []
        if isinstance(item, dict) and item.get("supported_shot_nos")
    ]
    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    selling_argument_mode = (
        _text(bundle.get("content_mode")).upper() == "SELLING_ARGUMENT"
    )
    if selling_argument_mode:
        # The governed selling argument already owns the utterance.  Give the
        # model at most one visual fact, preferring a fact explicitly linked to
        # that argument, rather than turning a 15s share into a feature list.
        direct_support = [
            item for item in atoms
            if _text(item.get("argument_relation")).upper() == "DIRECT_SUPPORT"
        ]
        atoms = direct_support or atoms
    preferred_count = 1
    selected, _ = select_voiceover_claim_atoms(
        atoms, preferred_count=preferred_count
    )
    selected_keys = {_text(item.get("claim_key")) for item in selected}
    expression["claim_atoms"] = selected
    argument = expression.get("argument_contract")
    if isinstance(argument, dict):
        content = argument.get("content")
        if isinstance(content, dict):
            content["proof_atoms"] = [
                item
                for item in content.get("proof_atoms") or []
                if isinstance(item, dict)
                and _text(item.get("claim_key")) in selected_keys
            ]
    return expression, selected


def _country_key(value: str) -> str:
    raw = _text(value).lower()
    if raw in {"th", "tha", "thailand", "泰国", "ประเทศไทย"}:
        return "TH"
    return raw.upper()


def _category_key(value: str) -> str:
    raw = _text(value).lower()
    if raw in {"女装", "womenswear", "women's wear", "women apparel"} or "女装" in raw:
        return "womenswear"
    if raw in {"配饰", "accessory", "accessories"} or "配饰" in raw:
        return "accessories"
    return raw


def _expression_family(category: str, product_type: str = "") -> str:
    """Return a rhetoric-only family; it never grants product facts.

    The approved sample library was originally curated under womenswear.  A
    wearable female-fashion accessory can safely learn its viewer relationship
    and cadence, while its product facts still come exclusively from the
    current selling-argument contract.
    """

    category_key = _category_key(category)
    material = f"{category_key} {_text(product_type).lower()}"
    if category_key == "womenswear":
        return "FEMALE_FASHION_WEARABLE"
    if category_key == "accessories" and any(
        token in material
        for token in (
            "丝巾", "围巾", "头巾", "耳饰", "耳环", "发饰", "发夹",
            "手链", "手镯", "手环", "手串",
            "silk_scarf", "scarf", "headscarf", "earring", "hair",
            "bracelet", "bangle",
        )
    ):
        return "FEMALE_FASHION_WEARABLE"
    return ""


def _approved_style_references(
    hook_id: str,
    *,
    target_country: str = "",
    top_category: str = "",
    product_type: str = "",
    limit: int = 2,
) -> List[Dict[str, Any]]:
    """Return governed same-hook samples using an explicit compatibility tier.

    Exact category evidence wins.  A same-hook, same-country sample from the
    shared female-fashion wearable family may teach rhetoric only.  There is
    deliberately no cross-hook fallback.
    """

    path = Path(VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH)
    try:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    assignments = [
        item for item in snapshot.get("assignments") or [] if isinstance(item, dict)
    ]
    matched_ids = [
        _text(item.get("example_id"))
        for item in sorted(
            (
                item for item in assignments
                if _text(item.get("archetype_id")) == hook_id
            ),
            key=lambda item: (
                -int(bool(item.get("is_primary"))),
                -float(item.get("match_confidence") or 0),
            ),
        )
        if _text(item.get("example_id"))
    ]
    examples = {
        _text(item.get("example_id")): item
        for item in snapshot.get("examples") or []
        if isinstance(item, dict) and item.get("source_authorized")
        and _text(item.get("quality_status")) == "approved_sample"
    }
    result: List[Dict[str, Any]] = []
    country_key = _country_key(target_country)
    category_key = _category_key(top_category)
    requested_family = _expression_family(top_category, product_type)
    for example_id in dict.fromkeys(matched_ids):
        example = examples.get(example_id)
        if not example:
            continue
        example_country = _country_key(_text(example.get("country")))
        example_category = _category_key(_text(example.get("category")))
        if country_key and example_country and country_key != example_country:
            continue
        match_tier = ""
        if category_key and example_category and category_key == example_category:
            match_tier = "EXACT_CATEGORY"
        elif (
            requested_family
            and requested_family
            == _expression_family(example_category, _text(example.get("product_type")))
        ):
            match_tier = "EXPRESSION_FAMILY"
        if not match_tier:
            continue
        excerpt = re.sub(r"\s+", " ", _text(example.get("raw_text")))[:680]
        if not excerpt:
            continue
        result.append({
            "reference_sample_id": example_id,
            "reference_excerpt": excerpt,
            "source_country": _text(example.get("country")),
            "source_category": _text(example.get("category")),
            "source_language": _text(example.get("language")),
            "match_tier": match_tier,
            "expression_family": requested_family,
            "usage_boundary": "只学习观众关系、节奏、衔接和信息密度；不得继承事实、材质、功效、CTA或原句",
        })
    result.sort(
        key=lambda item: (
            0 if item.get("match_tier") == "EXACT_CATEGORY" else 1,
            matched_ids.index(item["reference_sample_id"]),
        )
    )
    return result[:limit]


def _style_reference_selection_policy(references: List[Dict[str, Any]]) -> str:
    tiers = {_text(item.get("match_tier")) for item in references}
    if "EXACT_CATEGORY" in tiers:
        return "EXACT_HOOK_COUNTRY_CATEGORY"
    if "EXPRESSION_FAMILY" in tiers:
        return "EXACT_HOOK_COUNTRY_EXPRESSION_FAMILY"
    return "HOOK_ARCHETYPE_ONLY"


def _narrative_anchor_options(creative: Dict[str, Any]) -> List[Dict[str, str]]:
    grounding_mode = _text(creative.get("grounding_mode"))
    if grounding_mode == "PRODUCT_OBSERVATION":
        # In the simplified observational flow, action and scene support the
        # image only.  They are not a reason for the product fact and must not
        # become a staged "I just noticed it when..." voiceover device.
        return []
    if grounding_mode == "CONTENT_FIRST_WHOLE_VIDEO":
        # Restore a human speaking position without turning a prop or shot
        # action into the reason a product fact exists.
        result: List[Dict[str, str]] = []
        motivation = _text(creative.get("creator_motivation"))
        scene_moment = _text(creative.get("scene_moment"))
        if motivation:
            result.append({
                "source": "speaker_intent",
                "label": "人物此刻愿意分享的原因",
                "moment_zh": motivation,
            })
        if scene_moment:
            result.append({
                "source": "scene_moment",
                "label": "人物所处的普通生活时刻",
                "moment_zh": scene_moment,
            })
        return result[:2]
    labels = {
        "event_context": "人物正在完成的普通动作",
        "core_result_moment": "动作中可见的商品结果",
        "scene_moment": "当下生活时刻",
        "opening_event": "开场正在发生的事情",
    }
    result: List[Dict[str, str]] = []
    for key in ("event_context", "core_result_moment", "scene_moment", "opening_event"):
        value = _text(creative.get(key))
        if value and value.upper() != "UNAVAILABLE":
            result.append({"source": key, "label": labels[key], "moment_zh": value})
    return result[:3]


def _relationship_language_profile(
    hook_id: str,
    requested_device: str = "",
    target_language: str = "泰语",
) -> Dict[str, Any]:
    requested = _text(requested_device).upper()
    default_device = (
        "VIEWER_REFERENCE"
        if hook_id in {"AUDIENCE_NEED_CALLOUT", "PAIN_REFRAME", "USER_ADVOCACY_STANCE"}
        else "REACTION_OR_VIEWER_INVITATION"
    )
    language_key = _target_language_key(target_language)
    language_surfaces = {
        "th": {
            "audience_addresses": ["สาวๆ"],
            "viewer_reference_forms": ["ใครอยาก", "ใครที่กำลัง", "คนไหนชอบ"],
            "reaction_openers": ["เอาจริงนะ", "เพิ่งสังเกตว่า", "ดูนี่ก่อน"],
            "natural_particles": ["นะ", "ค่ะ", "แหละ", "เลย"],
        },
        "vi": {
            "audience_addresses": ["các bạn", "chị em"],
            "viewer_reference_forms": ["ai đang", "ai thích", "nếu bạn đang"],
            "reaction_openers": ["nói thật nhé", "mình vừa để ý", "nhìn này"],
            "natural_particles": ["nhé", "nè", "đấy", "luôn"],
        },
        "ms": {
            "audience_addresses": ["korang"],
            "viewer_reference_forms": ["siapa yang tengah", "kalau korang suka", "yang sedang cari"],
            "reaction_openers": ["jujur cakap", "baru perasan", "tengok ni"],
            "natural_particles": ["ya", "lah", "tau", "memang"],
        },
    }.get(language_key, {
        "audience_addresses": [],
        "viewer_reference_forms": [],
        "reaction_openers": [],
        "natural_particles": [],
    })
    return {
        "assigned_device": requested or "HOOK_DECIDES",
        "preferred_device": default_device,
        "target_language": target_language,
        **language_surfaces,
        "instruction": "这是软表达偏好，不是事实或质检约束。一次只自然使用一种关系装置，并立即进入具体需求、发现或事实；不要堆叠称呼、反问和语气词。",
        "hard_required": False,
    }


def _detect_relationship_device(target_text: str) -> str:
    """Lightweight provenance only; absence never rejects a generated copy."""
    text = _text(target_text)
    if "สาวๆ" in text:
        return "AUDIENCE_ADDRESS"
    if any(marker in text for marker in ("ใครอยาก", "ใครที่กำลัง", "คนไหนชอบ", "ใคร")):
        return "VIEWER_REFERENCE"
    if any(marker in text for marker in ("ดูนี่ก่อน", "ลองดู", "มาดู")):
        return "VIEWER_INVITATION"
    if any(marker in text for marker in ("สำหรับเรา", "เอาจริงนะ", "เพิ่งสังเกตว่า", "เรา")):
        return "PERSONAL_STANCE"
    return "NO_ADDRESS"


def _relationship_surface_text(target_text: str) -> str:
    text = _text(target_text)
    for marker in (
        "สาวๆ", "ใครอยาก", "ใครที่กำลัง", "คนไหนชอบ", "ใคร",
        "ดูนี่ก่อน", "ลองดู", "มาดู", "สำหรับเรา", "เอาจริงนะ",
        "เพิ่งสังเกตว่า",
    ):
        if marker in text:
            return marker
    return ""


def _hook_surface_status(hook_id: str, target_text: str) -> str:
    """Provide a conservative soft observation, never a quality gate."""

    text = _text(target_text)
    if not text:
        return "UNKNOWN"
    marker_groups = {
        "AUDIENCE_NEED_CALLOUT": (
            "ใครอยาก", "ใครที่กำลัง", "สาวๆ", "ไหม", "?",
        ),
        "PAIN_REFRAME": ("ไหม", "เคย", "กังวล", "ปัญหา", "?"),
        "DISCOVERY_RESULT_PROMISE": (
            "เพิ่งสังเกต", "พอลอง", "พอใส่", "เพิ่งเห็น",
        ),
        "DETAIL_SURPRISE": ("ดูนี่", "ลองดู", "ตรงนี้", "รายละเอียด", "สังเกต"),
        "VISUAL_RESULT_DIRECT": ("พอใส่", "ดู", "เห็น", "ลุค"),
        "USER_ADVOCACY_STANCE": ("สาวๆ", "ใคร", "สำหรับเรา", "เราว่า"),
        "GENERAL_PRODUCT_SHARE": ("สำหรับเรา", "เอาจริงนะ", "วันนี้", "ตัวนี้"),
    }
    markers = marker_groups.get(_text(hook_id).upper())
    if not markers:
        return "UNKNOWN"
    return "REALIZED" if any(marker in text for marker in markers) else "WEAK"


def run_central_complete_voiceover(
    *,
    product_code: str,
    target_country: str,
    target_language: str,
    top_category: str,
    product_type: str,
    direction: Dict[str, Any],
    visual_plan: Dict[str, Any],
    voiceover_root: str = "",
    voiceover_db_path: str = "",
    model_command: str = "",
    candidate_hook_id: str = "",
    relationship_device: str = "",
) -> Dict[str, Any]:
    hooks = load_active_voiceover_hooks(
        voiceover_root or None, db_path=voiceover_db_path or None
    )
    active_ids = {_text(item.get("hook_id")) for item in hooks if _text(item.get("hook_id"))}
    hook_policy = resolve_voiceover_hook_policy(
        direction, active_ids, requested_hook_id=candidate_hook_id
    )
    hook_id = _text(hook_policy.get("selected_hook_id"))
    hook_row = next(
        (item for item in hooks if _text(item.get("hook_id")) == hook_id), {}
    )
    approved_style_references = _approved_style_references(
        hook_id,
        target_country=target_country,
        top_category=top_category,
        product_type=product_type,
    )
    style_selection_policy = _style_reference_selection_policy(
        approved_style_references
    )
    hook_snapshot_hash = hook_knowledge_snapshot_hash()
    expression, selected_atoms = _expression_with_selected_claims(
        direction, visual_plan
    )
    argument = expression.get("argument_contract") if isinstance(expression.get("argument_contract"), dict) else {}
    content = argument.get("content") if isinstance(argument.get("content"), dict) else {}
    tension = content.get("audience_tension") if isinstance(content.get("audience_tension"), dict) else {}
    value = content.get("value_proposition") if isinstance(content.get("value_proposition"), dict) else {}
    selling_argument = content.get("selling_argument") if isinstance(content.get("selling_argument"), dict) else {}
    argument_context_alignment = (
        content.get("argument_context_alignment")
        if isinstance(content.get("argument_context_alignment"), dict) else {}
    )
    selling_argument_available = _text(selling_argument.get("status")).upper() == "AVAILABLE"
    content_mode = _text(
        direction.get("content_bundle_brief", {}).get("content_mode")
    ).upper() or (
        "SELLING_ARGUMENT" if selling_argument_available else "FACTUAL_OBSERVATION"
    )
    selling_argument_mode = content_mode == "SELLING_ARGUMENT" and selling_argument_available
    facts = [
        {
            "claim_key": _text(item.get("claim_key")),
            "fact_text": _text(item.get("fact_text")),
            "supported_shot_nos": list(item.get("supported_shot_nos") or []),
            "argument_relation": _text(item.get("argument_relation")) or "OPTIONAL_PRODUCT_DETAIL",
        }
        for item in selected_atoms
        if _text(item.get("claim_key")) and _text(item.get("fact_text"))
    ]
    if not facts and not selling_argument_mode:
        raise ValueError("批次方向没有可验证且有画面支持的口播事实")
    creative = expression.get("creative_voice_context") if isinstance(expression.get("creative_voice_context"), dict) else {}
    category_extension = (
        direction.get("category_execution_extension")
        if isinstance(direction.get("category_execution_extension"), dict)
        else {}
    )
    category_profile = (
        category_extension.get("profile")
        if isinstance(category_extension.get("profile"), dict)
        else {}
    )
    identity_authority = (
        category_profile.get("identity_authority")
        if isinstance(category_profile.get("identity_authority"), dict)
        else {}
    )
    retrieval_contract = (
        direction.get("retrieval_reference_contract")
        if isinstance(direction.get("retrieval_reference_contract"), dict)
        else {}
    )
    retrieved_speech_hook_pool = [
        {
            "cluster_id": _text(item.get("cluster_id")),
            "prototype_name": _text(item.get("prototype_name")),
            "opening_move": _text(item.get("opening_move")),
            "relation_mode": _text(item.get("relation_mode")),
            "argument_order": _text(item.get("argument_order")),
            "ending_pattern": _text(item.get("ending_pattern")),
            "target_language_examples": [
                {
                    "language": _text(example.get("language")),
                    "rhetorical_example": _text(example.get("rhetorical_example")),
                }
                for example in item.get("target_language_examples") or []
                if isinstance(example, dict)
            ][:2],
        }
        for item in retrieval_contract.get("speech_hook_pool") or []
        if isinstance(item, dict)
    ][:3]
    payload = {
        "schema_version": "original-batch-complete-voiceover-input-v2",
        "hook_execution_policy_version": HOOK_EXECUTION_POLICY_VERSION,
        "candidate_id": hook_id,
        "requested_hook_id": hook_id,
        "product_code": product_code,
        "target_country": target_country,
        "target_language": target_language,
        "top_category": top_category,
        "product_type": product_type,
        "creative_product_profile": creative_product_profile(product_type, top_category),
        "target_duration_seconds": 15,
        "content_mode": content_mode,
        "mainline_policy": (
            "SELLING_ARGUMENT_IS_PRIMARY"
            if selling_argument_mode
            else "VISIBLE_FACT_IS_PRIMARY"
        ),
        "fact_detail_policy": (
            "OPTIONAL_SECONDARY_DETAIL"
            if selling_argument_mode
            else "REQUIRED_FACTUAL_MAINLINE"
        ),
        "spoken_duration_preference_seconds": (
            [11, 15] if content_mode == "SELLING_ARGUMENT" else [7, 11]
        ),
        "content_mainline": _text(value.get("text")) or _text(expression.get("content_mainline")),
        "audience_tension": _text(tension.get("text")),
        "selling_argument": {
            key: selling_argument.get(key)
            for key in (
                "argument_id", "status", "core_value", "target_need",
                "proof_thesis", "decision_thesis", "allowed_strength",
                "verification_status", "evidence_requirement",
                "operator_priority", "proof_match_status",
                "claim_theme", "argument_theme", "concept_ids",
                "primary_demonstration_mode", "demonstration_policy",
                "voiceover_scope_policy",
            )
            if selling_argument.get(key) not in (None, "", [])
        },
        "argument_context_alignment": argument_context_alignment,
        "verified_facts": facts,
        "expression_density_contract": {
            "preferred_information_units": 2 if selling_argument_mode else 1,
            "max_usage_scenarios": 1,
            "max_supporting_facts": 1 if selling_argument_mode else len(facts),
            "second_selling_argument_allowed": False,
            "unlinked_visual_facts_policy": "OMIT",
            "instruction": (
                "用一个核心卖点加一个同主题信息完成自然论证；"
                "同主题信息可以是人工确认的使用情境、直接相关机理/结果、"
                "或一个直接相关可见细节；不得引入第二卖点。"
                if selling_argument_mode else
                "只完成当前可见事实的自然观察，不补字数。"
            ),
        },
        "mainline_scope": {
            "policy": "ONE_CORE_ARGUMENT_WITH_SAME_THEME_SUPPORT",
            "instruction": (
                "整条15秒口播只围绕当前 selling_argument.core_value 展开；"
                "verified_facts 最多作为同一主线的一个可见补充，不得并列引入其他用途、功效或材质主题。"
                + (
                    " 当前卖点已按 primary_demonstration_mode 收束，本条只说这一种主要用法，不枚举其他佩戴方式。"
                    if _text(selling_argument.get("voiceover_scope_policy"))
                    == "PRIMARY_DEMONSTRATION_MODE_ONLY"
                    else ""
                )
                if selling_argument_mode else
                "围绕当前可见事实完成一条自然观察。"
            ),
        },
        "category_identity_authority": {
            "product_subtype": _text(category_profile.get("product_subtype")),
            "pairing_mode": _text(identity_authority.get("pairing_mode")),
            "authority_source": _text(identity_authority.get("authority_source")),
            "must_not_assume": list(identity_authority.get("must_not_assume") or []),
            "instruction": (
                "pairing_mode=UNAVAILABLE 时，目标语言和中文对照都只能使用中性的耳饰/耳侧表达，不得推断单只或成对；"
                "PAIR/SINGLE 仅按该授权值表达。"
                if identity_authority else ""
            ),
        } if identity_authority else {},
        # The central hook archetype, rather than a second local wording table,
        # owns the whole rhetorical path.  These are canonical governed fields
        # loaded by load_active_voiceover_hooks().
        "hook_guidance": {
            key: hook_row.get(key)
            for key in (
                "hook_id", "hook_name", "hook_type", "core_intent",
                "attention_mechanisms", "minimal_structure", "relation_modes",
                "risk_tags", "allowed_visual_focuses", "required_evidence",
                "source", "source_version",
            )
            if hook_row.get(key) not in (None, "", [])
        },
        "retrieved_speech_hook_pool": {
            "status": "AVAILABLE" if retrieved_speech_hook_pool else "UNAVAILABLE",
            "authority": "RHETORIC_ONLY",
            "candidates": retrieved_speech_hook_pool,
            "instruction": (
                "这些真实口播样例只用于学习开口动作、观众关系、论证顺序、句子节奏和目标语言语感；"
                "当前 selling_argument 与 verified_facts 仍是唯一内容权威。不得继承样例中的商品、材质、"
                "功效、品牌、价格、具体宣称、CTA原句或逐句翻译。与当前 hook_guidance 不兼容时可以不用。"
            ),
        },
        "creative_voice_context": creative,
        "narrative_anchor_options": _narrative_anchor_options(creative),
        "approved_style_references": approved_style_references,
        "hook_knowledge": {
            "snapshot_hash": hook_snapshot_hash,
            "sample_status": (
                "AVAILABLE" if approved_style_references else "UNAVAILABLE"
            ),
            "sample_ids": [
                item["reference_sample_id"]
                for item in approved_style_references
            ],
            "selection_policy": style_selection_policy,
            "policy_version": HOOK_EXECUTION_POLICY_VERSION,
        },
        "relationship_language": _relationship_language_profile(
            hook_id, relationship_device, target_language
        ),
        "expression_freedom": {
            "allowed_without_claim_ref": [
                "audience_relationship",
                "light_reaction",
                "personal_selection_criterion",
                "personal_preference",
                "natural_transition",
                "light_personal_close",
            ],
            "boundary": "个人立场不得改写成对所有人的客观商品功效",
        },
        "forbidden_leaps": list(expression.get("forbidden_leaps") or []),
    }
    generated = _invoke_model(model_command, payload)
    model_provenance = (
        generated.get("_model_provenance")
        if isinstance(generated.get("_model_provenance"), dict)
        else {}
    )
    target = _text(generated.get("target_text"))
    translation = _text(generated.get("chinese_translation"))
    used_refs = [
        _text(item) for item in generated.get("used_claim_refs") or [] if _text(item)
    ]
    valid_refs = {_text(item.get("claim_key")) for item in facts}
    if not target or not translation:
        raise ValueError("中央完整口播缺少目标语言正文或中文对照")
    language_error = _target_language_error(target, target_language)
    if language_error:
        raise ValueError(language_error)
    if not set(used_refs).issubset(valid_refs):
        raise ValueError("中央完整口播的used_claim_refs不属于当前事实合同")
    if not used_refs and not selling_argument_mode:
        raise ValueError("事实观察口播至少需要一个可验证事实引用")
    selling_argument_id = _text(selling_argument.get("argument_id"))
    selling_argument_realization = _text(generated.get("selling_argument_realization"))
    selling_argument_realization_zh = _text(
        generated.get("selling_argument_realization_zh")
    )
    if selling_argument_mode:
        if _text(generated.get("used_selling_argument_id")) != selling_argument_id:
            raise ValueError("中央完整口播没有确认已使用当前授权卖点")
        if not selling_argument_realization or selling_argument_realization not in target:
            raise ValueError("中央完整口播没有返回正文中的卖点实际表达")
    if _text(generated.get("hook_id")) != hook_id:
        raise ValueError("中央完整口播没有保持请求的hook_id")

    shot_count = len(
        [item for item in visual_plan.get("shots") or [] if isinstance(item, dict)]
    )
    if not shot_count:
        raise ValueError("视觉方案没有可装配镜头")
    estimated_sec = _estimated_spoken_seconds(target, target_language)
    minimum_ready_sec = 9.5 if content_mode == "SELLING_ARGUMENT" else 6.5
    plan = {
        "voiceover_plan_schema_version": SCHEMA_VERSION,
        "bridge_version": "original-batch-complete-voiceover-v4-rhetoric-recovery",
        "copy_generation_mode": "CREATIVE_FULL_SCRIPT",
        "candidate_id": hook_id,
        "source": "CENTRAL_VOICEOVER_CREATIVE_FULL_SCRIPT",
        "hook_id": hook_id,
        "selected_hook_id": hook_id,
        "target_language": target_language,
        "target_language_key": _target_language_key(target_language),
        "language_validation": "PASSED",
        # Lineage and surface realization are intentionally separate.  The
        # command wrapper pins hook_id for reproducibility, so ID equality is
        # not evidence that the generated rhetoric actually realized the hook.
        "hook_structure_status": "PINNED",
        "hook_lineage_status": "PINNED",
        "hook_surface_status": (
            _hook_surface_status(hook_id, target)
            if _target_language_key(target_language) == "th"
            else "UNKNOWN"
        ),
        "selection_readiness": {
            "status": "READY_FOR_SELECTION" if minimum_ready_sec <= estimated_sec <= 15.0 else "DURATION_WARNING",
            "estimated_sec": estimated_sec,
            "auto_selectable": estimated_sec <= 15.0,
        },
        "selected_claim_ids": used_refs,
        "selected_claim_count": len(used_refs),
        "selected_selling_argument_id": (
            selling_argument_id if selling_argument_mode else ""
        ),
        "selling_argument_realization": selling_argument_realization,
        # This is a compact, respectful Chinese display projection authored
        # with the utterance.  It is optional during the rollout so an older
        # model response cannot block production; Feishu will safely degrade
        # rather than reveal the reviewed operator wording.
        "selling_argument_realization_zh": selling_argument_realization_zh,
        "expression_contract": expression,
        "copy_plan": {
            "schema_version": "creative-full-script-direct-v1",
            "mode": "COMPLETE_UTTERANCE_NO_DOWNSTREAM_REWRITE",
        },
        "lines": [{
            "shot_no": 1,
            "end_shot_no": shot_count,
            "start_ms": 0,
            "end_ms": 15000,
            "spoken_line_task": "complete_creator_thought",
            "voiceover_text_target_language": target,
            "voiceover_text_zh": translation,
            "used_claim_refs": used_refs,
        }],
        "silent_shots": [],
        "silent_windows": [],
        "minimum_silence_window_ms": 0,
        "total_duration_ms": 15000,
        "engine_provenance": {
            "contract_name": "creative_full_single_v1",
            "downstream_rewritten": False,
            "model": model_provenance,
            "hook_knowledge_snapshot_hash": hook_snapshot_hash,
            "hook_execution_policy_version": HOOK_EXECUTION_POLICY_VERSION,
        },
        "hook_knowledge_provenance": {
            "snapshot_hash": hook_snapshot_hash,
            "sample_status": (
                "AVAILABLE" if approved_style_references else "UNAVAILABLE"
            ),
            "sample_ids": [
                item["reference_sample_id"]
                for item in approved_style_references
            ],
            "selection_policy": style_selection_policy,
            "policy_version": HOOK_EXECUTION_POLICY_VERSION,
            "retrieved_speech_hook_run_id": _text(
                (retrieval_contract.get("active_runs") or {}).get("speech_hook")
                if isinstance(retrieval_contract.get("active_runs"), dict)
                else ""
            ),
            "retrieved_speech_hook_clusters": [
                item.get("cluster_id") for item in retrieved_speech_hook_pool
            ],
        },
        "relationship_surface": {
            "requested": _text(relationship_device) or "HOOK_DECIDES",
            "realized": _detect_relationship_device(target),
            "surface_text": _relationship_surface_text(target),
        },
    }
    validation = validate_voiceover_plan(plan, shot_count)
    if not validation["valid"]:
        raise ValueError("中央完整口播装配失败：" + "；".join(validation["issues"]))
    plan["validation"] = validation
    return plan
