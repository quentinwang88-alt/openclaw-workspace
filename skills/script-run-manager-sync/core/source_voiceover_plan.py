"""Build a structured generation route from an original video script.

Original scripts often contain draft per-shot speech.  It records the intended
visual progression, but is not approved voiceover for a later generated video:
the actual frames, duration and creator expression can differ.  This adapter
therefore passes structural direction only and leaves copy generation to the
central engine from verified video facts.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


_SHOT = re.compile(
    r"【镜头(?P<number>\d+)\|(?P<timing>[^|】]+)\|(?P<role>[^】]+)】"
    r"(?P<body>.*?)(?=\n【镜头\d+\||\n【情绪】|\Z)",
    re.S,
)
_SPOKEN = re.compile(r"(?:^|\n)口播\s*[:：]\s*(?P<text>[^\n\r]+)")
_PRODUCT = re.compile(r"【商品】\s*(?P<text>.*?)(?=\n【镜头\d+\||\Z)", re.S)
_EMOTION = re.compile(r"【情绪】\s*(?P<text>.*?)(?=\n【节奏】|\Z)", re.S)
_VISUAL_LINE = re.compile(r"(?:产品锚点|画面)\s*[:：]\s*(?P<text>[^\n\r]+)")


def build_original_voiceover_payload(script_text: str) -> Optional[Dict[str, Dict[str, Any]]]:
    """Return a V2 fresh-copy route for a structured original script.

    Explicit source speech is deliberately never copied to the execution plan.
    Its presence only proves the source has an authored visual progression;
    the central engine will create fresh speech from verified video facts.
    """

    text = str(script_text or "").strip()
    if "【镜头" not in text or "口播" not in text:
        return None
    matches = list(_SHOT.finditer(text))
    if not matches:
        return None

    cursor_ms = 0
    spoken_shot_count = 0
    for match in matches:
        spoken = _SPOKEN.search(match.group("body"))
        if not spoken:
            continue
        source_spoken = spoken.group("text").strip()
        if source_spoken in {"", "无", "无口播", "无旁白"}:
            continue
        _start_ms, end_ms = _timing_ms(match.group("timing"), cursor_ms)
        cursor_ms = max(cursor_ms, end_ms)
        spoken_shot_count += 1
    if not spoken_shot_count:
        return None

    product = _clean(_PRODUCT.search(text).group("text")) if _PRODUCT.search(text) else ""
    emotion = _clean(_EMOTION.search(text).group("text")) if _EMOTION.search(text) else ""
    verified_visual_facts = _extract_verified_visual_facts(text)
    product_category = _extract_product_category(text)
    contract = {
        "schema_version": "voiceover-expression-contract-v2",
        "source_kind": "original_script_visual_expression_plan",
        "content_mainline": product or "围绕当前原创视频的可见商品事实自然展开",
        "product_category": product_category,
        "verified_visual_facts": verified_visual_facts,
        "hook_contract": {
            "hook_id": "ORIGINAL_VISUAL_FIRST",
            "core_intent": "从画面中可验证的具体细节切入，以自然提问、发现反应或对目标人群的轻呼唤建立注意力",
            "minimal_structure": "attention-hook > connected-visual-proof > light-decision",
        },
        "hook_surface_contract": {
            "attention_move": "question_or_discovery",
            "surface_options": ["audience_callout", "discovery_reaction", "direct_question"],
        },
        "hook_surface_contracts": {
            "PARTICIPATION_CHOICE": {
                "attention_move": "detail_preference_question",
                "surface_options": ["which_detail_first", "detail_preference"],
                "instruction": "用两个已验证可见细节提出自然偏好问题，不做产品优劣比较；泰语必须用完整的‘สาวๆ เวลาเลือก...’句式",
                "required_thai_pattern": "สาวๆ เวลาเลือก{category} มอง{detail_a}หรือ{detail_b}ก่อน?",
            },
            "DETAIL_SURPRISE": {
                "attention_move": "specific_detail_discovery",
                "surface_options": ["direct_detail", "discovery_reaction"],
                "instruction": "直接从唯一具体细节切入，不使用泛化注意指令",
            },
        },
        "hook_preconditions": {
            "newness_authorized": False,
            "audience_tension_authorized": False,
            "comparison_authorized": False,
            "detail_choice_authorized": len(verified_visual_facts) >= 2,
            "social_proof_authorized": False,
            "audience_need_authorized": False,
            "visual_result_authorized": False,
        },
        "creative_voice_context": {
            "speaker_identity": "泰国女性时尚短视频创作者",
            "viewer_relationship": emotion or "像朋友分享一个刚发现的穿搭细节",
            "speaking_personality": "自然、有观察感、轻松但不夸张",
        },
        "structure_context": {
            "macro_family_key": "HOOK>PROOF>DECISION",
            "opening_mechanism": "QUESTION_OR_DISCOVERY",
            "source_script_has_spoken_beats": spoken_shot_count,
        },
        "speech_policy": {
            "generic_cta_required": False,
            "allow_non_claim_rhetoric": True,
            "soft_warning_polish_enabled": False,
            "plan_mode": "deterministic_semantic_segments",
            "claim_exactly_once": False,
            "claim_callback_policy": "hook_noun_preview_allowed",
            "char_range_is_hard_limit": False,
            "target_nonspace_char_range": [115, 165],
        },
        "conflict_policy": {"voiceover_priority_on_soft_alignment_conflict": True},
        "forbidden_leaps": [
            "不得复用或改写源脚本的逐镜口播作为成片文案依据",
            "只能表达当前视频可验证的商品事实，不得将可见结构扩写为显瘦、舒适、百搭等收益",
        ],
    }
    plan = {
        "schema_version": "voiceover-execution-plan-v1",
        "mode": "GENERATE_FROM_VERIFIED_VISUAL_FACTS",
        "source_kind": "original_script_visual_expression_plan",
        "expression_contract": contract,
        "generation_policy": "central_engine_must_generate_fresh_copy",
        "source_spoken_beats": spoken_shot_count,
    }
    return {"expression_contract": contract, "execution_plan": plan}


def _extract_verified_visual_facts(script_text: str) -> List[Dict[str, str]]:
    """Map explicit visual directions to the engine's low-risk fact vocabulary."""

    visual_text = " ".join(
        match.group("text") for match in _VISUAL_LINE.finditer(str(script_text or ""))
    )
    facts: List[Dict[str, str]] = []
    button_count = ""
    button_match = re.search(r"前襟[^。；,，]{0,30}?([一二三四五六七八九十\d]+)颗(?:可见)?纽扣", visual_text)
    if button_match:
        button_count = button_match.group(1)
    mappings = (
        (
            "pocket",
            "胸前带有口袋",
            "口袋",
            "core",
            bool(re.search(r"胸前口袋|左右口袋|贴袋", visual_text)),
        ),
        (
            "cropped_length",
            "短款衣长落在腰部附近",
            "短款",
            "normal",
            bool(re.search(r"短款|衣长[^。；,，]{0,20}(?:腰部|腰线)|下摆[^。；,，]{0,20}(?:腰部|腰线)", visual_text)),
        ),
        (
            "closure_detail",
            f"前襟有{button_count}颗可见纽扣" if button_count else "前襟带有可见纽扣",
            "前襟扣位",
            "core",
            bool(re.search(r"前襟[^。；,，]{0,30}(?:纽扣|扣位|按扣)|(?:纽扣|扣位|按扣)[^。；,，]{0,30}前襟", visual_text)),
        ),
        (
            "seam_detail",
            "可见明线与车线细节",
            "分割明线",
            "optional",
            bool(re.search(r"车线|走线|分割明线", visual_text)),
        ),
        ("stand_collar", "带有立领结构", "立领", "core", bool(re.search(r"立领", visual_text))),
    )
    for concept_key, exact_fact_zh, normalizer_text, operator_priority, present in mappings:
        if present:
            facts.append({
                "concept_key": concept_key,
                "exact_fact_zh": exact_fact_zh,
                "normalizer_text": normalizer_text,
                "operator_priority": operator_priority,
            })
    return facts


def _extract_product_category(script_text: str) -> Dict[str, str]:
    text = str(script_text or "")
    if "牛仔" in text and any(token in text for token in ("外套", "夹克")):
        return {"category_zh": "牛仔夹克", "target_language_hint": "แจ็กเก็ตยีนส์"}
    if any(token in text for token in ("外套", "夹克")):
        return {"category_zh": "外套", "target_language_hint": "เสื้อแจ็กเก็ต"}
    if any(token in text for token in ("裤子", "长裤", "短裤")):
        return {"category_zh": "裤子", "target_language_hint": "กางเกง"}
    return {}


def _timing_ms(value: str, cursor_ms: int) -> tuple[int, int]:
    raw = str(value or "").lower().replace("秒", "s").strip()
    numbers = [float(item) for item in re.findall(r"\d+(?:\.\d+)?", raw)]
    if "-" in raw and len(numbers) >= 2:
        start_ms, end_ms = int(numbers[0] * 1000), int(numbers[1] * 1000)
        return start_ms, max(start_ms + 1, end_ms)
    duration_ms = int((numbers[0] if numbers else 2.0) * 1000)
    return cursor_ms, cursor_ms + max(1, duration_ms)


def _clean(value: str) -> str:
    return " ".join(str(value or "").split())[:400]
