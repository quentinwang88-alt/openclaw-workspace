"""Thin complete-utterance bridge for batch original scripts.

The central voiceover engine still owns hook vocabulary and factual authority.
This bridge deliberately skips the old line/beat planner: one model call writes
one complete utterance and code only mounts it across the whole visual plan.
"""
from __future__ import annotations

import json
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from core.complete_script_v3 import creative_product_profile
from core.reality_reference import validate_voiceover_plan
from core.reality_voiceover_bridge import (
    HOOK_SURFACE_CONTRACTS,
    VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH,
    build_voiceover_expression_contract,
    load_active_voiceover_hooks,
    resolve_voiceover_hook_policy,
    select_voiceover_claim_atoms,
)


SCHEMA_VERSION = "creative-full-script-voiceover-v2-content-first"


def _text(value: Any) -> str:
    return str(value or "").strip()


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
    preferred_count = (
        2 if _text(bundle.get("content_mode")).upper() == "SELLING_ARGUMENT" else 1
    )
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


def _approved_style_references(hook_id: str, limit: int = 2) -> List[Dict[str, Any]]:
    """Read the governed snapshot directly; examples guide rhetoric, never facts."""

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
    fallback_ids = [
        example_id for example_id in examples if example_id not in matched_ids
    ]
    for example_id in dict.fromkeys([*matched_ids, *fallback_ids]):
        example = examples.get(example_id)
        if not example:
            continue
        excerpt = re.sub(r"\s+", " ", _text(example.get("raw_text")))[:680]
        if not excerpt:
            continue
        result.append({
            "reference_sample_id": example_id,
            "reference_excerpt": excerpt,
            "usage_boundary": "只学习观众关系、节奏、衔接和信息密度；不得继承事实或原句",
        })
        if len(result) >= limit:
            break
    return result


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
) -> Dict[str, Any]:
    requested = _text(requested_device).upper()
    default_device = (
        "VIEWER_REFERENCE"
        if hook_id in {"AUDIENCE_NEED_CALLOUT", "PAIN_REFRAME", "USER_ADVOCACY_STANCE"}
        else "REACTION_OR_VIEWER_INVITATION"
    )
    return {
        "assigned_device": requested or "HOOK_DECIDES",
        "preferred_device": default_device,
        # Keep the existing friendly Thai register, but do not make ทุกคน or
        # พวกเธอ the default surface.  Those expressions read as translated
        # “大家/你们” in this creator voice.
        "audience_addresses": ["สาวๆ"],
        "viewer_reference_forms": ["ใครอยาก", "ใครที่กำลัง", "คนไหนชอบ"],
        "reaction_openers": ["เอาจริงนะ", "เพิ่งสังเกตว่า", "ดูนี่ก่อน"],
        "natural_particles": ["นะ", "ค่ะ", "แหละ", "เลย"],
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
    expression, selected_atoms = _expression_with_selected_claims(
        direction, visual_plan
    )
    argument = expression.get("argument_contract") if isinstance(expression.get("argument_contract"), dict) else {}
    content = argument.get("content") if isinstance(argument.get("content"), dict) else {}
    tension = content.get("audience_tension") if isinstance(content.get("audience_tension"), dict) else {}
    value = content.get("value_proposition") if isinstance(content.get("value_proposition"), dict) else {}
    selling_argument = content.get("selling_argument") if isinstance(content.get("selling_argument"), dict) else {}
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
    payload = {
        "schema_version": "original-batch-complete-voiceover-input-v2",
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
            )
            if selling_argument.get(key) not in (None, "", [])
        },
        "verified_facts": facts,
        "hook_guidance": {
            **dict(HOOK_SURFACE_CONTRACTS.get(hook_id) or {}),
            **{
                key: hook_row.get(key)
                for key in (
                    "hook_id", "hook_name", "core_intent", "attention_mechanism",
                    "speech_act", "surface_options", "avoid", "notes",
                )
                if hook_row.get(key) not in (None, "", [])
            },
        },
        "creative_voice_context": creative,
        "narrative_anchor_options": _narrative_anchor_options(creative),
        "approved_style_references": _approved_style_references(hook_id),
        "relationship_language": _relationship_language_profile(
            hook_id, relationship_device
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
        raise ValueError("中央完整口播缺少泰语正文或中文对照")
    if not set(used_refs).issubset(valid_refs):
        raise ValueError("中央完整口播的used_claim_refs不属于当前事实合同")
    if not used_refs and not selling_argument_mode:
        raise ValueError("事实观察口播至少需要一个可验证事实引用")
    selling_argument_id = _text(selling_argument.get("argument_id"))
    selling_argument_realization = _text(generated.get("selling_argument_realization"))
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
    estimated_sec = round(len(re.sub(r"\s+", "", target)) / 13.0, 2)
    minimum_ready_sec = 9.5 if content_mode == "SELLING_ARGUMENT" else 6.5
    plan = {
        "voiceover_plan_schema_version": SCHEMA_VERSION,
        "bridge_version": "original-batch-complete-voiceover-v2-content-first",
        "copy_generation_mode": "CREATIVE_FULL_SCRIPT",
        "candidate_id": hook_id,
        "source": "CENTRAL_VOICEOVER_CREATIVE_FULL_SCRIPT",
        "hook_id": hook_id,
        "selected_hook_id": hook_id,
        "hook_structure_status": "MATCHED",
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
