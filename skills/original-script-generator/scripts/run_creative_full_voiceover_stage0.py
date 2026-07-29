#!/usr/bin/env python3
"""Isolated A/B test for complete voiceover creation.

Reads only governed facts and creative context from an existing stage-0 result.
It never sends old candidate wording to the model, writes Feishu, or generates video.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.complete_script_v3 import creative_product_profile, validate_complete_script
from core.reality_reference import (
    assemble_reality_script,
    validate_voiceover_plan,
    validate_voiceover_visual_grounding,
)
from scripts.run_reality_reference_stage0 import _render_storyboard


EXPECTED_IDS = [
    "FULL_A_NEED",
    "FULL_B_LIVED_MOMENT",
    "FULL_C_DETAIL_DISCOVERY",
]

CREATIVE_FULL_VOICEOVER_SCHEMA = "creative-full-script-voiceover-v1"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_direction(product: Dict[str, Any]) -> Dict[str, Any]:
    directions = [item for item in product.get("directions") or [] if isinstance(item, dict)]
    if not directions:
        raise ValueError(f"产品 {_text(product.get('product_code'))} 没有阶段0方向")
    return directions[0]


def _expression_contract(direction: Dict[str, Any]) -> Dict[str, Any]:
    for candidate in direction.get("voiceover_candidates") or []:
        if not isinstance(candidate, dict):
            continue
        contract = candidate.get("expression_contract")
        if isinstance(contract, dict) and contract:
            return contract
    contract = direction.get("voiceover_argument_contract")
    if isinstance(contract, dict) and contract.get("schema_version") == "voiceover-expression-contract-v2":
        return contract
    raise ValueError("阶段0结果缺少可复用的中央口播表达合同")


def _verified_facts(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    argument = contract.get("argument_contract") if isinstance(contract.get("argument_contract"), dict) else {}
    content = argument.get("content") if isinstance(argument.get("content"), dict) else {}
    raw = [item for item in content.get("proof_atoms") or [] if isinstance(item, dict)]
    facts: List[Dict[str, Any]] = []
    seen_text = set()
    for item in raw:
        fact_text = _text(item.get("fact_text"))
        claim_key = _text(item.get("claim_key"))
        normalized = re.sub(r"\s+", "", fact_text)
        if not fact_text or not claim_key or normalized in seen_text:
            continue
        seen_text.add(normalized)
        facts.append(
            {
                "claim_key": claim_key,
                "fact_text": fact_text,
                "supported_shot_nos": list(item.get("supported_shot_nos") or []),
            }
        )
    if facts:
        return facts[:3]
    for item in contract.get("claim_roles") or []:
        if not isinstance(item, dict):
            continue
        fact_text = _text(item.get("fact_zh"))
        claim_key = _text(item.get("claim_id"))
        normalized = re.sub(r"\s+", "", fact_text)
        if not fact_text or not claim_key or normalized in seen_text:
            continue
        seen_text.add(normalized)
        facts.append({"claim_key": claim_key, "fact_text": fact_text})
    return facts[:3]


def build_payload(product: Dict[str, Any]) -> Dict[str, Any]:
    direction = _first_direction(product)
    contract = _expression_contract(direction)
    diversity = (
        direction.get("creative_diversity_contract")
        if isinstance(direction.get("creative_diversity_contract"), dict)
        else {}
    )
    product_type = _text(product.get("product_type")) or _text(diversity.get("product_type"))
    top_category = _text(product.get("top_category")) or _text(diversity.get("category"))
    product_profile = _text(diversity.get("creative_product_profile")) or creative_product_profile(
        product_type, top_category
    )
    product_category = (
        contract.get("product_category")
        if isinstance(contract.get("product_category"), dict)
        else {}
    )
    display_family = _text(product.get("display_family")) or _text(
        product_category.get("display_family")
    )
    if not display_family:
        if any(token in product_type.lower() for token in ("围巾", "丝巾", "披肩", "帽", "scarf", "hat")):
            display_family = "apparel_accessory"
        elif product_profile in {"WORN_ACCESSORY", "HAND_STATIC_ACCESSORY"}:
            display_family = "accessory"
        elif product_profile == "WORN_APPAREL":
            display_family = "apparel"
    argument = contract.get("argument_contract") if isinstance(contract.get("argument_contract"), dict) else {}
    content = argument.get("content") if isinstance(argument.get("content"), dict) else {}
    tension = content.get("audience_tension") if isinstance(content.get("audience_tension"), dict) else {}
    value = content.get("value_proposition") if isinstance(content.get("value_proposition"), dict) else {}
    facts = _verified_facts(contract)
    if not facts:
        raise ValueError(f"产品 {_text(product.get('product_code'))} 没有可验证口播事实")
    return {
        "experiment_version": "creative-full-script-stage0-v1",
        "product_code": _text(product.get("product_code")),
        "target_country": _text(product.get("target_country")),
        "target_language": _text(product.get("target_language")),
        "top_category": top_category,
        "product_type": product_type,
        "creative_product_profile": product_profile,
        "display_family": display_family,
        "target_duration_seconds": 15,
        "content_mainline": _text(value.get("text")) or _text(contract.get("content_mainline")),
        "audience_tension": _text(tension.get("text")),
        "verified_facts": facts,
        "creative_voice_context": (
            contract.get("creative_voice_context")
            if isinstance(contract.get("creative_voice_context"), dict)
            else {}
        ),
        "narrative_anchor_options": [
            item for item in contract.get("narrative_anchor_options") or [] if isinstance(item, dict)
        ][:3],
        "approved_style_references": [
            item for item in contract.get("approved_style_references") or [] if isinstance(item, dict)
        ][:2],
        "relationship_language": (
            contract.get("relationship_language_profile")
            if isinstance(contract.get("relationship_language_profile"), dict)
            else {}
        ),
        "forbidden_leaps": list(contract.get("forbidden_leaps") or []),
    }


def invoke_model(model_command: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    command = shlex.split(model_command)
    if not command:
        raise ValueError("model command 不能为空")
    completed = subprocess.run(
        command,
        input=json.dumps(
            {"contract_name": "creative_full_script_v1", "payload": payload},
            ensure_ascii=False,
        ),
        text=True,
        capture_output=True,
        timeout=360,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "")[:1200]
        raise RuntimeError(f"完整口播模型失败(code={completed.returncode}): {detail}")
    try:
        parsed = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"完整口播模型返回非JSON: {completed.stdout[:1200]}") from exc
    if parsed.get("error"):
        raise RuntimeError(_text(parsed.get("error")))
    return parsed


def validate_result(payload: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    valid_refs = {_text(item.get("claim_key")) for item in payload.get("verified_facts") or []}
    candidates = [item for item in result.get("candidates") or [] if isinstance(item, dict)]
    issues: List[str] = []
    if [item.get("candidate_id") for item in candidates] != EXPECTED_IDS:
        issues.append("候选ID或顺序不符合完整直创合同")
    for item in candidates:
        candidate_id = _text(item.get("candidate_id"))
        target = _text(item.get("target_text"))
        translation = _text(item.get("chinese_translation"))
        refs = {_text(ref) for ref in item.get("used_claim_refs") or [] if _text(ref)}
        if not target or not translation:
            issues.append(f"{candidate_id}: 缺少泰语口播或中文翻译")
        if not refs:
            issues.append(f"{candidate_id}: 没有声明使用的已验证事实")
        if not refs.issubset(valid_refs):
            issues.append(f"{candidate_id}: 使用未知claim_ref")
        char_count = len(re.sub(r"\s+", "", target))
        estimated_sec = round(char_count / 13.0, 2)
        item["nonspace_thai_char_count"] = char_count
        item["estimated_delivery_seconds"] = estimated_sec
        item["selection_readiness"] = (
            "READY_FOR_BLIND_REVIEW" if 9.5 <= estimated_sec <= 15.0 else "DURATION_WARNING"
        )
    return {"valid": not issues, "issues": issues}


def build_selected_voiceover_plan(
    source_direction: Dict[str, Any],
    candidate: Dict[str, Any],
) -> Dict[str, Any]:
    """Adapt one approved complete utterance without rewriting its words."""

    visual_plan = (
        source_direction.get("visual_plan")
        if isinstance(source_direction.get("visual_plan"), dict)
        else {}
    )
    shot_count = len(
        [item for item in visual_plan.get("shots") or [] if isinstance(item, dict)]
    )
    if not shot_count:
        raise ValueError("源方向缺少可装配的visual_plan.shots")
    target_text = _text(candidate.get("target_text"))
    chinese_translation = _text(candidate.get("chinese_translation"))
    if not target_text or not chinese_translation:
        raise ValueError("已选完整口播缺少目标语言正文或中文对照")
    expression_contract = _expression_contract(source_direction)
    used_refs = [
        _text(item) for item in candidate.get("used_claim_refs") or [] if _text(item)
    ]
    valid_refs = {
        _text(item.get("claim_key"))
        for item in _verified_facts(expression_contract)
        if _text(item.get("claim_key"))
    }
    if not used_refs or not set(used_refs).issubset(valid_refs):
        raise ValueError("已选完整口播的used_claim_refs不属于当前表达合同")
    estimated_seconds = float(candidate.get("estimated_delivery_seconds") or 0)
    plan = {
        "voiceover_plan_schema_version": CREATIVE_FULL_VOICEOVER_SCHEMA,
        "bridge_version": "creative-full-script-downstream-v1",
        "copy_generation_mode": "CREATIVE_FULL_SCRIPT",
        "candidate_id": _text(candidate.get("candidate_id")),
        "source": "CENTRAL_VOICEOVER_CREATIVE_FULL_SCRIPT",
        "selection_readiness": {
            "status": _text(candidate.get("selection_readiness")),
            "estimated_sec": estimated_seconds,
            "auto_selectable": 9.5 <= estimated_seconds <= 15.0,
        },
        "selected_claim_ids": used_refs,
        "selected_claim_count": len(used_refs),
        "expression_contract": expression_contract,
        "copy_plan": {
            "schema_version": "creative-full-script-direct-v1",
            "mode": "COMPLETE_UTTERANCE_NO_DOWNSTREAM_REWRITE",
        },
        "lines": [
            {
                "shot_no": 1,
                "end_shot_no": shot_count,
                "start_ms": 0,
                "end_ms": 15000,
                "spoken_line_task": "complete_lived_moment",
                "voiceover_text_target_language": target_text,
                "voiceover_text_zh": chinese_translation,
                "used_claim_refs": used_refs,
            }
        ],
        "silent_shots": [],
        "silent_windows": [],
        "minimum_silence_window_ms": 0,
        "total_duration_ms": 15000,
        "engine_provenance": {
            "contract_name": "creative_full_script_v1",
            "candidate_id": _text(candidate.get("candidate_id")),
            "downstream_rewritten": False,
        },
    }
    validation = validate_voiceover_plan(plan, shot_count)
    if not validation["valid"]:
        raise ValueError("完整直创口播适配失败：" + "；".join(validation["issues"]))
    plan["validation"] = validation
    return plan


def _assembly_direction(source_direction: Dict[str, Any]) -> Dict[str, Any]:
    """Rehydrate only the two internal contracts omitted from stage-0 JSON."""

    visual_plan = (
        source_direction.get("visual_plan")
        if isinstance(source_direction.get("visual_plan"), dict)
        else {}
    )
    visual_shots = [
        item for item in visual_plan.get("shots") or [] if isinstance(item, dict)
    ]
    diversity = (
        source_direction.get("creative_diversity_contract")
        if isinstance(source_direction.get("creative_diversity_contract"), dict)
        else {}
    )
    hydrated = json.loads(json.dumps(source_direction, ensure_ascii=False))
    hydrated["structure_execution_plan"] = {
        "macro_family_key": _text(diversity.get("structure_family")),
        "shot_plan": [
            {
                "time_range": _text(item.get("duration")),
                "structure_beat": _text(item.get("structure_beat")),
                "carrier_mode": _text(item.get("carrier_mode")),
                "continuity_group": _text(item.get("continuity_group")),
                "opening_mechanism": _text(item.get("opening_mechanism")),
                "spoken_task_hint": (
                    "complete_voiceover" if index == 1 else "voiceover_continuation"
                ),
                "visual_task": _text(item.get("editorial_purpose"))
                or _text(item.get("shot_content")),
            }
            for index, item in enumerate(visual_shots, 1)
        ],
    }
    hydrated["execution_reference"] = {
        "execution_card_id": _text(source_direction.get("execution_card_id"))
        or _text(visual_plan.get("execution_card_id")),
        "execution_card_schema_version": "stage0-snapshot-rehydrated-v1",
        "source_profile_id": _text(source_direction.get("source_profile_id")),
        "unknown_fields": list(visual_plan.get("unknowns_preserved") or []),
    }
    return hydrated


def assemble_selected_candidate(
    source_product: Dict[str, Any],
    full_product: Dict[str, Any],
    candidate_id: str,
) -> Dict[str, Any]:
    source_direction = _first_direction(source_product)
    candidate = next(
        (
            item
            for item in full_product.get("candidates") or []
            if isinstance(item, dict) and _text(item.get("candidate_id")) == candidate_id
        ),
        None,
    )
    if candidate is None:
        raise ValueError(
            f"产品 {_text(source_product.get('product_code'))} 不存在候选 {candidate_id}"
        )
    direction = _assembly_direction(source_direction)
    voiceover_plan = build_selected_voiceover_plan(source_direction, candidate)
    visual_plan = direction["visual_plan"]
    first_shot = visual_plan.get("shots", [{}])[0]
    grounding = validate_voiceover_visual_grounding(
        voiceover_plan,
        primary_observation=_text(direction.get("p2_lite", {}).get("primary_observation")),
        first_shot_content=" ".join(
            filter(
                None,
                [
                    _text(first_shot.get("shot_content")),
                    _text(first_shot.get("observable_action")),
                ],
            )
        ),
    )
    # Complete-script mode is allowed to establish its scene before naming a
    # product fact.  Grounding therefore stays visible but non-blocking.
    voiceover_plan["validation"]["visual_grounding"] = grounding
    script = assemble_reality_script(
        direction=direction,
        visual_plan=visual_plan,
        voiceover_plan=voiceover_plan,
    )
    quality = validate_complete_script(script)
    script["quality_result"] = quality
    if script.get("continuous_voiceover", {}).get("target_language") != _text(
        candidate.get("target_text")
    ):
        raise ValueError("完整口播在下游装配中发生了改写")
    if script.get("continuous_voiceover", {}).get("chinese_translation") != _text(
        candidate.get("chinese_translation")
    ):
        raise ValueError("中文口播对照在下游装配中发生了改写")
    return {
        "product_code": _text(source_product.get("product_code")),
        "output_slot": _text(source_direction.get("output_slot")),
        "candidate_id": candidate_id,
        "copy_generation_mode": "CREATIVE_FULL_SCRIPT",
        "downstream_rewritten": False,
        "voiceover_plan": voiceover_plan,
        "script": script,
        "quality_result": quality,
        "final_video_prompt": _render_storyboard(script),
    }


def _write_downstream_outputs(
    output_dir: Path,
    source_products: Dict[str, Dict[str, Any]],
    full_products: List[Dict[str, Any]],
    candidate_id: str,
) -> Dict[str, Any]:
    assembled = [
        assemble_selected_candidate(
            source_products[_text(item.get("product_code"))], item, candidate_id
        )
        for item in full_products
    ]
    result = {
        "schema_version": "creative-full-script-downstream-stage0-v1",
        "candidate_id": candidate_id,
        "production_write": False,
        "video_generation": False,
        "products": assembled,
    }
    (output_dir / "selected_downstream_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    markdown = ["# 已选完整直创脚本", ""]
    for item in assembled:
        markdown.extend(
            [
                f"## 产品 {item['product_code']}｜{item['output_slot']}",
                "",
                item["final_video_prompt"],
                "",
            ]
        )
        (output_dir / f"{item['product_code']}_{item['output_slot']}_complete_script.md").write_text(
            item["final_video_prompt"] + "\n", encoding="utf-8"
        )
    (output_dir / "selected_complete_scripts.md").write_text(
        "\n".join(markdown), encoding="utf-8"
    )
    return result


def _old_candidates(product: Dict[str, Any]) -> List[Dict[str, str]]:
    direction = _first_direction(product)
    rows = []
    for candidate in direction.get("voiceover_candidates") or []:
        if not isinstance(candidate, dict):
            continue
        lines = [item for item in candidate.get("lines") or [] if isinstance(item, dict)]
        rows.append(
            {
                "candidate_id": _text(candidate.get("candidate_id")),
                "target_text": " ".join(
                    _text(item.get("voiceover_text_target_language")) for item in lines
                    if _text(item.get("voiceover_text_target_language"))
                ),
                "chinese_translation": " ".join(
                    _text(item.get("voiceover_text_zh")) for item in lines
                    if _text(item.get("voiceover_text_zh"))
                ),
            }
        )
    return rows[:3]


def _render_candidates(products: List[Dict[str, Any]]) -> str:
    lines = ["# Creative Full Script 阶段0候选", ""]
    for product in products:
        lines.extend([f"## 产品 {product['product_code']}", ""])
        for candidate in product.get("candidates") or []:
            lines.extend(
                [
                    f"### {candidate.get('candidate_id')}｜{candidate.get('entry_angle', '')}",
                    "",
                    f"- 估时：{candidate.get('estimated_delivery_seconds')}s｜{candidate.get('selection_readiness')}",
                    f"- 关系表达：{' / '.join(candidate.get('relationship_devices') or [])}",
                    f"- 泰语：{candidate.get('target_text', '')}",
                    f"- 中文：{candidate.get('chinese_translation', '')}",
                    f"- 人味依据：{candidate.get('why_it_feels_human', '')}",
                    "",
                ]
            )
    return "\n".join(lines)


def _build_blind_review(
    source_products: Dict[str, Dict[str, Any]], full_products: List[Dict[str, Any]]
) -> tuple[str, Dict[str, Any]]:
    lines = [
        "# 完整直创 vs 旧规划口播盲审",
        "",
        "每组只判断：开头吸引力、真人感、信息推进、中段是否像卖点清单、收尾是否自然。强制二选一。",
    ]
    answer_key: Dict[str, Any] = {}
    pair_no = 0
    for full_product in full_products:
        product_code = _text(full_product.get("product_code"))
        old = _old_candidates(source_products[product_code])
        new = full_product.get("candidates") or []
        for index, (old_item, new_item) in enumerate(zip(old, new), 1):
            pair_no += 1
            new_first = int(
                hashlib.sha256(f"{product_code}:{index}:full-ab-v1".encode()).hexdigest()[:2], 16
            ) % 2 == 0
            item_a = new_item if new_first else old_item
            item_b = old_item if new_first else new_item
            answer_key[f"pair_{pair_no}"] = {
                "product_code": product_code,
                "angle_index": index,
                "A": "creative_full_script" if new_first else "copy_plan_v2",
                "B": "copy_plan_v2" if new_first else "creative_full_script",
            }
            lines.extend(
                [
                    "",
                    f"## 对比组 {pair_no}｜产品 {product_code}",
                    "",
                    "### A",
                    "",
                    f"泰语：{item_a.get('target_text', '')}",
                    f"中文：{item_a.get('chinese_translation', '')}",
                    "",
                    "### B",
                    "",
                    f"泰语：{item_b.get('target_text', '')}",
                    f"中文：{item_b.get('chinese_translation', '')}",
                    "",
                    "选择：A / B；理由：________________",
                ]
            )
    return "\n".join(lines) + "\n", answer_key


def main() -> int:
    parser = argparse.ArgumentParser(description="完整口播直创阶段0 A/B（不写飞书、不生成视频）")
    parser.add_argument("--source-result", required=True)
    parser.add_argument("--product-code", action="append", dest="product_codes")
    parser.add_argument(
        "--model-command",
        default="python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py",
    )
    parser.add_argument(
        "--candidate-result",
        default="",
        help="复用已生成的creative_full_script_result.json，不再次调用模型",
    )
    parser.add_argument(
        "--selected-candidate-id",
        default="",
        help="选中后继续装配完整脚本与最终视频提示词；不会生成视频或写飞书",
    )
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    source_path = Path(args.source_result).expanduser().resolve()
    source = json.loads(source_path.read_text(encoding="utf-8"))
    source_products = {
        _text(item.get("product_code")): item
        for item in source.get("products") or []
        if isinstance(item, dict) and _text(item.get("product_code"))
    }
    selected_codes: Iterable[str] = args.product_codes or source_products.keys()
    output_products: List[Dict[str, Any]] = []
    if args.candidate_result:
        candidate_result_path = Path(args.candidate_result).expanduser().resolve()
        loaded_result = json.loads(candidate_result_path.read_text(encoding="utf-8"))
        loaded_products = {
            _text(item.get("product_code")): item
            for item in loaded_result.get("products") or []
            if isinstance(item, dict) and _text(item.get("product_code"))
        }
        for product_code in selected_codes:
            if product_code not in source_products:
                raise ValueError(f"source result中不存在产品 {product_code}")
            if product_code not in loaded_products:
                raise ValueError(f"candidate result中不存在产品 {product_code}")
            output_products.append(loaded_products[product_code])
        print(f"♻️ 复用完整直创候选：{candidate_result_path}")
    else:
        for product_code in selected_codes:
            if product_code not in source_products:
                raise ValueError(f"source result中不存在产品 {product_code}")
            print(f"🗣️ 完整直创产品: {product_code}")
            payload = build_payload(source_products[product_code])
            generated = invoke_model(args.model_command, payload)
            validation = validate_result(payload, generated)
            output_products.append(
                {
                    "product_code": product_code,
                    "status": "READY_FOR_BLIND_REVIEW" if validation["valid"] else "INVALID",
                    "validation": validation,
                    "input_snapshot": payload,
                    "candidates": generated.get("candidates") or [],
                }
            )

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "schema_version": "creative-full-script-stage0-result-v1",
        "source_result": str(source_path),
        "production_write": False,
        "video_generation": False,
        "products": output_products,
    }
    (output_dir / "creative_full_script_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "creative_full_script_candidates.md").write_text(
        _render_candidates(output_products), encoding="utf-8"
    )
    blind, answer_key = _build_blind_review(source_products, output_products)
    (output_dir / "blind_review.md").write_text(blind, encoding="utf-8")
    (output_dir / "blind_answer_key.json").write_text(
        json.dumps(answer_key, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if args.selected_candidate_id:
        downstream = _write_downstream_outputs(
            output_dir,
            source_products,
            output_products,
            _text(args.selected_candidate_id),
        )
        print(
            "完整脚本："
            f"{output_dir / 'selected_complete_scripts.md'} "
            f"({len(downstream['products'])}条)"
        )
    print(f"结果：{output_dir / 'creative_full_script_result.json'}")
    return 0 if all(item["validation"]["valid"] for item in output_products) else 2


if __name__ == "__main__":
    raise SystemExit(main())
