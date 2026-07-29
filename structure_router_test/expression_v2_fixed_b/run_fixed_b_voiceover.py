#!/usr/bin/env python3
"""Regenerate only the voiceover for the frozen Stage0 B visual plan."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
SKILL_ROOT = WORKSPACE / "skills" / "original-script-generator"
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.reality_reference import (  # noqa: E402
    validate_voiceover_plan,
    validate_voiceover_visual_grounding,
)
from core.reality_voiceover_bridge import run_central_voiceover  # noqa: E402


DEFAULT_SOURCE = (
    WORKSPACE
    / "structure_router_test"
    / "reality_reference_stage0_v2"
    / "stage0_result.json"
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _load_fixed_direction(source: Path, product_code: str, slot: str) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    payload = json.loads(source.read_text(encoding="utf-8"))
    product = next(
        item
        for item in payload.get("products", [])
        if _text(item.get("product_code")) == product_code
    )
    frozen = next(
        item
        for item in product.get("directions", [])
        if _text(item.get("output_slot")) == slot
    )
    script = frozen.get("script") if isinstance(frozen.get("script"), dict) else {}
    provenance = (
        script.get("reality_reference_provenance")
        if isinstance(script.get("reality_reference_provenance"), dict)
        else {}
    )
    constraints = (
        script.get("execution_constraints")
        if isinstance(script.get("execution_constraints"), dict)
        else {}
    )
    direction = {
        "direction_assignment_id": frozen.get("direction_assignment_id"),
        "content_bundle_brief": frozen.get("content_bundle_brief") or {},
        "p2_lite": frozen.get("p2_lite") or {},
        "structure_execution_plan": script.get("structure_execution_plan") or {},
        "execution_reference": {
            "execution_card_id": frozen.get("execution_card_id"),
            "source_profile_id": frozen.get("source_profile_id"),
            "source_video_id": frozen.get("source_video_id"),
            "visual_hook_type": (script.get("structure_execution_plan") or {}).get("opening_mechanism"),
            "proof_mechanisms": ["DETAIL_MACRO", "ANGLE_REVEAL"],
            "unknown_fields": constraints.get("reference_unknown_fields") or [],
            "execution_card_schema_version": provenance.get("execution_card_schema_version"),
        },
    }
    return product, frozen, direction


def _report(result: Dict[str, Any]) -> str:
    baseline = result["baseline_voiceover"]
    current = result["voiceover_v2"]
    lines = [
        "# 固定 B 画面｜中央口播表达合同 V2 文本复测",
        "",
        f"- 产品：`{result['product_code']}`",
        f"- 固定槽位：`{result['slot']}`",
        f"- 固定 execution_card：`{result['execution_card_id']}`",
        f"- 钩子：`{current.get('hook_id')}`",
        f"- 安全映射卖点数：`{current.get('selected_claim_count')}`",
        f"- 表达合同：`{(current.get('expression_contract') or {}).get('schema_version')}`",
        f"- 本地样本画像：`{((current.get('expression_contract') or {}).get('locale_expression_profile') or {}).get('status')}`",
        "",
        "## 旧 B",
        "",
    ]
    for item in baseline.get("lines") or []:
        lines.append(
            f"- 镜头{item.get('shot_no')}：{_text(item.get('voiceover_text_target_language'))}"
            f"（{_text(item.get('voiceover_text_zh'))}）"
        )
    lines.extend(["", "## V2", ""])
    for item in current.get("lines") or []:
        lines.append(
            f"- 镜头{item.get('shot_no')}｜{_text(item.get('spoken_line_task'))}："
            f"{_text(item.get('voiceover_text_target_language'))}"
            f"（{_text(item.get('voiceover_text_zh'))}）"
        )
    plan = current.get("copy_plan") or {}
    lines.extend(["", "## 表达计划", ""])
    for index, beat in enumerate(plan.get("beats") or [], 1):
        lines.append(
            f"- B{index}｜{beat.get('speech_act')}｜{beat.get('claim_role')}｜"
            f"{beat.get('speech_intent_zh')}"
        )
    validation = result.get("validation") or {}
    lines.extend(
        [
            "",
            "## 自动校验",
            "",
            f"- 结构与静默镜头：`{validation.get('voiceover_plan', {}).get('valid')}`",
            f"- 首句视觉落地：`{validation.get('visual_grounding', {}).get('valid')}`",
            f"- 问题：`{validation.get('issues') or []}`",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--product-code", default="1734482585843304442")
    parser.add_argument("--slot", default="S1")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--model-command", required=True)
    parser.add_argument("--qc-model-command", default="")
    args = parser.parse_args()

    source = Path(args.source).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    product, frozen, direction = _load_fixed_direction(
        source, args.product_code, args.slot
    )
    visual_plan = frozen.get("visual_plan") or {}
    voiceover = run_central_voiceover(
        product_code=args.product_code,
        target_country=_text(product.get("target_country")) or "TH",
        target_language=_text(product.get("target_language")) or "泰语",
        direction=direction,
        visual_plan=visual_plan,
        voiceover_root="/Users/likeu3/voiceover_copy_engine",
        voiceover_db_path=str(output_dir / "voiceover_expression_v2.sqlite3"),
        model_command=args.model_command,
        qc_model_command=args.qc_model_command or args.model_command,
    )
    plan_validation = validate_voiceover_plan(
        voiceover, len(visual_plan.get("shots") or [])
    )
    first_shot = (visual_plan.get("shots") or [{}])[0]
    grounding = validate_voiceover_visual_grounding(
        voiceover,
        primary_observation=_text((direction.get("p2_lite") or {}).get("primary_observation")),
        first_shot_content=_text(first_shot.get("shot_content")),
    )
    result = {
        "schema_version": "fixed-b-expression-v2-result-v1",
        "product_code": args.product_code,
        "slot": args.slot,
        "source_result": str(source),
        "execution_card_id": frozen.get("execution_card_id"),
        "visual_plan": visual_plan,
        "content_bundle_brief": direction.get("content_bundle_brief"),
        "baseline_voiceover": frozen.get("voiceover_plan") or {},
        "voiceover_v2": voiceover,
        "validation": {
            "voiceover_plan": plan_validation,
            "visual_grounding": grounding,
            "issues": [
                *(plan_validation.get("issues") or []),
                *(grounding.get("issues") or []),
            ],
        },
    }
    (output_dir / "fixed_b_expression_v2_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "fixed_b_expression_v2_review.md").write_text(
        _report(result), encoding="utf-8"
    )
    print(json.dumps({
        "result": str(output_dir / "fixed_b_expression_v2_result.json"),
        "review": str(output_dir / "fixed_b_expression_v2_review.md"),
        "hook_id": voiceover.get("hook_id"),
        "selected_claim_count": voiceover.get("selected_claim_count"),
        "valid": plan_validation.get("valid") and grounding.get("valid"),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
