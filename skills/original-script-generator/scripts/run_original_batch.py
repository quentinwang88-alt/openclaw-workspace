#!/usr/bin/env python3
"""V1 原创批次轻编排器 CLI — plan-only / script-only / show-plan"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="原创批次轻编排器 V1")
    parser.add_argument("--product-code", required=True)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--test-phase", default="INITIAL")
    parser.add_argument("--mode", default="plan-only", choices=["plan-only", "script-only"])
    parser.add_argument("--duration", type=float, default=15)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--batch-id", help="执行已冻结计划时使用")
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--show-plan", action="store_true", help="仅查看计划")
    parser.add_argument("--resume", action="store_true", help="断点续跑")
    parser.add_argument("--limit", type=int, default=0, help="script-only 模式最多执行N条")
    parser.add_argument("--delay-between-items", type=int, default=0, help="每两条之间等待秒数")
    parser.add_argument("--replan", action="store_true", help="重新规划（覆盖旧批次）")
    parser.add_argument("--voiceover-root", default="/Users/likeu3/voiceover_copy_engine")
    parser.add_argument(
        "--voiceover-model-command",
        default="python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py",
    )
    parser.add_argument("--voiceover-qc-model-command", default="")
    parser.add_argument("--blueprint-model", default="gpt-5.6-sol")
    parser.add_argument("--blueprint-reasoning", default="high")
    parser.add_argument(
        "--script-mode",
        default="legacy_v2",
        choices=["legacy_v2", "simplified_v1"],
        help="脚本执行链路；simplified_v1 为一次完整视觉脚本+中央口播",
    )
    parser.add_argument("--target-country", default="")
    parser.add_argument("--target-language", default="")
    args = parser.parse_args()

    from core.original_batch_models import BatchRequest, generate_request_id
    from core.original_batch_storage import BatchStorage
    from core.original_batch_executor import run_plan_only, run_script_only

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    storage = BatchStorage()
    storage.ensure_schema()

    if args.show_plan and args.batch_id:
        batch = storage.get_batch(args.batch_id)
        if not batch:
            print(f"批次不存在: {args.batch_id}", file=sys.stderr)
            return 1
        items = storage.get_items(args.batch_id)
        _print_batch(batch, items)
        return 0

    if args.mode == "plan-only":
        request = BatchRequest(
            request_id=generate_request_id(
                args.product_code, args.seed, args.test_phase, args.script_mode
            ),
            product_code=args.product_code,
            requested_count=args.count,
            test_phase=args.test_phase,
            duration_seconds=args.duration,
            execution_mode="PLAN_ONLY",
            random_seed=args.seed,
            target_country=args.target_country,
            target_language=args.target_language,
            script_mode=args.script_mode,
        )
        print(f"\n🧩 批次计划: {request.product_code} × {request.requested_count}")
        batch, items, summary = run_plan_only(
            request,
            output_dir=str(output_dir),
            voiceover_root=args.voiceover_root,
            voiceover_db_path=str(output_dir / "voiceover_stage0.sqlite3"),
        )
        _print_batch(batch, items)
        _save_report(output_dir, batch, items, summary)
        return 0

    if args.mode == "script-only":
        if not args.batch_id:
            print("script-only 模式需要 --batch-id", file=sys.stderr)
            return 1
        print(f"\n🧩 执行批次: {args.batch_id}")
        batch, items = run_script_only(
            args.batch_id,
            resume=args.resume,
            limit=args.limit,
            script_mode=args.script_mode,
            delay_between_items=args.delay_between_items,
            plan_only=False,
            voiceover_root=args.voiceover_root,
            voiceover_db_path=str(output_dir / "voiceover_stage0.sqlite3"),
            voiceover_model_command=args.voiceover_model_command,
            voiceover_qc_model_command=args.voiceover_qc_model_command,
            blueprint_model=args.blueprint_model,
            blueprint_reasoning=args.blueprint_reasoning,
        )
        _print_batch(batch, items)
        _save_report(output_dir, batch, items, batch.allocation_summary_json)
        return 0

    print("未知模式", file=sys.stderr)
    return 1


def _print_batch(batch, items):
    print(f"\n═══════════════════════════════════════")
    print(f"Batch: {batch.batch_id}")
    print(f"Request: {batch.request_id}")
    print(f"Product: {batch.product_code} | Phase: {batch.test_phase}")
    print(f"Status: {batch.status} | Planned: {batch.planned_count} | Ready: {batch.ready_count} | Failed: {batch.failed_count}")
    print(f"---------------------------------------")
    roles = {"STRUCTURE_MOTHER": 0, "CONTENT_VARIANT": 0, "HOOK_VARIANT": 0}
    for item in items:
        print(f"  [{item.item_index:02d}] {item.item_role:20s} | slot={item.compatibility_slot:3s} | hook={item.requested_hook_id:30s} | angle={item.content_angle_key:20s} | status={item.status}")
        if item.item_role in roles:
            roles[item.item_role] += 1
    print(f"---------------------------------------")
    print(f"  Structure mothers: {roles['STRUCTURE_MOTHER']} | Content variants: {roles['CONTENT_VARIANT']} | Hook variants: {roles['HOOK_VARIANT']}")
    print(f"═══════════════════════════════════════")


def _save_report(output_dir, batch, items, summary):
    def _result(item):
        try:
            value = json.loads(item.result_json) if item.result_json else {}
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    def _voiceover_surface(item):
        try:
            frozen = json.loads(item.frozen_direction_package_json or "{}")
        except Exception:
            frozen = {}
        seed = frozen.get("simplified_creative_seed") if isinstance(frozen, dict) else {}
        contract = seed.get("voiceover_surface_contract") if isinstance(seed, dict) else {}
        return contract if isinstance(contract, dict) else {}

    report = {
        "batch_id": batch.batch_id,
        "request_id": batch.request_id,
        "product_code": batch.product_code,
        "status": batch.status,
        "requested_count": batch.requested_count,
        "planned_count": batch.planned_count,
        "ready_count": batch.ready_count,
        "failed_count": batch.failed_count,
        "allocation_summary": json.loads(batch.allocation_summary_json) if batch.allocation_summary_json else {},
        "items": [
            {
                "batch_item_id": it.batch_item_id,
                "item_index": it.item_index,
                "item_role": it.item_role,
                "compatibility_slot": it.compatibility_slot,
                "structure": {
                    "selection_run_id": it.selection_run_id,
                    "direction_assignment_id": it.direction_assignment_id,
                    "macro_family_key": it.macro_family_key,
                    "carrier_mode": it.carrier_mode,
                },
                "content": {
                    "content_bundle_id": it.content_bundle_id,
                    "content_angle_key": it.content_angle_key,
                    "audience_tension_status": it.audience_tension_status,
                    "audience_tension_text": it.audience_tension_text,
                    "claim_keys": json.loads(it.claim_keys_json) if it.claim_keys_json else [],
                },
                "expression": {
                    "requested_hook_id": it.requested_hook_id,
                    "eligible_hook_ids": json.loads(it.eligible_hook_ids_json) if it.eligible_hook_ids_json else [],
                    "actual_hook_id": it.actual_hook_id,
                    "voiceover_surface_contract": _voiceover_surface(it),
                },
                "creative": {
                    "creative_contract_id": it.creative_contract_id,
                    "visual_signature": it.visual_signature,
                    "frozen_direction_package": bool(it.frozen_direction_package_json),
                },
                "status": it.status,
                "script_mode": _result(it).get("script_mode", ""),
                "stage_cache": _result(it).get("stage_cache", {}),
                "script": _result(it).get("script", {}),
            }
            for it in items
        ],
    }
    report_path = output_dir / "batch_plan_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    complete_path = output_dir / "batch_complete_scripts.md"
    complete_path.write_text(
        _render_complete_scripts_markdown(report), encoding="utf-8"
    )
    print(f"\n报告: {report_path}")
    print(f"完整脚本: {complete_path}")


def _md(value) -> str:
    text = str(value or "").strip()
    return text.replace("|", "\\|").replace("\n", "<br>") or "UNAVAILABLE"


def _render_complete_scripts_markdown(report) -> str:
    """Render the complete script without dropping production design fields."""

    lines = [
        "# 原创批次完整脚本",
        "",
        f"- Batch：`{_md(report.get('batch_id'))}`",
        f"- 产品：`{_md(report.get('product_code'))}`",
        f"- 状态：`{_md(report.get('status'))}`",
        f"- 完成：{int(report.get('ready_count') or 0)}/{int(report.get('planned_count') or 0)}",
        "",
    ]
    for item in report.get("items") or []:
        if not isinstance(item, dict):
            continue
        script = item.get("script") if isinstance(item.get("script"), dict) else {}
        structure = item.get("structure") if isinstance(item.get("structure"), dict) else {}
        expression = item.get("expression") if isinstance(item.get("expression"), dict) else {}
        creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
        lines.extend(
            [
                f"## [{int(item.get('item_index') or 0):02d}] {_md(item.get('compatibility_slot'))}",
                "",
                "| 项目 | 内容 |",
                "|---|---|",
                f"| 状态 | {_md(item.get('status'))} |",
                f"| 结构家族 | {_md(structure.get('macro_family_key'))} |",
                f"| 承载方式 | {_md(structure.get('carrier_mode'))} |",
                f"| 请求钩子 | {_md(expression.get('requested_hook_id'))} |",
                f"| 实际钩子 | {_md(expression.get('actual_hook_id'))} |",
                f"| 观众关系偏好 | {_md((expression.get('voiceover_surface_contract') or {}).get('relationship_device'))} |",
                f"| 内容角度 | {_md((item.get('content') or {}).get('content_angle_key'))} |",
                f"| 视觉签名 | {_md(creative.get('visual_signature'))} |",
                "",
            ]
        )
        if not script:
            lines.extend(["尚无完整脚本产物。", ""])
            continue

        concept = script.get("script_concept") if isinstance(script.get("script_concept"), dict) else {}
        production = script.get("production_design") if isinstance(script.get("production_design"), dict) else {}
        character = production.get("character") if isinstance(production.get("character"), dict) else {}
        outfit = production.get("outfit") if isinstance(production.get("outfit"), dict) else {}
        scene = production.get("scene") if isinstance(production.get("scene"), dict) else {}
        emotion = production.get("emotion") if isinstance(production.get("emotion"), dict) else {}
        product_usage = script.get("product_usage") if isinstance(script.get("product_usage"), dict) else {}
        voice = script.get("continuous_voiceover") if isinstance(script.get("continuous_voiceover"), dict) else {}

        lines.extend(
            [
                "### 创作概念",
                "",
                "| 项目 | 内容 |",
                "|---|---|",
                f"| 一句话创意 | {_md(concept.get('one_sentence_idea'))} |",
                f"| 观众需求 | {_md(concept.get('viewer_need'))} |",
                f"| 开场意图 | {_md(concept.get('hook_intent'))} |",
                f"| 宏观结构 | {_md(' > '.join(concept.get('macro_structure') or []))} |",
                "",
                "### 人物与表达",
                "",
                "| 项目 | 内容 |",
                "|---|---|",
                f"| 出镜方式 | {_md(production.get('presentation_mode'))} |",
                f"| 人物身份 | {_md(character.get('identity'))} |",
                f"| 外貌特征 | {_md(character.get('appearance'))} |",
                f"| 妆发 | {_md(character.get('hair_makeup'))} |",
                f"| 说话人格 | {_md(character.get('speaking_personality'))} |",
                "",
                "### 完整穿搭",
                "",
                "| 项目 | 内容 |",
                "|---|---|",
                f"| 基础穿搭 | {_md(outfit.get('base_outfit'))} |",
                f"| 商品角色 | {_md(outfit.get('product_role'))} |",
                f"| 配饰与道具 | {_md(outfit.get('accessories'))} |",
                "",
                "### 场景与情绪",
                "",
                "| 项目 | 内容 |",
                "|---|---|",
                f"| 地点 | {_md(scene.get('location'))} |",
                f"| 时刻 | {_md(scene.get('moment'))} |",
                f"| 光线 | {_md(scene.get('lighting'))} |",
                f"| 背景 | {_md(scene.get('background'))} |",
                f"| 开始状态 | {_md(emotion.get('starting_state'))} |",
                f"| 自然变化 | {_md(emotion.get('natural_change'))} |",
                f"| 结束状态 | {_md(emotion.get('ending_state'))} |",
                "",
                "### 商品身份与锚点",
                "",
                f"- 商品身份锚点：{_md('；'.join(product_usage.get('identity_anchors_preserved') or []))}",
                f"- 本条使用的证明事实：{_md('；'.join(product_usage.get('selling_points_used') or []))}",
                "",
                "### 连续口播",
                "",
                f"- 泰语：{_md(voice.get('target_text'))}",
                f"- 中文：{_md(voice.get('chinese_translation'))}",
                f"- 卖点实际表达：{_md(voice.get('selling_argument_realization'))}",
                "",
                "### 完整分镜",
                "",
                "| 镜头 | 时间 | 叙事角色 | 画面 | 动作 | 情绪 | 机位 | 商品锚点 |",
                "|---:|---|---|---|---|---|---|---|",
            ]
        )
        for index, shot in enumerate(script.get("storyboard") or [], 1):
            if not isinstance(shot, dict):
                continue
            lines.append(
                "| {shot_no} | {time} | {role} | {visual} | {action} | {emotion} | {camera} | {anchors} |".format(
                    shot_no=int(shot.get("shot_no") or index),
                    time=_md(shot.get("time_range")),
                    role=_md(shot.get("narrative_role")),
                    visual=_md(shot.get("visual_content")),
                    action=_md(shot.get("character_action")),
                    emotion=_md(shot.get("natural_emotion")),
                    camera=_md(shot.get("camera")),
                    anchors=_md("；".join(shot.get("product_anchors_visible") or [])),
                )
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
