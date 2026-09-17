#!/usr/bin/env python3
"""V1 原创批次轻编排器 CLI — plan-only / script-only / show-plan"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))


def _render_validation_blocks(validation) -> bool:
    """Does this audit say the delivered prompt contradicts its own contract?

    Lazy import on purpose: this script is imported by tests with a minimal
    environment, so a module-level ``core`` import would change import-time
    behaviour.  Without the renderer the judgement is impossible, so it fails
    *open* — reporting must never be the thing that blocks delivery.
    """

    try:
        from core.production_script_renderer import render_validation_blocks_delivery
    except Exception:  # noqa: BLE001
        return False
    return render_validation_blocks_delivery(validation)


def _mixed_rendered_counts(items, result_of) -> Dict[str, object]:
    """Aggregate the B2 counting axes from each item's rendered re-check.

    这四项由**成稿差异裁决**产出，不是"生成成功数"的另一种写法：同一批里
    四条都生成成功，仍可能只有两个独立主题。按唯一 ``script_id`` 去重，同一条
    稿不会因为跟多个参照比过而被计两次。

    ``None``（裁决不可用）与 ``0``（确实没有）必须分开：报告把"算不出来"印成
    0，会被读成"这批什么都没产出"。
    """

    try:
        from core.accessory_mixed_templates import (
            MIXED_VERDICT_EXACT_DUPLICATE,
            MIXED_VERDICT_NEEDS_REVIEW,
            MIXED_VERDICT_SURFACE_ONLY,
        )
    except Exception:  # noqa: BLE001 - reporting must never break a run
        return {
            "distinct_theme_count": None,
            "effective_variant_count": None,
            "duplicate_or_surface_count": None,
            "needs_review_count": None,
            "not_compared_count": None,
            "counts_available": False,
        }

    def _recheck(item):
        value = result_of(item).get("mixed_final_shot_recheck")
        return value if isinstance(value, dict) else {}

    def _key(item):
        return str(result_of(item).get("script_id") or getattr(item, "batch_item_id", ""))

    def _compared(recheck):
        return int(recheck.get("references_compared") or 0) > 0

    def _deduped(predicate):
        return len({_key(it) for it in items if predicate(_recheck(it))})

    return {
        "distinct_theme_count": _deduped(
            lambda r: _compared(r) and bool(r.get("counts_as_theme"))
        ),
        "effective_variant_count": _deduped(
            lambda r: _compared(r) and bool(r.get("counts_as_variant"))
        ),
        "duplicate_or_surface_count": _deduped(
            lambda r: str(r.get("review_status") or "").upper()
            in (MIXED_VERDICT_EXACT_DUPLICATE, MIXED_VERDICT_SURFACE_ONLY)
        ),
        "needs_review_count": _deduped(
            lambda r: bool(r)
            and (
                str(r.get("review_status") or "").upper()
                == MIXED_VERDICT_NEEDS_REVIEW
                or bool(r.get("montage_collapsed"))
            )
        ),
        "not_compared_count": sum(
            1 for it in items if _recheck(it) and not _compared(_recheck(it))
        ),
        "counts_available": True,
    }


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
    parser.add_argument(
        "--item-timeout-seconds",
        type=int,
        default=420,
        help="单条脚本最大等待秒数；0 表示关闭 item 级超时",
    )
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
    parser.add_argument(
        "--top-category",
        default="",
        help="可选：覆盖旧历史记录中的一级类目；新产品或旧泛类目记录调试时使用",
    )
    parser.add_argument(
        "--product-type",
        default="",
        help="可选：覆盖旧历史记录中的具体产品类型，例如抓夹、耳环；必须使用已登记类型",
    )
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
            top_category=args.top_category,
            product_type=args.product_type,
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
            item_timeout_seconds=args.item_timeout_seconds,
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

    def _creative_contract(item):
        try:
            frozen = json.loads(item.frozen_direction_package_json or "{}")
        except Exception:
            frozen = {}
        contract = (
            frozen.get("creative_diversity_contract")
            if isinstance(frozen, dict) else {}
        )
        return contract if isinstance(contract, dict) else {}

    def _semantic_contracts(item):
        try:
            frozen = json.loads(item.frozen_direction_package_json or "{}")
        except Exception:
            frozen = {}
        return {
            "semantic_spine_contract": (
                frozen.get("semantic_spine_contract")
                if isinstance(frozen.get("semantic_spine_contract"), dict)
                else {}
            ),
            "context_bridge_contract": (
                frozen.get("context_bridge_contract")
                if isinstance(frozen.get("context_bridge_contract"), dict)
                else {}
            ),
        }

    def _claim_action_contract(item):
        try:
            frozen = json.loads(item.frozen_direction_package_json or "{}")
        except Exception:
            frozen = {}
        contract = (
            frozen.get("claim_action_contract")
            if isinstance(frozen, dict) else {}
        )
        return contract if isinstance(contract, dict) else {}

    def _production_diagnostics(item):
        result = _result(item)
        if not isinstance(result.get("script"), dict):
            return {}
        try:
            from core.production_script_renderer import (
                build_production_projection,
            )

            projection = build_production_projection(batch=batch, item=item)
        except Exception as exc:
            return {"status": "UNAVAILABLE", "reason": str(exc)[:240]}
        return {
            "status": "AVAILABLE",
            "capture_rhythm_schema": projection.get(
                "capture_rhythm_schema", ""
            ),
            "visible_clip_count": projection.get("visible_clip_count", 0),
            "camera_setup_count": projection.get("camera_setup_count", 0),
            "shot_richness_status": projection.get(
                "shot_richness_status", ""
            ),
        }

    def _render_validation(item):
        """Persisted execution audit for one item (``{}`` when not applicable).

        Written at script-ready time and bound to the delivered prompt by
        ``prompt_hash`` + ``renderer_version``; the same object the final
        hand-off gates on, so the report and the gate cannot disagree.
        """

        value = _result(item).get("render_validation")
        return value if isinstance(value, dict) else {}

    def _mixed_delivery(item):
        """Why a generated mixed script was or was not delivered as independent.

        The operator only sees the item status otherwise, and "SCRIPT_DUPLICATE"
        without the verdict and the nearest item is not actionable.
        """

        recheck = _result(item).get("mixed_final_shot_recheck")
        if not isinstance(recheck, dict) or not recheck:
            return {}
        try:
            from core.accessory_mixed_templates import mixed_delivery_decision

            return mixed_delivery_decision(recheck)
        except Exception:
            return {}

    def _mixed_difference_counts(item):
        """B2 的两个计数轴：这条稿在成稿比对里算不算独立主题／有效变体。

        Kept per item so a reviewer can see *which* script was counted, next to
        the nearest reference and the dimensions that produced the verdict.
        """

        recheck = _result(item).get("mixed_final_shot_recheck")
        if not isinstance(recheck, dict) or not recheck:
            return {}
        return {
            "review_status": recheck.get("review_status", ""),
            "counts_as_theme": bool(recheck.get("counts_as_theme")),
            "counts_as_variant": bool(recheck.get("counts_as_variant")),
            "order_only": bool(recheck.get("order_only")),
            "references_compared": int(recheck.get("references_compared") or 0),
            "nearest_script_id": recheck.get("nearest_script_id", ""),
            "difference_dimensions": recheck.get("difference_dimensions") or [],
            "new_observation_points": recheck.get("new_observation_points") or [],
        }

    def _mixed_history_persistence(item):
        """成稿签名有没有真的写进台账（B4）。

        A script whose rendered signature never reached the ledger has **no**
        cross-batch protection: the next batch cannot compare 正文 against it.
        Reporting the write as done would claim protection that is not there,
        so the outcome travels into the report instead of being swallowed.
        """

        value = _result(item).get("mixed_rendered_history")
        return value if isinstance(value, dict) else {}

    duplicate_count = sum(
        1 for it in items if str(it.status or "") == "SCRIPT_DUPLICATE"
    )
    # A deterministic execution conflict (delivered prompt vs its own frozen
    # contract) is a *different* bucket from "similar to an earlier script": it
    # is not an originality judgement, it is a defect.  It is deducted from the
    # producible count here and reported separately rather than folded into
    # ready/failed, and it is deliberately not turned into SCRIPT_FAILED — see
    # ``render_validation_blocks_delivery``.
    execution_blocked_count = sum(
        1
        for it in items
        if _render_validation_blocks(_render_validation(it))
    )
    audit_error_count = sum(
        1
        for it in items
        if str(_render_validation(it).get("status") or "") == "AUDIT_ERROR"
    )
    counts = _mixed_rendered_counts(items, _result)
    # Only mixed items count: a non-mixed item has no rendered history slot at
    # all, and "0 persisted" there would be a false alarm rather than a gap.
    history_not_persisted_count = sum(
        1
        for it in items
        if isinstance(_result(it).get("mixed_final_shot_recheck"), dict)
        and _result(it).get("mixed_final_shot_recheck")
        and not _mixed_history_persistence(it).get("history_persisted", False)
    )
    report = {
        "batch_id": batch.batch_id,
        "request_id": batch.request_id,
        "product_code": batch.product_code,
        "target_country": batch.target_country,
        "target_language": batch.target_language,
        "status": batch.status,
        "requested_count": batch.requested_count,
        "planned_count": batch.planned_count,
        "ready_count": batch.ready_count,
        "failed_count": batch.failed_count,
        # "完成" alone cannot answer "how much *distinct* content did this batch
        # produce": a generated script whose shots repeat an earlier delivery is
        # excluded from the independent set and reported separately.
        "duplicate_count": duplicate_count,
        # 请求数 / 生成成功数 / 执行校验通过数 / 独立主题数 / 有效变体数 /
        # 重复与表层变化数 / 待复核数 / 可生产数 —— 八项分开。主题与变体来自
        # 成稿差异裁决并按唯一 script_id 去重；没有可比参照的条目单列
        # ``not_compared_count``，不得悄悄算进独立主题。
        **counts,
        "execution_blocked_count": execution_blocked_count,
        "execution_audit_error_count": audit_error_count,
        # 成稿签名没写进台账的条数。此时这些稿件**没有**跨批去重保护：下一批
        # 拿不到它们的正文，也谈不上去重。单列出来，避免报告声称已完成保护。
        "history_not_persisted_count": history_not_persisted_count,
        "execution_validation_pass_count": sum(
            1
            for it in items
            if str(_render_validation(it).get("status") or "") == "PASS"
        ),
        # "可生产"只认已经被判定为独立或有效变体的条目：重复/表层变化、执行
        # 校验未通过、待复核、以及没有可比参照因而无法确认独立的，都要单独扣掉。
        # 这一步故意保守 —— 把无法确认的条目算成可生产，就是把"生成成功"当质量。
        "producible_count": max(
            0,
            int(batch.ready_count or 0)
            - duplicate_count
            - execution_blocked_count
            - int(counts.get("needs_review_count") or 0)
            - int(counts.get("not_compared_count") or 0),
        ),
        "counts_note": (
            "独立主题数/有效变体数来自成稿差异裁决（按唯一 script_id 去重）；"
            "重复与表层变化数、待复核数同理。不得用生成成功数、模板排列数"
            "或背景数量代替。"
        ),
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
                    **_semantic_contracts(it),
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
                    "perceptual_signature": _creative_contract(it).get(
                        "perceptual_signature", ""
                    ),
                    "perceptual_repeat_status": _creative_contract(it).get(
                        "perceptual_repeat_status", "NEW"
                    ),
                    "frozen_direction_package": bool(it.frozen_direction_package_json),
                    "scene_family_key": _creative_contract(it).get("scene_family_key", ""),
                    "scene_motif": _creative_contract(it).get("scene_motif", ""),
                    "outfit_selection_contract": _creative_contract(it).get(
                        "outfit_selection_contract", {}
                    ),
                    "outfit_scene_affinity_contract": _creative_contract(it).get(
                        "outfit_scene_affinity_contract", {}
                    ),
                    "persona_selection_contract": _creative_contract(it).get(
                        "persona_selection_contract", {}
                    ),
                    "outfit_persona_affinity_contract": _creative_contract(it).get(
                        "outfit_persona_affinity_contract", {}
                    ),
                    "claim_action_contract": _claim_action_contract(it),
                },
                "status": it.status,
                "script_mode": _result(it).get("script_mode", ""),
                "mixed_delivery": _mixed_delivery(it),
                "mixed_difference_counts": _mixed_difference_counts(it),
                "mixed_history_persistence": _mixed_history_persistence(it),
                "render_validation": _render_validation(it),
                "stage_cache": _result(it).get("stage_cache", {}),
                "retrieval_reference_provenance": _result(it).get(
                    "retrieval_reference_provenance", {}
                ),
                "production_diagnostics": _production_diagnostics(it),
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


def _counts_section(report) -> List[str]:
    """八项分开汇报：请求 / 生成成功 / 执行校验通过 / 独立主题 / 有效变体 /
    重复与表层变化 / 待复核 / 可生产。

    The point of this section is that "完成 N 条" cannot be read as "N 条原创"：
    a batch can generate four scripts and still deliver one theme.  The two
    content axes come from the rendered difference verdicts (deduplicated by
    ``script_id``), never from the generation count or the template count.
    """

    if not report.get("counts_available", False):
        return [
            "## 内容差异计数",
            "",
            "- 差异裁决模块不可用，独立主题数与有效变体数**未计算**（不是 0）。",
            "",
        ]
    counts = [
        ("请求数", report.get("requested_count")),
        ("生成成功数", report.get("ready_count")),
        ("执行校验通过数", report.get("execution_validation_pass_count")),
        ("独立主题数", report.get("distinct_theme_count")),
        ("有效变体数", report.get("effective_variant_count")),
        ("重复／表层变化数", report.get("duplicate_or_surface_count")),
        ("待复核数", report.get("needs_review_count")),
        ("可生产数", report.get("producible_count")),
    ]
    lines = [
        "## 内容差异计数",
        "",
        "- " + "；".join(f"{label} {int(value or 0)}" for label, value in counts),
    ]
    not_compared = int(report.get("not_compared_count") or 0)
    if not_compared:
        lines.append(
            f"- 无可比参照因而未判定独立的条目：{not_compared}"
            "（既不是重复，也不算已确认的独立主题）"
        )
    not_persisted = int(report.get("history_not_persisted_count") or 0)
    if not_persisted:
        lines.append(
            f"- ⚠️ 成稿签名未写入台账的条目：{not_persisted}"
            "（这些稿件**没有**跨批去重保护：下一批读不到它们的正文，"
            "不得到报告里当成已完成去重）"
        )
    note = str(report.get("counts_note") or "").strip()
    if note:
        lines.append(f"- 口径：{note}")
    lines.append("")
    return lines


def _md(value) -> str:
    text = str(value or "").strip()
    return text.replace("|", "\\|").replace("\n", "<br>") or "UNAVAILABLE"


def _capacity_section(report) -> List[str]:
    """Surface *why* a batch produced fewer candidates than requested.

    All values already exist in ``allocation_summary``.  Operators previously
    saw only ``完成 0/20`` with no reason, which made a content-capacity
    shortage look identical to a model failure.  Nothing new is computed or
    invented here -- existing fields are only promoted into the readable report.
    """

    summary = report.get("allocation_summary")
    summary = summary if isinstance(summary, dict) else {}
    requested = int(report.get("requested_count") or 0)
    planned = int(report.get("planned_count") or 0)
    ready = int(report.get("ready_count") or 0)
    failed = int(report.get("failed_count") or 0)
    shortage = int(summary.get("shortage_count") or max(0, requested - planned))
    status = _md(summary.get("allocation_status") or report.get("status"))
    lines = [
        "## 内容容量与规划状态",
        "",
        f"- 请求 / 已规划 / 已就绪 / 失败：{requested} / {planned} / {ready} / {failed}",
        f"- 规划状态：`{status}`",
        (
            f"- 独立方向数：{int(summary.get('structure_count') or 0)}"
            "（受 `ORIGINAL_SCRIPT_DIRECTION_LIMIT` 控制，默认 4）"
        ),
        f"- 已用唯一卖点数：{int(summary.get('used_selling_argument_count') or 0)}",
    ]
    if shortage > 0:
        lines.append(
            f"- 未补足：{shortage} 条（属于内容容量不足，不是模型失败）"
        )
    reasons: Dict[str, int] = {}
    for entry in summary.get("deferred_content") or []:
        if not isinstance(entry, dict):
            continue
        reason = str(entry.get("downgrade_reason") or "").strip() or "UNSPECIFIED"
        reasons[reason] = reasons.get(reason, 0) + 1
    if reasons:
        detail = "；".join(
            f"{key} × {value}" for key, value in sorted(reasons.items())
        )
        lines.append(f"- 未补足原因：{detail}")
    lines.append("")
    return lines


def _render_complete_scripts_markdown(report) -> str:
    """Render the complete script without dropping production design fields."""

    lines = [
        "# 原创批次完整脚本",
        "",
        f"- Batch：`{_md(report.get('batch_id'))}`",
        f"- 产品：`{_md(report.get('product_code'))}`",
        f"- 状态：`{_md(report.get('status'))}`",
        f"- 完成：{int(report.get('ready_count') or 0)}/{int(report.get('planned_count') or 0)}",
    ]
    duplicate_count = int(report.get("duplicate_count") or 0)
    if duplicate_count:
        lines.append(
            f"- 其中成稿重复（不计入独立可用）：{duplicate_count}；"
            f"失败：{int(report.get('failed_count') or 0)}"
        )
    execution_blocked = int(report.get("execution_blocked_count") or 0)
    if execution_blocked:
        lines.append(
            f"- 执行校验未通过（不计入可生产、未提交视频）：{execution_blocked}"
        )
    lines.append(f"- 可生产：{int(report.get('producible_count') or 0)}")
    lines.extend(_counts_section(report))
    lines.append("")
    lines.extend(_capacity_section(report))
    for item in report.get("items") or []:
        if not isinstance(item, dict):
            continue
        script = item.get("script") if isinstance(item.get("script"), dict) else {}
        structure = item.get("structure") if isinstance(item.get("structure"), dict) else {}
        mixed_delivery = (
            item.get("mixed_delivery")
            if isinstance(item.get("mixed_delivery"), dict)
            else {}
        )
        mixed_history = (
            item.get("mixed_history_persistence")
            if isinstance(item.get("mixed_history_persistence"), dict)
            else {}
        )
        validation = (
            item.get("render_validation")
            if isinstance(item.get("render_validation"), dict)
            else {}
        )
        validation_issues = (
            validation.get("issues")
            if isinstance(validation.get("issues"), list)
            else []
        )
        validation_conflict = "、".join(
            f"镜{_md(issue.get('shot_id'))}[{_md(issue.get('module'))}]"
            f"{_md(issue.get('code'))}"
            for issue in validation_issues[:6]
            if isinstance(issue, dict)
        )
        expression = item.get("expression") if isinstance(item.get("expression"), dict) else {}
        creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
        outfit_contract = (
            creative.get("outfit_selection_contract")
            if isinstance(creative.get("outfit_selection_contract"), dict)
            else {}
        )
        persona_contract = (
            creative.get("persona_selection_contract")
            if isinstance(creative.get("persona_selection_contract"), dict)
            else {}
        )
        affinity_contract = (
            creative.get("outfit_persona_affinity_contract")
            if isinstance(creative.get("outfit_persona_affinity_contract"), dict)
            else {}
        )
        scene_affinity_contract = (
            creative.get("outfit_scene_affinity_contract")
            if isinstance(creative.get("outfit_scene_affinity_contract"), dict)
            else {}
        )
        outfit_recipe = (
            outfit_contract.get("outfit_recipe")
            if isinstance(outfit_contract.get("outfit_recipe"), dict)
            else {}
        )
        accessory_items = [
            str(value).strip()
            for value in (outfit_contract.get("accessory_items") or [])
            if str(value or "").strip()
        ]
        scene_match_label = {
            "MATCHED": "已匹配",
            "NO_PREFERENCE": "未配置偏好",
            "FALLBACK": "已回退",
            "NOT_APPLICABLE": "不适用",
        }.get(str(scene_affinity_contract.get("match_status") or "").upper(), "不适用")
        semantic_spine = (
            (item.get("content") or {}).get("semantic_spine_contract")
            if isinstance((item.get("content") or {}).get("semantic_spine_contract"), dict)
            else {}
        )
        semantic_thesis = (
            semantic_spine.get("script_thesis")
            if isinstance(semantic_spine.get("script_thesis"), dict)
            else {}
        )
        context_bridge = (
            (item.get("content") or {}).get("context_bridge_contract")
            if isinstance((item.get("content") or {}).get("context_bridge_contract"), dict)
            else {}
        )
        scene_relation = (
            context_bridge.get("scene_relation")
            if isinstance(context_bridge.get("scene_relation"), dict)
            else {}
        )
        lines.extend(
            [
                f"## [{int(item.get('item_index') or 0):02d}] {_md(item.get('compatibility_slot'))}",
                "",
                "| 项目 | 内容 |",
                "|---|---|",
                f"| 状态 | {_md(item.get('status'))} |",
                *(
                    [
                        f"| 成稿复核 | {_md(mixed_delivery.get('verdict'))}"
                        f"{'｜需人工复核' if mixed_delivery.get('requires_review') else ''} |",
                        f"| 复核说明 | {_md(mixed_delivery.get('reason'))} |",
                    ]
                    if mixed_delivery
                    else []
                ),
                *(
                    [
                        f"| 跨批保护 | "
                        f"{'已写入台账' if mixed_history.get('history_persisted') else '未写入台账：' + _md(mixed_history.get('reason'))} |"
                    ]
                    if mixed_history
                    else []
                ),
                *(
                    [
                        f"| 执行校验 | {_md(validation.get('status'))}"
                        f"{'｜' if validation_conflict else ''}{validation_conflict} |",
                        f"| 交付绑定 | {_md(validation.get('renderer_version'))}"
                        f" / {_md(validation.get('prompt_hash'))} |",
                    ]
                    if validation
                    else []
                ),
                f"| 结构家族 | {_md(structure.get('macro_family_key'))} |",
                f"| 承载方式 | {_md(structure.get('carrier_mode'))} |",
                f"| 请求钩子 | {_md(expression.get('requested_hook_id'))} |",
                f"| 实际钩子 | {_md(expression.get('actual_hook_id'))} |",
                f"| 观众关系偏好 | {_md((expression.get('voiceover_surface_contract') or {}).get('relationship_device'))} |",
                f"| 内容角度 | {_md((item.get('content') or {}).get('content_angle_key'))} |",
                f"| 主消费情境 | {_md(semantic_thesis.get('primary_narrative_context'))} |",
                f"| 核心购买理由 | {_md(semantic_thesis.get('core_buying_reason'))} |",
                f"| 场景语义关系 | {_md(scene_relation.get('relation'))} |",
                f"| 口播语境模式 | {_md(context_bridge.get('voiceover_context_mode'))} |",
                f"| 视觉签名 | {_md(creative.get('visual_signature'))} |",
                f"| 感知签名 | {_md(creative.get('perceptual_signature'))} |",
                f"| 跨批次重复状态 | {_md(creative.get('perceptual_repeat_status'))} |",
                f"| 卖点动作意图 | {_md((creative.get('claim_action_contract') or {}).get('proof_action_intent'))} |",
                f"| 选定动作模式 | {_md((creative.get('claim_action_contract') or {}).get('preferred_action_mode'))} |",
                f"| 人物模板 | {_md(persona_contract.get('persona_name') or persona_contract.get('persona_id'))} |",
                f"| 穿搭模板 | {_md(outfit_contract.get('template_display_name') or outfit_contract.get('template_id') or outfit_contract.get('silhouette_key'))} |",
                f"| 实际配饰 | {_md('；'.join(accessory_items) or outfit_recipe.get('other_accessories') or '不适用')} |",
                f"| 场景 | {_md(creative.get('scene_motif') or creative.get('scene_family_key'))} |",
                f"| 穿搭×场景匹配 | {_md(scene_match_label)} |",
                f"| 人物×穿搭匹配 | {_md(affinity_contract.get('match_status'))} |",
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
        reference_provenance = (
            item.get("retrieval_reference_provenance")
            if isinstance(item.get("retrieval_reference_provenance"), dict)
            else {}
        )
        reference_realization = (
            script.get("reference_realization")
            if isinstance(script.get("reference_realization"), dict)
            else {}
        )

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
                "### 真实案例参考（仅观察）",
                "",
                f"- 案例状态：{_md(reference_provenance.get('status'))}",
                f"- 类目匹配：{_md(reference_provenance.get('primary_category_match_status'))}",
                f"- 分镜观察承载：{_md(reference_provenance.get('storyboard_observed_carrier'))}",
                f"- 执行骨架：{_md(reference_provenance.get('reference_spine_id'))}",
                f"- 实际借鉴：{_md(reference_realization.get('status'))} / {_md('、'.join(reference_realization.get('adopted_parts') or []))}",
                f"- 改写说明：{_md(reference_realization.get('adaptation_notes'))}",
                "",
                "### 连续口播",
                "",
                f"- {_md(voice.get('target_language') or report.get('target_language') or '目标语言')}：{_md(voice.get('target_text'))}",
                f"- 中文：{_md(voice.get('chinese_translation'))}",
                f"- 卖点实际表达：{_md(voice.get('selling_argument_realization'))}",
                "",
                "### 完整分镜",
                "",
                "| 镜头 | 时间 | 叙事角色 | 拍摄单元/剪辑 | 画面 | 动作 | 情绪 | 机位 | 商品锚点 |",
                "|---:|---|---|---|---|---|---|---|---|",
            ]
        )
        for index, shot in enumerate(script.get("storyboard") or [], 1):
            if not isinstance(shot, dict):
                continue
            lines.append(
                "| {shot_no} | {time} | {role} | {capture} | {visual} | {action} | {emotion} | {camera} | {anchors} |".format(
                    shot_no=int(shot.get("shot_no") or index),
                    time=_md(shot.get("time_range")),
                    role=_md(shot.get("narrative_role")),
                    capture=_md(
                        f"{shot.get('capture_unit_id') or 'UNAVAILABLE'} / {shot.get('edit_before') or 'UNAVAILABLE'}"
                    ),
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
