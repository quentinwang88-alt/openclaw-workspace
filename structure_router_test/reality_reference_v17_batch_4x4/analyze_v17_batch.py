#!/usr/bin/env python3
"""Audit a reality-reference stage-0 batch without counting retries as scripts."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


SCRIPT_PATH = Path(__file__).resolve()
WORKSPACE_ROOT = SCRIPT_PATH.parents[2]
SKILL_ROOT = WORKSPACE_ROOT / "skills" / "original-script-generator"


def load_stage0(path: Path) -> Optional[Dict[str, Any]]:
    result_path = path / "stage0_result.json"
    if not result_path.exists():
        return None
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _dict_rows(db_path: Path, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return []
    with sqlite3.connect(str(db_path)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(sql, tuple(params)).fetchall()]


def load_creative_patterns(db_path: Path) -> List[Dict[str, Any]]:
    return _dict_rows(
        db_path,
        "SELECT * FROM creative_pattern_usage ORDER BY source_run_id, created_at, usage_id",
    )


def load_voiceover_results(db_path: Path) -> List[Dict[str, Any]]:
    rows = _dict_rows(
        db_path,
        """
        SELECT j.job_id, j.product_id, j.video_id, j.status AS job_status,
               j.failure_code, j.last_successful_step, j.revision,
               d.draft_id, d.draft_no, d.target_text, d.chinese_translation,
               d.estimated_duration_sec, d.estimate_upper_sec,
               d.qc_json, d.status AS draft_status
        FROM voiceover_jobs j
        LEFT JOIN voiceover_drafts d
          ON d.job_id = j.job_id
         AND d.draft_no = (
             SELECT MAX(d2.draft_no)
             FROM voiceover_drafts d2
             WHERE d2.job_id = j.job_id
         )
        ORDER BY j.product_id, j.created_at, j.job_id
        """,
    )
    for row in rows:
        try:
            row["qc"] = json.loads(row.get("qc_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            row["qc"] = {}
    return rows


def _batch_run_ids(data: Optional[Dict[str, Any]]) -> set[int]:
    result: set[int] = set()
    for product in (data or {}).get("products", []) or []:
        if not isinstance(product, dict):
            continue
        try:
            result.add(int(product.get("stage0_run_id")))
        except (TypeError, ValueError):
            continue
    return result


def dedupe_creative_patterns(
    rows: Sequence[Dict[str, Any]],
    *,
    batch_run_ids: Iterable[int] = (),
) -> Tuple[List[Dict[str, Any]], int]:
    """Keep one latest attempt per product + routed direction.

    If stage0_result identifies this batch's RDS run ids, unrelated historical
    rows are excluded first.  Otherwise the newest run per product/direction is
    retained so a retry cannot masquerade as an additional creative direction.
    """

    run_ids = {int(item) for item in batch_run_ids}
    scoped = [row for row in rows if int(row.get("source_run_id") or -1) in run_ids] if run_ids else list(rows)
    latest: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in scoped:
        key = (str(row.get("product_code") or ""), str(row.get("direction_id") or ""))
        prior = latest.get(key)
        current_order = (int(row.get("source_run_id") or -1), str(row.get("created_at") or ""), str(row.get("usage_id") or ""))
        prior_order = (
            int(prior.get("source_run_id") or -1),
            str(prior.get("created_at") or ""),
            str(prior.get("usage_id") or ""),
        ) if prior else (-1, "", "")
        if prior is None or current_order > prior_order:
            latest[key] = row
    deduped = sorted(
        latest.values(),
        key=lambda row: (str(row.get("product_code") or ""), str(row.get("direction_id") or "")),
    )
    return deduped, max(0, len(scoped) - len(deduped))


CORE_AXES = ("structure_family", "persona_role", "scene_motif", "opening_action", "action_grammar")


def compute_intra_diversity(patterns: Sequence[Dict[str, Any]], product_code: str) -> Dict[str, Any]:
    product_patterns = [row for row in patterns if str(row.get("product_code") or "") == product_code]
    if len(product_patterns) < 2:
        return {"verdict": "INSUFFICIENT", "pairs": [], "pass_rate": None, "avg_diff": None}
    pairs: List[Dict[str, Any]] = []
    for index, first in enumerate(product_patterns):
        for second in product_patterns[index + 1 :]:
            differences = [axis for axis in CORE_AXES if first.get(axis) != second.get(axis)]
            pairs.append(
                {
                    "pair": f"{first.get('direction_id')} vs {second.get('direction_id')}",
                    "diff_count": len(differences),
                    "diff_axes": differences,
                }
            )
    pass_rate = sum(1 for row in pairs if row["diff_count"] >= 2) / len(pairs)
    return {
        "verdict": "PASS" if pass_rate >= 0.75 else "FAIL",
        "pairs": pairs,
        "avg_diff": sum(row["diff_count"] for row in pairs) / len(pairs),
        "pass_rate": pass_rate,
    }


def compute_cross_product(patterns: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    signature_products: Dict[str, set[str]] = defaultdict(set)
    for row in patterns:
        signature = "|".join(str(row.get(axis) or "") for axis in CORE_AXES[1:])
        signature_products[signature].add(str(row.get("product_code") or ""))
    duplicates = {
        signature: sorted(products)
        for signature, products in signature_products.items()
        if len({item for item in products if item}) >= 2
    }
    return {
        "unique_signatures": len(signature_products),
        "cross_product_duplicate_count": len(duplicates),
        "duplicate_details": duplicates,
    }


def load_rds_stage_summary(
    data: Optional[Dict[str, Any]],
    patterns: Sequence[Dict[str, Any]],
    *,
    enabled: bool,
) -> Dict[str, Any]:
    run_ids = sorted(
        _batch_run_ids(data)
        or {
            int(row.get("source_run_id"))
            for row in patterns
            if str(row.get("source_run_id") or "").isdigit()
        }
    )
    if not enabled:
        return {"status": "SKIPPED", "run_ids": run_ids, "runs": {}}
    if not run_ids:
        return {"status": "UNAVAILABLE", "reason": "结果文件和创意登记均没有run_id", "run_ids": [], "runs": {}}
    try:
        if str(SKILL_ROOT) not in sys.path:
            sys.path.insert(0, str(SKILL_ROOT))
        from core.storage import PipelineStorage

        storage = PipelineStorage()
        summaries: Dict[str, Any] = {}
        for run_id in run_ids:
            stages = storage.query_stage_results(run_id)
            completed_direction_keys = sorted(
                {
                    f"{str(row['product_code'] or '')}:{str(row['stage_name'] or '').removeprefix('authenticity_review_')}"
                    for row in stages
                    if str(row["status"] or "") == "success"
                    and str(row["stage_name"] or "").startswith("authenticity_review_")
                }
            )
            summaries[str(run_id)] = {
                "stage_count": len(stages),
                "success_count": sum(1 for row in stages if str(row["status"] or "") == "success"),
                "failed_count": sum(1 for row in stages if str(row["status"] or "") == "failed"),
                "successful_stage_names": [str(row["stage_name"] or "") for row in stages if str(row["status"] or "") == "success"],
                "failed_stages": [
                    {
                        "stage_name": str(row["stage_name"] or ""),
                        "error_message": str(row["error_message"] or ""),
                    }
                    for row in stages
                    if str(row["status"] or "") == "failed"
                ],
                "completed_direction_keys": completed_direction_keys,
            }
        return {"status": "AVAILABLE", "run_ids": run_ids, "runs": summaries}
    except Exception as exc:  # RDS is useful corroboration, not a requirement to read local artifacts.
        return {"status": "UNAVAILABLE", "reason": str(exc)[:1000], "run_ids": run_ids, "runs": {}}


def build_metrics(
    data: Optional[Dict[str, Any]],
    raw_patterns: Sequence[Dict[str, Any]],
    voiceovers: Sequence[Dict[str, Any]],
    rds: Dict[str, Any],
) -> Dict[str, Any]:
    products = [item for item in (data or {}).get("products", []) or [] if isinstance(item, dict)]
    patterns, ignored_retries = dedupe_creative_patterns(
        raw_patterns,
        batch_run_ids=_batch_run_ids(data),
    )
    product_codes = sorted(
        {
            str(item.get("product_code") or "")
            for item in [*products, *patterns]
            if str(item.get("product_code") or "")
        }
    )
    scripts = [
        direction
        for product in products
        for direction in product.get("directions", []) or []
        if isinstance(direction, dict) and isinstance(direction.get("script"), dict)
    ]
    direction_errors = [
        error
        for product in products
        for error in product.get("direction_errors", []) or []
        if isinstance(error, dict)
    ]
    hard_failures = [
        direction
        for direction in scripts
        if not bool((direction.get("quality_result") or {}).get("valid", False))
    ]
    blueprint_model_counts = Counter(
        " / ".join(
            [
                str((direction.get("creative_blueprint") or {}).get("generation_provenance", {}).get("model") or "UNAVAILABLE"),
                str((direction.get("creative_blueprint") or {}).get("generation_provenance", {}).get("reasoning_effort") or "UNAVAILABLE"),
            ]
        )
        for direction in scripts
    )
    product_summary: Dict[str, Any] = {
        str(product.get("product_code") or ""): {
            "status": product.get("status", "UNKNOWN"),
            "selected_count": int(product.get("selected_count") or 0),
            "completed_count": int(product.get("completed_count") or len(product.get("directions", []) or [])),
            "failed_count": int(product.get("failed_count") or len(product.get("direction_errors", []) or [])),
            "completed_slots": [
                item.get("output_slot")
                for item in product.get("directions", []) or []
                if isinstance(item, dict) and item.get("script")
            ],
            "failed_slots": [
                item.get("output_slot")
                for item in product.get("direction_errors", []) or []
                if isinstance(item, dict)
            ],
        }
        for product in products
    }
    for code in product_codes:
        if code in product_summary:
            continue
        product_patterns = [row for row in patterns if str(row.get("product_code") or "") == code]
        product_summary[code] = {
            "status": "INCOMPLETE_ARTIFACTS",
            "selected_count": len(product_patterns),
            "completed_count": 0,
            "failed_count": 0,
            "completed_slots": [],
            "failed_slots": [],
        }
    rds_completed_keys = {
        direction_key
        for run in rds.get("runs", {}).values()
        for direction_key in run.get("completed_direction_keys", [])
    }
    rds_completed = len(rds_completed_keys)
    completed_script_count = len(scripts) if data is not None else rds_completed
    return {
        "verdict": "COMPLETED" if products and not direction_errors and len(scripts) > 0 else "BLOCKED_RETEST_REQUIRED",
        "stage0_result_available": data is not None,
        "requested_product_count": int((data or {}).get("requested_product_count") or len(product_codes)),
        "processed_product_count": len(product_codes),
        "product_summary": product_summary,
        "completed_script_count": completed_script_count,
        "direction_failure_count": len(direction_errors),
        "hard_qc_failure_count": len(hard_failures),
        "hard_qc_failure_rate": (len(hard_failures) / len(scripts)) if scripts else None,
        "blueprint_model_counts": dict(blueprint_model_counts),
        "creative_pattern_rows_raw": len(raw_patterns),
        "creative_directions_deduped": len(patterns),
        "retry_rows_ignored": ignored_retries,
        "per_product_diversity": {
            code: compute_intra_diversity(patterns, code) for code in product_codes
        },
        "cross_product_diversity": compute_cross_product(patterns),
        "voiceover": {
            "job_count": len(voiceovers),
            "ready_count": sum(
                1 for row in voiceovers if row.get("job_status") == "READY_FOR_TTS" or row.get("draft_status") == "READY_FOR_TTS"
            ),
            "failed_count": sum(
                1 for row in voiceovers if row.get("failure_code") or str(row.get("job_status") or "").upper().startswith("FAIL")
            ),
            "jobs": voiceovers,
        },
        "rds_stage_summary": rds,
    }


def render_report(metrics: Dict[str, Any]) -> str:
    lines = [
        "# V17 阶段0批次审计报告",
        "",
        f"- 判定：**{metrics['verdict']}**",
        f"- 产品：{metrics['processed_product_count']} / {metrics['requested_product_count']}",
        f"- 完整脚本：{metrics['completed_script_count']}",
        f"- 方向失败：{metrics['direction_failure_count']}",
        f"- 蓝图模型：{json.dumps(metrics['blueprint_model_counts'], ensure_ascii=False)}",
        f"- 创意登记：原始 {metrics['creative_pattern_rows_raw']}，去重后 {metrics['creative_directions_deduped']}，忽略重试 {metrics['retry_rows_ignored']}",
        "",
        "## 产品执行情况",
        "",
        "| 产品 | 状态 | 选中 | 完成 | 失败 | 完成槽位 | 失败槽位 |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for code, item in metrics["product_summary"].items():
        lines.append(
            f"| {code} | {item['status']} | {item['selected_count']} | {item['completed_count']} | {item['failed_count']} | "
            f"{', '.join(item['completed_slots']) or '-'} | {', '.join(item['failed_slots']) or '-'} |"
        )
    lines.extend(["", "## 多样性", ""])
    for code, item in metrics["per_product_diversity"].items():
        rate = "-" if item["pass_rate"] is None else f"{item['pass_rate']:.0%}"
        lines.append(f"- {code}：{item['verdict']}，两轴差异通过率 {rate}")
    cross = metrics["cross_product_diversity"]
    lines.append(
        f"- 跨产品：{cross['unique_signatures']} 个唯一组合，{cross['cross_product_duplicate_count']} 个跨不同产品的完全重复组合。"
    )
    lines.extend(
        [
            "",
            "## 口播与RDS佐证",
            "",
            f"- 口播任务：{metrics['voiceover']['job_count']}；READY：{metrics['voiceover']['ready_count']}；失败：{metrics['voiceover']['failed_count']}。",
            f"- RDS阶段记录：{metrics['rds_stage_summary'].get('status')}。",
            "",
            "> 机器规则通过仍只代表 MACHINE_SCREENED，不替代泰语母语审核和内容人工审核。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(SCRIPT_PATH.parent))
    parser.add_argument("--skip-rds", action="store_true")
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()

    data = load_stage0(output_dir)
    raw_patterns = load_creative_patterns(output_dir / "creative_pattern_stage0.sqlite3")
    voiceovers = load_voiceover_results(output_dir / "voiceover_stage0.sqlite3")
    rds = load_rds_stage_summary(data, raw_patterns, enabled=not args.skip_rds)
    metrics = build_metrics(data, raw_patterns, voiceovers, rds)

    metrics_path = output_dir / "batch_metrics.json"
    report_path = output_dir / "batch_execution_report.md"
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(render_report(metrics), encoding="utf-8")
    print(json.dumps({"metrics": str(metrics_path), "report": str(report_path), "verdict": metrics["verdict"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
