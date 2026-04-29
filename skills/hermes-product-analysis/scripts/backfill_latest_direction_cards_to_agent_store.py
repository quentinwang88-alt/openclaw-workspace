#!/usr/bin/env python3
"""Backfill latest direction-card data into the decoupled AgentDataStore tables."""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agent_data_store import AgentDataStore  # noqa: E402


DEFAULT_DB = ROOT / "artifacts" / "market_insight" / "market_insight.db"


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill latest direction cards into AgentDataStore tables.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(str(db_path))
    backup_path = ""
    if not args.no_backup:
        backup_path = str(db_path.with_suffix(".db.bak-{stamp}".format(stamp=time.strftime("%Y%m%d-%H%M%S"))))
        shutil.copy2(str(db_path), backup_path)

    latest_payloads = []
    with sqlite3.connect(str(db_path), timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        latest_runs = list(conn.execute("SELECT * FROM market_insight_runs WHERE is_latest = 1 ORDER BY country, category"))
        for run in latest_runs:
            cards = list(conn.execute("SELECT * FROM market_direction_cards WHERE run_id = ? ORDER BY direction_item_count DESC", (run["run_id"],)))
            latest_payloads.append((dict(run), [dict(card) for card in cards]))

    store = AgentDataStore(db_path)
    summaries = []
    for run, cards in latest_payloads:
            normalized_cards = [_normalize_card(row) for row in cards]
            report_payload = _loads(run["structured_report_json"], {})
            report_id = _report_id(run)
            business_summary = _business_summary(report_payload)
            store.save_market_direction_report(
                report_id=report_id,
                crawl_batch_id=str(run["batch_date"] or ""),
                market_id=str(run["country"] or ""),
                category_id=str(run["category"] or ""),
                report_version=str(report_payload.get("report_version") or "legacy_market_insight"),
                report_date=str(run["batch_date"] or ""),
                sample_count=int(run["total_product_count"] or 0),
                valid_sample_count=int(run["completed_product_count"] or 0),
                direction_count=int(run["direction_count"] or len(cards)),
                report_status=str(run["run_status"] or ""),
                business_summary_markdown=business_summary,
                full_report_markdown="",
                structured_json=report_payload,
            )
            direction_rows = [_direction_snapshot_payload(item) for item in normalized_cards]
            brief_rows = [item["direction_execution_brief"] for item in normalized_cards]
            store.save_market_direction_snapshots(
                report_id=report_id,
                crawl_batch_id=str(run["batch_date"] or ""),
                market_id=str(run["country"] or ""),
                category_id=str(run["category"] or ""),
                direction_rows=direction_rows,
            )
            brief_count = store.save_direction_execution_briefs(
                report_id=report_id,
                crawl_batch_id=str(run["batch_date"] or ""),
                market_id=str(run["country"] or ""),
                category_id=str(run["category"] or ""),
                briefs=brief_rows,
            )
            _update_card_briefs(db_path, normalized_cards)
            summaries.append(
                {
                    "market_id": run["country"],
                    "category_id": run["category"],
                    "report_id": report_id,
                    "direction_count": len(cards),
                    "brief_count": brief_count,
                }
            )
    print(json.dumps({"db_path": str(db_path), "backup_path": backup_path, "backfilled": summaries}, ensure_ascii=False, indent=2))
    return 0


def _normalize_card(row: sqlite3.Row) -> Dict[str, Any]:
    payload = dict(row)
    brief = _loads(row["direction_execution_brief_json"], {})
    direction_id = str(row["direction_canonical_key"] or row["direction_key"] or "").strip()
    if not direction_id:
        direction_id = "{market}__{category}__{name}".format(
            market=row["country"],
            category=row["category"],
            name=row["direction_name"],
        )
    brief["direction_id"] = direction_id
    brief["direction_name"] = str(brief.get("direction_name") or row["direction_name"] or "")
    brief["direction_action"] = str(brief.get("direction_action") or brief.get("primary_action") or row["decision_action"] or "observe")
    brief["task_type"] = str(brief.get("task_type") or _task_type_for_action(brief["direction_action"]))
    brief["target_pool"] = str(brief.get("target_pool") or _target_pool_for_action(brief["direction_action"]))
    brief["brief_source"] = str(brief.get("brief_source") or "generated")
    brief["brief_confidence"] = str(brief.get("brief_confidence") or "medium")
    brief["product_selection_requirements"] = _non_empty_list(
        brief.get("product_selection_requirements"),
        _fallback_requirements(row, brief),
    )
    brief["positive_signals"] = _non_empty_list(
        brief.get("positive_signals"),
        _fallback_positive_signals(row),
    )
    brief["negative_signals"] = _non_empty_list(
        brief.get("negative_signals"),
        _fallback_negative_signals(row, brief),
    )
    brief["sample_pool_requirements"] = _non_empty_list(
        brief.get("sample_pool_requirements"),
        _fallback_sample_pool_requirements(brief),
    )
    brief["content_requirements"] = _non_empty_list(
        brief.get("content_requirements"),
        _fallback_content_requirements(brief),
    )
    brief.setdefault("upgrade_condition", [])
    brief.setdefault("stop_condition", [])
    payload["direction_execution_brief"] = brief
    return payload


def _update_card_briefs(db_path: Path, cards: List[Dict[str, Any]]) -> None:
    with sqlite3.connect(str(db_path), timeout=30) as conn:
        for card in cards:
            conn.execute(
                "UPDATE market_direction_cards SET direction_execution_brief_json = ?, decision_action = ? WHERE id = ?",
                (
                    json.dumps(card["direction_execution_brief"], ensure_ascii=False),
                    str(card["direction_execution_brief"].get("direction_action") or ""),
                    card["id"],
                ),
            )


def _direction_snapshot_payload(card: Dict[str, Any]) -> Dict[str, Any]:
    brief = card["direction_execution_brief"]
    return {
        "direction_id": brief.get("direction_id"),
        "direction_name": card.get("direction_name"),
        "direction_group": card.get("direction_family"),
        "sample_count": card.get("direction_item_count"),
        "direction_action": brief.get("direction_action"),
        "task_type": brief.get("task_type"),
        "target_pool": brief.get("target_pool"),
        "competition_type": card.get("direction_tier"),
        "competition_structure": {
            "video_density": card.get("direction_video_density_avg"),
            "creator_density": card.get("direction_creator_density_avg"),
            "content_efficiency_source": card.get("content_efficiency_source"),
        },
        "new_product_signal": "",
        "old_product_dominance": "",
        "business_priority": card.get("priority_level") or "",
    }


def _fallback_requirements(row: sqlite3.Row, brief: Dict[str, Any]) -> List[str]:
    action = str(brief.get("direction_action") or "")
    if action in {"prioritize_low_cost_test", "cautious_test"}:
        return ["优先选择近90天新品或相似仿形款", "必须有明确产品记忆点或内容钩子", "价格和毛利满足小预算测试"]
    if action == "study_top_not_enter":
        return ["进入头部样本拆解", "区分可复制点和不可复制点", "不直接按方向铺货"]
    if action == "avoid":
        return ["不进入测款池", "仅保留必要样本用于复核"]
    return ["按方向匹配、产品本体、内容潜力和差异化判断"]


def _fallback_positive_signals(row: sqlite3.Row) -> List[str]:
    items: List[str] = []
    for column in ("top_value_points_json", "core_elements_json", "scene_tags_json"):
        items.extend(_loads(row[column], []))
    return [str(item) for item in items[:6] if str(item or "").strip()] or ["方向成交/样本信号可供参考"]


def _fallback_negative_signals(row: sqlite3.Row, brief: Dict[str, Any]) -> List[str]:
    action = str(brief.get("direction_action") or "")
    if action == "avoid":
        return ["当前不适合投入测款资源", "供给或竞争风险偏高"]
    if action == "study_top_not_enter":
        return ["普通铺货风险高", "头部胜出机制未拆清前不直接测款"]
    if str(row["direction_tier"] or "") == "crowded":
        return ["方向拥挤", "需要避免同质化跟款"]
    return ["缺少明确差异化时不进入高优先级"]


def _fallback_sample_pool_requirements(brief: Dict[str, Any]) -> List[str]:
    task = str(brief.get("task_type") or "")
    if task == "low_cost_test":
        return ["Top10头部样本", "近90天代表新品", "相似仿形候选"]
    if task in {"head_dissection", "new_winner_deep_dive", "old_product_replace"}:
        return ["头部样本", "代表新品/替代候选", "可复制点与不可复制点"]
    if task == "category_review":
        return ["other高销量样本", "other新品样本", "标题/视觉聚类样本"]
    return ["Top样本", "代表新品"]


def _fallback_content_requirements(brief: Dict[str, Any]) -> List[str]:
    task = str(brief.get("task_type") or "")
    if task == "low_cost_test":
        return ["每款3-5条内容", "7-10天小预算测试", "记录点击/加购/成交"]
    if task == "observe":
        return ["当前不占用内容资源，仅跟踪下一批信号"]
    if task == "category_review":
        return ["不生成内容，优先完成归类复核"]
    return ["拆解可复用内容钩子", "判断是否适合进入内容基线测试"]


def _task_type_for_action(action: str) -> str:
    return {
        "prioritize_low_cost_test": "low_cost_test",
        "cautious_test": "low_cost_test",
        "study_top_not_enter": "head_dissection",
        "strong_signal_verify": "signal_verify",
        "hidden_candidate": "hidden_candidate",
        "hidden_small_test": "low_cost_test",
        "observe": "observe",
        "avoid": "avoid",
    }.get(action, "observe")


def _target_pool_for_action(action: str) -> str:
    return {
        "prioritize_low_cost_test": "test_product_pool",
        "cautious_test": "test_product_pool",
        "study_top_not_enter": "head_reference_pool",
        "strong_signal_verify": "manual_review_pool",
        "hidden_candidate": "observe_pool",
        "hidden_small_test": "manual_review_pool",
        "observe": "observe_pool",
        "avoid": "eliminate",
    }.get(action, "observe_pool")


def _non_empty_list(value: Any, fallback: List[str]) -> List[str]:
    values = _loads(value, value if isinstance(value, list) else [])
    values = [str(item or "").strip() for item in values if str(item or "").strip()]
    return values or fallback


def _report_id(run: sqlite3.Row) -> str:
    return "{market}__{category}__{batch}__latest".format(
        market=run["country"],
        category=run["category"],
        batch=run["batch_date"],
    )


def _business_summary(report_payload: Dict[str, Any]) -> str:
    summary = report_payload.get("business_summary_markdown") or report_payload.get("business_summary") or ""
    if isinstance(summary, list):
        return "\n".join(str(item) for item in summary)
    return str(summary or "")


def _loads(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except ValueError:
        return default


if __name__ == "__main__":
    raise SystemExit(main())
