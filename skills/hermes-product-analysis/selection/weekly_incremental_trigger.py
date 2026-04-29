#!/usr/bin/env python3
"""Weekly Selection Agent trigger decisions.

This module is deliberately side-effect-light: it decides whether a batch
should run, wait, rerun, or be skipped.  The actual cron/Hermes scheduler can
call these helpers and then invoke the existing Market/Selection pipelines.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from src.agent_data_store import SELECTION_AGENT, AgentDataStore
from src.standardized_snapshot import StandardizedProductSnapshot


RETRY_ATTEMPT_LABELS = {
    0: "Tuesday 10:00",
    1: "Tuesday 12:00",
    2: "Tuesday 15:00",
    3: "Wednesday 10:00",
}


@dataclass(frozen=True)
class BatchKey:
    crawl_batch_id: str
    market_id: str
    category_id: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "crawl_batch_id": self.crawl_batch_id,
            "market_id": self.market_id,
            "category_id": self.category_id,
        }


@dataclass
class SelectionIncrementDecision:
    should_run: bool
    status: str
    reason: str
    batch_key: BatchKey
    batch_data_hash: str = ""
    rerun_reason: str = ""
    risk_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "should_run": self.should_run,
            "status": self.status,
            "reason": self.reason,
            "batch_key": self.batch_key.to_dict(),
            "batch_data_hash": self.batch_data_hash,
            "rerun_reason": self.rerun_reason,
            "risk_flags": list(self.risk_flags),
        }


@dataclass
class MarketBriefReadiness:
    ready: bool
    status: str
    reason: str
    report_id: str = ""
    brief_count: int = 0
    use_previous_batch: bool = False
    allow_fallback: bool = False
    risk_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ready": self.ready,
            "status": self.status,
            "reason": self.reason,
            "report_id": self.report_id,
            "brief_count": self.brief_count,
            "use_previous_batch": self.use_previous_batch,
            "allow_fallback": self.allow_fallback,
            "risk_flags": list(self.risk_flags),
        }


def group_ready_snapshots_by_batch(snapshots: Iterable[StandardizedProductSnapshot]) -> Dict[BatchKey, List[StandardizedProductSnapshot]]:
    grouped: Dict[BatchKey, List[StandardizedProductSnapshot]] = {}
    for snapshot in snapshots:
        if not snapshot.ready_for_agents:
            continue
        key = BatchKey(snapshot.crawl_batch_id, snapshot.market_id, snapshot.category_id)
        grouped.setdefault(key, []).append(snapshot)
    return grouped


def compute_batch_data_hash(snapshots: Iterable[StandardizedProductSnapshot]) -> str:
    """Stable hash for detecting same-batch standardized-data updates."""
    rows = []
    for item in snapshots:
        rows.append(
            {
                "product_snapshot_id": item.product_snapshot_id,
                "product_id": item.product_id,
                "main_image_url": item.main_image_url,
                "price_rmb": item.price_rmb,
                "sales_7d": item.sales_7d,
                "sales_30d": item.sales_30d,
                "product_age_days": item.product_age_days,
                "fastmoss_url": item.fastmoss_url,
                "data_quality_flags": item.data_quality_flags,
            }
        )
    payload = json.dumps(sorted(rows, key=lambda row: row["product_snapshot_id"]), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def check_selection_increment(
    store: AgentDataStore,
    batch_key: BatchKey,
    batch_data_hash: str = "",
) -> SelectionIncrementDecision:
    existing = store.get_agent_run(
        agent_name=SELECTION_AGENT,
        crawl_batch_id=batch_key.crawl_batch_id,
        market_id=batch_key.market_id,
        category_id=batch_key.category_id,
    )
    if not existing:
        return SelectionIncrementDecision(
            should_run=True,
            status="new_batch",
            reason="该 crawl_batch_id 尚未被 Selection Agent 消费。",
            batch_key=batch_key,
            batch_data_hash=batch_data_hash,
        )
    if existing.get("status") in {"failed", "partial", "waiting_for_manual_check"}:
        return SelectionIncrementDecision(
            should_run=True,
            status="rerun_required",
            reason="上一轮选品运行未成功，可重跑。",
            batch_key=batch_key,
            batch_data_hash=batch_data_hash,
            rerun_reason="previous_run_not_success",
        )
    old_hash = str(existing.get("batch_data_hash") or "")
    if batch_data_hash and old_hash and batch_data_hash != old_hash:
        return SelectionIncrementDecision(
            should_run=True,
            status="rerun_required",
            reason="同批次标准化数据 hash 已变化，需要重跑选品。",
            batch_key=batch_key,
            batch_data_hash=batch_data_hash,
            rerun_reason="standardized_data_changed",
        )
    if str(existing.get("status") or "") == "success":
        return SelectionIncrementDecision(
            should_run=False,
            status="skip_already_success",
            reason="同批次已成功运行且数据 hash 未变化。",
            batch_key=batch_key,
            batch_data_hash=batch_data_hash,
        )
    return SelectionIncrementDecision(
        should_run=True,
        status="rerun_required",
        reason="已有运行记录但状态不是 success，按可重跑处理。",
        batch_key=batch_key,
        batch_data_hash=batch_data_hash,
        rerun_reason="non_success_status",
    )


def check_market_brief_ready(
    store: AgentDataStore,
    batch_key: BatchKey,
    retry_attempt: int = 0,
    max_retry_count: int = 3,
) -> MarketBriefReadiness:
    report = store.latest_consumable_market_report(
        market_id=batch_key.market_id,
        category_id=batch_key.category_id,
        crawl_batch_id=batch_key.crawl_batch_id,
    )
    if report and report.get("crawl_batch_id") == batch_key.crawl_batch_id:
        brief_count = store.count_ready_briefs(batch_key.market_id, batch_key.category_id, batch_key.crawl_batch_id)
        if brief_count > 0:
            return MarketBriefReadiness(
                ready=True,
                status="ready",
                reason="同批次市场报告和 direction_execution_brief 已可消费。",
                report_id=str(report.get("report_id") or ""),
                brief_count=brief_count,
            )
    if retry_attempt < max_retry_count:
        return MarketBriefReadiness(
            ready=False,
            status="waiting_for_market_brief",
            reason="市场 brief 尚未 ready，等待下一次重试：{slot}".format(
                slot=RETRY_ATTEMPT_LABELS.get(retry_attempt + 1, "next retry")
            ),
        )
    previous = store.latest_consumable_market_report(batch_key.market_id, batch_key.category_id)
    if previous:
        return MarketBriefReadiness(
            ready=True,
            status="ready_with_previous_brief",
            reason="同批次 brief 缺失，已超过重试次数，允许使用最近一份可消费 brief。",
            report_id=str(previous.get("report_id") or ""),
            brief_count=store.count_ready_briefs(batch_key.market_id, batch_key.category_id, str(previous.get("crawl_batch_id") or "")),
            use_previous_batch=True,
            risk_flags=["brief_from_previous_batch"],
        )
    return MarketBriefReadiness(
        ready=True,
        status="ready_with_fallback_brief",
        reason="同类目无可消费 brief，已超过重试次数，允许 fallback brief 高风险运行。",
        allow_fallback=True,
        risk_flags=["brief_auto_generated"],
    )


def plan_selection_runs(
    store: AgentDataStore,
    snapshots: Iterable[StandardizedProductSnapshot],
    retry_attempt: int = 0,
) -> List[Dict[str, Any]]:
    plans: List[Dict[str, Any]] = []
    for batch_key, rows in group_ready_snapshots_by_batch(snapshots).items():
        batch_hash = compute_batch_data_hash(rows)
        increment = check_selection_increment(store, batch_key, batch_hash)
        if not increment.should_run:
            plans.append({"batch": batch_key.to_dict(), "decision": increment.to_dict()})
            continue
        brief = check_market_brief_ready(store, batch_key, retry_attempt=retry_attempt)
        plans.append(
            {
                "batch": batch_key.to_dict(),
                "decision": increment.to_dict(),
                "brief_readiness": brief.to_dict(),
                "trigger_selection_agent_run": bool(increment.should_run and brief.ready),
            }
        )
    return plans
