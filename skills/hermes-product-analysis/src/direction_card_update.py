#!/usr/bin/env python3
"""Direction-card update helpers for weekly signals and monthly stable reviews.

The upstream crawler owns raw collection and basic normalization.  This module
only separates stable direction-card definitions from weekly market signals so
Market Agent runs can update actions/briefs every week without rewriting the
taxonomy itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Iterable, List, Sequence


STABLE_FIELDS = [
    "direction_id",
    "direction_name",
    "direction_group",
    "core_user_scene",
    "core_value_point",
    "main_product_forms",
    "content_expression_route",
    "positive_product_signals",
    "negative_product_signals",
    "risk_flags",
    "default_product_angles",
    "default_scene_angles",
    "default_content_angles",
    "tag_dictionary_keywords",
    "mutual_exclusion_rules",
]

DYNAMIC_FIELDS = [
    "crawl_batch_id",
    "current_action",
    "task_type",
    "target_pool",
    "competition_structure",
    "competition_type",
    "new_product_entry_signal",
    "old_product_dominance",
    "content_supply_level",
    "creator_supply_level",
    "demand_level",
    "price_band_signal",
    "business_priority",
    "direction_execution_brief",
    "weekly_sample_pool_requirements",
    "last_updated_at",
]


@dataclass
class DirectionCardVersion:
    stable_version: str
    dynamic_version: str
    stable_fields_updated_at: str
    dynamic_fields_updated_at: str
    review_cycle: str = "monthly"
    signal_update_cycle: str = "weekly"

    def to_dict(self) -> Dict[str, str]:
        return {
            "stable_version": self.stable_version,
            "dynamic_version": self.dynamic_version,
            "stable_fields_updated_at": self.stable_fields_updated_at,
            "dynamic_fields_updated_at": self.dynamic_fields_updated_at,
            "review_cycle": self.review_cycle,
            "signal_update_cycle": self.signal_update_cycle,
        }


@dataclass
class MonthlyDirectionCardReview:
    market_id: str
    category_id: str
    review_period: List[str]
    stable_direction_updates: List[Dict[str, Any]] = field(default_factory=list)
    new_direction_candidates: List[Dict[str, Any]] = field(default_factory=list)
    merge_candidates: List[Dict[str, Any]] = field(default_factory=list)
    delete_or_deprecate_candidates: List[Dict[str, Any]] = field(default_factory=list)
    keyword_updates: List[Dict[str, Any]] = field(default_factory=list)
    threshold_update_suggestions: List[Dict[str, Any]] = field(default_factory=list)
    human_review_required: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "category_id": self.category_id,
            "review_period": list(self.review_period),
            "stable_direction_updates": list(self.stable_direction_updates),
            "new_direction_candidates": list(self.new_direction_candidates),
            "merge_candidates": list(self.merge_candidates),
            "delete_or_deprecate_candidates": list(self.delete_or_deprecate_candidates),
            "keyword_updates": list(self.keyword_updates),
            "threshold_update_suggestions": list(self.threshold_update_suggestions),
            "human_review_required": self.human_review_required,
        }


def split_direction_card_fields(card: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Split a direction card into monthly-stable and weekly-dynamic payloads."""
    stable = {field: card.get(field) for field in STABLE_FIELDS if field in card}
    dynamic = {field: card.get(field) for field in DYNAMIC_FIELDS if field in card}
    if "direction_action" in card and "current_action" not in dynamic:
        dynamic["current_action"] = card.get("direction_action")
    if "direction_execution_brief" in card and "direction_execution_brief" not in dynamic:
        dynamic["direction_execution_brief"] = card.get("direction_execution_brief")
    return {"stable_fields": stable, "dynamic_fields": dynamic}


def build_direction_card_version(
    market_id: str,
    category_id: str,
    batch_date: str,
    stable_version: str = "",
    stable_fields_updated_at: str = "",
) -> Dict[str, str]:
    """Build the version block consumed by reports, cards, and selection."""
    stable = stable_version or "{market}_{category}_v1.0".format(market=market_id, category=category_id)
    updated = stable_fields_updated_at or batch_date
    dynamic_version = "{market}_{category}_{week}".format(
        market=market_id,
        category=category_id,
        week=_iso_week(batch_date),
    )
    return DirectionCardVersion(
        stable_version=stable,
        dynamic_version=dynamic_version,
        stable_fields_updated_at=updated,
        dynamic_fields_updated_at=batch_date,
    ).to_dict()


def update_weekly_dynamic_fields(
    direction_cards: Iterable[Dict[str, Any]],
    crawl_batch_id: str,
    batch_date: str,
) -> List[Dict[str, Any]]:
    """Return direction cards with refreshed weekly dynamic metadata."""
    updated: List[Dict[str, Any]] = []
    for card in direction_cards:
        payload = dict(card)
        brief = dict(payload.get("direction_execution_brief") or {})
        action = (
            payload.get("current_action")
            or payload.get("direction_action")
            or brief.get("direction_action")
            or "observe"
        )
        payload.update(
            {
                "crawl_batch_id": crawl_batch_id,
                "current_action": action,
                "task_type": payload.get("task_type") or brief.get("task_type") or "",
                "target_pool": payload.get("target_pool") or brief.get("target_pool") or "",
                "weekly_sample_pool_requirements": (
                    payload.get("weekly_sample_pool_requirements")
                    or brief.get("sample_pool_requirements")
                    or []
                ),
                "last_updated_at": batch_date,
            }
        )
        updated.append(payload)
    return updated


def run_monthly_direction_card_review(
    market_id: str,
    category_id: str,
    recent_batches: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a conservative monthly review task from the latest four batches.

    This intentionally does not mutate stable fields.  It only emits a review
    payload that should wait for human approval before becoming a new stable
    version.
    """
    review_period = [str(item.get("crawl_batch_id") or item.get("batch_id") or "") for item in recent_batches if item]
    signal_counts: Dict[str, int] = {}
    sample_growth_candidates: List[Dict[str, Any]] = []
    new_direction_candidates: List[Dict[str, Any]] = []

    for batch in recent_batches:
        for direction in batch.get("directions", []) or []:
            direction_id = str(direction.get("direction_id") or direction.get("direction_name") or "")
            action = str(direction.get("current_action") or direction.get("direction_action") or "")
            if action in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test", "strong_signal_verify"}:
                signal_counts[direction_id] = signal_counts.get(direction_id, 0) + 1
            if str(direction.get("is_new_direction_candidate") or "").lower() in {"true", "1", "yes"}:
                new_direction_candidates.append(direction)
            if float(direction.get("sample_count_growth_ratio") or 0.0) >= 1.0:
                sample_growth_candidates.append(direction)

    stable_updates = [
        {
            "direction_id": direction_id,
            "reason": "最近4批中至少2批出现验证/核验级动作，建议人工复盘稳定字段。",
        }
        for direction_id, count in sorted(signal_counts.items())
        if count >= 2
    ]
    threshold_updates = [
        {
            "direction_id": str(item.get("direction_id") or item.get("direction_name") or ""),
            "reason": "样本数连续增长，建议复核阈值和低样本口径。",
        }
        for item in sample_growth_candidates[:10]
    ]
    return MonthlyDirectionCardReview(
        market_id=market_id,
        category_id=category_id,
        review_period=review_period,
        stable_direction_updates=stable_updates,
        new_direction_candidates=new_direction_candidates[:10],
        threshold_update_suggestions=threshold_updates,
        human_review_required=bool(stable_updates or new_direction_candidates or threshold_updates),
    ).to_dict()


def check_early_review_triggers(direction_history: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Identify early review tasks without directly changing stable fields."""
    by_direction: Dict[str, List[Dict[str, Any]]] = {}
    for item in direction_history:
        direction_id = str(item.get("direction_id") or item.get("direction_name") or "")
        if direction_id:
            by_direction.setdefault(direction_id, []).append(item)

    triggers: List[Dict[str, Any]] = []
    for direction_id, rows in by_direction.items():
        recent = rows[-2:]
        if len(recent) < 2:
            continue
        if all(str(row.get("new_product_entry_signal") or "") in {"medium", "high", "strong_new_entry", "moderate_new_entry"} for row in recent):
            triggers.append({"direction_id": direction_id, "trigger": "new_product_entry_signal_2w_upgrade"})
        if all(float(row.get("sample_count_growth_ratio") or 0.0) >= 1.0 for row in recent):
            triggers.append({"direction_id": direction_id, "trigger": "sample_count_growth_2w_over_100pct"})
        if all(str(row.get("direction_action") or row.get("current_action") or "") in {"prioritize_low_cost_test", "cautious_test", "study_top_not_enter", "hidden_small_test"} for row in recent):
            triggers.append({"direction_id": direction_id, "trigger": "action_upgraded_2w"})
    return triggers


def _iso_week(batch_date: str) -> str:
    try:
        parsed = date.fromisoformat(str(batch_date)[:10])
        year, week, _ = parsed.isocalendar()
        return "{year}W{week:02d}".format(year=year, week=week)
    except ValueError:
        return str(batch_date or "").replace("-", "")
