#!/usr/bin/env python3
"""规则标签引擎。"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.config import Settings, get_settings
from app.db import Database
from app.services.action_mapper import generate_action_plan
from app.services.validity import is_valid_creator_for_monitoring
from app.utils.date_utils import sort_stat_weeks


PRIMARY_TAG_PRIORITY = [
    "volatility_alert",
    "core_maintain",
    "potential_new",
    "reactivate",
    "stop_loss",
    "new_observe",
]


def build_record_key(creator_key: str) -> str:
    return creator_key


def _sorted_metrics(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    week_map = {row["stat_week"]: row for row in rows}
    return [week_map[week] for week in sort_stat_weeks(week_map.keys())]


def _two_week_uptrend(history: List[Dict[str, object]], field: str) -> bool:
    if len(history) < 2:
        return False
    return float(history[-1].get(field) or 0) > float(history[-2].get(field) or 0)


def _threshold_ready(thresholds: Dict[str, float], key: str) -> bool:
    return float(thresholds.get(key) or 0) > 0


def _two_week_stop_loss(history: List[Dict[str, object]], thresholds: Dict[str, float], settings: Settings) -> bool:
    if len(history) < 2:
        return False
    last_two = history[-2:]
    if all(int(row.get("content_action_count") or 0) > 0 and float(row.get("gmv") or 0) <= 0 for row in last_two):
        return True

    action_floor = thresholds["gmv_per_action_50p"] * settings.stop_loss_low_efficiency_ratio
    if action_floor > 0 and all(
        int(row.get("content_action_count") or 0) > 0 and float(row.get("gmv_per_action") or 0) < action_floor
        for row in last_two
    ):
        return True

    sample_floor = thresholds["gmv_per_sample_50p"] * settings.stop_loss_low_efficiency_ratio
    if sample_floor > 0 and all(
        int(row.get("shipped_sample_count") or 0) > 0 and float(row.get("gmv_per_sample") or 0) < sample_floor
        for row in last_two
    ):
        return True

    return False


def evaluate_creator_tags(
    current_row: Dict[str, object],
    history_rows: List[Dict[str, object]],
    prior_results: List[Dict[str, object]],
    thresholds: Dict[str, float],
    settings: Optional[Settings] = None,
) -> Dict[str, object]:
    settings = settings or get_settings()
    matched_primary: List[Tuple[str, str]] = []
    risk_tags: List[str] = []

    weeks_active = int(current_row.get("weeks_active_lifetime") or 0)
    weeks_with_gmv = int(current_row.get("weeks_with_gmv_lifetime") or 0)
    current_gmv = float(current_row.get("gmv") or 0)
    current_refund_rate = float(current_row.get("refund_rate") or 0)
    current_commission_rate = float(current_row.get("commission_rate") or 0)
    current_gmv_per_action = float(current_row.get("gmv_per_action") or 0)
    current_gmv_per_sample = float(current_row.get("gmv_per_sample") or 0)
    avg_weekly_gmv_4w = float(current_row.get("avg_weekly_gmv_4w") or 0)
    avg_gmv_per_action_4w = float(current_row.get("avg_gmv_per_action_4w") or 0)
    avg_refund_rate_4w = float(current_row.get("avg_refund_rate_4w") or 0)
    lifetime_gmv = float(current_row.get("gmv_lifetime") or 0)
    content_action_count = int(current_row.get("content_action_count") or 0)
    shipped_sample_count = int(current_row.get("shipped_sample_count") or 0)
    has_result = current_gmv > 0
    action_benchmark_ready = _threshold_ready(thresholds, "gmv_per_action_50p")
    sample_benchmark_ready = _threshold_ready(thresholds, "gmv_per_sample_50p")

    was_core_before = any(result.get("primary_tag") == "core_maintain" for result in prior_results)
    lifetime_top = lifetime_gmv >= thresholds.get("gmv_lifetime_80p", 0)
    current_top = float(current_row.get("gmv_4w") or 0) >= thresholds.get("gmv_4w_80p", 0)

    if (current_top and current_refund_rate <= thresholds["refund_rate_75p"]) or (
        lifetime_top and weeks_with_gmv > 0
    ):
        matched_primary.append(("core_maintain", "近4周或生命周期表现进入高位，且退款风险可控"))

    if (was_core_before or lifetime_top) and weeks_active >= settings.min_weeks_for_volatility:
        if avg_weekly_gmv_4w > 0 and current_gmv < settings.volatility_drop_ratio * avg_weekly_gmv_4w:
            matched_primary.append(("volatility_alert", "本周 GMV 明显低于近4周周均水平"))
        elif avg_gmv_per_action_4w > 0 and current_gmv_per_action < settings.volatility_drop_ratio * avg_gmv_per_action_4w:
            matched_primary.append(("volatility_alert", "本周单动作 GMV 明显低于近4周均值"))
        elif avg_refund_rate_4w > 0 and current_refund_rate > settings.refund_risk_multiplier * avg_refund_rate_4w and current_refund_rate > 0:
            matched_primary.append(("volatility_alert", "本周退款率相较近4周均值明显抬升"))

    potential_new = False
    efficiency_pass = (
        action_benchmark_ready
        and has_result
        and content_action_count > 0
        and current_gmv_per_action >= thresholds["gmv_per_action_50p"]
    ) or (
        sample_benchmark_ready
        and has_result
        and shipped_sample_count > 0
        and current_gmv_per_sample >= thresholds["gmv_per_sample_50p"]
    )
    if (
        weeks_active <= settings.new_creator_max_weeks
        and efficiency_pass
        and current_refund_rate <= thresholds["refund_rate_75p"]
        and has_result
        and (
            _two_week_uptrend(history_rows, "gmv")
            or _two_week_uptrend(history_rows, "order_count")
            or current_gmv >= thresholds["gmv_50p"]
        )
    ):
        potential_new = True
        matched_primary.append(("potential_new", "合作时间短，且已有真实结果与效率证明"))

    if (
        any(float(row.get("gmv") or 0) > 0 for row in history_rows[:-1])
        and (content_action_count == 0 or not has_result)
        and lifetime_gmv >= thresholds["gmv_50p"]
    ):
        matched_primary.append(("reactivate", "历史有结果，但近期动作或产出下降，适合回访激活"))

    stop_loss = _two_week_stop_loss(history_rows, thresholds, settings)
    if stop_loss:
        matched_primary.append(("stop_loss", "连续两周投入效率偏低或有动作无结果"))

    if weeks_active <= settings.new_creator_max_weeks and not potential_new and not stop_loss:
        matched_primary.append(("new_observe", "合作时间短，先保持观察"))

    if current_refund_rate > thresholds["refund_rate_75p"]:
        risk_tags.append("high_refund_risk")
    elif len(history_rows) >= 2 and float(history_rows[-1].get("refund_rate") or 0) > float(history_rows[-2].get("refund_rate") or 0):
        risk_tags.append("high_refund_risk")

    if current_commission_rate > thresholds["commission_rate_75p"] and current_gmv < thresholds["gmv_75p"]:
        risk_tags.append("high_commission_risk")

    if int(current_row.get("shipped_sample_count") or 0) > 0 and sample_benchmark_ready:
        if current_gmv_per_sample < thresholds["gmv_per_sample_50p"]:
            risk_tags.append("low_roi_input")
        elif len(history_rows) >= 2 and int(history_rows[-1].get("shipped_sample_count") or 0) > int(history_rows[-2].get("shipped_sample_count") or 0) and current_gmv <= float(history_rows[-2].get("gmv") or 0):
            risk_tags.append("low_roi_input")

    unique_primary = []
    seen = set()
    for tag, reason in matched_primary:
        if tag not in seen:
            unique_primary.append((tag, reason))
            seen.add(tag)

    selected_tag = "new_observe"
    selected_reason = "数据不足，默认观察"
    for candidate in PRIMARY_TAG_PRIORITY:
        for tag, reason in unique_primary:
            if tag == candidate:
                selected_tag = tag
                selected_reason = reason
                break
        if selected_tag == candidate:
            break

    secondary_tags = [tag for tag, _ in unique_primary if tag != selected_tag]
    action_plan = generate_action_plan(selected_tag, risk_tags)
    decision_reason = f"{selected_reason}；{action_plan['decision_reason']}"

    return {
        "primary_tag": selected_tag,
        "secondary_tags": secondary_tags,
        "risk_tags": risk_tags,
        "priority_level": action_plan["priority_level"],
        "decision_reason": decision_reason,
        "next_action": action_plan["next_action"],
    }


def run_tag_engine(
    stat_week: str,
    thresholds: Dict[str, float],
    store: str = "",
    db: Optional[Database] = None,
) -> None:
    settings = get_settings()
    database = db or Database()
    database.execute(
        "DELETE FROM creator_monitoring_result WHERE stat_week = :stat_week AND store = :store",
        {"stat_week": stat_week, "store": store},
    )
    current_rows = database.fetchall(
        """
        SELECT mt.*, cm.creator_key, cm.creator_name, cm.owner
        FROM creator_weekly_metrics mt
        JOIN creator_master cm ON cm.id = mt.creator_id
        WHERE mt.stat_week = :stat_week
          AND mt.store = :store
        """,
        {"stat_week": stat_week, "store": store},
    )

    payloads = []
    for current_row in current_rows:
        if not is_valid_creator_for_monitoring(current_row, settings):
            continue
        history_rows = database.fetchall(
            """
            SELECT *
            FROM creator_weekly_metrics
            WHERE creator_id = :creator_id
            """,
            {"creator_id": current_row["creator_id"]},
        )
        history_rows = _sorted_metrics(history_rows)
        prior_results = database.fetchall(
            """
            SELECT *
            FROM creator_monitoring_result
            WHERE creator_id = :creator_id
            """,
            {"creator_id": current_row["creator_id"]},
        )

        evaluated = evaluate_creator_tags(current_row, history_rows, prior_results, thresholds, settings)
        record_key = build_record_key(str(current_row["creator_key"]))
        payloads.append(
            {
                "stat_week": stat_week,
                "creator_id": current_row["creator_id"],
                "store": current_row.get("store") or "",
                "record_key": record_key,
                "primary_tag": evaluated["primary_tag"],
                "secondary_tags": ",".join(evaluated["secondary_tags"]),
                "risk_tags": ",".join(evaluated["risk_tags"]),
                "priority_level": evaluated["priority_level"],
                "rule_version": settings.rule_version,
                "decision_reason": evaluated["decision_reason"],
                "next_action": evaluated["next_action"],
                "owner": current_row.get("owner"),
            }
        )

    database.executemany(
        """
        INSERT INTO creator_monitoring_result (
            stat_week, creator_id, store, record_key, primary_tag, secondary_tags,
            risk_tags, priority_level, rule_version, decision_reason,
            next_action, owner
        ) VALUES (
            :stat_week, :creator_id, :store, :record_key, :primary_tag, :secondary_tags,
            :risk_tags, :priority_level, :rule_version, :decision_reason,
            :next_action, :owner
        )
        """,
        payloads,
    )
