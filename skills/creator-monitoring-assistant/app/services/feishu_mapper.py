#!/usr/bin/env python3
"""飞书字段映射。"""

from __future__ import annotations

from typing import Dict, List, Optional

from app.utils.date_utils import current_timestamp_text


SYSTEM_OWNED_FIELDS = [
    "record_key",
    "达人名称",
    "国家",
    "店铺",
    "当前统计周",
    "本周GMV",
    "上周GMV",
    "GMV环比",
    "本周内容动作数",
    "上周内容动作数",
    "动作数环比",
    "本周单动作GMV",
    "单动作GMV环比",
    "本周退款率",
    "退款率变化",
    "近4周GMV",
    "当前主标签",
    "当前风险标签",
    "优先级",
    "核心原因",
    "本周建议动作",
    "最近更新时间",
]


def _as_float(value: object) -> float:
    return float(value or 0)


def _as_int(value: object) -> int:
    return int(value or 0)


def _as_percent(value: object) -> Optional[float]:
    if value is None:
        return None
    return round(float(value) * 100, 2)


def get_system_owned_fields() -> List[str]:
    return SYSTEM_OWNED_FIELDS[:]


def build_feishu_record_fields(
    row: Dict[str, object],
    primary_field_name: Optional[str] = None,
) -> Dict[str, object]:
    payload = {
        "record_key": row["record_key"],
        "达人名称": row["creator_name"],
        "国家": row["country"],
        "店铺": row.get("store") or "",
        "当前统计周": row["stat_week"],
        "本周GMV": _as_float(row.get("gmv")),
        "上周GMV": _as_float(row.get("prev_gmv")),
        "GMV环比": _as_percent(row.get("gmv_wow")),
        "本周内容动作数": _as_int(row.get("content_action_count")),
        "上周内容动作数": _as_int(row.get("prev_content_action_count")),
        "动作数环比": _as_percent(row.get("action_count_wow")),
        "本周单动作GMV": _as_float(row.get("gmv_per_action")),
        "单动作GMV环比": _as_percent(row.get("gmv_per_action_wow")),
        "本周退款率": _as_percent(row.get("refund_rate")),
        "退款率变化": _as_percent(row.get("refund_rate_wow")),
        "近4周GMV": _as_float(row.get("gmv_4w")),
        "当前主标签": row.get("primary_tag") or "",
        "当前风险标签": row.get("risk_tags") or "",
        "优先级": row.get("priority_level") or "",
        "核心原因": row.get("decision_reason") or "",
        "本周建议动作": row.get("next_action") or "",
        "最近更新时间": current_timestamp_text(),
    }
    if primary_field_name and primary_field_name not in payload:
        payload[primary_field_name] = row["record_key"]
    return payload
