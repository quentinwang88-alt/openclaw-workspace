#!/usr/bin/env python3
"""标签到动作建议映射。"""

from __future__ import annotations

from typing import Dict, List


ACTION_PLAN_MAP = {
    "core_maintain": {
        "priority_level": "high",
        "decision_reason": "近4周结果稳定，属于核心贡献达人",
        "next_action": "继续维系，纳入下周重点跟进名单",
    },
    "volatility_alert": {
        "priority_level": "high",
        "decision_reason": "历史表现强，但近期结果或效率明显下滑",
        "next_action": "优先复盘近两周合作表现，暂不加大投入",
    },
    "potential_new": {
        "priority_level": "medium_high",
        "decision_reason": "合作时间短，但当前效率表现较好",
        "next_action": "继续测试 1-2 个相似方向，保持小批量观察",
    },
    "new_observe": {
        "priority_level": "medium",
        "decision_reason": "数据仍不足以稳定判断",
        "next_action": "继续观察，不做强结论",
    },
    "reactivate": {
        "priority_level": "medium",
        "decision_reason": "历史合作有效，但近期活跃度下降",
        "next_action": "尝试返场触达，优先回访历史有效方向",
    },
    "stop_loss": {
        "priority_level": "low",
        "decision_reason": "近期持续有动作但结果弱，投入回报不理想",
        "next_action": "降低优先级，暂停追加资源",
    },
}


def generate_action_plan(primary_tag: str, risk_tags: List[str]) -> Dict[str, str]:
    plan = ACTION_PLAN_MAP.get(primary_tag, ACTION_PLAN_MAP["new_observe"]).copy()
    if risk_tags:
        plan["decision_reason"] = f"{plan['decision_reason']}；风险：{', '.join(risk_tags)}"
    return plan

