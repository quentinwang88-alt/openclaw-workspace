"""
周度动作决策服务 — V1.1 达人关系运营闭环。

根据达人层级、关系阶段、冷却期、历史回复等判断本周建议动作：
- 关系维护 / 商品邀约 / 轻跟进 / 暂缓 / 放弃 / 人工查看
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional


# ── 冷却期配置 ──

COOLDOWN_DAYS = {
    "relationship_maintenance": {"A 类": 21, "B 类": 35, "C 类": 60, "D 类": 999},
    "product_invitation": {"A 类": 7, "B 类": 7, "C 类": 30, "D 类": 999},
    "follow_up": {"A 类": 30, "B 类": 999, "C 类": 999, "D 类": 999},
}


def days_since(iso_timestamp: Optional[str]) -> Optional[int]:
    """从 ISO 时间戳算到今天的天数。"""
    if not iso_timestamp:
        return None
    try:
        ts = int(iso_timestamp)
        return (datetime.now(timezone.utc) - datetime.fromtimestamp(ts / 1000, tz=timezone.utc)).days
    except (ValueError, TypeError):
        try:
            dt = datetime.fromisoformat(str(iso_timestamp).replace("Z", "+00:00"))
            return (datetime.now(timezone.utc) - dt).days
        except (ValueError, TypeError):
            return None


def has_strong_product_match(fit_categories: Optional[List[str]]) -> bool:
    """判断达人是否有强商品匹配。"""
    if not fit_categories or "暂无" in fit_categories:
        return False
    return True


def should_send_maintenance(creator_tier: str, last_contact_at: Optional[str],
                             history_relation: str, relationship_stage: str,
                             activity: str) -> bool:
    """判断是否需要发送关系维护。"""
    if history_relation == "陌生":
        return False
    if relationship_stage in ("合作中", "冷却", "放弃"):
        return False
    if activity in ("低", "停更"):
        return False

    days = days_since(last_contact_at)
    if days is None:
        return True  # 从未联系过，需要维护

    min_days = COOLDOWN_DAYS["relationship_maintenance"].get(creator_tier, 999)
    return days >= min_days


def decide_weekly_action(
    creator_tier: str,
    relationship_stage: str,
    activity: str,
    history_relation: str,
    last_contact_at: Optional[str],
    last_contact_type: Optional[str],
    no_reply_count: int,
    next_contact_after: Optional[str],
    fit_categories: Optional[List[str]],
    ai_confidence: Optional[float] = None,
) -> Dict[str, Any]:
    """根据达人状态判断本周建议动作。

    Returns:
        {
            "action": "关系维护/商品邀约/轻跟进/暂缓/放弃/人工查看",
            "reason": "决策原因（中文，给运营看）",
            "message_purpose": "relationship_maintenance/product_invitation/follow_up"
        }
    """
    # P0 硬排除
    if relationship_stage == "放弃":
        return {"action": "放弃", "reason": "关系阶段已标记为放弃", "message_purpose": None}
    if creator_tier == "D 类":
        return {"action": "放弃", "reason": "达人层级为 D 类", "message_purpose": None}
    if activity == "停更":
        return {"action": "放弃", "reason": "达人已停更", "message_purpose": None}
    if no_reply_count >= 4:
        return {"action": "放弃", "reason": f"连续未回复 {no_reply_count} 次（>=4 次）", "message_purpose": None}
    if relationship_stage == "合作中":
        return {"action": "暂缓", "reason": "合作进行中，不主动打扰", "message_purpose": None}

    # 冷却期检查
    if next_contact_after:
        next_date = next_contact_after
        try:
            if isinstance(next_contact_after, str):
                next_date = datetime.fromisoformat(str(next_contact_after).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
        if days_since(str(next_date)) is not None and days_since(str(next_date)) < 0:
            return {"action": "暂缓", "reason": f"冷却期未到，下次可联系时间 {next_contact_after}", "message_purpose": None}

    # 轻跟进判断
    if last_contact_type == "商品邀约":
        days = days_since(last_contact_at)
        if days is not None and 3 <= days <= 7 and creator_tier == "A 类":
            return {"action": "轻跟进", "reason": f"上次商品邀约 {days} 天未回复，建议轻跟进", "message_purpose": "follow_up"}

    # 商品邀约判断
    if has_strong_product_match(fit_categories):
        if creator_tier in ("A 类", "B 类"):
            return {"action": "商品邀约", "reason": "有强商品匹配，内容适配度高", "message_purpose": "product_invitation"}

    # 关系维护判断
    if should_send_maintenance(creator_tier, last_contact_at, history_relation, relationship_stage, activity):
        days = days_since(last_contact_at)
        reason = f"距离上次联系已有 {days} 天" if days else "尚未联系过"
        return {"action": "关系维护", "reason": f"{reason}，建议维护关系", "message_purpose": "relationship_maintenance"}

    # 人工查看判断
    if creator_tier == "A 类" and (ai_confidence is None or ai_confidence < 0.55):
        return {"action": "人工查看", "reason": "A 类达人但 AI 置信度低，建议人工判断", "message_purpose": None}

    return {"action": "暂缓", "reason": "暂无合适触达时机", "message_purpose": None}


def get_cooldown_after_send(action: str, creator_tier: str) -> Optional[str]:
    """发送后返回下次可联系时间（ISO 格式）。"""
    days = {
        "关系维护": COOLDOWN_DAYS["relationship_maintenance"].get(creator_tier, 35),
        "商品邀约": COOLDOWN_DAYS["product_invitation"].get(creator_tier, 7),
        "轻跟进": COOLDOWN_DAYS["follow_up"].get(creator_tier, 30),
    }.get(action, 30)

    if days >= 999:
        return None

    next_date = datetime.now(timezone.utc) + timedelta(days=days)
    return next_date.isoformat()


def update_after_send(action: str, creator_tier: str) -> Dict[str, Any]:
    """发送后要回写的字段。"""
    now = datetime.now(timezone.utc)
    fields = {
        "上次联系时间": int(now.timestamp() * 1000),
        "上次联系类型": action,
        "处理状态": "已发送",
        "发送结果": "已发送",
        "关系阶段": "冷却",
    }
    next_contact = get_cooldown_after_send(action, creator_tier)
    if next_contact:
        fields["下次可联系时间"] = int(datetime.fromisoformat(next_contact).timestamp() * 1000)
    return fields


def update_after_reply(reply_status: str) -> Dict[str, Any]:
    """达人回复后要回写的字段。"""
    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp() * 1000)

    base = {
        "上次回复时间": timestamp,
        "最新回复状态": reply_status,
        "处理状态": "已回复",
    }

    if reply_status == "感兴趣":
        base.update({
            "关系阶段": "热",
            "连续未回复次数": 0,
            "下次可联系时间": timestamp,
        })
    elif reply_status == "普通回复":
        base.update({
            "关系阶段": "温",
            "连续未回复次数": 0,
            "下次可联系时间": int((now + timedelta(days=3)).timestamp() * 1000),
        })
    elif reply_status == "拒绝":
        base.update({
            "关系阶段": "冷却",
            "下次可联系时间": int((now + timedelta(days=60)).timestamp() * 1000),
        })
    elif reply_status == "未回复":
        # 连续未回复次数 +1 由调用方处理
        base["关系阶段"] = "冷却"
    elif reply_status == "已读未回":
        # 已读但未回复，态度不明，暂冷却
        base["关系阶段"] = "冷却"
        base["下次可联系时间"] = int((now + timedelta(days=14)).timestamp() * 1000)
    elif reply_status == "无效回复":
        # 回复但无实质内容（表情/单字等），按未回复处理
        base["关系阶段"] = "冷却"

    return base
