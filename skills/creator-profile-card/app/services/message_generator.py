"""
私信话术生成器 — V1.1 达人关系运营闭环。

V1.1 核心改进：
- 支持三种 message_purpose：relationship_maintenance / product_invitation / follow_up
- product_invitation：先生成内容机会卡，再由机会卡生成话术
- relationship_maintenance：不推品、不索取、不逼回复
- follow_up：轻提醒、不催促、给拒绝空间
- 质量评分低于 8 分自动重写，最多重写 2 次
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..config import PROMPT_VERSION, LLM_MODEL, LOG_DB_PATH
from .llm_client import get_llm_client
from .validator import check_message_risk, calculate_message_quality, calculate_sample_nurture_quality, get_max_rewrite_count
from .content_opportunity_generator import generate_content_opportunity
from ..prompts.message_generation import MSG_SYSTEM_PROMPT, MSG_USER_PROMPT_TEMPLATE
from ..prompts.relationship_maintenance import MAINTENANCE_SYSTEM_PROMPT, MAINTENANCE_USER_PROMPT_TEMPLATE
from ..prompts.follow_up_message import FOLLOWUP_SYSTEM_PROMPT, FOLLOWUP_USER_PROMPT_TEMPLATE
from ..prompts.sample_pre_approval import SAMPLE_NURTURE_SYSTEM_PROMPT, SAMPLE_NURTURE_USER_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


def _init_log_db() -> None:
    """初始化私信日志数据库。"""
    db_path = Path(LOG_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS creator_message_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creator_url TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT '',
            target_language TEXT NOT NULL DEFAULT '',
            history_relation TEXT NOT NULL DEFAULT '',
            product_name TEXT DEFAULT '',
            product_category TEXT DEFAULT '',
            selling_points_json TEXT DEFAULT '[]',
            shooting_scenarios_json TEXT DEFAULT '[]',
            price_tier TEXT DEFAULT '',
            sample_available TEXT DEFAULT '',
            commission_info TEXT DEFAULT '',
            support_info TEXT DEFAULT '',
            profile_card_json TEXT DEFAULT '{}',
            content_opportunity_json TEXT DEFAULT '{}',
            message_cn_for_operator TEXT DEFAULT '',
            message_local TEXT DEFAULT '',
            why_this_message TEXT DEFAULT '',
            quality_score INTEGER DEFAULT 0,
            quality_breakdown_json TEXT DEFAULT '{}',
            rewrite_count INTEGER DEFAULT 0,
            risk_check_json TEXT DEFAULT '{}',
            raw_output_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    # V1.1 新增字段兼容：如果表已存在但没有这些列，尝试添加
    try:
        conn.execute("ALTER TABLE creator_message_logs ADD COLUMN content_opportunity_json TEXT DEFAULT '{}'")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE creator_message_logs ADD COLUMN quality_score INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE creator_message_logs ADD COLUMN quality_breakdown_json TEXT DEFAULT '{}'")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE creator_message_logs ADD COLUMN rewrite_count INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()


def generate_message(
    creator_url: str,
    market: str,
    target_language: str,
    history_relation: str,
    product_name: str = "",
    product_category: str = "",
    profile_card: Optional[Dict[str, Any]] = None,
    product_info: Optional[Dict[str, Any]] = None,
    cover_collage_images: Optional[List[str]] = None,
    cover_count: int = 20,
    recent_video_meta_text: str = "",
    skip_opportunity: bool = False,
    message_purpose: str = "product_invitation",
    relationship_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """生成达人私信草稿 — V1.1 达人关系运营闭环。

    支持三种 message_purpose：
    - product_invitation: 内容机会型商品邀约（需内容机会卡）
    - relationship_maintenance: 非带货关系维护
    - follow_up: 轻跟进

    Args:
        creator_url: 达人链接。
        market: 市场。
        target_language: 目标语言。
        history_relation: 历史关系。
        product_name: 商品名称（关系维护时可为空）。
        product_category: 商品类目（关系维护时可为空）。
        profile_card: 达人画像卡 dict。
        product_info: 商品详细信息 dict。
        cover_collage_images: 封面拼图路径列表。
        cover_count: 封面数量。
        recent_video_meta_text: 视频文字信息（可选）。
        skip_opportunity: 跳过内容机会卡生成（用于重写场景）。
        message_purpose: 触达目的（product_invitation/relationship_maintenance/follow_up）。
        relationship_context: 关系上下文 dict，包含：
            - creator_tier: 达人层级
            - relationship_stage: 关系阶段
            - days_since_last_contact: 距上次联系天数
            - last_contact_type: 上次联系类型
            - last_message_summary: 上次话术概要
            - last_contact_at: 上次联系时间

    Returns:
        {
            "message_cn_for_operator": "...",
            "message_local": "...",
            ... (同 V1.1 原结构)
            "message_purpose": "...",
        }
    """
    _init_log_db()

    profile_card = profile_card or {}
    product_info = product_info or {}
    cover_collage_images = cover_collage_images or []
    relationship_context = relationship_context or {}

    # ── 根据 purpose 分流 ──
    if message_purpose == "relationship_maintenance":
        return _generate_maintenance_message(
            creator_url=creator_url, market=market, target_language=target_language,
            history_relation=history_relation, profile_card=profile_card,
            relationship_context=relationship_context,
        )

    if message_purpose == "follow_up":
        return _generate_follow_up_message(
            creator_url=creator_url, market=market, target_language=target_language,
            product_name=product_name, product_category=product_category,
            relationship_context=relationship_context,
        )

    if message_purpose == "sample_pre_approval_nurture":
        return _generate_sample_nurture_message(
            creator_url=creator_url, market=market, target_language=target_language,
            history_relation=history_relation, profile_card=profile_card,
            product_info=product_info, relationship_context=relationship_context,
        )

    # ── product_invitation（原 V1.1 流程） ──
    selling_points = product_info.get("selling_points", [])
    if isinstance(selling_points, str):
        selling_points = [s.strip() for s in selling_points.split(",") if s.strip()]
    shooting_scenarios = product_info.get("shooting_scenarios", [])
    if isinstance(shooting_scenarios, str):
        shooting_scenarios = [s.strip() for s in shooting_scenarios.split(",") if s.strip()]

    price_tier = product_info.get("price_tier", "")
    sample_available = product_info.get("sample_available", "")
    commission_info = product_info.get("commission_info", "")
    support_info = product_info.get("support_info", "")

    # Step 1: 内容机会卡
    content_opportunity = {}
    if not skip_opportunity:
        try:
            content_opportunity = generate_content_opportunity(
                creator_url=creator_url, market=market, profile_card=profile_card,
                product_info=product_info, cover_collage_images=cover_collage_images,
                cover_count=cover_count, recent_video_meta_text=recent_video_meta_text,
            )
        except Exception as e:
            logger.warning("内容机会卡生成失败: %s", e)

    private = content_opportunity.get("private", {})
    if private.get("unsuited"):
        return {
            "message_cn_for_operator": "", "message_local": "", "why_this_message": "",
            "quality_score": 0, "quality_breakdown": {}, "rewrite_count": 0,
            "risk_check": {}, "content_opportunity": content_opportunity,
            "log_id": -1, "message_purpose": message_purpose,
            "error": f"达人内容与商品不适配: {private.get('unsuited_reason', '')}",
        }

    # Step 2: 话术 + 质量评分
    result = _generate_message_with_rewrite(
        creator_url=creator_url, market=market, target_language=target_language,
        history_relation=history_relation, profile_card=profile_card,
        product_info=product_info, content_opportunity=content_opportunity,
        product_name=product_name, product_category=product_category,
        selling_points=selling_points, shooting_scenarios=shooting_scenarios,
        price_tier=price_tier, sample_available=sample_available,
        commission_info=commission_info, support_info=support_info,
    )

    conn = sqlite3.connect(LOG_DB_PATH)
    conn.execute(
        """INSERT INTO creator_message_logs (
            creator_url, market, target_language, history_relation,
            product_name, product_category,
            selling_points_json, shooting_scenarios_json,
            price_tier, sample_available, commission_info, support_info,
            profile_card_json, content_opportunity_json,
            message_cn_for_operator, message_local, why_this_message,
            quality_score, quality_breakdown_json, rewrite_count,
            risk_check_json, raw_output_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (creator_url, market, target_language, history_relation,
         product_name, product_category,
         json.dumps(selling_points, ensure_ascii=False),
         json.dumps(shooting_scenarios, ensure_ascii=False),
         price_tier, sample_available, commission_info, support_info,
         json.dumps(profile_card, ensure_ascii=False),
         json.dumps(content_opportunity, ensure_ascii=False),
         result.get("message_cn_for_operator", ""),
         result.get("message_local", ""),
         result.get("why_this_message", ""),
         result.get("quality_score", 0),
         json.dumps(result.get("quality_breakdown", {}), ensure_ascii=False),
         result.get("rewrite_count", 0),
         json.dumps(result.get("risk_check", {}), ensure_ascii=False),
         json.dumps(result.get("raw_output", {}), ensure_ascii=False),
         datetime.now(timezone.utc).isoformat()),
    )
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    result["log_id"] = log_id
    result["content_opportunity"] = content_opportunity
    result["message_purpose"] = message_purpose
    return result


def _generate_maintenance_message(
    creator_url: str, market: str, target_language: str,
    history_relation: str, profile_card: Dict[str, Any],
    relationship_context: Dict[str, Any],
) -> Dict[str, Any]:
    """生成非带货关系维护话术。"""
    writable = profile_card.get("writable_fields", {})
    user_prompt = MAINTENANCE_USER_PROMPT_TEMPLATE.format(
        market=market, target_language=target_language,
        history_relation=history_relation,
        creator_url=creator_url,
        activity=writable.get("活跃度", ""),
        content_type=writable.get("内容类型", ""),
        visual_style=writable.get("画面风格", ""),
        fit_categories=", ".join(writable.get("适配类目", [])),
        creator_tier=relationship_context.get("creator_tier", ""),
        relationship_stage=relationship_context.get("relationship_stage", ""),
        days_since_last_contact=relationship_context.get("days_since_last_contact", "未知"),
    )
    llm = get_llm_client()
    try:
        raw = llm.call_json(prompt=user_prompt, system_prompt=MAINTENANCE_SYSTEM_PROMPT)
    except Exception as e:
        logger.error("维护话术生成失败: %s", e)
        return {"message_cn_for_operator": "", "message_local": "", "error": str(e), "log_id": -1, "message_purpose": "relationship_maintenance"}

    msg_cn = raw.get("message_cn_for_operator", "")
    msg_local = raw.get("message_local", "")
    risk = check_message_risk(msg_cn, msg_local)
    # 额外检查是否有商品词
    product_words = ["发饰", "耳饰", "项链", "开衫", "上衣", "套装", "佣金", "寄样", "带货"]
    for w in product_words:
        if w in msg_cn or w in msg_local:
            risk["has_product_mention"] = True

    # 写日志
    conn = sqlite3.connect(LOG_DB_PATH)
    conn.execute(
        """INSERT INTO creator_message_logs (
            creator_url, market, target_language, history_relation,
            product_name, product_category, profile_card_json,
            message_cn_for_operator, message_local, why_this_message,
            quality_score, risk_check_json, raw_output_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (creator_url, market, target_language, history_relation,
         "", "", json.dumps(profile_card, ensure_ascii=False),
         msg_cn, msg_local, raw.get("why_this_message", ""),
         0, json.dumps(risk, ensure_ascii=False),
         json.dumps(raw, ensure_ascii=False),
         datetime.now(timezone.utc).isoformat()),
    )
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    return {
        "message_cn_for_operator": msg_cn, "message_local": msg_local,
        "why_this_message": raw.get("why_this_message", ""),
        "risk_check": risk, "raw_output": raw,
        "log_id": log_id, "message_purpose": "relationship_maintenance",
    }


def _generate_follow_up_message(
    creator_url: str, market: str, target_language: str,
    product_name: str, product_category: str,
    relationship_context: Dict[str, Any],
) -> Dict[str, Any]:
    """生成轻跟进话术。"""
    user_prompt = FOLLOWUP_USER_PROMPT_TEMPLATE.format(
        market=market, target_language=target_language,
        creator_tier=relationship_context.get("creator_tier", ""),
        last_contact_type=relationship_context.get("last_contact_type", ""),
        last_contact_at=relationship_context.get("last_contact_at", ""),
        days_since_last_contact=relationship_context.get("days_since_last_contact", "未知"),
        last_message_summary=relationship_context.get("last_message_summary", ""),
        product_name=product_name, product_category=product_category,
    )
    llm = get_llm_client()
    try:
        raw = llm.call_json(prompt=user_prompt, system_prompt=FOLLOWUP_SYSTEM_PROMPT)
    except Exception as e:
        logger.error("跟进话术生成失败: %s", e)
        return {"message_cn_for_operator": "", "message_local": "", "error": str(e), "log_id": -1, "message_purpose": "follow_up"}

    msg_cn = raw.get("message_cn_for_operator", "")
    msg_local = raw.get("message_local", "")
    risk = check_message_risk(msg_cn, msg_local)

    conn = sqlite3.connect(LOG_DB_PATH)
    conn.execute(
        """INSERT INTO creator_message_logs (
            creator_url, market, target_language, history_relation,
            product_name, product_category,
            message_cn_for_operator, message_local, why_this_message,
            quality_score, risk_check_json, raw_output_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (creator_url, market, target_language, "",
         product_name, product_category,
         msg_cn, msg_local, raw.get("why_this_message", ""),
         0, json.dumps(risk, ensure_ascii=False),
         json.dumps(raw, ensure_ascii=False),
         datetime.now(timezone.utc).isoformat()),
    )
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    return {
        "message_cn_for_operator": msg_cn, "message_local": msg_local,
        "why_this_message": raw.get("why_this_message", ""),
        "risk_check": risk, "raw_output": raw,
        "log_id": log_id, "message_purpose": "follow_up",
    }


def _generate_sample_nurture_message(
    creator_url: str, market: str, target_language: str,
    history_relation: str, profile_card: Dict[str, Any],
    product_info: Dict[str, Any], relationship_context: Dict[str, Any],
) -> Dict[str, Any]:
    """生成样品批前沟通话术 — V1.3 加入达人风格锚点。

    根据达人擅长内容形式路由，结合视频封面风格生成个性化话术。
    """
    writable = profile_card.get("writable_fields", {}) if profile_card else {}
    content_mode = product_info.get("creator_content_mode", "") or writable.get("达人擅长内容形式", "短视频")
    applied_product = product_info.get("applied_sample_product", "") or product_info.get("product_name", "")
    sample_status = product_info.get("sample_application_status", "")

    # ── 构建达人风格锚点 ──
    ct = writable.get("内容类型", "") or ""
    vs = writable.get("画面风格", "") or ""
    style_descriptions = {
        ("穿搭", "镜前半身"): ("镜前半身日常穿搭，真实上身效果清楚", "镜前一镜到底试穿 / 出门前换装"),
        ("穿搭", "全身穿搭"): ("全身穿搭展示，整体搭配效果清楚", "全身镜前转圈展示 / 出门前look"),
        ("穿搭", "户外街拍"): ("户外街拍穿搭，场景感强", "户外自然光线穿搭 / 咖啡厅出门look"),
        ("妆发", "自拍近景"): ("近景妆发/发饰佩戴，头发细节清楚", "出门前近景整理发型 / 半扎发展示"),
        ("妆发", "镜前半身"): ("半身妆发穿搭，发饰和上衣搭配可见", "镜前半身妆发 / 发饰上身效果"),
        ("首饰试戴", "自拍近景"): ("近景首饰试戴，耳饰/项链细节清楚", "近景上脸试戴 / 通勤搭配展示"),
        ("好物分享", "自拍近景"): ("近景好物推荐，手持展示细节清楚", "桌面/近景手持展示 / 口播种草"),
        ("好物分享", "桌面展示"): ("桌面好物展示，商品细节清楚", "桌面开箱 / 手持近景讲解"),
        ("口播种草", "自拍近景"): ("近景口播讲解，字幕和卖点表达强", "近景手持讲解 / 上身对比展示"),
        ("居家生活", "家中生活流"): ("家中自然生活场景，日常感强", "家中随手拍 / 真实生活场景植入"),
    }
    key = (ct, vs)
    if key in style_descriptions:
        obs_style, best_mode = style_descriptions[key]
    else:
        obs_style = f"内容以{ct}为主，画面多为{vs}"
        best_mode = f"{'近景展示' if '近景' in vs or '自拍' in vs else '镜前试穿' if '镜前' in vs else '日常拍摄'}"
    content_strength = f"{obs_style}，适合低成本{'试穿/试戴' if ct in ('穿搭','首饰试戴','妆发') else '展示'}内容"

    style_anchor = {
        "observable_style": obs_style,
        "content_type": ct,
        "visual_style": vs,
        "content_strength": content_strength,
        "best_expression_mode": best_mode,
    }

    # 判断是否有直播证据（只有画面风格为"直播间感"才算有证据）
    has_live = (vs == "直播间感")

    # 提取达人名称
    creator_name = creator_url.split("@")[1].split("?")[0].split("/")[0] if "@" in creator_url else ""

    user_prompt = SAMPLE_NURTURE_USER_PROMPT_TEMPLATE.format(
        creator_url=creator_url,
        creator_name=creator_name,
        history_relation=history_relation,
        activity=writable.get("活跃度", ""),
        content_type=ct,
        visual_style=vs,
        fit_categories=", ".join(writable.get("适配类目", [])),
        creator_content_mode=content_mode,
        creator_tier=relationship_context.get("creator_tier", ""),
        observable_style=style_anchor["observable_style"],
        content_strength=style_anchor["content_strength"],
        best_expression_mode=style_anchor["best_expression_mode"],
        has_live_evidence=str(has_live).lower(),
        live_evidence_source="画面风格包含直播间感" if has_live else "无",
        applied_sample_product=applied_product,
        sample_application_status=sample_status,
        product_name=product_info.get("product_name", ""),
        product_category=product_info.get("product_category", ""),
        specific_product_type=product_info.get("specific_product_type", ""),
        core_selling_points=", ".join(product_info.get("core_selling_points", []) if isinstance(product_info.get("core_selling_points", []), list) else [str(product_info.get("core_selling_points", ""))]),
        short_video_scenes=", ".join(product_info.get("short_video_scenes", []) if isinstance(product_info.get("short_video_scenes", []), list) else [str(product_info.get("short_video_scenes", ""))]),
        live_talking_points=", ".join(product_info.get("live_talking_points", []) if isinstance(product_info.get("live_talking_points", []), list) else [str(product_info.get("live_talking_points", ""))]),
        avoid_claims=", ".join(product_info.get("avoid_claims", []) if isinstance(product_info.get("avoid_claims", []), list) else [str(product_info.get("avoid_claims", ""))]),
        policy_info=product_info.get("policy_info", ""),
    )

    llm = get_llm_client()
    try:
        raw = llm.call_json(prompt=user_prompt, system_prompt=SAMPLE_NURTURE_SYSTEM_PROMPT)
    except Exception as e:
        logger.error("样品批前沟通话术生成失败: %s", e)
        return {
            "message_cn_for_operator": "", "message_local": "",
            "error": str(e), "log_id": -1,
            "message_purpose": "sample_pre_approval_nurture",
        }

    msg_cn = raw.get("message_cn_for_operator", "")
    msg_local = raw.get("message_local", "")
    need_detail = raw.get("need_product_detail", False)

    if need_detail or (not msg_cn and not msg_local):
        return {
            "message_cn_for_operator": "", "message_local": "",
            "why_this_message": raw.get("why_this_message", ""),
            "risk_check": {},
            "raw_output": raw,
            "log_id": -1,
            "message_purpose": "sample_pre_approval_nurture",
            "need_product_detail": True,
            "error": "商品详情不足，无法生成话术",
        }

    risk = check_message_risk(msg_cn, msg_local)
    quality = calculate_sample_nurture_quality(msg_cn, msg_local, content_mode, applied_product, ct, vs, has_live)

    # 写日志
    conn = sqlite3.connect(LOG_DB_PATH)
    conn.execute(
        """INSERT INTO creator_message_logs (
            creator_url, market, target_language, history_relation,
            product_name, product_category, profile_card_json,
            message_cn_for_operator, message_local, why_this_message,
            quality_score, quality_breakdown_json, risk_check_json, raw_output_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (creator_url, market, target_language, history_relation,
         applied_product, product_info.get("product_category", ""),
         json.dumps(profile_card, ensure_ascii=False),
         msg_cn, msg_local, raw.get("why_this_message", ""),
         quality.get("quality_score", 0),
         json.dumps(quality, ensure_ascii=False),
         json.dumps(risk, ensure_ascii=False),
         json.dumps(raw, ensure_ascii=False),
         datetime.now(timezone.utc).isoformat()),
    )
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    return {
        "message_cn_for_operator": msg_cn,
        "message_local": msg_local,
        "why_this_message": raw.get("why_this_message", ""),
        "recommended_mode": raw.get("recommended_mode", ""),
        "quality_score": quality.get("quality_score", 0),
        "quality_breakdown": quality,
        "risk_check": risk,
        "raw_output": raw,
        "log_id": log_id,
        "message_purpose": "sample_pre_approval_nurture",
    }


def build_post_send_fields(message_purpose: str, creator_tier: str) -> Dict[str, Any]:
    """发送后自动计算需要回写的飞书字段。

    Args:
        message_purpose: relationship_maintenance / product_invitation / follow_up
        creator_tier: A 类 / B 类 / C 类 / D 类

    Returns:
        可回写飞书的 fields dict
    """
    from datetime import datetime, timezone, timedelta

    cooldown = {
        "relationship_maintenance": {"A 类": 21, "B 类": 35, "C 类": 60, "D 类": 999},
        "product_invitation": {"A 类": 7, "B 类": 7, "C 类": 30, "D 类": 999},
        "follow_up": {"A 类": 30, "B 类": 999, "C 类": 999, "D 类": 999},
    }

    purpose_to_type = {
        "relationship_maintenance": "关系维护",
        "product_invitation": "商品邀约",
        "follow_up": "轻跟进",
    }

    days = cooldown.get(message_purpose, {}).get(creator_tier, 30)
    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp() * 1000)

    fields = {
        "上次联系时间": timestamp,
        "上次联系类型": purpose_to_type.get(message_purpose, "关系维护"),
        "处理状态": "已发送",
        "发送结果": "已发送",
        "关系阶段": "冷却",
    }

    if days < 999:
        next_date = now + timedelta(days=days)
        fields["下次可联系时间"] = int(next_date.timestamp() * 1000)

    return fields


def _generate_message_with_rewrite(
    creator_url: str,
    market: str,
    target_language: str,
    history_relation: str,
    profile_card: Dict[str, Any],
    product_info: Dict[str, Any],
    content_opportunity: Dict[str, Any],
    product_name: str,
    product_category: str,
    selling_points: List[str],
    shooting_scenarios: List[str],
    price_tier: str,
    sample_available: str,
    commission_info: str,
    support_info: str,
) -> Dict[str, Any]:
    """生成话术 + 质量评分 + 自动重写。"""
    max_rewrites = get_max_rewrite_count()
    best_result = None
    best_score = -1

    for attempt in range(max_rewrites + 1):
        is_rewrite = attempt > 0
        raw_output, quality_result, risk_check = _call_message_llm(
            creator_url=creator_url,
            market=market,
            target_language=target_language,
            history_relation=history_relation,
            profile_card=profile_card,
            content_opportunity=content_opportunity,
            product_name=product_name,
            product_category=product_category,
            selling_points=selling_points,
            shooting_scenarios=shooting_scenarios,
            price_tier=price_tier,
            sample_available=sample_available,
            commission_info=commission_info,
            support_info=support_info,
            product_info=product_info,
            is_rewrite=is_rewrite,
            previous_issues=best_result.get("quality_issues", []) if best_result else [],
        )

        message_cn = raw_output.get("message_cn_for_operator", "")
        message_local = raw_output.get("message_local", "")
        why = raw_output.get("why_this_message", "")

        # 使用 AI 自评分，同时用规则引擎校验
        ai_score = raw_output.get("quality_score", 0)
        ai_breakdown = raw_output.get("quality_breakdown", {})

        # 规则引擎评分
        rule_quality = calculate_message_quality(
            message_cn=message_cn,
            message_local=message_local,
            content_opportunity=content_opportunity,
            product_category=product_category,
        )
        rule_score = rule_quality["quality_score"]

        # 取两者中较低的分
        quality_score = min(ai_score, rule_score) if ai_score > 0 else rule_score
        quality_breakdown = (
            rule_quality["breakdown"] if rule_score <= ai_score or ai_score <= 0
            else ai_breakdown
        )

        this_result = {
            "message_cn_for_operator": message_cn,
            "message_local": message_local,
            "why_this_message": why,
            "quality_score": quality_score,
            "quality_breakdown": quality_breakdown,
            "rewrite_count": attempt,
            "risk_check": risk_check,
            "raw_output": raw_output,
            "quality_issues": rule_quality.get("issues", []),
        }

        if quality_score > best_score:
            best_score = quality_score
            best_result = this_result

        if quality_score >= 7:
            logger.info("话术质量评分 %d，达标，无需重写（第 %d 次尝试）", quality_score, attempt + 1)
            return this_result

        if attempt < max_rewrites:
            logger.info(
                "话术质量评分 %d，低于 7 分，触发重写（第 %d/%d 次）。扣分项: %s",
                quality_score, attempt + 1, max_rewrites,
                rule_quality.get("issues", []),
            )
        else:
            logger.warning(
                "话术重写 %d 次后仍未达标（最佳评分 %d），输出当前最佳结果",
                max_rewrites, best_score,
            )

    # 所有重写都不达标，返回最佳结果
    if best_result:
        return best_result
    return {
        "message_cn_for_operator": "",
        "message_local": "",
        "why_this_message": "",
        "quality_score": 0,
        "quality_breakdown": {},
        "rewrite_count": max_rewrites,
        "risk_check": {},
        "raw_output": {},
        "quality_issues": ["所有重写均失败"],
    }


def _call_message_llm(
    creator_url: str,
    market: str,
    target_language: str,
    history_relation: str,
    profile_card: Dict[str, Any],
    content_opportunity: Dict[str, Any],
    product_name: str,
    product_category: str,
    selling_points: List[str],
    shooting_scenarios: List[str],
    price_tier: str,
    sample_available: str,
    commission_info: str,
    support_info: str,
    product_info: Dict[str, Any],
    is_rewrite: bool = False,
    previous_issues: Optional[List[str]] = None,
) -> tuple:
    """调用 LLM 生成私信话术 + 自评质量。

    Returns:
        (raw_output_dict, quality_result_dict, risk_check_dict)
    """
    # ── 提取画像卡字段 ──
    writable_fields = profile_card.get("writable_fields", {})
    activity = writable_fields.get("活跃度", profile_card.get("activity", ""))
    content_type = writable_fields.get("内容类型", profile_card.get("content_type", ""))
    visual_style = writable_fields.get("画面风格", profile_card.get("visual_style", ""))
    fit_categories = writable_fields.get("适配类目", profile_card.get("fit_categories", []))
    if isinstance(fit_categories, list):
        fit_categories_str = ", ".join(fit_categories)
    else:
        fit_categories_str = str(fit_categories or "")
    recommended = writable_fields.get("推荐商品/品类", profile_card.get("recommended_product_or_category", ""))
    communication_angle = writable_fields.get("沟通切入点", profile_card.get("communication_angle", ""))

    # ── 提取内容机会卡字段 ──
    observable_detail = content_opportunity.get("observable_detail", {}).get("value", "")
    creator_opportunity = content_opportunity.get("creator_content_opportunity", {}).get("value", "")
    recommended_shooting_scene = content_opportunity.get("recommended_shooting_scene", {}).get("value", "")
    product_fit_reason = content_opportunity.get("product_fit_reason", {}).get("value", "")
    message_core_angle = content_opportunity.get("message_core_angle", {}).get("value", "")
    avoid_angle = content_opportunity.get("avoid_angle", {}).get("value", "")

    # ── 提取商品信息 ──
    def _str(val):
        if isinstance(val, list):
            return ", ".join(str(v) for v in val)
        return str(val) if val else ""

    specific_product_type = _str(product_info.get("specific_product_type", ""))
    target_scene = _str(product_info.get("target_scene", []))
    creator_shooting_scene = _str(product_info.get("creator_shooting_scene", []))
    main_content_hook = product_info.get("main_content_hook", "")
    fit_body_or_style = _str(product_info.get("fit_body_or_style", []))
    avoid_claims = _str(product_info.get("avoid_claims", []))

    # ── 构建 prompt ──
    user_prompt = MSG_USER_PROMPT_TEMPLATE.format(
        market=market,
        target_language=target_language,
        history_relation=history_relation,
        creator_url=creator_url,
        activity=activity,
        content_type=content_type,
        visual_style=visual_style,
        fit_categories=fit_categories_str,
        recommended_product_or_category=recommended,
        communication_angle=communication_angle,
        observable_detail=observable_detail or "（未生成内容机会卡）",
        creator_content_opportunity=creator_opportunity or "",
        recommended_shooting_scene=recommended_shooting_scene or "",
        product_fit_reason=product_fit_reason or "",
        message_core_angle=message_core_angle or "",
        avoid_angle=avoid_angle or "",
        product_name=product_name,
        product_category=product_category,
        specific_product_type=specific_product_type,
        target_scene=target_scene,
        creator_shooting_scene=creator_shooting_scene,
        main_content_hook=main_content_hook,
        fit_body_or_style=fit_body_or_style,
        selling_points=", ".join(selling_points),
        shooting_scenarios=", ".join(shooting_scenarios),
        price_tier=price_tier,
        avoid_claims=avoid_claims,
        sample_available=sample_available,
        commission_info=commission_info,
        support_info=support_info,
    )

    # ── 重写时追加改进提示 ──
    if is_rewrite and previous_issues:
        issues_text = "\n".join(f"  - {iss}" for iss in previous_issues)
        user_prompt += f"""

━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ 这是第 {get_max_rewrite_count()} 次重写。上一次话术存在以下问题：

{issues_text}

请针对以上问题重新生成，必须显著改进。"""
    elif is_rewrite:
        user_prompt += """

━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ 这是重写请求。请生成一条更具体、更自然、拍摄场景更明确的话术。"""

    # ── 调用 LLM ──
    llm = get_llm_client()
    try:
        raw_output = llm.call_json(
            prompt=user_prompt,
            system_prompt=MSG_SYSTEM_PROMPT,
        )
    except Exception as e:
        logger.error("私信话术 LLM 调用失败: %s", e)
        return {
            "message_cn_for_operator": "",
            "message_local": "",
            "why_this_message": "",
            "quality_score": 0,
            "quality_breakdown": {},
            "risk_check": {},
        }, {"quality_score": 0, "breaksdown": {}, "issues": [str(e)]}, {}

    # ── 风险检查 ──
    message_cn = raw_output.get("message_cn_for_operator", "")
    message_local = raw_output.get("message_local", "")
    risk_check = check_message_risk(message_cn, message_local)

    # AI 自检
    ai_risk = raw_output.get("risk_check", {})
    if ai_risk.get("has_overpromise"):
        risk_check["has_overpromise"] = True
    if ai_risk.get("has_monitoring_feeling"):
        risk_check["has_monitoring_feeling"] = True
    if ai_risk.get("uses_unprovided_policy"):
        risk_check["uses_unprovided_policy"] = True
    if ai_risk.get("too_general"):
        risk_check["too_general"] = True

    return raw_output, {
        "quality_score": raw_output.get("quality_score", 0),
        "breakdown": raw_output.get("quality_breakdown", {}),
    }, risk_check
