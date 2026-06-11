"""
内容机会卡生成器 — V1.1 新增隐藏中间层。

编排：达人画像卡 + 商品信息 → AI 生成内容机会卡 → 落 SQLite 日志。

内容机会卡不写入飞书主表，只保存在日志中，用来约束话术不漂。
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..config import PROMPT_VERSION, LLM_MODEL, LOG_DB_PATH
from .llm_client import get_llm_client
from ..prompts.content_opportunity import (
    OPPORTUNITY_SYSTEM_PROMPT,
    OPPORTUNITY_USER_PROMPT_TEMPLATE,
)

logger = logging.getLogger(__name__)


def _init_log_db() -> None:
    """初始化内容机会卡日志数据库。"""
    db_path = Path(LOG_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS creator_content_opportunity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creator_url TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT '',
            product_name TEXT DEFAULT '',
            product_category TEXT DEFAULT '',
            profile_card_json TEXT DEFAULT '{}',
            product_info_json TEXT DEFAULT '{}',
            cover_count INTEGER DEFAULT 0,
            prompt_version TEXT NOT NULL DEFAULT 'v1.1',
            model_name TEXT NOT NULL DEFAULT '',
            raw_output_json TEXT DEFAULT '{}',
            observable_detail TEXT DEFAULT '',
            creator_content_opportunity TEXT DEFAULT '',
            recommended_shooting_scene TEXT DEFAULT '',
            product_fit_reason TEXT DEFAULT '',
            message_core_angle TEXT DEFAULT '',
            avoid_angle TEXT DEFAULT '',
            need_more_product_info INTEGER DEFAULT 0,
            missing_info_json TEXT DEFAULT '[]',
            unsuited INTEGER DEFAULT 0,
            unsuited_reason TEXT DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def generate_content_opportunity(
    creator_url: str,
    market: str,
    profile_card: Dict[str, Any],
    product_info: Dict[str, Any],
    cover_collage_images: Optional[List[str]] = None,
    cover_count: int = 20,
    recent_video_meta_text: str = "",
) -> Dict[str, Any]:
    """生成内容机会卡。

    Args:
        creator_url: 达人链接。
        market: 市场。
        profile_card: 达人画像卡 dict，来自 profile_generator 的输出。
        product_info: 商品信息 dict，包含以下字段（尽可能填）：
            - product_name: 商品名称
            - product_category: 商品类目
            - specific_product_type: 商品具体类型（如"薄开衫"）
            - target_scene: 目标使用场景列表
            - creator_shooting_scene: 达人拍摄场景建议列表
            - main_content_hook: 内容主钩子
            - fit_body_or_style: 适配身型/风格列表
            - selling_points: 商品卖点列表
            - shooting_scenarios: 拍摄场景参考列表
            - price_tier: 价格层级
            - avoid_claims: 避免声明列表
            - sample_available: 是否可寄样
            - commission_info: 佣金信息
            - support_info: 优惠/支持
        cover_collage_images: 封面拼图路径列表（传给 LLM 看图）。
        cover_count: 封面数量。
        recent_video_meta_text: 视频文字信息（可选）。

    Returns:
        {
            "observable_detail": {"value": "", "confidence": 0, "evidence": ""},
            "creator_content_opportunity": {"value": "", "confidence": 0},
            "recommended_shooting_scene": {"value": "", "confidence": 0},
            "product_fit_reason": {"value": "", "confidence": 0},
            "message_core_angle": {"value": "", "confidence": 0},
            "avoid_angle": {"value": ""},
            "private": {
                "need_more_product_info": bool,
                "missing_info": [],
                "unsuited": bool,
                "unsuited_reason": ""
            },
            "log_id": int,
        }
    """
    _init_log_db()

    cover_collage_images = cover_collage_images or []

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

    communication_angle = writable_fields.get("沟通切入点", profile_card.get("communication_angle", ""))

    # ── 提取商品信息字段（支持 list 和 str） ──
    def _as_comma_str(val, default=""):
        if isinstance(val, list):
            return ", ".join(str(v) for v in val)
        return str(val) if val else default

    product_name = product_info.get("product_name", "")
    product_category = product_info.get("product_category", "")
    specific_product_type = product_info.get("specific_product_type", "")
    target_scene = _as_comma_str(product_info.get("target_scene", []))
    creator_shooting_scene = _as_comma_str(product_info.get("creator_shooting_scene", []))
    main_content_hook = product_info.get("main_content_hook", "")
    fit_body_or_style = _as_comma_str(product_info.get("fit_body_or_style", []))
    selling_points = _as_comma_str(product_info.get("selling_points", []))
    shooting_scenarios = _as_comma_str(product_info.get("shooting_scenarios", []))
    price_tier = product_info.get("price_tier", "")
    avoid_claims = _as_comma_str(product_info.get("avoid_claims", []))
    sample_available = product_info.get("sample_available", "")
    commission_info = product_info.get("commission_info", "")
    support_info = product_info.get("support_info", "")

    # ── 封面拼图信息 ──
    cover_info_lines = []
    for i, img_path in enumerate(cover_collage_images):
        cover_info_lines.append(f"【封面拼图 {i+1}】（已作为图片附件上传）")
    cover_collage_info = "\n".join(cover_info_lines) if cover_info_lines else "（未提供封面拼图）"

    # ── 构建 prompt ──
    user_prompt = OPPORTUNITY_USER_PROMPT_TEMPLATE.format(
        creator_url=creator_url,
        market=market,
        activity=activity,
        content_type=content_type,
        visual_style=visual_style,
        fit_categories=fit_categories_str,
        cover_count=cover_count,
        cover_collage_info=cover_collage_info,
        recent_video_meta_text=recent_video_meta_text or "（未提供视频文字信息）",
        product_name=product_name,
        product_category=product_category,
        specific_product_type=specific_product_type,
        target_scene=target_scene,
        creator_shooting_scene=creator_shooting_scene,
        main_content_hook=main_content_hook,
        fit_body_or_style=fit_body_or_style,
        selling_points=selling_points,
        shooting_scenarios=shooting_scenarios,
        price_tier=price_tier,
        avoid_claims=avoid_claims,
        sample_available=sample_available,
        commission_info=commission_info,
        support_info=support_info,
    )

    # ── 构建图片列表 ──
    image_paths = []
    for img in cover_collage_images:
        if Path(img).exists():
            image_paths.append(img)

    # ── 调用 LLM ──
    llm = get_llm_client()
    try:
        raw_output = llm.call_json(
            prompt=user_prompt,
            image_paths=image_paths,
            system_prompt=OPPORTUNITY_SYSTEM_PROMPT,
        )
    except Exception as e:
        logger.error("内容机会卡 LLM 调用失败: %s", e)
        return {
            "observable_detail": {"value": "", "confidence": 0, "evidence": ""},
            "creator_content_opportunity": {"value": "", "confidence": 0},
            "recommended_shooting_scene": {"value": "", "confidence": 0},
            "product_fit_reason": {"value": "", "confidence": 0},
            "message_core_angle": {"value": "", "confidence": 0},
            "avoid_angle": {"value": ""},
            "private": {
                "need_more_product_info": True,
                "missing_info": [f"LLM 调用失败: {e}"],
                "unsuited": False,
                "unsuited_reason": "",
            },
            "log_id": -1,
            "error": str(e),
        }

    # ── 提取字段 ──
    obs = raw_output.get("observable_detail") or {}
    opp = raw_output.get("creator_content_opportunity") or {}
    scene = raw_output.get("recommended_shooting_scene") or {}
    fit = raw_output.get("product_fit_reason") or {}
    angle = raw_output.get("message_core_angle") or {}
    avoid = raw_output.get("avoid_angle") or {}
    private = raw_output.get("private") or {}

    observable_detail = obs.get("value", "")
    creator_opportunity = opp.get("value", "")
    recommended_shooting_scene = scene.get("value", "")
    product_fit_reason = fit.get("value", "")
    message_core_angle = angle.get("value", "")
    avoid_angle_val = avoid.get("value", "")
    need_more_product_info = private.get("need_more_product_info", False)
    missing_info = private.get("missing_info", [])
    unsuited = private.get("unsuited", False)
    unsuited_reason = private.get("unsuited_reason", "")

    # ── 写入日志数据库 ──
    conn = sqlite3.connect(LOG_DB_PATH)
    conn.execute(
        """
        INSERT INTO creator_content_opportunity_logs (
            creator_url, market, product_name, product_category,
            profile_card_json, product_info_json, cover_count,
            prompt_version, model_name, raw_output_json,
            observable_detail, creator_content_opportunity,
            recommended_shooting_scene, product_fit_reason,
            message_core_angle, avoid_angle,
            need_more_product_info, missing_info_json,
            unsuited, unsuited_reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            creator_url, market, product_name, product_category,
            json.dumps(profile_card, ensure_ascii=False),
            json.dumps(product_info, ensure_ascii=False),
            cover_count,
            PROMPT_VERSION, LLM_MODEL,
            json.dumps(raw_output, ensure_ascii=False),
            observable_detail,
            creator_opportunity,
            recommended_shooting_scene,
            product_fit_reason,
            message_core_angle,
            avoid_angle_val,
            1 if need_more_product_info else 0,
            json.dumps(missing_info, ensure_ascii=False),
            1 if unsuited else 0,
            unsuited_reason,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    return {
        "observable_detail": obs,
        "creator_content_opportunity": opp,
        "recommended_shooting_scene": scene,
        "product_fit_reason": fit,
        "message_core_angle": angle,
        "avoid_angle": avoid,
        "private": {
            "need_more_product_info": need_more_product_info,
            "missing_info": missing_info,
            "unsuited": unsuited,
            "unsuited_reason": unsuited_reason,
        },
        "log_id": log_id,
    }
