"""
画像生成器 — 编排 AI 分析 → 校验 → 落库 → 写飞书的完整流程。
"""
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..config import PROMPT_VERSION, LLM_MODEL, LOG_DB_PATH
from .llm_client import get_llm_client
from .validator import validate_profile_output
from .feishu_writer import write_profile_to_feishu
from ..prompts.profile_analysis import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


def _init_log_db() -> None:
    """初始化 AI 分析日志数据库。"""
    db_path = Path(LOG_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS creator_ai_analysis_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creator_url TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT '',
            history_relation TEXT NOT NULL DEFAULT '',
            cover_collage_image_paths TEXT DEFAULT '[]',
            recent_video_meta_text TEXT DEFAULT '',
            product_candidates_json TEXT DEFAULT '[]',
            cover_count INTEGER DEFAULT 0,
            has_publish_time INTEGER DEFAULT 1,
            has_product_pool INTEGER DEFAULT 0,
            prompt_version TEXT NOT NULL DEFAULT 'v1.0',
            model_name TEXT NOT NULL DEFAULT '',
            raw_input_json TEXT DEFAULT '{}',
            raw_output_json TEXT DEFAULT '{}',
            parsed_output_json TEXT DEFAULT '{}',
            writable_fields_json TEXT DEFAULT '{}',
            validation_results_json TEXT DEFAULT '[]',
            manual_review_required INTEGER DEFAULT 0,
            manual_review_reasons TEXT DEFAULT '[]',
            feishu_write_result TEXT DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def generate_profile_card(
    creator_url: str,
    market: str,
    history_relation: str,
    cover_collage_images: List[str],
    profile_header_image: Optional[str] = None,
    recent_video_meta_text: str = "",
    product_candidates: Optional[List[Dict[str, Any]]] = None,
    app_token: Optional[str] = None,
    table_id: Optional[str] = None,
    write_to_feishu: bool = True,
) -> Dict[str, Any]:
    """生成达人画像卡。

    Args:
        creator_url: 达人主页链接。
        market: 市场 (VN/TH/MY/PH)。
        history_relation: 历史关系。
        cover_collage_images: 封面拼图路径列表。
        profile_header_image: 主页头部截图路径（可选）。
        recent_video_meta_text: 视频文字信息（可选）。
        product_candidates: 商品候选池（可选）。
        app_token: 飞书 app_token。
        table_id: 表格 table_id。
        write_to_feishu: 是否写入飞书。

    Returns:
        {
            "creator_url": "...",
            "raw_output": {...},
            "writable_fields": {...},
            "validation_results": [...],
            "manual_review_required": bool,
            "feishu_result": {...} or None,
            "log_id": int,
        }
    """
    _init_log_db()

    product_candidates = product_candidates or []
    cover_count = len(cover_collage_images) * 10  # 每张拼图约 10 个封面，粗略估算
    has_publish_time = bool(recent_video_meta_text and recent_video_meta_text.strip())
    has_product_pool = bool(product_candidates)

    # ── 构建 prompt ──
    # 封面数量估计
    # 图片占位符：实际图片通过 call_json 的 image_paths 传入
    cover_placeholders = "\n".join(
        f"【封面拼图 {i+1}】（已作为图片附件上传）"
        for i in range(len(cover_collage_images))
    )

    profile_placeholder = (
        "（已作为图片附件上传）" if profile_header_image
        else "（未提供）"
    )

    product_text = json.dumps(product_candidates, ensure_ascii=False, indent=2) if product_candidates else "（未提供商品候选池）"

    user_prompt = USER_PROMPT_TEMPLATE.format(
        creator_url=creator_url,
        market=market,
        history_relation=history_relation,
        profile_header_image_placeholder=profile_placeholder,
        cover_collage_placeholders=cover_placeholders,
        recent_video_meta_text=recent_video_meta_text or "（未提供视频文字信息）",
        product_candidates=product_text,
    )

    # ── 构建图片列表 ──
    image_paths = []
    if profile_header_image and Path(profile_header_image).exists():
        image_paths.append(profile_header_image)
    for img in cover_collage_images:
        if Path(img).exists():
            image_paths.append(img)

    # ── 调用 LLM ──
    llm = get_llm_client()
    raw_input = {
        "creator_url": creator_url,
        "market": market,
        "history_relation": history_relation,
        "cover_count": cover_count,
        "image_paths": image_paths,
        "prompt": user_prompt,
    }

    try:
        raw_output = llm.call_json(
            prompt=user_prompt,
            image_paths=image_paths,
            system_prompt=SYSTEM_PROMPT,
        )
    except Exception as e:
        logger.error("LLM 调用失败: %s", e)
        return {
            "creator_url": creator_url,
            "error": str(e),
            "raw_output": {},
            "writable_fields": {},
            "validation_results": [],
            "manual_review_required": True,
            "feishu_result": None,
            "log_id": -1,
        }

    # ── 校验 ──
    writable_fields, validation_results, manual_review = validate_profile_output(
        raw_output,
        cover_count=cover_count,
        has_publish_time=has_publish_time,
        has_product_pool=has_product_pool,
    )

    # ── 写入飞书 ──
    feishu_result = None
    if write_to_feishu and writable_fields:
        try:
            feishu_result = write_profile_to_feishu(
                creator_url=creator_url,
                history_relation=history_relation,
                writable_fields=writable_fields,
                cover_collage_images=cover_collage_images,
                app_token=app_token,
                table_id=table_id,
            )
        except Exception as e:
            logger.error("飞书写入失败: %s", e)
            feishu_result = {"error": str(e)}

    # ── 写入日志数据库 ──
    conn = sqlite3.connect(LOG_DB_PATH)
    conn.execute(
        """
        INSERT INTO creator_ai_analysis_logs (
            creator_url, market, history_relation,
            cover_collage_image_paths, recent_video_meta_text,
            product_candidates_json, cover_count,
            has_publish_time, has_product_pool,
            prompt_version, model_name,
            raw_input_json, raw_output_json, parsed_output_json,
            writable_fields_json, validation_results_json,
            manual_review_required, manual_review_reasons,
            feishu_write_result, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            creator_url, market, history_relation,
            json.dumps(cover_collage_images, ensure_ascii=False),
            recent_video_meta_text,
            json.dumps(product_candidates, ensure_ascii=False),
            cover_count,
            1 if has_publish_time else 0,
            1 if has_product_pool else 0,
            PROMPT_VERSION, LLM_MODEL,
            json.dumps(raw_input, ensure_ascii=False),
            json.dumps(raw_output, ensure_ascii=False),
            json.dumps(raw_output, ensure_ascii=False),
            json.dumps(writable_fields, ensure_ascii=False),
            json.dumps(
                [{"field": r.field_name, "action": r.action, "errors": r.errors, "warnings": r.warnings}
                 for r in validation_results],
                ensure_ascii=False,
            ),
            1 if manual_review else 0,
            json.dumps(raw_output.get("manual_review_reasons", []), ensure_ascii=False),
            json.dumps(feishu_result, ensure_ascii=False) if feishu_result else "",
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    return {
        "creator_url": creator_url,
        "raw_output": raw_output,
        "writable_fields": writable_fields,
        "validation_results": [
            {
                "field": r.field_name,
                "action": r.action,
                "value": str(r.value)[:100],
                "errors": r.errors,
                "warnings": r.warnings,
            }
            for r in validation_results
        ],
        "manual_review_required": manual_review,
        "feishu_result": feishu_result,
        "log_id": log_id,
    }
