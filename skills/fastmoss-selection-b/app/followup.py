#!/usr/bin/env python3
"""跟进复盘同步。"""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.utils import build_followup_id, build_work_id, normalize_bool, safe_text, utc_now_iso


def build_followup_row(workspace_fields: Dict[str, Any], existing_row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    work_id = safe_text(workspace_fields.get("work_id"))
    if not work_id:
        work_id = build_work_id(
            safe_text(workspace_fields.get("batch_id")),
            safe_text(workspace_fields.get("product_id")),
        )
    followup_id = build_followup_id(work_id)
    base = dict(existing_row or {})
    base.update(
        {
            "followup_id": followup_id,
            "source_work_id": work_id,
            "batch_id": safe_text(workspace_fields.get("batch_id")),
            "product_id": safe_text(workspace_fields.get("product_id")),
            "country": safe_text(workspace_fields.get("country")),
            "category": safe_text(workspace_fields.get("category")),
            "product_name": safe_text(workspace_fields.get("product_name")),
            "followup_started_at": base.get("followup_started_at") or utc_now_iso(),
            "strategy": (
                safe_text(workspace_fields.get("strategy_suggestion"))
                or safe_text(workspace_fields.get("recommended_action"))
                or safe_text(base.get("strategy"))
                or "组合测试"
            ),
            "current_status": safe_text(base.get("current_status")) or "跟进中",
            "review_note": safe_text(workspace_fields.get("manual_note")),
            "writeback_experience_flag": 0,
            "updated_at": utc_now_iso(),
        }
    )
    return base


def should_sync_followup(workspace_fields: Dict[str, Any]) -> bool:
    return normalize_bool(workspace_fields.get("followup_flag"))
