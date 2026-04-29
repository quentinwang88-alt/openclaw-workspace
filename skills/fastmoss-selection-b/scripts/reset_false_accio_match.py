#!/usr/bin/env python3
"""把被错误识别为 Accio 已回收的批次恢复为待回收状态。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.pipeline import (  # noqa: E402
    BATCH_FIELDS,
    FastMossPipeline,
    _list_table_records,
    _record_fields,
    safe_text,
)
from app.utils import utc_now_iso  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="恢复错误匹配的 Accio 批次状态")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    settings = get_settings()
    pipeline = FastMossPipeline.from_settings(settings)

    batch_record = None
    for record in _list_table_records(pipeline.batch_client, settings.feishu_read_page_size):
        fields = _record_fields(record)
        if safe_text(fields.get(BATCH_FIELDS["batch_id"])) == args.batch_id:
            batch_record = pipeline._to_batch_record(record)
            break
    if not batch_record:
        raise RuntimeError("未找到批次: {batch_id}".format(batch_id=args.batch_id))

    selection_rows = pipeline.db.list_selection_records(args.batch_id)
    local_updates = []
    workspace_updates = []
    for row in selection_rows:
        work_id = safe_text(row.get("work_id"))
        if not work_id:
            continue
        local_updates.append(
            {
                "work_id": work_id,
                "accio_status": "待回收",
                "accio_source_url": None,
                "procurement_price_rmb": None,
                "procurement_price_range": None,
                "match_confidence": None,
                "abnormal_low_price": None,
                "accio_note": None,
                "gross_margin_amount": None,
                "gross_margin_rate": None,
                "distribution_margin_amount": None,
                "distribution_margin_rate": None,
                "hermes_status": "未开始",
                "content_potential_score": None,
                "differentiation_score": None,
                "fit_judgment": None,
                "strategy_suggestion": None,
                "recommended_action": None,
                "recommendation_reason": None,
                "risk_warning": None,
            }
        )
        workspace_updates.append(
            {
                "work_id": work_id,
                "推荐采购价_rmb": None,
                "Accio备注": None,
                "商品粗毛利率": None,
                "分销后毛利率": None,
                "打法建议": None,
                "Hermes推荐动作": None,
                "Hermes推荐理由": None,
                "Hermes风险提醒": None,
            }
        )

    pipeline.db.upsert_selection_records(local_updates)
    pipeline._upsert_workspace_partial_updates(workspace_updates)
    pipeline._persist_batch_state(
        batch_record,
        {
            BATCH_FIELDS["accio_status"]: "已发送",
            BATCH_FIELDS["hermes_status"]: "未开始",
            BATCH_FIELDS["overall_status"]: "规则完成待Accio",
            BATCH_FIELDS["error_message"]: "",
        },
        extra={
            "accio_requested_at": utc_now_iso(),
            "accio_response_at": None,
            "hermes_completed_at": None,
        },
    )

    archive_dir = settings.archive_root / args.batch_id
    response_path = archive_dir / "accio_response.json"
    if response_path.exists():
        response_path.unlink()

    print(
        json.dumps(
            {
                "batch_id": args.batch_id,
                "reset_rows": len(local_updates),
                "deleted_response_json": response_path.exists() is False,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
