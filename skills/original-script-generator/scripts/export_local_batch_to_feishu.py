#!/usr/bin/env python3
"""Idempotently export ready local batches to the Feishu production table.

This is for an explicitly reviewed local batch (for example a text-only
preview) which was not initiated from the operation-task table.  It never
generates content, changes a batch, or changes operation-task status.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.original_batch_storage import BatchStorage  # noqa: E402
from core.production_script_feishu import (  # noqa: E402
    PRODUCTION_SCRIPT_FIELD_NAMES,
    export_ready_batch,
    transfer_attachments,
)
from core.production_script_renderer import build_production_projection  # noqa: E402
from scripts.run_feishu_operation_tasks import (  # noqa: E402
    DEFAULT_OPERATION_URL,
    DEFAULT_SCRIPT_URL,
    _client,
)


def _ready_items(storage: BatchStorage, batch_id: str) -> Tuple[Any, List[Any]]:
    batch = storage.get_batch(batch_id)
    if not batch:
        raise RuntimeError(f"本地批次不存在: {batch_id}")
    items = storage.get_items(batch_id)
    not_ready = [
        f"{item.batch_item_id}={item.status}"
        for item in items
        if str(item.status or "") != "SCRIPT_READY"
    ]
    if not_ready:
        raise RuntimeError(f"批次含未就绪脚本，拒绝导出: {batch_id}: {', '.join(not_ready)}")
    if not items:
        raise RuntimeError(f"批次没有脚本: {batch_id}")
    return batch, items


def _source_images(operation_client: Any, script_client: Any, source_record_id: str) -> List[Dict[str, Any]]:
    """Transfer attached product images when the batch originated from a task row.

    A manual batch can have no matching source row.  That is legal; it is
    exported without attachments rather than guessing or using another
    product's images.
    """
    source_id = str(source_record_id or "").strip()
    if not source_id:
        return []
    source = next(
        (record for record in operation_client.list_records(page_size=100) if record.record_id == source_id),
        None,
    )
    if source is None:
        return []
    attachments = source.fields.get("产品图片（需填写）") or source.fields.get("产品图片") or []
    if not isinstance(attachments, list) or not attachments:
        return []
    return transfer_attachments(operation_client, script_client, attachments)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="将已完成的本地原创批次幂等写入飞书生产脚本表")
    parser.add_argument("--batch-id", action="append", required=True)
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--operation-url", default=DEFAULT_OPERATION_URL)
    parser.add_argument("--apply", action="store_true", help="不传时仅预检，不写飞书")
    args = parser.parse_args(list(argv) if argv is not None else None)

    storage = BatchStorage()
    selected = [_ready_items(storage, batch_id) for batch_id in args.batch_id]
    projections = [
        build_production_projection(batch=batch, item=item)
        for batch, items in selected
        for item in items
    ]
    script_ids = {str(item.get("script_id") or "").strip() for item in projections}
    if "" in script_ids or len(script_ids) != len(projections):
        raise RuntimeError("导出脚本ID缺失或重复")

    script_client = _client(args.script_url)
    existing_records = script_client.list_records(page_size=100)
    existing_by_key = {
        (
            str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["batch_id"]) or "").strip(),
            str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["batch_item_id"]) or "").strip(),
        )
        for record in existing_records
    }
    preflight_items = []
    for projection in projections:
        key = (
            str(projection.get("batch_id") or "").strip(),
            str(projection.get("batch_item_id") or "").strip(),
        )
        preflight_items.append({
            "product_code": projection.get("product_code"),
            "script_id": projection.get("script_id"),
            "batch_item_id": projection.get("batch_item_id"),
            "action": "update" if key in existing_by_key else "create",
        })
    preflight = {
        "mode": "APPLY" if args.apply else "DRY_RUN",
        "batches": [batch.batch_id for batch, _ in selected],
        "ready_count": len(projections),
        "items": preflight_items,
    }
    print(json.dumps({"preflight": preflight}, ensure_ascii=False, indent=2))
    if not args.apply:
        return 0

    operation_client = _client(args.operation_url)
    totals = {"created": 0, "updated": 0, "skipped": 0, "images_transferred": 0}
    for batch, items in selected:
        images = _source_images(
            operation_client, script_client, getattr(batch, "source_record_id", "")
        )
        summary = export_ready_batch(
            batch=batch,
            items=items,
            target_client=script_client,
            product_images=images,
            storage=storage,
        )
        for key in ("created", "updated", "skipped"):
            totals[key] += int(summary.get(key) or 0)
        totals["images_transferred"] += len(images)

    written = [
        record
        for record in script_client.list_records(page_size=100)
        if str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]) or "").strip() in script_ids
    ]
    if len(written) != len(projections):
        raise RuntimeError(f"飞书写入后校验失败: {len(written)} != {len(projections)}")
    print(json.dumps({"result": {**totals, "verified_count": len(written)}}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
