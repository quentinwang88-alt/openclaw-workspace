#!/usr/bin/env python3
"""Safely replace exact Feishu production-script batches from a local batch DB.

The command is intentionally narrow: old rows are selected only by explicit
batch IDs, new rows only by explicit local batch IDs, and an exact expected
count is required before any deletion.  Existing rows are backed up and are
restored automatically if exporting or post-write verification fails.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.original_batch_storage import BatchStorage  # noqa: E402
from core.production_script_feishu import (  # noqa: E402
    PRODUCTION_SCRIPT_FIELD_NAMES,
    export_ready_batch,
)
from core.production_script_renderer import build_production_projection  # noqa: E402
from scripts.run_feishu_operation_tasks import DEFAULT_SCRIPT_URL, _client  # noqa: E402


DEFAULT_BACKUP_ROOT = (
    Path.home()
    / ".openclaw"
    / "shared"
    / "data"
    / "original_script_feishu_replacements"
)


def _clean(values: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(str(value or "").strip() for value in values if str(value or "").strip()))


def _parse_expected_product_counts(values: Sequence[str]) -> Dict[str, int]:
    result: Dict[str, int] = {}
    for value in values:
        product_code, separator, count_text = str(value or "").partition("=")
        if not separator or not product_code.strip() or not count_text.strip().isdigit():
            raise ValueError("--expected-product-count 必须使用 产品编码=数量")
        result[product_code.strip()] = int(count_text.strip())
    return result


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except TypeError:
        return str(value)


def _record_backup(records: Sequence[Any]) -> List[Dict[str, Any]]:
    return [
        {
            "record_id": record.record_id,
            "fields": {
                key: _json_safe(value) for key, value in record.fields.items()
            },
        }
        for record in records
    ]


def _selected_local_items(
    storage: BatchStorage,
    batch_ids: Sequence[str],
    excluded_item_ids: Sequence[str],
) -> List[tuple[Any, Any, Dict[str, Any]]]:
    excluded = set(excluded_item_ids)
    selected: List[tuple[Any, Any, Dict[str, Any]]] = []
    for batch_id in batch_ids:
        batch = storage.get_batch(batch_id)
        if not batch:
            raise RuntimeError(f"本地批次不存在: {batch_id}")
        for item in storage.get_items(batch_id):
            if item.batch_item_id in excluded:
                continue
            if item.status != "SCRIPT_READY":
                raise RuntimeError(
                    f"批次 {batch_id} 含未就绪脚本: {item.batch_item_id}={item.status}"
                )
            projection = build_production_projection(batch=batch, item=item)
            if not str(projection.get("script_id") or "").strip():
                raise RuntimeError(f"脚本缺少稳定ID: {item.batch_item_id}")
            selected.append((batch, item, projection))
    return selected


def _verify_distribution(
    projections: Sequence[Dict[str, Any]],
    expected_count: int,
    expected_products: Dict[str, int],
) -> Dict[str, Any]:
    script_ids = [str(item.get("script_id") or "").strip() for item in projections]
    signatures = [str(item.get("creative_signature") or "").strip() for item in projections]
    product_counts = Counter(str(item.get("product_code") or "").strip() for item in projections)
    if len(projections) != expected_count:
        raise RuntimeError(f"新脚本数量不符: {len(projections)} != {expected_count}")
    if len(set(script_ids)) != expected_count:
        raise RuntimeError("新脚本存在重复脚本ID")
    if any(not signature for signature in signatures):
        raise RuntimeError("新脚本存在空创意签名")
    duplicate_signatures = sorted(
        signature for signature, count in Counter(signatures).items() if count > 1
    )
    if duplicate_signatures:
        raise RuntimeError(f"新脚本存在重复创意签名: {duplicate_signatures}")
    if expected_products and dict(product_counts) != expected_products:
        raise RuntimeError(
            f"新脚本产品分布不符: {dict(product_counts)} != {expected_products}"
        )
    return {
        "count": len(projections),
        "product_counts": dict(product_counts),
        "unique_script_ids": len(set(script_ids)),
        "unique_creative_signatures": len(set(signatures)),
        "unique_scene_summaries": len(
            {str(item.get("scene_summary") or "").strip() for item in projections}
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="按精确批次ID备份并替换原创视频生产脚本飞书记录"
    )
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--delete-batch-id", action="append", required=True)
    parser.add_argument("--include-batch-id", action="append", required=True)
    parser.add_argument("--exclude-item-id", action="append", default=[])
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--expected-product-count", action="append", default=[])
    parser.add_argument("--backup-root", default=str(DEFAULT_BACKUP_ROOT))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    db_path = str(Path(args.db_path).expanduser().resolve())
    os.environ["ORIGINAL_SCRIPT_GENERATOR_DB_PATH"] = db_path
    delete_batch_ids = _clean(args.delete_batch_id)
    include_batch_ids = _clean(args.include_batch_id)
    excluded_item_ids = _clean(args.exclude_item_id)
    expected_products = _parse_expected_product_counts(args.expected_product_count)

    storage = BatchStorage()
    selected = _selected_local_items(
        storage, include_batch_ids, excluded_item_ids
    )
    projections = [projection for _, _, projection in selected]
    new_summary = _verify_distribution(
        projections, args.expected_count, expected_products
    )

    client = _client(args.script_url)
    all_records = client.list_records(page_size=100)
    old_records = [
        record
        for record in all_records
        if str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["batch_id"]) or "").strip()
        in set(delete_batch_ids)
    ]
    if len(old_records) != args.expected_count:
        raise RuntimeError(
            f"待删除记录数量不符: {len(old_records)} != {args.expected_count}"
        )
    blocked = [
        record.record_id
        for record in old_records
        if bool(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["production_enabled"]))
        or str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["run_task_id"]) or "").strip()
    ]
    if blocked:
        raise RuntimeError(f"待删除记录已有生产引用，拒绝删除: {blocked}")

    old_product_counts = Counter(
        str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["product_code"]) or "").strip()
        for record in old_records
    )
    if expected_products and dict(old_product_counts) != expected_products:
        raise RuntimeError(
            f"旧记录产品分布不符: {dict(old_product_counts)} != {expected_products}"
        )

    product_images: Dict[str, List[Dict[str, Any]]] = {}
    store_ids: Dict[str, str] = {}
    for record in old_records:
        code = str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["product_code"]) or "").strip()
        images = record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["product_images"])
        if code and isinstance(images, list) and images and code not in product_images:
            product_images[code] = images
        store_id = str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["store_id"]) or "").strip()
        if code and store_id and code not in store_ids:
            store_ids[code] = store_id

    preflight = {
        "mode": "APPLY" if args.apply else "DRY_RUN",
        "delete_batch_ids": delete_batch_ids,
        "include_batch_ids": include_batch_ids,
        "excluded_item_ids": excluded_item_ids,
        "old_record_count": len(old_records),
        "old_product_counts": dict(old_product_counts),
        "new": new_summary,
    }
    print(json.dumps({"preflight": preflight}, ensure_ascii=False, indent=2))
    if not args.apply:
        return 0

    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path(args.backup_root).expanduser().resolve() / stamp
    backup_dir.mkdir(parents=True, exist_ok=False)
    backup_records = _record_backup(old_records)
    (backup_dir / "old_feishu_records.json").write_text(
        json.dumps(backup_records, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    (backup_dir / "replacement_preflight.json").write_text(
        json.dumps(preflight, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    selected_script_ids = {
        str(item.get("script_id") or "").strip() for item in projections
    }
    deleted = client.batch_delete_records(
        [record.record_id for record in old_records]
    )
    export_totals = {"created": 0, "updated": 0, "skipped": 0}
    try:
        for batch_id in include_batch_ids:
            batch = storage.get_batch(batch_id)
            batch_items = [
                item
                for item in storage.get_items(batch_id)
                if item.batch_item_id not in set(excluded_item_ids)
            ]
            summary = export_ready_batch(
                batch=batch,
                items=batch_items,
                target_client=client,
                product_images=product_images.get(batch.product_code, []),
                store_id=store_ids.get(batch.product_code, ""),
            )
            for key in export_totals:
                export_totals[key] += int(summary.get(key) or 0)

        written = [
            record
            for record in client.list_records(page_size=100)
            if str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]) or "").strip()
            in selected_script_ids
        ]
        written_products = Counter(
            str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["product_code"]) or "").strip()
            for record in written
        )
        if len(written) != args.expected_count:
            raise RuntimeError(
                f"飞书写入后数量校验失败: {len(written)} != {args.expected_count}"
            )
        if expected_products and dict(written_products) != expected_products:
            raise RuntimeError(
                f"飞书写入后产品分布失败: {dict(written_products)} != {expected_products}"
            )
    except Exception:
        partial = [
            record.record_id
            for record in client.list_records(page_size=100)
            if str(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]) or "").strip()
            in selected_script_ids
        ]
        client.batch_delete_records(partial)
        client.batch_create_records(
            [{"fields": item["fields"]} for item in backup_records]
        )
        raise

    result = {
        "backup_dir": str(backup_dir),
        "deleted": deleted,
        "export": export_totals,
        "verified_new_count": args.expected_count,
        "verified_product_counts": expected_products,
    }
    (backup_dir / "replacement_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({"result": result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
