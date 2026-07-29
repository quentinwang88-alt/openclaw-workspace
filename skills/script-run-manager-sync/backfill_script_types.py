#!/usr/bin/env python3
"""Create and backfill the run-manager 脚本类型 field without touching task state."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

from core.bitable import FeishuBitableClient, TableRecord
from core.sync import (
    RUN_MANAGER_SCRIPT_TYPE_OPTIONS,
    SCRIPT_FIELD_SPECS,
    SOURCE_FIELD_ALIASES,
    derive_task_metadata,
    normalize_run_manager_script_type,
    normalize_text,
    resolve_field_mapping,
)
from run_pipeline import DEFAULT_SOURCE_FEISHU_URL, DEFAULT_TARGET_FEISHU_URL, resolve_feishu_config


SCRIPT_TYPE_FIELD = "脚本类型"
DIRECT_TASK_SOURCES = {
    "轻量试穿视频",
    "口播增强补充镜头",
    *RUN_MANAGER_SCRIPT_TYPE_OPTIONS,
    "短视频复刻",
    "养号复刻",
}


def _source_value(fields: Dict[str, Any], mapping: Dict[str, str | None], logical_name: str) -> str:
    field_name = mapping.get(logical_name)
    return normalize_text(fields.get(field_name)) if field_name else ""


def source_record_script_type(record: TableRecord, mapping: Dict[str, str | None]) -> str:
    return normalize_run_manager_script_type(
        script_source=_source_value(record.fields, mapping, "script_source"),
        source_script_type=_source_value(record.fields, mapping, "source_script_type"),
        source_remake_record_id=_source_value(record.fields, mapping, "source_remake_record_id"),
        publish_purpose=_source_value(record.fields, mapping, "publish_purpose"),
        content_branch=_source_value(record.fields, mapping, "content_branch"),
    )


def plan_source_type_repairs(
    records: Sequence[TableRecord],
    mapping: Dict[str, str | None],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Repair type-only remake labels that could not have come from the remake pipeline."""
    type_field = mapping.get("source_script_type")
    if not type_field:
        return [], {"records": len(records), "planned_updates": 0, "record_ids": []}

    short_labels = {"短视频复刻", "短视频复刻脚本"}
    updates: List[Dict[str, Any]] = []
    for record in records:
        current = _source_value(record.fields, mapping, "source_script_type")
        if current not in short_labels:
            continue
        if source_record_script_type(record, mapping) == "短视频复刻脚本":
            continue
        updates.append({"record_id": record.record_id, "fields": {type_field: "原创脚本"}})

    return updates, {
        "records": len(records),
        "planned_updates": len(updates),
        "record_ids": [item["record_id"] for item in updates],
    }


def build_source_script_type_index(
    records: Sequence[TableRecord],
    mapping: Dict[str, str | None],
) -> Dict[str, Set[str]]:
    index: Dict[str, Set[str]] = defaultdict(set)
    for record in records:
        script_type = source_record_script_type(record, mapping)
        for spec in SCRIPT_FIELD_SPECS:
            field_name = mapping.get(spec["logical_name"])
            if not field_name or not normalize_text(record.fields.get(field_name)):
                continue
            script_id = normalize_text(derive_task_metadata(record, mapping, spec["task_suffix"])["script_id"])
            if script_id:
                index[script_id].add(script_type)
    return dict(index)


def classify_run_record(
    record: TableRecord,
    source_index: Dict[str, Set[str]],
) -> Tuple[str, str]:
    fields = record.fields
    task_source = normalize_text(fields.get("任务来源"))
    if task_source in DIRECT_TASK_SOURCES:
        return normalize_run_manager_script_type(task_source=task_source), "任务来源"

    script_id = normalize_text(fields.get("脚本ID")) or normalize_text(fields.get("内容ID"))
    candidates = source_index.get(script_id, set())
    if len(candidates) == 1:
        return next(iter(candidates)), "上游脚本ID"
    if len(candidates) > 1:
        return "", f"脚本ID分类冲突:{script_id}:{','.join(sorted(candidates))}"
    return "", f"无法匹配:{script_id or '<空脚本ID>'}"


def plan_backfill(
    run_records: Sequence[TableRecord],
    source_index: Dict[str, Set[str]],
    *,
    overwrite: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    updates: List[Dict[str, Any]] = []
    counts: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    errors: List[Dict[str, str]] = []
    valid_types = set(RUN_MANAGER_SCRIPT_TYPE_OPTIONS)

    for record in run_records:
        current = normalize_text(record.fields.get(SCRIPT_TYPE_FIELD))
        if current and not overwrite:
            counts[current if current in valid_types else "已有非标准值"] += 1
            reasons["保留已有值"] += 1
            continue

        script_type, reason = classify_run_record(record, source_index)
        if not script_type:
            counts["无法分类"] += 1
            errors.append({"record_id": record.record_id, "reason": reason})
            continue
        counts[script_type] += 1
        reasons[reason] += 1
        if current != script_type:
            updates.append({"record_id": record.record_id, "fields": {SCRIPT_TYPE_FIELD: script_type}})

    summary: Dict[str, Any] = {
        "records": len(run_records),
        "planned_updates": len(updates),
        "type_counts": dict(counts),
        "classification_reasons": dict(reasons),
        "errors": errors[:20],
        "error_count": len(errors),
    }
    return updates, summary


def ensure_script_type_field(client: FeishuBitableClient, *, dry_run: bool) -> Dict[str, Any]:
    field = next((item for item in client.list_fields() if item.field_name == SCRIPT_TYPE_FIELD), None)
    if field is not None:
        if int(field.field_type) != 3:
            raise RuntimeError(f"运行管理表【脚本类型】已存在，但不是单选字段: type={field.field_type}")
        return {"action": "exists", "field_type": field.field_type}

    if not dry_run:
        client.create_field(
            SCRIPT_TYPE_FIELD,
            field_type=3,
            ui_type="SingleSelect",
            property={"options": [{"name": item} for item in RUN_MANAGER_SCRIPT_TYPE_OPTIONS]},
        )
    return {"action": "would_create" if dry_run else "created", "options": list(RUN_MANAGER_SCRIPT_TYPE_OPTIONS)}


def batches(items: Sequence[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch_size = min(max(int(size), 1), 500)
    for start in range(0, len(items), batch_size):
        yield list(items[start:start + batch_size])


def current_type_counts(records: Sequence[TableRecord]) -> Dict[str, int]:
    return dict(Counter(normalize_text(record.fields.get(SCRIPT_TYPE_FIELD)) or "<空>" for record in records))


def main() -> None:
    parser = argparse.ArgumentParser(description="运行管理表脚本类型字段创建与幂等回填")
    parser.add_argument("--source-feishu-url", default=DEFAULT_SOURCE_FEISHU_URL)
    parser.add_argument("--target-feishu-url", default=DEFAULT_TARGET_FEISHU_URL)
    parser.add_argument("--apply", action="store_true", help="实际创建字段并写回；默认仅 dry-run")
    parser.add_argument("--overwrite", action="store_true", help="重新计算并覆盖已有脚本类型；默认只填空值")
    parser.add_argument(
        "--repair-source-mislabels",
        action="store_true",
        help="把原始脚本表中只有类型、没有复刻来源证据的“短视频复刻”改为“原创脚本”",
    )
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    source_app_token, source_table_id = resolve_feishu_config(args.source_feishu_url)
    target_app_token, target_table_id = resolve_feishu_config(args.target_feishu_url)
    source_client = FeishuBitableClient(source_app_token, source_table_id)
    target_client = FeishuBitableClient(target_app_token, target_table_id)

    source_mapping = resolve_field_mapping(source_client.list_field_names(), SOURCE_FIELD_ALIASES)
    source_records = source_client.list_records(page_size=500)
    run_records = target_client.list_records(page_size=500)
    source_updates, source_repair_summary = plan_source_type_repairs(source_records, source_mapping)
    if not args.repair_source_mislabels:
        source_updates = []
        source_repair_summary = {
            **source_repair_summary,
            "planned_updates": 0,
            "detected_updates": len(source_repair_summary["record_ids"]),
        }
    source_index = build_source_script_type_index(source_records, source_mapping)
    updates, summary = plan_backfill(run_records, source_index, overwrite=args.overwrite)
    schema = ensure_script_type_field(target_client, dry_run=not args.apply)

    output: Dict[str, Any] = {
        "mode": "apply" if args.apply else "dry-run",
        "schema": schema,
        "source_repairs": source_repair_summary,
        **summary,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    if summary["error_count"]:
        raise RuntimeError(f"存在 {summary['error_count']} 条无法安全分类的记录，已停止写入")

    if not args.apply:
        return
    for batch in batches(source_updates, args.batch_size):
        source_client.batch_update_records(batch)
    for batch in batches(updates, args.batch_size):
        target_client.batch_update_records(batch)

    verified = target_client.list_records(page_size=500)
    verification = current_type_counts(verified)
    if verification.get("<空>", 0):
        raise RuntimeError(f"回填后仍有空值: {verification['<空>']}")
    print(json.dumps({
        "verification": verification,
        "source_updated": len(source_updates),
        "run_manager_updated": len(updates),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
