#!/usr/bin/env python3
"""Independent Feishu operation/review workbench runner for organic seeding."""
from __future__ import annotations

import argparse
import atexit
import fcntl
import json
import sys
import traceback
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.content_branches.organic_seeding.pipeline import OrganicSeedingPipeline  # noqa: E402
from core.content_branches.organic_seeding.claim_adapter import CentralClaimProvider  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.llm_client import OriginalScriptLLMClient  # noqa: E402
from core.organic_seeding_feishu import (  # noqa: E402
    SEED_OPERATION_FIELDS, SEED_OPERATION_SELECT_OPTIONS,
    SEED_OPERATION_PRUNE_FIELDS, SEED_OPERATION_RENAMES,
    SEED_SCRIPT_FIELDS, SEED_SCRIPT_SELECT_OPTIONS, operation_record_values,
    SEED_SCRIPT_PRUNE_FIELDS, SEED_SCRIPT_RENAMES,
    result_to_feishu_fields,
)
from core.production_script_feishu import (  # noqa: E402
    ensure_fields, ensure_single_select_options, transfer_attachments,
)


LOCK_PATH = Path.home() / ".openclaw" / "shared" / "locks" / "organic-seeding-production.lock"


def _client(url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(url)
    if not info:
        raise ValueError(f"无法解析飞书链接: {url}")
    token = resolve_wiki_bitable_app_token(info.app_token) if "/wiki/" in url else info.app_token
    return FeishuBitableClient(token, info.table_id)


def _lock():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    handle = LOCK_PATH.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={__import__('os').getpid()}\n")
    handle.flush()
    return handle


def _update(client: FeishuBitableClient, record_id: str, **values: Any) -> None:
    mapping = {
        "status": "任务状态", "summary": "结果摘要", "error": "错误信息",
    }
    client.update_record_fields(
        record_id, {mapping[key]: value for key, value in values.items() if key in mapping and value is not None}
    )


def _download_images(client: FeishuBitableClient, task: dict, output_dir: Path) -> list[str]:
    return [
        str(client.download_attachment(attachment, output_dir / f"image_{index}"))
        for index, attachment in enumerate(task["product_images"][:4], 1)
    ]


def _require_fields(client: FeishuBitableClient, primary: str, specs: Sequence[Any]) -> None:
    names = {str(item.get("field_name") or "") for item in client.list_fields()}
    required = {primary, *(spec.name for spec in specs)}
    missing = sorted(required - names)
    if missing:
        raise RuntimeError("飞书表字段未初始化或不完整: " + "、".join(missing))


def _rename_fields(client: FeishuBitableClient, renames: dict[str, str]) -> None:
    fields = client.list_fields()
    by_name = {str(item.get("field_name") or ""): item for item in fields}
    for old_name, new_name in renames.items():
        if old_name not in by_name or new_name in by_name:
            continue
        field = by_name[old_name]
        client.update_field_name(
            str(field["field_id"]), new_name,
            field_type=int(field.get("type") or 1), property=field.get("property"),
        )


def _prune_fields(client: FeishuBitableClient, names: Sequence[str]) -> list[str]:
    fields = client.list_fields()
    by_name = {str(item.get("field_name") or ""): item for item in fields}
    deleted: list[str] = []
    for name in names:
        field = by_name.get(name)
        if not field:
            continue
        client.delete_field(str(field["field_id"]), name)
        deleted.append(name)
    return deleted


def _has_populated_records(client: FeishuBitableClient) -> bool:
    return any(bool(record.fields) for record in client.list_records(page_size=500))


def _allowed_task_statuses(
    *, record_id: str | None, item_indices: Sequence[int], rerun_completed: bool
) -> set[str]:
    statuses = {"待执行"}
    if record_id and item_indices:
        statuses.update({"部分完成", "失败"})
    if record_id and rerun_completed:
        # Older compact workbenches may have a completed summary while the
        # status cell is blank.  The explicit record id + rerun flag is the
        # operator authority; never broaden this fallback to queue scans.
        statuses.update({"", "已完成", "部分完成", "失败"})
    return statuses


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="种草视频运营任务表 -> 种草视频生产脚本")
    parser.add_argument("--operation-url", required=True)
    parser.add_argument("--script-url", required=True)
    parser.add_argument("--record-id")
    parser.add_argument("--item-index", action="append", type=int, default=[])
    parser.add_argument(
        "--target-script-record-id",
        help="精确重跑单个槽位时，原位更新指定的生产脚本 record_id，并保留其脚本ID与人工附件",
    )
    parser.add_argument(
        "--rerun-completed",
        action="store_true",
        help="仅配合 --record-id，显式允许重跑已完成任务；可与 --item-index 精确选择槽位",
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--setup-fields-only",
        action="store_true",
        help="只创建/补齐精简字段，不扫描或执行任务",
    )
    parser.add_argument(
        "--prune-fields", action="store_true",
        help="仅在两张表均无记录时，删除本流程旧版冗余字段",
    )
    parser.add_argument("--preview-only", action="store_true")
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning", default="high")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.prune_fields and not args.setup_fields_only:
        parser.error("--prune-fields 必须与 --setup-fields-only 一起使用")
    if args.item_index and not args.record_id:
        parser.error("--item-index 必须与 --record-id 一起使用")
    if args.rerun_completed and not args.record_id:
        parser.error("--rerun-completed 必须与 --record-id 一起使用")
    if args.target_script_record_id and (
        not args.record_id or len(args.item_index) != 1
    ):
        parser.error("--target-script-record-id 必须与 --record-id 和唯一一个 --item-index 一起使用")

    run_lock = None
    if not args.dry_run:
        run_lock = _lock()
        if run_lock is None:
            print("SKIPPED_LOCKED: 已有种草脚本任务运行")
            return 0
        atexit.register(run_lock.close)

    operation_client = _client(args.operation_url)
    script_client = _client(args.script_url)
    if args.setup_fields_only:
        if args.prune_fields:
            if _has_populated_records(operation_client) or _has_populated_records(script_client):
                raise RuntimeError("检测到表内已有非空记录，拒绝自动删除字段")
            _rename_fields(operation_client, SEED_OPERATION_RENAMES)
            _rename_fields(script_client, SEED_SCRIPT_RENAMES)
        operation_names = ensure_fields(
            operation_client, primary_field_name="任务ID", specs=SEED_OPERATION_FIELDS
        )
        script_names = ensure_fields(
            script_client, primary_field_name="种草脚本ID", specs=SEED_SCRIPT_FIELDS
        )
        ensure_single_select_options(operation_client, SEED_OPERATION_SELECT_OPTIONS)
        ensure_single_select_options(script_client, SEED_SCRIPT_SELECT_OPTIONS)
        deleted_operation = _prune_fields(operation_client, SEED_OPERATION_PRUNE_FIELDS) if args.prune_fields else []
        deleted_script = _prune_fields(script_client, SEED_SCRIPT_PRUNE_FIELDS) if args.prune_fields else []
        operation_names = operation_client.list_field_names()
        script_names = script_client.list_field_names()
        print(
            json.dumps(
                {
                    "status": "FIELDS_READY",
                    "operation_field_count": len(operation_names),
                    "script_field_count": len(script_names),
                    "deleted_operation_fields": deleted_operation,
                    "deleted_script_fields": deleted_script,
                },
                ensure_ascii=False,
            )
        )
        return 0
    _require_fields(operation_client, "任务ID", SEED_OPERATION_FIELDS)
    _require_fields(script_client, "种草脚本ID", SEED_SCRIPT_FIELDS)
    candidates = []
    for record in operation_client.list_records(page_size=500):
        if args.record_id and record.record_id != args.record_id:
            continue
        task = operation_record_values(record)
        allowed_statuses = _allowed_task_statuses(
            record_id=args.record_id,
            item_indices=args.item_index,
            rerun_completed=args.rerun_completed,
        )
        if task["status"] not in allowed_statuses:
            continue
        candidates.append((record, task))
        if args.limit and len(candidates) >= args.limit:
            break
    print(json.dumps({"pending_tasks": len(candidates), "records": [row.record_id for row, _ in candidates]}, ensure_ascii=False))
    if args.dry_run:
        return 0

    llm = None if args.preview_only else OriginalScriptLLMClient(
        route="primary", primary_model=args.model, primary_reasoning_effort=args.reasoning
    )
    claim_provider = CentralClaimProvider()
    pipeline = OrganicSeedingPipeline(llm_client=llm, claim_provider=claim_provider)
    script_records = list(script_client.list_records(page_size=500))
    existing = {
        str(record.fields.get("种草脚本ID") or "").strip(): record
        for record in script_records
        if str(record.fields.get("种草脚本ID") or "").strip()
    }
    target_script_record = next(
        (
            record for record in script_records
            if record.record_id == args.target_script_record_id
        ),
        None,
    ) if args.target_script_record_id else None
    if args.target_script_record_id and target_script_record is None:
        raise ValueError("TARGET_SCRIPT_RECORD_NOT_FOUND")
    runtime_root = Path.home() / ".openclaw" / "shared" / "data" / "organic_seeding_runs"
    failed_tasks = 0
    for record, task in candidates:
        try:
            if not task["product_code"] or not task["product_images"]:
                raise ValueError("产品编码和产品图片为必填")
            if target_script_record is not None and str(
                target_script_record.fields.get("产品编码") or ""
            ).strip() != task["product_code"]:
                raise ValueError("TARGET_SCRIPT_PRODUCT_MISMATCH")
            if not 1 <= task["count"] <= 20:
                raise ValueError("生成数必须在1到20之间")
            if task["duration_seconds"] <= 0:
                raise ValueError("时长必须大于0")
            _update(operation_client, record.record_id, status="执行中", error="")
            output_dir = runtime_root / task["task_id"].replace("/", "_")
            output_dir.mkdir(parents=True, exist_ok=True)
            image_paths = _download_images(operation_client, task, output_dir / "inputs")
            product_context = {
                "product_code": task["product_code"], "top_category": task["top_category"],
                "product_type": task["product_type"], "target_country": task["target_country"],
                "target_language": task["target_language"], "facts": task["facts"],
                "identity_anchors": task["identity_anchors"] or ["商品外观以本任务产品图片为唯一权威"],
                "negative_constraints": task["negative_constraints"],
            }
            governed_claim_snapshot = claim_provider.snapshot(task["product_code"])
            theme_input = {key: value for key, value in task["theme"].items() if value not in ("", [], None)}
            _update(operation_client, record.record_id, status="执行中")
            run = pipeline.run(
                request_id=f"FEISHU_SEED_{record.record_id}", product_context=product_context,
                count=task["count"], theme_inputs=[theme_input] if theme_input else [],
                image_paths=image_paths, duration_seconds=task["duration_seconds"],
                preview_only=args.preview_only,
                item_indices=args.item_index,
                governed_claim_snapshot=governed_claim_snapshot,
            )
            transferred = transfer_attachments(
                operation_client, script_client, task["product_images"]
            )
            for item in run["items"]:
                fields = result_to_feishu_fields(
                    result=item, run=run, task=task, product_images=transferred
                )
                prior = target_script_record or existing.get(item["script_id"])
                if target_script_record is not None:
                    preserved_script_id = str(
                        target_script_record.fields.get("种草脚本ID") or ""
                    ).strip()
                    if not preserved_script_id:
                        raise ValueError("TARGET_SCRIPT_ID_UNAVAILABLE")
                    fields["种草脚本ID"] = preserved_script_id
                if prior:
                    script_client.update_record_fields(prior.record_id, fields)
                else:
                    created_ids = script_client.batch_create_records([{"fields": fields}])
                    existing[item["script_id"]] = SimpleNamespace(
                        record_id=created_ids[0] if created_ids else ""
                    )
            final_status = "已完成" if run["status"] == "COMPLETED" else ("部分完成" if run["ready_count"] else "失败")
            _update(
                operation_client, record.record_id, status=final_status,
                summary=f"请求{run['requested_count']}条；完成{run['ready_count']}条；失败{run['failed_count']}条；发布策略=种草/不挂车",
                error="" if final_status == "已完成" else json.dumps(run["failures"], ensure_ascii=False)[:1800],
            )
        except Exception as exc:
            failed_tasks += 1
            _update(operation_client, record.record_id, status="失败", error=str(exc)[:1800])
            traceback.print_exc()
    return 1 if failed_tasks else 0


if __name__ == "__main__":
    raise SystemExit(main())
