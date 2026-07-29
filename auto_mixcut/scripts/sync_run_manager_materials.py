#!/usr/bin/env python3
"""Index or import generated videos from the shared Feishu run-manager table."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(WORKSPACE / "skills" / "script-run-manager-sync") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient  # type: ignore  # noqa: E402

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402
from auto_mixcut.skills.run_manager_material_import_skill import RunManagerMaterialImportSkill  # noqa: E402


APP_TOKEN = "UvErb5HRWaGESXsBs18cvB3FnEe"
TABLE_ID = "tbl4eKSVgHw8IyDh"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", nargs="?", choices=["scan", "index", "activate"], default="scan")
    parser.add_argument("--record-id")
    parser.add_argument("--product-id")
    parser.add_argument("--canonical-product-id")
    parser.add_argument("--source-flow")
    parser.add_argument("--date-from")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--cutover-at", default=os.environ.get("AUTO_MIXCUT_MATERIAL_AUTO_INGEST_CUTOVER_AT", ""))
    parser.add_argument("--apply", action="store_true", help="write OSS/RDS/Feishu; default is dry-run")
    parser.add_argument("--clear-attachments", action="store_true")
    args = parser.parse_args()

    if args.clear_attachments and not args.apply:
        parser.error("--clear-attachments requires --apply")
    if args.clear_attachments and not _env_flag("MATERIAL_FEISHU_ATTACHMENT_CLEANUP_ENABLED", False):
        parser.error("attachment cleanup is disabled; set MATERIAL_FEISHU_ATTACHMENT_CLEANUP_ENABLED=1 after downstream rollout")

    ctx = build_context()
    if args.apply:
        migrated = RDSRepositorySkill(ctx).init_db()
        if not migrated.success:
            print(json.dumps(migrated.to_dict(), ensure_ascii=False, indent=2))
            return 1
    client = FeishuBitableClient(app_token=APP_TOKEN, table_id=TABLE_ID)
    records = client.list_records(page_size=100)
    importer = RunManagerMaterialImportSkill(ctx, app_token=APP_TOKEN, table_id=TABLE_ID)
    results: list[dict[str, Any]] = []

    for record in records:
        if args.record_id and record.record_id != args.record_id:
            continue
        candidates = importer.inspect_record(
            record.record_id,
            record.fields or {},
            cutoff_at=args.cutover_at,
            force_legacy=args.mode == "activate",
            index_only=args.mode == "index",
        )
        for candidate in candidates:
            if args.product_id and candidate.product_id != args.product_id:
                continue
            if args.canonical_product_id and candidate.canonical_product_id != args.canonical_product_id:
                continue
            if args.source_flow and candidate.source_flow != args.source_flow:
                continue
            if args.date_from and candidate.completed_at and candidate.completed_at < args.date_from:
                continue
            result = importer.import_candidate(client, candidate, dry_run=not args.apply)
            item = result.to_dict()
            item["record_id"] = record.record_id
            item["source_key"] = candidate.source_key
            results.append(item)
            if args.apply:
                status = str((item.get("data") or {}).get("status") or (item.get("error") or {}).get("code") or "unknown")
                print(f"[{len(results)}/{args.limit or '?'}] {record.record_id} {status}", file=sys.stderr, flush=True)
            if args.limit and len(results) >= args.limit:
                break
        if args.limit and len(results) >= args.limit:
            break

    if args.apply:
        _write_back(client, results, clear_attachments=args.clear_attachments)
    summary = {
        "mode": args.mode,
        "dry_run": not args.apply,
        "cutover_at": args.cutover_at,
        "records_scanned": len(records),
        "candidates": len(results),
        "success": sum(1 for item in results if item.get("success")),
        "failed": sum(1 for item in results if not item.get("success")),
        "results": results,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 1 if summary["failed"] else 0


def _write_back(client: FeishuBitableClient, results: list[dict[str, Any]], *, clear_attachments: bool) -> None:
    by_record: dict[str, list[dict[str, Any]]] = {}
    for item in results:
        by_record.setdefault(str(item.get("record_id") or ""), []).append(item)
    for record_id, items in by_record.items():
        successful = [item for item in items if item.get("success")]
        failed = [item for item in items if not item.get("success")]
        data_rows = [item.get("data") or {} for item in successful]
        completed = [row for row in data_rows if row.get("status") in {"completed", "already_imported"}]
        unbound = [row for row in data_rows if row.get("status") == "stored_unbound"]
        if failed:
            errors = [str(((item.get("error") or {}).get("message")) or "unknown error") for item in failed]
            client.update_record_fields(record_id, {"素材入库状态": "失败", "素材入库错误": "\n".join(errors)[:2000], "飞书附件状态": "保留中"})
            continue
        if unbound and not completed:
            row = unbound[0]
            client.update_record_fields(
                record_id,
                {
                    "素材入库状态": "待补信息",
                    "OSS对象ID": str(row.get("oss_object_id") or ""),
                    "OSS路径": str(row.get("object_key") or ""),
                    "素材入库错误": "缺少可确认的全球产品ID，视频已归档OSS但未进入可用素材池",
                    "飞书附件状态": "保留中",
                },
            )
            continue
        if completed:
            asset_ids = [str(row.get("asset_id") or "") for row in completed if row.get("asset_id")]
            object_ids = [str(row.get("oss_object_id") or "") for row in completed if row.get("oss_object_id")]
            object_keys = [str(row.get("object_key") or "") for row in completed if row.get("object_key")]
            preview = next((str(row.get("preview_url") or "") for row in completed if row.get("preview_url")), "")
            fields: dict[str, Any] = {
                "素材入库状态": "已入库",
                "素材ID": " / ".join(asset_ids),
                "OSS对象ID": " / ".join(object_ids),
                "OSS路径": " / ".join(object_keys),
                "素材入库时间": int(datetime.now().timestamp() * 1000),
                "素材入库错误": "",
                "飞书附件状态": "可清理" if not clear_attachments else "已清理",
            }
            if preview:
                fields["OSS预览"] = {"link": preview, "text": "OSS预览"}
            if clear_attachments:
                fields["生成视频"] = []
            client.update_record_fields(record_id, fields)


def _env_flag(key: str, default: bool) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    raise SystemExit(main())
