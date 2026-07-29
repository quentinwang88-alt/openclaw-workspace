#!/usr/bin/env python3
"""Audit and optionally replace postprocessed light-review assets with clean initials."""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
for path in (ROOT, WORKSPACE / "skills" / "script-run-manager-sync"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from core.bitable import FeishuBitableClient  # type: ignore  # noqa: E402

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.run_manager_material_import_skill import RunManagerMaterialImportSkill  # noqa: E402


APP_TOKEN = "ZukCb6jNya0pUMsqb33cc4Ujnmb"
TABLE_ID = "tblOzefH1pgI7K9U"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product-id")
    parser.add_argument("--record-id")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    client = FeishuBitableClient(app_token=APP_TOKEN, table_id=TABLE_ID)
    importer = RunManagerMaterialImportSkill(
        ctx,
        app_token=APP_TOKEN,
        table_id=TABLE_ID,
        source_system="light_video_review",
    )
    result: dict[str, Any] = {
        "dry_run": not args.apply,
        "records_scanned": 0,
        "affected": 0,
        "already_repaired": 0,
        "raw_already_imported": 0,
        "raw_imported": 0,
        "quarantined_final_assets": 0,
        "failed": 0,
        "items": [],
    }
    result["inventory"] = _inventory(ctx, args.product_id)
    operations = []
    for record in client.list_records(page_size=500):
        if args.record_id and record.record_id != args.record_id:
            continue
        fields = dict(record.fields or {})
        if args.product_id and _text(fields.get("商品ID") or fields.get("产品ID")) != args.product_id:
            continue
        result["records_scanned"] += 1
        raw_candidates = importer.inspect_light_review_record(record.record_id, fields)
        if not raw_candidates or raw_candidates[0].source_stage != "raw_initial":
            continue
        former_final_candidates = _former_final_candidates(importer, record.record_id, fields)
        final_tokens = {
            str(row.get("file_token") or "")
            for row in (fields.get("最终视频") or [])
            if isinstance(row, dict) and row.get("file_token")
        }
        matching_final_registries = []
        impacted = []
        former_by_token = {
            str(candidate.attachment.get("file_token") or ""): candidate
            for candidate in former_final_candidates
        }
        registries = ctx.repo.list_where(
            "material_source_registry",
            "source_system='light_video_review' AND source_record_id=?",
            (record.record_id,),
        )
        for registry in registries:
            token = str(registry.get("attachment_token") or "")
            if (
                registry.get("ingest_status") == "completed"
                and registry.get("asset_id")
                and token in final_tokens
                and token in former_by_token
            ):
                matching_final_registries.append(registry)
                asset_id = str(registry.get("asset_id") or "")
                asset = ctx.repo.get("assets", "asset_id", asset_id) or {}
                segments = ctx.repo.list_where("segments", "asset_id=?", (asset_id,))
                fully_quarantined = (
                    str(asset.get("asset_status") or "") == "blocked"
                    and all(str(row.get("segment_status") or "") == "archived" for row in segments)
                )
                if not fully_quarantined:
                    impacted.append(registry)
        if not matching_final_registries:
            continue
        raw_candidate = raw_candidates[0]
        raw_registry = ctx.repo.get("material_source_registry", "source_key", raw_candidate.source_key) or {}
        raw_complete = str(raw_registry.get("ingest_status") or "") == "completed"
        if raw_complete and not impacted:
            result["already_repaired"] += 1
            continue
        result["affected"] += 1
        item = {
            "record_id": record.record_id,
            "product_id": raw_candidate.canonical_product_id or raw_candidate.product_id,
            "raw_status": raw_registry.get("ingest_status") or "not_imported",
            "final_asset_ids": [str(row.get("asset_id") or "") for row in impacted],
            "action": "would_import_raw_and_quarantine_final",
        }
        if not args.apply:
            result["items"].append(item)
        else:
            operations.append((raw_candidate, impacted, item))
        if args.limit and result["affected"] >= args.limit:
            break

    if args.apply and operations:
        with ThreadPoolExecutor(max_workers=max(1, min(4, int(args.workers)))) as pool:
            futures = {
                pool.submit(_repair_one, raw_candidate, impacted, item): item
                for raw_candidate, impacted, item in operations
            }
            for future in as_completed(futures):
                repaired = future.result()
                result["items"].append(repaired)
                if repaired["action"] == "raw_import_failed":
                    result["failed"] += 1
                    continue
                result["raw_already_imported" if repaired["raw_import_status"] == "already_imported" else "raw_imported"] += 1
                result["quarantined_final_assets"] += len(repaired.get("final_asset_ids") or [])

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 1 if result["failed"] else 0


def _former_final_candidates(
    importer: RunManagerMaterialImportSkill,
    record_id: str,
    fields: dict[str, Any],
):
    final_video = fields.get("最终视频")
    if not final_video:
        return []
    adapted = dict(fields)
    adapted.update(
        {
            "结果回传状态": "uploaded",
            "生成视频": final_video,
            "产品ID": fields.get("商品ID") or fields.get("产品ID") or "",
            "全球产品ID": fields.get("全球产品ID") or fields.get("商品ID") or "",
            "任务来源": "轻视频复核",
            "脚本ID": fields.get("视频任务ID") or "",
            "渠道": fields.get("生成渠道") or "",
            "模型": fields.get("生成模型") or "",
            "完成时间": fields.get("复核处理时间") or fields.get("最后修改时间") or fields.get("飞书记录创建时间") or "",
        }
    )
    return importer.inspect_record(record_id, adapted)


def _repair_one(raw_candidate: Any, impacted: list[dict[str, Any]], item: dict[str, Any]) -> dict[str, Any]:
    ctx = build_context()
    client = FeishuBitableClient(app_token=APP_TOKEN, table_id=TABLE_ID)
    importer = RunManagerMaterialImportSkill(
        ctx,
        app_token=APP_TOKEN,
        table_id=TABLE_ID,
        source_system="light_video_review",
    )
    imported = importer.import_candidate(client, raw_candidate, dry_run=False)
    status = str((imported.data or {}).get("status") or "") if imported.success else ""
    if not imported.success or status not in {"completed", "already_imported"}:
        return {**item, "action": "raw_import_failed", "error": imported.to_dict()}
    for registry in impacted:
        _quarantine_final_asset(ctx, registry)
    return {
        **item,
        "raw_asset_id": str((imported.data or {}).get("asset_id") or ""),
        "raw_import_status": status,
        "action": "raw_imported_final_quarantined",
    }


def _quarantine_final_asset(ctx: Any, registry: dict[str, Any]) -> None:
    asset_id = str(registry.get("asset_id") or "")
    asset = ctx.repo.get("assets", "asset_id", asset_id) or {}
    segments = ctx.repo.list_where("segments", "asset_id=?", (asset_id,))
    payload = dict(registry.get("source_payload_json") or {})
    if "postprocessed_quarantine" not in payload:
        payload["postprocessed_quarantine"] = {
            "asset_status_before": asset.get("asset_status"),
            "human_review_status_before": asset.get("human_review_status"),
            "segment_statuses_before": {
                str(row.get("segment_id") or ""): row.get("segment_status") for row in segments
            },
        }
    payload["source_stage"] = "postprocessed_final"
    ctx.repo.update(
        "assets",
        "asset_id",
        asset_id,
        {"asset_status": "blocked", "human_review_status": "superseded_postprocessed"},
    )
    for segment in segments:
        ctx.repo.update("segments", "segment_id", segment["segment_id"], {"segment_status": "archived"})
    ctx.repo.upsert(
        "material_source_registry",
        "source_key",
        {**registry, "legacy_flag": 1, "source_payload_json": payload},
    )


def _text(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or value.get("value") or "").strip()
    if isinstance(value, list):
        return " / ".join(filter(None, (_text(item) for item in value)))
    return str(value or "").strip()


def _inventory(ctx: Any, product_id: str | None) -> dict[str, Any]:
    registry_where = "source_system='light_video_review'"
    registry_params: tuple[Any, ...] = ()
    if product_id:
        registry_where += " AND (product_id=? OR canonical_product_id=?)"
        registry_params = (product_id, product_id)
    registries = ctx.repo.list_where("material_source_registry", registry_where, registry_params)
    registry_by_asset = {
        str(row.get("asset_id") or ""): row for row in registries if row.get("asset_id")
    }
    asset_where = "source_flow='light_video'"
    asset_params: tuple[Any, ...] = ()
    if product_id:
        asset_where += " AND (product_id=? OR canonical_product_id=?)"
        asset_params = (product_id, product_id)
    assets = ctx.repo.list_where("assets", asset_where, asset_params)
    source_stage_counts: dict[str, int] = {}
    for registry in registries:
        payload = registry.get("source_payload_json") or {}
        stage = str(payload.get("source_stage") or "unknown") if isinstance(payload, dict) else "unknown"
        source_stage_counts[stage] = source_stage_counts.get(stage, 0) + 1
    rows = []
    stage_asset_status_counts: dict[str, int] = {}
    stage_segment_status_counts: dict[str, int] = {}
    stage_tagged_segment_counts: dict[str, int] = {}
    for asset in assets:
        asset_id = str(asset.get("asset_id") or "")
        registry = registry_by_asset.get(asset_id) or {}
        payload = registry.get("source_payload_json") or {}
        stage = str(payload.get("source_stage") or "unregistered") if isinstance(payload, dict) else "unregistered"
        asset_status = str(asset.get("asset_status") or "unknown")
        stage_asset_status_counts[f"{stage}:{asset_status}"] = stage_asset_status_counts.get(f"{stage}:{asset_status}", 0) + 1
        segments = ctx.repo.list_where("segments", "asset_id=?", (asset_id,))
        for segment in segments:
            segment_status = str(segment.get("segment_status") or "unknown")
            status_key = f"{stage}:{segment_status}"
            stage_segment_status_counts[status_key] = stage_segment_status_counts.get(status_key, 0) + 1
            if ctx.repo.list_where("segment_tags", "segment_id=? LIMIT 1", (segment.get("segment_id"),)):
                stage_tagged_segment_counts[stage] = stage_tagged_segment_counts.get(stage, 0) + 1
        rows.append(
            {
                "asset_id": asset_id,
                "source_identity": asset.get("source_identity"),
                "source_record_id": asset.get("source_record_id"),
                "generation_type": asset.get("generation_type"),
                "asset_status": asset_status,
                "registry_source_stage": stage,
                "registry_file_name": registry.get("file_name"),
                "segment_count": len(segments),
            }
        )
    return {
        "registry_count": len(registries),
        "registry_source_stage_counts": source_stage_counts,
        "light_asset_count": len(assets),
        "assets_without_light_review_registry": sum(1 for asset in assets if str(asset.get("asset_id") or "") not in registry_by_asset),
        "stage_asset_status_counts": stage_asset_status_counts,
        "stage_segment_status_counts": stage_segment_status_counts,
        "stage_tagged_segment_counts": stage_tagged_segment_counts,
        "assets": rows[:100],
    }


if __name__ == "__main__":
    raise SystemExit(main())
