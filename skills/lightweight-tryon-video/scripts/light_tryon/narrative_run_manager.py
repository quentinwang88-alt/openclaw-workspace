from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

from .asset_ingestion import register_media_asset
from .database import LightTryonDB
from .run_manager_sync import SCRIPT_TYPE_LIGHT_VIDEO_SUPPLEMENT
from .utils import stable_hash


PROTECTED_RUN_STATUSES = {"已提交", "处理中", "生成中", "提交中", "部分提交", "已完成", "成功"}


def _attr(item: Any, name: str, default: Any = None) -> Any:
    return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)


def _records(client: Any) -> list[dict[str, Any]]:
    return [
        {"record_id": _attr(item, "record_id", ""), "fields": dict(_attr(item, "fields", {}) or {})}
        for item in client.list_records(page_size=500)
    ]


def _script_id(row: dict[str, Any]) -> str:
    fields = row.get("fields") or {}
    return str(fields.get("脚本ID") or fields.get("内容ID") or "").strip()


def _select_existing(rows: list[dict[str, Any]], bound_record_id: str = "") -> dict[str, Any]:
    """Prefer an already active record so a refresh can never create/rewrite a live submission."""
    def priority(row: dict[str, Any]) -> tuple[int, int, str]:
        fields = row.get("fields") or {}
        status = str(fields.get("状态") or "").strip()
        result_status = str(fields.get("结果回传状态") or "").strip().lower()
        protected = status in PROTECTED_RUN_STATUSES or result_status in {"uploaded", "observing", "submit_unconfirmed_observing"}
        bound = str(row.get("record_id") or "") == bound_record_id
        return (1 if protected else 0, 1 if bound else 0, str(row.get("record_id") or ""))

    return max(rows, key=priority)


def _upload_references(client: Any, paths: Iterable[str]) -> list[dict[str, str]]:
    uploaded: list[dict[str, str]] = []
    for raw in paths:
        path = Path(str(raw or "")).expanduser()
        if not path.is_file():
            continue
        content_type = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
        result = client.upload_attachment(
            path.read_bytes(), path.name, content_type, path.stat().st_size, parent_type="bitable_image",
        )
        uploaded.append({"file_token": str(result.get("file_token") or "")})
    if not uploaded:
        raise ValueError("补充镜头没有可上传的参考图")
    return uploaded


def sync_supplement_shots(
    db: LightTryonDB,
    run_client: Any,
    *,
    product_id: str,
    store_id: str = "myps01",
    channel: str = "即梦",
    model: str = "Seedance 2.0 VIP",
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_store = str(store_id or "").strip().lower()
    target_locale = (
        "th-TH"
        if normalized_store.startswith(("myps", "myalibaba", "thfz", "th"))
        else ""
    )
    shots = [
        shot for shot in db.list_product_supplement_shots(product_id)
        if str(shot.get("status") or "") in {"planned", "queued", "generating"}
    ]
    rows = _records(run_client)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if _script_id(row):
            grouped.setdefault(_script_id(row), []).append(row)
    product = db.get_product(product_id) or {}
    result: dict[str, Any] = {
        "candidates": len(shots), "created": 0, "updated": 0, "skipped": 0,
        "duplicate_records": 0, "items": [],
    }
    for shot in shots:
        prompt = shot.get("prompt_payload") or {}
        positive = str(prompt.get("positive_prompt") or "").strip()
        negative = str(prompt.get("negative_prompt") or "").strip()
        if not positive:
            result["items"].append({"shot_id": shot["shot_id"], "status": "blocked", "reason": "missing_prompt"})
            continue
        source_hash = stable_hash(shot["shot_id"], prompt, model, channel, store_id, length=24)
        qc = shot.get("qc_result") if isinstance(shot.get("qc_result"), dict) else {}
        candidates = grouped.get(shot["shot_id"], [])
        existing = _select_existing(candidates, str(qc.get("run_manager_record_id") or "")) if candidates else None
        duplicate_ids = [row["record_id"] for row in candidates if not existing or row["record_id"] != existing["record_id"]]
        result["duplicate_records"] += len(duplicate_ids)
        if existing and str(existing["fields"].get("来源指纹") or "") == source_hash:
            db.upsert_supplement_shot({**shot, "status": "queued", "qc_result": {
                **qc, "run_manager_record_id": existing["record_id"], "run_manager_source_hash": source_hash,
            }})
            result["skipped"] += 1
            result["items"].append({
                "shot_id": shot["shot_id"], "status": "skipped", "record_id": existing["record_id"],
                "duplicate_record_ids": duplicate_ids,
            })
            continue
        if existing:
            fields_now = existing.get("fields") or {}
            status_now = str(fields_now.get("状态") or "").strip()
            result_status_now = str(fields_now.get("结果回传状态") or "").strip().lower()
            if status_now in PROTECTED_RUN_STATUSES or result_status_now in {"uploaded", "observing", "submit_unconfirmed_observing"}:
                db.upsert_supplement_shot({**shot, "status": "generating", "qc_result": {
                    **qc, "run_manager_record_id": existing["record_id"],
                    "run_manager_source_hash": str(fields_now.get("来源指纹") or ""),
                }})
                result["skipped"] += 1
                result["items"].append({
                    "shot_id": shot["shot_id"], "status": "protected_active", "record_id": existing["record_id"],
                    "run_status": status_now, "duplicate_record_ids": duplicate_ids,
                })
                continue
        fields = {
            "任务名": (
                f"{product.get('product_name') or product_id}.SUP.{shot['shot_role']}."
                f"{str((shot.get('expected_tags') or {}).get('action_variant') or 'base')}"
            ),
            "内容ID": shot["shot_id"],
            "脚本ID": shot["shot_id"],
            "店铺ID": store_id,
            "目标语言": target_locale,
            "商品ID": product_id,
            "状态": "待处理",
            "提示词": f"{positive}\n\n负面要求：{negative}" if negative else positive,
            "免参考图": "否",
            "生成次数": 1,
            "模型": model,
            "视频比例": "9:16",
            "视频时长": 8,
            "分辨率": "720P",
            "渠道": channel,
            "任务来源": "口播增强补充镜头",
            "脚本类型": SCRIPT_TYPE_LIGHT_VIDEO_SUPPLEMENT,
            "首帧策略": "直接使用原始脚本参考图",
            "来源指纹": source_hash,
            "执行归属": "",
            "已提交次数": 0,
            "结果说明": "",
            "最新追踪ID": "",
            "结果回传状态": "",
            "错误信息": "",
        }
        if dry_run:
            result["items"].append({"shot_id": shot["shot_id"], "status": "would_update" if existing else "would_create"})
            continue
        fields["参考图"] = _upload_references(run_client, shot.get("reference_assets") or [])
        if existing:
            run_client.update_record_fields(existing["record_id"], fields)
            record_id = existing["record_id"]
            result["updated"] += 1
        else:
            ids = run_client.batch_create_records([{"fields": fields}])
            record_id = ids[0] if ids else ""
            result["created"] += 1
        db.upsert_supplement_shot({**shot, "status": "queued", "qc_result": {
            **qc, "run_manager_record_id": record_id, "run_manager_source_hash": source_hash,
        }})
        result["items"].append({
            "shot_id": shot["shot_id"], "status": "queued", "record_id": record_id,
            "duplicate_record_ids": duplicate_ids,
        })
    return result


def pull_supplement_results(
    db: LightTryonDB,
    run_client: Any,
    *,
    product_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    shots = {item["shot_id"]: item for item in db.list_product_supplement_shots(product_id)}
    output_root = Path(output_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "checked": 0, "returned": 0, "duplicate_returned": 0,
        "generating": 0, "failed": 0, "retry_planned": 0, "items": [],
    }
    duplicate_plan_versions: set[str] = set()
    for row in _records(run_client):
        fields = row["fields"]
        shot_id = str(fields.get("脚本ID") or fields.get("内容ID") or "").strip()
        shot = shots.get(shot_id)
        if not shot:
            continue
        result["checked"] += 1
        status = str(fields.get("状态") or "")
        result_status = str(fields.get("结果回传状态") or "")
        videos = fields.get("生成视频") if isinstance(fields.get("生成视频"), list) else []
        qc = shot.get("qc_result") if isinstance(shot.get("qc_result"), dict) else {}
        if result_status.lower() == "uploaded" and videos:
            source = videos[0]
            token = str(source.get("file_token") or "") if isinstance(source, dict) else ""
            if token and token == str(qc.get("run_manager_result_token") or "") and shot.get("output_asset_id"):
                result["items"].append({"shot_id": shot_id, "status": "skipped", "reason": "same_result"})
                continue
            content, _, _, _ = run_client.download_attachment_bytes(source)
            digest = hashlib.sha256(content).hexdigest()
            path = output_root / f"{shot_id}_{digest[:12]}.mp4"
            path.write_bytes(content)
            asset, created = register_media_asset(
                db, product_id, str(path), source_job_id=shot_id,
                source_type="ai_generated", expected_tags=shot.get("expected_tags") or {},
            )
            duplicate_of = str(asset.get("source_job_id") or "")
            is_duplicate_return = not created and duplicate_of and duplicate_of != shot_id
            if is_duplicate_return:
                variant = db.get_narrative_variant(str(shot.get("variant_id") or "")) or {}
                if variant.get("plan_version"):
                    duplicate_plan_versions.add(str(variant["plan_version"]))
                db.upsert_supplement_shot({
                    **shot,
                    "status": "duplicate_return",
                    "attempt_count": int(shot.get("attempt_count") or 0) + 1,
                    "output_asset_id": asset["asset_id"],
                    "last_error": f"DUPLICATE_RETURN:{duplicate_of}",
                    "qc_result": {
                        **qc,
                        "decision": "duplicate_return",
                        "duplicate_of_shot_id": duplicate_of,
                        "duplicate_asset_id": asset["asset_id"],
                        "run_manager_record_id": row["record_id"],
                        "run_manager_result_token": token,
                        "run_manager_trace_id": str(fields.get("最新追踪ID") or ""),
                        "file_sha256": digest,
                    },
                })
                result["duplicate_returned"] += 1
                result["items"].append({
                    "shot_id": shot_id,
                    "status": "duplicate_return",
                    "duplicate_of_shot_id": duplicate_of,
                    "asset_id": asset["asset_id"],
                })
            else:
                db.upsert_supplement_shot({**shot, "status": "received", "output_asset_id": asset["asset_id"], "qc_result": {
                    **qc, "run_manager_record_id": row["record_id"], "run_manager_result_token": token,
                    "run_manager_trace_id": str(fields.get("最新追踪ID") or ""), "file_sha256": digest,
                }})
                result["returned"] += 1
                result["items"].append({"shot_id": shot_id, "status": "returned", "asset_id": asset["asset_id"], "path": str(path)})
        elif status in {"失败", "阻塞"}:
            error = str(fields.get("错误信息") or fields.get("结果说明") or status)
            db.upsert_supplement_shot({**shot, "status": "failed", "last_error": error[:2000], "qc_result": {
                **qc, "run_manager_record_id": row["record_id"], "run_manager_trace_id": str(fields.get("最新追踪ID") or ""),
            }})
            result["failed"] += 1
            result["items"].append({"shot_id": shot_id, "status": status, "error": error[:2000]})
        else:
            db.upsert_supplement_shot({**shot, "status": "generating" if status != "待处理" else "queued", "qc_result": {
                **qc, "run_manager_record_id": row["record_id"], "run_manager_trace_id": str(fields.get("最新追踪ID") or ""),
            }})
            result["generating"] += 1
            result["items"].append({"shot_id": shot_id, "status": status or "待处理"})
    if duplicate_plan_versions:
        from .supplement_shots import plan_product_diversity_pool

        for version in sorted(duplicate_plan_versions):
            try:
                planned = plan_product_diversity_pool(
                    db,
                    product_id,
                    target_count=len(db.list_narrative_variants(product_id, plan_version=version)),
                    plan_version=version,
                )
                result["retry_planned"] += int(planned.get("created") or 0)
            except Exception as exc:
                result.setdefault("retry_plan_errors", []).append(str(exc))
    return result
