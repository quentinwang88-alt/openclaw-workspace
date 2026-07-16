from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

from .database import LightTryonDB
from .feishu_sync import GENERATION_CHANNEL_TO_BACKEND
from .prompting import PROMPT_BUILDER_VERSION, build_jimeng_record, build_prompt
from .utils import now_iso, stable_hash


RUN_MANAGER_FIELDS: tuple[dict[str, Any], ...] = (
    {"name": "任务名", "type": 1, "ui_type": "Text"},
    {"name": "内容ID", "type": 1, "ui_type": "Text"},
    {"name": "脚本ID", "type": 1, "ui_type": "Text"},
    {"name": "店铺ID", "type": 1, "ui_type": "Text"},
    {"name": "商品ID", "type": 1, "ui_type": "Text"},
    {"name": "状态", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": item} for item in ("待处理", "部分提交", "失败", "阻塞")]}},
    {"name": "提示词", "type": 1, "ui_type": "Text"},
    {"name": "参考图", "type": 17, "ui_type": "Attachment"},
    {"name": "免参考图", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": "否"}, {"name": "是"}]}},
    {"name": "生成次数", "type": 2, "ui_type": "Number"},
    {"name": "模型", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": item} for item in ("Seedance 2.0", "Seedance 2.0 VIP")]}},
    {"name": "视频比例", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": "9:16"}]}},
    {"name": "视频时长", "type": 2, "ui_type": "Number"},
    {"name": "分辨率", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": "720P"}]}},
    {"name": "渠道", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": item} for item in ("即梦", "iMini")]}},
    {"name": "任务来源", "type": 1, "ui_type": "Text"},
    {"name": "首帧策略", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": "直接使用原始脚本参考图"}]}},
    {"name": "来源指纹", "type": 1, "ui_type": "Text"},
    {"name": "执行归属", "type": 1, "ui_type": "Text"},
    {"name": "已提交次数", "type": 2, "ui_type": "Number"},
    {"name": "结果说明", "type": 1, "ui_type": "Text"},
    {"name": "最新追踪ID", "type": 1, "ui_type": "Text"},
    {"name": "结果回传状态", "type": 1, "ui_type": "Text"},
    {"name": "生成视频", "type": 17, "ui_type": "Attachment"},
    {"name": "错误信息", "type": 1, "ui_type": "Text"},
)

ACTIVE_STATUSES = {"待处理", "部分提交", "生成中", "提交中", "处理中"}
PATCHABLE_STATUSES = {"", "失败", "阻塞", "已提交", "已完成", "完成", "生成完成"}


def _attr(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _records(client: Any) -> list[dict[str, Any]]:
    return [
        {"record_id": _attr(item, "record_id", ""), "fields": dict(_attr(item, "fields", {}) or {})}
        for item in client.list_records(page_size=500)
    ]


def _group_review_records(client: Any) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _records(client):
        job_id = str(row["fields"].get("视频任务ID") or "").strip()
        if job_id:
            grouped.setdefault(job_id, []).append(row)
    return grouped


def _resolve_review_record(job: dict[str, Any], grouped: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    candidates = grouped.get(str(job.get("job_id") or ""), [])
    if not candidates:
        return None
    bound_record_id = str(job.get("feishu_review_record_id") or "")
    bound = next((item for item in candidates if item["record_id"] == bound_record_id), None)
    if bound:
        return bound
    if len(candidates) == 1:
        return candidates[0]
    raise ValueError(f"复核台存在 {len(candidates)} 条重复视频任务ID，且本地没有唯一绑定记录")


def ensure_run_manager_schema(client: Any, *, dry_run: bool = False) -> dict[str, Any]:
    existing = {_attr(item, "field_name", ""): item for item in client.list_fields()}
    actions: list[dict[str, Any]] = []
    for spec in RUN_MANAGER_FIELDS:
        current = existing.get(spec["name"])
        if current is None:
            actions.append({"operation": "create_field", "field": spec["name"], "type": spec["type"]})
            if not dry_run:
                client.create_field(spec["name"], spec["type"], spec["ui_type"], spec.get("property"))
            continue
        wanted = list((spec.get("property") or {}).get("options") or [])
        current_options = list((_attr(current, "property", {}) or {}).get("options") or [])
        current_names = {str(item.get("name") or "") for item in current_options}
        missing = [item for item in wanted if str(item.get("name") or "") not in current_names]
        if missing and int(spec["type"]) in {3, 4}:
            actions.append({"operation": "add_field_options", "field": spec["name"], "options": [item["name"] for item in missing]})
            if not dry_run:
                client.update_field(
                    _attr(current, "field_id"),
                    {**spec, "property": {"options": [*current_options, *missing]}},
                )
    return {"dry_run": dry_run, "actions": actions, "expected_field_count": len(RUN_MANAGER_FIELDS)}


def _duration(value: Any, fallback: int) -> int:
    text = str(value or "").strip().removesuffix("秒")
    if not text:
        return int(fallback)
    try:
        result = int(float(text))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"视频时长必须是 8 或 10，当前为: {value}") from exc
    if result not in {8, 10}:
        raise ValueError(f"视频时长只支持 8 或 10 秒，当前为: {result}")
    return result


def pull_generation_preferences(
    db: LightTryonDB,
    review_client: Any,
    *,
    job_ids: Iterable[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    selected = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    summary: dict[str, Any] = {"processed": 0, "updated": 0, "skipped": 0, "failed": 0, "errors": []}
    grouped = _group_review_records(review_client)
    for job_id, candidates in grouped.items():
        if selected and job_id not in selected:
            summary["skipped"] += 1
            continue
        job = db.get_job(job_id)
        if not job:
            summary["failed"] += 1
            summary["errors"].append(f"未知轻视频任务: {job_id}")
            continue
        try:
            record = _resolve_review_record(job, grouped)
            if not record:
                summary["skipped"] += 1
                continue
        except Exception as exc:
            summary["failed"] += 1
            summary["errors"].append(f"{job_id}: {exc}")
            continue
        fields = record["fields"]
        try:
            channel_display = str(fields.get("生成渠道") or "").strip()
            channel = GENERATION_CHANNEL_TO_BACKEND.get(channel_display, str(job.get("generation_channel") or "no_generate"))
            model = str(fields.get("生成模型") or job.get("generation_model") or "Seedance 2.0").strip()
            duration = _duration(fields.get("视频时长", fields.get("目标时长")), int(job.get("duration_seconds") or 8))
            rerun = bool(fields.get("重新提交生成"))
            changed = (
                channel != str(job.get("generation_channel") or "no_generate")
                or model != str(job.get("generation_model") or "Seedance 2.0")
                or duration != int(job.get("duration_seconds") or 8)
                or rerun
            )
            if changed:
                if not dry_run:
                    db.set_generation_preferences(job_id, channel=channel, model=model, duration_seconds=duration, rerun=rerun)
                    if model != str(job.get("generation_model") or "Seedance 2.0") or duration != int(job.get("duration_seconds") or 8):
                        payload = build_prompt(db.get_job_context(job_id))
                        db.update_prompt(job_id, payload, PROMPT_BUILDER_VERSION)
                summary["updated"] += 1
            else:
                summary["skipped"] += 1
            summary["processed"] += 1
        except Exception as exc:
            summary["failed"] += 1
            summary["errors"].append(f"{job_id}: {exc}")
            if not dry_run:
                review_client.update_record_fields(record["record_id"], {"队列同步状态": "阻塞", "队列错误信息": str(exc)[:2000]})
    return summary


def _upload_source_product_images(run_client: Any, product: dict[str, Any]) -> list[dict[str, str]]:
    uploaded_images: list[dict[str, str]] = []
    for raw_path in product.get("product_images") or []:
        image_path = Path(str(raw_path or "")).expanduser()
        if not image_path.is_file():
            continue
        suffix = image_path.suffix.lower()
        content_type = "image/png" if suffix == ".png" else "image/jpeg"
        uploaded = run_client.upload_attachment(
            image_path.read_bytes(), image_path.name, content_type, image_path.stat().st_size, parent_type="bitable_image",
        )
        uploaded_images.append({"file_token": str(uploaded.get("file_token") or "")})
    if not uploaded_images:
        raise ValueError("原始脚本表产品参考图没有可读取的本地缓存，无法同步到运行管理表")
    return uploaded_images


def sync_jobs_to_run_manager(
    db: LightTryonDB,
    review_client: Any,
    run_client: Any,
    *,
    job_ids: Iterable[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    store_id: str = "myps01",
) -> dict[str, Any]:
    selected = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    review_groups = _group_review_records(review_client)
    run_rows = _records(run_client)
    run_by_job: dict[str, dict[str, Any]] = {}
    for row in run_rows:
        key = str(row["fields"].get("脚本ID") or row["fields"].get("内容ID") or "").strip()
        if key:
            if key in run_by_job:
                raise ValueError(f"运行管理表存在重复轻视频脚本ID: {key}")
            run_by_job[key] = row
    jobs = [
        job for job in db.list_jobs()
        if str(job.get("generation_channel") or "no_generate") != "no_generate"
        and (not selected or job["job_id"] in selected)
    ]
    if limit is not None:
        jobs = jobs[: max(0, int(limit))]
    summary: dict[str, Any] = {"candidates": len(jobs), "created": 0, "updated": 0, "skipped": 0, "blocked": 0, "failed": 0, "items": []}
    for job in jobs:
        job_id = job["job_id"]
        review: dict[str, Any] | None = None
        try:
            review = _resolve_review_record(job, review_groups)
            if not review:
                raise ValueError("轻视频复核表不存在该任务，请先执行 push-review")
            if not job.get("prompt_payload") or job.get("prompt_version") == "unbuilt":
                raise ValueError("Prompt 尚未构建")
            model = str(job.get("generation_model") or "Seedance 2.0")
            duration = int(job.get("duration_seconds") or 8)
            if model not in {"Seedance 2.0", "Seedance 2.0 VIP"} or duration not in {8, 10}:
                raise ValueError(f"不支持的硬约束组合: {model} / {duration}秒")
            product = db.get_product(job["product_id"]) or {}
            target_store_id = str(store_id or "").strip()
            if not target_store_id:
                raise ValueError("运行管理表店铺ID配置为空")
            source_hash = stable_hash(
                job_id, job.get("prompt_payload"), model, duration, job.get("generation_channel"),
                product.get("product_images") or [], target_store_id, length=24,
            )
            existing = run_by_job.get(job_id)
            rerun = bool(job.get("generation_rerun"))
            if existing and str(existing["fields"].get("来源指纹") or "") == source_hash and not rerun:
                db.update_run_manager_sync(job_id, record_id=existing["record_id"], status="queued", source_hash=source_hash)
                summary["skipped"] += 1
                summary["items"].append({"job_id": job_id, "status": "skipped", "reason": "unchanged"})
                continue
            existing_status = str((existing or {}).get("fields", {}).get("状态") or "")
            unclaimed_pending = bool(
                existing
                and existing_status == "待处理"
                and not str(existing["fields"].get("执行归属") or "").strip()
                and int(float(existing["fields"].get("已提交次数") or 0)) == 0
                and not str(existing["fields"].get("最新追踪ID") or "").strip()
            )
            if existing and existing_status in ACTIVE_STATUSES and not unclaimed_pending:
                db.update_run_manager_sync(job_id, record_id=existing["record_id"], status="blocked", error="已有运行中的同任务，禁止重复提交")
                review_client.update_record_fields(review["record_id"], {"队列同步状态": "阻塞", "队列错误信息": "已有运行中的同任务，禁止重复提交"})
                summary["blocked"] += 1
                summary["items"].append({"job_id": job_id, "status": "blocked", "reason": "active_duplicate"})
                continue
            if existing and existing_status not in PATCHABLE_STATUSES and not unclaimed_pending:
                raise ValueError(f"现有运行任务状态不可重置: {existing['fields'].get('状态')}")
            context = db.get_job_context(job_id)
            base = build_jimeng_record(context, job["prompt_payload"])
            fields = {
                **base,
                "脚本ID": job_id,
                "店铺ID": target_store_id,
                "任务来源": "轻量试穿视频",
                "首帧策略": "直接使用原始脚本参考图",
                "来源指纹": source_hash,
                "模型": model,
                "视频时长": duration,
                "分辨率": "720P",
                "免参考图": "否",
                "渠道": {"jimeng": "即梦", "imini": "iMini", "auto": ""}.get(str(job.get("generation_channel")), ""),
                "状态": "待处理",
                "执行归属": "",
                "已提交次数": 0,
                "结果说明": "",
                "最新追踪ID": "",
                "结果回传状态": "",
                "错误信息": "",
            }
            allowed_fields = {spec["name"] for spec in RUN_MANAGER_FIELDS}
            fields = {key: value for key, value in fields.items() if key in allowed_fields}
            if not fields.get("渠道"):
                fields.pop("渠道", None)
            fields.pop("参考图", None)
            if dry_run:
                summary["items"].append({"job_id": job_id, "status": "would_update" if existing else "would_create", "fields": {key: value for key, value in fields.items() if key != "提示词"}})
                continue
            fields["参考图"] = _upload_source_product_images(run_client, product)
            if existing:
                run_client.update_record_fields(existing["record_id"], fields)
                record_id = existing["record_id"]
                summary["updated"] += 1
            else:
                record_ids = run_client.batch_create_records([{"fields": fields}])
                record_id = record_ids[0] if record_ids else ""
                summary["created"] += 1
            db.update_run_manager_sync(job_id, record_id=record_id, status="queued", source_hash=source_hash, clear_rerun=True)
            review_client.update_record_fields(review["record_id"], {
                "运行表记录ID": record_id, "队列同步状态": "已入队", "队列错误信息": "", "重新提交生成": False,
            })
            summary["items"].append({"job_id": job_id, "status": "updated" if existing else "created", "record_id": record_id})
        except Exception as exc:
            message = str(exc)[:2000]
            if not dry_run:
                db.update_run_manager_sync(job_id, status="failed", error=message)
                if review:
                    review_client.update_record_fields(review["record_id"], {"队列同步状态": "失败", "队列错误信息": message})
            summary["failed"] += 1
            summary["items"].append({"job_id": job_id, "status": "failed", "error": message})
    return summary


def pull_run_manager_results(
    db: LightTryonDB,
    review_client: Any,
    run_client: Any,
    *,
    job_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    selected = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    review_groups = _group_review_records(review_client)
    summary: dict[str, Any] = {"checked": 0, "returned": 0, "generating": 0, "failed": 0, "skipped": 0, "items": []}
    content_hash_owners: dict[str, str] = {}
    for row in _records(run_client):
        fields = row["fields"]
        job_id = str(fields.get("脚本ID") or fields.get("内容ID") or "").strip()
        if not job_id or (selected and job_id not in selected):
            continue
        job = db.get_job(job_id)
        if not job or str(job.get("generation_channel") or "no_generate") == "no_generate":
            summary["skipped"] += 1
            continue
        summary["checked"] += 1
        trace_id = str(fields.get("最新追踪ID") or "")
        status = str(fields.get("状态") or "")
        result_status = str(fields.get("结果回传状态") or "")
        videos = fields.get("生成视频") if isinstance(fields.get("生成视频"), list) else []
        try:
            review = _resolve_review_record(job, review_groups)
            if not review:
                raise ValueError("轻视频复核表不存在该任务")
            if result_status.lower() == "uploaded" and videos:
                source = videos[0]
                source_file_token = str(source.get("file_token") or "").strip() if isinstance(source, dict) else ""
                existing_sources = {
                    str(item.get("source_file_token") or "").strip()
                    for item in (job.get("raw_video_attachments") or [])
                    if isinstance(item, dict)
                }
                if (
                    str(job.get("run_manager_sync_status") or "") == "returned"
                    and source_file_token
                    and source_file_token in existing_sources
                ):
                    summary["skipped"] += 1
                    continue
                content, file_name, content_type, size = run_client.download_attachment_bytes(source)
                source_sha256 = hashlib.sha256(content).hexdigest()
                duplicate_owner = content_hash_owners.get(source_sha256)
                if duplicate_owner and duplicate_owner != job_id:
                    raise ValueError(f"生成视频内容与任务 {duplicate_owner} 完全重复，拒绝错误回流")
                content_hash_owners[source_sha256] = job_id
                uploaded = review_client.upload_attachment(content, file_name, content_type or "video/mp4", size, parent_type="bitable_file")
                attachments = [{
                    **uploaded,
                    "source_file_token": source_file_token,
                    "source_sha256": source_sha256,
                    "source_name": str(source.get("name") or file_name) if isinstance(source, dict) else file_name,
                }]
                review_client.update_record_fields(review["record_id"], {
                    "初始成片": [{"file_token": uploaded["file_token"]}], "生成状态": "生成成功", "生成失败原因": "",
                    "队列同步状态": "已回流", "队列错误信息": "", "最新追踪ID": trace_id,
                })
                db.set_run_manager_result(job_id, attachments=attachments, trace_id=trace_id)
                summary["returned"] += 1
                summary["items"].append({"job_id": job_id, "status": "returned", "trace_id": trace_id})
            elif status in {"失败", "阻塞"}:
                error = str(fields.get("错误信息") or fields.get("结果说明") or status)
                db.update_run_manager_sync(job_id, record_id=row["record_id"], status="failed" if status == "失败" else "blocked", error=error, trace_id=trace_id, result_status=result_status)
                review_client.update_record_fields(review["record_id"], {"生成状态": "生成失败", "生成失败原因": error[:2000], "队列同步状态": status, "队列错误信息": error[:2000], "最新追踪ID": trace_id})
                summary["failed"] += 1
                summary["items"].append({"job_id": job_id, "status": status, "error": error[:2000]})
            else:
                db.update_run_manager_sync(job_id, record_id=row["record_id"], status="generating", trace_id=trace_id, result_status=result_status)
                review_client.update_record_fields(review["record_id"], {"生成状态": "生成中", "队列同步状态": "生成中", "最新追踪ID": trace_id})
                summary["generating"] += 1
        except Exception as exc:
            error = str(exc)[:2000]
            db.update_run_manager_sync(job_id, record_id=row["record_id"], status="failed", error=error, trace_id=trace_id)
            review_client.update_record_fields(review["record_id"], {"队列同步状态": "失败", "队列错误信息": error})
            summary["failed"] += 1
            summary["items"].append({"job_id": job_id, "status": "failed", "error": error})
    return summary
