from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path
from typing import Any, Iterable

from .database import LightTryonDB
from .feishu_sync import GENERATION_CHANNEL_TO_BACKEND
from .prompting import PROMPT_BUILDER_VERSION, build_jimeng_record, build_prompt
from .utils import now_iso, stable_hash


SCRIPT_TYPE_ORIGINAL = "原创脚本"
SCRIPT_TYPE_SHORT_VIDEO_REMAKE = "短视频复刻脚本"
SCRIPT_TYPE_NURTURE = "养号脚本"
SCRIPT_TYPE_LIGHT_VIDEO = "轻视频脚本"
SCRIPT_TYPE_LIGHT_VIDEO_SUPPLEMENT = "轻视频补素材脚本"
RUN_MANAGER_SCRIPT_TYPE_OPTIONS = (
    SCRIPT_TYPE_ORIGINAL,
    SCRIPT_TYPE_SHORT_VIDEO_REMAKE,
    SCRIPT_TYPE_NURTURE,
    SCRIPT_TYPE_LIGHT_VIDEO,
    SCRIPT_TYPE_LIGHT_VIDEO_SUPPLEMENT,
)


RUN_MANAGER_FIELDS: tuple[dict[str, Any], ...] = (
    {"name": "任务名", "type": 1, "ui_type": "Text"},
    {"name": "内容ID", "type": 1, "ui_type": "Text"},
    {"name": "脚本ID", "type": 1, "ui_type": "Text"},
    {"name": "店铺ID", "type": 1, "ui_type": "Text"},
    {"name": "商品ID", "type": 1, "ui_type": "Text"},
    {"name": "全球产品ID", "type": 1, "ui_type": "Text"},
    {"name": "目标语言", "type": 1, "ui_type": "Text"},
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
    {"name": "脚本类型", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": item} for item in RUN_MANAGER_SCRIPT_TYPE_OPTIONS]}},
    {"name": "首帧策略", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": "直接使用原始脚本参考图"}]}},
    {"name": "来源指纹", "type": 1, "ui_type": "Text"},
    {"name": "执行归属", "type": 1, "ui_type": "Text"},
    {"name": "已提交次数", "type": 2, "ui_type": "Number"},
    {"name": "结果说明", "type": 1, "ui_type": "Text"},
    {"name": "最新追踪ID", "type": 1, "ui_type": "Text"},
    {"name": "结果回传状态", "type": 1, "ui_type": "Text"},
    {"name": "生成视频", "type": 17, "ui_type": "Attachment"},
    {"name": "素材入库状态", "type": 3, "ui_type": "SingleSelect", "property": {"options": [{"name": item} for item in ("未处理", "入库中", "已入库", "待补信息", "失败", "已跳过")]}},
    {"name": "素材ID", "type": 1, "ui_type": "Text"},
    {"name": "OSS对象ID", "type": 1, "ui_type": "Text"},
    {"name": "OSS路径", "type": 1, "ui_type": "Text"},
    {"name": "错误信息", "type": 1, "ui_type": "Text"},
)

ACTIVE_STATUSES = {"待处理", "部分提交", "已提交", "生成中", "提交中", "处理中"}
PATCHABLE_STATUSES = {"", "失败", "阻塞", "已完成", "完成", "生成完成"}
TERMINAL_RESULT_FAILURE_MARKERS = (
    "failed", "failure", "missing_asset", "rejected", "审核失败", "复核失败", "缺素材",
)


class DuplicateRunManagerIDError(ValueError):
    """Raised when one canonical light-video ID maps to multiple run rows."""


def run_record_uses_voiceover(fields: dict[str, Any]) -> bool:
    """Return True when a run-manager record belongs to the voiceover path.

    Voiceover finals deliberately keep only the mixed narration plus the lowered
    original sound. They must never be routed into the independent BGM renderer.
    """
    requested = fields.get("是否配口播")
    if requested is True or requested == 1:
        return True
    if str(requested or "").strip().lower() in {"true", "yes", "on", "是", "勾选"}:
        return True
    voiceover_status = str(fields.get("口播状态") or "").strip()
    return voiceover_status not in {"", "未启用", "不配口播", "无需口播"}


def _attr(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _records(client: Any) -> list[dict[str, Any]]:
    return [
        {"record_id": _attr(item, "record_id", ""), "fields": dict(_attr(item, "fields", {}) or {})}
        for item in client.list_records(page_size=500)
    ]


def _update_remote_if_changed(client: Any, row: dict[str, Any], desired: dict[str, Any]) -> bool:
    current = row.get("fields") or {}
    changed = {key: value for key, value in desired.items() if current.get(key) != value}
    if not changed:
        return False
    client.update_record_fields(row["record_id"], changed)
    current.update(changed)
    return True


def _local_run_sync_matches(
    job: dict[str, Any], *, record_id: str, status: str, error: str = "",
    trace_id: str = "", result_status: str = "", source_hash: str = "",
) -> bool:
    return bool(
        str(job.get("run_manager_record_id") or "") == str(record_id or "")
        and str(job.get("run_manager_sync_status") or "") == str(status or "")
        and str(job.get("run_manager_sync_error") or "") == str(error or "")
        and (not trace_id or str(job.get("run_manager_trace_id") or "") == str(trace_id))
        and (not result_status or str(job.get("run_manager_result_status") or "") == str(result_status))
        and (not source_hash or str(job.get("run_manager_source_hash") or "") == str(source_hash))
    )


def _oss_source_enabled() -> bool:
    return str(os.environ.get("LIGHT_VIDEO_OSS_SOURCE_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _run_result_has_video(fields: dict[str, Any]) -> bool:
    videos = fields.get("生成视频")
    if isinstance(videos, list) and any(isinstance(item, dict) and item.get("file_token") for item in videos):
        return True
    return _oss_source_enabled() and bool(str(fields.get("OSS对象ID") or fields.get("OSS路径") or "").strip())


def _result_status_is_terminal_failure(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return bool(normalized and any(marker in normalized for marker in TERMINAL_RESULT_FAILURE_MARKERS))


def _download_oss_video(fields: dict[str, Any]) -> tuple[bytes, str, str, int]:
    root = Path(__file__).resolve().parents[4] / "auto_mixcut"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from auto_mixcut.core.bootstrap import build_context
    from auto_mixcut.core.storage_paths import resolve_oss_object_path

    ctx = build_context()
    object_id = str(fields.get("OSS对象ID") or "").strip().split(" / ", 1)[0]
    if object_id:
        resolved = resolve_oss_object_path(ctx, object_id, "light_run_manager")
        if not resolved.success:
            message = resolved.error.message if resolved.error else "OSS object resolve failed"
            raise RuntimeError(message)
        path = Path(str(resolved.data["path"]))
    else:
        object_key = str(fields.get("OSS路径") or "").strip().split(" / ", 1)[0]
        if not object_key:
            raise ValueError("运行表没有可用的OSS对象")
        path = ctx.settings.temp_root / "light_run_manager" / Path(object_key).name
        downloaded = ctx.oss.download(object_key, path)
        if not downloaded.success:
            message = downloaded.error.message if downloaded.error else "OSS download failed"
            raise RuntimeError(message)
    content = path.read_bytes()
    if not content:
        raise ValueError("OSS视频为空")
    return content, path.name or "generated.mp4", "video/mp4", len(content)


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


def _group_run_records(client: Any) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _records(client):
        identity_values = {
            str(row["fields"].get(field_name) or "").strip()
            for field_name in ("脚本ID", "内容ID", "任务名")
        } - {""}
        for job_id in identity_values:
            grouped.setdefault(job_id, []).append(row)
    return grouped


def _resolve_run_record(
    job: dict[str, Any],
    grouped: dict[str, list[dict[str, Any]]],
    *,
    prefer_result: bool = False,
) -> dict[str, Any] | None:
    candidates = grouped.get(str(job.get("job_id") or ""), [])
    if not candidates:
        return None
    expected_id = str(job.get("job_id") or "").strip()
    for item in candidates:
        fields = item.get("fields") or {}
        mismatches = {
            field_name: str(fields.get(field_name) or "").strip()
            for field_name in ("任务名", "内容ID", "脚本ID")
            if str(fields.get(field_name) or "").strip() != expected_id
        }
        if mismatches:
            raise DuplicateRunManagerIDError(
                f"运行管理表身份字段不一致，已停止入队和回流: {expected_id} / "
                f"{item.get('record_id')} / {mismatches}"
            )
    if len(candidates) > 1:
        record_ids = ", ".join(str(item.get("record_id") or "") for item in candidates)
        raise DuplicateRunManagerIDError(
            f"运行管理表存在 {len(candidates)} 条重复轻视频内容ID，已停止入队和回流: "
            f"{job.get('job_id')} ({record_ids})"
        )
    bound_record_id = str(job.get("run_manager_record_id") or "").strip()
    bound = next((item for item in candidates if item["record_id"] == bound_record_id), None)
    if prefer_result:
        ready = [
            item for item in candidates
            if str(item["fields"].get("结果回传状态") or "").lower() == "uploaded"
            and _run_result_has_video(item["fields"])
        ]
        if len(ready) == 1:
            return ready[0]
    if bound:
        return bound
    if len(candidates) == 1:
        return candidates[0]
    return None


def _persisted_content_hash_owners(db: LightTryonDB) -> dict[str, str]:
    owners: dict[str, str] = {}
    for item in db.list_jobs():
        job_id = str(item.get("job_id") or "").strip()
        digest = str(item.get("run_manager_result_sha256") or "").strip()
        if not digest:
            for attachment in item.get("raw_video_attachments") or []:
                if isinstance(attachment, dict):
                    digest = str(attachment.get("source_sha256") or "").strip()
                    if digest:
                        break
        if job_id and digest:
            owners.setdefault(digest, job_id)
    return owners


def _quarantine_unclaimed_duplicate_rows(
    run_client: Any,
    candidates: Iterable[dict[str, Any]],
    message: str,
) -> int:
    """Stop only duplicate rows that have not been claimed or produced output."""
    quarantined = 0
    for item in candidates:
        fields = item.get("fields") or {}
        status = str(fields.get("状态") or "").strip()
        already_quarantined = status == "阻塞" and "重复轻视频内容ID" in str(
            fields.get("错误信息") or fields.get("结果说明") or ""
        )
        claimed = bool(
            str(fields.get("最新追踪ID") or "").strip()
            or int(float(fields.get("已提交次数") or 0)) > 0
        )
        if already_quarantined or claimed or _run_result_has_video(fields) or status not in {"", "待处理", "失败", "阻塞"}:
            continue
        quarantine_fields = {
            "状态": "阻塞",
            "错误信息": message[:2000],
            "结果说明": "检测到重复轻视频内容ID；未领取记录已自动隔离，等待去重",
        }
        run_client.update_record_fields(item["record_id"], quarantine_fields)
        fields.update(quarantine_fields)
        quarantined += 1
    return quarantined


def _run_row_is_protected(fields: dict[str, Any]) -> bool:
    return bool(
        _run_result_has_video(fields)
        or str(fields.get("最新追踪ID") or "").strip()
        or int(float(fields.get("已提交次数") or 0)) > 0
    )


def _select_duplicate_keeper(
    job: dict[str, Any], candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    valid_results = [
        item for item in candidates
        if _run_result_has_video(item.get("fields") or {})
        and str((item.get("fields") or {}).get("结果回传状态") or "").strip().lower() == "uploaded"
    ]
    if len(valid_results) == 1:
        active_others = []
        for item in candidates:
            if item is valid_results[0]:
                continue
            fields = item.get("fields") or {}
            result_status = str(fields.get("结果回传状态") or "").strip().lower()
            submitted = bool(
                str(fields.get("最新追踪ID") or "").strip()
                or int(float(fields.get("已提交次数") or 0)) > 0
            )
            if submitted and result_status in {"", "pending", "submitted", "processing", "generating"}:
                active_others.append(item)
        if not active_others:
            return valid_results[0], "only_valid_video_result_row"

    protected = [item for item in candidates if _run_row_is_protected(item.get("fields") or {})]
    if len(protected) == 1:
        return protected[0], "only_submitted_or_result_row"
    if len(protected) > 1:
        return None, "multiple_submitted_or_result_rows"

    bound_id = str(job.get("run_manager_record_id") or "").strip()
    bound = [item for item in candidates if str(item.get("record_id") or "") == bound_id]
    if len(bound) == 1:
        return bound[0], "local_bound_row"

    source_hash = str(job.get("run_manager_source_hash") or "").strip()
    matching = [
        item for item in candidates
        if source_hash and str((item.get("fields") or {}).get("来源指纹") or "").strip() == source_hash
    ]
    if len(matching) == 1:
        return matching[0], "only_current_source_hash_row"
    return None, "no_unique_safe_keeper"


def cleanup_run_manager_duplicates(
    db: LightTryonDB,
    review_client: Any,
    run_client: Any,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Delete only duplicate LTV rows for which one keeper is provably safe."""
    run_groups = _group_run_records(run_client)
    review_groups = _group_review_records(review_client)
    plans: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for job in db.list_jobs():
        job_id = str(job.get("job_id") or "").strip()
        candidates = run_groups.get(job_id, [])
        if len(candidates) <= 1:
            continue
        keeper, reason = _select_duplicate_keeper(job, candidates)
        if keeper is None:
            skipped.append({
                "job_id": job_id, "reason": reason,
                "local_bound_record_id": str(job.get("run_manager_record_id") or ""),
                "record_ids": [str(item.get("record_id") or "") for item in candidates],
                "candidates": [
                    {
                        "record_id": str(item.get("record_id") or ""),
                        "status": str((item.get("fields") or {}).get("状态") or ""),
                        "submitted": int(float((item.get("fields") or {}).get("已提交次数") or 0)),
                        "trace_id": str((item.get("fields") or {}).get("最新追踪ID") or ""),
                        "result_status": str((item.get("fields") or {}).get("结果回传状态") or ""),
                        "has_video": _run_result_has_video(item.get("fields") or {}),
                        "source_hash": str((item.get("fields") or {}).get("来源指纹") or ""),
                    }
                    for item in candidates
                ],
            })
            continue
        delete_ids = [
            str(item.get("record_id") or "") for item in candidates
            if str(item.get("record_id") or "") != str(keeper.get("record_id") or "")
        ]
        keeper_fields = keeper.get("fields") or {}
        duplicate_only_block = bool(
            str(keeper_fields.get("状态") or "") == "阻塞"
            and "重复轻视频内容ID" in str(
                keeper_fields.get("错误信息") or keeper_fields.get("结果说明") or ""
            )
            and not _run_row_is_protected(keeper_fields)
        )
        plans.append({
            "job_id": job_id,
            "keep_record_id": str(keeper.get("record_id") or ""),
            "delete_record_ids": delete_ids,
            "keeper_reason": reason,
            "requeue_keeper": duplicate_only_block,
        })

    result: dict[str, Any] = {
        "dry_run": dry_run,
        "duplicate_groups": len(plans) + len(skipped),
        "safe_groups": len(plans),
        "ambiguous_groups": len(skipped),
        "planned_delete": sum(len(item["delete_record_ids"]) for item in plans),
        "deleted": 0,
        "requeued": 0,
        "repaired": 0,
        "repair_errors": [],
        "plans": plans,
        "skipped": skipped,
    }
    if dry_run or not plans:
        return result

    delete_ids = [record_id for plan in plans for record_id in plan["delete_record_ids"]]
    result["deleted"] = int(run_client.batch_delete_records(delete_ids)) if delete_ids else 0
    if result["deleted"] != len(delete_ids):
        raise RuntimeError(f"运行表重复记录计划删除 {len(delete_ids)} 条，实际删除 {result['deleted']} 条")

    for plan in plans:
        try:
            job_id = plan["job_id"]
            keeper_id = plan["keep_record_id"]
            job = db.get_job(job_id) or {}
            keeper = next(item for item in run_groups[job_id] if item["record_id"] == keeper_id)
            keeper_fields = keeper.get("fields") or {}
            if plan["requeue_keeper"]:
                reset_fields = {
                    "状态": "待处理", "执行归属": "", "已提交次数": 0,
                    "结果说明": "", "最新追踪ID": "", "结果回传状态": "", "错误信息": "",
                }
                run_client.update_record_fields(keeper_id, reset_fields)
                keeper_fields.update(reset_fields)
                result["requeued"] += 1
            if _run_result_has_video(keeper_fields) or _run_row_is_protected(keeper_fields):
                local_status = "generating"
                review_status = "生成中"
            else:
                local_status = "queued"
                review_status = "已入队"
            db.update_run_manager_sync(
                job_id, record_id=keeper_id, status=local_status, error="",
                source_hash=str(keeper_fields.get("来源指纹") or job.get("run_manager_source_hash") or ""),
            )
            try:
                review = _resolve_review_record(job, review_groups)
            except ValueError:
                review = None
            if review:
                review_client.update_record_fields(review["record_id"], {
                    "运行表记录ID": keeper_id, "队列同步状态": review_status, "队列错误信息": "",
                })
            result["repaired"] += 1
        except Exception as exc:
            result["repair_errors"].append({"job_id": plan["job_id"], "error": str(exc)[:2000]})
    return result


def backfill_run_manager_result_hashes(
    db: LightTryonDB,
    run_client: Any,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Backfill durable video hashes and report cross-job collisions."""
    run_groups = _group_run_records(run_client)
    jobs = [
        item for item in db.list_jobs()
        if str(item.get("run_manager_result_status") or "").lower() == "uploaded"
        and not str(item.get("run_manager_result_sha256") or "").strip()
    ]
    owners = _persisted_content_hash_owners(db)
    result: dict[str, Any] = {
        "dry_run": dry_run, "candidates": len(jobs), "from_metadata": 0,
        "downloaded": 0, "updated": 0, "missing": [], "conflicts": [],
        "unique_index": False,
    }
    for job in jobs:
        job_id = str(job.get("job_id") or "")
        digest = ""
        source = ""
        for attachment in job.get("raw_video_attachments") or []:
            if isinstance(attachment, dict):
                digest = str(attachment.get("source_sha256") or "").strip().lower()
                if digest:
                    source = "metadata"
                    break
        if not digest:
            try:
                row = _resolve_run_record(job, run_groups, prefer_result=True)
                if not row or not _run_result_has_video(row.get("fields") or {}):
                    result["missing"].append({"job_id": job_id, "reason": "run_result_not_found"})
                    continue
                fields = row["fields"]
                videos = fields.get("生成视频") if isinstance(fields.get("生成视频"), list) else []
                if videos:
                    content, _, _, _ = run_client.download_attachment_bytes(videos[0])
                else:
                    content, _, _, _ = _download_oss_video(fields)
                digest = hashlib.sha256(content).hexdigest()
                source = "download"
            except Exception as exc:
                result["missing"].append({"job_id": job_id, "reason": str(exc)[:500]})
                continue
        owner = owners.get(digest)
        if owner and owner != job_id:
            result["conflicts"].append({"sha256": digest, "owner_job_id": owner, "job_id": job_id})
            continue
        owners[digest] = job_id
        result["from_metadata" if source == "metadata" else "downloaded"] += 1
        if not dry_run:
            db.set_run_manager_result_sha256(job_id, digest)
        result["updated"] += 1
    if not dry_run and not result["conflicts"]:
        db.init_schema()
        with db.connection() as conn:
            index = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name='uq_video_jobs_run_result_sha256'"
            ).fetchone()
            result["unique_index"] = index is not None
    return result


def build_run_manager_sync_snapshots(
    review_client: Any,
    run_client: Any,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    """Read each remote table once for a complete queue/pull synchronization pass."""
    return _group_review_records(review_client), _group_run_records(run_client)


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


def resolve_run_manager_identity(product: dict[str, Any], legacy_store_id: str = "") -> tuple[str, str]:
    """Resolve business-facing product/store IDs without leaking internal OSG keys."""
    internal_product_id = str(product.get("product_id") or "").strip()
    source_record_id = str(product.get("source_script_record_id") or "").strip()
    source_product_code = str(product.get("source_product_code") or "").strip()
    account_id = str(product.get("account_id") or "").strip()

    if source_record_id:
        if not source_product_code:
            raise ValueError(f"原始脚本商品缺少产品编码: {source_record_id}")
        if source_product_code.startswith("OSG_"):
            raise ValueError(f"原始脚本产品编码不能使用内部商品键: {source_product_code}")
        if not account_id:
            raise ValueError(f"原始脚本商品缺少店铺ID: {source_record_id}")
        return source_product_code, account_id

    business_product_id = source_product_code or internal_product_id
    target_store_id = account_id or str(legacy_store_id or "").strip()
    if not business_product_id:
        raise ValueError("运行管理表商品ID为空")
    if not target_store_id:
        raise ValueError("运行管理表店铺ID为空")
    return business_product_id, target_store_id


def build_run_manager_source_hash(
    job: dict[str, Any],
    product: dict[str, Any],
    business_product_id: str,
    target_store_id: str,
) -> str:
    return stable_hash(
        job["job_id"], job.get("prompt_payload"),
        str(job.get("generation_model") or "Seedance 2.0"),
        int(job.get("duration_seconds") or 8),
        job.get("generation_channel"), product.get("product_images") or [],
        business_product_id, target_store_id, length=24,
    )


def pull_generation_preferences(
    db: LightTryonDB,
    review_client: Any,
    *,
    job_ids: Iterable[str] | None = None,
    dry_run: bool = False,
    review_groups: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    selected = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    summary: dict[str, Any] = {"processed": 0, "updated": 0, "skipped": 0, "failed": 0, "errors": []}
    grouped = review_groups if review_groups is not None else _group_review_records(review_client)
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
    review_groups: dict[str, list[dict[str, Any]]] | None = None,
    run_groups: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    selected = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    review_groups = review_groups if review_groups is not None else _group_review_records(review_client)
    run_groups = run_groups if run_groups is not None else _group_run_records(run_client)
    jobs = [
        job for job in db.list_jobs()
        if str(job.get("generation_channel") or "no_generate") != "no_generate"
        and (not selected or job["job_id"] in selected)
    ]
    if limit is not None:
        jobs = jobs[: max(0, int(limit))]
    summary: dict[str, Any] = {"candidates": len(jobs), "created": 0, "updated": 0, "skipped": 0, "blocked": 0, "failed": 0, "items": []}
    live_groups_refreshed = False
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
            business_product_id, target_store_id = resolve_run_manager_identity(product, store_id)
            source_hash = build_run_manager_source_hash(
                job, product, business_product_id, target_store_id,
            )
            existing = _resolve_run_record(job, run_groups)
            # The caller's snapshot can become stale while product images and
            # preferences are prepared. Re-read immediately before deciding to
            # create so a record inserted by an overlapping sync is reused.
            if existing is None and not dry_run and not live_groups_refreshed:
                live_groups = _group_run_records(run_client)
                run_groups.update(live_groups)
                live_groups_refreshed = True
                existing = _resolve_run_record(job, live_groups)
            elif existing is None and live_groups_refreshed:
                existing = _resolve_run_record(job, run_groups)
            rerun = bool(job.get("generation_rerun"))
            if (
                existing
                and not rerun
                and str(existing["fields"].get("结果回传状态") or "").lower() == "uploaded"
                and _run_result_has_video(existing["fields"])
            ):
                summary["skipped"] += 1
                summary["items"].append({"job_id": job_id, "status": "skipped", "reason": "result_already_uploaded"})
                continue
            if existing and not rerun and _result_status_is_terminal_failure(existing["fields"].get("结果回传状态")):
                summary["skipped"] += 1
                summary["items"].append({"job_id": job_id, "status": "skipped", "reason": "terminal_result_failure"})
                continue
            if existing and str(existing["fields"].get("来源指纹") or "") == source_hash and not rerun:
                remote_status = str(existing["fields"].get("状态") or "")
                local_status = "generating" if remote_status in ACTIVE_STATUSES and remote_status != "待处理" else "queued"
                if not _local_run_sync_matches(
                    job, record_id=existing["record_id"], status=local_status, source_hash=source_hash,
                ):
                    db.update_run_manager_sync(
                        job_id, record_id=existing["record_id"], status=local_status, source_hash=source_hash,
                    )
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
                active_error = "已有运行中的同任务，禁止重复提交"
                if not _local_run_sync_matches(
                    job, record_id=existing["record_id"], status="blocked", error=active_error,
                ):
                    db.update_run_manager_sync(
                        job_id, record_id=existing["record_id"], status="blocked", error=active_error,
                    )
                _update_remote_if_changed(review_client, review, {
                    "队列同步状态": "阻塞", "队列错误信息": active_error,
                })
                summary["blocked"] += 1
                summary["items"].append({"job_id": job_id, "status": "blocked", "reason": "active_duplicate"})
                continue
            if existing and existing_status not in PATCHABLE_STATUSES and not unclaimed_pending:
                raise ValueError(f"现有运行任务状态不可重置: {existing['fields'].get('状态')}")
            context = db.get_job_context(job_id)
            base = build_jimeng_record(context, job["prompt_payload"])
            fields = {
                **base,
                "任务名": job_id,
                "内容ID": job_id,
                "脚本ID": job_id,
                "商品ID": business_product_id,
                "全球产品ID": business_product_id,
                "店铺ID": target_store_id,
                "任务来源": "轻量试穿视频",
                "脚本类型": SCRIPT_TYPE_LIGHT_VIDEO,
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
                if not record_id:
                    raise ValueError("运行管理表创建成功但未返回记录ID")
                run_groups[job_id] = [{"record_id": record_id, "fields": dict(fields)}]
                summary["created"] += 1
            db.update_run_manager_sync(job_id, record_id=record_id, status="queued", source_hash=source_hash, clear_rerun=True)
            review_client.update_record_fields(review["record_id"], {
                "运行表记录ID": record_id, "队列同步状态": "已入队", "队列错误信息": "", "重新提交生成": False,
            })
            summary["items"].append({"job_id": job_id, "status": "updated" if existing else "created", "record_id": record_id})
        except Exception as exc:
            message = str(exc)[:2000]
            is_duplicate = isinstance(exc, DuplicateRunManagerIDError)
            if not dry_run:
                db.update_run_manager_sync(job_id, status="blocked" if is_duplicate else "failed", error=message)
                quarantined = (
                    _quarantine_unclaimed_duplicate_rows(run_client, run_groups.get(job_id, []), message)
                    if is_duplicate else 0
                )
                if review:
                    _update_remote_if_changed(review_client, review, {
                        "队列同步状态": "阻塞" if is_duplicate else "失败", "队列错误信息": message,
                    })
            else:
                quarantined = 0
            summary["blocked" if is_duplicate else "failed"] += 1
            summary["items"].append({
                "job_id": job_id, "status": "blocked" if is_duplicate else "failed", "error": message,
                **({"quarantined_run_rows": quarantined} if is_duplicate else {}),
            })
    return summary


def pull_run_manager_results(
    db: LightTryonDB,
    review_client: Any,
    run_client: Any,
    *,
    job_ids: Iterable[str] | None = None,
    review_groups: dict[str, list[dict[str, Any]]] | None = None,
    run_groups: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    selected = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    review_groups = review_groups if review_groups is not None else _group_review_records(review_client)
    summary: dict[str, Any] = {
        "checked": 0, "returned": 0, "generating": 0, "blocked": 0,
        "failed": 0, "skipped": 0, "items": [],
    }
    content_hash_owners = _persisted_content_hash_owners(db)
    run_groups = run_groups if run_groups is not None else _group_run_records(run_client)
    jobs_by_id = {
        str(item.get("job_id") or ""): item for item in db.list_jobs()
        if str(item.get("generation_channel") or "no_generate") != "no_generate"
    }
    for job_id, job in jobs_by_id.items():
        if not job_id or job_id not in run_groups or (selected and job_id not in selected):
            continue
        review: dict[str, Any] | None = None
        try:
            review = _resolve_review_record(job, review_groups)
            if not review:
                raise ValueError("轻视频复核表不存在该任务")
            row = _resolve_run_record(job, run_groups, prefer_result=True)
            if not row:
                continue
            fields = row["fields"]
            summary["checked"] += 1
            trace_id = str(fields.get("最新追踪ID") or "")
            status = str(fields.get("状态") or "")
            result_status = str(fields.get("结果回传状态") or "")
            videos = fields.get("生成视频") if isinstance(fields.get("生成视频"), list) else []
            oss_ready = _oss_source_enabled() and bool(str(fields.get("OSS对象ID") or fields.get("OSS路径") or "").strip())
            if result_status.lower() == "uploaded" and (videos or oss_ready):
                source = videos[0] if videos else {}
                source_file_token = str(source.get("file_token") or "").strip() if isinstance(source, dict) else ""
                if not source_file_token and oss_ready:
                    source_file_token = f"oss:{str(fields.get('OSS对象ID') or fields.get('OSS路径') or '').strip()}"
                existing_sources = {
                    str(item.get("source_file_token") or "").strip()
                    for item in (job.get("raw_video_attachments") or [])
                    if isinstance(item, dict)
                }
                same_stable_source = bool(
                    source_file_token
                    and source_file_token == str(job.get("run_manager_result_source_token") or "").strip()
                )
                if (
                    source_file_token
                    and (
                        same_stable_source
                        or (
                            str(job.get("run_manager_sync_status") or "") == "returned"
                            and source_file_token in existing_sources
                        )
                    )
                ):
                    if same_stable_source and str(job.get("run_manager_sync_status") or "") != "returned":
                        db.update_run_manager_sync(
                            job_id, record_id=row["record_id"], status="returned", error="",
                            trace_id=trace_id, result_status=result_status,
                        )
                        _update_remote_if_changed(review_client, review, {
                            "生成状态": "生成成功", "生成失败原因": "", "队列同步状态": "已回流",
                            "队列错误信息": "", "最新追踪ID": trace_id,
                        })
                    summary["skipped"] += 1
                    continue
                if videos:
                    content, file_name, content_type, size = run_client.download_attachment_bytes(source)
                else:
                    content, file_name, content_type, size = _download_oss_video(fields)
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
                db.update_run_manager_sync(
                    job_id,
                    record_id=row["record_id"],
                    status="returned",
                    error="",
                    trace_id=trace_id,
                    result_status=result_status,
                )
                db.set_run_manager_result(
                    job_id,
                    attachments=attachments,
                    trace_id=trace_id,
                    source_file_token=source_file_token,
                    source_sha256=source_sha256,
                )
                summary["returned"] += 1
                summary["items"].append({"job_id": job_id, "status": "returned", "trace_id": trace_id})
            elif status in {"失败", "阻塞"} or _result_status_is_terminal_failure(result_status):
                error = str(fields.get("错误信息") or fields.get("结果说明") or result_status or status)
                terminal_result_failure = _result_status_is_terminal_failure(result_status)
                local_status = "failed" if status == "失败" or terminal_result_failure else "blocked"
                review_queue_status = "失败" if terminal_result_failure else status
                if not _local_run_sync_matches(
                    job, record_id=row["record_id"], status=local_status, error=error,
                    trace_id=trace_id, result_status=result_status,
                ):
                    db.update_run_manager_sync(
                        job_id, record_id=row["record_id"], status=local_status, error=error,
                        trace_id=trace_id, result_status=result_status,
                    )
                _update_remote_if_changed(review_client, review, {
                    "生成状态": "生成失败", "生成失败原因": error[:2000], "队列同步状态": review_queue_status,
                    "队列错误信息": error[:2000], "最新追踪ID": trace_id,
                })
                summary["failed"] += 1
                summary["items"].append({"job_id": job_id, "status": review_queue_status, "error": error[:2000]})
            else:
                if not _local_run_sync_matches(
                    job, record_id=row["record_id"], status="generating",
                    trace_id=trace_id, result_status=result_status,
                ):
                    db.update_run_manager_sync(
                        job_id, record_id=row["record_id"], status="generating",
                        trace_id=trace_id, result_status=result_status,
                    )
                _update_remote_if_changed(review_client, review, {
                    "生成状态": "生成中", "队列同步状态": "生成中", "最新追踪ID": trace_id,
                })
                summary["generating"] += 1
        except Exception as exc:
            error = str(exc)[:2000]
            is_duplicate = isinstance(exc, DuplicateRunManagerIDError)
            db.update_run_manager_sync(job_id, status="blocked" if is_duplicate else "failed", error=error)
            quarantined = (
                _quarantine_unclaimed_duplicate_rows(run_client, run_groups.get(job_id, []), error)
                if is_duplicate else 0
            )
            if review:
                _update_remote_if_changed(review_client, review, {
                    "队列同步状态": "阻塞" if is_duplicate else "失败", "队列错误信息": error,
                })
            summary["blocked" if is_duplicate else "failed"] += 1
            summary["items"].append({
                "job_id": job_id, "status": "blocked" if is_duplicate else "failed", "error": error,
                **({"quarantined_run_rows": quarantined} if is_duplicate else {}),
            })
    return summary
