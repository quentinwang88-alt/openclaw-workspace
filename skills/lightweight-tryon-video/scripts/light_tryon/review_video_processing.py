from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any, Callable, Iterable

from .database import LightTryonDB
from .utils import now_iso, stable_hash
from .workers import render_postprocessed_video


REVIEW_POSTPROCESS_VERSION = "review-postprocess-v1.1.0"


def _stable_attachments(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [
        {
            key: item.get(key)
            for key in ("file_token", "name", "size", "type")
            if item.get(key) is not None
        }
        for item in value
        if isinstance(item, dict) and (item.get("file_token") or item.get("url"))
    ]


def _safe_suffix(file_name: str, content_type: str) -> str:
    suffix = Path(file_name).suffix.lower()
    if suffix in {".mp4", ".mov", ".m4v", ".webm"}:
        return suffix
    guessed = mimetypes.guess_extension(content_type or "") or ".mp4"
    return guessed if guessed in {".mp4", ".mov", ".m4v", ".webm"} else ".mp4"


def process_review_videos(
    db: LightTryonDB,
    client: Any,
    *,
    job_ids: Iterable[str] | None = None,
    limit: int | None = None,
    force: bool = False,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
    subtitle_font_name: str = "Arial Unicode MS",
    renderer: Callable[..., dict[str, Any]] = render_postprocessed_video,
) -> dict[str, Any]:
    selected = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    records = client.list_records(page_size=500)
    candidates: list[tuple[Any, dict[str, Any], list[dict[str, Any]]]] = []
    for record in records:
        fields = dict(record.fields or {})
        job_id = str(fields.get("视频任务ID") or "").strip()
        initial = _stable_attachments(fields.get("初始成片"))
        if not job_id or not initial or (selected and job_id not in selected):
            continue
        candidates.append((record, fields, initial))
    if limit is not None:
        candidates = candidates[: max(0, int(limit))]

    summary: dict[str, Any] = {
        "candidates": len(candidates), "processed": 0, "skipped": 0, "failed": 0, "items": [],
    }
    target_root = db.path.parent / "review_videos"
    for record, fields, initial in candidates:
        job_id = str(fields.get("视频任务ID") or "").strip()
        job = db.get_job(job_id)
        if not job:
            summary["failed"] += 1
            summary["items"].append({"job_id": job_id, "status": "failed", "error": "本地不存在该视频任务"})
            continue
        prompt_payload = job.get("prompt_payload") or {}
        subtitle_plan = prompt_payload.get("subtitle_plan") or {}
        brand_plan = prompt_payload.get("brand_plan") or {}
        source_hash = stable_hash(
            REVIEW_POSTPROCESS_VERSION, initial, subtitle_plan, brand_plan, length=24,
        )
        existing_final = _stable_attachments(fields.get("最终视频"))
        if (
            not force
            and source_hash == str(job.get("review_video_source_hash") or "")
            and str(job.get("review_video_process_status") or "") == "success"
            and existing_final
        ):
            summary["skipped"] += 1
            summary["items"].append({"job_id": job_id, "status": "skipped", "reason": "unchanged"})
            continue

        job_dir = target_root / job_id / source_hash
        job_dir.mkdir(parents=True, exist_ok=True)
        try:
            db.update_review_video_processing(
                job_id,
                status="processing",
                source_hash=source_hash,
                raw_attachments=initial,
                error="",
            )
            attachment = (fields.get("初始成片") or [])[0]
            content, file_name, content_type, _ = client.download_attachment_bytes(attachment)
            if not str(content_type or "").startswith("video/") and Path(file_name).suffix.lower() not in {".mp4", ".mov", ".m4v", ".webm"}:
                raise ValueError(f"初始成片不是支持的视频文件: {file_name}")
            raw_path = job_dir / f"initial{_safe_suffix(file_name, content_type)}"
            raw_path.write_bytes(content)
            final_path = job_dir / f"{job_id}_final.mp4"
            cover_path = job_dir / f"{job_id}_cover.jpg"
            result = renderer(
                raw_path,
                final_path,
                subtitle_plan,
                brand_plan,
                cover_output=cover_path,
                ffmpeg_bin=ffmpeg_bin,
                ffprobe_bin=ffprobe_bin,
                subtitle_font_name=subtitle_font_name,
            )
            rendered = Path(str(result.get("output_video_path") or final_path)).expanduser().resolve()
            if not rendered.is_file() or rendered.stat().st_size <= 0:
                raise RuntimeError("后处理没有生成有效最终视频")
            uploaded = client.upload_attachment(
                rendered.read_bytes(), rendered.name, "video/mp4", rendered.stat().st_size,
                parent_type="bitable_file",
            )
            final_attachments = [uploaded]
            client.update_record_fields(
                record.record_id,
                {"最终视频": final_attachments, "数据同步状态": "已同步", "同步错误信息": ""},
            )
            db.set_postprocessed_assets(
                job_id,
                str(rendered),
                str(result.get("output_cover_path") or cover_path),
            )
            db.update_review_video_processing(
                job_id,
                status="success",
                source_hash=source_hash,
                raw_path=str(raw_path),
                raw_attachments=initial,
                final_attachments=final_attachments,
                error="",
                processed_at=now_iso(),
            )
            summary["processed"] += 1
            summary["items"].append({
                "job_id": job_id, "status": "success", "output_video_path": str(rendered),
                "final_file_token": uploaded.get("file_token") or "",
            })
        except Exception as exc:
            message = str(exc)[:2000]
            db.update_review_video_processing(
                job_id,
                status="failed",
                source_hash=source_hash,
                raw_attachments=initial,
                error=message,
            )
            try:
                client.update_record_fields(
                    record.record_id,
                    {"数据同步状态": "同步失败", "同步错误信息": message},
                )
            except Exception:
                pass
            summary["failed"] += 1
            summary["items"].append({"job_id": job_id, "status": "failed", "error": message})
    return summary
