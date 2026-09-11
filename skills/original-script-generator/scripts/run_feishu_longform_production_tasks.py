#!/usr/bin/env python3
"""Run checked long-form production rows to final video and write back Feishu."""
from __future__ import annotations

import argparse
import atexit
import fcntl
import mimetypes
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.longform.production_runner import run_longform_job_to_final  # noqa: E402
from core.longform.main_schedule_bridge import enqueue_longform_final  # noqa: E402
from core.longform.audio import validate_finalized_media  # noqa: E402
from core.longform.storage import DEFAULT_ASSET_ROOT, LongformStorage  # noqa: E402
from core.production_script_feishu import (  # noqa: E402
    PRODUCTION_SCRIPT_FIELD_NAMES,
    PRODUCTION_SCRIPT_FIELDS,
    ensure_fields,
)
from scripts.run_feishu_operation_tasks import DEFAULT_SCRIPT_URL, _client  # noqa: E402


DEFAULT_LOCK = (
    Path.home() / ".openclaw" / "shared" / "locks" / "longform-video-production.lock"
)


def _checked(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, list):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "是", "已勾选"}


def _lock(path: Path = DEFAULT_LOCK):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
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


FEISHU_UPLOAD_LIMIT_BYTES = 20_000_000
FEISHU_UPLOAD_TARGET_BYTES = 19_000_000


def _feishu_upload_ready_video(path: Path) -> Path:
    """Keep the local master untouched and make an upload-safe MP4 when needed.

    Feishu's ``upload_all`` rejects videos over its roughly 20 MB single-file
    limit.  Long-form masters can exceed that limit even when the source
    segments are valid, so the Feishu copy is lightly re-encoded only at the
    write-back boundary.
    """
    if path.stat().st_size < FEISHU_UPLOAD_TARGET_BYTES:
        return path
    target = path.with_name(f"{path.stem}_feishu_upload.mp4")
    if not target.is_file() or target.stat().st_mtime < path.stat().st_mtime:
        command = [
            "/Users/likeu3/.local/bin/ffmpeg", "-y", "-i", str(path),
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "29",
            "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart",
            str(target),
        ]
        completed = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, check=False,
        )
        if completed.returncode != 0 or not target.is_file():
            raise RuntimeError(
                f"飞书上传前压缩失败: {completed.stderr[-1200:]}"
            )
    if target.stat().st_size >= FEISHU_UPLOAD_LIMIT_BYTES:
        raise RuntimeError(
            f"视频压缩后仍超过飞书单文件限制: {target.stat().st_size} bytes"
        )
    return target


def _upload_video(client: Any, path: Path) -> dict:
    path = _feishu_upload_ready_video(path)
    # The upload copy may be a re-encoded derivative of the validated master,
    # so verify the actual bytes that Feishu will receive as well.
    validate_finalized_media(path)
    content_type = mimetypes.guess_type(path.name)[0] or "video/mp4"
    return client.upload_attachment(
        content=path.read_bytes(),
        file_name=path.name,
        content_type=content_type,
        size=path.stat().st_size,
        parent_type="bitable_file",
    )


def _upload_image(client: Any, path: Path) -> dict:
    if not path.is_file():
        raise RuntimeError(f"长视频首帧不存在: {path}")
    if path.stat().st_size >= FEISHU_UPLOAD_LIMIT_BYTES:
        raise RuntimeError(
            f"长视频首帧超过飞书单文件限制: {path.stat().st_size} bytes"
        )
    content_type = mimetypes.guess_type(path.name)[0] or "image/png"
    return client.upload_attachment(
        content=path.read_bytes(),
        file_name=path.name,
        content_type=content_type,
        size=path.stat().st_size,
        parent_type="bitable_file",
    )


def _longform_first_frame_writeback(
    client: Any,
    storage: LongformStorage,
    job_id: str,
    current_fields: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Return a one-time Feishu K0 attachment update without blocking media.

    Long-form owns K0 in its isolated state machine.  This projection only
    makes the generated frame visible in the production-script workbench; it
    never hands the row to the short-video run manager.
    """

    field_name = PRODUCTION_SCRIPT_FIELD_NAMES["longform_first_frame"]
    if current_fields.get(field_name):
        return {}, ""
    try:
        job = storage.get_job(job_id) or {}
        segments = list(job.get("segments") or [])
        path = Path(str((segments[0] if segments else {}).get("start_frame_path") or ""))
        if not str(path) or str(path) == "." or not path.is_file():
            raise RuntimeError("长视频 K0 尚未登记或本地文件不存在")
        return {field_name: [_upload_image(client, path)]}, ""
    except Exception as exc:
        return {}, str(exc)[:500]


def _list_records_with_transient_retry(
    client: Any, *, page_size: int = 500, delays: tuple[int, ...] = (3, 8)
) -> list[Any]:
    """Retry only Feishu's documented transient materialization response."""
    for attempt in range(len(delays) + 1):
        try:
            return list(client.list_records(page_size=page_size))
        except Exception as exc:
            if "Data not ready" not in str(exc) or attempt >= len(delays):
                raise
            time.sleep(delays[attempt])
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="原创生产脚本表 -> 长视频完整生产")
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--record-id")
    parser.add_argument("--product-code")
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-real-submit", action="store_true")
    parser.add_argument("--allow-external-tts", action="store_true")
    parser.add_argument("--asset-root", default=str(DEFAULT_ASSET_ROOT))
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--max-wait-seconds", type=int, default=1200)
    parser.add_argument(
        "--voiceover-model-command",
        default="python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py",
    )
    args = parser.parse_args()
    if not 1 <= args.limit <= 20:
        parser.error("--limit 必须在1到20之间")
    if not args.dry_run and (not args.allow_real_submit or not args.allow_external_tts):
        parser.error("正式长视频生产必须同时显式允许H3与Edge TTS")

    handle = None
    if not args.dry_run:
        handle = _lock()
        if handle is None:
            print("SKIPPED_LOCKED: 已有长视频生产任务运行")
            return 0
        atexit.register(handle.close)

    client = _client(args.script_url)
    # A read-only OpenClaw `check` must not mutate the Feishu schema.  The
    # provisioning command creates these fields once; formal runs can repair a
    # missing field before writing status.
    if not args.dry_run:
        ensure_fields(client, primary_field_name="脚本ID", specs=PRODUCTION_SCRIPT_FIELDS)
    f = PRODUCTION_SCRIPT_FIELD_NAMES
    records = _list_records_with_transient_retry(client, page_size=500)
    if args.record_id:
        records = [record for record in records if record.record_id == args.record_id]
    candidates = []
    for record in records:
        fields = record.fields
        if str(fields.get(f["video_format"]) or "").strip() != "长视频":
            continue
        code = str(fields.get(f["product_code"]) or "").strip()
        if args.product_code and code != args.product_code:
            continue
        job_id = str(fields.get(f["longform_job_id"]) or "").strip()
        if not job_id:
            continue
        if _checked(fields.get(f["production_enabled"])):
            action = "produce"
        elif (
            str(fields.get(f["longform_status"]) or "").strip() == "已完成"
            and _checked(fields.get(f["publish_confirmed"]))
        ):
            action = "publish_only"
        else:
            continue
        candidates.append((record, code, job_id, action))
        if len(candidates) >= args.limit:
            break

    print(f"待执行长视频生产/发布脚本: {len(candidates)}")
    for record, code, job_id, action in candidates:
        print(f"- {record.record_id} | {code} | {job_id} | {action}")
    if args.dry_run:
        return 0

    storage = LongformStorage()
    storage.ensure_schema()
    failed = 0
    for record, code, job_id, action in candidates:
        if action == "publish_only":
            try:
                job = storage.get_job(job_id) or {}
                final_path = Path(str(job.get("final_video_path") or ""))
                queued = enqueue_longform_final(
                    record_id=record.record_id,
                    job_id=job_id,
                    final_video_path=final_path,
                    fields=record.fields,
                )
                client.update_record_fields(record.record_id, {
                    f["publish_confirmed"]: False,
                    f["sync_result"]: "长视频成片已进入主发布队列待排期",
                    f["sync_time"]: int(time.time() * 1000),
                    f["run_task_id"]: job_id,
                })
                print(
                    f"已进入发布队列: {record.record_id} | "
                    f"{queued['canonical_script_key']}"
                )
            except Exception as exc:
                failed += 1
                client.update_record_fields(record.record_id, {
                    f["sync_result"]: f"长视频排班接入失败：{str(exc)[:800]}",
                    f["sync_time"]: int(time.time() * 1000),
                })
                print(f"排班接入失败: {record.record_id}: {exc}", file=sys.stderr)
            continue
        try:
            client.update_record_fields(record.record_id, {
                f["longform_status"]: "生成中",
                f["longform_error"]: "",
                f["sync_result"]: "长视频生产执行中",
            })
            result = run_longform_job_to_final(
                job_id,
                storage=storage,
                asset_root=args.asset_root,
                allow_real_submit=True,
                allow_external_tts=True,
                poll_interval_seconds=max(5, args.poll_interval_seconds),
                max_wait_seconds=max(30, args.max_wait_seconds),
                voiceover_model_command=args.voiceover_model_command,
            )
            first_frame_fields, first_frame_error = _longform_first_frame_writeback(
                client, storage, job_id, record.fields,
            )
            if str(result.get("status") or "") == "WAITING_REMOTE":
                sync_result = "H3远端生成中，下一轮自动续跑"
                if first_frame_error:
                    sync_result += f"；长视频首帧展示回写失败：{first_frame_error}"
                client.update_record_fields(record.record_id, {
                    f["longform_status"]: "生成中",
                    f["longform_error"]: "",
                    f["processing_status"]: "已送生产",
                    **first_frame_fields,
                    f["sync_result"]: sync_result,
                    f["sync_time"]: int(time.time() * 1000),
                    f["run_task_id"]: job_id,
                })
                print(f"等待远端续跑: {record.record_id} | {job_id}")
                continue
            if str(result.get("status") or "") != "FINAL_READY":
                raise RuntimeError(f"长视频未到FINAL_READY: {result.get('status')}")
            final_path = Path(str(result.get("final_video_path") or ""))
            if not final_path.is_file():
                raise RuntimeError("长视频FINAL_READY但成片文件不存在")
            validate_finalized_media(final_path)
            attachment = _upload_video(client, final_path)
            publish_result = None
            publish_error = ""
            if _checked(record.fields.get(f["publish_confirmed"])):
                try:
                    publish_result = enqueue_longform_final(
                        record_id=record.record_id,
                        job_id=job_id,
                        final_video_path=final_path,
                        fields=record.fields,
                    )
                except Exception as exc:
                    # A publish-queue problem must not relabel a successfully
                    # rendered master as a generation failure.
                    publish_error = str(exc)[:800]
            sync_result = "长视频完整生产已完成"
            if first_frame_error:
                sync_result += f"；长视频首帧展示回写失败：{first_frame_error}"
            if publish_result:
                sync_result += "；已进入主发布队列待排期"
            elif publish_error:
                sync_result += f"；排班接入失败：{publish_error}"
            client.update_record_fields(record.record_id, {
                f["longform_status"]: "已完成",
                **first_frame_fields,
                f["longform_video"]: [attachment],
                f["longform_error"]: "",
                f["processing_status"]: "已送生产",
                f["production_enabled"]: False,
                **({f["publish_confirmed"]: False} if publish_result else {}),
                f["sync_result"]: sync_result,
                f["sync_time"]: int(time.time() * 1000),
                f["run_task_id"]: job_id,
            })
            print(f"完成: {record.record_id} | {job_id} | {final_path}")
            if publish_error:
                print(f"排班接入失败: {record.record_id}: {publish_error}", file=sys.stderr)
        except Exception as exc:
            failed += 1
            first_frame_fields, first_frame_error = _longform_first_frame_writeback(
                client, storage, job_id, record.fields,
            )
            failure_text = str(exc)[:1800]
            if first_frame_error and "尚未登记" not in first_frame_error:
                failure_text += f"；长视频首帧展示回写失败：{first_frame_error}"
            client.update_record_fields(record.record_id, {
                f["longform_status"]: "生成失败",
                **first_frame_fields,
                f["longform_error"]: failure_text[:1800],
                f["processing_status"]: "同步失败",
                f["sync_result"]: f"长视频生成失败：{str(exc)[:800]}",
                f["sync_time"]: int(time.time() * 1000),
            })
            print(f"失败: {record.record_id}: {exc}", file=sys.stderr)
            traceback.print_exc()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
