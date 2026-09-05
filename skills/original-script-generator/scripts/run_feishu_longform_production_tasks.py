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
    content_type = mimetypes.guess_type(path.name)[0] or "video/mp4"
    return client.upload_attachment(
        content=path.read_bytes(),
        file_name=path.name,
        content_type=content_type,
        size=path.stat().st_size,
        parent_type="bitable_file",
    )


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
    records = client.list_records(page_size=500)
    if args.record_id:
        records = [record for record in records if record.record_id == args.record_id]
    candidates = []
    for record in records:
        fields = record.fields
        if str(fields.get(f["video_format"]) or "").strip() != "长视频":
            continue
        if not _checked(fields.get(f["production_enabled"])):
            continue
        code = str(fields.get(f["product_code"]) or "").strip()
        if args.product_code and code != args.product_code:
            continue
        job_id = str(fields.get(f["longform_job_id"]) or "").strip()
        if not job_id:
            continue
        candidates.append((record, code, job_id))
        if len(candidates) >= args.limit:
            break

    print(f"待执行长视频生产脚本: {len(candidates)}")
    for record, code, job_id in candidates:
        print(f"- {record.record_id} | {code} | {job_id}")
    if args.dry_run:
        return 0

    storage = LongformStorage()
    storage.ensure_schema()
    failed = 0
    for record, code, job_id in candidates:
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
            if str(result.get("status") or "") != "FINAL_READY":
                raise RuntimeError(f"长视频未到FINAL_READY: {result.get('status')}")
            final_path = Path(str(result.get("final_video_path") or ""))
            if not final_path.is_file():
                raise RuntimeError("长视频FINAL_READY但成片文件不存在")
            attachment = _upload_video(client, final_path)
            client.update_record_fields(record.record_id, {
                f["longform_status"]: "已完成",
                f["longform_video"]: [attachment],
                f["longform_error"]: "",
                f["processing_status"]: "已送生产",
                f["production_enabled"]: False,
                f["sync_result"]: "长视频完整生产已完成",
                f["sync_time"]: int(time.time() * 1000),
                f["run_task_id"]: job_id,
            })
            print(f"完成: {record.record_id} | {job_id} | {final_path}")
        except Exception as exc:
            failed += 1
            client.update_record_fields(record.record_id, {
                f["longform_status"]: "生成失败",
                f["longform_error"]: str(exc)[:1800],
                f["processing_status"]: "同步失败",
                f["sync_result"]: f"长视频生成失败：{str(exc)[:800]}",
                f["sync_time"]: int(time.time() * 1000),
            })
            print(f"失败: {record.record_id}: {exc}", file=sys.stderr)
            traceback.print_exc()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
