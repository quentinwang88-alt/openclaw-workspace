#!/usr/bin/env python3
"""OpenClaw-side immediate trigger for rows checked as 【立即同步】."""

from __future__ import annotations

import fcntl
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient
from core.manual_source import resolve_manual_field_mapping
from core.sync import normalize_checkbox, now_text
from run_pipeline import DEFAULT_MANUAL_SOURCE_FEISHU_URL, resolve_feishu_config


REQUEST_FIELD = "立即同步"
OUTER_SCHEDULE_LOCK = Path("/tmp/com.likeu3.script-run-manager-sync.lock")
TRIGGER_LOCK = Path("/tmp/script_run_manager_manual_trigger.pid")


@contextmanager
def process_lock():
    with TRIGGER_LOCK.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _status_field(mapping: dict) -> str:
    return str(mapping.get("sync_status") or "")


def main() -> int:
    with process_lock() as acquired:
        if not acquired:
            print("manual-sync-trigger skipped: previous trigger is still running")
            return 0

        app_token, table_id = resolve_feishu_config(DEFAULT_MANUAL_SOURCE_FEISHU_URL)
        client = FeishuBitableClient(app_token=app_token, table_id=table_id)
        field_names = client.list_field_names()
        if REQUEST_FIELD not in field_names:
            raise RuntimeError(f"人工脚本表缺少字段【{REQUEST_FIELD}】")
        mapping = resolve_manual_field_mapping(field_names)
        records = client.list_records(page_size=100)
        requested = [record for record in records if normalize_checkbox(record.fields.get(REQUEST_FIELD))]
        if not requested:
            print("manual-sync-trigger: no requested rows")
            return 0

        if OUTER_SCHEDULE_LOCK.exists():
            print(f"manual-sync-trigger deferred: scheduled sync is active ({len(requested)} rows)")
            return 0

        synced = 0
        deferred = 0
        for record in requested:
            updates = {}
            sync_field = mapping.get("sync_enabled")
            if sync_field and not normalize_checkbox(record.fields.get(sync_field)):
                updates[sync_field] = True
            status_field = _status_field(mapping)
            if status_field:
                updates[status_field] = f"OpenClaw 已接收立即同步请求：{now_text()}"
            if updates:
                client.update_record_fields(record.record_id, updates)

            command = [
                sys.executable,
                str(SKILL_DIR / "run_pipeline.py"),
                "--mode",
                "manual",
                "--source-kind",
                "manual",
                "--record-id",
                record.record_id,
            ]
            result = subprocess.run(
                command,
                cwd=str(SKILL_DIR),
                capture_output=True,
                text=True,
                timeout=600,
                check=False,
            )
            if result.returncode != 0:
                deferred += 1
                if status_field:
                    detail = (result.stderr or result.stdout or "同步进程未成功启动").strip().replace("\n", " ")[:300]
                    client.update_record_fields(
                        record.record_id,
                        {status_field: f"OpenClaw 立即同步未启动，将自动重试：{detail}"},
                    )
                print(f"manual-sync-trigger deferred: record_id={record.record_id} exit={result.returncode}")
                continue

            # The pipeline is responsible for final success/failure status.  The request
            # checkbox is consumed after one explicit OpenClaw attempt to avoid repeat runs.
            client.update_record_fields(record.record_id, {REQUEST_FIELD: False})
            synced += 1
            print(f"manual-sync-trigger completed: record_id={record.record_id}")

        print(f"manual-sync-trigger summary: completed={synced} deferred={deferred}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
