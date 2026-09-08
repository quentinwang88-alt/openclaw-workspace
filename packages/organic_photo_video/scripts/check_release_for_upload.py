#!/usr/bin/env python3
"""Read-only live release verification. Does not enqueue, generate or publish."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import socket
import sys

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
for entry in (str(PACKAGE_ROOT.parents[1]), str(PACKAGE_ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from workspace_support import load_repo_env
from services.release_gate import freeze_photo_release, freeze_release, ReleaseGateError


SAFE_RELEASE_REASONS = {
    "发布前任务或选中成片不存在": "task_or_render_missing",
    "当前验收版本与队列冻结清单不同，禁止上传": "queue_manifest_changed",
    "Workflow V2 没有当前版本的已验收成片": "current_release_missing",
    "已验收 revision 不存在或归属不匹配": "revision_ownership_mismatch",
    "成片输入指纹与当前冻结 revision 不一致": "render_fingerprint_mismatch",
    "成片检查缺失、过期或检查来源无效": "render_review_invalid",
    "已验收成片文件 SHA256 已变化": "render_file_changed",
    "已验收版本没有镜头选择清单": "shot_selection_missing",
    "Workflow V2 没有当前版本的已验收图文包": "current_photo_release_missing",
    "已验收图文 revision 或内容包不存在或归属不匹配": "photo_revision_ownership_mismatch",
    "图文输入或选图与当前冻结 revision 不一致": "photo_selection_mismatch",
    "已验收图文包清单缺失或版本不匹配": "photo_package_mismatch",
    "图文包检查缺失、过期或检查来源无效": "photo_review_invalid",
}


def failure_payload(exc: Exception, stage: str = "unknown") -> dict:
    """Emit safe diagnostics, never exception strings/DSNs or raw tracebacks."""
    errno = exc.args[0] if exc.args and type(exc.args[0]) is int else None
    transient = isinstance(exc, (TimeoutError, ConnectionError, socket.gaierror))
    if type(exc).__module__.startswith("pymysql"):
        transient = errno in {1040, 1205, 1213, 2002, 2003, 2006, 2013}
    code = ("release_mismatch" if isinstance(exc, ReleaseGateError)
            else "dependency_unavailable" if transient else "verification_error")
    return {"ok": False, "code": code, "retryable": code == "dependency_unavailable",
            "submission_not_sent": True, "errno": errno, "stage": stage,
            "error_type": type(exc).__name__,
            "reason": SAFE_RELEASE_REASONS.get(str(exc), "unspecified") if isinstance(exc, ReleaseGateError) else "unspecified"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--media-kind", choices=("video", "native_photo"), default="video")
    args = parser.parse_args()
    stage = "repository_import"
    try:
        from repositories.rds_repository import RdsRepository
        stage = "environment_load"
        load_repo_env()
        stage = "repository_config"
        repository = RdsRepository.from_env()
        stage = "task_lookup"
        task = repository.get_task(args.task_id)
        if task is None:
            raise ReleaseGateError("发布前任务或选中成片不存在")
        if args.media_kind == "native_photo":
            stage = "photo_release_freeze"
            manifest = freeze_photo_release(repository, task)
        else:
            if not task.selected_render_id:
                raise ReleaseGateError("发布前任务或选中成片不存在")
            stage = "render_lookup"
            render = repository.get_render(task.selected_render_id)
            stage = "release_freeze"
            manifest = freeze_release(repository, task, render)
        stage = "manifest_compare"
        if manifest["manifest_sha256"] != args.manifest_sha256:
            raise ReleaseGateError("当前验收版本与队列冻结清单不同，禁止上传")
    except Exception as exc:
        print(json.dumps(failure_payload(exc, stage), ensure_ascii=False))
        return 1
    print("release_verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
