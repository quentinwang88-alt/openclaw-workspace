#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("AUTO_MIXCUT_ROOT", str(REPO_ROOT))

from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402


STATUS_MAP = {"approved": "可发布", "rejected": "不可发布"}


def main() -> int:
    ctx = build_context()
    client = AutoMixcutFeishuClient("成片质检表")
    rows = ctx.repo.list_where(
        "outputs",
        "feishu_record_id IS NOT NULL AND human_quality_status IN ('approved','rejected')",
    )
    updated = []
    skipped = []
    for row in rows:
        record_id = str(row.get("feishu_record_id") or "")
        if not record_id.startswith("rec"):
            skipped.append({"output_id": row["output_id"], "record_id": record_id, "reason": "fake_record"})
            continue
        status = STATUS_MAP.get(row.get("human_quality_status"))
        if not status:
            continue
        client.update_record(record_id, {"人工质检状态": status})
        updated.append({"output_id": row["output_id"], "record_id": record_id, "status": status})
    print(json.dumps({"success": True, "updated": updated, "skipped": skipped}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
