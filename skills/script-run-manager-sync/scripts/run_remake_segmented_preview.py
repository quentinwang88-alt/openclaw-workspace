#!/usr/bin/env python3
"""Build local, non-paid execution previews for segmented remake rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = WORKSPACE / "packages" / "remake_video_execution"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from remake_video_execution.coverage import validate_coverage  # noqa: E402
from remake_video_execution.audio import build_frozen_audio_plan  # noqa: E402
from remake_video_execution.parser import parse_source  # noqa: E402
from remake_video_execution.planner import plan_segments  # noqa: E402
from remake_video_execution.repository import RemakeExecutionRepository  # noqa: E402
from remake_video_execution.source import freeze_record  # noqa: E402
from remake_video_execution.subtitles import build_subtitle_plan  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="复刻分段视频本地预览（不提交媒体、不写飞书）")
    parser.add_argument("--snapshot", required=True, help="{record_id,fields} 数组 JSON")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--freeze-plan", action="store_true", help="写入独立本地任务库")
    parser.add_argument("--db-path", default="")
    args = parser.parse_args()
    records = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    repository = RemakeExecutionRepository(args.db_path) if args.db_path else RemakeExecutionRepository()
    if args.freeze_plan:
        repository.ensure_schema()
    summary = []
    for item in records:
        fields = item.get("fields") or {}
        if str(fields.get("脚本来源") or "").strip() != "视频复刻":
            continue
        try:
            duration = int(float(fields.get("视频时长") or 15))
        except (TypeError, ValueError):
            duration = 15
        if duration <= 15:
            continue
        source = freeze_record(item["record_id"], fields, item.get("structured_source"))
        execution = parse_source(source)
        plan = plan_segments(execution)
        audio_plan, audio_issues = build_frozen_audio_plan(execution)
        issues = [*plan.issues, *validate_coverage(execution, plan), *audio_issues]
        payload = {
            "source": source.to_dict(), "execution_plan": execution.to_dict(),
            "segment_plan": plan.to_dict(), "audio_plan": audio_plan,
            "subtitle_plan": build_subtitle_plan(execution),
            "issues": [issue.__dict__ for issue in issues],
        }
        target = output / f"{source.script_id}.json"
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        job_id = "remake_" + hashlib.sha256(
            f"{source.record_id}:{source.source_revision_hash}".encode("utf-8")
        ).hexdigest()[:20]
        if args.freeze_plan:
            job_id = repository.save_plan(job_id, source.to_dict(), plan.to_dict())
        summary.append({
            "record_id": source.record_id, "script_id": source.script_id,
            "job_id": job_id,
            "segments": len(plan.segments),
            "blocking": [issue.code for issue in issues if issue.severity == "BLOCK"],
            "output": str(target),
        })
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
