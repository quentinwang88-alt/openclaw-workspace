from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from .coverage import validate_coverage
from .audio import build_frozen_audio_plan
from .parser import parse_source
from .planner import plan_segments
from .repository import RemakeExecutionRepository
from .source import freeze_record
from .references import resolve_production_references
from .subtitles import build_subtitle_plan


def main() -> int:
    parser = argparse.ArgumentParser(description="复刻长视频执行计划工具")
    parser.add_argument("action", choices=("check", "plan"))
    parser.add_argument("--input", required=True, help="单条或多条 {record_id, fields} JSON")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--source-kind", default="")
    parser.add_argument("--record-id", action="append", default=[])
    parser.add_argument("--product-id", default="")
    parser.add_argument("--market", default="")
    parser.add_argument("--sku-id", default="DEFAULT")
    parser.add_argument("--reference-image-pack-id", default="")
    parser.add_argument("--reference-group-id", default="", help="operation:<运营记录ID> 或 amc:<包ID>")
    parser.add_argument("--reference-source", choices=("auto", "operation", "amc"), default="auto")
    args = parser.parse_args()
    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    records = payload if isinstance(payload, list) else [payload]
    if args.record_id:
        wanted = set(args.record_id)
        records = [item for item in records if item.get("record_id") in wanted]
    if args.source_kind:
        records = [
            item for item in records
            if str((item.get("fields") or {}).get("脚本来源") or "").strip() == args.source_kind
        ]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = []
    repository = RemakeExecutionRepository(args.db_path) if args.db_path else RemakeExecutionRepository()
    if args.action == "plan":
        repository.ensure_schema()
    for item in records:
        source = freeze_record(item["record_id"], item["fields"], item.get("structured_source"))
        if args.action == "plan" and (source.product_id or args.product_id):
            source = resolve_production_references(
                source, output_dir / "references" / (source.script_id or source.record_id),
                product_id=args.product_id, market=args.market, sku_id=args.sku_id,
                pack_id=args.reference_image_pack_id,
                group_id=args.reference_group_id, reference_source=args.reference_source,
            )
        plan = parse_source(source)
        segment_plan = plan_segments(plan)
        audio_plan, audio_issues = build_frozen_audio_plan(plan)
        issues = [*segment_plan.issues, *validate_coverage(plan, segment_plan), *audio_issues]
        result = {"source": source.to_dict(), "execution_plan": plan.to_dict(),
                  "segment_plan": segment_plan.to_dict(), "audio_plan": audio_plan,
                  "subtitle_plan": build_subtitle_plan(plan),
                  "issues": [issue.__dict__ for issue in issues]}
        target = output_dir / f"{source.script_id or source.record_id}.json"
        target.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        job_id = "remake_" + hashlib.sha256(
            f"{source.record_id}:{source.source_revision_hash}".encode()
        ).hexdigest()[:20]
        if args.action == "plan":
            job_id = repository.save_plan(job_id, source.to_dict(), segment_plan.to_dict())
        summary.append({"record_id": source.record_id, "script_id": source.script_id,
                        "job_id": job_id, "segments": len(segment_plan.segments),
                        "blocking_issues": [issue.code for issue in issues if issue.severity == "BLOCK"],
                        "output": str(target)})
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
