#!/usr/bin/env python3
"""Reconcile rendered segment/BGM usage counters from durable output records."""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.ids import new_id
from auto_mixcut.skills.usage_counter_skill import is_good_rendered_output, is_rejected_rendered_output, reconcile_product_segment_usage


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product-id", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-bgm", action="store_true", help="only reconcile segment counters; leave global BGM counters untouched")
    args = parser.parse_args()

    ctx = build_context()
    where = "product_id=?" if args.product_id else "1=1"
    params = (args.product_id,) if args.product_id else ()
    outputs = ctx.repo.list_where("outputs", where, params)
    rendered_outputs_for_segments = [output for output in outputs if is_good_rendered_output(output)]
    rejected_outputs_for_segments = [output for output in outputs if is_rejected_rendered_output(output)]
    output_ids = [output["output_id"] for output in rendered_outputs_for_segments]
    rejected_output_ids = [output["output_id"] for output in rejected_outputs_for_segments]

    segment_counts: Counter[str] = Counter()
    if output_ids:
        placeholders = ",".join(["?"] * len(output_ids))
        rows = ctx.repo.list_where("output_segments", f"output_id IN ({placeholders})", tuple(output_ids))
        segment_counts.update(str(row.get("segment_id") or "") for row in rows if row.get("segment_id"))
    rejected_segment_counts: Counter[str] = Counter()
    if rejected_output_ids:
        placeholders = ",".join(["?"] * len(rejected_output_ids))
        rows = ctx.repo.list_where("output_segments", f"output_id IN ({placeholders})", tuple(rejected_output_ids))
        rejected_segment_counts.update(str(row.get("segment_id") or "") for row in rows if row.get("segment_id"))

    bgm_counts: Counter[str] = Counter()
    bgm_events = []
    rendered_outputs_for_bgm = []
    rejected_outputs_for_bgm = []
    if not args.skip_bgm:
        all_outputs = ctx.repo.list_where("outputs", "1=1")
        rendered_outputs_for_bgm = [output for output in all_outputs if is_good_rendered_output(output)]
        rejected_outputs_for_bgm = [output for output in all_outputs if is_rejected_rendered_output(output)]
        for output in rendered_outputs_for_bgm:
            bgm_id = str(((output.get("bgm_plan_json") or {}).get("bgm_id")) or "")
            if not bgm_id:
                continue
            bgm_counts[bgm_id] += 1
            bgm_events.append(
                {
                    "event_id": new_id("BGMUSE"),
                    "bgm_id": bgm_id,
                    "output_id": output.get("output_id"),
                    "batch_id": output.get("batch_id"),
                    "product_id": output.get("product_id"),
                    "template_id": output.get("template_id"),
                    "usage_status": "rendered",
                    "quality_status": output.get("machine_quality_status"),
                    "reason": "reconciled_from_outputs",
                    "created_at": datetime.utcnow().isoformat(timespec="seconds"),
                }
            )
        for output in rejected_outputs_for_bgm:
            bgm_id = str(((output.get("bgm_plan_json") or {}).get("bgm_id")) or "")
            if not bgm_id:
                continue
            bgm_events.append(
                {
                    "event_id": new_id("BGMUSE"),
                    "bgm_id": bgm_id,
                    "output_id": output.get("output_id"),
                    "batch_id": output.get("batch_id"),
                    "product_id": output.get("product_id"),
                    "template_id": output.get("template_id"),
                    "usage_status": "rejected",
                    "quality_status": output.get("machine_quality_status"),
                    "reason": "reconciled_from_outputs",
                    "created_at": datetime.utcnow().isoformat(timespec="seconds"),
                }
            )

    if not args.dry_run:
        if args.product_id:
            segment_result = reconcile_product_segment_usage(ctx, args.product_id)
            touched_segments = int(segment_result.get("segments_touched") or 0)
        else:
            all_segments = ctx.repo.list_where("segments", where, params)
            touched_segments = 0
            for segment in all_segments:
                segment_id = str(segment.get("segment_id") or "")
                count = int(segment_counts.get(segment_id, 0))
                rejected_count = int(rejected_segment_counts.get(segment_id, 0))
                ctx.repo.update("segments", "segment_id", segment_id, {"used_in_outputs_count": count, "used_in_rejected_outputs_count": rejected_count})
                touched_segments += 1

        if not args.skip_bgm:
            existing = ctx.repo.list_where("bgm_usage_events", "reason='reconciled_from_outputs'")
            for row in existing:
                ctx.repo.delete_where("bgm_usage_events", "event_id=?", (row["event_id"],))
            for event in bgm_events:
                ctx.repo.insert("bgm_usage_events", event)
            for track in ctx.repo.list_where("bgm_tracks", "1=1"):
                ctx.repo.update("bgm_tracks", "bgm_id", track["bgm_id"], {"usage_count": int(bgm_counts.get(track["bgm_id"], 0))})
            for bgm_id, count in bgm_counts.items():
                ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, {"usage_count": int(count)})
    else:
        all_segments = ctx.repo.list_where("segments", where, params)
        touched_segments = len(all_segments)

    print(
        {
            "product_id": args.product_id or "*",
            "outputs_seen": len(outputs),
            "good_rendered_outputs": len(rendered_outputs_for_segments),
            "rejected_rendered_outputs": len(rejected_outputs_for_segments),
            "segments_touched": touched_segments,
            "segments_with_usage": len(segment_counts),
            "segments_with_rejected_usage": len(rejected_segment_counts),
            "global_good_rendered_outputs_for_bgm": len(rendered_outputs_for_bgm),
            "global_rejected_rendered_outputs_for_bgm": len(rejected_outputs_for_bgm),
            "bgm_tracks_with_usage": len(bgm_counts),
            "skip_bgm": args.skip_bgm,
            "dry_run": args.dry_run,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
