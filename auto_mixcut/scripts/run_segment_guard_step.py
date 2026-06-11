#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.core.result import Result  # noqa: E402
from auto_mixcut.skills.ai_anchor_check_skill import AIAnchorCheckSkill  # noqa: E402
from auto_mixcut.skills.ai_generated_consistency_skill import AIGeneratedConsistencySkill  # noqa: E402
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill  # noqa: E402
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill  # noqa: E402
from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402
from auto_mixcut.skills.segment_fingerprint_skill import SegmentFingerprintSkill  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one bounded per-segment guard repair step.")
    parser.add_argument("--step", choices=["frame_sample", "fingerprint", "tag_poll", "consistency", "anchor_check", "effective_roles"], required=True)
    parser.add_argument("--segment-id", required=True)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--index", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps(init.to_dict(), ensure_ascii=False, default=str))
        return 1

    if args.step == "frame_sample":
        res = FrameSampleSkill(ctx).sample_segment(args.segment_id)
    elif args.step == "fingerprint":
        res = SegmentFingerprintSkill(ctx).fingerprint_segment(args.segment_id)
    elif args.step == "tag_poll":
        segment = ctx.repo.get("segments", "segment_id", args.segment_id)
        if not segment:
            res = Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": args.segment_id})
        elif not args.product_id:
            res = Result.fail("PRODUCT_ID_REQUIRED", "product_id is required for tag_poll", {"segment_id": args.segment_id})
        else:
            res = AITaggingSkill(ctx)._poll_segment(args.product_id, segment, args.index, "v1.0", args.force)
    elif args.step == "consistency":
        res = AIGeneratedConsistencySkill(ctx).check_segment(args.segment_id, force=args.force)
    elif args.step == "anchor_check":
        res = AIAnchorCheckSkill(ctx).check_segment(args.segment_id, force=args.force)
    elif args.step == "effective_roles":
        res = EffectiveRoleSkill(ctx).compute_segment(args.segment_id)
    else:
        res = Result.fail("UNKNOWN_GUARD_STEP", f"unknown step: {args.step}", {"segment_id": args.segment_id})

    print(json.dumps(res.to_dict(), ensure_ascii=False, default=str))
    return 0 if res.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
