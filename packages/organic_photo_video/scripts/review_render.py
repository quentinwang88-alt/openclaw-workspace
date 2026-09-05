#!/usr/bin/env python3
"""Record the final visual decision for one Workflow V2 rendered video.

Technical video QC is intentionally insufficient for Workflow V2. This
command is the explicit final gate after an operator has viewed the exact
rendered MP4. A passed decision pins the render and its revision for publish;
it does not submit anything to NeoBund.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for entry in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from workspace_support import load_repo_env  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.workflow_v2 import RenderReviewService  # noqa: E402


def _dimensions(raw: str) -> dict:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError("--dimensions must be a JSON object") from exc
    if not isinstance(value, dict):
        raise argparse.ArgumentTypeError("--dimensions must be a JSON object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--render-id", required=True)
    parser.add_argument("--decision", choices=("passed", "failed", "waived"), required=True)
    parser.add_argument("--reviewer", required=True, help="human reviewer/account name")
    parser.add_argument(
        "--dimensions", type=_dimensions,
        default={"render_visual_quality": "human_reviewed"},
        help='JSON, e.g. {"continuity":88,"typography":92,"naturalness":85}',
    )
    parser.add_argument("--reason", action="append", default=[])
    parser.add_argument("--notes", default="")
    args = parser.parse_args()

    load_repo_env()
    repository = RdsRepository.from_env()
    review = RenderReviewService(repository).record(
        args.task_id,
        render_id=args.render_id,
        decision=args.decision,
        dimensions=args.dimensions,
        reason_codes=args.reason,
        evidence={"notes": args.notes, "review_mode": "human_final_video_review"},
        reviewer_type="human",
        reviewer=args.reviewer,
    )
    task = repository.get_task(args.task_id)
    render = repository.get_render(args.render_id)
    print(json.dumps({
        "task_id": args.task_id,
        "render_id": args.render_id,
        "decision": review.decision,
        "released_revision_id": getattr(task, "released_revision_id", None),
        "publish_ready": bool(getattr(render, "publish_ready", False)),
    }, ensure_ascii=False, indent=2))
    return 0 if review.decision in {"passed", "waived"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
