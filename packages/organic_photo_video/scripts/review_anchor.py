#!/usr/bin/env python3
"""Run the V2 scoped visual review for one generated anchor candidate."""

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
from services.visual_qa import CreatorCrmVisualQaAdapter  # noqa: E402
from services.workflow_v2 import AnchorReviewService, RevisionAssetResolver  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--asset-id", default="", help="default: the only/selected anchor candidate")
    parser.add_argument("--reviewer", default="opv_anchor_vision")
    args = parser.parse_args()
    load_repo_env()
    repository = RdsRepository.from_env()
    task = repository.get_task(args.task_id)
    if task is None or not task.active_revision_id:
        raise RuntimeError("task has no active Workflow V2 revision")
    revision = repository.get_task_revision(task.active_revision_id)
    if revision is None:
        raise RuntimeError("active revision is missing")
    asset_id = args.asset_id
    if not asset_id:
        selected = (revision.asset_manifest_json.get("selected") or {}).get("anchor")
        candidates = revision.asset_manifest_json.get("candidates") or {}
        asset_id = str(selected or (next(iter(candidates), "")))
    asset = RevisionAssetResolver.candidate(revision, asset_id)
    result = AnchorReviewService(repository).review_with_adapter(
        args.task_id, asset_id=asset_id, adapter=CreatorCrmVisualQaAdapter(), reviewer=args.reviewer
    )
    print(json.dumps({
        "task_id": args.task_id, "asset_id": asset["asset_id"],
        "decision": result.review.decision, "advanced": result.advanced,
        "revision_id": result.revision.revision_id,
        "selection_hash": result.revision.selection_hash,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
