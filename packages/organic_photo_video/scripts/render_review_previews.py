#!/usr/bin/env python3
"""Render a watermarked local OPV review preview without approving or releasing it."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402
from services.review_preview import ReviewPreviewService  # noqa: E402
from services.video_renderer import FFmpegStillRenderer  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id", nargs="+", help="explicit OPV task IDs")
    parser.add_argument(
        "--output-root",
        default=str(Path.home() / ".openclaw" / "shared" / "data" / "organic_photo_video"),
        help="local review-artifact root; no database render record is created",
    )
    args = parser.parse_args()

    service = ReviewPreviewService(
        RdsRepository.from_env(), FFmpegStillRenderer(), output_root=Path(args.output_root)
    )
    results = []
    for task_id in args.task_id:
        preview = service.create(task_id)
        results.append(
            {
                "task_id": preview.task_id,
                "preview_only": True,
                "publish_ready": False,
                "output_path": preview.output_path,
                "manifest_path": preview.manifest_path,
                "input_fingerprint": preview.input_fingerprint,
                "duration_ms": preview.duration_ms,
            }
        )
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
