#!/usr/bin/env python3
"""Explicit test-only stdin JSON -> warning JSON; default is a zero-model dry-run."""
import argparse
import json
import os
import re
from pathlib import Path
import sys

from wsr_content_review import review_job


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply-model", action="store_true")
    parser.add_argument("--job", type=Path)
    parser.add_argument("--state-dir", type=Path, default=Path(os.environ.get(
        "WSR_CONTENT_REVIEW_STATE_DIR", str(Path.home() / ".openclaw/shared/data/wsr_content_review"))))
    args = parser.parse_args()
    try:
        job = json.loads(args.job.read_text() if args.job else sys.stdin.read())
        result = review_job(job, args.state_dir, apply_model=args.apply_model)
    except Exception as exc:
        result = {"status": "unknown", "summary": "内容检查输入或运行异常：" + type(exc).__name__,
                  "evidence": [], "attempts": 0, "cached": False}
        if isinstance(exc, ValueError) and re.fullmatch(r"[A-Z_]{3,100}", str(exc)):
            result["error_code"] = str(exc)
    print(json.dumps(result, ensure_ascii=False))
    return 0  # warning-only: never turn a completed video into a generation failure


if __name__ == "__main__":
    raise SystemExit(main())
