#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.final_video_qc_async_skill import FinalVideoQCAsyncSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


def main() -> int:
    parser = argparse.ArgumentParser(description="Run final video QC asynchronously for one rendered batch.")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps(init.to_dict(), ensure_ascii=False, indent=2, default=str))
        return 1
    res = FinalVideoQCAsyncSkill(ctx).run_batch(args.batch_id)
    print(json.dumps(res.to_dict(), ensure_ascii=False, indent=2, default=str))
    return 0 if res.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
