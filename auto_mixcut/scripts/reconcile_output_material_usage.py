#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.output_material_usage_skill import OutputMaterialUsageSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild output_material_usage from durable output_segments.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--output-id")
    group.add_argument("--batch-id")
    group.add_argument("--product-id")
    args = parser.parse_args()
    ctx = build_context()
    initialized = RDSRepositorySkill(ctx).init_db()
    if not initialized.success:
        print(json.dumps(initialized.to_dict(), ensure_ascii=False))
        return 1
    skill = OutputMaterialUsageSkill(ctx)
    result = skill.refresh_output(args.output_id) if args.output_id else skill.refresh_batch(args.batch_id) if args.batch_id else skill.refresh_product(args.product_id)
    print(json.dumps(result.to_dict(), ensure_ascii=False, default=str))
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
