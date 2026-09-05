#!/usr/bin/env python3
"""Read-only real-library rotation/admission report. No models, intake or writes."""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import replace
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT.parents[1]):
    sys.path.insert(0, str(path))
from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository
from services.asset_resolver import LightTryonAssetReader
from services.asset_readiness import check_generation_assets
from services.batch_diversity_planner import BatchDiversityPlanner
from services.feishu_workflow import ProductionPresetCatalog
from services.product_reference_resolver import ProductReferenceResolver


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("product_id")
    parser.add_argument("--quantity", type=int, default=9)
    parser.add_argument("--preset", default="TH｜穿搭拆解首图｜均衡变体")
    parser.add_argument("--record-key", default="asset-readiness-audit")
    parser.add_argument("--require-production", action="store_true", help="仅在内存中按 active 门槛核验，绝不修改账户")
    args = parser.parse_args()
    load_repo_env()
    repository, assets = RdsRepository.from_env(), LightTryonAssetReader()
    specs = ProductionPresetCatalog().resolve_batch(args.preset, args.record_key, args.quantity)
    resolver = ProductReferenceResolver(repository)  # no candidate sync source
    products = [resolver.resolve_snapshot(args.product_id, selection_key=f"{args.record_key}:{index}",
        account_id=spec.account_id, allow_history_bootstrap=False) for index, spec in enumerate(specs, 1)]
    assignments = BatchDiversityPlanner(repository, assets).plan(record_id=args.record_key, specs=specs, products=products)
    rows = []
    for assignment in assignments:
        spec, product = assignment.spec, assignment.product_snapshot
        account = repository.get_account_profile(spec.account_id)
        if args.require_production:
            account = replace(account, status="active")
        plan = {
            "recipe": {"id": spec.recipe_id},
            "persona": {"ref_id": spec.persona_ref, "snapshot": assets.get_persona(spec.persona_ref)},
            "look": {"ref_id": spec.look_ref, "snapshot": assets.get_look(spec.look_ref)},
            "scene": {"ref_id": spec.scene_ref, "snapshot": assets.get_scene(spec.scene_ref)},
            "shots": [{"composition_contract": {"framing": "garment_neck_to_upper_thigh"}}],
        }
        rows.append({"axes": assignment.metadata["axes"], "admission": check_generation_assets(account, product, plan)})
    ready = all(row["admission"]["ready"] for row in rows)
    print(json.dumps({"mode": "read_only", "scope": "asset_planning_not_generated_output_acceptance",
        "ready": ready, "counts": {key: dict(Counter(row["axes"][key] for row in rows))
            for key in ("persona_ref", "look_ref", "scene_ref", "reference_pack_id")}, "rows": rows}, ensure_ascii=False, indent=2))
    return 0 if ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
