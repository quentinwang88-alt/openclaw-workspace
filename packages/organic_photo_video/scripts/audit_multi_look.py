#!/usr/bin/env python3
"""Plan two multi-look sets from real read-only inputs; never create RDS tasks."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for value in (ROOT.parents[1], ROOT):
    sys.path.insert(0, str(value))
from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository
from domain.models import ContentTask
from services.asset_resolver import LightTryonAssetReader
from services.product_reference_resolver import ProductReferenceResolver
from services.feishu_workflow import ProductionPresetCatalog
from services.batch_diversity_planner import BatchDiversityPlanner
from services.content_planner import ContentPlannerService
from services.text_overlay import apply_overlay_profile


class ReadOnlyPlanningRepo:
    """Writes confined to local draft objects; no write delegation possible."""
    def __init__(self, actual):
        self.actual, self.tasks = actual, {}
    def __getattr__(self, name):
        if name.startswith(("get_", "list_")):
            return getattr(self.actual, name)
        raise AttributeError(name)
    def get_task(self, key):
        return self.tasks[key]
    def update_task_plan(self, key, **fields):
        for name, value in fields.items():
            setattr(self.tasks[key], name, value)
    def transition_task(self, key, before, after, **kw):
        assert self.tasks[key].task_status == before
        self.tasks[key].task_status = after


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("product_id")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    load_repo_env()
    repo, reader = RdsRepository.from_env(), LightTryonAssetReader()
    specs = ProductionPresetCatalog().resolve_batch("TH｜五套穿搭｜拆解首图", "multi-look-readonly-audit", 2)
    resolver = ProductReferenceResolver(repo)
    products = [resolver.resolve_snapshot(args.product_id, account_id=s.account_id,
                selection_key=f"multi-look-audit:{i}", allow_history_bootstrap=False) for i, s in enumerate(specs)]
    assignments = BatchDiversityPlanner(repo, reader).plan(record_id="multi-look-readonly-audit", specs=specs, products=products)
    memory = ReadOnlyPlanningRepo(repo)
    plans = []
    for i, item in enumerate(assignments, 1):
        s = item.spec
        task = ContentTask(task_id=f"local-multi-look-audit-{i}", idempotency_key=f"local-audit-{i}",
            account_id=s.account_id, product_id=args.product_id, target_country=s.market,
            target_locale="th-TH", task_status="draft", theme_id=s.theme_id,
            product_snapshot_json={"product": item.product_snapshot})
        memory.tasks[task.task_id] = task
        result = ContentPlannerService(memory, asset_reader=reader).plan_task(task.task_id,
            theme_id=s.theme_id, recipe_id=s.recipe_id, hook_strategy=s.hook_strategy, variant_index=i)
        plans.append(apply_overlay_profile(result.plan, recipe_id=s.recipe_id, locale="th-TH", profile_id="OVERLAY_LIGHT_V1"))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"database_writes": 0, "model_calls": 0, "plans": plans}, ensure_ascii=False, indent=2))
    print(json.dumps([{ "pages": len(p["shots"]), "looks": [s["look_ref"] for s in p["outfit_sequence"]],
        "states": [s["outfit_state_ref"] for s in p["shots"]], "duration_ms": sum(s["duration_ms"] for s in p["shots"])}
        for p in plans], ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
