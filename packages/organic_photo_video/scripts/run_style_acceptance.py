#!/usr/bin/env python3
"""Audit real style selection, optionally produce TWO unpublished test sets.

Default is read-only for RDS/Feishu and never calls a model. --apply creates
idempotent testing tasks through the existing production services. No publisher
or Feishu writer is instantiated. The local manifest freezes the chosen inputs.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for value in (ROOT, ROOT.parents[1]):
    sys.path.insert(0, str(value))

from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository
from services.asset_resolver import LightTryonAssetReader
from services.batch_diversity_planner import BatchDiversityPlanner
from services.content_planner import _presentation_profile
from services.feishu_workflow import ProductionPresetCatalog
from services.product_reference_resolver import ProductReferenceResolver
from services.workflow_v2 import canonical_hash


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("product_id")
    parser.add_argument("--account-id", default="OPV_TH_TEST_001")
    parser.add_argument("--run-key", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_repo_env()
    repo, assets = RdsRepository.from_env(), LightTryonAssetReader()
    account = repo.get_account_profile(args.account_id)
    if account is None or account.status != "testing":
        raise ValueError("acceptance runner is restricted to testing accounts")
    profile = _presentation_profile(account)
    if profile.get("background_mode") != "solid_color" or account.operating_rules_json.get("look_selection_mode") != "auto_library":
        raise ValueError("deploy and read back the pure-color/auto-library contract before acceptance")
    receipt = canonical_hash({"profile": profile, "look_selection_mode": account.operating_rules_json.get("look_selection_mode")})
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (manifest["run_key"] != args.run_key or manifest["product_id"] != args.product_id
                or manifest["account_receipt"] != receipt):
            raise ValueError("existing acceptance inputs differ; use a new run key and output directory")
    else:
        catalog = ProductionPresetCatalog()
        resolver = ProductReferenceResolver(repo)  # read-only packs; no Feishu sync source
        specs = []
        for i in range(5):
            specs.extend(catalog.resolve_batch("TH｜随机养号组合｜轻文字", f"{args.run_key}:audit:{i}", 6))
        products = [resolver.resolve_snapshot(args.product_id, selection_key=f"{args.run_key}:{i}",
                    account_id=args.account_id, allow_history_bootstrap=False) for i in range(6)]
        assignments = BatchDiversityPlanner(repo, assets).plan(record_id=args.run_key + ":audit", specs=specs,
            products=[dict(products[i % len(products)]) for i in range(len(specs))])
        sample_specs = [catalog.resolve_batch(name, args.run_key, 1)[0] for name in
            ("TH｜穿搭拆解首图｜均衡变体", "TH｜一衣多穿｜轻文字")]
        samples = BatchDiversityPlanner(repo, assets).plan(record_id=args.run_key + ":samples", specs=sample_specs,
            products=[dict(products[0]), dict(products[1])])
        manifest = {"run_key": args.run_key, "product_id": args.product_id, "account_receipt": receipt,
            "profile": profile, "mode": "testing_unpublished", "simulated_count": len(assignments),
            "library_total": len(assets.list_look_ids()),
            "counts": {key: dict(Counter(str(a.metadata["axes"].get(key, "")) for a in assignments))
                for key in ("look_ref", "outfit_fingerprint", "visible_silhouette", "silhouette_key", "persona_ref", "reference_pack_id")},
            "simulation": [{"metadata": a.metadata, "look": assets.get_look(a.spec.look_ref)} for a in assignments],
            "samples": [{"spec": asdict(a.spec), "product": a.product_snapshot, "metadata": a.metadata} for a in samples]}
        dump(manifest_path, manifest)
    print(json.dumps({"mode": "apply" if args.apply else "read_only", "manifest": str(manifest_path),
        "simulated_count": manifest["simulated_count"], "counts": manifest["counts"],
        "samples": [{"look": s["spec"]["look_ref"], "alternate": s["product"].get("planned_alternate_look_ref"),
                    "recipe": s["spec"]["recipe_id"]} for s in manifest["samples"]]}, ensure_ascii=False), flush=True)
    if not args.apply:
        return 0
    from services.content_story import generate_product_image_story
    from services.hero_first import HeroFirstProducer
    from services.image_generator import OpenAIImageGenerator
    from services.technical_production import TechnicalProductionFlow
    from services.video_render_flow import VideoRenderFlow
    from services.video_renderer import FFmpegStillRenderer
    from services.feishu_workflow import PresetTask
    results_path = args.output_dir / "results.json"
    results = []
    prepared = []
    for i, entry in enumerate(manifest["samples"], 1):
        spec = PresetTask(**entry["spec"])
        if spec.account_id != args.account_id:
            raise ValueError("sample preset points to a different account")
        rows = generate_product_image_story(repo, product_id=args.product_id, market=spec.market, language=spec.language,
            recipe_id=spec.recipe_id, account_id=args.account_id, theme_id=spec.theme_id, hook_strategy=spec.hook_strategy,
            product_snapshot=dict(entry["product"]), asset_reader=assets, source_type="opv_style_acceptance",
            source_record_id_prefix=f"{args.run_key}:{i}:{spec.recipe_id}", operator="user_requested_style_acceptance",
            persona_ref=spec.persona_ref, look_ref=spec.look_ref, scene_ref=spec.scene_ref, variant_index_offset=i - 1)
        task_id = rows[0]["task_id"]
        print(json.dumps({"sample": i, "task_id": task_id, "phase": "planned"}), flush=True)
        task = repo.get_task(task_id)
        if canonical_hash(task.plan_json.get("presentation_profile") or {}) != canonical_hash(manifest["profile"]):
            raise ValueError("frozen task visual profile differs from acceptance manifest")
        prepared.append((i, task_id))
    dump(args.output_dir / "tasks.json", [{"sample": i, "task_id": task_id} for i, task_id in prepared])
    for i, task_id in prepared:
        print(json.dumps({"sample": i, "task_id": task_id, "phase": "production_start"}), flush=True)
        TechnicalProductionFlow(repo, HeroFirstProducer(repo, OpenAIImageGenerator(), technical_only=True),
                                VideoRenderFlow(repo, FFmpegStillRenderer())).run(task_id, overlay_profile_id="OVERLAY_LIGHT_V1")
        task = repo.get_task(task_id)
        render = repo.get_render(task.selected_render_id)
        result = {"sample": i, "task_id": task_id, "status": task.task_status, "plan": task.plan_json,
                  "video": render.output_url, "timeline": render.timeline_json, "media_qc": render.qc_json,
                  "active_revision": task.active_revision_id, "released_revision": task.released_revision_id,
                  "published": False}
        results.append(result)
        dump(results_path, results)
        print(json.dumps({"sample": i, "task_id": task_id, "phase": "complete", "video": render.output_url}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
