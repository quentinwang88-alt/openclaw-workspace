#!/usr/bin/env python3
"""Read-only production verification; refresh a local acceptance report."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for value in (ROOT, ROOT.parents[1]):
    sys.path.insert(0, str(value))
from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository
from services.technical_production import TechnicalCheckService
from services.release_gate import file_hash, expected_render_fingerprint
from services.workflow_v2 import RevisionAssetResolver


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    load_repo_env()
    repo = RdsRepository.from_env()
    checks = TechnicalCheckService(repo)
    results = []
    for item in json.loads((args.output_dir / "tasks.json").read_text()):
        task = repo.get_task(item["task_id"])
        assert repo.get_account_profile(task.account_id).status == "testing"
        assert not repo.list_publish_records_by_task(task.task_id)
        assert task.active_revision_id == task.released_revision_id
        revision = checks.revision(task)
        for spec in revision.plan_snapshot_json["plan"]["shots"]:
            checks._shot(revision, RevisionAssetResolver.selected(revision, f"shot:{spec['slot_index']}"))
        render = repo.get_render(task.selected_render_id)
        assert render.qc_json["passed"] and render.origin_revision_id == revision.revision_id
        assert render.input_fingerprint == expected_render_fingerprint(revision)
        assert file_hash(render.output_url) == render.output_sha256
        results.append({**item, "status": task.task_status, "plan": task.plan_json,
            "video": render.output_url, "timeline": render.timeline_json, "media_qc": render.qc_json,
            "active_revision": task.active_revision_id, "released_revision": task.released_revision_id,
            "published": False, "publish_records": 0, "verification": "selected_files_and_frozen_inputs_passed"})
    (args.output_dir / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    print(json.dumps([{k: r[k] for k in ("task_id", "video", "released_revision", "publish_records", "verification")}
                      for r in results], ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
