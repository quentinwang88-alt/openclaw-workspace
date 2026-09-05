#!/usr/bin/env python3
"""Read-only checks of a scoped OPV batch's saved images and rendered videos."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PACKAGE_ROOT), str(PACKAGE_ROOT.parents[1])]


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record-id", required=True)
    parser.add_argument("--verify-media", action="store_true")
    parser.add_argument("--compact", action="store_true", help="omit individual image paths")
    args = parser.parse_args()
    from workspace_support import load_repo_env
    load_repo_env()
    from repositories.rds_repository import RdsRepository
    repository = RdsRepository.from_env()
    batch = repository.get_production_batch(args.record_id)
    tasks = repository.list_tasks_by_source_prefix("feishu_opv", args.record_id)
    result = {"record_id": args.record_id,
              "expected_count": batch.expected_count if batch else None,
              "batch_status": batch.batch_status if batch else None,
              "task_count": len(tasks), "tasks": [], "issues": []}
    for task in tasks:
        revision = repository.get_task_revision(task.active_revision_id) if task.active_revision_id else None
        selected = ((revision.asset_manifest_json or {}).get("selected") or {}) if revision else {}
        shots = []
        for shot in repository.list_shots(task.task_id):
            if revision and shot.origin_revision_id != revision.revision_id and shot.shot_id not in selected.values():
                continue
            info = {"slot": shot.slot_index, "id": shot.shot_id, "status": shot.shot_status,
                    "selected": selected.get(f"shot:{shot.slot_index}") == shot.shot_id,
                    "path": shot.image_url}
            if args.verify_media and shot.image_url:
                try:
                    from PIL import Image
                    path = Path(shot.image_url)
                    with Image.open(path) as image:
                        image.load()
                        info["dimensions"] = list(image.size)
                    if digest(path) != shot.image_sha256:
                        raise ValueError("image SHA256 mismatch")
                    if info["dimensions"] != [shot.image_width, shot.image_height]:
                        raise ValueError("image dimensions mismatch")
                    info["media_verified"] = True
                except Exception as exc:
                    info["media_verified"] = False
                    result["issues"].append(f"{task.task_id} P{shot.slot_index}: {exc}")
            shots.append(info)
        item = {"task_id": task.task_id, "status": task.task_status,
                "recipe": task.recipe_id, "active_revision": task.active_revision_id,
                "released_revision": task.released_revision_id,
                "shots": shots, "render": None,
                "publish_records": len(repository.list_publish_records_by_task(task.task_id))}
        render = repository.get_render(task.selected_render_id) if task.selected_render_id else None
        if render:
            video = {"id": render.render_id, "path": render.output_url,
                     "qc_status": render.qc_status, "duration_ms": render.duration_ms,
                     "publish_ready": render.publish_ready}
            if args.verify_media:
                try:
                    path = Path(render.output_url)
                    if digest(path) != render.output_sha256:
                        raise ValueError("video SHA256 mismatch")
                    ffmpeg = shutil.which("ffmpeg") or str(Path.home() / ".local/bin/ffmpeg")
                    completed = subprocess.run(
                        [ffmpeg, "-v", "error", "-xerror", "-i", str(path), "-f", "null", "-"],
                        capture_output=True, text=True, timeout=60,
                    )
                    if completed.returncode:
                        raise ValueError("video decode failed: " + completed.stderr[-500:])
                    video["media_verified"] = True
                except Exception as exc:
                    video["media_verified"] = False
                    result["issues"].append(f"{task.task_id} video: {exc}")
            item["render"] = video
        item["generated_count"] = sum(row["status"] in {"generated", "approved"} for row in shots)
        item["selected_count"] = sum(row["selected"] for row in shots)
        item["shot_states"] = {state: sum(row["status"] == state for row in shots)
                               for state in sorted({row["status"] for row in shots})}
        if args.compact:
            item.pop("shots")
        result["tasks"].append(item)
    result["render_count"] = sum(bool(row["render"]) for row in result["tasks"])
    if batch and len(tasks) != batch.expected_count:
        result["issues"].append("batch task count mismatch")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 2 if result["issues"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
