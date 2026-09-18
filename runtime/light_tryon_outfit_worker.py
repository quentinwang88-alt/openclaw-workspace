#!/usr/bin/env python3
"""Bridge light-tryon visual plans to the openai-image skill.

Reads the light-tryon worker JSON protocol from stdin and prints:
{"output_image_path": "...", "image_version": "..."}
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
OPENAI_IMAGE = WORKSPACE / "skills" / "openai-image" / "run_pipeline.py"
OUTPUT_ROOT = WORKSPACE / "skills" / "lightweight-tryon-video" / "var" / "generated_outfits"


def main() -> int:
    payload = json.load(sys.stdin)
    visual_plan = payload.get("visual_plan") or {}
    request = payload.get("request") or {}
    plan_id = str(visual_plan.get("visual_plan_id") or "light_tryon_outfit").strip()
    source_record_id = str(visual_plan.get("source_record_id") or "unknown_source").strip()
    prompt = str(request.get("prompt") or "").strip()
    references = request.get("references") or []

    image_paths = []
    for item in references:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value") or "").strip()
        if value and Path(value).expanduser().is_file():
            image_paths.append(str(Path(value).expanduser().resolve()))

    if not prompt:
        raise ValueError("Missing outfit prompt")
    if not image_paths:
        raise ValueError("Missing local reference images")

    out_dir = OUTPUT_ROOT / source_record_id
    out_dir.mkdir(parents=True, exist_ok=True)
    task = {
        "task_id": f"{plan_id}_v1",
        "task_type": "light_tryon_outfit",
        "target_field": "outfit_image_path",
        "mode": "edit",
        "prompt": prompt,
        "input_image_path": image_paths[0],
        "input_image_paths": image_paths,
        "size": "1024x1536",
        "quality": "medium",
        "output_format": "png",
        "output_dir": str(out_dir),
        "metadata": {
            "source_record_id": source_record_id,
            "visual_plan_id": plan_id,
            "product_code": visual_plan.get("product_code") or "",
        },
    }

    with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as handle:
        json.dump(task, handle, ensure_ascii=False)
        input_path = Path(handle.name)

    try:
        completed = subprocess.run(
            [sys.executable, str(OPENAI_IMAGE), "--input", str(input_path)],
            cwd=str(WORKSPACE),
            text=True,
            capture_output=True,
            check=False,
            timeout=900,
        )
    finally:
        input_path.unlink(missing_ok=True)

    stdout = completed.stdout.strip()
    if completed.returncode != 0:
        detail = stdout or completed.stderr.strip()
        raise RuntimeError(f"openai-image failed: {detail[-1200:]}")

    result = _parse_json_from_stdout(stdout)
    paths = result.get("output_image_paths") or []
    if not paths:
        raise RuntimeError(f"openai-image returned no output image: {stdout[-1200:]}")

    print(json.dumps({
        "output_image_path": paths[0],
        "image_version": f"openai-image-{result.get('model') or 'gpt-image-2'}",
    }, ensure_ascii=False))
    return 0


def _parse_json_from_stdout(stdout: str) -> dict:
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        pass
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if not line.startswith("{"):
            continue
        candidate = "\n".join(lines[index:])
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    raise ValueError(f"Could not parse openai-image JSON output: {stdout[-1200:]}")


if __name__ == "__main__":
    raise SystemExit(main())
