"""Unattended keyframe preparation and final execution for Feishu long-form rows."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from core.longform.contracts import stable_id
from core.longform.h3_gateway import H3Gateway
from core.longform.storage import DEFAULT_ASSET_ROOT, LongformStorage
from core.longform.workflow import run_to_final
from scripts.run_first_frame_tasks import _generate_image
from scripts.run_longform_original import (
    _k0_reference_paths,
    _register_initial_keyframes,
    _segment_entry_reference_paths,
    _reference_image_guidance,
)


def _json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        loaded = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def ensure_unattended_keyframes(
    storage: LongformStorage,
    job_id: str,
    *,
    asset_root: str | Path = DEFAULT_ASSET_ROOT,
) -> Dict[str, Any]:
    """Generate K0 plus any planned bridge/entry frames without human pauses."""

    row = storage.get_job(job_id)
    if not row:
        raise RuntimeError(f"找不到长视频任务: {job_id}")
    segments = list(row.get("segments") or [])
    if not segments:
        raise RuntimeError("长视频任务没有片段")
    if str(segments[0].get("status") or "") != "PLANNED":
        return {"job_id": job_id, "status": "ALREADY_REGISTERED"}

    plan = _json(row.get("plan_json"))
    keyframes = _json(row.get("keyframe_package_json"))
    root = Path(asset_root).expanduser().resolve() / job_id
    first_dir = root / "first_frame"
    first_dir.mkdir(parents=True, exist_ok=True)
    k0_contract = _json(keyframes.get("K0"))
    k0_prompt = str(k0_contract.get("prompt") or "").strip()
    if not k0_prompt:
        raise RuntimeError("长视频任务缺少K0提示词")
    k0 = Path(_generate_image(
        prompt=_reference_image_guidance(keyframes, _k0_reference_paths(keyframes)) + k0_prompt,
        reference_paths=_k0_reference_paths(keyframes),
        output_dir=first_dir,
        asset_id=stable_id("LFK0_", {"job_id": job_id, "prompt": k0_prompt}),
    ))

    planned_values = []
    previous_frame = str(k0)
    raw_references = _segment_entry_reference_paths(keyframes, str(k0))[1:]
    boundaries = list(_json(plan.get("bridge_contract")).get("boundaries") or [])
    for index, boundary in enumerate(boundaries, 1):
        if str(boundary.get("boundary_mode") or "CONTINUOUS") != "CONTINUOUS":
            continue
        key = f"K{index}_PLANNED"
        contract = _json(keyframes.get(key))
        prompt = str(contract.get("prompt") or "").strip()
        if not prompt:
            raise RuntimeError(f"长视频任务缺少{key}提示词")
        output_dir = root / "planned_frames" / key
        output_dir.mkdir(parents=True, exist_ok=True)
        generated = Path(_generate_image(
            prompt=_reference_image_guidance(keyframes, [previous_frame, *raw_references]) + prompt,
            reference_paths=[previous_frame, *raw_references],
            output_dir=output_dir,
            asset_id=stable_id(
                "LFPF_", {"job_id": job_id, "key": key, "prompt": prompt}
            ),
        ))
        planned_values.append(f"K{index}={generated}")
        previous_frame = str(generated)

    return _register_initial_keyframes(
        storage,
        row,
        plan,
        job_id,
        str(k0),
        planned_values,
        scene_entry_values=[],
        auto_generate_scene_entries=True,
        auto_generate_k0=False,
        asset_root=str(Path(asset_root).expanduser().resolve()),
    )


def run_longform_job_to_final(
    job_id: str,
    *,
    storage: LongformStorage | None = None,
    asset_root: str | Path = DEFAULT_ASSET_ROOT,
    allow_real_submit: bool,
    allow_external_tts: bool,
    poll_interval_seconds: int = 30,
    max_wait_seconds: int = 1200,
    voiceover_model_command: str = "",
) -> Dict[str, Any]:
    storage = storage or LongformStorage()
    storage.ensure_schema()
    row = storage.get_job(job_id)
    if not row:
        raise RuntimeError(f"找不到长视频任务: {job_id}")
    needs_h3 = any(
        str(item.get("status") or "") != "READY"
        for item in row.get("segments") or []
    )
    preflight = H3Gateway(
        state_root=Path(asset_root).expanduser().resolve() / job_id / "h3_state"
    ).preflight(require_api_key=needs_h3)
    if needs_h3 and not preflight["ready"]:
        raise RuntimeError("；".join(preflight["problems"]))
    ensure_unattended_keyframes(storage, job_id, asset_root=asset_root)
    kwargs = {}
    if voiceover_model_command:
        kwargs["voiceover_model_command"] = voiceover_model_command
    return run_to_final(
        storage,
        job_id,
        asset_root=asset_root,
        allow_real_submit=allow_real_submit,
        allow_external_tts=allow_external_tts,
        manual_bridge=False,
        poll_interval_seconds=poll_interval_seconds,
        max_wait_seconds=max_wait_seconds,
        **kwargs,
    )
