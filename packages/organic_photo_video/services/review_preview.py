"""Local-only, explicitly watermarked review previews.

This module is intentionally outside :mod:`video_render_flow`: a preview is
not a ``VideoRender`` row, is never selected on a task and cannot satisfy the
release gate.  It exists so operators can inspect a complete image group
without first converting model aesthetic observations into release state.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, List, Sequence

from services.video_renderer import TimelineSlot, build_timeline, sha256_file


PREVIEW_WATERMARK = "待审核预览 · 不可发布"
PREVIEW_SCHEMA_VERSION = "opv-review-preview-v1"


class ReviewPreviewError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReviewPreview:
    """Immutable result for a local review artifact, never a release object."""

    task_id: str
    manifest_path: str
    output_path: str
    input_fingerprint: str
    output_sha256: str
    duration_ms: int
    media_qc: Dict[str, Any]


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ReviewPreviewService:
    """Render a review copy from currently selected, technically valid images.

    The service has no repository write method by design.  Its only side
    effect is a local video plus a sidecar manifest under ``review_previews``.
    Callers must run the normal group approval and formal render path to make a
    master eligible for release.
    """

    def __init__(self, repository: Any, renderer: Any, *, output_root: Path):
        self._repository = repository
        self._renderer = renderer
        self._output_root = Path(output_root)

    def create(self, task_id: str) -> ReviewPreview:
        task = self._repository.get_task(task_id)
        if task is None:
            raise ReviewPreviewError(f"task {task_id} not found")
        task, shots = self._selected_task_and_shots(task)
        plan = task.plan_json or {}
        plan_shots = list(plan.get("shots") or [])
        if not plan_shots:
            raise ReviewPreviewError("task has no planned shots")
        self._validate_shots(plan_shots, shots)

        assets = [
            {
                "slot_index": shot.slot_index,
                "shot_id": shot.shot_id,
                "shot_version": shot.shot_version,
                "path": str(Path(shot.image_url).resolve()),
                "sha256": shot.image_sha256,
            }
            for shot in sorted(shots, key=lambda item: item.slot_index)
        ]
        fingerprint = _canonical_hash(
            {
                "schema_version": PREVIEW_SCHEMA_VERSION,
                "task_id": task.task_id,
                # The frozen version ID makes a V2 preview stale when its
                # selected manifest is changed, even when a path is reused.
                "active_revision_id": getattr(task, "active_revision_id", None),
                "plan": plan_shots,
                "assets": assets,
                "watermark": PREVIEW_WATERMARK,
            }
        )
        output_dir = self._output_root / task.task_id / "review_previews"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{task.task_id}_{fingerprint[:16]}_review.mp4"
        manifest_path = output_path.with_suffix(".preview.json")

        timeline = build_timeline(
            plan_shots,
            {shot.slot_index: shot for shot in shots},
            {shot.slot_index: shot.image_url for shot in shots},
        )
        slots = [
            replace(slot, review_watermark_text=PREVIEW_WATERMARK)
            for slot in timeline
        ]
        expected_ms = sum(slot.planned_ms for slot in slots)
        ok, error = self._renderer.render(slots, output_path)
        if not ok:
            raise ReviewPreviewError(f"preview render failed: {error}")
        media_qc = dict(self._renderer.qc_video(output_path, expected_ms) or {})
        if not media_qc.get("passed"):
            raise ReviewPreviewError("preview media QC failed")

        output_sha256 = str(media_qc.get("sha256") or sha256_file(output_path))
        manifest = {
            "schema_version": PREVIEW_SCHEMA_VERSION,
            "preview_only": True,
            "publish_ready": False,
            "task_id": task.task_id,
            "active_revision_id": getattr(task, "active_revision_id", None),
            "input_fingerprint": fingerprint,
            "watermark": PREVIEW_WATERMARK,
            "assets": assets,
            "output_path": str(output_path.resolve()),
            "output_sha256": output_sha256,
            "duration_ms": media_qc.get("duration_ms"),
            "media_qc": media_qc,
        }
        manifest["manifest_sha256"] = _canonical_hash(manifest)
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return ReviewPreview(
            task_id=task.task_id,
            manifest_path=str(manifest_path),
            output_path=str(output_path),
            input_fingerprint=fingerprint,
            output_sha256=output_sha256,
            duration_ms=int(media_qc.get("duration_ms") or 0),
            media_qc=media_qc,
        )

    def _selected_task_and_shots(self, task: Any):
        """Resolve V2 selections without modifying revision or task state."""
        from services.workflow_v2 import (
            RevisionAssetResolver,
            WorkflowV2Error,
            frozen_task_view,
            workflow_v2_enabled,
        )

        if not workflow_v2_enabled(task):
            return task, self._latest_shots(task.task_id)
        try:
            frozen = frozen_task_view(self._repository, task)
            revision = self._repository.get_task_revision(task.active_revision_id)
            if revision is None:
                raise WorkflowV2Error("active revision is missing")
            by_id = {shot.shot_id: shot for shot in self._repository.list_shots(task.task_id)}
            selected = []
            for item in frozen.plan_json.get("shots") or []:
                slot = int(item["slot_index"])
                asset = RevisionAssetResolver.selected(revision, f"shot:{slot}")
                shot = by_id.get(asset.get("asset_id"))
                if shot is None or shot.image_sha256 != asset.get("sha256"):
                    raise WorkflowV2Error(f"selection for slot {slot} is stale")
                selected.append(replace(shot, image_url=str(asset["path"])))
            return frozen, selected
        except (WorkflowV2Error, KeyError, TypeError) as exc:
            raise ReviewPreviewError(f"preview selection is not ready: {exc}") from exc

    def _latest_shots(self, task_id: str) -> List[Any]:
        getter = getattr(self._repository, "list_latest_shots", None)
        if callable(getter):
            return list(getter(task_id))
        rows = list(self._repository.list_shots(task_id))
        latest: Dict[int, Any] = {}
        for shot in rows:
            current = latest.get(int(shot.slot_index))
            if current is None or (shot.shot_version, shot.is_selected) > (
                current.shot_version,
                current.is_selected,
            ):
                latest[int(shot.slot_index)] = shot
        return [latest[index] for index in sorted(latest)]

    @staticmethod
    def _validate_shots(plan_shots: Sequence[Dict[str, Any]], shots: Sequence[Any]) -> None:
        expected = {int(item["slot_index"]) for item in plan_shots}
        by_slot = {int(shot.slot_index): shot for shot in shots}
        if set(by_slot) != expected:
            raise ReviewPreviewError(
                f"preview requires exactly the planned slots; expected {sorted(expected)}, got {sorted(by_slot)}"
            )
        for slot in sorted(expected):
            shot = by_slot[slot]
            path = Path(str(shot.image_url or ""))
            if not path.is_file():
                raise ReviewPreviewError(f"slot {slot} image is not a readable local file")
            expected_sha = str(shot.image_sha256 or "")
            if not expected_sha or sha256_file(path) != expected_sha:
                raise ReviewPreviewError(f"slot {slot} image SHA256 does not match selected evidence")
            if int(shot.image_width or 0) <= 0 or int(shot.image_height or 0) <= 0:
                raise ReviewPreviewError(f"slot {slot} image is missing technical dimensions")
