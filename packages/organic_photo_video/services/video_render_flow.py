"""Video render flow (Stage D approval + Stage E rendering).

- ``approve_group`` records the Stage D human decision (V1 hard rule: machine
  never bypasses human confirmation), approves all shot rows and moves the
  task image_review -> rendering.
- ``render`` creates/reuses an opv_video_render row, runs the FFmpeg still
  renderer, runs media QC, and moves the task rendering -> video_review.
  A failed render row is requeued in place (render_status failed -> queued),
  matching the recovery rule "video failures reuse the approved images".
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from domain.models import (
    LookFeedback,
    VideoRender,
    generate_prefixed_id,
    utc_now,
)
from domain import statuses as _statuses
from domain.statuses import (
    QC_FAILED,
    QC_PASSED,
    RENDER_COMPLETED,
    RENDER_FAILED,
    RENDER_QUEUED,
    RENDER_RENDERING,
    SHOT_APPROVED,
    SHOT_GENERATED,
    TASK_IMAGE_REVIEW,
    TASK_RENDERING,
    TASK_VIDEO_REVIEW,
)

OUTPUT_SUBDIR = "renders"


class VideoRenderFlowError(RuntimeError):
    pass


class VideoRenderFlow:
    def __init__(self, repository, renderer, output_root: Optional[Path] = None):
        self._repository = repository
        self._renderer = renderer
        self._output_root = (
            Path(output_root)
            if output_root
            else Path.home() / ".openclaw" / "shared" / "data" / "organic_photo_video"
        )

    # ------------------------------------------------------------------
    # Stage D: human group approval
    # ------------------------------------------------------------------

    def approve_group(self, task_id: str, reviewer: str, notes: str = ""):
        task = self._require_task(task_id)
        if task.task_status != TASK_IMAGE_REVIEW:
            raise VideoRenderFlowError(
                f"task {task_id} status {task.task_status!r} is not image_review"
            )
        if not reviewer or not reviewer.strip():
            raise VideoRenderFlowError("reviewer is required for group approval")
        shots = self._latest_shots(task_id)
        if len(shots) != 5:
            raise VideoRenderFlowError(
                f"group approval requires five latest shots, got {len(shots)}"
            )
        for shot in shots:
            if shot.shot_status != SHOT_GENERATED or shot.qa_status != QC_PASSED:
                raise VideoRenderFlowError(
                    f"slot {shot.slot_index} is {shot.shot_status}/"
                    f"{shot.qa_status}; all slots must be generated+passed"
                )
        account = self._repository.get_account_profile(task.account_id)
        visual_qa_required = bool(
            account
            and (account.operating_rules_json or {}).get("visual_qa_required", False)
        )
        visual_stage = ((task.group_qa_json or {}).get("stage_c_visual") or {})
        if visual_qa_required and visual_stage.get("decision") != "passed":
            raise VideoRenderFlowError(
                "visual QA is required by the account; run VisualQaService.review_task first"
            )
        plan = task.plan_json or {}
        stage_d = {
            "decision": "group_approved",
            "reviewer": reviewer,
            "approved_at": utc_now().isoformat(timespec="seconds"),
            "notes": notes,
            "visual_model_qa": visual_stage.get("decision", "not_required"),
        }
        merged_group_qa = {**(task.group_qa_json or {}), "stage_d": stage_d}
        feedback = LookFeedback(
            feedback_id=generate_prefixed_id("opv_fb"),
            task_id=task_id,
            account_id=task.account_id,
            product_id=task.product_id,
            look_ref_id=(plan.get("look") or {}).get("ref_id"),
            feedback_type="human_review",
            decision="approve",
            reason_codes_json=["stage_d_group_approval"],
            score_json={},
            evidence_json={
                "approved_slots": [s.slot_index for s in shots],
                "shot_versions": {
                    str(s.slot_index): s.shot_version for s in shots
                },
            },
            reviewer=reviewer,
            notes=notes or "group approved for rendering",
        )
        cover = next((s for s in shots if s.slot_index == 1), None)
        package_fields = {
            "cover_image_id": cover.shot_id if cover else None,
            "cover_title": (task.copy_json or {}).get("title"),
            "hashtags_json": list((task.copy_json or {}).get("hashtags") or []),
            "caption": (task.copy_json or {}).get("caption"),
            "selected_image_ids_json": [
                s.shot_id for s in sorted(shots, key=lambda value: value.slot_index)
            ],
            "qa_summary_json": merged_group_qa,
        }
        atomic = getattr(self._repository, "commit_group_approval", None)
        if callable(atomic):
            atomic(
                task_id=task_id,
                shots=shots,
                feedback=feedback,
                group_qa_json=merged_group_qa,
                package_fields=package_fields,
            )
            return self._require_task(task_id)
        for shot in shots:
            selector = getattr(self._repository, "select_shot_version", None)
            if selector is not None:
                selector(task_id, shot.slot_index, shot.shot_id)
            else:
                setter = getattr(self._repository, "set_shot_selected", None)
                if setter is not None:
                    setter(shot.shot_id, True)
                else:
                    shot.is_selected = True
        for shot in shots:
            self._repository.update_shot_status(
                shot.shot_id, SHOT_GENERATED, SHOT_APPROVED
            )
        self._repository.insert_look_feedback(feedback)
        self._repository.update_task_plan(
            task_id,
            group_qa_json=merged_group_qa,
        )
        from services.content_package import advance_package_for_task

        advance_package_for_task(
            self._repository,
            task_id,
            _statuses.PACKAGE_READY,
            **package_fields,
        )
        self._repository.transition_task(
            task_id, TASK_IMAGE_REVIEW, TASK_RENDERING
        )
        return self._require_task(task_id)

    # ------------------------------------------------------------------
    # Stage E: render
    # ------------------------------------------------------------------

    def render(self, task_id: str) -> VideoRender:
        task = self._require_task(task_id)
        if task.task_status != TASK_RENDERING:
            raise VideoRenderFlowError(
                f"task {task_id} status {task.task_status!r} is not rendering; "
                "run approve_group first"
            )
        plan = task.plan_json or {}
        shots = self._latest_shots(task_id)
        if len(shots) != len(plan.get("shots", [])):
            raise VideoRenderFlowError(
                f"task {task_id} plan has {len(plan.get('shots', []))} slots "
                f"but {len(shots)} shot rows exist"
            )
        account = self._repository.get_account_profile(task.account_id)
        render_contract = plan.get("render_contract") or {}
        preset = self._repository.get_render_preset(
            str(
                render_contract.get("preset_id")
                or (account.default_render_preset_id if account else None)
                or ""
            )
        )
        if preset is None:
            raise VideoRenderFlowError("render preset not resolvable for task")

        render_row = self._ensure_render_row(task, preset.render_preset_id, shots)
        if render_row.render_status == RENDER_QUEUED:
            self._repository.update_render_status(
                render_row.render_id, RENDER_QUEUED, RENDER_RENDERING
            )

        slots = self._build_slots(plan, shots)
        expected_ms = sum(s.planned_ms for s in slots)
        output_path = (
            self._output_root / task_id / OUTPUT_SUBDIR
            / f"{task_id}_v{render_row.render_version}.mp4"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)

        from services.video_renderer import build_timeline

        timeline = build_timeline(
            plan["shots"],
            {s.slot_index: s for s in shots},
            {s.slot_index: s.image_url for s in shots},
        )
        ok, error = self._renderer.render(slots, output_path)
        if not ok:
            self._repository.update_render_output(
                render_row.render_id,
                duration_ms=None,
                output_oss_object_id=None,
                output_url=None,
                output_sha256=None,
                output_metadata_json={"error": error},
                qc_status=QC_FAILED,
                qc_json={"error": error},
                publish_ready=False,
            )
            self._repository.update_render_status(
                render_row.render_id, RENDER_RENDERING, RENDER_FAILED
            )
            refreshed = self._repository.get_render(render_row.render_id)
            return refreshed or render_row

        qc = self._renderer.qc_video(output_path, expected_ms)
        self._repository.update_render_output(
            render_row.render_id,
            duration_ms=qc.get("duration_ms"),
            output_oss_object_id=None,  # OSS upload lands with the publish phase
            output_url=str(output_path),
            output_sha256=qc.get("sha256"),
            output_metadata_json={
                "width": qc.get("width"),
                "height": qc.get("height"),
                "fps": int(render_contract.get("fps") or preset.fps or 30),
                "codec": qc.get("codec"),
                "audio": "none_pending_neobund_test",
                "timeline_ms": expected_ms,
            },
            qc_status=QC_PASSED if qc.get("passed") else QC_FAILED,
            qc_json=qc,
            publish_ready=bool(qc.get("passed")),
        )
        self._repository.update_render_status(
            render_row.render_id, RENDER_RENDERING, RENDER_COMPLETED
        )
        if bool(qc.get("passed")):
            self._repository.update_task_plan(
                task_id, selected_render_id=render_row.render_id
            )
            from services.content_package import advance_package_for_task

            finder = getattr(self._repository, "get_content_package_by_task", None)
            package = finder(task_id) if callable(finder) else None
            render_ids = list(
                (package.render_ids_json if package else []) or []
            )
            if render_row.render_id not in render_ids:
                render_ids.append(render_row.render_id)
            advance_package_for_task(
                self._repository,
                task_id,
                _statuses.PACKAGE_RENDERED,
                render_ids_json=render_ids,
            )
        self._repository.transition_task(task_id, TASK_RENDERING, TASK_VIDEO_REVIEW)
        refreshed = self._repository.get_render(render_row.render_id)
        return refreshed or render_row

    # ------------------------------------------------------------------

    def _ensure_render_row(
        self, task, preset_id: str, shots
    ) -> VideoRender:
        existing = self._repository.list_renders(task.task_id)
        if existing:
            latest = existing[0]  # list_renders orders by version DESC
            if latest.render_status == RENDER_FAILED:
                self._repository.update_render_status(
                    latest.render_id, RENDER_FAILED, RENDER_QUEUED
                )
                latest.render_status = RENDER_QUEUED
                return latest
            if latest.render_status == RENDER_COMPLETED:
                # Operator re-edit (e.g. pacing change): a completed render is
                # immutable history, so the next edit becomes a new version.
                row = self._new_render_row(
                    task, preset_id, shots, version=latest.render_version + 1
                )
                self._repository.insert_render(row)
                return self._repository.get_render(row.render_id) or row
            return latest  # queued/rendering: resume in place
        row = self._new_render_row(task, preset_id, shots, version=1)
        self._repository.insert_render(row)
        return self._repository.get_render(row.render_id) or row

    def _new_render_row(self, task, preset_id: str, shots, *, version: int) -> VideoRender:
        plan = task.plan_json or {}
        return VideoRender(
            render_id=generate_prefixed_id("opv_render"),
            task_id=task.task_id,
            render_version=version,
            render_preset_id=preset_id,
            copy_snapshot_json=task.copy_json or {},
            shot_selection_json=[
                {
                    "slot_index": s.slot_index,
                    "shot_id": s.shot_id,
                    "shot_version": s.shot_version,
                    "image_sha256": s.image_sha256,
                }
                for s in shots
            ],
            timeline_json=self._timeline_dicts(plan, shots),
        )

    def _timeline_dicts(self, plan: Dict[str, Any], shots) -> List[Dict[str, Any]]:
        from services.video_renderer import build_timeline

        timeline = build_timeline(
            plan.get("shots", []),
            {s.slot_index: s for s in shots},
            {s.slot_index: s.image_url for s in shots},
        )
        return [
            {
                "slot_index": s.slot_index,
                "shot_id": s.shot_id,
                "image": s.image_path,
                "start_ms": s.start_ms,
                "planned_ms": s.planned_ms,
                "effective_ms": s.effective_ms,
                "motion_preset": s.motion_preset,
                "transition_out": s.transition_out,
            }
            for s in timeline
        ]

    def _build_slots(self, plan: Dict[str, Any], shots):
        from services.video_renderer import TimelineSlot, build_timeline

        timeline = build_timeline(
            plan.get("shots", []),
            {s.slot_index: s for s in shots},
            {s.slot_index: s.image_url for s in shots},
        )
        return [
            TimelineSlot(
                slot_index=s.slot_index,
                shot_id=s.shot_id,
                shot_version=s.shot_version,
                image_path=s.image_path,
                motion_preset=s.motion_preset,
                transition_out=s.transition_out,
                planned_ms=s.planned_ms,
                effective_ms=s.effective_ms,
                start_ms=s.start_ms,
            )
            for s in timeline
        ]

    def _require_task(self, task_id: str):
        task = self._repository.get_task(task_id)
        if task is None:
            raise VideoRenderFlowError(f"task {task_id} not found")
        return task

    def _latest_shots(self, task_id: str) -> List[Any]:
        from services.hero_first import HeroFirstProducer

        return HeroFirstProducer._latest_per_slot(
            self._repository.list_shots(task_id)
        )
