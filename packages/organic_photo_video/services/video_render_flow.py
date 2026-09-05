"""Video render flow (Stage D release + Stage E rendering).

- ``approve_group`` records either a human decision or an operator-configured
  automatic technical release, approves all shot rows and moves the task
  image_review -> rendering.
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
    # Stage D: group release (human or operator-configured automatic mode)
    # ------------------------------------------------------------------

    def approve_group(
        self,
        task_id: str,
        reviewer: str,
        notes: str = "",
        *,
        approval_mode: str = "human",
    ):
        task = self._require_task(task_id)
        if task.task_status != TASK_IMAGE_REVIEW:
            raise VideoRenderFlowError(
                f"task {task_id} status {task.task_status!r} is not image_review"
            )
        if not reviewer or not reviewer.strip():
            raise VideoRenderFlowError("reviewer is required for group approval")
        if approval_mode not in {"human", "operator_auto", "technical"}:
            raise VideoRenderFlowError(
                f"unsupported approval_mode: {approval_mode!r}"
            )
        shots = self._shots_for_task(task)
        from domain.contracts import expected_plan_shot_count
        count = expected_plan_shot_count(task.plan_json or {})
        if len(shots) != count or {s.slot_index for s in shots} != set(range(1, count + 1)):
            raise VideoRenderFlowError(
                f"group approval requires {count} frozen planned shots, got {len(shots)}"
            )
        for shot in shots:
            allowed = {SHOT_GENERATED, SHOT_APPROVED} if self._workflow_v2(task) else {SHOT_GENERATED}
            if shot.shot_status not in allowed or (not self._workflow_v2(task) and shot.qa_status != QC_PASSED):
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
        group_review = self._require_v2_content_gate(task, shots) if self._workflow_v2(task) else None
        if not self._workflow_v2(task) and visual_qa_required and visual_stage.get("decision") != "passed":
            raise VideoRenderFlowError(
                "visual QA is required by the account; run VisualQaService.review_task first"
            )
        plan = task.plan_json or {}
        is_auto = approval_mode in {"operator_auto", "technical"}
        stage_d = {
            "decision": (
                "auto_released_for_render" if is_auto else "group_approved"
            ),
            "approval_mode": approval_mode,
            "reviewer": reviewer,
            "approved_at": utc_now().isoformat(timespec="seconds"),
            "notes": notes,
            "visual_model_qa": visual_stage.get("decision", "not_required"),
        }
        if approval_mode == "technical":
            stage_d.update({"production_policy": "technical_only", "visual_model_qa": "not_performed",
                            "decision": "technical_ready_for_render"})
        merged_group_qa = {**(task.group_qa_json or {}), "stage_d": stage_d}
        feedback = LookFeedback(
            feedback_id=generate_prefixed_id("opv_fb"),
            task_id=task_id,
            account_id=task.account_id,
            product_id=task.product_id,
            look_ref_id=(plan.get("look") or {}).get("ref_id"),
            feedback_type=(
                "automatic_technical_gate" if is_auto else "human_review"
            ),
            decision="approve",
            reason_codes_json=[
                "operator_owned_source_auto_render"
                if is_auto else "stage_d_group_approval"
            ],
            score_json={},
            evidence_json={
                "approved_slots": [s.slot_index for s in shots],
                "shot_versions": {
                    str(s.slot_index): s.shot_version for s in shots
                },
            },
            reviewer=reviewer,
            notes=notes or (
                "operator-owned source passed technical media checks; "
                "released for rendering"
                if is_auto else "group approved for rendering"
            ),
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
            approval_guard = {}
            if group_review is not None:
                revision = self._repository.get_task_revision(task.active_revision_id)
                approval_guard = {"expected_revision_id": task.active_revision_id,
                                  "expected_selection_hash": revision.selection_hash,
                                  "group_review_id": group_review.review_id}
            atomic(
                task_id=task_id,
                shots=shots,
                feedback=feedback,
                group_qa_json=merged_group_qa,
                package_fields=package_fields,
                **approval_guard,
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
            if shot.shot_status != SHOT_APPROVED:
                self._repository.update_shot_status(shot.shot_id, SHOT_GENERATED, SHOT_APPROVED)
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

    def render(
        self, task_id: str, *, overlay_profile_id: Optional[str] = None
    ) -> VideoRender:
        task = self._require_task(task_id)
        if task.task_status != TASK_RENDERING:
            raise VideoRenderFlowError(
                f"task {task_id} status {task.task_status!r} is not rendering; "
                "run approve_group first"
            )
        plan = task.plan_json or {}
        if overlay_profile_id:
            from services.text_overlay import apply_overlay_profile

            plan = apply_overlay_profile(
                plan,
                recipe_id=str(task.recipe_id or ""),
                locale=task.target_locale,
                profile_id=overlay_profile_id,
            )
        shots = self._shots_for_task(task)
        if len(shots) != len(plan.get("shots", [])):
            raise VideoRenderFlowError(
                f"task {task_id} plan has {len(plan.get('shots', []))} slots "
                f"but {len(shots)} shot rows exist"
            )
        if self._workflow_v2(task):
            self._require_v2_content_gate(task, shots)
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

        render_row = self._ensure_render_row(
            task, preset.render_preset_id, shots, plan=plan
        )
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
            if self._workflow_v2(task):
                self._repository.transition_task(task_id, TASK_RENDERING, TASK_VIDEO_REVIEW)
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
                "overlay_profile_id": overlay_profile_id,
            },
            qc_status=QC_PASSED if qc.get("passed") else QC_FAILED,
            qc_json=qc,
            # V2 requires a separate review of the actual rendered motion and
            # typography.  Technical ffmpeg QC alone may still publish V1.
            publish_ready=bool(qc.get("passed")) and not self._workflow_v2(task),
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
        self,
        task,
        preset_id: str,
        shots,
        *,
        plan: Optional[Dict[str, Any]] = None,
    ) -> VideoRender:
        existing = self._repository.list_renders(task.task_id)
        if self._workflow_v2(task) and existing:
            current = [row for row in existing if row.origin_revision_id == task.active_revision_id]
            if not current:
                row = self._new_render_row(
                    task, preset_id, shots,
                    version=max(r.render_version for r in existing) + 1, plan=plan,
                )
                self._repository.insert_render(row)
                return self._repository.get_render(row.render_id) or row
            existing = current
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
                    task,
                    preset_id,
                    shots,
                    version=latest.render_version + 1,
                    plan=plan,
                )
                self._repository.insert_render(row)
                return self._repository.get_render(row.render_id) or row
            return latest  # queued/rendering: resume in place
        row = self._new_render_row(
            task, preset_id, shots, version=1, plan=plan
        )
        self._repository.insert_render(row)
        return self._repository.get_render(row.render_id) or row

    def _new_render_row(
        self,
        task,
        preset_id: str,
        shots,
        *,
        version: int,
        plan: Optional[Dict[str, Any]] = None,
    ) -> VideoRender:
        plan = plan or task.plan_json or {}
        origin_revision_id = None
        input_fingerprint = ""
        if self._workflow_v2(task):
            from services.workflow_v2 import canonical_hash
            origin_revision_id = str(getattr(task, "active_revision_id", "") or "")
            input_fingerprint = canonical_hash({
                "revision": origin_revision_id,
                "selection": [
                    {"slot": s.slot_index, "shot": s.shot_id, "sha256": s.image_sha256}
                    for s in shots
                ],
                "copy": task.copy_json or {},
                "render_contract": (plan or {}).get("render_contract") or {},
            })
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
            origin_revision_id=origin_revision_id,
            input_fingerprint=input_fingerprint,
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
                "fit_mode": s.fit_mode,
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
                fit_mode=s.fit_mode,
                start_ms=s.start_ms,
                overlay_text=s.overlay_text,
                overlay_spec=s.overlay_spec,
            )
            for s in timeline
        ]

    def _require_task(self, task_id: str):
        task = self._repository.get_task(task_id)
        if task is None:
            raise VideoRenderFlowError(f"task {task_id} not found")
        from services.workflow_v2 import frozen_task_view
        return frozen_task_view(self._repository, task)

    def _require_v2_content_gate(self, task, shots):
        from services.media_qc import MediaQcError, require_shot_media_qc
        from services.workflow_v2 import ScopedReviewService, WorkflowV2Error
        from domain.contracts import expected_plan_shot_count
        count = expected_plan_shot_count(task.plan_json or {})
        if len(shots) != count or {shot.slot_index for shot in shots} != set(range(1, count + 1)):
            raise VideoRenderFlowError(f"V2 formal rendering requires all {count} selected technical-ready shots")
        try:
            for shot in shots:
                require_shot_media_qc(shot)
            return ScopedReviewService(self._repository).require_passed(task, scope="group", target_id="group")
        except (MediaQcError, WorkflowV2Error, OSError) as exc:
            raise VideoRenderFlowError(str(exc)) from exc

    def _latest_shots(self, task_id: str) -> List[Any]:
        from services.hero_first import HeroFirstProducer

        return HeroFirstProducer._latest_per_slot(
            self._repository.list_shots(task_id)
        )

    def _shots_for_task(self, task) -> List[Any]:
        from services.workflow_v2 import RevisionAssetResolver, workflow_v2_enabled

        if not workflow_v2_enabled(task):
            return self._latest_shots(task.task_id)
        revision_id = str(getattr(task, "active_revision_id", "") or "")
        revision = self._repository.get_task_revision(revision_id)
        if revision is None:
            raise VideoRenderFlowError("Workflow V2 task has no active revision")
        by_id = {shot.shot_id: shot for shot in self._repository.list_shots(task.task_id)}
        output = []
        for index in range(1, len((task.plan_json or {}).get("shots") or []) + 1):
            try:
                asset = RevisionAssetResolver.selected(revision, f"shot:{index}")
            except Exception as exc:  # selection is a fail-closed release contract
                raise VideoRenderFlowError(f"Workflow V2 selection for slot {index} is missing") from exc
            shot = by_id.get(asset.get("asset_id"))
            if shot is None or shot.image_sha256 != asset.get("sha256"):
                raise VideoRenderFlowError(f"Workflow V2 selection for slot {index} is stale")
            output.append(shot)
        return output

    @staticmethod
    def _workflow_v2(task) -> bool:
        from services.workflow_v2 import workflow_v2_enabled
        return workflow_v2_enabled(task)
