"""Production continuation using media checks only; never calls a vision reviewer.

The existing review states remain durable checkpoints for old tasks. Technical
records describe file/input checks, not a claim that the content looks good.
Publishing is deliberately outside this service.
"""
from __future__ import annotations

from domain.contracts import ContractViolationError, expected_plan_shot_count

from services.media_qc import require_shot_media_qc
from services.release_gate import TECHNICAL_REVIEWER, freeze_release, expected_render_fingerprint, file_hash
from services.workflow_v2 import (
    AnchorReviewService, RenderReviewService, RevisionAssetResolver,
    ScopedReviewService, WorkflowV2Error, canonical_hash, workflow_v2_enabled,
)


class TechnicalCheckService:
    def __init__(self, repository):
        self.repository = repository

    def revision(self, task):
        revision = self.repository.get_task_revision(task.active_revision_id)
        if (revision is None or revision.task_id != task.task_id
                or canonical_hash(revision.plan_snapshot_json) != revision.input_snapshot_hash):
            raise WorkflowV2Error("frozen revision inputs changed or are missing")
        selected = revision.asset_manifest_json.get("selected") or {}
        # The original empty-manifest bootstrap hash is retained for compatibility.
        if selected and canonical_hash(selected) != revision.selection_hash:
            raise WorkflowV2Error("frozen selection hash changed")
        return revision

    @staticmethod
    def evidence(scope, revision, **extra):
        return {"schema_version": "opv-technical-check-v1", "production_policy": "technical_only",
                "scope": scope, "visual_review_performed": False,
                "input_snapshot_hash": revision.input_snapshot_hash,
                "selection_hash": revision.selection_hash,
                "checks": {"frozen_inputs": True, "file_hash": True, "media_qc": True,
                           "decoded_images": True, "render_input_binding": True}, **extra}

    def _shot(self, revision, asset):
        shot = next((s for s in self.repository.list_shots(revision.task_id)
                     if s.shot_id == asset["asset_id"]), None)
        if shot is None or shot.image_sha256 != asset["sha256"] or shot.image_url != asset["path"]:
            raise WorkflowV2Error("selected image row does not match frozen asset")
        require_shot_media_qc(shot, decode=True)
        return shot

    def anchor(self, task_id):
        task = self.repository.get_task(task_id)
        revision = self.revision(task)
        slot = int((revision.plan_snapshot_json.get("plan") or {}).get("anchor_slot") or 1)
        chosen = (revision.asset_manifest_json.get("selected") or {}).get("anchor")
        if not chosen:
            rows = [s for s in self.repository.list_shots(task_id)
                    if s.slot_index == slot and s.origin_revision_id == revision.revision_id
                    and s.shot_status in {"generated", "approved"}
                    and s.shot_id in (revision.asset_manifest_json.get("candidates") or {})]
            if len(rows) != 1:
                raise WorkflowV2Error("anchor candidate is missing or ambiguous; explicit rework is required")
            chosen = rows[0].shot_id
        asset = RevisionAssetResolver.candidate(revision, chosen)
        shot = self._shot(revision, asset)
        if shot.slot_index != slot:
            raise WorkflowV2Error("anchor selection points to the wrong slot")
        return AnchorReviewService(self.repository).record(
            task_id, asset_id=chosen, decision="passed", dimensions={},
            reviewer_type="technical", reviewer=TECHNICAL_REVIEWER,
            evidence=self.evidence("anchor", revision, asset_id=chosen, image_sha256=shot.image_sha256),
            expected_lock_version=revision.lock_version,
        )

    def group(self, task_id):
        task = self.repository.get_task(task_id)
        revision = self.revision(task)
        plan = revision.plan_snapshot_json.get("plan") or {}
        specs = plan.get("shots") or []
        try:
            count = expected_plan_shot_count(plan)
        except ContractViolationError as exc:
            raise WorkflowV2Error(str(exc)) from exc
        expected_slots = set(range(1, count + 1))
        if {int(s["slot_index"]) for s in specs} != expected_slots or len(specs) != count:
            raise WorkflowV2Error(f"technical group requires exactly {count} frozen planned slots")
        shots = [self._shot(revision, RevisionAssetResolver.selected(revision, f"shot:{int(s['slot_index'])}")) for s in specs]
        if {s.slot_index for s in shots} != expected_slots:
            raise WorkflowV2Error("selected shots do not match their slot keys")
        service = ScopedReviewService(self.repository)
        fingerprint = service.fingerprint(task, revision, scope="group")
        matches = [r for r in self.repository.list_quality_reviews(revision.revision_id, scope="group", target_id="group")
                   if r.input_fingerprint == fingerprint]
        from services.release_gate import trusted_review
        if matches and matches[-1].reviewer_type == "technical" and matches[-1].decision == "passed" and trusted_review(matches[-1]):
            return matches[-1]
        return service.record(task_id, scope="group", target_id="group", decision="passed", dimensions={},
            reviewer_type="technical", reviewer=TECHNICAL_REVIEWER,
            evidence=self.evidence("group", revision, image_hashes={s.shot_id: s.image_sha256 for s in shots}))

    def render(self, task_id):
        task = self.repository.get_task(task_id)
        revision = self.revision(task)
        render = self.repository.get_render(task.selected_render_id or "")
        if (render is None or render.render_status != "completed" or render.qc_status != "passed"
                or (render.qc_json or {}).get("passed") is not True
                or render.origin_revision_id != revision.revision_id
                or expected_render_fingerprint(revision) != render.input_fingerprint
                or not render.output_sha256 or file_hash(render.output_url) != render.output_sha256):
            raise WorkflowV2Error("render technical QC or frozen input/file binding is not valid")
        # Verify all exact source bytes once more before freezing the release.
        for spec in (revision.plan_snapshot_json.get("plan") or {}).get("shots") or []:
            self._shot(revision, RevisionAssetResolver.selected(revision, f"shot:{int(spec['slot_index'])}"))
        return RenderReviewService(self.repository).record(task_id, render_id=render.render_id,
            decision="passed", dimensions={}, reviewer_type="technical", reviewer=TECHNICAL_REVIEWER,
            evidence=self.evidence("render", revision, media_qc=dict(render.qc_json)))


class TechnicalProductionFlow:
    def __init__(self, repository, producer, renderer_flow):
        self.repository, self.producer, self.renderer_flow = repository, producer, renderer_flow
        self.checks = TechnicalCheckService(repository)

    def run(self, task_id, *, overlay_profile_id=None):
        for _ in range(8):
            task = self.repository.get_task(task_id)
            if task is None or not workflow_v2_enabled(task):
                raise WorkflowV2Error("technical production continuation requires a V2 task")
            if task.released_revision_id == task.active_revision_id and task.released_revision_id:
                freeze_release(self.repository, task, self.repository.get_render(task.selected_render_id or ""))
                return task
            rows = self.repository.list_shots(task_id)
            if any(s.shot_status == "generating" and s.origin_revision_id == task.active_revision_id for s in rows):
                raise WorkflowV2Error("generation outcome unknown; refusing duplicate submission")
            state = task.task_status
            if state == "rework_pending":
                revision = self.checks.revision(task)
                self.repository.transition_task(task_id, state, (revision.rework_spec_json or {}).get("resume_stage") or "image_generating")
            elif state in {"planned", "hero_generating", "image_generating", "failed"}:
                report = self.producer.produce(task_id)
                if report.task_status == "failed" or any(s.status == "failed" for s in report.slots):
                    raise WorkflowV2Error("generation incomplete; explicit execution retries only failed or unstarted slots")
            elif state == "anchor_review":
                self.checks.anchor(task_id)
            elif state == "image_review":
                revision = self.checks.revision(task)
                selected = revision.asset_manifest_json.get("selected") or {}
                if any(f"shot:{int(s['slot_index'])}" not in selected for s in revision.plan_snapshot_json["plan"]["shots"]):
                    # Complete images stay frozen. Only absent/failed slots are produced.
                    self.repository.transition_task(task_id, state, "image_generating")
                    continue
                self.checks.group(task_id)
                self.renderer_flow.approve_group(task_id, reviewer=TECHNICAL_REVIEWER,
                    approval_mode="technical", notes="Technical checks only; no visual or human approval.")
            elif state == "rendering":
                self.checks.group(task_id)
                renders = [r for r in self.repository.list_renders(task_id) if r.origin_revision_id == task.active_revision_id]
                if any(r.render_status == "rendering" for r in renders):
                    raise WorkflowV2Error("render outcome unknown; refusing duplicate render")
                completed = [r for r in renders if r.render_status == "completed" and r.qc_status == "passed"
                             and r.input_fingerprint == expected_render_fingerprint(self.checks.revision(task))]
                if completed:
                    if len(completed) != 1:
                        raise WorkflowV2Error("multiple completed outputs require an explicit selection")
                    self.repository.update_task_plan(task_id, selected_render_id=completed[0].render_id)
                    self.repository.transition_task(task_id, state, "video_review")
                else:
                    rendered = self.renderer_flow.render(task_id, overlay_profile_id=overlay_profile_id)
                    if rendered.qc_status != "passed":
                        raise WorkflowV2Error("render technical QC failed; explicit render rework is required")
            elif state == "video_review":
                self.checks.render(task_id)
            else:
                raise WorkflowV2Error(f"task state {state!r} cannot continue production")
        raise WorkflowV2Error("production continuation exceeded its bounded state transitions")
