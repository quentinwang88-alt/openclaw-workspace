"""Hero-first shot production orchestrator (Stage C).

Gate rules (MODEL_HANDOFF 3.3 / 5.3, extended for recipe anchors):

1. The recipe's anchor slot is generated first. Remaining slots start only
   after that anchor exists and passes media QC. Legacy plans default to P1.
2. Anchor failure moves the task to ``failed`` with
   ``hero_generation_failed`` and never touches the remaining slots; recovery
   re-enters via ``failed -> hero_generating``.
3. Every slot retries as a NEW ``shot_version`` row (rejection is terminal per
   version); failures in P2-P5 do not block the others and the task still
   reaches ``image_review`` where a human decides (Stage D gate).
"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from domain import statuses as _statuses_module
from domain.models import (
    ContentShot,
    ContentTask,
    generate_prefixed_id,
)
from domain.statuses import (
    SHOT_FAILED,
    SHOT_GENERATED,
    SHOT_GENERATING,
    SHOT_PLANNED,
    QC_FAILED,
    QC_PASSED,
    TASK_FAILED,
    TASK_ANCHOR_REVIEW,
    TASK_HERO_GENERATING,
    TASK_IMAGE_GENERATING,
    TASK_IMAGE_REVIEW,
    TASK_PLANNED,
)

EXPECTED_WIDTH = 1024
EXPECTED_HEIGHT = 1536


class HeroFirstError(RuntimeError):
    pass


@dataclass
class SlotReport:
    slot_index: int
    shot_version: int
    status: str
    image_path: Optional[str] = None
    error: str = ""


@dataclass
class ProduceReport:
    task_id: str
    task_status: str
    hero_ok: bool
    slots: List[SlotReport] = field(default_factory=list)


class HeroFirstProducer:
    HERO_SLOT = 1

    def __init__(
        self,
        repository,
        generator,
        output_root: Optional[Path] = None,
        decomposer=None,
        asset_readiness_gate=None,
        technical_only=False,
    ):
        self._repository = repository
        # Existing callers keep injecting the approved photo generator.  The
        # router adds deterministic board shots without changing that public
        # constructor or the model-auth path.
        from services.shot_producer_router import ShotProducerRouter

        self._generator = (
            generator
            if isinstance(generator, ShotProducerRouter)
            else ShotProducerRouter(generator)
        )
        self._output_root = (
            Path(output_root)
            if output_root
            else Path.home() / ".openclaw" / "shared" / "data" / "organic_photo_video"
        )
        self._decomposer = decomposer
        # Low-level diagnostic callers may still pause at anchor_review;
        # the production entrypoints explicitly use technical-only continuation.
        self._technical_only = technical_only
        if asset_readiness_gate is None:
            from services.asset_readiness import require_generation_assets
            asset_readiness_gate = require_generation_assets
        self._asset_readiness_gate = asset_readiness_gate

    # ------------------------------------------------------------------
    # Shot rows
    # ------------------------------------------------------------------

    def ensure_shots(self, task_id: str) -> List[ContentShot]:
        """Create planned shot rows from the plan if absent; return latest per slot."""
        task = self._require_task(task_id)
        if not task.plan_json:
            raise HeroFirstError(f"task {task_id} has no plan; run the planner first")
        existing = self._repository.list_shots(task_id)
        if self._workflow_v2(task) and task.active_revision_id:
            return self._working_revision_shots(self._ensure_v2_revision(task), existing)
        if existing:
            return self._latest_per_slot(existing)
        origin_revision_id = (
            str(getattr(task, "active_revision_id", "") or "")
            if self._workflow_v2(task) else None
        )
        for plan_shot in task.plan_json["shots"]:
            self._repository.insert_shot(
                ContentShot(
                    shot_id=generate_prefixed_id("opv_shot"),
                    task_id=task_id,
                    slot_index=int(plan_shot["slot_index"]),
                    slot_role=plan_shot["slot_role"],
                    duration_ms=(
                        int(plan_shot["duration_ms"])
                        if plan_shot.get("duration_ms") is not None else None
                    ),
                    shot_status=SHOT_PLANNED,
                    narrative_purpose=plan_shot.get("purpose"),
                    motion_preset=plan_shot.get("motion_preset", "slow_push"),
                    transition_out=plan_shot.get("transition_out", "short_dissolve"),
                    overlay_text=plan_shot.get("overlay_text") or None,
                    generation_prompt=plan_shot.get("generation_prompt"),
                    source_refs_json=list(plan_shot.get("source_refs", [])),
                    narrative_function=plan_shot.get("narrative_function"),
                    product_focus=plan_shot.get("product_focus"),
                    overlay_spec_json=plan_shot.get("overlay_spec"),
                    transition_hint=plan_shot.get("transition_hint"),
                    continuity_constraints_json=list(
                        plan_shot.get("continuity_constraints", [])
                    ),
                    outfit_state_ref=plan_shot.get("outfit_state_ref"),
                    origin_revision_id=origin_revision_id,
                )
            )
        return self._latest_per_slot(self._repository.list_shots(task_id))

    def _working_revision_shots(self, task, existing):
        """Reuse explicit selections, allocate only missing slots in this revision."""
        from services.workflow_v2 import RevisionAssetResolver
        revision = self._repository.get_task_revision(task.active_revision_id)
        selected = revision.asset_manifest_json.get("selected") or {}
        by_id = {shot.shot_id: shot for shot in existing}
        result = []
        for spec in task.plan_json["shots"]:
            slot = int(spec["slot_index"])
            if f"shot:{slot}" in selected:
                asset = RevisionAssetResolver.selected(revision, f"shot:{slot}")
                row = by_id.get(asset["asset_id"])
                if row is None or row.image_sha256 != asset["sha256"]:
                    raise HeroFirstError(f"frozen selection P{slot} is stale")
                result.append(row)
                continue
            current = [s for s in existing if s.slot_index == slot and s.origin_revision_id == task.active_revision_id]
            if self._multi_look(task) and selected.get("anchor"):
                # The LOOK_01 photo is an immutable source, not the final P1 board.
                current = [s for s in current if s.shot_id != selected["anchor"]]
            if current:
                result.append(max(current, key=lambda s: s.shot_version))
                continue
            version = max((s.shot_version for s in existing if s.slot_index == slot), default=0) + 1
            row = ContentShot(
                shot_id=generate_prefixed_id("opv_shot"), task_id=task.task_id,
                slot_index=slot, slot_role=spec["slot_role"],
                duration_ms=(int(spec["duration_ms"])
                             if spec.get("duration_ms") is not None else None),
                shot_version=version,
                origin_revision_id=task.active_revision_id,
                narrative_purpose=spec.get("purpose"),
                motion_preset=spec.get("motion_preset", "slow_push"),
                transition_out=spec.get("transition_out", "cut"),
                overlay_text=spec.get("overlay_text") or None,
                generation_prompt=spec.get("generation_prompt"),
                source_refs_json=list(spec.get("source_refs") or []),
                narrative_function=spec.get("narrative_function"), product_focus=spec.get("product_focus"),
                overlay_spec_json=spec.get("overlay_spec"), transition_hint=spec.get("transition_hint"),
                continuity_constraints_json=list(spec.get("continuity_constraints") or []),
                outfit_state_ref=spec.get("outfit_state_ref"),
            )
            self._repository.insert_shot(row)
            result.append(row)
        return result

    def regenerate_slot(self, task_id: str, slot_index: int) -> SlotReport:
        """Retry one slot as a new shot_version (single-slot recovery rule)."""
        latest = self._latest_per_slot(self._repository.list_shots(task_id))
        current = next((s for s in latest if s.slot_index == slot_index), None)
        if current is None:
            raise HeroFirstError(f"task {task_id} has no shot for slot {slot_index}")
        task = self._require_task(task_id)
        if self._multi_look(task) and slot_index == self._anchor_slot(task):
            raise HeroFirstError("multi-look P1 requires explicit board or anchor rework")
        new_shot = ContentShot(
            shot_id=generate_prefixed_id("opv_shot"),
            task_id=task_id,
            slot_index=current.slot_index,
            slot_role=current.slot_role,
            duration_ms=current.duration_ms,
            shot_version=current.shot_version + 1,
            shot_status=SHOT_PLANNED,
            narrative_purpose=current.narrative_purpose,
            motion_preset=current.motion_preset,
            transition_out=current.transition_out,
            overlay_text=current.overlay_text,
            generation_prompt=current.generation_prompt,
            source_refs_json=list(current.source_refs_json),
            narrative_function=current.narrative_function,
            product_focus=current.product_focus,
            overlay_spec_json=current.overlay_spec_json,
            transition_hint=current.transition_hint,
            continuity_constraints_json=list(
                current.continuity_constraints_json
            ),
            outfit_state_ref=current.outfit_state_ref,
        )
        if self._workflow_v2(task):
            task = self._ensure_v2_revision(task)
            if task.task_status not in {TASK_HERO_GENERATING, TASK_ANCHOR_REVIEW, TASK_IMAGE_GENERATING, TASK_IMAGE_REVIEW}:
                raise HeroFirstError("V2 re-generation requires an explicit review/rework stage")
            new_shot.origin_revision_id = task.active_revision_id
        self._repository.insert_shot(new_shot)
        continuity = self._hero_continuity_reference(task, slot_index)
        report = self._generate_slot(task, new_shot, continuity_reference_images=continuity)
        if self._workflow_v2(task) and slot_index == self._anchor_slot(task) and report.image_path:
            self._register_anchor_candidate(task, new_shot, report.image_path)
        elif self._workflow_v2(task) and report.image_path:
            self._register_slot_selection(task, new_shot, report.image_path)
        return report

    # ------------------------------------------------------------------
    # Main flow
    # ------------------------------------------------------------------

    def produce(self, task_id: str) -> ProduceReport:
        task = self._require_task(task_id)
        status = task.task_status
        v2 = self._workflow_v2(task)
        if v2:
            task = self._ensure_v2_revision(task)
            status = task.task_status
            if status == "rework_pending":
                revision = self._repository.get_task_revision(task.active_revision_id)
                destination = (revision.rework_spec_json or {}).get("resume_stage") or TASK_IMAGE_GENERATING
                self._repository.transition_task(task_id, status, destination)
                task = self._ensure_v2_revision(self._require_task(task_id))
                status = task.task_status
        if any(s.shot_status == SHOT_GENERATING and (not v2 or s.origin_revision_id == task.active_revision_id)
               for s in self._repository.list_shots(task_id)):
            raise HeroFirstError("generation outcome unknown; refusing duplicate submission")
        if v2 and self._reuse_only_photo(task):
            return self._produce_reused_photo(task)
        if v2 and status == TASK_ANCHOR_REVIEW:
            hero = next(
                (shot for shot in self._latest_per_slot(self._repository.list_shots(task_id))
                 if shot.slot_index == self._anchor_slot(task)),
                None,
            )
            return ProduceReport(
                task_id=task_id,
                task_status=TASK_ANCHOR_REVIEW,
                hero_ok=bool(hero and hero.shot_status == SHOT_GENERATED and hero.image_url),
                slots=[SlotReport(
                    slot_index=hero.slot_index, shot_version=hero.shot_version,
                    status=hero.shot_status, image_path=hero.image_url,
                )] if hero else [],
            )
        if status == TASK_FAILED:
            if task.failure_code != "hero_generation_failed":
                raise HeroFirstError(
                    f"task {task_id} failed for {task.failure_code!r}; "
                    "only hero failures auto-resume here"
                )
            self._repository.transition_task(
                task_id, TASK_FAILED, TASK_HERO_GENERATING
            )
        elif status == TASK_PLANNED:
            self._repository.transition_task(
                task_id, TASK_PLANNED, TASK_HERO_GENERATING
            )
        elif status not in (TASK_HERO_GENERATING, TASK_IMAGE_GENERATING):
            # hero_generating/image_generating re-entry = resume after a crash
            raise HeroFirstError(
                f"task {task_id} status {status!r} is not producible"
            )

        from services.content_package import advance_package_for_task

        advance_package_for_task(self._repository, task_id, _statuses_module.PACKAGE_GENERATING)
        shots = self.ensure_shots(task_id)
        # Anchor-frame-first: the recipe may pin a visual anchor other than
        # P1 (e.g. visual_transform recipes freeze the final look as P5).
        anchor_slot = int((task.plan_json or {}).get("anchor_slot") or self.HERO_SLOT)
        hero = next(
            (s for s in shots if s.slot_index == anchor_slot),
            shots[0],
        )
        if v2 and status == TASK_IMAGE_GENERATING:
            hero = self._selected_anchor_shot(task, shots)
        hero_done = (
            hero.shot_status in ({SHOT_GENERATED, "approved"} if v2 else {SHOT_GENERATED})
            and hero.qa_status == QC_PASSED
        )
        if v2 and self._technical_only and hero.shot_status in {SHOT_GENERATED, "approved"}:
            from services.media_qc import require_shot_media_qc
            require_shot_media_qc(hero, decode=True)
            hero_done = True
        if hero_done:
            hero_report = SlotReport(
                slot_index=hero.slot_index,
                shot_version=hero.shot_version,
                status=SHOT_GENERATED,
                image_path=hero.image_url,
            )
        else:
            if self._require_task(task_id).task_status == TASK_IMAGE_GENERATING:
                raise HeroFirstError(
                    f"task {task_id} is in image_generating but its hero shot "
                    "is missing; inspect manually instead of auto-regenerating"
                )
            hero_report = self._generate_slot(task, hero,
                plan_shot_override=self._anchor_photo_spec(task) if self._multi_look(task) else None)
        report = ProduceReport(
            task_id=task_id,
            task_status=TASK_HERO_GENERATING,
            hero_ok=hero_report.status == SHOT_GENERATED,
            slots=[hero_report],
        )
        if not report.hero_ok:
            self._repository.transition_task(
                task_id,
                TASK_HERO_GENERATING,
                TASK_FAILED,
                failure_code="hero_generation_failed",
                failure_detail=hero_report.error or "hero media QC failed",
                increment_retry=True,
            )
            report.task_status = TASK_FAILED
            return report

        if v2 and self._require_task(task_id).task_status == TASK_HERO_GENERATING:
            if not hero_report.image_path:
                raise HeroFirstError("V2 anchor generation returned no image path")
            self._register_anchor_candidate(task, hero, hero_report.image_path)
            self._repository.transition_task(
                task_id, TASK_HERO_GENERATING, TASK_ANCHOR_REVIEW
            )
            report.task_status = TASK_ANCHOR_REVIEW
            return report

        if self._require_task(task_id).task_status == TASK_HERO_GENERATING:
            self._repository.transition_task(
                task_id, TASK_HERO_GENERATING, TASK_IMAGE_GENERATING
            )
        for shot in sorted(shots, key=lambda s: s.slot_index):
            if shot.shot_id == hero.shot_id or (not self._multi_look(task) and shot.slot_index == hero.slot_index):
                continue
            reusable = shot.shot_status in ({SHOT_GENERATED, "approved"} if v2 else {SHOT_GENERATED}) and shot.qa_status == QC_PASSED
            if v2 and self._technical_only and shot.shot_status in {SHOT_GENERATED, "approved"}:
                from services.media_qc import require_shot_media_qc
                require_shot_media_qc(shot, decode=True)
                reusable = True
            if reusable:
                if v2:
                    self._register_slot_selection(task, shot, shot.image_url)
                continue  # resume: slot already produced
            report.slots.append(
                self._generate_slot(
                    task,
                    shot,
                    continuity_reference_images=[hero_report.image_path]
                    if hero_report.image_path else [],
                )
            )
            if v2 and report.slots[-1].image_path:
                self._register_slot_selection(task, shot, report.slots[-1].image_path)
        self._repository.transition_task(
            task_id, TASK_IMAGE_GENERATING, TASK_IMAGE_REVIEW
        )
        latest_shots = self.ensure_shots(task_id) if v2 else self._latest_per_slot(self._repository.list_shots(task_id))
        selected_ids = [
            shot.shot_id
            for shot in sorted(latest_shots, key=lambda value: value.slot_index)
            if shot.shot_status in {SHOT_GENERATED, "approved"} and shot.image_url
        ]
        advance_package_for_task(
            self._repository,
            task_id,
            _statuses_module.PACKAGE_QA_REVIEW,
            selected_image_ids_json=selected_ids,
        )
        refreshed = self._require_task(task_id)
        report.task_status = refreshed.task_status
        return report

    # ------------------------------------------------------------------

    def _generate_slot(
        self,
        task: ContentTask,
        shot: ContentShot,
        *,
        continuity_reference_images: Optional[List[str]] = None,
        plan_shot_override: Optional[Dict[str, Any]] = None,
    ) -> SlotReport:
        from services.workflow_v2 import frozen_task_view
        task = frozen_task_view(self._repository, task)
        account_reader = getattr(self._repository, "get_account_profile", None)
        account = account_reader(task.account_id) if callable(account_reader) else None
        self._asset_readiness_gate(account, (task.product_snapshot_json or {}).get("product") or {}, task.plan_json or {})
        if shot.shot_status == SHOT_PLANNED:
            self._repository.update_shot_status(shot.shot_id, SHOT_PLANNED, SHOT_GENERATING)
        elif shot.shot_status == SHOT_FAILED:
            self._repository.update_shot_status(shot.shot_id, SHOT_FAILED, SHOT_GENERATING)
        plan_shot = deepcopy(plan_shot_override) if plan_shot_override is not None else self._plan_shot_for(task, shot)
        board_spec = dict(plan_shot.get("board_spec") or {})
        from services.outfit_decomposer import decomposition_ready

        board_assets_ready = decomposition_ready(
            {"decomposition_assets": board_spec.get("decomposition_assets") or {}, "shots": [plan_shot]}
        )
        if (
            plan_shot.get("shot_kind") == "composite_board"
            and board_spec.get("decomposition_required")
            and not board_assets_ready
        ):
            from services.outfit_decomposer import (
                OutfitDecomposer,
                OutfitDecompositionError,
            )

            try:
                decomposition_root = self._output_root
                if self._workflow_v2(task):
                    decomposition_root = decomposition_root / "revisions" / task.active_revision_id
                decomposer = self._decomposer or OutfitDecomposer(self._repository, output_root=decomposition_root)
                anchor_paths = list(continuity_reference_images or [])
                task = decomposer.ensure(
                    task, anchor_path=anchor_paths[0] if anchor_paths else ""
                )
                if self._workflow_v2(task):
                    task = self._ensure_v2_revision(task)
                plan_shot = self._plan_shot_for(task, shot)
            except OutfitDecompositionError as exc:
                error = f"outfit decomposition failed: {exc}"
                self._repository.update_shot_status(
                    shot.shot_id, SHOT_GENERATING, SHOT_FAILED
                )
                self._repository.update_shot_qa(
                    shot.shot_id,
                    qa_status=QC_FAILED,
                    qa_json={"error": error},
                    failure_detail=error,
                )
                return SlotReport(
                    slot_index=shot.slot_index,
                    shot_version=shot.shot_version,
                    status=SHOT_FAILED,
                    error=error,
                )
        output_dir = (
            self._output_root / task.task_id / "shots"
        )
        from services.image_generator import ShotGenerationRequest

        request = ShotGenerationRequest(
            task_id=task.task_id,
            slot_index=shot.slot_index,
            slot_role=shot.slot_role,
            shot_version=shot.shot_version,
            plan_shot=plan_shot,
            product=task.product_snapshot_json.get("product", {}),
            persona_snapshot=self._persona_snapshot(task),
            look_snapshot=self._plan_section(task, "look"),
            scene_snapshot=self._plan_section(task, "scene"),
            output_dir=str(output_dir),
            continuity_reference_images=list(continuity_reference_images or []),
            product_facts=dict((task.plan_json or {}).get("product_facts") or {}),
            outfit_state=self._outfit_state(task, plan_shot),
            recipe_execution={
                **dict((task.plan_json or {}).get("recipe_execution") or {}),
                "locale": str(task.target_locale or ""),
            },
            camera_hint=str(plan_shot.get("camera_hint") or ""),
            reference_roles=self._reference_roles(task, shot),
        )
        outcome = self._generator.generate_shot(request)
        if not outcome.ok:
            self._repository.update_shot_status(
                shot.shot_id, SHOT_GENERATING, SHOT_FAILED
            )
            self._repository.update_shot_qa(
                shot.shot_id,
                qa_status=QC_FAILED,
                qa_json={"error": outcome.error},
                failure_detail=outcome.error,
            )
            return SlotReport(
                slot_index=shot.slot_index,
                shot_version=shot.shot_version,
                status=SHOT_FAILED,
                error=outcome.error,
            )
        self._repository.update_shot_generation_result(
            shot.shot_id,
            generation_provider=outcome.provider,
            generation_model=outcome.model,
            generation_request_id=outcome.request_id,
            image_oss_object_id=None,  # OSS upload lands with Phase 2 rendering
            image_url=outcome.image_path,
            image_sha256=self._sha256(outcome.image_path),
            image_width=outcome.width,
            image_height=outcome.height,
        )
        self._repository.update_shot_status(
            shot.shot_id, SHOT_GENERATING, SHOT_GENERATED
        )
        self._repository.update_shot_qa(
            shot.shot_id,
            qa_status=QC_PASSED,
            qa_json={
                "media_qc": {
                    "width": outcome.width,
                    "height": outcome.height,
                    "sha256": self._sha256(outcome.image_path),
                    "size_ok": True,
                }
            },
        )
        # In V2 an anchor is a candidate until its scoped review selects it.
        # Other slots retain the legacy selection projection for compatibility.
        if not self._workflow_v2(task):
            self._select_version(shot)
        return SlotReport(
            slot_index=shot.slot_index,
            shot_version=shot.shot_version,
            status=SHOT_GENERATED,
            image_path=outcome.image_path,
        )

    # ------------------------------------------------------------------

    def _require_task(self, task_id: str) -> ContentTask:
        task = self._repository.get_task(task_id)
        if task is None:
            raise HeroFirstError(f"task {task_id} not found")
        return task

    @staticmethod
    def _workflow_v2(task: ContentTask) -> bool:
        from services.workflow_v2 import workflow_v2_enabled
        return workflow_v2_enabled(task)

    @staticmethod
    def _anchor_slot(task: ContentTask) -> int:
        return int((task.plan_json or {}).get("anchor_slot") or HeroFirstProducer.HERO_SLOT)

    def _ensure_v2_revision(self, task: ContentTask) -> ContentTask:
        from services.workflow_v2 import RevisionService, frozen_task_view
        RevisionService(self._repository).ensure_working(task)
        return frozen_task_view(self._repository, self._require_task(task.task_id))

    def _register_anchor_candidate(
        self, task: ContentTask, shot: ContentShot, path: str
    ) -> None:
        from services.workflow_v2 import RevisionService
        revision = RevisionService(self._repository).ensure_working(task)
        if self._require_task(task.task_id).active_revision_id != task.active_revision_id:
            raise HeroFirstError("late anchor result belongs to an inactive revision")
        # A retry can resume after the provider succeeded but before manifest
        # persistence.  Candidate IDs are stable shot IDs, so duplicate work
        # becomes a no-op rather than changing the selected asset.
        existing = (revision.asset_manifest_json.get("candidates") or {}).get(shot.shot_id)
        if existing:
            return
        RevisionService(self._repository).register_candidate(
            revision,
            {
                "asset_id": shot.shot_id,
                "asset_type": "shot",
                "slot_index": shot.slot_index,
                "path": path,
                "sha256": self._sha256(path),
                "parent_asset_ids": [],
                # Prompt/config inputs are frozen in the revision snapshot;
                # this identifies the physical generated candidate itself.
                "parameters_hash": hashlib.sha256(
                    f"slot={shot.slot_index};version={shot.shot_version}".encode()
                ).hexdigest(),
            },
        )

    def _selected_anchor_shot(
        self, task: ContentTask, shots: List[ContentShot]
    ) -> ContentShot:
        from services.workflow_v2 import RevisionAssetResolver
        revision_id = str(getattr(task, "active_revision_id", "") or "")
        revision = self._repository.get_task_revision(revision_id)
        if revision is None:
            raise HeroFirstError("V2 task has no active revision")
        asset = RevisionAssetResolver.selected(revision, "anchor")
        expected_slot = self._anchor_slot(task)
        if int(asset.get("slot_index") or 0) != expected_slot:
            raise HeroFirstError("selected V2 anchor does not match recipe anchor slot")
        shot = next((item for item in self._repository.list_shots(task.task_id) if item.shot_id == asset["asset_id"]), None)
        if shot is None or shot.image_sha256 != asset.get("sha256"):
            raise HeroFirstError("selected V2 anchor no longer matches its frozen asset")
        return shot

    def _register_slot_selection(
        self, task: ContentTask, shot: ContentShot, path: str
    ) -> None:
        """Add a non-anchor shot to the exact V2 group selection manifest."""
        from services.workflow_v2 import RevisionService
        service = RevisionService(self._repository)
        revision = service.ensure_working(task)
        if self._require_task(task.task_id).active_revision_id != task.active_revision_id:
            raise HeroFirstError("late shot result belongs to an inactive revision")
        if (revision.asset_manifest_json.get("selected") or {}).get(f"shot:{shot.slot_index}") == shot.shot_id:
            return
        anchor_id = (revision.asset_manifest_json.get("selected") or {}).get("anchor")
        if shot.shot_id not in (revision.asset_manifest_json.get("candidates") or {}):
            revision = service.register_candidate(
                revision,
                {
                    "asset_id": shot.shot_id, "asset_type": "shot",
                    "slot_index": shot.slot_index, "path": path,
                    "sha256": self._sha256(path), "parent_asset_ids": [anchor_id] if anchor_id else [],
                    "parameters_hash": hashlib.sha256(
                        f"slot={shot.slot_index};version={shot.shot_version}".encode()
                    ).hexdigest(),
                },
            )
        service.select(revision, selection_key=f"shot:{shot.slot_index}", asset_id=shot.shot_id)
        self._select_version(shot)

    @staticmethod
    def _reuse_only_photo(task: ContentTask) -> bool:
        shots = list((task.plan_json or {}).get("shots") or [])
        return (
            str(getattr(task, "media_kind", "video") or "video") == "native_photo"
            and bool(shots)
            and all(item.get("shot_kind") == "reused_asset" for item in shots)
        )

    def _produce_reused_photo(self, task: ContentTask) -> ProduceReport:
        """Copy and freeze reusable sources without an artificial anchor gate."""
        task_id = task.task_id
        if task.task_status == TASK_PLANNED:
            self._repository.transition_task(task_id, TASK_PLANNED, TASK_IMAGE_GENERATING)
        elif task.task_status == TASK_IMAGE_REVIEW:
            shots = self.ensure_shots(task_id)
            return ProduceReport(
                task_id=task_id, task_status=TASK_IMAGE_REVIEW, hero_ok=True,
                slots=[SlotReport(
                    slot_index=item.slot_index, shot_version=item.shot_version,
                    status=item.shot_status, image_path=item.image_url,
                ) for item in shots],
            )
        elif task.task_status != TASK_IMAGE_GENERATING:
            raise HeroFirstError(
                f"reused photo task {task_id} status {task.task_status!r} is not producible"
            )
        from services.content_package import advance_package_for_task
        advance_package_for_task(
            self._repository, task_id, _statuses_module.PACKAGE_GENERATING
        )
        task = self._ensure_v2_revision(self._require_task(task_id))
        rows = self.ensure_shots(task_id)
        reports = []
        for shot in sorted(rows, key=lambda item: item.slot_index):
            if shot.shot_status in {SHOT_GENERATED, "approved"} and shot.image_url:
                self._register_slot_selection(task, shot, shot.image_url)
                reports.append(SlotReport(
                    slot_index=shot.slot_index, shot_version=shot.shot_version,
                    status=shot.shot_status, image_path=shot.image_url,
                ))
                continue
            report = self._generate_slot(task, shot)
            reports.append(report)
            if report.status != SHOT_GENERATED or not report.image_path:
                self._repository.transition_task(
                    task_id, TASK_IMAGE_GENERATING, TASK_FAILED,
                    failure_code="asset_reuse_failed", failure_detail=report.error,
                    increment_retry=True,
                )
                return ProduceReport(
                    task_id=task_id, task_status=TASK_FAILED,
                    hero_ok=False, slots=reports,
                )
            self._register_slot_selection(task, shot, report.image_path)
        self._repository.transition_task(
            task_id, TASK_IMAGE_GENERATING, TASK_IMAGE_REVIEW
        )
        advance_package_for_task(
            self._repository, task_id, _statuses_module.PACKAGE_QA_REVIEW,
            selected_image_ids_json=[item.shot_id for item in rows],
        )
        return ProduceReport(
            task_id=task_id, task_status=TASK_IMAGE_REVIEW,
            hero_ok=True, slots=reports,
        )

    def _select_version(self, shot: ContentShot) -> None:
        selector = getattr(self._repository, "select_shot_version", None)
        if selector is not None:
            selector(shot.task_id, shot.slot_index, shot.shot_id)
        else:  # compatibility with small in-memory adapters
            for candidate in self._repository.list_shots(shot.task_id):
                if candidate.slot_index == shot.slot_index and candidate.is_selected:
                    self._repository.set_shot_selected(candidate.shot_id, False)
            self._repository.set_shot_selected(shot.shot_id, True)

    def _plan_shot_for(self, task: ContentTask, shot: ContentShot) -> Dict[str, Any]:
        for plan_shot in (task.plan_json or {}).get("shots", []):
            if int(plan_shot["slot_index"]) == shot.slot_index:
                return plan_shot
        return {
            "slot_index": shot.slot_index,
            "slot_role": shot.slot_role,
            "purpose": shot.narrative_purpose or "",
            "overlay_text": shot.overlay_text or "",
        }

    @staticmethod
    def _multi_look(task: ContentTask) -> bool:
        return ((task.plan_json or {}).get("recipe_execution") or {}).get("content_goal") == "multi_look"

    def _anchor_photo_spec(self, task: ContentTask) -> Dict[str, Any]:
        """Generate LOOK_01 source pixels without changing the frozen board plan."""
        frozen = (task.plan_json or {}).get("anchor_photo_spec")
        if frozen:
            spec = deepcopy(frozen)
            board = next(s for s in task.plan_json["shots"] if int(s["slot_index"]) == self._anchor_slot(task))
            if (spec.get("shot_kind") != "generated_photo" or spec.get("outfit_state_ref") != board.get("outfit_state_ref")
                    or int(spec.get("slot_index") or 0) != self._anchor_slot(task)):
                raise HeroFirstError("frozen anchor photo does not match P1 outfit")
            spec.pop("board_spec", None)
            return spec
        first = deepcopy(next(s for s in task.plan_json["shots"] if int(s["slot_index"]) == self._anchor_slot(task)))
        first.pop("board_spec", None)
        first.update(shot_kind="generated_photo", slot_role="full_look", overlay_text="",
                     purpose="LOOK_01 独立人物底图，仅作为首图拆解的人物与身份来源；不是第二套穿搭",
                     camera_hint="正面完整全身，腰位平视，头顶和鞋底完整，手臂自然舒展",
                     overlay_spec={"enabled": False}, shot_grammar="MULTI_LOOK_ANCHOR")
        first["composition_contract"] = {"framing": "full_body", "camera_angle": "waist_level",
            "instruction": "正面完整全身，人物高度约85–92%，完整展示LOOK_01全部单品；自然比例，不机械拉伸"}
        return first

    def _plan_section(self, task: ContentTask, section: str) -> Dict[str, Any]:
        return ((task.plan_json or {}).get(section) or {}).get("snapshot") or {}

    def _persona_snapshot(self, task: ContentTask) -> Dict[str, Any]:
        return self._plan_section(task, "persona")

    def _hero_continuity_reference(self, task: ContentTask, slot_index: int) -> List[str]:
        anchor_slot = int((task.plan_json or {}).get("anchor_slot") or self.HERO_SLOT)
        if slot_index == anchor_slot:
            return []
        if self._workflow_v2(task):
            anchor = self._selected_anchor_shot(task, [])
            return [anchor.image_url]
        hero = next(
            (
                shot
                for shot in self._latest_per_slot(
                    self._repository.list_shots(task.task_id)
                )
                if shot.slot_index == anchor_slot
                and shot.shot_status == SHOT_GENERATED
                and shot.qa_status == QC_PASSED
                and shot.image_url
            ),
            None,
        )
        return [hero.image_url] if hero else []

    @staticmethod
    def _outfit_state(task: ContentTask, plan_shot: Dict[str, Any]) -> Dict[str, Any]:
        state_ref = str(plan_shot.get("outfit_state_ref") or "FINAL")
        return dict(
            ((task.plan_json or {}).get("outfit_states") or {}).get(state_ref)
            or {}
        )

    @staticmethod
    def _reference_roles(task: ContentTask, shot: ContentShot) -> Dict[str, Any]:
        anchor_slot = int((task.plan_json or {}).get("anchor_slot") or 1)
        return {
            "anchor_slot": anchor_slot,
            "anchor_reference_role": [
                "persona_identity",
                "product_identity",
                "lighting_reference",
            ],
            "copy_exact_anchor_outfit": bool(
                shot.slot_index == anchor_slot
                or ((task.plan_json or {}).get("recipe_execution") or {}).get(
                    "transform_mode"
                ) != "controlled_outfit_change"
            ),
        }

    @staticmethod
    def _latest_per_slot(shots: List[ContentShot]) -> List[ContentShot]:
        by_slot: Dict[int, ContentShot] = {}
        for shot in shots:
            current = by_slot.get(shot.slot_index)
            if current is None or shot.shot_version > current.shot_version:
                by_slot[shot.slot_index] = shot
        return [by_slot[slot] for slot in sorted(by_slot)]

    @staticmethod
    def _sha256(path: Optional[str]) -> Optional[str]:
        if not path:
            return None
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
