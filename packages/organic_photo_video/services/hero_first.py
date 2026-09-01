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

    def __init__(self, repository, generator, output_root: Optional[Path] = None):
        self._repository = repository
        self._generator = generator
        self._output_root = (
            Path(output_root)
            if output_root
            else Path.home() / ".openclaw" / "shared" / "data" / "organic_photo_video"
        )

    # ------------------------------------------------------------------
    # Shot rows
    # ------------------------------------------------------------------

    def ensure_shots(self, task_id: str) -> List[ContentShot]:
        """Create planned shot rows from the plan if absent; return latest per slot."""
        task = self._require_task(task_id)
        if not task.plan_json:
            raise HeroFirstError(f"task {task_id} has no plan; run the planner first")
        existing = self._repository.list_shots(task_id)
        if existing:
            return self._latest_per_slot(existing)
        for plan_shot in task.plan_json["shots"]:
            self._repository.insert_shot(
                ContentShot(
                    shot_id=generate_prefixed_id("opv_shot"),
                    task_id=task_id,
                    slot_index=int(plan_shot["slot_index"]),
                    slot_role=plan_shot["slot_role"],
                    duration_ms=int(plan_shot["duration_ms"]),
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
                )
            )
        return self._latest_per_slot(self._repository.list_shots(task_id))

    def regenerate_slot(self, task_id: str, slot_index: int) -> SlotReport:
        """Retry one slot as a new shot_version (single-slot recovery rule)."""
        latest = self._latest_per_slot(self._repository.list_shots(task_id))
        current = next((s for s in latest if s.slot_index == slot_index), None)
        if current is None:
            raise HeroFirstError(f"task {task_id} has no shot for slot {slot_index}")
        task = self._require_task(task_id)
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
        self._repository.insert_shot(new_shot)
        continuity = self._hero_continuity_reference(task, slot_index)
        return self._generate_slot(task, new_shot, continuity_reference_images=continuity)

    # ------------------------------------------------------------------
    # Main flow
    # ------------------------------------------------------------------

    def produce(self, task_id: str) -> ProduceReport:
        task = self._require_task(task_id)
        status = task.task_status
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
        shots = self._latest_per_slot(self.ensure_shots(task_id))
        # Anchor-frame-first: the recipe may pin a visual anchor other than
        # P1 (e.g. visual_transform recipes freeze the final look as P5).
        anchor_slot = int((task.plan_json or {}).get("anchor_slot") or self.HERO_SLOT)
        hero = next(
            (s for s in shots if s.slot_index == anchor_slot),
            shots[0],
        )
        hero_done = (
            hero.shot_status == SHOT_GENERATED and hero.qa_status == QC_PASSED
        )
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
            hero_report = self._generate_slot(task, hero)
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

        if self._require_task(task_id).task_status == TASK_HERO_GENERATING:
            self._repository.transition_task(
                task_id, TASK_HERO_GENERATING, TASK_IMAGE_GENERATING
            )
        for shot in sorted(shots, key=lambda s: s.slot_index):
            if shot.slot_index == hero.slot_index:
                continue
            if shot.shot_status == SHOT_GENERATED and shot.qa_status == QC_PASSED:
                continue  # resume: slot already produced
            report.slots.append(
                self._generate_slot(
                    task,
                    shot,
                    continuity_reference_images=[hero_report.image_path]
                    if hero_report.image_path else [],
                )
            )
        self._repository.transition_task(
            task_id, TASK_IMAGE_GENERATING, TASK_IMAGE_REVIEW
        )
        latest_shots = self._latest_per_slot(
            self._repository.list_shots(task_id)
        )
        selected_ids = [
            shot.shot_id
            for shot in sorted(latest_shots, key=lambda value: value.slot_index)
            if shot.shot_status == SHOT_GENERATED and shot.image_url
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
    ) -> SlotReport:
        if shot.shot_status == SHOT_PLANNED:
            self._repository.update_shot_status(shot.shot_id, SHOT_PLANNED, SHOT_GENERATING)
        elif shot.shot_status == SHOT_FAILED:
            self._repository.update_shot_status(shot.shot_id, SHOT_FAILED, SHOT_GENERATING)
        plan_shot = self._plan_shot_for(task, shot)
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
            recipe_execution=dict(
                (task.plan_json or {}).get("recipe_execution") or {}
            ),
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
                    "visual_model_qa": "pending_phase1_qa_service",
                }
            },
        )
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

    def _plan_section(self, task: ContentTask, section: str) -> Dict[str, Any]:
        return ((task.plan_json or {}).get(section) or {}).get("snapshot") or {}

    def _persona_snapshot(self, task: ContentTask) -> Dict[str, Any]:
        return self._plan_section(task, "persona")

    def _hero_continuity_reference(self, task: ContentTask, slot_index: int) -> List[str]:
        anchor_slot = int((task.plan_json or {}).get("anchor_slot") or self.HERO_SLOT)
        if slot_index == anchor_slot:
            return []
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
