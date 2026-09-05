"""Visual-model QA orchestration for individual shots and the five-shot set.

The model provider stays behind ``adapter.review`` so production can reuse the
workspace's approved vision route.  Media QC and visual QA remain separate:
file dimensions alone never claim product/persona consistency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import hashlib
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from domain.statuses import QC_FAILED, QC_PASSED, TASK_IMAGE_REVIEW

SINGLE_DIMENSIONS = (
    "product_fidelity",
    "persona_fidelity",
    "look_fidelity",
    "scene_fit",
    "creator_realism",
)
GROUP_DIMENSIONS = (
    "product_consistency",
    "persona_consistency",
    "look_consistency",
    "scene_consistency",
    "shot_diversity",
)
PASS_SCORE = 80
HARD_GATES_ONLY = "hard_gates_only"
HARD_FAIL_KEYS = (
    "product_fidelity",
    "product_consistency",
    "persona_fidelity",
    "persona_consistency",
)
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
CREATOR_CRM_DIR = WORKSPACE_ROOT / "skills" / "creator-crm"


class VisualQaError(RuntimeError):
    pass


class VisualQaTimeout(VisualQaError):
    """Service failure, never a visual pass or a content rejection."""


def assessment_v2_enabled(contract: Mapping[str, Any]) -> bool:
    """Opt in through a frozen contract, not a mutable profile lookup."""
    return (contract.get("assessment_schema_version") == 2
            and contract.get("decision_policy") == HARD_GATES_ONLY)


def review_decision_policy(contract: Mapping[str, Any]) -> str:
    if assessment_v2_enabled(contract):
        return HARD_GATES_ONLY
    return "independent_dimensions" if contract else "legacy_average"


def review_shot_context(plan: Mapping[str, Any], slot_index: int, **fallback) -> Dict[str, Any]:
    source = next((item for item in plan.get("shots") or []
                   if int(item.get("slot_index") or 0) == int(slot_index)), {})
    return {**fallback, **dict(source), "slot_index": int(slot_index)}


def select_quality_rules(
    contract: Mapping[str, Any], scope: str, legacy_dimensions: Sequence[str] = (),
    *, shots: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Tuple[Tuple[str, ...], Dict[str, Dict[str, Any]]]:
    configured = contract.get("dimensions") or {}
    if not isinstance(configured, Mapping) or not configured:
        return tuple(legacy_dimensions), {}
    rules = {}
    v2 = assessment_v2_enabled(contract)
    for key, raw in configured.items():
        rule = dict(raw or {}) if isinstance(raw, Mapping) else {}
        if scope not in list(rule.get("scope") or ["single", "group"]):
            continue
        if v2:
            applicability = rule.get("applies_to") or {}
            allowed = {"slot_indexes", "slot_roles", "shot_kinds", "exclude_slot_roles"}
            if not isinstance(applicability, Mapping) or set(applicability) - allowed:
                raise VisualQaError(f"invalid applies_to for quality dimension {key}")
            for field_name, values in applicability.items():
                expected = int if field_name == "slot_indexes" else str
                if (not isinstance(values, list) or not values
                        or any(isinstance(value, bool) or not isinstance(value, expected) for value in values)):
                    raise VisualQaError(f"invalid applies_to.{field_name} for quality dimension {key}")
            if shots is None and applicability:
                raise VisualQaError(f"shot context required for quality dimension {key}")
            relevant = []
            for shot in shots or []:
                if ("slot_indexes" in applicability and shot.get("slot_index") not in applicability["slot_indexes"]
                        or "slot_roles" in applicability and shot.get("slot_role") not in applicability["slot_roles"]
                        or "shot_kinds" in applicability and shot.get("shot_kind") not in applicability["shot_kinds"]
                        or shot.get("slot_role") in applicability.get("exclude_slot_roles", [])):
                    continue
                relevant.append(int(shot.get("slot_index") or 0))
            if applicability and not relevant:
                continue
            if shots is not None:
                rule["applicable_slot_indexes"] = relevant
        rules[str(key)] = rule
    if not rules:
        raise VisualQaError(f"quality profile has no applicable dimensions for scope {scope!r}")
    return tuple(rules), rules


def file_hash(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass
class VisualQaDecision:
    passed: bool
    score: Optional[int]
    dimensions: Dict[str, int]
    reason_codes: List[str] = field(default_factory=list)
    notes: str = ""
    provider: str = ""
    model: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)
    next_actions: List[str] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class VisualQaReport:
    task_id: str
    passed: bool
    single: Dict[int, VisualQaDecision]
    group: VisualQaDecision


class VisualQaService:
    """Run real adapter reviews and persist auditable structured results."""

    def __init__(
        self,
        repository,
        adapter,
        *,
        inter_request_delay_seconds: float = 0,
        total_timeout_seconds: float = 600,
    ):
        if adapter is None or not callable(getattr(adapter, "review", None)):
            raise VisualQaError("a visual QA adapter with review(payload) is required")
        self._repository = repository
        self._adapter = adapter
        self._inter_request_delay_seconds = max(
            0.0, float(inter_request_delay_seconds)
        )
        self._request_count = 0
        self._total_timeout_seconds = max(1.0, float(total_timeout_seconds))
        self._deadline = None

    def review_task(self, task_id: str, *, force_recheck: bool = False) -> VisualQaReport:
        self._deadline = time.monotonic() + self._total_timeout_seconds
        self._request_count = 0
        task = self._repository.get_task(task_id)
        if task is None:
            raise VisualQaError(f"task {task_id} not found")
        if task.task_status != TASK_IMAGE_REVIEW:
            raise VisualQaError(
                f"task {task_id} status {task.task_status!r} is not image_review"
            )
        from services.workflow_v2 import RevisionAssetResolver, canonical_hash, workflow_v2_enabled
        rows = self._repository.list_shots(task_id)
        v2 = workflow_v2_enabled(task)
        revision = None
        selection_hash = None
        plan = task.plan_json or {}
        product = task.product_snapshot_json.get("product", {})
        if v2:
            revision = self._repository.get_task_revision(task.active_revision_id)
            if revision is None:
                raise VisualQaError("active revision missing")
            selection_hash = revision.selection_hash
            plan = revision.plan_snapshot_json.get("plan") or {}
            product = (revision.plan_snapshot_json.get("product_snapshot") or {}).get("product", {})
            by_id = {shot.shot_id: shot for shot in rows}
            shots = []
            for index in range(1, len(plan.get("shots") or []) + 1):
                asset = RevisionAssetResolver.selected(revision, f"shot:{index}")
                shot = by_id.get(asset["asset_id"])
                if (shot is None or shot.image_sha256 != asset["sha256"]
                        or not shot.image_url or file_hash(shot.image_url) != asset["sha256"]):
                    raise VisualQaError("selected revision image changed or missing")
                shots.append(shot)
        else:
            shots = self._latest(rows)
        if len(shots) != 5 or any(not shot.image_url for shot in shots):
            raise VisualQaError("visual QA requires five generated image paths")

        quality_contract = plan.get("quality_contract") or {}
        policy = review_decision_policy(quality_contract)
        contexts = [review_shot_context(plan, shot.slot_index, slot_role=shot.slot_role,
                                       shot_kind="generated_photo") for shot in shots]
        if v2 and assessment_v2_enabled(quality_contract):
            return self._review_group_first(task, revision, plan, product, shots, contexts,
                                            force_recheck=force_recheck)
        single_dimensions, single_rules = self._dimension_rules(
            quality_contract, "single", SINGLE_DIMENSIONS, shots=contexts
        )
        group_dimensions, group_rules = self._dimension_rules(
            quality_contract, "group", GROUP_DIMENSIONS, shots=contexts
        )
        independent = bool(quality_contract)
        reference_hashes = {}
        if v2:
            persona = ((plan.get("persona") or {}).get("snapshot") or {})
            references = list(product.get("reference_images") or []) + list(persona.get("local_reference_images") or [])
            reference_hashes = {path: file_hash(path) for path in references if isinstance(path, str) and Path(path).is_file()}

        single: Dict[int, VisualQaDecision] = {}
        for shot in shots:
            context = review_shot_context(plan, shot.slot_index, slot_role=shot.slot_role,
                                          shot_kind="generated_photo")
            if assessment_v2_enabled(quality_contract):
                single_dimensions, single_rules = self._dimension_rules(
                    quality_contract, "single", SINGLE_DIMENSIONS, shots=[context]
                )
            payload = {
                "scope": "single_shot",
                "task_id": task_id,
                "slot_index": shot.slot_index,
                "image_paths": [shot.image_url],
                "product": product,
                "plan": plan,
                "required_dimensions": list(single_dimensions),
                "dimension_rules": single_rules,
                "decision_policy": policy,
                "hard_fail_keys": [
                    key for key, rule in single_rules.items()
                    if rule.get("hard_gate")
                ] or ["product_fidelity"],
            }
            if assessment_v2_enabled(quality_contract):
                payload["current_shot"] = context
                payload["shots_being_reviewed"] = [context]
            fingerprint = canonical_hash({
                "revision": revision.revision_id if revision else None,
                "shot": shot.shot_id, "sha256": shot.image_sha256,
                "plan": plan, "product": product, "rules": single_rules,
                "reference_hashes": reference_hashes,
            })
            previous_qa = shot.qa_json or {}
            cached = previous_qa.get("visual_model_qa") or {}
            cache_valid = not force_recheck and (not v2 or cached.get("input_fingerprint") == fingerprint)
            decision = self._resume_decision(
                previous_qa if cache_valid else None,
                single_dimensions,
                single_rules if independent else None,
                decision_policy=policy,
            )
            if decision is None:
                decision = normalize_decision(
                    self._review(payload),
                    single_dimensions,
                    dimension_rules=single_rules if independent else None,
                    decision_policy=policy,
                )
            single[shot.slot_index] = decision
            previous = shot.qa_json or {}
            self._repository.update_shot_qa(
                shot.shot_id,
                qa_status=QC_PASSED if decision.passed else QC_FAILED,
                qa_json={
                    **previous,
                    "visual_model_qa": {**decision_to_dict(decision), "input_fingerprint": fingerprint},
                },
                failure_detail=None if decision.passed else ",".join(decision.reason_codes),
            )

        if all(decision.passed for decision in single.values()):
            group = normalize_decision(
                self._review(
                    {
                        "scope": "five_shot_group",
                        "task_id": task_id,
                        "image_paths": [shot.image_url for shot in shots],
                        "product": product,
                        "plan": plan,
                        "required_dimensions": list(group_dimensions),
                        "dimension_rules": group_rules,
                        "decision_policy": policy,
                        **({"shots_being_reviewed": contexts} if assessment_v2_enabled(quality_contract) else {}),
                        "hard_fail_keys": [
                            key for key, rule in group_rules.items()
                            if rule.get("hard_gate")
                        ] or ["product_consistency"],
                    }
                ),
                group_dimensions,
                dimension_rules=group_rules if independent else None,
                decision_policy=policy,
            )
        else:
            group = VisualQaDecision(
                passed=False,
                score=0,
                dimensions={key: 0 for key in group_dimensions},
                reason_codes=["single_shot_qa_failed"],
                notes="group review skipped until every single shot passes",
            )
        if v2:
            current = self._repository.get_task(task_id)
            current_revision = self._repository.get_task_revision(revision.revision_id)
            if (current.active_revision_id != revision.revision_id or current_revision is None
                    or current_revision.selection_hash != selection_hash):
                raise VisualQaError("整组审核期间素材选择已变化，请按当前版本重新审核")
        existing = task.group_qa_json or {}
        self._repository.update_task_plan(
            task_id,
            group_qa_json={
                **existing,
                "stage_c_visual": {
                    "decision": "passed" if group.passed else "failed",
                    "single": {
                        str(slot): decision_to_dict(decision)
                        for slot, decision in single.items()
                    },
                    "group": decision_to_dict(group),
                },
            },
        )
        return VisualQaReport(task_id, group.passed, single, group)

    @staticmethod
    def _reference_hashes(plan, product):
        persona = ((plan.get("persona") or {}).get("snapshot") or {})
        references = list(product.get("reference_images") or []) + list(
            persona.get("local_reference_images") or persona.get("reference_images") or [])
        return {path: file_hash(path) for path in references if isinstance(path, str) and Path(path).is_file()}

    @staticmethod
    def _require_media(shots):
        from services.media_qc import MediaQcError, require_shot_media_qc
        try:
            for shot in shots:
                require_shot_media_qc(shot)
        except (MediaQcError, OSError) as exc:
            raise VisualQaError(str(exc)) from exc

    def _review_group_first(self, task, revision, plan, product, shots, contexts, *, force_recheck):
        from services.workflow_v2 import canonical_hash
        self._require_media(shots)
        contract = plan.get("quality_contract") or {}
        dimensions, rules = select_quality_rules(contract, "group", shots=contexts)
        selection_hash = revision.selection_hash
        reference_hashes = self._reference_hashes(plan, product)
        fingerprint = canonical_hash({"revision": revision.revision_id, "selection": selection_hash,
            "images": [{"id": shot.shot_id, "sha256": shot.image_sha256} for shot in shots],
            "plan": plan, "product": product, "rules": rules, "reference_hashes": reference_hashes})
        previous = (task.group_qa_json or {}).get("stage_c_visual") or {}
        cached = previous.get("group") if (not force_recheck and previous.get("mode") == "group_first"
            and previous.get("input_fingerprint") == fingerprint) else None
        group = self._resume_decision({"visual_model_qa": cached} if cached else None, dimensions, rules,
                                      decision_policy=HARD_GATES_ONLY)
        if group is None:
            group = normalize_decision(self._review({
                "scope": "five_shot_group", "task_id": task.task_id,
                "image_paths": [shot.image_url for shot in shots], "product": product, "plan": plan,
                "shots_being_reviewed": contexts, "required_dimensions": list(dimensions),
                "dimension_rules": rules, "decision_policy": HARD_GATES_ONLY,
                "hard_fail_keys": [key for key, rule in rules.items() if rule.get("hard_gate")],
            }), dimensions, dimension_rules=rules, decision_policy=HARD_GATES_ONLY)
        current = self._repository.get_task(task.task_id)
        current_revision = self._repository.get_task_revision(revision.revision_id)
        if (current is None or current.task_status != TASK_IMAGE_REVIEW
                or current.active_revision_id != revision.revision_id or current_revision is None
                or current_revision.selection_hash != selection_hash
                or self._reference_hashes(plan, product) != reference_hashes):
            raise VisualQaError("selected group or references changed while content review was running")
        self._require_media(shots)
        # No per-shot model passes are manufactured, and technical qa_status
        # remains owned by media QC. A group review is the content decision.
        self._repository.update_task_plan(task.task_id, group_qa_json={**(current.group_qa_json or {}),
            "stage_c_visual": {"mode": "group_first", "input_fingerprint": fingerprint,
                "decision": "passed" if group.passed else "failed", "single": {},
                "group": decision_to_dict(group)}})
        return VisualQaReport(task.task_id, group.passed, {}, group)

    def review_shot(self, task_id: str, slot_index: int, *, force_recheck: bool = False) -> VisualQaDecision:
        """Explicit diagnostic/repair check; a single pass never approves a group."""
        from services.workflow_v2 import RevisionAssetResolver, ScopedReviewService, canonical_hash, workflow_v2_enabled
        self._deadline, self._request_count = time.monotonic() + self._total_timeout_seconds, 0
        task = self._repository.get_task(task_id)
        if task is None or task.task_status != TASK_IMAGE_REVIEW or not workflow_v2_enabled(task):
            raise VisualQaError("targeted content review requires a V2 image_review task")
        revision = self._repository.get_task_revision(task.active_revision_id)
        if revision is None:
            raise VisualQaError("active revision missing")
        plan = revision.plan_snapshot_json.get("plan") or {}
        contract = plan.get("quality_contract") or {}
        if not assessment_v2_enabled(contract):
            raise VisualQaError("targeted content review requires the explicit V2 assessment policy")
        asset = RevisionAssetResolver.selected(revision, f"shot:{int(slot_index)}")
        shot = next((item for item in self._repository.list_shots(task_id) if item.shot_id == asset["asset_id"]), None)
        if shot is None or shot.slot_index != int(slot_index) or shot.image_sha256 != asset["sha256"]:
            raise VisualQaError("targeted image selection is missing or stale")
        self._require_media([shot])
        selection_hash = revision.selection_hash
        product = (revision.plan_snapshot_json.get("product_snapshot") or {}).get("product", {})
        context = review_shot_context(plan, shot.slot_index, slot_role=shot.slot_role, shot_kind="generated_photo")
        dimensions, rules = select_quality_rules(contract, "single", shots=[context])
        references = self._reference_hashes(plan, product)
        fingerprint = canonical_hash({"revision": revision.revision_id, "image": shot.image_sha256,
            "plan": plan, "product": product, "rules": rules, "reference_hashes": references})
        previous = dict(shot.qa_json or {})
        cached = previous.get("targeted_visual_qa") or {}
        decision = self._resume_decision({"visual_model_qa": cached}
            if not force_recheck and cached.get("input_fingerprint") == fingerprint else None,
            dimensions, rules, decision_policy=HARD_GATES_ONLY)
        if decision is None:
            decision = normalize_decision(self._review({"scope": "single_shot", "task_id": task_id,
                "slot_index": shot.slot_index, "current_shot": context, "shots_being_reviewed": [context],
                "image_paths": [shot.image_url], "product": product, "plan": plan,
                "required_dimensions": list(dimensions), "dimension_rules": rules,
                "decision_policy": HARD_GATES_ONLY,
                "hard_fail_keys": [key for key, rule in rules.items() if rule.get("hard_gate")],
            }), dimensions, dimension_rules=rules, decision_policy=HARD_GATES_ONLY)
        current, current_revision = self._repository.get_task(task_id), self._repository.get_task_revision(revision.revision_id)
        if (current is None or current.task_status != TASK_IMAGE_REVIEW
                or current.active_revision_id != revision.revision_id or current_revision is None
                or current_revision.selection_hash != selection_hash or self._reference_hashes(plan, product) != references):
            raise VisualQaError("targeted review inputs changed while model review was running")
        self._require_media([shot])
        self._repository.update_shot_qa(shot.shot_id, qa_status=shot.qa_status,
            qa_json={**previous, "targeted_visual_qa": {**decision_to_dict(decision), "input_fingerprint": fingerprint}},
            failure_detail=shot.failure_detail)
        if not decision.passed:
            # One evidenced hard error invalidates a group's earlier pass;
            # the converse is deliberately false: one good photo is not a group pass.
            ScopedReviewService(self._repository).record(task_id, scope="group", target_id="group",
                decision="failed", dimensions=decision.dimensions, reason_codes=decision.reason_codes,
                evidence={"source_scope": "targeted_single", "slot_index": shot.slot_index,
                          "asset_id": shot.shot_id, "assessment": decision_to_dict(decision)},
                reviewer_type="model", reviewer="opv_targeted_vision")
            self._repository.update_task_plan(task_id, group_qa_json={**(current.group_qa_json or {}),
                "stage_c_visual": {"mode": "targeted_single_failure", "decision": "failed",
                    "single": {str(slot_index): decision_to_dict(decision)}, "group": {"passed": False}}})
        return decision

    def _review(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        if self._request_count and self._inter_request_delay_seconds:
            time.sleep(self._inter_request_delay_seconds)
        deadline = self._deadline if self._deadline is not None else time.monotonic() + self._total_timeout_seconds
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise VisualQaTimeout("整组审核已达到总时限；已完成单图结果已保存，请重试审核")
        print(f"[visual-qa] reviewing {payload.get('scope')} P{payload.get('slot_index', 'group')}", flush=True)
        result = self._adapter.review({**payload, "review_timeout_seconds": remaining})
        self._request_count += 1
        return result

    @staticmethod
    def _resume_decision(
        qa_json: Optional[Mapping[str, Any]],
        dimensions: Sequence[str],
        dimension_rules: Optional[Mapping[str, Mapping[str, Any]]],
        *, decision_policy: str = "independent_dimensions",
    ) -> Optional[VisualQaDecision]:
        existing = dict((qa_json or {}).get("visual_model_qa") or {})
        existing_dimensions = existing.get("dimensions") or {}
        if set(existing_dimensions) != set(dimensions):
            return None
        try:
            return normalize_decision(
                existing,
                dimensions,
                dimension_rules=dimension_rules,
                decision_policy=decision_policy,
            )
        except VisualQaError:
            return None

    @staticmethod
    def _dimension_rules(
        quality_contract: Mapping[str, Any],
        scope: str,
        legacy_dimensions: Sequence[str],
        *, shots: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> Tuple[Tuple[str, ...], Dict[str, Dict[str, Any]]]:
        return select_quality_rules(quality_contract, scope, legacy_dimensions, shots=shots)

    @staticmethod
    def _latest(shots):
        by_slot = {}
        for shot in shots:
            current = by_slot.get(shot.slot_index)
            if current is None or shot.shot_version > current.shot_version:
                by_slot[shot.slot_index] = shot
        return [by_slot[index] for index in sorted(by_slot)]




class RenderVisualQaService:
    """Review samples of the actual MP4; never mistake media QC for visual QA."""

    def __init__(self, repository, adapter, *, ffmpeg_bin=None):
        self._repository = repository
        self._adapter = adapter
        from services.video_renderer import FFmpegStillRenderer
        self._ffmpeg = FFmpegStillRenderer._resolve_binary(
            ffmpeg_bin, env_name="OPV_FFMPEG_BIN", executable="ffmpeg"
        )

    def review_task(self, task_id: str, *, render_id: str, reviewer="opv_render_vision"):
        from services.workflow_v2 import RenderReviewService, workflow_v2_enabled
        task = self._repository.get_task(task_id)
        render = self._repository.get_render(render_id)
        if (task is None or not workflow_v2_enabled(task) or render is None
                or render.task_id != task_id or task.task_status != "video_review"
                or render.origin_revision_id != task.active_revision_id
                or render.qc_status != "passed"):
            raise VisualQaError("final visual review requires the current V2 video_review render")
        if not render.output_url or file_hash(render.output_url) != render.output_sha256:
            raise VisualQaError("render bytes changed after technical QC")
        revision = self._repository.get_task_revision(task.active_revision_id)
        if revision is None:
            raise VisualQaError("active revision missing")
        plan = revision.plan_snapshot_json.get("plan") or {}
        quality_contract = plan.get("quality_contract") or {}
        policy = review_decision_policy(quality_contract)
        rules = {
            key: dict(rule) for key, rule in (plan.get("quality_contract", {}).get("dimensions") or {}).items()
            if "render" in (rule.get("scope") or [])
        } or {
            "product_fidelity": {"min_score": 80, "hard_gate": True},
            "creator_naturalness": {"min_score": 78, "hard_gate": True},
            "shot_role_fidelity": {"min_score": 80, "hard_gate": True},
            "render_framing": {"min_score": 80, "hard_gate": True},
        }
        contexts = [dict(shot) for shot in plan.get("shots") or []]
        if assessment_v2_enabled(quality_contract):
            _, rules = select_quality_rules(quality_contract, "render", shots=contexts)
        samples = self._extract_frames(render)
        raw = self._adapter.review({
            "scope": "render", "task_id": task_id,
            "image_paths": [sample["path"] for sample in samples],
            "frame_samples": samples,
            "product": (revision.plan_snapshot_json.get("product_snapshot") or {}).get("product", {}),
            "plan": {**plan, "copy": render.copy_snapshot_json},
            "required_dimensions": list(rules), "dimension_rules": rules,
            "decision_policy": policy if quality_contract else "independent_dimensions",
            **({"shots_being_reviewed": contexts} if assessment_v2_enabled(quality_contract) else {}),
            "hard_fail_keys": [key for key, rule in rules.items() if rule.get("hard_gate")],
        })
        decision = normalize_decision(raw, tuple(rules), dimension_rules=rules,
                                      decision_policy=policy)
        current = self._repository.get_task(task_id)
        current_revision = self._repository.get_task_revision(task.active_revision_id)
        if (current is None or current.active_revision_id != revision.revision_id
                or current.task_status != "video_review" or current_revision is None
                or current_revision.selection_hash != revision.selection_hash
                or file_hash(render.output_url) != render.output_sha256):
            raise VisualQaError("render/revision changed while final review was running")
        return RenderReviewService(self._repository).record(
            task_id, render_id=render_id,
            decision="passed" if decision.passed else "failed",
            dimensions=decision.dimensions, reason_codes=decision.reason_codes,
            evidence={"notes": decision.notes, "provider": decision.provider,
                      "warnings": decision.warnings,
                      "model": decision.model, "frame_samples": samples,
                      "coverage": "midpoint_and_end_per_timeline_shot; sampled_frames_not_continuous_playback"},
            reviewer_type="model", reviewer=reviewer,
        )

    def _extract_frames(self, render) -> List[Dict[str, Any]]:
        path = Path(render.output_url)
        root = path.parent / "review_frames" / render.render_id
        root.mkdir(parents=True, exist_ok=True)
        timeline = render.timeline_json or []
        if not timeline:
            raise VisualQaError("render timeline missing; cannot select representative frames")
        samples = []
        for item in timeline:
            start = float(item.get("start_ms") or 0)
            length = float(item.get("effective_ms") or item.get("planned_ms") or 0)
            if length <= 0:
                raise VisualQaError("invalid render timeline duration")
            for phase, offset in (("middle", length * .5), ("end", max(0, length - 80))):
                timestamp = min(start + offset, max(0, float(render.duration_ms) - 40)) / 1000
                output = root / f"P{int(item['slot_index'])}_{phase}.jpg"
                try:
                    completed = subprocess.run(
                        [self._ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
                         "-ss", str(timestamp), "-i", str(path), "-frames:v", "1", str(output)],
                        capture_output=True, text=True, timeout=30,
                    )
                except (OSError, subprocess.TimeoutExpired) as exc:
                    raise VisualQaError("final review frame extraction failed") from exc
                if completed.returncode or not output.is_file():
                    raise VisualQaError("final review frame extraction failed")
                samples.append({"slot_index": item["slot_index"], "phase": phase,
                                "seconds": timestamp, "path": str(output), "sha256": file_hash(str(output))})
        return samples


# Module-level cache with namespace-hijack recovery: the openai-image skill
# also exposes a "core" package, so whichever imports last wins sys.modules.
# Caching the client class after first import makes both sides coexist.
_CREATOR_CRM_CACHE: Dict[str, Any] = {}


def _load_creator_crm_client():
    import sys

    if _CREATOR_CRM_CACHE.get("factory") is None:
        if str(CREATOR_CRM_DIR) not in sys.path:
            sys.path.insert(0, str(CREATOR_CRM_DIR))
        try:
            from core.llm_analyzer import LLMClient
        except ModuleNotFoundError:
            for root in ("core", "app"):
                for name in [
                    m
                    for m in list(sys.modules)
                    if m == root or m.startswith(root + ".")
                ]:
                    del sys.modules[name]
            if str(CREATOR_CRM_DIR) not in sys.path:
                sys.path.insert(0, str(CREATOR_CRM_DIR))
            from core.llm_analyzer import LLMClient
        _CREATOR_CRM_CACHE["factory"] = LLMClient
    return _CREATOR_CRM_CACHE["factory"]

class CreatorCrmVisualQaAdapter:
    """Production adapter reusing creator-crm's configured vision LLM route."""

    def __init__(self, client=None, *, timeout_seconds: float = 120):
        self._client = client
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def _load_client(self):
        if self._client is None:
            self._client = _load_creator_crm_client()()
        return self._client

    def review(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        if self._client is not None:
            return self._review_direct(payload)
        timeout = min(self._timeout_seconds, float(payload.get("review_timeout_seconds") or self._timeout_seconds))
        try:
            result = subprocess.run(
                [sys.executable, "-u", str(PACKAGE_ROOT / "services" / "visual_qa_worker.py")],
                input=json.dumps(dict(payload), ensure_ascii=False), text=True,
                capture_output=True, timeout=max(0.01, timeout), cwd=str(PACKAGE_ROOT),
            )
        except subprocess.TimeoutExpired as exc:
            raise VisualQaTimeout(
                f"视觉审核服务超过 {timeout:.0f} 秒，已终止本次调用；请重试审核或由操作者确认"
            ) from exc
        if result.returncode:
            # Provider logs can include request metadata; do not echo them to Feishu.
            raise VisualQaError(f"视觉审核服务异常（worker exit {result.returncode}），请重试审核")
        try:
            value = json.loads(result.stdout)
        except (ValueError, TypeError) as exc:
            raise VisualQaError("视觉审核服务返回无效 JSON") from exc
        if not isinstance(value, dict):
            raise VisualQaError("视觉审核服务返回非对象 JSON")
        return value

    def _review_direct(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        generated = [str(path) for path in payload.get("image_paths") or []]
        product = payload.get("product") or {}
        product_refs = [
            str(path)
            for path in product.get("reference_images") or []
            if isinstance(path, str) and Path(path).is_file()
        ]
        persona = ((payload.get("plan") or {}).get("persona") or {}).get("snapshot") or {}
        persona_refs = [
            str(path)
            for path in persona.get("local_reference_images")
            or persona.get("reference_images")
            or []
            if isinstance(path, str) and Path(path).is_file()
        ]
        images = product_refs + persona_refs + generated
        if not generated or not all(Path(path).is_file() for path in generated):
            raise VisualQaError("visual QA generated image path missing")
        dimensions = list(payload.get("required_dimensions") or [])
        prompt = self._prompt(
            payload, len(product_refs), len(persona_refs), dimensions
        )
        client = self._load_client()
        response = client.chat_with_multiple_images(images, prompt, max_tokens=1800)
        result = client.parse_json_response(response)
        if not isinstance(result, dict):
            raise VisualQaError("vision model returned non-object JSON")
        result.setdefault("provider", "creator-crm-vision-route")
        result.setdefault("model", str(getattr(client, "_working_model", "") or getattr(client, "model", "")))
        return result

    @staticmethod
    def _prompt(
        payload: Mapping[str, Any],
        product_ref_count: int,
        persona_ref_count: int,
        dimensions: List[str],
    ) -> str:
        scope = str(payload.get("scope") or "")
        plan = payload.get("plan") or {}
        contract = {
            "scope": scope,
            "image_order": {
                "product_reference_images_first": product_ref_count,
                "persona_reference_images_next": persona_ref_count,
                "generated_images_after": len(payload.get("image_paths") or []),
            },
            "plan_anchors": {
                "persona": plan.get("persona"),
                "look": plan.get("look"),
                "scene": plan.get("scene"),
                "shots": plan.get("shots"),
                "product_facts": plan.get("product_facts"),
                "outfit_plan": plan.get("outfit_plan"),
                "outfit_states": plan.get("outfit_states"),
                "recipe_execution": plan.get("recipe_execution"),
            },
            "score_dimensions": dimensions,
            "dimension_rules": payload.get("dimension_rules") or {},
            "decision_policy": payload.get("decision_policy") or "legacy_average",
            "hard_fail_keys": list(payload.get("hard_fail_keys") or []),
            "frame_samples": payload.get("frame_samples") or [],
        }
        if assessment_v2_enabled(plan.get("quality_contract") or {}):
            current = payload.get("current_shot")
            if current is None and payload.get("slot_index") is not None:
                current = review_shot_context(plan, int(payload["slot_index"]))
            contract["current_shot"] = current
            contract["shots_being_reviewed"] = (payload.get("shots_being_reviewed")
                or ([current] if current is not None else plan.get("shots") or []))
            policy_instruction = (
                "所有指定维度给出0-100整数，但只有hard_gate=true的适用维度低于min_score才可阻断。"
                "hard_gate=false的低分仅是优化建议，不得据此返回passed=false或要求先返工再继续。"
                "每个维度只评applicable_slot_indexes列出的图位，严格按current_shot或shots_being_reviewed区分图位与镜头角色。"
                "不要对未列出的维度评分，不要因商品细节镜头未展示脸部、全身或拼贴卡而扣不适用的分。"
                "不得把未观察到的内容当作已确认错误或编造证据。"
            )
        else:
            policy_instruction = (
                "所有指定维度必须给出0-100整数。若 decision_policy=independent_dimensions，"
                "每个维度必须分别达到其 min_score，禁止用平均分掩盖失败维度；否则使用旧平均分规则。"
            )
        return (
            "你是跨境女装图文内容视觉质检员。严格比较商品参考图、人物参考图与生成图，"
            "并按计划锚点判断人物、穿搭、场景、真实创作者感和镜头差异。"
            "商品颜色、版型、结构、图案或材质明显漂移时，对商品相关维度打低于70分。"
            "生成图人物若与人物参考图不是同一身份，或五张图人物身份明显漂移，"
            "对应人物维度必须低于70分。不要因画面好看掩盖商品或人物错误。"
            "商品细节镜头应展示参考可验证的衣物结构；脸部主导、过度磨皮或局部失真须降低对应维度。"
            "scope=render 时逐帧检查实际视频中的裁切、文字可读性、人物比例和商品展示；"
            "只根据给出的关键帧判断，不能声称已观察到未提供的连续运动。"
            "只输出一个 JSON 对象，不要 Markdown。"
            "JSON 格式必须是："
            '{"passed":true,"dimensions":{"维度名":0},'
            '"reason_codes":[],"notes":"简短证据"}。'
            + policy_instruction + "\n质检合同：" + json.dumps(contract, ensure_ascii=False)
        )


def normalize_decision(
    raw: Mapping[str, Any],
    dimensions,
    *,
    dimension_rules: Optional[Mapping[str, Mapping[str, Any]]] = None,
    decision_policy: str = "independent_dimensions",
) -> VisualQaDecision:
    if not isinstance(raw, Mapping):
        raise VisualQaError("visual QA adapter must return an object")
    scores: Dict[str, int] = {}
    raw_scores = raw.get("dimensions") or {}
    for key in dimensions:
        value = raw_scores.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise VisualQaError(f"visual QA dimension {key!r} must be numeric")
        scores[key] = max(0, min(100, int(round(value))))
    rules = dict(dimension_rules or {})
    next_actions: List[str] = []
    warnings: List[Dict[str, Any]] = []
    if rules:
        failures = []
        for key, score_value in scores.items():
            rule = dict(rules.get(key) or {})
            threshold = int(rule.get("min_score") or PASS_SCORE)
            if score_value < threshold:
                if decision_policy == HARD_GATES_ONLY and rule.get("hard_gate") is False:
                    warnings.append({"dimension": key, "score": score_value, "min_score": threshold,
                                     "suggested_action": str(rule.get("fail_action") or "")})
                    continue
                failures.append(key)
                action = str(rule.get("fail_action") or "regenerate_offending_slot")
                if action not in next_actions:
                    next_actions.append(action)
        score = None
        if decision_policy == HARD_GATES_ONLY:
            # A legacy aggregate boolean must not turn a soft warning into a
            # veto. Unexplained contradictory evidence is not a content pass.
            if raw.get("passed") is False and not failures and not warnings:
                raise VisualQaError("review decision contradicts its dimension evidence; explicit review required")
            passed = not failures
        else:
            passed = bool(raw.get("passed", not failures)) and not failures
    else:
        score = int(round(sum(scores.values()) / len(scores)))
        hard_fail = any(scores[key] < 70 for key in HARD_FAIL_KEYS if key in scores)
        passed = (
            bool(raw.get("passed", score >= PASS_SCORE))
            and score >= PASS_SCORE
            and not hard_fail
        )
    return VisualQaDecision(
        passed=passed,
        score=score,
        dimensions=scores,
        reason_codes=[str(v) for v in raw.get("reason_codes") or []],
        notes=str(raw.get("notes") or ""),
        provider=str(raw.get("provider") or ""),
        model=str(raw.get("model") or ""),
        raw=dict(raw),
        next_actions=next_actions,
        warnings=warnings,
    )


def decision_to_dict(decision: VisualQaDecision) -> Dict[str, Any]:
    return {
        "passed": decision.passed,
        "score": decision.score,
        "dimensions": decision.dimensions,
        "reason_codes": decision.reason_codes,
        "notes": decision.notes,
        "provider": decision.provider,
        "model": decision.model,
        "next_actions": decision.next_actions,
        "warnings": decision.warnings,
    }
