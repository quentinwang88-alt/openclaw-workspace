"""Versioned selection, review and rework primitives for OPV Workflow V2.

The module deliberately keeps physical media in the existing shot/render
tables.  A revision only freezes inputs and records which physical artifacts
are selected.  This prevents the historical ``latest per slot`` behaviour
from changing an already-reviewed output.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, replace
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from domain.models import (
    ContentTask,
    QualityReview,
    TaskRevision,
    generate_prefixed_id,
)
from domain.statuses import TASK_ANCHOR_REVIEW, TASK_IMAGE_GENERATING


class WorkflowV2Error(RuntimeError):
    pass


class RevisionSelectionError(WorkflowV2Error):
    pass


def review_actor(reviewer_type: str, reviewer: str) -> str:
    """An assistant-authored observation is never a human confirmation."""
    if any(marker in reviewer.lower() for marker in ("codex", "chatgpt", "assistant")):
        return "assistant"
    if reviewer_type not in {"human", "model", "assistant", "technical"}:
        raise WorkflowV2Error("unknown review actor type")
    return reviewer_type


VALID_SCOPES = frozenset(
    {"anchor", "photo", "cutout", "board", "group", "photo_package", "render"}
)
VALID_DECISIONS = frozenset({"passed", "failed", "waived"})


def canonical_hash(value: Any) -> str:
    """Hash structured contracts deterministically; dict key order is irrelevant."""
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def workflow_v2_enabled(task: ContentTask) -> bool:
    plan = task.plan_json or {}
    return int(getattr(task, "workflow_version", 1) or 1) >= 2 or int(
        plan.get("workflow_version") or 1
    ) >= 2


def frozen_task_view(repository, task: ContentTask) -> ContentTask:
    """Read generation inputs from the active revision, not mutable task fields."""
    if not workflow_v2_enabled(task) or not task.active_revision_id:
        return task
    revision = repository.get_task_revision(task.active_revision_id)
    if revision is None or revision.task_id != task.task_id:
        raise WorkflowV2Error("active revision is missing or belongs to another task")
    snapshot = revision.plan_snapshot_json or {}
    plan = copy.deepcopy(snapshot.get("plan") or {})
    # Derived assets are outputs, not changes to the frozen creative contract.
    # Reuse them only when they were extracted from this selected anchor.
    runtime = (task.plan_json or {}).get("decomposition_assets") or {}
    anchor_id = (revision.asset_manifest_json.get("selected") or {}).get("anchor")
    anchor = (revision.asset_manifest_json.get("candidates") or {}).get(anchor_id) or {}
    person = (runtime.get("assets") or {}).get("person_cutout") or {}
    if (anchor and person.get("source_sha256") == anchor.get("sha256")
            and person.get("source_asset_id") == anchor.get("asset_id")):
        plan["decomposition_assets"] = copy.deepcopy(runtime)
        for shot in plan.get("shots") or []:
            if shot.get("shot_kind") == "composite_board":
                shot.setdefault("board_spec", {})["decomposition_assets"] = copy.deepcopy(runtime)
    return replace(
        task, plan_json=plan,
        product_snapshot_json=copy.deepcopy(snapshot.get("product_snapshot") or {}),
        copy_json=copy.deepcopy(snapshot.get("copy") if "copy" in snapshot else plan.get("copy") or {}),
    )


def _quality_profile(plan: Mapping[str, Any]) -> tuple[str, int]:
    contract = dict(plan.get("quality_contract") or {})
    return (
        str(contract.get("quality_profile_id") or "legacy-media-qc"),
        int(contract.get("quality_profile_version") or 1),
    )


class RevisionService:
    """Create revisions and update their selection manifest through CAS."""

    def __init__(self, repository):
        self._repository = repository

    def ensure_working(self, task: ContentTask, *, operator: str = "system") -> TaskRevision:
        active_id = str(getattr(task, "active_revision_id", "") or "")
        if active_id:
            current = self._repository.get_task_revision(active_id)
            if current is None:
                raise WorkflowV2Error(f"task {task.task_id} points to missing revision {active_id}")
            if current.revision_status != "working":
                raise WorkflowV2Error(f"task {task.task_id} revision {active_id} is not working")
            return current
        snapshot = {
            "plan": copy.deepcopy(task.plan_json or {}),
            "product_snapshot": copy.deepcopy(task.product_snapshot_json or {}),
            "copy": copy.deepcopy(task.copy_json or (task.plan_json or {}).get("copy") or {}),
            "workflow_version": 2,
        }
        revision = TaskRevision(
            revision_id=generate_prefixed_id("opv_rev"),
            task_id=task.task_id,
            revision_no=len(self._repository.list_task_revisions(task.task_id)) + 1,
            plan_snapshot_json=snapshot,
            input_snapshot_hash=canonical_hash(snapshot),
            asset_manifest_json={"schema_version": "opv-selection-manifest-v1", "candidates": {}, "selected": {}},
            selection_hash=canonical_hash({"candidates": {}, "selected": {}}),
            created_by=operator,
        )
        self._repository.create_revision_and_activate(
            revision, expected_task_row_version=int(getattr(task, "row_version", 1) or 1)
        )
        return self._repository.get_task_revision(revision.revision_id) or revision

    @staticmethod
    def _next_manifest(
        revision: TaskRevision, *, candidate: Optional[Mapping[str, Any]] = None,
        select_key: Optional[str] = None, select_asset_id: Optional[str] = None,
        invalidate: Iterable[str] = (),
    ) -> Dict[str, Any]:
        manifest = copy.deepcopy(revision.asset_manifest_json or {})
        manifest.setdefault("schema_version", "opv-selection-manifest-v1")
        candidates = manifest.setdefault("candidates", {})
        selected = manifest.setdefault("selected", {})
        if candidate is not None:
            asset_id = str(candidate.get("asset_id") or "")
            if not asset_id:
                raise RevisionSelectionError("candidate asset_id is required")
            candidates[asset_id] = dict(candidate)
        if select_key is not None:
            if not select_asset_id or select_asset_id not in candidates:
                raise RevisionSelectionError("selected asset must be a known candidate")
            selected[str(select_key)] = str(select_asset_id)
        for key in invalidate:
            selected.pop(str(key), None)
        return manifest

    def register_candidate(
        self, revision: TaskRevision, candidate: Mapping[str, Any]
    ) -> TaskRevision:
        required = {"asset_id", "asset_type", "sha256", "path"}
        missing = required - set(candidate)
        if missing:
            raise RevisionSelectionError("candidate missing " + ", ".join(sorted(missing)))
        manifest = self._next_manifest(revision, candidate=candidate)
        return self._repository.update_revision_manifest(
            revision.revision_id,
            expected_lock_version=revision.lock_version,
            asset_manifest_json=manifest,
            selection_hash=canonical_hash(manifest.get("selected") or {}),
        )

    def select(
        self, revision: TaskRevision, *, selection_key: str, asset_id: str,
        invalidate: Sequence[str] = (),
    ) -> TaskRevision:
        manifest = self._next_manifest(
            revision, select_key=selection_key, select_asset_id=asset_id,
            invalidate=invalidate,
        )
        return self._repository.update_revision_manifest(
            revision.revision_id,
            expected_lock_version=revision.lock_version,
            asset_manifest_json=manifest,
            selection_hash=canonical_hash(manifest.get("selected") or {}),
        )


class RevisionAssetResolver:
    """Resolve only explicitly selected revision assets; never fall back to latest."""

    @staticmethod
    def candidate(revision: TaskRevision, asset_id: str) -> Dict[str, Any]:
        item = dict((revision.asset_manifest_json.get("candidates") or {}).get(asset_id) or {})
        if not item:
            raise RevisionSelectionError(f"asset {asset_id} is not in revision {revision.revision_id}")
        if not str(item.get("sha256") or "") or not str(item.get("path") or ""):
            raise RevisionSelectionError(f"asset {asset_id} lacks immutable path/hash")
        return item

    @classmethod
    def selected(cls, revision: TaskRevision, key: str) -> Dict[str, Any]:
        asset_id = str((revision.asset_manifest_json.get("selected") or {}).get(key) or "")
        if not asset_id:
            raise RevisionSelectionError(
                f"revision {revision.revision_id} has no selected asset for {key!r}"
            )
        return cls.candidate(revision, asset_id)

    @classmethod
    def fingerprint(cls, revision: TaskRevision, keys: Sequence[str], extra: Any = None) -> str:
        assets = [cls.selected(revision, key) for key in keys]
        compact = [
            {
                "asset_id": item["asset_id"], "sha256": item["sha256"],
                "parents": item.get("parent_asset_ids") or [],
                "params": item.get("parameters_hash") or "",
            }
            for item in assets
        ]
        return canonical_hash({"revision": revision.revision_id, "assets": compact, "extra": extra or {}})


@dataclass
class AnchorReviewResult:
    revision: TaskRevision
    review: QualityReview
    advanced: bool


class AnchorReviewService:
    """Persist an anchor decision and only then allow V2 group expansion."""

    def __init__(self, repository):
        self._repository = repository
        self._revisions = RevisionService(repository)

    def record(
        self, task_id: str, *, asset_id: str, decision: str,
        dimensions: Mapping[str, Any], reason_codes: Sequence[str] = (),
        evidence: Optional[Mapping[str, Any]] = None, reviewer_type: str = "model",
        reviewer: str = "system", expected_lock_version: Optional[int] = None,
    ) -> AnchorReviewResult:
        if decision not in VALID_DECISIONS:
            raise WorkflowV2Error(f"unknown review decision {decision!r}")
        reviewer_type = review_actor(reviewer_type, reviewer)
        if reviewer_type == "technical":
            from services.release_gate import TECHNICAL_REVIEWER, technical_evidence_valid
            if reviewer != TECHNICAL_REVIEWER or not technical_evidence_valid(evidence, "anchor"):
                raise WorkflowV2Error("invalid technical anchor evidence")
        task = self._repository.get_task(task_id)
        if task is None or not workflow_v2_enabled(task):
            raise WorkflowV2Error("anchor review requires a Workflow V2 task")
        if task.task_status != TASK_ANCHOR_REVIEW:
            raise WorkflowV2Error(f"task {task_id} is not awaiting anchor review")
        revision = self._revisions.ensure_working(task, operator=reviewer)
        if expected_lock_version is not None and revision.lock_version != expected_lock_version:
            raise RevisionSelectionError("anchor selection changed before review")
        asset = RevisionAssetResolver.candidate(revision, asset_id)
        profile_id, profile_version = _quality_profile(revision.plan_snapshot_json.get("plan") or {})
        fingerprint = canonical_hash({
            "anchor": {"asset_id": asset_id, "sha256": asset["sha256"]},
            "inputs": revision.input_snapshot_hash,
            "profile": [profile_id, profile_version],
        })
        review = QualityReview(
            review_id=generate_prefixed_id("opv_qr"), revision_id=revision.revision_id,
            scope="anchor", target_id=asset_id, input_fingerprint=fingerprint,
            quality_profile_id=profile_id, quality_profile_version=profile_version,
            decision=decision, reviewer_type=reviewer_type,
            dimensions_json=dict(dimensions), reason_codes_json=[str(v) for v in reason_codes],
            evidence_json=dict(evidence or {}), reviewer=reviewer,
        )
        previous = [r for r in (self._repository.list_quality_reviews(revision.revision_id, scope="anchor", target_id=asset_id) if reviewer_type == "technical" else [])
                    if reviewer_type == "technical" and r.reviewer_type == "technical"
                    and r.input_fingerprint == fingerprint and r.decision == decision]
        if previous:
            review = previous[-1]
        else:
            self._repository.insert_quality_review(review)
        if decision not in {"passed", "waived"} or reviewer_type == "assistant":
            return AnchorReviewResult(revision=revision, review=review, advanced=False)
        revision = self._revisions.select(
            revision, selection_key="anchor", asset_id=asset_id,
            invalidate=("board", "group", "render"),
        )
        from domain.contracts import is_multi_look_plan
        if not is_multi_look_plan(revision.plan_snapshot_json.get("plan") or {}):
            revision = self._revisions.select(
                revision, selection_key=f"shot:{int(asset.get('slot_index') or 0)}", asset_id=asset_id
            )
        self._repository.transition_task(
            task_id, TASK_ANCHOR_REVIEW, TASK_IMAGE_GENERATING
        )
        return AnchorReviewResult(revision=revision, review=review, advanced=True)

    def review_with_adapter(
        self, task_id: str, *, asset_id: str, adapter, reviewer: str = "vision"
    ) -> AnchorReviewResult:
        """Run a scoped vision review over one V2 anchor candidate.

        The existing provider adapter already receives product/persona images;
        this method supplies a smaller contract than a five-shot group review
        and records the result against the exact candidate fingerprint.
        """
        from services.visual_qa import (
            assessment_v2_enabled, normalize_decision, review_decision_policy,
            review_shot_context, select_quality_rules,
        )

        task = self._repository.get_task(task_id)
        if task is None:
            raise WorkflowV2Error(f"task {task_id} not found")
        revision = self._revisions.ensure_working(task, operator=reviewer)
        asset = RevisionAssetResolver.candidate(revision, asset_id)
        plan = revision.plan_snapshot_json.get("plan") or {}
        quality_contract = plan.get("quality_contract") or {}
        policy = review_decision_policy(quality_contract)
        context = review_shot_context(plan, int(asset.get("slot_index") or 0), shot_kind="generated_photo")
        all_rules = dict(quality_contract.get("dimensions") or {})
        rules = {
            key: dict(rule or {}) for key, rule in all_rules.items()
            if "anchor" in list((rule or {}).get("scope") or [])
        }
        if assessment_v2_enabled(quality_contract):
            _, rules = select_quality_rules(quality_contract, "anchor", shots=[context])
        if not rules:
            rules = {
                "product_fidelity": {"min_score": 80, "hard_gate": True},
                "outfit_quality": {"min_score": 78, "hard_gate": True},
                "persona_consistency": {"min_score": 80, "hard_gate": True},
                "anchor_naturalness": {"min_score": 78, "hard_gate": True},
                "anchor_composition": {"min_score": 78, "hard_gate": True},
            }
        raw = adapter.review({
            "scope": "anchor",
            "task_id": task_id,
            "slot_index": int(asset.get("slot_index") or 0),
            "image_paths": [asset["path"]],
            "product": ((revision.plan_snapshot_json.get("product_snapshot") or {}).get("product", {})
                        if assessment_v2_enabled(quality_contract) else (task.product_snapshot_json or {}).get("product", {})),
            "plan": plan,
            "required_dimensions": list(rules),
            "dimension_rules": rules,
            "decision_policy": policy if quality_contract else "independent_dimensions",
            **({"current_shot": context, "shots_being_reviewed": [context]}
               if assessment_v2_enabled(quality_contract) else {}),
            "hard_fail_keys": [key for key, rule in rules.items() if rule.get("hard_gate")],
        })
        decision = normalize_decision(raw, tuple(rules), dimension_rules=rules, decision_policy=policy)
        return self.record(
            task_id,
            asset_id=asset_id,
            decision="passed" if decision.passed else "failed",
            dimensions=decision.dimensions,
            reason_codes=decision.reason_codes,
            evidence={
                "notes": decision.notes,
                "provider": decision.provider,
                "model": decision.model,
                "next_actions": decision.next_actions,
                "warnings": decision.warnings,
            },
            reviewer_type="model",
            reviewer=reviewer,
            expected_lock_version=revision.lock_version,
        )


class ScopedReviewService:
    """Append-only group/render decisions tied to revision selection fingerprints."""

    def __init__(self, repository):
        self._repository = repository

    @staticmethod
    def selection_keys(
        scope: str, task: ContentTask, *, package: Any = None
    ) -> list[str]:
        if scope in {"group", "render"}:
            count = len((task.plan_json or {}).get("shots") or [])
            return [f"shot:{index}" for index in range(1, count + 1)]
        if scope == "photo_package":
            manifest = getattr(package, "photo_manifest_json", None) or {}
            slides = list(manifest.get("slides") or [])
            if not slides:
                raise WorkflowV2Error("photo_package has no final slides")
            indexes = [int(item.get("index") or 0) for item in slides]
            if indexes != list(range(1, len(slides) + 1)):
                raise WorkflowV2Error("photo_package slide indexes must be consecutive")
            return [f"slide:{index}" for index in indexes]
        raise WorkflowV2Error(f"scope {scope!r} requires explicit selection keys")

    def fingerprint(
        self, task: ContentTask, revision: TaskRevision, *, scope: str,
        extra: Any = None, package: Any = None,
    ) -> str:
        return RevisionAssetResolver.fingerprint(
            revision, self.selection_keys(scope, task, package=package), extra=extra or {
                "quality_contract": (revision.plan_snapshot_json.get("plan") or {}).get("quality_contract") or {},
            }
        )

    def record(
        self, task_id: str, *, scope: str, target_id: str, decision: str,
        dimensions: Mapping[str, Any], reason_codes: Sequence[str] = (),
        evidence: Optional[Mapping[str, Any]] = None, reviewer_type: str = "human",
        reviewer: str = "operator", extra_fingerprint_input: Any = None,
    ) -> QualityReview:
        if scope not in {"group", "photo_package", "render"} or decision not in VALID_DECISIONS:
            raise WorkflowV2Error(
                "only group/photo_package/render passed, failed or waived reviews are supported"
            )
        reviewer_type = review_actor(reviewer_type, reviewer)
        if reviewer_type == "technical":
            from services.release_gate import TECHNICAL_REVIEWER, technical_evidence_valid
            if reviewer != TECHNICAL_REVIEWER or not technical_evidence_valid(evidence, scope):
                raise WorkflowV2Error("invalid scoped technical evidence")
        task = self._repository.get_task(task_id)
        if task is None or not workflow_v2_enabled(task) or not task.active_revision_id:
            raise WorkflowV2Error("scoped review requires an active Workflow V2 revision")
        revision = self._repository.get_task_revision(task.active_revision_id)
        if revision is None:
            raise WorkflowV2Error("active revision is missing")
        package = None
        if scope == "photo_package":
            package = self._repository.get_content_package(target_id)
            if package is None or package.task_id != task_id:
                raise WorkflowV2Error("photo_package review target is missing or belongs to another task")
        profile_id, profile_version = _quality_profile(revision.plan_snapshot_json.get("plan") or {})
        review = QualityReview(
            review_id=generate_prefixed_id("opv_qr"), revision_id=revision.revision_id,
            scope=scope, target_id=target_id,
            input_fingerprint=self.fingerprint(
                task, revision, scope=scope, extra=extra_fingerprint_input,
                package=package,
            ),
            quality_profile_id=profile_id, quality_profile_version=profile_version,
            decision=decision, reviewer_type=reviewer_type,
            dimensions_json=dict(dimensions), reason_codes_json=[str(v) for v in reason_codes],
            evidence_json=dict(evidence or {}), reviewer=reviewer,
        )
        self._repository.insert_quality_review(review)
        return review

    def require_passed(
        self, task: ContentTask, *, scope: str, target_id: str,
        extra_fingerprint_input: Any = None,
    ) -> QualityReview:
        if not task.active_revision_id:
            raise WorkflowV2Error("Workflow V2 task has no active revision")
        revision = self._repository.get_task_revision(task.active_revision_id)
        if revision is None:
            raise WorkflowV2Error("active revision is missing")
        package = None
        if scope == "photo_package":
            package = self._repository.get_content_package(target_id)
            if package is None or package.task_id != task.task_id:
                raise WorkflowV2Error("photo_package review target is missing or belongs to another task")
        expected = self.fingerprint(
            task, revision, scope=scope, extra=extra_fingerprint_input,
            package=package,
        )
        matches = self._repository.list_quality_reviews(
            revision.revision_id, scope=scope, target_id=target_id
        )
        review = next((item for item in reversed(matches) if item.input_fingerprint == expected), None)
        from services.release_gate import trusted_review
        if review is None or review.decision not in {"passed", "waived"} or not trusted_review(review):
            raise WorkflowV2Error(
                f"Workflow V2 {scope} review is missing or stale for {target_id}"
            )
        return review


def photo_package_fingerprint_input(package: Any) -> Dict[str, Any]:
    """Return the non-file portion that a final photo review must freeze."""
    manifest = dict(getattr(package, "photo_manifest_json", None) or {})
    slides = []
    for item in manifest.get("slides") or []:
        slides.append({
            "index": int(item.get("index") or 0),
            "asset_id": str(item.get("asset_id") or ""),
            "sha256": str(item.get("sha256") or ""),
            "width": int(item.get("width") or 0),
            "height": int(item.get("height") or 0),
            "mime_type": str(item.get("mime_type") or ""),
        })
    result = {
        "schema_version": str(manifest.get("schema_version") or ""),
        "media_kind": str(manifest.get("media_kind") or ""),
        "content_package_id": str(package.content_package_id),
        "template_id": str(manifest.get("template_id") or ""),
        "template_version": int(manifest.get("template_version") or 0),
        "cover_index": int(manifest.get("cover_index") or 0),
        "copy": copy.deepcopy(manifest.get("copy") or {}),
        "slides": slides,
    }
    binding_version = int(manifest.get("theme_copy_binding_version") or 0)
    if binding_version:
        result["theme_copy_binding_version"] = binding_version
        result["theme_brief"] = copy.deepcopy(manifest.get("theme_brief") or {})
    return result


class PhotoPackageReviewService:
    """Review and atomically release the actual, text-baked photo carousel."""

    def __init__(self, repository):
        self._repository = repository
        self._reviews = ScopedReviewService(repository)

    def record(
        self, task_id: str, *, decision: str, dimensions: Mapping[str, Any],
        reason_codes: Sequence[str] = (), evidence: Optional[Mapping[str, Any]] = None,
        reviewer_type: str = "human", reviewer: str = "operator",
    ) -> QualityReview:
        task = self._repository.get_task(task_id)
        if task is None or not workflow_v2_enabled(task) or not task.active_revision_id:
            raise WorkflowV2Error("photo package review requires an active Workflow V2 task")
        if decision in {"passed", "waived"}:
            from services.release_gate import require_photo_content_allowed
            require_photo_content_allowed(self._repository, task)
        if str(getattr(task, "media_kind", "video") or "video") != "native_photo":
            raise WorkflowV2Error("photo package review requires media_kind=native_photo")
        package_id = str(getattr(task, "content_package_id", "") or "")
        package = self._repository.get_content_package(package_id) if package_id else None
        if package is None or package.task_id != task_id:
            raise WorkflowV2Error("photo package is missing or belongs to another task")
        manifest = dict(getattr(package, "photo_manifest_json", None) or {})
        slides = list(manifest.get("slides") or [])
        if manifest.get("schema_version") != "opv-photo-package-v1" or not slides:
            raise WorkflowV2Error("photo package manifest is missing or invalid")
        if task.task_status != "photo_packaging":
            raise WorkflowV2Error("photo package review requires photo_packaging status")
        revision = self._repository.get_task_revision(task.active_revision_id)
        if revision is None or revision.revision_status != "working":
            raise WorkflowV2Error("photo package revision is not working")
        from services.release_gate import file_hash
        for index, item in enumerate(slides, 1):
            asset = RevisionAssetResolver.selected(revision, f"slide:{index}")
            if (int(item.get("index") or 0) != index
                    or str(item.get("asset_id") or "") != asset["asset_id"]
                    or str(item.get("sha256") or "") != asset["sha256"]
                    or file_hash(asset["path"]) != asset["sha256"]):
                raise WorkflowV2Error(f"final photo slide:{index} changed before review")
        extra = photo_package_fingerprint_input(package)
        review = self._reviews.record(
            task_id, scope="photo_package", target_id=package_id,
            decision=decision, dimensions=dimensions, reason_codes=reason_codes,
            evidence=evidence, reviewer_type=reviewer_type, reviewer=reviewer,
            extra_fingerprint_input=extra,
        )
        from services.release_gate import trusted_photo_content_review
        if trusted_photo_content_review(review):
            self._repository.release_revision(
                task_id, revision.revision_id,
                expected_task_row_version=int(getattr(task, "row_version", 1) or 1),
                review_id=review.review_id, review_scope="photo_package",
                review_target_id=package_id,
                expected_review_fingerprint=review.input_fingerprint,
                expected_selection_hash=revision.selection_hash,
                expected_input_snapshot_hash=revision.input_snapshot_hash,
            )
        elif decision in {"passed", "waived"}:
            # Keep technical/model evidence append-only, but never turn it into
            # permission to publish a native-photo package.
            return review
        return review


class RenderReviewService:
    """Release a V2 render only after a review of the actual video output."""

    def __init__(self, repository):
        self._repository = repository

    def record(
        self, task_id: str, *, render_id: str, decision: str,
        dimensions: Mapping[str, Any], reason_codes: Sequence[str] = (),
        evidence: Optional[Mapping[str, Any]] = None, reviewer_type: str = "human",
        reviewer: str = "operator",
    ) -> QualityReview:
        if decision not in VALID_DECISIONS:
            raise WorkflowV2Error(f"unknown render decision {decision!r}")
        reviewer_type = review_actor(reviewer_type, reviewer)
        task = self._repository.get_task(task_id)
        render = self._repository.get_render(render_id)
        if reviewer_type == "technical":
            from services.release_gate import TECHNICAL_REVIEWER, technical_evidence_valid
            if reviewer != TECHNICAL_REVIEWER or not technical_evidence_valid(evidence, "render"):
                raise WorkflowV2Error("invalid technical render evidence")
        if task is None or render is None or not workflow_v2_enabled(task):
            raise WorkflowV2Error("render review requires a V2 task and render")
        if render.origin_revision_id != task.active_revision_id or not render.input_fingerprint:
            raise WorkflowV2Error("render does not belong to the active frozen revision")
        if render.qc_status != "passed" or not render.output_sha256:
            raise WorkflowV2Error("render media QC is not passed")
        revision = self._repository.get_task_revision(task.active_revision_id)
        if revision is None:
            raise WorkflowV2Error("active revision is missing")
        matches = [r for r in self._repository.list_quality_reviews(revision.revision_id, scope="render", target_id=render_id)
                   if r.input_fingerprint == render.input_fingerprint
                   and (r.evidence_json or {}).get("render_sha256") == render.output_sha256]
        from services.release_gate import expected_render_fingerprint, file_hash
        if (canonical_hash(revision.plan_snapshot_json) != revision.input_snapshot_hash
                or canonical_hash(revision.asset_manifest_json.get("selected") or {}) != revision.selection_hash
                or expected_render_fingerprint(revision) != render.input_fingerprint):
            raise WorkflowV2Error("render does not match the frozen selected input set")
        if file_hash(render.output_url) != render.output_sha256:
            raise WorkflowV2Error("render file changed before terminal review")
        if revision.revision_status == "released":
            if matches and matches[-1].decision == decision and task.released_revision_id == revision.revision_id and task.selected_render_id == render_id:
                return matches[-1]
            raise WorkflowV2Error("released review is immutable; create a rework revision")
        if task.task_status != "video_review" or task.selected_render_id != render_id:
            raise WorkflowV2Error("render review requires the selected video at video_review")
        profile_id, profile_version = _quality_profile(revision.plan_snapshot_json.get("plan") or {})
        review = QualityReview(
            review_id=generate_prefixed_id("opv_qr"), revision_id=revision.revision_id,
            scope="render", target_id=render_id, input_fingerprint=render.input_fingerprint,
            quality_profile_id=profile_id, quality_profile_version=profile_version,
            decision=decision, reviewer_type=reviewer_type,
            dimensions_json=dict(dimensions), reason_codes_json=[str(v) for v in reason_codes],
            evidence_json={**dict(evidence or {}), "render_sha256": render.output_sha256},
            reviewer=reviewer,
        )
        self._repository.insert_quality_review(review)
        if decision in {"passed", "waived"} and reviewer_type != "assistant":
            self._repository.release_revision(
                task_id, revision.revision_id,
                expected_task_row_version=int(getattr(task, "row_version", 1) or 1),
                render_id=render_id, review_id=review.review_id,
                expected_selection_hash=revision.selection_hash,
                expected_input_snapshot_hash=revision.input_snapshot_hash,
            )
        return review


class ReworkService:
    """Create an explicit child revision; no released material is overwritten."""

    VALID_SCOPES = frozenset({"plan", "anchor", "shot", "cutout", "board", "render", "timing"})

    def __init__(self, repository, publication_guard=None):
        self._repository = repository
        if publication_guard is None:
            from services.release_gate import assert_main_queue_rework_allowed
            publication_guard = assert_main_queue_rework_allowed
        self._publication_guard = publication_guard

    def begin(
        self, task_id: str, *, expected_revision_id: str, expected_lock_version: int,
        scope: str, reason: str, idempotency_key: str, operator: str = "operator",
        slot_indexes: Sequence[int] = (), timing_ms: Mapping[int, int] | None = None,
    ) -> TaskRevision:
        if scope not in self.VALID_SCOPES:
            raise WorkflowV2Error(f"unsupported rework scope {scope!r}")
        if not idempotency_key.strip() or not reason.strip():
            raise WorkflowV2Error("rework requires a reason and idempotency key")
        task = self._repository.get_task(task_id)
        if task is None:
            raise WorkflowV2Error("rework task is missing")
        self._publication_guard(task_id)
        for existing in self._repository.list_task_revisions(task_id):
            spec = existing.rework_spec_json or {}
            if spec.get("idempotency_key") == idempotency_key:
                if existing.parent_revision_id != expected_revision_id or spec.get("scope") != scope:
                    raise RevisionSelectionError("rework key was already used for another request")
                if sorted(spec.get("slot_indexes") or []) != sorted({int(value) for value in slot_indexes}):
                    raise RevisionSelectionError("rework key was already used for different slots")
                if scope == "render" and task.active_revision_id == existing.revision_id:
                    parent = self._repository.get_task_revision(existing.parent_revision_id)
                    source = self._render_group_source(task, parent, existing.plan_snapshot_json, existing.asset_manifest_json)
                    if source is not None:
                        self._record_reused_group(task, existing, source)
                return existing
        if task.active_revision_id != expected_revision_id:
            raise RevisionSelectionError("active revision changed before rework")
        if task.task_status not in {"anchor_review", "image_review", "video_review", "rework_pending"}:
            raise WorkflowV2Error("rework is only allowed from a review stage")
        publish_reader = getattr(self._repository, "list_publish_records_by_task", None)
        if callable(publish_reader) and publish_reader(task_id):
            raise WorkflowV2Error("task has a publication record; resolve scheduling before rework")
        parent = self._repository.get_task_revision(expected_revision_id)
        if parent is None or parent.lock_version != expected_lock_version:
            raise RevisionSelectionError("revision changed before rework")
        inherited = copy.deepcopy(parent.asset_manifest_json or {})
        selected = inherited.setdefault("selected", {})
        plan_snapshot = copy.deepcopy(parent.plan_snapshot_json)
        plan = plan_snapshot.get("plan") or {}
        slots = sorted({int(value) for value in slot_indexes})
        valid_slots = {int(shot["slot_index"]) for shot in plan.get("shots") or []}
        if scope == "timing":
            requested = {int(key): int(value) for key, value in (timing_ms or {}).items()}
            if set(requested) != valid_slots or any(value < 500 for value in requested.values()):
                raise WorkflowV2Error("timing rework requires a duration for every slot of at least 500ms")
            for shot in plan.get("shots") or []:
                shot["duration_ms"] = requested[int(shot["slot_index"])]
        if scope == "shot" and (not slots or not set(slots) <= valid_slots):
            raise WorkflowV2Error("shot rework requires valid explicit slot indexes")
        anchor_slot = int(plan.get("anchor_slot") or 1)
        if scope == "shot" and anchor_slot in slots:
            raise WorkflowV2Error("use anchor rework when changing the continuity anchor")
        board_slots = [int(s["slot_index"]) for s in plan.get("shots") or [] if s.get("shot_kind") == "composite_board"]
        invalid = set({
            "plan": tuple(selected),
            "anchor": tuple(selected),
            "shot": ("group", "render", *[f"shot:{slot}" for slot in slots]),
            "cutout": ("board", "group", "render", *[f"shot:{slot}" for slot in board_slots]),
            "board": ("board", "group", "render", *[f"shot:{slot}" for slot in board_slots]),
            "render": ("render",),
            "timing": ("group", "render"),
        }[scope])
        if scope == "cutout":
            invalid.update(key for key in selected if key.startswith("cutout:"))
        for key in invalid:
            selected.pop(key, None)
        if scope in {"plan", "anchor", "cutout"}:
            plan.pop("decomposition_assets", None)
            for shot in plan.get("shots") or []:
                (shot.get("board_spec") or {}).pop("decomposition_assets", None)
        # Do not carry UI command tokens or failed automatic-review attempts
        # into a new creative revision.
        inherited.pop("workbench_review", None)
        inherited.pop("auto_review_attempts", None)
        reused_group = (self._render_group_source(task, parent, plan_snapshot, inherited)
                        if scope == "render" else None)
        resume_stage = "hero_generating" if scope in {"plan", "anchor"} else (
            "rendering" if scope == "render" and reused_group is not None else "image_generating"
        )
        if scope == "timing":
            resume_stage = "image_review"
        revision = TaskRevision(
            revision_id=generate_prefixed_id("opv_rev"), task_id=task_id,
            revision_no=len(self._repository.list_task_revisions(task_id)) + 1,
            parent_revision_id=parent.revision_id,
            plan_snapshot_json=plan_snapshot,
            input_snapshot_hash=canonical_hash(plan_snapshot),
            asset_manifest_json=inherited,
            selection_hash=canonical_hash(selected),
            rework_spec_json={
                "scope": scope, "reason": reason, "idempotency_key": idempotency_key,
                "invalidated_selection_keys": sorted(invalid),
                "slot_indexes": slots, "timing_ms": dict(timing_ms or {}), "resume_stage": resume_stage,
            },
            created_by=operator,
        )
        self._repository.create_revision_and_activate(
            revision, expected_task_row_version=int(getattr(task, "row_version", 1) or 1),
            transition_to="rework_pending",
        )
        if scope == "timing":
            from services.content_package import advance_package_for_task
            from domain.statuses import PACKAGE_QA_REVIEW
            advance_package_for_task(self._repository, task_id, PACKAGE_QA_REVIEW)
        revision = self._repository.get_task_revision(revision.revision_id) or revision
        if reused_group is not None:
            self._record_reused_group(self._repository.get_task(task_id), revision, reused_group)
        return revision

    def _render_group_source(self, task, parent, plan_snapshot, manifest):
        """Only unchanged frozen inputs/selection/bytes may reuse a content review."""
        if parent is None or canonical_hash(parent.plan_snapshot_json) != parent.input_snapshot_hash:
            return None
        if canonical_hash(plan_snapshot) != parent.input_snapshot_hash:
            return None
        from domain.contracts import ContractViolationError, expected_plan_shot_count
        plan = plan_snapshot.get("plan") or {}
        try:
            count = expected_plan_shot_count(plan)
        except ContractViolationError:
            return None
        if len(plan.get("shots") or []) != count:
            return None
        original = parent.asset_manifest_json.get("selected") or {}
        selected = manifest.get("selected") or {}
        if canonical_hash(original) != parent.selection_hash:
            return None
        # The render selection alone is expected to disappear during render
        # rework. Every content selection (not merely its local path) must match.
        if {key: value for key, value in original.items() if key != "render"} != selected:
            return None
        parent_task = replace(task, active_revision_id=parent.revision_id,
                              plan_json=parent.plan_snapshot_json.get("plan") or {})
        try:
            source = ScopedReviewService(self._repository).require_passed(parent_task, scope="group", target_id="group")
        except WorkflowV2Error:
            return None
        from services.media_qc import MediaQcError, require_shot_media_qc
        by_id = {shot.shot_id: shot for shot in self._repository.list_shots(task.task_id)}
        for index in range(1, len(parent_task.plan_json.get("shots") or []) + 1):
            key = f"shot:{index}"
            asset_id = selected.get(key)
            old_asset = (parent.asset_manifest_json.get("candidates") or {}).get(asset_id)
            new_asset = (manifest.get("candidates") or {}).get(asset_id)
            shot = by_id.get(asset_id)
            if (not old_asset or new_asset != old_asset or shot is None
                    or shot.slot_index != index or shot.image_sha256 != old_asset.get("sha256")):
                return None
            try:
                require_shot_media_qc(shot)
            except (MediaQcError, OSError):
                return None
        return source

    def _record_reused_group(self, task, revision, source):
        service = ScopedReviewService(self._repository)
        expected = service.fingerprint(task, revision, scope="group")
        existing = self._repository.list_quality_reviews(revision.revision_id, scope="group", target_id="group")
        if any(review.input_fingerprint == expected for review in existing):
            return  # recovery is idempotent and never overwrites a newer decision
        service.record(task.task_id, scope="group", target_id="group", decision=source.decision,
            dimensions=source.dimensions_json, reason_codes=source.reason_codes_json,
            evidence={**(source.evidence_json or {}), "reused_review_id": source.review_id,
                      "source_revision_id": source.revision_id, "reuse_reason": "render_only_unchanged_frozen_content",
                      "model_called": False}, reviewer_type=source.reviewer_type, reviewer=source.reviewer)
