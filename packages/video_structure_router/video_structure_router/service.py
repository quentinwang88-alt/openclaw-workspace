"""Public orchestration service for selecting reusable video structures."""

from __future__ import annotations

from typing import Any, Dict, Optional

from .models import DirectionAssignment, RouteRequest, SelectionResult
from .policy import (
    COMPATIBILITY_POLICY_VERSION,
    FAMILY_POLICY_VERSION,
    FEEDBACK_POLICY,
    POLICY_VERSION,
    build_structure_contract,
    select_candidates,
    stable_hash,
)
from .repository import RDSStructureRepository


class StructureRouterService:
    def __init__(self, repository: Optional[Any] = None, storage: Optional[Any] = None):
        self.repository = repository or RDSStructureRepository()
        self.storage = storage

    def select(self, request: RouteRequest) -> SelectionResult:
        candidates = self.repository.load_candidates()
        candidate_material = [candidate.snapshot_dict() for candidate in candidates]
        candidate_hash = stable_hash(candidate_material)
        input_snapshot = {
            "candidate_hash": candidate_hash,
            "candidate_count": len(candidates),
            "cluster_run_ids": sorted({candidate.source_run_id for candidate in candidates}),
            "prototype_versions": sorted(
                {
                    f"{candidate.prototype_id}:{candidate.cluster_version}"
                    for candidate in candidates
                    if candidate.prototype_id
                }
            ),
            "extractor_versions": sorted({item for candidate in candidates for item in candidate.extractor_versions}),
            "feature_schema_versions": sorted({item for candidate in candidates for item in candidate.feature_schema_versions}),
            "compatibility_matrix_versions": sorted(
                {item for candidate in candidates for item in candidate.compatibility_matrix_versions}
            ),
            "family_policy_version": FAMILY_POLICY_VERSION,
            "compatibility_policy_version": COMPATIBILITY_POLICY_VERSION,
            "evidence_mapping_version": "cluster-status-profile-type-v1",
            "feedback_policy": FEEDBACK_POLICY,
            "lineage_version": "UNAVAILABLE",
            "routing_context": {
                "allowed_carriers": list(
                    (request.capabilities or {}).get("allowed_carriers") or []
                ),
                "recent_cluster_usage": dict(
                    (request.capabilities or {}).get("recent_cluster_usage") or {}
                ),
            },
        }
        data_snapshot_hash = stable_hash(input_snapshot)
        selected, degraded, diagnostics = select_candidates(candidates, request)
        input_snapshot["selection_diagnostics"] = diagnostics

        deterministic_material = {
            "request": request.to_dict(),
            "policy_version": POLICY_VERSION,
            "data_snapshot_hash": data_snapshot_hash,
        }
        selection_run_id = "SR_" + stable_hash(deterministic_material)[:24]
        assignments = []
        for index, (candidate, score, role) in enumerate(selected, 1):
            output_slot = f"S{index}" if request.consumer_flow == "ORIGINAL_SCRIPT" and index <= 4 else f"D{index}"
            assignment_id = "SRA_" + stable_hash(
                {
                    "selection_run_id": selection_run_id,
                    "index": index,
                    "candidate_key": candidate.candidate_key,
                }
            )[:24]
            contract = build_structure_contract(
                candidate,
                selection_run_id=selection_run_id,
                direction_assignment_id=assignment_id,
                direction_index=index,
                output_slot=output_slot,
                direction_role=role,
                policy_version=POLICY_VERSION,
                data_snapshot_hash=data_snapshot_hash,
            )
            assignments.append(
                DirectionAssignment(
                    direction_assignment_id=assignment_id,
                    selection_run_id=selection_run_id,
                    direction_index=index,
                    output_slot=output_slot,
                    direction_role=role,
                    selection_score=score,
                    candidate=candidate,
                    structure_contract=contract,
                )
            )

        result = SelectionResult(
            selection_run_id=selection_run_id,
            request=request,
            policy_version=POLICY_VERSION,
            selection_status="DEGRADED_DIVERSITY" if degraded else "SELECTED",
            degraded_reasons=degraded,
            input_snapshot=input_snapshot,
            data_snapshot_hash=data_snapshot_hash,
            assignments=assignments,
        )
        if self.storage is not None:
            self.storage.persist_selection(result)
        return result
