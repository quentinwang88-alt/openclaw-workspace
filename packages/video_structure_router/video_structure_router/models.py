"""Typed contracts used by the video structure routing layer."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class RouteRequest:
    request_id: str
    consumer_flow: str
    product_code: str
    target_country: str
    category: str
    product_type: str
    direction_count: int = 4
    duration_seconds: float = 15.0
    random_seed: int = 0
    capabilities: Dict[str, Any] = field(default_factory=dict)
    product_context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StructureCandidate:
    candidate_key: str
    source_kind: str
    source_run_id: str
    cluster_id: int
    cluster_version: str
    prototype_id: str
    cluster_status: str
    evidence_tier: str
    macro_structure_name: str
    structure_description: str
    beat_sequence: List[str]
    required_beats: List[str]
    optional_beats: List[str]
    content_carrier: str
    continuity_mode: str
    cut_density: str
    visual_hook_type: str
    proof_mechanisms: List[str]
    ending_pattern: str
    shot_count_min: Optional[int]
    shot_count_max: Optional[int]
    shot_count_median: Optional[float]
    duration_median: Optional[float]
    member_count: int
    distinct_videos: int
    cohesion: float
    extraction_confidence: float
    categories: List[str]
    countries: List[str]
    variation_axes: List[str]
    representative_cases: List[Dict[str, Any]]
    extractor_versions: List[str]
    feature_schema_versions: List[str]
    compatibility_matrix_versions: List[str]
    profile_types: List[str]
    independence_levels: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def macro_family_key(self) -> str:
        return ">".join(self.beat_sequence) if self.beat_sequence else "UNAVAILABLE"

    @property
    def shot_count_band(self) -> str:
        if self.shot_count_median is None:
            return "UNAVAILABLE"
        if self.shot_count_median <= 1.5:
            return "SINGLE"
        if self.shot_count_median <= 3.5:
            return "LOW"
        if self.shot_count_median <= 6.5:
            return "MEDIUM"
        return "HIGH"

    @property
    def visual_archetype(self) -> Dict[str, str]:
        return {
            "content_carrier": self.content_carrier or "UNAVAILABLE",
            "continuity_mode": self.continuity_mode or "UNAVAILABLE",
            "shot_count_band": self.shot_count_band,
            "cut_density": self.cut_density or "UNAVAILABLE",
            "visual_hook_type": self.visual_hook_type or "UNAVAILABLE",
        }

    @property
    def visual_archetype_key(self) -> str:
        values = self.visual_archetype
        return "|".join(
            f"{key}={values[key]}"
            for key in (
                "content_carrier",
                "continuity_mode",
                "shot_count_band",
                "cut_density",
                "visual_hook_type",
            )
        )

    def snapshot_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["macro_family_key"] = self.macro_family_key
        value["visual_archetype_key"] = self.visual_archetype_key
        return value


@dataclass
class DirectionAssignment:
    direction_assignment_id: str
    selection_run_id: str
    direction_index: int
    output_slot: str
    direction_role: str
    selection_score: float
    candidate: StructureCandidate
    structure_contract: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "direction_assignment_id": self.direction_assignment_id,
            "selection_run_id": self.selection_run_id,
            "direction_index": self.direction_index,
            "output_slot": self.output_slot,
            "direction_role": self.direction_role,
            "selection_score": self.selection_score,
            "candidate_key": self.candidate.candidate_key,
            "source_kind": self.candidate.source_kind,
            "source_run_id": self.candidate.source_run_id,
            "cluster_id": self.candidate.cluster_id,
            "cluster_version": self.candidate.cluster_version,
            "prototype_id": self.candidate.prototype_id,
            "evidence_tier": self.candidate.evidence_tier,
            "macro_family_key": self.candidate.macro_family_key,
            "visual_archetype_key": self.candidate.visual_archetype_key,
            "structure_contract": self.structure_contract,
        }


@dataclass
class SelectionResult:
    selection_run_id: str
    request: RouteRequest
    policy_version: str
    selection_status: str
    degraded_reasons: List[str]
    input_snapshot: Dict[str, Any]
    data_snapshot_hash: str
    assignments: List[DirectionAssignment]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "selection_run_id": self.selection_run_id,
            "request": self.request.to_dict(),
            "policy_version": self.policy_version,
            "selection_status": self.selection_status,
            "degraded_reasons": list(self.degraded_reasons),
            "input_snapshot": self.input_snapshot,
            "data_snapshot_hash": self.data_snapshot_hash,
            "assignments": [item.to_dict() for item in self.assignments],
        }


@dataclass
class ContractValidationResult:
    valid: bool
    blocking_issues: List[str]
    warnings: List[str]
    observed: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
