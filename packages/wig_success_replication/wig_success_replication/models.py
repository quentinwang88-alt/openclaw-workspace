"""Strict Pydantic contracts for all model-facing V1 Lite payloads."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


NonEmpty = Annotated[str, Field(min_length=1)]
CompactText = Annotated[str, Field(min_length=1, max_length=600)]
RuleText = Annotated[str, Field(min_length=1, max_length=320)]
SummaryText = Annotated[str, Field(min_length=1, max_length=1600)]
PromptText = Annotated[str, Field(min_length=1, max_length=8000)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MotherStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    PENDING_CONFIRMATION = "pending_confirmation"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    DISABLED = "disabled"


class ProductFactStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    PENDING_CONFIRMATION = "pending_confirmation"
    CONFIRMED = "confirmed"
    NEEDS_IMAGES = "needs_images"
    FAILED = "failed"
    DISABLED = "disabled"


class BatchStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class Slot(StrictModel):
    slot_id: NonEmpty
    segment_id: NonEmpty
    time_start: float = Field(ge=0)
    time_end: float = Field(gt=0)
    source_content: CompactText
    narrative_function: CompactText
    traffic_role: Literal["hook", "reveal", "proof", "benefit", "transition", "cta", "other"]
    lock_level: Literal["hard_lock", "conditional_lock", "free_variable"]
    product_dependency: Literal["low", "medium", "high"]
    must_keep: list[RuleText] = Field(max_length=10)
    allowed_mutations: list[RuleText] = Field(max_length=10)
    forbidden_mutations: list[RuleText] = Field(max_length=10)
    same_product_rule: CompactText
    cross_product_rule: CompactText
    inference_confidence: Literal["high", "medium", "low"]
    evidence_type: Literal["observed", "script_inference", "product_fact", "human_input"]

    @model_validator(mode="after")
    def validate_timing(self) -> "Slot":
        if self.time_end <= self.time_start:
            raise ValueError("time_end must be greater than time_start")
        return self


class Segment(StrictModel):
    segment_id: NonEmpty
    duration_seconds: float = Field(gt=0)
    slots: list[Slot] = Field(min_length=1, max_length=12)
    end_frame_anchor: str = Field(max_length=600)
    next_segment_start_requirement: str = Field(max_length=600)


class SourceIntegrity(StrictModel):
    source_script_hash: NonEmpty
    source_script_must_not_be_overwritten: Literal[True] = True


class ValidationEvidence(StrictModel):
    description: CompactText
    validation_level: Literal["candidate_validated", "human_validated"] = "candidate_validated"
    causality_limit: CompactText


class CoreMechanism(StrictModel):
    summary: CompactText
    hook_engine: CompactText
    reveal_engine: CompactText
    proof_engine: CompactText
    conversion_engine: CompactText


class GlobalRules(StrictModel):
    hard_locks: list[RuleText] = Field(min_length=1, max_length=20)
    conditional_locks: list[RuleText] = Field(max_length=20)
    free_variables: list[RuleText] = Field(max_length=20)
    forbidden_mutations: list[RuleText] = Field(min_length=1, max_length=20)


class AssetContract(StrictModel):
    face_reference_rule: NonEmpty
    product_image_rule: NonEmpty
    reference_video_rule: NonEmpty
    minimum_product_images: int = Field(ge=1)


class ContinuityContract(StrictModel):
    product_identity_must_remain_stable: Literal[True] = True
    cross_segment_requirements: list[RuleText] = Field(max_length=12)


class ClaimContract(StrictModel):
    allowed_claim_sources: list[Literal["confirmed_product_fact", "human_input"]]
    forbidden_claim_sources: list[Literal["visual_imagination", "template_assumption"]]
    source_script_claim_risks: list[RuleText] = Field(max_length=12)


class LocalizationContract(StrictModel):
    language: Literal["es-MX"] = "es-MX"
    tone: CompactText
    timing_risks: list[RuleText] = Field(max_length=12)
    silent_optimization_forbidden: Literal[True] = True


class VariantPolicy(StrictModel):
    single_core_mutation: Literal[True] = True
    same_product_default_count: int = Field(default=12, ge=1)
    cross_product_default_count: int = Field(default=4, ge=1)


class MotherCorePoint(StrictModel):
    point_id: NonEmpty
    requirement: CompactText
    evidence_source: Literal["model_extracted", "legacy_derived"] = "model_extracted"


class ExecutionSummary(StrictModel):
    """Mechanism is invariant; baseline filming is an example, not a lock."""
    schema_version: Literal["1"] = "1"
    core_mechanisms: list[MotherCorePoint] = Field(min_length=1, max_length=5)
    baseline_shooting: list[CompactText] = Field(default_factory=list, max_length=8)
    variable_expression: list[CompactText] = Field(default_factory=list, max_length=10)
    provenance: Literal["model_extracted", "legacy_derived"] = "model_extracted"
    legacy_constraints: list[RuleText] = Field(default_factory=list, max_length=40)


class MotherProcessingProvenance(StrictModel):
    input_fingerprint: NonEmpty
    implementation_fingerprint: NonEmpty
    policy_version: NonEmpty


class MotherContract(StrictModel):
    schema_version: Literal["1.0"] = "1.0"
    mother_id: NonEmpty
    mother_version: int = Field(ge=1)
    market: Literal["MX"] = "MX"
    category: Literal["wig"] = "wig"
    source_integrity: SourceIntegrity
    validation_evidence: ValidationEvidence
    core_mechanism: CoreMechanism
    # Successful 15-second scripts commonly need 5-6 semantic beats. Eight
    # keeps the contract bounded without forcing unrelated beats to merge.
    segments: list[Segment] = Field(min_length=1, max_length=8)
    global_rules: GlobalRules
    asset_contract: AssetContract
    continuity_contract: ContinuityContract
    claim_contract: ClaimContract
    localization_contract: LocalizationContract
    variant_policy: VariantPolicy
    risks: list[RuleText] = Field(max_length=20)
    human_summary: SummaryText
    # Empty keeps previously persisted contracts loadable without rewriting them.
    core_points: list[MotherCorePoint] = Field(default_factory=list, max_length=5)
    execution_summary: Optional[ExecutionSummary] = None
    processing_provenance: Optional[MotherProcessingProvenance] = None

    @model_validator(mode="after")
    def core_point_count(self):
        if self.core_points and not 3 <= len(self.core_points) <= 5:
            raise ValueError("mother core_points must contain 3-5 entries when present")
        if len({point.point_id for point in self.core_points}) != len(self.core_points):
            raise ValueError("mother core point IDs must be unique")
        return self


class MotherReviewIssue(StrictModel):
    code: NonEmpty
    severity: Literal["warning", "blocking"]
    summary: CompactText
    correction_applied: bool


class MotherReview(StrictModel):
    schema_version: Literal["1.0"] = "1.0"
    final_contract: MotherContract
    issues: list[MotherReviewIssue] = Field(max_length=20)
    human_summary: SummaryText
    # Historical persisted reviews were actually run. New default production
    # explicitly writes not_run rather than fabricating a successful review.
    review_status: Literal["completed", "not_run"] = "completed"


class Appearance(StrictModel):
    length: Literal["short", "medium", "long", "unknown"]
    texture: Literal["straight", "slight_wave", "wave", "curly", "unknown"]
    color: NonEmpty
    bangs: Literal["none", "straight_bangs", "air_bangs", "curtain_bangs", "other", "unknown"]
    layers: Literal["none", "normal", "high", "unknown"]
    face_framing: Literal["yes", "no", "unknown"]


class ProductFactCard(StrictModel):
    schema_version: Literal["1.0"] = "1.0"
    product_id: NonEmpty
    market: list[Literal["MX"]] = Field(min_length=1)
    category: Literal["wig"] = "wig"
    appearance: Appearance
    confirmed_selling_points: list[str]
    visual_proof_actions: list[str]
    forbidden_claims: list[str]
    uncertain_points: list[str]
    evidence_notes: list[str]
    human_confirmed: bool = False


class VariantPlanItem(StrictModel):
    variant_type: Literal[
        "high_fidelity_h1",
        "high_fidelity_h2",
        "high_fidelity_h3",
        "general_hook_rebuild",
        "general_reveal_rebuild",
        "general_proof_rebuild",
        "general_pacing_rebuild",
        "general_cta_rebuild",
    ]
    sequence_no: int = Field(ge=1, le=20)
    replication_mode: Literal["high_fidelity", "general"]
    creative_route: Literal["H1", "H2", "H3", "G1", "G2", "G3", "G4", "G5"]
    variant_key: NonEmpty
    mutation_key: NonEmpty
    change_dimensions: list[Literal[
        "baseline",
        "visual_shell",
        "hook",
        "reveal",
        "proof_selection",
        "proof_order",
        "timing",
        "voiceover",
        "cta",
        "ending",
    ]] = Field(min_length=1, max_length=5)
    core_mutations: list[str] = Field(min_length=1, max_length=5)
    instruction: CompactText


class PromptQA(StrictModel):
    passed: bool
    issues: list[str]


class CreativeSignature(StrictModel):
    # Commercial anchors are checked by purpose-aware QA. Nurture scripts
    # describe their own mother-derived visual/action/payoff anchors instead.
    core_anchors: list[RuleText] = Field(min_length=1, max_length=10)
    hook_type: CompactText
    opening_action: CompactText
    reveal_method: CompactText
    voiceover_text: str = Field(max_length=1600)
    timing_pattern: list[CompactText] = Field(min_length=1, max_length=12)
    proof_actions: list[CompactText] = Field(max_length=8)
    proof_order: list[CompactText] = Field(max_length=8)
    cta_expression: str = Field(max_length=600)
    ending_composition: CompactText
    changed_dimensions: list[Literal[
        "baseline",
        "visual_shell",
        "hook",
        "reveal",
        "proof_selection",
        "proof_order",
        "timing",
        "voiceover",
        "cta",
        "ending",
    ]] = Field(default_factory=list, max_length=5)


class ReplicationPrompt(StrictModel):
    variant_type: NonEmpty
    sequence_no: int = Field(ge=1, le=20)
    replication_mode: Literal["high_fidelity", "general"]
    creative_route: Literal["H1", "H2", "H3", "G1", "G2", "G3", "G4", "G5"]
    variant_key: NonEmpty
    mutation_key: NonEmpty
    change_summary: CompactText
    full_prompt: PromptText
    creative_signature: CreativeSignature
    qa: PromptQA


class ReplicationCompileOutput(StrictModel):
    schema_version: Literal["1.0"] = "1.0"
    batch_id: NonEmpty
    mother_id: NonEmpty
    mother_version: int = Field(ge=1)
    product_id: NonEmpty
    relationship: Literal["same_product", "cross_product"]
    outputs: list[ReplicationPrompt] = Field(max_length=20)


class ProductFactExtraction(StrictModel):
    schema_version: Literal["1.0"] = "1.0"
    fact: ProductFactCard
    human_summary: NonEmpty


SCHEMA_MODELS: dict[str, type[BaseModel]] = {
    "mother_contract": MotherContract,
    "mother_review": MotherReview,
    "product_fact": ProductFactExtraction,
    "replication_output": ReplicationCompileOutput,
}
