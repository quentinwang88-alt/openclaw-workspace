from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


SCHEMA_VERSION = "remake-execution-plan-v1"


@dataclass(frozen=True)
class Issue:
    code: str
    severity: str
    message: str
    source_span: str = ""
    affected_stage: str = "PLAN"


@dataclass(frozen=True)
class SourceSnapshot:
    record_id: str
    script_id: str
    source_kind: str
    raw_prompt: str
    duration_ms: int
    target_country: str = ""
    target_language: str = ""
    publish_purpose: str = ""
    cart_enabled: str = ""
    source_voiceover: str = ""
    source_voiceover_zh: str = ""
    structured_source: Dict[str, Any] = field(default_factory=dict)
    reference_manifest: List[Dict[str, Any]] = field(default_factory=list)
    source_revision_hash: str = ""
    product_id: str = ""
    reference_image_pack_id: str = ""
    reference_image_version: int = 0
    reference_selection: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Shot:
    shot_id: str
    start_ms: int
    end_ms: int
    body: str
    source_span: str
    scene_id: str = ""
    spoken_lines: List[str] = field(default_factory=list)
    screen_texts: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionPlan:
    source: SourceSnapshot
    duration_ms: int
    aspect_ratio: str
    requested_resolution: str
    shots: List[Shot]
    global_requirements: str
    audio_mode: str
    issues: List[Issue]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GenerationSegment:
    segment_id: str
    ordinal: int
    global_start_ms: int
    global_end_ms: int
    requested_duration_seconds: int
    source_shot_slices: List[Dict[str, Any]]
    incoming_boundary: str
    prompt: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SegmentPlan:
    source_revision_hash: str
    duration_ms: int
    segments: List[GenerationSegment]
    issues: List[Issue]
    schema_version: str = "remake-segment-plan-v1"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
