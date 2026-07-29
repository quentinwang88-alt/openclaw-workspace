"""Thin routing layer between discovered video structures and production flows."""

from .models import (
    ContractValidationResult,
    DirectionAssignment,
    RouteRequest,
    SelectionResult,
    StructureCandidate,
)
from .service import StructureRouterService
from .storage import RouterStorage
from .validator import (
    validate_direction_diversity,
    validate_script_against_contract,
    validate_video_prompt_against_contract,
)

__all__ = [
    "ContractValidationResult",
    "DirectionAssignment",
    "RouteRequest",
    "RouterStorage",
    "SelectionResult",
    "StructureCandidate",
    "StructureRouterService",
    "validate_direction_diversity",
    "validate_script_against_contract",
    "validate_video_prompt_against_contract",
]
