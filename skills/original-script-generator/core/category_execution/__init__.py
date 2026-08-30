"""Optional category execution extensions for the shared script pipeline.

The common original-script path owns orchestration, structure, claims, scenes,
voiceover and persistence.  Category adapters only add physical execution
context when a registered product type needs it.  Products without a matching
adapter must remain byte-for-byte compatible with the pre-extension path.
"""

from .registry import (
    ACCESSORY_PROFILE_ENV,
    build_category_blueprint_guidance,
    build_category_video_brief,
    compile_category_execution_extension,
    project_category_capture_rhythm_contract,
    reconcile_anchor_category_contract,
    resolve_category_argument_execution,
    resolve_category_carrier_execution,
    validate_category_execution_identity,
)

__all__ = [
    "ACCESSORY_PROFILE_ENV",
    "build_category_blueprint_guidance",
    "build_category_video_brief",
    "compile_category_execution_extension",
    "project_category_capture_rhythm_contract",
    "reconcile_anchor_category_contract",
    "resolve_category_argument_execution",
    "resolve_category_carrier_execution",
    "validate_category_execution_identity",
]
