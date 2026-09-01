"""Isolated 25-45 second variable-segment original-video sidecar.

Nothing in this package is imported by the stable 15-second production path.
The long-form CLI is the sole entry point until the text/video pilot passes.
"""

from .contracts import LONGFORM_SCHEMA_VERSION, validate_master_contract
from .planner import compile_longform_plan

__all__ = ["LONGFORM_SCHEMA_VERSION", "compile_longform_plan", "validate_master_contract"]
