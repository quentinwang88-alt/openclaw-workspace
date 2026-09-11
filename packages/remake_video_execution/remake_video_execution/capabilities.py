from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProviderCapabilities:
    provider: str = "minimax-h3"
    max_segment_seconds: int = 15
    max_segments: int = 12
    integer_duration_only: bool = True
    supports_first_frame: bool = True
    supports_first_last_frame: bool = True
    supports_dialogue_lipsync: bool = False

