"""Interface shared by optional category execution adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class CategoryExecutionAdapter(ABC):
    """A thin category policy plug-in for the common production path."""

    domain = ""

    @abstractmethod
    def supports(self, *, product_type: str, top_category: str) -> bool:
        """Return whether this adapter owns the explicitly registered type."""

    @abstractmethod
    def compile_profile(
        self,
        *,
        product_type: str,
        top_category: str,
        anchor_card: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Compile one deterministic extension without model inference."""

    @abstractmethod
    def resolve_carrier_execution(
        self,
        extension: Dict[str, Any],
        *,
        presentation_mode: str,
    ) -> Dict[str, Any]:
        """Interpret the profile under the already-routed carrier."""

    @abstractmethod
    def build_blueprint_guidance(
        self,
        extension: Dict[str, Any],
        *,
        carrier_execution: Dict[str, Any],
    ) -> str:
        """Render compact, category-only visual guidance."""

    @abstractmethod
    def build_video_brief(
        self,
        extension: Dict[str, Any],
        *,
        carrier_execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Render the compact category hand-off consumed by the video prompt."""

    def validate_identity(
        self,
        extension: Dict[str, Any],
        *,
        script: Dict[str, Any],
    ) -> List[str]:
        """Return deterministic product-identity conflicts for this category."""

        return []
