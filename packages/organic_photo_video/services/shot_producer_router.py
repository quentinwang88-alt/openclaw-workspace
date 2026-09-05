"""Route generated-photo and deterministic composite-board shot requests."""

from __future__ import annotations

from services.composite_board_renderer import CompositeBoardRenderer
from services.image_generator import GenerationOutcome, ShotGenerationRequest


class ShotProducerRouter:
    def __init__(self, photo_generator, board_renderer=None):
        self.photo_generator = photo_generator
        self.board_renderer = board_renderer or CompositeBoardRenderer()

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        kind = str(request.plan_shot.get("shot_kind") or "generated_photo")
        if kind == "generated_photo":
            return self.photo_generator.generate_shot(request)
        if kind == "composite_board":
            return self.board_renderer.generate_shot(request)
        return GenerationOutcome(ok=False, error=f"unsupported shot_kind: {kind}")
