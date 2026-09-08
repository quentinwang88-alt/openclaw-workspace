"""Route generated-photo and deterministic composite-board shot requests."""

from __future__ import annotations

import hashlib
from pathlib import Path
import shutil

from services.composite_board_renderer import CompositeBoardRenderer
from services.image_generator import (
    GenerationOutcome, ShotGenerationRequest, read_image_dimensions,
)


class ReusedAssetProducer:
    """Freeze an operator-approved source image without invoking an AI model."""

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        source = Path(str(request.plan_shot.get("asset_path") or "")).expanduser()
        expected = str(request.plan_shot.get("asset_sha256") or "")
        if not source.is_file():
            return GenerationOutcome(
                ok=False, provider="asset-reuse", model="file-copy-v1",
                error="reused asset_path is missing",
            )
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        if expected and digest != expected:
            return GenerationOutcome(
                ok=False, provider="asset-reuse", model="file-copy-v1",
                error="reused asset SHA256 changed",
            )
        suffix = source.suffix.lower() if source.suffix.lower() in {".png", ".jpg", ".jpeg"} else ".jpg"
        target = Path(request.output_dir) / (
            f"{request.task_id}_P{request.slot_index}_v{request.shot_version}_reuse{suffix}"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.resolve() != target.resolve():
            shutil.copy2(source, target)
        dimensions = read_image_dimensions(str(target))
        if dimensions is None:
            target.unlink(missing_ok=True)
            return GenerationOutcome(
                ok=False, provider="asset-reuse", model="file-copy-v1",
                error="reused asset is not a decodable PNG/JPEG",
            )
        return GenerationOutcome(
            ok=True, image_path=str(target), provider="asset-reuse",
            model="file-copy-v1", request_id=f"reuse:{digest}",
            width=dimensions[0], height=dimensions[1],
            raw={"source_path": str(source.resolve()), "source_sha256": digest},
        )


class ShotProducerRouter:
    def __init__(self, photo_generator, board_renderer=None):
        self.photo_generator = photo_generator
        self.board_renderer = board_renderer or CompositeBoardRenderer()
        self.reused_asset_producer = ReusedAssetProducer()

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        kind = str(request.plan_shot.get("shot_kind") or "generated_photo")
        if kind == "generated_photo":
            return self.photo_generator.generate_shot(request)
        if kind == "composite_board":
            return self.board_renderer.generate_shot(request)
        if kind == "template_card":
            return self.board_renderer.generate_shot(request)
        if kind == "reused_asset":
            return self.reused_asset_producer.generate_shot(request)
        return GenerationOutcome(ok=False, error=f"unsupported shot_kind: {kind}")
