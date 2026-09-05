"""Validate the saved technical QC of an exact physical shot, not model scores."""
from __future__ import annotations

import hashlib
from pathlib import Path


class MediaQcError(ValueError):
    pass


def require_shot_media_qc(shot, *, decode: bool = False) -> str:
    qc = (shot.qa_json or {}).get("media_qc") or {}
    if shot.shot_status not in {"generated", "approved"} or qc.get("size_ok") is not True:
        raise MediaQcError(f"P{shot.slot_index} has no passed technical image QC")
    expected = str(shot.image_sha256 or "")
    if not expected or qc.get("sha256") != expected:
        raise MediaQcError(f"P{shot.slot_index} technical QC does not match the image hash")
    for name in ("width", "height"):
        recorded = qc.get(name)
        current = getattr(shot, f"image_{name}", None)
        if not isinstance(recorded, int) or isinstance(recorded, bool) or recorded <= 0 or current != recorded:
            raise MediaQcError(f"P{shot.slot_index} technical image dimensions are missing or changed")
    path = Path(str(shot.image_url or ""))
    if not path.is_file():
        raise MediaQcError(f"P{shot.slot_index} technical image file is missing")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != expected:
        raise MediaQcError(f"P{shot.slot_index} image bytes changed after technical QC")
    if decode:
        from PIL import Image
        try:
            with Image.open(path) as image:
                image.load()
                if image.size != (shot.image_width, shot.image_height):
                    raise ValueError("decoded dimensions do not match saved QC")
        except Exception as exc:
            raise MediaQcError(f"P{shot.slot_index} image cannot be decoded consistently") from exc
    return expected
