"""Normalize and validate outfit-decomposition cutouts.

Some image providers return a baked light checkerboard even when transparency
is requested.  The normalizer removes only edge-connected near-neutral pixels,
preserving enclosed white garments, crops to the foreground, and adds a small
transparent safety margin.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from PIL import Image, ImageDraw, ImageChops


REQUIRED_ROLES = ("person_cutout", "target_product", "top_inner", "bottom")


def _adaptive_anchor_mask(image: Image.Image):
    """Local matte for an already solid-background anchor, without a model.

    Learn the *actual* softly lit background from both outside strips per row.
    Connected silhouette and hole filling keep white garments opaque; darkness
    or whiteness alone never determines foreground. Ambiguous subjects fall
    back to an intact region instead of deleting unseen white apparel.
    """
    import numpy as np
    from scipy import ndimage

    rgb = np.asarray(image.convert("RGB"), dtype=np.float32)
    height, width = rgb.shape[:2]
    strip = max(5, int(width * .05))
    left = ndimage.gaussian_filter1d(np.median(rgb[:, :strip], axis=1), max(2, height / 100), axis=0)
    right = ndimage.gaussian_filter1d(np.median(rgb[:, -strip:], axis=1), max(2, height / 100), axis=0)
    x = np.linspace(0, 1, width, dtype=np.float32)[None, :, None]
    expected = left[:, None, :] * (1 - x) + right[:, None, :] * x
    distance = np.sqrt(np.mean((rgb - expected) ** 2, axis=2))
    border_noise = np.concatenate((distance[:, :strip].ravel(), distance[:, -strip:].ravel()))
    threshold = max(8.0, float(np.percentile(border_noise, 99)) * 1.65)
    initial = distance > threshold
    initial = ndimage.binary_closing(initial, iterations=max(1, width // 350))
    labels, count = ndimage.label(initial)
    if not count:
        return None
    sizes = np.bincount(labels.ravel())
    sizes[0] = 0
    foreground = labels == sizes.argmax()
    foreground = ndimage.binary_fill_holes(foreground)
    ys, xs = np.nonzero(foreground)
    if not len(ys) or (ys.max() - ys.min()) < height * .6 or len(ys) < width * height * .07:
        return None
    # Grow through nearby, lower-contrast connected edge pixels, but not across
    # arbitrary background. This retains cuffs/shoes while rejecting the backdrop.
    allowed = (distance > max(4.5, threshold * .6)) & ndimage.binary_dilation(foreground, iterations=max(4, width // 100))
    foreground = ndimage.binary_propagation(foreground, mask=allowed | foreground)
    foreground = ndimage.binary_fill_holes(foreground)
    # A soft neutral contact shadow is not part of the body. Suppress only
    # low-contrast neutral dark patches near the bottom, away from confirmed
    # garment/shoe pixels; preserve bright white fabric and deep/dark footwear.
    residual = rgb - expected
    shade = residual.mean(axis=2)
    chromatic_residual = np.std(residual, axis=2)
    distance_to_solid = ndimage.distance_transform_edt(~(distance > max(25., threshold * 2.5)))
    lower = np.arange(height)[:, None] > height * .84
    soft_shadow = (lower & (shade < -3) & (shade > -30) & (chromatic_residual < 3.5)
                   & (distance_to_solid > max(3, width // 140)))
    foreground &= ~soft_shadow
    foreground = ndimage.binary_fill_holes(foreground)
    # Slightly protect the edge before a subpixel feather; RGB inside stays exact.
    foreground = ndimage.binary_dilation(foreground, iterations=1)
    alpha = ndimage.gaussian_filter(foreground.astype(np.float32), .65)
    alpha[foreground] = 1.0
    result = image.convert("RGBA")
    result.putalpha(Image.fromarray(np.uint8(np.clip(alpha * 255, 0, 255))))
    bbox = result.getchannel("A").getbbox()
    return result.crop(bbox), {"background_noise_p99": round(float(np.percentile(border_noise, 99)), 3),
                              "segmentation_threshold": round(threshold, 3), "source_crop": list(bbox)}


def normalize_anchor_region(source: Path, destination: Path, *, background: str) -> Dict[str, Any]:
    """Extract a solid-background anchor without redrawing or brightness keying.

    First use the actual backdrop's colour/noise model and connected silhouette.
    If too ambiguous, retain an opaque region (middle 70%, entire height) rather
    than deleting unseen white-on-white apparel. The mode is explicit in QA.
    """
    image = Image.open(source).convert("RGBA")
    masked = _adaptive_anchor_mask(image)
    if masked is not None:
        region, metadata = masked
        destination.parent.mkdir(parents=True, exist_ok=True)
        region.save(destination, format="PNG", optimize=True)
        return {"passed": True, "checks": {"readable": True, "pixels_preserved": True},
                "width": region.width, "height": region.height,
                "composition_mode": "adaptive_background_matte", "background_color": background,
                "mask_method": "row_background_model_connected_silhouette_v1", **metadata}
    rgb = image.convert("RGB")
    delta = ImageChops.difference(rgb, Image.new("RGB", image.size, background))
    channels = delta.split()
    difference = ImageChops.lighter(ImageChops.lighter(channels[0], channels[1]), channels[2])
    bbox = difference.point(lambda value: 255 if value > 2 else 0).getbbox()
    if bbox:
        margin = max(12, int(image.width * .08))
        left = max(0, min(bbox[0] - margin, int(image.width * .15)))
        right = min(image.width, max(bbox[2] + margin, int(image.width * .85)))
    else:
        left, right = 0, image.width
    region = image.crop((left, 0, right, image.height))
    destination.parent.mkdir(parents=True, exist_ok=True)
    region.save(destination, format="PNG", optimize=True)
    return {"passed": True, "checks": {"readable": True, "pixels_preserved": True},
            "width": region.width, "height": region.height,
            "composition_mode": "same_background_region", "background_color": background,
            "source_crop": [left, 0, right, image.height]}


def transparent_reference_is_usable(path: str) -> bool:
    """Technical eligibility only; never infer transparency by whitening pixels."""
    try:
        with Image.open(path) as image:
            return bool(cutout_quality(image)["passed"])
    except (OSError, ValueError):
        return False


def refresh_manifest_anchor_pixels(
    manifest: Mapping[str, Any], *, anchor_path: Path, output_dir: Path, background: str
) -> Dict[str, Any]:
    """Build a new local board manifest; preserve all garment assets byte-for-byte.

    Intended for an explicit board-only revision. No repository calls, no image
    model, and no writes to the former person asset. The caller binds the new
    manifest to its new revision through the normal rework service.
    """
    updated = deepcopy(dict(manifest))
    person = dict((updated.get("assets") or {}).get("person_cutout") or {})
    destination = Path(output_dir) / "person_cutout_normalized.png"
    if person.get("path") and destination.resolve() == Path(person["path"]).resolve():
        raise CutoutAssetError("anchor refresh requires a new output directory; frozen assets cannot be overwritten")
    qa = normalize_anchor_region(Path(anchor_path), destination, background=background)
    updated.setdefault("assets", {})["person_cutout"] = {
        **person, "role": "person_cutout", "path": str(destination),
        "source_path": str(anchor_path), "source_type": "anchor_pixels_preserved",
        "composition_mode": qa["composition_mode"], "background_color": background,
        "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
        "qa_status": "passed", "qa": qa, "review_type": "technical_only",
    }
    (Path(output_dir) / "manifest.json").write_text(
        json.dumps(updated, ensure_ascii=False, indent=2), encoding="utf-8")
    return updated


class CutoutAssetError(ValueError):
    pass


def _is_chroma_green(rgb) -> bool:
    red, green, blue = rgb[:3]
    return green >= 150 and green - max(red, blue) >= 60


def chroma_green_to_alpha(image: Image.Image) -> Image.Image:
    """Key a vivid green provider background while preserving white apparel."""
    rgba = image.convert("RGBA")
    pixels = []
    for red, green, blue, original_alpha in rgba.getdata():
        excess = green - max(red, blue)
        if green >= 150 and excess >= 60:
            # Feather only the narrow anti-aliased edge band.
            alpha = 0 if excess >= 95 else int(255 * (95 - excess) / 35)
            pixels.append((red, min(green, max(red, blue)), blue, min(original_alpha, alpha)))
        else:
            # Remove low-dominance green spill left on hair and garment edges.
            # This does not touch cyan/blue garments because their blue channel
            # remains equal to or stronger than green.
            clean_green = max(red, blue) if green >= max(red, blue) + 10 else green
            pixels.append((red, clean_green, blue, original_alpha))
    rgba.putdata(pixels)
    return rgba


def edge_background_to_alpha(image: Image.Image) -> Image.Image:
    rgba = image.convert("RGBA")
    width, height = rgba.size
    points = [(0, 0), (width - 1, 0), (0, height - 1), (width - 1, height - 1)]
    eligible = Image.new("L", rgba.size)
    eligible.putdata([
        255
        if max(red, green, blue) - min(red, green, blue) <= 12
        and min(red, green, blue) >= 225
        else 0
        for red, green, blue, _alpha in rgba.getdata()
    ])
    for point in points:
        if eligible.getpixel(point) == 255:
            ImageDraw.floodfill(eligible, point, 128, thresh=0)
    alpha = rgba.getchannel("A")
    alpha.putdata([
        0 if marker == 128 else original
        for marker, original in zip(eligible.getdata(), alpha.getdata())
    ])
    rgba.putalpha(alpha)
    return rgba


def cutout_quality(image: Image.Image) -> Dict[str, Any]:
    rgba = image.convert("RGBA")
    alpha = rgba.getchannel("A")
    extrema = alpha.getextrema()
    bbox = alpha.getbbox()
    total = max(rgba.width * rgba.height, 1)
    foreground = sum(1 for value in alpha.getdata() if value > 16)
    fraction = foreground / total
    checks = {
        "has_transparency": extrema[0] == 0,
        "has_foreground": extrema[1] > 16 and bbox is not None,
        "foreground_fraction": 0.05 <= fraction <= 0.92,
        "minimum_size": rgba.width >= 256 and rgba.height >= 256,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "width": rgba.width,
        "height": rgba.height,
        "alpha_extrema": list(extrema),
        "foreground_fraction": round(fraction, 4),
    }


def normalize_cutout(source: Path, destination: Path, *, padding_ratio: float = 0.04) -> Dict[str, Any]:
    image = Image.open(source).convert("RGBA")
    alpha = image.getchannel("A")
    if alpha.getextrema()[0] == 255:
        corners = [
            image.getpixel((0, 0)), image.getpixel((image.width - 1, 0)),
            image.getpixel((0, image.height - 1)),
            image.getpixel((image.width - 1, image.height - 1)),
        ]
        image = (
            chroma_green_to_alpha(image)
            if sum(_is_chroma_green(value) for value in corners) >= 3
            else edge_background_to_alpha(image)
        )
    bbox = image.getchannel("A").getbbox()
    if not bbox:
        raise CutoutAssetError(f"{source.name} has no foreground after normalization")
    cropped = image.crop(bbox)
    padding = max(12, int(max(cropped.size) * padding_ratio))
    output = Image.new(
        "RGBA", (cropped.width + padding * 2, cropped.height + padding * 2),
        (0, 0, 0, 0),
    )
    output.alpha_composite(cropped, (padding, padding))
    qc = cutout_quality(output)
    if not qc["passed"]:
        raise CutoutAssetError(f"{source.name} cutout QC failed: {qc}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    output.save(destination, format="PNG", optimize=True)
    return qc


def build_asset_manifest(
    *,
    task_id: str,
    sources: Mapping[str, str],
    output_dir: Path,
    source_types: Optional[Mapping[str, str]] = None,
    source_metadata: Optional[Mapping[str, Mapping[str, Any]]] = None,
    item_roles: Optional[Sequence[str]] = None,
    anchor_background: str = "",
) -> Dict[str, Any]:
    required_roles = ("person_cutout", *item_roles) if item_roles is not None else REQUIRED_ROLES
    if item_roles is not None and (len(item_roles) not in (2, 3) or item_roles[0] != "target_product" or len(set(item_roles)) != len(item_roles)):
        raise CutoutAssetError("dynamic board requires target product plus one or two real companions")
    missing = [role for role in required_roles if not Path(str(sources.get(role) or "")).is_file()]
    if missing:
        raise CutoutAssetError("missing decomposition roles: " + ", ".join(missing))
    assets: Dict[str, Any] = {}
    for role in required_roles:
        source = Path(str(sources[role]))
        destination = output_dir / f"{role}_normalized.png"
        qc = (normalize_anchor_region(source, destination, background=anchor_background)
              if role == "person_cutout" and anchor_background else normalize_cutout(source, destination))
        digest = hashlib.sha256(destination.read_bytes()).hexdigest()
        assets[role] = {
            "role": role,
            "path": str(destination),
            "source_path": str(source),
            "source_type": str((source_types or {}).get(role) or "ai_derived"),
            "sha256": digest,
            "qa_status": "passed",
            "qa": qc,
            **({"composition_mode": qc["composition_mode"], "background_color": anchor_background}
               if role == "person_cutout" and anchor_background else {}),
            **dict((source_metadata or {}).get(role) or {}),
        }
    manifest = {
        "schema_version": "opv-outfit-decomposition-v2" if item_roles is not None else "opv-outfit-decomposition-v1",
        "task_id": task_id,
        "status": "ready",
        "assets": assets,
        **({"item_roles": list(item_roles), "required_roles": list(required_roles),
            "item_count_policy": "dynamic_2_or_3"} if item_roles is not None else {}),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest
