"""Cheap, deterministic visual profiling for photo style references.

The profile is intentionally small.  It gives the business planner enough
facts to avoid choosing a palette or presentation that contradicts uploaded
references without adding another paid model call to every task.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

from PIL import Image, ImageFilter, ImageStat


ANALYZER_VERSION = 1


class PhotoStyleReferenceAnalysisError(ValueError):
    pass


def _crop_content(image: Image.Image) -> Image.Image:
    """Drop the common screenshot chrome while retaining the visual body."""
    width, height = image.size
    top = round(height * .06)
    bottom = max(top + 1, round(height * .82))
    return image.crop((0, top, width, bottom)).convert("RGB").resize((180, 240))


def _skin_ratio(image: Image.Image) -> float:
    pixels = list(image.convert("YCbCr").getdata())
    skin = sum(1 for _y, cb, cr in pixels if 77 <= cb <= 127 and 133 <= cr <= 173)
    return skin / max(1, len(pixels))


def _colour_votes(image: Image.Image) -> dict[str, int]:
    votes: dict[str, int] = {}
    quantized = image.quantize(colors=12).convert("RGB")
    for r, g, b in quantized.getdata():
        maximum, minimum = max(r, g, b), min(r, g, b)
        if maximum < 42:
            name = "black"
        elif minimum > 215:
            name = "cream" if r > b + 5 else "white"
        elif maximum - minimum < 18:
            name = "gray"
        elif r > g * 1.18 and r > b * 1.32:
            name = "burgundy" if r < 120 else ("camel" if g > b * 1.25 else "warm_brown")
        elif r > b * 1.28 and g > b * 1.12:
            name = "camel" if r > 145 else "warm_brown"
        elif b > r * 1.16:
            name = "denim_blue" if b > g * .9 else "navy"
        elif g > r * 1.08:
            name = "olive"
        else:
            name = "neutral"
        votes[name] = votes.get(name, 0) + 1
    return votes


class PhotoStyleReferenceAnalyzer:
    """Extract planning facts from local reference images."""

    def analyze(self, paths: Sequence[str]) -> dict[str, Any]:
        resolved = [Path(value).expanduser().resolve() for value in paths]
        if not resolved or any(not path.is_file() for path in resolved):
            raise PhotoStyleReferenceAnalysisError("风格参考图缺失或不可读取")
        votes: dict[str, int] = {}
        skin_ratios, brightness, contrast, edge_levels = [], [], [], []
        source_hashes = []
        for path in resolved:
            source_hashes.append(hashlib.sha256(path.read_bytes()).hexdigest())
            with Image.open(path) as opened:
                image = _crop_content(opened)
            for name, count in _colour_votes(image).items():
                votes[name] = votes.get(name, 0) + count
            skin_ratios.append(_skin_ratio(image))
            gray = image.convert("L")
            stat = ImageStat.Stat(gray)
            brightness.append(float(stat.mean[0]))
            contrast.append(float(stat.stddev[0]))
            edge_levels.append(float(ImageStat.Stat(gray.filter(ImageFilter.FIND_EDGES)).mean[0]))

        ranked = [name for name, _count in sorted(votes.items(), key=lambda item: (-item[1], item[0]))]
        useful = [name for name in ranked if name not in {"neutral", "white"}][:4]
        warm_score = sum(votes.get(name, 0) for name in ("warm_brown", "camel", "cream", "burgundy"))
        cool_score = sum(votes.get(name, 0) for name in ("navy", "denim_blue", "gray", "black"))
        temperature = "warm" if warm_score > cool_score * 1.15 else ("cool" if cool_score > warm_score * 1.25 else "neutral")
        if temperature == "warm":
            useful = list(dict.fromkeys([
                "warm_brown", "camel", "cream",
                *[name for name in useful if name not in {"black", "gray"}],
            ]))[:4]
        mean_skin = sum(skin_ratios) / len(skin_ratios)
        # A person photo normally has a small, localized skin region.  Very
        # low ratios indicate no person; very high ratios are usually the
        # warm beige/brown surfaces used by editorial garment flat lays.
        presentation = "FLAT_LAY" if mean_skin < .018 or mean_skin > .30 else "MODEL_FULL_BODY"
        mean_contrast = sum(contrast) / len(contrast)
        mean_edge = sum(edge_levels) / len(edge_levels)
        lighting = "directional_high_contrast" if mean_contrast >= 48 else "soft_diffused"
        background = (
            "textured_warm_surface" if presentation == "FLAT_LAY" and temperature == "warm"
            else ("plain_surface" if presentation == "FLAT_LAY" else "creator_environment")
        )
        season = "autumn" if warm_score >= cool_score or "olive" in useful else "cool_weather"
        style_tags = ["layered", "daily_outfit"]
        if presentation == "FLAT_LAY":
            style_tags += ["editorial_flat_lay", "garment_layout"]
        if temperature == "warm":
            style_tags += ["warm_neutral", "heritage"]
        return {
            "schema_version": "opv-photo-style-profile-v1",
            "analyzer_version": ANALYZER_VERSION,
            "presentation_type": presentation,
            "season": season,
            "palette": useful or ["neutral"],
            "temperature": temperature,
            "materials": ["knit", "denim", "layered_fabric"],
            "style_tags": style_tags,
            "lighting": lighting,
            "background": background,
            "avoid_tags": (["studio_model", "cool_monochrome"] if presentation == "FLAT_LAY" and temperature == "warm" else []),
            "confidence": round(min(.96, .62 + len(resolved) * .07 + (mean_contrast + mean_edge) / 1000), 2),
            "source_hashes": source_hashes,
            "metrics": {
                "skin_ratio": round(mean_skin, 4),
                "brightness": round(sum(brightness) / len(brightness), 1),
                "contrast": round(mean_contrast, 1),
            },
        }


def validate_style_alignment(expected: Mapping[str, Any], generated_path: str) -> None:
    """Fail cheaply when Look A visibly leaves the reference direction."""
    actual = PhotoStyleReferenceAnalyzer().analyze([generated_path])
    if expected.get("presentation_type") == "FLAT_LAY" and actual.get("presentation_type") != "FLAT_LAY":
        raise PhotoStyleReferenceAnalysisError("首张成图未保持参考图的平铺展示方式")
    expected_temperature = str(expected.get("temperature") or "neutral")
    actual_temperature = str(actual.get("temperature") or "neutral")
    if expected_temperature in {"warm", "cool"} and actual_temperature not in {expected_temperature, "neutral"}:
        raise PhotoStyleReferenceAnalysisError("首张成图的整体色温与参考图冲突")
