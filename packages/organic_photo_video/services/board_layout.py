"""Versioned layout and presentation-variant configuration for board shots."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAYOUT_PATH = PACKAGE_ROOT / "config" / "layouts" / "LAYOUT_OUTFIT_BREAKDOWN_V1.json"
DEFAULT_POLICY_PATH = PACKAGE_ROOT / "config" / "variant_policies" / "BALANCED_4_V1.json"
LAYOUT_SCHEMA = "opv-board-layout-v1"
POLICY_SCHEMA = "opv-variant-policy-v1"


class BoardLayoutError(ValueError):
    pass


def _read(path: Path) -> Dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise BoardLayoutError(f"{path.name} must contain an object")
    return payload


def load_board_layout(path: Path | None = None) -> Dict[str, Any]:
    payload = _read(path or DEFAULT_LAYOUT_PATH)
    errors = validate_board_layout(payload)
    if errors:
        raise BoardLayoutError("; ".join(errors))
    return payload


def load_variant_policy(path: Path | None = None) -> Dict[str, Any]:
    payload = _read(path or DEFAULT_POLICY_PATH)
    errors = validate_variant_policy(payload)
    if errors:
        raise BoardLayoutError("; ".join(errors))
    return payload


def validate_board_layout(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    if payload.get("schema_version") != LAYOUT_SCHEMA:
        errors.append(f"schema_version must be {LAYOUT_SCHEMA}")
    canvas = payload.get("canvas") or {}
    if int(canvas.get("width") or 0) < 896 or int(canvas.get("height") or 0) < 1593:
        errors.append("canvas must be at least 896px wide and 9:16 portrait")
    width = int(canvas.get("width") or 0)
    height = int(canvas.get("height") or 0)
    if width and height and abs((width / height) - (9 / 16)) > 0.02:
        errors.append("canvas must be 9:16 portrait")
    variants = payload.get("variants") or {}
    if not isinstance(variants, Mapping) or not variants:
        errors.append("variants must not be empty")
    required_boxes = {"hero", "target_product", "companion_1", "companion_2"}
    if (payload.get("text") or {}).get("enabled", True):
        required_boxes.add("item_list")
    for name, spec in variants.items():
        missing = required_boxes - set((spec or {}).keys())
        if missing:
            errors.append(f"variant {name} missing boxes {sorted(missing)}")
    return errors


def validate_variant_policy(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    if payload.get("schema_version") != POLICY_SCHEMA:
        errors.append(f"schema_version must be {POLICY_SCHEMA}")
    if payload.get("mode") not in {"layout_only", "balanced", "content_only"}:
        errors.append("mode must be layout_only, balanced, or content_only")
    if not list(payload.get("layout_variants") or []):
        errors.append("layout_variants must not be empty")
    if not list(payload.get("copy_variants") or []):
        errors.append("copy_variants must not be empty")
    return errors


def presentation_variant(
    *,
    record_key: str,
    variant_index: int,
    policy: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    config = dict(policy or load_variant_policy())
    layouts = list(config.get("layout_variants") or ["LEFT_HERO"])
    copies = list(config.get("copy_variants") or ["outfit_formula"])
    palettes = list(config.get("palette_variants") or ["WARM_WHITE"])
    offset = int.from_bytes(
        hashlib.sha256(str(record_key).encode("utf-8")).digest()[:4], "big"
    )
    index = max(1, int(variant_index)) - 1
    axes = {
        "layout_variant": layouts[(offset + index) % len(layouts)],
        "copy_variant": copies[(offset + index) % len(copies)],
        "palette_variant": palettes[(offset + index) % len(palettes)],
    }
    signature_source = json.dumps(
        {"record_key": str(record_key), "variant_index": index + 1, **axes},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "variant_index": index + 1,
        "variant_policy_id": str(config.get("variant_policy_id") or ""),
        **axes,
        "presentation_signature": hashlib.sha256(signature_source.encode("utf-8")).hexdigest(),
    }
