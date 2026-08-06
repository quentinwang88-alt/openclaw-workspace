"""Registry and feature gate for optional category execution extensions."""

from __future__ import annotations

import os
import copy
from typing import Any, Dict, List, Optional

from .accessory import AccessoryExecutionAdapter
from .base import CategoryExecutionAdapter
from core.product_type_resolution import normalize_product_type


ACCESSORY_PROFILE_ENV = "ORIGINAL_SCRIPT_ACCESSORY_PROFILE_ENABLED"
_ADAPTERS = (AccessoryExecutionAdapter(),)
_SCARF_TYPES = {"scarf", "winter_scarf", "silk_scarf", "headscarf"}


def _flag_enabled(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _accessory_profile_enabled(enabled: Optional[bool]) -> bool:
    if enabled is not None:
        return bool(enabled)
    return _flag_enabled(os.environ.get(ACCESSORY_PROFILE_ENV, "0"))


def _adapter_for_extension(
    extension: Dict[str, Any],
) -> Optional[CategoryExecutionAdapter]:
    domain = str(extension.get("domain") or "").strip().upper()
    return next((adapter for adapter in _ADAPTERS if adapter.domain == domain), None)


def compile_category_execution_extension(
    *,
    product_type: str,
    top_category: str,
    anchor_card: Dict[str, Any],
    enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Compile a matching extension or return an empty dict.

    Empty means the common pipeline must omit the field entirely.  This is
    intentional: apparel snapshots, prompts and cache keys stay unchanged.
    """

    if not _accessory_profile_enabled(enabled):
        return {}
    for adapter in _ADAPTERS:
        if adapter.supports(product_type=product_type, top_category=top_category):
            return adapter.compile_profile(
                product_type=product_type,
                top_category=top_category,
                anchor_card=anchor_card,
            )
    return {}


def reconcile_anchor_category_contract(
    anchor_card: Dict[str, Any],
    *,
    product_type: str,
    top_category: str,
) -> Dict[str, Any]:
    """Align the legacy P1 contract with an explicit registered scarf type.

    P1 may contribute observations, but it must not reclassify a known scarf
    subtype or force silk/head scarves through the historical winter-scarf
    contract.  Products outside this small registered set remain byte-for-byte
    unchanged.
    """

    resolved = normalize_product_type(product_type, top_category)
    if not (
        resolved.recognized_by_registry
        and resolved.canonical_type in _SCARF_TYPES
    ):
        return anchor_card
    result = copy.deepcopy(anchor_card)
    raw = result.get("category_execution_contract")
    contract = dict(raw) if isinstance(raw, dict) else {}
    canonical = resolved.canonical_type
    result_guidance = {
        "winter_scarf": "清楚展示围巾已经围好或披好后与脖颈、肩部及上半身穿搭的关系",
        "silk_scarf": "清楚展示丝巾已经搭配完成后与颈部、领口及上半身的点缀关系",
        "headscarf": "清楚展示头巾已经完成造型后的位置、轮廓、头发状态与穿搭关系",
        "scarf": "保守展示围巾已经佩戴后与肩颈及上半身穿搭的关系",
    }[canonical]
    contract.update({
        "display_family": "apparel_accessory",
        "product_subtype": canonical,
        "placement_zone": (
            "head_face" if canonical == "headscarf" else "neck_shoulder"
        ),
        "operation_policy": (
            "process_forbidden"
            if canonical == "headscarf"
            else "result_first_process_avoid"
        ),
        "category_authority_source": "PRODUCT_TYPE_REGISTRY_AND_CATEGORY_EXTENSION",
        "primary_visual_result": result_guidance,
        "result_priority": result_guidance,
    })
    confidence = (
        dict(contract.get("field_confidence"))
        if isinstance(contract.get("field_confidence"), dict)
        else {}
    )
    for key in (
        "product_subtype", "placement_zone", "operation_policy",
        "primary_visual_result",
    ):
        confidence[key] = "high"
    contract["field_confidence"] = confidence
    if canonical in {"silk_scarf", "headscarf", "scarf"}:
        # Remove an inherited winter default without claiming another season.
        season = (
            dict(contract.get("season_context"))
            if isinstance(contract.get("season_context"), dict)
            else {}
        )
        if season.get("primary_season") == "winter":
            season["primary_season"] = "unknown"
        if season.get("weather_signal") == "cold":
            season["weather_signal"] = "unknown"
        contract["season_context"] = season
        co_styling = (
            dict(contract.get("co_styling_hint"))
            if isinstance(contract.get("co_styling_hint"), dict)
            else {}
        )
        winter_only = {"winter_coat", "wool_coat", "knit_sweater", "basic_turtleneck"}
        co_styling["pair_with"] = [
            item for item in co_styling.get("pair_with") or []
            if str(item or "").strip() not in winter_only
        ][:4]
        contract["co_styling_hint"] = co_styling
    result["category_execution_contract"] = contract
    return result


def resolve_category_carrier_execution(
    extension: Dict[str, Any],
    *,
    presentation_mode: str,
) -> Dict[str, Any]:
    adapter = _adapter_for_extension(extension)
    if adapter is None:
        return {}
    return adapter.resolve_carrier_execution(
        extension, presentation_mode=presentation_mode
    )


def build_category_blueprint_guidance(
    extension: Dict[str, Any],
    *,
    carrier_execution: Dict[str, Any],
) -> str:
    adapter = _adapter_for_extension(extension)
    if adapter is None:
        return ""
    return adapter.build_blueprint_guidance(
        extension, carrier_execution=carrier_execution
    )


def build_category_video_brief(
    extension: Dict[str, Any],
    *,
    carrier_execution: Dict[str, Any],
) -> Dict[str, Any]:
    adapter = _adapter_for_extension(extension)
    if adapter is None:
        return {}
    return adapter.build_video_brief(
        extension, carrier_execution=carrier_execution
    )


def validate_category_execution_identity(
    extension: Dict[str, Any],
    *,
    script: Dict[str, Any],
) -> List[str]:
    adapter = _adapter_for_extension(extension)
    if adapter is None:
        return []
    return adapter.validate_identity(extension, script=script)
