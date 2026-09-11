"""MX wig four-choice execution flow: identity constants and dispatch guards.

The whole ``mx_wig_choice_v1`` flow must stay behind explicit dispatch points —
Thai (and all other) tasks keep their original code paths byte-for-byte.  Every
predicate here therefore checks the full condition set, never a single market
or category field:

- recipe dispatch: media_kind=native_photo + market=MX + category=wig
  + recipe_id=PHOTO_MX_PICK_YOUR_HAIR_V2 + execution_flow=mx_wig_choice_v1
- frozen-request dispatch: same five conditions read back from the frozen
  request snapshot.

Frozen batches resume under the flow they were frozen with; later preset or
sheet edits never retarget them.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from domain.contracts import MX_WIG_CHOICE_FLOW

MX_WIG_RECIPE_ID = "PHOTO_MX_PICK_YOUR_HAIR_V2"
MX_WIG_RECIPE_KEY = "MX_PICK_YOUR_HAIR"
MX_WIG_ACCOUNT_ID = "OPV_MX_PHOTO_001"
MX_WIG_MARKET = "MX"
MX_WIG_CATEGORY = "wig"
MX_WIG_THEME_KEY = "MX_WEEKEND_HAIR_CHOICE"
MX_WIG_DEFAULT_THEME_LABEL_ZH = "周末换个发型，你选哪款？"
MX_WIG_STORE_ID = "MXJF01"
MX_WIG_SOURCE_ROLES = ("hair_a", "hair_b", "hair_c", "hair_d")
MX_WIG_LOGIC_KEY = "wig_four_choice_es"
MX_WIG_ASSET_SET_KEY = "MX_WIG_CHOICE_GEN"


def recipe_is_mx_wig_choice(recipe: Any) -> bool:
    """True only for the exact frozen MX wig four-choice recipe version."""
    if recipe is None:
        return False
    recipe_id = str(getattr(recipe, "recipe_id", "") or "")
    spec = dict(getattr(recipe, "recipe_spec_json", {}) or {})
    return (
        recipe_id == MX_WIG_RECIPE_ID
        and spec.get("execution_flow") == MX_WIG_CHOICE_FLOW
        and spec.get("media_kind") == "native_photo"
        and spec.get("category_key") == MX_WIG_CATEGORY
        and MX_WIG_MARKET in list(spec.get("markets") or [])
    )


def request_is_mx_wig_choice(request: Mapping[str, Any]) -> bool:
    """True only for frozen requests carrying the full MX wig dispatch set."""
    if not isinstance(request, Mapping):
        return False
    if request.get("execution_flow") != MX_WIG_CHOICE_FLOW:
        return False
    if request.get("recipe_id") != MX_WIG_RECIPE_ID:
        return False
    if request.get("category_key") != MX_WIG_CATEGORY:
        return False
    if request.get("market") != MX_WIG_MARKET:
        return False
    snapshot = dict(request.get("recipe_snapshot") or {})
    spec = snapshot.get("recipe_spec_json")
    if isinstance(spec, str):
        # Frozen recipe rows keep recipe_spec_json as a serialized JSON string.
        try:
            spec = json.loads(spec)
        except json.JSONDecodeError:
            return False
    spec = dict(spec or {})
    return (
        spec.get("execution_flow") == MX_WIG_CHOICE_FLOW
        and spec.get("media_kind") == "native_photo"
        and spec.get("category_key") == MX_WIG_CATEGORY
        and MX_WIG_MARKET in list(spec.get("markets") or [])
    )


def batch_is_mx_wig_choice(manifest: Mapping[str, Any]) -> bool:
    """True only for frozen batches whose entries carry the MX wig flow."""
    entries = list((manifest or {}).get("entries") or [])
    return bool(entries) and all(
        request_is_mx_wig_choice(entry.get("request") or {}) for entry in entries
    )
