"""Schema-aware recipe binding checks.

Review fix (2026-09-13), P0-1.  Until now every consumer assumed a native-photo
Recipe names its own ``category_key`` and its own ``markets`` list — that is the
``opv-photo-recipe-v1`` contract, and ``PHOTO_TH_TRAVEL_OUTFIT_V2`` still is one.
``opv-photo-recipe-v2`` deliberately drops both fields:

* the **market** is owned by the Market Pack (`market_policy`), so one recipe can
  serve several countries without being edited;
* the **category** is bound by *capability* rather than by name
  (`required_category_capabilities`), so one recipe can serve several categories
  whose adapters declare the same capabilities.

Consumers that still compared the literal fields therefore rejected every v2
Recipe before it could reach the pipeline.  The split lives here, in one place,
so the request factory and the planner cannot drift apart.

Nothing here reads files, environment or the network.
"""
from __future__ import annotations

from typing import Any, List, Mapping, Optional

from domain.contracts import (
    PHOTO_RECIPE_SPEC_SCHEMA_VERSION as PHOTO_RECIPE_V1_SCHEMA_VERSION,
    PHOTO_RECIPE_V2_SCHEMA_VERSION,
)
from services.photo_category_registry import (
    UnknownPhotoCategoryError,
    get_photo_category_adapter,
)

__all__ = [
    "PHOTO_RECIPE_V1_SCHEMA_VERSION",
    "PHOTO_RECIPE_V2_SCHEMA_VERSION",
    "MARKET_PACK_REQUIRED",
    "MARKET_PACK_OPTIONAL",
    "recipe_schema_version",
    "is_v2_recipe",
    "is_supported_photo_recipe",
    "category_binding_errors",
    "market_binding_errors",
    "product_mode_errors",
]

#: The recipe cannot be planned at all until an active Market Pack covers the
#: requested market.  This is the fail-loud value a country-agnostic recipe
#: must declare; ``MARKET_PACK_OPTIONAL`` exists so a future recipe can opt out
#: deliberately instead of by omission.
MARKET_PACK_REQUIRED = "MARKET_PACK_REQUIRED"
MARKET_PACK_OPTIONAL = "MARKET_PACK_OPTIONAL"

SUPPORTED_SCHEMAS = (PHOTO_RECIPE_V1_SCHEMA_VERSION, PHOTO_RECIPE_V2_SCHEMA_VERSION)


def recipe_schema_version(recipe_spec: Optional[Mapping[str, Any]]) -> str:
    return str((recipe_spec or {}).get("schema_version") or "")


def is_v2_recipe(recipe_spec: Optional[Mapping[str, Any]]) -> bool:
    """True for the country-agnostic contract."""
    return recipe_schema_version(recipe_spec) == PHOTO_RECIPE_V2_SCHEMA_VERSION


def is_supported_photo_recipe(recipe_spec: Optional[Mapping[str, Any]]) -> bool:
    return recipe_schema_version(recipe_spec) in SUPPORTED_SCHEMAS


def category_binding_errors(
    recipe_spec: Optional[Mapping[str, Any]],
    *,
    category_key: str,
    recipe_id: str = "",
) -> List[str]:
    """Whether this Recipe may serve ``category_key``.

    v1: literal ``category_key`` equality (unchanged behaviour).
    v2: every ``required_category_capabilities`` entry must be advertised by the
    adapter registered for ``category_key``.  The category *name* is therefore
    free to differ between markets — a scarf and a garment can share one recipe
    as long as their adapters expose the same capabilities.
    """
    spec = dict(recipe_spec or {})
    prefix = f"{recipe_id}: " if recipe_id else ""
    if not is_v2_recipe(spec):
        if spec.get("category_key") != category_key:
            return [prefix + "recipe category does not match the requested category"]
        return []
    required = [str(item) for item in (spec.get("required_category_capabilities") or [])]
    if not required:
        return [
            prefix
            + "v2 recipe must declare required_category_capabilities; "
            "a capability-free recipe cannot be bound to a category"
        ]
    try:
        adapter = get_photo_category_adapter(category_key)
    except UnknownPhotoCategoryError as exc:
        return [prefix + str(exc)]
    missing = [name for name in required if name not in adapter.capabilities]
    if missing:
        return [
            prefix
            + f"category adapter {adapter.category_key!r} is missing required "
            "capabilities: " + ", ".join(missing)
        ]
    return []


def market_binding_errors(
    recipe_spec: Optional[Mapping[str, Any]],
    *,
    market: str,
    locale: str = "",
    repository: Any = None,
    account_id: str = "",
    market_pack: Any = None,
) -> List[str]:
    """Whether this Recipe may serve ``market``.

    v1: literal ``markets`` list (unchanged behaviour).
    v2: ``market_policy`` decides.  ``MARKET_PACK_REQUIRED`` needs a Market Pack
    that is **active** and whose country/locale match the request, otherwise the
    recipe would silently become market-bound.  The pack is resolved either from
    an explicitly supplied ``market_pack`` (the planner already loaded it) or
    from the account's ``default_market_pack_id``.

    A caller that can supply neither is rejected: an unverifiable market claim is
    exactly what this check exists to prevent.
    """
    spec = dict(recipe_spec or {})
    if not is_v2_recipe(spec):
        if market not in (spec.get("markets") or []):
            return ["recipe does not support the requested market"]
        return []
    policy = str(spec.get("market_policy") or "")
    if not policy:
        return ["v2 recipe must declare market_policy"]
    if policy != MARKET_PACK_REQUIRED:
        # ``MARKET_PACK_OPTIONAL``: the recipe states it is market-independent.
        return []
    if market_pack is None:
        if repository is None or not account_id:
            return [
                "MARKET_PACK_REQUIRED: 缺少账号或其 Market Pack，无法校验市场"
            ]
        account = repository.get_account_profile(account_id)
        pack_id = str(getattr(account, "default_market_pack_id", "") or "")
        if account is None or not pack_id:
            return [
                "MARKET_PACK_REQUIRED: 账号未绑定 Market Pack，无法校验市场"
            ]
        market_pack = repository.get_market_pack(pack_id)
        if market_pack is None:
            return [f"MARKET_PACK_REQUIRED: Market Pack 不存在：{pack_id}"]
    status = str(getattr(market_pack, "status", "") or "")
    country = str(getattr(market_pack, "target_country", "") or "")
    pack_locale = str(getattr(market_pack, "target_locale", "") or "")
    pack_id = str(getattr(market_pack, "market_pack_id", "") or "")
    if status != "active":
        return [
            f"MARKET_PACK_REQUIRED: Market Pack {pack_id} 状态为 {status or '未知'}，"
            "未启用前不得进入生产链路"
        ]
    if country != market:
        return [
            f"MARKET_PACK_REQUIRED: Market Pack {pack_id} 面向 {country}，"
            f"与请求市场 {market} 不符"
        ]
    if locale and pack_locale and pack_locale != locale:
        return [
            f"MARKET_PACK_REQUIRED: Market Pack {pack_id} 发布语言为 {pack_locale}，"
            f"与请求语言 {locale} 不符"
        ]
    return []


def product_mode_errors(
    recipe_spec: Optional[Mapping[str, Any]], *, product_mode: str
) -> List[str]:
    """``product_modes`` is shared by both schemas."""
    if product_mode not in (dict(recipe_spec or {}).get("product_modes") or []):
        return ["recipe does not support the requested product mode"]
    return []
