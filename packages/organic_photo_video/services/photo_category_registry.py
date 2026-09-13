"""Category Adapter registry for the native photo pipeline.

VN scarf cross-market work, Phase 1.  This module is deliberately a
*relocation*, not a redesign: everything the womenswear adapter exposes is
copied verbatim from the three ad-hoc category tables that used to live inline
in the pipeline.  The point of Phase 1 is that TH V2 output does not change,
so any table that disagrees with another table is preserved as-is and recorded
here instead of being silently "tidied".

Provenance of every table:

``product_slot_by_category``
    ``services/photo_reference_vision.py`` ``_normalize_travel_plan`` —
    ``target_fields = {"outerwear": ("outerwear", "外套"), ...}``.
    Drives ``look[slot] = f"指定商品{label}（以商品参考图为准）"``.
    Lookup key: ``str(category).strip().lower()``.

``product_display_label_by_category``
    ``services/image_generator.py`` ``product_label = {...}.get(cat, "目标商品")``.
    **Diverges from the table above**: shorter key set (no shoes aliases) and a
    ``目标`` prefix.  ``shoes`` therefore yields ``目标商品`` there while the
    planning table yields ``指定商品鞋履（以商品参考图为准）``.  Kept as-is.

``product_reference_priority_by_slot``
    ``services/product_reference_resolver.py``
    ``select_product_references_for_slot`` — the per-role ``role_order`` map,
    with the same fallback order for unknown roles.

``identity_attributes`` / ``required_product_roles``
    The implicit contract already asserted by the generator prompt
    (``image_generator.py`` 【指定商品身份锁】: 颜色、图案、材质观感、形状和结构)
    and by the recipe ``asset_requirements.required_roles``.

Nothing in this module reads files, environment or the network.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple


class UnknownPhotoCategoryError(LookupError):
    """Raised when a category key has no registered adapter."""


@dataclass(frozen=True)
class PhotoCategoryAdapter:
    """Everything the pipeline needs to know about one product category."""

    category_key: str
    capabilities: Tuple[str, ...]
    accepted_product_categories: Tuple[str, ...]
    main_product_slot: str
    product_label_zh: str
    required_product_roles: Tuple[str, ...]
    product_reference_priority_by_slot: Mapping[str, Tuple[str, ...]]
    identity_attributes: Tuple[str, ...]
    # --- legacy-exact tables (see module docstring) -------------------------
    product_slot_by_category: Mapping[str, Tuple[str, str]] = field(default_factory=dict)
    product_display_label_by_category: Mapping[str, str] = field(default_factory=dict)
    default_product_display_label_zh: str = "目标商品"
    #: Which product categories *are* the garment that owns a slot.
    #:
    #: This is deliberately narrower than ``product_slot_by_category``: the
    #: planning side puts ``dress`` into the outerwear slot, but the generation
    #: side only lets an ``outerwear`` product take over that slot. Phase 1
    #: preserves that discrepancy instead of harmonising it.
    product_own_category_by_slot: Mapping[str, Tuple[str, ...]] = field(
        default_factory=dict)


#: Product fields copied into ``look["target_product"]``.
#: Provenance: ``photo_reference_vision._normalize_travel_plan``.
TARGET_PRODUCT_FIELDS: Tuple[str, ...] = (
    "product_id", "product_name", "category", "reference_pack_id",
    "reference_pack_version",
)

#: Default per-role product reference order for roles without an entry.
#: Provenance: ``product_reference_resolver.select_product_references_for_slot``.
DEFAULT_ROLE_PRIORITY: Tuple[str, ...] = ("front", "side", "back", "lifestyle", "detail")


WOMENSWEAR_V1 = PhotoCategoryAdapter(
    category_key="womenswear",
    capabilities=(
        "wearable_styling",
        "travel_look",
        "product_embedding",
        "layering_progression",
    ),
    accepted_product_categories=(
        "outerwear", "top", "bottom", "dress", "shoes", "shoe", "footwear",
    ),
    main_product_slot="outerwear",
    product_label_zh="目标商品",
    required_product_roles=("look_a", "look_b", "look_c", "look_d"),
    product_reference_priority_by_slot={
        "hero": ("front", "side", "back", "lifestyle"),
        "full_look": ("front", "back", "side"),
        "lifestyle": ("front", "lifestyle", "side"),
        "detail": ("front", "detail"),
        "second_angle": ("front", "back", "side", "lifestyle"),
    },
    identity_attributes=(
        "dominant_color",
        "pattern",
        "material_appearance",
        "silhouette",
        "length",
        "collar",
        "placket",
        "cuff",
    ),
    product_slot_by_category={
        "outerwear": ("outerwear", "外套"),
        "top": ("top_inner", "上装"),
        "bottom": ("bottom", "下装"),
        "dress": ("outerwear", "连衣裙"),
        "shoes": ("shoes", "鞋履"),
        "shoe": ("shoes", "鞋履"),
        "footwear": ("shoes", "鞋履"),
    },
    product_display_label_by_category={
        "outerwear": "目标外套",
        "dress": "目标连衣裙",
        "top": "目标上装",
        "bottom": "目标下装",
    },
    product_own_category_by_slot={
        "outerwear": ("outerwear",),
    },
)


_ADAPTERS: Dict[str, PhotoCategoryAdapter] = {
    WOMENSWEAR_V1.category_key: WOMENSWEAR_V1,
}


def _build_product_category_index() -> Dict[str, PhotoCategoryAdapter]:
    index: Dict[str, PhotoCategoryAdapter] = {}
    for adapter in _ADAPTERS.values():
        for token in adapter.accepted_product_categories:
            index.setdefault(token, adapter)
    return index


_PRODUCT_CATEGORY_INDEX: Dict[str, PhotoCategoryAdapter] = _build_product_category_index()


def get_photo_category_adapter(category_key: str) -> PhotoCategoryAdapter:
    """Return the registered adapter for a recipe/config category key.

    Raises:
        UnknownPhotoCategoryError: when nothing is registered for that key.
    """
    token = str(category_key or "").strip().lower()
    adapter = _ADAPTERS.get(token)
    if adapter is None:
        raise UnknownPhotoCategoryError("未注册的类目适配器：" + (token or "（空）"))
    return adapter


def registered_category_keys() -> Tuple[str, ...]:
    return tuple(sorted(_ADAPTERS))


def adapter_for_product_category(product_category: Any) -> Optional[PhotoCategoryAdapter]:
    """Return the adapter that accepts a *product* category, or None.

    ``None`` means "no adapter claims this category"; callers must then fall
    back to their pre-adapter behaviour. This is how the wig path keeps its
    current output while womenswear migrates.
    """
    token = str(product_category or "").strip().lower()
    return _PRODUCT_CATEGORY_INDEX.get(token) if token else None


def resolve_product_slot(
    adapter: Optional[PhotoCategoryAdapter], category_token: str,
) -> Optional[Tuple[str, str]]:
    """Map a *normalized* product category to ``(slot, label_zh)``.

    The caller owns normalization, on purpose: the planning site uses
    ``strip().lower()`` while the generation site uses ``lower()`` only, and
    unifying them here would change behaviour for padded input.
    """
    if adapter is None:
        return None
    return adapter.product_slot_by_category.get(str(category_token or ""))


def resolve_product_display_label(category_token: str) -> str:
    """Map a *normalized* product category to the generator's display label."""
    adapter = _PRODUCT_CATEGORY_INDEX.get(str(category_token or ""))
    if adapter is None:
        return "目标商品"
    return adapter.product_display_label_by_category.get(
        str(category_token or ""), adapter.default_product_display_label_zh,
    )


def role_priority_for_slot(
    adapter: Optional[PhotoCategoryAdapter], slot_role: Any,
) -> Tuple[str, ...]:
    """Per-role product reference order, with the shared fallback."""
    if adapter is not None:
        found = adapter.product_reference_priority_by_slot.get(str(slot_role))
        if found:
            return tuple(found)
    return DEFAULT_ROLE_PRIORITY


def product_owns_slot(
    adapter: Optional[PhotoCategoryAdapter], slot: str, category_token: str,
) -> bool:
    """Whether this product category *is* the garment that owns ``slot``.

    Distinct from :func:`resolve_product_slot`, which answers "where does this
    category get written". Generation-side code needs the former: a dress is
    planned into the outerwear slot but must not be treated as an outerwear
    garment when overriding ``outfit_state``.

    The caller owns normalization, as elsewhere in this module.
    """
    if adapter is None:
        return False
    owned = adapter.product_own_category_by_slot.get(str(slot)) or ()
    return str(category_token or "") in owned


def apply_target_product_to_look(
    *, adapter: Optional[PhotoCategoryAdapter], look: Mapping[str, Any],
    product_snapshot: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Freeze the operator's product into one planned look.

    Reproduces ``photo_reference_vision._normalize_travel_plan`` exactly:

    * ``target_product`` is written whenever a product id is present, even for
      a category no adapter claims.
    * the garment slot override only happens when the category maps to a slot.

    ``adapter=None`` means "pick one from the product's own category"; pass an
    explicit adapter when the recipe's category key is authoritative.
    """
    normalized = dict(look or {})
    product = dict(product_snapshot or {})
    if not str(product.get("product_id") or ""):
        return normalized
    normalized["target_product"] = {
        key: product.get(key) for key in TARGET_PRODUCT_FIELDS
        if product.get(key) not in (None, "")
    }
    category = str(product.get("category") or "").strip().lower()
    resolved = adapter if adapter is not None else adapter_for_product_category(category)
    target = resolve_product_slot(resolved, category)
    if target:
        normalized[target[0]] = f"指定商品{target[1]}（以商品参考图为准）"
    return normalized


def build_product_qa_contract(
    *, adapter: PhotoCategoryAdapter,
    product_snapshot: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Machine-readable form of the product identity lock.

    Mirrors what the generator prompt already asserts under
    【指定商品身份锁】. Phase 1 only *describes* that contract -- no caller
    consumes it yet, so wiring it cannot change today's output. Phase 3 uses it
    for the scarf QA fields.
    """
    product = dict(product_snapshot or {})
    return {
        "schema_version": "opv-photo-product-qa-v1",
        "category_key": adapter.category_key,
        "product_id": str(product.get("product_id") or ""),
        "product_label_zh": adapter.product_label_zh,
        "main_product_slot": adapter.main_product_slot,
        "accepted_product_categories": list(adapter.accepted_product_categories),
        "identity_attributes": list(adapter.identity_attributes),
        "required_product_roles": list(adapter.required_product_roles),
        "must_keep": [
            "颜色", "图案", "材质观感", "形状与结构",
            "版型", "衣长", "领型", "前襟", "袖口",
        ],
        "forbidden": [
            "替换成相似款", "重新设计指定商品",
            "用文案覆盖商品参考图的颜色和材质",
        ],
        "reference_pack_id": str(product.get("reference_pack_id") or ""),
        "reference_pack_version": product.get("reference_pack_version"),
    }
