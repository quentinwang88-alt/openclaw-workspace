from __future__ import annotations

import os
from typing import Any

from .context import SkillContext
from .product_identity_resolver_skill import ProductIdentityResolverSkill


def canonical_selection_enabled() -> bool:
    value = os.environ.get("MATERIAL_CANONICAL_PRODUCT_SELECTION_ENABLED", "0")
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def list_material_segments(ctx: SkillContext, product_id: str) -> list[dict[str, Any]]:
    if not canonical_selection_enabled():
        return ctx.repo.list_where("segments", "product_id=?", (product_id,))
    identity = ProductIdentityResolverSkill(ctx).resolve(product_id=product_id)
    canonical = identity.canonical_product_id
    if not canonical:
        return ctx.repo.list_where("segments", "product_id=?", (product_id,))
    execution_product_ids = {product_id}
    try:
        aliases = ctx.repo.list_where(
            "product_identity_aliases",
            "canonical_product_id=? AND status='active'",
            (canonical,),
        )
        execution_product_ids.update(
            str(row.get("product_id") or "").strip()
            for row in aliases
            if str(row.get("product_id") or "").strip()
        )
    except Exception:
        pass
    placeholders = ",".join("?" for _ in execution_product_ids)
    return ctx.repo.list_where(
        "segments",
        f"canonical_product_id=? OR (COALESCE(canonical_product_id,'')='' AND product_id IN ({placeholders}))",
        (canonical, *sorted(execution_product_ids)),
    )
