from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from auto_mixcut.core.result import Result

from .context import SkillContext


@dataclass(frozen=True)
class ProductIdentity:
    product_id: str
    canonical_product_id: str
    resolution: str
    alias_id: str = ""


class ProductIdentityResolverSkill:
    """Resolve store/market execution products to one global material identity."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def resolve(
        self,
        *,
        product_id: str = "",
        canonical_product_id: str = "",
        local_product_id: str = "",
        store_id: str = "",
        market: str = "",
    ) -> ProductIdentity:
        product_id = _text(product_id)
        explicit = _text(canonical_product_id)
        if explicit:
            return ProductIdentity(product_id=product_id or explicit, canonical_product_id=explicit, resolution="explicit")

        if product_id:
            product = self.ctx.repo.get("products", "product_id", product_id) or {}
            product_canonical = _text(product.get("canonical_product_id"))
            if product_canonical:
                return ProductIdentity(product_id=product_id, canonical_product_id=product_canonical, resolution="product")
            alias = self._first_alias("product_id=? AND status='active' ORDER BY id", (product_id,))
            if alias:
                return _identity(product_id, alias, "product_alias")
            if product:
                return ProductIdentity(product_id=product_id, canonical_product_id=product_id, resolution="existing_product")

        local_product_id = _text(local_product_id)
        if local_product_id:
            where = "local_product_id=? AND status='active'"
            params: list[Any] = [local_product_id]
            if _text(store_id):
                where += " AND (store_id=? OR COALESCE(store_id,'')='')"
                params.append(_text(store_id))
            if _text(market):
                where += " AND (market=? OR COALESCE(market,'')='')"
                params.append(_text(market))
            alias = self._first_alias(where + " ORDER BY id", tuple(params))
            if alias:
                return _identity(product_id or _text(alias.get("product_id")), alias, "local_alias")

        return ProductIdentity(product_id=product_id, canonical_product_id="", resolution="unresolved")

    def bind_alias(
        self,
        canonical_product_id: str,
        *,
        product_id: str = "",
        local_product_id: str = "",
        store_id: str = "",
        market: str = "",
        alias_type: str = "product_id",
        alias_value: str = "",
        source: str = "manual",
    ) -> Result:
        canonical_product_id = _text(canonical_product_id)
        if not canonical_product_id:
            return Result.fail("CANONICAL_PRODUCT_REQUIRED", "canonical_product_id is required")
        alias_value = _text(alias_value) or _text(product_id) or _text(local_product_id)
        seed = "|".join([canonical_product_id, alias_type, alias_value, _text(store_id), _text(market)])
        alias_id = f"PALIAS_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:24].upper()}"
        row = {
            "alias_id": alias_id,
            "canonical_product_id": canonical_product_id,
            "product_id": _text(product_id),
            "local_product_id": _text(local_product_id),
            "store_id": _text(store_id),
            "market": _text(market),
            "alias_type": _text(alias_type) or "product_id",
            "alias_value": alias_value,
            "status": "active",
            "source": _text(source) or "manual",
        }
        written = self.ctx.repo.upsert("product_identity_aliases", "alias_id", row)
        if not written.success:
            return written
        if _text(product_id) and self.ctx.repo.get("products", "product_id", _text(product_id)):
            self.ctx.repo.update(
                "products",
                "product_id",
                _text(product_id),
                {"canonical_product_id": canonical_product_id},
            )
        return Result.ok(row)

    def _first_alias(self, where: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        try:
            rows = self.ctx.repo.list_where("product_identity_aliases", where, params)
        except Exception:
            return None
        return rows[0] if rows else None


def _identity(product_id: str, alias: dict[str, Any], resolution: str) -> ProductIdentity:
    return ProductIdentity(
        product_id=product_id or _text(alias.get("product_id")),
        canonical_product_id=_text(alias.get("canonical_product_id")),
        resolution=resolution,
        alias_id=_text(alias.get("alias_id")),
    )


def _text(value: Any) -> str:
    return str(value or "").strip()
