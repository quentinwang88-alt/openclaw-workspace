#!/usr/bin/env python3
"""Resolve or bind cross-market product aliases for the global material pool."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.product_identity_resolver_skill import ProductIdentityResolverSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["resolve", "bind"])
    parser.add_argument("--canonical-product-id", default="")
    parser.add_argument("--product-id", default="")
    parser.add_argument("--local-product-id", default="")
    parser.add_argument("--store-id", default="")
    parser.add_argument("--market", default="")
    parser.add_argument("--alias-type", default="product_id")
    parser.add_argument("--source", default="manual")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    resolver = ProductIdentityResolverSkill(ctx)
    if args.mode == "resolve":
        identity = resolver.resolve(
            product_id=args.product_id,
            canonical_product_id=args.canonical_product_id,
            local_product_id=args.local_product_id,
            store_id=args.store_id,
            market=args.market,
        )
        print(json.dumps(identity.__dict__, ensure_ascii=False, indent=2))
        return 0 if identity.canonical_product_id else 1

    if not args.apply:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "would_bind": {
                        "canonical_product_id": args.canonical_product_id,
                        "product_id": args.product_id,
                        "local_product_id": args.local_product_id,
                        "store_id": args.store_id,
                        "market": args.market,
                        "alias_type": args.alias_type,
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    migrated = RDSRepositorySkill(ctx).init_db()
    if not migrated.success:
        print(json.dumps(migrated.to_dict(), ensure_ascii=False, indent=2))
        return 1
    result = resolver.bind_alias(
        args.canonical_product_id,
        product_id=args.product_id,
        local_product_id=args.local_product_id,
        store_id=args.store_id,
        market=args.market,
        alias_type=args.alias_type,
        source=args.source,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
