#!/usr/bin/env python3
"""Read-only report of the product pack and per-shot reference routing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402
from services.product_reference_resolver import (  # noqa: E402
    ProductReferenceResolver,
    has_detail_reference,
    select_product_references_for_slot,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("product_id")
    args = parser.parse_args()
    product = ProductReferenceResolver(
        RdsRepository.from_env()
    ).resolve_snapshot(args.product_id, allow_history_bootstrap=False)
    slots = {
        role: select_product_references_for_slot(product, role)
        for role in ("hero", "full_look", "lifestyle", "detail", "second_angle")
    }
    print(json.dumps({
        "product_id": args.product_id,
        "reference_pack_id": product.get("reference_pack_id"),
        "reference_pack_version": product.get("reference_pack_version"),
        "variant_key": product.get("variant_key"),
        "status": product.get("reference_status"),
        "selection_reason": product.get("selection_reason"),
        "visual_qa_policy": product.get("visual_qa_policy"),
        "has_detail_reference": has_detail_reference(product),
        "reference_roles": product.get("reference_roles"),
        "slot_references": slots,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
