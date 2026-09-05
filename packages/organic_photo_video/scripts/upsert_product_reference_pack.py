#!/usr/bin/env python3
"""Create or preview one operator-owned OPV product reference pack."""

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
from services.product_reference_resolver import ProductReferenceResolver  # noqa: E402


def parse_asset(value: str):
    if "=" not in value:
        raise argparse.ArgumentTypeError("--asset must be ROLE=/absolute/path")
    role, raw_path = value.split("=", 1)
    path = Path(raw_path).expanduser().resolve()
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"image not found: {path}")
    return role.strip(), str(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--product-name", required=True)
    parser.add_argument("--category", default="")
    parser.add_argument("--variant-key", default="default")
    parser.add_argument("--asset", action="append", type=parse_asset, required=True)
    parser.add_argument("--default", action="store_true")
    parser.add_argument("--source-type", default="operator_import")
    parser.add_argument("--source-ref", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    repository = RdsRepository.from_env()
    roles = [item[0] for item in args.asset]
    paths = [item[1] for item in args.asset]
    pack = ProductReferenceResolver(repository).build_pack(
        product_id=args.product_id,
        product_name=args.product_name,
        category=args.category,
        references=paths,
        variant_key=args.variant_key,
        explicit_roles=roles,
        is_default=args.default,
        source_type=args.source_type,
        source_ref=args.source_ref,
        persist=args.apply,
    )
    print(json.dumps({
        "mode": "apply" if args.apply else "dry_run",
        "pack": {
            "pack_id": pack.pack_id,
            "product_id": pack.product_id,
            "variant_key": pack.variant_key,
            "pack_version": pack.pack_version,
            "status": pack.status,
            "is_default": pack.is_default,
            "asset_fingerprint": pack.asset_fingerprint,
            "assets": pack.assets_json,
        },
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
