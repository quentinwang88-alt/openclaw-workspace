#!/usr/bin/env python3
"""Import all active auto_mixcut SKU image packs into OPV reference packs."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
AUTO_MIXCUT_ROOT = WORKSPACE_ROOT / "auto_mixcut"
for value in (str(WORKSPACE_ROOT), str(AUTO_MIXCUT_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()
# This bridge is specifically for the shared production image-pack tables.
# Keep an explicit caller setting authoritative, but do not silently fall back
# to auto_mixcut's local SQLite development database.
if os.environ.get("LIKEU_AI_DATABASE_URL") or os.environ.get("AUTO_MIXCUT_DATABASE_URL"):
    os.environ.setdefault("AUTO_MIXCUT_DB_PROVIDER", "mysql")

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.product_reference_resolver import ProductReferenceResolver  # noqa: E402


ROLE_MAP = {
    "main": "front", "primary": "front", "front": "front",
    "back": "back", "side": "side", "detail": "detail",
    "scene": "lifestyle", "lifestyle": "lifestyle",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--market", default="")
    parser.add_argument("--config", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    ctx = build_context(args.config or None)
    where = "product_id=? AND status='active'"
    params = [args.product_id]
    if args.market:
        where += " AND market=?"
        params.append(args.market)
    rows = ctx.repo.list_where(
        "product_reference_image_packs",
        where + " ORDER BY version DESC, id DESC",
        tuple(params),
    )
    latest = {}
    for row in rows:
        key = str(row.get("sku_id") or "DEFAULT")
        latest.setdefault(key, row)

    repository = RdsRepository.from_env()
    resolver = ProductReferenceResolver(repository)
    imported = []
    errors = []
    cache_root = (
        Path.home() / ".openclaw" / "shared" / "data"
        / "opv_reference_cache" / args.product_id
    )
    download_script = AUTO_MIXCUT_ROOT / "scripts" / "resolve_reference_image_pack.py"
    for sku_id, row in sorted(latest.items()):
        pack_id = str(row.get("reference_image_pack_id") or "")
        destination = cache_root / sku_id / f"v{int(row.get('version') or 1)}"
        command = [
            sys.executable, str(download_script),
            "--reference-image-pack-id", pack_id,
            "--output-dir", str(destination),
            "--no-anchor-import",
        ]
        if args.config:
            command.extend(["--config", args.config])
        completed = subprocess.run(command, text=True, capture_output=True)
        try:
            payload = json.loads((completed.stdout or "").strip().splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            payload = {"success": False, "error": (completed.stderr or completed.stdout)[-600:]}
        if completed.returncode or not payload.get("success"):
            errors.append({"reference_image_pack_id": pack_id, "detail": payload})
            continue
        images = list(payload.get("images") or [])
        paths = [str(item.get("local_path") or "") for item in images]
        roles = [ROLE_MAP.get(str(item.get("image_role") or "").lower(), "unknown") for item in images]
        variant_key = str(row.get("sku_label") or sku_id or "DEFAULT")
        pack = resolver.build_pack(
            product_id=args.product_id,
            product_name=args.product_id,
            category="",
            references=paths,
            explicit_roles=roles,
            variant_key=variant_key,
            is_default=(sku_id == "DEFAULT"),
            source_type="auto_mixcut_reference_pack",
            source_ref=pack_id,
            persist=args.apply,
        )
        imported.append({
            "source_pack_id": pack_id,
            "sku_id": sku_id,
            "variant_key": variant_key,
            "opv_pack_id": pack.pack_id,
            "image_count": len(pack.assets_json),
            "mode": "apply" if args.apply else "preview",
        })
    print(json.dumps({
        "product_id": args.product_id,
        "source_active_pack_count": len(latest),
        "imported": imported,
        "errors": errors,
    }, ensure_ascii=False, indent=2))
    return 2 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
