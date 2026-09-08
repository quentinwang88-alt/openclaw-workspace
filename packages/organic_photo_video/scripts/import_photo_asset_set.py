#!/usr/bin/env python3
"""Validate and optionally import a reusable native-photo asset set.

Input JSON uses ``opv-asset-set-v1`` and keeps human labels beside local file
paths.  SHA256 values are calculated from the files at import time, so an
enabled set cannot silently point at changed bytes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402
from domain.models import AssetSet  # noqa: E402
from services.asset_set_service import AssetSetService, validate_asset_set  # noqa: E402


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize(path: Path) -> AssetSet:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "opv-asset-set-v1":
        raise ValueError("asset set schema_version must be opv-asset-set-v1")
    assets = []
    for index, raw in enumerate(payload.get("assets") or [], 1):
        source = Path(str(raw.get("path") or "")).expanduser()
        if not source.is_absolute():
            source = (path.parent / source).resolve()
        if not source.is_file():
            raise FileNotFoundError(f"asset {index} does not exist: {source}")
        assets.append({
            "asset_id": str(raw.get("asset_id") or f"asset_{index:02d}"),
            "path": str(source.resolve()), "sha256": _hash(source),
            "role": str(raw.get("role") or "source"),
            "tags": dict(raw.get("tags") or {}),
            **({"display_label": raw["display_label"]} if "display_label" in raw else {}),
        })
    result = AssetSet(
        asset_set_id=str(payload.get("asset_set_id") or ""),
        asset_set_key=str(payload.get("asset_set_key") or ""),
        asset_set_version=int(payload.get("asset_set_version") or 1),
        category_key=str(payload.get("category_key") or ""),
        market=(str(payload.get("market") or "").upper() or None),
        status=str(payload.get("status") or "draft"),
        tags_json=dict(payload.get("tags") or {}),
        manifest_json={"assets": assets, "pairs": list(payload.get("pairs") or []),
                       **({"content_approval": payload["content_approval"]} if "content_approval" in payload else {})},
    )
    validate_asset_set(result, verify_files=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_json", type=Path)
    parser.add_argument("--apply", action="store_true", help="write the validated set to RDS")
    args = parser.parse_args()
    asset_set = normalize(args.input_json.resolve())
    output = {
        "mode": "apply" if args.apply else "dry-run",
        "asset_set_id": asset_set.asset_set_id,
        "category_key": asset_set.category_key, "market": asset_set.market,
        "status": asset_set.status,
        "asset_count": len(asset_set.manifest_json["assets"]),
        "assets": asset_set.manifest_json["assets"],
    }
    if args.apply:
        load_repo_env()
        from repositories.rds_repository import RdsRepository
        AssetSetService(RdsRepository.from_env()).save(asset_set)
        output["written"] = True
    else:
        output["written"] = False
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
