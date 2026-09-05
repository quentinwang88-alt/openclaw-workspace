"""Pure, token-independent identity for ordered reference image bytes."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
from typing import Any


ROLES = {"person_identity", "product", "composite_first_frame"}


def bytes_sha256(content: bytes) -> str:
    if not content:
        raise ValueError("empty reference image")
    return hashlib.sha256(content).hexdigest()


def build_reference_manifest(assets: list[dict[str, Any]]) -> dict[str, Any]:
    if not assets:
        raise ValueError("empty reference manifest")
    copied = deepcopy(assets)
    identity = []
    for index, asset in enumerate(copied, 1):
        if asset.get("index", index) != index or asset.get("role") not in ROLES:
            raise ValueError("reference role/order mismatch")
        asset["index"] = index
        digest = str(asset.get("original_sha256") or "")
        if not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("reference requires original bytes SHA256")
        identity.append({"index": index, "role": asset["role"], "original_sha256": digest})
    raw = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return {"schema_version": "2", "manifest_id": "sha256:" + hashlib.sha256(raw).hexdigest(), "reference_assets": copied}


def validate_reference_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    if str(manifest.get("schema_version")) != "2":
        raise ValueError("unsupported reference manifest version")
    rebuilt = build_reference_manifest(manifest.get("reference_assets") or [])
    if rebuilt["manifest_id"] != manifest.get("manifest_id"):
        raise ValueError("reference manifest hash mismatch")
    return rebuilt


def rebind_reference_manifest(manifest: dict[str, Any], token_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Rebind only to transfers whose bytes were verified by the caller."""
    result = validate_reference_manifest(manifest)
    for asset in result["reference_assets"]:
        previous = str(asset.get("file_token") or "")
        copied = token_map.get(previous)
        if copied is None:
            raise ValueError("reference transfer is missing")
        if copied.get("original_sha256") != asset["original_sha256"]:
            raise ValueError("reference transfer bytes changed")
        if not copied.get("file_token"):
            raise ValueError("reference transfer token missing")
        asset["source_file_token"] = previous
        asset["file_token"] = copied["file_token"]
    return result
