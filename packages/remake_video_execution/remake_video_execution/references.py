"""Read-only product pack resolution and immutable local reference snapshots."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .contracts import SourceSnapshot


def resolve_pack(repo: Any, product_id: str, *, market: str = "", sku_id: str = "DEFAULT",
                 pack_id: str = "") -> dict:
    # Do not call get_active_images: it also provisions production tables.
    if pack_id:
        pack = repo.get("product_reference_image_packs", "reference_image_pack_id", pack_id)
        if not pack or str(pack.get("product_id")) != product_id:
            raise ValueError("REFERENCE_PACK_PRODUCT_MISMATCH")
        if (market and pack.get("market") != market) or pack.get("sku_id") != sku_id:
            raise ValueError("REFERENCE_PACK_SCOPE_MISMATCH")
    else:
        where = "product_id=? AND sku_id=? AND status='active'"
        params = [product_id, sku_id]
        if market:
            where += " AND market=?"
            params.append(market)
        packs = repo.list_where("product_reference_image_packs", where + " ORDER BY version DESC", tuple(params))
        if not packs:
            raise ValueError("REFERENCE_PACK_NOT_FOUND")
        if len({p.get("market") for p in packs}) > 1:
            raise ValueError("REFERENCE_PACK_MARKET_AMBIGUOUS")
        pack = packs[0]
    images = repo.list_where("product_reference_images",
                            "reference_image_pack_id=? AND status='active' ORDER BY image_index",
                            (pack["reference_image_pack_id"],))
    if not images or (pack.get("image_count") and len(images) != int(pack["image_count"])):
        raise ValueError("REFERENCE_PACK_IMAGES_INCOMPLETE")
    if any(not i.get("file_hash") or not i.get("object_key") for i in images):
        raise ValueError("REFERENCE_PACK_IMAGE_IDENTITY_MISSING")
    return {"pack": pack, "images": images}


def freeze_pack(source: SourceSnapshot, resolved: dict, oss: Any, output_dir: Path) -> SourceSnapshot:
    pack = resolved["pack"]
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for index, image in enumerate(resolved["images"], 1):
        digest = str(image["file_hash"]).lower()
        path = output_dir / f"{index:02d}-{digest}{Path(image['object_key']).suffix}"
        if not path.is_file():
            result = oss.download(image["object_key"], path)
            if not result.success:
                raise RuntimeError("REFERENCE_PACK_DOWNLOAD_FAILED")
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != digest:
            raise ValueError("REFERENCE_PACK_IMAGE_HASH_MISMATCH")
        manifest.append({"role": "PRODUCT_AUTHORITY", "image_role": image["image_role"],
                         "reference_image_id": image["reference_image_id"],
                         "image_index": image["image_index"], "sha256": actual,
                         "object_key": image["object_key"], "path": str(path.resolve())})
    identity = {"reference_image_pack_id": pack["reference_image_pack_id"],
                "reference_image_version": int(pack["version"]),
                "product_id": str(pack["product_id"]), "market": pack["market"],
                "sku_id": pack["sku_id"], "images": [{k: v for k, v in i.items() if k != "path"} for i in manifest]}
    selection = {"provider": "amc", "group_id": "amc:" + pack["reference_image_pack_id"],
                 "source_record_id": "", "content_fingerprint": hashlib.sha256(
                     "\n".join(sorted({i["sha256"] for i in manifest})).encode()).hexdigest(),
                 "content_version_hash": hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
                 "status": "FROZEN"}
    revision = hashlib.sha256(json.dumps({"source_revision_hash": source.source_revision_hash,
                                         "reference_pack": identity}, sort_keys=True).encode()).hexdigest()
    return SourceSnapshot(**{**source.to_dict(), "product_id": identity["product_id"],
                            "reference_image_pack_id": identity["reference_image_pack_id"],
                            "reference_image_version": identity["reference_image_version"],
                            "reference_selection": selection,
                            "reference_manifest": [i for i in source.reference_manifest
                                                   if i.get("role") != "PRODUCT_AUTHORITY"] + manifest,
                            "source_revision_hash": revision})


def frozen_product_paths(source: SourceSnapshot) -> list[str]:
    images = [i for i in source.reference_manifest if i.get("role") == "PRODUCT_AUTHORITY"]
    if not images:
        raise ValueError("FROZEN_PRODUCT_REFERENCES_MISSING")
    for image in images:
        path = Path(image.get("path") or "")
        legacy = not source.reference_image_pack_id and not source.reference_selection
        if not path.is_file() or (not (legacy and not image.get("sha256")) and
                                 hashlib.sha256(path.read_bytes()).hexdigest() != image.get("sha256")):
            raise ValueError("FROZEN_PRODUCT_REFERENCE_CHANGED")
    return [i["path"] for i in images]


def resolve_production_references(source: SourceSnapshot, output_dir: Path, *, product_id: str = "",
                                  market: str = "", sku_id: str = "DEFAULT", pack_id: str = "",
                                  group_id: str = "", reference_source: str = "auto",
                                  candidate_source: Any = None, amc_resolver: Any = None) -> SourceSnapshot:
    if reference_source not in {"auto", "operation", "amc"}:
        raise ValueError("REFERENCE_SOURCE_INVALID")
    selection = source.reference_selection
    legacy_frozen = any(i.get("role") == "PRODUCT_AUTHORITY" and i.get("path") for i in source.reference_manifest)
    if source.reference_image_pack_id or selection.get("status") == "FROZEN" or (legacy_frozen and not selection):
        if pack_id and pack_id != source.reference_image_pack_id:
            raise ValueError("FROZEN_REFERENCE_PACK_CHANGED_REPLAN_REQUIRED")
        if product_id and source.product_id != product_id:
            raise ValueError("REFERENCE_PACK_PRODUCT_MISMATCH")
        if group_id and group_id != selection.get("group_id"):
            raise ValueError("FROZEN_REFERENCE_GROUP_CHANGED_REPLAN_REQUIRED")
        frozen_product_paths(source)
        return source
    from .reference_sources import operation_source, discover_groups, select_group, freeze_group
    product_id = product_id or source.product_id
    if not product_id:
        raise ValueError("REFERENCE_PRODUCT_ID_REQUIRED")
    bound = str(selection.get("group_id") or source.structured_source.get("reference_group_id") or "")
    requested = bound or group_id
    if requested and not requested.startswith(("operation:", "amc:")):
        raise ValueError("REFERENCE_GROUP_ID_INVALID")
    # Bound source identity outranks command-line preferences.
    use_operation = requested.startswith("operation:") or (not requested and not pack_id and reference_source != "amc")
    if use_operation:
        groups = discover_groups(candidate_source if candidate_source is not None else operation_source(), product_id)
        group = select_group(groups, bound_group=bound, explicit_group=group_id)
        if group is not None:
            return freeze_group(source, group, output_dir)
        if reference_source == "operation":
            raise ValueError("REFERENCE_GROUP_NOT_FOUND")
    if requested.startswith("amc:"):
        pack_id = requested[len("amc:"):]
    resolver = amc_resolver or _resolve_amc_references
    return resolver(source, output_dir, product_id=product_id, market=market, sku_id=sku_id, pack_id=pack_id)


def _resolve_amc_references(source: SourceSnapshot, output_dir: Path, *, product_id: str,
                            market: str, sku_id: str, pack_id: str) -> SourceSnapshot:
    import sys
    workspace = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(workspace / "auto_mixcut"))
    sys.path.insert(0, str(workspace))
    from workspace_support import load_repo_env
    load_repo_env([workspace / "auto_mixcut" / ".env.local", workspace / "auto_mixcut" / ".env"])
    from auto_mixcut.core.config import Settings
    from auto_mixcut.adapters.repository import MySQLRepository
    from auto_mixcut.adapters.oss import build_oss
    settings = Settings.load()
    if not settings.database_url:
        raise ValueError("PRODUCTION_REFERENCE_DATABASE_UNAVAILABLE")
    if not (settings.aliyun_oss_endpoint and settings.aliyun_access_key_id and settings.aliyun_access_key_secret):
        raise ValueError("PRODUCTION_REFERENCE_OSS_UNAVAILABLE")
    # This adapter targets production packs, never auto_mixcut's local dev OSS.
    settings.oss_provider = "aliyun"
    resolved = resolve_pack(MySQLRepository.from_url(settings.database_url), product_id or source.product_id,
                            market=market, sku_id=sku_id, pack_id=pack_id)
    return freeze_pack(source, resolved, build_oss(settings), output_dir)
