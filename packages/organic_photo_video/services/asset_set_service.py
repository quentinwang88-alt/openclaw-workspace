"""Validate and deterministically select reusable native-photo asset sets."""

from __future__ import annotations

import hashlib
import copy
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from domain.models import AssetSet
from domain.photo_contracts import placeholder_errors


class AssetSetError(ValueError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_asset_set(asset_set: AssetSet, *, verify_files: bool = True) -> None:
    if not asset_set.asset_set_id or not asset_set.asset_set_key:
        raise AssetSetError("asset set id/key are required")
    if not asset_set.category_key:
        raise AssetSetError("asset set category_key is required")
    if type(asset_set.asset_set_version) is not int or asset_set.asset_set_version < 1:
        raise AssetSetError("asset set version must be a positive integer")
    if asset_set.status not in {"draft", "enabled", "disabled"}:
        raise AssetSetError("asset set status must be draft/enabled/disabled")
    assets = (asset_set.manifest_json or {}).get("assets")
    if not isinstance(assets, list) or not assets:
        raise AssetSetError("asset set manifest.assets must be non-empty")
    seen = set()
    for index, item in enumerate(assets):
        if not isinstance(item, Mapping):
            raise AssetSetError(f"asset {index} must be an object")
        asset_id = str(item.get("asset_id") or "").strip()
        path = Path(str(item.get("path") or "")).expanduser()
        expected = str(item.get("sha256") or "").strip()
        if not asset_id or asset_id in seen or len(expected) != 64:
            raise AssetSetError(f"asset {index} has missing/duplicate id or invalid SHA256")
        seen.add(asset_id)
        label = item.get("display_label")
        if label is not None:
            if not (isinstance(label, str) or (isinstance(label, Mapping)
                    and all(isinstance(k, str) and k and isinstance(v, str) for k, v in label.items()))):
                raise AssetSetError(f"asset {asset_id} display_label must be a string or locale/string map")
            if placeholder_errors(label):
                raise AssetSetError(f"asset {asset_id} display_label cannot contain placeholders")
        if verify_files and (not path.is_file() or _sha256(path) != expected):
            raise AssetSetError(f"asset {asset_id} file is missing or changed")
    pairs = (asset_set.manifest_json or {}).get("pairs") or []
    for pair in pairs:
        members = list(pair.get("asset_ids") or []) if isinstance(pair, Mapping) else []
        if len(members) < 2 or any(member not in seen for member in members):
            raise AssetSetError("asset set pair references missing assets")


class AssetSetService:
    def __init__(self, repository: Any):
        self.repository = repository

    def save(self, asset_set: AssetSet) -> AssetSet:
        validate_asset_set(asset_set, verify_files=asset_set.status == "enabled")
        existing = self.repository.get_asset_set(asset_set.asset_set_id)
        if existing:
            immutable = ("asset_set_key", "asset_set_version", "category_key", "market", "tags_json", "manifest_json")
            if any(getattr(existing, field) != getattr(asset_set, field) for field in immutable):
                raise AssetSetError("asset set content is immutable; use a new id/version (status retirement is allowed)")
        self.repository.upsert_asset_set(asset_set)
        return self.repository.get_asset_set(asset_set.asset_set_id) or asset_set

    @staticmethod
    def tags_match(available: Mapping[str, Any], required: Mapping[str, Any]) -> bool:
        def same(a, b):
            return type(a) is type(b) and a == b
        for key, value in required.items():
            actual = available.get(key)
            if isinstance(value, list):
                if not isinstance(actual, list) or not all(any(same(v, a) for a in actual) for v in value):
                    return False
            elif not same(actual, value) and not (isinstance(actual, list) and any(same(value, a) for a in actual)):
                return False
        return True

    def candidates(
        self, *, category_key: str, market: str,
        tags: Optional[Mapping[str, Any]] = None,
        asset_set_keys: Sequence[str] = (), requirements: Optional[Mapping[str, Any]] = None,
        verify_files: bool = True,
    ) -> list[AssetSet]:
        candidates = self.repository.list_asset_sets(
            category_key=category_key, market=market, status="enabled"
        )
        # Choose the latest enabled version per stable key BEFORE matching
        # variables. Never silently fall back to obsolete compatible versions.
        latest: Dict[str, AssetSet] = {}
        for candidate in candidates:
            if (candidate.status != "enabled" or candidate.category_key != category_key
                    or candidate.market not in {None, market}
                    or (asset_set_keys and candidate.asset_set_key not in asset_set_keys)):
                continue
            old = latest.get(candidate.asset_set_key)
            if old and old.asset_set_version == candidate.asset_set_version and old.asset_set_id != candidate.asset_set_id:
                raise AssetSetError("ambiguous asset set key/version")
            if old is None or candidate.asset_set_version > old.asset_set_version:
                latest[candidate.asset_set_key] = candidate
        requirements = dict(requirements or {})
        required = {**dict(tags or {}), **dict(requirements.get("required_tags") or {})}
        matched = []
        for candidate in latest.values():
            if not self.tags_match(candidate.tags_json or {}, required):
                continue
            try:
                validate_asset_set(candidate, verify_files=verify_files)
                self.validate_requirements(candidate, requirements)
            except AssetSetError:
                continue
            matched.append(candidate)
        return sorted(matched, key=lambda item: (item.asset_set_key, item.asset_set_id))

    @classmethod
    def validate_requirements(cls, asset_set: AssetSet, requirements: Mapping[str, Any]) -> None:
        roles = requirements.get("required_roles") or []
        ordered = cls.ordered_assets(asset_set, roles=roles)
        by_role = {item.get("role"): item["asset_id"] for item in ordered}
        pairs = (asset_set.manifest_json or {}).get("pairs") or []
        for required_pair in requirements.get("required_pairs") or []:
            members = {by_role.get(role) for role in required_pair["roles"]}
            if None in members or not any(
                pair.get("relation") == required_pair["relation"]
                and members.issubset(set(pair.get("asset_ids") or [])) for pair in pairs
            ):
                raise AssetSetError("NEEDS_ASSET: required role relationship is missing")

    def select(
        self, *, category_key: str, market: str, content_key: str,
        tags: Optional[Mapping[str, Any]] = None,
        asset_set_keys: Sequence[str] = (), requirements: Optional[Mapping[str, Any]] = None,
        asset_set_id: Optional[str] = None,
    ) -> AssetSet:
        matched = self.candidates(category_key=category_key, market=market, tags=tags,
                                  asset_set_keys=asset_set_keys, requirements=requirements)
        if asset_set_id:
            matched = [item for item in matched if item.asset_set_id == asset_set_id]
        if not matched:
            raise AssetSetError("NEEDS_ASSET: no latest enabled asset set matches market/category/variables/roles")
        digest = hashlib.sha256(str(content_key).encode("utf-8")).digest()
        return matched[int.from_bytes(digest[:8], "big") % len(matched)]

    @staticmethod
    def freeze(asset_set: AssetSet) -> Dict[str, Any]:
        return copy.deepcopy(asset_set.to_row())

    def from_frozen(self, snapshot: Mapping[str, Any], *, category_key: str, market: str,
                    tags: Mapping[str, Any], requirements: Mapping[str, Any]) -> AssetSet:
        asset_set = AssetSet.from_row(snapshot)
        # A batch owns its frozen bytes. Later retirement/version rollover is
        # allowed for new work and must not silently change an in-flight batch.
        if (asset_set.status != "enabled"
                or asset_set.category_key != category_key or asset_set.market not in {None, market}
                or not self.tags_match(asset_set.tags_json or {}, {**dict(tags), **dict(requirements.get("required_tags") or {})})):
            raise AssetSetError("NEEDS_ASSET: frozen asset set is incompatible")
        validate_asset_set(asset_set, verify_files=True)
        self.validate_requirements(asset_set, requirements)
        return asset_set

    @staticmethod
    def ordered_assets(
        asset_set: AssetSet, *, roles: Sequence[str] = (), count: int = 5,
    ) -> list[Dict[str, Any]]:
        assets = [dict(item) for item in (asset_set.manifest_json or {}).get("assets") or []]
        if roles:
            by_role: Dict[str, list[Dict[str, Any]]] = {}
            for item in assets:
                by_role.setdefault(str(item.get("role") or ""), []).append(item)
            ordered = []
            for role in roles:
                choices = by_role.get(str(role)) or []
                if len(choices) != 1:
                    raise AssetSetError(f"NEEDS_ASSET: role {role!r} must have exactly one source")
                ordered.append(choices[0])
            return ordered
        if len(assets) < count:
            raise AssetSetError(f"NEEDS_ASSET: expected {count} reusable images, got {len(assets)}")
        return assets[:count]
