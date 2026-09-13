"""Deterministic product reference-pack resolution for operator-owned images.

The resolver deliberately does not score images or run visual-model QA. The
operator's image group is the appearance authority. Automation only checks
that files are readable, deduplicates bytes, chooses an explicit/default pack,
and freezes that exact version into the content task.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from domain.models import ProductReferencePack
from services.photo_category_registry import (
    WOMENSWEAR_V1,
    adapter_for_product_category,
    role_priority_for_slot,
)

PACK_READY = "ready"
PACK_LIMITED = "limited"
PACK_BLOCKED = "blocked"
PACK_RETIRED = "retired"
RESOLVABLE_STATUSES = {PACK_READY, PACK_LIMITED}
SUPPORTED_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
ASSET_ROLES = {"front", "back", "side", "detail", "lifestyle", "unknown"}
DEFAULT_ROLE_ORDER = ("front", "back", "lifestyle", "detail")


class ProductReferenceResolutionError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _local_path(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        return str(value.get("local_path") or value.get("path") or "").strip()
    return ""


def _role_paths(product: Mapping[str, Any]) -> Dict[str, List[str]]:
    raw = product.get("reference_roles") or {}
    if not isinstance(raw, Mapping):
        return {}
    return {
        str(role): [str(value) for value in values or [] if str(value).strip()]
        for role, values in raw.items()
        if str(role) in ASSET_ROLES
    }


def select_product_references_for_slot(
    product: Mapping[str, Any], slot_role: str
) -> List[str]:
    """Choose at most three frozen product refs for one storyboard role.

    Historical snapshots without ``reference_roles`` retain their old behavior
    and pass every configured reference, so existing tasks remain reproducible.
    """
    roles = _role_paths(product)
    fallback = [
        _local_path(value) for value in product.get("reference_images") or []
        if _local_path(value)
    ]
    if not roles:
        return fallback
    # Phase 1 等价搬迁：本表原为函数内联常量，现由 Category Adapter 提供。
    # Phase 3 起按**商品类目**取适配器（scarf 与 womenswear 的参考图优先级不同）。
    # 未被任何适配器认领的类目（如 wig，或缺 category）回退到 WOMENSWEAR 那张
    # 旧表 —— 这张表历史上服务所有商品，回退而非改用共享默认序，才能保证
    # 缺类目商品的取图顺序与迁移前逐字一致。
    adapter = adapter_for_product_category(product.get("category")) or WOMENSWEAR_V1
    role_order = role_priority_for_slot(adapter, slot_role)
    selected: List[str] = []
    for role in role_order:
        for value in roles.get(role, []):
            if value not in selected:
                selected.append(value)
                break
        if len(selected) >= 3:
            break
    return selected or fallback[:3]


def has_detail_reference(product: Mapping[str, Any]) -> bool:
    return bool(_role_paths(product).get("detail"))


class ProductReferenceResolver:
    def __init__(self, repository, *, candidate_source=None):
        self.repository = repository
        self.candidate_source = candidate_source
        self._synced_products = set()
        self._rotation_history = {}

    def resolve_snapshot(
        self,
        product_id: str,
        *,
        selection_key: str = "",
        variant_key: str = "",
        allow_history_bootstrap: bool = True,
        account_id: str = "",
    ) -> Dict[str, Any]:
        self._sync_candidate_packs(product_id)
        packs = [
            pack for pack in self.repository.list_product_reference_packs(product_id)
            if pack.status in RESOLVABLE_STATUSES
        ]
        if not packs and allow_history_bootstrap:
            pack = self.bootstrap_from_shared_cache(product_id)
            if pack is None:
                pack = self.bootstrap_from_history(product_id)
            packs = [pack] if pack else []
        # A retired old version must never compete with the newest version of
        # the same operator-owned image group.  This also makes retries stable
        # after a group receives a new version.
        latest_by_variant: Dict[str, ProductReferencePack] = {}
        for candidate in packs:
            current = latest_by_variant.get(candidate.variant_key)
            if current is None or candidate.pack_version > current.pack_version:
                latest_by_variant[candidate.variant_key] = candidate
        usage_counts = None
        history_getter = getattr(self.repository, "list_recent_diversity_axes", None)
        if account_id and selection_key and history_getter is not None:
            prefix = selection_key.rpartition(":")[0] or selection_key
            history_key = (account_id, prefix)
            if history_key not in self._rotation_history:
                from collections import Counter
                self._rotation_history[history_key] = Counter(
                    str(row.get("reference_pack_id") or "") for row in history_getter(
                        account_id, exclude_source_record_id=prefix, limit=100
                    )
                )
            usage_counts = self._rotation_history[history_key]
        pack, reason = self._choose_pack(
            product_id,
            list(latest_by_variant.values()),
            selection_key=selection_key,
            variant_key=variant_key,
            usage_counts=usage_counts,
        )
        product = self._snapshot_from_pack(pack, reason)
        authority_category = self._authoritative_category(packs)
        if not str(product.get("category") or "").strip() and authority_category:
            product["category"] = authority_category
        if (
            pack.source_type == "feishu_short_video_operation"
            or self._is_placeholder_name(product.get("product_name"), product_id)
        ):
            product["product_name"] = self._generic_product_name(
                str(product.get("category") or "")
            )
        if not product.get("reference_images"):
            raise ProductReferenceResolutionError(
                f"商品 {product_id} 的参考包没有可读取的本地图片"
            )
        return product

    def _sync_candidate_packs(self, product_id: str) -> None:
        """Import distinct operation-table rows before selecting a new snapshot."""
        if self.candidate_source is None or product_id in self._synced_products:
            return
        try:
            candidates = self.candidate_source.list_candidates(product_id)
            existing = list(self.repository.list_product_reference_packs(product_id))
            authority_category = self._authoritative_category(existing)
            known_content = {
                self._content_fingerprint(pack.assets_json) for pack in existing
                if self._content_fingerprint(pack.assets_json)
            }
            for candidate in candidates:
                candidate_hashes = []
                for path in self._usable_paths(candidate.references):
                    candidate_hashes.append(_sha256(path))
                content_fingerprint = self._content_fingerprint(
                    [{"sha256": value} for value in candidate_hashes]
                )
                if not content_fingerprint or content_fingerprint in known_content:
                    continue
                pack = self.build_pack(
                    product_id=product_id,
                    product_name=(
                        candidate.product_name
                        if not self._is_placeholder_name(candidate.product_name, product_id)
                        else self._generic_product_name(
                            candidate.category or authority_category
                        )
                    ),
                    category=candidate.category or authority_category,
                    references=candidate.references,
                    variant_key=f"operation_{candidate.record_id}"[:96],
                    is_default=False,
                    source_type="feishu_short_video_operation",
                    source_ref=candidate.record_id,
                    persist=True,
                )
                known_content.add(self._content_fingerprint(pack.assets_json))
            self._synced_products.add(product_id)
        except Exception as exc:  # noqa: BLE001 - make source failure explicit
            raise ProductReferenceResolutionError(
                f"商品 {product_id} 同步短视频运营任务表图片失败：{exc}"
            ) from exc

    @staticmethod
    def _is_placeholder_name(value: Any, product_id: str = "") -> bool:
        text = str(value or "").strip()
        return not text or text.isdigit() or (product_id and text == str(product_id))

    @classmethod
    def _authoritative_category(
        cls, packs: Sequence[ProductReferencePack]
    ) -> str:
        """Inherit only variant-neutral category metadata from sibling packs."""
        ordered = sorted(
            packs,
            key=lambda item: (
                0 if item.is_default else 1,
                0 if not cls._is_placeholder_name(item.product_name, item.product_id) else 1,
                -int(item.pack_version or 0),
                item.variant_key,
            ),
        )
        return next(
            (str(item.category).strip() for item in ordered if str(item.category or "").strip()),
            "",
        )

    @staticmethod
    def _generic_product_name(category: str) -> str:
        return {
            "outerwear": "目标外套",
            "dress": "目标连衣裙",
            "top": "目标上装",
            "bottom": "目标下装",
        }.get(str(category or "").strip().lower(), "目标商品")

    @staticmethod
    def _content_fingerprint(assets: Sequence[Mapping[str, Any]]) -> str:
        """Fingerprint an image set independent of source, roles, and ordering."""
        hashes = sorted({
            str(item.get("sha256") or "").strip() for item in assets
            if str(item.get("sha256") or "").strip()
        })
        if not hashes:
            return ""
        return hashlib.sha256("\n".join(hashes).encode("ascii")).hexdigest()

    def bootstrap_from_history(
        self, product_id: str
    ) -> Optional[ProductReferencePack]:
        getter = getattr(self.repository, "list_product_snapshot_candidates", None)
        candidates = getter(product_id, limit=20) if callable(getter) else []
        for candidate in candidates:
            product = dict(candidate.get("product") or {})
            if self._usable_paths(product.get("reference_images") or []):
                return self.build_pack(
                    product_id=product_id,
                    product_name=str(product.get("product_name") or product_id),
                    category=str(product.get("category") or ""),
                    references=list(product.get("reference_images") or []),
                    variant_key=str(product.get("variant_key") or "default"),
                    reference_roles=product.get("reference_roles") or {},
                    is_default=True,
                    source_type="opv_history_bootstrap",
                    source_ref=str(candidate.get("task_id") or ""),
                    persist=True,
                )
        return None

    def bootstrap_from_shared_cache(
        self, product_id: str
    ) -> Optional[ProductReferencePack]:
        configured = str(
            os.environ.get("ORIGINAL_SCRIPT_PRODUCT_REFERENCE_CACHE_ROOT") or ""
        ).strip()
        root = (
            Path(configured).expanduser()
            if configured
            else Path.home()
            / ".openclaw"
            / "shared"
            / "data"
            / "original_product_reference_cache"
        )
        product_root = root / str(product_id)
        if not product_root.is_dir():
            return None
        references = sorted(
            str(path)
            for path in product_root.rglob("*")
            if path.is_file() and path.suffix.lower() in SUPPORTED_IMAGE_SUFFIXES
        )[:4]
        if not references:
            return None
        return self.build_pack(
            product_id=product_id,
            product_name=str(product_id),
            category="",
            references=references,
            variant_key="default",
            is_default=True,
            source_type="original_product_reference_cache",
            source_ref=str(product_root),
            persist=True,
        )

    def build_pack(
        self,
        *,
        product_id: str,
        product_name: str,
        category: str,
        references: Sequence[Any],
        variant_key: str = "default",
        reference_roles: Optional[Mapping[str, Sequence[str]]] = None,
        explicit_roles: Optional[Sequence[str]] = None,
        is_default: bool = False,
        source_type: str = "operator",
        source_ref: str = "",
        persist: bool = False,
    ) -> ProductReferencePack:
        assets = self._assets(
            references,
            reference_roles=reference_roles or {},
            explicit_roles=explicit_roles,
        )
        if not assets:
            raise ProductReferenceResolutionError(
                f"商品 {product_id} 没有可读取的本地参考图"
            )
        fingerprint = hashlib.sha256(json.dumps(
            {
                "product_id": str(product_id),
                "variant_key": str(variant_key or "default"),
                "assets": [
                    {"sha256": item["sha256"], "role": item["role"]}
                    for item in assets
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        existing = list(self.repository.list_product_reference_packs(product_id))
        same = next(
            (
                pack for pack in existing
                if pack.variant_key == (variant_key or "default")
                and pack.asset_fingerprint == fingerprint
            ),
            None,
        )
        if same is not None:
            if is_default and not same.is_default:
                same.is_default = True
                if persist:
                    self.repository.upsert_product_reference_pack(same)
            return same
        versions = [
            pack.pack_version for pack in existing
            if pack.variant_key == (variant_key or "default")
        ]
        roles = {str(item["role"]) for item in assets}
        has_second_angle = bool(roles & {"back", "side"})
        status = (
            PACK_READY
            if "front" in roles and has_second_angle and "detail" in roles
            else PACK_LIMITED
        )
        pack = ProductReferencePack(
            pack_id=f"opv_prp_{fingerprint[:20]}",
            product_id=str(product_id),
            variant_key=str(variant_key or "default"),
            pack_version=max(versions, default=0) + 1,
            product_name=product_name or str(product_id),
            category=category or None,
            status=status,
            is_default=bool(is_default),
            assets_json=assets,
            asset_fingerprint=fingerprint,
            source_type=source_type or None,
            source_ref=source_ref or None,
            selection_reason="operator_owned_images_no_visual_scoring",
        )
        if persist:
            self.repository.upsert_product_reference_pack(pack)
        return pack

    @staticmethod
    def _choose_pack(
        product_id: str,
        packs: Sequence[ProductReferencePack],
        *,
        selection_key: str = "",
        variant_key: str = "",
        usage_counts: Optional[Mapping[str, int]] = None,
    ) -> Tuple[ProductReferencePack, str]:
        if not packs:
            raise ProductReferenceResolutionError(
                f"RDS 中没有产品 {product_id} 的有效商品参考包，请先完成产品图片入库"
            )
        if variant_key:
            matched = [pack for pack in packs if pack.variant_key == variant_key]
            if not matched:
                variants = ", ".join(sorted({pack.variant_key for pack in packs}))
                raise ProductReferenceResolutionError(
                    f"产品 {product_id} 没有参考组 {variant_key}；当前可用：{variants}"
                )
            return matched[0], "explicit_variant_pack"
        if selection_key and len(packs) > 1:
            ordered = sorted(packs, key=lambda item: (item.variant_key, item.pack_id))
            prefix, separator, ordinal = selection_key.rpartition(":")
            if separator and ordinal.isdigit() and int(ordinal) > 0:
                digest = hashlib.sha256(prefix.encode("utf-8")).digest()
                offset = int.from_bytes(digest[:8], "big") % len(ordered)
                if usage_counts is not None:
                    # Simulate preceding ordinals from the same frozen history;
                    # retries and out-of-order resolution select the same pack.
                    rotated = ordered[offset:] + ordered[:offset]
                    counts = {p.pack_id: 0 for p in rotated}
                    for _ in range(int(ordinal)):
                        selected = min(rotated, key=lambda p: (counts[p.pack_id], int(usage_counts.get(p.pack_id, 0))))
                        counts[selected.pack_id] += 1
                    return selected, "historical_least_used_batch_rotation"
                index = (offset + int(ordinal) - 1) % len(ordered)
            else:
                digest = hashlib.sha256(selection_key.encode("utf-8")).digest()
                index = int.from_bytes(digest[:8], "big") % len(ordered)
            return ordered[index], "stable_batch_variant_rotation"
        defaults = [pack for pack in packs if pack.is_default]
        if len(defaults) == 1:
            return defaults[0], "explicit_default_pack"
        if len(defaults) > 1:
            variants = ", ".join(sorted({pack.variant_key for pack in defaults}))
            raise ProductReferenceResolutionError(
                f"产品 {product_id} 存在多个默认参考包（{variants}），请只保留一个默认组"
            )
        if len(packs) == 1:
            return packs[0], "single_active_pack"
        variants = ", ".join(sorted({pack.variant_key for pack in packs}))
        raise ProductReferenceResolutionError(
            f"产品 {product_id} 存在多个参考组（{variants}）但没有默认组，请先设置默认组"
        )

    @staticmethod
    def _snapshot_from_pack(
        pack: ProductReferencePack, selection_reason: str
    ) -> Dict[str, Any]:
        assets = [
            item for item in pack.assets_json
            if item.get("usable") and Path(str(item.get("local_path") or "")).is_file()
        ]
        roles: Dict[str, List[str]] = {}
        reference_assets = []
        for item in assets:
            actual_hash = _sha256(Path(str(item["local_path"])))
            if item.get("sha256") and actual_hash != item["sha256"]:
                raise ProductReferenceResolutionError("商品图包文件内容已改变，请重新入库后再生成")
            reference_assets.append({"local_path": str(item["local_path"]), "sha256": actual_hash})
            role = str(item.get("role") or "unknown")
            roles.setdefault(role, []).append(str(item["local_path"]))
        return {
            "product_id": pack.product_id,
            "product_name": pack.product_name or pack.product_id,
            "category": pack.category or "",
            "reference_pack_id": pack.pack_id,
            "reference_pack_version": pack.pack_version,
            "variant_key": pack.variant_key,
            "reference_status": pack.status,
            "selection_reason": selection_reason,
            "asset_fingerprint": pack.asset_fingerprint,
            "reference_images": [str(item["local_path"]) for item in assets],
            "reference_assets": reference_assets,
            "reference_roles": roles,
            "visual_qa_policy": "operator_preview_only",
        }

    @staticmethod
    def _usable_paths(references: Iterable[Any]) -> List[Path]:
        paths: List[Path] = []
        for value in references:
            raw = _local_path(value)
            path = Path(raw).expanduser() if raw else None
            if (
                path is not None
                and path.is_file()
                and path.stat().st_size > 0
                and path.suffix.lower() in SUPPORTED_IMAGE_SUFFIXES
            ):
                paths.append(path.resolve())
        return paths

    def _assets(
        self,
        references: Sequence[Any],
        *,
        reference_roles: Mapping[str, Sequence[str]],
        explicit_roles: Optional[Sequence[str]],
    ) -> List[Dict[str, Any]]:
        role_by_path: Dict[str, str] = {}
        for role, values in reference_roles.items():
            if str(role) not in ASSET_ROLES:
                continue
            for value in values or []:
                role_by_path[str(Path(str(value)).expanduser())] = str(role)
                role_by_path[str(Path(str(value)).expanduser().resolve())] = str(role)
        seen = set()
        assets: List[Dict[str, Any]] = []
        for index, path in enumerate(self._usable_paths(references)):
            digest = _sha256(path)
            if digest in seen:
                continue
            seen.add(digest)
            explicit = (
                str(explicit_roles[index])
                if explicit_roles and index < len(explicit_roles)
                else ""
            )
            role = explicit or role_by_path.get(str(path)) or (
                DEFAULT_ROLE_ORDER[index]
                if index < len(DEFAULT_ROLE_ORDER) else "unknown"
            )
            if role not in ASSET_ROLES:
                raise ProductReferenceResolutionError(f"不支持的图片角色：{role}")
            assets.append({
                "asset_id": f"opv_asset_{digest[:16]}",
                "local_path": str(path),
                "sha256": digest,
                "role": role,
                "usable": True,
                "sort_order": len(assets) + 1,
            })
        return assets
