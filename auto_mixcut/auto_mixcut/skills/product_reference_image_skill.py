from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from auto_mixcut.adapters.oss import file_sha256
from auto_mixcut.core.result import Result

from .context import SkillContext


class ProductReferenceImageSkill:
    """Product-level reference image pack stored once in OSS and reused by modules."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def ensure_pack(
        self,
        product_id: str,
        market: str | None = None,
        sku_id: str = "DEFAULT",
        sku_label: str = "",
        source_images: Iterable[dict[str, Any]] | None = None,
        source: str = "feishu_anchor_card",
        force: bool = False,
        anchor_snapshot: dict[str, Any] | None = None,
    ) -> Result:
        ready = ensure_reference_image_tables(self.ctx)
        if not ready.success:
            return ready
        market = _norm_market(market or _product_market(self.ctx, product_id))
        sku_id = _norm_sku_id(sku_id)
        images = [dict(item) for item in (source_images or []) if item.get("path")]
        active = self.get_active_pack(product_id, market=market, sku_id=sku_id)
        if active.success and active.data.get("pack") and not force:
            if not images:
                return active
            existing_hashes = {img.get("file_hash") for img in active.data.get("images", []) if img.get("file_hash")}
            incoming_hashes = {_path_hash(Path(img["path"])) for img in images}
            if incoming_hashes and incoming_hashes.issubset(existing_hashes):
                return active
        if not images:
            if active.success and active.data.get("pack"):
                return active
            return Result.fail("REFERENCE_IMAGES_REQUIRED", "source_images required when no active reference image pack exists", {"product_id": product_id, "market": market, "sku_id": sku_id})

        next_version = _next_version(self.ctx, product_id, market, sku_id)
        if active.success and active.data.get("pack"):
            self.ctx.repo.update("product_reference_image_packs", "reference_image_pack_id", active.data["pack"]["reference_image_pack_id"], {"status": "archived"})
        pack_id = _pack_id(market, product_id, sku_id, next_version)
        uploaded_images = []
        for index, image in enumerate(images, start=1):
            path = Path(image["path"])
            if not path.exists():
                return Result.fail("REFERENCE_IMAGE_NOT_FOUND", "reference image file missing", {"path": str(path)})
            role = _norm_role(image.get("image_role") or ("main" if index == 1 else "detail"))
            digest = _path_hash(path)
            reused = _find_existing_image(self.ctx, market, product_id, sku_id, digest)
            if reused:
                oss_object_id = reused["oss_object_id"]
                object_key = reused.get("object_key", "")
            else:
                ext = path.suffix.lower().lstrip(".") or "jpg"
                object_key = f"auto_mixcut/reference_images/{market}/{product_id}/{sku_id}/v{next_version}/{role}_{index:03d}_{digest[:8].upper()}.{ext}"
                uploaded = self.ctx.oss.upload(path, object_key)
                if not uploaded.success:
                    return uploaded
                oss_row = dict(uploaded.data, object_type="reference_image", mime_type=_mime(path))
                saved = self.ctx.repo.upsert("oss_objects", "object_id", oss_row)
                if not saved.success:
                    return saved
                oss_object_id = oss_row["object_id"]
            reference_image_id = _image_id(market, product_id, sku_id, next_version, role, digest)
            row = {
                "reference_image_id": reference_image_id,
                "reference_image_pack_id": pack_id,
                "market": market,
                "product_id": product_id,
                "sku_id": sku_id,
                "sku_label": sku_label,
                "image_role": role,
                "image_index": index,
                "oss_object_id": oss_object_id,
                "object_key": object_key,
                "file_hash": digest,
                "phash": "",
                "file_name": path.name,
                "mime_type": _mime(path),
                "width": int(image.get("width") or 0),
                "height": int(image.get("height") or 0),
                "source_file_token": str(image.get("source_file_token") or ""),
                "source_url": str(image.get("source_url") or ""),
                "status": "active",
            }
            write = self.ctx.repo.upsert("product_reference_images", "reference_image_id", row)
            if not write.success:
                return write
            uploaded_images.append(row)
        primary = uploaded_images[0] if uploaded_images else {}
        pack_row = {
            "reference_image_pack_id": pack_id,
            "market": market,
            "product_id": product_id,
            "sku_id": sku_id,
            "sku_label": sku_label,
            "version": next_version,
            "status": "active",
            "source": source,
            "image_count": len(uploaded_images),
            "primary_image_oss_object_id": primary.get("oss_object_id", ""),
            "primary_preview_url": self._preview_url(primary.get("object_key")),
            "anchor_snapshot_json": anchor_snapshot or {},
        }
        saved_pack = self.ctx.repo.upsert("product_reference_image_packs", "reference_image_pack_id", pack_row)
        if not saved_pack.success:
            return saved_pack
        return Result.ok({"pack": pack_row, "images": [_with_preview(self, img) for img in uploaded_images]})

    def get_active_pack(self, product_id: str, market: str | None = None, sku_id: str = "DEFAULT") -> Result:
        ready = ensure_reference_image_tables(self.ctx)
        if not ready.success:
            return ready
        market = _norm_market(market or _product_market(self.ctx, product_id))
        sku_id = _norm_sku_id(sku_id)
        packs = self.ctx.repo.list_where(
            "product_reference_image_packs",
            "product_id=? AND market=? AND sku_id=? AND status='active' ORDER BY version DESC LIMIT 1",
            (product_id, market, sku_id),
        )
        if not packs:
            return Result.ok({"pack": None, "images": []})
        pack = packs[0]
        images = self.ctx.repo.list_where(
            "product_reference_images",
            "reference_image_pack_id=? AND status='active' ORDER BY image_index",
            (pack["reference_image_pack_id"],),
        )
        return Result.ok({"pack": pack, "images": [_with_preview(self, img) for img in images]})

    def get_active_images(self, product_id: str, market: str | None = None, sku_id: str = "DEFAULT", roles: list[str] | None = None) -> Result:
        pack = self.get_active_pack(product_id, market=market, sku_id=sku_id)
        if not pack.success:
            return pack
        roles_set = {_norm_role(role) for role in (roles or [])}
        images = pack.data.get("images", [])
        if roles_set:
            images = [img for img in images if img.get("image_role") in roles_set]
        return Result.ok({"pack": pack.data.get("pack"), "images": images})

    def refresh_pack(self, product_id: str, market: str | None = None, sku_id: str = "DEFAULT", source_images: Iterable[dict[str, Any]] | None = None, sku_label: str = "", anchor_snapshot: dict[str, Any] | None = None) -> Result:
        return self.ensure_pack(product_id, market=market, sku_id=sku_id, sku_label=sku_label, source_images=source_images, force=True, anchor_snapshot=anchor_snapshot)

    def archive_pack(self, reference_image_pack_id: str) -> Result:
        ready = ensure_reference_image_tables(self.ctx)
        if not ready.success:
            return ready
        return self.ctx.repo.update("product_reference_image_packs", "reference_image_pack_id", reference_image_pack_id, {"status": "archived"})

    def _preview_url(self, object_key: str | None) -> str:
        if not object_key:
            return ""
        try:
            return self.ctx.oss.signed_url(object_key)
        except Exception:
            return ""


def ensure_reference_image_tables(ctx: SkillContext) -> Result:
    try:
        with ctx.repo.connect() as conn:
            if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
                with conn.cursor() as cur:
                    for statement in _mysql_statements():
                        cur.execute(statement)
            else:
                for statement in _sqlite_statements():
                    conn.execute(statement)
        return Result.ok()
    except Exception as exc:
        return Result.fail("REFERENCE_IMAGE_TABLE_FAILED", str(exc))


def _sqlite_statements() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS product_reference_image_packs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          reference_image_pack_id TEXT NOT NULL UNIQUE,
          market TEXT,
          product_id TEXT NOT NULL,
          sku_id TEXT DEFAULT 'DEFAULT',
          sku_label TEXT,
          version INTEGER DEFAULT 1,
          status TEXT,
          source TEXT,
          image_count INTEGER DEFAULT 0,
          primary_image_oss_object_id TEXT,
          primary_preview_url TEXT,
          anchor_snapshot_json TEXT,
          created_at TEXT,
          updated_at TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS product_reference_images (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          reference_image_id TEXT NOT NULL UNIQUE,
          reference_image_pack_id TEXT NOT NULL,
          market TEXT,
          product_id TEXT NOT NULL,
          sku_id TEXT DEFAULT 'DEFAULT',
          sku_label TEXT,
          image_role TEXT,
          image_index INTEGER DEFAULT 1,
          oss_object_id TEXT,
          object_key TEXT,
          file_hash TEXT,
          phash TEXT,
          file_name TEXT,
          mime_type TEXT,
          width INTEGER,
          height INTEGER,
          source_file_token TEXT,
          source_url TEXT,
          status TEXT,
          created_at TEXT,
          updated_at TEXT
        )
        """,
    ]


def _mysql_statements() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS product_reference_image_packs (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          reference_image_pack_id VARCHAR(256) NOT NULL UNIQUE,
          market VARCHAR(64),
          product_id VARCHAR(128) NOT NULL,
          sku_id VARCHAR(128) DEFAULT 'DEFAULT',
          sku_label VARCHAR(256),
          version INT DEFAULT 1,
          status VARCHAR(64),
          source VARCHAR(128),
          image_count INT DEFAULT 0,
          primary_image_oss_object_id VARCHAR(128),
          primary_preview_url TEXT,
          anchor_snapshot_json JSON,
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_ref_pack_product (market, product_id, sku_id, status)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS product_reference_images (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          reference_image_id VARCHAR(256) NOT NULL UNIQUE,
          reference_image_pack_id VARCHAR(256) NOT NULL,
          market VARCHAR(64),
          product_id VARCHAR(128) NOT NULL,
          sku_id VARCHAR(128) DEFAULT 'DEFAULT',
          sku_label VARCHAR(256),
          image_role VARCHAR(64),
          image_index INT DEFAULT 1,
          oss_object_id VARCHAR(128),
          object_key TEXT,
          file_hash VARCHAR(128),
          phash VARCHAR(128),
          file_name VARCHAR(512),
          mime_type VARCHAR(128),
          width INT,
          height INT,
          source_file_token VARCHAR(256),
          source_url TEXT,
          status VARCHAR(64),
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_ref_img_pack (reference_image_pack_id),
          KEY idx_ref_img_hash (market, product_id, sku_id, file_hash)
        )
        """,
    ]


def _with_preview(skill: ProductReferenceImageSkill, image: dict[str, Any]) -> dict[str, Any]:
    item = dict(image)
    item["preview_url"] = skill._preview_url(item.get("object_key"))
    return item


def _find_existing_image(ctx: SkillContext, market: str, product_id: str, sku_id: str, file_hash: str) -> dict[str, Any] | None:
    rows = ctx.repo.list_where(
        "product_reference_images",
        "market=? AND product_id=? AND sku_id=? AND file_hash=? AND status!='deleted' ORDER BY id DESC LIMIT 1",
        (market, product_id, sku_id, file_hash),
    )
    return rows[0] if rows else None


def _next_version(ctx: SkillContext, product_id: str, market: str, sku_id: str) -> int:
    rows = ctx.repo.list_where(
        "product_reference_image_packs",
        "product_id=? AND market=? AND sku_id=? ORDER BY version DESC LIMIT 1",
        (product_id, market, sku_id),
    )
    return int(rows[0].get("version") or 0) + 1 if rows else 1


def _product_market(ctx: SkillContext, product_id: str) -> str:
    product = ctx.repo.get("products", "product_id", product_id) or {}
    return str(product.get("market") or "NA")


def _pack_id(market: str, product_id: str, sku_id: str, version: int) -> str:
    return f"REFPACK_{_id_part(market)}_{_id_part(product_id)}_{_id_part(sku_id)}_V{version}"


def _image_id(market: str, product_id: str, sku_id: str, version: int, role: str, digest: str) -> str:
    return f"REFIMG_{_id_part(market)}_{_id_part(product_id)}_{_id_part(sku_id)}_V{version}_{_id_part(role)}_{digest[:8].upper()}"


def _id_part(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(value or "")).strip("_").upper() or "NA"


def _norm_market(value: str | None) -> str:
    return _id_part(value or "NA")


def _norm_sku_id(value: str | None) -> str:
    return _id_part(value or "DEFAULT")


def _norm_role(value: str | None) -> str:
    text = str(value or "other").strip().lower()
    return text if text in {"main", "detail", "tryon", "structure", "color_swatch", "other"} else "other"


def _path_hash(path: Path) -> str:
    return file_sha256(path)


def _mime(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".png":
        return "image/png"
    if suffix == ".webp":
        return "image/webp"
    return "application/octet-stream"
