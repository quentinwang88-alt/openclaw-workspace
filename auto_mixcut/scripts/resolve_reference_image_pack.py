#!/usr/bin/env python3
"""Resolve and download product reference images from the active OSS image pack."""
from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
BITABLE_PATH = WORKSPACE / "skills" / "script-run-manager-sync"
if str(BITABLE_PATH) not in sys.path:
    sys.path.insert(0, str(BITABLE_PATH))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.product_reference_image_skill import ProductReferenceImageSkill, ensure_reference_image_tables  # noqa: E402
from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


ANCHOR_QUEUE_URL = "https://gcngopvfvo0q.feishu.cn/wiki/V35wwjDLYiMFeTkiVFPc7SM5nvd?table=tbl2QRHwF7g9CmaF&view=vewv752AHQ"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product-id", default="")
    parser.add_argument("--market", default="")
    parser.add_argument("--sku-id", default="DEFAULT")
    parser.add_argument("--reference-image-pack-id", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--config", default="")
    parser.add_argument("--anchor-queue-url", default=ANCHOR_QUEUE_URL)
    parser.add_argument("--no-anchor-import", action="store_true")
    args = parser.parse_args()

    try:
        ctx = build_context(args.config or None)
        ready = ensure_reference_image_tables(ctx)
        if not ready.success:
            return _fail("REFERENCE_IMAGE_TABLE_FAILED", ready.to_dict())

        skill = ProductReferenceImageSkill(ctx)
        resolved = _resolve_pack(ctx, skill, args)
        pack = resolved.get("pack")
        images = _dedupe_images(resolved.get("images") or [])
        if not pack or not images:
            return _fail(
                "REFERENCE_IMAGE_PACK_NOT_FOUND",
                {
                    "product_id": args.product_id,
                    "market": args.market,
                    "sku_id": args.sku_id or "DEFAULT",
                    "reference_image_pack_id": args.reference_image_pack_id,
                },
            )

        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        downloaded = []
        for index, image in enumerate(images, start=1):
            object_key = str(image.get("object_key") or "").strip()
            if not object_key:
                return _fail("REFERENCE_IMAGE_OBJECT_KEY_MISSING", {"image": image})
            ext = Path(object_key).suffix or _ext_from_mime(str(image.get("mime_type") or ""))
            file_name = f"{index:02d}-{_safe_name(image.get('image_role') or 'image')}-{_safe_name(image.get('reference_image_id') or index)}{ext or '.jpg'}"
            dest = output_dir / file_name
            preview_url = str(image.get("preview_url") or "").strip()
            signed_url = _direct_signed_url(ctx.oss, object_key)
            urls = [url for url in [preview_url, signed_url] if url]
            last_error = ""
            for url in urls:
                try:
                    _download_url(url, dest)
                    last_error = ""
                    break
                except Exception as exc:
                    last_error = str(exc)
                    if dest.exists() and dest.stat().st_size <= 0:
                        dest.unlink()

            if last_error or not dest.exists():
                result = ctx.oss.download(object_key, dest)
                if not result.success:
                    if dest.exists() and dest.stat().st_size <= 0:
                        dest.unlink()
                    return _fail(
                        "REFERENCE_IMAGE_DOWNLOAD_FAILED",
                        {
                            "oss_download": result.to_dict(),
                            "preview_url": preview_url,
                            "signed_url_available": bool(signed_url),
                            "preview_error": last_error,
                        },
                    )
            if not dest.exists() or dest.stat().st_size <= 0:
                if dest.exists():
                    dest.unlink()
                return _fail("REFERENCE_IMAGE_DOWNLOAD_EMPTY", {"object_key": object_key, "dest": str(dest)})
            item = dict(image)
            item["local_path"] = str(dest)
            downloaded.append(item)

        print(
            json.dumps(
                {
                    "success": True,
                    "pack": pack,
                    "images": downloaded,
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return 0
    except Exception as exc:
        return _fail("REFERENCE_IMAGE_RESOLVE_ERROR", {"error": str(exc)})


def _resolve_pack(ctx: Any, skill: ProductReferenceImageSkill, args: argparse.Namespace) -> Dict[str, Any]:
    pack_id = str(args.reference_image_pack_id or "").strip()
    if pack_id:
        pack = ctx.repo.get("product_reference_image_packs", "reference_image_pack_id", pack_id)
        if not pack:
            return {"pack": None, "images": []}
        active = skill.get_active_images(pack.get("product_id"), market=pack.get("market"), sku_id=pack.get("sku_id") or "DEFAULT")
        if active.success and active.data.get("pack", {}).get("reference_image_pack_id") == pack_id:
            return active.data
        images = ctx.repo.list_where(
            "product_reference_images",
            "reference_image_pack_id=? AND status='active' ORDER BY image_index",
            (pack_id,),
        )
        images_with_preview = []
        for image in images:
            oss_object = ctx.repo.get("oss_objects", "object_id", image.get("oss_object_id")) if image.get("oss_object_id") else None
            images_with_preview.append(dict(image, preview_url=skill._preview_url(image.get("object_key"), oss_object)))
        return {"pack": pack, "images": images_with_preview}

    result = skill.get_active_images(args.product_id, market=args.market or None, sku_id=args.sku_id or "DEFAULT")
    if not result.success:
        raise RuntimeError(json.dumps(result.to_dict(), ensure_ascii=False, default=str))
    if result.data and result.data.get("pack"):
        return result.data
    if args.no_anchor_import:
        return result.data or {"pack": None, "images": []}
    return _import_from_anchor_card(ctx, skill, args)


def _dedupe_images(images: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for image in images:
        key = (
            str(image.get("reference_image_id") or "").strip(),
            str(image.get("object_key") or image.get("oss_object_key") or "").strip(),
            str(image.get("file_hash") or "").strip(),
        )
        fallback = tuple(item for item in key if item)
        if fallback and fallback in seen:
            continue
        if fallback:
            seen.add(fallback)
        deduped.append(image)
    return deduped


def _import_from_anchor_card(ctx: Any, skill: ProductReferenceImageSkill, args: argparse.Namespace) -> Dict[str, Any]:
    if not args.product_id:
        return {"pack": None, "images": []}
    client = _resolve_client(args.anchor_queue_url)
    anchor_fields = _find_anchor_fields(client, args.product_id)
    if not anchor_fields:
        return {"pack": None, "images": [], "error": "anchor_card_missing"}
    attachments = _attachments(anchor_fields.get("商品主图"))
    if not attachments:
        return {"pack": None, "images": [], "error": "anchor_card_missing_product_images"}

    source_images: List[Dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix=f"refpack_{args.product_id}_") as tmpdir:
        tmp_root = Path(tmpdir)
        for index, attachment in enumerate(attachments, start=1):
            content, file_name, _content_type, _size = client.download_attachment_bytes(attachment)
            safe_name = _safe_name(file_name or f"reference_{index}.jpg")
            image_path = tmp_root / f"{index:03d}_{safe_name}"
            image_path.write_bytes(content)
            source_images.append(
                {
                    "path": str(image_path),
                    "image_role": "main" if index == 1 else "detail",
                    "source_file_token": str(attachment.get("file_token") or ""),
                    "source_url": str(attachment.get("url") or ""),
                }
            )
        packed = skill.ensure_pack(
            args.product_id,
            market=args.market or None,
            sku_id=args.sku_id or "DEFAULT",
            sku_label=str(anchor_fields.get("SKU ID") or ""),
            source_images=source_images,
            source="feishu_anchor_card",
            anchor_snapshot={"商品主图数量": len(source_images), "AI生成锚点卡": _text(anchor_fields.get("AI生成锚点卡"))[:2000]},
        )
    if not packed.success:
        raise RuntimeError(json.dumps(packed.to_dict(), ensure_ascii=False, default=str))
    return packed.data or {"pack": None, "images": []}


def _resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def _find_anchor_fields(client: FeishuBitableClient, product_id: str) -> Dict[str, Any]:
    matched: List[Dict[str, Any]] = []
    for record in client.list_records(page_size=100):
        fields = record.fields or {}
        if _text(fields.get("商品ID")) == str(product_id):
            matched.append(fields)
    if not matched:
        return {}
    confirmed = [fields for fields in matched if _text(fields.get("人工确认状态")) in {"已确认", "confirmed"}]
    selected = confirmed[-1] if confirmed else matched[-1]
    if _attachments(selected.get("商品主图")):
        return selected
    image_source = next((fields for fields in reversed(matched) if _attachments(fields.get("商品主图"))), None)
    if not image_source:
        return selected
    merged = dict(selected)
    merged["商品主图"] = image_source.get("商品主图")
    return merged


def _attachments(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    return []


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return "\n".join(item for item in (_text(item) for item in value) if item).strip()
    return str(value).strip()


def _safe_name(value: Any) -> str:
    text = str(value or "image")
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return text[:96] or "image"


def _ext_from_mime(mime: str) -> str:
    lower = mime.lower()
    if "png" in lower:
        return ".png"
    if "webp" in lower:
        return ".webp"
    if "jpeg" in lower or "jpg" in lower:
        return ".jpg"
    return ".jpg"


def _download_url(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "auto-mixcut-reference-image-resolver/1.0"})
    context = ssl._create_unverified_context() if url.lower().startswith("https://") else None
    with urllib.request.urlopen(request, timeout=120, context=context) as response:
        data = response.read()
    if not data:
        raise RuntimeError("empty response")
    dest.write_bytes(data)


def _direct_signed_url(oss: Any, object_key: str) -> str:
    bucket = getattr(oss, "_bucket", None)
    if not bucket:
        return ""
    try:
        return bucket.sign_url("GET", object_key, 86400, slash_safe=True)
    except Exception:
        return ""


def _fail(code: str, data: Dict[str, Any]) -> int:
    print(json.dumps({"success": False, "code": code, "data": data}, ensure_ascii=False, default=str), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
