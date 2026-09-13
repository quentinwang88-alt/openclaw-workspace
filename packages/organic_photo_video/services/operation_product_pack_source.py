"""Read product image groups from the short-video operation workbench.

This adapter is deliberately read-only.  One Feishu record is one candidate
pack; records are never merged and no visual scoring is performed.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence


DEFAULT_OPERATION_WIKI_TOKEN = "RJxHw0uAkiJPkSkXvMvcq3hXn5B"
DEFAULT_OPERATION_TABLE_ID = "tblr8C7uvGIPBQar"
PRODUCT_ID_FIELDS = ("产品编码（需填写）", "产品编码", "商品编码")
PRODUCT_IMAGE_FIELDS = ("产品图片（需填写）", "产品图片")
PRODUCT_NAME_FIELDS = ("产品名称", "产品标题", "商品名称")
CATEGORY_FIELDS = ("产品类型（需填写）", "产品类型", "产品类目", "类目")
PRODUCT_CATEGORY_ALIASES = {
    "外套": "outerwear",
    "夹克": "outerwear",
    "开衫": "outerwear",
    "大衣": "outerwear",
    "风衣": "outerwear",
    "羽绒服": "outerwear",
    "轻外套": "outerwear",
    "上衣": "top",
    "衬衫": "top",
    "T恤": "top",
    "针织衫": "top",
    "女装": "womenwear",
    "配饰": "accessory",
    # VN scarf cross-market, Phase 3: the scarf category alias.  Only the
    # operator-facing local names are added here; the sync mechanism itself is
    # untouched, and ASCII categories pass through unchanged.
    "围巾": "scarf",
    "披肩": "scarf",
    "丝巾": "scarf",
    "脖套": "scarf",
}
IMAGE_SUFFIX_BY_MIME = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return "".join(
            str(item.get("text") or item.get("name") or "")
            if isinstance(item, Mapping) else str(item)
            for item in value
        ).strip()
    return str(value).strip()


def _first_field(fields: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        value = fields.get(name)
        if value not in (None, "", []):
            return value
    return None


def _safe_segment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return cleaned or "unknown"


def normalize_product_category(value: Any) -> str:
    """Return the canonical product type used by OPV look compatibility."""
    text = _text(value)
    if not text:
        return ""
    return PRODUCT_CATEGORY_ALIASES.get(text, text if text.isascii() else "")


@dataclass(frozen=True)
class OperationProductPackCandidate:
    record_id: str
    product_id: str
    references: List[str]
    product_name: str = ""
    category: str = ""


class FeishuOperationProductPackSource:
    """Materialize matching Feishu attachment groups into a stable local cache."""

    def __init__(
        self,
        client=None,
        *,
        client_factory: Optional[Callable[[], Any]] = None,
        cache_root: Optional[Path] = None,
    ):
        if client is None and client_factory is None:
            raise ValueError("client or client_factory is required")
        self._client = client
        self._client_factory = client_factory
        self.cache_root = Path(cache_root or (
            Path.home() / ".openclaw" / "shared" / "data"
            / "opv_operation_reference_cache"
        )).expanduser()

    def list_candidates(self, product_id: str) -> List[OperationProductPackCandidate]:
        requested = str(product_id).strip()
        candidates: List[OperationProductPackCandidate] = []
        client = self._get_client()
        for record in client.list_records(page_size=500):
            fields = record.fields or {}
            current = _text(_first_field(fields, PRODUCT_ID_FIELDS))
            if current != requested:
                continue
            attachments = _first_field(fields, PRODUCT_IMAGE_FIELDS) or []
            if not isinstance(attachments, list):
                continue
            references = self._materialize_group(
                requested, str(record.record_id), attachments
            )
            if references:
                candidates.append(OperationProductPackCandidate(
                    record_id=str(record.record_id),
                    product_id=requested,
                    references=references,
                    product_name=_text(_first_field(fields, PRODUCT_NAME_FIELDS)),
                    category=normalize_product_category(
                        _first_field(fields, CATEGORY_FIELDS)
                    ),
                ))
        return candidates

    def _get_client(self):
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    def _materialize_group(
        self, product_id: str, record_id: str, attachments: Sequence[Any]
    ) -> List[str]:
        group_root = self.cache_root / _safe_segment(product_id) / _safe_segment(record_id)
        group_root.mkdir(parents=True, exist_ok=True)
        manifest_path = group_root / "manifest.json"
        manifest = self._load_manifest(manifest_path)
        cached = dict(manifest.get("files") or {})
        result: List[str] = []
        updated: Dict[str, str] = {}
        for index, attachment in enumerate(attachments, start=1):
            if not isinstance(attachment, Mapping):
                continue
            token = str(attachment.get("file_token") or "").strip()
            if not token:
                continue
            prior = group_root / str(cached.get(token) or "")
            if cached.get(token) and prior.is_file() and prior.stat().st_size > 0:
                path = prior
            else:
                content, name, content_type, _size = (
                    self._get_client().download_attachment_bytes(dict(attachment))
                )
                suffix = self._image_suffix(name, content_type)
                if not suffix or not content:
                    continue
                digest = hashlib.sha256(content).hexdigest()
                path = group_root / f"{index:02d}_{digest[:16]}{suffix}"
                if not path.exists():
                    path.write_bytes(content)
            updated[token] = path.name
            result.append(str(path.resolve()))
        self._write_manifest(manifest_path, {"files": updated})
        return result

    @staticmethod
    def _image_suffix(name: str, content_type: str) -> str:
        suffix = Path(str(name)).suffix.lower()
        if suffix == ".jpeg":
            suffix = ".jpg"
        if suffix in {".jpg", ".png", ".webp"}:
            return suffix
        mime = str(content_type or mimetypes.guess_type(str(name))[0] or "").lower()
        return IMAGE_SUFFIX_BY_MIME.get(mime, "")

    @staticmethod
    def _load_manifest(path: Path) -> Dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    @staticmethod
    def _write_manifest(path: Path, payload: Dict[str, Any]) -> None:
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temporary.replace(path)
