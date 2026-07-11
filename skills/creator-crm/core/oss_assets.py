#!/usr/bin/env python3
"""OSS storage helpers for Creator CRM process assets."""

from __future__ import annotations

import hashlib
import mimetypes
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return default


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_path_part(value: Any, fallback: str = "unknown") -> str:
    text = str(value or "").strip().lower()
    text = text.lstrip("@")
    text = re.sub(r"[^a-zA-Z0-9._-]+", "_", text).strip("._-")
    return text[:80] or fallback


def ascii_header_filename(name: str) -> str:
    return "".join(ch if 32 <= ord(ch) < 127 and ch not in {'"', "\\"} else "_" for ch in name)


@dataclass
class OSSUploadResult:
    provider: str
    bucket: str
    object_key: str
    public_url: str
    file_name: str
    file_size: int
    file_hash: str
    storage_status: str = "uploaded"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "bucket": self.bucket,
            "object_key": self.object_key,
            "public_url": self.public_url,
            "file_name": self.file_name,
            "file_size": self.file_size,
            "file_hash": self.file_hash,
            "storage_status": self.storage_status,
        }


class CreatorAssetStorage:
    def __init__(
        self,
        provider: str = "",
        bucket: str = "",
        root: str = "",
        endpoint: str = "",
        access_key_id: str = "",
        access_key_secret: str = "",
        security_token: str = "",
        public_base_url: str = "",
        object_prefix: str = "",
    ):
        self.provider = (provider or _env_first("CREATOR_CRM_OSS_PROVIDER", "AUTO_MIXCUT_OSS_PROVIDER", default="local")).strip().lower()
        self.bucket = bucket or _env_first("CREATOR_CRM_OSS_BUCKET", "AUTO_MIXCUT_OSS_BUCKET", "ALIYUN_OSS_BUCKET", "AUTO_MIXCUT_BUCKET", default="creator-crm")
        self.root = Path(root or _env_first("CREATOR_CRM_OSS_ROOT", "AUTO_MIXCUT_OSS_ROOT", default="/tmp/creator-crm-oss")).resolve()
        self.endpoint = endpoint or _env_first("CREATOR_CRM_ALIYUN_OSS_ENDPOINT", "AUTO_MIXCUT_ALIYUN_OSS_ENDPOINT", "ALIYUN_OSS_ENDPOINT")
        self.access_key_id = access_key_id or _env_first("CREATOR_CRM_ALIYUN_ACCESS_KEY_ID", "AUTO_MIXCUT_ALIYUN_ACCESS_KEY_ID", "ALIYUN_OSS_ACCESS_KEY_ID")
        self.access_key_secret = access_key_secret or _env_first("CREATOR_CRM_ALIYUN_ACCESS_KEY_SECRET", "AUTO_MIXCUT_ALIYUN_ACCESS_KEY_SECRET", "ALIYUN_OSS_ACCESS_KEY_SECRET")
        self.security_token = security_token or _env_first("CREATOR_CRM_ALIYUN_SECURITY_TOKEN", "AUTO_MIXCUT_ALIYUN_SECURITY_TOKEN", "ALIYUN_OSS_SECURITY_TOKEN")
        self.public_base_url = (public_base_url or _env_first("CREATOR_CRM_ALIYUN_OSS_PUBLIC_BASE_URL", "AUTO_MIXCUT_ALIYUN_OSS_PUBLIC_BASE_URL", "ALIYUN_OSS_PUBLIC_BASE_URL", "CREATOR_CRM_OSS_PUBLIC_BASE_URL")).rstrip("/")
        self.object_prefix = (object_prefix or os.environ.get("CREATOR_CRM_OSS_PREFIX") or "creator-crm").strip("/")
        self._bucket_client = None

    @classmethod
    def from_env(cls) -> Optional["CreatorAssetStorage"]:
        enabled = os.environ.get("CREATOR_CRM_OSS_ENABLED", "").strip().lower()
        provider = os.environ.get("CREATOR_CRM_OSS_PROVIDER") or os.environ.get("AUTO_MIXCUT_OSS_PROVIDER") or ""
        if enabled in {"0", "false", "no"}:
            return None
        if not provider and enabled not in {"1", "true", "yes"}:
            return None
        return cls()

    def upload_grid(self, source: Path, *, creator_uid: str = "", tk_handle: str = "", run_id: str = "") -> OSSUploadResult:
        source = Path(source)
        creator_part = safe_path_part(creator_uid or tk_handle)
        date_part = datetime.now().strftime("%Y/%m/%d")
        timestamp = datetime.now().strftime("%H%M%S")
        object_key = f"{self.object_prefix}/grids/{date_part}/{creator_part}/{timestamp}_{safe_path_part(run_id, 'run')}_{source.name}"
        return self.upload(source, object_key)

    def upload(self, source: Path, object_key: str) -> OSSUploadResult:
        if self.provider == "local":
            return self._upload_local(source, object_key)
        if self.provider == "aliyun":
            return self._upload_aliyun(source, object_key)
        raise RuntimeError(f"unknown CREATOR_CRM_OSS_PROVIDER: {self.provider}")

    def signed_url(self, object_key: str, expires_seconds: int = 86400) -> str:
        if self.provider == "local":
            base_url = os.environ.get("CREATOR_CRM_OSS_PUBLIC_BASE_URL", "").rstrip("/")
            if base_url:
                return f"{base_url}/{object_key}?expires={expires_seconds}"
            return f"file://{self.root / object_key}?expires={expires_seconds}"
        if self.public_base_url:
            return f"{self.public_base_url}/{object_key}"
        bucket = self._get_aliyun_bucket()
        return bucket.sign_url("GET", object_key, expires_seconds, slash_safe=True)

    def delete(self, object_key: str) -> None:
        if self.provider == "local":
            (self.root / object_key).unlink(missing_ok=True)
            return
        bucket = self._get_aliyun_bucket()
        bucket.delete_object(object_key)

    def _upload_local(self, source: Path, object_key: str) -> OSSUploadResult:
        dest = self.root / object_key
        dest.parent.mkdir(parents=True, exist_ok=True)
        source_hash = file_sha256(source)
        source_size = source.stat().st_size
        if source.resolve() != dest.resolve():
            shutil.copy2(source, dest)
        if dest.stat().st_size != source_size or file_sha256(dest) != source_hash:
            raise RuntimeError(f"local OSS upload verify failed: {object_key}")
        return OSSUploadResult(
            provider="local",
            bucket=self.bucket,
            object_key=object_key,
            public_url=self.signed_url(object_key),
            file_name=source.name,
            file_size=source_size,
            file_hash=source_hash,
        )

    def _upload_aliyun(self, source: Path, object_key: str) -> OSSUploadResult:
        source_hash = file_sha256(source)
        source_size = source.stat().st_size
        headers = {
            "x-oss-meta-sha256": source_hash,
            "x-oss-meta-file-size": str(source_size),
            "Content-Type": mimetypes.guess_type(source.name)[0] or "application/octet-stream",
            "Content-Disposition": f'inline; filename="{ascii_header_filename(source.name)}"',
        }
        bucket = self._get_aliyun_bucket()
        last_exc: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                bucket.put_object_from_file(object_key, str(source), headers=headers)
                head = bucket.head_object(object_key)
                remote_size = int(getattr(head, "content_length", 0) or 0)
                if remote_size != source_size:
                    raise RuntimeError(f"uploaded size mismatch: local={source_size}, remote={remote_size}")
                return OSSUploadResult(
                    provider="aliyun",
                    bucket=self.bucket,
                    object_key=object_key,
                    public_url=self.signed_url(object_key),
                    file_name=source.name,
                    file_size=source_size,
                    file_hash=source_hash,
                )
            except Exception as exc:
                last_exc = exc
                if attempt >= 3 or not self._is_retryable(exc):
                    break
                time.sleep(0.5 * attempt)
        raise RuntimeError(f"Aliyun OSS upload failed: {last_exc}")

    def _get_aliyun_bucket(self):
        if self._bucket_client:
            return self._bucket_client
        missing = []
        if not self.bucket:
            missing.append("CREATOR_CRM_OSS_BUCKET")
        if not self.endpoint:
            missing.append("CREATOR_CRM_ALIYUN_OSS_ENDPOINT")
        if not self.access_key_id:
            missing.append("CREATOR_CRM_ALIYUN_ACCESS_KEY_ID")
        if not self.access_key_secret:
            missing.append("CREATOR_CRM_ALIYUN_ACCESS_KEY_SECRET")
        if missing:
            raise RuntimeError(f"Aliyun OSS config missing: {', '.join(missing)}")
        try:
            import oss2
        except ImportError as exc:
            raise RuntimeError("oss2 package is required for CREATOR_CRM_OSS_PROVIDER=aliyun") from exc
        auth = oss2.StsAuth(self.access_key_id, self.access_key_secret, self.security_token) if self.security_token else oss2.Auth(self.access_key_id, self.access_key_secret)
        self._bucket_client = oss2.Bucket(auth, self.endpoint, self.bucket)
        return self._bucket_client

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        text = str(exc).lower()
        if any(token in text for token in ["accessdenied", "forbidden", "invalidaccesskeyid", "signaturedoesnotmatch", "status': 403", "status=403"]):
            return False
        return any(token in text for token in ["ssleoferror", "connection reset", "read timed out", "connect timeout", "service unavailable"])
