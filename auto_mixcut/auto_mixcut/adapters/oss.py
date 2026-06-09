from __future__ import annotations

import hashlib
import mimetypes
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result


class LocalOSS:
    def __init__(self, root: Path, bucket: str):
        self.root = root
        self.bucket = bucket
        self.root.mkdir(parents=True, exist_ok=True)

    def upload(self, source: Path, object_key: str) -> Result:
        try:
            dest = self.root / object_key
            dest.parent.mkdir(parents=True, exist_ok=True)
            source_size = source.stat().st_size
            source_hash = file_sha256(source)
            if source.resolve() != dest.resolve():
                shutil.copy2(source, dest)
            dest_size = dest.stat().st_size
            dest_hash = file_sha256(dest)
            if dest_size != source_size or dest_hash != source_hash:
                return Result.fail(
                    "OSS_UPLOAD_VERIFY_FAILED",
                    "uploaded file size/hash does not match source",
                    {"source": str(source), "object_key": object_key, "source_size": source_size, "dest_size": dest_size},
                )
            meta = {
                "object_id": new_id("OSS"),
                "bucket": self.bucket,
                "object_key": object_key,
                "file_name": source.name,
                "file_ext": source.suffix.lstrip("."),
                "file_size": dest_size,
                "file_hash": dest_hash,
                "storage_status": "uploaded",
            }
            return Result.ok(meta)
        except Exception as exc:
            return Result.fail("OSS_UPLOAD_FAILED", str(exc), {"source": str(source), "object_key": object_key})

    def download(self, object_key: str, dest: Path) -> Result:
        try:
            source = self.root / object_key
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest)
            return Result.ok({"path": str(dest)})
        except Exception as exc:
            return Result.fail("OSS_DOWNLOAD_FAILED", str(exc), {"object_key": object_key})

    def signed_url(self, object_key: str, expires_seconds: int = 86400) -> str:
        base_url = os.environ.get("AUTO_MIXCUT_PREVIEW_BASE_URL", "").rstrip("/")
        if base_url:
            return f"{base_url}/{object_key}?expires={expires_seconds}"
        return f"file://{self.root / object_key}?expires={expires_seconds}"

    def delete(self, object_key: str) -> Result:
        try:
            path = self.root / object_key
            path.unlink(missing_ok=True)
            return Result.ok({"object_key": object_key, "storage_status": "deleted"})
        except Exception as exc:
            return Result.fail("OSS_DELETE_FAILED", str(exc), {"object_key": object_key})


class AliyunOSS:
    def __init__(
        self,
        bucket: str,
        endpoint: str,
        access_key_id: str,
        access_key_secret: str,
        security_token: str = "",
        public_base_url: str = "",
    ):
        missing = []
        if not bucket:
            missing.append("AUTO_MIXCUT_OSS_BUCKET")
        if not endpoint:
            missing.append("AUTO_MIXCUT_ALIYUN_OSS_ENDPOINT")
        if not access_key_id:
            missing.append("AUTO_MIXCUT_ALIYUN_ACCESS_KEY_ID")
        if not access_key_secret:
            missing.append("AUTO_MIXCUT_ALIYUN_ACCESS_KEY_SECRET")
        if missing:
            raise RuntimeError(f"Aliyun OSS config missing: {', '.join(missing)}")
        try:
            import oss2
        except ImportError as exc:
            raise RuntimeError("oss2 package is required for AUTO_MIXCUT_OSS_PROVIDER=aliyun") from exc

        self.bucket_name = bucket
        self.endpoint = endpoint
        self.public_base_url = public_base_url.rstrip("/")
        if security_token:
            auth = oss2.StsAuth(access_key_id, access_key_secret, security_token)
        else:
            auth = oss2.Auth(access_key_id, access_key_secret)
        self._bucket = oss2.Bucket(auth, endpoint, bucket)

    def upload(self, source: Path, object_key: str) -> Result:
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                return self._upload_once(source, object_key)
            except Exception as exc:
                last_exc = exc
                if not _is_retryable_oss_error(exc) or attempt >= 3:
                    break
                time.sleep(0.5 * attempt)
        exc = last_exc or RuntimeError("unknown upload error")
        return Result.fail("OSS_UPLOAD_FAILED", str(exc), {"source": str(source), "object_key": object_key, "provider": "aliyun", "retry_attempts": 3})

    def _upload_once(self, source: Path, object_key: str) -> Result:
        try:
            source_size = source.stat().st_size
            source_hash = file_sha256(source)
            headers = {
                "x-oss-meta-sha256": source_hash,
                "x-oss-meta-file-size": str(source_size),
                "Content-Type": mimetypes.guess_type(source.name)[0] or "application/octet-stream",
                "Content-Disposition": f'inline; filename="{_ascii_header_filename(source.name)}"',
            }
            self._bucket.put_object_from_file(object_key, str(source), headers=headers)
            head = self._bucket.head_object(object_key)
            remote_size = int(getattr(head, "content_length", 0) or 0)
            remote_hash = _header_get(getattr(head, "headers", {}), "x-oss-meta-sha256")
            if remote_size != source_size or (remote_hash and remote_hash != source_hash):
                return Result.fail(
                    "OSS_UPLOAD_VERIFY_FAILED",
                    "uploaded object size/hash metadata does not match source",
                    {"source": str(source), "object_key": object_key, "source_size": source_size, "remote_size": remote_size},
                )
            meta = {
                "object_id": new_id("OSS"),
                "bucket": self.bucket_name,
                "object_key": object_key,
                "file_name": source.name,
                "file_ext": source.suffix.lstrip("."),
                "file_size": source_size,
                "file_hash": source_hash,
                "storage_status": "uploaded",
            }
            return Result.ok(meta)
        except Exception as exc:
            raise exc

    def download(self, object_key: str, dest: Path) -> Result:
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            self._bucket.get_object_to_file(object_key, str(dest))
            return Result.ok({"path": str(dest)})
        except Exception as exc:
            return Result.fail("OSS_DOWNLOAD_FAILED", str(exc), {"object_key": object_key, "provider": "aliyun"})

    def signed_url(self, object_key: str, expires_seconds: int = 86400) -> str:
        if self.public_base_url:
            return f"{self.public_base_url}/{object_key}"
        params = {}
        if Path(object_key).suffix.lower() == ".mp4":
            params["response-content-disposition"] = f'inline; filename="{_ascii_header_filename(Path(object_key).name)}"'
        return self._bucket.sign_url("GET", object_key, expires_seconds, params=params, slash_safe=True)

    def delete(self, object_key: str) -> Result:
        try:
            self._bucket.delete_object(object_key)
            return Result.ok({"object_key": object_key, "storage_status": "deleted", "provider": "aliyun"})
        except Exception as exc:
            return Result.fail("OSS_DELETE_FAILED", str(exc), {"object_key": object_key, "provider": "aliyun"})


def build_oss(settings: Any):
    if settings.oss_provider == "local":
        return LocalOSS(settings.oss_root, settings.bucket)
    if settings.oss_provider == "aliyun":
        return AliyunOSS(
            bucket=settings.bucket,
            endpoint=settings.aliyun_oss_endpoint,
            access_key_id=settings.aliyun_access_key_id,
            access_key_secret=settings.aliyun_access_key_secret,
            security_token=settings.aliyun_security_token,
            public_base_url=settings.aliyun_public_base_url,
        )
    raise RuntimeError(f"unknown AUTO_MIXCUT_OSS_PROVIDER: {settings.oss_provider}")


def _is_retryable_oss_error(exc: Exception) -> bool:
    text = str(exc).lower()
    if any(token in text for token in ["accessdenied", "forbidden", "invalidaccesskeyid", "signaturedoesnotmatch", "status': 403", "status=403"]):
        return False
    return any(
        token in text
        for token in [
            "ssleoferror",
            "eof occurred",
            "connection reset",
            "connection aborted",
            "read timed out",
            "connect timeout",
            "max retries exceeded",
            "temporarily unavailable",
            "service unavailable",
        ]
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _header_get(headers: Any, key: str) -> str:
    if not headers:
        return ""
    if hasattr(headers, "get"):
        return str(headers.get(key) or headers.get(key.lower()) or headers.get(key.upper()) or "").strip()
    return ""


def _ascii_header_filename(name: str) -> str:
    safe = []
    for char in Path(str(name or "file")).name:
        code = ord(char)
        safe.append(char if 32 <= code < 127 and char not in {'"', "\\"} else "_")
    value = "".join(safe).strip("._ ")
    return value or "file"
