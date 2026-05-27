from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from typing import Dict

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
            shutil.copy2(source, dest)
            meta = {
                "object_id": new_id("OSS"),
                "bucket": self.bucket,
                "object_key": object_key,
                "file_name": source.name,
                "file_ext": source.suffix.lstrip("."),
                "file_size": dest.stat().st_size,
                "file_hash": file_sha256(dest),
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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
