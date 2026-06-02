from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.result import Result


def resolve_oss_object_path(ctx, object_id: str | None, cache_group: str = "objects") -> Result:
    if not object_id:
        return Result.fail("OSS_OBJECT_ID_REQUIRED", "object_id is required")
    obj = ctx.repo.get("oss_objects", "object_id", object_id)
    if not obj:
        return Result.fail("OSS_OBJECT_NOT_FOUND", "oss object not found", {"object_id": object_id})
    object_key = obj.get("object_key")
    if not object_key:
        return Result.fail("OSS_OBJECT_KEY_MISSING", "oss object has no object_key", {"object_id": object_id})

    local_path = ctx.settings.oss_root / object_key
    if getattr(ctx.settings, "oss_provider", "local") == "local" and local_path.exists():
        return Result.ok({"path": str(local_path), "object": obj, "source": "local"})

    cache_path = ctx.settings.oss_cache_root / cache_group / object_key
    if cache_path.exists() and _cache_valid(cache_path, obj):
        return Result.ok({"path": str(cache_path), "object": obj, "source": "cache"})

    downloaded = ctx.oss.download(object_key, cache_path)
    if not downloaded.success:
        return downloaded
    if not cache_path.exists():
        return Result.fail("OSS_DOWNLOAD_FAILED", "download finished but local cache file is missing", {"object_key": object_key, "path": str(cache_path)})
    if not _cache_valid(cache_path, obj):
        return Result.fail("OSS_DOWNLOAD_VERIFY_FAILED", "downloaded cache file size/hash does not match metadata", {"object_key": object_key, "path": str(cache_path)})
    return Result.ok({"path": str(cache_path), "object": obj, "source": "download"})


def require_oss_object_path(ctx, object_id: str | None, cache_group: str = "objects") -> Path | None:
    resolved = resolve_oss_object_path(ctx, object_id, cache_group)
    if not resolved.success:
        return None
    return Path(resolved.data["path"])


def _cache_valid(path: Path, obj: dict) -> bool:
    expected_size = int(obj.get("file_size") or 0)
    if expected_size and path.stat().st_size != expected_size:
        return False
    expected_hash = str(obj.get("file_hash") or "")
    if expected_hash:
        from auto_mixcut.adapters.oss import file_sha256
        return file_sha256(path) == expected_hash
    return True
