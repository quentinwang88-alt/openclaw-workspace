"""Read-only operation-table discovery; no OPV resolver or database writes."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from .contracts import SourceSnapshot


def _module(name: str, path: Path):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def operation_source():
    workspace = Path(__file__).resolve().parents[3]
    source = _module("remake_opv_operation_source", workspace / "packages/organic_photo_video/services/operation_product_pack_source.py")
    bitable = _module("remake_reference_bitable", workspace / "skills/script-run-manager-sync/core/bitable.py")

    class TimedClient(bitable.FeishuBitableClient):
        def list_records(self, page_size=500, limit=None):
            self.record_times = {}
            records = []
            token = None
            while True:
                params = {"page_size": 500, "automatic_fields": "true"}
                if token:
                    params["page_token"] = token
                response = self._request("GET", f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records",
                                         headers=self._headers(), params=params).json()
                if response.get("code") != 0:
                    raise RuntimeError("REFERENCE_OPERATION_QUERY_FAILED:" + str(response.get("msg")))
                data = response.get("data") or {}
                for row in data.get("items") or []:
                    record_id = row["record_id"]
                    self.record_times[record_id] = {key: row.get(key) for key in ("created_time", "last_modified_time")}
                    records.append(bitable.TableRecord(record_id=record_id, fields=row.get("fields") or {}))
                if not data.get("has_more"):
                    return records
                next_token = data.get("page_token")
                if not next_token or next_token == token:
                    raise RuntimeError("REFERENCE_OPERATION_PAGINATION_INVALID")
                token = next_token

    class StrictSource(source.FeishuOperationProductPackSource):
        @property
        def record_times(self):
            return getattr(self._get_client(), "record_times", {})

        def _materialize_group(self, product_id, record_id, attachments):
            # OPV skips invalid/empty attachments; execution may not silently
            # accept an incomplete group or treat a download failure as absence.
            paths = super()._materialize_group(product_id, record_id, attachments)
            if len(paths) != len(attachments):
                raise ValueError("REFERENCE_GROUP_IMAGES_INCOMPLETE:" + record_id)
            for raw in paths:
                path = Path(raw)
                cached_digest = path.stem.partition("_")[2]
                if cached_digest and not hashlib.sha256(path.read_bytes()).hexdigest().startswith(cached_digest):
                    raise ValueError("REFERENCE_GROUP_CACHE_CHANGED:" + record_id)
            return paths

    return StrictSource(client_factory=lambda: TimedClient(
        bitable.resolve_wiki_bitable_app_token(source.DEFAULT_OPERATION_WIKI_TOKEN),
        source.DEFAULT_OPERATION_TABLE_ID,
    ))


def discover_groups(candidate_source: Any, product_id: str) -> list[dict]:
    candidates = candidate_source.list_candidates(product_id)
    groups = []
    for candidate in candidates:
        images = []
        for index, raw in enumerate(candidate.references, 1):
            path = Path(raw)
            if not path.is_file() or not path.stat().st_size:
                raise ValueError("REFERENCE_GROUP_IMAGE_UNAVAILABLE:" + candidate.record_id)
            images.append({"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                           "image_index": index, "image_role": "unknown", "role": "PRODUCT_AUTHORITY"})
        if not images:
            raise ValueError("REFERENCE_GROUP_IMAGES_INCOMPLETE:" + candidate.record_id)
        content = hashlib.sha256("\n".join(sorted({i["sha256"] for i in images})).encode()).hexdigest()
        groups.append({"provider": "operation", "group_id": "operation:" + candidate.record_id,
                       "source_record_id": candidate.record_id, "product_id": product_id,
                       "record_times": dict(getattr(candidate_source, "record_times", {}).get(candidate.record_id) or {}),
                       "content_fingerprint": content, "images": images})
    return sorted(groups, key=lambda g: g["group_id"])


def select_group(groups: list[dict], *, bound_group: str = "", explicit_group: str = "") -> dict | None:
    requested = bound_group or explicit_group
    if requested:
        selected = next((g for g in groups if g["group_id"] == requested), None)
        if selected is None:
            raise ValueError("REFERENCE_GROUP_NOT_FOUND:" + requested)
        return {**selected, "selection_basis": {"policy": "SOURCE_BINDING" if bound_group else "EXPLICIT_GROUP"}}
    distinct = {}
    for group in groups:
        distinct.setdefault(group["content_fingerprint"], group)
    if len(distinct) > 1:
        # System metadata is requested with automatic_fields=true. Never use
        # execution timestamps, record IDs, API order, or local cache mtime.
        times = [(g.get("record_times") or {}).get("last_modified_time") for g in groups]
        if all(isinstance(t, (int, float)) and not isinstance(t, bool) and t > 100_000_000_000 for t in times):
            latest = max(times)
            newest = [g for g, t in zip(groups, times) if t == latest]
            if len({g["content_fingerprint"] for g in newest}) == 1:
                return {**sorted(newest, key=lambda g: g["group_id"])[0], "selection_basis": {
                    "policy": "LATEST_RECORD_MODIFIED_TIME", "timestamp_field": "last_modified_time",
                    "timestamp_ms": latest, "timestamp_scope": "RECORD_NOT_ATTACHMENT",
                    "candidate_record_count": len(groups), "distinct_content_count": len(distinct)}}
        raise ValueError("REFERENCE_GROUP_AMBIGUOUS:" + json.dumps([
            {"group_id": g["group_id"], "source_record_id": g["source_record_id"], "image_count": len(g["images"])}
            for g in distinct.values()], ensure_ascii=False))
    selected = next(iter(distinct.values()), None)
    if selected:
        timed = [g for g in groups if isinstance((g.get("record_times") or {}).get("last_modified_time"), (int, float))]
        if timed:
            selected = max(timed, key=lambda g: g["record_times"]["last_modified_time"])
        return {**selected, "selection_basis": {"policy": "UNIQUE_CONTENT_GROUP"}}
    return None


def freeze_group(source: SourceSnapshot, group: dict, output_dir: Path) -> SourceSnapshot:
    output_dir.mkdir(parents=True, exist_ok=True)
    images = []
    for image in group["images"]:
        path = Path(image["path"])
        target = output_dir / f"{image['image_index']:02d}-{image['sha256']}{path.suffix}"
        if not target.exists():
            shutil.copyfile(path, target)
        if hashlib.sha256(target.read_bytes()).hexdigest() != image["sha256"]:
            raise ValueError("FROZEN_PRODUCT_REFERENCE_CHANGED")
        images.append({**image, "path": str(target.resolve())})
    identity = {k: v for k, v in group.items() if k != "images"}
    identity["ordered_image_sha256s"] = [i["sha256"] for i in images]
    identity["content_version_hash"] = hashlib.sha256(json.dumps({
        "group_id": identity["group_id"], "ordered_image_sha256s": identity["ordered_image_sha256s"],
    }, sort_keys=True).encode()).hexdigest()
    identity["status"] = "FROZEN"
    revision = hashlib.sha256(json.dumps({"source_revision_hash": source.source_revision_hash,
                                         "reference_selection": identity}, sort_keys=True).encode()).hexdigest()
    return SourceSnapshot(**{**source.to_dict(), "product_id": group["product_id"],
                            "reference_selection": identity,
                            "reference_manifest": [i for i in source.reference_manifest
                                                   if i.get("role") != "PRODUCT_AUTHORITY"] + images,
                            "source_revision_hash": revision})
