from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from core.storage import default_db_path


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
ROLE_PRIORITY = {
    "PRODUCT_REFERENCE": 0,
    "PERSONA_REFERENCE": 1,
    "SUPPORTING_REFERENCE": 2,
    "COMPOSITE_FIRST_FRAME": 3,
}
ROLE_AUTHORITY = {
    "PRODUCT_REFERENCE": "PRODUCT_APPEARANCE_AUTHORITY",
    "PERSONA_REFERENCE": "PERSONA_IDENTITY_AUTHORITY",
    "COMPOSITE_FIRST_FRAME": "COMPOSITION_ONLY",
    "SUPPORTING_REFERENCE": "SUPPORTING_ONLY",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _local_paths(value: Any, *, role: str = "SUPPORTING_REFERENCE") -> list[Dict[str, str]]:
    results: list[Dict[str, str]] = []
    if isinstance(value, Mapping):
        local_role = _text(value.get("role")) or role
        for key in ("local_path", "cached_path", "path"):
            candidate = Path(_text(value.get(key))).expanduser()
            if candidate.is_file() and candidate.suffix.lower() in IMAGE_SUFFIXES:
                results.append({"role": local_role, "source_path": str(candidate.resolve()),
                                "expected_sha256": _text(value.get("sha256"))})
        for key, nested in value.items():
            normalized = str(key).lower()
            nested_role = local_role
            if any(token in normalized for token in (
                "persona_reference", "persona_asset", "人物参考", "人物图片",
            )):
                nested_role = "PERSONA_REFERENCE"
            elif any(token in normalized for token in (
                "product_reference", "product_image", "product_asset",
                "商品参考", "商品图片", "产品图片",
            )):
                nested_role = "PRODUCT_REFERENCE"
            elif any(token in normalized for token in (
                "composite_first_frame", "unified_first_frame", "统一首帧",
            )):
                nested_role = "COMPOSITE_FIRST_FRAME"
            results.extend(_local_paths(nested, role=nested_role))
    elif isinstance(value, (list, tuple)):
        for item in value:
            results.extend(_local_paths(item, role=role))
    return results


def _ready_first_frame_assets(script_id: str) -> list[Dict[str, str]]:
    if not script_id:
        return []
    db = default_db_path()
    if not db.is_file():
        return []
    try:
        with sqlite3.connect(str(db), timeout=2) as conn:
            row = conn.execute(
                """SELECT a.local_path, a.asset_id, a.asset_fingerprint, a.contract_json
                   FROM original_first_frame_binding b
                   JOIN original_first_frame_asset a
                     ON a.asset_fingerprint=b.asset_fingerprint
                   WHERE b.script_id=? AND b.status='READY' AND a.status='READY'
                   LIMIT 1""",
                (script_id,),
            ).fetchone()
    except sqlite3.Error:
        return []
    if not row:
        return []
    results: list[Dict[str, str]] = []
    try:
        contract = json.loads(_text(row[3]) or "{}")
    except json.JSONDecodeError:
        contract = {}
    for item in contract.get("cached_reference_assets") or []:
        if not isinstance(item, Mapping):
            continue
        path = Path(_text(item.get("local_path"))).expanduser()
        if path.is_file():
            results.append({
                "role": _text(item.get("role")) or "SUPPORTING_REFERENCE",
                "source_path": str(path.resolve()),
                "source_asset_id": "",
                "source_fingerprint": _text(item.get("sha256")),
            })
    path = Path(_text(row[0])).expanduser()
    if not path.is_file():
        return results
    results.append({
        "role": "COMPOSITE_FIRST_FRAME",
        "source_path": str(path.resolve()),
        "source_asset_id": _text(row[1]),
        "source_fingerprint": _text(row[2]),
    })
    return results


def freeze_reference_assets(
    *,
    job_id: str,
    asset_root: str | Path,
    source_script_id: str = "",
    materials: Iterable[Any] = (),
    persona_lock: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Copy currently available references into the isolated longform job."""

    candidates: list[Dict[str, str]] = []
    for material in materials:
        candidates.extend(_local_paths(material))
    candidates.extend(_ready_first_frame_assets(source_script_id))
    unique: Dict[str, Dict[str, str]] = {}
    for item in candidates:
        path = Path(item["source_path"])
        if path.is_file():
            digest = _sha256(path)
            expected = _text(item.get("expected_sha256"))
            if expected and item.get("role") in {"PRODUCT_REFERENCE", "PERSONA_REFERENCE"} and digest != expected:
                raise ValueError("REFERENCE_CONTENT_CHANGED: 冻结参考图与已分析图片指纹不一致，请恢复原图或replan")
            previous = unique.get(digest)
            if previous is None or ROLE_PRIORITY.get(
                item.get("role") or "SUPPORTING_REFERENCE", 99
            ) < ROLE_PRIORITY.get(previous.get("role") or "SUPPORTING_REFERENCE", 99):
                unique[digest] = item

    target_root = Path(asset_root).expanduser().resolve() / job_id / "frozen_references"
    target_root.mkdir(parents=True, exist_ok=True)
    frozen = []
    ordered = sorted(
        unique.items(),
        key=lambda pair: (ROLE_PRIORITY.get(pair[1].get("role") or "", 99), pair[0]),
    )
    for index, (digest, item) in enumerate(ordered, 1):
        source = Path(item["source_path"])
        target = target_root / f"ref_{index:02d}_{digest[:10]}{source.suffix.lower()}"
        if not target.is_file():
            shutil.copy2(source, target)
        role = item.get("role") or "SUPPORTING_REFERENCE"
        frozen.append({
            "role": role,
            "authority": ROLE_AUTHORITY.get(role, "SUPPORTING_ONLY"),
            "local_path": str(target),
            "sha256": digest,
            "source_asset_id": item.get("source_asset_id") or "",
            "source_fingerprint": item.get("source_fingerprint") or "",
        })
    roles = {item["role"] for item in frozen}
    if "PRODUCT_REFERENCE" in roles and "PERSONA_REFERENCE" in roles:
        reference_mode = "RAW_PRODUCT_AND_PERSONA"
    elif "PRODUCT_REFERENCE" in roles:
        reference_mode = "RAW_PRODUCT"
    elif "COMPOSITE_FIRST_FRAME" in roles:
        reference_mode = "COMPOSITE_FALLBACK"
    elif "PERSONA_REFERENCE" in roles:
        reference_mode = "PERSONA_ONLY"
    else:
        reference_mode = "UNAVAILABLE"
    frozen_persona_hashes = sorted(
        item["sha256"] for item in frozen if item["role"] == "PERSONA_REFERENCE"
    )
    persona = dict(persona_lock or {})
    manifest_persona_lock = {
        "persona_id": _text(persona.get("persona_id")),
        "structured_snapshot_hash": _text(persona.get("structured_snapshot_hash")),
        "reference_asset_sha256s": frozen_persona_hashes,
        "status": (
            "FROZEN" if _text(persona.get("persona_id")) and frozen_persona_hashes
            else "UNAVAILABLE"
        ),
    }
    manifest = {
        "schema_version": "longform-frozen-reference-assets-v2-authority",
        "status": "AVAILABLE" if frozen else "UNAVAILABLE",
        "job_id": job_id,
        "reference_mode": reference_mode,
        "product_authority_status": (
            "RAW_PRODUCT_AVAILABLE"
            if "PRODUCT_REFERENCE" in roles else "COMPOSITE_ONLY"
            if "COMPOSITE_FIRST_FRAME" in roles else "UNAVAILABLE"
        ),
        "assets": frozen,
        "persona_lock": manifest_persona_lock,
        "authority_note": (
            "PRODUCT_REFERENCE控制商品；PERSONA_REFERENCE控制人物身份；"
            "COMPOSITE_FIRST_FRAME仅作为已批准合成构图参考，不伪装成商品原图。"
        ),
    }
    (target_root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest
