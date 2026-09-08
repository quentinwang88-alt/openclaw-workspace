"""Persona Pack: role-aware persona reference readiness for human scenes.

A persona becomes a system asset ("who the person is") with typed evidence:
face identity, full-body proportion, and motion. Scene-model production
requires at least ``MIN_HUMAN_SCENE_REFS`` approved local references covering
both face and full-body roles; a single close-up selfie can no longer pass
preflight. Legacy reference JSON without ``role`` stays readable — it just
does not satisfy the human-scene evidence contract.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

FACE_ROLES = {"FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER"}
BODY_ROLES = {"BODY_FULL_NEUTRAL", "BODY_FULL_MOTION"}
FULL_BODY_ROLES = {"BODY_FULL_NEUTRAL", "BODY_FULL_MOTION"}
KNOWN_ROLES = FACE_ROLES | BODY_ROLES
MIN_HUMAN_SCENE_REFS = 3
# Identity order for generation references: face evidence first, then body.
ROLE_ORDER = [
    "FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER",
    "BODY_FULL_NEUTRAL", "BODY_FULL_MOTION",
]


def normalize_reference_items(persona_snapshot: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Normalize legacy/current persona reference JSON into typed items.

    Accepts dicts (``local_path``/``role``/``approved``/``sha256``) or plain
    path strings; missing ``role`` yields ``""`` (readable, never role-evidence).
    """
    raw_items: List[Any] = []
    for key in ("reference_items", "reference_assets", "reference_images"):
        value = persona_snapshot.get(key)
        if isinstance(value, list) and value:
            raw_items = list(value)
            break
    if not raw_items:
        raw_items = [
            {"local_path": str(path)}
            for path in (persona_snapshot.get("local_reference_images") or [])
        ]
    items: List[Dict[str, Any]] = []
    for entry in raw_items:
        if isinstance(entry, str):
            entry: Dict[str, Any] = {"local_path": entry}
        elif not isinstance(entry, Mapping):
            continue
        path = str(entry.get("local_path") or entry.get("path") or "").strip()
        if not path or not Path(path).is_file():
            continue
        item = {
            "local_path": path,
            "sha256": str(entry.get("sha256") or _sha256(Path(path))),
            "role": str(entry.get("role") or "").strip().upper(),
            "approved": bool(entry.get("approved", True)),
        }
        if entry.get("name"):
            item["name"] = str(entry.get("name"))
        if entry.get("is_primary") is not None:
            item["is_primary"] = bool(entry.get("is_primary"))
        if entry.get("note"):
            item["note"] = str(entry.get("note"))
        items.append(item)
    return items


def build_persona_pack(persona_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    """Summarize an approved, file-backed persona pack from a persona snapshot."""
    items = [
        dict(item) for item in normalize_reference_items(persona_snapshot)
        if item["approved"]
    ]
    roles = {item["role"] for item in items}
    return {
        "persona_pack_id": str(
            persona_snapshot.get("persona_id")
            or persona_snapshot.get("ref_id") or ""
        ),
        "items": items,
        "roles": sorted(role for role in roles if role in KNOWN_ROLES),
        "face_count": len([item for item in items if item["role"] in FACE_ROLES]),
        "full_body_count": len([item for item in items if item["role"] in FULL_BODY_ROLES]),
        "approved_count": len(items),
    }


def evaluate_persona_pack(
    pack: Mapping[str, Any], *, minimum_refs: int = MIN_HUMAN_SCENE_REFS,
) -> Dict[str, Any]:
    """Program-side readiness verdict; every failure carries an explicit code."""
    issues: List[Dict[str, str]] = []
    approved = int(pack.get("approved_count") or 0)
    if approved < minimum_refs:
        issues.append({
            "code": "persona_pack_insufficient_refs",
            "message": f"真人场景人物包需至少 {minimum_refs} 张已批准本地参考，当前 {approved} 张",
        })
    if not pack.get("face_count"):
        issues.append({
            "code": "persona_pack_face_missing",
            "message": "人物包缺少正面/三四分之一侧面脸部证据（FACE_FRONT_NEUTRAL 或 FACE_THREE_QUARTER）",
        })
    if not pack.get("full_body_count"):
        issues.append({
            "code": "persona_pack_full_body_missing",
            "message": "人物包缺少全身证据（BODY_FULL_NEUTRAL 或 BODY_FULL_MOTION）；单张近景自拍不能支撑真人场景生产",
        })
    return {
        "ready": not issues,
        "persona_pack_id": str(pack.get("persona_pack_id") or ""),
        "roles": list(pack.get("roles") or []),
        "approved_count": approved,
        "issues": issues,
    }


def require_human_scene_pack(persona_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    """Evaluate and raise-ready summary for human-scene production."""
    pack = build_persona_pack(persona_snapshot)
    evaluation = evaluate_persona_pack(pack)
    return {"pack": pack, "evaluation": evaluation}


def identity_reference_paths(pack: Mapping[str, Any]) -> List[str]:
    """Pack paths ordered for identity anchoring (face first, then body)."""
    items = [dict(item) for item in pack.get("items") or [] if item.get("local_path")]
    items.sort(key=lambda item: ROLE_ORDER.index(item["role"])
               if item.get("role") in ROLE_ORDER else len(ROLE_ORDER))
    return [str(item["local_path"]) for item in items]


def ordered_reference_items(persona_snapshot: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Approved, file-backed items in identity order, for generation references."""
    items = [
        dict(item) for item in normalize_reference_items(persona_snapshot)
        if item["approved"]
    ]
    items.sort(key=lambda item: ROLE_ORDER.index(item["role"])
               if item.get("role") in ROLE_ORDER else len(ROLE_ORDER))
    return items


def select_identity_references(persona_snapshot: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Default generation identity set: primary face + one suitable full body.

    人物身份与拍照姿态分离（2026-09-08）：主脸图负责长相/发型/身材，
    表情、视线、动作由当页场景决定——歪头/微笑的辅助图不再默认传入，
    避免生成端照搬母图姿态。没有合适全身图时允许只用主图。
    """
    items = ordered_reference_items(persona_snapshot)
    primary = next(
        (item for item in items
         if item.get("is_primary") and item.get("role") in FACE_ROLES),
        None,
    )
    face = primary or next(
        (item for item in items if item.get("role") in FACE_ROLES), None)
    body = next(
        (item for item in items if item.get("role") in FULL_BODY_ROLES), None)
    selected = [item for item in (face, body) if item]
    return selected or items[:1]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
