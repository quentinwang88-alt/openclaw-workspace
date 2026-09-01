#!/usr/bin/env python3
"""Import locally mirrored persona reference packs into the shared template DB."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict

SKILL_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SKILL_ROOT.parents[1]
LIGHTWEIGHT_SCRIPTS = SKILL_ROOT.parent / "lightweight-tryon-video" / "scripts"
if str(LIGHTWEIGHT_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(LIGHTWEIGHT_SCRIPTS))

from light_tryon.database import LightTryonDB  # noqa: E402

DEFAULT_DB_PATH = (
    SKILL_ROOT.parent / "lightweight-tryon-video" / "var" / "light_tryon.sqlite3"
)


def _hash(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def import_manifest(manifest_path: Path, db_path: Path, *, apply: bool) -> Dict[str, Any]:
    manifest_path = manifest_path.expanduser().resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "persona-reference-manifest-v1":
        raise ValueError("unsupported persona manifest schema")
    db = LightTryonDB(db_path.expanduser().resolve())
    db.init_schema()
    imported = []
    for raw in payload.get("personas") or []:
        row = dict(raw)
        persona_id = str(row.get("persona_id") or "").strip()
        if not persona_id:
            raise ValueError("persona_id is required")
        references = []
        for value in row.get("reference_images") or []:
            path = Path(str(value))
            if not path.is_absolute():
                path = manifest_path.parent / path
            path = path.resolve()
            if not path.is_file():
                raise FileNotFoundError(f"{persona_id} reference missing: {path}")
            references.append({
                "local_path": str(path),
                "name": path.name,
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "type": "image/png" if path.suffix.lower() == ".png" else "image/jpeg",
            })
        if not references:
            raise ValueError(f"{persona_id} requires at least one reference")
        row["reference_images"] = references
        row["source_hash"] = _hash({**row, "reference_images": references})
        if apply:
            db.upsert_template("persona", row)
        imported.append({
            "persona_id": persona_id,
            "status": row.get("status"),
            "reference_count": len(references),
            "source_hash": row["source_hash"],
        })
    return {
        "applied": apply,
        "db_path": str(db_path.expanduser().resolve()),
        "manifest_path": str(manifest_path),
        "personas": imported,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = import_manifest(Path(args.manifest), Path(args.db_path), apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
