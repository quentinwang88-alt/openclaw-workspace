"""Asset resolver: read persona / look / scene snapshots from the existing
lightweight-tryon asset SQLite so plans can embed reproducible snapshots.

OPV does not duplicate asset rows into RDS; it references them by ref_id and
snapshots their content into ``plan_json`` (per MODEL_HANDOFF 3.2: shared
underlying capabilities, independent business routes).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
DEFAULT_DB_PATH = (
    WORKSPACE_ROOT / "skills" / "lightweight-tryon-video" / "var" / "light_tryon.sqlite3"
)

READABLE_STATUSES = ("enabled", "testing")


class AssetNotFoundError(KeyError):
    pass


class LightTryonAssetReader:
    """Read-only accessor over the lightweight-tryon asset tables."""

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = Path(db_path) if db_path else DEFAULT_DB_PATH

    def _fetch(self, sql: str, params: tuple) -> sqlite3.Row:
        if not self._db_path.exists():
            raise AssetNotFoundError(f"asset database missing: {self._db_path}")
        connection = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(sql, params).fetchone()
        finally:
            connection.close()
        if row is None:
            raise AssetNotFoundError(f"asset not found: {params[0]!r}")
        return row

    def get_persona(self, ref_id: str) -> Dict[str, Any]:
        row = self._fetch(
            "SELECT persona_id, persona_name, status, prompt_core, markets, reference_images, "
            "feishu_record_id, sync_status, last_synced_at, source_payload "
            "FROM persona_templates WHERE persona_id=?",
            (ref_id,),
        )
        if row["status"] not in READABLE_STATUSES:
            raise AssetNotFoundError(f"persona {ref_id} is {row['status']!r}")
        references = json.loads(row["reference_images"] or "[]")
        local_paths = self.local_reference_paths(references)
        reference_assets = [
            {"local_path": path, "sha256": self._sha256(Path(path))}
            for path in local_paths
        ]
        snapshot = {
            "ref_id": row["persona_id"],
            "persona_id": row["persona_id"],
            "name": row["persona_name"],
            "status": row["status"],
            "prompt_core": row["prompt_core"],
            "markets": json.loads(row["markets"] or "[]"),
            "reference_images": references,
            "local_reference_images": local_paths,
            "reference_assets": reference_assets,
            "source": {
                "authority": "original_persona_template_library",
                "feishu_record_id": row["feishu_record_id"],
                "sync_status": row["sync_status"],
                "last_synced_at": row["last_synced_at"],
                "payload": json.loads(row["source_payload"] or "{}"),
            },
        }
        snapshot["structured_snapshot_hash"] = hashlib.sha256(
            json.dumps(
                {
                    "persona_id": snapshot["persona_id"],
                    "name": snapshot["name"],
                    "prompt_core": snapshot["prompt_core"],
                    "markets": snapshot["markets"],
                    "reference_asset_sha256s": [
                        item["sha256"] for item in reference_assets
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return snapshot

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def local_reference_paths(references: List[Any]) -> List[str]:
        paths: List[str] = []
        for entry in references:
            if isinstance(entry, str) and Path(entry).is_file():
                paths.append(str(Path(entry)))
            elif isinstance(entry, dict):
                candidate = str(entry.get("local_path") or "").strip()
                if candidate and Path(candidate).is_file():
                    paths.append(candidate)
        return paths

    def get_look(self, ref_id: str) -> Dict[str, Any]:
        row = self._fetch(
            "SELECT styling_id, styling_name, status, vibe_tag, prompt_core, "
            "outfit_recipe, silhouette_key, applicable_product_codes "
            "FROM styling_templates WHERE styling_id=?",
            (ref_id,),
        )
        if row["status"] not in READABLE_STATUSES:
            raise AssetNotFoundError(f"look {ref_id} is {row['status']!r}")
        return {
            "ref_id": row["styling_id"],
            "name": row["styling_name"],
            "vibe_tag": json.loads(row["vibe_tag"] or "[]"),
            "prompt_core": row["prompt_core"],
            "recipe": json.loads(row["outfit_recipe"] or "{}"),
            "silhouette_key": row["silhouette_key"],
            "applicable_product_codes": json.loads(
                row["applicable_product_codes"] or "[]"
            ),
        }

    def get_scene(self, ref_id: str) -> Dict[str, Any]:
        row = self._fetch(
            "SELECT scene_id, scene_name, status, prompt_core, prompt_negative, "
            "required_anchors, forbidden_elements "
            "FROM scene_templates WHERE scene_id=?",
            (ref_id,),
        )
        if row["status"] not in READABLE_STATUSES:
            raise AssetNotFoundError(f"scene {ref_id} is {row['status']!r}")
        return {
            "ref_id": row["scene_id"],
            "name": row["scene_name"],
            "prompt_core": row["prompt_core"],
            "prompt_negative": row["prompt_negative"],
            "required_anchors": json.loads(row["required_anchors"] or "[]"),
            "forbidden_elements": json.loads(row["forbidden_elements"] or "[]"),
        }
