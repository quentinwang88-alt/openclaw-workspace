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

from services.styling_normalizer import normalize_product_id, normalize_styling_recipe, outfit_fingerprint, outfit_visual_features

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
        # Typed persona-pack items keep the full reference entry (role/approved)
        # so human-scene readiness can verify face/full-body evidence.
        reference_items = []
        for entry in references:
            if isinstance(entry, str):
                entry = {"local_path": entry}
            if not isinstance(entry, dict):
                continue
            path = str(entry.get("local_path") or "").strip()
            if not path or not Path(path).is_file():
                continue
            item = dict(entry)
            item.setdefault("sha256", self._sha256(Path(path)))
            reference_items.append(item)
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
            "reference_items": reference_items,
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
            "outfit_recipe, silhouette_key, applicable_product_codes, "
            "applicable_product_type, product_fit, supported_demonstration_modes, "
            "scene_families, style_intensity, climate_profile, priority, "
            "inner_type, inner_color, bottom_type, bottom_color, bottom_fit, accessory_level, "
            "footwear_visibility, base_outfit_direction, target_role, "
            "preferred_persona_ids, feishu_record_id, sync_status, last_synced_at "
            "FROM styling_templates WHERE styling_id=?",
            (ref_id,),
        )
        if row["status"] not in READABLE_STATUSES:
            raise AssetNotFoundError(f"look {ref_id} is {row['status']!r}")
        explicit_recipe = self._json_object(row["outfit_recipe"])
        recipe, field_sources = normalize_styling_recipe(dict(row), explicit_recipe)
        raw_codes = self._json_list(row["applicable_product_codes"])
        codes = list(dict.fromkeys(normalize_product_id(v) for v in raw_codes if normalize_product_id(v)))
        item_refs = dict(recipe.get("item_refs") or {})
        if item_refs:
            recipe = {key: value for key, value in recipe.items() if key != "item_refs"}
        return {
            "ref_id": row["styling_id"],
            "name": row["styling_name"],
            "status": row["status"],
            "vibe_tag": self._json_list(row["vibe_tag"]),
            "prompt_core": row["prompt_core"],
            "recipe": recipe,
            "item_refs": item_refs,
            "recipe_source": "outfit_recipe" if explicit_recipe else "normalized_columns",
            "recipe_field_sources": field_sources,
            "recipe_normalization_version": 2,
            "outfit_fingerprint": outfit_fingerprint(recipe),
            "visual_features": outfit_visual_features(recipe),
            "silhouette_key": row["silhouette_key"],
            "applicable_product_codes": codes,
            "raw_applicable_product_codes": raw_codes,
            "compatibility": {
                "raw_product_codes": raw_codes,
                "normalized_product_codes": codes,
                "product_types": self._json_list(row["applicable_product_type"]),
                "product_fits": self._json_list(row["product_fit"]),
                "demonstration_modes": self._json_list(row["supported_demonstration_modes"]),
                "scene_families": self._json_list(row["scene_families"]),
                "style_intensity": str(row["style_intensity"] or ""),
                "climate_profile": str(row["climate_profile"] or ""),
                "target_role": str(row["target_role"] or ""),
                "preferred_persona_ids": self._json_list(row["preferred_persona_ids"]),
            },
            "priority": int(row["priority"] or 0),
            "source": {
                "feishu_record_id": str(row["feishu_record_id"] or ""),
                "sync_status": str(row["sync_status"] or ""),
                "last_synced_at": str(row["last_synced_at"] or ""),
                "normalization_notes": ["product_code_formatting_artifacts_removed"] if codes != raw_codes else [],
            },
        }

    def list_look_ids(self, statuses=None) -> List[str]:
        """Discover readable library rows without mutating the underlying library."""
        statuses = tuple(READABLE_STATUSES if statuses is None else statuses)
        if not statuses:
            return []
        if any(status not in READABLE_STATUSES for status in statuses):
            raise ValueError("only enabled/testing styling rows are readable")
        if not self._db_path.exists():
            raise AssetNotFoundError(f"asset database missing: {self._db_path}")
        with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as connection:
            rows = connection.execute(
                "SELECT styling_id FROM styling_templates WHERE status IN ("
                + ",".join("?" for _ in statuses) + ") ORDER BY styling_id", statuses,
            ).fetchall()
        return [str(row[0]) for row in rows]

    def list_looks(self, statuses=None) -> List[Dict[str, Any]]:
        return [self.get_look(ref) for ref in self.list_look_ids(statuses)]

    @staticmethod
    def _json_list(value: Any) -> List[Any]:
        try:
            parsed = json.loads(value or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        return parsed if isinstance(parsed, list) else []

    @staticmethod
    def _json_object(value: Any) -> Dict[str, Any]:
        try:
            parsed = json.loads(value or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @classmethod
    def _normalized_look_recipe(cls, row: sqlite3.Row) -> Dict[str, Any]:
        """Make legacy Feishu styling rows executable by the OPV planner."""
        return normalize_styling_recipe(dict(row))[0]

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
            "status": row["status"],
            "prompt_core": row["prompt_core"],
            "prompt_negative": row["prompt_negative"],
            "required_anchors": json.loads(row["required_anchors"] or "[]"),
            "forbidden_elements": json.loads(row["forbidden_elements"] or "[]"),
        }
