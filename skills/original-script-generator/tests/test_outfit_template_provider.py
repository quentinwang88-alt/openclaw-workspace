from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.complete_script_v3 import build_creative_diversity_contract
from core.outfit_template_provider import (
    get_outfit_template_provider_snapshot,
    load_structured_outfit_templates,
)


def _direction() -> dict:
    return {
        "direction_assignment_id": "SRA_OUTFIT_PROVIDER",
        "output_slot": "S1",
        "cluster_id": 8,
        "execution_reference": {"content_carrier": "WEARER_ACTIVE"},
        "structure_execution_plan": {"macro_family_key": "HOOK>PROOF"},
    }


class OutfitTemplateProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "outfit.sqlite3"
        conn = sqlite3.connect(self.path)
        conn.executescript(
            """
            CREATE TABLE styling_templates (
                styling_id TEXT PRIMARY KEY,
                styling_name TEXT NOT NULL,
                status TEXT NOT NULL,
                applicable_product_codes TEXT NOT NULL DEFAULT '[]',
                applicable_product_type TEXT NOT NULL DEFAULT '[]',
                product_fit TEXT NOT NULL DEFAULT '[]',
                bottom_type TEXT,
                bottom_color TEXT,
                bottom_fit TEXT,
                inner_type TEXT,
                inner_color TEXT,
                inner_requirements TEXT,
                accessory_level TEXT,
                footwear_visibility TEXT,
                vibe_tag TEXT,
                config_version TEXT,
                priority INTEGER NOT NULL DEFAULT 0,
                prompt_core TEXT,
                notes TEXT
            );
            """
        )
        rows = [
            (
                "STYLE_EXACT", "TITLE_MUST_NOT_LEAK", "enabled", json.dumps(["SKU_1"]),
                json.dumps(["outerwear"]), json.dumps(["短款"]), "midi_skirt",
                json.dumps(["黑色"]), json.dumps(["高腰"]), "纯色背心", "白色",
                "不遮挡外套门襟", "minimal", "optional", json.dumps(["日常干净"]),
                "STYLE_V9", 90, "PROMPT_MUST_NOT_LEAK", "NOTE_MUST_NOT_LEAK",
            ),
            (
                "STYLE_GENERIC", "GENERIC_TITLE", "enabled", json.dumps(["*"]),
                json.dumps(["outerwear"]), json.dumps(["不限"]), "straight_jeans",
                json.dumps(["蓝色牛仔"]), json.dumps(["直筒"]), "基础T恤", "白色",
                "", "none", "not_required", json.dumps(["日常干净"]),
                "STYLE_V2", 10, "GENERIC_PROMPT", "",
            ),
            (
                "STYLE_PRIVATE", "PRIVATE_TITLE", "enabled", json.dumps([]),
                json.dumps(["outerwear"]), json.dumps(["不限"]), "wide_leg_pants",
                json.dumps(["米色"]), json.dumps(["高腰"]), "", "", "", "none",
                "optional", json.dumps(["轻通勤"]), "STYLE_V1", 100, "PRIVATE_PROMPT", "",
            ),
        ]
        conn.executemany(
            "INSERT INTO styling_templates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
        conn.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_exact_and_wildcard_are_visible_but_blank_is_private(self) -> None:
        candidates = load_structured_outfit_templates(
            product_code="SKU_1", product_type="外套", db_path=self.path,
        )
        self.assertEqual(["STYLE_EXACT", "STYLE_GENERIC"], [item["template_id"] for item in candidates])
        exact = candidates[0]
        self.assertEqual("EXACT_PRODUCT_CODE", exact["match_scope"])
        serialized = json.dumps(exact, ensure_ascii=False)
        self.assertNotIn("TITLE_MUST_NOT_LEAK", serialized)
        self.assertNotIn("PROMPT_MUST_NOT_LEAK", serialized)
        self.assertIn("白色纯色背心", exact["base_outfit_direction"])
        self.assertIn("黑色", exact["base_outfit_direction"])
        self.assertIn("styling_name", exact["ignored_unstructured_fields"])

    def test_original_allocator_prefers_exact_template_then_keeps_soft_fallback(self) -> None:
        with patch.dict(os.environ, {"ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_DB_PATH": str(self.path)}):
            contract = build_creative_diversity_contract(
                product_code="SKU_1",
                country="泰国",
                category="女装",
                product_type="外套",
                direction=_direction(),
                recent_usage=[],
            )
        outfit = contract["outfit_selection_contract"]
        self.assertEqual("LIGHTWEIGHT_TEMPLATE", outfit["source_type"])
        self.assertEqual("STYLE_EXACT", outfit["template_id"])
        self.assertFalse(outfit["hard_required"])

    def test_missing_product_code_column_degrades_to_empty(self) -> None:
        broken = Path(self.tmp.name) / "broken.sqlite3"
        conn = sqlite3.connect(broken)
        conn.execute("CREATE TABLE styling_templates (styling_id TEXT, status TEXT)")
        conn.commit()
        conn.close()
        self.assertEqual(
            [],
            load_structured_outfit_templates(
                product_code="SKU_1", product_type="外套", db_path=broken,
            ),
        )

    def test_provider_snapshot_is_unknown_for_legacy_schema_without_sync_time(self) -> None:
        snapshot = get_outfit_template_provider_snapshot(db_path=self.path)
        self.assertEqual("UNKNOWN", snapshot["refresh_status"])
        self.assertEqual(3, snapshot["template_count"])
        self.assertEqual(3, snapshot["enabled_template_count"])
        self.assertEqual([], snapshot["soft_warnings"])

    def test_stale_snapshot_only_emits_a_soft_warning(self) -> None:
        stale = Path(self.tmp.name) / "stale.sqlite3"
        conn = sqlite3.connect(stale)
        conn.execute(
            "CREATE TABLE styling_templates (status TEXT, updated_at TEXT, last_synced_at TEXT)"
        )
        conn.execute(
            "INSERT INTO styling_templates VALUES (?,?,?)",
            ("enabled", "2000-01-01T00:00:00+08:00", "2000-01-01T00:00:00+08:00"),
        )
        conn.commit()
        conn.close()
        with patch.dict(os.environ, {"ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_STALE_HOURS": "1"}):
            snapshot = get_outfit_template_provider_snapshot(db_path=stale)
        self.assertEqual("STALE", snapshot["refresh_status"])
        self.assertTrue(snapshot["soft_warnings"])
