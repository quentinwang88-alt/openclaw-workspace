import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.product_selling_argument_adapter import load_verified_selling_point_catalog


class ProductSellingArgumentAdapterTest(unittest.TestCase):
    def test_only_verified_benefits_and_results_become_arguments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE product_claim_sources (
                        claim_source_id TEXT, source_type TEXT, source_ref TEXT
                    )"""
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT,
                        created_at TEXT
                    )"""
                )
                conn.executemany(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?)",
                    [
                        ("S1", "operator_input", "feishu:1"),
                        ("S2", "official_spec", "system:2"),
                        ("S3", "official_spec", "system:3"),
                        ("S4", "official_spec", "system:4"),
                    ],
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("P1", "VERIFIED", "B1", "S1", "C1", "4、空调房、降温时可以穿", "适合作为降温环境的外搭", "benefit", "function", "source_plus_video", "soft_only", "normal", "", "1"),
                        ("P1", "VERIFIED", "R1", "S2", "C2", "显腿长", "腿部线条视觉更修长", "visual_result", "fit", "source_plus_video", "moderate", "core", "", "2"),
                        ("P1", "VERIFIED", "F1", "S3", "C3", "口袋", "带有口袋结构", "feature", "function", "video_positive", "factual", "normal", "", "3"),
                        ("P1", "PROPOSED", "B2", "S4", "C4", "百搭", "不需要复杂搭配", "benefit", "style", "source_plus_video", "soft_only", "normal", "", "4"),
                    ],
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog("P1")
            self.assertEqual(result["status"], "AVAILABLE")
            self.assertEqual([item["value_id"] for item in result["catalog"]], ["CENTRAL_B1", "CENTRAL_R1"])
            benefit, visual_result = result["catalog"]
            self.assertEqual(benefit["primary_selling_point"], "空调房、降温时可以穿")
            self.assertEqual(benefit["canonical_selling_point"], "适合作为降温环境的外搭")
            self.assertEqual(benefit["source_type"], "operator_input")
            self.assertEqual(benefit["verification_status"], "VERIFIED")
            self.assertEqual(benefit["evidence_requirement"], "source_plus_video")
            self.assertEqual(benefit["visual_dependency"], "FLEXIBLE")
            self.assertEqual(benefit["compatible_carriers"], [])
            self.assertEqual(visual_result["visual_dependency"], "WEARER_REQUIRED")
            self.assertEqual(visual_result["compatible_carriers"], ["WEARER_ACTIVE", "MIXED"])
            self.assertEqual(result["evidence_claims"][0]["claim_id"], "F1")

    def test_operator_claim_wins_duplicate_concept(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE product_claim_sources (claim_source_id TEXT, source_type TEXT, source_ref TEXT)"
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT, created_at TEXT
                    )"""
                )
                conn.executemany(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?)",
                    [("OFF", "official_spec", "old"), ("OP", "operator_input", "feishu")],
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("P1", "VERIFIED", "OLD", "OFF", "C1", "百搭", "便于日常搭配", "benefit", "style", "source_plus_video", "factual", "normal", "", "1"),
                        ("P1", "VERIFIED", "NEW", "OP", "C1", "1、旅行带一件，上班和休闲都能穿", "便于日常搭配", "benefit", "style", "source_plus_video", "factual", "core", "", "2"),
                    ],
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog("P1")
            self.assertEqual(len(result["catalog"]), 1)
            self.assertEqual(result["catalog"][0]["value_id"], "CENTRAL_NEW")
            self.assertEqual(
                result["catalog"][0]["primary_selling_point"],
                "旅行带一件，上班和休闲都能穿",
            )


if __name__ == "__main__":
    unittest.main()
