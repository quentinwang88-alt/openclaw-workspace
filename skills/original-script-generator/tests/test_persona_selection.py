import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from core.persona_selection import (
    build_outfit_persona_affinity_contract,
    select_persona_contract,
)
from core.persona_template_provider import load_persona_templates
from core.simplified_complete_script import normalize_simplified_visual_script


class PersonaSelectionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "persona.sqlite3"
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """CREATE TABLE persona_templates (
                persona_id TEXT PRIMARY KEY, persona_name TEXT, status TEXT,
                gender TEXT, age_group TEXT, body_type TEXT, hair_style TEXT,
                hair_color TEXT, skin_tone TEXT, face_visibility TEXT,
                makeup_style TEXT, vibe TEXT, prompt_core TEXT,
                prompt_negative TEXT, priority INTEGER, markets TEXT,
                reference_images TEXT, config_version TEXT,
                consistency_version TEXT, source_hash TEXT,
                source_payload TEXT, updated_at TEXT
            )"""
        )
        self.conn = conn

    def tearDown(self):
        self.conn.close()
        self.temp.cleanup()

    def _insert(
        self, persona_id, *, references, product_types, priority=0,
        categories=None, modes=None, presentations=None, captures=None,
        status="enabled",
    ):
        self.conn.execute(
            "INSERT INTO persona_templates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                persona_id, persona_id, status, "female", "22-28",
                "natural_slim", "long_hair", "black", "natural",
                "visible", "light_social", json.dumps(["young_fashion"]),
                "真实泰国年轻创作者", "不要塑料皮肤", priority,
                json.dumps(["TH"]), json.dumps(references), "V1", "PERSONA_V1",
                "", json.dumps({
                    "applicable_categories": categories or ["配饰"],
                    "applicable_product_types": product_types,
                    "supported_demonstration_modes": modes or ["HEAD_WORN", "NECK_WORN"],
                    "supported_presentation_modes": presentations or [],
                    "supported_capture_modes": captures or [],
                    "identity_text": "二十多岁的泰国年轻创作者",
                    "appearance_text": "自然身材和未精修肤质",
                    "body_proportion_text": "上身4下身6，头身比约1:7.2",
                    "hair_makeup_text": "黑色长发和轻社交妆",
                    "speaking_personality": "自然直接的朋友式分享",
                }), "2026-08-09",
            ),
        )
        self.conn.commit()

    def test_prompt_only_persona_is_not_approved(self):
        self._insert("P0", references=[], product_types=["headscarf"])
        snapshot = load_persona_templates(db_path=self.db_path)
        self.assertEqual(snapshot["status"], "UNAVAILABLE")
        self.assertIn("P0", " ".join(snapshot["soft_warnings"]))

    def test_configured_persona_with_reference_needs_no_enable_switch(self):
        self._insert(
            "P_CONFIGURED",
            references=[{"file_token": "persona_ref_configured"}],
            product_types=["headscarf"],
            status="testing",
        )
        contract, _, _ = select_persona_contract(
            product_type="头巾", top_category="配饰", country="泰国",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="HEAD_WORN", seed=7, recent_usage=[],
            db_path=str(self.db_path),
        )
        self.assertEqual("AVAILABLE", contract["availability"])
        self.assertEqual("P_CONFIGURED", contract["persona_id"])

    def test_headscarf_selects_approved_persona_and_requires_composite(self):
        self._insert(
            "P1", references=[{"file_token": "persona_ref_1"}],
            product_types=["headscarf"],
        )
        contract, recent, batch = select_persona_contract(
            product_type="头巾", top_category="配饰", country="泰国",
            presentation_mode="PERSON_ON_CAMERA", capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="HEAD_WORN", seed=7, recent_usage=[],
            db_path=str(self.db_path),
        )
        self.assertEqual(contract["availability"], "AVAILABLE")
        self.assertEqual(contract["persona_id"], "P1")
        self.assertEqual(
            contract["reference_strategy"], "PERSONA_PRODUCT_COMPOSITE_REQUIRED"
        )
        self.assertIn("PRODUCT_REFERENCE_PERSON", contract["non_authorities"])
        self.assertEqual(recent, 0)
        self.assertEqual(batch, 0)

    def test_womens_outerwear_selects_approved_apparel_persona(self):
        self._insert(
            "P_APPAREL",
            references=[{"file_token": "apparel_persona_ref"}],
            product_types=["outerwear", "light_top"],
            categories=["女装"],
            modes=["GARMENT_WORN"],
            presentations=["PERSON_ON_CAMERA"],
            captures=["CREATOR_SELF_SHOT"],
        )
        contract, _, _ = select_persona_contract(
            product_type="外套", top_category="女装", country="泰国",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="GARMENT_WORN", seed=9, recent_usage=[],
            db_path=str(self.db_path),
        )
        self.assertEqual("AVAILABLE", contract["availability"])
        self.assertEqual("P_APPAREL", contract["persona_id"])
        self.assertEqual(
            "PERSONA_PRODUCT_COMPOSITE_PREFERRED",
            contract["reference_strategy"],
        )
        self.assertNotIn("上身4下身6", contract["script_projection"]["appearance"])
        self.assertEqual(
            "上身4下身6，头身比约1:7.2",
            contract["identity_lock"]["body_proportion_text"],
        )

    def test_persona_presentation_scope_is_respected(self):
        self._insert(
            "P_HAND_ONLY",
            references=[{"file_token": "hand_ref"}],
            product_types=["outerwear"], categories=["女装"],
            modes=["GARMENT_WORN"], presentations=["MIXED"],
        )
        contract, _, _ = select_persona_contract(
            product_type="外套", top_category="女装", country="泰国",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="GARMENT_WORN", seed=9, recent_usage=[],
            db_path=str(self.db_path),
        )
        self.assertEqual("UNAVAILABLE_COMPATIBLE_TEMPLATE", contract["availability"])

    def test_same_batch_rotates_persona(self):
        self._insert("P1", references=["/tmp/p1.png"], product_types=["headscarf"])
        self._insert("P2", references=["/tmp/p2.png"], product_types=["headscarf"])
        first, _, _ = select_persona_contract(
            product_type="头巾", top_category="配饰", country="TH",
            presentation_mode="PERSON_ON_CAMERA", capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="HEAD_WORN", seed=4, recent_usage=[],
            db_path=str(self.db_path),
        )
        second, _, batch = select_persona_contract(
            product_type="头巾", top_category="配饰", country="TH",
            presentation_mode="PERSON_ON_CAMERA", capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="HEAD_WORN", seed=4,
            recent_usage=[{
                "_batch_reserved": True,
                "metadata": {"persona_selection_contract": first},
            }],
            db_path=str(self.db_path),
        )
        self.assertNotEqual(first["persona_id"], second["persona_id"])
        self.assertEqual(batch, 0)

    def test_outfit_preferred_persona_wins_when_compatible(self):
        self._insert(
            "P_HIGH", references=["/tmp/high.png"],
            product_types=["headscarf"], priority=100,
        )
        self._insert(
            "P_OUTFIT", references=["/tmp/outfit.png"],
            product_types=["headscarf"], priority=1,
        )
        contract, _, _ = select_persona_contract(
            product_type="头巾", top_category="配饰", country="TH",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="HEAD_WORN", seed=4, recent_usage=[],
            preferred_persona_ids=["P_OUTFIT"],
            db_path=str(self.db_path),
        )
        self.assertEqual("P_OUTFIT", contract["persona_id"])
        self.assertEqual("MATCHED", contract["preference_match_status"])

        affinity = build_outfit_persona_affinity_contract(
            {
                "template_id": "STYLE_1",
                "template_version": "V2",
                "source_type": "LIGHTWEIGHT_TEMPLATE",
                "preferred_persona_ids": ["P_OUTFIT"],
            },
            contract,
        )
        self.assertEqual("MATCHED", affinity["match_status"])
        self.assertEqual("P_OUTFIT", affinity["selected_persona_id"])
        self.assertFalse(affinity["hard_required"])

    def test_incompatible_outfit_preference_falls_back_without_blocking(self):
        self._insert(
            "P_COMPATIBLE", references=["/tmp/compatible.png"],
            product_types=["headscarf"],
        )
        self._insert(
            "P_INCOMPATIBLE", references=["/tmp/incompatible.png"],
            product_types=["outerwear"], categories=["女装"],
            modes=["GARMENT_WORN"],
        )
        contract, _, _ = select_persona_contract(
            product_type="头巾", top_category="配饰", country="TH",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            demonstration_mode="HEAD_WORN", seed=4, recent_usage=[],
            preferred_persona_ids=["P_INCOMPATIBLE"],
            db_path=str(self.db_path),
        )
        self.assertEqual("P_COMPATIBLE", contract["persona_id"])
        self.assertEqual("FALLBACK", contract["preference_match_status"])

    def test_static_direction_does_not_allocate_persona(self):
        self._insert("P1", references=["/tmp/p1.png"], product_types=["headscarf"])
        contract, _, _ = select_persona_contract(
            product_type="头巾", top_category="配饰", country="TH",
            presentation_mode="STATIC_PRODUCT", capture_mode="STATIC_PRODUCT_RECORD",
            demonstration_mode="HEAD_WORN", seed=4, recent_usage=[],
            db_path=str(self.db_path),
        )
        self.assertEqual(contract["availability"], "NOT_APPLICABLE")

    def test_visual_normalization_projects_frozen_persona(self):
        contract = {
            "availability": "AVAILABLE",
            "persona_id": "P_LOCKED",
            "script_projection": {
                "identity": "冻结身份",
                "appearance": "冻结外貌",
                "hair_makeup": "冻结妆发",
                "speaking_personality": "冻结说话人格",
            },
        }
        normalized = normalize_simplified_visual_script(
            {
                "production_design": {
                    "presentation_mode": "PERSON_ON_CAMERA",
                    "character": {
                        "identity": "模型自由身份",
                        "appearance": "模型自由外貌",
                    },
                }
            },
            {
                "creative_seed_id": "S1",
                "creative_direction": {"capture_mode": "CREATOR_SELF_SHOT"},
                "diversity_context": {"persona_selection_contract": contract},
            },
            generation_provenance={"model": "test"},
        )
        character = normalized["production_design"]["character"]
        self.assertEqual(character["persona_id"], "P_LOCKED")
        self.assertEqual(character["identity"], "冻结身份")
        self.assertEqual(character["appearance"], "冻结外貌")


if __name__ == "__main__":
    unittest.main()
