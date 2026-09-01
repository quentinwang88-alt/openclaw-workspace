#!/usr/bin/env python3
"""Title scheme + copy composer tests (shipped TH scheme must stay valid)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services import copy_writer
from services.copy_writer import (
    CopySchemeError,
    build_hashtags,
    compose_proposals,
    load_scheme,
    propose_copy,
    theme_content_key,
)


def travel_plan() -> dict:
    return {
        "schema_version": "opv-plan-v1",
        "theme": {"id": "THEME_TH_TRAVEL_DEPARTURE_V1", "topic": "ลุคสนามบิน"},
        "shots": [],
    }


def cafe_plan() -> dict:
    return {
        "schema_version": "opv-plan-v1",
        "theme": {"id": "THEME_TH_CAFE_DATE_V1", "topic": "คาเฟ่"},
        "shots": [],
    }


class TitleSchemeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scheme = load_scheme()

    def test_shipped_scheme_passes_validation(self) -> None:
        self.assertEqual(copy_writer.validate_scheme(self.scheme), [])

    def test_theme_content_key_mapping(self) -> None:
        self.assertEqual(theme_content_key(travel_plan()), "travel_departure")
        self.assertEqual(theme_content_key(cafe_plan()), "cafe")
        self.assertEqual(theme_content_key({"theme": {"id": "THEME_X_V1"}}), "default")

    def test_proposals_respect_rules_and_theme(self) -> None:
        proposals = compose_proposals(travel_plan(), self.scheme, product_category="puffer_jacket")
        self.assertGreaterEqual(len(proposals), 3)
        rules = self.scheme["title_rules"]
        for proposal in proposals:
            self.assertLessEqual(len(proposal["title"]), rules["max_chars"])
            for banned in rules["banned_words"]:
                self.assertNotIn(banned.lower(), proposal["title"].lower())
        # travel theme ranks the travel hook among proposals
        formula_ids = [p["formula_id"] for p in proposals]
        self.assertIn("travel_hook", formula_ids)
        self.assertIn("ขึ้นเครื่อง", " ".join(p["title"] for p in proposals))

    def test_first_proposal_uses_top_weighted_formula(self) -> None:
        proposals = compose_proposals(travel_plan(), self.scheme)
        top = proposals[0]
        weights = {
            f["id"]: f["weight"]
            for f in self.scheme["hook_formulas"]
            if not f.get("themes") or "travel_departure" in f["themes"]
        }
        best = max(weights.items(), key=lambda kv: kv[1])[0]
        self.assertEqual(top["formula_id"], best)

    def test_hashtags_deduped_and_capped(self) -> None:
        tags = build_hashtags(travel_plan(), self.scheme)
        self.assertEqual(len(tags), len(set(tags)))
        count_min, count_max = self.scheme["hashtag_strategy"]["count"]
        self.assertGreaterEqual(len(tags), count_min)
        self.assertLessEqual(len(tags), count_max)
        self.assertIn("#ป้ายยา", tags)
        self.assertIn("#ลุคสนามบิน", tags)

    def test_propose_copy_is_flagged_for_confirmation(self) -> None:
        copy = propose_copy(travel_plan(), self.scheme, product_category="puffer_jacket")
        self.assertEqual(copy["status"], "proposed_needs_confirmation")
        self.assertEqual(copy["title"], copy["proposals"][0]["title"])
        self.assertNotIn("浅蓝", copy["caption"])  # Thai-facing copy stays Thai
        self.assertTrue(copy["cover_text"])

    def test_broken_scheme_fails_validation(self) -> None:
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "TITLE_BAD.json"
            bad.write_text(json.dumps({"schema_version": "wrong"}), encoding="utf-8")
            with self.assertRaises(CopySchemeError):
                load_scheme(bad)


if __name__ == "__main__":
    unittest.main()
