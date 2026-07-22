from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load {}".format(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


enrich = load_module("enrich_wigs_voc_test", SKILL_DIR / "scripts" / "enrich_wigs_voc.py")
runner = load_module("run_voc_insight_test", SKILL_DIR / "scripts" / "run_voc_insight.py")


class WigsVocTest(unittest.TestCase):
    def test_product_form_precedence(self):
        self.assertEqual(
            enrich.classify_form("Extensiones de cabello sintético con 5 clips, aptas para crochet"),
            "clip_in_extension",
        )
        self.assertEqual(
            enrich.classify_form("Extensiones de cabello jumbo para trenzas 60cm"),
            "braiding_hair",
        )
        self.assertEqual(
            enrich.classify_form("Extensión de coleta ondulada de 30 pulgadas"),
            "ponytail_extension",
        )

    def test_invalid_and_risk_classification(self):
        taxonomy = enrich.load_json(SKILL_DIR / "references" / "wigs_voc_taxonomy_v1.json")
        base = {
            "batch_id": "B",
            "fastmoss_product_id": "P",
            "voc_rank": 1,
            "pool_type": "classic",
            "market": "MX",
            "category_key": "wigs",
            "product_title": "Peluca premium con flequillo",
        }
        invalid = enrich.enrich_row({**base, "voc_text": "Item: Por defecto"}, taxonomy)
        self.assertFalse(invalid["is_valid_voc"])
        self.assertEqual(invalid["invalid_reason"], "variant_only")

        risk = enrich.enrich_row(
            {**base, "voc_text": "Se enredó muy fácilmente y parece para una sola puesta"}, taxonomy
        )
        self.assertIn("tangle_shedding_issue", risk["signal_tags"])
        self.assertIn("quality_durability_issue", risk["signal_tags"])
        self.assertNotIn("tangle_resistance", risk["signal_tags"])

    def test_negated_shine_and_tangle_are_positive_not_risks(self):
        taxonomy = enrich.load_json(SKILL_DIR / "references" / "wigs_voc_taxonomy_v1.json")
        base = {
            "batch_id": "B",
            "fastmoss_product_id": "P",
            "voc_rank": 1,
            "pool_type": "classic",
            "market": "MX",
            "category_key": "wigs",
            "product_title": "Extensiones de cabello con clips",
        }
        row = enrich.enrich_row(
            {**base, "voc_text": "No son brillosas y no se enredan, se ven naturales"}, taxonomy
        )
        self.assertEqual(row["sentiment"], "positive")
        self.assertIn("natural_look", row["signal_tags"])
        self.assertIn("tangle_resistance", row["signal_tags"])
        self.assertNotIn("synthetic_shine_issue", row["signal_tags"])
        self.assertNotIn("tangle_shedding_issue", row["signal_tags"])

    def test_bangs_hairpiece_is_not_full_wig(self):
        self.assertEqual(
            enrich.classify_form("1 Pieza de 6 Pulgadas de Flequillo de Cabello Sintético"),
            "hairpiece_extension",
        )

    def test_category_thresholds_are_unambiguous(self):
        self.assertEqual(runner.category_confidence(3, 10, 29, 0.5, 0.2, False)[0], runner.FORM_OBSERVE_ONLY)
        self.assertEqual(runner.category_confidence(3, 10, 30, 0.5, 0.2, False)[0], runner.CATEGORY_CANDIDATE)
        self.assertEqual(runner.category_confidence(3, 10, 80, 0.5, 0.2, False)[0], runner.CATEGORY_ADS_CANDIDATE)
        self.assertEqual(runner.category_confidence(3, 10, 80, 0.7, 0.2, False)[0], runner.FORM_PARTIAL)

    def test_unknown_negative_signal_becomes_risk(self):
        meta = runner.signal_meta_for_tag(
            "unregistered_problem",
            {"sentiment_counts": {"negative": 3, "positive": 0}},
        )
        self.assertEqual(meta["insight_type"], "pain_point")
        self.assertEqual(meta["insight_role"], "risk_guard")

    def test_wig_risk_taxonomy_never_declares_selling_role(self):
        taxonomy = json.loads((SKILL_DIR / "references" / "wigs_voc_taxonomy_v1.json").read_text())
        for tag, meta in taxonomy["signals"].items():
            if tag.endswith("_issue"):
                self.assertEqual(meta["insight_role"], "risk_guard", tag)
                self.assertEqual(meta["insight_type"], "pain_point", tag)


if __name__ == "__main__":
    unittest.main()
