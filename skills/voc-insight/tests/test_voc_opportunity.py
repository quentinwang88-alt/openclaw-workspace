from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.atomic_evidence import build_atomic_evidence
from core.opportunity_aggregation import confidence_for_metrics, metrics_for_rows
from core.opportunity_synthesis import FORBIDDEN_FIELDS, build_opportunity_cards, validate_cards


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load {}".format(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


enrich = load_module("enrich_wigs_voc_opportunity_test", SKILL_DIR / "scripts" / "enrich_wigs_voc.py")
report = load_module("build_voc_opportunity_report_test", SKILL_DIR / "scripts" / "build_voc_opportunity_report.py")
aspect_taxonomy = json.loads((SKILL_DIR / "references" / "voc_aspect_taxonomy_v1.json").read_text())
adapters = json.loads((SKILL_DIR / "references" / "category_opportunity_adapters.json").read_text())
wigs_taxonomy = json.loads((SKILL_DIR / "references" / "wigs_voc_taxonomy_v1.json").read_text())


def enriched(text: str, product_id: str = "P1", rank: int = 1, title: str = "Extensiones de cabello con clips"):
    return enrich.enrich_row({
        "batch_id": "B1",
        "fastmoss_product_id": product_id,
        "voc_rank": rank,
        "pool_type": "classic",
        "market": "MX",
        "category_key": "wigs",
        "product_title": title,
        "voc_text": text,
    }, wigs_taxonomy)


def atomic_row(index: int, product: str, tag: str, polarity: str = "positive",
               sample_pool: str = "natural_distribution", scope: str = "direct"):
    return {
        "atomic_evidence_id": "A{}".format(index),
        "evidence_id": "E{}".format(index),
        "batch_id": "B1",
        "source_batch_id": "B{}".format(1 + (index % 2)),
        "market": "MX",
        "category_key": "wigs",
        "product_id": product,
        "product_form": "clip_in_extension",
        "product_form_label": "卡扣接发",
        "pool_type": "classic",
        "sample_pool": sample_pool,
        "evidence_scope": scope,
        "aspect_group": "appearance",
        "signal_tag": tag,
        "polarity": polarity,
        "source_text": "evidence {}".format(index),
    }


class VocOpportunityTest(unittest.TestCase):
    def test_mixed_review_becomes_multiple_atomic_evidence(self):
        row = enriched(
            "Coleta color claro más claro de lo que se muestra en foto, "
            "se ve bonita y tiene jareta para tener mejor agarre",
            title="Extensión de coleta ondulada",
        )
        atomic = build_atomic_evidence([row], aspect_taxonomy)
        tags = {item["signal_tag"] for item in atomic}
        self.assertIn("color_match_issue", tags)
        self.assertIn("appearance_style", tags)
        self.assertIn("hold_stability", tags)
        self.assertEqual(len({item["atomic_evidence_id"] for item in atomic}), len(atomic))
        self.assertTrue(all(item["source_text"] in row["voc_text"] for item in atomic))

    def test_negated_shine_never_becomes_shine_risk(self):
        row = enriched("No son brillosas, casi no brilla y se ven muy naturales")
        atomic = build_atomic_evidence([row], aspect_taxonomy)
        tags = {item["signal_tag"] for item in atomic}
        self.assertIn("natural_look", tags)
        self.assertNotIn("synthetic_shine_issue", tags)

    def test_negated_recommendation_is_not_positive_repeat_purchase(self):
        row = enriched("No las recomiendo, llegaron muy pocas mechas")
        atomic = build_atomic_evidence([row], aspect_taxonomy)
        self.assertNotIn("repeat_purchase", {item["signal_tag"] for item in atomic})
        self.assertEqual("negative", row["sentiment"])

    def test_non_core_wig_products_are_invalid(self):
        for title in (
            "Hair Styling Waxes Creme Stick for Flyaway Hair and Smooth Wig",
            "Kit de Tinsel para Cabello con Brillos Coloridos",
            "Gorra con peluca de moda hip hop, gorro de calavera",
        ):
            row = enriched("Excelente producto, me encantó", title=title)
            self.assertFalse(row["is_valid_voc"], title)
            self.assertTrue(row["invalid_reason"].startswith("non_core_"))

    def test_bangs_claw_clip_is_hairpiece_not_full_wig(self):
        row = enriched(
            "Se ve natural y es fácil de poner",
            title="Women's Cartoon Bangs Realistic Medium Length Wig, Secure Claw Clip Attachment",
        )
        self.assertEqual("hairpiece_extension", row["product_form"])

    def test_single_product_volume_cannot_raise_direction(self):
        rows = [atomic_row(i, "P1", "natural_look") for i in range(1, 25)]
        metrics = metrics_for_rows(rows)
        self.assertEqual(metrics["direct_product_count"], 1)
        self.assertEqual(confidence_for_metrics(metrics), "signal_only")

    def test_diagnostic_only_is_capped_at_emerging(self):
        rows = [
            atomic_row(i, "P{}".format(1 + i % 5), "synthetic_shine_issue", "negative", "diagnostic_risk")
            for i in range(1, 22)
        ]
        metrics = metrics_for_rows(rows)
        self.assertEqual(confidence_for_metrics(metrics), "emerging_opportunity")

    def test_proxy_evidence_is_not_direct_product_coverage(self):
        rows = [atomic_row(1, "P1", "natural_look", scope="direct")]
        rows.extend(atomic_row(i, "PX{}".format(i), "natural_look", scope="category_proxy") for i in range(2, 8))
        metrics = metrics_for_rows(rows)
        self.assertEqual(metrics["product_count"], 7)
        self.assertEqual(metrics["direct_product_count"], 1)
        self.assertEqual(metrics["proxy_evidence_count"], 6)

    def test_cards_are_traceable_and_forbidden_fields_absent(self):
        rows = []
        for i in range(1, 7):
            rows.append(atomic_row(i, "P{}".format(i), "natural_look"))
        rows.extend([
            atomic_row(7, "P7", "synthetic_shine_issue", "negative"),
            atomic_row(8, "P8", "synthetic_shine_issue", "negative"),
            atomic_row(9, "P9", "synthetic_shine_issue", "negative"),
            atomic_row(10, "P10", "synthetic_shine_issue", "negative"),
        ])
        cards = build_opportunity_cards(rows, adapters, "RUN", "MX", "wigs", "2026-07-18T00:00:00Z")
        self.assertTrue(cards)
        validation = validate_cards(cards, rows)
        self.assertTrue(validation["passed"], validation)
        for card in cards:
            self.assertFalse(FORBIDDEN_FIELDS & set(card))
            self.assertTrue(card["evidence_refs"])

        payload = {
            "batch_id": "B1", "source_batches": ["B1"], "market": "MX", "category_key": "wigs",
            "summary": {"valid_review_count": 10, "atomic_evidence_count": len(rows), "evidence_product_count": 10},
            "quality_gate": {"passed": True}, "opportunity_cards": cards,
            "atomic_evidence": rows, "spec_risk_library": [],
        }
        markdown = report.build_markdown(payload)
        for forbidden_text in ("推荐测品", "建议淘汰", "待找货"):
            self.assertNotIn(forbidden_text, markdown)

        poisoned = dict(cards[0])
        poisoned["metrics"] = dict(poisoned["metrics"], product_potential_score=99)
        self.assertFalse(validate_cards([poisoned], rows)["passed"])


if __name__ == "__main__":
    unittest.main()
