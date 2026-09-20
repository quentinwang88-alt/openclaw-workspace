"""F3: a count is read as a local assertion, not as a keyword hit.

The judge used to ask "does the word 双层 appear anywhere in the approved anchor
text?".  Two things break with that question:

* ``不是单层`` contains ``单层`` and was therefore admitted as a **single-layer**
  necklace.  That is the dangerous direction -- it lets a multi-layer product
  into the single-layer contract, which is the one thing the V1 profile promises
  not to do.
* ``int(raw)`` turns ``True`` into 1 (``bool`` *is* an ``int`` in Python) and
  truncates ``1.8`` into 1, so an illegal literal arrives at the judge as a
  perfectly plausible count.

Each count now carries local evidence -- ``value`` / ``status`` / ``source_ref``
/ ``source_text`` / ``polarity`` -- and the polarity is decided *locally*: the
marker has to sit immediately next to the term it applies to, so one 不是 at the
far end of a sentence cannot invalidate every count in the paragraph.

Every case in ``TheMinimumRegressionTable`` is a row of the plan's §4 table,
kept in the table's own words.
"""
import unittest

from core.accessory_mixed_templates import EVIDENCE_UNKNOWN, EVIDENCE_VERIFIED
from core.necklace_mixed_profile import (
    COUNT_POLARITY_AFFIRMED,
    COUNT_POLARITY_NEGATED,
    COUNT_POLARITY_UNCERTAIN,
    COUNT_SOURCE_ANCHOR_FIELD,
    COUNT_SOURCE_ANCHOR_TEXT,
    COUNT_SOURCE_UNSOURCED,
    COUNT_STATUS_CONFLICT,
    COUNT_STATUS_UNKNOWN,
    COUNT_STATUS_VERIFIED,
    NECKLACE_EVIDENCE_INCOMPLETE,
    NECKLACE_V1_ELIGIBLE,
    build_necklace_product_evidence,
    judge_necklace_eligibility,
    resolve_necklace_count_evidence,
    resolve_necklace_structure_counts,
)

#: Both parts confirmed.  Everything below is about the *counts*.
_CONFIRMED_PARTS = {
    "has_chain": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
    "has_pendant": {"state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"},
}

_EVIDENCE_KEYS = {
    "value",
    "status",
    "source_ref",
    "source_text",
    "polarity",
    "term",
    "candidates",
}


def evidence_for(anchor_card, *, hard_anchors=None):
    texts = hard_anchors if hard_anchors is not None else anchor_card.get("hard_anchors") or []
    return resolve_necklace_count_evidence(anchor_card=anchor_card, anchor_texts=texts)


def verdict_for(anchor_card, *, hard_anchors=None, parts=None):
    counts = evidence_for(anchor_card, hard_anchors=hard_anchors)
    return judge_necklace_eligibility(
        part_evidence=_CONFIRMED_PARTS if parts is None else parts, counts=counts
    )


def judged(counts, *, evidence_ref=""):
    return judge_necklace_eligibility(
        part_evidence=_CONFIRMED_PARTS, counts=counts, evidence_ref=evidence_ref
    )


class TheMinimumRegressionTable(unittest.TestCase):
    """The plan's §4 table, row by row."""

    def test_row_1_a_single_layer_chain_with_one_pendant_passes(self):
        verdict = verdict_for({"hard_anchors": ["单层链条", "只有一个吊坠"]})
        self.assertTrue(verdict["eligible"], verdict["reason"])
        self.assertEqual(1, verdict["layer_count"])
        self.assertEqual(1, verdict["pendant_count"])

    def test_row_2_an_unconfirmed_pendant_refuses_with_a_named_gap(self):
        verdict = verdict_for(
            {"hard_anchors": ["单层链条带吊坠", "不能认定单吊坠"]}
        )
        self.assertFalse(verdict["eligible"])
        self.assertEqual(1, verdict["layer_count"])
        self.assertIsNone(verdict["pendant_count"])
        self.assertIn("pendant_count:UNKNOWN", verdict["reason"])

    def test_row_3_a_denied_two_layers_then_a_stated_one_layer_passes(self):
        verdict = verdict_for(
            {"hard_anchors": ["不是双层", "是单层链条", "只有一个吊坠"]}
        )
        self.assertTrue(verdict["eligible"], verdict["reason"])
        self.assertEqual(1, verdict["layer_count"])
        self.assertEqual(1, verdict["pendant_count"])

    def test_row_4_a_denied_one_layer_then_a_stated_two_layers_refuses(self):
        verdict = verdict_for(
            {"hard_anchors": ["并非单层", "是双层链", "只有一个吊坠"]}
        )
        self.assertFalse(verdict["eligible"])
        self.assertEqual(2, verdict["layer_count"])
        self.assertIn("layer_count:2", verdict["reason"])

    def test_row_5_a_chain_and_a_pendant_with_no_count_refuses(self):
        verdict = verdict_for({"hard_anchors": ["有链条和吊坠", "数量看不清"]})
        self.assertFalse(verdict["eligible"])
        self.assertIsNone(verdict["layer_count"])
        self.assertIsNone(verdict["pendant_count"])
        self.assertIn("layer_count:UNKNOWN", verdict["reason"])
        self.assertIn("pendant_count:UNKNOWN", verdict["reason"])

    def test_row_6_a_field_that_says_one_against_an_anchor_that_says_two_refuses(self):
        verdict = verdict_for(
            {"hard_anchors": ["双层链", "单吊坠"], "layer_count": 1}
        )
        self.assertFalse(verdict["eligible"])
        self.assertIsNone(verdict["layer_count"])
        self.assertIn("layer_count:CONFLICT", verdict["reason"])
        self.assertEqual(
            COUNT_STATUS_CONFLICT,
            verdict["layer_count_evidence"]["status"],
        )

    def test_row_7_a_coiled_product_without_a_stated_count_refuses(self):
        # "盘成两圈" is a shape in the photo, not a layer count.  The term table
        # has no entry for it, so this must stay UNKNOWN rather than be read as
        # 双层 by resemblance.
        verdict = verdict_for({"hard_anchors": ["商品盘成两圈", "但未确认层数"]})
        self.assertFalse(verdict["eligible"])
        self.assertIn("layer_count:UNKNOWN", verdict["reason"])

    def test_row_8_illegal_literals_refuse_and_never_become_one(self):
        for raw in (True, False, 1.8, 0, -1, "-1", "1.8", "", "  ", None, [], {}):
            with self.subTest(raw=repr(raw)):
                evidence = evidence_for(
                    {"hard_anchors": ["链条", "吊坠"], "layer_count": raw, "pendant_count": raw}
                )
                for key in ("layer_count", "pendant_count"):
                    with self.subTest(key=key):
                        self.assertIsNone(evidence[key]["value"])
                        self.assertEqual(COUNT_STATUS_UNKNOWN, evidence[key]["status"])
                verdict = judge_necklace_eligibility(
                    part_evidence=_CONFIRMED_PARTS, counts=evidence
                )
                self.assertFalse(verdict["eligible"])
                self.assertIsNone(verdict["layer_count"])
                self.assertIsNone(verdict["pendant_count"])


class StructuredCountsMustBeStrictlyPositiveIntegers(unittest.TestCase):
    """§4.2 -- and the two ways a bare ``int()`` used to lie."""

    def test_a_boolean_is_not_the_count_one(self):
        # ``isinstance(True, int)`` is True, so this used to arrive as 1.
        evidence = evidence_for({"hard_anchors": [], "layer_count": True})
        self.assertIsNone(evidence["layer_count"]["value"])
        self.assertEqual(COUNT_STATUS_UNKNOWN, evidence["layer_count"]["status"])

    def test_a_decimal_is_not_truncated(self):
        # ``int(1.8)`` is 1.  A truncated count is a guessed count.
        evidence = evidence_for({"hard_anchors": [], "layer_count": 1.8})
        self.assertIsNone(evidence["layer_count"]["value"])
        self.assertEqual(COUNT_STATUS_UNKNOWN, evidence["layer_count"]["status"])

    def test_zero_and_negatives_are_not_counts(self):
        for raw in (0, -1, 0.0, -2.5, "0", "-3"):
            with self.subTest(raw=raw):
                evidence = evidence_for({"hard_anchors": [], "layer_count": raw})
                self.assertIsNone(evidence["layer_count"]["value"])

    def test_a_legal_integer_is_read_from_the_field_and_says_so(self):
        evidence = evidence_for({"hard_anchors": [], "layer_count": "2"})
        item = evidence["layer_count"]
        self.assertEqual(2, item["value"])
        self.assertEqual(COUNT_STATUS_VERIFIED, item["status"])
        self.assertEqual("anchor_card.layer_count", item["source_ref"])
        self.assertEqual(COUNT_POLARITY_AFFIRMED, item["polarity"])

    def test_an_integral_float_states_the_same_value_as_an_integer(self):
        # 1.0 is not a truncation of anything: it states one exactly.  Refusing
        # it would refuse a genuine single-layer product over its JSON literal.
        for raw in (1.0, 2.0):
            with self.subTest(raw=raw):
                evidence = evidence_for({"hard_anchors": [], "layer_count": raw})
                self.assertEqual(int(raw), evidence["layer_count"]["value"])

    def test_the_structured_field_also_accepts_the_structure_prefixed_name(self):
        evidence = evidence_for({"hard_anchors": [], "structure_layer_count": 1})
        self.assertEqual(1, evidence["layer_count"]["value"])
        self.assertEqual(
            "anchor_card.structure_layer_count", evidence["layer_count"]["source_ref"]
        )

    def test_an_illegal_field_does_not_block_a_legal_anchor_statement(self):
        # The bad field is dropped, not silently coerced, and the text is then
        # the only source -- which is the point: one bad literal must not make
        # the whole product unknowable, and it must not become a 1 either.
        evidence = evidence_for({"hard_anchors": ["单层链条带吊坠"], "layer_count": True})
        self.assertEqual(1, evidence["layer_count"]["value"])
        self.assertEqual("hard_anchors[0]", evidence["layer_count"]["source_ref"])
        verdict = judge_necklace_eligibility(
            part_evidence=_CONFIRMED_PARTS, counts=evidence
        )
        refs = {part["part"]: part for part in verdict["evidence_refs"]}
        self.assertEqual(COUNT_SOURCE_ANCHOR_TEXT, refs["layer_count"]["source"])


class TextIsReadAsLocalAssertions(unittest.TestCase):
    """§4.3 and §4.4 -- polarity is decided next to the term, not per paragraph."""

    def test_a_negated_two_layers_does_not_produce_two(self):
        item = evidence_for({"hard_anchors": ["不是双层"]})["layer_count"]
        self.assertIsNone(item["value"])
        self.assertEqual(COUNT_POLARITY_NEGATED, item["polarity"])
        self.assertEqual("双层", item["term"])

    def test_a_negated_one_layer_does_not_produce_one(self):
        item = evidence_for({"hard_anchors": ["并非单层"]})["layer_count"]
        self.assertIsNone(item["value"])
        self.assertEqual(COUNT_POLARITY_NEGATED, item["polarity"])

    def test_an_uncertain_pendant_does_not_produce_one(self):
        item = evidence_for({"hard_anchors": ["不能确认单吊坠"]})["pendant_count"]
        self.assertIsNone(item["value"])
        self.assertEqual(COUNT_POLARITY_UNCERTAIN, item["polarity"])
        self.assertEqual("单吊坠", item["term"])

    def test_an_uncertainty_after_the_term_is_read_too(self):
        item = evidence_for({"hard_anchors": ["单吊坠无法确认"]})["pendant_count"]
        self.assertIsNone(item["value"])
        self.assertEqual(COUNT_POLARITY_UNCERTAIN, item["polarity"])

    def test_a_distant_negation_does_not_invalidate_the_whole_sentence(self):
        # §4.4 verbatim: "不能把整段存在一个'不是'就全部否定".  The 不是 here is
        # about the colour, three clauses away from the layer statement.
        item = evidence_for({"hard_anchors": ["这条项链不是银色的，单层链条"]})["layer_count"]
        self.assertEqual(1, item["value"])
        self.assertEqual(COUNT_POLARITY_AFFIRMED, item["polarity"])

    def test_a_negation_inside_a_word_is_not_a_negation_of_the_term(self):
        # 非常规 must not read as 非 + 规… layered chain.
        item = evidence_for({"hard_anchors": ["非常规双层链"]})["layer_count"]
        self.assertEqual(2, item["value"])
        self.assertEqual(COUNT_POLARITY_AFFIRMED, item["polarity"])

    def test_a_hedged_and_a_negated_reading_are_distinguishable(self):
        self.assertEqual(
            COUNT_POLARITY_UNCERTAIN,
            evidence_for({"hard_anchors": ["不能确认双层"]})["layer_count"]["polarity"],
        )
        self.assertEqual(
            COUNT_POLARITY_NEGATED,
            evidence_for({"hard_anchors": ["不是双层"]})["layer_count"]["polarity"],
        )

    def test_two_affirmations_of_the_same_value_agree(self):
        item = evidence_for({"hard_anchors": ["单层链条", "单条链，单吊坠"]})["layer_count"]
        self.assertEqual(1, item["value"])
        self.assertEqual(COUNT_POLARITY_AFFIRMED, item["polarity"])


class AConflictIsKeptRatherThanRanked(unittest.TestCase):
    """§4.6 -- two sources that disagree must not be silently prioritised."""

    def test_a_field_against_an_affirmative_anchor_is_a_conflict(self):
        item = evidence_for({"hard_anchors": ["双层链"], "layer_count": 1})["layer_count"]
        self.assertIsNone(item["value"])
        self.assertEqual(COUNT_STATUS_CONFLICT, item["status"])
        self.assertEqual(
            {1, 2}, {candidate["value"] for candidate in item["candidates"]}
        )

    def test_the_conflict_names_both_sides_with_their_own_sources(self):
        item = evidence_for({"hard_anchors": ["双层链"], "layer_count": 1})["layer_count"]
        refs = {candidate["source_ref"]: candidate for candidate in item["candidates"]}
        self.assertIn("anchor_card.layer_count", refs)
        self.assertIn("hard_anchors[0]", refs)
        self.assertEqual("双层", refs["hard_anchors[0]"]["term"])
        self.assertIn("双层链", refs["hard_anchors[0]"]["source_text"])

    def test_an_agreeing_field_and_anchor_are_verified_not_conflicted(self):
        item = evidence_for({"hard_anchors": ["单层链条"], "layer_count": 1})["layer_count"]
        self.assertEqual(COUNT_STATUS_VERIFIED, item["status"])
        self.assertEqual(1, item["value"])

    def test_a_denied_anchor_does_not_conflict_with_the_field(self):
        # "不是双层" is not a competing claim of two, so the field stands.
        item = evidence_for({"hard_anchors": ["不是双层"], "layer_count": 1})["layer_count"]
        self.assertEqual(COUNT_STATUS_VERIFIED, item["status"])
        self.assertEqual(1, item["value"])

    def test_two_affirmative_anchors_that_disagree_are_a_conflict(self):
        item = evidence_for({"hard_anchors": ["单层链条", "双层链"]})["layer_count"]
        self.assertIsNone(item["value"])
        self.assertEqual(COUNT_STATUS_CONFLICT, item["status"])

    def test_a_conflict_refuses_and_is_not_reported_as_a_value(self):
        verdict = judged(
            evidence_for({"hard_anchors": ["双层链"], "layer_count": 1})
        )
        self.assertFalse(verdict["eligible"])
        self.assertIsNone(verdict["layer_count"])
        self.assertIn("layer_count:CONFLICT", verdict["unresolved"])
        self.assertNotIn("layer_count:1", verdict["unresolved"])
        self.assertNotIn("layer_count:2", verdict["unresolved"])


class AProvenanceIsNeverInvented(unittest.TestCase):
    """§4.8 -- the legacy integer stays readable, but without fake provenance."""

    def test_a_legacy_bare_integer_still_refuses_nothing(self):
        verdict = judged({"layer_count": 1, "pendant_count": 1})
        self.assertTrue(verdict["eligible"], verdict["reason"])

    def test_a_bare_integer_is_not_reported_as_structure_counts(self):
        verdict = judged({"layer_count": 1, "pendant_count": 1})
        refs = {part["part"]: part for part in verdict["evidence_refs"]}
        for part in ("layer_count", "pendant_count"):
            with self.subTest(part=part):
                self.assertEqual(COUNT_SOURCE_UNSOURCED, refs[part]["source"])
                self.assertNotIn("STRUCTURE_COUNTS", refs[part]["source"])

    def test_an_anchor_card_field_reports_its_own_provenance(self):
        verdict = judged(
            {"layer_count": 1, "pendant_count": 1, "layer_count_source": COUNT_SOURCE_ANCHOR_FIELD}
        )
        refs = {part["part"]: part for part in verdict["evidence_refs"]}
        self.assertEqual(COUNT_SOURCE_ANCHOR_FIELD, refs["layer_count"]["source"])
        self.assertEqual(COUNT_SOURCE_UNSOURCED, refs["pendant_count"]["source"])

    def test_an_anchor_statement_reports_the_anchor_as_its_provenance(self):
        verdict = judged(
            {"layer_count": 1, "pendant_count": 1, "layer_count_source": "ANCHOR_TEXT:单层"}
        )
        refs = {part["part"]: part for part in verdict["evidence_refs"]}
        self.assertEqual(COUNT_SOURCE_ANCHOR_TEXT, refs["layer_count"]["source"])

    def test_the_count_refs_carry_the_sentence_and_the_polarity(self):
        verdict = verdict_for({"hard_anchors": ["单层链条", "单吊坠"]})
        refs = {part["part"]: part for part in verdict["evidence_refs"]}
        self.assertEqual("单层", refs["layer_count"]["term"])
        self.assertEqual("单层链条", refs["layer_count"]["source_text"])
        self.assertEqual("hard_anchors[0]", refs["layer_count"]["source_ref"])
        self.assertEqual(COUNT_POLARITY_AFFIRMED, refs["layer_count"]["polarity"])
        self.assertEqual(COUNT_SOURCE_ANCHOR_TEXT, refs["layer_count"]["source"])

    def test_a_refusal_keeps_the_evidence_that_produced_it(self):
        verdict = verdict_for({"hard_anchors": ["单层链条带吊坠", "不能确认单吊坠"]})
        self.assertFalse(verdict["eligible"])
        self.assertTrue(verdict["reason"].startswith(NECKLACE_EVIDENCE_INCOMPLETE))
        item = verdict["pendant_count_evidence"]
        self.assertEqual(COUNT_STATUS_UNKNOWN, item["status"])
        self.assertEqual(COUNT_POLARITY_UNCERTAIN, item["polarity"])
        self.assertEqual("单吊坠", item["term"])
        self.assertEqual("不能确认单吊坠", item["source_text"])

    def test_a_pass_keeps_the_evidence_that_produced_it(self):
        verdict = verdict_for({"hard_anchors": ["单层链条", "单吊坠"]})
        self.assertEqual(NECKLACE_V1_ELIGIBLE, verdict["reason"])
        self.assertEqual(
            COUNT_STATUS_VERIFIED, verdict["layer_count_evidence"]["status"]
        )
        self.assertEqual(
            COUNT_STATUS_VERIFIED, verdict["pendant_count_evidence"]["status"]
        )


class OnlyApprovedAnchorsAreRead(unittest.TestCase):
    """§4.1 -- hard anchors and sourced structured fields, nothing else."""

    def test_display_anchors_never_produce_a_count(self):
        # A display anchor is a presentation idea the model wrote.  "单层链条
        # 展示" and "双吊坠造型" must not become a layer or pendant count.
        evidence = build_necklace_product_evidence(
            anchor_card={
                "hard_anchors": ["链条", "吊坠"],
                "display_anchors": ["单层链条展示", "双吊坠造型"],
            }
        )
        counts = evidence["counts"]
        self.assertNotIn("layer_count", counts)
        self.assertNotIn("pendant_count", counts)
        self.assertEqual(
            COUNT_STATUS_UNKNOWN, counts["layer_count_count_evidence"]["status"]
        )

    def test_display_anchors_cannot_override_a_hard_anchor(self):
        evidence = build_necklace_product_evidence(
            anchor_card={
                "hard_anchors": ["单层链条", "单吊坠"],
                "display_anchors": ["双层链条展示"],
            }
        )
        self.assertEqual(1, evidence["counts"]["layer_count"])
        self.assertEqual(1, evidence["counts"]["pendant_count"])
        verdict = judged(evidence["counts"])
        self.assertTrue(verdict["eligible"], verdict["reason"])

    def test_the_product_name_is_never_a_count_source(self):
        # "项链" proves neither a chain nor a pendant, let alone how many.
        evidence = build_necklace_product_evidence(
            anchor_card={"product_name": "单层单吊坠项链", "hard_anchors": []}
        )
        self.assertNotIn("layer_count", evidence["counts"])
        self.assertNotIn("pendant_count", evidence["counts"])


class ExistenceIsNotACount(unittest.TestCase):
    """§4.7 -- "has a pendant" does not say how many."""

    def test_a_confirmed_pendant_does_not_prove_one_pendant(self):
        evidence = build_necklace_product_evidence(
            anchor_card={"hard_anchors": ["有链条和吊坠"]}
        )
        parts = evidence["part_evidence"]
        self.assertEqual(EVIDENCE_VERIFIED, parts["has_chain"]["state"])
        self.assertEqual(EVIDENCE_VERIFIED, parts["has_pendant"]["state"])
        verdict = judged(evidence["counts"])
        self.assertFalse(verdict["eligible"])
        self.assertIn("pendant_count:UNKNOWN", verdict["reason"])

    def test_a_visible_chain_segment_is_not_a_layer_count(self):
        evidence = build_necklace_product_evidence(
            anchor_card={"hard_anchors": ["可见的链段", "吊坠"]}
        )
        self.assertNotIn("layer_count", evidence["counts"])

    def test_an_unconfirmed_part_refuses_even_with_both_counts(self):
        # The counts and the parts answer different questions; both must hold.
        verdict = judge_necklace_eligibility(
            part_evidence={"has_chain": {"state": EVIDENCE_VERIFIED}},
            counts=evidence_for({"hard_anchors": ["单层链条", "单吊坠"]}),
        )
        self.assertFalse(verdict["eligible"])
        self.assertIn("has_pendant:UNKNOWN", verdict["reason"])


class TheEvidenceShapeIsComplete(unittest.TestCase):
    """Every count carries the five documented keys, whatever its outcome."""

    def _all_evidence(self):
        cases = [
            {"hard_anchors": ["单层链条", "单吊坠"]},
            {"hard_anchors": ["不是双层"]},
            {"hard_anchors": ["双层链"], "layer_count": 1},
            {"hard_anchors": [], "layer_count": True},
            {"hard_anchors": []},
        ]
        for card in cases:
            yield card, evidence_for(card)

    def test_every_count_carries_every_key(self):
        for card, evidence in self._all_evidence():
            for key, item in evidence.items():
                with self.subTest(card=card, key=key):
                    self.assertEqual(_EVIDENCE_KEYS, set(item))

    def test_the_status_is_one_of_the_three_documented_values(self):
        for card, evidence in self._all_evidence():
            for key, item in evidence.items():
                with self.subTest(card=card, key=key):
                    self.assertIn(
                        item["status"],
                        {COUNT_STATUS_VERIFIED, COUNT_STATUS_UNKNOWN, COUNT_STATUS_CONFLICT},
                    )

    def test_the_polarity_is_one_of_the_three_documented_values(self):
        for card, evidence in self._all_evidence():
            for key, item in evidence.items():
                with self.subTest(card=card, key=key):
                    self.assertIn(
                        item["polarity"],
                        {
                            COUNT_POLARITY_AFFIRMED,
                            COUNT_POLARITY_NEGATED,
                            COUNT_POLARITY_UNCERTAIN,
                        },
                    )

    def test_a_verified_count_always_has_a_value_and_a_source(self):
        for card, evidence in self._all_evidence():
            for key, item in evidence.items():
                if item["status"] != COUNT_STATUS_VERIFIED:
                    continue
                with self.subTest(card=card, key=key):
                    self.assertIsNotNone(item["value"])
                    self.assertTrue(item["source_ref"])

    def test_candidates_are_only_populated_for_a_conflict(self):
        for card, evidence in self._all_evidence():
            for key, item in evidence.items():
                with self.subTest(card=card, key=key):
                    if item["status"] == COUNT_STATUS_CONFLICT:
                        self.assertTrue(item["candidates"])
                    else:
                        self.assertEqual([], item["candidates"])


class TheStructureCountsMappingStaysCompatible(unittest.TestCase):
    """The legacy integer shape survives beside the new evidence."""

    def test_a_verified_count_is_still_a_bare_integer(self):
        counts = resolve_necklace_structure_counts(
            anchor_card={"hard_anchors": ["单层链条", "单吊坠"]}
        )
        self.assertEqual(1, counts["layer_count"])
        self.assertEqual(1, counts["pendant_count"])
        self.assertEqual("ANCHOR_TEXT:单层", counts["layer_count_source"])
        self.assertIn("单吊坠", counts["pendant_count_source"])

    def test_an_unstated_count_is_absent_and_its_evidence_is_not(self):
        counts = resolve_necklace_structure_counts(anchor_card={"hard_anchors": ["链条", "吊坠"]})
        self.assertNotIn("layer_count", counts)
        self.assertNotIn("pendant_count", counts)
        self.assertEqual(
            COUNT_STATUS_UNKNOWN, counts["layer_count_count_evidence"]["status"]
        )

    def test_the_evidence_travels_beside_the_integer(self):
        counts = resolve_necklace_structure_counts(
            anchor_card={"hard_anchors": ["不是双层", "是单层链条", "只有一个吊坠"]}
        )
        self.assertEqual(1, counts["layer_count"])
        item = counts["layer_count_count_evidence"]
        self.assertEqual(COUNT_STATUS_VERIFIED, item["status"])
        self.assertEqual("单层", item["term"])

    def test_the_judge_reads_the_same_answer_from_either_shape(self):
        card = {"hard_anchors": ["不是双层", "是单层链条", "只有一个吊坠"]}
        from_evidence = judged(evidence_for(card))
        from_counts = judged(
            resolve_necklace_structure_counts(
                anchor_card=card, anchor_texts=card["hard_anchors"]
            )
        )
        self.assertEqual(from_evidence["eligible"], from_counts["eligible"])
        self.assertEqual(from_evidence["layer_count"], from_counts["layer_count"])
        self.assertEqual(from_evidence["pendant_count"], from_counts["pendant_count"])

    def test_a_field_and_a_hard_anchor_through_the_full_product_path(self):
        evidence = build_necklace_product_evidence(
            anchor_card={"hard_anchors": ["双层链", "单吊坠"], "layer_count": 1}
        )
        verdict = judged(evidence["counts"])
        self.assertFalse(verdict["eligible"])
        self.assertIn("layer_count:CONFLICT", verdict["reason"])
        self.assertEqual(1, evidence["counts"]["pendant_count"])


if __name__ == "__main__":
    unittest.main()
