#!/usr/bin/env python3
"""Locale Pack / Destination Catalog resolvers must reproduce the legacy tables.

Phase 2 moves every publish label out of the recipe into a Locale Pack while the
v1 recipe keeps its inline ``label_th`` / ``destination_labels_th`` /
``temperature_labels_th`` tables.  These tests pin the two paths together: the
pack must return exactly the strings the legacy tables return, and an incomplete
pack must fail loudly instead of silently serving another language.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config import loader  # noqa: E402
from domain.contracts import (  # noqa: E402
    ContractViolationError, validate_destination_catalog_payload,
    validate_locale_pack_payload,
)
from services import photo_locale  # noqa: E402

V2_RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / "PHOTO_TH_TRAVEL_OUTFIT_V2.json"
V2_POLICY_PATH = PACKAGE_ROOT / "config" / "photo_planning_policies" / "TH_TRAVEL_OUTFIT_V1.json"
LOCALE_PACK_PATH = PACKAGE_ROOT / "config" / "locales" / "LOCALE_TH_TH_V1.json"
DESTINATION_PATH = PACKAGE_ROOT / "config" / "destinations" / "EAST_ASIA_COOL_V1.json"


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class LegacyEquivalenceTest(unittest.TestCase):
    """``locale_pack=None`` must equal the v1 inline tables byte for byte."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.v2_contract = _read(V2_RECIPE_PATH)["recipe_spec"]["travel_contract"]
        cls.v2_policy = _read(V2_POLICY_PATH)
        cls.locale_pack = loader.load_locale_pack_file(LOCALE_PACK_PATH)

    def test_moment_labels_have_a_pack_for_every_v1_moment(self):
        legacy = photo_locale.travel_moment_labels(self.v2_contract)
        packed = photo_locale.travel_moment_labels(
            self.v2_contract, locale_pack=self.locale_pack
        )
        self.assertTrue(legacy)
        self.assertEqual(legacy, packed)

    def test_destination_labels_have_a_pack_for_every_v1_key(self):
        legacy = photo_locale.destination_labels(self.v2_contract)
        packed = photo_locale.destination_labels(
            self.v2_contract, locale_pack=self.locale_pack
        )
        self.assertTrue(legacy)
        self.assertEqual(legacy, packed)

    def test_temperature_labels_keep_every_v1_band(self):
        legacy = photo_locale.temperature_labels(self.v2_contract)
        packed = photo_locale.temperature_labels(
            self.v2_contract, locale_pack=self.locale_pack
        )
        self.assertTrue(legacy)
        # The pack may declare bands the v1 enum never used (e.g. minus_10_0c for
        # Seoul/Sapporo); every v1 band must still resolve to the same string.
        for key, value in legacy.items():
            self.assertEqual(packed.get(key), value, key)

    def test_family_copy_matches_the_v1_policy_for_every_family(self):
        families = list(self.v2_policy["families"])
        self.assertEqual(len(families), 8)
        for family in families:
            with self.subTest(family=family["family_id"]):
                legacy = photo_locale.family_copy(family)
                packed = photo_locale.family_copy(family, locale_pack=self.locale_pack)
                self.assertEqual(legacy["title"], packed["title"])
                self.assertEqual(legacy["cover"], packed["cover"])
                self.assertEqual(legacy["caption"], packed["caption"])
                expected_labels = {
                    look["role"]: look["display_label"] for look in family["looks"]
                }
                self.assertEqual(expected_labels, packed["look_labels"])

    def test_travel_copy_tokens_resolve_identically(self):
        variables = {"destination": "seoul", "temperature_band": "0_5c"}
        legacy = photo_locale.travel_copy_tokens(
            variables, travel_contract=self.v2_contract
        )
        packed = photo_locale.travel_copy_tokens(
            variables, travel_contract=self.v2_contract, locale_pack=self.locale_pack
        )
        self.assertEqual(legacy, packed)
        self.assertEqual(legacy, {"destination": "โซล", "temperature": "0-5°C"})

    def test_unknown_destination_or_band_resolves_to_empty(self):
        legacy = photo_locale.travel_copy_tokens(
            {"destination": "sapporo", "temperature_band": "minus_10_0c"},
            travel_contract=self.v2_contract,
        )
        self.assertEqual(legacy, {"destination": "", "temperature": ""})


class LocalePackContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.locale_pack = loader.load_locale_pack_file(LOCALE_PACK_PATH)

    def test_shipped_pack_declares_no_fallback(self):
        self.assertIs(self.locale_pack["fallback_allowed"], False)
        self.assertEqual(self.locale_pack["locale"], "th-TH")

    def test_missing_label_family_fails_loudly(self):
        broken = json.loads(json.dumps(self.locale_pack))
        del broken["labels"]["travel_moments"]
        with self.assertRaises(photo_locale.PhotoLocaleError):
            photo_locale.travel_moment_labels(None, locale_pack=broken)

    def test_missing_family_copy_fails_loudly(self):
        family = {"family_id": "not_in_pack", "looks": []}
        with self.assertRaises(photo_locale.PhotoLocaleError):
            photo_locale.family_copy(family, locale_pack=self.locale_pack)

    def test_fallback_true_is_rejected_by_the_contract(self):
        payload = json.loads(json.dumps(self.locale_pack))
        payload["fallback_allowed"] = True
        self.assertIn(
            "fallback_allowed must be false; locale packs may not fall back silently",
            validate_locale_pack_payload(payload),
        )

    def test_empty_label_value_is_rejected(self):
        payload = json.loads(json.dumps(self.locale_pack))
        payload["labels"]["generic"]["cta"] = ""
        errors = validate_locale_pack_payload(payload)
        self.assertTrue(any("generic.cta" in error for error in errors), errors)


class DestinationCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = loader.load_destination_catalog_file(DESTINATION_PATH)

    def test_catalog_covers_the_v1_scope(self):
        self.assertEqual(self.catalog["catalog_id"], "EAST_ASIA_COOL_V1")
        got = {entry["destination_id"] for entry in self.catalog["destinations"]}
        self.assertEqual(got, {
            "HANOI_WINTER", "SA_PA", "HA_GIANG", "DA_LAT",
            "SEOUL_WINTER", "GANGWON_PYEONGCHANG",
            "TOKYO", "OSAKA_KYOTO", "SAPPORO_HOKKAIDO",
            "BEIJING", "SHANGHAI", "HARBIN",
        })

    def test_catalog_stores_no_publish_language(self):
        for entry in self.catalog["destinations"]:
            for forbidden in ("label", "labels", "locale", "name_localized"):
                self.assertNotIn(forbidden, entry, entry["destination_id"])

    def test_publish_language_in_a_destination_is_rejected(self):
        payload = json.loads(json.dumps(self.catalog))
        payload["destinations"][0]["label"] = "Hà Nội"
        errors = validate_destination_catalog_payload(payload)
        self.assertTrue(any("publish language" in error for error in errors), errors)

    def test_cool_cities_never_default_to_snow(self):
        for destination_id in ("TOKYO", "SHANGHAI", "HANOI_WINTER", "SEOUL_WINTER"):
            with self.subTest(destination=destination_id):
                self.assertFalse(
                    photo_locale.snow_scene_allowed(self.catalog, destination_id)
                )
                # Even an explicit request is refused for FORBIDDEN cities.
                if destination_id in ("TOKYO", "SHANGHAI"):
                    self.assertFalse(
                        photo_locale.snow_scene_allowed(
                            self.catalog, destination_id, requested=True
                        )
                    )

    def test_explicit_snow_destinations_default_to_snow(self):
        for destination_id in ("SAPPORO_HOKKAIDO", "HARBIN", "GANGWON_PYEONGCHANG"):
            with self.subTest(destination=destination_id):
                self.assertTrue(
                    photo_locale.snow_scene_allowed(self.catalog, destination_id)
                )

    def test_snow_default_requires_a_snow_climate(self):
        payload = json.loads(json.dumps(self.catalog))
        entry = next(
            item for item in payload["destinations"] if item["destination_id"] == "TOKYO"
        )
        entry["snow_scene_policy"] = "DEFAULT"
        errors = validate_destination_catalog_payload(payload)
        self.assertTrue(any("snow climate_family" in error for error in errors), errors)

    def test_destinations_for_country_is_groups_in_catalog_order(self):
        self.assertEqual(
            photo_locale.destinations_for_country(self.catalog, "VN"),
            ("HANOI_WINTER", "SA_PA", "HA_GIANG", "DA_LAT"),
        )
        self.assertEqual(
            photo_locale.destinations_for_country(self.catalog, "KR"),
            ("SEOUL_WINTER", "GANGWON_PYEONGCHANG"),
        )
        self.assertEqual(photo_locale.destinations_for_country(self.catalog, "XX"), ())

    def test_unknown_destination_raises(self):
        with self.assertRaises(photo_locale.PhotoLocaleError):
            photo_locale.snow_scene_allowed(self.catalog, "NOWHERE")


class DestinationCatalogFailureTest(unittest.TestCase):
    def test_duplicate_destination_id_is_rejected(self):
        payload = {
            "schema_version": "opv-photo-destination-catalog-v1",
            "catalog_id": "C", "catalog_version": 1, "status": "draft",
            "default_snow_scene_policy": "OPTIONAL_NOT_DEFAULT",
            "destinations": [
                {"destination_id": "X", "destination_country": "KR",
                 "destination_city": "X", "climate_family": "cool_city",
                 "temperature_bands": ["0_5c"], "allowed_seasons": ["winter"],
                 "travel_moments": ["cafe_visit"]},
                {"destination_id": "X", "destination_country": "KR",
                 "destination_city": "X", "climate_family": "cool_city",
                 "temperature_bands": ["0_5c"], "allowed_seasons": ["winter"],
                 "travel_moments": ["cafe_visit"]},
            ],
        }
        errors = validate_destination_catalog_payload(payload)
        self.assertTrue(any("duplicate destination_id" in error for error in errors), errors)

    def test_shipped_catalog_is_listed_in_the_seed_bundle(self):
        bundle = loader.load_seed_bundle()
        self.assertEqual(
            [item["catalog_id"] for item in bundle.destination_catalogs],
            ["EAST_ASIA_COOL_V1"],
        )
        self.assertEqual(
            [item["locale_pack_id"] for item in bundle.locale_packs],
            ["LOCALE_TH_TH_V1"],
        )

    def test_invalid_catalog_fails_at_load_time(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "BAD_CATALOG.json"
            bad.write_text('{"schema_version": "opv-photo-destination-catalog-v1"}',
                           encoding="utf-8")
            with self.assertRaises(ContractViolationError):
                loader.load_destination_catalog_file(bad)


class LocaleModulePurityTest(unittest.TestCase):
    def test_locale_module_has_no_io_or_service_side_effects(self):
        source = (PACKAGE_ROOT / "services" / "photo_locale.py").read_text(encoding="utf-8")
        for forbidden in ("import os", "requests", "urllib", "sqlite", "open(",
                          "Path.home", "feishu", "from services."):
            self.assertNotIn(forbidden, source, forbidden)


if __name__ == "__main__":
    unittest.main()
