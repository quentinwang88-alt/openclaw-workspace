#!/usr/bin/env python3
"""Phase 4: the VN scarf activation config and its offline canaries.

Phase 4 adds *configuration*, not new machinery: a VN Market Pack, a Vietnamese
Locale/Copy Pack, destination weights, a persona/account binding, two disabled
VN presets and three offline canary manifests.  These tests pin the properties
that make the activation safe:

* the VN config loads and declares no silent fallback (vi-VN or nothing);
* no Thai text leaks into any Vietnamese artefact;
* binding the country-agnostic V3 to VN changes **only** market and locale;
* VN stays unreachable from production: presets disabled, no publish route,
  account paused, copy DRAFT, market pack draft;
* the three canary manifests are reproducible from the shipped configs.
"""
from __future__ import annotations

import copy
import json
import re
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from config import loader  # noqa: E402
from domain.contracts import validate_locale_pack_payload  # noqa: E402
from services import photo_locale  # noqa: E402
from services.photo_content_planner import (  # noqa: E402
    get_planning_flow, plan_th_choice_batch, recipe_has_planning_policy,
)
from services.photo_execution_context import build_execution_context  # noqa: E402
from services.photo_locale import PhotoLocaleError  # noqa: E402

import photo_vn_canary_fixture as canary_fixture  # noqa: E402

CONFIG_DIR = PACKAGE_ROOT / "config"
MARKET_PACK_PATH = CONFIG_DIR / "market_packs" / "MP_VN_DEFAULT_v1.json"
LOCALE_PACK_PATH = CONFIG_DIR / "locales" / "LOCALE_VI_VN_V1.json"
TH_LOCALE_PACK_PATH = CONFIG_DIR / "locales" / "LOCALE_TH_TH_V1.json"
DESTINATION_PATH = CONFIG_DIR / "destinations" / "EAST_ASIA_COOL_V1.json"
ACCOUNT_PATH = CONFIG_DIR / "accounts" / "OPV_VN_TEST_001.json"
PRESETS_PATH = CONFIG_DIR / "feishu_production_presets.json"
ROUTES_PATH = CONFIG_DIR / "main_publish_routes.json"
TRAVEL_RECIPE_PATH = CONFIG_DIR / "recipes" / "PHOTO_TRAVEL_OUTFIT_V3.json"
MATCHING_RECIPE_PATH = CONFIG_DIR / "recipes" / "PHOTO_MATCHING_CHOICE_V3.json"

TRAVEL_RECIPE_ID = "PHOTO_TRAVEL_OUTFIT_V3"
MATCHING_RECIPE_ID = "PHOTO_MATCHING_CHOICE_V3"
VN_COPY_PACKS = {
    TRAVEL_RECIPE_ID: ("VN_TRAVEL_OUTFIT_V1", "travel_scene_four_looks"),
    MATCHING_RECIPE_ID: ("VN_SCARF_MATCHING_V1", "scarf_four_looks"),
}
THEME_KEY = "COOL_WEATHER_TRAVEL"
THAI_RANGE = re.compile(r"[\u0e00-\u0e7f]")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _vi_pack() -> dict:
    return loader.load_locale_pack_file(LOCALE_PACK_PATH)


def _th_pack() -> dict:
    return loader.load_locale_pack_file(TH_LOCALE_PACK_PATH)


def _theme(pack: dict) -> dict:
    return {
        "theme_key": THEME_KEY,
        "cta": pack["labels"]["generic"]["cta"],
        "visual_brief": "VN 围巾旅行穿搭：同一人物、同一目的地视觉体系、统一色彩基调",
    }


def _plan(recipe_id: str, *, reference_mode: str, locale_pack: dict,
          record_id: str, style_profile=None,
          variables=None, product_snapshot=None) -> dict:
    """Plan one batch offline for the given recipe/market binding."""
    recipe = loader.load_content_recipe_file(
        TRAVEL_RECIPE_PATH if recipe_id == TRAVEL_RECIPE_ID else MATCHING_RECIPE_PATH
    )
    spec = copy.deepcopy(recipe.recipe_spec_json)
    profile = spec["execution_profiles"][0]
    locale = str(locale_pack["locale"])
    templates = profile["copy_variants_by_locale"][locale]["copy_variants"]
    travel_contract = spec.get("travel_contract")
    plan = plan_th_choice_batch(
        record_id=record_id, recipe_id=recipe_id,
        theme=_theme(locale_pack), reference_mode=reference_mode, count=1,
        style_profile=style_profile, travel_contract=travel_contract,
        copy_templates=templates, recipe_spec=spec,
        variables=variables or {"destination": "seoul", "temperature_band": "0_5c"},
        locale_pack=locale_pack,
    )
    return plan


def _all_strings(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        out: list[str] = []
        for item in value.values():
            out.extend(_all_strings(item))
        return out
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            out.extend(_all_strings(item))
        return out
    return []


class VnMarketPackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pack = _read(MARKET_PACK_PATH)

    def test_pack_targets_vietnam_and_vietnamese(self):
        self.assertEqual(self.pack["schema_version"], "opv-market-pack-v1")
        self.assertEqual(self.pack["market_pack_id"], "MP_VN_DEFAULT_V1")
        self.assertEqual(self.pack["target_country"], "VN")
        self.assertEqual(self.pack["target_locale"], "vi-VN")
        self.assertEqual(self.pack["climate_zone"], "tropical_monsoon")
        # The pack never stores a publish language: only a locale *id*.
        self.assertFalse(THAI_RANGE.search(json.dumps(self.pack, ensure_ascii=False)))

    def test_pack_ships_as_a_draft_and_may_not_fall_back(self):
        self.assertEqual(self.pack["status"], "draft")
        self.assertIs(self.pack["copy_rules"]["fallback_allowed"], False)

    def test_copy_rules_require_a_native_review(self):
        rules = self.pack["copy_rules"]
        self.assertEqual(rules["locale"], "vi-VN")
        self.assertIn("NATIVE_APPROVED", rules["review_requirement"])
        for field in ("title", "caption", "hashtags", "cover_text"):
            self.assertIn(field, rules["must_localize"])

    def test_destination_weights_are_declared_over_the_live_token_vocabulary(self):
        weights = self.pack["topic_rules"]["destination_weights"]
        self.assertTrue(weights)
        spec = _read(TRAVEL_RECIPE_PATH)["recipe_spec"]
        # The recipe's destination enum is still the legacy V2 vocabulary (Phase 3
        # §7.1 item 4 leaves the catalog-id switch to Phase 5), so weights may
        # only cover keys that are actually selectable today.
        self.assertLessEqual(set(weights), set(spec["variables_schema"]["destination"]["values"]))

    def test_destination_weights_match_the_locale_destination_labels(self):
        weights = self.pack["topic_rules"]["destination_weights"]
        labels = _vi_pack()["labels"]["destinations"]
        self.assertEqual(set(weights), set(labels))

    def test_content_family_weights_cover_both_vn_lines(self):
        weights = self.pack["topic_rules"]["content_family_weights"]
        self.assertEqual(
            weights, {"scarf_matching": 5, "scarf_travel": 7}
        )

    def test_travel_theme_priority_keeps_temperature_last(self):
        priority = self.pack["topic_rules"]["travel_theme_priority"]
        self.assertEqual(priority[0], "color_pairing")
        self.assertEqual(priority[-1], "temperature_dressing")

    def test_scarf_rule_never_claims_warmth(self):
        banned = "\n".join(self.pack["safety_rules"]["banned"])
        self.assertIn("guarantees warmth", banned)
        self.assertIn("never the face", self.pack["visual_rules"]["scarf_visibility"])

    def test_pack_is_listed_in_the_seed_bundle(self):
        ids = [pack.market_pack_id for pack in loader.load_seed_bundle().market_packs]
        self.assertIn("MP_VN_DEFAULT_V1", ids)


class VnLocalePackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pack = _vi_pack()
        cls.v3_policy = json.loads(
            (CONFIG_DIR / "photo_planning_policies" / "TRAVEL_OUTFIT_V3.json")
            .read_text(encoding="utf-8")
        )
        cls.matching_policy = json.loads(
            (CONFIG_DIR / "photo_planning_policies" / "MATCHING_CHOICE_V3.json")
            .read_text(encoding="utf-8")
        )

    def test_pack_identity_and_no_fallback(self):
        self.assertEqual(self.pack["schema_version"], "opv-photo-locale-pack-v1")
        self.assertEqual(self.pack["locale_pack_id"], "LOCALE_VI_VN_V1")
        self.assertEqual(self.pack["locale"], "vi-VN")
        self.assertEqual(self.pack["status"], "draft")
        self.assertIs(self.pack["fallback_allowed"], False)

    def test_pack_covers_every_travel_moment_and_family(self):
        moments = _read(TRAVEL_RECIPE_PATH)["recipe_spec"]["travel_contract"]["moments"]
        labels = self.pack["labels"]["travel_moments"]
        for moment in moments:
            self.assertTrue(labels.get(moment["key"]), moment["key"])
        for family in self.v3_policy["families"]:
            entry = self.pack["family_copy"][family["family_id"]]
            for field in ("title", "cover", "caption"):
                self.assertTrue(entry[field], f"{family['family_id']}.{field}")
            roles = {look["role"] for look in family["looks"]}
            self.assertEqual(roles, set(entry["look_labels"]))

    def test_pack_covers_every_matching_family(self):
        for family in self.matching_policy["families"]:
            entry = self.pack["family_copy"][family["family_id"]]
            for field in ("title", "cover", "caption"):
                self.assertTrue(entry[field], f"{family['family_id']}.{field}")
            roles = {look["role"] for look in family["looks"]}
            self.assertEqual(roles, set(entry["look_labels"]))

    def test_pack_owns_the_complete_look_copy(self):
        variants = photo_locale.complete_look_copy(self.pack)
        self.assertEqual(len(variants), 4)
        for entry in variants:
            for field in ("title", "cover", "caption"):
                self.assertTrue(entry[field], field)

    def test_generic_labels_are_vietnamese(self):
        generic = photo_locale.locale_pack_labels(self.pack, "generic")
        self.assertTrue(generic["cta"])
        self.assertTrue(generic["look_label"])
        self.assertFalse(THAI_RANGE.search(json.dumps(generic, ensure_ascii=False)))

    def test_destination_and_temperature_labels_are_present(self):
        weights = _read(MARKET_PACK_PATH)["topic_rules"]["destination_weights"]
        labels = photo_locale.destination_labels(None, locale_pack=self.pack)
        for key in weights:
            self.assertTrue(labels.get(key), key)
        bands = photo_locale.temperature_labels(None, locale_pack=self.pack)
        for band in ("0_5c", "5_10c", "10_15c", "15_22c", "minus_10_0c"):
            self.assertTrue(bands.get(band), band)


class NoThaiLeakTest(unittest.TestCase):
    """Spec §8.1/§8.4: a Vietnamese artefact may contain no Thai at all."""

    def test_locale_pack_has_no_thai(self):
        text = LOCALE_PACK_PATH.read_text(encoding="utf-8")
        self.assertIsNone(THAI_RANGE.search(text), "vi-VN locale pack contains Thai")

    def test_copy_packs_have_no_thai(self):
        for recipe_id, (pack_id, _) in VN_COPY_PACKS.items():
            with self.subTest(pack=pack_id):
                text = (CONFIG_DIR / "copy_packs" / f"{pack_id}.tsv").read_text(
                    encoding="utf-8"
                )
                self.assertIsNone(THAI_RANGE.search(text), f"{pack_id} contains Thai")

    def test_market_pack_has_no_thai(self):
        text = MARKET_PACK_PATH.read_text(encoding="utf-8")
        self.assertIsNone(THAI_RANGE.search(text), "VN market pack contains Thai")

    def test_every_planned_vn_string_is_thai_free(self):
        pack = _vi_pack()
        plan = _plan(
            TRAVEL_RECIPE_ID, reference_mode="COMPLETE_LOOK", locale_pack=pack,
            record_id="no-thai-leak",
        )
        for text in _all_strings(plan["items"]):
            self.assertIsNone(THAI_RANGE.search(text), text)

    def test_complete_look_copy_is_thai_free(self):
        for entry in photo_locale.complete_look_copy(_vi_pack()):
            self.assertIsNone(THAI_RANGE.search(json.dumps(entry, ensure_ascii=False)))


class VnFailLoudTest(unittest.TestCase):
    """An incomplete VN pack must raise, never silently serve Thai/English."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pack = _vi_pack()

    def test_missing_family_copy_raises(self):
        family = {"family_id": "not_in_the_vn_pack", "looks": []}
        with self.assertRaises(PhotoLocaleError):
            photo_locale.family_copy(family, locale_pack=self.pack)

    def test_missing_complete_look_copy_raises(self):
        broken = json.loads(json.dumps(self.pack))
        del broken["complete_look_copy"]
        with self.assertRaises(PhotoLocaleError):
            photo_locale.complete_look_copy(broken)

    def test_empty_complete_look_variant_is_rejected_by_the_contract(self):
        payload = json.loads(json.dumps(self.pack))
        payload["complete_look_copy"][0]["title"] = ""
        errors = validate_locale_pack_payload(payload)
        self.assertTrue(any("complete_look_copy" in error for error in errors), errors)

    def test_missing_label_family_raises(self):
        broken = json.loads(json.dumps(self.pack))
        del broken["labels"]["travel_moments"]
        with self.assertRaises(PhotoLocaleError):
            photo_locale.travel_moment_labels(None, locale_pack=broken)

    def test_fallback_allowed_true_is_rejected(self):
        payload = json.loads(json.dumps(self.pack))
        payload["fallback_allowed"] = True
        self.assertTrue(validate_locale_pack_payload(payload))


class VnCopyPackTest(unittest.TestCase):
    def test_every_vn_line_has_a_draft_copy_pack(self):
        for recipe_id, (pack_id, profile_id) in VN_COPY_PACKS.items():
            with self.subTest(pack=pack_id):
                variants = loader.load_photo_copy_pack(
                    CONFIG_DIR / "copy_packs" / f"{pack_id}.tsv",
                    expected_recipe_id=recipe_id, expected_profile_id=profile_id,
                )
                self.assertEqual(len(variants), 4)
                for variant in variants:
                    copy_block = variant["copy"]
                    self.assertEqual(copy_block["language_review_status"], "DRAFT")
                    self.assertTrue(copy_block["title"])
                    self.assertEqual(len(copy_block["slide_texts"]), 5)

    def test_vn_packs_are_wired_into_their_recipes(self):
        travels = _read(TRAVEL_RECIPE_PATH)["recipe_spec"]["locale_copy_packs"]
        self.assertEqual(travels["vi-VN"], "VN_TRAVEL_OUTFIT_V1")
        matching = _read(MATCHING_RECIPE_PATH)["recipe_spec"]["locale_copy_packs"]
        self.assertEqual(matching, {"vi-VN": "VN_SCARF_MATCHING_V1"})

    def test_resolved_recipe_carries_one_copy_set_per_locale(self):
        recipe = loader.load_content_recipe_file(TRAVEL_RECIPE_PATH)
        profile = recipe.recipe_spec_json["execution_profiles"][0]
        self.assertEqual(
            sorted(profile["copy_variants_by_locale"]), ["th-TH", "vi-VN"]
        )
        # The default (legacy) set must stay the TH one.
        self.assertEqual(
            profile["copy_variants"],
            profile["copy_variants_by_locale"]["th-TH"]["copy_variants"],
        )


class VnLineConfigTest(unittest.TestCase):
    def test_both_vn_lines_have_a_planning_policy(self):
        self.assertTrue(recipe_has_planning_policy(TRAVEL_RECIPE_ID))
        self.assertTrue(recipe_has_planning_policy(MATCHING_RECIPE_ID))
        self.assertEqual(get_planning_flow(TRAVEL_RECIPE_ID), "travel_two_step")
        self.assertEqual(get_planning_flow(MATCHING_RECIPE_ID), "reference_contract_v1")

    def test_recipes_stay_draft_and_carry_no_market_or_category(self):
        for path in (TRAVEL_RECIPE_PATH, MATCHING_RECIPE_PATH):
            with self.subTest(recipe=path.name):
                recipe = loader.load_content_recipe_file(path)
                self.assertEqual(recipe.status, "draft")
                spec = recipe.recipe_spec_json
                for forbidden in ("markets", "category_key", "locale"):
                    self.assertNotIn(forbidden, spec)

    def test_scarf_adapter_satisfies_either_recipe(self):
        from services.photo_category_registry import SCARF_V1

        for path in (TRAVEL_RECIPE_PATH, MATCHING_RECIPE_PATH):
            with self.subTest(recipe=path.name):
                spec = _read(path)["recipe_spec"]
                required = set(spec["required_category_capabilities"])
                self.assertLessEqual(required, set(SCARF_V1.capabilities))
                self.assertEqual(spec["market_policy"], "MARKET_PACK_REQUIRED")


class VnOfflinePlanningTest(unittest.TestCase):
    """VN + scarf plans offline for all three reference modes (spec §7 Phase 4.5)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pack = _vi_pack()
        cls.spec = copy.deepcopy(
            loader.load_content_recipe_file(TRAVEL_RECIPE_PATH).recipe_spec_json
        )

    def _style_profile(self) -> dict:
        return canary_fixture._style_profile(self.spec, "seoul", "0_5c")

    def test_style_plan_is_vietnamese_with_four_ordered_looks(self):
        plan = _plan(
            TRAVEL_RECIPE_ID, reference_mode="STYLE", locale_pack=self.pack,
            record_id="vn-style", style_profile=self._style_profile(),
        )
        item = plan["items"][0]
        self.assertEqual(
            [look["role"] for look in item["looks"]],
            ["look_a", "look_b", "look_c", "look_d"],
        )
        self.assertTrue(item["copy"]["title"])
        self.assertIsNone(THAI_RANGE.search(item["copy"]["title"]))
        self.assertIsNone(THAI_RANGE.search(item["copy"]["caption"]))

    def test_product_attachment_writes_the_scarf_into_accessories(self):
        from services.photo_category_registry import (
            SCARF_V1, apply_target_product_to_look,
        )

        plan = _plan(
            TRAVEL_RECIPE_ID, reference_mode="STYLE", locale_pack=self.pack,
            record_id="vn-style-product", style_profile=self._style_profile(),
        )
        product = {"product_id": "S1", "category": "scarf", "product_name": "围巾"}
        for look in plan["items"][0]["looks"]:
            applied = apply_target_product_to_look(
                adapter=SCARF_V1, look=look, product_snapshot=product
            )
            with self.subTest(role=look["role"]):
                self.assertEqual(applied["accessories"], "指定商品围巾（以商品参考图为准）")
                self.assertEqual(applied["outerwear"], look["outerwear"])

    def test_complete_look_plan_generates_no_looks(self):
        plan = _plan(
            TRAVEL_RECIPE_ID, reference_mode="COMPLETE_LOOK",
            locale_pack=self.pack, record_id="vn-complete-look",
        )
        item = plan["items"][0]
        self.assertEqual(item["schema_version"], "opv-photo-content-plan-item-v1")
        self.assertEqual(item["looks"], [])
        self.assertTrue(item["copy"]["title"])
        self.assertIsNone(THAI_RANGE.search(item["copy"]["caption"]))

    def test_matching_line_plans_in_both_visual_modes(self):
        for mode in ("STYLE", "COMPLETE_LOOK"):
            with self.subTest(mode=mode):
                plan = _plan(
                    MATCHING_RECIPE_ID, reference_mode=mode, locale_pack=self.pack,
                    record_id=f"vn-matching-{mode}", style_profile=None,
                    variables={"destination": "hanoi", "temperature_band": "10_15c"},
                )
                item = plan["items"][0]
                self.assertTrue(item["copy"]["title"])
                for text in _all_strings(item["copy"]):
                    self.assertIsNone(THAI_RANGE.search(text), text)

    def test_unsupported_reference_mode_fails_loudly(self):
        with self.assertRaises(Exception):
            _plan(
                MATCHING_RECIPE_ID, reference_mode="NOT_A_MODE", locale_pack=self.pack,
                record_id="vn-bad-mode", style_profile=None,
            )


class VnBindsOnlyMarketAndLocaleTest(unittest.TestCase):
    """Spec §8.3: V3 + scarf under VN changes market/language, nothing else."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.vn_pack = _vi_pack()
        cls.th_pack = _th_pack()
        cls.spec = copy.deepcopy(
            loader.load_content_recipe_file(TRAVEL_RECIPE_PATH).recipe_spec_json
        )
        cls.catalog = loader.load_destination_catalog_file(DESTINATION_PATH)

    def _context(self, *, country: str, locale: str, pack_id: str, copy_pack_id: str):
        destination = next(
            entry for entry in self.catalog["destinations"]
            if entry["destination_id"] == "SEOUL_WINTER"
        )
        return build_execution_context(
            recipe_id=TRAVEL_RECIPE_ID, recipe_version=1,
            planning_flow="travel_two_step",
            category_key="scarf", category_profile_id="SCARF_V1",
            category_profile_version=1, main_product_slot="accessories",
            market_country=country, market_pack_id=pack_id, market_pack_version=1,
            locale=locale, locale_pack_id="LOCALE_" + locale.replace("-", "_").upper(),
            locale_pack_version=1, copy_pack_id=copy_pack_id,
            reference_mode="STYLE", input_fingerprint="fp-1",
            reference_hashes=["ref-1"], destination=destination,
            persona={"persona_ref_id": "P1", "persona_pack_id": "PP1"},
        )

    def test_only_market_and_locale_differ_between_th_and_vn(self):
        th = self._context(country="TH", locale="th-TH", pack_id="MP_TH_DEFAULT_V1",
                           copy_pack_id="TH_TRAVEL_OUTFIT_V3")
        vn = self._context(country="VN", locale="vi-VN", pack_id="MP_VN_DEFAULT_V1",
                           copy_pack_id="VN_TRAVEL_OUTFIT_V1")
        self.assertEqual(
            {key for key in th if th[key] != vn[key]}, {"market", "locale"}
        )

    def test_identical_looks_but_localized_copy(self):
        style_profile = canary_fixture._style_profile(self.spec, "seoul", "0_5c")
        th_plan = _plan(
            TRAVEL_RECIPE_ID, reference_mode="STYLE", locale_pack=self.th_pack,
            record_id="th-vs-vn", style_profile=style_profile,
        )
        vn_plan = _plan(
            TRAVEL_RECIPE_ID, reference_mode="STYLE", locale_pack=self.vn_pack,
            record_id="th-vs-vn", style_profile=style_profile,
        )
        th_item, vn_item = th_plan["items"][0], vn_plan["items"][0]
        # The executable content is identical...
        self.assertEqual(th_item["looks"], vn_item["looks"])
        self.assertEqual(th_item["difference_axes"], vn_item["difference_axes"])
        # ...and only the publish copy is localized.
        self.assertNotEqual(th_item["copy"]["title"], vn_item["copy"]["title"])
        self.assertTrue(THAI_RANGE.search(th_item["copy"]["title"]))
        self.assertIsNone(THAI_RANGE.search(vn_item["copy"]["title"]))


class VnProductionGateTest(unittest.TestCase):
    """Spec §9: nothing about VN may be reachable from production yet."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.presets = _read(PRESETS_PATH)["presets"]

    def _vn_presets(self) -> list[dict]:
        return [p for p in self.presets if p.get("category_key") == "scarf"
                and any(t.get("market") == "VN" for t in p.get("tasks") or [])]

    def test_exactly_two_vn_presets_and_both_are_disabled(self):
        vn = self._vn_presets()
        self.assertEqual(len(vn), 2)
        for preset in vn:
            with self.subTest(preset=preset["name"]):
                self.assertEqual(preset["status"], "disabled")
                self.assertTrue(preset["disabled_reason"])
                self.assertEqual(preset["media_kind"], "native_photo")
                for task in preset["tasks"]:
                    self.assertEqual(task["account_id"], "OPV_VN_TEST_001")
                    self.assertEqual(task["language"], "vi-VN")

    def test_vn_presets_route_to_the_two_vn_recipes(self):
        recipes = {
            task["recipe_id"]
            for preset in self._vn_presets() for task in preset["tasks"]
        }
        self.assertEqual(recipes, {TRAVEL_RECIPE_ID, MATCHING_RECIPE_ID})

    def test_no_vn_publish_route_exists(self):
        routes = _read(ROUTES_PATH)["routes"]
        self.assertNotIn("VN", routes)

    def test_vn_account_is_paused_and_cannot_publish(self):
        account = _read(ACCOUNT_PATH)
        self.assertEqual(account["schema_version"], "opv-account-profile-v1")
        self.assertEqual(account["target_country"], "VN")
        self.assertEqual(account["default_locale"], "vi-VN")
        self.assertEqual(account["status"], "paused")
        self.assertEqual(account["timezone"], "Asia/Ho_Chi_Minh")
        rules = account["operating_rules"]
        self.assertIs(rules["publishing_enabled"], False)
        self.assertEqual(rules["default_locale_pack_id"], "LOCALE_VI_VN_V1")
        self.assertEqual(rules["default_destination_catalog_id"], "EAST_ASIA_COOL_V1")

    def test_vn_account_declares_an_approved_persona_pool(self):
        rules = _read(ACCOUNT_PATH)["operating_rules"]
        pool = rules["allowed_persona_refs"]
        # Spec §7 Phase 4.3: pick and accept 2-3 persona templates.
        self.assertGreaterEqual(len(pool), 2)
        self.assertLessEqual(len(pool), 3)
        self.assertIn(_read(ACCOUNT_PATH)["persona_ref_id"], pool)
        self.assertEqual(rules["human_review_required"], True)
        self.assertEqual(rules["visual_qa_required"], True)

    def test_account_defaults_point_at_the_vn_config(self):
        account = _read(ACCOUNT_PATH)
        self.assertEqual(account["default_market_pack_id"], "MP_VN_DEFAULT_V1")
        self.assertEqual(account["default_render_preset_id"], "RP_STILL_FASTCUT_10S_V1")


class VnCanaryManifestTest(unittest.TestCase):
    """Spec §14.2: Phase 4 ships one offline manifest per reference mode."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.rebuilt = canary_fixture.build_all()

    def test_three_manifests_cover_the_three_reference_modes(self):
        self.assertEqual(
            {canary_id: manifest["reference_mode"]
             for canary_id, manifest in self.rebuilt.items()},
            {
                "VN_SCARF_STYLE": "STYLE",
                "VN_SCARF_STYLE_PRODUCT": "STYLE",
                "VN_SCARF_COMPLETE_LOOK": "COMPLETE_LOOK",
            },
        )

    def test_shipped_fixtures_match_a_fresh_rebuild(self):
        for canary_id, manifest in self.rebuilt.items():
            with self.subTest(canary=canary_id):
                path = canary_fixture.fixture_path(canary_id)
                self.assertTrue(path.is_file(), path)
                shipped = _read(path)
                for key, value in manifest.items():
                    self.assertEqual(shipped.get(key), value, f"{canary_id}.{key}")
                self.assertIn("_fixture_note", shipped)

    def test_every_manifest_is_vn_bound(self):
        for canary_id, manifest in self.rebuilt.items():
            with self.subTest(canary=canary_id):
                context = manifest["context"]
                self.assertEqual(context["market"]["country"], "VN")
                self.assertEqual(context["market"]["market_pack_id"], "MP_VN_DEFAULT_V1")
                self.assertEqual(context["locale"]["locale"], "vi-VN")
                self.assertEqual(context["locale"]["locale_pack_id"], "LOCALE_VI_VN_V1")
                self.assertEqual(context["category"]["key"], "scarf")
                self.assertEqual(context["category"]["main_product_slot"], "accessories")

    def test_no_manifest_contains_thai(self):
        for canary_id, manifest in self.rebuilt.items():
            with self.subTest(canary=canary_id):
                text = json.dumps(manifest, ensure_ascii=False)
                self.assertIsNone(THAI_RANGE.search(text), canary_id)

    def test_unresolved_bindings_are_declared_in_every_manifest(self):
        for canary_id, manifest in self.rebuilt.items():
            with self.subTest(canary=canary_id):
                bindings = {item["binding"] for item in manifest["unresolved"]}
                self.assertEqual(bindings, {
                    "asset_set_keys", "copy_pack_language_review_status",
                    "market_pack_status", "account_status",
                })
                for item in manifest["unresolved"]:
                    self.assertIs(item["delivered"], False, item["binding"])

    def test_style_and_style_product_share_one_content_plan(self):
        style = self.rebuilt["VN_SCARF_STYLE"]
        product = self.rebuilt["VN_SCARF_STYLE_PRODUCT"]
        self.assertEqual(style["content_plan"], product["content_plan"])
        self.assertNotIn("product", style["context"])
        self.assertEqual(
            product["context"]["product"]["category"], "scarf"
        )

    def test_product_canary_freezes_the_scarf_into_all_four_looks(self):
        manifest = self.rebuilt["VN_SCARF_STYLE_PRODUCT"]
        application = manifest["product_application"]
        self.assertTrue(application)
        for family, looks in application.items():
            self.assertEqual(set(looks), {"look_a", "look_b", "look_c", "look_d"})
            for role, entry in looks.items():
                self.assertEqual(entry["accessories"], "指定商品围巾（以商品参考图为准）")
        contract = manifest["product_qa_contract"]
        self.assertEqual(contract["category_key"], "scarf")
        self.assertEqual(contract["main_product_slot"], "accessories")
        self.assertEqual(len(contract["qa_fields"]), 10)

    def test_complete_look_manifest_carries_four_uploaded_hashes(self):
        manifest = self.rebuilt["VN_SCARF_COMPLETE_LOOK"]
        self.assertEqual(len(manifest["context"]["reference"]["reference_hashes"]), 4)
        self.assertEqual(manifest["content_plan"]["items"][0]["looks"], [])
        self.assertNotIn("product_application", manifest)

    def test_manifests_record_the_vn_account_binding(self):
        for canary_id, manifest in self.rebuilt.items():
            with self.subTest(canary=canary_id):
                binding = manifest["account_binding"]
                self.assertEqual(binding["account_id"], "OPV_VN_TEST_001")
                self.assertEqual(binding["status"], "paused")
                self.assertIs(binding["publishing_enabled"], False)
                self.assertTrue(binding["allowed_persona_refs"])


class NoVnServiceBranchTest(unittest.TestCase):
    """Spec §11/§9: VN must be configuration, never a service-level branch."""

    def test_no_photo_vn_or_scarf_vn_prefix_in_services(self):
        offenders = []
        for path in sorted((PACKAGE_ROOT / "services").glob("*.py")):
            text = path.read_text(encoding="utf-8")
            for token in ("PHOTO_VN_", "SCARF_VN_", "MARKET_VN"):
                if token in text:
                    offenders.append(f"{path.name}:{token}")
        self.assertEqual(offenders, [])

    def test_no_country_literal_branch_in_the_planner(self):
        text = (PACKAGE_ROOT / "services" / "photo_content_planner.py").read_text(
            encoding="utf-8"
        )
        for token in ('"VN"', "'VN'", '"TH"', "'TH'"):
            self.assertNotIn(token, text, token)


if __name__ == "__main__":
    unittest.main()
