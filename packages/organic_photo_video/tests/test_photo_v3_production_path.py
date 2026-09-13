#!/usr/bin/env python3
"""Review gate: VN + scarf + STYLE+PRODUCT through the *real* production chain.

The 2026-09-13 review asked for at least one test that actually calls
``PhotoRequestFactory.build_batch()`` and ``PhotoReusePlannerService.plan_task()``
for a VN scarf run, rather than re-deriving the locale/category by hand.  Until
this file existed, every VN test went through ``plan_th_choice_batch`` (the
vision/theme planner) or hand-picked ``copy_variants_by_locale`` directly, so
three P0 defects stayed invisible:

* **P0-1** — both consumers still compared the *literal* ``category_key`` and
  ``markets`` fields, which an ``opv-photo-recipe-v2`` Recipe deliberately does
  not carry, so every v2 Recipe was rejected before it could be frozen.  The
  binding is capability + ``market_policy`` now, and these tests freeze and plan
  a v2 Recipe end to end.
* **P0-2** — the frozen copy came from the *sorted-first* locale (always Thai)
  and the audited ``{destination}``/``{temperature}`` tokens still resolved from
  the legacy inline Thai tables.  Both halves now read the request's own Locale
  Pack; a VN request freezes Vietnamese text with no Thai anywhere.
* **P0-3** — ``SCARF_QA_FIELDS`` was declarable but had no runtime consumer, so
  a missing / obscured / restructured scarf could only ever surface as the
  generic ``product_matches`` boolean.  The scarf contract is now resolved from
  the product's own category adapter by the production QA path and blocks.

Scope notes (deliberate, so the test cannot be mistaken for a launch gate):

* ``MP_VN_DEFAULT_V1`` ships as ``draft``; the operational enablement is
  simulated by flipping the in-memory pack to ``active``.  The draft pack is
  itself asserted to fail loudly, so this file proves *both* that enabling works
  and that staying draft is still refused.
* ``PHOTO_MATCHING_CHOICE_V3`` already ships ``asset_set_keys=["VN_SCARF_CHOICE"]``
  (the scarf line's own key), so the scarf freeze needs no override at all.
  ``PHOTO_TRAVEL_OUTFIT_V3`` still ships the TH key — the Phase 4 canary declares
  that as an unresolved binding — so the travel token check pins the VN asset set
  explicitly and documents why.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
for _path in (PACKAGE_ROOT, TESTS_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from config import loader  # noqa: E402
from domain.models import AssetSet, ContentRecipe  # noqa: E402
from services.photo_category_registry import (  # noqa: E402
    adapter_for_product_category,
)
from services.photo_copy import contract_copy_tokens  # noqa: E402
from services.photo_planner import PhotoPlannerError, PhotoReusePlannerService  # noqa: E402
from services.photo_recipe_contract import category_binding_errors  # noqa: E402
from services.photo_reference_vision import PhotoReferenceVisionService  # noqa: E402
from services.photo_request_factory import (  # noqa: E402
    PhotoRequestError, PhotoRequestFactory, _bound_locale_pack, fingerprint,
    validate_frozen_request,
)
from services.photo_travel_qa import (  # noqa: E402
    FAILURE_OUTFIT_MISMATCH, FAILURE_PERSON_DISASTER, FOOTWEAR_TYPES,
    PRODUCT_QA_FAILURE_FIELDS, TravelSemanticQAError,
    failed_roles_from_travel_qa, moment_rules_from_contract, normalize_travel_qa,
)

import photo_vn_canary_fixture as canary_fixture  # noqa: E402

CONFIG_DIR = PACKAGE_ROOT / "config"
MATCHING_RECIPE_PATH = CONFIG_DIR / "recipes" / "PHOTO_MATCHING_CHOICE_V3.json"
TRAVEL_RECIPE_PATH = CONFIG_DIR / "recipes" / "PHOTO_TRAVEL_OUTFIT_V3.json"
MARKET_PACK_PATH = CONFIG_DIR / "market_packs" / "MP_VN_DEFAULT_v1.json"
ACCOUNT_PATH = CONFIG_DIR / "accounts" / "OPV_VN_TEST_001.json"
DESTINATION_PATH = CONFIG_DIR / "destinations" / "EAST_ASIA_COOL_V1.json"

MATCHING_RECIPE_ID = "PHOTO_MATCHING_CHOICE_V3"
TRAVEL_RECIPE_ID = "PHOTO_TRAVEL_OUTFIT_V3"
MARKET_PACK_ID = "MP_VN_DEFAULT_V1"
ACCOUNT_ID = "OPV_VN_TEST_001"
LOCALE = "vi-VN"
SCARF_ASSET_SET_KEY = "VN_SCARF_CHOICE"
TASK_ID = "vn-scarf-production-task"

THAI_RANGE = re.compile(r"[\u0e00-\u0e7f]")
CJK_RANGE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")

#: The scarf the operator would freeze in.  Same shape as the Phase 4 canary
#: product, but this run goes through the production freeze path.
PRODUCT = {
    "product_id": "VN_SCARF_PROD_001",
    "product_name": "格纹羊毛围巾",
    "category": "scarf",
    "reference_pack_id": "PK_VN_SCARF_PROD",
    "reference_pack_version": 1,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_scarf_asset_set(root: Path) -> AssetSet:
    """A VN scarf look pack: four verified, distinct looks in one accessory.

    Everything ``freeze_content_card`` demands is real here — four distinct
    ``(outerwear_id, bottom_id)`` combinations back ``visual_rules.distinct_looks``,
    and the source qualification approves the scarf logic key rather than a
    garment one, so a scarf pack cannot silently reuse a womenswear approval.
    """
    assets = []
    approval = {
        "schema_version": "opv-source-qualification-v1",
        "reviewer": "test_vn_scarf_production_path",
        "allowed_logic_keys": ["scarf_styling_choice", "travel_scene_outfit_choice"],
        "source_hashes": {},
        "attributes": {},
    }
    outerwear = ["coat-a", "coat-b", "coat-c", "coat-d"]
    for index, letter in enumerate("abcd"):
        path = root / f"vn-scarf-look-{letter}.jpg"
        Image.new("RGB", (80, 120), (index * 40 + 20, 60, 110)).save(path)
        asset_id = f"vn-scarf-{letter}"
        assets.append({
            "asset_id": asset_id,
            "path": str(path),
            "sha256": _sha256(path),
            "role": f"look_{letter}",
            "display_label": {
                "vi-VN": f"Khăn {letter.upper()}",
                "zh-CN": f"造型 {letter.upper()}",
            },
        })
        approval["source_hashes"][asset_id] = _sha256(path)
        approval["attributes"][asset_id] = {
            "outerwear_id": outerwear[index],
            "bottom_id": f"bottom-{index}",
        }
    return AssetSet(
        asset_set_id="aset-vn-scarf-1",
        asset_set_key=SCARF_ASSET_SET_KEY,
        category_key="scarf",
        market="VN",
        asset_set_version=1,
        status="enabled",
        manifest_json={"assets": assets, "content_approval": approval},
        tags_json={
            # One scarf pack serves both VN lines; the two recipes screen it by
            # their own asset_match_keys / required_tags.
            "choice_axis": ["scarf_pairing", "pants_or_skirt"],
            "use_cases": ["scarf_styling_choice", "travel_outfit_choice"],
            "scene": ["Photo", "Cafe", "Street"],
            "style": ["korean_clean", "minimal"],
        },
    )


class VnScarfProductionRepo:
    """The minimum repository surface ``build_batch`` + ``plan_task`` touch.

    Mirrors ``test_photo_planner.PlannerRepo`` plus the reference-data lookups
    the v2 contract needs (account profile, Market Pack), so the chain under test
    is the production one and not a stub that skips the market binding.
    """

    def __init__(self, root: Path, *, recipe_path: Path, pack_active: bool = True):
        self.task = SimpleNamespace(
            task_id=TASK_ID, task_status="draft", media_kind="native_photo",
            category_key="scarf", target_country="VN", target_locale=LOCALE,
            product_mode="NO_PRODUCT", product_id=None, product_snapshot_json={},
            market_pack_id=MARKET_PACK_ID, requested_shot_count=5,
            idempotency_key="vn-scarf-stable-key", row_version=1, plan_json={},
            copy_json={}, content_package_id=None, storyboard_version=None,
            workflow_version=1, active_revision_id=None,
        )
        self.recipe = loader.load_content_recipe_file(recipe_path)
        # The recipe ships as draft (Phase 4 canary); "seeded" is what the
        # repository would report once the operator activates the line.
        self.recipe.status = "active"
        self.pack = loader.load_market_pack_file(MARKET_PACK_PATH)
        self.pack.status = "active" if pack_active else "draft"
        self.account = loader.load_account_import_file(ACCOUNT_PATH)
        self.asset_set = _build_scarf_asset_set(root)
        self.package = None
        self.revision = None

    # --- task ---------------------------------------------------------------
    def get_task(self, task_id):
        return self.task if task_id == self.task.task_id else None

    def update_task_plan(self, task_id, **fields):
        assert task_id == self.task.task_id
        for key, value in fields.items():
            setattr(self.task, key, copy.deepcopy(value))

    def update_task_requested_shot_count(self, task_id, *, requested_shot_count):
        assert task_id == self.task.task_id
        self.task.requested_shot_count = int(requested_shot_count)

    def transition_task(self, task_id, source, target, **_kwargs):
        assert task_id == self.task.task_id and self.task.task_status == source
        self.task.task_status = target
        return self.task

    # --- reference data -----------------------------------------------------
    def get_content_recipe(self, recipe_id):
        return self.recipe if recipe_id == self.recipe.recipe_id else None

    def get_market_pack(self, market_pack_id):
        return self.pack if market_pack_id == self.pack.market_pack_id else None

    def get_account_profile(self, account_id):
        return self.account if account_id == self.account.account_id else None

    # --- assets -------------------------------------------------------------
    def get_asset_set(self, asset_set_id):
        return self.asset_set if asset_set_id == self.asset_set.asset_set_id else None

    def list_asset_sets(self, **_kwargs):
        return [self.asset_set]

    # --- package / revision -------------------------------------------------
    def get_content_package_by_task(self, task_id):
        return self.package if self.package and task_id == self.task.task_id else None

    def insert_content_package(self, package):
        self.package = package

    def get_content_package(self, package_id):
        return self.package if self.package and package_id == self.package.content_package_id else None

    def list_task_revisions(self, _task_id):
        return [] if self.revision is None else [self.revision]

    def create_revision_and_activate(self, revision, **_kwargs):
        self.revision = revision
        self.task.active_revision_id = revision.revision_id
        self.task.workflow_version = 2
        self.task.row_version += 1
        return self.task

    def get_task_revision(self, revision_id):
        return self.revision if self.revision and revision_id == self.revision.revision_id else None


class VnScarfProductionPathTest(unittest.TestCase):
    """The freeze + plan chain a real VN scarf row would take."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = VnScarfProductionRepo(
            Path(self.temp.name), recipe_path=MATCHING_RECIPE_PATH)
        self.layouts = loader.load_board_layouts()
        self.spec = SimpleNamespace(
            recipe_id=MATCHING_RECIPE_ID, market="VN", language=LOCALE,
            account_id=ACCOUNT_ID,
        )
        self.factory = PhotoRequestFactory(self.repo, layouts=self.layouts)

    def _freeze(self, **kwargs):
        return self.factory.build_batch(
            record_id="vn-scarf-record", specs=[self.spec],
            category_key="scarf", **kwargs,
        )

    @staticmethod
    def _spec_of(request) -> dict:
        """The recipe spec as the *frozen* request carries it (a row, not a dict)."""
        return ContentRecipe.from_row(request["recipe_snapshot"]).recipe_spec_json

    def _attach_product(self, request):
        """Exactly what the production freeze does for a product-mode row."""
        request["product_id"] = PRODUCT["product_id"]
        request["product_snapshot"] = dict(PRODUCT)
        request["request_sha256"] = fingerprint({
            key: value for key, value in request.items() if key != "request_sha256"
        })
        validate_frozen_request(request)
        return request

    def _plan(self, request, **kwargs):
        return PhotoReusePlannerService(self.repo).plan_task(
            self.repo.task.task_id, recipe_id=request["recipe_id"],
            variables=request["variables"], copy_block=request["copy"],
            layout=request["layout_snapshot"], asset_set_id=request["asset_set_id"],
            recipe_snapshot=request["recipe_snapshot"],
            asset_snapshot=request["asset_snapshot"],
            content_card=request["content_card"], **kwargs
        )

    # --- P0-1: a v2 Recipe can be frozen at all -----------------------------
    def test_v2_recipe_freezes_by_capability_and_market_policy(self):
        request = self._freeze()[0]
        spec = self._spec_of(request)
        # The literal fields the old consumers compared are genuinely absent...
        self.assertNotIn("category_key", spec)
        self.assertNotIn("markets", spec)
        self.assertEqual(spec["market_policy"], "MARKET_PACK_REQUIRED")
        # ...yet the request binds category + market + language.
        self.assertEqual(request["category_key"], "scarf")
        self.assertEqual(request["market"], "VN")
        self.assertEqual(request["locale"], LOCALE)
        self.assertEqual(request["asset_set_key"], SCARF_ASSET_SET_KEY)
        validate_frozen_request(request)

    def test_capability_binding_accepts_a_superset_and_rejects_strangers(self):
        """The approved contract: capabilities, not a category name."""
        spec = self._spec_of(self._freeze()[0])
        required = set(spec["required_category_capabilities"])
        # The scarf adapter declares exactly the required capabilities...
        self.assertEqual(set(adapter_for_product_category("scarf").capabilities), required)
        # ...so the capability check itself passes for it.
        self.assertEqual(category_binding_errors(spec, category_key="scarf"), [])
        # A category nobody registered cannot claim the recipe.
        stranger = category_binding_errors(spec, category_key="footwear")
        self.assertTrue(stranger)
        self.assertIn("未注册的类目适配器", "; ".join(stranger))
        with self.assertRaises(PhotoRequestError) as caught:
            self.factory.build_batch(
                record_id="vn-scarf-record", specs=[self.spec],
                category_key="footwear",
            )
        self.assertIn("未注册的类目适配器", str(caught.exception))

    def test_a_category_adapter_without_the_capabilities_is_rejected(self):
        """A garment-only adapter must not claim a scarf/travel recipe."""
        spec = self._spec_of(self._freeze()[0])
        from services import photo_recipe_contract as contract
        from services.photo_category_registry import WOMENSWEAR_V1

        stub = copy.copy(WOMENSWEAR_V1)
        object.__setattr__(stub, "capabilities", ("wearable_styling",))
        original = contract.get_photo_category_adapter
        contract.get_photo_category_adapter = lambda key: stub
        try:
            errors = contract.category_binding_errors(spec, category_key="womenswear")
        finally:
            contract.get_photo_category_adapter = original
        self.assertTrue(errors)
        self.assertIn("missing required capabilities", "; ".join(errors))

    # --- P0-2: the frozen copy is Vietnamese -------------------------------
    def test_frozen_copy_is_vietnamese_with_no_thai_or_cjk(self):
        request = self._freeze()[0]
        copy_block = request["copy"]
        for field in ("title", "caption"):
            self.assertTrue(copy_block.get(field), field)
        visible = [copy_block["title"], copy_block["caption"]] + list(
            copy_block["slide_texts"])
        for text in visible:
            with self.subTest(text=text):
                self.assertIsNone(THAI_RANGE.search(text))
                self.assertIsNone(CJK_RANGE.search(text))
        self.assertEqual(len(copy_block["slide_texts"]), 5)
        # The look labels are resolved from the scarf pack's own display labels.
        self.assertIn("Khăn A", copy_block["slide_texts"][1])

    def test_the_thai_copy_pack_is_not_the_default_any_more(self):
        """The regression the review found: Thai text under a VN request."""
        request = self._freeze()[0]
        by_locale = self._spec_of(request)["execution_profiles"][0][
            "copy_variants_by_locale"]
        # Only the VN copy pack is bound, so there is no Thai set to fall into.
        self.assertEqual(sorted(by_locale), [LOCALE])
        variant_ids = [item["copy_id"] for item in by_locale[LOCALE]["copy_variants"]]
        self.assertIn(request["copy_variant_id"], variant_ids)
        chosen = next(item for item in by_locale[LOCALE]["copy_variants"]
                      if item["copy_id"] == request["copy_variant_id"])
        # The frozen copy is the chosen VN variant verbatim — labels aside.
        self.assertEqual(request["copy"]["hashtags"], chosen["copy"]["hashtags"])
        self.assertEqual(request["copy"]["language_review_status"], "DRAFT")
        self.assertEqual(len(request["copy"]["hashtags"]), 3)
        for hashtag in request["copy"]["hashtags"]:
            self.assertTrue(hashtag.startswith("#") and len(hashtag) > 1)

    def test_unsupported_publish_language_fails_loudly(self):
        self.spec.language = "es-MX"
        with self.assertRaises(PhotoRequestError):
            self._freeze()

    # --- STYLE+PRODUCT ------------------------------------------------------
    def test_style_plus_product_freezes_the_scarf_onto_the_request(self):
        request = self._attach_product(self._freeze()[0])
        self.assertEqual(request["product_id"], PRODUCT["product_id"])
        self.assertEqual(request["product_snapshot"]["category"], "scarf")
        # The category adapter is what turns that snapshot into the accessory
        # slot; the scarf never overwrites a garment slot.
        adapter = adapter_for_product_category(PRODUCT["category"])
        self.assertEqual(adapter.main_product_slot, "accessories")
        self.assertEqual(adapter.category_key, "scarf")

    # --- the planner consumes the frozen request ---------------------------
    def test_plan_task_plans_the_frozen_scarf_request(self):
        request = self._attach_product(self._freeze()[0])
        result = self._plan(request)
        plan = result["plan"]
        self.assertEqual(plan["category_key"], "scarf")
        self.assertEqual(plan["market_pack"], {
            "id": MARKET_PACK_ID, "version": self.repo.pack.pack_version,
            "country": "VN", "locale": LOCALE,
        })
        # Reused assets only: a scarf pack is frozen by the factory, not
        # re-generated by the planner.
        self.assertEqual(plan["production_policy"], {"mode": "ASSET_REUSE", "ai_image_calls": 0})
        self.assertEqual(len(plan["slides"]), 5)
        self.assertEqual([shot["slot_role"] for shot in plan["shots"]],
                         ["look_a", "look_b", "look_c", "look_d"])
        self.assertEqual(self.repo.task.task_status, "planned")
        self.assertIsNotNone(self.repo.task.active_revision_id)
        for text in [plan["copy"]["title"], plan["copy"]["caption"]] + plan["copy"]["slide_texts"]:
            self.assertIsNone(THAI_RANGE.search(text), text)
        self.assertIsNotNone(result["content_package_id"])

    def test_planner_rechecks_the_market_gate_on_its_own(self):
        """A pack that goes back to draft blocks planning, not just freezing."""
        request = self._attach_product(self._freeze()[0])
        self.repo.pack.status = "draft"
        with self.assertRaises(PhotoPlannerError) as caught:
            self._plan(request)
        self.assertIn("MARKET_PACK_REQUIRED", str(caught.exception))
        self.assertEqual(self.repo.task.task_status, "draft")

    # --- the shipped draft pack is still a hard gate ------------------------
    def test_shipped_draft_market_pack_blocks_the_whole_chain(self):
        self.repo.pack.status = "draft"
        with self.assertRaises(PhotoRequestError) as caught:
            self._freeze()
        self.assertIn("MARKET_PACK_REQUIRED", str(caught.exception))
        # A task with no Market Pack at all is refused before asset matching.
        self.repo.task.market_pack_id = ""
        self.repo.pack.status = "active"
        with self.assertRaises(PhotoPlannerError) as caught:
            PhotoReusePlannerService(self.repo).plan_task(
                self.repo.task.task_id, recipe_id=MATCHING_RECIPE_ID,
                variables={"choice_axis": "scarf_pairing"}, copy_block={},
                layout={}, asset_set_id=self.repo.asset_set.asset_set_id,
            )
        self.assertIn("MARKET_PACK_REQUIRED", str(caught.exception))


class VnDegradedCopyTemplateTest(unittest.TestCase):
    """The degraded travel copy is language-owned too, never Thai (P0-2).

    When the model's copy fails the publish-language check the planner degrades
    to a template instead of blocking generation (spec §7.3).  That template used
    to be Thai unconditionally, so a VN task would have published Thai text with
    a Vietnamese title — the failure mode the review called out.
    """

    def setUp(self):
        self.pack = loader.load_locale_pack_file(
            CONFIG_DIR / "locales" / "LOCALE_VI_VN_V1.json")

    def test_the_pack_template_is_vietnamese(self):
        from services.photo_locale import travel_copy_template

        template = travel_copy_template(self.pack, 1, topic_zh="河内冷季")
        self.assertIsNotNone(template)
        for field in ("title", "cover", "caption", "cta", "look_label"):
            self.assertTrue(template.get(field), field)
        self.assertIsNone(THAI_RANGE.search(json.dumps(template, ensure_ascii=False)))
        self.assertIsNone(CJK_RANGE.search(template["title"]))
        self.assertEqual(template["look_label"], "Look {letter}")

    def test_every_rotation_is_thai_free_and_the_copy_actually_rotates(self):
        from services.photo_locale import travel_copy_template

        captions = {
            travel_copy_template(self.pack, index)["caption"]
            for index in range(1, 5)
        }
        self.assertEqual(len(captions), 4)
        for caption in captions:
            self.assertIsNone(THAI_RANGE.search(caption))

    def test_no_pack_keeps_the_legacy_inline_thai_template(self):
        from services.photo_locale import travel_copy_template
        from services.photo_reference_vision import _travel_copy_template

        # The helper itself declines, so the caller keeps the legacy branch.
        self.assertIsNone(travel_copy_template(None, 1, topic_zh="河内冷季"))
        legacy = _travel_copy_template(None, 1, {"thai_fallback": {}}, "河内冷季")
        self.assertTrue(THAI_RANGE.search(json.dumps(legacy, ensure_ascii=False)))
        self.assertEqual(legacy["title"], "4 ลุคทริป")
        self.assertEqual(legacy["look_label"], "ลุค {letter}")
        # With a pack bound the same call is Vietnamese instead.
        bound = _travel_copy_template(self.pack, 1, {"thai_fallback": {}}, "河内冷季")
        self.assertIsNone(THAI_RANGE.search(json.dumps(bound, ensure_ascii=False)))


class LegacyV1FactoryPathUnchangedTest(unittest.TestCase):
    """The new Locale-Pack branch must stay unreachable for every v1 Recipe.

    Threading a Locale Pack into the factory is only safe because a v1 Recipe
    declares no ``locale_copy_packs`` and therefore keeps reading its own inline
    Thai tables.  Pin that, so a future recipe cannot silently switch which
    tables the factory reads.
    """

    def test_no_v1_recipe_binds_a_locale_pack(self):
        recipes = loader.load_content_recipes()
        v1 = [r for r in recipes
              if r.recipe_spec_json.get("schema_version") != "opv-photo-recipe-v2"]
        self.assertTrue(v1)
        for recipe in v1:
            with self.subTest(recipe=recipe.recipe_id):
                self.assertFalse(recipe.recipe_spec_json.get("locale_copy_packs"))
                self.assertIsNone(_bound_locale_pack(recipe.recipe_spec_json, "th-TH"))

    def test_v1_copy_tokens_are_unchanged_by_the_new_kwarg(self):
        recipe = next(r for r in loader.load_content_recipes()
                      if r.recipe_id == "PHOTO_TH_TRAVEL_OUTFIT_V2")
        variables = {"destination": "seoul", "temperature_band": "0_5c"}
        legacy = contract_copy_tokens(recipe.recipe_spec_json, variables)
        explicit = contract_copy_tokens(
            recipe.recipe_spec_json, variables, locale_pack=None)
        self.assertEqual(legacy, explicit)
        self.assertTrue(all(legacy.values()))
        # The TH labels still come from the recipe's own inline tables.
        self.assertTrue(THAI_RANGE.search(legacy["destination"]), legacy)


class VnTravelV3FactoryTokenTest(unittest.TestCase):
    """The travel line's audited tokens must come from the Locale Pack too.

    ``PHOTO_TRAVEL_OUTFIT_V3`` drops the inline ``destination_labels_th`` /
    ``temperature_labels_th`` tables, so before the fix the factory resolved both
    tokens to the empty string and froze ``"Gợi ý phối đồ du lịch "`` — a
    destination-free title that no reviewer would catch from a Thai-leak check.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = VnScarfProductionRepo(
            Path(self.temp.name), recipe_path=TRAVEL_RECIPE_PATH)
        self.spec = SimpleNamespace(
            recipe_id=TRAVEL_RECIPE_ID, market="VN", language=LOCALE,
            account_id=ACCOUNT_ID,
        )

    def _freeze(self):
        return PhotoRequestFactory(
            self.repo, layouts=loader.load_board_layouts(),
        ).build_batch(
            record_id="vn-travel-record", specs=[self.spec],
            category_key="scarf",
            # V3 still ships the TH asset-set key (Phase 4 canary declares this
            # as an unresolved binding); pinning the VN pack is what Phase 5
            # will encode in the recipe itself.
            overrides=[{"asset_set_key": SCARF_ASSET_SET_KEY}],
        )[0]

    def test_travel_tokens_resolve_to_vietnamese_labels(self):
        request = self._freeze()
        # The profile's own variables: destination generic_cool_city / 15_22c.
        self.assertEqual(request["variables"]["destination"], "generic_cool_city")
        self.assertEqual(request["variables"]["temperature_band"], "15_22c")
        pack = loader.load_locale_pack_file(CONFIG_DIR / "locales" / "LOCALE_VI_VN_V1.json")
        expected_destination = pack["labels"]["destinations"]["generic_cool_city"]
        expected_temperature = pack["labels"]["temperature_bands"]["15_22c"]
        copy_block = request["copy"]
        for text in [copy_block["title"], copy_block["caption"]] + list(copy_block["slide_texts"]):
            with self.subTest(text=text):
                self.assertIsNone(THAI_RANGE.search(text), text)
                self.assertNotIn("{destination}", text)
                self.assertNotIn("{temperature}", text)
        filled = copy_block["title"] + copy_block["caption"]
        self.assertIn(expected_destination, filled)
        self.assertIn(expected_temperature, filled)


class ScarfQaContractRuntimeTest(unittest.TestCase):
    """P0-3: the declared scarf QA fields actually decide pass/fail at runtime.

    The QA prompt and the normalizer are the two places the production QA path
    uses them, so both are exercised here through the same adapter lookup
    ``review_travel_pages`` performs — not by passing the field list by hand.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.recipe = loader.load_content_recipe_file(TRAVEL_RECIPE_PATH)
        cls.spec = copy.deepcopy(cls.recipe.recipe_spec_json)
        cls.pack = loader.load_locale_pack_file(CONFIG_DIR / "locales" / "LOCALE_VI_VN_V1.json")
        style_profile = canary_fixture._style_profile(cls.spec, "seoul", "0_5c")
        cls.looks = style_profile["recommended_sets"][0]["looks"]
        cls.rules = moment_rules_from_contract(cls.spec["travel_contract"])
        cls.adapter = adapter_for_product_category(PRODUCT["category"])
        cls.qa_fields = tuple(cls.adapter.qa_fields)
        cls.qa_rules = tuple(cls.adapter.qa_rules)

    def _page(self, look, **overrides):
        page = {
            "role": look["role"],
            "observed_moment": look["travel_moment"],
            "scene_evidence": [f"{look['travel_moment']} 画面证据"],
            "product_matches": True,
            "outfit_matches": True,
            "outfit_severity": "NONE",
            "weather_matches": True,
            "mobility_matches": True,
            "observed_footwear_type": look["footwear_type"],
            "person_flags": {
                "face_or_limb_deformity": False, "obvious_unnatural_tilt": False,
                "similar_fixed_smile": False, "similar_gaze": False,
                "similar_head_pose": False,
            },
            "repair_instruction": "",
        }
        # The model is asked for every declared product field; a real response
        # carries all of them, so anything the check reads must be present.
        for field in PRODUCT_QA_FAILURE_FIELDS:
            page.setdefault(field, True)
        page.update(overrides)
        return page

    def _raw(self, **overrides_by_role):
        pages = [self._page(look, **overrides_by_role.get(look["role"], {}))
                 for look in self.looks]
        return {"pages": pages, "style_uniform": True,
                "destination_conflict": False, "notes": "ok"}

    def _normalize(self, raw, *, qa_fields=None):
        return normalize_travel_qa(
            raw, look_plans=self.looks, moment_rules=self.rules,
            footwear_types=FOOTWEAR_TYPES, has_product=True,
            product_qa_fields=self.qa_fields if qa_fields is None else qa_fields,
        )

    def test_scarf_contract_is_declared_by_the_product_category(self):
        self.assertEqual(self.adapter.category_key, "scarf")
        self.assertEqual(len(self.qa_fields), 10)
        self.assertTrue(self.qa_rules)
        # Only the adapter's declared product fields are failure-bearing.
        self.assertTrue(set(PRODUCT_QA_FAILURE_FIELDS).issubset(set(self.qa_fields)))

    def test_qa_prompt_carries_the_scarf_checks_and_not_womenswear(self):
        prompt = PhotoReferenceVisionService._travel_qa_prompt(
            page_plans=self.looks, reference_count=2, image_count=4,
            product_reference_count=1, style_reference_count=1,
            product_context=dict(PRODUCT), travel_place="Seoul",
            qa_fields=self.qa_fields, qa_rules=self.qa_rules,
        )
        self.assertIn("【指定商品逐项判定】", prompt)
        for field in ("product_present", "visibility_sufficient",
                      "dominant_color_matches", "pattern_family_matches",
                      "edge_or_fringe_matches", "length_volume_plausible",
                      "face_unobscured"):
            self.assertIn(f'"{field}":true', prompt)
        self.assertIn("遮挡人物面部", prompt)
        # A category that declares no per-product QA keeps the legacy prompt.
        womenswear = adapter_for_product_category("outerwear")
        legacy = PhotoReferenceVisionService._travel_qa_prompt(
            page_plans=self.looks, reference_count=2, image_count=4,
            product_reference_count=1, style_reference_count=1,
            product_context=dict(PRODUCT), travel_place="Seoul",
            qa_fields=tuple(womenswear.qa_fields),
            qa_rules=tuple(womenswear.qa_rules),
        )
        self.assertNotIn("【指定商品逐项判定】", legacy)

    def test_all_true_passes_and_reports_every_product_field(self):
        result = self._normalize(self._raw())
        self.assertTrue(result["passed"])
        for page in result["roles"]:
            self.assertTrue(page["passed"], page["role"])
            self.assertEqual(
                set(page["product_qa"]), set(PRODUCT_QA_FAILURE_FIELDS))

    def test_missing_scarf_fails_the_page_and_asks_for_regeneration(self):
        """The behaviour the generic 配饰-is-MINOR rule used to swallow."""
        result = self._normalize(self._raw(look_a={"product_present": False}))
        self.assertFalse(result["passed"])
        failed = {page["role"]: page for page in result["roles"] if not page["passed"]}
        self.assertEqual(set(failed), {"look_a"})
        page = failed["look_a"]
        self.assertEqual(page["failure_code"], FAILURE_OUTFIT_MISMATCH)
        self.assertIn("缺失", page["repair_instruction"])
        self.assertFalse(page["product_qa"]["product_present"])
        # failed_roles_from_travel_qa() drives the targeted regeneration.
        self.assertEqual(
            failed_roles_from_travel_qa(result, ["look_a", "look_b", "look_c", "look_d"]),
            ["look_a"],
        )

    def test_scarf_over_the_face_is_a_person_disaster(self):
        result = self._normalize(self._raw(look_c={"face_unobscured": False}))
        page = next(p for p in result["roles"] if p["role"] == "look_c")
        self.assertFalse(page["passed"])
        self.assertEqual(page["failure_code"], FAILURE_PERSON_DISASTER)
        self.assertIn("面部", page["repair_instruction"])

    def test_restructured_scarf_fails_without_outfit_severity_relief(self):
        result = self._normalize(self._raw(
            look_b={"edge_or_fringe_matches": False, "outfit_severity": "MINOR"}))
        page = next(p for p in result["roles"] if p["role"] == "look_b")
        self.assertEqual(page["failure_code"], FAILURE_OUTFIT_MISMATCH)
        self.assertIn("流苏", page["repair_instruction"])

    def test_without_the_scarf_contract_the_same_response_passes(self):
        """Womenswear's empty qa_fields keep the historical leniency exactly."""
        raw = self._raw(look_a={"product_present": False})
        legacy = self._normalize(raw, qa_fields=())
        self.assertTrue(legacy["passed"])
        for page in legacy["roles"]:
            self.assertNotIn("product_qa", page)

    def test_missing_product_field_is_incomplete_qa_not_a_pass(self):
        raw = self._raw()
        del raw["pages"][0]["product_present"]
        with self.assertRaises(TravelSemanticQAError) as caught:
            self._normalize(raw)
        self.assertIn("product_present", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
